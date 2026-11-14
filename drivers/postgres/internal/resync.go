package driver

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/datazip-inc/olake/drivers/abstract"
	"github.com/datazip-inc/olake/drivers/postgres/internal/waljs"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"
)

type resyncSnapshot struct {
	streamID string
	name     string
	boundary pglogrepl.LSN
	tx       *sqlx.Tx
	pool     *sqlx.DB
	exporter *pgconn.PgConn
}

func (p *Postgres) ValidateResync(ctx context.Context, target types.StreamInterface) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if target.GetSyncMode() == types.STRICTCDC {
		return fmt.Errorf("atomic PostgreSQL resync requires a backfill-capable target")
	}
	if target.GetSyncMode() == types.CDC && !p.CDCSupport {
		return fmt.Errorf("atomic PostgreSQL CDC resync requires CDC configuration")
	}
	return nil
}

func (p *Postgres) PrepareResync(ctx context.Context, target types.StreamInterface) (err error) {
	if options, ok := types.ResyncFromContext(ctx); ok && options.RecoverOnly {
		return fmt.Errorf("recovery-only resync cannot prepare a source snapshot")
	}
	if err := utils.CheckExecutionFence(ctx); err != nil {
		return err
	}
	if err := p.ValidateResync(ctx, target); err != nil {
		return err
	}
	if target.GetSyncMode() == types.CDC {
		global := p.state.GetGlobal()
		if global == nil || global.State == nil {
			return fmt.Errorf("atomic PostgreSQL resync requires the full saved CDC checkpoint")
		}
		var saved waljs.WALState
		if err := utils.Unmarshal(global.State, &saved); err != nil {
			return fmt.Errorf("invalid saved resync checkpoint: %w", err)
		}
		if saved.LSN == "" {
			return fmt.Errorf("atomic PostgreSQL resync requires a saved CDC LSN")
		}
		if _, err := pglogrepl.ParseLSN(saved.LSN); err != nil {
			return fmt.Errorf("invalid saved resync LSN: %w", err)
		}
	}
	if p.resync != nil {
		return fmt.Errorf("a PostgreSQL resync snapshot is already active")
	}
	snapshot := &resyncSnapshot{streamID: target.ID()}
	p.resync = snapshot
	defer func() {
		if err != nil {
			err = errors.Join(err, p.CleanupResync(context.WithoutCancel(ctx)))
		}
	}()
	if target.GetSyncMode() == types.CDC {
		slot, err := waljs.GetSlotPosition(ctx, p.client, p.cdcConfig.ReplicationSlot)
		if err != nil {
			return err
		}
		if slot.Plugin != "pgoutput" && slot.Plugin != "wal2json" {
			return fmt.Errorf("atomic PostgreSQL resync does not support output plugin %q", slot.Plugin)
		}
		config, err := p.prepareWALJSConfig(target)
		if err != nil {
			return err
		}
		snapshot.exporter, err = waljs.ConnectReplication(ctx, config)
		if err != nil {
			return err
		}
		// The existing job slot remains untouched. The temporary slot exports an exact
		// snapshot/commit boundary using replication privilege, with no table write lock.
		name := "olake_resync_" + strings.ToLower(utils.ULID())
		if err := utils.CheckExecutionFence(ctx); err != nil {
			return err
		}
		created, err := pglogrepl.CreateReplicationSlot(ctx, snapshot.exporter, name, slot.Plugin, pglogrepl.CreateReplicationSlotOptions{
			Temporary: true, Mode: pglogrepl.LogicalReplication, SnapshotAction: "EXPORT_SNAPSHOT",
		})
		if err != nil {
			return fmt.Errorf("failed to export resync snapshot; one spare logical replication slot is required: %w", err)
		}
		snapshot.name = created.SnapshotName
		snapshot.boundary, err = pglogrepl.ParseLSN(created.ConsistentPoint)
		if err != nil {
			return err
		}
		if snapshot.name == "" {
			return fmt.Errorf("temporary logical slot returned no resync snapshot")
		}
		return nil
	}
	tlsConfig, err := p.config.buildTLSConfig()
	if err != nil {
		return err
	}
	// A dedicated exporter leaves every configured read connection available to chunks.
	snapshot.pool, err = p.openPool(ctx, p.config.Connection, tlsConfig)
	if err != nil {
		return err
	}
	snapshot.pool.SetMaxOpenConns(1)
	snapshot.tx, err = snapshot.pool.BeginTxx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	if err != nil {
		return err
	}
	if err := utils.CheckExecutionFence(ctx); err != nil {
		return err
	}
	if err = snapshot.tx.QueryRowContext(ctx, "SELECT pg_export_snapshot()").Scan(&snapshot.name); err != nil {
		return fmt.Errorf("failed to export resync snapshot: %w", err)
	}
	return nil
}

func (p *Postgres) beginBackfillTx(ctx context.Context, conn *sqlx.Conn, stream types.StreamInterface) (*sql.Tx, error) {
	tx, err := conn.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead})
	if err != nil {
		return nil, err
	}
	if p.resync != nil && p.resync.streamID == stream.ID() {
		if _, err := tx.ExecContext(ctx, "SET TRANSACTION SNAPSHOT "+pq.QuoteLiteral(p.resync.name)); err != nil {
			_ = tx.Rollback()
			return nil, fmt.Errorf("failed to import target resync snapshot: %w", err)
		}
	}
	return tx, nil
}

func (p *Postgres) streamResyncChanges(ctx context.Context, saved waljs.WALState, metadata map[string]any, callback abstract.CDCMsgFn) (_ any, err error) {
	defer func() {
		if err != nil && p.replicator != nil {
			err = errors.Join(err, waljs.Cleanup(context.WithoutCancel(ctx), p.replicator.Socket()))
			p.replicator = nil
		}
	}()
	if p.resync == nil {
		return nil, fmt.Errorf("resync CDC requires a prepared source snapshot")
	}
	// Every target snapshot scan has finished before sequential CDC starts. Releasing
	// the exporter also drops its temporary slot before opening the job's WAL sender.
	if p.resync.exporter != nil {
		closeErr := p.resync.exporter.Close(ctx)
		p.resync.exporter = nil
		if closeErr != nil {
			return nil, fmt.Errorf("failed to release resync snapshot exporter: %w", closeErr)
		}
	}
	slot, err := waljs.GetSlotPosition(ctx, p.client, p.cdcConfig.ReplicationSlot)
	if err != nil {
		return nil, err
	}
	if err := validateGlobalState(saved, slot.LSN); err != nil {
		return nil, err
	}
	config, err := p.prepareWALJSConfig(p.streams...)
	if err != nil {
		return nil, err
	}
	config.ReplayAfter, err = p.resyncReplayAfter(metadata)
	if err != nil {
		return nil, err
	}
	// Never bound a fresh replacement by an old table's crash-recovery metadata.
	// Unrelated committed tables skip only their own already-published transactions.
	p.replicator, err = waljs.NewReplicator(ctx, config, slot, nil, p.dataTypeConverter, p.columnType)
	if err != nil {
		return nil, err
	}
	if err := p.replicator.StreamChanges(ctx, p.client, callback); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if p.replicator.Socket().TransactionOpen || p.replicator.Socket().ClientXLogPos < p.replicator.Socket().CurrentWalPosition {
		return nil, fmt.Errorf("resync CDC ended before reaching its post-snapshot WAL boundary")
	}
	return waljs.WALState{LSN: p.replicator.Socket().ClientXLogPos.String()}, nil
}

func (p *Postgres) resyncReplayAfter(metadata map[string]any) (map[string]pglogrepl.LSN, error) {
	positions := make(map[string]pglogrepl.LSN, len(metadata)+1)
	for streamID, raw := range metadata {
		if raw == nil {
			continue
		}
		encoded, ok := raw.(string)
		if !ok {
			return nil, fmt.Errorf("invalid CDC metadata for stream %s: expected string, got %T", streamID, raw)
		}
		var state waljs.WALState
		if err := json.Unmarshal([]byte(encoded), &state); err != nil {
			return nil, fmt.Errorf("invalid CDC metadata for stream %s: %w", streamID, err)
		}
		position, err := pglogrepl.ParseLSN(state.LSN)
		if err != nil {
			return nil, fmt.Errorf("invalid CDC metadata LSN for stream %s: %w", streamID, err)
		}
		positions[streamID] = position
	}
	// Prepare cleared the old target metadata. A later target checkpoint belongs
	// to this staged attempt and may already have committed before a sibling failed.
	positions[p.resync.streamID] = max(positions[p.resync.streamID], p.resync.boundary)
	return positions, nil
}

func (p *Postgres) ResyncCheckpoint(ctx context.Context, checkpoint *types.State) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(p.streams) == 0 {
		return nil
	}
	if p.replicator == nil {
		return fmt.Errorf("resync CDC finished without a final WAL position")
	}
	if p.replicator.Socket().TransactionOpen || p.replicator.Socket().ClientXLogPos < p.replicator.Socket().CurrentWalPosition {
		return fmt.Errorf("resync checkpoint requires complete CDC catchup")
	}
	checkpoint.Lock()
	defer checkpoint.Unlock()
	if checkpoint.Global == nil {
		return fmt.Errorf("resync CDC checkpoint has no global state")
	}
	checkpoint.Global.State = waljs.WALState{LSN: p.replicator.Socket().ClientXLogPos.String()}
	return nil
}

func (p *Postgres) RecoverResync(ctx context.Context, checkpoint *types.State, _ types.StreamInterface) error {
	if err := utils.CheckExecutionFence(ctx); err != nil {
		return err
	}
	if !p.CDCSupport {
		return nil
	}
	if checkpoint == nil || checkpoint.Global == nil || checkpoint.Global.State == nil {
		// CDC configuration does not imply this saved catalog selected any CDC streams.
		// The protocol validates the published marker and full checkpoint before recovery.
		return nil
	}
	var committed waljs.WALState
	if err := utils.Unmarshal(checkpoint.Global.State, &committed); err != nil {
		return fmt.Errorf("failed to read published resync checkpoint: %w", err)
	}
	target, err := pglogrepl.ParseLSN(committed.LSN)
	if err != nil {
		return err
	}
	slot, err := waljs.GetSlotPosition(ctx, p.client, p.cdcConfig.ReplicationSlot)
	if err != nil {
		return err
	}
	if slot.LSN > target {
		return fmt.Errorf("replication slot %s is ahead of published resync checkpoint %s; refusing to restore an older checkpoint", slot.LSN, target)
	}
	if slot.LSN == target {
		return nil
	}
	confirmed, err := waljs.AdvanceLSN(ctx, p.client, p.cdcConfig.ReplicationSlot, target.String())
	if err != nil {
		return err
	}
	if confirmed != target {
		return fmt.Errorf("replication slot acknowledged %s instead of published checkpoint %s", confirmed, target)
	}
	return nil
}

func (p *Postgres) CleanupResync(ctx context.Context) error {
	var err error
	if p.replicator != nil {
		err = waljs.Cleanup(ctx, p.replicator.Socket())
		p.replicator = nil
	}
	snapshot := p.resync
	p.resync = nil
	if snapshot == nil {
		return err
	}
	if snapshot.tx != nil {
		if rollbackErr := snapshot.tx.Rollback(); rollbackErr != nil && !errors.Is(rollbackErr, sql.ErrTxDone) {
			err = errors.Join(err, rollbackErr)
		}
	}
	if snapshot.exporter != nil {
		err = errors.Join(err, snapshot.exporter.Close(ctx))
	}
	if snapshot.pool != nil {
		err = errors.Join(err, snapshot.pool.Close())
	}
	return err
}
