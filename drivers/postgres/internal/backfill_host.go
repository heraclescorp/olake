package driver

import (
	"context"
	"database/sql"
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils/errs"
	"github.com/datazip-inc/olake/utils/logger"
	"github.com/jmoiron/sqlx"
)

// Aurora log sequence numbers are a different sequence from PostgreSQL WAL LSNs (replication slot,
// pg_current_wal_lsn), so they are only ever compared with each other: the writer's durable_lsn
// against a reader's own current_read_lsn.
const (
	auroraInstanceQuery          = `SELECT aurora_db_instance_identifier(), pg_is_in_recovery()`
	auroraClusterInstancesQuery  = `SELECT server_id FROM aurora_replica_status()`
	auroraWriterDurableLSNQuery  = `SELECT durable_lsn::bigint FROM aurora_replica_status() WHERE session_id = 'MASTER_SESSION_ID'`
	auroraReaderAppliedLSNQuery  = `SELECT current_read_lsn::bigint FROM aurora_replica_status() WHERE server_id = aurora_db_instance_identifier()`
	readerCatchUpTimeout         = 15 * time.Minute
	readerCatchUpPollInterval    = 100 * time.Millisecond
	readerCatchUpProgressLogging = 10 * time.Second
)

// sourceQuerier is satisfied by both a connection pool and a single connection.
type sourceQuerier interface {
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

// readerBarrier holds, per stream, the writer's Aurora durable_lsn captured the first time this run's
// backfill of the stream needs the backfill host. Everything that backfill must not miss is settled on
// the writer before then: the CDC bookmark (PreCDC) and an incremental stream's max cursor
// (FetchMaxCursorValues). A reader that has applied the Aurora log through that point serves new
// snapshots containing all of it.
type readerBarrier struct {
	writer  *sqlx.DB
	mu      sync.Mutex
	targets map[string]int64
}

func newReaderBarrier(writer *sqlx.DB) *readerBarrier {
	return &readerBarrier{writer: writer, targets: make(map[string]int64)}
}

func (b *readerBarrier) targetFor(ctx context.Context, streamID string) (int64, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if target, captured := b.targets[streamID]; captured {
		return target, nil
	}
	target, err := writerDurableLSN(ctx, b.writer)
	if err != nil {
		return 0, err
	}
	b.targets[streamID] = target
	logger.Infof("stream[%s]: backfill reads wait until the backfill host has applied the writer's Aurora durable_lsn %d", streamID, target)
	return target, nil
}

func writerDurableLSN(ctx context.Context, writer sourceQuerier) (int64, error) {
	var durable sql.NullInt64
	if err := writer.QueryRowContext(ctx, auroraWriterDurableLSNQuery).Scan(&durable); err != nil {
		return 0, fmt.Errorf("failed to read the writer's Aurora durable_lsn: %w", err)
	}
	if !durable.Valid {
		return 0, errs.Precondition(errs.UnsupportedFeature, codeAuroraLSNUnavailable,
			fmt.Errorf("aurora_replica_status reports no durable_lsn for the writer"))
	}
	return durable.Int64, nil
}

func readAppliedLSN(ctx context.Context, reader sourceQuerier) (int64, error) {
	var applied sql.NullInt64
	if err := reader.QueryRowContext(ctx, auroraReaderAppliedLSNQuery).Scan(&applied); err != nil {
		return 0, fmt.Errorf("failed to read the backfill host's Aurora current_read_lsn: %w", err)
	}
	if !applied.Valid {
		return 0, errs.Precondition(errs.UnsupportedFeature, codeAuroraLSNUnavailable,
			fmt.Errorf("aurora_replica_status reports no current_read_lsn for the backfill host instance"))
	}
	return applied.Int64, nil
}

// waitForAppliedLSN polls the reader's applied position until it reaches target, returning the last
// applied position and how long it waited. The timeout bounds a stuck or far-behind reader; it never
// lets a read start before the reader has caught up.
func waitForAppliedLSN(ctx context.Context, readApplied func(context.Context) (int64, error), target int64, timeout, pollInterval time.Duration) (int64, time.Duration, error) {
	start := time.Now()
	lastProgressLog := start
	for {
		applied, err := readApplied(ctx)
		if err != nil {
			return 0, time.Since(start), err
		}
		if applied >= target {
			return applied, time.Since(start), nil
		}
		waited := time.Since(start)
		if waited >= timeout {
			return applied, waited, fmt.Errorf("backfill host applied Aurora LSN %d did not reach the writer's durable_lsn %d within %s", applied, target, timeout)
		}
		if time.Since(lastProgressLog) >= readerCatchUpProgressLogging {
			logger.Infof("waiting for the backfill host to apply Aurora LSN %d: applied %d after %s", target, applied, waited.Round(time.Second))
			lastProgressLog = time.Now()
		}
		timer := time.NewTimer(pollInterval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return applied, time.Since(start), ctx.Err()
		case <-timer.C:
		}
	}
}

type backfillInitialization struct {
	done   chan struct{}
	cancel context.CancelFunc
	err    error
}

func (p *Postgres) CheckConnection(ctx context.Context) error {
	return p.ensureBackfillHost(ctx)
}

func (p *Postgres) ensureBackfillHost(ctx context.Context) error {
	if p.config.BackfillConnection == nil {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	p.backfillMu.Lock()
	if p.backfillClosed {
		p.backfillMu.Unlock()
		return sql.ErrConnDone
	}
	if p.backfillBarrier != nil {
		p.backfillMu.Unlock()
		return nil
	}
	if init := p.backfillInit; init != nil {
		p.backfillMu.Unlock()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-init.done:
			return init.err
		}
	}
	ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	init := &backfillInitialization{done: make(chan struct{}), cancel: cancel}
	p.backfillInit = init
	p.backfillMu.Unlock()

	reader, err := p.setupBackfillHost(ctx)
	p.backfillMu.Lock()
	if err == nil {
		err = ctx.Err()
		if err == nil && p.backfillClosed {
			err = sql.ErrConnDone
		}
		if err == nil {
			p.backfillClient = reader
			p.backfillBarrier = newReaderBarrier(p.client)
		} else {
			p.backfillMu.Unlock()
			if closeErr := reader.Close(); closeErr != nil {
				logger.Warnf("failed to close canceled backfill host pool: %s", closeErr)
			}
			p.backfillMu.Lock()
		}
	}
	init.err = err
	p.backfillInit = nil
	close(init.done)
	p.backfillMu.Unlock()
	return err
}

// setupBackfillHost connects to BackfillHost and refuses anything but a reader of the writer's own
// Aurora cluster, so table copies never silently come from the writer or from another database.
func (p *Postgres) setupBackfillHost(ctx context.Context) (_ *sqlx.DB, err error) {
	tlsConfig, err := p.config.buildBackfillTLSConfig()
	if err != nil {
		return nil, fmt.Errorf("failed to build backfill host tls config: %w", err)
	}
	reader, err := p.openPool(ctx, p.config.BackfillConnection, tlsConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to backfill host %s: %w", p.config.BackfillHost, err)
	}
	defer func() {
		if err != nil {
			if closeErr := reader.Close(); closeErr != nil {
				logger.Warnf("failed to close invalid backfill host pool: %s", closeErr)
			}
		}
	}()

	var instance string
	var inRecovery bool
	if err := reader.QueryRowContext(ctx, auroraInstanceQuery).Scan(&instance, &inRecovery); err != nil {
		return nil, fmt.Errorf("backfill host %s must be an Aurora PostgreSQL reader instance: %w", p.config.BackfillHost, err)
	}
	if !inRecovery {
		return nil, errs.Precondition(errs.ConfigInvalid, codeBackfillHostIsWriter,
			fmt.Errorf("backfill host %s is Aurora instance %s, which is the writer; use a reader instance endpoint", p.config.BackfillHost, instance))
	}

	var clusterInstances []string
	if err := p.client.SelectContext(ctx, &clusterInstances, auroraClusterInstancesQuery); err != nil {
		return nil, fmt.Errorf("failed to list Aurora cluster instances on the writer: %w", err)
	}
	if !slices.Contains(clusterInstances, instance) {
		return nil, errs.Precondition(errs.ConfigInvalid, codeBackfillHostOutsideCluster,
			fmt.Errorf("backfill host %s is Aurora instance %s, which is not in the writer's cluster (instances: %s)",
				p.config.BackfillHost, instance, strings.Join(clusterInstances, ", ")))
	}

	applied, err := readAppliedLSN(ctx, reader)
	if err != nil {
		return nil, err
	}
	durable, err := writerDurableLSN(ctx, p.client)
	if err != nil {
		return nil, err
	}
	logger.Infof("backfill host %s is Aurora reader %s of the writer's cluster: current_read_lsn=%d, writer durable_lsn=%d",
		p.config.BackfillHost, instance, applied, durable)
	return reader, nil
}

// backfillConn returns a dedicated backfill connection. With an Aurora reader configured, its instance
// has applied the Aurora log through the stream's barrier before it is returned, so the caller's next
// snapshot on the same connection contains every commit the writer had made durable by then.
func (p *Postgres) backfillConn(ctx context.Context, stream types.StreamInterface) (*sqlx.Conn, error) {
	if err := p.ensureBackfillHost(ctx); err != nil {
		return nil, err
	}
	conn, err := p.backfillClient.Connx(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to acquire a backfill connection: %w", err)
	}
	if p.backfillBarrier == nil {
		return conn, nil
	}

	target, err := p.backfillBarrier.targetFor(ctx, stream.ID())
	if err != nil {
		closeBackfillConn(conn)
		return nil, err
	}
	applied, waited, err := waitForAppliedLSN(ctx, func(ctx context.Context) (int64, error) {
		return readAppliedLSN(ctx, conn)
	}, target, readerCatchUpTimeout, readerCatchUpPollInterval)
	if err != nil {
		closeBackfillConn(conn)
		return nil, fmt.Errorf("stream[%s]: backfill host is not caught up with the writer: %w", stream.ID(), err)
	}
	if waited > readerCatchUpPollInterval {
		logger.Infof("stream[%s]: backfill host caught up after %s (applied %d >= writer durable_lsn %d)", stream.ID(), waited.Round(time.Millisecond), applied, target)
	} else {
		logger.Debugf("stream[%s]: backfill host already caught up (applied %d >= writer durable_lsn %d)", stream.ID(), applied, target)
	}
	return conn, nil
}

func closeBackfillConn(conn *sqlx.Conn) {
	if err := conn.Close(); err != nil {
		logger.Warnf("failed to release backfill connection: %s", err)
	}
}
