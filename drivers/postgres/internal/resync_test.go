package driver

import (
	"context"
	"crypto/tls"
	"fmt"
	"net/url"
	"sync"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/datazip-inc/olake/drivers/abstract"
	"github.com/datazip-inc/olake/drivers/postgres/internal/waljs"
	"github.com/datazip-inc/olake/types"
	"github.com/jackc/pglogrepl"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/require"
)

func TestValidateResyncAllowsCommittedCheckpointRecovery(t *testing.T) {
	p := &Postgres{CDCSupport: true, config: &Config{BackfillConnection: &url.URL{Host: "reader"}}}
	target := types.NewStream("items", "public", nil).Wrap(0)
	target.Stream.SyncMode = types.CDC
	// Marker recovery precedes fresh-attempt state validation and snapshot creation.
	require.NoError(t, p.ValidateResync(context.Background(), target))
	target.Stream.SyncMode = types.STRICTCDC
	require.ErrorContains(t, p.ValidateResync(context.Background(), target), "backfill-capable")
}

func TestResyncReplayUsesSnapshotBoundaryNotOldRecoveryLSN(t *testing.T) {
	p := &Postgres{resync: &resyncSnapshot{streamID: "public.target", boundary: pglogrepl.LSN(0x800)}}
	positions, err := p.resyncReplayAfter(map[string]any{
		"public.target": `{"lsn":"0/200"}`,
		"public.other":  `{"lsn":"0/500"}`,
		"public.third":  `{"lsn":"0/600"}`,
	})
	require.NoError(t, err)
	require.Equal(t, pglogrepl.LSN(0x800), positions["public.target"])
	require.Equal(t, pglogrepl.LSN(0x500), positions["public.other"])
	require.Equal(t, pglogrepl.LSN(0x600), positions["public.third"])
	positions, err = p.resyncReplayAfter(map[string]any{"public.target": `{"lsn":"0/900"}`})
	require.NoError(t, err)
	require.Equal(t, pglogrepl.LSN(0x900), positions["public.target"], "retry must not duplicate target CDC committed before a sibling close failed")
}

func TestResyncTargetSnapshotRoutesWriterWithBackfillHost(t *testing.T) {
	p, writer := newMockPostgres(t)
	reader, readerMock := newMockPostgres(t)
	configureBackfillTest(t, p)
	target := types.NewStream("items", "public", nil).Wrap(0)
	other := types.NewStream("other", "public", nil).Wrap(0)
	p.resync = &resyncSnapshot{streamID: target.ID(), name: "00000003-0000001B-1"}
	p.backfillClient = reader.client
	p.backfillBarrier = &readerBarrier{targets: map[string]int64{other.ID(): 100}}

	writer.ExpectBegin()
	writer.ExpectExec("SET TRANSACTION SNAPSHOT '00000003-0000001B-1'").WillReturnResult(sqlmock.NewResult(0, 0))
	writer.ExpectQuery("SELECT 1").WillReturnRows(sqlmock.NewRows([]string{"one"}).AddRow(1))
	writer.ExpectRollback()
	conn, err := p.backfillConn(context.Background(), target)
	require.NoError(t, err)
	tx, err := p.beginBackfillTx(context.Background(), conn, target)
	require.NoError(t, err)
	var one int
	require.NoError(t, tx.QueryRowContext(context.Background(), "SELECT 1").Scan(&one))
	require.NoError(t, tx.Rollback())
	closeBackfillConn(conn)

	readerMock.ExpectQuery(auroraReaderAppliedLSNQuery).WillReturnRows(sqlmock.NewRows([]string{"current_read_lsn"}).AddRow(100))
	readerMock.ExpectQuery("SELECT 2").WillReturnRows(sqlmock.NewRows([]string{"two"}).AddRow(2))
	conn, err = p.backfillConn(context.Background(), other)
	require.NoError(t, err)
	require.NoError(t, conn.QueryRowContext(context.Background(), "SELECT 2").Scan(&one))
	closeBackfillConn(conn)
	require.NoError(t, writer.ExpectationsWereMet())
	require.NoError(t, readerMock.ExpectationsWereMet())
}

func TestResyncIncrementalCursorUsesImportedSnapshot(t *testing.T) {
	p, mock := newMockPostgres(t)
	p.config = &Config{}
	target := types.NewStream("items", "public", nil).Wrap(0)
	target.Stream.CursorField = "updated_at:id"
	p.resync = &resyncSnapshot{streamID: target.ID(), name: "snapshot"}
	mock.ExpectBegin()
	mock.ExpectExec("SET TRANSACTION SNAPSHOT 'snapshot'").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectQuery(`SELECT MAX("updated_at"), MAX("id") FROM "public"."items"`).WillReturnRows(sqlmock.NewRows([]string{"max_time", "max_id"}).AddRow("2026-11-12", int64(9007199254740993)))
	mock.ExpectRollback()
	primary, secondary, err := p.FetchMaxCursorValues(context.Background(), target)
	require.NoError(t, err)
	require.Equal(t, "2026-11-12", primary)
	require.Equal(t, int64(9007199254740993), secondary)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestResyncNonCDCExportsOneSharedSnapshotWithoutReplication(t *testing.T) {
	p, _ := newMockPostgres(t)
	exporter, exportMock := newMockPostgres(t)
	p.config = &Config{}
	p.CDCSupport = true
	p.state = &types.State{RWMutex: &sync.RWMutex{}, Type: types.GlobalType}
	p.poolOpener = func(context.Context, *url.URL, *tls.Config) (*sqlx.DB, error) { return exporter.client, nil }
	target := types.NewStream("items", "public", nil).Wrap(0)
	target.Stream.SyncMode = types.FULLREFRESH
	exportMock.ExpectBegin()
	exportMock.ExpectQuery("SELECT pg_export_snapshot()").WillReturnRows(sqlmock.NewRows([]string{"snapshot"}).AddRow("shared-snapshot"))
	exportMock.ExpectRollback()
	exportMock.ExpectClose()
	require.NoError(t, p.PrepareResync(context.Background(), target))
	require.Equal(t, "shared-snapshot", p.resync.name)
	require.NoError(t, p.CleanupResync(context.Background()))
	require.NoError(t, exportMock.ExpectationsWereMet())
}

func TestRecoverPublishedResyncAcknowledgesOnlyCommittedCheckpoint(t *testing.T) {
	for _, slotLSN := range []string{"0/100", "0/300", "0/400"} {
		t.Run(slotLSN, func(t *testing.T) {
			p, mock := newMockPostgres(t)
			p.CDCSupport = true
			p.cdcConfig.ReplicationSlot = "saved_slot"
			checkpoint := &types.State{RWMutex: &sync.RWMutex{}, Global: &types.GlobalState{State: waljs.WALState{LSN: "0/300"}}}
			mock.ExpectQuery(fmt.Sprintf(waljs.ReplicationSlotTempl, "saved_slot")).WillReturnRows(sqlmock.NewRows([]string{"plugin", "slot_type", "confirmed_flush_lsn", "current_lsn"}).AddRow("pgoutput", "logical", slotLSN, "0/500"))
			if slotLSN == "0/100" {
				mock.ExpectQuery(fmt.Sprintf(waljs.AdvanceLSNTemplate, "saved_slot", "0/300")).WillReturnRows(sqlmock.NewRows([]string{"end_lsn"}).AddRow("0/300"))
			}
			ctx := types.WithResync(context.Background(), &types.ResyncOptions{RecoverOnly: true})
			err := p.RecoverResync(ctx, checkpoint, types.NewStream("items", "public", nil).Wrap(0))
			if slotLSN == "0/400" {
				require.ErrorContains(t, err, "ahead")
			} else {
				require.NoError(t, err)
			}
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

type resyncCheckpointReplicator struct {
	socket *waljs.Socket
}

func (r resyncCheckpointReplicator) Socket() *waljs.Socket { return r.socket }
func (resyncCheckpointReplicator) StreamChanges(context.Context, *sqlx.DB, abstract.CDCMsgFn) error {
	return nil
}

func TestResyncCheckpointRejectsIncompleteTransactions(t *testing.T) {
	for _, outcome := range []string{"complete", "open_transaction", "not_caught_up"} {
		t.Run(outcome, func(t *testing.T) {
			socket := &waljs.Socket{ClientXLogPos: 0x900, CurrentWalPosition: 0x800}
			if outcome == "open_transaction" {
				socket.TransactionOpen = true
			}
			if outcome == "not_caught_up" {
				socket.ClientXLogPos = 0x700
			}
			target := types.NewStream("items", "public", nil).Wrap(0)
			p := &Postgres{streams: []types.StreamInterface{target}, replicator: resyncCheckpointReplicator{socket: socket}}
			checkpoint := &types.State{RWMutex: &sync.RWMutex{}, Global: &types.GlobalState{State: waljs.WALState{LSN: "0/100"}}}
			err := p.ResyncCheckpoint(context.Background(), checkpoint)
			if outcome == "complete" {
				require.NoError(t, err)
				require.Equal(t, waljs.WALState{LSN: "0/900"}, checkpoint.Global.State)
			} else {
				require.ErrorContains(t, err, "complete CDC catchup")
				require.Equal(t, waljs.WALState{LSN: "0/100"}, checkpoint.Global.State)
			}
		})
	}
}

func TestResyncPreCDCWithoutGlobalCheckpointNeverAdvancesSlot(t *testing.T) {
	for _, missing := range []string{"global", "global-state"} {
		t.Run(missing, func(t *testing.T) {
			p, mock := newMockPostgres(t)
			p.CDCSupport = true
			p.cdcConfig.ReplicationSlot = "saved_slot"
			p.state = &types.State{RWMutex: &sync.RWMutex{}, Type: types.GlobalType, SuppressPersistence: true}
			if missing == "global-state" {
				p.state.Global = &types.GlobalState{Streams: types.NewSet("public.unrelated")}
			}
			originalGlobal := p.state.Global
			unrelated := &types.StreamState{Namespace: "public", Stream: "unrelated"}
			unrelated.HoldsValue.Store(true)
			unrelated.State.Store("cursor", "unchanged")
			p.state.Streams = []*types.StreamState{unrelated}
			mock.ExpectQuery(fmt.Sprintf(waljs.ReplicationSlotTempl, "saved_slot")).WillReturnRows(
				sqlmock.NewRows([]string{"plugin", "slot_type", "confirmed_flush_lsn", "current_lsn"}).
					AddRow("pgoutput", "logical", "0/100", "0/500"))
			ctx := types.WithResync(context.Background(), &types.ResyncOptions{Namespace: "public", Table: "target", ID: "operation"})
			err := p.PreCDC(ctx, nil)
			require.ErrorContains(t, err, "saved global CDC checkpoint")
			require.True(t, originalGlobal == p.state.Global)
			require.Equal(t, []*types.StreamState{unrelated}, p.state.Streams)
			cursor, ok := unrelated.State.Load("cursor")
			require.True(t, ok)
			require.Equal(t, "unchanged", cursor)
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}
