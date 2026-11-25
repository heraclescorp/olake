package driver

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/datazip-inc/olake/drivers/abstract"
	"github.com/datazip-inc/olake/drivers/postgres/internal/waljs"
	"github.com/datazip-inc/olake/types"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"
	"github.com/stretchr/testify/require"
)

// This source-only smoke requires an explicitly supplied disposable database with
// wal_level=logical and publication/replication-slot privileges. It never runs by
// default and does not substitute for the CLI/native-destination recovery smoke.
func TestOrdinaryRecoveryBoundedReplayIntegration(t *testing.T) {
	for _, plugin := range []string{"pgoutput", "wal2json"} {
		for _, checkpointed := range []bool{false, true} {
			for _, allCovered := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/checkpointed=%t/all_tables_covered=%t", plugin, checkpointed, allCovered), func(t *testing.T) {
					testOrdinaryRecoveryBoundedReplay(t, plugin, allCovered, checkpointed, false)
				})
			}
		}
	}
}

func TestOrdinaryRecoveryWal2JSONStopsBeforeBufferedFutureTransactionIntegration(t *testing.T) {
	testOrdinaryRecoveryBoundedReplay(t, "wal2json", false, false, true)
}

func testOrdinaryRecoveryBoundedReplay(t *testing.T, plugin string, allCovered, checkpointed, betweenTransactions bool) {
	dsn := os.Getenv("OLAKE_POSTGRES_TEST_DSN")
	if dsn == "" {
		t.Skip("set OLAKE_POSTGRES_TEST_DSN to a disposable PostgreSQL database with wal_level=logical")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	db, err := sqlx.ConnectContext(ctx, "pgx", dsn)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	name := fmt.Sprintf("olake_ordinary_%d", time.Now().UnixNano())
	names := []string{name + "_first", name + "_second"}
	for _, table := range names {
		_, err := db.ExecContext(ctx, "CREATE TABLE public."+pq.QuoteIdentifier(table)+" (id integer PRIMARY KEY, value text)")
		require.NoError(t, err)
		table := table
		t.Cleanup(func() {
			cleanup, stop := context.WithTimeout(context.Background(), 10*time.Second)
			defer stop()
			_, err := db.ExecContext(cleanup, "DROP TABLE IF EXISTS public."+pq.QuoteIdentifier(table))
			require.NoError(t, err)
		})
	}
	_, err = db.ExecContext(ctx, "CREATE PUBLICATION "+pq.QuoteIdentifier(name)+" FOR TABLE public."+pq.QuoteIdentifier(names[0])+", public."+pq.QuoteIdentifier(names[1]))
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanup, stop := context.WithTimeout(context.Background(), 10*time.Second)
		defer stop()
		_, err := db.ExecContext(cleanup, "DROP PUBLICATION "+pq.QuoteIdentifier(name))
		require.NoError(t, err)
	})
	var lower string
	err = db.GetContext(ctx, &lower, "SELECT lsn::text FROM pg_create_logical_replication_slot($1, $2)", name, plugin)
	var pluginErr *pgconn.PgError
	if plugin == "wal2json" && errors.As(err, &pluginErr) && pluginErr.Code == "58P01" {
		t.Skip("wal2json output plugin is not installed in the disposable PostgreSQL database")
	}
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanup, stop := context.WithTimeout(context.Background(), 10*time.Second)
		defer stop()
		_, err := db.ExecContext(cleanup, "SELECT pg_drop_replication_slot($1)", name)
		require.NoError(t, err)
	})
	insertBoth := func(id int) {
		tx, err := db.BeginTxx(ctx, nil)
		require.NoError(t, err)
		defer tx.Rollback()
		for _, table := range names {
			_, err := tx.ExecContext(ctx, "INSERT INTO public."+pq.QuoteIdentifier(table)+" VALUES ($1, $2)", id, fmt.Sprintf("row-%d", id))
			require.NoError(t, err)
		}
		require.NoError(t, tx.Commit())
	}
	insertBoth(1)
	if checkpointed {
		insertBoth(2)
		insertBoth(3)
	}
	// Peek (not consume/ACK) the plugin's complete COMMIT end, rather than
	// deriving a replay boundary from wall-clock time or the current WAL tip.
	peekQuery := "SELECT data FROM pg_logical_slot_peek_binary_changes($1, NULL, NULL, 'proto_version', '1', 'publication_names', $2)"
	peekArgs := []any{name, name}
	if plugin == "wal2json" {
		peekQuery = "SELECT data FROM pg_logical_slot_peek_binary_changes($1, NULL, NULL, 'include-lsn', 'on', 'include-timestamp', 'on')"
		peekArgs = []any{name}
	}
	rows, err := db.QueryxContext(ctx, peekQuery, peekArgs...)
	require.NoError(t, err)
	var target pglogrepl.LSN
	for rows.Next() {
		var data []byte
		require.NoError(t, rows.Scan(&data))
		if plugin == "pgoutput" {
			message, err := pglogrepl.Parse(data)
			require.NoError(t, err)
			if commit, ok := message.(*pglogrepl.CommitMessage); ok {
				target = commit.TransactionEndLSN
			}
		} else {
			var transaction waljs.WALMessage
			require.NoError(t, json.Unmarshal(data, &transaction))
			target, err = pglogrepl.ParseLSN(transaction.NextLSN)
			require.NoError(t, err)
		}
	}
	require.NoError(t, rows.Err())
	require.NoError(t, rows.Close())
	require.NotZero(t, target)
	if betweenTransactions {
		lastCommit := target
		_, err = db.ExecContext(ctx, "CHECKPOINT")
		require.NoError(t, err)
		var rawTarget string
		require.NoError(t, db.GetContext(ctx, &rawTarget, "SELECT pg_current_wal_lsn()::text"))
		target, err = pglogrepl.ParseLSN(rawTarget)
		require.NoError(t, err)
		require.Greater(t, target, lastCommit)
	}
	if checkpointed {
		insertBoth(4)
	} else {
		insertBoth(2)
	}

	connection, err := url.Parse(dsn)
	require.NoError(t, err)
	var database string
	require.NoError(t, db.GetContext(ctx, &database, "SELECT current_database()"))
	var streams []types.StreamInterface
	for _, table := range names {
		stream := types.NewStream(table, "public", nil).Wrap(0)
		stream.Stream.SyncMode = types.CDC
		stream.Stream.SourceDefinedPrimaryKey.Insert("id")
		stream.Stream.UpsertField("id", types.Int32, false, false)
		stream.Stream.UpsertField("value", types.String, false, false)
		streams = append(streams, stream)
	}
	baseline := map[string]any{"lsn": lower, "opaque": json.Number("9007199254740993")}
	state := &types.State{RWMutex: &sync.RWMutex{}, Type: types.GlobalType, SuppressPersistence: true,
		Global: &types.GlobalState{State: baseline, Streams: types.NewSet(streams[0].ID(), streams[1].ID(), "public.unrelated")}}
	newSource := func() *Postgres {
		return &Postgres{client: db, config: &Config{Connection: connection, Host: connection.Hostname(), Database: database}, CDCSupport: true,
			cdcConfig: CDC{ReplicationSlot: name, Publication: name, InitialWaitTime: 20}, state: state, typeMapping: pgTypeToDataTypes()}
	}
	p := newSource()
	t.Cleanup(func() { require.NoError(t, p.CleanupOrdinaryRecovery(context.Background())) })
	options := &types.OrdinaryRecoveryOptions{}
	if checkpointed {
		options.CheckpointInterval = time.Nanosecond
	}
	recoveryCtx := types.WithOrdinaryRecovery(ctx, options)
	options.SourceIdentity, err = p.ValidateOrdinaryRecovery(recoveryCtx, streams, lower)
	require.NoError(t, err)
	positions := map[string]string{streams[0].ID(): lower, streams[1].ID(): target.String()}
	if allCovered {
		positions[streams[0].ID()] = target.String()
	}
	metadata := make(map[string]any, len(streams))
	var got []abstract.CDCChange
	parsedLower, err := pglogrepl.ParseLSN(lower)
	require.NoError(t, err)
	previousAck, expectedStart := parsedLower, parsedLower
	batches := 0
	for {
		options.Plan, err = p.PrepareOrdinaryRecovery(recoveryCtx, streams, lower, positions)
		require.NoError(t, err)
		require.Equal(t, target.String(), options.Plan.TargetLSN)
		require.Empty(t, options.Plan.BatchTargetLSN)
		require.NoError(t, p.PreCDC(recoveryCtx, streams))
		for stream, position := range positions {
			metadata[stream] = fmt.Sprintf(`{"lsn":%q}`, position)
		}
		final, err := p.StreamChanges(recoveryCtx, 0, metadata, func(_ context.Context, change abstract.CDCChange) error {
			if change.SourceColumns == nil {
				got = append(got, change)
			}
			return nil
		})
		require.NoError(t, err)
		batch, err := pglogrepl.ParseLSN(options.Plan.BatchTargetLSN)
		require.NoError(t, err)
		require.Equal(t, waljs.WALState{LSN: batch.String()}, final)
		require.LessOrEqual(t, batch, target)
		if p.replicator != nil {
			require.Equal(t, expectedStart, p.replicator.Socket().RecoveryStartLSN)
			require.Equal(t, previousAck, p.replicator.Socket().ConfirmedFlushLSN)
		}
		slot, err := waljs.GetSlotPosition(ctx, db, name)
		require.NoError(t, err)
		require.Equal(t, previousAck, slot.LSN)
		batches++
		require.LessOrEqual(t, batches, 3)
		if !allCovered {
			positions[streams[0].ID()] = batch.String()
		}
		if checkpointed && !allCovered && batches == 1 {
			require.Less(t, batch, target)
			require.Equal(t, baseline, state.Global.State)
			require.NoError(t, p.CleanupOrdinaryRecovery(ctx))
			state.Global.State, err = ordinaryRecoveryCheckpoint(state.Global.State, batch.String())
			require.NoError(t, err)
			p = newSource()
			options = &types.OrdinaryRecoveryOptions{SourceIdentity: options.SourceIdentity, CheckpointLSN: batch.String(), CheckpointInterval: time.Nanosecond,
				Plan: &types.OrdinaryRecoveryPlan{LowerLSN: lower, TargetLSN: target.String()}}
			recoveryCtx = types.WithOrdinaryRecovery(ctx, options)
			identity, err := p.ValidateOrdinaryRecovery(recoveryCtx, streams, lower)
			require.NoError(t, err)
			require.Equal(t, options.SourceIdentity, identity)
		} else {
			require.NoError(t, p.AcknowledgeOrdinaryRecovery(recoveryCtx, options.Plan))
			previousAck = batch
			require.Equal(t, batch.String(), state.Global.State.(map[string]any)["lsn"])
			require.NoError(t, p.CleanupOrdinaryRecovery(ctx))
			options.CheckpointLSN = batch.String()
		}
		expectedStart = batch
		if batch == target {
			break
		}
	}
	if allCovered {
		require.Empty(t, got)
	} else {
		expectedRows := 1
		if checkpointed {
			expectedRows = 3
			require.Equal(t, 3, batches)
		}
		require.Len(t, got, expectedRows)
		for index, change := range got {
			require.Equal(t, streams[0].ID(), change.Stream.ID())
			require.EqualValues(t, index+1, change.Data["id"])
		}
	}
	require.Equal(t, target.String(), state.Global.State.(map[string]any)["lsn"])
	require.Equal(t, baseline["opaque"], state.Global.State.(map[string]any)["opaque"])

	// Simulate losing the worker checkpoint after ACK. Every durable destination
	// table now covers target, while the immutable saved baseline still says lower.
	state.Global.State = baseline
	retry := newSource()
	t.Cleanup(func() { require.NoError(t, retry.CleanupOrdinaryRecovery(context.Background())) })
	retryOptions := &types.OrdinaryRecoveryOptions{}
	retryCtx := types.WithOrdinaryRecovery(ctx, retryOptions)
	retryOptions.SourceIdentity, err = retry.ValidateOrdinaryRecovery(retryCtx, streams, lower)
	require.NoError(t, err)
	require.Equal(t, options.SourceIdentity, retryOptions.SourceIdentity)
	retryOptions.Plan, err = retry.PrepareOrdinaryRecovery(retryCtx, streams, lower, map[string]string{streams[0].ID(): target.String(), streams[1].ID(): target.String()})
	require.NoError(t, err)
	for stream := range metadata {
		metadata[stream] = fmt.Sprintf(`{"lsn":%q}`, target.String())
	}
	_, err = retry.StreamChanges(retryCtx, 0, metadata, func(context.Context, abstract.CDCChange) error {
		return fmt.Errorf("already covered recovery emitted a fresh event")
	})
	require.NoError(t, err)
	require.NoError(t, retry.AcknowledgeOrdinaryRecovery(retryCtx, retryOptions.Plan))
	require.Equal(t, target.String(), state.Global.State.(map[string]any)["lsn"])
	require.Equal(t, 3, state.Global.Streams.Len())
	require.True(t, state.Global.Streams.Exists("public.unrelated"))
	slot, err := waljs.GetSlotPosition(ctx, db, name)
	require.NoError(t, err)
	require.Equal(t, target, slot.LSN, "future transaction must remain unacknowledged")
}
