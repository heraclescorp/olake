package driver

import (
	"context"
	"encoding/json"
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
	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"
	"github.com/stretchr/testify/require"
)

// This source-only smoke requires an explicitly supplied disposable database with
// wal_level=logical and publication/replication-slot privileges. It never runs by
// default and does not substitute for the CLI/native-destination recovery smoke.
func TestOrdinaryRecoveryBoundedReplayIntegration(t *testing.T) {
	for _, allCovered := range []bool{false, true} {
		t.Run(fmt.Sprintf("all_tables_covered=%t", allCovered), func(t *testing.T) {
			testOrdinaryRecoveryBoundedReplay(t, allCovered, false)
		})
	}
	t.Run("resume_after_intermediate_ack", func(t *testing.T) {
		testOrdinaryRecoveryBoundedReplay(t, false, true)
	})
}

func testOrdinaryRecoveryBoundedReplay(t *testing.T, allCovered, intermediateACK bool) {
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
	require.NoError(t, db.GetContext(ctx, &lower, "SELECT lsn::text FROM pg_create_logical_replication_slot($1, 'pgoutput')", name))
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
	// Peek (not consume/ACK) the plugin's complete COMMIT end, rather than
	// deriving a replay boundary from wall-clock time or the current WAL tip.
	peekTarget := func() pglogrepl.LSN {
		rows, err := db.QueryxContext(ctx, "SELECT data FROM pg_logical_slot_peek_binary_changes($1, NULL, NULL, 'proto_version', '1', 'publication_names', $2)", name, name)
		require.NoError(t, err)
		defer rows.Close()
		var target pglogrepl.LSN
		for rows.Next() {
			var data []byte
			require.NoError(t, rows.Scan(&data))
			message, err := pglogrepl.Parse(data)
			require.NoError(t, err)
			if commit, ok := message.(*pglogrepl.CommitMessage); ok {
				target = commit.TransactionEndLSN
			}
		}
		require.NoError(t, rows.Err())
		require.NotZero(t, target)
		return target
	}
	target := peekTarget()
	replayAfter := lower
	wantID := 1
	if intermediateACK {
		replayAfter = target.String()
		var advanced string
		require.NoError(t, db.GetContext(ctx, &advanced, "SELECT end_lsn::text FROM pg_replication_slot_advance($1, $2::pg_lsn)", name, replayAfter))
		require.Equal(t, replayAfter, advanced)
		insertBoth(2)
		target = peekTarget()
		wantID = 2
	}
	insertBoth(wantID + 1) // A real committed transaction beyond the authoritative bound.

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
	recoveryCtx := types.WithOrdinaryRecovery(ctx, options)
	options.SourceIdentity, err = p.ValidateOrdinaryRecovery(recoveryCtx, streams, lower)
	require.NoError(t, err)
	positions := map[string]string{streams[0].ID(): replayAfter, streams[1].ID(): target.String()}
	if allCovered {
		positions[streams[0].ID()] = target.String()
	}
	options.Plan, err = p.PrepareOrdinaryRecovery(recoveryCtx, streams, lower, positions)
	require.NoError(t, err)
	require.Equal(t, target.String(), options.Plan.TargetLSN)
	require.NoError(t, p.PreCDC(recoveryCtx, streams))
	metadata := map[string]any{streams[0].ID(): fmt.Sprintf(`{"lsn":%q}`, positions[streams[0].ID()]), streams[1].ID(): fmt.Sprintf(`{"lsn":%q}`, target.String())}
	var got []abstract.CDCChange
	final, err := p.StreamChanges(recoveryCtx, 0, metadata, func(_ context.Context, change abstract.CDCChange) error {
		if change.SourceColumns == nil {
			got = append(got, change)
		}
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, waljs.WALState{LSN: target.String()}, final)
	if allCovered {
		require.Empty(t, got)
	} else {
		require.Len(t, got, 1)
		require.Equal(t, streams[0].ID(), got[0].Stream.ID())
		require.EqualValues(t, wantID, got[0].Data["id"])
	}
	slot, err := waljs.GetSlotPosition(ctx, db, name)
	require.NoError(t, err)
	parsedReadStart, err := pglogrepl.ParseLSN(replayAfter)
	require.NoError(t, err)
	require.Equal(t, parsedReadStart, slot.LSN, "bounded source read must not acknowledge before destination confirmation")
	require.Equal(t, baseline, state.Global.State)
	// The source API is called only after the protocol's destination proof gate.
	require.NoError(t, p.AcknowledgeOrdinaryRecovery(recoveryCtx, options.Plan))
	require.Equal(t, target.String(), state.Global.State.(map[string]any)["lsn"])
	require.Equal(t, baseline["opaque"], state.Global.State.(map[string]any)["opaque"])
	require.NoError(t, p.CleanupOrdinaryRecovery(ctx))

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
	slot, err = waljs.GetSlotPosition(ctx, db, name)
	require.NoError(t, err)
	require.Equal(t, target, slot.LSN, "future transaction must remain unacknowledged")
}
