package driver

import (
	"context"
	"errors"
	"net/url"
	"os"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/destination"
	"github.com/datazip-inc/olake/drivers/abstract"
	"github.com/datazip-inc/olake/drivers/postgres/internal/waljs"
	"github.com/datazip-inc/olake/types"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/require"
)

func toastChange(id int64) abstract.CDCChange {
	stream := &types.ConfiguredStream{Stream: types.NewStream("users", "public", nil)}
	stream.GetStream().SourceDefinedPrimaryKey.Insert("id")
	return abstract.CDCChange{Stream: stream, Kind: "update", Data: map[string]any{"id": id, "doc": constants.UnavailableValue, "explicit": nil}, UnavailableColumns: []string{"doc"}}
}

func TestSourceToastBatchesDeduplicatesAndPreservesOrder(t *testing.T) {
	p, mock := newMockPostgres(t)
	p.typeMapping = pgTypeToDataTypes()
	// Results are deliberately in the reverse of WAL order. Query only missing columns.
	mock.ExpectQuery(`SELECT "id", "doc" FROM "public"."users" WHERE ("id" = $1) OR ("id" = $2)`).WithArgs(int64(1), int64(2)).WillReturnRows(sqlmock.NewRowsWithColumnDefinition(sqlmock.NewColumn("id").OfType("INT8", int64(0)), sqlmock.NewColumn("doc").OfType("TEXT", "")).AddRow(int64(2), nil).AddRow(int64(1), "source"))
	var got []abstract.CDCChange
	b := &sourceToastBatch{postgres: p, callback: func(_ context.Context, c abstract.CDCChange) error { got = append(got, c); return nil }}
	ctx := context.Background()
	original := toastChange(1)
	require.NoError(t, b.push(ctx, original))
	ordinary := toastChange(1)
	ordinary.UnavailableColumns = nil
	ordinary.Data["doc"] = "explicit WAL"
	require.NoError(t, b.push(ctx, ordinary))
	require.NoError(t, b.push(ctx, toastChange(2)))
	require.NoError(t, b.push(ctx, toastChange(1)))
	require.Empty(t, got)
	require.NoError(t, b.flush(ctx))
	require.Len(t, got, 4)
	require.Equal(t, "source", got[0].Data["doc"])
	require.Equal(t, "explicit WAL", got[1].Data["doc"])
	require.Nil(t, got[2].Data["doc"])
	require.Equal(t, "source", got[3].Data["doc"])
	for _, c := range got {
		require.Empty(t, c.UnavailableColumns)
		require.Nil(t, c.Data["explicit"])
	}
	require.Equal(t, constants.UnavailableValue, original.Data["doc"])
}

func TestSourceToastFailuresNeverEmitMissingValues(t *testing.T) {
	for _, tc := range []string{"missing row", "query error", "iteration error", "missing key", "no keys", "canceled"} {
		t.Run(tc, func(t *testing.T) {
			p, mock := newMockPostgres(t)
			p.typeMapping = pgTypeToDataTypes()
			c := toastChange(1)
			switch tc {
			case "missing key":
				delete(c.Data, "id")
			case "no keys":
				c.Stream.GetStream().SourceDefinedPrimaryKey = types.NewSet[string]()
			case "canceled":
			default:
				q := mock.ExpectQuery(`SELECT "id", "doc" FROM "public"."users" WHERE ("id" = $1)`).WithArgs(int64(1))
				if tc == "query error" {
					q.WillReturnError(errors.New("offline"))
				} else {
					rows := sqlmock.NewRowsWithColumnDefinition(sqlmock.NewColumn("id").OfType("INT8", int64(0)), sqlmock.NewColumn("doc").OfType("TEXT", ""))
					if tc == "iteration error" {
						rows.AddRow(int64(1), "x").RowError(0, errors.New("read failed"))
					}
					q.WillReturnRows(rows)
				}
			}
			emitted := false
			b := &sourceToastBatch{postgres: p, callback: func(context.Context, abstract.CDCChange) error { emitted = true; return nil }}
			require.NoError(t, b.push(context.Background(), c))
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if tc == "canceled" {
				cancel()
			}
			require.Error(t, b.flush(ctx))
			require.False(t, emitted)
		})
	}
}

func TestSourceToastQuotesIdentifiersAndCompositeKeys(t *testing.T) {
	p, mock := newMockPostgres(t)
	p.typeMapping = pgTypeToDataTypes()
	c := toastChange(1)
	c.Stream.GetStream().Namespace = `my"schema`
	c.Stream.GetStream().Name = `my"table`
	c.Stream.GetStream().SourceDefinedPrimaryKey = types.NewSet[string]("tenant", "id")
	c.Data["tenant"] = "a' OR true --"
	mock.ExpectQuery(`SELECT "id", "tenant", "doc" FROM "my""schema"."my""table" WHERE ("id" = $1 AND "tenant" = $2)`).WithArgs(int64(1), c.Data["tenant"]).WillReturnRows(sqlmock.NewRowsWithColumnDefinition(sqlmock.NewColumn("id").OfType("INT8", int64(0)), sqlmock.NewColumn("tenant").OfType("TEXT", ""), sqlmock.NewColumn("doc").OfType("TEXT", "")).AddRow(int64(1), c.Data["tenant"], "ok"))
	b := &sourceToastBatch{postgres: p, callback: func(_ context.Context, c abstract.CDCChange) error { require.Equal(t, "ok", c.Data["doc"]); return nil }}
	require.NoError(t, b.push(context.Background(), c))
	require.NoError(t, b.flush(context.Background()))
}

func TestToastRecoveryConfiguration(t *testing.T) {
	for _, c := range []CDC{{}, {ToastValueSource: "source"}, {ToastValueSource: "destination"}} {
		require.Equal(t, c.ToastValueSource != "destination", c.sourceToastRecovery())
		require.NoError(t, c.validateToastRecovery())
	}
	for _, c := range []CDC{{ToastValueSource: "typo"}} {
		require.Error(t, c.validateToastRecovery())
	}
	p := &Postgres{config: &Config{UpdateMethod: map[string]any{"replication_slot": "slot", "toast_value_source": "source"}}}
	require.Nil(t, p.SupportedUpdateTypes())
	p.CDCSupport = true
	p.config.Connection, _ = url.Parse("postgres://localhost/test")
	config, err := p.prepareWALJSConfig()
	require.NoError(t, err)
	require.True(t, config.PreserveToastValues, "WAL decoding must always preserve unavailable values")
}

type toastTestReplicator struct {
	changes []abstract.CDCChange
	err     error
	socket  waljs.Socket
}

func (r *toastTestReplicator) Socket() *waljs.Socket { return &r.socket }
func (r *toastTestReplicator) StreamChanges(ctx context.Context, _ *sqlx.DB, cb abstract.CDCMsgFn) error {
	for _, c := range r.changes {
		if err := cb(ctx, c); err != nil {
			return err
		}
	}
	return r.err
}

func TestSourceToastDoesNotFlushAfterReplicationError(t *testing.T) {
	p, _ := newMockPostgres(t)
	// Omitted methodology must enter the hydration path, not forward placeholders.
	p.cdcConfig = CDC{}
	p.replicator = &toastTestReplicator{changes: []abstract.CDCChange{toastChange(1)}, err: errors.New("WAL interrupted")}
	require.ErrorContains(t, p.streamChangesWithToast(context.Background(), func(context.Context, abstract.CDCChange) error { t.Fatal("unexpected callback"); return nil }), "WAL interrupted")
}

func TestSourceToastBinaryAndArrayConversion(t *testing.T) {
	old := constants.LoadedStateVersion
	constants.LoadedStateVersion = 8
	t.Cleanup(func() { constants.LoadedStateVersion = old })
	p, mock := newMockPostgres(t)
	p.typeMapping = pgTypeToDataTypes()
	c := toastChange(1)
	c.UnavailableColumns = []string{"doc", "bytes", "tags"}
	mock.ExpectQuery(`SELECT "id", "doc", "bytes", "tags" FROM "public"."users" WHERE ("id" = $1)`).WithArgs(int64(1)).WillReturnRows(sqlmock.NewRowsWithColumnDefinition(sqlmock.NewColumn("id").OfType("INT8", int64(0)), sqlmock.NewColumn("doc").OfType("JSONB", ""), sqlmock.NewColumn("bytes").OfType("BYTEA", []byte{}), sqlmock.NewColumn("tags").OfType("_TEXT", "")).AddRow(int64(1), `{"a":1}`, []byte{0, 255, 128}, `{one,two}`))
	b := &sourceToastBatch{postgres: p, callback: func(_ context.Context, c abstract.CDCChange) error {
		require.Equal(t, []byte{0, 255, 128}, c.Data["bytes"])
		require.Equal(t, `{"a":1}`, c.Data["doc"])
		require.NotNil(t, c.Data["tags"])
		return nil
	}}
	require.NoError(t, b.push(context.Background(), c))
	require.NoError(t, b.flush(context.Background()))
}

func TestSourceToastCallbackFailureAndIncompleteTransaction(t *testing.T) {
	p, _ := newMockPostgres(t)
	p.cdcConfig = CDC{ToastValueSource: "source"}
	p.replicator = &toastTestReplicator{changes: []abstract.CDCChange{toastChange(1)}, socket: waljs.Socket{TransactionOpen: true}}
	require.ErrorContains(t, p.streamChangesWithToast(context.Background(), func(context.Context, abstract.CDCChange) error { t.Fatal("unexpected emit"); return nil }), "incomplete")
	c := toastChange(1)
	c.UnavailableColumns = nil
	c.Data["doc"] = "full"
	p.replicator = &toastTestReplicator{changes: []abstract.CDCChange{c}}
	require.ErrorContains(t, p.streamChangesWithToast(context.Background(), func(context.Context, abstract.CDCChange) error { return errors.New("writer failed") }), "writer failed")
}

func TestSourceToastFlushesAtSchemaBoundaryAndBatchLimit(t *testing.T) {
	for _, boundary := range []string{"schema", "limit"} {
		t.Run(boundary, func(t *testing.T) {
			p, mock := newMockPostgres(t)
			p.typeMapping = pgTypeToDataTypes()
			mock.ExpectQuery(`SELECT "id", "doc" FROM "public"."users" WHERE ("id" = $1)`).WithArgs(int64(1)).WillReturnRows(sqlmock.NewRowsWithColumnDefinition(sqlmock.NewColumn("id").OfType("INT8", int64(0)), sqlmock.NewColumn("doc").OfType("TEXT", "")).AddRow(int64(1), "value"))
			var got []abstract.CDCChange
			b := &sourceToastBatch{postgres: p, callback: func(_ context.Context, c abstract.CDCChange) error { got = append(got, c); return nil }}
			require.NoError(t, b.push(context.Background(), toastChange(1)))
			if boundary == "schema" {
				require.NoError(t, b.push(context.Background(), abstract.CDCChange{SourceColumns: &types.SourceColumnsChange{}}))
				require.Len(t, got, 2)
				require.NotNil(t, got[1].SourceColumns)
			} else {
				c := toastChange(1)
				c.UnavailableColumns = nil
				c.Data["doc"] = "WAL"
				for i := 1; i < sourceToastBatchRows; i++ {
					require.NoError(t, b.push(context.Background(), c))
				}
				require.Len(t, got, sourceToastBatchRows)
			}
			require.Empty(t, b.pending)
		})
	}
}

// Exercise hydration and the real destination buffer together: tiny WAL events
// expand beyond the payload budget, without inflating source-byte statistics.
func TestSourceToastExpandedPayloadBoundsDestinationFlushes(t *testing.T) {
	p, mock := newMockPostgres(t)
	p.typeMapping = pgTypeToDataTypes()
	part := strings.Repeat("p", int(constants.MaxDestinationBatchBytes*2/5))
	oversized := strings.Repeat("x", int(constants.MaxDestinationBatchBytes+1))
	mock.ExpectQuery(`SELECT "id", "doc" FROM "public"."users" WHERE ("id" = $1) OR ("id" = $2) OR ("id" = $3) OR ("id" = $4)`).WithArgs(int64(1), int64(2), int64(3), int64(4)).WillReturnRows(sqlmock.NewRowsWithColumnDefinition(sqlmock.NewColumn("id").OfType("INT8", int64(0)), sqlmock.NewColumn("doc").OfType("TEXT", "")).AddRow(int64(4), nil).AddRow(int64(3), oversized).AddRow(int64(2), part).AddRow(int64(1), part))

	ctx := context.Background()
	sink := &toastPayloadWriter{}
	writerType := types.DestinationType("source-toast-payload-test")
	destination.RegisteredWriters[writerType] = func(context.Context, any) (destination.Writer, func(context.Context), error) {
		return sink, nil, nil
	}
	t.Cleanup(func() { delete(destination.RegisteredWriters, writerType) })
	stream := toastChange(1).Stream
	pool, err := destination.NewWriterPool(ctx, &types.WriterConfig{Type: writerType}, []types.StreamInterface{stream}, 1000)
	require.NoError(t, err)
	t.Cleanup(func() { pool.Shutdown(ctx) })
	writer, _, err := pool.NewWriter(ctx, stream)
	require.NoError(t, err)
	batch := &sourceToastBatch{postgres: p, callback: func(ctx context.Context, change abstract.CDCChange) error {
		require.Equal(t, int64(11), change.Bytes, "hydration must not replace WAL read sizes")
		return writer.Push(ctx, types.CreateRawRecord(change.Data, nil, change.UnavailableColumns), change.Bytes)
	}}
	walValue := toastChange(1)
	walValue.UnavailableColumns = nil
	walValue.Data["doc"] = "explicit WAL"
	afterNull := toastChange(4)
	afterNull.UnavailableColumns = nil
	afterNull.Data["doc"] = "after source NULL"
	changes := []abstract.CDCChange{toastChange(1), walValue, toastChange(2), toastChange(1), toastChange(3), toastChange(4), afterNull}
	for _, change := range changes {
		change.Bytes = 11
		require.NoError(t, batch.push(ctx, change))
	}
	require.NoError(t, batch.flush(ctx))
	require.NoError(t, writer.Close(ctx, nil))

	var batchLengths []int
	var emitted []types.RawRecord
	for _, records := range sink.batches {
		batchLengths = append(batchLengths, len(records))
		var payloadBytes int64
		for _, record := range records {
			payloadBytes += record.PayloadSize()
			require.Empty(t, record.UnavailableColumns)
			require.Nil(t, record.Data["explicit"], "explicit WAL NULL must survive hydration")
		}
		if payloadBytes > constants.MaxDestinationBatchBytes {
			require.Len(t, records, 1, "an indivisible oversized row must be flushed alone")
			require.Equal(t, int64(3), records[0].Data["id"])
		}
		emitted = append(emitted, records...)
	}
	require.Equal(t, []int{3, 1, 1, 2}, batchLengths)
	var ids []int64
	for _, record := range emitted {
		ids = append(ids, record.Data["id"].(int64))
	}
	require.Equal(t, []int64{1, 1, 2, 1, 3, 4, 4}, ids)
	require.Len(t, emitted[0].Data["doc"], len(part))
	require.Equal(t, "explicit WAL", emitted[1].Data["doc"])
	require.Len(t, emitted[2].Data["doc"], len(part))
	require.Len(t, emitted[3].Data["doc"], len(part))
	require.Len(t, emitted[4].Data["doc"], len(oversized))
	require.Nil(t, emitted[5].Data["doc"])
	require.Equal(t, "after source NULL", emitted[6].Data["doc"])
	require.Equal(t, int64(len(changes)), pool.GetStats().ReadCount.Load())
	require.Equal(t, int64(11*len(changes)), pool.GetStats().BytesRead.Load())
	require.NoError(t, mock.ExpectationsWereMet())
}

type toastPayloadWriter struct {
	batches [][]types.RawRecord
}

func (*toastPayloadWriter) Spec() any                   { return nil }
func (*toastPayloadWriter) Type() string                { return "source-toast-payload-test" }
func (*toastPayloadWriter) Check(context.Context) error { return nil }
func (*toastPayloadWriter) Setup(context.Context, types.StreamInterface, any, *destination.Options) (any, *types.MetadataState, error) {
	return struct{}{}, nil, nil
}
func (w *toastPayloadWriter) Write(_ context.Context, records []types.RawRecord) error {
	w.batches = append(w.batches, records)
	return nil
}
func (*toastPayloadWriter) FlattenAndCleanData(_ context.Context, records []types.RawRecord) (bool, []types.RawRecord, any, error) {
	return false, records, nil, nil
}
func (*toastPayloadWriter) EvolveSchema(_ context.Context, schema, _ any) (any, error) {
	return schema, nil
}
func (*toastPayloadWriter) ApplySourceColumns(_ context.Context, schema any, _ types.SourceColumnsChange) (any, error) {
	return schema, nil
}
func (*toastPayloadWriter) DropStreams(context.Context, []types.StreamInterface) error { return nil }
func (*toastPayloadWriter) Close(context.Context, any) error                           { return nil }

// Runs only against an explicitly supplied disposable database, never production.
func TestSourceToastPostgresIntegration(t *testing.T) {
	dsn := os.Getenv("OLAKE_SOURCE_TOAST_TEST_DSN")
	if dsn == "" {
		t.Skip("set OLAKE_SOURCE_TOAST_TEST_DSN to a disposable PostgreSQL database")
	}
	db, err := sqlx.Connect("pgx", dsn)
	require.NoError(t, err)
	defer db.Close()
	ctx := context.Background()
	_, err = db.ExecContext(ctx, `CREATE SCHEMA olake_source_toast_test; CREATE TABLE olake_source_toast_test.records (id bigint PRIMARY KEY, doc text, raw bytea, tags text[]); INSERT INTO olake_source_toast_test.records VALUES (1, repeat(md5('toast'),10000), decode('00ff805c','hex'), ARRAY['one','two'])`)
	require.NoError(t, err)
	defer db.ExecContext(ctx, `DROP SCHEMA olake_source_toast_test CASCADE`)
	p := &Postgres{client: db, typeMapping: pgTypeToDataTypes()}
	old := constants.LoadedStateVersion
	constants.LoadedStateVersion = 8
	t.Cleanup(func() { constants.LoadedStateVersion = old })
	c := toastChange(1)
	c.Stream.GetStream().Namespace = "olake_source_toast_test"
	c.Stream.GetStream().Name = "records"
	c.UnavailableColumns = []string{"doc", "raw", "tags"}
	emitted := false
	b := &sourceToastBatch{postgres: p, callback: func(_ context.Context, c abstract.CDCChange) error {
		emitted = true
		require.Len(t, c.Data["doc"], 320000)
		require.Equal(t, []byte{0, 255, 128, 92}, c.Data["raw"])
		require.Equal(t, "{one,two}", c.Data["tags"])
		require.Empty(t, c.UnavailableColumns)
		return nil
	}}
	require.NoError(t, b.push(ctx, c))
	require.NoError(t, b.flush(ctx))
	require.True(t, emitted)
}

func TestPreCDCSourceToastRejectsStandbyAndMissingKeys(t *testing.T) {
	for _, tc := range []string{"standby", "missing keys", "invalid source"} {
		t.Run(tc, func(t *testing.T) {
			p, mock := newMockPostgres(t)
			p.cdcConfig = CDC{ToastValueSource: "source"}
			c := toastChange(1)
			switch tc {
			case "standby":
				mock.ExpectQuery("SELECT pg_is_in_recovery()").WillReturnRows(sqlmock.NewRows([]string{"pg_is_in_recovery"}).AddRow(true))
			case "missing keys":
				c.Stream.GetStream().SourceDefinedPrimaryKey = nil
			case "invalid source":
				p.cdcConfig.ToastValueSource = "typo"
			}
			require.Error(t, p.PreCDC(context.Background(), []types.StreamInterface{c.Stream}))
		})
	}
}
