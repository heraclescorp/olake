package driver

import (
	"context"
	"errors"
	"os"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/datazip-inc/olake/constants"
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
	for _, c := range []CDC{{}, {PreserveToastValues: true}, {PreserveToastValues: true, ToastValueSource: "source"}, {ToastValueSource: "destination"}} {
		require.NoError(t, c.validateToastRecovery())
	}
	for _, c := range []CDC{{ToastValueSource: "source"}, {PreserveToastValues: true, ToastValueSource: "typo"}} {
		require.Error(t, c.validateToastRecovery())
	}
	p := &Postgres{config: &Config{UpdateMethod: map[string]any{"replication_slot": "slot", "preserve_toast_values": true, "toast_value_source": "source"}}}
	require.Nil(t, p.SupportedUpdateTypes())
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
	p.cdcConfig = CDC{PreserveToastValues: true, ToastValueSource: "source"}
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
	p.cdcConfig = CDC{PreserveToastValues: true, ToastValueSource: "source"}
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
			p.cdcConfig = CDC{PreserveToastValues: true, ToastValueSource: "source"}
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
