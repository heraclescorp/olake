package iceberg

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	parquetfile "github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/table"
	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/destination"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils/typeutils"
	"github.com/stretchr/testify/require"
)

// sourceTypesStream is a normalized stream of the given source columns, read from a database named
// as the native writer fixture's catalog expects.
func sourceTypesStream(columns map[string]types.DataType) *types.ConfiguredStream {
	stream := types.NewStream("records", "db", nil)
	stream.DestinationDatabase, stream.DestinationTable = "db", "records"
	stream.UpsertField(constants.OlakeID, types.String, false, true)
	stream.UpsertField(constants.OpType, types.String, false, true)
	stream.UpsertField(constants.OlakeTimestamp, types.TimestampMicro, false, true)
	for name, dataType := range columns {
		stream.UpsertField(name, dataType, true, false)
	}
	normalized, update := true, string(types.UpdateTypeEquality)
	return &types.ConfiguredStream{Stream: stream, StreamMetadata: types.StreamMetadata{Normalization: &normalized, UpdateType: &update, UseSourceColumnNames: true}}
}

// writeSourceRows drives the Iceberg writer the way a writer thread does: flatten and validate,
// evolve the schema when the batch needs it, write and commit.
func writeSourceRows(ctx context.Context, t *testing.T, cfg *Config, stream *types.ConfiguredStream, cat catalog.Catalog, rows map[string]map[string]any) *table.Table {
	t.Helper()
	ice := &Iceberg{config: cfg, catalog: cat}
	global, _, err := ice.Setup(ctx, stream, nil, &destination.Options{ThreadID: "source-types", Backfill: true})
	require.NoError(t, err)
	records := make([]types.RawRecord, 0, len(rows))
	for id, data := range rows {
		records = append(records, types.RawRecord{Data: data, OlakeColumns: map[string]any{
			constants.OlakeID: id, constants.OpType: "r", constants.OlakeTimestamp: time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC),
		}})
	}
	evolve, records, recordsSchema, err := ice.FlattenAndCleanData(ctx, records)
	require.NoError(t, err)
	if evolve {
		_, err = ice.EvolveSchema(ctx, global, recordsSchema)
		require.NoError(t, err)
	}
	require.NoError(t, ice.Write(ctx, records))
	require.NoError(t, ice.Close(ctx, nil))
	tbl, err := cat.LoadTable(ctx, table.Identifier{"db", "records"})
	require.NoError(t, err)
	return tbl
}

func reformatted(t *testing.T, dataType types.DataType, value any) any {
	t.Helper()
	converted, err := typeutils.ReformatValue(dataType, value)
	require.NoError(t, err)
	return converted
}

// sourceDecimal is a numeric(16,2) value as the PostgreSQL converter produces it.
func sourceDecimal(t *testing.T, text string) typeutils.Decimal {
	t.Helper()
	value, err := typeutils.ReformatDecimal(types.Decimal.Of(16, 2), text)
	require.NoError(t, err)
	return value
}

// tableValues reads every row back, rendering each value exactly: a decimal at its column's scale,
// a date as its day and a timestamp as its UTC instant.
func tableValues(t *testing.T, tbl *table.Table) (map[string]map[string]any, *arrow.Schema) {
	t.Helper()
	scan := tbl.Scan()
	defer scan.Close()
	schema, records, err := scan.ToArrowRecords(context.Background())
	require.NoError(t, err)
	idIndex := schema.FieldIndices(constants.OlakeID)[0]
	rows := make(map[string]map[string]any)
	for batch, err := range records {
		require.NoError(t, err)
		ids := batch.Column(idIndex).(*array.String)
		for row := range int(batch.NumRows()) {
			values := make(map[string]any)
			for index, field := range schema.Fields() {
				column := batch.Column(index)
				if column.IsNull(row) {
					continue
				}
				switch column := column.(type) {
				case *array.Decimal128:
					values[field.Name] = column.Value(row).ToString(field.Type.(*arrow.Decimal128Type).Scale)
				case *array.Date32:
					values[field.Name] = column.Value(row).FormattedString()
				case *array.Timestamp:
					values[field.Name] = column.Value(row).ToTime(field.Type.(*arrow.TimestampType).Unit)
				case *array.Float64:
					values[field.Name] = column.Value(row)
				}
			}
			rows[ids.Value(row)] = values
		}
		batch.Release()
	}
	return rows, schema
}

// New tables hold a constrained numeric as an exact decimal, a date as a date and a timestamp
// without time zone as its wall clock; an unconstrained numeric stays a double and a timestamptz
// keeps its instant.
func TestNativeWriterCreatesSourceTypedColumns(t *testing.T) {
	ctx := context.Background()
	cfg, _, cat := nativeWriterFixture(t, types.UpdateTypeEquality, true, "")
	amount, ok := types.DecimalOf(16, 2)
	require.True(t, ok)
	stream := sourceTypesStream(map[string]types.DataType{
		"amount": amount, "ratio": types.Float64, "expires_on": types.Date,
		"created_at": types.TimestampNTZ, "seen_at": types.Timestamp,
	})
	row := func(money, day, wall, instant string) map[string]any {
		return map[string]any{
			"amount": reformatted(t, amount, money), "ratio": reformatted(t, types.Float64, money),
			"expires_on": reformatted(t, types.Date, day), "created_at": reformatted(t, types.TimestampNTZ, wall),
			"seen_at": reformatted(t, types.Timestamp, instant),
		}
	}
	tbl := writeSourceRows(ctx, t, cfg, stream, cat, map[string]map[string]any{
		"a": row("48.10", "2026-01-31", "2026-01-31 23:30:00", "2026-01-31 23:30:00+05:30"),
		"b": row("0.30", "1999-12-31", "1999-12-31 00:00:00.123456", "1999-12-31 00:00:00+00:00"),
	})

	columns := make(map[string]string)
	for _, field := range tbl.Schema().Fields() {
		columns[field.Name] = field.Type.String()
	}
	require.Equal(t, "decimal(16, 2)", columns["amount"])
	require.Equal(t, "double", columns["ratio"])
	require.Equal(t, "date", columns["expires_on"])
	require.Equal(t, "timestamp", columns["created_at"])
	require.Equal(t, "timestamptz", columns["seen_at"])

	rows, schema := tableValues(t, tbl)
	require.Equal(t, map[string]map[string]any{
		"a": {
			"amount": "48.10", "ratio": 48.1, "expires_on": "2026-01-31",
			"created_at": time.Date(2026, 1, 31, 23, 30, 0, 0, time.UTC), "seen_at": time.Date(2026, 1, 31, 18, 0, 0, 0, time.UTC),
			constants.OlakeTimestamp: time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC),
		},
		"b": {
			"amount": "0.30", "ratio": 0.3, "expires_on": "1999-12-31",
			"created_at": time.Date(1999, 12, 31, 0, 0, 0, 123456000, time.UTC), "seen_at": time.Date(1999, 12, 31, 0, 0, 0, 0, time.UTC),
			constants.OlakeTimestamp: time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC),
		},
	}, rows)
	createdAt, _ := schema.FieldsByName("created_at")
	require.Empty(t, createdAt[0].Type.(*arrow.TimestampType).TimeZone, "a timestamp without time zone reads back without one")
	seenAt, _ := schema.FieldsByName("seen_at")
	require.NotEmpty(t, seenAt[0].Type.(*arrow.TimestampType).TimeZone)

	// The data files carry the parquet logical types Snowflake and Spark map back.
	entries, err := nativeLiveEntries(ctx, tbl)
	require.NoError(t, err)
	require.NotEmpty(t, entries)
	fs, err := tbl.FS(ctx)
	require.NoError(t, err)
	for _, entry := range entries {
		input, err := fs.Open(entry.DataFile().FilePath())
		require.NoError(t, err)
		reader, err := parquetfile.NewParquetReader(input)
		require.NoError(t, err)
		logical := make(map[string]string)
		adjusted := make(map[string]bool)
		for index := range reader.MetaData().Schema.NumColumns() {
			column := reader.MetaData().Schema.Column(index)
			logical[column.Name()] = column.LogicalType().String()
			if timestamp, ok := column.LogicalType().(interface{ IsAdjustedToUTC() bool }); ok {
				adjusted[column.Name()] = timestamp.IsAdjustedToUTC()
			}
		}
		require.NoError(t, reader.Close())
		require.Equal(t, "Decimal(precision=16, scale=2)", logical["amount"])
		require.Equal(t, "Date", logical["expires_on"])
		require.False(t, adjusted["created_at"], "a timestamp without time zone is not adjusted to UTC")
		require.True(t, adjusted["seen_at"])
	}
}

// A table created before numeric, date and timestamp columns got their own types holds them as
// double and timestamptz. It keeps those types and stores the new source values exactly as the
// old source values were stored.
func TestNativeWriterOldTypedColumnsTakeNewSourceTypes(t *testing.T) {
	ctx := context.Background()
	cfg, _, cat := nativeWriterFixture(t, types.UpdateTypeEquality, true, "")
	oldStream := sourceTypesStream(map[string]types.DataType{"amount": types.Float64, "expires_on": types.Timestamp, "created_at": types.Timestamp})
	// the values the old mapping produced for numeric(16,2) '48.10', date '2026-01-31' and
	// timestamp '2026-01-31 23:30:00'
	writeSourceRows(ctx, t, cfg, oldStream, cat, map[string]map[string]any{"old": {
		"amount":     reformatted(t, types.Float64, "48.10"),
		"expires_on": reformatted(t, types.Timestamp, time.Date(2026, 1, 31, 0, 0, 0, 0, time.UTC)),
		"created_at": reformatted(t, types.Timestamp, "2026-01-31 23:30:00"),
	}})

	amount, ok := types.DecimalOf(16, 2)
	require.True(t, ok)
	newStream := sourceTypesStream(map[string]types.DataType{"amount": amount, "expires_on": types.Date, "created_at": types.TimestampNTZ})
	tbl := writeSourceRows(ctx, t, cfg, newStream, cat, map[string]map[string]any{"new": {
		"amount":     reformatted(t, amount, "48.10"),
		"expires_on": reformatted(t, types.Date, time.Date(2026, 1, 31, 0, 0, 0, 0, time.UTC)),
		"created_at": reformatted(t, types.TimestampNTZ, "2026-01-31 23:30:00"),
	}})

	for name, want := range map[string]string{"amount": "double", "expires_on": "timestamptz", "created_at": "timestamptz"} {
		field, found := tbl.Schema().FindFieldByName(name)
		require.True(t, found)
		require.Equal(t, want, field.Type.String(), "column %s keeps its existing type", name)
	}
	rows, _ := tableValues(t, tbl)
	require.Equal(t, rows["old"], rows["new"])
	require.Equal(t, map[string]any{
		"amount": 48.1, "expires_on": time.Date(2026, 1, 31, 0, 0, 0, 0, time.UTC),
		"created_at": time.Date(2026, 1, 31, 23, 30, 0, 0, time.UTC), constants.OlakeTimestamp: time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC),
	}, rows["new"])
}

// A numeric whose precision grows widens its decimal column, which Iceberg allows; the scale
// stays.
func TestNativeWriterWidensDecimalPrecision(t *testing.T) {
	ctx := context.Background()
	cfg, _, cat := nativeWriterFixture(t, types.UpdateTypeEquality, true, "")
	narrow, _ := types.DecimalOf(10, 2)
	wide, _ := types.DecimalOf(16, 2)
	writeSourceRows(ctx, t, cfg, sourceTypesStream(map[string]types.DataType{"amount": narrow}), cat,
		map[string]map[string]any{"narrow": {"amount": reformatted(t, narrow, "12345678.90")}})
	tbl := writeSourceRows(ctx, t, cfg, sourceTypesStream(map[string]types.DataType{"amount": narrow}), cat,
		map[string]map[string]any{"wide": {"amount": reformatted(t, wide, "12345678901234.56")}})
	field, found := tbl.Schema().FindFieldByName("amount")
	require.True(t, found)
	require.Equal(t, "decimal(16, 2)", field.Type.String())
	rows, _ := tableValues(t, tbl)
	require.Equal(t, "12345678.90", rows["narrow"]["amount"])
	require.Equal(t, "12345678901234.56", rows["wide"]["amount"])
}

// A NaN numeric, which no Iceberg decimal holds, is written as null; a double column keeps it.
func TestNativeWriterNaNDecimal(t *testing.T) {
	ctx := context.Background()
	cfg, _, cat := nativeWriterFixture(t, types.UpdateTypeEquality, true, "")
	amount, _ := types.DecimalOf(6, 4)
	tbl := writeSourceRows(ctx, t, cfg, sourceTypesStream(map[string]types.DataType{"rate": amount, "legacy_rate": types.Float64}), cat,
		map[string]map[string]any{"nan": {"rate": reformatted(t, amount, "NaN"), "legacy_rate": reformatted(t, amount, "NaN")}})
	rows, _ := tableValues(t, tbl)
	require.NotContains(t, rows["nan"], "rate")
	require.Equal(t, "NaN", strconv.FormatFloat(rows["nan"]["legacy_rate"].(float64), 'g', -1, 64))
}
