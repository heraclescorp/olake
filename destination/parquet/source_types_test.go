package parquet

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils/typeutils"
	pqgo "github.com/parquet-go/parquet-go"
	"github.com/stretchr/testify/require"
)

// Exact source values land in parquet logical types: a decimal as its unscaled integer, a date as
// its day and a wall-clock timestamp without being adjusted to UTC.
func TestWritesSourceLogicalTypes(t *testing.T) {
	money, _ := types.DecimalOf(16, 2)
	rate, _ := types.DecimalOf(6, 4)
	dir := t.TempDir()
	stream := newStream(true, "", map[string]types.DataType{
		"id": types.Int64, "name": types.String, "amount": money, "rate": rate, "expires_on": types.Date, "created_at": types.TimestampNTZ,
	})
	p := newParquet(t, dir, stream, 0, 0)
	value := func(dataType types.DataType, text string) any {
		converted, err := typeutils.ReformatValue(dataType, text)
		require.NoError(t, err)
		return converted
	}
	writeBatch(t, p, []types.RawRecord{rec(1, map[string]any{
		"amount": value(money, "48.10"), "rate": value(rate, "NaN"),
		"expires_on": value(types.Date, "2026-01-31"), "created_at": value(types.TimestampNTZ, "2026-01-31 23:30:00"),
	})})
	require.NoError(t, p.Close(context.Background(), nil))

	files := parquetFiles(t, dir)
	require.Len(t, files, 1)
	file, err := os.Open(files[0])
	require.NoError(t, err)
	defer file.Close()
	info, err := file.Stat()
	require.NoError(t, err)
	pf, err := pqgo.OpenFile(file, info.Size())
	require.NoError(t, err)

	leaf := func(name string) pqgo.LeafColumn {
		column, found := pf.Schema().Lookup(name)
		require.True(t, found, name)
		return column
	}
	amount, expires, created := leaf("amount"), leaf("expires_on"), leaf("created_at")
	require.NotNil(t, amount.Node.Type().LogicalType().Decimal)
	require.Equal(t, int32(16), amount.Node.Type().LogicalType().Decimal.Precision)
	require.Equal(t, int32(2), amount.Node.Type().LogicalType().Decimal.Scale)
	require.NotNil(t, expires.Node.Type().LogicalType().Date)
	require.NotNil(t, created.Node.Type().LogicalType().Timestamp)
	require.False(t, created.Node.Type().LogicalType().Timestamp.IsAdjustedToUTC)

	rows := make([]pqgo.Row, 1)
	n, _ := pf.RowGroups()[0].Rows().ReadRows(rows)
	require.Equal(t, 1, n)
	byColumn := make(map[int]pqgo.Value)
	for _, v := range rows[0] {
		byColumn[v.Column()] = v
	}
	require.Equal(t, int64(4810), byColumn[amount.ColumnIndex].Int64())
	require.True(t, byColumn[leaf("rate").ColumnIndex].IsNull(), "NaN, which no decimal holds, is null")
	require.Equal(t, int32(20484), byColumn[expires.ColumnIndex].Int32())
	require.Equal(t, time.Date(2026, 1, 31, 23, 30, 0, 0, time.UTC).UnixMicro(), byColumn[created.ColumnIndex].Int64())
}
