package typeutils

import (
	"encoding/json"
	"math"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
	"github.com/stretchr/testify/require"
)

func TestReformatDecimalIsExact(t *testing.T) {
	money := types.Decimal.Of(16, 2)
	for _, tc := range []struct {
		dataType types.DataType
		value    any
		want     string
		unscaled int64
	}{
		{money, "48.10", "48.10", 4810},
		{money, "0.30", "0.30", 30},
		{money, "48.1", "48.10", 4810},
		{money, "-0.05", "-0.05", -5},
		{money, "0", "0.00", 0},
		{money, "1e2", "100.00", 10000},
		{money, "4.810E1", "48.10", 4810},
		{money, "48.100", "48.10", 4810},
		{money, []byte("48.10"), "48.10", 4810},
		{money, json.Number("48.10"), "48.10", 4810},
		{money, 48.1, "48.10", 4810},
		{money, int64(7), "7.00", 700},
		{types.Decimal.Of(6, 4), "0.0125", "0.0125", 125},
		{types.Decimal.Of(10, 0), "-42", "-42", -42},
	} {
		d, err := ReformatDecimal(tc.dataType, tc.value)
		require.NoError(t, err, "%v", tc.value)
		require.Equal(t, tc.want, d.String())
		unscaled, scale := d.Unscaled()
		require.Equal(t, decimal128.FromI64(tc.unscaled), unscaled)
		_, wantScale, _ := types.DecimalParams(tc.dataType)
		require.Equal(t, int32(wantScale), scale)
		require.Equal(t, tc.dataType, d.Type())
		require.Equal(t, tc.dataType, TypeFromValue(d))
		encoded, err := json.Marshal(d)
		require.NoError(t, err)
		require.Equal(t, tc.want, string(encoded))
		bound, err := d.Value()
		require.NoError(t, err)
		require.Equal(t, tc.want, bound)
	}

	widest := "-12345678901234567890123456789012345678"
	d, err := ReformatDecimal(types.Decimal.Of(38, 0), widest)
	require.NoError(t, err)
	require.Equal(t, widest, d.String())
	d, err = ReformatDecimal(types.Decimal.Of(38, 20), "123456789012345678.12345678901234567891")
	require.NoError(t, err)
	require.Equal(t, "123456789012345678.12345678901234567891", d.String())

	for _, value := range []any{"48.105", "12345678901234567.00", "abc", "", "-", "1e", "Infinity", "4 8", true, time.Now()} {
		_, err := ReformatDecimal(money, value)
		require.Error(t, err, "%v", value)
	}
	_, err = ReformatDecimal(types.Decimal, "1")
	require.Error(t, err, "the bare pattern is no decimal type")
}

// A value a double column held before decimal columns is exactly what Float64 returns, so such a
// column keeps receiving the same doubles, and an olake id keyed on a decimal stays the same.
func TestDecimalKeepsDoubleCompatibility(t *testing.T) {
	for _, text := range []string{"48.10", "0.30", "12345678901234.56", "-0.05", "123456789012345678"} {
		dataType := types.Decimal.Of(20, 2)
		d, err := ReformatDecimal(dataType, text)
		require.NoError(t, err)
		old, err := ReformatValue(types.Float64, text)
		require.NoError(t, err)
		require.Equal(t, old, d.Float64())
		value, err := ReformatValue(types.Float64, d)
		require.NoError(t, err)
		require.Equal(t, old, value)
		require.Equal(t, utils.GetKeysHash(map[string]any{"id": old}, "id"), utils.GetKeysHash(map[string]any{"id": d}, "id"))
		require.Equal(t, utils.GetKeysHash(map[string]any{"id": old, "k": "x"}, "id", "k"), utils.GetKeysHash(map[string]any{"id": d, "k": "x"}, "id", "k"))
	}
}

func TestDecimalNaN(t *testing.T) {
	d, err := ReformatDecimal(types.Decimal.Of(6, 4), "NaN")
	require.NoError(t, err)
	require.True(t, d.IsNaN())
	require.True(t, math.IsNaN(d.Float64()))
	require.Equal(t, "NaN", d.String())
	encoded, err := json.Marshal(d)
	require.NoError(t, err)
	require.JSONEq(t, `"NaN"`, string(encoded))
	one, err := ReformatDecimal(types.Decimal.Of(6, 4), "1")
	require.NoError(t, err)
	require.Equal(t, 1, Compare(d, one), "NaN sorts above every number")
	require.Equal(t, 0, Compare(d, d))
}

func TestCompareDecimal(t *testing.T) {
	d, err := ReformatDecimal(types.Decimal.Of(16, 2), "48.10")
	require.NoError(t, err)
	bound, err := ParseFilterValue(types.Decimal.Of(16, 2), "48.105")
	require.NoError(t, err)
	require.Equal(t, types.Decimal.Of(5, 3), bound.(Decimal).Type(), "a filter bound keeps its own scale")
	require.Equal(t, -1, Compare(d, bound))
	require.Equal(t, 1, Compare(bound, d))
	require.Equal(t, 0, Compare(d, 48.1), "a decimal compares with a double column's bound")
	require.Equal(t, 1, Compare(48.2, d))
	require.Equal(t, 0, Compare(d, json.Number("48.1")))
	require.Equal(t, -1, Compare(int64(48), mustParseDecimal(t, "48.01")))
}

func mustParseDecimal(t *testing.T, text string) Decimal {
	t.Helper()
	d, err := ParseDecimal(text)
	require.NoError(t, err)
	return d
}

func TestReformatTemporalTypes(t *testing.T) {
	newYork, err := time.LoadLocation("America/New_York")
	require.NoError(t, err)
	midnight := time.Date(2026, 1, 31, 0, 0, 0, 0, time.UTC)
	wall := time.Date(2026, 1, 31, 23, 30, 0, 0, time.UTC)
	for _, value := range []any{"2026-01-31", midnight, time.Date(2026, 1, 31, 23, 0, 0, 0, newYork), []byte("2026-01-31")} {
		d, err := ReformatValue(types.Date, value)
		require.NoError(t, err)
		require.Equal(t, Date{midnight}, d, "%v reads as its own wall-clock day", value)
		require.Equal(t, types.Date, TypeFromValue(d))
	}
	for _, value := range []any{"2026-01-31 23:30:00", wall, time.Date(2026, 1, 31, 23, 30, 0, 0, newYork)} {
		ts, err := ReformatValue(types.TimestampNTZ, value)
		require.NoError(t, err)
		require.Equal(t, TimestampNTZ{wall}, ts, "%v keeps its wall clock", value)
		require.Equal(t, types.TimestampNTZ, TypeFromValue(ts))
	}
	ts, err := ReformatValue(types.TimestampNTZ, "2026-01-31 23:30:00.123456")
	require.NoError(t, err)
	require.Equal(t, 123456000, ts.(TimestampNTZ).Nanosecond())

	// a timestamptz column takes them as the instants it stored for them before
	stored, err := ReformatDate(Date{midnight}, true)
	require.NoError(t, err)
	require.Equal(t, midnight, stored)
	stored, err = ReformatDate(TimestampNTZ{wall}, true)
	require.NoError(t, err)
	require.Equal(t, wall, stored)
	require.Equal(t, 0, Compare(Date{midnight}, midnight))
	require.Equal(t, 1, Compare(TimestampNTZ{wall}, midnight))

	// out-of-range and unparseable values fall back as timestamps do
	for _, value := range []any{"infinity", "0000-01-01", "10000-01-01"} {
		old, oldErr := ReformatValue(types.Timestamp, value)
		d, err := ReformatValue(types.Date, value)
		ts, tsErr := ReformatValue(types.TimestampNTZ, value)
		require.Equal(t, oldErr != nil, err != nil, "%v", value)
		require.Equal(t, oldErr != nil, tsErr != nil, "%v", value)
		if oldErr == nil {
			require.Equal(t, DateOf(old.(time.Time)), d)
			require.Equal(t, TimestampNTZOf(old.(time.Time)), ts)
		}
	}

	bound, err := ParseFilterValue(types.Date, "2026-01-31")
	require.NoError(t, err)
	require.Equal(t, Date{midnight}, bound)
	_, err = ParseFilterValue(types.TimestampNTZ, "not-a-time")
	require.Error(t, err)

	// database/sql binds them as times
	value, err := Date{midnight}.Value()
	require.NoError(t, err)
	require.Equal(t, midnight, value)
	value, err = TimestampNTZ{wall}.Value()
	require.NoError(t, err)
	require.Equal(t, wall, value)
}

func TestFlattenKeepsTypedValues(t *testing.T) {
	d, err := ReformatDecimal(types.Decimal.Of(16, 2), "48.10")
	require.NoError(t, err)
	record := types.Record{"amount": d, "day": Date{time.Date(2026, 1, 31, 0, 0, 0, 0, time.UTC)}, "at": TimestampNTZ{time.Date(2026, 1, 31, 23, 30, 0, 0, time.UTC)}}
	flattened, err := NewFlattener(func(s string) string { return s }).Flatten(record)
	require.NoError(t, err)
	require.Equal(t, record, flattened)
}
