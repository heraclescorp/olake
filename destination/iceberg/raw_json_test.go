package iceberg

import (
	"context"
	"encoding/json"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/types"
	"github.com/stretchr/testify/require"
)

func TestRawDataPreservesNonFiniteNumbers(t *testing.T) {
	for _, tc := range []struct {
		name  string
		value any
		want  string
	}{
		{"NaN", math.NaN(), "NaN"},
		{"positive infinity", math.Inf(1), "Infinity"},
		{"negative infinity", math.Inf(-1), "-Infinity"},
		{"float32 NaN", float32(math.NaN()), "NaN"},
		{"float32 positive infinity", float32(math.Inf(1)), "Infinity"},
		{"float32 negative infinity", float32(math.Inf(-1)), "-Infinity"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			normalize := false
			writer := &Iceberg{stream: &types.ConfiguredStream{StreamMetadata: types.StreamMetadata{Normalization: &normalize}}}
			source := map[string]any{
				"purchase_balance":    tc.value,
				"installment_balance": 123.45,
				"small_float":         float32(0.1),
				"nested":              map[string]any{"values": []any{tc.value, nil, true}},
				"binary":              []byte{0, 255},
				"timestamp":           time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
				"exact":               json.Number("9007199254740993"),
			}
			_, records, _, err := writer.FlattenAndCleanData(context.Background(), []types.RawRecord{{Data: source, OlakeColumns: map[string]any{constants.OlakeID: "synthetic-row"}}})
			require.NoError(t, err)
			raw := records[0].Data[constants.StringifiedData].(string)
			decoder := json.NewDecoder(strings.NewReader(raw))
			decoder.UseNumber()
			var got map[string]any
			require.NoError(t, decoder.Decode(&got))
			require.Equal(t, tc.want, got["purchase_balance"])
			require.Equal(t, json.Number("123.45"), got["installment_balance"])
			require.Equal(t, json.Number("0.1"), got["small_float"])
			require.Equal(t, []any{tc.want, nil, true}, got["nested"].(map[string]any)["values"])
			require.Equal(t, "AP8=", got["binary"])
			require.Equal(t, "2026-01-01T00:00:00Z", got["timestamp"])
			require.Equal(t, json.Number("9007199254740993"), got["exact"])
			require.Equal(t, "synthetic-row", records[0].Data[constants.OlakeID])
			require.IsType(t, tc.value, source["purchase_balance"])
			require.IsType(t, tc.value, source["nested"].(map[string]any)["values"].([]any)[0])
			for _, original := range []any{source["purchase_balance"], source["nested"].(map[string]any)["values"].([]any)[0]} {
				var number float64
				switch value := original.(type) {
				case float32:
					number = float64(value)
				case float64:
					number = value
				}
				switch tc.want {
				case "NaN":
					require.True(t, math.IsNaN(number))
				case "Infinity":
					require.True(t, math.IsInf(number, 1))
				case "-Infinity":
					require.True(t, math.IsInf(number, -1))
				}
			}
		})
	}
}

func TestRawDataRejectsUnsupportedValues(t *testing.T) {
	normalize := false
	writer := &Iceberg{stream: &types.ConfiguredStream{StreamMetadata: types.StreamMetadata{Normalization: &normalize}}}
	cycle := map[string]any{}
	cycle["self"] = cycle
	for _, tc := range []struct {
		value     any
		errorText string
	}{
		{make(chan int), "unsupported type"},
		{json.Number("not-a-number"), "invalid number literal"},
		{cycle, "encountered a cycle"},
	} {
		_, _, _, err := writer.FlattenAndCleanData(context.Background(), []types.RawRecord{{Data: map[string]any{"balance": math.NaN(), "invalid": tc.value}}})
		require.ErrorContains(t, err, tc.errorText)
	}
}
