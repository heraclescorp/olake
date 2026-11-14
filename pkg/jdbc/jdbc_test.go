package jdbc

import (
	"encoding/json"
	"testing"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestSQLFilterBinary tests that a binary filter value is hex from state version 8 and text on older state. The
// schema is read from catalog JSON under that state version, as a sync reads its catalog.
func TestSQLFilterBinary(t *testing.T) {
	tests := []struct {
		name         string
		stateVersion int
		value        string
		expected     string
	}{
		{name: "hex", stateVersion: 8, value: "616263", expected: "`data` = X'616263'"},
		{name: "text before state version 8", stateVersion: 7, value: "616263", expected: "`data` = '616263'"},
		{name: "non-hex text before state version 8", stateVersion: 7, value: "update12", expected: "`data` = 'update12'"},
	}

	old := constants.LoadedStateVersion
	t.Cleanup(func() { constants.LoadedStateVersion = old })

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			constants.LoadedStateVersion = tc.stateVersion
			schema := types.NewTypeSchema()
			require.NoError(t, json.Unmarshal([]byte(`{"properties": {"data": {"type": ["binary"]}}}`), schema))
			stream := &types.ConfiguredStream{
				Stream: &types.Stream{Name: "users", Namespace: "db", Schema: schema},
				StreamMetadata: types.StreamMetadata{
					Normalization: new(true),
					FilterConfig: &types.FilterConfig{
						Conditions: []types.FilterCondition{{Column: "data", Operator: "=", Value: tc.value}},
					},
				},
			}

			filter, err := SQLFilter(stream, "mysql", "")
			require.NoError(t, err)
			assert.Equal(t, tc.expected, filter)
		})
	}
}

func TestPostgresChunkScanQueryBoundsAndFilter(t *testing.T) {
	stream := types.NewStream("events", "public", nil).Wrap(0)
	for _, test := range []struct {
		name   string
		chunk  types.Chunk
		filter string
		query  string
	}{
		{"unbounded", types.Chunk{}, "", `SELECT * FROM "public"."events"`},
		{"filter only", types.Chunk{}, `"active" = true`, `SELECT * FROM "public"."events" WHERE "active" = true`},
		{"lower bound", types.Chunk{Min: 10}, "", `SELECT * FROM "public"."events" WHERE "id" >= 10`},
		{"upper bound", types.Chunk{Max: 20}, "", `SELECT * FROM "public"."events" WHERE "id" < 20`},
		{"bounded", types.Chunk{Min: 10, Max: 20}, "", `SELECT * FROM "public"."events" WHERE "id" >= 10 AND "id" < 20`},
		{"bounded filter", types.Chunk{Min: 10, Max: 20}, `"active" = true OR "id" = 11`, `SELECT * FROM "public"."events" WHERE ("id" >= 10 AND "id" < 20) AND ("active" = true OR "id" = 11)`},
	} {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.query, PostgresChunkScanQuery(stream, "id", test.chunk, test.filter))
		})
	}
}
