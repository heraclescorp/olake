package abstract

import (
	"bytes"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/types"
	"github.com/stretchr/testify/require"
)

func TestCheckpointedTailSkipRequiresPostgresCDCPlanBindingAndLower(t *testing.T) {
	for _, scenario := range []string{"valid", "full_refresh", "strict_cdc", "other_driver", "uncontrolled", "no_binding", "no_plan", "no_global", "zero_lower", "invalid_lower"} {
		t.Run(scenario, func(t *testing.T) {
			stream := types.NewStream("items", "public", nil).Wrap(0)
			stream.GetStream().SyncMode = types.CDC
			state := &types.State{RWMutex: &sync.RWMutex{}, Type: types.GlobalType, SuppressPersistence: true, BackfillCheckpoint: true, Global: &types.GlobalState{State: map[string]any{"lsn": "0/100"}, Streams: types.NewSet[string]()}}
			require.NoError(t, state.SetBackfillPlan(stream.Self(), types.NewSet(types.Chunk{})))
			state.SetBackfillTable(stream.Self(), &types.BackfillTableBinding{TableUUID: "table", ShapeSHA256: "shape"})
			driver := string(constants.Postgres)
			switch scenario {
			case "full_refresh":
				stream.GetStream().SyncMode = types.FULLREFRESH
			case "strict_cdc":
				stream.GetStream().SyncMode = types.STRICTCDC
			case "other_driver":
				driver = "mysql"
			case "uncontrolled":
				state.BackfillCheckpoint = false
			case "no_binding":
				state.SetBackfillTable(stream.Self(), nil)
			case "no_plan":
				state = &types.State{RWMutex: &sync.RWMutex{}, SuppressPersistence: true, BackfillCheckpoint: true, Global: state.Global}
				state.SetBackfillTable(stream.Self(), &types.BackfillTableBinding{TableUUID: "table", ShapeSHA256: "shape"})
			case "no_global":
				state.Global = nil
			case "zero_lower":
				state.Global.State = map[string]any{"lsn": "0/0"}
			case "invalid_lower":
				state.Global.State = map[string]any{"lsn": "invalid"}
			}
			require.Equal(t, scenario == "valid", canSkipCheckpointedTail(state, stream, driver))
		})
	}
}

func TestPostgresBackfillPlanBoundariesSurviveDurableJSON(t *testing.T) {
	for _, tc := range []struct {
		name       string
		column     string
		columnType types.DataType
		min        any
		want       string
	}{
		{"float", "id", types.Float64, float64(1000000), "1000000"},
		{"integer_precision", "id", types.Int64, int64(9007199254740993), "9007199254740993"},
		{"timestamp", "id", types.TimestampMicro, time.Date(2026, 11, 20, 12, 30, 0, 123456000, time.UTC), "'2026-11-20T12:30:00.123456Z'"},
		{"binary", "id", types.Binary, []byte{0, 1, 255}, "decode('0001ff','hex')"},
		{"numeric_bytes", "id", types.Float64, []byte("1000000.125"), "1000000.125"},
		{"text", "id", types.String, "O'Reilly", "'O''Reilly'"},
		{"ctid", "", types.String, "'(0,0)'", "'(0,0)'"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stream := types.NewStream("items", "public", nil).Wrap(0)
			stream.StreamMetadata.ChunkColumn = tc.column
			stream.GetStream().UpsertField("id", tc.columnType, false, false)
			plan, err := canonicalPostgresBackfillPlan(stream, types.NewSet(types.Chunk{Min: tc.min}))
			require.NoError(t, err)
			original := plan.Array()[0]
			require.Equal(t, tc.want, fmt.Sprint(original.Min))
			data, err := json.Marshal(plan.Array())
			require.NoError(t, err)
			var restored []types.Chunk
			decoder := json.NewDecoder(bytes.NewReader(data))
			decoder.UseNumber()
			require.NoError(t, decoder.Decode(&restored))
			require.Equal(t, fmt.Sprint(original.Min), fmt.Sprint(restored[0].Min), "chunk IDs must be identical before and after persistence")
		})
	}
}
