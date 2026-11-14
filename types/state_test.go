package types

import (
	"sync"
	"testing"

	"github.com/goccy/go-json"
	"github.com/stretchr/testify/require"
)

func resumePlanTestState(syncMode SyncMode) (*State, *ConfiguredStream) {
	stream := NewStream("items", "public", nil).Wrap(0)
	stream.Stream.SyncMode = syncMode
	return &State{RWMutex: &sync.RWMutex{}, Type: StreamType, Version: 8, SuppressPersistence: true}, stream
}

func TestFullRefreshResumePlanSurvivesRestart(t *testing.T) {
	for _, syncMode := range []SyncMode{FULLREFRESH, CDC} {
		t.Run(string(syncMode), func(t *testing.T) {
			state, stream := resumePlanTestState(syncMode)
			plan := NewSet(Chunk{Min: int64(1), Max: int64(10)}, Chunk{Min: int64(10), Max: nil})
			state.SetResumePlan(stream, plan)
			state.SetChunks(stream, NewSet(plan.Array()...))

			restarted, err := state.Clone()
			require.NoError(t, err)

			recovered := restarted.GetResumePlan(stream)
			require.NotNil(t, recovered, "a restarted full refresh must reuse the same plan")
			require.Equal(t, plan.Len(), recovered.Len())
			recoveredMins := []string{}
			for _, chunk := range recovered.Array() {
				recoveredMins = append(recoveredMins, chunk.Min.(json.Number).String())
			}
			require.ElementsMatch(t, []string{"1", "10"}, recoveredMins)
			require.Equal(t, plan.Len(), restarted.GetChunks(stream).Len())
		})
	}
}

func TestFullRefreshResumePlanRetiresAfterLastChunk(t *testing.T) {
	state, stream := resumePlanTestState(FULLREFRESH)
	completed, tail := Chunk{Min: int64(1), Max: int64(10)}, Chunk{Min: int64(10), Max: nil}
	plan := NewSet(completed, tail)
	state.SetResumePlan(stream, plan)
	state.SetChunks(stream, NewSet(plan.Array()...))

	require.Equal(t, 1, state.RemoveChunk(stream, completed))
	require.NotNil(t, state.GetResumePlan(stream), "the plan must outlive partial progress")
	require.Equal(t, plan.Len(), state.GetResumePlan(stream).Len(), "progress must not shrink the plan")

	require.Zero(t, state.RemoveChunk(stream, tail))
	require.Nil(t, state.GetResumePlan(stream), "a finished full refresh must not leave a stale plan")
	require.True(t, state.HasCompletedBackfill(stream))
}

func TestResumePlanLeavesCursorsUntouched(t *testing.T) {
	state, stream := resumePlanTestState(CDC)
	state.SetCursor(stream, "lsn", "0/100")
	state.SetResumePlan(stream, NewSet(Chunk{Min: int64(1), Max: nil}))

	require.Equal(t, "0/100", state.GetCursor(stream, "lsn"))
	state.RemoveChunk(stream, Chunk{Min: int64(1), Max: nil})
	require.Equal(t, "0/100", state.GetCursor(stream, "lsn"))
}
