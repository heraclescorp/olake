package types

import (
	"encoding/json"
	"errors"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBackfillPlanAndCompletionAreAtomicDurableTransitions(t *testing.T) {
	state, stream := resumePlanTestState(CDC)
	state.Type = GlobalType
	state.Global = &GlobalState{State: map[string]any{"lsn": "0/100"}, Streams: NewSet[string]()}
	state.BackfillCheckpoint = true
	var snapshots [][]byte
	state.BackfillPersist = func(data []byte) error {
		snapshots = append(snapshots, append([]byte(nil), data...))
		return nil
	}
	first, tail := Chunk{Min: nil, Max: int64(9007199254740993)}, Chunk{Min: int64(9007199254740993), Max: nil}
	require.NoError(t, state.SetBackfillPlan(stream, NewSet(first, tail)))
	require.Equal(t, 1, state.RemoveChunk(stream, first))
	require.Zero(t, state.RemoveChunk(stream, tail))
	require.NotNil(t, state.GetResumePlan(stream), "empty pending set cannot retire a plan before completed GLOBAL membership")
	require.False(t, state.HasCompletedBackfill(stream))
	require.NoError(t, state.CompleteBackfill(stream.ID()))
	require.True(t, state.HasCompletedBackfill(stream))
	require.Nil(t, state.GetResumePlan(stream))
	require.Equal(t, 2, state.GetBackfillPlan(stream).Len())
	publications := len(snapshots)
	require.NoError(t, state.CompleteBackfill(stream.ID()))
	require.Len(t, snapshots, publications)
	for _, data := range snapshots {
		recovered := &State{RWMutex: &sync.RWMutex{}}
		require.NoError(t, json.Unmarshal(data, recovered))
		require.Equal(t, 2, recovered.GetBackfillPlan(stream).Len())
		require.NotNil(t, recovered.GetChunks(stream))
		if recovered.HasCompletedBackfill(stream) {
			require.Nil(t, recovered.GetResumePlan(stream))
			require.Zero(t, recovered.GetChunks(stream).Len())
		} else {
			require.NotNil(t, recovered.GetResumePlan(stream))
			require.Equal(t, 2, recovered.GetResumePlan(stream).Len())
		}
		for _, chunk := range recovered.GetBackfillPlan(stream).Array() {
			if chunk.Min != nil {
				require.Equal(t, "9007199254740993", chunk.Min.(json.Number).String())
			}
		}
	}
}

func TestBackfillPersistenceFailureSticksBeforeDependentDispatch(t *testing.T) {
	state, stream := resumePlanTestState(CDC)
	failure := errors.New("durable candidate unavailable")
	calls := 0
	state.BackfillPersist = func([]byte) error { calls++; return failure }
	require.ErrorIs(t, state.SetBackfillPlan(stream, NewSet(Chunk{})), failure)
	require.ErrorIs(t, state.PersistBackfill(), failure)
	state.SetCursor(stream, "cursor", 42)
	require.ErrorIs(t, state.ErrBackfillPersistence(), failure)
	require.Equal(t, 1, calls)
}
