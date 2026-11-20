package protocol

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
	"github.com/stretchr/testify/require"
)

func backfillRecoveryFixture(t *testing.T) (*types.State, []types.StreamInterface) {
	t.Helper()
	saved := catalogOf(stream{name: "partial", mode: types.CDC, updateType: string(types.UpdateTypeEquality)}, stream{name: "complete", mode: types.CDC, updateType: string(types.UpdateTypeEquality)}, stream{name: "unstarted", mode: types.CDC, updateType: string(types.UpdateTypeEquality)})
	selected := make([]types.StreamInterface, len(saved.Streams))
	for index, stream := range saved.Streams {
		stream.Stream.SourceDefinedPrimaryKey = types.NewSet("id")
		selected[index] = stream
	}
	checkpoint := &types.State{RWMutex: &sync.RWMutex{}, Type: types.GlobalType, Global: &types.GlobalState{State: map[string]any{"lsn": "0/100", "large": json.Number("9007199254740993")}, Streams: types.NewSet[string]()}, SuppressPersistence: true, BackfillCheckpoint: true}
	checkpoint.BackfillSource = &types.BackfillSourceBinding{SourceSHA256: strings.Repeat("a", 64), DestinationSHA256: strings.Repeat("b", 64), CatalogSHA256: strings.Repeat("c", 64), ServerIdentity: "live-system-identity", SlotName: "slot"}
	for _, stream := range selected[:2] {
		plan := types.NewSet(types.Chunk{Min: nil, Max: json.Number("9007199254740993")}, types.Chunk{Min: json.Number("9007199254740993"), Max: nil})
		require.NoError(t, checkpoint.SetBackfillPlan(stream.Self(), plan))
		checkpoint.SetBackfillTable(stream.Self(), &types.BackfillTableBinding{TableUUID: "00000000-0000-0000-0000-000000000001", ShapeSHA256: strings.Repeat("a", 64)})
	}
	require.NoError(t, checkpoint.CompleteBackfill(selected[1].ID()))
	return checkpoint, selected
}

func TestBackfillCheckpointPublishesSeparateFencedEvidenceAndPreservesLower(t *testing.T) {
	dir := t.TempDir()
	fence := filepath.Join(dir, "execution-fence.json")
	require.NoError(t, os.WriteFile(fence, []byte(`{"version":1,"execution_id":"original","state":"open"}`), 0600))
	ctx := utils.WithExecution(context.Background(), &utils.ExecutionOptions{ID: "original", LaunchID: "launch", Fence: fence, Command: "sync"})
	checkpoint, selected := backfillRecoveryFixture(t)
	authoritative := filepath.Join(dir, "state.json")
	require.NoError(t, os.WriteFile(authoritative, []byte("{}"), 0600))
	candidate := types.BackfillRecoveryCheckpoint{Version: 1, ExecutionID: "original", LaunchID: "launch", SourceSHA256: strings.Repeat("a", 64), DestinationSHA256: strings.Repeat("b", 64), CatalogSHA256: strings.Repeat("c", 64), ServerIdentity: "live-system-identity", SlotName: "slot"}
	var lower string
	output := filepath.Join(dir, backfillCheckpointName)
	checkpoint.BackfillPersist = func(data []byte) error {
		return persistBackfillCheckpoint(ctx, &candidate, &lower, output, "", data, selected)
	}
	require.NoError(t, checkpoint.PersistBackfill())
	beforeTransition, err := os.ReadFile(output)
	require.NoError(t, err)
	require.NoError(t, checkpoint.SetBackfillPlan(selected[2].Self(), types.NewSet(types.Chunk{})))
	duringTransition, err := os.ReadFile(output)
	require.NoError(t, err)
	require.Equal(t, beforeTransition, duringTransition, "a plan awaiting its atomic table-identity binding must not replace the valid durable candidate")
	checkpoint.SetBackfillTable(selected[2].Self(), &types.BackfillTableBinding{TableUUID: "00000000-0000-0000-0000-000000000003", ShapeSHA256: strings.Repeat("a", 64)})
	require.NoError(t, checkpoint.PersistBackfill())
	checkpoint.SetGlobal(map[string]any{"lsn": "0/900", "large": json.Number("9007199254740993")})
	require.NoError(t, checkpoint.ErrBackfillPersistence())
	encoded, err := os.ReadFile(output)
	require.NoError(t, err)
	var observed types.BackfillRecoveryCheckpoint
	require.NoError(t, strictRecoveryJSON(encoded, &observed))
	require.Equal(t, "original", observed.ExecutionID)
	require.Equal(t, "launch", observed.LaunchID)
	require.Contains(t, string(observed.State), `"lsn":"0/100"`)
	require.Contains(t, string(observed.State), "9007199254740993")
	require.NoError(t, validateBackfillRecoveryState(observed.State, selected))
	digest, err := utils.CanonicalStateSHA256(observed.State)
	require.NoError(t, err)
	require.Equal(t, digest, observed.StateSHA256)
	saved, err := os.ReadFile(authoritative)
	require.NoError(t, err)
	require.Equal(t, "{}", string(saved))
	require.NoError(t, os.WriteFile(fence, []byte(`{"version":1,"execution_id":"original","state":"closed"}`), 0600))
	require.Error(t, checkpoint.PersistBackfill())
	afterFence, err := os.ReadFile(output)
	require.NoError(t, err)
	require.Equal(t, encoded, afterFence)
	require.NoError(t, os.WriteFile(fence, []byte(`{"version":1,"execution_id":"original","state":"open"}`), 0600))
	require.Error(t, checkpoint.PersistBackfill(), "a failed candidate publication cannot be silently retried into dependent work")
}

func TestBackfillCheckpointReadRejectsForeignCorruptAndMissingEvidence(t *testing.T) {
	oldOrigin := ordinaryRecoveryOriginPath
	t.Cleanup(func() { ordinaryRecoveryOriginPath = oldOrigin })
	dir := t.TempDir()
	ordinaryRecoveryOriginPath = filepath.Join(dir, "ordinary-recovery-origin.json")
	checkpoint, _ := backfillRecoveryFixture(t)
	data, err := checkpoint.MarshalJSON()
	require.NoError(t, err)
	origin := recoveryTestOrigin()
	origin.Backfill = true
	origin.SavedStateSHA256 = strings.Repeat("d", 64)
	origin.BaselineStateSHA256, err = utils.CanonicalStateSHA256(data)
	require.NoError(t, err)
	candidate := types.BackfillRecoveryCheckpoint{Version: 1, ExecutionID: origin.OriginalExecutionID, LaunchID: origin.OriginalLaunchID, SourceSHA256: origin.SourceSHA256, DestinationSHA256: origin.DestinationSHA256, CatalogSHA256: origin.CatalogSHA256, StateSHA256: origin.BaselineStateSHA256, State: data, ServerIdentity: "live-system", SlotName: "slot"}
	publish := func(candidate types.BackfillRecoveryCheckpoint) {
		encoded, err := json.Marshal(candidate)
		require.NoError(t, err)
		sum := sha256.Sum256(encoded)
		origin.BackfillCheckpointSHA256 = hex.EncodeToString(sum[:])
		require.NoError(t, os.WriteFile(filepath.Join(dir, backfillCheckpointName), encoded, 0600))
	}
	_, err = loadBackfillRecoveryCheckpoint(origin, data)
	require.Error(t, err)
	publish(candidate)
	_, err = loadBackfillRecoveryCheckpoint(origin, data)
	require.NoError(t, err)
	for _, mutate := range []func(*types.BackfillRecoveryCheckpoint){
		func(c *types.BackfillRecoveryCheckpoint) { c.ExecutionID = "foreign" },
		func(c *types.BackfillRecoveryCheckpoint) { c.LaunchID = "foreign" },
		func(c *types.BackfillRecoveryCheckpoint) { c.SourceSHA256 = strings.Repeat("b", 64) },
		func(c *types.BackfillRecoveryCheckpoint) { c.State = json.RawMessage(`{}`) },
		func(c *types.BackfillRecoveryCheckpoint) { c.ServerIdentity = "" },
		func(c *types.BackfillRecoveryCheckpoint) { c.SlotName = "" },
	} {
		changed := candidate
		mutate(&changed)
		publish(changed)
		_, err := loadBackfillRecoveryCheckpoint(origin, data)
		require.Error(t, err)
	}
	publish(candidate)
	require.NoError(t, os.WriteFile(filepath.Join(dir, backfillCheckpointName), []byte(`{}`), 0600))
	_, err = loadBackfillRecoveryCheckpoint(origin, data)
	require.ErrorContains(t, err, "differs from immutable origin")
}

func TestBackfillCheckpointRejectsIncompletePlansWithoutInventingProgress(t *testing.T) {
	checkpoint, selected := backfillRecoveryFixture(t)
	original, err := checkpoint.MarshalJSON()
	require.NoError(t, err)
	require.NoError(t, validateBackfillRecoveryState(original, selected))
	for name, mutate := range map[string]func(map[string]any){
		"missing_lsn":       func(s map[string]any) { delete(s["global"].(map[string]any)["state"].(map[string]any), "lsn") },
		"foreign_completed": func(s map[string]any) { s["global"].(map[string]any)["streams"] = []string{"public.foreign"} },
		"duplicate_completed": func(s map[string]any) {
			s["global"].(map[string]any)["streams"] = []string{"public.complete", "public.complete"}
		},
		"missing_binding": func(s map[string]any) {
			delete(s["streams"].([]any)[0].(map[string]any)["state"].(map[string]any), types.BackfillTableKey)
		},
		"missing_plan": func(s map[string]any) {
			delete(s["streams"].([]any)[0].(map[string]any)["state"].(map[string]any), types.ResumePlanKey)
		},
		"missing_retained_plan": func(s map[string]any) {
			delete(s["streams"].([]any)[1].(map[string]any)["state"].(map[string]any), types.BackfillPlanKey)
		},
		"unknown_pending": func(s map[string]any) {
			s["streams"].([]any)[0].(map[string]any)["state"].(map[string]any)[types.ChunksKey] = []any{map[string]any{"min": "foreign", "max": nil}}
		},
		"duplicate_stream": func(s map[string]any) { entries := s["streams"].([]any); s["streams"] = append(entries, entries[0]) },
	} {
		t.Run(name, func(t *testing.T) {
			var object map[string]any
			require.NoError(t, json.Unmarshal(original, &object))
			mutate(object)
			data, err := json.Marshal(object)
			require.NoError(t, err)
			require.Error(t, validateBackfillRecoveryState(data, selected))
		})
	}
}

func TestBackfillCandidateDoesNotChangeLegacySettledCDCOrEnrollment(t *testing.T) {
	checkpoint, selected := backfillRecoveryFixture(t)
	legacy := &types.State{RWMutex: &sync.RWMutex{}, Type: types.GlobalType, Global: &types.GlobalState{State: map[string]any{"lsn": "0/100"}, Streams: types.NewSet(selected[0].ID(), selected[1].ID(), selected[2].ID())}, SuppressPersistence: true}
	require.False(t, backfillCheckpointEligible(legacy, selected))
	legacy.Global.Streams.Remove(selected[2].ID())
	require.False(t, backfillCheckpointEligible(legacy, selected), "new enrollment on a legacy CDC checkpoint must keep the existing normal path without fabricated historic bindings")
	require.False(t, legacy.BackfillCheckpoint)
	require.Empty(t, legacy.Streams)
	require.True(t, backfillCheckpointEligible(checkpoint, selected), "modern retained partial plans remain resumable")
	checkpoint.Global.Streams.Insert(selected[0].ID(), selected[2].ID())
	require.False(t, backfillCheckpointEligible(checkpoint, selected))
}

func TestBackfillOriginRequiresCandidateAndSavedStateBindings(t *testing.T) {
	origin := recoveryTestOrigin()
	origin.Backfill = true
	require.Error(t, validateOrdinaryRecoveryOrigin(origin))
	origin.BackfillCheckpointSHA256 = strings.Repeat("b", 64)
	require.Error(t, validateOrdinaryRecoveryOrigin(origin))
	origin.SavedStateSHA256 = strings.Repeat("c", 64)
	require.NoError(t, validateOrdinaryRecoveryOrigin(origin))
	origin.Backfill = false
	require.Error(t, validateOrdinaryRecoveryOrigin(origin), "CDC-only origins cannot smuggle candidate bindings")
	origin.Backfill = true
	origin.BackfillCheckpointSHA256 = strings.Repeat("B", 64)
	require.Error(t, validateOrdinaryRecoveryOrigin(origin))
}

func TestBackfillSourcePinDoesNotBlockHealthyModernEnrollment(t *testing.T) {
	checkpoint, selected := backfillRecoveryFixture(t)
	saved := checkpoint.BackfillSource
	candidate := &types.BackfillRecoveryCheckpoint{SourceSHA256: saved.SourceSHA256, DestinationSHA256: saved.DestinationSHA256, CatalogSHA256: strings.Repeat("d", 64)}
	eligible, err := backfillCheckpointConfigMatches(checkpoint, selected, candidate)
	require.Error(t, err, "saved unfinished resume plans keep the original frozen scope")
	require.False(t, eligible)
	require.NoError(t, checkpoint.CompleteBackfill(selected[0].ID()))
	eligible, err = backfillCheckpointConfigMatches(checkpoint, selected, candidate)
	require.NoError(t, err, "all old tables complete plus a newly enrolled table follows the existing healthy normal path")
	require.False(t, eligible, "mixed enrollment must not fabricate a new historical candidate scope")
	candidate.CatalogSHA256 = saved.CatalogSHA256
	eligible, err = backfillCheckpointConfigMatches(checkpoint, selected, candidate)
	require.NoError(t, err)
	require.True(t, eligible, "same frozen initial catalog with unstarted tables may continue")
}
