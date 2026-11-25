package protocol

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
)

func recoveryBatchFixture(t *testing.T) (*types.State, types.OrdinaryRecoveryResult, []types.StreamInterface) {
	t.Helper()
	discardStrayLogs(t)
	t.Setenv(constants.EnvS3ConfigFolder, "")
	oldResult, oldURI, oldOrigin, oldID := ordinaryRecoveryResultPath, ordinaryRecoveryResultURI, ordinaryRecoveryOriginPath, executionID
	oldState := viper.GetString(constants.StatePath)
	t.Cleanup(func() {
		ordinaryRecoveryResultPath, ordinaryRecoveryResultURI, ordinaryRecoveryOriginPath, executionID = oldResult, oldURI, oldOrigin, oldID
		viper.Set(constants.StatePath, oldState)
	})
	dir := t.TempDir()
	ordinaryRecoveryResultPath, ordinaryRecoveryResultURI = filepath.Join(dir, "ordinary-recovery-result.json"), ""
	ordinaryRecoveryOriginPath = filepath.Join(dir, "ordinary-recovery-origin.json")
	viper.Set(constants.StatePath, filepath.Join(dir, "state.json"))
	checkpoint := &types.State{RWMutex: &sync.RWMutex{}, Type: types.GlobalType, Version: 1, Global: &types.GlobalState{State: map[string]any{"lsn": "0/200", "large": json.Number("9007199254740993")}, Streams: types.NewSet("public.a", "public.b", "public.unrelated")}, SuppressPersistence: true}
	result := types.OrdinaryRecoveryResult{OrdinaryRecoveryOrigin: recoveryTestOrigin(), ExecutionID: "recovery-one", LaunchID: "original", LowerLSN: "0/100", TargetLSN: "0/200", ServerIdentity: "system", SlotName: "slot", ToastValueSemantics: "current_source_lookup", Tables: []types.OrdinaryRecoveryTableProof{{Stream: "public.a", TableUUID: "00000000-0000-0000-0000-000000000001", FenceSnapshotID: 10, CommittedLSN: "0/200"}, {Stream: "public.b", TableUUID: "00000000-0000-0000-0000-000000000002", FenceSnapshotID: 20, CommittedLSN: "0/300"}}}
	saved := catalogOf(stream{name: "a", mode: types.CDC, updateType: string(types.UpdateTypeEquality)}, stream{name: "b", mode: types.CDC, updateType: string(types.UpdateTypeEquality)})
	for _, item := range saved.Streams {
		item.Stream.SourceDefinedPrimaryKey = types.NewSet("id")
	}
	selected, _, err := ordinaryRecoverySelected(saved, checkpoint)
	require.NoError(t, err)
	return checkpoint, result, selected
}

func TestRecoveryCheckpointIntervalAlwaysPositive(t *testing.T) {
	old := recoveryCheckpointInterval
	t.Cleanup(func() { recoveryCheckpointInterval = old })
	recoveryCheckpointInterval = "5m"
	cmd := &cobra.Command{Use: "sync"}
	cmd.Flags().String("recovery-checkpoint-interval", "5m", "")
	t.Setenv("OLAKE_RECOVERY_CHECKPOINT_INTERVAL", "17s")
	interval, err := ordinaryRecoveryInterval(cmd)
	require.NoError(t, err)
	require.Equal(t, 17*time.Second, interval)
	for _, value := range []string{"", "0", "0s", "-1s", "invalid"} {
		t.Setenv("OLAKE_RECOVERY_CHECKPOINT_INTERVAL", value)
		_, err := ordinaryRecoveryInterval(cmd)
		require.Error(t, err)
	}
	require.NoError(t, cmd.Flags().Set("recovery-checkpoint-interval", "2m"))
	recoveryCheckpointInterval = "2m"
	interval, err = ordinaryRecoveryInterval(cmd)
	require.NoError(t, err)
	require.Equal(t, 2*time.Minute, interval)
}

func TestRecoveryCheckpointPublicationPrecedesAcknowledgementAndFinalArtifacts(t *testing.T) {
	checkpoint, result, selected := recoveryBatchFixture(t)
	statePath := viper.GetString(constants.StatePath)
	require.NoError(t, os.WriteFile(statePath, []byte("immutable baseline"), 0600))
	originalReceipt := filepath.Join(filepath.Dir(statePath), "execution-result.json")
	require.NoError(t, os.WriteFile(originalReceipt, []byte("original receipt"), 0600))
	local, _, err := utils.OrdinaryRecoveryCheckpointPaths(ordinaryRecoveryResultPath, "", result.ExecutionID, result.TargetLSN)
	require.NoError(t, err)
	ackFailure := errors.New("crash while acknowledging")
	acknowledgements := 0
	_, _, err = persistOrdinaryRecoveryBatch(context.Background(), checkpoint, result, "0/300", "", "", func() error {
		acknowledgements++
		data, err := os.ReadFile(local)
		require.NoError(t, err)
		var observed types.OrdinaryRecoveryCheckpoint
		require.NoError(t, strictRecoveryJSON(data, &observed))
		require.NoError(t, validateOrdinaryRecoveryCheckpoint(observed, result.OrdinaryRecoveryOrigin, selected, "0/100"))
		require.Contains(t, string(observed.State), "9007199254740993")
		return ackFailure
	})
	require.ErrorIs(t, err, ackFailure)
	require.Equal(t, 1, acknowledgements)
	data, err := os.ReadFile(local)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(filepath.Dir(ordinaryRecoveryOriginPath), utils.OrdinaryRecoveryCheckpointName), data, 0600))
	executionID = "recovery-two"
	resumed, digest, err := loadOrdinaryRecoveryCheckpoint(context.Background(), result.OrdinaryRecoveryOrigin)
	require.NoError(t, err)
	require.Len(t, digest, 64)
	require.NoError(t, validateOrdinaryRecoveryCheckpoint(*resumed, result.OrdinaryRecoveryOrigin, selected, "0/100"))
	require.NoError(t, validateOrdinaryRecoveryResumeProofs(*resumed, result.Tables))
	for path, want := range map[string]string{statePath: "immutable baseline", originalReceipt: "original receipt"} {
		observed, err := os.ReadFile(path)
		require.NoError(t, err)
		require.Equal(t, want, string(observed))
	}
	_, err = os.Stat(ordinaryRecoveryResultPath)
	require.True(t, os.IsNotExist(err))
	require.Error(t, writeOrdinaryRecoveryArtifacts(context.Background(), checkpoint, result), "ahead table proof is allowed for a batch, not final settlement")
}

func TestRecoveryCheckpointPublicationFailureNeverAcknowledges(t *testing.T) {
	checkpoint, result, _ := recoveryBatchFixture(t)
	require.NoError(t, os.WriteFile(filepath.Join(filepath.Dir(ordinaryRecoveryResultPath), "ordinary-recovery-checkpoints"), []byte("not a directory"), 0600))
	acknowledged := false
	_, _, err := persistOrdinaryRecoveryBatch(context.Background(), checkpoint, result, "0/300", "", "", func() error {
		acknowledged = true
		return nil
	})
	require.Error(t, err)
	require.False(t, acknowledged)
}

func TestRecoveryCheckpointImmutableChainAndFinalOnlySettlement(t *testing.T) {
	checkpoint, result, _ := recoveryBatchFixture(t)
	first, firstHash, err := persistOrdinaryRecoveryBatch(context.Background(), checkpoint, result, "0/300", "", "", func() error { return nil })
	require.NoError(t, err)
	final, err := ordinaryRecoveryCompletedState(checkpoint, "0/300")
	require.NoError(t, err)
	result.TargetLSN = "0/300"
	for index := range result.Tables {
		result.Tables[index].CommittedLSN = "0/300"
	}
	last, _, err := persistOrdinaryRecoveryBatch(context.Background(), final, result, "0/300", firstHash, first.ExecutionID, func() error { return nil })
	require.NoError(t, err)
	require.Equal(t, firstHash, last.PreviousCheckpointSHA256)
	require.Equal(t, first.ExecutionID, last.PreviousExecutionID)
	_, err = os.Stat(ordinaryRecoveryResultPath)
	require.True(t, os.IsNotExist(err), "even final checkpoint publication is not settlement")
	require.NoError(t, writeOrdinaryRecoveryArtifacts(context.Background(), final, result))
	_, err = os.Stat(ordinaryRecoveryResultPath)
	require.NoError(t, err)
	acknowledged := false
	_, _, err = persistOrdinaryRecoveryBatch(context.Background(), final, result, "0/300", strings.Repeat("b", 64), "other-runtime", func() error { acknowledged = true; return nil })
	require.Error(t, err, "same checkpoint name must never replace committed evidence")
	require.False(t, acknowledged)
}

func TestRecoveryCheckpointRejectsUnboundOrRegressedProgress(t *testing.T) {
	checkpoint, result, selected := recoveryBatchFixture(t)
	manifest, _, err := persistOrdinaryRecoveryBatch(context.Background(), checkpoint, result, "0/300", "", "", func() error { return nil })
	require.NoError(t, err)
	for _, mutate := range []func(*types.OrdinaryRecoveryCheckpoint){
		func(c *types.OrdinaryRecoveryCheckpoint) { c.SourceSHA256 = strings.Repeat("b", 64) },
		func(c *types.OrdinaryRecoveryCheckpoint) { c.StateSHA256 = strings.Repeat("b", 64) },
		func(c *types.OrdinaryRecoveryCheckpoint) { c.LowerLSN = "0/101" },
		func(c *types.OrdinaryRecoveryCheckpoint) { c.FinalTargetLSN = "0/100" },
		func(c *types.OrdinaryRecoveryCheckpoint) { c.ExecutionID = c.OriginalExecutionID },
		func(c *types.OrdinaryRecoveryCheckpoint) { c.LaunchID = "foreign" },
		func(c *types.OrdinaryRecoveryCheckpoint) { c.PreviousExecutionID = "unbound" },
		func(c *types.OrdinaryRecoveryCheckpoint) { c.Tables = c.Tables[:1] },
		func(c *types.OrdinaryRecoveryCheckpoint) { c.Tables[0].CommittedLSN = "0/100" },
		func(c *types.OrdinaryRecoveryCheckpoint) { c.Tables[0].CommittedLSN = "0/400" },
		func(c *types.OrdinaryRecoveryCheckpoint) { c.Tables[0].Stream = "public.foreign" },
	} {
		changed := *manifest
		changed.Tables = append([]types.OrdinaryRecoveryTableProof(nil), manifest.Tables...)
		mutate(&changed)
		require.Error(t, validateOrdinaryRecoveryCheckpoint(changed, result.OrdinaryRecoveryOrigin, selected, "0/100"))
	}
	for _, mutate := range []func([]types.OrdinaryRecoveryTableProof){
		func(p []types.OrdinaryRecoveryTableProof) { p[1].CommittedLSN = "0/200" },
		func(p []types.OrdinaryRecoveryTableProof) { p[0].TableUUID = "00000000-0000-0000-0000-000000000003" },
		func(p []types.OrdinaryRecoveryTableProof) { p[0].CommittedLSN = "0/400" },
		func(p []types.OrdinaryRecoveryTableProof) { p[0] = p[1] },
	} {
		proofs := append([]types.OrdinaryRecoveryTableProof(nil), result.Tables...)
		mutate(proofs)
		require.Error(t, validateOrdinaryRecoveryResumeProofs(*manifest, proofs))
	}
}

func TestRecoveryCheckpointPreservesOriginalStateOutsideProgress(t *testing.T) {
	checkpoint, result, selected := recoveryBatchFixture(t)
	manifest, _, err := persistOrdinaryRecoveryBatch(context.Background(), checkpoint, result, "0/300", "", "", func() error { return nil })
	require.NoError(t, err)
	require.NoError(t, validateOrdinaryRecoveryCheckpointBaseline(*manifest, checkpoint, selected))
	for _, replacement := range [][2]string{
		{`9007199254740993`, `9007199254740994`},
		{`public.unrelated`, `public.foreign`},
		{`"version":1`, `"version":2`},
	} {
		changed := *manifest
		changed.State = bytes.ReplaceAll(manifest.State, []byte(replacement[0]), []byte(replacement[1]))
		require.NotEqual(t, manifest.State, changed.State)
		require.Error(t, validateOrdinaryRecoveryCheckpointBaseline(changed, checkpoint, selected))
	}
	local := filepath.Join(filepath.Dir(ordinaryRecoveryOriginPath), utils.OrdinaryRecoveryCheckpointName)
	data, err := json.Marshal(manifest)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(local, data, 0600))
	executionID = manifest.ExecutionID
	_, _, err = loadOrdinaryRecoveryCheckpoint(context.Background(), result.OrdinaryRecoveryOrigin)
	require.ErrorContains(t, err, "retired runtime")
	executionID = "successor"
	data = append(data, []byte(` {}`)...)
	require.NoError(t, os.WriteFile(local, data, 0600))
	_, _, err = loadOrdinaryRecoveryCheckpoint(context.Background(), result.OrdinaryRecoveryOrigin)
	require.Error(t, err)
}
