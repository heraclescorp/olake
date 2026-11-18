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

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
)

func recoveryTestOrigin() types.OrdinaryRecoveryOrigin {
	digest := strings.Repeat("a", 64)
	return types.OrdinaryRecoveryOrigin{Version: 1, OriginalExecutionID: "original", OriginalLaunchID: "original", OriginalReceiptSHA256: digest, SourceSHA256: digest, DestinationSHA256: digest, CatalogSHA256: digest, BaselineStateSHA256: digest}
}

func TestOrdinaryRecoveryCatalogHashesExactNamedBytes(t *testing.T) {
	dir := t.TempDir()
	legacy, available, selected := filepath.Join(dir, "streams.json"), filepath.Join(dir, "available_streams.json"), filepath.Join(dir, "selected_streams.json")
	require.NoError(t, os.WriteFile(legacy, []byte("{}\n"), 0600))
	require.NoError(t, os.WriteFile(available, []byte("[]"), 0600))
	require.NoError(t, os.WriteFile(selected, []byte("{}"), 0600))
	digest, err := ordinaryRecoveryCatalogSHA256("", available, selected)
	require.NoError(t, err)
	want := sha256.Sum256([]byte("available_streams.json\x002\x00[]selected_streams.json\x002\x00{}"))
	require.Equal(t, hex.EncodeToString(want[:]), digest)
	legacyDigest, err := ordinaryRecoveryCatalogSHA256(legacy, "", "")
	require.NoError(t, err)
	require.NotEqual(t, digest, legacyDigest)
	require.NoError(t, os.WriteFile(selected, []byte(" {}"), 0600))
	changed, err := ordinaryRecoveryCatalogSHA256("", available, selected)
	require.NoError(t, err)
	require.NotEqual(t, digest, changed, "raw-byte config binding must detect even whitespace drift")
	for _, paths := range [][3]string{{"", "", ""}, {legacy, available, selected}, {"", available, ""}, {"", "", selected}} {
		_, err := ordinaryRecoveryCatalogSHA256(paths[0], paths[1], paths[2])
		require.Error(t, err)
	}
}

func TestOrdinaryRecoveryRejectsOriginAndConfigDrift(t *testing.T) {
	oldSource, oldDestination, oldStreams, oldAvailable, oldSelected, oldOrigin := configPath, destinationConfigPath, streamsPath, availableStreamsPath, selectedStreamsPath, ordinaryRecoveryOriginPath
	t.Cleanup(func() {
		configPath, destinationConfigPath, streamsPath, availableStreamsPath, selectedStreamsPath, ordinaryRecoveryOriginPath = oldSource, oldDestination, oldStreams, oldAvailable, oldSelected, oldOrigin
	})
	dir := t.TempDir()
	configPath, destinationConfigPath, streamsPath = filepath.Join(dir, "source.json"), filepath.Join(dir, "destination.json"), filepath.Join(dir, "streams.json")
	availableStreamsPath, selectedStreamsPath = "", ""
	ordinaryRecoveryOriginPath = filepath.Join(dir, "ordinary-recovery-origin.json")
	for _, path := range []string{configPath, destinationConfigPath, streamsPath} {
		require.NoError(t, os.WriteFile(path, []byte("{}"), 0600))
	}
	origin := recoveryTestOrigin()
	var err error
	origin.SourceSHA256, err = recoveryFileSHA256(configPath)
	require.NoError(t, err)
	origin.DestinationSHA256, err = recoveryFileSHA256(destinationConfigPath)
	require.NoError(t, err)
	origin.CatalogSHA256, err = ordinaryRecoveryCatalogSHA256(streamsPath, "", "")
	require.NoError(t, err)
	encoded, err := json.Marshal(origin)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(ordinaryRecoveryOriginPath, encoded, 0600))
	require.NoError(t, verifyOrdinaryRecoveryConfig(origin))
	for _, path := range []string{configPath, destinationConfigPath, streamsPath} {
		require.NoError(t, os.WriteFile(path, []byte("{\"changed\":true}"), 0600))
		require.Error(t, verifyOrdinaryRecoveryConfig(origin))
		require.NoError(t, os.WriteFile(path, []byte("{}"), 0600))
	}
	changed := origin
	changed.OriginalReceiptSHA256 = strings.Repeat("b", 64)
	encoded, err = json.Marshal(changed)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(ordinaryRecoveryOriginPath, encoded, 0600))
	require.ErrorContains(t, verifyOrdinaryRecoveryConfig(origin), "receipt binding changed")
}

func TestOrdinaryRecoveryOnlyFrozenCompleteCDCSelection(t *testing.T) {
	discardStrayLogs(t)
	for _, mode := range []types.SyncMode{types.CDC, types.STRICTCDC} {
		saved := catalogOf(stream{name: "target", mode: mode, updateType: string(types.UpdateTypeEquality)})
		saved.Streams[0].Stream.SourceDefinedPrimaryKey = types.NewSet("id")
		checkpoint := &types.State{RWMutex: &sync.RWMutex{}, Type: types.GlobalType, Global: &types.GlobalState{State: map[string]any{"lsn": "0/100"}, Streams: types.NewSet("public.target", "public.unrelated")}, SuppressPersistence: true}
		selected, lower, err := ordinaryRecoverySelected(saved, checkpoint)
		require.NoError(t, err)
		require.Len(t, selected, 1)
		require.Equal(t, "0/100", lower)
		checkpoint.Global.Streams.Remove("public.target")
		_, _, err = ordinaryRecoverySelected(saved, checkpoint)
		require.Error(t, err, "never rebackfill a missing member, including strictCDC")
	}
	saved := catalogOf(stream{name: "target", mode: types.CDC, updateType: string(types.UpdateTypeEquality)}, stream{name: "missing", mode: types.CDC, schemaMissing: true})
	saved.Streams[0].Stream.SourceDefinedPrimaryKey = types.NewSet("id")
	checkpoint := &types.State{RWMutex: &sync.RWMutex{}, Type: types.GlobalType, Global: &types.GlobalState{State: map[string]any{"lsn": "0/100"}, Streams: types.NewSet("public.target", "public.missing")}, SuppressPersistence: true}
	_, _, err := ordinaryRecoverySelected(saved, checkpoint)
	require.ErrorContains(t, err, "cannot omit")
}

func TestOrdinaryRecoveryFlagsNeverReopenOriginalFence(t *testing.T) {
	oldOrigin, oldResult, oldExecution, oldLaunch, oldFence, oldState, oldSave, oldDiscover, oldPrefix := ordinaryRecoveryOriginPath, ordinaryRecoveryResultPath, executionID, executionLaunchID, executionFence, statePath, noSave, discoverSchema, destinationDatabasePrefix
	t.Cleanup(func() {
		ordinaryRecoveryOriginPath, ordinaryRecoveryResultPath, executionID, executionLaunchID, executionFence, statePath, noSave, discoverSchema, destinationDatabasePrefix = oldOrigin, oldResult, oldExecution, oldLaunch, oldFence, oldState, oldSave, oldDiscover, oldPrefix
	})
	ordinaryRecoveryOriginPath, ordinaryRecoveryResultPath = "ordinary-recovery-origin.json", "ordinary-recovery-result.json"
	executionID, executionLaunchID, statePath, noSave, discoverSchema, destinationDatabasePrefix = "recovery", "original", "state.json", false, false, ""
	executionFence = filepath.Join(t.TempDir(), "execution-fence.json")
	require.NoError(t, os.WriteFile(executionFence, []byte(`{"version":1,"execution_id":"recovery","state":"open"}`), 0600))
	cmd := &cobra.Command{Use: "sync"}
	cmd.SetContext(utils.WithExecution(context.Background(), &utils.ExecutionOptions{ID: executionID, LaunchID: executionLaunchID, Fence: executionFence}))
	require.NoError(t, validateOrdinaryRecoveryFlags(cmd))
	require.NoError(t, os.WriteFile(executionFence, []byte(`{"version":1,"execution_id":"recovery","state":"closed"}`), 0600))
	require.Error(t, validateOrdinaryRecoveryFlags(cmd))
	unchanged, err := os.ReadFile(executionFence)
	require.NoError(t, err)
	require.Contains(t, string(unchanged), `"state":"closed"`)
	cmd.Use = "resync"
	require.Error(t, validateOrdinaryRecoveryFlags(cmd))
}

func TestOrdinaryRecoveryServerPinSurvivesInterruptedEpochAndRejectsDrift(t *testing.T) {
	t.Setenv(constants.EnvS3ConfigFolder, "")
	oldOrigin := ordinaryRecoveryOriginPath
	t.Cleanup(func() { ordinaryRecoveryOriginPath = oldOrigin })
	ordinaryRecoveryOriginPath = filepath.Join(t.TempDir(), "ordinary-recovery-origin.json")
	origin := recoveryTestOrigin()
	require.NoError(t, pinOrdinaryRecoveryServer(context.Background(), origin, "system-one", "slot"))
	require.NoError(t, pinOrdinaryRecoveryServer(context.Background(), origin, "system-one", "slot"), "same immutable pin reused in successor")
	require.Error(t, pinOrdinaryRecoveryServer(context.Background(), origin, "system-two", "slot"))
	origin.SourceSHA256 = strings.Repeat("b", 64)
	require.Error(t, pinOrdinaryRecoveryServer(context.Background(), origin, "system-one", "slot"))
}

func TestOrdinaryRecoverySeparateResultBindsExactCommonBoundaryAndFullState(t *testing.T) {
	dir := t.TempDir()
	oldResult, oldURI, oldState := ordinaryRecoveryResultPath, ordinaryRecoveryResultURI, viper.GetString(constants.StatePath)
	t.Cleanup(func() {
		ordinaryRecoveryResultPath, ordinaryRecoveryResultURI = oldResult, oldURI
		viper.Set(constants.StatePath, oldState)
	})
	ordinaryRecoveryResultPath, ordinaryRecoveryResultURI = filepath.Join(dir, "ordinary-recovery-result.json"), ""
	stateFile := filepath.Join(dir, "state.json")
	viper.Set(constants.StatePath, stateFile)
	originalReceipt := filepath.Join(dir, "execution-result.json")
	require.NoError(t, os.WriteFile(originalReceipt, []byte("immutable original receipt"), 0600))
	checkpoint := &types.State{RWMutex: &sync.RWMutex{}, Type: types.GlobalType, Global: &types.GlobalState{State: map[string]any{"lsn": "0/300", "large": json.Number("9007199254740993")}, Streams: types.NewSet("public.a", "public.b", "public.unrelated")}, SuppressPersistence: true}
	result := types.OrdinaryRecoveryResult{OrdinaryRecoveryOrigin: recoveryTestOrigin(), ExecutionID: "recovery", LaunchID: "original", LowerLSN: "0/100", TargetLSN: "0/300", ServerIdentity: "system-one", SlotName: "slot", ToastValueSemantics: "current_source_lookup", Tables: []types.OrdinaryRecoveryTableProof{{Stream: "public.a", TableUUID: "00000000-0000-0000-0000-000000000001", FenceSnapshotID: 10, CommittedLSN: "0/300"}, {Stream: "public.b", TableUUID: "00000000-0000-0000-0000-000000000002", FenceSnapshotID: 20, CommittedLSN: "0/300"}}}
	result.Tables[1].CommittedLSN = "0/200"
	require.Error(t, writeOrdinaryRecoveryArtifacts(context.Background(), checkpoint, result))
	_, err := os.Stat(stateFile)
	require.True(t, os.IsNotExist(err), "mixed common boundary must not publish state")
	result.Tables[1].CommittedLSN = "0/400"
	require.Error(t, writeOrdinaryRecoveryArtifacts(context.Background(), checkpoint, result), "ahead is not exact common boundary")
	result.Tables[1].CommittedLSN = "0/300"
	require.NoError(t, writeOrdinaryRecoveryArtifacts(context.Background(), checkpoint, result))
	stateBytes, err := os.ReadFile(stateFile)
	require.NoError(t, err)
	require.Contains(t, string(stateBytes), "9007199254740993")
	require.Contains(t, string(stateBytes), "public.unrelated")
	resultBytes, err := os.ReadFile(ordinaryRecoveryResultPath)
	require.NoError(t, err)
	var observed types.OrdinaryRecoveryResult
	require.NoError(t, json.Unmarshal(resultBytes, &observed))
	digest, err := utils.CanonicalStateSHA256(stateBytes)
	require.NoError(t, err)
	require.Equal(t, digest, observed.StateSHA256)
	require.Equal(t, "original", observed.LaunchID)
	original, err := os.ReadFile(originalReceipt)
	require.NoError(t, err)
	require.Equal(t, "immutable original receipt", string(original))
}

func TestOrdinaryRecoveryOriginRequiresCompleteImmutableBindings(t *testing.T) {
	origin := recoveryTestOrigin()
	require.NoError(t, validateOrdinaryRecoveryOrigin(origin))
	for _, mutate := range []func(*types.OrdinaryRecoveryOrigin){
		func(o *types.OrdinaryRecoveryOrigin) { o.Version = 2 },
		func(o *types.OrdinaryRecoveryOrigin) { o.OriginalExecutionID = "" },
		func(o *types.OrdinaryRecoveryOrigin) { o.OriginalLaunchID = " " },
		func(o *types.OrdinaryRecoveryOrigin) { o.OriginalReceiptSHA256 = "" },
		func(o *types.OrdinaryRecoveryOrigin) { o.SourceSHA256 = strings.Repeat("A", 64) },
		func(o *types.OrdinaryRecoveryOrigin) { o.DestinationSHA256 = "not-a-hash" },
		func(o *types.OrdinaryRecoveryOrigin) { o.CatalogSHA256 = strings.Repeat("a", 63) },
		func(o *types.OrdinaryRecoveryOrigin) { o.BaselineStateSHA256 = "" },
	} {
		changed := origin
		mutate(&changed)
		require.Error(t, validateOrdinaryRecoveryOrigin(changed))
	}
	for _, malformed := range []string{`null`, `[]`, `{}`, `{} {}`, `{"version":1,"unknown":true}`} {
		var decoded types.OrdinaryRecoveryOrigin
		err := strictRecoveryJSON([]byte(malformed), &decoded)
		if err == nil {
			err = validateOrdinaryRecoveryOrigin(decoded)
		}
		require.Error(t, err, malformed)
	}
}

func TestOrdinaryRecoveryInitialStateLoadPreservesExactBaseline(t *testing.T) {
	baseline := []byte(`{"type":"GLOBAL","version":1,"global":{"state":{"lsn":"0/100","unchanged_large":9007199254740993,"nested":{"max":18446744073709551615}},"streams":["public.a"]}}`)
	path := filepath.Join(t.TempDir(), "state.json")
	require.NoError(t, os.WriteFile(path, baseline, 0600))
	checkpoint := &types.State{RWMutex: &sync.RWMutex{}}
	require.NoError(t, loadOrdinaryRecoveryState(path, checkpoint))
	// This is the typed checkpoint used by the initial sync pre-run log, not a
	// later recovery-only reload after float64 has already rounded its values.
	encoded, err := checkpoint.MarshalJSON()
	require.NoError(t, err)
	want, err := utils.CanonicalStateSHA256(baseline)
	require.NoError(t, err)
	got, err := utils.CanonicalStateSHA256(encoded)
	require.NoError(t, err)
	require.Equal(t, want, got)
	require.Contains(t, string(encoded), "9007199254740993")
	require.Contains(t, string(encoded), "18446744073709551615")
	for _, malformed := range []string{`null`, `[]`, `{} {}`, `{} trailing`} {
		require.Error(t, decodeOrdinaryRecoveryState([]byte(malformed), &types.State{}))
	}
}
