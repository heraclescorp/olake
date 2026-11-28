package protocol

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
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
	for _, receiptHash := range []string{"", strings.Repeat("b", 64)} {
		changed := origin
		changed.OriginalReceiptSHA256 = receiptHash
		encoded, err = json.Marshal(changed)
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(ordinaryRecoveryOriginPath, encoded, 0600))
		require.ErrorContains(t, verifyOrdinaryRecoveryConfig(origin), "receipt binding changed")
	}
	origin.OriginalReceiptSHA256 = ""
	encoded, err = json.Marshal(origin)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(ordinaryRecoveryOriginPath, encoded, 0600))
	require.NoError(t, verifyOrdinaryRecoveryConfig(origin))
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
	t.Setenv(constants.EnvStorageMode, "nfs")
	t.Setenv(constants.EnvS3ConfigFolder, "")
	oldOrigin, oldResult, oldExecution, oldLaunch, oldFence, oldState, oldSave, oldDiscover, oldPrefix := ordinaryRecoveryOriginPath, ordinaryRecoveryResultPath, executionID, executionLaunchID, executionFence, statePath, noSave, discoverSchema, destinationDatabasePrefix
	oldCheckpoint := backfillRecoveryCheckpointPath
	t.Cleanup(func() {
		ordinaryRecoveryOriginPath, ordinaryRecoveryResultPath, executionID, executionLaunchID, executionFence, statePath, noSave, discoverSchema, destinationDatabasePrefix = oldOrigin, oldResult, oldExecution, oldLaunch, oldFence, oldState, oldSave, oldDiscover, oldPrefix
		backfillRecoveryCheckpointPath = oldCheckpoint
	})
	ordinaryRecoveryOriginPath, ordinaryRecoveryResultPath = "ordinary-recovery-origin.json", "ordinary-recovery-result.json"
	backfillRecoveryCheckpointPath = ""
	executionID, executionLaunchID, statePath, noSave, discoverSchema, destinationDatabasePrefix = "recovery", "original", "state.json", false, false, ""
	executionFence = filepath.Join(t.TempDir(), "execution-fence.json")
	require.NoError(t, os.WriteFile(executionFence, []byte(`{"version":1,"execution_id":"recovery","state":"open"}`), 0600))
	cmd := &cobra.Command{Use: "sync"}
	cmd.SetContext(utils.WithExecution(context.Background(), &utils.ExecutionOptions{ID: executionID, LaunchID: executionLaunchID, Fence: executionFence}))
	require.NoError(t, validateOrdinaryRecoveryFlags(cmd))
	require.NoError(t, utils.ResolveS3Paths(cmd.Context(), nil, nil))
	require.NoError(t, os.WriteFile(executionFence, []byte(`{"version":1,"execution_id":"recovery","state":"closed"}`), 0600))
	require.Error(t, utils.ResolveS3Paths(cmd.Context(), nil, nil))
	unchanged, err := os.ReadFile(executionFence)
	require.NoError(t, err)
	require.Contains(t, string(unchanged), `"state":"closed"`)
	backfillRecoveryCheckpointPath = backfillCheckpointName
	require.NoError(t, validateOrdinaryRecoveryFlags(cmd))
	ordinaryRecoveryOriginPath, ordinaryRecoveryResultPath, executionLaunchID = "", "", ""
	require.ErrorContains(t, validateOrdinaryRecoveryFlags(cmd), "only valid for ordinary recovery")
	ordinaryRecoveryOriginPath, ordinaryRecoveryResultPath, executionLaunchID = "ordinary-recovery-origin.json", "ordinary-recovery-result.json", "original"
	cmd.Use = "resync"
	require.Error(t, validateOrdinaryRecoveryFlags(cmd))
}

func TestOrdinaryRecoveryColdS3StartupPublishesNoStartReceipt(t *testing.T) {
	if os.Getenv("OLAKE_TEST_COLD_S3_RECOVERY") != "1" {
		child := exec.Command(os.Args[0], "-test.run=^TestOrdinaryRecoveryColdS3StartupPublishesNoStartReceipt$", "-test.count=1")
		child.Env = append(os.Environ(), "OLAKE_TEST_COLD_S3_RECOVERY=1")
		output, err := child.CombinedOutput()
		require.NoError(t, err, "%s", output)
		return
	}
	dir := t.TempDir()
	receipts := make(chan []byte, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/execution-fence.json"):
			fmt.Fprint(w, `{"version":1,"execution_id":"recovery","state":"open"}`)
		case r.Method == http.MethodGet:
			fmt.Fprint(w, `{}`)
		case r.Method == http.MethodPut && r.URL.Path == "/test-bucket/runtime/execution-result.json":
			data, err := io.ReadAll(r.Body)
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			receipts <- data
		default:
			http.Error(w, "unexpected storage mutation", http.StatusBadRequest)
		}
	}))
	defer server.Close()
	t.Setenv(constants.EnvStorageMode, constants.StorageModeS3)
	t.Setenv(constants.EnvS3ConfigFolder, "s3://test-bucket/runtime")
	t.Setenv("TMPDIR", dir)
	t.Setenv(constants.EnvS3Endpoint, server.URL)
	t.Setenv(constants.EnvS3Region, "us-east-2")
	t.Setenv(constants.EnvS3AccessKeyID, "test")
	t.Setenv(constants.EnvS3SecretAccessKey, "test")
	ordinaryRecoveryOriginPath, ordinaryRecoveryResultPath = filepath.Join(dir, "ordinary-recovery-origin.json"), filepath.Join(dir, "ordinary-recovery-result.json")
	executionID, executionLaunchID, executionFence = "recovery", "original", "s3://test-bucket/runtime/execution-fence.json"
	configPath, destinationConfigPath, statePath = filepath.Join(dir, "source.json"), filepath.Join(dir, "destination.json"), filepath.Join(dir, "state.json")
	noSave, discoverSchema, destinationDatabasePrefix = false, false, ""
	stopped := errors.New("stop before source and destination setup")
	root := &cobra.Command{Use: "olake", PersistentPreRunE: RootCmd.PersistentPreRunE, SilenceErrors: true, SilenceUsage: true}
	root.AddCommand(&cobra.Command{Use: "sync", RunE: func(*cobra.Command, []string) error { return stopped }})
	root.SetArgs([]string{"sync"})
	require.ErrorIs(t, Execute(root), stopped)
	var data []byte
	select {
	case data = <-receipts:
	default:
		t.Fatal("cold S3 startup did not publish its execution receipt")
	}
	var receipt utils.ExecutionResult
	require.NoError(t, json.Unmarshal(data, &receipt))
	require.Equal(t, "not_started", receipt.Outcome)
	require.Equal(t, "recovery", receipt.ExecutionID)
	require.Equal(t, "original", receipt.LaunchID)
	require.Empty(t, receipt.StateSHA256)
}

func TestOrdinaryRecoveryCheckpointCLILocalizesAndRejectsUnboundInputs(t *testing.T) {
	scenario := os.Getenv("OLAKE_TEST_CHECKPOINT_SCENARIO")
	if scenario == "" {
		for _, mode := range []string{"uri", "config-folder"} {
			for _, input := range []string{"valid", "missing-flag", "missing-object", "wrong-hash", "other-job", "non-backfill"} {
				t.Run(mode+"/"+input, func(t *testing.T) {
					child := exec.Command(os.Args[0], "-test.run=^TestOrdinaryRecoveryCheckpointCLILocalizesAndRejectsUnboundInputs$", "-test.count=1")
					child.Env = append(os.Environ(), "OLAKE_TEST_CHECKPOINT_SCENARIO="+mode+"/"+input)
					output, err := child.CombinedOutput()
					require.NoError(t, err, "%s", output)
				})
			}
		}
		return
	}
	mode, input, _ := strings.Cut(scenario, "/")
	dir := t.TempDir()
	t.Setenv("TMPDIR", dir)
	checkpoint, _ := backfillRecoveryFixture(t)
	baseline, err := checkpoint.MarshalJSON()
	require.NoError(t, err)
	origin := recoveryTestOrigin()
	origin.Backfill, origin.SavedStateSHA256 = true, strings.Repeat("d", 64)
	origin.BaselineStateSHA256, err = utils.CanonicalStateSHA256(baseline)
	require.NoError(t, err)
	candidate := types.BackfillRecoveryCheckpoint{Version: 1, ExecutionID: origin.OriginalExecutionID, LaunchID: origin.OriginalLaunchID, SourceSHA256: origin.SourceSHA256, DestinationSHA256: origin.DestinationSHA256, CatalogSHA256: origin.CatalogSHA256, StateSHA256: origin.BaselineStateSHA256, State: baseline, ServerIdentity: "synthetic-system", SlotName: "synthetic-slot"}
	if input == "other-job" {
		candidate.ExecutionID = "other-job-runtime"
	}
	candidateBytes, err := json.Marshal(candidate)
	require.NoError(t, err)
	sum := sha256.Sum256(candidateBytes)
	origin.BackfillCheckpointSHA256 = hex.EncodeToString(sum[:])
	if input == "wrong-hash" {
		candidateBytes = append(candidateBytes, '\n')
	}
	if input == "missing-flag" {
		require.NoError(t, os.WriteFile(filepath.Join(dir, backfillCheckpointName), candidateBytes, 0600))
	}
	if input == "non-backfill" {
		origin.Backfill, origin.SavedStateSHA256, origin.BackfillCheckpointSHA256 = false, "", ""
	}
	originBytes, err := json.Marshal(origin)
	require.NoError(t, err)
	objects := map[string][]byte{
		"ordinary-recovery-origin.json": originBytes,
		backfillCheckpointName:          candidateBytes,
		"state.json":                    baseline,
		"source.json":                   []byte(`{}`),
		"destination.json":              []byte(`{}`),
		"execution-fence.json":          []byte(`{"version":1,"execution_id":"recovery","state":"open"}`),
	}
	if input == "missing-object" {
		delete(objects, backfillCheckpointName)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || !strings.HasPrefix(r.URL.Path, "/synthetic-bucket/runtime/") {
			http.Error(w, "unexpected storage operation", http.StatusBadRequest)
			return
		}
		if data, ok := objects[strings.TrimPrefix(r.URL.Path, "/synthetic-bucket/runtime/")]; ok {
			_, _ = w.Write(data)
			return
		}
		http.NotFound(w, r)
	}))
	defer server.Close()
	t.Setenv(constants.EnvStorageMode, constants.StorageModeS3)
	t.Setenv(constants.EnvS3ConfigFolder, "")
	t.Setenv(constants.EnvS3Endpoint, server.URL)
	t.Setenv(constants.EnvS3Region, "us-east-2")
	t.Setenv(constants.EnvS3AccessKeyID, "synthetic")
	t.Setenv(constants.EnvS3SecretAccessKey, "synthetic")
	prefix := "s3://synthetic-bucket/runtime/"
	path := func(name string) string { return prefix + name }
	if mode == "config-folder" {
		t.Setenv(constants.EnvS3ConfigFolder, prefix)
		path = func(name string) string { return filepath.Join("/mnt/config", name) }
	}
	stopped := errors.New("stop before any source or destination work")
	launched := false
	root := &cobra.Command{Use: "olake", PersistentPreRunE: RootCmd.PersistentPreRunE, SilenceErrors: true, SilenceUsage: true}
	root.PersistentFlags().AddFlagSet(RootCmd.PersistentFlags())
	sync := &cobra.Command{Use: "sync", PreRunE: func(*cobra.Command, []string) error {
		data, err := os.ReadFile(ordinaryRecoveryOriginPath)
		if err != nil {
			return err
		}
		var localizedOrigin types.OrdinaryRecoveryOrigin
		if err := strictRecoveryJSON(data, &localizedOrigin); err != nil {
			return err
		}
		if err := validateOrdinaryRecoveryOrigin(localizedOrigin); err != nil {
			return err
		}
		_, err = loadBackfillRecoveryCheckpoint(backfillRecoveryCheckpointPath, localizedOrigin, baseline)
		return err
	}, RunE: func(*cobra.Command, []string) error {
		launched = true
		return stopped
	}}
	sync.Flags().AddFlagSet(syncCmd.Flags())
	root.AddCommand(sync)
	args := []string{"sync", "--config", path("source.json"), "--destination", path("destination.json"), "--state", path("state.json"), "--execution-id", "recovery", "--execution-launch-id", "original", "--execution-fence", prefix + "execution-fence.json", "--ordinary-recovery-origin", path("ordinary-recovery-origin.json"), "--ordinary-recovery-result", path("ordinary-recovery-result.json")}
	if input != "missing-flag" {
		args = append(args, "--backfill-recovery-checkpoint", path(backfillCheckpointName))
	}
	root.SetArgs(args)
	err = root.Execute()
	if input == "valid" {
		require.ErrorIs(t, err, stopped)
		require.True(t, launched)
		require.Equal(t, filepath.Join(dir, backfillCheckpointName), backfillRecoveryCheckpointPath)
		localized, err := os.ReadFile(backfillRecoveryCheckpointPath)
		require.NoError(t, err)
		require.Equal(t, candidateBytes, localized)
	} else {
		require.Error(t, err)
		require.NotErrorIs(t, err, stopped)
		require.False(t, launched, "invalid recovery inputs must fail before launching data work")
		for _, name := range []string{"ordinary-recovery-result.json", "execution-result.json"} {
			_, err := os.Stat(filepath.Join(dir, name))
			require.True(t, os.IsNotExist(err), "invalid recovery must not publish settlement evidence")
		}
	}
}

func TestOrdinaryRecoveryServerPinSurvivesInterruptedEpochAndRejectsDrift(t *testing.T) {
	t.Setenv(constants.EnvS3ConfigFolder, "")
	oldOrigin := ordinaryRecoveryOriginPath
	t.Cleanup(func() { ordinaryRecoveryOriginPath = oldOrigin })
	ordinaryRecoveryOriginPath = filepath.Join(t.TempDir(), "ordinary-recovery-origin.json")
	origin := recoveryTestOrigin()
	pinPath := filepath.Join(filepath.Dir(ordinaryRecoveryOriginPath), "ordinary-recovery-server.json")
	require.NoError(t, checkOrdinaryRecoveryServer(context.Background(), origin, "system-one", "slot", false))
	_, err := os.Stat(pinPath)
	require.True(t, os.IsNotExist(err), "preflight must not publish the first durable identity binding")
	require.NoError(t, checkOrdinaryRecoveryServer(context.Background(), origin, "system-one", "slot", true))
	stored, err := os.ReadFile(pinPath)
	require.NoError(t, err)
	for _, publish := range []bool{false, true} {
		require.NoError(t, checkOrdinaryRecoveryServer(context.Background(), origin, "system-one", "slot", publish), "same immutable pin reused in successor")
		require.Error(t, checkOrdinaryRecoveryServer(context.Background(), origin, "system-two", "slot", publish))
		require.Error(t, checkOrdinaryRecoveryServer(context.Background(), origin, "system-one", "other-slot", publish))
		changed := origin
		changed.SourceSHA256 = strings.Repeat("b", 64)
		require.Error(t, checkOrdinaryRecoveryServer(context.Background(), changed, "system-one", "slot", publish))
		changed = origin
		changed.OriginalLaunchID = "foreign-launch"
		require.Error(t, checkOrdinaryRecoveryServer(context.Background(), changed, "system-one", "slot", publish))
	}
	unchanged, err := os.ReadFile(pinPath)
	require.NoError(t, err)
	require.Equal(t, stored, unchanged)
	require.NoError(t, os.WriteFile(pinPath, []byte(`{"version":1,"unknown":true}`), 0600))
	require.Error(t, checkOrdinaryRecoveryServer(context.Background(), origin, "system-one", "slot", false))
	require.NoError(t, os.Remove(pinPath))
	require.NoError(t, os.Mkdir(pinPath, 0700))
	require.Error(t, checkOrdinaryRecoveryServer(context.Background(), origin, "system-one", "slot", false), "unreadable is not absent")
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
	result.OriginalReceiptSHA256 = ""
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
	require.Equal(t, result.OrdinaryRecoveryOrigin, observed.OrdinaryRecoveryOrigin)
	original, err := os.ReadFile(originalReceipt)
	require.NoError(t, err)
	require.Equal(t, "immutable original receipt", string(original))
}

func TestOrdinaryRecoveryOriginRequiresCompleteImmutableBindings(t *testing.T) {
	origin := recoveryTestOrigin()
	require.NoError(t, validateOrdinaryRecoveryOrigin(origin))
	origin.OriginalReceiptSHA256 = ""
	require.NoError(t, validateOrdinaryRecoveryOrigin(origin), "a missing original receipt is not a fabricated digest")
	for _, mutate := range []func(*types.OrdinaryRecoveryOrigin){
		func(o *types.OrdinaryRecoveryOrigin) { o.Version = 2 },
		func(o *types.OrdinaryRecoveryOrigin) { o.OriginalExecutionID = "" },
		func(o *types.OrdinaryRecoveryOrigin) { o.OriginalLaunchID = " " },
		func(o *types.OrdinaryRecoveryOrigin) { o.OriginalReceiptSHA256 = " " },
		func(o *types.OrdinaryRecoveryOrigin) { o.OriginalReceiptSHA256 = strings.Repeat("A", 64) },
		func(o *types.OrdinaryRecoveryOrigin) { o.SourceSHA256 = "" },
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
