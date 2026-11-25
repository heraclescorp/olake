package protocol

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aws/smithy-go"
	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/destination"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
	"github.com/datazip-inc/olake/utils/logger"
	"github.com/google/uuid"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var (
	ordinaryRecoveryOriginPath     string
	ordinaryRecoveryResultPath     string
	ordinaryRecoveryResultURI      string
	backfillRecoveryCheckpointPath string
	recoveryCheckpointInterval     string
)

func init() {
	syncCmd.Flags().StringVar(&ordinaryRecoveryOriginPath, "ordinary-recovery-origin", "", "Immutable original controlled-sync artifact bindings; requires --ordinary-recovery-result")
	syncCmd.Flags().StringVar(&ordinaryRecoveryResultPath, "ordinary-recovery-result", "", "Separate proof-backed bounded recovery result; never replaces the original execution receipt")
	syncCmd.Flags().StringVar(&backfillRecoveryCheckpointPath, "backfill-recovery-checkpoint", "", "Frozen durable candidate input; required only for ordinary recovery of a backfill origin")
	syncCmd.Flags().StringVar(&recoveryCheckpointInterval, "recovery-checkpoint-interval", "5m", "Positive soft transaction-boundary interval for retained recovery checkpoints (OLAKE_RECOVERY_CHECKPOINT_INTERVAL)")
}

func validateOrdinaryRecoveryFlags(cmd *cobra.Command) error {
	if _, err := ordinaryRecoveryInterval(cmd); err != nil {
		return err
	}
	origin, result := strings.TrimSpace(ordinaryRecoveryOriginPath), strings.TrimSpace(ordinaryRecoveryResultPath)
	if origin == "" && result == "" {
		if strings.TrimSpace(backfillRecoveryCheckpointPath) != "" {
			return fmt.Errorf("--backfill-recovery-checkpoint is only valid for ordinary recovery")
		}
		if executionLaunchID != "" {
			return fmt.Errorf("--execution-launch-id is only valid for controlled ordinary recovery")
		}
		return nil
	}
	if cmd.Name() != "sync" || origin == "" || result == "" {
		return fmt.Errorf("ordinary recovery requires sync with both --ordinary-recovery-origin and --ordinary-recovery-result")
	}
	if strings.TrimSpace(executionID) == "" || strings.TrimSpace(executionFence) == "" || strings.TrimSpace(executionLaunchID) == "" {
		return fmt.Errorf("ordinary recovery requires --execution-id, --execution-launch-id and --execution-fence")
	}
	if noSave || statePath == "" || discoverSchema || destinationDatabasePrefix != "" {
		return fmt.Errorf("ordinary recovery requires saved state and the unchanged original catalog/destination; --no-save, --discover-schema and --destination-database-prefix are unsupported")
	}
	if filepath.Base(origin) != "ordinary-recovery-origin.json" || filepath.Base(result) != "ordinary-recovery-result.json" {
		return fmt.Errorf("ordinary recovery artifacts must be named ordinary-recovery-origin.json and ordinary-recovery-result.json")
	}
	return nil
}

func strictRecoveryJSON(data []byte, out any) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(out); err != nil {
		return err
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return fmt.Errorf("recovery artifact contains trailing JSON")
	}
	return nil
}

func validateOrdinaryRecoveryOrigin(origin types.OrdinaryRecoveryOrigin) error {
	if origin.Version != 1 || strings.TrimSpace(origin.OriginalExecutionID) == "" || strings.TrimSpace(origin.OriginalLaunchID) == "" {
		return fmt.Errorf("ordinary recovery origin has an invalid version or original identity")
	}
	if origin.Backfill != (origin.BackfillCheckpointSHA256 != "" && origin.SavedStateSHA256 != "") || (!origin.Backfill && (origin.BackfillCheckpointSHA256 != "" || origin.SavedStateSHA256 != "")) {
		return fmt.Errorf("ordinary recovery backfill requires candidate and saved-state digest bindings")
	}
	for name, digest := range map[string]string{
		"original_receipt_sha256": origin.OriginalReceiptSHA256,
		"source_sha256":           origin.SourceSHA256, "destination_sha256": origin.DestinationSHA256,
		"catalog_sha256": origin.CatalogSHA256, "baseline_state_sha256": origin.BaselineStateSHA256,
		"backfill_checkpoint_sha256": origin.BackfillCheckpointSHA256, "saved_state_sha256": origin.SavedStateSHA256,
	} {
		if !origin.Backfill && (name == "backfill_checkpoint_sha256" || name == "saved_state_sha256") {
			continue
		}
		if name == "original_receipt_sha256" && digest == "" {
			continue
		}
		decoded, err := hex.DecodeString(digest)
		if err != nil || len(decoded) != sha256.Size || digest != strings.ToLower(digest) {
			return fmt.Errorf("ordinary recovery origin has invalid %s", name)
		}
	}
	return nil
}

func recoveryFileSHA256(path string) (string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:]), nil
}

// Catalog framing is shared with the worker: logical filename, NUL, decimal
// exact byte length, NUL, bytes. Split and legacy catalogs are never mixed.
func ordinaryRecoveryCatalogSHA256(legacy, available, selected string) (string, error) {
	if (legacy == "") == (available == "" && selected == "") || (available == "") != (selected == "") {
		return "", fmt.Errorf("ordinary recovery requires exactly one complete legacy or split catalog")
	}
	hash := sha256.New()
	for _, file := range []struct{ name, path string }{
		{"streams.json", legacy}, {"available_streams.json", available}, {"selected_streams.json", selected},
	} {
		if file.path == "" {
			continue
		}
		data, err := os.ReadFile(file.path)
		if err != nil {
			return "", err
		}
		io.WriteString(hash, file.name)
		hash.Write([]byte{0})
		io.WriteString(hash, strconv.Itoa(len(data)))
		hash.Write([]byte{0})
		hash.Write(data)
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}

func verifyOrdinaryRecoveryConfig(origin types.OrdinaryRecoveryOrigin) error {
	if ordinaryRecoveryOriginPath != "" {
		data, err := os.ReadFile(ordinaryRecoveryOriginPath)
		if err != nil {
			return err
		}
		var stored types.OrdinaryRecoveryOrigin
		if err := strictRecoveryJSON(data, &stored); err != nil {
			return err
		}
		if stored != origin {
			return fmt.Errorf("ordinary recovery immutable origin or original receipt binding changed")
		}
	}
	for _, file := range []struct{ name, path, expected string }{
		{"source", configPath, origin.SourceSHA256}, {"destination", destinationConfigPath, origin.DestinationSHA256},
	} {
		digest, err := recoveryFileSHA256(file.path)
		if err != nil {
			return err
		}
		if digest != file.expected {
			return fmt.Errorf("ordinary recovery %s configuration differs from original frozen bytes", file.name)
		}
	}
	digest, err := ordinaryRecoveryCatalogSHA256(streamsPath, availableStreamsPath, selectedStreamsPath)
	if err != nil {
		return err
	}
	if digest != origin.CatalogSHA256 {
		return fmt.Errorf("ordinary recovery catalog differs from original frozen bytes")
	}
	return nil
}

func prepareOrdinaryRecovery(cmd *cobra.Command) error {
	if ordinaryRecoveryOriginPath == "" {
		return nil
	}
	if connector.Type() != string(constants.Postgres) || destinationConfig.Type != types.Iceberg {
		return fmt.Errorf("ordinary recovery supports only PostgreSQL to native REST/S3 Tables Iceberg")
	}
	data, err := os.ReadFile(ordinaryRecoveryOriginPath)
	if err != nil {
		return err
	}
	var origin types.OrdinaryRecoveryOrigin
	if err := strictRecoveryJSON(data, &origin); err != nil {
		return fmt.Errorf("invalid ordinary recovery origin: %w", err)
	}
	if err := validateOrdinaryRecoveryOrigin(origin); err != nil {
		return err
	}
	if executionLaunchID != origin.OriginalLaunchID || executionID == origin.OriginalExecutionID {
		return fmt.Errorf("ordinary recovery must retain the original launch in a fresh runtime, never reopen the original runtime")
	}
	if err := verifyOrdinaryRecoveryConfig(origin); err != nil {
		return err
	}
	baseline, err := os.ReadFile(statePath)
	if err != nil {
		return err
	}
	digest, err := utils.CanonicalStateSHA256(baseline)
	if err != nil || digest != origin.BaselineStateSHA256 {
		return fmt.Errorf("ordinary recovery baseline does not match original checkpoint: %w", errors.Join(err, errors.New("baseline digest mismatch")))
	}
	// Decode the verified bytes again so a file change between the initial
	// pre-run load and binding verification cannot leave a different checkpoint.
	state = &types.State{RWMutex: &sync.RWMutex{}}
	if err := decodeOrdinaryRecoveryState(baseline, state); err != nil {
		return err
	}
	interval, err := ordinaryRecoveryInterval(cmd)
	if err != nil {
		return err
	}
	options := &types.OrdinaryRecoveryOptions{Origin: origin, CheckpointInterval: interval}
	options.BackfillCheckpoint, err = loadBackfillRecoveryCheckpoint(backfillRecoveryCheckpointPath, origin, baseline)
	if err != nil {
		return err
	}
	if origin.Backfill {
		options.BackfillState, err = state.Clone()
		if err != nil {
			return err
		}
		binding := options.BackfillCheckpoint.SourceBinding()
		state.BackfillSource = &binding
		state.BackfillCheckpoint = true
	}
	state.SuppressPersistence = true
	ordinaryRecoveryResultPath, ordinaryRecoveryResultURI, err = utils.ResolveArtifactOutput(ordinaryRecoveryResultPath, "ordinary-recovery-result.json", configPath)
	if err != nil {
		return err
	}
	for _, input := range []string{configPath, destinationConfigPath, statePath, streamsPath, availableStreamsPath, selectedStreamsPath, ordinaryRecoveryOriginPath, backfillRecoveryCheckpointPath} {
		if input != "" && filepath.Clean(input) == filepath.Clean(ordinaryRecoveryResultPath) {
			return fmt.Errorf("ordinary recovery result must not replace an input artifact")
		}
	}
	options.Checkpoint, options.CheckpointSHA256, err = loadOrdinaryRecoveryCheckpoint(cmd.Context(), origin)
	if err != nil {
		return err
	}
	if options.Checkpoint != nil {
		options.CheckpointLSN = options.Checkpoint.TargetLSN
		options.Plan = &types.OrdinaryRecoveryPlan{LowerLSN: options.Checkpoint.LowerLSN, TargetLSN: options.Checkpoint.FinalTargetLSN}
		if origin.Backfill {
			options.BackfillTargetLSN = options.Checkpoint.FinalTargetLSN
		}
	}
	ctx := types.WithOrdinaryRecovery(cmd.Context(), options)
	if origin.Backfill {
		ctx = types.WithBackfillBindingJournal(ctx, newBackfillBindingJournal(ctx, options.BackfillCheckpoint))
	}
	cmd.SetContext(ctx)
	return nil
}

func loadOrdinaryRecoveryState(path string, checkpoint *types.State) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	return decodeOrdinaryRecoveryState(data, checkpoint)
}

func decodeOrdinaryRecoveryState(data []byte, checkpoint *types.State) error {
	if _, err := utils.CanonicalStateSHA256(data); err != nil {
		return err
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	return decoder.Decode(checkpoint)
}

func ordinaryRecoverySelected(saved *types.Catalog, checkpoint *types.State) ([]types.StreamInterface, string, error) {
	return ordinaryRecoverySelectedMode(saved, checkpoint, false)
}

func ordinaryRecoverySelectedMode(saved *types.Catalog, checkpoint *types.State, backfill bool) ([]types.StreamInterface, string, error) {
	if checkpoint == nil || checkpoint.Type != types.GlobalType || checkpoint.Global == nil || checkpoint.Global.State == nil || checkpoint.Global.Streams == nil {
		return nil, "", fmt.Errorf("ordinary recovery requires a saved GLOBAL LSN and completed backfill membership")
	}
	var global struct {
		LSN string `json:"lsn"`
	}
	if err := utils.Unmarshal(checkpoint.Global.State, &global); err != nil || global.LSN == "" {
		return nil, "", fmt.Errorf("ordinary recovery requires a saved global PostgreSQL LSN")
	}
	selected := make(map[string]bool)
	if saved.SelectedStreams != nil {
		for namespace, entries := range saved.SelectedStreams {
			for _, entry := range entries {
				id := namespace + "." + entry.StreamName
				if selected[id] {
					return nil, "", fmt.Errorf("ordinary recovery selected stream %s is duplicated", id)
				}
				selected[id] = true
			}
		}
	}
	classification, err := classifyStreams(saved, nil, checkpoint, types.Iceberg)
	if err != nil {
		return nil, "", err
	}
	seen := make(map[string]bool)
	for _, stream := range classification.SelectedStreams {
		if seen[stream.ID()] || (stream.GetSyncMode() != types.CDC && stream.GetSyncMode() != types.STRICTCDC) || (!backfill && !checkpoint.HasCompletedBackfill(stream.Self())) || (backfill && stream.GetSyncMode() != types.CDC) || stream.GetStream().SourceDefinedPrimaryKey == nil || stream.GetStream().SourceDefinedPrimaryKey.Len() == 0 {
			return nil, "", fmt.Errorf("ordinary recovery stream %s requires unique saved CDC/strictCDC selection, completed backfill and primary keys", stream.ID())
		}
		seen[stream.ID()] = true
	}
	if saved.SelectedStreams != nil {
		if len(seen) != len(selected) {
			return nil, "", fmt.Errorf("ordinary recovery cannot omit invalid or missing frozen selected streams")
		}
		for id := range selected {
			if !seen[id] {
				return nil, "", fmt.Errorf("ordinary recovery selected stream %s is unavailable", id)
			}
		}
	} else if len(seen) != len(saved.Streams) {
		return nil, "", fmt.Errorf("ordinary recovery cannot omit invalid original catalog streams")
	}
	sort.Slice(classification.SelectedStreams, func(i, j int) bool {
		return classification.SelectedStreams[i].ID() < classification.SelectedStreams[j].ID()
	})
	if backfill {
		data, err := checkpoint.MarshalJSON()
		if err != nil {
			return nil, "", err
		}
		if err := validateBackfillRecoveryState(data, classification.SelectedStreams); err != nil {
			return nil, "", err
		}
	}
	return classification.SelectedStreams, global.LSN, nil
}

type ordinaryRecoveryServer struct {
	Version          int    `json:"version"`
	OriginalLaunchID string `json:"original_launch_id"`
	SourceSHA256     string `json:"source_sha256"`
	ServerIdentity   string `json:"server_identity"`
	SlotName         string `json:"slot_name"`
}

func checkOrdinaryRecoveryServer(ctx context.Context, origin types.OrdinaryRecoveryOrigin, identity, slot string, publish bool) error {
	if strings.TrimSpace(identity) == "" || strings.TrimSpace(slot) == "" {
		return fmt.Errorf("ordinary recovery source identity or replication slot is missing")
	}
	pin := ordinaryRecoveryServer{1, origin.OriginalLaunchID, origin.SourceSHA256, identity, slot}
	local, uri, err := utils.ResolveArtifactOutput(filepath.Join(filepath.Dir(ordinaryRecoveryOriginPath), "ordinary-recovery-server.json"), "ordinary-recovery-server.json", configPath)
	if err != nil {
		return err
	}
	var publishErr error
	if publish {
		data, err := json.Marshal(pin)
		if err != nil {
			return err
		}
		publishErr = utils.PublishImmutableArtifact(ctx, local, uri, data)
	}
	stored, err := utils.ReadImmutableArtifact(ctx, local, uri)
	if err != nil {
		var apiError smithy.APIError
		missing := os.IsNotExist(err) || (errors.As(err, &apiError) && (apiError.ErrorCode() == "NoSuchKey" || apiError.ErrorCode() == "NotFound"))
		if !publish && missing {
			return nil
		}
		return errors.Join(publishErr, err)
	}
	var observed ordinaryRecoveryServer
	if err := strictRecoveryJSON(stored, &observed); err != nil {
		return err
	}
	if observed != pin {
		return fmt.Errorf("ordinary recovery source identity changed from its immutable first-recovery pin")
	}
	return nil
}

func runOrdinaryRecovery(cmd *cobra.Command) (err error) {
	ctx := cmd.Context()
	options, _ := types.OrdinaryRecoveryFromContext(ctx)
	defer func() {
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
		defer cancel()
		cleanupErr := connector.CleanupOrdinaryRecovery(cleanupCtx)
		utils.SourceDrained(ctx, cleanupErr)
		err = errors.Join(err, cleanupErr)
	}()
	selected, lower, err := ordinaryRecoverySelectedMode(catalog, state, options.Origin.Backfill)
	if err != nil {
		return err
	}
	if options.Checkpoint != nil {
		if err := validateOrdinaryRecoveryCheckpoint(*options.Checkpoint, options.Origin, selected, lower); err != nil {
			return err
		}
		if err := validateOrdinaryRecoveryCheckpointBaseline(*options.Checkpoint, state, selected); err != nil {
			return err
		}
		options.BackfillComplete = options.Origin.Backfill
	}
	connector.SetupState(state)
	if err := connector.Setup(ctx); err != nil {
		return err
	}
	if err := destination.ValidateCopyOnWrite(destinationConfig, selected); err != nil {
		return err
	}
	identity, err := connector.ValidateOrdinaryRecovery(ctx, selected, lower)
	if err != nil {
		return err
	}
	var source struct {
		UpdateMethod struct {
			ReplicationSlot  string `json:"replication_slot"`
			ToastValueSource string `json:"toast_value_source"`
		} `json:"update_method"`
	}
	if err := utils.UnmarshalFile(configPath, &source, true); err != nil {
		return err
	}
	if options.Origin.Backfill && (identity != options.BackfillCheckpoint.ServerIdentity || source.UpdateMethod.ReplicationSlot != options.BackfillCheckpoint.SlotName) {
		return fmt.Errorf("backfill recovery live source identity or slot differs from the durable candidate")
	}
	semantics := "current_source_lookup"
	if strings.ToLower(strings.TrimSpace(source.UpdateMethod.ToastValueSource)) == "destination" {
		semantics = "destination"
	}
	if saved := options.Checkpoint; saved != nil && (saved.ServerIdentity != identity || saved.SlotName != source.UpdateMethod.ReplicationSlot || saved.ToastValueSemantics != semantics) {
		return fmt.Errorf("ordinary recovery checkpoint source, slot or TOAST semantics changed")
	}
	if err := checkOrdinaryRecoveryServer(ctx, options.Origin, identity, source.UpdateMethod.ReplicationSlot, false); err != nil {
		return err
	}
	options.SourceIdentity = identity
	for {
		completed, result, final, batchErr := runOrdinaryRecoveryBatch(ctx, selected, lower, identity, source.UpdateMethod.ReplicationSlot, semantics, options)
		if batchErr != nil {
			return batchErr
		}
		if final {
			return writeOrdinaryRecoveryArtifacts(ctx, completed, result)
		}
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
		cleanupErr := connector.CleanupOrdinaryRecovery(cleanupCtx)
		cancel()
		if cleanupErr != nil {
			return cleanupErr
		}
	}
}

func runOrdinaryRecoveryBatch(ctx context.Context, selected []types.StreamInterface, lower, identity, slot, semantics string, options *types.OrdinaryRecoveryOptions) (*types.State, types.OrdinaryRecoveryResult, bool, error) {
	var empty types.OrdinaryRecoveryResult
	pool, err := destination.NewWriterPool(ctx, destinationConfig, selected, batchSize)
	if err != nil {
		return nil, empty, false, err
	}
	defer pool.Shutdown(context.WithoutCancel(ctx))
	if options.Origin.Backfill && !options.BackfillComplete {
		if err := pool.PrepareBackfillRecovery(ctx, selected, lower); err != nil {
			return nil, empty, false, err
		}
		if err := connector.ResumeOrdinaryBackfill(ctx, pool, selected); err != nil {
			return nil, empty, false, err
		}
		options.BackfillComplete = true
		options.BackfillTargetLSN, err = pool.BackfillRecoveryTarget(ctx)
		if err != nil {
			return nil, empty, false, err
		}
		if options.BackfillTargetLSN == "" {
			options.BackfillTargetLSN, err = connector.CaptureBackfillRecoveryTarget(ctx, lower)
			if err != nil {
				return nil, empty, false, err
			}
		}
	}
	proofs, err := pool.PrepareOrdinaryRecovery(ctx, selected, lower)
	if err != nil {
		return nil, empty, false, err
	}
	if saved := options.Checkpoint; saved != nil {
		if err := validateOrdinaryRecoveryResumeProofs(*saved, proofs); err != nil {
			return nil, empty, false, err
		}
		resumed := &types.State{RWMutex: &sync.RWMutex{}, SuppressPersistence: true}
		if err := decodeOrdinaryRecoveryState(saved.State, resumed); err != nil {
			return nil, empty, false, err
		}
		state = resumed
		connector.SetupState(state)
	}
	if err := checkOrdinaryRecoveryServer(ctx, options.Origin, identity, slot, true); err != nil {
		return nil, empty, false, err
	}
	positions := make(map[string]string, len(proofs))
	for _, proof := range proofs {
		positions[proof.Stream] = proof.CommittedLSN
	}
	plan, err := connector.PrepareOrdinaryRecovery(ctx, selected, lower, positions)
	if err != nil {
		return nil, empty, false, err
	}
	if plan.LowerLSN != lower || (options.Plan != nil && plan.TargetLSN != options.Plan.TargetLSN) {
		return nil, empty, false, fmt.Errorf("ordinary recovery immutable range changed between batches")
	}
	options.Plan = plan
	if err := connector.Read(ctx, pool, nil, selected, nil); err != nil {
		return nil, empty, false, err
	}
	boundary, err := ordinaryRecoveryLSN(plan.BatchTargetLSN)
	finalTarget, finalErr := ordinaryRecoveryLSN(plan.TargetLSN)
	previous := lower
	if options.CheckpointLSN != "" {
		previous = options.CheckpointLSN
	}
	previousLSN, previousErr := ordinaryRecoveryLSN(previous)
	if err != nil || finalErr != nil || previousErr != nil || boundary < previousLSN || boundary > finalTarget || (boundary == previousLSN && boundary != finalTarget) {
		return nil, empty, false, fmt.Errorf("ordinary recovery did not complete a valid advancing batch")
	}
	proofs, err = pool.ConfirmOrdinaryRecovery(ctx, selected, proofs, plan.BatchTargetLSN)
	if err != nil {
		return nil, empty, false, err
	}
	if err := verifyOrdinaryRecoveryConfig(options.Origin); err != nil {
		return nil, empty, false, err
	}
	if options.Origin.Backfill {
		if _, err := loadBackfillRecoveryCheckpoint(backfillRecoveryCheckpointPath, options.Origin, options.BackfillCheckpoint.State); err != nil {
			return nil, empty, false, err
		}
	}
	completed, err := ordinaryRecoveryCompletedState(state, plan.BatchTargetLSN)
	if err != nil {
		return nil, empty, false, err
	}
	result := types.OrdinaryRecoveryResult{OrdinaryRecoveryOrigin: options.Origin, ExecutionID: executionID, LaunchID: executionLaunchID, LowerLSN: lower, TargetLSN: plan.BatchTargetLSN, ServerIdentity: identity, SlotName: slot, ToastValueSemantics: semantics, Tables: proofs}
	previousExecution := ""
	if options.Checkpoint != nil {
		previousExecution = options.Checkpoint.ExecutionID
	}
	manifest, digest, err := persistOrdinaryRecoveryBatch(ctx, completed, result, plan.TargetLSN, options.CheckpointSHA256, previousExecution, func() error {
		return connector.AcknowledgeOrdinaryRecovery(ctx, plan)
	})
	if err != nil {
		return nil, empty, false, err
	}
	state = completed
	connector.SetupState(state)
	options.Checkpoint = manifest
	options.CheckpointSHA256 = digest
	options.CheckpointLSN = plan.BatchTargetLSN
	if !plan.BatchSourceTime.IsZero() {
		logger.Infof("ordinary recovery completed batch checkpoint_lsn=%s source_timestamp=%s final_target_lsn=%s", plan.BatchTargetLSN, plan.BatchSourceTime.UTC().Format(time.RFC3339Nano), plan.TargetLSN)
	}
	return completed, result, boundary == finalTarget, nil
}

func writeOrdinaryRecoveryArtifacts(ctx context.Context, checkpoint *types.State, result types.OrdinaryRecoveryResult) error {
	if err := validateOrdinaryRecoveryResult(checkpoint, result); err != nil {
		return err
	}
	data, err := checkpoint.MarshalJSON()
	if err != nil {
		return err
	}
	canonical, err := canonicalResyncState(data)
	if err != nil {
		return err
	}
	result.StateSHA256, err = utils.CanonicalStateSHA256(canonical)
	if err != nil {
		return err
	}
	if err := writeResyncArtifact(viper.GetString(constants.StatePath), canonical); err != nil {
		return err
	}
	encoded, err := json.Marshal(result)
	if err != nil {
		return err
	}
	if err := utils.PublishImmutableArtifact(ctx, ordinaryRecoveryResultPath, ordinaryRecoveryResultURI, encoded); err != nil {
		return err
	}
	observed, err := utils.ReadImmutableArtifact(ctx, ordinaryRecoveryResultPath, ordinaryRecoveryResultURI)
	if err != nil {
		return err
	}
	if !bytes.Equal(observed, encoded) {
		return fmt.Errorf("ordinary recovery result read-after-create mismatch")
	}
	return nil
}

func ordinaryRecoveryLSN(value string) (uint64, error) {
	high, low, ok := strings.Cut(value, "/")
	if !ok || high == "" || low == "" {
		return 0, fmt.Errorf("invalid PostgreSQL LSN %q", value)
	}
	hi, err := strconv.ParseUint(high, 16, 32)
	if err != nil {
		return 0, err
	}
	lo, err := strconv.ParseUint(low, 16, 32)
	if err != nil {
		return 0, err
	}
	return hi<<32 | lo, nil
}

func validateOrdinaryRecoveryResult(checkpoint *types.State, result types.OrdinaryRecoveryResult) error {
	return validateOrdinaryRecoveryProof(checkpoint, result, result.TargetLSN)
}

func validateOrdinaryRecoveryProof(checkpoint *types.State, result types.OrdinaryRecoveryResult, finalTarget string) error {
	if err := validateOrdinaryRecoveryOrigin(result.OrdinaryRecoveryOrigin); err != nil {
		return err
	}
	if strings.TrimSpace(result.ExecutionID) == "" || result.ExecutionID == result.OriginalExecutionID || result.LaunchID != result.OriginalLaunchID || strings.TrimSpace(result.ServerIdentity) == "" || strings.TrimSpace(result.SlotName) == "" || (result.ToastValueSemantics != "current_source_lookup" && result.ToastValueSemantics != "destination") {
		return fmt.Errorf("ordinary recovery result has incomplete runtime or source identity")
	}
	lower, err := ordinaryRecoveryLSN(result.LowerLSN)
	if err != nil {
		return err
	}
	target, err := ordinaryRecoveryLSN(result.TargetLSN)
	if err != nil || target < lower {
		return fmt.Errorf("ordinary recovery result target is invalid or behind baseline")
	}
	upper, err := ordinaryRecoveryLSN(finalTarget)
	if err != nil || upper < target {
		return fmt.Errorf("ordinary recovery checkpoint exceeds its immutable final target")
	}
	if checkpoint == nil || checkpoint.Type != types.GlobalType || checkpoint.Global == nil || checkpoint.Global.State == nil || checkpoint.Global.Streams == nil || len(result.Tables) == 0 {
		return fmt.Errorf("ordinary recovery result requires a full GLOBAL checkpoint and table proofs")
	}
	var global struct {
		LSN string `json:"lsn"`
	}
	if err := utils.Unmarshal(checkpoint.Global.State, &global); err != nil {
		return err
	}
	position, err := ordinaryRecoveryLSN(global.LSN)
	if err != nil || position != target {
		return fmt.Errorf("ordinary recovery state does not prove the target checkpoint")
	}
	seen := make(map[string]bool, len(result.Tables))
	for _, proof := range result.Tables {
		parsedUUID, uuidErr := uuid.Parse(proof.TableUUID)
		position, lsnErr := ordinaryRecoveryLSN(proof.CommittedLSN)
		if proof.Stream == "" || seen[proof.Stream] || uuidErr != nil || parsedUUID == uuid.Nil || proof.FenceSnapshotID <= 0 || lsnErr != nil || position < target || position > upper || !checkpoint.Global.Streams.Exists(proof.Stream) {
			return fmt.Errorf("ordinary recovery table proof %q does not cover the bounded common boundary", proof.Stream)
		}
		if result.Backfill {
			var found bool
			for _, entry := range checkpoint.Streams {
				if entry.Namespace+"."+entry.Stream != proof.Stream {
					continue
				}
				if found {
					return fmt.Errorf("backfill recovery result duplicates stream state")
				}
				found = true
				bindingValue, _ := entry.State.Load(types.BackfillTableKey)
				var binding types.BackfillTableBinding
				if err := utils.Unmarshal(bindingValue, &binding); err != nil || binding.TableUUID != proof.TableUUID {
					return fmt.Errorf("backfill recovery result lost destination identity binding")
				}
				for _, key := range []string{types.ChunksKey, types.ResumePlanKey} {
					if value, exists := entry.State.Load(key); exists {
						chunks, ok := value.(*types.Set[types.Chunk])
						if !ok || chunks.Len() != 0 {
							return fmt.Errorf("backfill recovery result retains unfinished backfill work")
						}
					}
				}
				value, exists := entry.State.Load(types.BackfillPlanKey)
				plan, ok := value.(*types.Set[types.Chunk])
				if !exists || !ok || plan.Len() == 0 {
					return fmt.Errorf("backfill recovery result lost its immutable completed plan")
				}
			}
			if !found {
				return fmt.Errorf("backfill recovery result has no completed table state")
			}
		}
		seen[proof.Stream] = true
	}
	return nil
}
