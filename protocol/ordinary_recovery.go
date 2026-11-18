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

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/destination"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
	"github.com/google/uuid"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var (
	ordinaryRecoveryOriginPath string
	ordinaryRecoveryResultPath string
	ordinaryRecoveryResultURI  string
)

func init() {
	syncCmd.Flags().StringVar(&ordinaryRecoveryOriginPath, "ordinary-recovery-origin", "", "Immutable original controlled-sync artifact bindings; requires --ordinary-recovery-result")
	syncCmd.Flags().StringVar(&ordinaryRecoveryResultPath, "ordinary-recovery-result", "", "Separate proof-backed bounded recovery result; never replaces the original execution receipt")
}

func validateOrdinaryRecoveryFlags(cmd *cobra.Command) error {
	origin, result := strings.TrimSpace(ordinaryRecoveryOriginPath), strings.TrimSpace(ordinaryRecoveryResultPath)
	if origin == "" && result == "" {
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
	for name, digest := range map[string]string{
		"original_receipt_sha256": origin.OriginalReceiptSHA256,
		"source_sha256":           origin.SourceSHA256, "destination_sha256": origin.DestinationSHA256,
		"catalog_sha256": origin.CatalogSHA256, "baseline_state_sha256": origin.BaselineStateSHA256,
	} {
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
	state.SuppressPersistence = true
	ordinaryRecoveryResultPath, ordinaryRecoveryResultURI, err = utils.ResolveArtifactOutput(ordinaryRecoveryResultPath, "ordinary-recovery-result.json", configPath)
	if err != nil {
		return err
	}
	for _, input := range []string{configPath, destinationConfigPath, statePath, streamsPath, availableStreamsPath, selectedStreamsPath, ordinaryRecoveryOriginPath} {
		if input != "" && filepath.Clean(input) == filepath.Clean(ordinaryRecoveryResultPath) {
			return fmt.Errorf("ordinary recovery result must not replace an input artifact")
		}
	}
	cmd.SetContext(types.WithOrdinaryRecovery(cmd.Context(), &types.OrdinaryRecoveryOptions{Origin: origin}))
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
		if seen[stream.ID()] || (stream.GetSyncMode() != types.CDC && stream.GetSyncMode() != types.STRICTCDC) || !checkpoint.HasCompletedBackfill(stream.Self()) || stream.GetStream().SourceDefinedPrimaryKey == nil || stream.GetStream().SourceDefinedPrimaryKey.Len() == 0 {
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
	return classification.SelectedStreams, global.LSN, nil
}

type ordinaryRecoveryServer struct {
	Version          int    `json:"version"`
	OriginalLaunchID string `json:"original_launch_id"`
	SourceSHA256     string `json:"source_sha256"`
	ServerIdentity   string `json:"server_identity"`
	SlotName         string `json:"slot_name"`
}

func pinOrdinaryRecoveryServer(ctx context.Context, origin types.OrdinaryRecoveryOrigin, identity, slot string) error {
	if strings.TrimSpace(identity) == "" || strings.TrimSpace(slot) == "" {
		return fmt.Errorf("ordinary recovery source identity or replication slot is missing")
	}
	pin := ordinaryRecoveryServer{1, origin.OriginalLaunchID, origin.SourceSHA256, identity, slot}
	data, err := json.Marshal(pin)
	if err != nil {
		return err
	}
	local, uri, err := utils.ResolveArtifactOutput(filepath.Join(filepath.Dir(ordinaryRecoveryOriginPath), "ordinary-recovery-server.json"), "ordinary-recovery-server.json", configPath)
	if err != nil {
		return err
	}
	publishErr := utils.PublishImmutableArtifact(ctx, local, uri, data)
	stored, err := utils.ReadImmutableArtifact(ctx, local, uri)
	if err != nil {
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
	// Even pre-read failures need positive source retirement; cleanup below owns
	// the replication connection until after the common-boundary proof and ack.
	defer func() {
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
		defer cancel()
		cleanupErr := connector.CleanupOrdinaryRecovery(cleanupCtx)
		utils.SourceDrained(ctx, cleanupErr)
		err = errors.Join(err, cleanupErr)
	}()
	selected, lower, err := ordinaryRecoverySelected(catalog, state)
	if err != nil {
		return err
	}
	connector.SetupState(state)
	if err := connector.Setup(ctx); err != nil {
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
	// GetConfigRef allocates a fresh source config; calling it after Setup would
	// erase the driver's live connection/slot settings. Decode the frozen file.
	if err := utils.UnmarshalFile(configPath, &source, true); err != nil {
		return err
	}
	if err := pinOrdinaryRecoveryServer(ctx, options.Origin, identity, source.UpdateMethod.ReplicationSlot); err != nil {
		return err
	}
	options.SourceIdentity = identity
	pool, err := destination.NewWriterPool(ctx, destinationConfig, selected, batchSize)
	if err != nil {
		return err
	}
	defer pool.Shutdown(context.WithoutCancel(ctx))
	proofs, err := pool.PrepareOrdinaryRecovery(ctx, selected, lower)
	if err != nil {
		return err
	}
	positions := make(map[string]string, len(proofs))
	for _, proof := range proofs {
		positions[proof.Stream] = proof.CommittedLSN
	}
	plan, err := connector.PrepareOrdinaryRecovery(ctx, selected, lower, positions)
	if err != nil {
		return err
	}
	options.Plan = plan
	if err := connector.Read(ctx, pool, nil, selected, nil); err != nil {
		return err
	}
	proofs, err = pool.ConfirmOrdinaryRecovery(ctx, selected, proofs, plan.TargetLSN)
	if err != nil {
		return err
	}
	if err := verifyOrdinaryRecoveryConfig(options.Origin); err != nil {
		return err
	}
	if err := connector.AcknowledgeOrdinaryRecovery(ctx, plan); err != nil {
		return err
	}
	if err := utils.CheckExecutionFence(ctx); err != nil {
		return err
	}
	semantics := "current_source_lookup"
	if strings.ToLower(strings.TrimSpace(source.UpdateMethod.ToastValueSource)) == "destination" {
		semantics = "destination"
	}
	result := types.OrdinaryRecoveryResult{OrdinaryRecoveryOrigin: options.Origin, ExecutionID: executionID, LaunchID: executionLaunchID, LowerLSN: plan.LowerLSN, TargetLSN: plan.TargetLSN, ServerIdentity: identity, SlotName: source.UpdateMethod.ReplicationSlot, ToastValueSemantics: semantics, Tables: proofs}
	return writeOrdinaryRecoveryArtifacts(ctx, state, result)
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
	if err := validateOrdinaryRecoveryOrigin(result.OrdinaryRecoveryOrigin); err != nil {
		return err
	}
	if result.ExecutionID == "" || result.ExecutionID == result.OriginalExecutionID || result.LaunchID != result.OriginalLaunchID || result.ServerIdentity == "" || result.SlotName == "" || (result.ToastValueSemantics != "current_source_lookup" && result.ToastValueSemantics != "destination") {
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
		if proof.Stream == "" || seen[proof.Stream] || uuidErr != nil || parsedUUID == uuid.Nil || proof.FenceSnapshotID <= 0 || lsnErr != nil || position != target || !checkpoint.Global.Streams.Exists(proof.Stream) {
			return fmt.Errorf("ordinary recovery table proof %q does not prove the exact common boundary", proof.Stream)
		}
		seen[proof.Stream] = true
	}
	return nil
}
