package protocol

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
	"github.com/google/uuid"
	"github.com/spf13/cobra"
)

const backfillCheckpointName = "backfill-recovery-checkpoint.json"

var errBackfillTableUnbound = errors.New("backfill table identity has not been durably bound")

func configureBackfillCheckpoint(cmd *cobra.Command, selected []types.StreamInterface) error {
	ctx := cmd.Context()
	execution, controlled := utils.ExecutionFromContext(ctx)
	if !controlled || connector.Type() != string(constants.Postgres) || destinationConfig.Type != types.Iceberg || noSave {
		return nil
	}
	if _, resync := types.ResyncFromContext(ctx); resync {
		return nil
	}
	if !backfillCheckpointEligible(state, selected) {
		return nil
	}
	if state.Global == nil {
		for _, streamState := range state.Streams {
			if streamState.HoldsValue.Load() {
				return nil
			}
		}
		state.SetType(types.GlobalType)
	}
	if state.Type != types.GlobalType {
		return nil
	}
	if err := utils.CheckExecutionFence(ctx); err != nil {
		return err
	}
	candidate := types.BackfillRecoveryCheckpoint{Version: 1, ExecutionID: execution.ID, LaunchID: execution.LaunchID}
	if candidate.LaunchID == "" {
		candidate.LaunchID = execution.ID
	}
	var err error
	candidate.SourceSHA256, err = recoveryFileSHA256(configPath)
	if err != nil {
		return err
	}
	candidate.DestinationSHA256, err = recoveryFileSHA256(destinationConfigPath)
	if err != nil {
		return err
	}
	candidate.CatalogSHA256, err = ordinaryRecoveryCatalogSHA256(streamsPath, availableStreamsPath, selectedStreamsPath)
	if err != nil {
		return err
	}
	continueScope, err := backfillCheckpointConfigMatches(state, selected, &candidate)
	if err != nil || !continueScope {
		return err
	}
	output := filepath.Join(filepath.Dir(execution.Fence), backfillCheckpointName)
	if strings.HasPrefix(execution.Fence, "s3://") {
		output = execution.Fence[:strings.LastIndex(execution.Fence, "/")+1] + backfillCheckpointName
	}
	local, uri, err := utils.ResolveArtifactOutput(output, backfillCheckpointName, configPath)
	if err != nil {
		return err
	}
	ownership := candidate
	var lower string
	state.BackfillCheckpoint = true
	state.BackfillPersist = func(data []byte) error {
		return persistBackfillCheckpoint(ctx, &candidate, &lower, local, uri, data, selected)
	}
	state.BackfillIdentity = func(identity, slot string) error {
		if strings.TrimSpace(identity) == "" || strings.TrimSpace(slot) == "" {
			return fmt.Errorf("backfill candidate requires live PostgreSQL system identity and replication slot")
		}
		if candidate.ServerIdentity != "" && (candidate.ServerIdentity != identity || candidate.SlotName != slot) {
			return fmt.Errorf("backfill candidate source identity changed")
		}
		if saved := state.BackfillSource; saved != nil && (saved.ServerIdentity != identity || saved.SlotName != slot) {
			return fmt.Errorf("partial backfill retry PostgreSQL identity or slot differs from its original source")
		}
		candidate.ServerIdentity, candidate.SlotName = identity, slot
		if state.BackfillSource == nil {
			binding := candidate.SourceBinding()
			state.BackfillSource = &binding
		}
		if err := state.PersistBackfill(); err != nil {
			return err
		}
		if ownership.ServerIdentity == "" {
			ownership.ServerIdentity, ownership.SlotName = identity, slot
			state.RLock()
			ownership.State, err = state.MarshalJSON()
			state.RUnlock()
			if err != nil {
				return err
			}
			ownership.StateSHA256, err = utils.CanonicalStateSHA256(ownership.State)
			if err != nil {
				return err
			}
		}
		return nil
	}
	ctx = types.WithBackfillCheckpoint(ctx, &ownership)
	cmd.SetContext(types.WithBackfillBindingJournal(ctx, newBackfillBindingJournal(ctx, &ownership)))
	return nil
}

func backfillCheckpointEligible(checkpoint *types.State, selected []types.StreamInterface) bool {
	incomplete := false
	started := false
	selectedIDs := make(map[string]bool, len(selected))
	for _, stream := range selected {
		if stream.GetSyncMode() != types.CDC || stream.GetStream().SourceDefinedPrimaryKey == nil || stream.GetStream().SourceDefinedPrimaryKey.Len() == 0 {
			return false
		}
		selectedIDs[stream.ID()] = true
		completed := checkpoint.HasCompletedBackfill(stream.Self())
		if completed && (checkpoint.GetBackfillTable(stream.Self()) == nil || checkpoint.GetBackfillPlan(stream.Self()) == nil) {
			return false
		}
		if !completed && (checkpoint.GetResumePlan(stream.Self()) != nil || checkpoint.GetChunks(stream.Self()) != nil) && checkpoint.GetBackfillTable(stream.Self()) == nil && checkpoint.BackfillSource == nil {
			return false
		}
		started = started || checkpoint.GetBackfillTable(stream.Self()) != nil || checkpoint.GetResumePlan(stream.Self()) != nil || checkpoint.GetChunks(stream.Self()) != nil
		incomplete = incomplete || !completed
	}
	if global := checkpoint.GetGlobal(); global != nil && global.Streams != nil {
		for _, id := range global.Streams.Array() {
			if !selectedIDs[id] {
				return false
			}
		}
	}
	return incomplete && (!started || checkpoint.BackfillSource != nil)
}

func backfillCheckpointConfigMatches(checkpoint *types.State, selected []types.StreamInterface, candidate *types.BackfillRecoveryCheckpoint) (bool, error) {
	saved := checkpoint.BackfillSource
	if saved == nil || (saved.SourceSHA256 == candidate.SourceSHA256 && saved.DestinationSHA256 == candidate.DestinationSHA256 && saved.CatalogSHA256 == candidate.CatalogSHA256) {
		return true, nil
	}
	for _, stream := range selected {
		if !checkpoint.HasCompletedBackfill(stream.Self()) && (checkpoint.GetResumePlan(stream.Self()) != nil || checkpoint.GetChunks(stream.Self()) != nil) {
			return false, fmt.Errorf("partial backfill retry configuration differs from its immutable source, destination, or catalog binding")
		}
	}
	return false, nil
}

func persistBackfillCheckpoint(ctx context.Context, candidate *types.BackfillRecoveryCheckpoint, lower *string, local, uri string, data []byte, selected []types.StreamInterface) error {
	if candidate.ServerIdentity == "" {
		return nil
	}
	if err := utils.CheckExecutionFence(ctx); err != nil {
		return err
	}
	if err := validateBackfillRecoveryState(data, selected); err != nil {
		if !errors.Is(err, errBackfillTableUnbound) {
			return err
		}
		if len(candidate.State) != 0 {
			return nil
		}
		data, err = omitUnboundBackfillStreams(data)
		if err != nil {
			return err
		}
	}
	var object map[string]any
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	if err := decoder.Decode(&object); err != nil {
		return err
	}
	global, ok := object["global"].(map[string]any)
	if !ok {
		return fmt.Errorf("backfill candidate requires the original GLOBAL LSN")
	}
	position, ok := global["state"].(map[string]any)
	if !ok {
		return fmt.Errorf("backfill candidate requires the original PostgreSQL checkpoint")
	}
	lsn, _ := position["lsn"].(string)
	parsed, err := ordinaryRecoveryLSN(lsn)
	if err != nil || parsed == 0 {
		return fmt.Errorf("backfill candidate has no valid nonzero original LSN")
	}
	if *lower == "" {
		*lower = lsn
	}
	position["lsn"] = *lower
	candidate.State, err = json.Marshal(object)
	if err != nil {
		return err
	}
	candidate.StateSHA256, err = utils.CanonicalStateSHA256(candidate.State)
	if err != nil {
		return err
	}
	encoded, err := json.Marshal(candidate)
	if err != nil {
		return err
	}
	if err := writeResyncArtifact(local, encoded); err != nil {
		return fmt.Errorf("persist provisional backfill checkpoint: %w", err)
	}
	if err := utils.CheckExecutionFence(ctx); err != nil {
		return err
	}
	if err := utils.UploadArtifactOutput(ctx, local, uri, false); err != nil {
		return fmt.Errorf("publish provisional backfill checkpoint: %w", err)
	}
	return nil
}

func loadBackfillRecoveryCheckpoint(path string, origin types.OrdinaryRecoveryOrigin, baseline []byte) (*types.BackfillRecoveryCheckpoint, error) {
	if !origin.Backfill {
		if strings.TrimSpace(path) != "" {
			return nil, fmt.Errorf("--backfill-recovery-checkpoint requires a backfill ordinary recovery origin")
		}
		return nil, nil
	}
	if strings.TrimSpace(path) == "" {
		return nil, fmt.Errorf("backfill ordinary recovery requires --backfill-recovery-checkpoint")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("backfill recovery requires its copied durable candidate: %w", err)
	}
	digest := sha256.Sum256(data)
	if hex.EncodeToString(digest[:]) != origin.BackfillCheckpointSHA256 {
		return nil, fmt.Errorf("backfill recovery candidate differs from immutable origin")
	}
	var candidate types.BackfillRecoveryCheckpoint
	if err := strictRecoveryJSON(data, &candidate); err != nil {
		return nil, err
	}
	if candidate.Version != 1 || candidate.ExecutionID != origin.OriginalExecutionID || candidate.LaunchID != origin.OriginalLaunchID || candidate.SourceSHA256 != origin.SourceSHA256 || candidate.DestinationSHA256 != origin.DestinationSHA256 || candidate.CatalogSHA256 != origin.CatalogSHA256 || candidate.StateSHA256 != origin.BaselineStateSHA256 || strings.TrimSpace(candidate.ServerIdentity) == "" || strings.TrimSpace(candidate.SlotName) == "" {
		return nil, fmt.Errorf("backfill recovery candidate has foreign runtime, configuration, checkpoint, or source bindings")
	}
	stateDigest, err := utils.CanonicalStateSHA256(candidate.State)
	if err != nil || stateDigest != candidate.StateSHA256 {
		return nil, fmt.Errorf("backfill recovery candidate state digest is invalid")
	}
	baselineDigest, err := utils.CanonicalStateSHA256(baseline)
	if err != nil || baselineDigest != candidate.StateSHA256 {
		return nil, fmt.Errorf("backfill recovery baseline differs from durable candidate")
	}
	return &candidate, nil
}

func validateBackfillRecoveryState(data []byte, selected []types.StreamInterface) error {
	var checkpoint struct {
		Type   string `json:"type"`
		Global struct {
			State struct {
				LSN string `json:"lsn"`
			} `json:"state"`
			Streams []string `json:"streams"`
		} `json:"global"`
		Streams []struct {
			Stream    string                     `json:"stream"`
			Namespace string                     `json:"namespace"`
			SyncMode  string                     `json:"sync_mode"`
			State     map[string]json.RawMessage `json:"state"`
		} `json:"streams"`
	}
	if err := json.Unmarshal(data, &checkpoint); err != nil {
		return err
	}
	lsn, err := ordinaryRecoveryLSN(checkpoint.Global.State.LSN)
	if err != nil || lsn == 0 || checkpoint.Type != string(types.GlobalType) || checkpoint.Global.Streams == nil {
		return fmt.Errorf("backfill recovery requires original GLOBAL LSN and explicit completed membership (type=%q valid_lsn=%t membership_present=%t)", checkpoint.Type, err == nil && lsn != 0, checkpoint.Global.Streams != nil)
	}
	streams := make(map[string]bool, len(selected))
	for _, stream := range selected {
		streams[stream.ID()] = true
	}
	completed := make(map[string]bool)
	for _, id := range checkpoint.Global.Streams {
		if !streams[id] || completed[id] {
			return fmt.Errorf("backfill recovery contains foreign or duplicate completed membership")
		}
		completed[id] = true
	}
	seen := make(map[string]bool)
	unbound := false
	for _, entry := range checkpoint.Streams {
		id := entry.Namespace + "." + entry.Stream
		if !streams[id] || seen[id] || entry.SyncMode != string(types.CDC) {
			return fmt.Errorf("backfill recovery contains foreign, duplicate, or non-CDC stream state")
		}
		seen[id] = true
		if value := entry.State[types.BackfillTableKey]; !completed[id] && (len(value) == 0 || bytes.Equal(value, []byte("null"))) {
			unbound = true
		} else {
			var binding types.BackfillTableBinding
			if err := strictRecoveryJSON(entry.State[types.BackfillTableKey], &binding); err != nil {
				return fmt.Errorf("backfill recovery stream %s lacks immutable destination identity", id)
			}
			tableID, err := uuid.Parse(binding.TableUUID)
			shape, shapeErr := hex.DecodeString(binding.ShapeSHA256)
			if err != nil || tableID == uuid.Nil || shapeErr != nil || len(shape) != sha256.Size || binding.ShapeSHA256 != strings.ToLower(binding.ShapeSHA256) {
				return fmt.Errorf("backfill recovery stream %s has invalid destination identity", id)
			}
		}
		fullPlan, err := backfillChunks(entry.State[types.BackfillPlanKey], true)
		if err != nil || len(fullPlan) == 0 {
			return fmt.Errorf("backfill recovery stream %s lacks retained complete immutable plan", id)
		}
		plan, err := backfillChunks(entry.State[types.ResumePlanKey], !completed[id])
		if err != nil {
			return fmt.Errorf("backfill recovery stream %s has invalid immutable plan: %w", id, err)
		}
		pending, err := backfillChunks(entry.State[types.ChunksKey], true)
		if err != nil {
			return fmt.Errorf("backfill recovery stream %s has invalid pending chunks: %w", id, err)
		}
		if completed[id] {
			if len(plan) != 0 || len(pending) != 0 {
				return fmt.Errorf("backfill recovery completed stream %s retains unfinished chunks", id)
			}
		} else {
			if len(plan) == 0 {
				return fmt.Errorf("backfill recovery stream %s has no immutable resume plan", id)
			}
			if len(plan) != len(fullPlan) {
				return fmt.Errorf("backfill recovery stream %s resume plan differs from retained immutable plan", id)
			}
			for chunk := range plan {
				if !fullPlan[chunk] {
					return fmt.Errorf("backfill recovery stream %s resume plan differs from retained immutable plan", id)
				}
			}
			for chunk := range pending {
				if !plan[chunk] {
					return fmt.Errorf("backfill recovery stream %s pending chunk is outside its immutable plan", id)
				}
			}
		}
	}
	for id := range completed {
		if !seen[id] {
			return fmt.Errorf("backfill recovery completed stream %s lacks destination binding", id)
		}
	}
	if unbound {
		return errBackfillTableUnbound
	}
	return nil
}

func backfillChunks(data []byte, required bool) (map[string]bool, error) {
	chunks := make(map[string]bool)
	if len(data) == 0 {
		if required {
			return nil, fmt.Errorf("missing chunk array")
		}
		return chunks, nil
	}
	var entries []map[string]json.RawMessage
	if err := json.Unmarshal(data, &entries); err != nil || entries == nil {
		return nil, fmt.Errorf("chunks must be an array")
	}
	for _, entry := range entries {
		if len(entry) != 2 || len(entry["min"]) == 0 || len(entry["max"]) == 0 {
			return nil, fmt.Errorf("chunk must contain exact min and max boundaries")
		}
		encoded, err := json.Marshal(entry)
		if err != nil {
			return nil, err
		}
		key, err := utils.CanonicalStateSHA256(encoded)
		if err != nil || chunks[key] {
			return nil, fmt.Errorf("invalid or duplicate chunk")
		}
		chunks[key] = true
	}
	return chunks, nil
}

func omitUnboundBackfillStreams(data []byte) ([]byte, error) {
	var object map[string]json.RawMessage
	if err := json.Unmarshal(data, &object); err != nil {
		return nil, err
	}
	var entries []json.RawMessage
	if err := json.Unmarshal(object["streams"], &entries); err != nil {
		return nil, err
	}
	retained := make([]json.RawMessage, 0, len(entries))
	for _, raw := range entries {
		var entry struct {
			State map[string]json.RawMessage `json:"state"`
		}
		if err := json.Unmarshal(raw, &entry); err != nil {
			return nil, err
		}
		if binding := entry.State[types.BackfillTableKey]; len(binding) != 0 && !bytes.Equal(binding, []byte("null")) {
			retained = append(retained, raw)
		}
	}
	var err error
	object["streams"], err = json.Marshal(retained)
	if err != nil {
		return nil, err
	}
	return json.Marshal(object)
}
