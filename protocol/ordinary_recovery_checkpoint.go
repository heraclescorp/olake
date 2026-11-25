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
	"sync"
	"time"

	"github.com/aws/smithy-go"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
	"github.com/datazip-inc/olake/utils/logger"
	"github.com/spf13/cobra"
)

var (
	ordinaryRecoveryCheckpointInputURI    string
	ordinaryRecoveryCheckpointInputOrigin string
)

func ordinaryRecoveryInterval(cmd *cobra.Command) (time.Duration, error) {
	value := recoveryCheckpointInterval
	if !cmd.Flags().Changed("recovery-checkpoint-interval") {
		if configured, exists := os.LookupEnv("OLAKE_RECOVERY_CHECKPOINT_INTERVAL"); exists {
			value = configured
		}
	}
	interval, err := time.ParseDuration(value)
	if err != nil || interval <= 0 {
		return 0, fmt.Errorf("recovery checkpoint interval must be a positive Go duration: %q", value)
	}
	return interval, nil
}

func loadOrdinaryRecoveryCheckpoint(ctx context.Context, origin types.OrdinaryRecoveryOrigin) (*types.OrdinaryRecoveryCheckpoint, string, error) {
	input := filepath.Join(filepath.Dir(ordinaryRecoveryOriginPath), utils.OrdinaryRecoveryCheckpointName)
	if ordinaryRecoveryOriginPath == ordinaryRecoveryCheckpointInputOrigin && ordinaryRecoveryCheckpointInputURI != "" {
		input = ordinaryRecoveryCheckpointInputURI
	} else if strings.HasPrefix(ordinaryRecoveryOriginPath, "s3://") {
		input = ordinaryRecoveryOriginPath[:strings.LastIndex(ordinaryRecoveryOriginPath, "/")+1] + utils.OrdinaryRecoveryCheckpointName
	}
	local, uri, err := utils.ResolveArtifactOutput(input, utils.OrdinaryRecoveryCheckpointName, configPath)
	if err != nil {
		return nil, "", err
	}
	data, err := utils.ReadImmutableArtifact(ctx, local, uri)
	if err != nil {
		var apiError smithy.APIError
		if os.IsNotExist(err) || (errors.As(err, &apiError) && (apiError.ErrorCode() == "NoSuchKey" || apiError.ErrorCode() == "NotFound")) {
			return nil, "", nil
		}
		return nil, "", err
	}
	var checkpoint types.OrdinaryRecoveryCheckpoint
	if err := strictRecoveryJSON(data, &checkpoint); err != nil {
		return nil, "", fmt.Errorf("invalid ordinary recovery checkpoint: %w", err)
	}
	if checkpoint.OrdinaryRecoveryOrigin != origin || checkpoint.ExecutionID == executionID {
		return nil, "", fmt.Errorf("ordinary recovery checkpoint must bind the immutable origin and a retired runtime")
	}
	sum := sha256.Sum256(data)
	return &checkpoint, hex.EncodeToString(sum[:]), nil
}

func validateOrdinaryRecoveryCheckpoint(checkpoint types.OrdinaryRecoveryCheckpoint, origin types.OrdinaryRecoveryOrigin, selected []types.StreamInterface, lower string) error {
	if checkpoint.OrdinaryRecoveryOrigin != origin || checkpoint.LowerLSN != lower {
		return fmt.Errorf("ordinary recovery checkpoint changed the immutable origin or baseline")
	}
	state := &types.State{RWMutex: &sync.RWMutex{}}
	if err := decodeOrdinaryRecoveryState(checkpoint.State, state); err != nil {
		return err
	}
	canonical, err := canonicalResyncState(checkpoint.State)
	if err != nil || !bytes.Equal(canonical, checkpoint.State) {
		return fmt.Errorf("ordinary recovery checkpoint state is not canonical")
	}
	digest, err := utils.CanonicalStateSHA256(checkpoint.State)
	if err != nil || digest != checkpoint.StateSHA256 {
		return fmt.Errorf("ordinary recovery checkpoint state digest mismatch")
	}
	if err := validateOrdinaryRecoveryProof(state, checkpoint.OrdinaryRecoveryResult, checkpoint.FinalTargetLSN); err != nil {
		return err
	}
	if (checkpoint.PreviousCheckpointSHA256 == "") != (checkpoint.PreviousExecutionID == "") {
		return fmt.Errorf("ordinary recovery checkpoint has an incomplete predecessor binding")
	}
	if checkpoint.PreviousCheckpointSHA256 != "" {
		digest, err := hex.DecodeString(checkpoint.PreviousCheckpointSHA256)
		if err != nil || len(digest) != sha256.Size || strings.ToLower(checkpoint.PreviousCheckpointSHA256) != checkpoint.PreviousCheckpointSHA256 || checkpoint.PreviousExecutionID == origin.OriginalExecutionID || strings.TrimSpace(checkpoint.PreviousExecutionID) == "" {
			return fmt.Errorf("ordinary recovery checkpoint has an invalid predecessor binding")
		}
	}
	if _, _, err := utils.OrdinaryRecoveryCheckpointPaths(ordinaryRecoveryResultPath, "", checkpoint.ExecutionID, checkpoint.TargetLSN); err != nil {
		return err
	}
	if selected != nil {
		if len(selected) != len(checkpoint.Tables) {
			return fmt.Errorf("ordinary recovery checkpoint does not prove the full frozen selection")
		}
		members := make(map[string]bool, len(checkpoint.Tables))
		for _, proof := range checkpoint.Tables {
			members[proof.Stream] = true
		}
		for _, stream := range selected {
			if !members[stream.ID()] || !state.HasCompletedBackfill(stream.Self()) {
				return fmt.Errorf("ordinary recovery checkpoint omits selected stream %s", stream.ID())
			}
		}
	}
	return nil
}

func validateOrdinaryRecoveryResumeProofs(checkpoint types.OrdinaryRecoveryCheckpoint, observed []types.OrdinaryRecoveryTableProof) error {
	if len(observed) != len(checkpoint.Tables) {
		return fmt.Errorf("ordinary recovery fresh table proofs do not cover the checkpoint")
	}
	prior := make(map[string]types.OrdinaryRecoveryTableProof, len(checkpoint.Tables))
	for _, proof := range checkpoint.Tables {
		prior[proof.Stream] = proof
	}
	final, err := ordinaryRecoveryLSN(checkpoint.FinalTargetLSN)
	if err != nil {
		return err
	}
	for _, proof := range observed {
		saved, exists := prior[proof.Stream]
		position, err := ordinaryRecoveryLSN(proof.CommittedLSN)
		minimum, savedErr := ordinaryRecoveryLSN(saved.CommittedLSN)
		if !exists || err != nil || savedErr != nil || proof.TableUUID != saved.TableUUID || proof.FenceSnapshotID <= 0 || position < minimum || position > final {
			return fmt.Errorf("ordinary recovery table %s no longer proves the saved checkpoint", proof.Stream)
		}
		delete(prior, proof.Stream)
	}
	return nil
}

func ordinaryRecoveryCompletedState(current *types.State, target string) (*types.State, error) {
	completed, err := current.Clone()
	if err != nil {
		return nil, err
	}
	if completed.Global == nil || completed.Global.State == nil {
		return nil, fmt.Errorf("ordinary recovery cannot checkpoint missing GLOBAL state")
	}
	data, err := json.Marshal(completed.Global.State)
	if err != nil {
		return nil, err
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	var global map[string]any
	if err := decoder.Decode(&global); err != nil || global == nil {
		return nil, fmt.Errorf("ordinary recovery cannot checkpoint malformed GLOBAL state")
	}
	global["lsn"] = target
	completed.Global.State = global
	completed.SuppressPersistence = true
	return completed, nil
}

func persistOrdinaryRecoveryBatch(ctx context.Context, state *types.State, result types.OrdinaryRecoveryResult, finalTarget, previousHash, previousExecution string, acknowledge func() error) (*types.OrdinaryRecoveryCheckpoint, string, error) {
	data, err := state.MarshalJSON()
	if err != nil {
		return nil, "", err
	}
	canonical, err := canonicalResyncState(data)
	if err != nil {
		return nil, "", err
	}
	result.StateSHA256, err = utils.CanonicalStateSHA256(canonical)
	if err != nil {
		return nil, "", err
	}
	checkpoint := &types.OrdinaryRecoveryCheckpoint{OrdinaryRecoveryResult: result, FinalTargetLSN: finalTarget, State: canonical, PreviousCheckpointSHA256: previousHash, PreviousExecutionID: previousExecution}
	if err := validateOrdinaryRecoveryCheckpoint(*checkpoint, result.OrdinaryRecoveryOrigin, nil, result.LowerLSN); err != nil {
		return nil, "", err
	}
	encoded, err := json.Marshal(checkpoint)
	if err != nil {
		return nil, "", err
	}
	local, uri, err := utils.OrdinaryRecoveryCheckpointPaths(ordinaryRecoveryResultPath, ordinaryRecoveryResultURI, result.ExecutionID, result.TargetLSN)
	if err != nil {
		return nil, "", err
	}
	digest, err := utils.PublishVerifiedRecoveryCheckpoint(ctx, local, uri, encoded)
	if err != nil {
		return nil, "", err
	}
	logger.Infof("ordinary recovery persisted checkpoint checkpoint_lsn=%s persisted_at=%s final_target_lsn=%s", result.TargetLSN, time.Now().UTC().Format(time.RFC3339Nano), finalTarget)
	if err := acknowledge(); err != nil {
		return nil, "", err
	}
	if err := utils.CheckExecutionFence(ctx); err != nil {
		return nil, "", err
	}
	return checkpoint, digest, nil
}

func validateOrdinaryRecoveryCheckpointBaseline(checkpoint types.OrdinaryRecoveryCheckpoint, baseline *types.State, selected []types.StreamInterface) error {
	type stateView struct {
		Type    types.StateType `json:"type"`
		Version int             `json:"version"`
		Global  struct {
			State   map[string]json.RawMessage `json:"state"`
			Streams []string                   `json:"streams"`
		} `json:"global"`
		Streams        []json.RawMessage `json:"streams"`
		BackfillSource json.RawMessage   `json:"backfill_source"`
	}
	data, err := baseline.MarshalJSON()
	if err != nil {
		return err
	}
	var original, resumed stateView
	if err := json.Unmarshal(data, &original); err != nil {
		return err
	}
	if err := json.Unmarshal(checkpoint.State, &resumed); err != nil {
		return err
	}
	if original.Type != resumed.Type || original.Version != resumed.Version || !ordinaryRecoveryJSONEqual(original.BackfillSource, resumed.BackfillSource) || len(original.Global.State) != len(resumed.Global.State) {
		return fmt.Errorf("ordinary recovery checkpoint changed immutable state metadata")
	}
	for key, value := range original.Global.State {
		if key != "lsn" && !ordinaryRecoveryJSONEqual(value, resumed.Global.State[key]) {
			return fmt.Errorf("ordinary recovery checkpoint changed preserved GLOBAL field %s", key)
		}
	}
	members := make(map[string]bool, len(original.Global.Streams)+len(selected))
	selection := make(map[string]bool, len(selected))
	for _, id := range original.Global.Streams {
		members[id] = true
	}
	for _, stream := range selected {
		members[stream.ID()] = true
		selection[stream.ID()] = true
	}
	if len(resumed.Global.Streams) != len(members) {
		return fmt.Errorf("ordinary recovery checkpoint changed preserved GLOBAL membership")
	}
	for _, id := range resumed.Global.Streams {
		if !members[id] {
			return fmt.Errorf("ordinary recovery checkpoint added or duplicated GLOBAL membership")
		}
		delete(members, id)
	}
	entries := make(map[string]json.RawMessage, len(original.Streams))
	for _, raw := range original.Streams {
		var entry struct {
			Stream    string `json:"stream"`
			Namespace string `json:"namespace"`
		}
		if err := json.Unmarshal(raw, &entry); err != nil {
			return err
		}
		id := entry.Namespace + "." + entry.Stream
		if entries[id] != nil {
			return fmt.Errorf("ordinary recovery baseline contains duplicate stream state")
		}
		entries[id] = raw
	}
	seen := make(map[string]bool, len(resumed.Streams))
	for _, raw := range resumed.Streams {
		var entry struct {
			Stream    string                     `json:"stream"`
			Namespace string                     `json:"namespace"`
			State     map[string]json.RawMessage `json:"state"`
		}
		if err := json.Unmarshal(raw, &entry); err != nil {
			return err
		}
		id := entry.Namespace + "." + entry.Stream
		prior, exists := entries[id]
		if seen[id] || (!exists && (!checkpoint.Backfill || !selection[id])) {
			return fmt.Errorf("ordinary recovery checkpoint added or duplicated stream state")
		}
		seen[id] = true
		if !checkpoint.Backfill || !selection[id] {
			if !ordinaryRecoveryJSONEqual(prior, raw) {
				return fmt.Errorf("ordinary recovery checkpoint changed preserved stream state %s", id)
			}
		} else if exists {
			var saved struct {
				State map[string]json.RawMessage `json:"state"`
			}
			if err := json.Unmarshal(prior, &saved); err != nil {
				return err
			}
			before, err := backfillChunks(saved.State[types.BackfillPlanKey], true)
			after, afterErr := backfillChunks(entry.State[types.BackfillPlanKey], true)
			if err != nil || afterErr != nil || len(before) != len(after) {
				return fmt.Errorf("ordinary recovery checkpoint changed immutable backfill plan %s", id)
			}
			for chunk := range before {
				if !after[chunk] {
					return fmt.Errorf("ordinary recovery checkpoint changed immutable backfill plan %s", id)
				}
			}
			binding := saved.State[types.BackfillTableKey]
			if len(binding) != 0 && !bytes.Equal(binding, []byte("null")) && !ordinaryRecoveryJSONEqual(binding, entry.State[types.BackfillTableKey]) {
				return fmt.Errorf("ordinary recovery checkpoint changed immutable table binding %s", id)
			}
		}
		delete(entries, id)
	}
	if len(entries) != 0 {
		return fmt.Errorf("ordinary recovery checkpoint dropped preserved stream state")
	}
	if checkpoint.Backfill {
		return validateBackfillRecoveryState(checkpoint.State, selected)
	}
	return nil
}

func ordinaryRecoveryJSONEqual(left, right []byte) bool {
	if len(left) == 0 || len(right) == 0 {
		return len(left) == len(right)
	}
	var values [2]any
	for index, data := range [][]byte{left, right} {
		decoder := json.NewDecoder(bytes.NewReader(data))
		decoder.UseNumber()
		if err := decoder.Decode(&values[index]); err != nil {
			return false
		}
	}
	one, err := json.Marshal(values[0])
	if err != nil {
		return false
	}
	two, err := json.Marshal(values[1])
	return err == nil && bytes.Equal(one, two)
}
