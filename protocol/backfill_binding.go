package protocol

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/aws/smithy-go"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
	s3util "github.com/datazip-inc/olake/utils/s3"
	"github.com/google/uuid"
)

func backfillBindingName(stream string) string {
	digest := sha256.Sum256([]byte(stream))
	return "backfill-recovery-binding-" + hex.EncodeToString(digest[:]) + ".json"
}

func backfillBindingOutput(ctx context.Context, stream string) (string, string, error) {
	execution, ok := utils.ExecutionFromContext(ctx)
	if !ok || execution.Fence == "" {
		return "", "", fmt.Errorf("backfill binding journal requires a fixed runtime fence")
	}
	name := backfillBindingName(stream)
	output := filepath.Join(filepath.Dir(execution.Fence), name)
	if strings.HasPrefix(execution.Fence, "s3://") {
		output = execution.Fence[:strings.LastIndex(execution.Fence, "/")+1] + name
	}
	return utils.ResolveArtifactOutput(output, name, configPath)
}

func readBackfillBindingArtifact(ctx context.Context, local, uri string) ([]byte, error) {
	if uri == "" {
		return os.ReadFile(local)
	}
	bucket, key, ok := strings.Cut(strings.TrimPrefix(uri, "s3://"), "/")
	if !strings.HasPrefix(uri, "s3://") || !ok || bucket == "" || key == "" {
		return nil, fmt.Errorf("invalid backfill binding artifact URI")
	}
	object, err := s3util.GetObject(ctx, bucket, key)
	if err != nil {
		return nil, err
	}
	defer object.Body.Close()
	return io.ReadAll(object.Body)
}

func missingBackfillBinding(err error) bool {
	var apiError smithy.APIError
	return os.IsNotExist(err) || (errors.As(err, &apiError) && (apiError.ErrorCode() == "NoSuchKey" || apiError.ErrorCode() == "NotFound"))
}

func backfillBindingEnvelope(checkpoint *types.BackfillRecoveryCheckpoint, stream string) (types.BackfillRecoveryBinding, error) {
	var state struct {
		Global struct {
			State struct {
				LSN string `json:"lsn"`
			} `json:"state"`
		} `json:"global"`
	}
	if checkpoint == nil || json.Unmarshal(checkpoint.State, &state) != nil || checkpoint.Version != 1 || checkpoint.ExecutionID == "" || checkpoint.LaunchID == "" || checkpoint.ServerIdentity == "" || checkpoint.SlotName == "" || stream == "" {
		return types.BackfillRecoveryBinding{}, fmt.Errorf("backfill binding requires immutable execution and live source evidence")
	}
	lower, err := ordinaryRecoveryLSN(state.Global.State.LSN)
	if err != nil || lower == 0 {
		return types.BackfillRecoveryBinding{}, fmt.Errorf("backfill binding requires the original nonzero GLOBAL LSN")
	}
	return types.BackfillRecoveryBinding{Version: 1, OriginalExecutionID: checkpoint.ExecutionID, OriginalLaunchID: checkpoint.LaunchID, SourceSHA256: checkpoint.SourceSHA256, DestinationSHA256: checkpoint.DestinationSHA256, CatalogSHA256: checkpoint.CatalogSHA256, ServerIdentity: checkpoint.ServerIdentity, SlotName: checkpoint.SlotName, LowerLSN: state.Global.State.LSN, Stream: stream}, nil
}

func validateBackfillBinding(data []byte, expected types.BackfillRecoveryBinding) (*types.BackfillBindingJournalEntry, error) {
	var observed types.BackfillRecoveryBinding
	if err := strictRecoveryJSON(data, &observed); err != nil {
		return nil, err
	}
	if observed.Version != expected.Version || observed.OriginalExecutionID != expected.OriginalExecutionID || observed.OriginalLaunchID != expected.OriginalLaunchID || observed.SourceSHA256 != expected.SourceSHA256 || observed.DestinationSHA256 != expected.DestinationSHA256 || observed.CatalogSHA256 != expected.CatalogSHA256 || observed.ServerIdentity != expected.ServerIdentity || observed.SlotName != expected.SlotName || observed.LowerLSN != expected.LowerLSN || observed.Stream != expected.Stream {
		return nil, fmt.Errorf("backfill binding journal differs from immutable execution, configuration, source, lower LSN, or stream")
	}
	tableID, err := uuid.Parse(observed.TableUUID)
	shape, shapeErr := hex.DecodeString(observed.ShapeSHA256)
	plan, planErr := backfillChunks(observed.Plan, true)
	if err != nil || tableID == uuid.Nil || shapeErr != nil || len(shape) != sha256.Size || observed.ShapeSHA256 != strings.ToLower(observed.ShapeSHA256) || planErr != nil || len(plan) == 0 {
		return nil, fmt.Errorf("backfill binding journal has invalid UUID, shape, or exact plan")
	}
	return &observed.BackfillBindingJournalEntry, nil
}

func equalBackfillBindingEntries(first, second *types.BackfillBindingJournalEntry) bool {
	if first == nil || second == nil || first.TableUUID != second.TableUUID || first.ShapeSHA256 != second.ShapeSHA256 {
		return false
	}
	left, leftErr := backfillChunks(first.Plan, true)
	right, rightErr := backfillChunks(second.Plan, true)
	if leftErr != nil || rightErr != nil || len(left) == 0 || len(left) != len(right) {
		return false
	}
	for chunk := range left {
		if !right[chunk] {
			return false
		}
	}
	return true
}

func newBackfillBindingJournal(ctx context.Context, checkpoint *types.BackfillRecoveryCheckpoint) *types.BackfillBindingJournal {
	var mu sync.Mutex
	read := func(stream string) (*types.BackfillBindingJournalEntry, error) {
		if err := utils.CheckExecutionFence(ctx); err != nil {
			return nil, err
		}
		expected, err := backfillBindingEnvelope(checkpoint, stream)
		if err != nil {
			return nil, err
		}
		local, uri, err := backfillBindingOutput(ctx, stream)
		if err != nil {
			return nil, err
		}
		data, err := readBackfillBindingArtifact(ctx, local, uri)
		if missingBackfillBinding(err) {
			return nil, nil
		}
		if err != nil {
			return nil, err
		}
		return validateBackfillBinding(data, expected)
	}
	return &types.BackfillBindingJournal{
		Read: func(stream string) (*types.BackfillBindingJournalEntry, error) {
			mu.Lock()
			defer mu.Unlock()
			return read(stream)
		},
		Create: func(stream string, entry *types.BackfillBindingJournalEntry) error {
			mu.Lock()
			defer mu.Unlock()
			observed, err := read(stream)
			if err != nil {
				return err
			}
			if observed != nil {
				if !equalBackfillBindingEntries(observed, entry) {
					return fmt.Errorf("backfill binding journal is immutable and differs from the table or full plan")
				}
				return nil
			}
			envelope, err := backfillBindingEnvelope(checkpoint, stream)
			if err != nil || entry == nil {
				return errors.Join(err, fmt.Errorf("backfill binding requires a complete table entry"))
			}
			envelope.BackfillBindingJournalEntry = *entry
			data, err := json.Marshal(envelope)
			if err != nil {
				return err
			}
			if _, err := validateBackfillBinding(data, envelope); err != nil {
				return err
			}
			local, uri, err := backfillBindingOutput(ctx, stream)
			if err != nil {
				return err
			}
			if err := utils.CheckExecutionFence(ctx); err != nil {
				return err
			}
			publishErr := utils.PublishImmutableArtifact(ctx, local, uri, data)
			observed, err = read(stream)
			if err != nil || !equalBackfillBindingEntries(observed, entry) {
				return errors.Join(publishErr, err, fmt.Errorf("backfill binding journal lacks authoritative create-only persistence"))
			}
			return nil
		},
	}
}
