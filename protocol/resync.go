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
	"strings"
	"time"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/destination"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
	"github.com/datazip-inc/olake/utils/errs"
	"github.com/datazip-inc/olake/utils/logger"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

const resyncMaximumDuration = 12 * time.Hour

var (
	resyncNamespace   string
	resyncTable       string
	resyncID          string
	resyncResultPath  string
	resyncResultURI   string
	resyncFailurePath string
	resyncFailureURI  string
)

var resyncCmd = &cobra.Command{
	Use:   "resync",
	Short: "Atomically rebuild one saved selected Iceberg table without interrupting readers",
	Long:  "Rebuild one saved selected Iceberg table in isolation, then publish one atomic replacement under the same table identity. Old data remains readable until publication. Each fresh attempt is limited to 12 hours; an expired attempt leaves old data intact. Unsupported source, destination, or catalog versions fail without clearing data.",
	Args:  cobra.NoArgs,
	PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
		if err := validateResyncFlags(); err != nil {
			return err
		}
		if err := syncCmd.PersistentPreRunE(cmd, args); err != nil {
			return err
		}
		if destinationConfig.Type != types.Iceberg {
			return fmt.Errorf("atomic resync requires an Iceberg destination; destination will not be cleared")
		}
		var err error
		resyncResultPath, resyncResultURI, err = utils.ResolveArtifactOutput(filepath.Join(filepath.Dir(viper.GetString(constants.StatePath)), "resync-result.json"), "resync-result.json", configPath)
		if err != nil {
			return err
		}
		resyncFailurePath, resyncFailureURI, err = utils.ResolveArtifactOutput(filepath.Join(filepath.Dir(viper.GetString(constants.StatePath)), "resync-failure.json"), "resync-failure.json", configPath)
		if err != nil {
			return err
		}
		for _, path := range []string{resyncResultPath, resyncFailurePath} {
			if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
				return fmt.Errorf("remove previous resync evidence: %w", err)
			}
		}
		return writeResyncEvidence(cmd.Context(), resyncFailurePath, resyncFailureURI, resyncID, resyncNamespace, resyncTable, "in_progress")
	},
	RunE: runResync,
}

func init() {
	resyncCmd.Flags().StringVar(&resyncNamespace, "resync-namespace", "", "Required source namespace of the saved selected table")
	resyncCmd.Flags().StringVar(&resyncTable, "resync-table", "", "Required source name of the saved selected table")
	resyncCmd.Flags().StringVar(&resyncID, "resync-id", "", "Required stable operation ID; reuse unchanged when retrying this request")
}

func validateResyncFlags() error {
	for _, flag := range []struct{ name, value string }{
		{"resync-namespace", resyncNamespace},
		{"resync-table", resyncTable},
		{"resync-id", resyncID},
		{"state", statePath},
	} {
		if strings.TrimSpace(flag.value) == "" {
			return errs.Precondition(errs.ConfigInvalid, codeFlagMissing, fmt.Errorf("--%s is required for atomic resync", flag.name))
		}
	}
	if noSave {
		return fmt.Errorf("atomic resync requires final state and publication receipt; --no-save is not supported")
	}
	return nil
}

func runResync(cmd *cobra.Command, _ []string) (err error) {
	ctx, cancel := context.WithTimeout(cmd.Context(), resyncMaximumDuration)
	defer cancel()
	ctx = types.WithResync(ctx, &types.ResyncOptions{Namespace: resyncNamespace, Table: resyncTable, ID: resyncID})
	defer recoverToError(&err)
	if err := connector.Setup(ctx); err != nil {
		return err
	}
	sourceCleaned := false
	cleanupSource := func() error {
		if sourceCleaned {
			return nil
		}
		if err := connector.CleanupResync(context.Background()); err != nil {
			return err
		}
		sourceCleaned = true
		return nil
	}
	defer func() {
		if cleanupErr := cleanupSource(); cleanupErr != nil {
			err = errors.Join(err, fmt.Errorf("resync source cleanup: %w", cleanupErr))
		}
	}()

	streams, err := connector.Discover(ctx, maxDiscoverThreads, !discoverSchema)
	if err != nil {
		return err
	}
	// Classification may prune ordinary sync state. A resync retains every saved checkpoint.
	savedStreams := state.Streams
	classification, err := classifyStreams(catalog, streams, state, destinationConfig.Type)
	state.Streams = savedStreams
	if err != nil {
		return err
	}
	target, err := selectedResyncTarget(catalog, classification, resyncNamespace, resyncTable)
	if err != nil {
		return err
	}
	connector.SetupState(state)
	if err := connector.ValidateResync(ctx, target); err != nil {
		return err
	}

	pool, err := destination.NewWriterPool(ctx, destinationConfig, classification.SelectedStreams, batchSize)
	if err != nil {
		return err
	}
	defer pool.Shutdown(context.Background())
	defer func() {
		if !pool.ResyncPublished() {
			if abortErr := pool.AbortResync(context.Background()); abortErr != nil {
				err = errors.Join(err, fmt.Errorf("abort resync: %w", abortErr))
				return
			}
			if cleanupErr := errors.Join(cleanupSource(), pool.FinishResync(context.Background())); cleanupErr != nil {
				err = errors.Join(err, fmt.Errorf("resync cleanup: %w", cleanupErr))
				return
			}
			if evidenceErr := writeResyncFailure(resyncFailurePath, resyncFailureURI, resyncID, resyncNamespace, resyncTable); evidenceErr != nil {
				err = errors.Join(err, fmt.Errorf("emit definite resync failure: %w", evidenceErr))
			}
		}
	}()
	checkpoint, err := pool.PrepareResync(ctx, target, resyncID, state)
	if err != nil {
		return err
	}
	if checkpoint != nil {
		state = checkpoint
		state.SuppressPersistence = true
		connector.SetupState(state)
		if err := connector.RecoverResync(ctx, checkpoint, target); err != nil {
			return err
		}
	} else {
		state.SuppressPersistence = true
		if err := connector.PrepareResync(ctx, target); err != nil {
			return err
		}
		if _, err := connector.ClearState([]types.StreamInterface{target}); err != nil {
			return err
		}
		full, cdc, incremental := resyncReadStreams(target, classification)
		if err := connector.Read(ctx, pool, full, cdc, incremental); err != nil {
			return fmt.Errorf("atomic resync failed before confirmed completion: %w", err)
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if !pool.ResyncPublished() {
		return fmt.Errorf("resync did not confirm atomic destination publication")
	}
	if err := cleanupSource(); err != nil {
		return fmt.Errorf("resync source cleanup: %w", err)
	}
	if err := pool.FinishResync(context.Background()); err != nil {
		return fmt.Errorf("resync destination cleanup: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	uuid, snapshot := pool.ResyncReceipt()
	if err := writeResyncArtifacts(viper.GetString(constants.StatePath), resyncResultPath, state, resyncReceipt{
		Version: 1, ResyncID: resyncID, Namespace: resyncNamespace, StreamName: resyncTable,
		TableUUID: uuid, SnapshotID: snapshot,
	}); err != nil {
		return err
	}
	logger.Infof("Atomic resync completed for %s.%s at snapshot %d", resyncNamespace, resyncTable, snapshot)
	return nil
}

func resyncReadStreams(target types.StreamInterface, classification *StreamClassification) (full, cdc, incremental []types.StreamInterface) {
	cdc = classification.CDCStreams
	if target.GetSyncMode() == types.FULLREFRESH {
		full = []types.StreamInterface{target}
	} else if target.GetSyncMode() == types.INCREMENTAL {
		incremental = []types.StreamInterface{target}
	}
	return full, cdc, incremental
}

func selectedResyncTarget(saved *types.Catalog, classification *StreamClassification, namespace, name string) (types.StreamInterface, error) {
	configured := 0
	for _, stream := range saved.Streams {
		if stream.Namespace() == namespace && stream.Name() == name {
			configured++
		}
	}
	if configured != 1 {
		return nil, fmt.Errorf("atomic resync requires exactly one saved target %s.%s; found %d", namespace, name, configured)
	}
	if saved.SelectedStreams != nil {
		selected := 0
		for _, stream := range saved.SelectedStreams[namespace] {
			if stream.StreamName == name {
				selected++
			}
		}
		if selected != 1 {
			return nil, fmt.Errorf("atomic resync target %s.%s must be selected exactly once", namespace, name)
		}
	}
	var target types.StreamInterface
	for _, stream := range classification.SelectedStreams {
		if stream.Namespace() == namespace && stream.Name() == name {
			if target != nil {
				return nil, fmt.Errorf("atomic resync target %s.%s is duplicated", namespace, name)
			}
			target = stream
		}
	}
	if target == nil {
		return nil, fmt.Errorf("atomic resync target %s.%s is not a valid selected stream", namespace, name)
	}
	switch target.GetSyncMode() {
	case types.CDC, types.FULLREFRESH, types.INCREMENTAL:
		return target, nil
	default:
		return nil, fmt.Errorf("atomic resync target %s.%s is not backfill-capable in mode %s", namespace, name, target.GetSyncMode())
	}
}

type resyncReceipt struct {
	Version     int    `json:"version"`
	ResyncID    string `json:"resync_id"`
	Namespace   string `json:"namespace"`
	StreamName  string `json:"stream_name"`
	TableUUID   string `json:"table_uuid"`
	SnapshotID  int64  `json:"snapshot_id"`
	StateSHA256 string `json:"state_sha256"`
}

func canonicalResyncState(data []byte) ([]byte, error) {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	var object map[string]any
	if err := decoder.Decode(&object); err != nil {
		return nil, fmt.Errorf("decode final resync state: %w", err)
	}
	if object == nil {
		return nil, fmt.Errorf("final resync state must be a JSON object")
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		return nil, fmt.Errorf("final resync state contains trailing JSON")
	}
	return json.Marshal(object)
}

func writeResyncArtifacts(stateFile, receiptFile string, finalState *types.State, receipt resyncReceipt) error {
	if receipt.Version != 1 || strings.TrimSpace(receipt.ResyncID) == "" || strings.TrimSpace(receipt.TableUUID) == "" || receipt.SnapshotID <= 0 {
		return fmt.Errorf("destination publication receipt is incomplete")
	}
	data, err := finalState.MarshalJSON()
	if err != nil {
		return err
	}
	canonical, err := canonicalResyncState(data)
	if err != nil {
		return err
	}
	digest := sha256.Sum256(canonical)
	receipt.StateSHA256 = hex.EncodeToString(digest[:])
	encodedReceipt, err := json.Marshal(receipt)
	if err != nil {
		return err
	}
	if err := writeResyncArtifact(stateFile, canonical); err != nil {
		return fmt.Errorf("emit committed resync state: %w", err)
	}
	if err := writeResyncArtifact(receiptFile, encodedReceipt); err != nil {
		return fmt.Errorf("emit resync publication receipt: %w", err)
	}
	return nil
}

func writeResyncFailure(path, uri, operationID, namespace, stream string) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	return writeResyncEvidence(ctx, path, uri, operationID, namespace, stream, "not_published")
}

func writeResyncEvidence(ctx context.Context, path, uri, operationID, namespace, stream, publication string) error {
	data, err := json.Marshal(struct {
		Version     int    `json:"version"`
		ResyncID    string `json:"resync_id"`
		Namespace   string `json:"namespace"`
		StreamName  string `json:"stream_name"`
		Publication string `json:"publication"`
	}{1, operationID, namespace, stream, publication})
	if err != nil {
		return err
	}
	if err := writeResyncArtifact(path, data); err != nil {
		return err
	}
	return utils.UploadArtifactOutput(ctx, path, uri, false)
}

func writeResyncArtifact(path string, data []byte) error {
	file, err := os.CreateTemp(filepath.Dir(path), ".resync-*")
	if err != nil {
		return err
	}
	defer os.Remove(file.Name())
	if _, err := file.Write(data); err != nil {
		file.Close()
		return err
	}
	if err := file.Sync(); err != nil {
		file.Close()
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	if err := os.Rename(file.Name(), path); err != nil {
		return err
	}
	directory, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	defer directory.Close()
	return directory.Sync()
}
