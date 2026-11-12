package iceberg

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/datazip-inc/olake/destination"
	"github.com/datazip-inc/olake/destination/iceberg/proto"
	"github.com/datazip-inc/olake/types"
)

type resyncControl struct {
	stream     types.StreamInterface
	id         string
	deadline   time.Time
	published  bool
	aborted    bool
	checkpoint string
	tableUUID  string
	snapshotID int64
}

func (i *Iceberg) PrepareResync(ctx context.Context, stream types.StreamInterface, operationID string, baseline *types.State) (*types.State, error) {
	i.resyncMu.Lock()
	defer i.resyncMu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if strings.TrimSpace(operationID) == "" {
		return nil, fmt.Errorf("resync operation ID must not be blank")
	}
	deadline, ok := ctx.Deadline()
	if !ok || time.Until(deadline) > destination.ResyncMaxDuration {
		return nil, fmt.Errorf("atomic resync requires a deadline no more than 12 hours away")
	}
	if i.resync != nil {
		return nil, fmt.Errorf("a resync has already been prepared on this destination")
	}
	if baseline == nil {
		return nil, fmt.Errorf("atomic resync requires the full baseline checkpoint")
	}
	encoded, err := json.Marshal(baseline)
	if err != nil {
		return nil, fmt.Errorf("failed to encode resync baseline checkpoint: %w", err)
	}
	canonical, err := canonicalResyncCheckpoint(string(encoded), "")
	if err != nil {
		return nil, err
	}
	i.resync = &resyncControl{stream: stream, id: operationID, deadline: deadline}
	response, err := i.resyncRequest(ctx, proto.IcebergPayload_PREPARE_RESYNC, string(canonical))
	if err != nil {
		return nil, fmt.Errorf("failed to prepare atomic resync: %w", err)
	}
	if !response.GetResyncPublished() {
		return nil, nil
	}
	return i.acceptPublishedResync(response, "")
}

func (i *Iceberg) PublishResync(ctx context.Context, checkpoint *types.State) error {
	i.resyncMu.Lock()
	defer i.resyncMu.Unlock()
	if i.resync == nil || i.resync.aborted {
		return fmt.Errorf("atomic resync has not been prepared or was aborted")
	}
	if checkpoint == nil {
		return fmt.Errorf("atomic resync requires the full final checkpoint")
	}
	encoded, err := json.Marshal(checkpoint)
	if err != nil {
		return fmt.Errorf("failed to encode resync checkpoint: %w", err)
	}
	canonical, err := canonicalResyncCheckpoint(string(encoded), "")
	if err != nil {
		return err
	}
	if i.resync.published {
		_, err := canonicalResyncCheckpoint(i.resync.checkpoint, string(canonical))
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if !time.Now().Before(i.resync.deadline) {
		return fmt.Errorf("atomic resync exceeded its 12-hour attempt deadline")
	}
	publishCtx, cancel := context.WithDeadline(ctx, i.resync.deadline)
	defer cancel()
	response, publishErr := i.resyncRequest(publishCtx, proto.IcebergPayload_PUBLISH_RESYNC, string(canonical))
	if publishErr == nil {
		_, publishErr = i.acceptPublishedResync(response, string(canonical))
		if publishErr == nil {
			return nil
		}
	}
	// A cancelled/lost response is not evidence of a failed catalog commit. Read
	// the durable marker without cancellation, and never discard uncertain files.
	statusCtx, statusCancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
	defer statusCancel()
	status, statusErr := i.resyncRequest(statusCtx, proto.IcebergPayload_RESYNC_STATUS, "")
	if statusErr == nil && status.GetResyncPublished() {
		_, statusErr = i.acceptPublishedResync(status, string(canonical))
		if statusErr == nil {
			return nil
		}
	}
	return fmt.Errorf("atomic resync publication is unconfirmed; retry the same resync ID to recover: %w", errors.Join(publishErr, statusErr))
}

// AbortResync succeeds only when the JVM has fenced this attempt against any
// subsequent publication. An unresolved publish must retain its execution fence.
func (i *Iceberg) AbortResync(ctx context.Context) error {
	i.resyncMu.Lock()
	defer i.resyncMu.Unlock()
	if i.resync == nil {
		return fmt.Errorf("atomic resync destination has not been prepared")
	}
	if i.resync.aborted {
		return nil
	}
	if i.resync.published {
		return fmt.Errorf("cannot abort an already published atomic resync")
	}
	response, err := i.resyncRequest(ctx, proto.IcebergPayload_ABORT_RESYNC, "")
	if err != nil {
		return fmt.Errorf("failed to abort atomic resync: %w", err)
	}
	if response.GetResyncPublished() {
		return fmt.Errorf("cannot abort an already published atomic resync")
	}
	i.resync.aborted = true
	return nil
}

func (i *Iceberg) ResyncPublished() bool {
	i.resyncMu.Lock()
	defer i.resyncMu.Unlock()
	return i.resync != nil && i.resync.published
}

func (i *Iceberg) ResyncReceipt() (string, int64) {
	i.resyncMu.Lock()
	defer i.resyncMu.Unlock()
	if i.resync == nil || !i.resync.published {
		return "", 0
	}
	return i.resync.tableUUID, i.resync.snapshotID
}

func (i *Iceberg) resyncRequest(ctx context.Context, kind proto.IcebergPayload_PayloadType, checkpoint string) (*proto.RecordIngestResponse, error) {
	response, err := i.server.client.SendRecords(ctx, &proto.IcebergPayload{
		Type: kind,
		Metadata: &proto.IcebergPayload_Metadata{
			DestTableName:    i.resync.stream.GetDestinationTable(),
			Namespace:        i.resync.stream.GetDestinationDatabase(&i.config.IcebergDatabase),
			ResyncId:         i.resync.id,
			ResyncCheckpoint: checkpoint,
			ResyncDeadlineMs: i.resync.deadline.UnixMilli(),
		},
	})
	if err != nil {
		return nil, err
	}
	if response == nil || !response.GetSuccess() {
		return nil, fmt.Errorf("atomic resync control request was rejected: %s", response.GetResult())
	}
	return response, nil
}

func (i *Iceberg) acceptPublishedResync(response *proto.RecordIngestResponse, expectedCheckpoint string) (*types.State, error) {
	if !response.GetResyncPublished() || response.GetResyncGeneration() != i.resync.id {
		return nil, fmt.Errorf("destination did not confirm the requested atomic resync generation")
	}
	if strings.TrimSpace(response.GetResyncTableUuid()) == "" || response.GetSnapshotId() <= 0 {
		return nil, fmt.Errorf("published atomic resync is missing its table UUID or positive snapshot ID")
	}
	checkpoint, err := canonicalResyncCheckpoint(response.GetResyncCheckpoint(), expectedCheckpoint)
	if err != nil {
		return nil, err
	}
	state := &types.State{RWMutex: &sync.RWMutex{}}
	decoder := json.NewDecoder(bytes.NewReader(checkpoint))
	decoder.UseNumber()
	if err := decoder.Decode(state); err != nil {
		return nil, fmt.Errorf("failed to decode published resync checkpoint: %w", err)
	}
	if state.Type != types.GlobalType && state.Type != types.StreamType && state.Type != types.MixedType {
		return nil, fmt.Errorf("published atomic resync checkpoint has no valid state type")
	}
	i.resync.published = true
	i.resync.checkpoint = response.GetResyncCheckpoint()
	i.resync.tableUUID = response.GetResyncTableUuid()
	i.resync.snapshotID = response.GetSnapshotId()
	return state, nil
}

func canonicalResyncCheckpoint(checkpoint, expected string) ([]byte, error) {
	canonicalize := func(raw string) ([]byte, error) {
		if !json.Valid([]byte(raw)) {
			return nil, fmt.Errorf("published atomic resync has no valid full checkpoint")
		}
		var value any
		decoder := json.NewDecoder(strings.NewReader(raw))
		decoder.UseNumber()
		if err := decoder.Decode(&value); err != nil {
			return nil, err
		}
		object, ok := value.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("atomic resync requires a full checkpoint JSON object")
		}
		// Only known state collections are unordered. Cursor arrays retain their order.
		if global, ok := object["global"].(map[string]any); ok {
			if streams, ok := global["streams"].([]any); ok {
				names := make([]string, len(streams))
				for index, stream := range streams {
					name, ok := stream.(string)
					if !ok {
						return nil, fmt.Errorf("published checkpoint contains a non-string stream identity")
					}
					names[index] = name
				}
				slices.Sort(names)
				global["streams"] = names
			}
		}
		if streams, ok := object["streams"].([]any); ok {
			for _, stream := range streams {
				if record, ok := stream.(map[string]any); ok {
					if state, ok := record["state"].(map[string]any); ok {
						if chunks, ok := state[types.ChunksKey].([]any); ok {
							ordered, err := canonicalResyncRecords(chunks)
							if err != nil {
								return nil, err
							}
							state[types.ChunksKey] = ordered
						}
					}
				}
			}
			ordered, err := canonicalResyncRecords(streams)
			if err != nil {
				return nil, err
			}
			object["streams"] = ordered
		}
		return json.Marshal(value)
	}
	canonical, err := canonicalize(checkpoint)
	if err != nil {
		return nil, err
	}
	if expected != "" {
		wanted, err := canonicalize(expected)
		if err != nil {
			return nil, err
		}
		if !bytes.Equal(canonical, wanted) {
			return nil, fmt.Errorf("published atomic resync checkpoint differs from the final source checkpoint")
		}
	}
	return canonical, nil
}

// canonicalResyncRecords encodes each set member once, before comparing sort keys.
func canonicalResyncRecords(records []any) ([]json.RawMessage, error) {
	encoded := make([]json.RawMessage, len(records))
	for index, record := range records {
		value, err := json.Marshal(record)
		if err != nil {
			return nil, err
		}
		encoded[index] = value
	}
	slices.SortFunc(encoded, func(left, right json.RawMessage) int {
		return bytes.Compare(left, right)
	})
	return encoded, nil
}

func (i *Iceberg) FinishResync(_ context.Context) error {
	return i.server.shutdownResync()
}
