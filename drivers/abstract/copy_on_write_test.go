package abstract

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/destination"
	"github.com/datazip-inc/olake/types"
	"github.com/stretchr/testify/require"
)

type cowCDCDriver struct {
	metadataCDCDriver
	ack func(context.Context, any) error
}

func (cowCDCDriver) ValidateCopyOnWrite(context.Context, []types.StreamInterface) error { return nil }
func (d cowCDCDriver) AcknowledgeCDCCheckpoint(ctx context.Context, state any) error {
	return d.ack(ctx, state)
}

func TestCopyOnWriteBatchSoftLimits(t *testing.T) {
	now := time.Now()
	options := types.DefaultCOWOptions()
	for _, scenario := range []string{"idle", "pending", "rows", "bytes", "interval"} {
		t.Run(scenario, func(t *testing.T) {
			batch := cowBatch{options: options, rows: 1, bytes: 1, started: now}
			switch scenario {
			case "idle":
				batch.rows = 0
				batch.started = now.Add(-time.Hour)
			case "rows":
				batch.rows = int64(options.MaxBatchRows)
			case "bytes":
				batch.bytes = int64(options.MaxBatchBytes)
			case "interval":
				batch.started = now.Add(-time.Duration(options.FlushIntervalSeconds) * time.Second)
			}
			require.Equal(t, scenario != "idle" && scenario != "pending", batch.due(now))
		})
	}
}

func TestCopyOnWriteClosesAndReopensQuietTablesBeforeAck(t *testing.T) {
	for _, failQuiet := range []bool{false, true} {
		t.Run(fmt.Sprintf("quiet_failure=%t", failQuiet), func(t *testing.T) {
			streams := metadataStreams(2)
			var mu sync.Mutex
			setups := make(map[string]int)
			committed := make(map[string]string)
			committedOverlap := make(map[string]bool)
			var operations []any
			failure := errors.New("quiet table commit uncertain")
			pool := newMetadataWriterPool(t, streams, metadataWriterHooks{
				Setup: func(_ context.Context, stream types.StreamInterface, _ *destination.Options) (*types.MetadataState, error) {
					mu.Lock()
					defer mu.Unlock()
					setups[stream.ID()]++
					if committed[stream.ID()] != "" {
						overlap := committedOverlap[stream.ID()]
						return &types.MetadataState{State: committed[stream.ID()], DedupInserts: &overlap}, nil
					}
					return nil, nil
				},
				Write: func(_ context.Context, _ types.StreamInterface, _ *destination.Options, records []types.RawRecord) error {
					mu.Lock()
					defer mu.Unlock()
					for _, record := range records {
						operations = append(operations, record.OlakeColumns[constants.OpType])
					}
					return nil
				},
				Close: func(ctx context.Context, stream types.StreamInterface, _ *destination.Options, raw any) error {
					if err := ctx.Err(); err != nil {
						return err
					}
					metadata, ok := raw.(*types.MetadataState)
					if !ok {
						return fmt.Errorf("missing common checkpoint")
					}
					if failQuiet && stream.ID() == streams[1].ID() {
						return failure
					}
					mu.Lock()
					defer mu.Unlock()
					committed[stream.ID()] = metadata.State.(string)
					if metadata.DedupInserts == nil {
						return fmt.Errorf("missing overlap state")
					}
					committedOverlap[stream.ID()] = *metadata.DedupInserts
					return nil
				},
			})
			// The test-only writer is registered without a native destination. Enable
			// COW after constructing its pool to exercise the source barrier itself.
			streams[0].Self().StreamMetadata.MergeStrategy = types.CopyOnWrite
			appendMode := false
			streams[0].Self().StreamMetadata.AppendMode = &appendMode
			options := types.DefaultCOWOptions()
			options.MaxBatchRows = 1
			streams[0].Self().StreamMetadata.CowOptions = &options
			acks := 0
			driver := cowCDCDriver{ack: func(_ context.Context, state any) error {
				mu.Lock()
				defer mu.Unlock()
				for _, stream := range streams {
					if committed[stream.ID()] != `{"lsn":"0/210"}` || setups[stream.ID()] != 2 {
						return fmt.Errorf("ACK overtook quiet table close/reopen")
					}
				}
				acks++
				return nil
			}}
			driver.streamChanges = func(ctx context.Context, _ int, _ map[string]any, callback CDCMsgFn) (any, error) {
				row := CDCChange{Stream: streams[0], Kind: "insert", Data: map[string]any{"id": int64(1)}, Bytes: 8}
				if err := callback(ctx, row); err != nil {
					return nil, err
				}
				if acks != 0 {
					return nil, fmt.Errorf("row ACKed before complete COMMIT")
				}
				if err := callback(ctx, CDCChange{CommitState: map[string]string{"lsn": "0/210"}}); err != nil {
					return nil, err
				}
				if err := callback(ctx, row); err != nil {
					return nil, err
				}
				return map[string]string{"lsn": "0/310"}, nil
			}
			err := NewAbstractDriver(context.Background(), driver).streamChanges(context.Background(), pool, 0, streams)
			if failQuiet {
				require.ErrorIs(t, err, failure)
				require.Zero(t, acks, "one successful table is not global ACK proof")
			} else {
				require.NoError(t, err)
				require.Equal(t, 1, acks)
				require.Equal(t, []any{"i", "i"}, operations, "intermediate generations must preserve the backfill overlap window")
				for _, stream := range streams {
					require.Equal(t, `{"lsn":"0/310"}`, committed[stream.ID()])
					require.False(t, committedOverlap[stream.ID()], "only terminal catchup clears overlap")
				}
			}
		})
	}
}
