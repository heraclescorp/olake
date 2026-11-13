package abstract

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/destination"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils/errs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type metadataCDCDriver struct {
	stubDriver
	streamChanges func(context.Context, int, map[string]any, CDCMsgFn) (any, error)
	postCDC       func(context.Context, int) error
}

func (d metadataCDCDriver) StreamChanges(ctx context.Context, index int, metadata map[string]any, callback CDCMsgFn) (any, error) {
	return d.streamChanges(ctx, index, metadata, callback)
}

func (d metadataCDCDriver) PostCDC(ctx context.Context, index int) error {
	if d.postCDC != nil {
		return d.postCDC(ctx, index)
	}
	return nil
}

func metadataStreams(count int) []types.StreamInterface {
	streams := make([]types.StreamInterface, count)
	for index := range streams {
		streams[index] = types.NewStream(fmt.Sprintf("table_%d", index), "public", nil).WithPrimaryKey("id").Wrap(0)
	}
	return streams
}

type metadataCDCRun struct {
	done chan struct{}
	err  error
}

func startMetadataCDC(ctx context.Context, t *testing.T, cancel context.CancelFunc, driver DriverInterface, pool *destination.WriterPool, index int, streams []types.StreamInterface) *metadataCDCRun {
	t.Helper()
	run := &metadataCDCRun{done: make(chan struct{})}
	go func() {
		defer close(run.done)
		run.err = NewAbstractDriver(ctx, driver).streamChanges(ctx, pool, index, streams)
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-run.done:
		case <-time.After(10 * time.Second):
			t.Error("CDC lifecycle did not join after cancellation")
		}
	})
	return run
}

func awaitMetadataEvent[T any](ctx context.Context, t *testing.T, events <-chan T) T {
	t.Helper()
	select {
	case event := <-events:
		return event
	case <-ctx.Done():
		t.Fatalf("waiting for CDC lifecycle event: %v", ctx.Err())
		var zero T
		return zero
	}
}

func TestCDCWriterSetupParallelBarrier(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	streams := metadataStreams(2*constants.CDCMetadataConcurrency + 3)
	indices := make(map[string]int, len(streams))
	setupMetadata := make(map[string]*types.MetadataState, len(streams))
	for index, stream := range streams {
		indices[stream.ID()] = index
		if index%4 != 0 {
			metadata := &types.MetadataState{State: "position-" + stream.ID()}
			if index%4 != 3 {
				dedup := index%4 == 1
				metadata.DedupInserts = &dedup
			}
			setupMetadata[stream.ID()] = metadata
		}
	}

	started := make(chan string, len(streams))
	release := make(chan struct{})
	var active, maximum, completed, writes, closes atomic.Int32
	var setupContexts sync.Map
	var postCalls atomic.Int32

	pool := newMetadataWriterPool(t, streams, metadataWriterHooks{
		Setup: func(setupCtx context.Context, stream types.StreamInterface, opts *destination.Options) (*types.MetadataState, error) {
			current := active.Add(1)
			defer active.Add(-1)
			for previous := maximum.Load(); current > previous; previous = maximum.Load() {
				if maximum.CompareAndSwap(previous, current) {
					break
				}
			}
			if current > constants.CDCMetadataConcurrency {
				return nil, fmt.Errorf("too many active setups: %d", current)
			}
			if !opts.ApplyFilter || opts.Backfill || opts.KeepDeletesThroughFilter || !strings.HasPrefix(opts.ThreadID, stream.ID()+"_") {
				return nil, fmt.Errorf("wrong CDC writer options for %s: %+v", stream.ID(), opts)
			}
			setupContexts.Store(stream.ID(), setupCtx)
			started <- stream.ID()
			select {
			case <-release:
			case <-setupCtx.Done():
				return nil, setupCtx.Err()
			}
			completed.Add(1)
			return setupMetadata[stream.ID()], nil
		},
		Write: func(writeCtx context.Context, stream types.StreamInterface, _ *destination.Options, records []types.RawRecord) error {
			if err := writeCtx.Err(); err != nil {
				return fmt.Errorf("writer context died after setup: %w", err)
			}
			if len(records) != 1 {
				return fmt.Errorf("unexpected record count: %d", len(records))
			}
			expected := "i"
			if indices[stream.ID()]%4 == 2 {
				expected = "c"
			}
			if got := records[0].OlakeColumns[constants.OpType]; got != expected {
				return fmt.Errorf("stream %s dedup association: got %v, want %s", stream.ID(), got, expected)
			}
			writes.Add(1)
			return nil
		},
		Close: func(closeCtx context.Context, stream types.StreamInterface, _ *destination.Options, metadata any) error {
			if err := closeCtx.Err(); err != nil {
				return fmt.Errorf("close context died after setup for %s: %w", stream.ID(), err)
			}
			state, ok := metadata.(*types.MetadataState)
			if !ok || state.State != "final-position" || state.DedupInserts == nil || *state.DedupInserts {
				return fmt.Errorf("unexpected committed CDC metadata for %s: %+v", stream.ID(), metadata)
			}
			closes.Add(1)
			return nil
		},
	})
	driver := metadataCDCDriver{
		stubDriver: stubDriver{typ: string(constants.Postgres)},
		streamChanges: func(sourceCtx context.Context, index int, metadata map[string]any, callback CDCMsgFn) (any, error) {
			if index != 7 || sourceCtx.Err() != nil || int(completed.Load()) != len(streams) || active.Load() != 0 || len(metadata) != len(streams) {
				return nil, fmt.Errorf("source crossed setup barrier early: index=%d, ctx=%v, completed=%d, active=%d, metadata=%d", index, sourceCtx.Err(), completed.Load(), active.Load(), len(metadata))
			}
			for _, stream := range streams {
				setupCtx, ok := setupContexts.Load(stream.ID())
				if !ok || setupCtx.(context.Context).Err() != nil || setupCtx != sourceCtx {
					return nil, fmt.Errorf("stream %s did not retain its live CDC context", stream.ID())
				}
				var expected any
				if indices[stream.ID()]%4 != 0 {
					expected = "position-" + stream.ID()
				}
				if got, exists := metadata[stream.ID()]; !exists || got != expected {
					return nil, fmt.Errorf("stream %s metadata association: got %v (exists %t), want %v", stream.ID(), got, exists, expected)
				}
				if err := callback(sourceCtx, CDCChange{Stream: stream, Kind: "insert", Data: map[string]any{"id": stream.ID()}}); err != nil {
					return nil, err
				}
			}
			return "final-position", nil
		},
		postCDC: func(postCtx context.Context, index int) error {
			postCalls.Add(1)
			if postCtx.Err() != nil || index != 7 || int(closes.Load()) != len(streams) {
				return fmt.Errorf("PostCDC preceded completed live writers: ctx=%v, index=%d, closes=%d", postCtx.Err(), index, closes.Load())
			}
			return nil
		},
	}
	run := startMetadataCDC(ctx, t, cancel, driver, pool, 7, streams)
	for range constants.CDCMetadataConcurrency {
		awaitMetadataEvent(ctx, t, started)
	}
	assert.EqualValues(t, constants.CDCMetadataConcurrency, active.Load())
	assert.Zero(t, completed.Load())
	close(release)
	awaitMetadataEvent(ctx, t, run.done)
	require.NoError(t, run.err)
	assert.EqualValues(t, constants.CDCMetadataConcurrency, maximum.Load())
	assert.EqualValues(t, len(streams), completed.Load())
	assert.EqualValues(t, len(streams), writes.Load())
	assert.EqualValues(t, len(streams), closes.Load())
	assert.EqualValues(t, 1, postCalls.Load())
}

func TestCDCWriterSetupFailureJoinsAndCleans(t *testing.T) {
	for _, test := range []struct {
		name        string
		panicSetup  bool
		lateSuccess bool
	}{
		{name: "error"},
		{name: "panic", panicSetup: true},
		{name: "success returned after cancellation", lateSuccess: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			streams := metadataStreams(3)
			failure := errs.Precondition(errs.NetworkUnreachable, "postgres.setup_failed", errors.New("setup failed"))
			started := make(chan string, len(streams))
			fail := make(chan struct{})
			canceled := make(chan struct{})
			release := make(chan struct{})
			var exited, closes, reads, postCalls, acknowledgements atomic.Int32
			var violationsMu sync.Mutex
			var violations []error
			recordViolation := func(err error) {
				violationsMu.Lock()
				defer violationsMu.Unlock()
				violations = append(violations, err)
			}
			pool := newMetadataWriterPool(t, streams, metadataWriterHooks{
				Setup: func(setupCtx context.Context, stream types.StreamInterface, _ *destination.Options) (*types.MetadataState, error) {
					started <- stream.ID()
					switch stream.ID() {
					case streams[0].ID():
						return &types.MetadataState{State: "old-position"}, nil
					case streams[1].ID():
						select {
						case <-fail:
						case <-setupCtx.Done():
							return nil, setupCtx.Err()
						}
						if test.panicSetup {
							panic("setup exploded")
						}
						return nil, failure
					default:
						<-setupCtx.Done()
						close(canceled)
						select {
						case <-release:
						case <-ctx.Done():
						}
						exited.Add(1)
						if test.lateSuccess {
							return &types.MetadataState{State: "old-position"}, nil
						}
						return nil, setupCtx.Err()
					}
				},
				Close: func(closeCtx context.Context, stream types.StreamInterface, _ *destination.Options, metadata any) error {
					if closeCtx.Err() == nil || exited.Load() != 1 || metadata != nil {
						recordViolation(fmt.Errorf("unsafe failed-setup cleanup for %s: ctx=%v, exited=%d, metadata=%v", stream.ID(), closeCtx.Err(), exited.Load(), metadata))
					}
					closes.Add(1)
					return nil
				},
			})
			expectedCloses := int32(1)
			if test.lateSuccess {
				expectedCloses++
			}
			driver := metadataCDCDriver{
				stubDriver: stubDriver{typ: string(constants.Postgres)},
				streamChanges: func(context.Context, int, map[string]any, CDCMsgFn) (any, error) {
					reads.Add(1)
					return "advanced-position", nil
				},
				postCDC: func(postCtx context.Context, _ int) error {
					postCalls.Add(1)
					if postCtx.Err() == nil {
						acknowledgements.Add(1)
					}
					if closes.Load() != expectedCloses || exited.Load() != 1 {
						recordViolation(fmt.Errorf("PostCDC preceded joined setup cleanup: closes=%d, exited=%d", closes.Load(), exited.Load()))
					}
					return nil
				},
			}
			run := startMetadataCDC(ctx, t, cancel, driver, pool, 0, streams)
			for range streams {
				awaitMetadataEvent(ctx, t, started)
			}
			close(fail)
			awaitMetadataEvent(ctx, t, canceled)
			assert.Zero(t, closes.Load(), "cleanup must wait for in-flight setup")
			assert.Zero(t, postCalls.Load())
			assert.Zero(t, reads.Load())
			close(release)
			awaitMetadataEvent(ctx, t, run.done)

			require.Error(t, run.err)
			if test.panicSetup {
				assert.Contains(t, run.err.Error(), "panic recovered: setup exploded")
				assert.Equal(t, codeWriterPanicRecovered, errs.From(errs.Classify(run.err)).Code)
			} else {
				assert.ErrorIs(t, run.err, failure, "setup failure must not be replaced by another worker's cancellation")
				assert.Equal(t, "postgres.setup_failed", errs.From(errs.Classify(run.err)).Code)
			}
			assert.Zero(t, reads.Load())
			assert.Zero(t, acknowledgements.Load())
			assert.Equal(t, expectedCloses, closes.Load())
			assert.EqualValues(t, 1, postCalls.Load())
			assert.Empty(t, violations)
		})
	}
}

func TestCDCWriterSetupContextCancellation(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cdcCtx, cancelCDC := context.WithCancel(ctx)
	streams := metadataStreams(2)
	started := make(chan string, len(streams))
	canceled := make(chan struct{})
	release := make(chan struct{})
	var exited, closes, reads, postCalls, acknowledgements atomic.Int32
	var unsafeCleanup atomic.Bool
	pool := newMetadataWriterPool(t, streams, metadataWriterHooks{
		Setup: func(setupCtx context.Context, stream types.StreamInterface, _ *destination.Options) (*types.MetadataState, error) {
			started <- stream.ID()
			if stream.ID() == streams[0].ID() {
				return &types.MetadataState{State: "old-position"}, nil
			}
			<-setupCtx.Done()
			close(canceled)
			select {
			case <-release:
			case <-ctx.Done():
			}
			exited.Add(1)
			return nil, setupCtx.Err()
		},
		Close: func(closeCtx context.Context, _ types.StreamInterface, _ *destination.Options, metadata any) error {
			if closeCtx.Err() == nil || exited.Load() != 1 || metadata != nil {
				unsafeCleanup.Store(true)
			}
			closes.Add(1)
			return nil
		},
	})
	driver := metadataCDCDriver{
		stubDriver: stubDriver{typ: string(constants.Postgres)},
		streamChanges: func(context.Context, int, map[string]any, CDCMsgFn) (any, error) {
			reads.Add(1)
			return "advanced-position", nil
		},
		postCDC: func(postCtx context.Context, _ int) error {
			postCalls.Add(1)
			if postCtx.Err() == nil {
				acknowledgements.Add(1)
			}
			if closes.Load() != 1 || exited.Load() != 1 {
				unsafeCleanup.Store(true)
			}
			return nil
		},
	}
	run := startMetadataCDC(cdcCtx, t, cancelCDC, driver, pool, 0, streams)
	for range streams {
		awaitMetadataEvent(ctx, t, started)
	}
	cancelCDC()
	awaitMetadataEvent(ctx, t, canceled)
	assert.Zero(t, closes.Load())
	assert.Zero(t, postCalls.Load())
	close(release)
	awaitMetadataEvent(ctx, t, run.done)

	assert.ErrorIs(t, run.err, context.Canceled)
	assert.Zero(t, reads.Load())
	assert.Zero(t, acknowledgements.Load())
	assert.EqualValues(t, 1, closes.Load())
	assert.EqualValues(t, 1, postCalls.Load())
	assert.False(t, unsafeCleanup.Load())
}

func TestCDCWriterSetupFailureStopsQueuedWork(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	streams := metadataStreams(constants.CDCMetadataConcurrency + 3)
	started := make(chan string, len(streams))
	fail := make(chan struct{})
	failure := errors.New("first setup failed")
	var setups, closes, reads, postCalls atomic.Int32
	var unsafeCleanup atomic.Bool
	pool := newMetadataWriterPool(t, streams, metadataWriterHooks{
		Setup: func(setupCtx context.Context, stream types.StreamInterface, _ *destination.Options) (*types.MetadataState, error) {
			setups.Add(1)
			started <- stream.ID()
			if stream.ID() == streams[0].ID() {
				select {
				case <-fail:
					return nil, failure
				case <-setupCtx.Done():
					return nil, setupCtx.Err()
				}
			}
			<-setupCtx.Done()
			return &types.MetadataState{State: "old-position"}, nil
		},
		Close: func(closeCtx context.Context, _ types.StreamInterface, _ *destination.Options, metadata any) error {
			if closeCtx.Err() == nil || metadata != nil {
				unsafeCleanup.Store(true)
			}
			closes.Add(1)
			return nil
		},
	})
	driver := metadataCDCDriver{
		stubDriver: stubDriver{typ: string(constants.Postgres)},
		streamChanges: func(context.Context, int, map[string]any, CDCMsgFn) (any, error) {
			reads.Add(1)
			return "advanced-position", nil
		},
		postCDC: func(postCtx context.Context, _ int) error {
			postCalls.Add(1)
			if postCtx.Err() == nil || closes.Load() != constants.CDCMetadataConcurrency-1 {
				unsafeCleanup.Store(true)
			}
			return nil
		},
	}
	run := startMetadataCDC(ctx, t, cancel, driver, pool, 0, streams)
	for range constants.CDCMetadataConcurrency {
		awaitMetadataEvent(ctx, t, started)
	}
	close(fail)
	awaitMetadataEvent(ctx, t, run.done)

	assert.ErrorIs(t, run.err, failure)
	assert.EqualValues(t, constants.CDCMetadataConcurrency, setups.Load(), "queued jobs must not create writers after cancellation")
	assert.EqualValues(t, constants.CDCMetadataConcurrency-1, closes.Load())
	assert.Zero(t, reads.Load())
	assert.EqualValues(t, 1, postCalls.Load())
	assert.False(t, unsafeCleanup.Load())
}

func TestCDCWriterSetupCanceledBeforeScheduling(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	streams := metadataStreams(1)
	var setups, reads, acknowledgements atomic.Int32
	pool := newMetadataWriterPool(t, streams, metadataWriterHooks{
		Setup: func(context.Context, types.StreamInterface, *destination.Options) (*types.MetadataState, error) {
			setups.Add(1)
			return nil, errors.New("setup called after cancellation")
		},
	})
	driver := metadataCDCDriver{
		stubDriver: stubDriver{typ: string(constants.Postgres)},
		streamChanges: func(context.Context, int, map[string]any, CDCMsgFn) (any, error) {
			reads.Add(1)
			return "advanced-position", nil
		},
		postCDC: func(postCtx context.Context, _ int) error {
			if postCtx.Err() == nil {
				acknowledgements.Add(1)
			}
			return nil
		},
	}
	err := NewAbstractDriver(ctx, driver).streamChanges(ctx, pool, 0, streams)
	assert.ErrorIs(t, err, context.Canceled)
	assert.Zero(t, setups.Load())
	assert.Zero(t, reads.Load())
	assert.Zero(t, acknowledgements.Load())
}
