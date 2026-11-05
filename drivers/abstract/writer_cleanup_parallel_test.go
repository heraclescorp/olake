package abstract

import (
	"context"
	"errors"
	"fmt"
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

func cleanupTestStreams(count int) []types.StreamInterface {
	streams := make([]types.StreamInterface, count)
	for i := range streams {
		streams[i] = types.NewStream(fmt.Sprintf("cleanup_%02d", i), "public", nil).Wrap(0)
	}
	return streams
}

func cleanupTestWriters(t *testing.T, ctx context.Context, pool *destination.WriterPool, streams []types.StreamInterface) map[string]*destination.WriterThread {
	t.Helper()
	writers := make(map[string]*destination.WriterThread, len(streams))
	for _, stream := range streams {
		writer, _, err := pool.NewWriter(ctx, stream, destination.WithThreadID(stream.ID()))
		require.NoError(t, err)
		writers[stream.ID()] = writer
	}
	return writers
}

func waitCleanupEvent(t *testing.T, events <-chan struct{}) {
	t.Helper()
	select {
	case <-events:
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for writer lifecycle event")
	}
}

func waitCleanupResult(t *testing.T, result <-chan error) error {
	t.Helper()
	select {
	case err := <-result:
		return err
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for writer cleanup")
		return nil
	}
}

func TestHandleWriterCleanupParallelMetadata(t *testing.T) {
	type position struct {
		LSN string `json:"lsn"`
	}
	type observation struct {
		streamID string
		metadata any
		ctxErr   error
	}
	for _, perStream := range []bool{false, true} {
		t.Run(fmt.Sprintf("per_stream_%t", perStream), func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			t.Cleanup(cancel)
			streams := cleanupTestStreams(constants.CDCMetadataConcurrency*2 + 3)
			shared := &position{LSN: "0/200"}
			var metadata any = shared
			byStream := make(map[string]any, len(streams))
			if perStream {
				for i, stream := range streams {
					if i%3 != 0 {
						byStream[stream.ID()] = &position{LSN: fmt.Sprintf("0/%d", i)}
					}
				}
				metadata = byStream
			}

			release := make(chan struct{})
			var releaseOnce sync.Once
			unblock := func() { releaseOnce.Do(func() { close(release) }) }
			t.Cleanup(unblock)
			entered := make(chan struct{}, len(streams))
			observations := make(chan observation, len(streams))
			var active, maximum atomic.Int32
			pool := newMetadataWriterPool(t, streams, metadataWriterHooks{
				Close: func(ctx context.Context, stream types.StreamInterface, _ *destination.Options, metadata any) error {
					running := active.Add(1)
					defer active.Add(-1)
					for {
						old := maximum.Load()
						if running <= old || maximum.CompareAndSwap(old, running) {
							break
						}
					}
					entered <- struct{}{}
					select {
					case <-release:
					case <-ctx.Done():
						return ctx.Err()
					}
					observations <- observation{stream.ID(), metadata, ctx.Err()}
					return nil
				},
			})
			writers := cleanupTestWriters(t, ctx, pool, streams)
			writers["nil_writer"] = nil
			clearDedup := false
			result := make(chan error, 1)
			go func() {
				var err error
				func() {
					defer handleWriterCleanup(ctx, cancel, &err, writers, "", &metadata, &clearDedup)
				}()
				result <- err
			}()

			for range constants.CDCMetadataConcurrency {
				waitCleanupEvent(t, entered)
			}
			assert.Equal(t, int32(constants.CDCMetadataConcurrency), maximum.Load())
			select {
			case <-result:
				t.Fatal("cleanup returned while commits were blocked")
			default:
			}
			unblock()
			require.NoError(t, waitCleanupResult(t, result))
			assert.Equal(t, int32(constants.CDCMetadataConcurrency), maximum.Load())
			assert.NoError(t, ctx.Err(), "successful cleanup must not cancel its parent")
			require.Len(t, observations, len(streams))
			seen := make(map[string]bool, len(streams))
			for range streams {
				got := <-observations
				assert.False(t, seen[got.streamID], "writer closed twice")
				seen[got.streamID] = true
				assert.NoError(t, got.ctxErr)
				var expectedValue any = shared
				if perStream {
					expectedValue = byStream[got.streamID]
				}
				if expectedValue == nil {
					assert.Nil(t, got.metadata)
					continue
				}
				expected, err := types.SetMetadataState(expectedValue, "")
				require.NoError(t, err)
				expected.DedupInserts = &clearDedup
				assert.Equal(t, expected, got.metadata)
			}
			assert.Equal(t, &position{LSN: "0/200"}, shared)
			if perStream {
				for i, stream := range streams {
					if i%3 != 0 {
						assert.Equal(t, &position{LSN: fmt.Sprintf("0/%d", i)}, byStream[stream.ID()])
					}
				}
			}
		})
	}
}

type cleanupBarrierDriver struct {
	stubDriver
	metadata any
	postCDC  func(context.Context) error
}

func (d cleanupBarrierDriver) StreamChanges(context.Context, int, map[string]any, CDCMsgFn) (any, error) {
	return d.metadata, nil
}

func (d cleanupBarrierDriver) PostCDC(ctx context.Context, _ int) error {
	return d.postCDC(ctx)
}

func TestCDCCleanupCommitBarrier(t *testing.T) {
	for _, outcome := range []string{"success", "errors", "panic"} {
		t.Run(outcome, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			t.Cleanup(cancel)
			streams := cleanupTestStreams(constants.CDCMetadataConcurrency + 3)
			firstFailure := errors.New("first commit failed")
			secondFailure := errors.New("second commit failed")
			release := make(chan struct{})
			var releaseOnce sync.Once
			unblock := func() { releaseOnce.Do(func() { close(release) }) }
			t.Cleanup(unblock)
			entered := make(chan struct{}, len(streams))
			contextErrors := make(chan error, len(streams))
			pool := newMetadataWriterPool(t, streams, metadataWriterHooks{
				Close: func(ctx context.Context, stream types.StreamInterface, _ *destination.Options, _ any) error {
					contextErrors <- ctx.Err()
					entered <- struct{}{}
					if stream.ID() == streams[0].ID() {
						select {
						case <-release:
						case <-ctx.Done():
							return ctx.Err()
						}
					}
					if stream.ID() == streams[1].ID() {
						if outcome == "errors" {
							return firstFailure
						}
						if outcome == "panic" {
							panic("commit panic")
						}
					}
					if stream.ID() == streams[2].ID() && outcome == "errors" {
						return secondFailure
					}
					return nil
				},
			})
			postCalled := make(chan error, 1)
			var acknowledged atomic.Bool
			var metadata any = "0/300"
			driver := NewAbstractDriver(ctx, cleanupBarrierDriver{
				stubDriver: stubDriver{typ: "postgres", cdcSupported: true},
				metadata:   metadata,
				postCDC: func(ctx context.Context) error {
					postCalled <- ctx.Err()
					if ctx.Err() == nil {
						acknowledged.Store(true)
					}
					return nil
				},
			})
			result := make(chan error, 1)
			go func() { result <- driver.streamChanges(ctx, pool, 0, streams) }()
			for range streams {
				waitCleanupEvent(t, entered)
			}
			select {
			case <-postCalled:
				t.Fatal("PostCDC ran before every commit completed")
			default:
			}
			select {
			case <-result:
				t.Fatal("CDC returned before every commit completed")
			default:
			}
			assert.False(t, acknowledged.Load())
			unblock()
			err := waitCleanupResult(t, result)
			postErr := waitCleanupResult(t, postCalled)
			require.Len(t, contextErrors, len(streams))
			for range streams {
				assert.NoError(t, <-contextErrors, "a sibling failure must not cancel later commits")
			}
			if outcome == "success" {
				assert.NoError(t, err)
				assert.NoError(t, postErr)
				assert.True(t, acknowledged.Load())
				return
			}
			require.Error(t, err)
			assert.ErrorIs(t, postErr, context.Canceled)
			assert.False(t, acknowledged.Load())
			switch outcome {
			case "errors":
				assert.ErrorIs(t, err, firstFailure)
				assert.ErrorIs(t, err, secondFailure)
			case "panic":
				failure := errs.From(errs.Classify(err))
				assert.Equal(t, errs.InternalError, failure.Category)
				assert.Equal(t, codeWriterPanicRecovered, failure.Code)
				assert.ErrorContains(t, err, "panic recovered: commit panic")
			}
		})
	}
}

func TestHandleWriterCleanupPreservesPriorAndCloseErrors(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	streams := cleanupTestStreams(2)
	priorCause := errors.New("lost source position")
	prior := errs.Precondition(errs.CDCPositionLost, "postgres.lsn_lost", priorCause)
	closeErrors := []error{errors.New("first close failure"), errors.New("second close failure")}
	var closed atomic.Int32
	pool := newMetadataWriterPool(t, streams, metadataWriterHooks{
		Close: func(ctx context.Context, stream types.StreamInterface, _ *destination.Options, _ any) error {
			closed.Add(1)
			assert.ErrorIs(t, ctx.Err(), context.Canceled)
			if stream.ID() == streams[0].ID() {
				return closeErrors[0]
			}
			return closeErrors[1]
		},
	})
	writers := cleanupTestWriters(t, ctx, pool, streams)
	err := prior
	func() {
		defer handleWriterCleanup(ctx, cancel, &err, writers, "", nil, nil)
	}()
	assert.Equal(t, int32(len(streams)), closed.Load())
	assert.ErrorIs(t, err, priorCause)
	for _, closeErr := range closeErrors {
		assert.ErrorIs(t, err, closeErr)
	}
	assert.Equal(t, errs.CDCPositionLost, errs.From(errs.Classify(err)).Category)
}

func TestHandleWriterCleanupHonorsCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	streams := cleanupTestStreams(constants.CDCMetadataConcurrency + 3)
	entered := make(chan struct{}, len(streams))
	var closed atomic.Int32
	pool := newMetadataWriterPool(t, streams, metadataWriterHooks{
		Close: func(ctx context.Context, _ types.StreamInterface, _ *destination.Options, _ any) error {
			entered <- struct{}{}
			<-ctx.Done()
			closed.Add(1)
			return ctx.Err()
		},
	})
	writers := cleanupTestWriters(t, ctx, pool, streams)
	result := make(chan error, 1)
	go func() {
		var err error
		func() {
			defer handleWriterCleanup(ctx, cancel, &err, writers, "", nil, nil)
		}()
		result <- err
	}()
	for range constants.CDCMetadataConcurrency {
		waitCleanupEvent(t, entered)
	}
	cancel()
	assert.ErrorIs(t, waitCleanupResult(t, result), context.Canceled)
	assert.Equal(t, int32(len(streams)), closed.Load())
}

func TestHandleWriterCleanupSingleWriterMetadata(t *testing.T) {
	for _, incremental := range []bool{false, true} {
		t.Run(fmt.Sprintf("incremental_%t", incremental), func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			t.Cleanup(cancel)
			streams := cleanupTestStreams(1)
			var received any
			pool := newMetadataWriterPool(t, streams, metadataWriterHooks{
				Close: func(ctx context.Context, _ types.StreamInterface, _ *destination.Options, metadata any) error {
					assert.NoError(t, ctx.Err())
					received = metadata
					return nil
				},
			})
			writer, _, err := pool.NewWriter(ctx, streams[0], destination.WithThreadID("single_thread"))
			require.NoError(t, err)
			var metadata any
			var metadataPtr *any
			if incremental {
				metadata = map[string]any{"cursor": "2026-11-05", "tie_breaker": 7}
				metadataPtr = &metadata
			}
			func() {
				defer handleWriterCleanup(ctx, cancel, &err, writer, "single_thread", metadataPtr, nil)
			}()
			require.NoError(t, err)
			assert.NoError(t, ctx.Err())
			if incremental {
				expected, setErr := types.SetMetadataState(metadata, "single_thread")
				require.NoError(t, setErr)
				assert.Equal(t, expected, received)
				assert.Equal(t, map[string]any{"cursor": "2026-11-05", "tie_breaker": 7}, metadata)
			} else {
				assert.Nil(t, received)
			}
		})
	}
}

func TestCDCCleanupInvalidMetadataAbortsCommits(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	streams := metadataStreams(constants.CDCMetadataConcurrency + 3)
	var closed atomic.Int32
	var unsafeCommit, acknowledged atomic.Bool
	pool := newMetadataWriterPool(t, streams, metadataWriterHooks{
		Close: func(closeCtx context.Context, _ types.StreamInterface, _ *destination.Options, _ any) error {
			if closeCtx.Err() == nil {
				unsafeCommit.Store(true)
			}
			closed.Add(1)
			return closeCtx.Err()
		},
	})
	driver := metadataCDCDriver{
		stubDriver: stubDriver{typ: "postgres"},
		streamChanges: func(context.Context, int, map[string]any, CDCMsgFn) (any, error) {
			return func() {}, nil
		},
		postCDC: func(postCtx context.Context, _ int) error {
			if postCtx.Err() == nil {
				acknowledged.Store(true)
			}
			return postCtx.Err()
		},
	}
	err := NewAbstractDriver(ctx, driver).streamChanges(ctx, pool, 0, streams)
	require.Error(t, err)
	assert.ErrorIs(t, err, context.Canceled)
	assert.False(t, unsafeCommit.Load())
	assert.False(t, acknowledged.Load())
	assert.EqualValues(t, len(streams), closed.Load())
}
