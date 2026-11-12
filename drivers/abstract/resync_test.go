package abstract

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/datazip-inc/olake/destination"
	"github.com/datazip-inc/olake/types"
	"github.com/stretchr/testify/require"
)

type resyncTestDriver struct {
	stubDriver
	checkpoint func(*types.State) error
	post       func() error
	chunks     func(types.StreamInterface) (*types.Set[types.Chunk], error)
}

func (*resyncTestDriver) ValidateResync(context.Context, types.StreamInterface) error { return nil }
func (*resyncTestDriver) PrepareResync(context.Context, types.StreamInterface) error  { return nil }
func (*resyncTestDriver) RecoverResync(context.Context, *types.State, types.StreamInterface) error {
	return nil
}
func (*resyncTestDriver) CleanupResync(context.Context) error { return nil }
func (d *resyncTestDriver) ResyncCheckpoint(_ context.Context, state *types.State) error {
	return d.checkpoint(state)
}
func (*resyncTestDriver) ChangeStreamConfig() (bool, bool, bool) { return true, false, false }
func (*resyncTestDriver) StreamChanges(context.Context, int, map[string]any, CDCMsgFn) (any, error) {
	return map[string]any{"lsn": "0/300"}, nil
}
func (d *resyncTestDriver) PostCDC(context.Context, int) error { return d.post() }
func (d *resyncTestDriver) GetOrSplitChunks(_ context.Context, _ *destination.WriterPool, stream types.StreamInterface) (*types.Set[types.Chunk], error) {
	if d.chunks != nil {
		return d.chunks(stream)
	}
	return types.NewSet[types.Chunk](), nil
}

func (*resyncTestDriver) FetchMaxCursorValues(context.Context, types.StreamInterface) (any, any, error) {
	return int64(10), nil, nil
}

func (*resyncTestDriver) ChunkIterator(ctx context.Context, _ types.StreamInterface, _ types.Chunk, process BackfillMsgFn) error {
	return process(ctx, map[string]any{"offset": int64(10)}, 8)
}

func TestResyncGlobalPublicationPrecedesAck(t *testing.T) {
	for _, outcome := range []string{"full_refresh", "incremental", "target_failure", "unrelated_failure", "publish_failure", "ack_failure", "cancellation"} {
		t.Run(outcome, func(t *testing.T) {
			base, cancel := context.WithCancel(context.Background())
			defer cancel()
			ctx := types.WithResync(base, &types.ResyncOptions{Namespace: "public", Table: "items", ID: "operation"})
			streams := cleanupTestStreams(2)
			state := &types.State{RWMutex: &sync.RWMutex{}, Type: types.GlobalType, SuppressPersistence: true}
			state.SetGlobal(map[string]any{"lsn": "0/100"}, streams[0].ID(), streams[1].ID())
			state.SetCursor(streams[1].Self(), "offset", int64(42))
			var closed atomic.Int32
			pool := newMetadataWriterPool(t, streams, metadataWriterHooks{Close: func(context.Context, types.StreamInterface, *destination.Options, any) error {
				closed.Add(1)
				return nil
			}})
			var published, acknowledged atomic.Bool
			driver := &resyncTestDriver{stubDriver: stubDriver{typ: "postgres", cdcSupported: true}}
			driver.checkpoint = func(checkpoint *types.State) error {
				checkpoint.Global.State = map[string]any{"lsn": "0/300"}
				return nil
			}
			driver.post = func() error {
				if !published.Load() {
					return errors.New("acknowledgement preceded publication")
				}
				if outcome == "ack_failure" {
					return errors.New("source acknowledgement failed after publication")
				}
				acknowledged.Store(true)
				return nil
			}
			a := NewAbstractDriver(ctx, driver)
			a.SetupState(state)
			require.NoError(t, a.streamChanges(ctx, pool, 0, streams))
			require.Equal(t, int32(len(streams)), closed.Load())
			require.False(t, acknowledged.Load(), "CDC phase completion must not acknowledge resync")

			targetDone, unrelatedDone := make(chan struct{}), make(chan struct{})
			failure := errors.New("later finalization failed")
			a.GlobalCtxGroup.Add(func(ctx context.Context) error {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-targetDone:
				}
				if outcome == "target_failure" {
					return failure
				}
				return nil
			})
			a.GlobalConnGroup.Add(func(ctx context.Context) error {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-unrelatedDone:
				}
				if outcome == "unrelated_failure" {
					return failure
				}
				return nil
			})
			result := make(chan error, 1)
			go func() {
				result <- a.completeRead(ctx, func(_ context.Context, checkpoint *types.State) error {
					if closed.Load() != int32(len(streams)) {
						return errors.New("publication preceded writer closes")
					}
					if checkpoint.SuppressPersistence {
						return errors.New("durable checkpoint is suppressed")
					}
					if checkpoint.Global.State.(map[string]any)["lsn"] != "0/300" {
						return errors.New("checkpoint lost pending LSN")
					}
					if checkpoint.GetCursor(streams[1].Self(), "offset") == nil {
						return errors.New("checkpoint lost unrelated stream")
					}
					if outcome == "publish_failure" {
						return failure
					}
					published.Store(true)
					return nil
				})
			}()
			require.False(t, published.Load())
			close(targetDone)
			require.False(t, published.Load(), "unrelated finalization has not joined")
			if outcome == "cancellation" {
				cancel()
			}
			close(unrelatedDone)
			err := waitCleanupResult(t, result)
			if outcome == "full_refresh" || outcome == "incremental" {
				require.NoError(t, err)
				require.True(t, published.Load())
				require.True(t, acknowledged.Load())
			} else {
				require.Error(t, err)
				require.Equal(t, outcome == "ack_failure", published.Load())
				require.False(t, acknowledged.Load())
			}
			require.True(t, state.SuppressPersistence, "CLI alone may emit final state")
		})
	}
}

func TestResyncReadLaterBackfillFailureNeverAcknowledges(t *testing.T) {
	ctx := types.WithResync(context.Background(), &types.ResyncOptions{Namespace: "public", Table: "target", ID: "operation"})
	target := types.NewStream("target", "public", nil).Wrap(0)
	other := types.NewStream("other", "public", nil).Wrap(0)
	state := &types.State{RWMutex: &sync.RWMutex{}, Type: types.GlobalType, SuppressPersistence: true}
	state.SetGlobal(map[string]any{"lsn": "0/100"}, other.ID())
	var acknowledgements atomic.Int32
	closeEntered, releaseClose := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(releaseClose) }) }
	t.Cleanup(unblock)
	driver := &resyncTestDriver{stubDriver: stubDriver{typ: "postgres", cdcSupported: true}, post: func() error {
		acknowledgements.Add(1)
		return nil
	}, chunks: func(types.StreamInterface) (*types.Set[types.Chunk], error) {
		<-closeEntered
		return nil, errors.New("target planning failed after CDC started")
	}}
	pool := newMetadataWriterPool(t, []types.StreamInterface{target, other}, metadataWriterHooks{Close: func(ctx context.Context, _ types.StreamInterface, _ *destination.Options, _ any) error {
		close(closeEntered)
		<-releaseClose
		return ctx.Err()
	}})
	a := NewAbstractDriver(ctx, driver)
	a.SetupState(state)
	result := make(chan error, 1)
	go func() {
		result <- a.Read(ctx, pool, []types.StreamInterface{target}, []types.StreamInterface{other}, nil)
	}()
	waitCleanupEvent(t, closeEntered)
	select {
	case <-result:
		t.Fatal("failed Read returned while a CDC writer was still closing")
	default:
	}
	unblock()
	require.Error(t, waitCleanupResult(t, result))
	require.Zero(t, acknowledgements.Load())
}

func TestResyncClearsOnlyTargetInMemory(t *testing.T) {
	target := types.NewStream("target", "public", nil).Wrap(0)
	other := types.NewStream("other", "public", nil).Wrap(0)
	state := &types.State{RWMutex: &sync.RWMutex{}, Type: types.GlobalType, SuppressPersistence: true}
	state.SetGlobal(map[string]any{"lsn": "0/100"}, target.ID(), other.ID())
	state.SetCursor(target, "offset", int64(1))
	state.SetCursor(other, "offset", int64(99))
	a := NewAbstractDriver(context.Background(), stubDriver{})
	a.SetupState(state)
	_, err := a.ClearState([]types.StreamInterface{target})
	require.NoError(t, err)
	require.Nil(t, state.GetCursor(target, "offset"))
	require.Equal(t, int64(99), state.GetCursor(other, "offset"))
	require.False(t, state.HasCompletedBackfill(target))
	require.True(t, state.HasCompletedBackfill(other))
	require.Equal(t, "0/100", state.GetGlobal().State.(map[string]any)["lsn"])
}

func TestAtomicResyncUnsupportedSourceFailsClosed(t *testing.T) {
	a := NewAbstractDriver(context.Background(), stubDriver{typ: "unsupported"})
	target := types.NewStream("target", "public", nil).Wrap(0)
	require.ErrorContains(t, a.ValidateResync(context.Background(), target), "not supported")
}

type resyncPublicationWriter struct {
	metadataTestWriter
	publish func(*types.State) error
}

func (*resyncPublicationWriter) PrepareResync(context.Context, types.StreamInterface, string, *types.State) (*types.State, error) {
	return nil, nil
}
func (w *resyncPublicationWriter) PublishResync(_ context.Context, checkpoint *types.State) error {
	return w.publish(checkpoint)
}
func (*resyncPublicationWriter) AbortResync(context.Context) error { return nil }
func (*resyncPublicationWriter) ResyncPublished() bool             { return true }
func (*resyncPublicationWriter) ResyncReceipt() (string, int64)    { return "test-table-uuid", 1 }

func TestResyncReadFinalizesFullAndIncrementalTargets(t *testing.T) {
	for _, mode := range []types.SyncMode{types.FULLREFRESH, types.INCREMENTAL} {
		t.Run(string(mode), func(t *testing.T) {
			ctx := types.WithResync(context.Background(), &types.ResyncOptions{Namespace: "public", Table: "target", ID: "operation"})
			target := types.NewStream("target", "public", nil).Wrap(0)
			target.Stream.SyncMode = mode
			target.Stream.CursorField = "offset"
			target.Stream.UpsertField("offset", types.Int64, false, false)
			other := types.NewStream("other", "public", nil).Wrap(0)
			other.Stream.SyncMode = types.CDC
			streams := []types.StreamInterface{target, other}
			state := &types.State{RWMutex: &sync.RWMutex{}, Type: types.GlobalType, SuppressPersistence: true}
			state.SetGlobal(map[string]any{"lsn": "0/100"}, target.ID(), other.ID())
			state.SetCursor(other, "offset", int64(99))
			var published atomic.Bool
			var acknowledgements, closes atomic.Int32
			driver := &resyncTestDriver{stubDriver: stubDriver{typ: "postgres", cdcSupported: true}}
			driver.chunks = func(types.StreamInterface) (*types.Set[types.Chunk], error) {
				return types.NewSet(types.Chunk{Min: int64(0), Max: nil}), nil
			}
			driver.checkpoint = func(checkpoint *types.State) error {
				checkpoint.Global.State = map[string]any{"lsn": "0/300"}
				return nil
			}
			driver.post = func() error {
				if !published.Load() {
					return errors.New("CDC acknowledgement preceded target publication")
				}
				acknowledgements.Add(1)
				return nil
			}
			writerType := types.DestinationType("resync-read-" + string(mode))
			destination.RegisteredWriters[writerType] = func(context.Context, any) (destination.Writer, func(context.Context), error) {
				writer := &resyncPublicationWriter{metadataTestWriter: metadataTestWriter{hooks: metadataWriterHooks{Close: func(context.Context, types.StreamInterface, *destination.Options, any) error {
					closes.Add(1)
					return nil
				}}}}
				writer.publish = func(checkpoint *types.State) error {
					minimumCloses := int32(2)
					if mode == types.INCREMENTAL {
						minimumCloses++
						if !checkpoint.HasCompletedBackfill(target) {
							return errors.New("incremental target checkpoint was not finalized")
						}
					}
					if closes.Load() != minimumCloses {
						return errors.New("publication did not follow every target and CDC writer close")
					}
					if checkpoint.GetCursor(other, "offset") == nil {
						return errors.New("unrelated checkpoint was lost")
					}
					published.Store(true)
					return nil
				}
				return writer, nil, nil
			}
			t.Cleanup(func() { delete(destination.RegisteredWriters, writerType) })
			pool, err := destination.NewWriterPool(ctx, &types.WriterConfig{Type: writerType}, streams, 1)
			require.NoError(t, err)
			t.Cleanup(func() { pool.Shutdown(context.Background()) })
			a := NewAbstractDriver(ctx, driver)
			a.SetupState(state)
			_, err = a.ClearState([]types.StreamInterface{target})
			require.NoError(t, err)
			var full, incremental []types.StreamInterface
			if mode == types.FULLREFRESH {
				full = []types.StreamInterface{target}
			} else {
				incremental = []types.StreamInterface{target}
			}
			require.NoError(t, a.Read(ctx, pool, full, []types.StreamInterface{other}, incremental))
			require.True(t, published.Load())
			require.Equal(t, int32(1), acknowledgements.Load())
			require.True(t, state.SuppressPersistence)
		})
	}
}
