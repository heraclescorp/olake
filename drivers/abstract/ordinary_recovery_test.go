package abstract

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/datazip-inc/olake/destination"
	"github.com/datazip-inc/olake/types"
	"github.com/stretchr/testify/require"
)

type ordinaryRecoveryTestDriver struct {
	stubDriver
	post       atomic.Int32
	ack        atomic.Int32
	backfill   atomic.Int32
	cleanup    atomic.Int32
	boundaries []string
	reads      int
}

func (*ordinaryRecoveryTestDriver) ValidateOrdinaryRecovery(context.Context, []types.StreamInterface, string) (string, error) {
	return "validated-live-identity", nil
}
func (*ordinaryRecoveryTestDriver) PrepareOrdinaryRecovery(_ context.Context, _ []types.StreamInterface, lower string, positions map[string]string) (*types.OrdinaryRecoveryPlan, error) {
	return &types.OrdinaryRecoveryPlan{LowerLSN: lower, TargetLSN: "0/500", ReplayAfter: positions}, nil
}
func (d *ordinaryRecoveryTestDriver) AcknowledgeOrdinaryRecovery(context.Context, *types.OrdinaryRecoveryPlan) error {
	d.ack.Add(1)
	return nil
}
func (d *ordinaryRecoveryTestDriver) CleanupOrdinaryRecovery(context.Context) error {
	d.cleanup.Add(1)
	return nil
}
func (d *ordinaryRecoveryTestDriver) StreamChanges(ctx context.Context, _ int, _ map[string]any, _ CDCMsgFn) (any, error) {
	boundary := "0/500"
	if d.reads < len(d.boundaries) {
		boundary = d.boundaries[d.reads]
	}
	d.reads++
	if options, ok := types.OrdinaryRecoveryFromContext(ctx); ok && options.Plan != nil {
		options.Plan.BatchTargetLSN = boundary
	}
	return struct {
		LSN string `json:"lsn"`
	}{LSN: boundary}, nil
}
func (d *ordinaryRecoveryTestDriver) PostCDC(context.Context, int) error {
	d.post.Add(1)
	return errors.New("ordinary recovery must not call PostCDC")
}
func (d *ordinaryRecoveryTestDriver) GetOrSplitChunks(context.Context, *destination.WriterPool, types.StreamInterface) (*types.Set[types.Chunk], error) {
	d.backfill.Add(1)
	return nil, errors.New("ordinary recovery must not plan backfill")
}

func TestOrdinaryRecoveryReadJoinsEveryWriterWithoutAckOrBackfill(t *testing.T) {
	for _, failClose := range []bool{false, true} {
		t.Run(map[bool]string{false: "complete", true: "partial_commit"}[failClose], func(t *testing.T) {
			streams := cleanupTestStreams(3)
			for _, stream := range streams {
				stream.GetStream().SyncMode = types.CDC
			}
			plan := &types.OrdinaryRecoveryPlan{LowerLSN: "0/100", TargetLSN: "0/500", ReplayAfter: map[string]string{}}
			ctx := types.WithOrdinaryRecovery(context.Background(), &types.OrdinaryRecoveryOptions{SourceIdentity: "validated-live-identity", Plan: plan})
			state := &types.State{RWMutex: &sync.RWMutex{}, Type: types.GlobalType, SuppressPersistence: true,
				Global: &types.GlobalState{State: map[string]any{"lsn": "0/100", "opaque": "preserved"}, Streams: types.NewSet(streams[0].ID(), streams[1].ID(), streams[2].ID())}}
			driver := &ordinaryRecoveryTestDriver{stubDriver: stubDriver{typ: "postgres", cdcSupported: true}}
			entered := make(chan struct{}, len(streams))
			release := make(chan struct{})
			var once sync.Once
			unblock := func() { once.Do(func() { close(release) }) }
			t.Cleanup(unblock)
			var closed atomic.Int32
			pool := newOrdinaryRecoveryWriterPool(t, ctx, streams, metadataWriterHooks{Close: func(_ context.Context, stream types.StreamInterface, _ *destination.Options, metadata any) error {
				entered <- struct{}{}
				<-release
				closed.Add(1)
				if failClose && stream.ID() == streams[0].ID() {
					return errors.New("one table commit failed")
				}
				checkpoint, ok := metadata.(*types.MetadataState)
				require.True(t, ok, "zero-row tables must still checkpoint the common target")
				require.Equal(t, `{"lsn":"0/500"}`, checkpoint.State)
				return nil
			}})
			for _, stream := range streams {
				plan.ReplayAfter[stream.ID()] = "0/100"
			}
			_, prepareErr := pool.PrepareOrdinaryRecovery(ctx, streams, plan.LowerLSN)
			require.NoError(t, prepareErr)
			a := NewAbstractDriver(ctx, driver)
			a.SetupState(state)
			result := make(chan error, 1)
			go func() { result <- a.Read(ctx, pool, nil, streams, nil) }()
			for range streams {
				waitCleanupEvent(t, entered)
			}
			select {
			case <-result:
				t.Fatal("Read returned before all writer closes joined")
			default:
			}
			unblock()
			err := waitCleanupResult(t, result)
			if failClose {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, int32(len(streams)), closed.Load())
			require.Zero(t, driver.post.Load())
			require.Zero(t, driver.ack.Load(), "protocol confirmation alone authorizes explicit ACK")
			require.Zero(t, driver.backfill.Load())
			require.Equal(t, "0/100", state.Global.State.(map[string]any)["lsn"])
			require.Equal(t, len(streams), state.Global.Streams.Len())
			require.NoError(t, a.CleanupOrdinaryRecovery(ctx))
			require.Equal(t, int32(1), driver.cleanup.Load())
		})
	}
}

func TestOrdinaryRecoveryReadReopensFreshPoolsAcrossCompletedBatches(t *testing.T) {
	streams := cleanupTestStreams(3)
	for _, stream := range streams {
		stream.GetStream().SyncMode = types.CDC
	}
	options := &types.OrdinaryRecoveryOptions{SourceIdentity: "validated-live-identity"}
	ctx := types.WithOrdinaryRecovery(context.Background(), options)
	state := &types.State{RWMutex: &sync.RWMutex{}, Type: types.GlobalType, SuppressPersistence: true,
		Global: &types.GlobalState{State: map[string]any{"lsn": "0/100", "opaque": "preserved"}, Streams: types.NewSet(streams[0].ID(), streams[1].ID(), streams[2].ID())}}
	driver := &ordinaryRecoveryTestDriver{stubDriver: stubDriver{typ: "postgres", cdcSupported: true}, boundaries: []string{"0/300", "0/500"}}
	a := NewAbstractDriver(ctx, driver)
	a.SetupState(state)
	previous := "0/100"
	for index, boundary := range driver.boundaries {
		options.Plan = &types.OrdinaryRecoveryPlan{LowerLSN: "0/100", TargetLSN: "0/500", ReplayAfter: make(map[string]string)}
		for _, stream := range streams {
			options.Plan.ReplayAfter[stream.ID()] = previous
		}
		var closed atomic.Int32
		pool := newOrdinaryRecoveryWriterPool(t, ctx, streams, metadataWriterHooks{Close: func(_ context.Context, _ types.StreamInterface, _ *destination.Options, metadata any) error {
			checkpoint, ok := metadata.(*types.MetadataState)
			require.True(t, ok)
			require.Equal(t, fmt.Sprintf(`{"lsn":%q}`, boundary), checkpoint.State)
			closed.Add(1)
			return nil
		}})
		_, err := pool.PrepareOrdinaryRecovery(ctx, streams, options.Plan.LowerLSN)
		require.NoError(t, err)
		require.NoError(t, a.Read(ctx, pool, nil, streams, nil))
		require.Equal(t, int32(len(streams)), closed.Load())
		require.Equal(t, "0/500", options.Plan.TargetLSN)
		require.Equal(t, boundary, options.Plan.BatchTargetLSN)
		require.Equal(t, previous, state.Global.State.(map[string]any)["lsn"])
		require.Equal(t, int32(index), driver.ack.Load())
		require.NoError(t, a.AcknowledgeOrdinaryRecovery(ctx, options.Plan))
		require.NoError(t, a.CleanupOrdinaryRecovery(ctx))
		previous = boundary
		options.CheckpointLSN = boundary
		state.SetGlobal(map[string]any{"lsn": boundary, "opaque": "preserved"})
	}
	require.Equal(t, int32(2), driver.cleanup.Load())
	require.Equal(t, int32(2), driver.ack.Load())
	require.Zero(t, driver.post.Load())
	require.Zero(t, driver.backfill.Load())
}

func TestOrdinaryRecoveryRejectsFreshOrResyncReads(t *testing.T) {
	stream := types.NewStream("items", "public", nil).Wrap(0)
	a := NewAbstractDriver(context.Background(), &ordinaryRecoveryTestDriver{})
	ctx := types.WithOrdinaryRecovery(context.Background(), &types.OrdinaryRecoveryOptions{})
	require.ErrorContains(t, a.Read(ctx, nil, []types.StreamInterface{stream}, nil, nil), "frozen")
	require.ErrorContains(t, a.Read(ctx, nil, nil, nil, []types.StreamInterface{stream}), "frozen")
	resync := types.WithResync(ctx, &types.ResyncOptions{})
	require.ErrorContains(t, a.Read(resync, nil, nil, []types.StreamInterface{stream}, nil), "frozen")
}

func TestOrdinaryRecoveryWrappersRejectUnsupportedSources(t *testing.T) {
	a := NewAbstractDriver(context.Background(), stubDriver{typ: "unsupported"})
	_, err := a.ValidateOrdinaryRecovery(context.Background(), nil, "0/100")
	require.ErrorContains(t, err, "not supported")
	_, err = a.PrepareOrdinaryRecovery(context.Background(), nil, "0/100", nil)
	require.ErrorContains(t, err, "not supported")
	require.ErrorContains(t, a.AcknowledgeOrdinaryRecovery(context.Background(), nil), "not supported")
	require.ErrorContains(t, a.CleanupOrdinaryRecovery(context.Background()), "not supported")
}

type ordinaryRecoveryTestWriter struct{ metadataTestWriter }

func (*ordinaryRecoveryTestWriter) PrepareOrdinaryRecovery(ctx context.Context, streams []types.StreamInterface, lower string) ([]types.OrdinaryRecoveryTableProof, error) {
	if options, ok := types.OrdinaryRecoveryFromContext(ctx); ok && options.CheckpointLSN != "" {
		lower = options.CheckpointLSN
	}
	proofs := make([]types.OrdinaryRecoveryTableProof, len(streams))
	for index, stream := range streams {
		proofs[index] = types.OrdinaryRecoveryTableProof{Stream: stream.ID(), TableUUID: stream.ID(), FenceSnapshotID: 1, CommittedLSN: lower}
	}
	return proofs, nil
}

func (*ordinaryRecoveryTestWriter) ConfirmOrdinaryRecovery(_ context.Context, _ []types.StreamInterface, proofs []types.OrdinaryRecoveryTableProof, _ string) ([]types.OrdinaryRecoveryTableProof, error) {
	return proofs, nil
}

func newOrdinaryRecoveryWriterPool(t *testing.T, ctx context.Context, streams []types.StreamInterface, hooks metadataWriterHooks) *destination.WriterPool {
	t.Helper()
	writerType := types.DestinationType(fmt.Sprintf("ordinary-recovery-test-%d", metadataWriterPoolID.Add(1)))
	destination.RegisteredWriters[writerType] = func(context.Context, any) (destination.Writer, func(context.Context), error) {
		return &ordinaryRecoveryTestWriter{metadataTestWriter{hooks: hooks}}, nil, nil
	}
	t.Cleanup(func() { delete(destination.RegisteredWriters, writerType) })
	pool, err := destination.NewWriterPool(ctx, &types.WriterConfig{Type: writerType}, streams, 1)
	require.NoError(t, err)
	t.Cleanup(func() { pool.Shutdown(context.Background()) })
	return pool
}
