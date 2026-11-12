package destination

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/pkg/indexdb"
	"github.com/datazip-inc/olake/types"
	"github.com/stretchr/testify/require"
)

type poolResyncAdapter struct {
	checkpoint *types.State
	baseline   *types.State
	published  bool
	prepared   int
	finished   int
	finishErr  error
}

func (a *poolResyncAdapter) PrepareResync(_ context.Context, _ types.StreamInterface, _ string, baseline *types.State) (*types.State, error) {
	a.baseline = baseline
	a.prepared++
	return a.checkpoint, nil
}

func (a *poolResyncAdapter) PublishResync(context.Context, *types.State) error {
	a.published = true
	return nil
}

func (a *poolResyncAdapter) AbortResync(context.Context) error { return nil }
func (a *poolResyncAdapter) ResyncPublished() bool             { return a.published }
func (a *poolResyncAdapter) ResyncReceipt() (string, int64)    { return "uuid", 42 }
func (a *poolResyncAdapter) FinishResync(context.Context) error {
	a.finished++
	return a.finishErr
}

func TestPoolPrepareRecoversBeforeAllocatingTargetIndex(t *testing.T) {
	stream := types.NewStream("events", "public", nil).Wrap(0)
	checkpoint := &types.State{RWMutex: &sync.RWMutex{}, Type: types.GlobalType}
	adapter := &poolResyncAdapter{checkpoint: checkpoint, published: true}
	pool := &WriterPool{resync: adapter}
	artifact := &writerSchema{needsIndex: true}
	pool.writerSchema.Store(stream.ID(), artifact)
	// Any attempted target index allocation would fail. Durable recovery must not
	// touch the canonical index or create an unnecessary new attempt directory.
	t.Setenv(constants.IndexDBDir, "/dev/null/invalid-index-parent")
	ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
	defer cancel()
	baseline := &types.State{RWMutex: &sync.RWMutex{}, Type: types.GlobalType}
	recovered, err := pool.PrepareResync(ctx, stream, "operation-1", baseline)
	require.NoError(t, err)
	require.Same(t, checkpoint, recovered)
	require.Nil(t, artifact.streamIndex)
	require.Equal(t, 1, adapter.prepared)
	require.Same(t, baseline, adapter.baseline)
	require.True(t, pool.ResyncPublished())
}

func TestPoolTargetAttemptDoesNotOpenCanonicalOrChangeOtherStream(t *testing.T) {
	t.Setenv(constants.IndexDBDir, t.TempDir())
	target := types.NewStream("events", "public", nil).Wrap(0)
	other := types.NewStream("other", "public", nil).Wrap(0)
	canonical, err := indexdb.Open(target)
	require.NoError(t, err)
	defer func() { require.NoError(t, canonical.Close()) }()
	pending := types.NewStreamIndexThread(canonical)
	pending.Put("old-row", types.RowLocation{FilePath: "old.parquet", Position: 7})
	snapshot := int64(11)
	require.NoError(t, canonical.Commit(pending, &snapshot))
	otherIndex, err := indexdb.Open(other)
	require.NoError(t, err)
	pool := &WriterPool{resync: &poolResyncAdapter{}}
	targetArtifact := &writerSchema{needsIndex: true}
	otherArtifact := &writerSchema{needsIndex: true, streamIndex: otherIndex, schema: "unchanged"}
	pool.writerSchema.Store(target.ID(), targetArtifact)
	pool.writerSchema.Store(other.ID(), otherArtifact)
	ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
	defer cancel()
	checkpoint, err := pool.PrepareResync(ctx, target, "operation-1", &types.State{Type: types.StreamType})
	require.NoError(t, err)
	require.Nil(t, checkpoint)
	require.NotNil(t, targetArtifact.streamIndex)
	_, found, err := targetArtifact.streamIndex.Lookup("old-row")
	require.NoError(t, err)
	require.False(t, found)
	_, found, err = canonical.Lookup("old-row")
	require.NoError(t, err)
	require.True(t, found)
	require.Same(t, otherIndex, otherArtifact.streamIndex)
	require.Equal(t, "unchanged", otherArtifact.schema)
	require.NoError(t, pool.FinishResync(ctx))
}

func TestPoolResyncRejectsUnsupportedAndStartedTarget(t *testing.T) {
	stream := types.NewStream("events", "public", nil).Wrap(0)
	ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
	defer cancel()
	pool := &WriterPool{}
	_, err := pool.PrepareResync(ctx, stream, "operation-1", &types.State{Type: types.StreamType})
	require.ErrorContains(t, err, "does not support")
	require.Error(t, pool.AbortResync(ctx))
	adapter := &poolResyncAdapter{}
	pool.resync = adapter
	pool.writerSchema.Store(stream.ID(), &writerSchema{schema: "already-open"})
	_, err = pool.PrepareResync(ctx, stream, "operation-1", &types.State{Type: types.StreamType})
	require.ErrorContains(t, err, "before opening")
	require.Zero(t, adapter.prepared)
}

func TestPoolPrepareRejectsMissingBaselineBeforeDestination(t *testing.T) {
	adapter := &poolResyncAdapter{}
	pool := &WriterPool{resync: adapter}
	stream := types.NewStream("events", "public", nil).Wrap(0)
	pool.writerSchema.Store(stream.ID(), &writerSchema{})
	ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
	defer cancel()
	_, err := pool.PrepareResync(ctx, stream, "operation-1", nil)
	require.ErrorContains(t, err, "full baseline checkpoint")
	require.Zero(t, adapter.prepared)
}

type closingResyncIndex struct {
	types.StreamIndex
	closed int
	err    error
}

func (i *closingResyncIndex) Close() error { i.closed++; return i.err }

func TestPoolFinishReportsCleanupErrorsBeforeSuccessAndIsShutdownSafe(t *testing.T) {
	adapter := &poolResyncAdapter{finishErr: errors.New("JVM shutdown failed")}
	index := &closingResyncIndex{err: errors.New("index close failed")}
	shutdowns := 0
	pool := &WriterPool{resync: adapter, shutdown: func(context.Context) { shutdowns++ }}
	pool.writerSchema.Store("target", &writerSchema{streamIndex: index})
	err := pool.FinishResync(context.Background())
	require.ErrorContains(t, err, "JVM shutdown failed")
	require.ErrorContains(t, err, "index close failed")
	pool.Shutdown(context.Background())
	require.Equal(t, 1, index.closed)
	require.Equal(t, 1, adapter.finished)
	require.Equal(t, 1, shutdowns)
	uuid, snapshot := pool.ResyncReceipt()
	require.Equal(t, "uuid", uuid)
	require.Equal(t, int64(42), snapshot)
}

type resyncContextWriter struct {
	Writer
}

func (*resyncContextWriter) Check(context.Context) error { return nil }

func (*resyncContextWriter) Setup(context.Context, types.StreamInterface, any, *Options) (any, *types.MetadataState, error) {
	return nil, nil, nil
}

func TestWriterFactoriesReceiveResyncContextBeforeSharedStartup(t *testing.T) {
	writerType := types.DestinationType("resync-context-test")
	previous, existed := RegisteredWriters[writerType]
	options := &types.ResyncOptions{Namespace: "public", Table: "events", ID: "operation-1"}
	var contexts []context.Context
	RegisteredWriters[writerType] = func(ctx context.Context, _ any) (Writer, func(context.Context), error) {
		contexts = append(contexts, ctx)
		actual, ok := types.ResyncFromContext(ctx)
		require.True(t, ok)
		require.Same(t, options, actual)
		return &resyncContextWriter{}, nil, nil
	}
	t.Cleanup(func() {
		if existed {
			RegisteredWriters[writerType] = previous
		} else {
			delete(RegisteredWriters, writerType)
		}
	})
	ctx := types.WithResync(context.Background(), options)
	stream := types.NewStream("events", "public", nil).Wrap(0)
	pool, err := NewWriterPool(ctx, &types.WriterConfig{Type: writerType}, []types.StreamInterface{stream}, 1)
	require.NoError(t, err)
	t.Cleanup(func() { pool.Shutdown(context.Background()) })
	_, _, err = pool.NewWriter(ctx, stream)
	require.NoError(t, err)
	require.Len(t, contexts, 2)
}
