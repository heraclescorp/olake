package abstract

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"testing"

	"github.com/datazip-inc/olake/destination"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
	"github.com/datazip-inc/olake/utils/typeutils"
	"github.com/stretchr/testify/require"
)

// The source selects real rows by the supplied bounds, independently of the plan
// and destination evidence. A resumed range cannot pass by echoing its chunk ID.
type backfillResumeSource struct {
	stubDriver
	rows       int
	chunks     []types.Chunk
	emitted    []int
	afterRead  error
	beforeRead func() error
}

func (*backfillResumeSource) GetOrSplitChunks(context.Context, *destination.WriterPool, types.StreamInterface) (*types.Set[types.Chunk], error) {
	return nil, errors.New("a resume must retain the full saved plan")
}

func (s *backfillResumeSource) ChunkIterator(ctx context.Context, _ types.StreamInterface, chunk types.Chunk, emit BackfillMsgFn) error {
	s.chunks = append(s.chunks, chunk)
	if s.beforeRead != nil {
		if err := s.beforeRead(); err != nil {
			return err
		}
	}
	for id := range s.rows {
		if chunk.Min != nil && typeutils.Compare(id, chunk.Min) < 0 {
			continue
		}
		if chunk.Max != nil && typeutils.Compare(id, chunk.Max) >= 0 {
			continue
		}
		s.emitted = append(s.emitted, id)
		if err := emit(ctx, map[string]any{"id": id, "value": fmt.Sprintf("row-%d", id)}, 1); err != nil {
			return err
		}
	}
	return s.afterRead
}

type backfillResumeFixture struct {
	stream       types.StreamInterface
	plan         *types.Set[types.Chunk]
	ordered      []types.Chunk
	binding      types.BackfillTableBinding
	state        *types.State
	mu           sync.Mutex
	published    map[string][]types.RawRecord
	staged       map[string][]types.RawRecord
	commits      []string
	rejectOld    bool
	rejectAll    bool
	snapshotErr  error
	readSnapshot func(context.Context) error
}

func newBackfillResumeFixture(t *testing.T, count, committed int) *backfillResumeFixture {
	t.Helper()
	stream := types.NewStream("resume_items", "public", nil).Wrap(0)
	stream.GetStream().SyncMode = types.CDC
	stream.GetStream().UpsertField("id", types.Int64, false, false)
	stream.GetStream().UpsertField("value", types.String, false, false)
	stream.GetStream().SourceDefinedPrimaryKey.Insert("id")
	stream.StreamMetadata.ChunkColumn = "id"
	f := &backfillResumeFixture{
		stream:    stream,
		binding:   types.BackfillTableBinding{TableUUID: "resume-table", ShapeSHA256: "resume-shape"},
		published: make(map[string][]types.RawRecord),
		staged:    make(map[string][]types.RawRecord),
		state: &types.State{RWMutex: &sync.RWMutex{}, Type: types.GlobalType, SuppressPersistence: true, BackfillCheckpoint: true,
			Global: &types.GlobalState{State: map[string]any{"lsn": "0/100"}, Streams: types.NewSet[string]()}},
	}
	for index := range count {
		chunk := types.Chunk{Min: index, Max: index + 1}
		if index == 0 {
			chunk.Min = nil
		}
		if index == count-1 {
			chunk.Max = nil
		}
		f.ordered = append(f.ordered, chunk)
		if index < committed {
			f.published[backfillResumeChunkID(stream, chunk)] = []types.RawRecord{{Data: map[string]any{"id": index, "value": fmt.Sprintf("row-%d", index)}}}
		}
	}
	f.plan = types.NewSet(f.ordered...)
	require.NoError(t, f.state.SetBackfillPlan(stream.Self(), f.plan))
	f.state.SetBackfillTable(stream.Self(), &f.binding)
	return f
}

func backfillResumeChunkID(stream types.StreamInterface, chunk types.Chunk) string {
	return generateThreadID(stream.ID(), fmt.Sprintf("min[%v]-max[%v]", chunk.Min, chunk.Max))
}

func backfillResumeIDs(stream types.StreamInterface, plan *types.Set[types.Chunk]) []string {
	if plan == nil {
		return nil
	}
	ids := make([]string, 0, plan.Len())
	for _, chunk := range plan.Array() {
		ids = append(ids, backfillResumeChunkID(stream, chunk))
	}
	slices.Sort(ids)
	return ids
}

func backfillResumeContext(t *testing.T) (context.Context, string) {
	t.Helper()
	fence := filepath.Join(t.TempDir(), "execution-fence.json")
	require.NoError(t, os.WriteFile(fence, []byte(`{"version":1,"execution_id":"resume","state":"open"}`), 0600))
	ctx := utils.WithExecution(context.Background(), &utils.ExecutionOptions{ID: "resume", LaunchID: "resume-launch", Fence: fence, Command: "sync"})
	return types.WithBackfillCheckpoint(ctx, &types.BackfillRecoveryCheckpoint{Version: 1, ExecutionID: "resume", LaunchID: "resume-launch"}), fence
}

func (f *backfillResumeFixture) newPool(t *testing.T, ctx context.Context, snapshotSupported bool) *destination.WriterPool {
	t.Helper()
	writerType := types.DestinationType(fmt.Sprintf("backfill-resume-test-%d", metadataWriterPoolID.Add(1)))
	destination.RegisteredWriters[writerType] = func(context.Context, any) (destination.Writer, func(context.Context), error) {
		writer := backfillResumeWriter{ordinaryRecoveryTestWriter: ordinaryRecoveryTestWriter{metadataTestWriter{hooks: metadataWriterHooks{
			Setup: func(_ context.Context, _ types.StreamInterface, opts *destination.Options) (*types.MetadataState, error) {
				f.mu.Lock()
				defer f.mu.Unlock()
				_, old := f.published[opts.ThreadID]
				if f.rejectAll || (old && f.rejectOld) {
					return nil, fmt.Errorf("forbidden writer open for committed chunk %s", opts.ThreadID)
				}
				f.staged[opts.ThreadID] = nil
				ids := make([]string, 0, len(f.published))
				for id := range f.published {
					ids = append(ids, id)
				}
				return &types.MetadataState{FullRefreshCommittedIDs: ids}, nil
			},
			Write: func(_ context.Context, _ types.StreamInterface, opts *destination.Options, records []types.RawRecord) error {
				f.mu.Lock()
				defer f.mu.Unlock()
				f.staged[opts.ThreadID] = append(f.staged[opts.ThreadID], records...)
				return nil
			},
			Close: func(ctx context.Context, _ types.StreamInterface, opts *destination.Options, _ any) error {
				f.mu.Lock()
				defer f.mu.Unlock()
				if ctx.Err() != nil {
					delete(f.staged, opts.ThreadID)
					return nil
				}
				if records := f.staged[opts.ThreadID]; len(records) > 0 {
					f.published[opts.ThreadID] = records
				} else if _, exists := f.published[opts.ThreadID]; !exists {
					f.published[opts.ThreadID] = nil
				}
				f.commits = append(f.commits, opts.ThreadID)
				delete(f.staged, opts.ThreadID)
				return nil
			},
		}}}, fixture: f}
		if snapshotSupported {
			return &backfillResumeSnapshotWriter{writer}, nil, nil
		}
		return &writer, nil, nil
	}
	t.Cleanup(func() { delete(destination.RegisteredWriters, writerType) })
	pool, err := destination.NewWriterPool(ctx, &types.WriterConfig{Type: writerType}, []types.StreamInterface{f.stream}, 100)
	require.NoError(t, err)
	t.Cleanup(func() { pool.Shutdown(context.Background()) })
	return pool
}

type backfillResumeWriter struct {
	ordinaryRecoveryTestWriter
	fixture *backfillResumeFixture
}

func (w *backfillResumeWriter) PrepareBackfillTable(context.Context, types.StreamInterface) (*types.BackfillTableBinding, error) {
	binding := w.fixture.binding
	return &binding, nil
}

func (*backfillResumeWriter) PrepareBackfillRecovery(context.Context, []types.StreamInterface, string) error {
	return nil
}

func (w *backfillResumeWriter) GetBackfillPlan(context.Context, types.StreamInterface) (*types.Set[types.Chunk], error) {
	return types.NewSet(w.fixture.ordered...), nil
}

func (w *backfillResumeWriter) PrepareBackfillPlan(_ context.Context, stream types.StreamInterface, plan *types.Set[types.Chunk]) error {
	if !slices.Equal(backfillResumeIDs(stream, w.fixture.plan), backfillResumeIDs(stream, plan)) {
		return errors.New("resume prepared a shrunken plan")
	}
	return nil
}

func (*backfillResumeWriter) BackfillRecoveryTarget(context.Context) (string, error) {
	return "0/200", nil
}

type backfillResumeSnapshotWriter struct{ backfillResumeWriter }

func (w *backfillResumeSnapshotWriter) GetBackfillCommittedIDs(ctx context.Context, stream types.StreamInterface, plan *types.Set[types.Chunk], expected types.BackfillTableBinding) (map[string]struct{}, error) {
	f := w.fixture
	if stream.ID() != f.stream.ID() || expected != f.binding || !slices.Equal(backfillResumeIDs(stream, f.plan), backfillResumeIDs(stream, plan)) {
		return nil, errors.New("snapshot request lost the full plan or binding")
	}
	if f.readSnapshot != nil {
		if err := f.readSnapshot(ctx); err != nil {
			return nil, err
		}
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	ids := make(map[string]struct{}, len(f.published))
	for id := range f.published {
		ids[id] = struct{}{}
	}
	return ids, f.snapshotErr
}

func (f *backfillResumeFixture) run(t *testing.T, ctx context.Context, pool *destination.WriterPool, source *backfillResumeSource) (error, chan string) {
	t.Helper()
	driver := NewAbstractDriver(ctx, source)
	driver.GlobalConnGroup = utils.NewCGroupWithLimit(ctx, 1)
	driver.SetupState(f.state)
	completed := make(chan string, 2)
	err := driver.Backfill(ctx, completed, pool, f.stream)
	return errors.Join(err, driver.GlobalConnGroup.Block()), completed
}

func TestBackfillResumeCommittedPrefixReachesPendingWithoutWriterOrCheckpointReplay(t *testing.T) {
	const committed, pending = 128, 3
	f := newBackfillResumeFixture(t, committed+pending, committed)
	ctx, _ := backfillResumeContext(t)
	f.rejectOld = true
	// A stale local pending flag is not evidence: two uncommitted ranges have
	// disappeared locally and must be restored from the immutable full plan.
	f.state.SetChunks(f.stream.Self(), types.NewSet(f.ordered[len(f.ordered)-1]))
	wantPending := backfillResumeIDs(f.stream, types.NewSet(f.ordered[committed:]...))
	wantFull := backfillResumeIDs(f.stream, f.plan)
	var durable *types.State
	var boundaries [][]string
	budget := 2 // Binding persistence plus one pending-complement publication.
	f.state.BackfillPersist = func(data []byte) error {
		budget--
		if budget < 0 {
			return errors.New("committed prefix exhausted the startup persistence budget")
		}
		checkpoint := &types.State{RWMutex: &sync.RWMutex{}}
		if err := json.Unmarshal(data, checkpoint); err != nil {
			return err
		}
		durable = checkpoint
		boundaries = append(boundaries, backfillResumeIDs(f.stream, checkpoint.GetChunks(f.stream.Self())))
		return nil
	}
	interrupted := errors.New("interrupt after first pending source range")
	source := &backfillResumeSource{stubDriver: stubDriver{typ: "postgres", cdcSupported: true}, rows: committed + pending, afterRead: interrupted,
		beforeRead: func() error {
			if durable == nil || !slices.Equal(wantPending, backfillResumeIDs(f.stream, durable.GetChunks(f.stream.Self()))) {
				return errors.New("first pending source read preceded durable pending complement")
			}
			if !slices.Equal(wantFull, backfillResumeIDs(f.stream, durable.GetResumePlan(f.stream.Self()))) || !slices.Equal(wantFull, backfillResumeIDs(f.stream, durable.GetBackfillPlan(f.stream.Self()))) {
				return errors.New("startup shrank an immutable plan")
			}
			return nil
		}}
	err, completed := f.run(t, ctx, f.newPool(t, ctx, true), source)
	require.ErrorIs(t, err, interrupted)
	require.NoError(t, f.state.ErrBackfillPersistence())
	require.Equal(t, []types.Chunk{f.ordered[committed]}, source.chunks)
	require.Equal(t, []int{committed}, source.emitted)
	require.Empty(t, completed)
	require.Empty(t, f.commits, "skipping must not re-commit old chunks, and an interrupted new chunk must abort")
	require.Equal(t, wantPending, backfillResumeIDs(f.stream, f.state.GetChunks(f.stream.Self())))
	complementPublications := 0
	for _, boundary := range boundaries {
		if slices.Equal(boundary, wantPending) {
			complementPublications++
		} else {
			require.Equal(t, backfillResumeIDs(f.stream, types.NewSet(f.ordered[len(f.ordered)-1])), boundary, "committed chunks must not be removed and persisted individually")
		}
	}
	require.Equal(t, 1, complementPublications, "persist the authoritative complement once, not once per committed chunk")

	f.state.BackfillPersist = nil
	source = &backfillResumeSource{stubDriver: stubDriver{typ: "postgres", cdcSupported: true}, rows: committed + pending}
	err, completed = f.run(t, ctx, f.newPool(t, ctx, true), source)
	require.NoError(t, err)
	require.Equal(t, f.ordered[committed:], source.chunks)
	require.Equal(t, []int{committed, committed + 1, committed + 2}, source.emitted)
	require.ElementsMatch(t, wantPending, f.commits)
	require.Len(t, f.published, committed+pending)
	for index, chunk := range f.ordered {
		records := f.published[backfillResumeChunkID(f.stream, chunk)]
		require.Len(t, records, 1)
		require.Equal(t, index, records[0].Data["id"])
		require.Equal(t, fmt.Sprintf("row-%d", index), records[0].Data["value"])
	}
	require.Len(t, completed, 1)
	require.Equal(t, f.stream.ID(), <-completed)
	require.True(t, f.state.HasCompletedBackfill(f.stream.Self()))
	require.Zero(t, f.state.GetChunks(f.stream.Self()).Len())
	require.Nil(t, f.state.GetResumePlan(f.stream.Self()))
	require.Equal(t, wantFull, backfillResumeIDs(f.stream, f.state.GetBackfillPlan(f.stream.Self())))
}

func TestBackfillResumeAllCommittedCompletesWithoutSourceOrWriter(t *testing.T) {
	for _, recovery := range []bool{false, true} {
		t.Run(fmt.Sprintf("ordinary_recovery_%t", recovery), func(t *testing.T) {
			f := newBackfillResumeFixture(t, 32, 32)
			ctx, _ := backfillResumeContext(t)
			if recovery {
				execution, _ := utils.ExecutionFromContext(ctx)
				ctx = utils.WithExecution(context.Background(), execution)
				ctx = types.WithOrdinaryRecovery(ctx, &types.OrdinaryRecoveryOptions{Origin: types.OrdinaryRecoveryOrigin{Backfill: true}, BackfillState: f.state})
				f.state.BackfillCheckpoint = false
			}
			f.rejectAll = true
			pool := f.newPool(t, ctx, true)
			if recovery {
				require.NoError(t, pool.PrepareBackfillRecovery(ctx, []types.StreamInterface{f.stream}, "0/100"))
			}
			var durable *types.State
			f.state.BackfillPersist = func(data []byte) error {
				checkpoint := &types.State{RWMutex: &sync.RWMutex{}}
				if err := json.Unmarshal(data, checkpoint); err != nil {
					return err
				}
				durable = checkpoint
				return nil
			}
			source := &backfillResumeSource{stubDriver: stubDriver{typ: "postgres", cdcSupported: true}, rows: 32}
			err, completed := f.run(t, ctx, pool, source)
			require.NoError(t, err)
			require.Empty(t, source.chunks)
			require.Empty(t, source.emitted)
			require.Empty(t, f.commits)
			require.Len(t, f.published, 32)
			require.Len(t, completed, 1)
			require.Equal(t, f.stream.ID(), <-completed)
			require.NotNil(t, durable)
			require.True(t, durable.HasCompletedBackfill(f.stream.Self()))
			require.Zero(t, durable.GetChunks(f.stream.Self()).Len())
			require.Nil(t, durable.GetResumePlan(f.stream.Self()))
			require.Equal(t, backfillResumeIDs(f.stream, f.plan), backfillResumeIDs(f.stream, durable.GetBackfillPlan(f.stream.Self())))
		})
	}
}

func TestBackfillResumeFailuresPreserveDurableRecoveryBoundary(t *testing.T) {
	for _, failure := range []string{"malformed", "foreign", "foreign_pending", "fence_revoked", "canceled", "persist_canceled"} {
		t.Run(failure, func(t *testing.T) {
			f := newBackfillResumeFixture(t, 8, 8)
			ctx, fence := backfillResumeContext(t)
			ctx, cancel := context.WithCancel(ctx)
			t.Cleanup(cancel)
			f.rejectAll = true
			if failure == "foreign_pending" {
				pending := types.NewSet(f.ordered...)
				pending.Insert(types.Chunk{Min: -2, Max: -1})
				f.state.SetChunks(f.stream.Self(), pending)
			}
			wantPending := backfillResumeIDs(f.stream, f.state.GetChunks(f.stream.Self()))
			if failure == "persist_canceled" {
				wantPending = []string{}
			}
			durable, cloneErr := f.state.Clone()
			require.NoError(t, cloneErr)
			f.state.BackfillPersist = func(data []byte) error {
				checkpoint := &types.State{RWMutex: &sync.RWMutex{}}
				if err := json.Unmarshal(data, checkpoint); err != nil {
					return err
				}
				durable = checkpoint
				if failure == "persist_canceled" && checkpoint.GetChunks(f.stream.Self()).Len() == 0 {
					cancel()
				}
				return nil
			}
			var expected error
			switch failure {
			case "malformed", "foreign":
				expected = fmt.Errorf("%s committed metadata", failure)
				f.snapshotErr = expected // Even nonempty evidence accompanied by an error is inadmissible.
			case "fence_revoked":
				f.readSnapshot = func(context.Context) error {
					return os.WriteFile(fence, []byte(`{"version":1,"execution_id":"resume","state":"closed"}`), 0600)
				}
			case "canceled":
				expected = context.Canceled
				f.readSnapshot = func(context.Context) error { cancel(); return nil }
			case "persist_canceled":
				expected = context.Canceled
			}
			source := &backfillResumeSource{stubDriver: stubDriver{typ: "postgres", cdcSupported: true}, rows: 8}
			err, completed := f.run(t, ctx, f.newPool(t, ctx, true), source)
			require.Error(t, err)
			if expected != nil {
				require.ErrorIs(t, err, expected)
			}
			if failure == "foreign_pending" {
				require.ErrorContains(t, err, "pending backfill chunks are outside the immutable plan")
			}
			require.Empty(t, source.chunks)
			require.Empty(t, source.emitted)
			require.Empty(t, f.commits)
			require.Empty(t, completed)
			require.False(t, f.state.HasCompletedBackfill(f.stream.Self()))
			require.Equal(t, wantPending, backfillResumeIDs(f.stream, f.state.GetChunks(f.stream.Self())))
			require.Equal(t, backfillResumeIDs(f.stream, f.plan), backfillResumeIDs(f.stream, f.state.GetResumePlan(f.stream.Self())))
			require.False(t, durable.HasCompletedBackfill(f.stream.Self()))
			require.Equal(t, wantPending, backfillResumeIDs(f.stream, durable.GetChunks(f.stream.Self())))
			require.Equal(t, backfillResumeIDs(f.stream, f.plan), backfillResumeIDs(f.stream, durable.GetResumePlan(f.stream.Self())))
		})
	}
}

func TestBackfillResumeUnsupportedAndLegacyTailKeepPerWriterFallback(t *testing.T) {
	for _, legacy := range []bool{false, true} {
		t.Run(fmt.Sprintf("legacy_%t", legacy), func(t *testing.T) {
			f := newBackfillResumeFixture(t, 5, 5)
			ctx, _ := backfillResumeContext(t)
			if legacy {
				ctx = context.Background()
				f.state.BackfillCheckpoint = false
				f.snapshotErr = errors.New("legacy backfill must not request authoritative skip evidence")
			}
			source := &backfillResumeSource{stubDriver: stubDriver{typ: "postgres", cdcSupported: true}, rows: 5}
			err, completed := f.run(t, ctx, f.newPool(t, ctx, legacy), source)
			require.NoError(t, err)
			if legacy {
				require.Equal(t, []types.Chunk{f.ordered[4]}, source.chunks, "an unbound legacy tail must still be read despite committed metadata")
				require.Equal(t, []int{4}, source.emitted)
				require.Equal(t, 4, f.published[backfillResumeChunkID(f.stream, f.ordered[4])][0].Data["id"])
			} else {
				require.Empty(t, source.chunks, "unsupported destinations retain the existing per-writer committed check")
				require.Empty(t, source.emitted)
			}
			require.ElementsMatch(t, backfillResumeIDs(f.stream, f.plan), f.commits, "fallback must still perform writer cleanup for every old chunk")
			require.Len(t, completed, 1)
			require.Equal(t, f.stream.ID(), <-completed)
			require.True(t, f.state.HasCompletedBackfill(f.stream.Self()))
		})
	}
}
