package destination

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/datazip-inc/olake/types"
	"github.com/stretchr/testify/require"
)

const writerCloseTestTimeout = 5 * time.Second

// The store keeps staged data separate from published data, like a destination
// whose Close either commits a chunk with its checkpoint or discards the chunk.
type writerCloseTestStore struct {
	mu             sync.Mutex
	staged         map[string]int
	prepared       map[string]bool
	published      map[string]int
	checkpoints    map[string]any
	aborted        map[string]bool
	preparations   map[string]int
	activeCommits  int
	maximumCommits int
}

func newWriterCloseTestStore() *writerCloseTestStore {
	return &writerCloseTestStore{
		staged:       make(map[string]int),
		prepared:     make(map[string]bool),
		published:    make(map[string]int),
		checkpoints:  make(map[string]any),
		aborted:      make(map[string]bool),
		preparations: make(map[string]int),
	}
}

type writerCloseTestWriter struct {
	Writer          // Methods not used by WriterPool.NewWriter, Push, or Close.
	store           *writerCloseTestStore
	id              string
	requiresPrepare bool
	writeErr        error
	afterWrite      func(context.Context) error
	beforeCommit    func(context.Context) error
}

func (*writerCloseTestWriter) Setup(context.Context, types.StreamInterface, any, *Options) (any, *types.MetadataState, error) {
	return "shared-schema", nil, nil
}

func (*writerCloseTestWriter) FlattenAndCleanData(_ context.Context, records []types.RawRecord) (bool, []types.RawRecord, any, error) {
	retained := make([]types.RawRecord, 0, len(records))
	for _, record := range records {
		if record.Data["filtered"] != true {
			retained = append(retained, record)
		}
	}
	return false, retained, nil, nil
}

func (w *writerCloseTestWriter) Write(ctx context.Context, records []types.RawRecord) error {
	w.store.mu.Lock()
	w.store.staged[w.id] += len(records)
	w.store.mu.Unlock()
	if w.afterWrite != nil {
		if err := w.afterWrite(ctx); err != nil {
			return err
		}
	}
	return w.writeErr
}

func (w *writerCloseTestWriter) abort() {
	w.store.mu.Lock()
	defer w.store.mu.Unlock()
	delete(w.store.staged, w.id)
	delete(w.store.prepared, w.id)
	w.store.aborted[w.id] = true
}

func (w *writerCloseTestWriter) Close(ctx context.Context, checkpoint any) error {
	if ctx.Err() != nil {
		w.abort()
		// A successful abort must not erase an earlier flush/preparation error or
		// turn a canceled commit into successful accounting in WriterThread.Close.
		return nil
	}
	w.store.mu.Lock()
	if w.requiresPrepare && !w.store.prepared[w.id] {
		w.store.mu.Unlock()
		return fmt.Errorf("chunk %s was not finalized before commit", w.id)
	}
	w.store.activeCommits++
	if w.store.activeCommits > w.store.maximumCommits {
		w.store.maximumCommits = w.store.activeCommits
	}
	w.store.mu.Unlock()
	defer func() {
		w.store.mu.Lock()
		w.store.activeCommits--
		w.store.mu.Unlock()
	}()

	if w.beforeCommit != nil {
		if err := w.beforeCommit(ctx); err != nil {
			w.abort()
			return err
		}
	}
	if err := ctx.Err(); err != nil {
		w.abort()
		return err
	}
	w.store.mu.Lock()
	defer w.store.mu.Unlock()
	w.store.published[w.id] = w.store.staged[w.id]
	w.store.checkpoints[w.id] = checkpoint
	delete(w.store.staged, w.id)
	delete(w.store.prepared, w.id)
	return nil
}

type writerCloseTestPreparer struct {
	*writerCloseTestWriter
	prepare func(context.Context) error
}

func (w *writerCloseTestPreparer) PrepareClose(ctx context.Context) error {
	w.store.mu.Lock()
	w.store.preparations[w.id]++
	staged := w.store.staged[w.id]
	w.store.mu.Unlock()
	if staged == 0 {
		return fmt.Errorf("chunk %s finalized before its flush completed", w.id)
	}
	if w.prepare != nil {
		if err := w.prepare(ctx); err != nil {
			return err
		}
	}
	w.store.mu.Lock()
	w.store.prepared[w.id] = true
	w.store.mu.Unlock()
	return nil
}

// Like the native writer, direct Close still finalizes any remaining files.
// Before the optional preparation phase existed this ran under the shared stream
// lock, so a blocked finalizer also blocked its same-stream peer's commit.
func (w *writerCloseTestPreparer) Close(ctx context.Context, checkpoint any) error {
	w.store.mu.Lock()
	prepared := w.store.prepared[w.id]
	w.store.mu.Unlock()
	if ctx.Err() == nil && !prepared {
		if err := w.PrepareClose(ctx); err != nil {
			w.abort()
			return err
		}
	}
	return w.writerCloseTestWriter.Close(ctx, checkpoint)
}

func writerCloseTestPool(t *testing.T, ctx context.Context, writers ...Writer) (*WriterPool, []*WriterThread) {
	t.Helper()
	stream := types.NewStream("events", "public", nil).Wrap(0)
	next := 0
	pool := &WriterPool{
		stats:     &Stats{},
		batchSize: 100,
		initWriter: func(context.Context, any) (Writer, func(context.Context), error) {
			writer := writers[next]
			next++
			return writer, nil, nil
		},
	}
	pool.writerSchema.Store(stream.ID(), &writerSchema{})
	threads := make([]*WriterThread, len(writers))
	for i := range writers {
		var err error
		threads[i], _, err = pool.NewWriter(ctx, stream, WithThreadID(fmt.Sprintf("chunk-%d", i)))
		require.NoError(t, err)
	}
	return pool, threads
}

func writerCloseTestPush(t *testing.T, ctx context.Context, thread *WriterThread, filtered bool, sourceBytes int64) {
	t.Helper()
	record := types.CreateRawRecord(map[string]any{"filtered": filtered}, nil, nil)
	require.NoError(t, thread.Push(ctx, record, sourceBytes))
}

func writerCloseTestAwait(t *testing.T, signal <-chan struct{}, operation string) {
	t.Helper()
	timer := time.NewTimer(writerCloseTestTimeout)
	defer timer.Stop()
	select {
	case <-signal:
	case <-timer.C:
		t.Fatalf("timed out waiting for %s", operation)
	}
}

func writerCloseTestGate(ctx context.Context, release <-chan struct{}) error {
	select {
	case <-release:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

type writerCloseTestResult struct {
	done chan struct{}
	err  error
}

func writerCloseTestStart(t *testing.T, ctx context.Context, cancel context.CancelFunc, thread *WriterThread, checkpoint any) *writerCloseTestResult {
	t.Helper()
	result := &writerCloseTestResult{done: make(chan struct{})}
	go func() {
		result.err = thread.Close(ctx, checkpoint)
		close(result.done)
	}()
	t.Cleanup(func() {
		cancel()
		timer := time.NewTimer(writerCloseTestTimeout)
		defer timer.Stop()
		select {
		case <-result.done:
		case <-timer.C:
			t.Error("writer Close did not exit after cleanup cancellation")
		}
	})
	return result
}

func TestWriterThreadCloseFinalizesSameStreamWhilePeerPreparationIsBlocked(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	store := newWriterCloseTestStore()
	preparing := make(chan struct{})
	release := make(chan struct{})
	first := &writerCloseTestPreparer{
		writerCloseTestWriter: &writerCloseTestWriter{store: store, id: "first", requiresPrepare: true},
		prepare: func(ctx context.Context) error {
			close(preparing)
			return writerCloseTestGate(ctx, release)
		},
	}
	second := &writerCloseTestPreparer{
		writerCloseTestWriter: &writerCloseTestWriter{store: store, id: "second", requiresPrepare: true},
	}
	pool, threads := writerCloseTestPool(t, ctx, first, second)
	writerCloseTestPush(t, ctx, threads[0], false, 11)
	writerCloseTestPush(t, ctx, threads[1], false, 23)
	firstResult := writerCloseTestStart(t, ctx, cancel, threads[0], "first-checkpoint")
	writerCloseTestAwait(t, preparing, "first chunk's file finalization")

	// The second same-stream chunk must flush, finalize, and publish without
	// waiting for the first chunk's file finalization. Taking the stream commit
	// mutex around preparation reproduces the old head-of-line blocking here.
	secondResult := writerCloseTestStart(t, ctx, cancel, threads[1], "second-checkpoint")
	writerCloseTestAwait(t, secondResult.done, "peer commit while first preparation is blocked")
	require.NoError(t, secondResult.err)
	func() {
		store.mu.Lock()
		defer store.mu.Unlock()
		require.Equal(t, map[string]int{"second": 1}, store.published)
		require.Equal(t, map[string]any{"second": "second-checkpoint"}, store.checkpoints)
		require.Equal(t, 1, store.staged["first"])
	}()
	require.Equal(t, int64(1), pool.GetStats().ThreadCount.Load())

	close(release)
	writerCloseTestAwait(t, firstResult.done, "first commit after releasing preparation")
	require.NoError(t, firstResult.err)
	require.Equal(t, map[string]int{"first": 1, "second": 1}, store.published)
	require.Equal(t, map[string]any{"first": "first-checkpoint", "second": "second-checkpoint"}, store.checkpoints)
	require.Empty(t, store.staged)
	require.Empty(t, store.aborted)
	require.Equal(t, int64(2), pool.GetStats().ReadCount.Load())
	require.Equal(t, int64(34), pool.GetStats().BytesRead.Load())
	require.Zero(t, pool.GetStats().ThreadCount.Load())
}

func TestWriterThreadCloseSerializesSameStreamCommitWithAndWithoutPreparation(t *testing.T) {
	for _, prepares := range []bool{true, false} {
		t.Run(fmt.Sprintf("preparation=%t", prepares), func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			store := newWriterCloseTestStore()
			firstCommitting := make(chan struct{})
			secondCommitting := make(chan struct{})
			secondFlushed := make(chan struct{})
			secondPrepared := make(chan struct{})
			release := make(chan struct{})
			first := &writerCloseTestWriter{store: store, id: "first", requiresPrepare: prepares}
			first.beforeCommit = func(ctx context.Context) error {
				close(firstCommitting)
				return writerCloseTestGate(ctx, release)
			}
			second := &writerCloseTestWriter{store: store, id: "second", requiresPrepare: prepares}
			second.afterWrite = func(context.Context) error { close(secondFlushed); return nil }
			second.beforeCommit = func(context.Context) error { close(secondCommitting); return nil }
			var firstWriter, secondWriter Writer = first, second
			if prepares {
				firstWriter = &writerCloseTestPreparer{writerCloseTestWriter: first}
				secondWriter = &writerCloseTestPreparer{
					writerCloseTestWriter: second,
					prepare:               func(context.Context) error { close(secondPrepared); return nil },
				}
			}
			pool, threads := writerCloseTestPool(t, ctx, firstWriter, secondWriter)
			writerCloseTestPush(t, ctx, threads[0], false, 11)
			writerCloseTestPush(t, ctx, threads[1], false, 23)
			firstResult := writerCloseTestStart(t, ctx, cancel, threads[0], "first-checkpoint")
			writerCloseTestAwait(t, firstCommitting, "first commit")
			secondResult := writerCloseTestStart(t, ctx, cancel, threads[1], "second-checkpoint")
			writerCloseTestAwait(t, secondFlushed, "second chunk flush during peer commit")
			if prepares {
				writerCloseTestAwait(t, secondPrepared, "second chunk preparation during peer commit")
			}

			// Both threads share this stream artifact. Close must retain its lock
			// for the entire blocked destination commit, not merely for preparation
			// or the beginning of the commit. This assertion needs no scheduling
			// delay or timeout-based inference that the peer has not committed.
			if threads[0].streamArtifact.mu.TryLock() {
				threads[0].streamArtifact.mu.Unlock()
				t.Fatal("destination commit did not retain the stream commit lock")
			}
			func() {
				store.mu.Lock()
				defer store.mu.Unlock()
				require.Empty(t, store.published)
				require.Equal(t, 1, store.activeCommits)
			}()
			close(release)
			writerCloseTestAwait(t, firstResult.done, "first serialized commit")
			writerCloseTestAwait(t, secondResult.done, "second serialized commit")
			require.NoError(t, firstResult.err)
			require.NoError(t, secondResult.err)
			writerCloseTestAwait(t, secondCommitting, "second destination commit")
			require.Equal(t, 1, store.maximumCommits)
			require.Equal(t, map[string]int{"first": 1, "second": 1}, store.published)
			require.Equal(t, map[string]any{"first": "first-checkpoint", "second": "second-checkpoint"}, store.checkpoints)
			require.Empty(t, store.staged)
			require.Empty(t, store.aborted)
			require.Equal(t, int64(2), pool.GetStats().ReadCount.Load())
			require.Equal(t, int64(34), pool.GetStats().BytesRead.Load())
			require.Zero(t, pool.GetStats().ThreadCount.Load())
		})
	}
}

func TestWriterThreadCloseAbortsAndRollsBackWithoutPublishing(t *testing.T) {
	flushErr := errors.New("final flush failed")
	prepareErr := errors.New("file finalization failed")
	for _, failure := range []string{"flush", "prepare", "cancel after flush", "cancel during preparation", "cancel after preparation"} {
		t.Run(failure, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			store := newWriterCloseTestStore()
			committed := &writerCloseTestWriter{store: store, id: "committed"}
			failing := &writerCloseTestPreparer{
				writerCloseTestWriter: &writerCloseTestWriter{store: store, id: "failed", requiresPrepare: true},
			}
			wantErr := prepareErr
			wantPreparations := 1
			switch failure {
			case "flush":
				failing.writeErr = flushErr
				wantErr = flushErr
				wantPreparations = 0
			case "prepare":
				failing.prepare = func(context.Context) error { return prepareErr }
			case "cancel after flush":
				failing.afterWrite = func(context.Context) error { cancel(); return nil }
				wantErr = context.Canceled
				wantPreparations = 0
			case "cancel during preparation":
				failing.prepare = func(ctx context.Context) error { cancel(); return ctx.Err() }
				wantErr = context.Canceled
			case "cancel after preparation":
				// A preparer may finish successfully just as its caller is canceled.
				// Close must still abort, even when destination abort returns nil.
				failing.prepare = func(context.Context) error { cancel(); return nil }
				wantErr = context.Canceled
			}
			pool, threads := writerCloseTestPool(t, ctx, committed, failing)
			writerCloseTestPush(t, ctx, threads[0], false, 17)
			committedResult := writerCloseTestStart(t, ctx, cancel, threads[0], "committed-checkpoint")
			writerCloseTestAwait(t, committedResult.done, "baseline committed chunk")
			require.NoError(t, committedResult.err)
			writerCloseTestPush(t, ctx, threads[1], false, 31)
			writerCloseTestPush(t, ctx, threads[1], true, 47)
			result := writerCloseTestStart(t, ctx, cancel, threads[1], "must-not-publish")
			writerCloseTestAwait(t, result.done, "failed chunk abort")
			require.ErrorIs(t, result.err, wantErr)
			require.Equal(t, wantPreparations, store.preparations["failed"])
			require.Equal(t, map[string]int{"committed": 1}, store.published)
			require.Equal(t, map[string]any{"committed": "committed-checkpoint"}, store.checkpoints)
			require.Equal(t, map[string]bool{"failed": true}, store.aborted)
			require.Empty(t, store.staged)
			require.Empty(t, store.prepared)
			require.Equal(t, int64(1), pool.GetStats().ReadCount.Load())
			require.Zero(t, pool.GetStats().RecordsFiltered.Load())
			require.Equal(t, int64(17), pool.GetStats().BytesRead.Load())
			require.Zero(t, pool.GetStats().ThreadCount.Load())
		})
	}
}
