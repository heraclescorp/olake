package iceberg

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	iceio "github.com/apache/iceberg-go/io"
	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
	s3util "github.com/datazip-inc/olake/utils/s3"
	"github.com/stretchr/testify/require"
)

type nativeResyncIOGate struct {
	entered     chan struct{}
	release     chan struct{}
	enteredOnce sync.Once
	releaseOnce sync.Once
}

func newNativeResyncIOGate(t *testing.T) *nativeResyncIOGate {
	t.Helper()
	gate := &nativeResyncIOGate{entered: make(chan struct{}), release: make(chan struct{})}
	t.Cleanup(gate.unblock)
	return gate
}

func (g *nativeResyncIOGate) block() {
	g.enteredOnce.Do(func() { close(g.entered) })
	<-g.release
}

func (g *nativeResyncIOGate) unblock() {
	g.releaseOnce.Do(func() { close(g.release) })
}

func nativeResyncAwait[T any](t *testing.T, result <-chan T, operation string) T {
	t.Helper()
	select {
	case value := <-result:
		return value
	case <-time.After(5 * time.Second):
		t.Fatalf("%s blocked behind another private file's I/O", operation)
		var zero T
		return zero
	}
}

type nativeResyncGatedFS struct {
	iceio.WriteFileIO
	path  string
	phase string
	gate  *nativeResyncIOGate
}

func (s *nativeResyncGatedFS) Create(path string) (iceio.FileWriter, error) {
	if path == s.path && s.phase == "create" {
		s.gate.block()
	}
	writer, err := s.WriteFileIO.Create(path)
	if err != nil {
		return nil, err
	}
	return &nativeResyncGatedWriter{FileWriter: writer, fs: s, path: path}, nil
}

func (s *nativeResyncGatedFS) Remove(path string) error {
	if path == s.path && s.phase == "remove" {
		s.gate.block()
	}
	return s.WriteFileIO.Remove(path)
}

type nativeResyncGatedWriter struct {
	iceio.FileWriter
	fs   *nativeResyncGatedFS
	path string
}

func (w *nativeResyncGatedWriter) Write(data []byte) (int, error) {
	if w.path == w.fs.path && w.fs.phase == "write" {
		w.fs.gate.block()
	}
	return w.FileWriter.Write(data)
}

func (w *nativeResyncGatedWriter) Close() error {
	if w.path == w.fs.path && w.fs.phase == "close" {
		w.fs.gate.block()
	}
	return w.FileWriter.Close()
}

func TestNativeResyncPrivateBlockedIODoesNotSerializeWritersOrAbort(t *testing.T) {
	for _, phase := range []string{"create", "write", "close", "remove"} {
		t.Run(phase, func(t *testing.T) {
			ctx := resyncTestContext(t)
			cfg, stream, cat := nativeWriterFixture(t, types.UpdateTypeEquality, true, "")
			seed := openNativeTestWriter(ctx, t, cfg, stream, cat, nil, "seed", true)
			require.NoError(t, seed.Write(ctx, []types.RawRecord{nativeTestRecord("original", "r", 1, "old")}))
			require.NoError(t, seed.Close(ctx, nil))
			adapter := &Iceberg{config: cfg, catalog: cat}
			_, err := adapter.PrepareResync(ctx, stream, "operation-1", resyncTestState())
			require.NoError(t, err)
			baseFS, err := adapter.resync.stage.fileIO(ctx)
			require.NoError(t, err)
			root := t.TempDir()
			blockedPath := filepath.Join(root, "blocked.bin")
			gate := newNativeResyncIOGate(t)
			storage := &nativeResyncGatedFS{WriteFileIO: baseFS.(*nativeResyncIO).IO.(iceio.WriteFileIO), path: blockedPath, phase: phase, gate: gate}
			stage := &nativeResyncIO{IO: storage, attempt: adapter.resync, ctx: ctx}
			peerPath := filepath.Join(root, "peer.bin")
			peer, err := stage.Create(peerPath)
			require.NoError(t, err)
			guardPath := filepath.Join(root, "guard.bin")
			guard, err := stage.Create(guardPath)
			require.NoError(t, err)
			var blocked iceio.FileWriter
			if phase != "create" {
				blocked, err = stage.Create(blockedPath)
				require.NoError(t, err)
				if phase == "close" || phase == "remove" {
					_, err = blocked.Write([]byte("retained private bytes"))
					require.NoError(t, err)
				}
				if phase == "remove" {
					require.NoError(t, blocked.Close())
				}
			}
			blockedDone := make(chan error, 1)
			go func() {
				switch phase {
				case "create":
					_, err := stage.Create(blockedPath)
					blockedDone <- err
				case "write":
					_, err := blocked.Write([]byte("retained private bytes"))
					blockedDone <- err
				case "close":
					blockedDone <- blocked.Close()
				case "remove":
					blockedDone <- stage.Remove(blockedPath)
				}
			}()
			nativeResyncAwait(t, gate.entered, "entering blocked "+phase)

			peerDone := make(chan error, 1)
			go func() {
				if _, err := peer.Write([]byte("independent writer")); err != nil {
					peerDone <- err
					return
				}
				peerDone <- peer.Close()
			}()
			require.NoError(t, nativeResyncAwait(t, peerDone, "writing and finalizing independent file"))
			data, err := os.ReadFile(peerPath)
			require.NoError(t, err)
			require.Equal(t, "independent writer", string(data))

			publishDone := make(chan error, 1)
			go func() { publishDone <- adapter.PublishResync(ctx, resyncTestState()) }()
			require.ErrorContains(t, nativeResyncAwait(t, publishDone, "rejecting publication during private I/O"), "close every target writer")
			abortDone := make(chan error, 1)
			go func() { abortDone <- adapter.AbortResync(context.Background()) }()
			require.NoError(t, nativeResyncAwait(t, abortDone, "aborting during private I/O"))
			n, err := guard.Write([]byte("must not reach storage"))
			require.Error(t, err)
			require.Zero(t, n)
			data, err = os.ReadFile(guardPath)
			require.NoError(t, err)
			require.Empty(t, data)

			gate.unblock()
			blockedErr := nativeResyncAwait(t, blockedDone, "finishing private I/O after abort")
			if phase == "remove" {
				require.NoError(t, blockedErr, "known-unpublished cleanup may finish after abort")
			} else {
				require.Error(t, blockedErr)
			}
			_, err = stage.Create(filepath.Join(root, "after-abort.bin"))
			require.Error(t, err)
			require.NoFileExists(t, filepath.Join(root, "after-abort.bin"))
			if phase != "create" && phase != "remove" {
				data, err = os.ReadFile(blockedPath)
				require.NoError(t, err)
				require.Equal(t, "retained private bytes", string(data), "an abort must not delete or truncate an admitted private write")
			}
			if phase != "remove" {
				require.NoError(t, stage.Remove(blockedPath), "a successfully created path remains owned even when abort races Create")
			}
			require.NoFileExists(t, blockedPath)
			require.Error(t, adapter.PublishResync(ctx, resyncTestState()))
			public, err := cat.LoadTable(ctx, seed.table.Identifier())
			require.NoError(t, err)
			require.Equal(t, nativeSnapshotID(seed.table), nativeSnapshotID(public))
			require.Equal(t, map[string]int64{"original": 1}, nativeVisibleValues(t, public))
		})
	}
}

func TestNativeResyncPrivateWritesRejectTerminalLocalState(t *testing.T) {
	for _, terminal := range []string{"cancellation", "context-deadline", "attempt-deadline", "aborted", "failed", "unknown", "published"} {
		t.Run(terminal, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			t.Cleanup(cancel)
			if terminal == "context-deadline" {
				ctx, cancel = context.WithTimeout(context.Background(), 100*time.Millisecond)
				t.Cleanup(cancel)
			}
			attempt := &resyncControl{deadline: time.Now().Add(time.Hour)}
			stage := &nativeResyncIO{IO: iceio.LocalFS{}, attempt: attempt, ctx: ctx}
			path := filepath.Join(t.TempDir(), "private.bin")
			writer, err := stage.Create(path)
			require.NoError(t, err)
			_, err = writer.Write([]byte("before terminal state"))
			require.NoError(t, err)
			switch terminal {
			case "cancellation":
				cancel()
			case "context-deadline":
				<-ctx.Done()
			case "attempt-deadline":
				attempt.deadline = time.Now().Add(-time.Second)
			case "aborted":
				require.NoError(t, (&Iceberg{resync: attempt}).AbortResync(context.Background()))
			case "failed":
				attempt.failed = true
			case "unknown":
				attempt.unknown = true
			case "published":
				attempt.published = true
			}
			n, err := writer.Write([]byte("forbidden write"))
			require.Error(t, err)
			require.Zero(t, n)
			if terminal == "cancellation" {
				require.ErrorIs(t, err, context.Canceled)
			}
			if terminal == "context-deadline" {
				require.ErrorIs(t, err, context.DeadlineExceeded)
			}
			copied, err := writer.(io.ReaderFrom).ReadFrom(strings.NewReader("forbidden copy"))
			require.Error(t, err)
			require.Zero(t, copied)
			require.Error(t, writer.Close())
			nextPath := filepath.Join(filepath.Dir(path), "next.bin")
			_, err = stage.Create(nextPath)
			require.Error(t, err)
			require.NoFileExists(t, nextPath)
			data, err := os.ReadFile(path)
			require.NoError(t, err)
			require.Equal(t, "before terminal state", string(data))
			if terminal == "unknown" || terminal == "published" {
				require.Error(t, stage.Remove(path), "possibly published files must be retained")
				require.FileExists(t, path)
			}
		})
	}
}

type nativeResyncS3Fence struct {
	gets      atomic.Int64
	closed    atomic.Bool
	blockNext atomic.Pointer[nativeResyncIOGate]
}

func nativeResyncS3FenceContext(t *testing.T) (context.Context, *nativeResyncS3Fence) {
	t.Helper()
	fence := &nativeResyncS3Fence{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/resync-tests/fence.json" {
			t.Errorf("unexpected S3 request: %s %s", r.Method, r.URL.Path)
			http.Error(w, "unexpected S3 request", http.StatusBadRequest)
			return
		}
		fence.gets.Add(1)
		if gate := fence.blockNext.Swap(nil); gate != nil {
			gate.block()
		}
		w.Header().Set("Content-Type", "application/json")
		state := "open"
		if fence.closed.Load() {
			state = "closed"
		}
		_, _ = io.WriteString(w, `{"version":1,"execution_id":"runtime","state":"`+state+`"}`)
	}))
	t.Cleanup(server.Close)
	t.Setenv(constants.EnvStorageMode, constants.StorageModeS3)
	t.Setenv(constants.EnvS3Region, "us-east-1")
	t.Setenv(constants.EnvS3AccessKeyID, "test-access")
	t.Setenv(constants.EnvS3SecretAccessKey, "test-secret")
	t.Setenv(constants.EnvS3Endpoint, server.URL)
	bucket, prefix := s3util.JobBucket, s3util.JobPrefix
	t.Cleanup(func() { s3util.JobBucket, s3util.JobPrefix = bucket, prefix })
	require.NoError(t, s3util.Init(context.Background()))
	ctx := utils.WithExecution(resyncTestContext(t), &utils.ExecutionOptions{ID: "runtime", Fence: "s3://resync-tests/fence.json"})
	return ctx, fence
}

func TestNativeResyncPrivateFragmentsDoNotFetchS3FenceAndRevocationBlocksPublication(t *testing.T) {
	ctx, fence := nativeResyncS3FenceContext(t)
	cfg, stream, cat := nativeWriterFixture(t, types.UpdateTypeEquality, true, "")
	seed := openNativeTestWriter(ctx, t, cfg, stream, cat, nil, "seed", true)
	require.NoError(t, seed.Write(ctx, []types.RawRecord{nativeTestRecord("original", "r", 1, "old")}))
	require.NoError(t, seed.Close(ctx, nil))
	adapter := &Iceberg{config: cfg, catalog: cat}
	_, err := adapter.PrepareResync(ctx, stream, "operation-1", resyncTestState())
	require.NoError(t, err)
	replacement := openNativeTestWriter(ctx, t, cfg, stream, cat, nil, "replacement", true)
	parquetBefore := fence.gets.Load()
	require.NoError(t, replacement.Write(ctx, []types.RawRecord{nativeTestRecord("replacement", "r", 2, "new")}))
	require.Equal(t, int64(2), fence.gets.Load()-parquetBefore, "actual Parquet output may fetch the fence only for writer admission and Create, never for column fragments")
	require.NoError(t, replacement.Close(ctx, nil))
	require.Equal(t, map[string]int64{"replacement": 2}, nativeVisibleValues(t, replacement.table))
	fs, err := adapter.resync.stage.fileIO(ctx)
	require.NoError(t, err)
	stage := fs.(*nativeResyncIO)
	privatePath := filepath.Join(t.TempDir(), "private-fragments.bin")
	writer, err := stage.Create(privatePath)
	require.NoError(t, err)
	before := fence.gets.Load()
	require.Positive(t, before, "the fixture must exercise the real S3 fence client")
	fragment := bytes.Repeat([]byte("private fragment\n"), 8)
	var expected bytes.Buffer
	for range 256 {
		n, err := writer.Write(fragment)
		require.NoError(t, err)
		require.Equal(t, len(fragment), n)
		_, _ = expected.Write(fragment)
	}
	copied, err := writer.(io.ReaderFrom).ReadFrom(io.LimitReader(strings.NewReader("copied private bytes"), 20))
	require.NoError(t, err)
	require.Equal(t, int64(20), copied)
	_, _ = expected.WriteString("copied private bytes")
	require.Equal(t, before, fence.gets.Load(), "buffered Write and ReadFrom fragments must not issue S3 fence GETs")
	require.NoError(t, writer.Close())
	data, err := os.ReadFile(privatePath)
	require.NoError(t, err)
	require.Equal(t, expected.Bytes(), data)

	fence.closed.Store(true)
	before = fence.gets.Load()
	require.Error(t, adapter.PublishResync(ctx, resyncTestState()))
	require.Greater(t, fence.gets.Load(), before, "publication must freshly observe the revoked remote fence")
	require.False(t, adapter.ResyncPublished())
	public, err := cat.LoadTable(context.Background(), seed.table.Identifier())
	require.NoError(t, err)
	require.Equal(t, nativeSnapshotID(seed.table), nativeSnapshotID(public))
	require.Equal(t, map[string]int64{"original": 1}, nativeVisibleValues(t, public))
	require.NoError(t, adapter.AbortResync(context.Background()), "revocation must not prevent the local abort tombstone")
	require.NoError(t, stage.Remove(privatePath), "known-unpublished owned files can be cleaned up after revocation")
	require.NoFileExists(t, privatePath)
	entries, err := nativeLiveEntries(context.Background(), seed.table)
	require.NoError(t, err)
	require.NotEmpty(t, entries)
	for _, entry := range entries {
		require.Error(t, stage.Remove(entry.DataFile().FilePath()), "cleanup must never remove original files")
	}
	require.Equal(t, map[string]int64{"original": 1}, nativeVisibleValues(t, public))
}

func TestNativeResyncRemotePrivateBoundaryDoesNotHoldAttemptMutex(t *testing.T) {
	for _, phase := range []string{"create", "close"} {
		t.Run(phase, func(t *testing.T) {
			ctx, fence := nativeResyncS3FenceContext(t)
			attempt := &resyncControl{deadline: time.Now().Add(time.Hour)}
			stage := &nativeResyncIO{IO: iceio.LocalFS{}, attempt: attempt, ctx: ctx}
			root := t.TempDir()
			peerPath := filepath.Join(root, "peer.bin")
			peer, err := stage.Create(peerPath)
			require.NoError(t, err)
			blockedPath := filepath.Join(root, "blocked.bin")
			var blocked iceio.FileWriter
			if phase == "close" {
				blocked, err = stage.Create(blockedPath)
				require.NoError(t, err)
				_, err = blocked.Write([]byte("private bytes"))
				require.NoError(t, err)
			}
			gate := newNativeResyncIOGate(t)
			fence.blockNext.Store(gate)
			done := make(chan error, 1)
			go func() {
				if phase == "create" {
					_, err := stage.Create(blockedPath)
					done <- err
					return
				}
				done <- blocked.Close()
			}()
			nativeResyncAwait(t, gate.entered, "entering remote "+phase+" boundary")
			peerDone := make(chan error, 1)
			go func() {
				_, err := peer.Write([]byte("independent private bytes"))
				peerDone <- err
			}()
			require.NoError(t, nativeResyncAwait(t, peerDone, "writing while another boundary awaits S3"))
			data, err := os.ReadFile(peerPath)
			require.NoError(t, err)
			require.Equal(t, "independent private bytes", string(data))
			abortDone := make(chan error, 1)
			go func() { abortDone <- (&Iceberg{resync: attempt}).AbortResync(context.Background()) }()
			require.NoError(t, nativeResyncAwait(t, abortDone, "aborting while a boundary awaits S3"))
			gate.unblock()
			require.Error(t, nativeResyncAwait(t, done, "rechecking abort after remote boundary"))
			if phase == "create" {
				require.NoFileExists(t, blockedPath, "an OPEN response must not revive an aborted attempt")
			} else {
				data, err = os.ReadFile(blockedPath)
				require.NoError(t, err)
				require.Equal(t, "private bytes", string(data))
			}
		})
	}
}
