package iceberg

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	apacheiceberg "github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
	"github.com/datazip-inc/olake/types"
	"github.com/stretchr/testify/require"
)

func TestNativeBackfillRollingAndCDC(t *testing.T) {
	for _, mode := range []types.UpdateType{types.UpdateTypeEquality, types.UpdateTypePosition, types.UpdateTypeDeletionVector} {
		t.Run(string(mode), func(t *testing.T) {
			ctx := context.Background()
			cfg, stream, cat := nativeWriterFixture(t, mode, true, "/{part, identity}")
			var index *nativeMemoryIndex
			if mode != types.UpdateTypeEquality {
				index = &nativeMemoryIndex{rows: make(map[string]types.RowLocation)}
			}
			writer := openNativeTestWriter(ctx, t, cfg, stream, cat, index, "rolling", true)
			require.NoError(t, writer.Write(ctx, []types.RawRecord{nativeTestRecord("a", "r", 1, "one")}))
			require.NoError(t, writer.Write(ctx, []types.RawRecord{nativeTestRecord("b", "r", 2, "one")}))
			require.Empty(t, writer.dataFiles)
			require.Len(t, writer.backfillFiles, 1)
			var first *nativeBackfillFile
			for _, f := range writer.backfillFiles {
				first = f
			}
			require.EqualValues(t, 2, first.rows)
			require.Positive(t, first.output.size.Load())
			if index != nil {
				a, found, err := writer.indexThread.Lookup("a")
				require.NoError(t, err)
				require.True(t, found)
				b, found, err := writer.indexThread.Lookup("b")
				require.NoError(t, err)
				require.True(t, found)
				require.Equal(t, first.path, a.FilePath)
				require.Zero(t, a.Position)
				require.Equal(t, first.path, b.FilePath)
				require.EqualValues(t, 1, b.Position)
				require.Empty(t, index.rows)
			}
			writer.backfillTargetBytes = first.output.size.Load() + 1
			require.NoError(t, writer.Write(ctx, []types.RawRecord{nativeTestRecord("c", "r", 3, "one")}))
			require.Len(t, writer.dataFiles, 1)
			require.EqualValues(t, 3, writer.dataFiles[0].Count())
			require.Equal(t, first.path, writer.dataFiles[0].FilePath())
			writer.backfillTargetBytes = nativeBackfillTargetBytes
			require.NoError(t, writer.Write(ctx, []types.RawRecord{nativeTestRecord("d", "r", 4, "one")}))
			if index != nil {
				c, found, err := writer.indexThread.Lookup("c")
				require.NoError(t, err)
				require.True(t, found)
				require.Equal(t, first.path, c.FilePath)
				require.EqualValues(t, 2, c.Position)
				d, found, err := writer.indexThread.Lookup("d")
				require.NoError(t, err)
				require.True(t, found)
				require.NotEqual(t, first.path, d.FilePath)
				require.Zero(t, d.Position)
			}
			require.NoError(t, writer.Close(ctx, map[string]any{"backfill": 1}))
			require.Len(t, writer.dataFiles, 2)
			require.Equal(t, map[string]int64{"a": 1, "b": 2, "c": 3, "d": 4}, nativeVisibleValues(t, writer.table))
			if index != nil {
				for id, expected := range map[string]int64{"a": 1, "b": 2, "c": 3, "d": 4} {
					loc := index.rows[id]
					rows, err := writer.ReadRows(ctx, loc.FilePath, []int64{loc.Position}, []string{"value"})
					require.NoError(t, err)
					require.Equal(t, expected, rows[loc.Position]["value"])
				}
			}
			cdc := openNativeTestWriter(ctx, t, cfg, stream, cat, index, "cdc", false)
			require.NoError(t, cdc.Write(ctx, []types.RawRecord{nativeTestRecord("b", "u", 20, "two"), nativeTestRecord("d", "u", 40, "one")}))
			require.NoError(t, cdc.Close(ctx, nil))
			require.Equal(t, map[string]int64{"a": 1, "b": 20, "c": 3, "d": 40}, nativeVisibleValues(t, cdc.table))
			if index != nil {
				positions, vectors, err := nativeDeletedPositions(ctx, cdc.table, first.path)
				require.NoError(t, err)
				require.Equal(t, []int64{1}, positions)
				if mode == types.UpdateTypeDeletionVector {
					require.Len(t, vectors, 1)
				} else {
					require.Empty(t, vectors)
				}
			} else {
				entries, err := nativeLiveEntries(ctx, cdc.table)
				require.NoError(t, err)
				found := false
				for _, entry := range entries {
					found = found || entry.DataFile().ContentType() == apacheiceberg.EntryContentEqDeletes
				}
				require.True(t, found)
			}
		})
	}
}

func TestNativeBackfillPartitionEvictionAndReadable(t *testing.T) {
	ctx := context.Background()
	cfg, stream, cat := nativeWriterFixture(t, types.UpdateTypePosition, true, "/{part, identity}")
	index := &nativeMemoryIndex{rows: make(map[string]types.RowLocation)}
	w := openNativeTestWriter(ctx, t, cfg, stream, cat, index, "eviction", true)
	w.backfillPartitionLimit = 2
	for _, row := range []types.RawRecord{nativeTestRecord("a", "r", 1, "a"), nativeTestRecord("b", "r", 2, "b"), nativeTestRecord("a2", "r", 3, "a")} {
		require.NoError(t, w.Write(ctx, []types.RawRecord{row}))
	}
	b, found, err := w.indexThread.Lookup("b")
	require.NoError(t, err)
	require.True(t, found)
	require.NoError(t, w.Write(ctx, []types.RawRecord{nativeTestRecord("c", "r", 4, "c")}))
	require.Len(t, w.backfillFiles, 2)
	require.Len(t, w.dataFiles, 1)
	require.Equal(t, b.FilePath, w.dataFiles[0].FilePath())
	a, found, err := w.indexThread.Lookup("a2")
	require.NoError(t, err)
	require.True(t, found)
	require.EqualValues(t, 1, a.Position)
	require.NoError(t, w.EnsureReadable(ctx, []string{a.FilePath}))
	rows, err := w.ReadRows(ctx, a.FilePath, []int64{0, 1}, []string{"value"})
	require.NoError(t, err)
	require.EqualValues(t, 1, rows[0]["value"])
	require.EqualValues(t, 3, rows[1]["value"])
	require.NoError(t, w.Write(ctx, []types.RawRecord{nativeTestRecord("a3", "r", 5, "a")}))
	a3, found, err := w.indexThread.Lookup("a3")
	require.NoError(t, err)
	require.True(t, found)
	require.NotEqual(t, a.FilePath, a3.FilePath)
	require.Zero(t, a3.Position)
	require.NoError(t, w.Close(ctx, nil))
	require.Equal(t, map[string]int64{"a": 1, "a2": 3, "a3": 5, "b": 2, "c": 4}, nativeVisibleValues(t, w.table))
}

func TestNativeBackfillSchemaChangesAndLargeRow(t *testing.T) {
	ctx := context.Background()
	cfg, stream, cat := nativeWriterFixture(t, types.UpdateTypePosition, true, "")
	index := &nativeMemoryIndex{rows: make(map[string]types.RowLocation)}
	w := openNativeTestWriter(ctx, t, cfg, stream, cat, index, "schema", true)
	require.NoError(t, w.Write(ctx, []types.RawRecord{nativeTestRecord("old", "r", 1, "p")}))
	columns := w.schemaMap()
	columns["payload"] = "string"
	require.NoError(t, w.EvolveSchema(ctx, columns))
	require.Len(t, w.dataFiles, 1)
	oldPath := w.dataFiles[0].FilePath()
	_, found := w.dataSchemas[oldPath].FindFieldByName("payload")
	require.False(t, found)
	large := strings.Repeat("large-row-", 800000)
	row := nativeTestRecord("large", "r", 2, "p")
	row.Data["payload"] = large
	require.NoError(t, w.Write(ctx, []types.RawRecord{row}))
	require.NoError(t, w.AlterSourceColumns(ctx, []*nativeSourceColumn{{Key: "added", IceType: "long"}}, nil))
	require.Len(t, w.dataFiles, 2)
	middle := w.dataFiles[1].FilePath()
	_, found = w.dataSchemas[middle].FindFieldByName("payload")
	require.True(t, found)
	_, found = w.dataSchemas[middle].FindFieldByName("added")
	require.False(t, found)
	last := nativeTestRecord("new", "r", 3, "p")
	last.Data["payload"], last.Data["added"] = "new", int64(7)
	require.NoError(t, w.Write(ctx, []types.RawRecord{last}))
	require.NoError(t, w.Close(ctx, nil))
	require.Equal(t, map[string]int64{"old": 1, "large": 2, "new": 3}, nativeVisibleValues(t, w.table))
	loc := index.rows["large"]
	read, err := w.ReadRows(ctx, loc.FilePath, []int64{loc.Position}, []string{"payload"})
	require.NoError(t, err)
	require.Equal(t, large, read[loc.Position]["payload"])
}

type nativeBackfillFaultFS struct {
	iceio.WriteFileIO
	failWrite  atomic.Bool
	failClose  atomic.Bool
	blockWrite atomic.Bool
	blockClose atomic.Bool
	entered    chan struct{}
	release    chan struct{}
	once       sync.Once
}

type nativeBackfillFaultWriter struct {
	iceio.FileWriter
	fs *nativeBackfillFaultFS
}

func (fs *nativeBackfillFaultFS) Create(path string) (iceio.FileWriter, error) {
	file, err := fs.WriteFileIO.Create(path)
	if err != nil {
		return nil, err
	}
	return &nativeBackfillFaultWriter{FileWriter: file, fs: fs}, nil
}

func (w *nativeBackfillFaultWriter) Write(data []byte) (int, error) {
	if w.fs.blockWrite.Load() {
		w.fs.once.Do(func() { close(w.fs.entered); <-w.fs.release })
	}
	if w.fs.failWrite.Load() {
		return 0, errors.New("injected fragment error")
	}
	return w.FileWriter.Write(data)
}

func (w *nativeBackfillFaultWriter) Close() error {
	if w.fs.blockClose.Load() {
		w.fs.once.Do(func() { close(w.fs.entered); <-w.fs.release })
	}
	err := w.FileWriter.Close()
	if w.fs.failClose.Load() {
		return errors.Join(err, errors.New("injected upload close error"))
	}
	return err
}

func nativeBackfillInjectFS(t *testing.T, w *nativeWriter) *nativeBackfillFaultFS {
	t.Helper()
	fs, err := w.table.FS(context.Background())
	require.NoError(t, err)
	writable, ok := fs.(iceio.WriteFileIO)
	require.True(t, ok)
	fault := &nativeBackfillFaultFS{WriteFileIO: writable, entered: make(chan struct{}), release: make(chan struct{})}
	w.table = table.New(w.table.Identifier(), w.table.Metadata(), w.table.MetadataLocation(), func(context.Context) (iceio.IO, error) { return fault, nil }, w.cat)
	return fault
}

func TestNativeBackfillFailureAbortAndRetry(t *testing.T) {
	for _, failure := range []string{"fragment", "close", "interrupted-close", "interrupted-write", "canceled-close", "abort"} {
		t.Run(failure, func(t *testing.T) {
			ctx := context.Background()
			cfg, stream, cat := nativeWriterFixture(t, types.UpdateTypePosition, true, "")
			index := &nativeMemoryIndex{rows: make(map[string]types.RowLocation)}
			w := openNativeTestWriter(ctx, t, cfg, stream, cat, index, "failed", true)
			fault := nativeBackfillInjectFS(t, w)
			require.NoError(t, w.Write(ctx, []types.RawRecord{nativeTestRecord("a", "r", 1, "p")}))
			loc, found, err := w.indexThread.Lookup("a")
			require.NoError(t, err)
			require.True(t, found)
			switch failure {
			case "fragment":
				fault.failWrite.Store(true)
				err = w.Write(ctx, []types.RawRecord{nativeTestRecord("b", "r", 2, "p")})
				require.ErrorContains(t, err, "injected fragment error")
			case "close":
				fault.failClose.Store(true)
				err = w.Close(ctx, map[string]any{"unsafe": true})
				require.ErrorContains(t, err, "injected upload close error")
			case "interrupted-close", "interrupted-write":
				if failure == "interrupted-close" {
					fault.blockClose.Store(true)
				} else {
					fault.blockWrite.Store(true)
				}
				workCtx, cancel := context.WithCancel(ctx)
				result := make(chan error, 1)
				go func() {
					if failure == "interrupted-close" {
						result <- w.Close(workCtx, map[string]any{"unsafe": true})
					} else {
						result <- w.Write(workCtx, []types.RawRecord{nativeTestRecord("b", "r", 2, "p")})
					}
				}()
				<-fault.entered
				cancel()
				close(fault.release)
				err = <-result
				require.ErrorIs(t, err, context.Canceled)
			case "canceled-close":
				workCtx, cancel := context.WithCancel(ctx)
				cancel()
				err = w.Close(workCtx, nil)
				require.ErrorIs(t, err, context.Canceled)
			case "abort":
				w.Abort()
			}
			require.Empty(t, index.rows)
			require.Zero(t, index.snapshot)
			require.Nil(t, w.table.CurrentSnapshot())
			_, openErr := fault.Open(loc.FilePath)
			require.Error(t, openErr)
			require.Error(t, w.Write(ctx, []types.RawRecord{nativeTestRecord("x", "r", 5, "p")}))
			if failure != "abort" {
				require.ErrorIs(t, w.Close(ctx, nil), err)
			}
			retry := openNativeTestWriter(ctx, t, cfg, stream, cat, index, "retry", true)
			require.NoError(t, retry.Write(ctx, []types.RawRecord{nativeTestRecord("a", "r", 1, "p")}))
			require.NoError(t, retry.Close(ctx, map[string]any{"safe": true}))
			require.Equal(t, map[string]int64{"a": 1}, nativeVisibleValues(t, retry.table))
		})
	}
}

func TestNativeBackfillCoalescesManyActualBatches(t *testing.T) {
	ctx := context.Background()
	cfg, stream, cat := nativeWriterFixture(t, types.UpdateTypeEquality, true, "")
	w := openNativeTestWriter(ctx, t, cfg, stream, cat, nil, "many-batches", true)
	const batches, rowsPerBatch = 64, 128
	for batch := range batches {
		rows := make([]types.RawRecord, rowsPerBatch)
		for row := range rows {
			n := batch*rowsPerBatch + row
			rows[row] = nativeTestRecord(fmt.Sprint(n), "r", int64(n), "p")
		}
		require.NoError(t, w.Write(ctx, rows))
	}
	require.Empty(t, w.dataFiles)
	require.Len(t, w.backfillFiles, 1)
	require.NoError(t, w.Close(ctx, nil))
	require.Len(t, w.dataFiles, 1)
	require.EqualValues(t, batches*rowsPerBatch, w.dataFiles[0].Count())
	require.Positive(t, w.dataFiles[0].FileSizeBytes())
	require.Len(t, nativeVisibleValues(t, w.table), batches*rowsPerBatch)
}

func TestNativeAtomicResyncBackfillRollsAndPublishes(t *testing.T) {
	ctx := resyncTestContext(t)
	cfg, stream, cat := nativeWriterFixture(t, types.UpdateTypeEquality, true, "")
	seed := openNativeTestWriter(ctx, t, cfg, stream, cat, nil, "seed", true)
	require.NoError(t, seed.Write(ctx, []types.RawRecord{nativeTestRecord("old", "r", 1, "p")}))
	require.NoError(t, seed.Close(ctx, nil))
	adapter := &Iceberg{config: cfg, catalog: cat}
	_, err := adapter.PrepareResync(ctx, stream, "rolling-resync", resyncTestState())
	require.NoError(t, err)
	w := openNativeTestWriter(ctx, t, cfg, stream, cat, nil, "resync", true)
	for n := range 3 {
		require.NoError(t, w.Write(ctx, []types.RawRecord{nativeTestRecord(fmt.Sprint(n), "r", int64(n), "p")}))
	}
	for _, file := range w.backfillFiles {
		w.backfillTargetBytes = file.output.size.Load() + 1
	}
	require.NoError(t, w.Write(ctx, []types.RawRecord{nativeTestRecord("3", "r", 3, "p")}))
	require.Len(t, w.dataFiles, 1)
	w.backfillTargetBytes = nativeBackfillTargetBytes
	require.NoError(t, w.Write(ctx, []types.RawRecord{nativeTestRecord("4", "r", 4, "p")}))
	require.NoError(t, w.Close(ctx, nil))
	require.Len(t, w.dataFiles, 2)
	require.NoError(t, adapter.PublishResync(ctx, resyncTestState()))
	published, err := cat.LoadTable(ctx, w.table.Identifier())
	require.NoError(t, err)
	require.Equal(t, map[string]int64{"0": 0, "1": 1, "2": 2, "3": 3, "4": 4}, nativeVisibleValues(t, published))
}

func TestNativeBackfillUnknownCommitRetainsFilesAndRepairsIndex(t *testing.T) {
	ctx := context.Background()
	cfg, stream, realCatalog := nativeWriterFixture(t, types.UpdateTypePosition, true, "")
	seed := openNativeTestWriter(ctx, t, cfg, stream, realCatalog, nil, "setup", true)
	seed.Abort()
	index := &nativeMemoryIndex{rows: make(map[string]types.RowLocation)}
	cat := &nativeCommitFaultCatalog{Catalog: realCatalog}
	w := openNativeTestWriter(ctx, t, cfg, stream, cat, index, "unknown", true)
	require.NoError(t, w.Write(ctx, []types.RawRecord{nativeTestRecord("a", "r", 1, "p")}))
	loc, found, err := w.indexThread.Lookup("a")
	require.NoError(t, err)
	require.True(t, found)
	cat.fault = func(ctx context.Context, id table.Identifier, reqs []table.Requirement, updates []table.Update) (bool, error) {
		_, _, err := realCatalog.CommitTable(ctx, id, reqs, updates)
		require.NoError(t, err)
		return true, errors.New("injected lost commit response")
	}
	err = w.Close(ctx, map[string]any{"state": "published"})
	require.ErrorContains(t, err, "outcome is unknown")
	require.True(t, w.uncertain)
	require.Empty(t, index.rows)
	w.Abort()
	fs, err := w.table.FS(ctx)
	require.NoError(t, err)
	file, err := fs.Open(loc.FilePath)
	require.NoError(t, err)
	require.NoError(t, file.Close())
	published, err := realCatalog.LoadTable(ctx, w.table.Identifier())
	require.NoError(t, err)
	require.Equal(t, map[string]int64{"a": 1}, nativeVisibleValues(t, published))
	require.Contains(t, published.Properties()[nativeRecoveryProperty], "published")
	recovered := openNativeTestWriter(ctx, t, cfg, stream, realCatalog, index, "recover", true)
	defer recovered.Abort()
	require.Equal(t, nativeSnapshotID(published), index.snapshot)
	require.Equal(t, loc, index.rows["a"])
}

func TestNativeBackfillIndexFollowsSourceOrderAcrossPartitions(t *testing.T) {
	ctx := context.Background()
	cfg, stream, cat := nativeWriterFixture(t, types.UpdateTypePosition, true, "/{part, identity}")
	index := &nativeMemoryIndex{rows: make(map[string]types.RowLocation)}
	w := openNativeTestWriter(ctx, t, cfg, stream, cat, index, "ordering", true)
	require.NoError(t, w.Write(ctx, []types.RawRecord{nativeTestRecord("same", "r", 1, "z"), nativeTestRecord("same", "r", 2, "a")}))
	require.NoError(t, w.Close(ctx, nil))
	loc := index.rows["same"]
	rows, err := w.ReadRows(ctx, loc.FilePath, []int64{loc.Position}, []string{"value", "part"})
	require.NoError(t, err)
	require.EqualValues(t, 2, rows[loc.Position]["value"])
	require.Equal(t, "a", rows[loc.Position]["part"])
}

func TestNativeResyncAbortCleansOpenBackfillStream(t *testing.T) {
	ctx := resyncTestContext(t)
	cfg, stream, cat := nativeWriterFixture(t, types.UpdateTypeEquality, true, "")
	seed := openNativeTestWriter(ctx, t, cfg, stream, cat, nil, "seed", true)
	require.NoError(t, seed.Write(ctx, []types.RawRecord{nativeTestRecord("old", "r", 1, "p")}))
	require.NoError(t, seed.Close(ctx, nil))
	adapter := &Iceberg{config: cfg, catalog: cat}
	_, err := adapter.PrepareResync(ctx, stream, "aborted-stream", resyncTestState())
	require.NoError(t, err)
	w := openNativeTestWriter(ctx, t, cfg, stream, cat, nil, "private", true)
	require.NoError(t, w.Write(ctx, []types.RawRecord{nativeTestRecord("new", "r", 2, "p")}))
	require.Len(t, w.backfillFiles, 1)
	var path string
	for _, f := range w.backfillFiles {
		path = f.path
	}
	w.Abort()
	fs, err := seed.table.FS(ctx)
	require.NoError(t, err)
	_, err = fs.Open(path)
	require.Error(t, err)
	require.Error(t, adapter.PublishResync(ctx, resyncTestState()))
	public, err := cat.LoadTable(ctx, seed.table.Identifier())
	require.NoError(t, err)
	require.Equal(t, map[string]int64{"old": 1}, nativeVisibleValues(t, public))
}

func TestNativeBackfillLargeRowWriteFailureRemainsRecoverable(t *testing.T) {
	ctx := context.Background()
	cfg, stream, cat := nativeWriterFixture(t, types.UpdateTypePosition, true, "")
	index := &nativeMemoryIndex{rows: make(map[string]types.RowLocation)}
	w := openNativeTestWriter(ctx, t, cfg, stream, cat, index, "large-failure", true)
	columns := w.schemaMap()
	columns["payload"] = "string"
	require.NoError(t, w.EvolveSchema(ctx, columns))
	fault := nativeBackfillInjectFS(t, w)
	require.NoError(t, w.Write(ctx, []types.RawRecord{nativeTestRecord("first", "r", 1, "p")}))
	loc, found, err := w.indexThread.Lookup("first")
	require.NoError(t, err)
	require.True(t, found)
	large := strings.Repeat("large-row-", 800000)
	row := nativeTestRecord("large", "r", 2, "p")
	row.Data["payload"] = large
	fault.failWrite.Store(true)
	err = w.Write(ctx, []types.RawRecord{row})
	require.ErrorContains(t, err, "injected fragment error")
	require.ErrorIs(t, w.Close(ctx, nil), err)
	require.Empty(t, index.rows)
	require.Zero(t, index.snapshot)
	_, err = fault.Open(loc.FilePath)
	require.Error(t, err)
	retry := openNativeTestWriter(ctx, t, cfg, stream, cat, index, "large-retry", true)
	require.NoError(t, retry.Write(ctx, []types.RawRecord{nativeTestRecord("first", "r", 1, "p"), row}))
	require.NoError(t, retry.Close(ctx, nil))
	require.Equal(t, map[string]int64{"first": 1, "large": 2}, nativeVisibleValues(t, retry.table))
	loc = index.rows["large"]
	rows, err := retry.ReadRows(ctx, loc.FilePath, []int64{loc.Position}, []string{"payload"})
	require.NoError(t, err)
	require.Equal(t, large, rows[loc.Position]["payload"])
}
