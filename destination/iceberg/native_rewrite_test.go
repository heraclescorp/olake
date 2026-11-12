package iceberg

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"testing"

	parquetfile "github.com/apache/arrow-go/v18/parquet/file"
	apacheiceberg "github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
	"github.com/datazip-inc/olake/types"
	"github.com/stretchr/testify/require"
)

func nativePendingLocation(t *testing.T, writer *nativeWriter, id string) types.RowLocation {
	t.Helper()
	if writer.indexThread == nil {
		location, found := writer.latest[id]
		require.True(t, found)
		return location
	}
	location, found, err := writer.indexThread.Lookup(id)
	require.NoError(t, err)
	require.True(t, found)
	return location
}

func TestNativeStagedRewritePreservesSchemaPartitionAndIndex(t *testing.T) {
	for _, mode := range []types.UpdateType{types.UpdateTypeEquality, types.UpdateTypePosition, types.UpdateTypeDeletionVector} {
		t.Run(string(mode), func(t *testing.T) {
			ctx := context.Background()
			cfg, stream, cat := nativeWriterFixture(t, mode, true, "/{part, identity}")
			var index *nativeMemoryIndex
			if mode != types.UpdateTypeEquality {
				index = &nativeMemoryIndex{rows: make(map[string]types.RowLocation)}
			}
			writer := openNativeTestWriter(t, ctx, cfg, stream, cat, index, "staged-rewrite", false)
			require.NoError(t, writer.Write(ctx, []types.RawRecord{nativeTestRecord("a", "u", 1, "one"), nativeTestRecord("b", "u", 2, "one"), nativeTestRecord("c", "u", 3, "two")}))
			originalB := nativePendingLocation(t, writer, "b")
			originalC := nativePendingLocation(t, writer, "c")
			require.Equal(t, int64(1), originalB.Position)
			// TOAST recovery can read an earlier write before Close. Compaction
			// must not alter that path or its ordinals during Write/evolution.
			require.NoError(t, writer.EnsureReadable(ctx, []string{originalB.FilePath, originalC.FilePath}))
			rows, err := writer.ReadRows(ctx, originalB.FilePath, []int64{0, originalB.Position}, []string{"value", "blob"})
			require.NoError(t, err)
			require.Equal(t, int64(1), rows[0]["value"])
			require.Equal(t, int64(2), rows[1]["value"])
			columns := writer.schemaMap()
			columns["later"] = "string"
			require.NoError(t, writer.EvolveSchema(ctx, columns))
			a := nativeTestRecord("a", "u", 4, "one")
			a.Data["later"] = "new-schema"
			require.NoError(t, writer.Write(ctx, []types.RawRecord{a, nativeTestRecord("c", "u", 5, "three")}))
			require.NoError(t, writer.EnsureReadable(ctx, []string{originalB.FilePath}))
			require.NoError(t, writer.Close(ctx, map[string]any{"state": "compacted"}))
			require.Equal(t, map[string]int64{"a": 4, "b": 2, "c": 5}, nativeVisibleValues(t, writer.table))
			finalB := writer.latest["b"]
			if index != nil {
				finalB = index.rows["b"]
				require.Equal(t, nativeSnapshotID(writer.table), index.snapshot)
			}
			require.NotEqual(t, originalB.FilePath, finalB.FilePath)
			require.Zero(t, finalB.Position)
			rows, err = writer.ReadRows(ctx, finalB.FilePath, []int64{finalB.Position}, []string{"value", "blob", "later"})
			require.NoError(t, err)
			require.Equal(t, int64(2), rows[0]["value"])
			require.Equal(t, []byte{0, 1, 2, 3}, rows[0]["blob"])
			require.Nil(t, rows[0]["later"])
			fs, err := writer.table.FS(ctx)
			require.NoError(t, err)
			for _, path := range []string{originalB.FilePath, originalC.FilePath} {
				_, err := fs.Open(path)
				require.Error(t, err)
			}
			entries, err := nativeLiveEntries(ctx, writer.table)
			require.NoError(t, err)
			for _, entry := range entries {
				require.Equal(t, apacheiceberg.EntryContentData, entry.DataFile().ContentType(), "own superseded rows require no delete file")
			}
		})
	}
}

type nativeRewriteFaultFS struct {
	iceio.WriteFileIO
	failAt  int
	creates int
	created []string
}

func (fs *nativeRewriteFaultFS) Create(path string) (iceio.FileWriter, error) {
	fs.creates++
	if fs.creates == fs.failAt {
		return nil, errors.New("injected staged rewrite upload failure")
	}
	writer, err := fs.WriteFileIO.Create(path)
	if err == nil {
		fs.created = append(fs.created, path)
	}
	return writer, err
}

func TestNativeStagedRewriteFailureKeepsPendingIndexAtomic(t *testing.T) {
	ctx := context.Background()
	cfg, stream, cat := nativeWriterFixture(t, types.UpdateTypePosition, true, "/{part, identity}")
	index := &nativeMemoryIndex{rows: make(map[string]types.RowLocation)}
	writer := openNativeTestWriter(t, ctx, cfg, stream, cat, index, "rewrite-fails", false)
	require.NoError(t, writer.Write(ctx, []types.RawRecord{nativeTestRecord("a", "u", 1, "one"), nativeTestRecord("x", "u", 2, "one"), nativeTestRecord("b", "u", 3, "two"), nativeTestRecord("y", "u", 4, "two")}))
	require.NoError(t, writer.Write(ctx, []types.RawRecord{nativeTestRecord("a", "u", 5, "three"), nativeTestRecord("b", "u", 6, "three")}))
	pending := writer.indexThread
	beforeX := nativePendingLocation(t, writer, "x")
	beforeY := nativePendingLocation(t, writer, "y")
	originalPaths := slices.Collect(maps.Keys(writer.paths))
	baseFS, err := writer.table.FS(ctx)
	require.NoError(t, err)
	writeFS, ok := baseFS.(iceio.WriteFileIO)
	require.True(t, ok)
	fault := &nativeRewriteFaultFS{WriteFileIO: writeFS, failAt: 2}
	writer.table = table.New(writer.table.Identifier(), writer.table.Metadata(), writer.table.MetadataLocation(), func(context.Context) (iceio.IO, error) { return fault, nil }, cat)
	err = writer.Close(ctx, map[string]any{"state": "must-not-commit"})
	require.ErrorContains(t, err, "injected staged rewrite upload failure")
	require.Len(t, fault.created, 1, "first replacement must succeed before the injected failure")
	afterX, found, err := pending.Lookup("x")
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, beforeX, afterX)
	afterY, found, err := pending.Lookup("y")
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, beforeY, afterY)
	require.Empty(t, index.rows)
	require.Zero(t, index.snapshot)
	for _, path := range append(originalPaths, fault.created...) {
		_, err := baseFS.Open(path)
		require.Error(t, err)
	}
	fresh, err := cat.LoadTable(ctx, writer.table.Identifier())
	require.NoError(t, err)
	require.Nil(t, fresh.CurrentSnapshot())
	require.NotContains(t, fresh.Properties()[nativeRecoveryProperty], "must-not-commit")
}

func TestNativeStagedRewriteAbortCleansReplacementFiles(t *testing.T) {
	ctx := context.Background()
	cfg, stream, cat := nativeWriterFixture(t, types.UpdateTypePosition, true, "")
	index := &nativeMemoryIndex{rows: make(map[string]types.RowLocation)}
	writer := openNativeTestWriter(t, ctx, cfg, stream, cat, index, "rewrite-abort", false)
	require.NoError(t, writer.Write(ctx, []types.RawRecord{nativeTestRecord("a", "u", 1, "one"), nativeTestRecord("b", "u", 2, "one"), nativeTestRecord("a", "u", 3, "one")}))
	original := nativePendingLocation(t, writer, "b")
	require.NoError(t, writer.compactStagedDataFiles(ctx))
	location := nativePendingLocation(t, writer, "b")
	require.NotEqual(t, original.FilePath, location.FilePath)
	require.Zero(t, location.Position)
	paths := slices.Collect(maps.Keys(writer.paths))
	require.NotEmpty(t, paths)
	writer.Abort()
	fs, err := writer.table.FS(ctx)
	require.NoError(t, err)
	for _, path := range paths {
		_, err := fs.Open(path)
		require.Error(t, err)
	}
	require.Empty(t, index.rows)
	require.Zero(t, index.snapshot)
}

func TestNativeStagedRewriteStreamsAcrossPhysicalBatches(t *testing.T) {
	ctx := context.Background()
	cfg, stream, cat := nativeWriterFixture(t, types.UpdateTypePosition, true, "")
	index := &nativeMemoryIndex{rows: make(map[string]types.RowLocation)}
	writer := openNativeTestWriter(t, ctx, cfg, stream, cat, index, "rewrite-batches", false)
	const count = 9000
	records := make([]types.RawRecord, count)
	for row := range records {
		records[row] = nativeTestRecord(fmt.Sprint(row), "u", int64(row), "one")
	}
	require.NoError(t, writer.Write(ctx, records))
	var updates []types.RawRecord
	for _, position := range []int{0, 8191, 8192, count - 1} {
		updates = append(updates, nativeTestRecord(fmt.Sprint(position), "u", int64(count+position), "one"))
	}
	require.NoError(t, writer.Write(ctx, updates))
	require.NoError(t, writer.Close(ctx, map[string]any{"state": "streamed"}))
	for _, position := range []int{1, 8190, 8193, count - 2} {
		location := index.rows[fmt.Sprint(position)]
		rows, err := writer.ReadRows(ctx, location.FilePath, []int64{location.Position}, []string{"value"})
		require.NoError(t, err)
		require.Equal(t, int64(position), rows[location.Position]["value"])
	}
	entries, err := nativeLiveEntries(ctx, writer.table)
	require.NoError(t, err)
	var total int64
	for _, entry := range entries {
		require.Equal(t, apacheiceberg.EntryContentData, entry.DataFile().ContentType())
		total += entry.DataFile().Count()
	}
	fs, err := writer.table.FS(ctx)
	require.NoError(t, err)
	for _, dataFile := range writer.dataFiles {
		input, err := fs.Open(dataFile.FilePath())
		require.NoError(t, err)
		reader, err := parquetfile.NewParquetReader(input)
		require.NoError(t, err)
		require.Equal(t, 1, reader.NumRowGroups(), "9,000-row rewrites must not inherit 8,192-row scan batch boundaries")
		require.NoError(t, reader.Close())
	}
	require.Equal(t, int64(count), total)
	require.Len(t, index.rows, count)
}

func TestNativeStagedRewriteUnknownCommitKeepsPublishedReplacements(t *testing.T) {
	ctx := context.Background()
	cfg, stream, realCatalog := nativeWriterFixture(t, types.UpdateTypeDeletionVector, true, "")
	index := &nativeMemoryIndex{rows: make(map[string]types.RowLocation)}
	seed := openNativeTestWriter(t, ctx, cfg, stream, realCatalog, index, "create", true)
	seed.Abort()
	cat := &nativeCommitFaultCatalog{Catalog: realCatalog}
	writer := openNativeTestWriter(t, ctx, cfg, stream, cat, index, "rewrite-unknown", false)
	require.NoError(t, writer.Write(ctx, []types.RawRecord{nativeTestRecord("a", "u", 1, "one"), nativeTestRecord("b", "u", 2, "one"), nativeTestRecord("a", "u", 3, "one")}))
	original := nativePendingLocation(t, writer, "b")
	cat.fault = func(ctx context.Context, id table.Identifier, reqs []table.Requirement, updates []table.Update) (bool, error) {
		_, _, err := realCatalog.CommitTable(ctx, id, reqs, updates)
		require.NoError(t, err)
		return true, errors.New("connection reset after publishing compacted data")
	}
	require.Error(t, writer.Close(ctx, map[string]any{"state": "published"}))
	require.True(t, writer.uncertain)
	writer.Abort()
	fs, err := writer.table.FS(ctx)
	require.NoError(t, err)
	for _, file := range writer.dataFiles {
		require.NotEqual(t, original.FilePath, file.FilePath())
		input, err := fs.Open(file.FilePath())
		require.NoError(t, err)
		require.NoError(t, input.Close())
	}
	require.Empty(t, index.rows)
	require.Zero(t, index.snapshot, "unknown outcomes must not checkpoint the local index")
	recovered := openNativeTestWriter(t, ctx, cfg, stream, realCatalog, index, "recover", false)
	require.Equal(t, map[string]int64{"a": 3, "b": 2}, nativeVisibleValues(t, recovered.table))
	require.Equal(t, int64(0), index.rows["b"].Position)
	require.Equal(t, nativeSnapshotID(recovered.table), index.snapshot)
	require.Contains(t, recovered.table.Properties()[nativeRecoveryProperty], "published")
	recovered.Abort()
}
