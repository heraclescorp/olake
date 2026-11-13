package iceberg

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	apacheiceberg "github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
	"github.com/apache/iceberg-go/table/dv"
	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/destination"
	"github.com/datazip-inc/olake/pkg/indexdb"
	"github.com/datazip-inc/olake/types"
	"github.com/stretchr/testify/require"
)

type nativeIndexTestCatalog struct{ metadata table.Metadata }

func (c *nativeIndexTestCatalog) LoadTable(context.Context, table.Identifier) (*table.Table, error) {
	return table.New([]string{"test", "index"}, c.metadata, "", func(context.Context) (iceio.IO, error) { return iceio.LocalFS{}, nil }, c), nil
}

func (c *nativeIndexTestCatalog) CommitTable(_ context.Context, _ table.Identifier, requirements []table.Requirement, updates []table.Update) (table.Metadata, string, error) {
	for _, requirement := range requirements {
		if err := requirement.Validate(c.metadata); err != nil {
			return nil, "", err
		}
	}
	metadata, err := table.UpdateTableMetadata(c.metadata, updates, "")
	if err != nil {
		return nil, "", err
	}
	c.metadata = metadata
	return metadata, "", nil
}

func nativeIndexTestTable(t *testing.T, version int) *table.Table {
	t.Helper()
	schema := apacheiceberg.NewSchema(0,
		apacheiceberg.NestedField{ID: 1, Name: constants.OlakeID, Type: apacheiceberg.PrimitiveTypes.String, Required: true},
		apacheiceberg.NestedField{ID: 2, Name: "payload", Type: apacheiceberg.PrimitiveTypes.String},
		apacheiceberg.NestedField{ID: 3, Name: constants.OpType, Type: apacheiceberg.PrimitiveTypes.String},
	)
	location := filepath.ToSlash(t.TempDir())
	metadata, err := table.NewMetadata(schema, apacheiceberg.UnpartitionedSpec, table.UnsortedSortOrder, location,
		apacheiceberg.Properties{table.PropertyFormatVersion: fmt.Sprint(version), table.ParquetRowGroupLimitKey: "2"})
	require.NoError(t, err)
	cat := &nativeIndexTestCatalog{metadata: metadata}
	tbl, err := cat.LoadTable(context.Background(), nil)
	require.NoError(t, err)
	return tbl
}

func appendNativeIndexRows(t *testing.T, tbl *table.Table, rows string) *table.Table {
	t.Helper()
	schema, err := table.SchemaToArrowSchema(tbl.Schema(), nil, true, false)
	require.NoError(t, err)
	data, err := array.TableFromJSON(memory.DefaultAllocator, schema, []string{rows})
	require.NoError(t, err)
	defer data.Release()
	reader := array.NewTableReader(data, -1)
	defer reader.Release()
	tbl, err = tbl.Append(context.Background(), reader, nil)
	require.NoError(t, err)
	return tbl
}

func nativeIndexRows(ids ...string) string {
	rows := "["
	for i, id := range ids {
		if i != 0 {
			rows += ","
		}
		rows += fmt.Sprintf(`{%q:%q,"payload":%q,%q:"u"}`, constants.OlakeID, id, "value-"+id, constants.OpType)
	}
	return rows + "]"
}

func openNativeTestIndex(t *testing.T) types.StreamIndex {
	t.Helper()
	t.Setenv(constants.IndexDBDir, t.TempDir())
	index, err := indexdb.Open(types.NewStream("events", "public", nil).Wrap(0))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, index.Close()) })
	return index
}

// Write bounded Arrow batches so large invalidation fixtures do not construct
// a second, JSON-encoded copy of every key or tiny table-configured row groups.
func writeNativeIndexData(t *testing.T, tbl *table.Table, path string, count int, key func(int) string) apacheiceberg.DataFile {
	t.Helper()
	schema, err := table.SchemaToArrowSchema(tbl.Schema(), nil, true, false)
	require.NoError(t, err)
	output, err := os.Create(path)
	require.NoError(t, err)
	writer, err := pqarrow.NewFileWriter(schema, output, parquet.NewWriterProperties(), pqarrow.DefaultWriterProps())
	require.NoError(t, err)
	builder := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer builder.Release()
	for start := 0; start < count; start += 8192 {
		end := min(start+8192, count)
		builder.Reserve(end - start)
		for row := start; row < end; row++ {
			id := key(row)
			builder.Field(0).(*array.StringBuilder).Append(id)
			builder.Field(1).(*array.StringBuilder).Append("value-" + id)
			builder.Field(2).(*array.StringBuilder).Append("u")
		}
		record := builder.NewRecordBatch()
		err := writer.Write(record)
		record.Release()
		require.NoError(t, err)
	}
	require.NoError(t, writer.Close())
	info, err := os.Stat(path)
	require.NoError(t, err)
	file, err := apacheiceberg.NewDataFileBuilder(*apacheiceberg.UnpartitionedSpec, apacheiceberg.EntryContentData, path, apacheiceberg.ParquetFile, nil, nil, nil, int64(count), info.Size())
	require.NoError(t, err)
	return file.Build()
}

func writeNativeIndexDelete(t *testing.T, projection *apacheiceberg.Schema, rows string, equality bool, count int64) apacheiceberg.DataFile {
	t.Helper()
	schema, err := table.SchemaToArrowSchema(projection, nil, true, false)
	require.NoError(t, err)
	data, err := array.TableFromJSON(memory.DefaultAllocator, schema, []string{rows})
	require.NoError(t, err)
	defer data.Release()
	path := filepath.Join(t.TempDir(), "delete.parquet")
	output, err := os.Create(path)
	require.NoError(t, err)
	writer, err := pqarrow.NewFileWriter(schema, output, parquet.NewWriterProperties(), pqarrow.DefaultWriterProps())
	require.NoError(t, err)
	reader := array.NewTableReader(data, -1)
	defer reader.Release()
	for reader.Next() {
		require.NoError(t, writer.Write(reader.RecordBatch()))
	}
	require.NoError(t, reader.Err())
	require.NoError(t, writer.Close())
	info, err := os.Stat(path)
	require.NoError(t, err)
	content := apacheiceberg.EntryContentPosDeletes
	if equality {
		content = apacheiceberg.EntryContentEqDeletes
	}
	builder, err := apacheiceberg.NewDataFileBuilder(*apacheiceberg.UnpartitionedSpec, content, path, apacheiceberg.ParquetFile, nil, nil, nil, count, info.Size())
	require.NoError(t, err)
	if equality {
		builder.EqualityFieldIDs([]int{1})
	}
	return builder.Build()
}

func commitNativeIndexDeletes(t *testing.T, tbl *table.Table, files ...apacheiceberg.DataFile) *table.Table {
	t.Helper()
	tx := tbl.NewTransaction()
	delta := tx.NewRowDelta(nil)
	for _, file := range files {
		delta.AddDeletes(file)
	}
	require.NoError(t, delta.Commit(context.Background()))
	tbl, err := tx.Commit(context.Background())
	require.NoError(t, err)
	return tbl
}

// Only data files explicitly denied by the test are blocked. Metadata,
// manifests, and delete files still use the real filesystem.
type nativeIndexReadScopeIO struct {
	iceio.IO
	denied map[string]bool
	mu     sync.Mutex
	opens  map[string]int
}

func (fs *nativeIndexReadScopeIO) Open(name string) (iceio.File, error) {
	fs.mu.Lock()
	if fs.opens != nil {
		fs.opens[name]++
	}
	fs.mu.Unlock()
	if fs.denied[name] {
		return nil, fmt.Errorf("untouched data file must not be read: %s", name)
	}
	return fs.IO.Open(name)
}

func nativeIndexWithReadScope(tbl *table.Table, denied ...string) *table.Table {
	fs := &nativeIndexReadScopeIO{IO: iceio.LocalFS{}, denied: make(map[string]bool, len(denied))}
	for _, path := range denied {
		fs.denied[path] = true
	}
	return table.New(tbl.Identifier(), tbl.Metadata(), tbl.MetadataLocation(), func(context.Context) (iceio.IO, error) { return fs, nil }, nil)
}

func requireNativeIndexMappings(t *testing.T, index types.StreamIndex, tbl *table.Table, expected map[string]types.RowLocation, absent ...string) {
	t.Helper()
	for key, want := range expected {
		got, found, err := index.Lookup(key)
		require.NoError(t, err)
		require.True(t, found, key)
		require.Equal(t, want, got, key)
		rows, err := readNativeRows(context.Background(), tbl, got.FilePath, []int64{got.Position}, []string{constants.OlakeID})
		require.NoError(t, err)
		require.Equal(t, key, rows[got.Position][constants.OlakeID])
	}
	for _, key := range absent {
		_, found, err := index.Lookup(key)
		require.NoError(t, err)
		require.False(t, found, key)
	}
	checkpoint, err := index.LastCommittedSnapshot()
	require.NoError(t, err)
	require.Equal(t, tbl.CurrentSnapshot().SnapshotID, checkpoint)
}

func requireNativeVisibleIDs(t *testing.T, tbl *table.Table, expected ...string) {
	t.Helper()
	scan := tbl.Scan()
	defer scan.Close()
	schema, records, err := scan.ToArrowRecords(context.Background())
	require.NoError(t, err)
	fields := schema.FieldIndices(constants.OlakeID)
	require.Len(t, fields, 1)
	var actual []string
	for record, err := range records {
		require.NoError(t, err)
		ids := record.Column(fields[0]).(*array.String)
		for row := range ids.Len() {
			actual = append(actual, ids.Value(row))
		}
		record.Release()
	}
	require.ElementsMatch(t, expected, actual)
}

func TestNativeIndexReplacementSkipsUntouchedFiles(t *testing.T) {
	ctx := context.Background()
	untouchedIDs := make([]string, 256)
	for row := range untouchedIDs {
		untouchedIDs[row] = fmt.Sprintf("untouched-%d", row)
	}
	tbl := appendNativeIndexRows(t, nativeIndexTestTable(t, 2), nativeIndexRows(untouchedIDs...))
	tbl = appendNativeIndexRows(t, tbl, nativeIndexRows("replace", "tail"))
	index := openNativeTestIndex(t)
	require.NoError(t, reconcileNativeTableIndex(ctx, index, tbl, constants.OlakeID))
	untouched, found, err := index.Lookup(untouchedIDs[0])
	require.NoError(t, err)
	require.True(t, found)
	previous, found, err := index.Lookup("replace")
	require.NoError(t, err)
	require.True(t, found)
	files, err := planNativeIndexFiles(ctx, tbl, tbl.CurrentSnapshot().SnapshotID)
	require.NoError(t, err)
	var removed apacheiceberg.DataFile
	for _, task := range files {
		if task.File.FilePath() == previous.FilePath {
			removed = task.File
		}
	}
	require.NotNil(t, removed)
	replacement := appendNativeIndexRows(t, nativeIndexTestTable(t, 2), nativeIndexRows("tail", "replace"))
	added, err := planNativeIndexFiles(ctx, replacement, replacement.CurrentSnapshot().SnapshotID)
	require.NoError(t, err)
	require.Len(t, added, 1)
	tx := tbl.NewTransaction()
	require.NoError(t, tx.ReplaceFiles(ctx, []apacheiceberg.DataFile{removed}, []apacheiceberg.DataFile{added[0].File}, nil, nil))
	tbl, err = tx.Commit(ctx)
	require.NoError(t, err)
	require.NoError(t, reconcileNativeTableIndex(ctx, index, nativeIndexWithReadScope(tbl, untouched.FilePath, removed.FilePath()), constants.OlakeID))
	expected := map[string]types.RowLocation{
		"tail":    {FilePath: added[0].File.FilePath(), Position: 0},
		"replace": {FilePath: added[0].File.FilePath(), Position: 1},
	}
	for row, key := range untouchedIDs {
		expected[key] = types.RowLocation{FilePath: untouched.FilePath, Position: int64(row)}
	}
	requireNativeIndexMappings(t, index, tbl, expected)
	requireNativeVisibleIDs(t, tbl, append(untouchedIDs, "tail", "replace")...)
}

func TestNativeIndexChangedDeletesSkipUntouchedNewerMappings(t *testing.T) {
	for _, version := range []int{2, 3} {
		t.Run(fmt.Sprintf("v%d", version), func(t *testing.T) {
			ctx := context.Background()
			tbl := appendNativeIndexRows(t, nativeIndexTestTable(t, version), nativeIndexRows("duplicate", "removed", "other", "tail"))
			files, err := planNativeIndexFiles(ctx, tbl, tbl.CurrentSnapshot().SnapshotID)
			require.NoError(t, err)
			require.Len(t, files, 1)
			affected := files[0].File.FilePath()
			tbl = appendNativeIndexRows(t, tbl, nativeIndexRows("duplicate", "removed", "other", "untouched"))
			index := openNativeTestIndex(t)
			require.NoError(t, reconcileNativeTableIndex(ctx, index, tbl, constants.OlakeID))
			newer, found, err := index.Lookup("duplicate")
			require.NoError(t, err)
			require.True(t, found)
			expected := map[string]types.RowLocation{"tail": {FilePath: affected, Position: 3}}
			for position, key := range []string{"duplicate", "removed", "other", "untouched"} {
				expected[key] = types.RowLocation{FilePath: newer.FilePath, Position: int64(position)}
			}
			var previousDeletes []apacheiceberg.DataFile
			for step := 1; step <= 2; step++ {
				var deletes []apacheiceberg.DataFile
				if version == 3 {
					writer := dv.NewDVWriter(iceio.LocalFS{}, func(int32) *apacheiceberg.PartitionSpec { return apacheiceberg.UnpartitionedSpec })
					positions := []int64{1}
					if step == 2 {
						positions = append(positions, 2)
					}
					require.NoError(t, writer.Add(affected, positions, 0, nil))
					deletes, err = writer.Flush(ctx, filepath.Join(t.TempDir(), "changed.puffin"))
					require.NoError(t, err)
				} else {
					deletes = []apacheiceberg.DataFile{writeNativeIndexDelete(t, apacheiceberg.PositionalDeleteSchema, fmt.Sprintf(`[{"file_path":%q,"pos":%d}]`, affected, step), false, 1)}
				}
				tx := tbl.NewTransaction()
				delta := tx.NewRowDelta(nil).AddDeletes(deletes...)
				if version == 3 {
					delta.RemoveDeletes(previousDeletes...)
				}
				require.NoError(t, delta.Commit(ctx))
				tbl, err = tx.Commit(ctx)
				require.NoError(t, err)
				previousDeletes = deletes
				require.NoError(t, reconcileNativeTableIndex(ctx, index, nativeIndexWithReadScope(tbl, newer.FilePath), constants.OlakeID))
				requireNativeIndexMappings(t, index, tbl, expected)
				visible := []string{"duplicate", "tail", "duplicate", "removed", "other", "untouched"}
				if step == 1 {
					visible = append(visible, "other")
				}
				requireNativeVisibleIDs(t, tbl, visible...)
			}
		})
	}
}

func TestNativeIndexPreservedSequenceRewriteRechecksPathRank(t *testing.T) {
	ctx := context.Background()
	tbl := nativeIndexTestTable(t, 2)
	dir := t.TempDir()
	key := func(int) string { return "k" }
	a := writeNativeIndexData(t, tbl, filepath.Join(dir, "a.parquet"), 1, key)
	b := writeNativeIndexData(t, tbl, filepath.Join(dir, "b.parquet"), 1, key)
	c := writeNativeIndexData(t, tbl, filepath.Join(dir, "0-c.parquet"), 1, key)
	tx := tbl.NewTransaction()
	require.NoError(t, tx.ReplaceFiles(ctx, nil, []apacheiceberg.DataFile{a, b}, nil, nil))
	tbl, err := tx.Commit(ctx)
	require.NoError(t, err)
	sequence := tbl.CurrentSnapshot().SequenceNumber
	index := openNativeTestIndex(t)
	require.NoError(t, reconcileNativeTableIndex(ctx, index, tbl, constants.OlakeID))
	requireNativeIndexMappings(t, index, tbl, map[string]types.RowLocation{"k": {FilePath: b.FilePath(), Position: 0}})
	tx = tbl.NewTransaction()
	require.NoError(t, tx.NewRewrite(nil).DeleteFile(b).AddDataFile(c).DataSequenceNumber(sequence).Commit(ctx))
	tbl, err = tx.Commit(ctx)
	require.NoError(t, err)
	require.NoError(t, reconcileNativeTableIndex(ctx, index, nativeIndexWithReadScope(tbl, b.FilePath()), constants.OlakeID))
	cold := openNativeTestIndex(t)
	require.NoError(t, reconcileNativeTableIndex(ctx, cold, tbl, constants.OlakeID))
	expected := map[string]types.RowLocation{"k": {FilePath: a.FilePath(), Position: 0}}
	requireNativeIndexMappings(t, cold, tbl, expected)
	requireNativeIndexMappings(t, index, tbl, expected)
}

func TestNativeIndexShrinkingAndRemovedDeletesRestoreRowsWithoutReadingNewerFile(t *testing.T) {
	for _, version := range []int{2, 3} {
		t.Run(fmt.Sprintf("v%d", version), func(t *testing.T) {
			ctx := context.Background()
			tbl := appendNativeIndexRows(t, nativeIndexTestTable(t, version), nativeIndexRows("duplicate", "shrink", "restore", "tail"))
			files, err := planNativeIndexFiles(ctx, tbl, tbl.CurrentSnapshot().SnapshotID)
			require.NoError(t, err)
			require.Len(t, files, 1)
			affected := files[0].File.FilePath()
			tbl = appendNativeIndexRows(t, tbl, nativeIndexRows("duplicate", "untouched"))
			index := openNativeTestIndex(t)
			var previousDeletes []apacheiceberg.DataFile
			for step, positions := range [][]int64{{0, 1, 2}, {0, 2}, nil} {
				var deletes []apacheiceberg.DataFile
				if len(positions) != 0 {
					if version == 3 {
						writer := dv.NewDVWriter(iceio.LocalFS{}, func(int32) *apacheiceberg.PartitionSpec { return apacheiceberg.UnpartitionedSpec })
						require.NoError(t, writer.Add(affected, positions, 0, nil))
						deletes, err = writer.Flush(ctx, filepath.Join(t.TempDir(), "restored.puffin"))
						require.NoError(t, err)
					} else {
						for _, position := range positions {
							deletes = append(deletes, writeNativeIndexDelete(t, apacheiceberg.PositionalDeleteSchema, fmt.Sprintf(`[{"file_path":%q,"pos":%d}]`, affected, position), false, 1))
						}
					}
				}
				tx := tbl.NewTransaction()
				if len(previousDeletes) == 0 {
					require.NoError(t, tx.NewRowDelta(nil).AddDeletes(deletes...).Commit(ctx))
				} else {
					rewrite := tx.NewRewrite(nil)
					for _, file := range previousDeletes {
						rewrite.DeleteFile(file)
					}
					for _, file := range deletes {
						rewrite.AddDeleteFile(file)
					}
					require.NoError(t, rewrite.Commit(ctx))
				}
				tbl, err = tx.Commit(ctx)
				require.NoError(t, err)
				previousDeletes = deletes
				if step == 0 {
					require.NoError(t, reconcileNativeTableIndex(ctx, index, tbl, constants.OlakeID))
				}
				newer, found, err := index.Lookup("duplicate")
				require.NoError(t, err)
				require.True(t, found)
				if step != 0 {
					require.NoError(t, reconcileNativeTableIndex(ctx, index, nativeIndexWithReadScope(tbl, newer.FilePath), constants.OlakeID))
				}
				expected := map[string]types.RowLocation{
					"duplicate": newer,
					"untouched": {FilePath: newer.FilePath, Position: 1},
					"tail":      {FilePath: affected, Position: 3},
				}
				absent := []string{"shrink", "restore"}
				visible := []string{"tail", "duplicate", "untouched"}
				if step >= 1 {
					expected["shrink"] = types.RowLocation{FilePath: affected, Position: 1}
					absent = []string{"restore"}
					visible = append(visible, "shrink")
				}
				if step == 2 {
					expected["restore"] = types.RowLocation{FilePath: affected, Position: 2}
					absent = nil
					visible = append(visible, "duplicate", "restore")
				}
				requireNativeIndexMappings(t, index, tbl, expected, absent...)
				requireNativeVisibleIDs(t, tbl, visible...)
			}
		})
	}
}

func TestNativeIndexBulkInvalidationReadsSurvivingFileAtMostOnce(t *testing.T) {
	ctx := context.Background()
	tbl := appendNativeIndexRows(t, nativeIndexTestTable(t, 2), nativeIndexRows("survivor"))
	files, err := planNativeIndexFiles(ctx, tbl, tbl.CurrentSnapshot().SnapshotID)
	require.NoError(t, err)
	require.Len(t, files, 1)
	survivor := files[0].File.FilePath()
	// More than two former 200k-key fallback chunks must not rescan survivor.
	const invalidKeys = 400001
	removed := writeNativeIndexData(t, tbl, filepath.Join(t.TempDir(), "invalidated.parquet"), invalidKeys, func(row int) string { return fmt.Sprintf("invalid-%06d", row) })
	tx := tbl.NewTransaction()
	require.NoError(t, tx.ReplaceFiles(ctx, nil, []apacheiceberg.DataFile{removed}, nil, nil))
	tbl, err = tx.Commit(ctx)
	require.NoError(t, err)
	index := openNativeTestIndex(t)
	require.NoError(t, reconcileNativeTableIndex(ctx, index, tbl, constants.OlakeID))
	removedPaths := map[string]struct{}{removed.FilePath(): {}}
	count := 0
	require.NoError(t, index.RangeFiles(removedPaths, func(string, types.RowLocation) error { count++; return nil }))
	require.Equal(t, invalidKeys, count)
	tx = tbl.NewTransaction()
	require.NoError(t, tx.ReplaceFiles(ctx, []apacheiceberg.DataFile{removed}, nil, nil, nil))
	tbl, err = tx.Commit(ctx)
	require.NoError(t, err)
	fs := &nativeIndexReadScopeIO{IO: iceio.LocalFS{}, denied: map[string]bool{removed.FilePath(): true}, opens: make(map[string]int)}
	scoped := table.New(tbl.Identifier(), tbl.Metadata(), tbl.MetadataLocation(), func(context.Context) (iceio.IO, error) { return fs, nil }, nil)
	require.NoError(t, reconcileNativeTableIndex(ctx, index, scoped, constants.OlakeID))
	fs.mu.Lock()
	survivorOpens := fs.opens[survivor]
	fs.mu.Unlock()
	require.LessOrEqual(t, survivorOpens, 1, "bulk repair must not rescan surviving Parquet per invalid-key chunk")
	count = 0
	require.NoError(t, index.RangeFiles(removedPaths, func(string, types.RowLocation) error { count++; return nil }))
	require.Zero(t, count)
	requireNativeIndexMappings(t, index, tbl, map[string]types.RowLocation{"survivor": {FilePath: survivor, Position: 0}}, "invalid-000000", "invalid-200000", "invalid-400000")
}

func TestNativeIndexAffectedNewestFallsBackToSurvivingDuplicate(t *testing.T) {
	for _, change := range []string{"removal", "position-delete", "deletion-vector"} {
		t.Run(change, func(t *testing.T) {
			ctx := context.Background()
			version := 2
			if change == "deletion-vector" {
				version = 3
			}
			tbl := appendNativeIndexRows(t, nativeIndexTestTable(t, version), nativeIndexRows("untouched", "duplicate"))
			index := openNativeTestIndex(t)
			require.NoError(t, reconcileNativeTableIndex(ctx, index, tbl, constants.OlakeID))
			older, found, err := index.Lookup("duplicate")
			require.NoError(t, err)
			require.True(t, found)
			tbl = appendNativeIndexRows(t, tbl, nativeIndexRows("duplicate", "stale"))
			require.NoError(t, reconcileNativeTableIndex(ctx, index, tbl, constants.OlakeID))
			newest, found, err := index.Lookup("duplicate")
			require.NoError(t, err)
			require.True(t, found)
			require.NotEqual(t, older.FilePath, newest.FilePath)
			switch change {
			case "removal":
				files, err := planNativeIndexFiles(ctx, tbl, tbl.CurrentSnapshot().SnapshotID)
				require.NoError(t, err)
				var removed apacheiceberg.DataFile
				for _, task := range files {
					if task.File.FilePath() == newest.FilePath {
						removed = task.File
					}
				}
				require.NotNil(t, removed)
				tx := tbl.NewTransaction()
				require.NoError(t, tx.ReplaceFiles(ctx, []apacheiceberg.DataFile{removed}, nil, nil, nil))
				tbl, err = tx.Commit(ctx)
				require.NoError(t, err)
			case "position-delete":
				deleted := writeNativeIndexDelete(t, apacheiceberg.PositionalDeleteSchema, fmt.Sprintf(`[{"file_path":%q,"pos":0},{"file_path":%q,"pos":1}]`, newest.FilePath, newest.FilePath), false, 2)
				tbl = commitNativeIndexDeletes(t, tbl, deleted)
			case "deletion-vector":
				writer := dv.NewDVWriter(iceio.LocalFS{}, func(int32) *apacheiceberg.PartitionSpec { return apacheiceberg.UnpartitionedSpec })
				require.NoError(t, writer.Add(newest.FilePath, []int64{0, 1}, 0, nil))
				deletes, err := writer.Flush(ctx, filepath.Join(t.TempDir(), "fallback.puffin"))
				require.NoError(t, err)
				tbl = commitNativeIndexDeletes(t, tbl, deletes...)
			}
			reconcileTable := tbl
			if change == "removal" {
				reconcileTable = nativeIndexWithReadScope(tbl, newest.FilePath)
			}
			require.NoError(t, reconcileNativeTableIndex(ctx, index, reconcileTable, constants.OlakeID))
			requireNativeIndexMappings(t, index, tbl, map[string]types.RowLocation{
				"duplicate": older,
				"untouched": {FilePath: older.FilePath, Position: 0},
			}, "stale")
			requireNativeVisibleIDs(t, tbl, "untouched", "duplicate")
		})
	}
}

func TestNativeIndexNewestWinsAndPhysicalRows(t *testing.T) {
	ctx := context.Background()
	tbl := appendNativeIndexRows(t, nativeIndexTestTable(t, 2), nativeIndexRows("duplicate", "removed", "third", "duplicate", "tail"))
	index := openNativeTestIndex(t)
	require.NoError(t, reconcileNativeTableIndex(ctx, index, tbl, constants.OlakeID))
	old, found, err := index.Lookup("duplicate")
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, int64(3), old.Position)
	rows, err := readNativeRows(ctx, tbl, old.FilePath, []int64{4, 3}, []string{"payload", "missing_column"})
	require.NoError(t, err)
	require.Equal(t, "value-duplicate", rows[3]["payload"])
	require.Equal(t, "value-tail", rows[4]["payload"])
	require.NotContains(t, rows[3], "missing_column")

	tbl = appendNativeIndexRows(t, tbl, nativeIndexRows("duplicate", "new"))
	require.NoError(t, reconcileNativeTableIndex(ctx, index, tbl, constants.OlakeID))
	latest, found, err := index.Lookup("duplicate")
	require.NoError(t, err)
	require.True(t, found)
	require.NotEqual(t, old.FilePath, latest.FilePath)
	require.Zero(t, latest.Position)
	checkpoint, err := index.LastCommittedSnapshot()
	require.NoError(t, err)
	require.Equal(t, tbl.CurrentSnapshot().SnapshotID, checkpoint)
}

func TestNativeIndexChangedPositionDeletesRemoveRows(t *testing.T) {
	ctx := context.Background()
	tbl := appendNativeIndexRows(t, nativeIndexTestTable(t, 2), nativeIndexRows("first", "removed", "tail"))
	index := openNativeTestIndex(t)
	require.NoError(t, reconcileNativeTableIndex(ctx, index, tbl, constants.OlakeID))
	location, _, err := index.Lookup("removed")
	require.NoError(t, err)
	deleted := writeNativeIndexDelete(t, apacheiceberg.PositionalDeleteSchema, fmt.Sprintf(`[{"file_path":%q,"pos":1}]`, location.FilePath), false, 1)
	tbl = commitNativeIndexDeletes(t, tbl, deleted)
	require.NoError(t, reconcileNativeTableIndex(ctx, index, tbl, constants.OlakeID))
	_, found, err := index.Lookup("removed")
	require.NoError(t, err)
	require.False(t, found)
	tail, found, err := index.Lookup("tail")
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, int64(2), tail.Position, "deletes must not renumber survivors")
	positions, err := readNativePositionDeletes(ctx, tbl, deleted.FilePath())
	require.NoError(t, err)
	require.Equal(t, []int64{1}, positions[location.FilePath])
}

func TestNativeIndexEqualityMigrationRespectsSequence(t *testing.T) {
	ctx := context.Background()
	tbl := appendNativeIndexRows(t, nativeIndexTestTable(t, 2), nativeIndexRows("deleted", "live", "deleted"))
	oldFiles, err := planNativeIndexFiles(ctx, tbl, tbl.CurrentSnapshot().SnapshotID)
	require.NoError(t, err)
	projection, err := tbl.Schema().Select(true, constants.OlakeID)
	require.NoError(t, err)
	deleted := writeNativeIndexDelete(t, projection, fmt.Sprintf(`[{%q:"deleted"}]`, constants.OlakeID), true, 1)
	tbl = commitNativeIndexDeletes(t, tbl, deleted)
	tbl = appendNativeIndexRows(t, tbl, nativeIndexRows("deleted"))
	positions, oldDeletes, err := collectNativeEqualityDeletePositions(ctx, tbl)
	require.NoError(t, err)
	require.Len(t, oldDeletes, 1)
	require.Equal(t, map[string][]int64{oldFiles[0].File.FilePath(): {0, 2}}, positions)
	index := openNativeTestIndex(t)
	require.NoError(t, reconcileNativeTableIndex(ctx, index, tbl, constants.OlakeID))
	latest, found, err := index.Lookup("deleted")
	require.NoError(t, err)
	require.True(t, found)
	require.NotEqual(t, oldFiles[0].File.FilePath(), latest.FilePath)
}

func TestNativeIndexDeletionVectorKeepsPhysicalOrdinals(t *testing.T) {
	ctx := context.Background()
	tbl := appendNativeIndexRows(t, nativeIndexTestTable(t, 3), nativeIndexRows("first", "removed", "tail"))
	files, err := planNativeIndexFiles(ctx, tbl, tbl.CurrentSnapshot().SnapshotID)
	require.NoError(t, err)
	writer := dv.NewDVWriter(iceio.LocalFS{}, func(int32) *apacheiceberg.PartitionSpec { return apacheiceberg.UnpartitionedSpec })
	require.NoError(t, writer.Add(files[0].File.FilePath(), []int64{1}, 0, nil))
	deletes, err := writer.Flush(ctx, filepath.Join(t.TempDir(), "delete.puffin"))
	require.NoError(t, err)
	tbl = commitNativeIndexDeletes(t, tbl, deletes...)
	index := openNativeTestIndex(t)
	require.NoError(t, reconcileNativeTableIndex(ctx, index, tbl, constants.OlakeID))
	_, found, err := index.Lookup("removed")
	require.NoError(t, err)
	require.False(t, found)
	location, found, err := index.Lookup("tail")
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, int64(2), location.Position)
}

type nativeIndexFailCheckpoint struct {
	types.StreamIndex
	fail bool
}

func (index *nativeIndexFailCheckpoint) Commit(batch *types.StreamIndexThread, checkpoint *int64) error {
	if index.fail && checkpoint != nil && *checkpoint != 0 {
		return errors.New("checkpoint unavailable")
	}
	return index.StreamIndex.Commit(batch, checkpoint)
}

func TestNativeIndexFailedCheckpointDoesNotAdvance(t *testing.T) {
	ctx := context.Background()
	tbl := appendNativeIndexRows(t, nativeIndexTestTable(t, 2), nativeIndexRows("before"))
	index := &nativeIndexFailCheckpoint{StreamIndex: openNativeTestIndex(t)}
	require.NoError(t, reconcileNativeTableIndex(ctx, index, tbl, constants.OlakeID))
	before := tbl.CurrentSnapshot().SnapshotID
	tbl = appendNativeIndexRows(t, tbl, nativeIndexRows("after"))
	index.fail = true
	require.ErrorContains(t, reconcileNativeTableIndex(ctx, index, tbl, constants.OlakeID), "checkpoint unavailable")
	checkpoint, err := index.LastCommittedSnapshot()
	require.NoError(t, err)
	require.Equal(t, before, checkpoint)
	index.fail = false
	require.NoError(t, reconcileNativeTableIndex(ctx, index, tbl, constants.OlakeID))
	_, found, err := index.Lookup("after")
	require.NoError(t, err)
	require.True(t, found)
}

func TestNativeIndexRejectsMissingCheckpointAncestor(t *testing.T) {
	ctx := context.Background()
	tbl := appendNativeIndexRows(t, nativeIndexTestTable(t, 2), nativeIndexRows("live"))
	index := openNativeTestIndex(t)
	checkpoint := int64(777)
	pending := types.NewStreamIndexThread(index)
	pending.Put("retained", types.RowLocation{FilePath: "previous.parquet", Position: 4})
	require.NoError(t, index.Commit(pending, &checkpoint))
	require.ErrorContains(t, reconcileNativeTableIndex(ctx, index, tbl, constants.OlakeID), "not an ancestor")
	location, found, err := index.Lookup("retained")
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, int64(4), location.Position, "unsafe ancestry must not mutate the index")
}

func TestNativeRowsFollowRenamedFieldIDs(t *testing.T) {
	ctx := context.Background()
	tbl := appendNativeIndexRows(t, nativeIndexTestTable(t, 2), nativeIndexRows("live"))
	tasks, err := planNativeIndexFiles(ctx, tbl, tbl.CurrentSnapshot().SnapshotID)
	require.NoError(t, err)
	tx := tbl.NewTransaction()
	require.NoError(t, tx.UpdateSchema(true, false).RenameColumn([]string{"payload"}, "renamed").Commit())
	tbl, err = tx.Commit(ctx)
	require.NoError(t, err)
	rows, err := readNativeRows(ctx, tbl, tasks[0].File.FilePath(), []int64{0}, []string{"renamed"})
	require.NoError(t, err)
	require.Equal(t, "value-live", rows[0]["renamed"])
	_, err = readNativeRows(ctx, tbl, tasks[0].File.FilePath(), []int64{-1}, []string{"renamed"})
	require.ErrorContains(t, err, "must not be negative")
	_, err = readNativeRows(ctx, tbl, tasks[0].File.FilePath(), []int64{99}, []string{"renamed"})
	require.ErrorContains(t, err, "beyond the data file")
}

func TestNativeIndexPartialFlushThenRewriteRebuilds(t *testing.T) {
	ctx := context.Background()
	tbl := appendNativeIndexRows(t, nativeIndexTestTable(t, 2), nativeIndexRows("retained"))
	index := &nativeIndexFailCheckpoint{StreamIndex: openNativeTestIndex(t)}
	require.NoError(t, reconcileNativeTableIndex(ctx, index, tbl, constants.OlakeID))
	tx := tbl.NewTransaction()
	require.NoError(t, tx.SetProperties(apacheiceberg.Properties{table.ParquetRowGroupLimitKey: "100000"}))
	tbl, err := tx.Commit(ctx)
	require.NoError(t, err)
	schema, err := table.SchemaToArrowSchema(tbl.Schema(), nil, true, false)
	require.NoError(t, err)
	builder := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer builder.Release()
	builder.Reserve(indexScanFlushRows + 1)
	for range indexScanFlushRows + 1 {
		builder.Field(0).(*array.StringBuilder).Append("partial")
		builder.Field(1).(*array.StringBuilder).Append("value")
		builder.Field(2).(*array.StringBuilder).Append("u")
	}
	batch := builder.NewRecordBatch()
	defer batch.Release()
	reader, err := array.NewRecordReader(schema, []arrow.RecordBatch{batch})
	require.NoError(t, err)
	defer reader.Release()
	tbl, err = tbl.Append(ctx, reader, nil)
	require.NoError(t, err)
	index.fail = true
	require.ErrorContains(t, reconcileNativeTableIndex(ctx, index, tbl, constants.OlakeID), "checkpoint unavailable")
	checkpoint, err := index.LastCommittedSnapshot()
	require.NoError(t, err)
	require.Zero(t, checkpoint, "partial publication must invalidate the old checkpoint")
	partial, found, err := index.Lookup("partial")
	require.NoError(t, err)
	require.True(t, found, "the bounded flush persisted rows before the failed final checkpoint")
	tasks, err := planNativeIndexFiles(ctx, tbl, tbl.CurrentSnapshot().SnapshotID)
	require.NoError(t, err)
	var removed apacheiceberg.DataFile
	for _, task := range tasks {
		if task.File.FilePath() == partial.FilePath {
			removed = task.File
		}
	}
	require.NotNil(t, removed)
	tx = tbl.NewTransaction()
	require.NoError(t, tx.ReplaceFiles(ctx, []apacheiceberg.DataFile{removed}, nil, nil, nil))
	tbl, err = tx.Commit(ctx)
	require.NoError(t, err)
	index.fail = false
	require.NoError(t, reconcileNativeTableIndex(ctx, index, tbl, constants.OlakeID))
	_, found, err = index.Lookup("partial")
	require.NoError(t, err)
	require.False(t, found, "rewrite recovery must remove partial-scan keys absent from the table")
	_, found, err = index.Lookup("retained")
	require.NoError(t, err)
	require.True(t, found)
}

func TestNativeIndexedCreateReadSupersedesPendingVersions(t *testing.T) {
	for _, mode := range []types.UpdateType{types.UpdateTypePosition, types.UpdateTypeDeletionVector} {
		for _, testCase := range []struct {
			name           string
			operations     []string
			separateWrites bool
		}{
			{name: "create-delete-create-batch", operations: []string{"c", "d", "c"}},
			{name: "create-delete-create-writes", operations: []string{"c", "d", "c"}, separateWrites: true},
			{name: "create-delete-read", operations: []string{"c", "d", "r"}},
			{name: "read-create-writes", operations: []string{"r", "c"}, separateWrites: true},
		} {
			t.Run(string(mode)+"/"+testCase.name, func(t *testing.T) {
				ctx := context.Background()
				cfg, stream, cat := nativeWriterFixture(t, mode, true, "/{part, identity}")
				index := &nativeMemoryIndex{rows: make(map[string]types.RowLocation)}
				seed := openNativeTestWriter(ctx, t, cfg, stream, cat, index, "seed", true)
				require.NoError(t, seed.Write(ctx, []types.RawRecord{nativeTestRecord("seed", "r", 0, "seed")}))
				require.NoError(t, seed.Close(ctx, nil))
				writer := openNativeTestWriter(ctx, t, cfg, stream, cat, index, "pending-versions", false)
				var records []types.RawRecord
				for idx, operation := range testCase.operations {
					records = append(records, nativeTestRecord("a", operation, int64(idx+1), fmt.Sprint(idx)))
				}
				if testCase.separateWrites {
					for _, record := range records {
						require.NoError(t, writer.Write(ctx, []types.RawRecord{record}))
					}
				} else {
					require.NoError(t, writer.Write(ctx, records))
				}
				require.Nil(t, writer.indexReads, "events that only supersede this thread must not retain committed-index reads")
				require.Len(t, writer.indexTouched, 1)
				require.NoError(t, writer.Close(ctx, map[string]any{"state": "latest-event"}))
				require.Equal(t, map[string]int64{"seed": 0, "a": int64(len(records))}, nativeVisibleValues(t, writer.table))
				location := index.rows["a"]
				rows, err := writer.ReadRows(ctx, location.FilePath, []int64{location.Position}, []string{constants.OpType})
				require.NoError(t, err)
				require.Equal(t, testCase.operations[len(testCase.operations)-1], rows[location.Position][constants.OpType])
			})
		}
	}
}

func TestNativeIndexedStateOnlyCommitAllowsAdvancedSnapshot(t *testing.T) {
	for _, mode := range []types.UpdateType{types.UpdateTypePosition, types.UpdateTypeDeletionVector} {
		t.Run(string(mode), func(t *testing.T) {
			ctx := context.Background()
			cfg, stream, cat := nativeWriterFixture(t, mode, true, "")
			index := &nativeMemoryIndex{rows: make(map[string]types.RowLocation)}
			seed := openNativeTestWriter(ctx, t, cfg, stream, cat, index, "seed", true)
			require.NoError(t, seed.Write(ctx, []types.RawRecord{nativeTestRecord("seed", "r", 1, "one")}))
			require.NoError(t, seed.Close(ctx, nil))
			checkpoint := index.snapshot
			heartbeat := openNativeTestWriter(ctx, t, cfg, stream, cat, index, "heartbeat", false)
			external := openNativeTestWriter(ctx, t, cfg, stream, cat, nil, "external", true)
			require.NoError(t, external.Write(ctx, []types.RawRecord{nativeTestRecord("external", "r", 2, "one")}))
			require.NoError(t, external.Close(ctx, nil))
			require.NotEqual(t, checkpoint, nativeSnapshotID(external.table))
			require.NoError(t, heartbeat.Close(ctx, map[string]any{"state": `{"captured_cdc_pos":"0/456"}`}))
			require.Equal(t, nativeSnapshotID(external.table), nativeSnapshotID(heartbeat.table))
			require.Contains(t, heartbeat.table.Properties()[nativeRecoveryProperty], "0/456")
			require.Equal(t, checkpoint, index.snapshot, "state-only commits cannot checkpoint rows they never indexed")
			_, indexedExternal := index.rows["external"]
			require.False(t, indexedExternal)
			recovered := openNativeTestWriter(ctx, t, cfg, stream, cat, index, "next-sync", false)
			require.Equal(t, nativeSnapshotID(external.table), index.snapshot)
			_, indexedExternal = index.rows["external"]
			require.True(t, indexedExternal)
			recovered.Abort()
		})
	}
}

// This checks the writer-side memory invariant rather than asserting counts on
// a mock Lookup method: only a key whose committed row can feed a delete belongs
// in indexReads. All writes still use real native files and catalog metadata.
func TestNativeIndexedReadProvenanceBoundedByCommittedDeletes(t *testing.T) {
	for _, mode := range []types.UpdateType{types.UpdateTypePosition, types.UpdateTypeDeletionVector} {
		t.Run(string(mode), func(t *testing.T) {
			ctx := context.Background()
			cfg, stream, cat := nativeWriterFixture(t, mode, true, "")
			index := &nativeMemoryIndex{rows: make(map[string]types.RowLocation)}
			const count = 256
			backfill := openNativeTestWriter(ctx, t, cfg, stream, cat, index, "backfill", true)
			records := make([]types.RawRecord, count)
			for row := range records {
				records[row] = nativeTestRecord(fmt.Sprint(row), "r", int64(row), "one")
			}
			require.NoError(t, backfill.Write(ctx, records))
			require.Nil(t, backfill.indexReads)
			require.Nil(t, backfill.indexTouched)
			backfill.Abort()
			creates := openNativeTestWriter(ctx, t, cfg, stream, cat, index, "creates", false)
			for row := range records {
				records[row] = nativeTestRecord(fmt.Sprint(row), "c", int64(row), "one")
			}
			require.NoError(t, creates.Write(ctx, records))
			require.Nil(t, creates.indexReads)
			require.Len(t, creates.indexTouched, count)
			require.NoError(t, creates.Write(ctx, []types.RawRecord{nativeTestRecord("0", "d", 1, "one"), nativeTestRecord("0", "c", 2, "one")}))
			require.Nil(t, creates.indexReads)
			require.Len(t, creates.indexTouched, count)
			require.NoError(t, creates.Write(ctx, []types.RawRecord{nativeTestRecord("committed-probe", "u", 3, "one"), nativeTestRecord("committed-probe", "u", 4, "one")}))
			require.Len(t, creates.indexReads, 1)
			_, captured := creates.indexReads["committed-probe"]
			require.True(t, captured)
			creates.Abort()
		})
	}
}

func TestNativeInitialDefaultsPreservePhysicalRowsAndIndex(t *testing.T) {
	for _, deletionVector := range []bool{false, true} {
		name := "position-deletes"
		version := 2
		var initialDefault, replacementDefault apacheiceberg.Literal
		var initialValue, replacementValue any
		if deletionVector {
			name = "deletion-vector"
			version = 3
			initialDefault, replacementDefault = apacheiceberg.NewLiteral(int64(7)), apacheiceberg.NewLiteral(int64(11))
			initialValue, replacementValue = int64(7), int64(11)
		}
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			tbl := appendNativeIndexRows(t, nativeIndexTestTable(t, version), nativeIndexRows("first", "removed", "middle", "fourth", "tail"))
			index := &nativeMemoryIndex{rows: make(map[string]types.RowLocation)}
			require.NoError(t, reconcileNativeTableIndex(ctx, index, tbl, constants.OlakeID))
			original := index.rows["tail"]
			require.Equal(t, int64(4), original.Position)
			payload, found := tbl.Schema().FindFieldByName("payload")
			require.True(t, found)

			if deletionVector {
				writer := dv.NewDVWriter(iceio.LocalFS{}, func(int32) *apacheiceberg.PartitionSpec { return apacheiceberg.UnpartitionedSpec })
				require.NoError(t, writer.Add(original.FilePath, []int64{1}, 0, nil))
				deletes, err := writer.Flush(ctx, filepath.Join(t.TempDir(), "defaults.puffin"))
				require.NoError(t, err)
				tbl = commitNativeIndexDeletes(t, tbl, deletes...)
			} else {
				deleted := writeNativeIndexDelete(t, apacheiceberg.PositionalDeleteSchema, fmt.Sprintf(`[{"file_path":%q,"pos":1}]`, original.FilePath), false, 1)
				tbl = commitNativeIndexDeletes(t, tbl, deleted)
			}
			require.NoError(t, reconcileNativeTableIndex(ctx, index, tbl, constants.OlakeID))
			require.NotContains(t, index.rows, "removed")
			require.Equal(t, original, index.rows["tail"])

			tx := tbl.NewTransaction()
			require.NoError(t, tx.UpdateSchema(true, false).AddColumn([]string{"added"}, apacheiceberg.PrimitiveTypes.Int64, "", false, initialDefault).Commit())
			tbl, err := tx.Commit(ctx)
			require.NoError(t, err)
			added, found := tbl.Schema().FindFieldByName("added")
			require.True(t, found)
			require.Equal(t, initialValue, added.InitialDefault)

			// Upstream v0.7.0 cannot scan a default-only (zero physical column)
			// projection. Include a stored ID to exercise defaults and ordinals.
			rows, err := readNativeRows(ctx, tbl, original.FilePath, []int64{4, 1, 2, 0}, []string{constants.OlakeID, "added"})
			require.NoError(t, err)
			require.Len(t, rows, 4)
			for _, position := range []int64{0, 1, 2, 4} {
				require.Equal(t, initialValue, rows[position]["added"])
				require.NotEmpty(t, rows[position][constants.OlakeID])
			}
			rows, err = readNativeRows(ctx, tbl, original.FilePath, []int64{4}, []string{"payload", "added"})
			require.NoError(t, err)
			require.Equal(t, map[string]any{"payload": "value-tail", "added": initialValue}, rows[4])
			require.NoError(t, reconcileNativeTableIndex(ctx, index, tbl, constants.OlakeID))
			require.Equal(t, original, index.rows["tail"], "schema commits must not move indexed physical rows")

			tbl = appendNativeIndexRows(t, tbl, fmt.Sprintf(`[{%q:"null-before","payload":null,"added":null},{%q:"value-before","payload":"old-field-value","added":9}]`, constants.OlakeID, constants.OlakeID))
			require.NoError(t, reconcileNativeTableIndex(ctx, index, tbl, constants.OlakeID))
			nullBefore, valueBefore := index.rows["null-before"], index.rows["value-before"]
			require.Equal(t, nullBefore.FilePath, valueBefore.FilePath)
			rows, err = readNativeRows(ctx, tbl, nullBefore.FilePath, []int64{nullBefore.Position, valueBefore.Position}, []string{"payload", "added"})
			require.NoError(t, err)
			require.Equal(t, map[string]any{"payload": nil, "added": nil}, rows[nullBefore.Position], "an explicit NULL must not become an initial default")
			require.Equal(t, map[string]any{"payload": "old-field-value", "added": int64(9)}, rows[valueBefore.Position])

			tx = tbl.NewTransaction()
			require.NoError(t, tx.UpdateSchema(true, false).DeleteColumn([]string{"payload"}).DeleteColumn([]string{"added"}).Commit())
			tbl, err = tx.Commit(ctx)
			require.NoError(t, err)
			rows, err = readNativeRows(ctx, tbl, valueBefore.FilePath, []int64{valueBefore.Position}, []string{constants.OlakeID, "payload", "added"})
			require.NoError(t, err)
			require.Equal(t, map[string]any{constants.OlakeID: "value-before"}, rows[valueBefore.Position], "dropped fields must be absent even while their bytes remain in the file")

			tx = tbl.NewTransaction()
			require.NoError(t, tx.UpdateSchema(true, false).
				AddColumn([]string{"payload"}, apacheiceberg.PrimitiveTypes.String, "", false, nil).
				AddColumn([]string{"added"}, apacheiceberg.PrimitiveTypes.Int64, "", false, replacementDefault).Commit())
			tbl, err = tx.Commit(ctx)
			require.NoError(t, err)
			replacementPayload, found := tbl.Schema().FindFieldByName("payload")
			require.True(t, found)
			replacementAdded, found := tbl.Schema().FindFieldByName("added")
			require.True(t, found)
			require.NotEqual(t, payload.ID, replacementPayload.ID)
			require.NotEqual(t, added.ID, replacementAdded.ID)
			require.Equal(t, replacementValue, replacementAdded.InitialDefault)

			rows, err = readNativeRows(ctx, tbl, original.FilePath, []int64{1, 4}, []string{constants.OlakeID, "payload", "added"})
			require.NoError(t, err)
			for _, position := range []int64{1, 4} {
				delete(rows[position], constants.OlakeID)
				require.Equal(t, map[string]any{"payload": nil, "added": replacementValue}, rows[position], "same-name re-adds must not resurrect dropped field IDs")
			}
			rows, err = readNativeRows(ctx, tbl, nullBefore.FilePath, []int64{nullBefore.Position, valueBefore.Position}, []string{constants.OlakeID, "payload", "added"})
			require.NoError(t, err)
			for _, position := range []int64{nullBefore.Position, valueBefore.Position} {
				delete(rows[position], constants.OlakeID)
				require.Equal(t, map[string]any{"payload": nil, "added": replacementValue}, rows[position], "both values and NULLs belonging to dropped IDs are absent from the replacement fields")
			}

			// A cold index rebuild must still filter the original deletes and
			// preserve the original path/ordinal after every schema transition.
			rebuilt := &nativeMemoryIndex{rows: make(map[string]types.RowLocation)}
			require.NoError(t, reconcileNativeTableIndex(ctx, rebuilt, tbl, constants.OlakeID))
			require.Equal(t, index.rows, rebuilt.rows)
			require.Equal(t, original, rebuilt.rows["tail"])
			require.NotContains(t, rebuilt.rows, "removed")

			tbl = appendNativeIndexRows(t, tbl, fmt.Sprintf(`[{%q:"null-after","payload":null,"added":null},{%q:"value-after","payload":"replacement-value","added":13}]`, constants.OlakeID, constants.OlakeID))
			require.NoError(t, reconcileNativeTableIndex(ctx, rebuilt, tbl, constants.OlakeID))
			nullAfter, valueAfter := rebuilt.rows["null-after"], rebuilt.rows["value-after"]
			require.Equal(t, nullAfter.FilePath, valueAfter.FilePath)
			rows, err = readNativeRows(ctx, tbl, nullAfter.FilePath, []int64{nullAfter.Position, valueAfter.Position}, []string{"payload", "added"})
			require.NoError(t, err)
			require.Equal(t, map[string]any{"payload": nil, "added": nil}, rows[nullAfter.Position])
			require.Equal(t, map[string]any{"payload": "replacement-value", "added": int64(13)}, rows[valueAfter.Position])
			require.Equal(t, original, rebuilt.rows["tail"])
		})
	}
}

func TestNativeSourceChangesInvalidateSameThreadTOASTCarry(t *testing.T) {
	for _, mode := range []types.UpdateType{types.UpdateTypePosition, types.UpdateTypeDeletionVector} {
		t.Run(string(mode), func(t *testing.T) {
			ctx := context.Background()
			cfg, stream, cat := nativeWriterFixture(t, mode, true, "")
			stream.Stream.UpsertField("stable", types.String, true, false)
			stream.StreamMetadata.DropRemovedColumns = true
			index := &nativeMemoryIndex{rows: make(map[string]types.RowLocation)}
			seed := openNativeTestWriter(ctx, t, cfg, stream, cat, index, "defaults-seed", true)
			seedRow := nativeTestRecord("row", "r", 1, "table-old")
			seedRow.Data["stable"] = "table-stable"
			seedDeleted := nativeTestRecord("deleted", "r", 2, "table-deleted")
			seedDeleted.Data["stable"] = "table-deleted-stable"
			require.NoError(t, seed.Write(ctx, []types.RawRecord{seedRow, seedDeleted}))
			require.NoError(t, seed.Close(ctx, nil))

			dest := &Iceberg{config: cfg, catalog: cat}
			_, _, err := dest.Setup(ctx, stream, nil, &destination.Options{ThreadID: "defaults-cdc", TableIndex: index})
			require.NoError(t, err)
			writer := dest.writer.(*nativeWriter)
			t.Cleanup(writer.Abort)
			t.Cleanup(dest.unavailableValues.Close)
			resolver := dest.unavailableValues

			first := nativeTestRecord("row", "u", 3, "carried-old")
			first.Data["stable"] = "carried-stable"
			second := nativeTestRecord("row", "u", 4, placeholder)
			second.Data["stable"] = placeholder
			second.UnavailableColumns = []string{"part", "stable"}
			deleted := nativeTestRecord("deleted", "d", 2, "table-deleted")
			deleted.Data["stable"] = "table-deleted-stable"
			_, records, _, err := dest.FlattenAndCleanData(ctx, []types.RawRecord{first, second, deleted})
			require.NoError(t, err)
			require.NoError(t, dest.Write(ctx, records))
			location := nativePendingLocation(t, writer, "row")
			require.Equal(t, "carried-old", resolver.carry["row"].values["part"])
			require.Equal(t, "carried-stable", resolver.carry["row"].values["stable"])
			tombstone := resolver.carry["deleted"]
			require.True(t, tombstone.deleted)
			oldField, found := writer.table.Schema().FindFieldByName("part")
			require.True(t, found)

			_, err = dest.ApplySourceColumns(ctx, dest.schema, types.SourceColumnsChange{KeepColumns: []string{"value", "blob", "stable"}})
			require.NoError(t, err)
			require.NotContains(t, resolver.tracked, "part")
			require.NotContains(t, resolver.carry["row"].values, "part")
			require.Equal(t, "carried-stable", resolver.carry["row"].values["stable"])
			require.Same(t, tombstone, resolver.carry["deleted"])
			_, err = dest.ApplySourceColumns(ctx, dest.schema, types.SourceColumnsChange{
				KeepColumns: []string{"value", "blob", "stable"},
				Columns: []types.SourceColumn{
					{Name: "part", Type: types.String, ExistingRowValue: "replacement-default"},
					{Name: "new_toast", Type: types.String, ExistingRowValue: "added-default"},
				},
			})
			require.NoError(t, err)
			newField, found := writer.table.Schema().FindFieldByName("part")
			require.True(t, found)
			require.NotEqual(t, oldField.ID, newField.ID)
			require.Equal(t, location, nativePendingLocation(t, writer, "row"), "source schema events must not change pending physical indexing")
			require.Same(t, tombstone, resolver.carry["deleted"])

			// Upstream v0.7.0 cannot recover a projection containing only
			// added/re-added fields. Read them alongside a stored field instead.
			physical, readErr := writer.ReadRows(ctx, location.FilePath, []int64{location.Position}, []string{"value", "part", "new_toast"})
			require.NoError(t, readErr)
			require.Contains(t, physical[location.Position], "value")
			if mode == types.UpdateTypeDeletionVector {
				require.Equal(t, "replacement-default", physical[location.Position]["part"])
				require.Equal(t, "added-default", physical[location.Position]["new_toast"])
			} else {
				require.Nil(t, newField.InitialDefault)
				require.Nil(t, physical[location.Position]["part"])
				require.Nil(t, physical[location.Position]["new_toast"])
			}
			next := nativeTestRecord("row", "u", 5, "replacement-value")
			next.Data["stable"], next.Data["new_toast"] = placeholder, "replacement-value"
			next.UnavailableColumns = []string{"stable"}
			beforeReads := resolver.fromTable
			_, records, _, err = dest.FlattenAndCleanData(ctx, []types.RawRecord{next})
			require.NoError(t, err)
			require.Equal(t, beforeReads, resolver.fromTable, "unchanged columns retain their same-thread carry")
			require.NoError(t, dest.Write(ctx, records))
			require.Equal(t, "carried-stable", next.Data["stable"])

			// A deleted key still points at physical history, but schema changes
			// must not erase its same-thread tombstone and revive those values.
			resurrect := nativeTestRecord("deleted", "u", 6, placeholder)
			resurrect.Data["stable"], resurrect.Data["new_toast"] = placeholder, placeholder
			resurrect.UnavailableColumns = []string{"part", "stable", "new_toast"}
			beforeReads = resolver.fromTable
			require.NoError(t, resolver.Resolve(ctx, []types.RawRecord{resurrect}))
			require.Equal(t, placeholder, resurrect.Data["part"])
			require.Equal(t, placeholder, resurrect.Data["stable"])
			require.Equal(t, placeholder, resurrect.Data["new_toast"])
			require.Equal(t, beforeReads, resolver.fromTable)
			require.Same(t, tombstone, resolver.carry["deleted"])
		})
	}
}

func TestNativeRESTCoordinatorRejectsDeletesAfterExternalCompaction(t *testing.T) {
	for _, mode := range []types.UpdateType{types.UpdateTypePosition, types.UpdateTypeDeletionVector} {
		t.Run(string(mode), func(t *testing.T) {
			ctx := context.Background()
			cfg, stream, cat, boundary := nativeRESTWriterFixture(t, mode,
				nativeTestRecord("a", "r", 1, "one"))
			// Seed separate files so the external rewrite really compacts them
			// into one file, rather than merely replacing an identical layout.
			seedB := openNativeTestWriter(ctx, t, cfg, stream, boundary.cat, nil, "second-input", true)
			require.NoError(t, seedB.Write(ctx, []types.RawRecord{nativeTestRecord("b", "r", 2, "one")}))
			require.NoError(t, seedB.Close(ctx, nil))
			index := &nativeMemoryIndex{rows: make(map[string]types.RowLocation)}
			stale := openNativeTestWriter(ctx, t, cfg, stream, cat, index, "stale-delete", false)
			checkpoint, originalA := index.snapshot, index.rows["a"]
			require.NoError(t, stale.Write(ctx, []types.RawRecord{nativeTestRecord("a", "u", 3, "one")}))
			staged := stale.dataFiles[0].FilePath()

			// A separate catalog owner compacts files, reordering rows on v2.
			// This intentionally bypasses the REST client's in-process coordinator.
			compactor := openNativeTestWriter(ctx, t, cfg, stream, boundary.cat, nil, "external-compactor", true)
			rows := []types.RawRecord{nativeTestRecord("b", "r", 2, "one"), nativeTestRecord("a", "r", 1, "one")}
			if mode == types.UpdateTypeDeletionVector {
				// Preserve the implicit v3 row IDs: a and b were assigned
				// consecutive IDs in separate files and now share one file.
				rows[0], rows[1] = rows[1], rows[0]
			}
			require.NoError(t, compactor.Write(ctx, rows))
			entries, err := nativeLiveEntries(ctx, compactor.table)
			require.NoError(t, err)
			var removed []apacheiceberg.DataFile
			for _, entry := range entries {
				if entry.DataFile().ContentType() == apacheiceberg.EntryContentData {
					removed = append(removed, entry.DataFile())
				}
			}
			require.Len(t, removed, 2)
			require.Len(t, compactor.dataFiles, 1)
			if mode == types.UpdateTypeDeletionVector {
				var firstA, firstB *int64
				for _, file := range removed {
					switch file.FilePath() {
					case index.rows["a"].FilePath:
						firstA = file.FirstRowID()
					case index.rows["b"].FilePath:
						firstB = file.FirstRowID()
					}
				}
				require.NotNil(t, firstA)
				require.NotNil(t, firstB)
				require.Equal(t, *firstA+1, *firstB)
				replacement := compactor.dataFiles[0]
				builder, err := apacheiceberg.NewDataFileBuilder(compactor.table.Spec(), replacement.ContentType(), replacement.FilePath(), replacement.FileFormat(), replacement.Partition(), nil, nil, replacement.Count(), replacement.FileSizeBytes())
				require.NoError(t, err)
				builder.ColumnSizes(replacement.ColumnSizes()).ValueCounts(replacement.ValueCounts()).
					NullValueCounts(replacement.NullValueCounts()).NaNValueCounts(replacement.NaNValueCounts()).
					LowerBoundValues(replacement.LowerBoundValues()).UpperBoundValues(replacement.UpperBoundValues()).
					SplitOffsets(replacement.SplitOffsets()).FirstRowID(*firstA)
				if order := replacement.SortOrderID(); order != nil {
					builder.SortOrderID(*order)
				}
				compactor.dataFiles[0] = builder.Build()
			}
			tx := compactor.table.NewTransaction()
			require.NoError(t, tx.ReplaceFiles(ctx, removed, compactor.dataFiles, nil, nil))
			compacted, err := tx.Commit(ctx)
			require.NoError(t, err)
			clear(compactor.paths)
			compactor.Abort()
			require.NotEqual(t, checkpoint, nativeSnapshotID(compacted))
			require.Equal(t, map[string]int64{"a": 1, "b": 2}, nativeVisibleValues(t, compacted))
			if mode == types.UpdateTypeDeletionVector {
				compactedEntries, err := nativeLiveEntries(ctx, compacted)
				require.NoError(t, err)
				require.Len(t, compactedEntries, 1)
				require.Equal(t, compactor.dataFiles[0].FirstRowID(), compactedEntries[0].DataFile().FirstRowID(), "compaction must preserve the original v3 row ID range")
			}

			require.ErrorIs(t, stale.Close(ctx, map[string]any{"state": `{"unsafe":3}`}), table.ErrCommitFailed)
			require.Equal(t, []string{http.MethodGet, http.MethodPost, http.MethodGet}, boundary.tableRequests(), "definite server conflict must refresh once, then refuse stale positional deletes without another POST")
			require.False(t, stale.uncertain, "a rejected stale snapshot is not an ambiguous publication")
			require.Equal(t, checkpoint, index.snapshot)
			require.Equal(t, originalA, index.rows["a"])
			persisted, err := boundary.cat.LoadTable(ctx, compacted.Identifier())
			require.NoError(t, err)
			require.Equal(t, nativeSnapshotID(compacted), nativeSnapshotID(persisted))
			require.Equal(t, map[string]int64{"a": 1, "b": 2}, nativeVisibleValues(t, persisted))
			require.NotContains(t, persisted.Properties()[nativeRecoveryProperty], "unsafe")
			_, err = os.Stat(staged)
			require.ErrorIs(t, err, os.ErrNotExist, "definitively unpublished staged data can be cleaned up")

			recovered := openNativeTestWriter(ctx, t, cfg, stream, cat, index, "after-compaction", false)
			require.Equal(t, []string{http.MethodGet, http.MethodPost, http.MethodGet, http.MethodGet}, boundary.tableRequests())
			require.Equal(t, nativeSnapshotID(compacted), index.snapshot)
			require.NotEqual(t, originalA.FilePath, index.rows["a"].FilePath)
			if mode == types.UpdateTypeDeletionVector {
				require.Equal(t, int64(0), index.rows["a"].Position)
				require.Equal(t, int64(1), index.rows["b"].Position)
			} else {
				require.Equal(t, int64(1), index.rows["a"].Position)
				require.Equal(t, int64(0), index.rows["b"].Position)
			}
			require.NoError(t, recovered.Write(ctx, []types.RawRecord{nativeTestRecord("a", "u", 4, "one")}))
			require.NoError(t, recovered.Close(ctx, map[string]any{"state": `{"safe":4}`}))
			require.Equal(t, []string{http.MethodGet, http.MethodPost, http.MethodGet, http.MethodGet, http.MethodPost}, boundary.tableRequests())
			require.Equal(t, map[string]int64{"a": 4, "b": 2}, nativeVisibleValues(t, recovered.table))
		})
	}
}

func TestNativeRESTCoordinatorConcurrentIndexedCDC(t *testing.T) {
	for _, mode := range []types.UpdateType{types.UpdateTypePosition, types.UpdateTypeDeletionVector} {
		t.Run(string(mode), func(t *testing.T) {
			ctx := context.Background()
			cfg, stream, cat, _ := nativeRESTWriterFixture(t, mode, nativeTestRecord("a", "r", 1, "one"), nativeTestRecord("b", "r", 2, "one"))
			index := &nativeMemoryIndex{rows: make(map[string]types.RowLocation)}
			first := openNativeTestWriter(ctx, t, cfg, stream, cat, index, "parallel-a", false)
			second := openNativeTestWriter(ctx, t, cfg, stream, cat, index, "parallel-b", false)
			original := index.rows["a"]
			require.Equal(t, original.FilePath, index.rows["b"].FilePath)
			require.NoError(t, first.Write(ctx, []types.RawRecord{nativeTestRecord("a", "u", 10, "one")}))
			require.NoError(t, second.Write(ctx, []types.RawRecord{nativeTestRecord("b", "u", 20, "one")}))
			start := make(chan struct{})
			errors := make(chan error, 2)
			var group sync.WaitGroup
			for _, writer := range []*nativeWriter{first, second} {
				group.Add(1)
				go func(writer *nativeWriter) {
					defer group.Done()
					<-start
					errors <- writer.Close(ctx, nil)
				}(writer)
			}
			close(start)
			group.Wait()
			close(errors)
			for err := range errors {
				require.NoError(t, err)
			}
			fresh, err := cat.LoadTable(ctx, first.table.Identifier())
			require.NoError(t, err)
			require.Equal(t, map[string]int64{"a": 10, "b": 20}, nativeVisibleValues(t, fresh))
			deleted, _, err := nativeDeletedPositions(ctx, fresh, original.FilePath)
			require.NoError(t, err)
			require.Equal(t, []int64{0, 1}, deleted, "both deletes from the original file must survive vector replacement")
			require.Equal(t, nativeSnapshotID(fresh), index.snapshot)
			winner := openNativeTestWriter(ctx, t, cfg, stream, cat, index, "same-key-first", false)
			stale := openNativeTestWriter(ctx, t, cfg, stream, cat, index, "same-key-stale", false)
			require.NoError(t, winner.Write(ctx, []types.RawRecord{nativeTestRecord("a", "u", 30, "one")}))
			require.NoError(t, stale.Write(ctx, []types.RawRecord{nativeTestRecord("a", "u", 40, "one")}))
			require.NoError(t, winner.Close(ctx, nil))
			require.ErrorIs(t, stale.Close(ctx, nil), table.ErrConflictingDataFiles)
			fresh, err = cat.LoadTable(ctx, winner.table.Identifier())
			require.NoError(t, err)
			require.Equal(t, map[string]int64{"a": 30, "b": 20}, nativeVisibleValues(t, fresh))
		})
	}
}

func TestNativeRESTCoordinatorRecoversAfterCommittedIndexFailure(t *testing.T) {
	for _, mode := range []types.UpdateType{types.UpdateTypePosition, types.UpdateTypeDeletionVector} {
		t.Run(string(mode), func(t *testing.T) {
			ctx := context.Background()
			cfg, stream, cat, _ := nativeRESTWriterFixture(t, mode, nativeTestRecord("a", "r", 1, "one"))
			index := &nativeMemoryIndex{rows: make(map[string]types.RowLocation)}
			writer := openNativeTestWriter(ctx, t, cfg, stream, cat, index, "checkpoint-fails", false)
			original, checkpoint := index.rows["a"], index.snapshot
			require.NoError(t, writer.Write(ctx, []types.RawRecord{nativeTestRecord("a", "u", 9, "one")}))
			index.commitErr = errors.New("injected durable index failure")
			require.ErrorContains(t, writer.Close(ctx, map[string]any{"state": "committed-cursor"}), "local index checkpoint failed")
			fresh, err := cat.LoadTable(ctx, writer.table.Identifier())
			require.NoError(t, err)
			require.Equal(t, map[string]int64{"a": 9}, nativeVisibleValues(t, fresh))
			require.Contains(t, fresh.Properties()[nativeRecoveryProperty], "committed-cursor")
			require.Equal(t, checkpoint, index.snapshot)
			require.Equal(t, original, index.rows["a"])
			index.commitErr = nil
			recovered := openNativeTestWriter(ctx, t, cfg, stream, cat, index, "checkpoint-recovery", false)
			require.Equal(t, nativeSnapshotID(fresh), index.snapshot)
			require.NotEqual(t, original, index.rows["a"])
			require.NoError(t, recovered.Write(ctx, []types.RawRecord{nativeTestRecord("a", "u", 10, "one")}))
			require.NoError(t, recovered.Close(ctx, map[string]any{"state": "recovered-cursor"}))
			require.Equal(t, map[string]int64{"a": 10}, nativeVisibleValues(t, recovered.table))
			require.Equal(t, nativeSnapshotID(recovered.table), index.snapshot)
		})
	}
}

func TestResyncGenerationForcesFullScanAndDropsOldPhysicalLocations(t *testing.T) {
	ctx := context.Background()
	tbl := appendNativeIndexRows(t, nativeIndexTestTable(t, 2), nativeIndexRows("live-row"))
	index := openNativeTestIndex(t)
	pending := types.NewStreamIndexThread(index)
	pending.Put("deleted-old-row", types.RowLocation{FilePath: "old.parquet", Position: 3})
	oldSnapshot := int64(11)
	require.NoError(t, index.Commit(pending, &oldSnapshot))
	require.NoError(t, indexdb.ReconcileGeneration(index, "completed-resync"))
	require.NoError(t, reconcileNativeTableIndex(ctx, index, tbl, constants.OlakeID))
	_, found, err := index.Lookup("deleted-old-row")
	require.NoError(t, err)
	require.False(t, found)
	location, found, err := index.Lookup("live-row")
	require.NoError(t, err)
	require.True(t, found)
	require.NotEqual(t, "old.parquet", location.FilePath)
	require.Equal(t, int64(0), location.Position)
	snapshot, err := index.LastCommittedSnapshot()
	require.NoError(t, err)
	require.Equal(t, nativeSnapshotID(tbl), snapshot)
	// A second reconciliation must retain both generation and recovered rows.
	require.NoError(t, indexdb.ReconcileGeneration(index, "completed-resync"))
	require.NoError(t, reconcileNativeTableIndex(ctx, index, tbl, constants.OlakeID))
	recovered, found, err := index.Lookup("live-row")
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, location, recovered)
}
