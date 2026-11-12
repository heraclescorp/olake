package iceberg

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
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

func writeNativeIndexDelete(t *testing.T, tbl *table.Table, projection *apacheiceberg.Schema, rows string, equality bool, count int64) apacheiceberg.DataFile {
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

func TestNativeIndexRebuildRemovesPositionDeletedRows(t *testing.T) {
	ctx := context.Background()
	tbl := appendNativeIndexRows(t, nativeIndexTestTable(t, 2), nativeIndexRows("first", "removed", "tail"))
	index := openNativeTestIndex(t)
	require.NoError(t, reconcileNativeTableIndex(ctx, index, tbl, constants.OlakeID))
	location, _, err := index.Lookup("removed")
	require.NoError(t, err)
	deleted := writeNativeIndexDelete(t, tbl, apacheiceberg.PositionalDeleteSchema, fmt.Sprintf(`[{"file_path":%q,"pos":1}]`, location.FilePath), false, 1)
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
	deleted := writeNativeIndexDelete(t, tbl, projection, fmt.Sprintf(`[{%q:"deleted"}]`, constants.OlakeID), true, 1)
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
