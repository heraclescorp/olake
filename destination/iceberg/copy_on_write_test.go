package iceberg

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	apacheiceberg "github.com/apache/iceberg-go"

	"github.com/apache/iceberg-go/catalog"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/destination"
	"github.com/datazip-inc/olake/types"
	"github.com/stretchr/testify/require"
)

func nativeCOWFixture(t *testing.T, mode types.UpdateType) (*Config, *types.ConfiguredStream, catalog.Catalog) {
	t.Helper()
	cfg, stream, cat := nativeWriterFixture(t, mode, true, "/{part, identity}")
	stream.Stream.WithPrimaryKey("id", "tenant")
	stream.Stream.UpsertField("id", types.Int64, false, false)
	stream.Stream.UpsertField("tenant", types.String, false, false)
	stream.StreamMetadata.AppendMode = new(false)
	stream.StreamMetadata.MergeStrategy = types.CopyOnWrite
	return cfg, stream, cat
}

func nativeCOWRecord(label, operation string, key int64, tenant string, value int64) types.RawRecord {
	record := nativeTestRecord(label, operation, value, "one")
	record.Data["id"], record.Data["tenant"] = key, tenant
	return record
}

type nativeCOWManifestScopeIO struct {
	iceio.IO
	blocked map[string]error
}

func (fs *nativeCOWManifestScopeIO) Open(name string) (iceio.File, error) {
	if err := fs.blocked[name]; err != nil {
		return nil, err
	}
	return fs.IO.Open(name)
}

func nativeCOWManifestScope(t *testing.T, tbl *table.Table) *nativeCOWManifestScopeIO {
	t.Helper()
	ctx := context.Background()
	fs, err := tbl.FS(ctx)
	require.NoError(t, err)
	manifests, err := tbl.CurrentSnapshot().Manifests(fs)
	require.NoError(t, err)
	guard := &nativeCOWManifestScopeIO{IO: fs, blocked: make(map[string]error)}
	for _, manifest := range manifests {
		if manifest.ManifestContent() == apacheiceberg.ManifestContentData {
			guard.blocked[manifest.FilePath()] = errors.New("COW validation must not read data manifests")
		}
	}
	return guard
}

// Tiny, real local-file/Hadoop-catalog smoke; no services or production access.
func TestNativeCopyOnWriteCanonicalIDsAcrossFlushes(t *testing.T) {
	ctx := context.Background()
	cfg, stream, cat := nativeCOWFixture(t, types.UpdateTypeDeletionVector)
	seed := openNativeTestWriter(ctx, t, cfg, stream, cat, nil, "seed", true)
	require.Equal(t, 2, seed.table.Metadata().Version(), "inactive saved dv must not create v3")
	require.NoError(t, seed.Write(ctx, []types.RawRecord{
		nativeCOWRecord("1a", "r", 9007199254740993, "a", 1), nativeCOWRecord("1b", "r", 9007199254740993, "b", 2),
		nativeCOWRecord("2a", "r", 9007199254740994, "a", 3), nativeCOWRecord("2b", "r", 9007199254740994, "b", 4),
		nativeCOWRecord("", "r", 0, "", 5),
	}))
	require.NoError(t, seed.Close(ctx, map[string]any{"state": "start"}))
	writer := openNativeTestWriter(ctx, t, cfg, stream, cat, nil, "cdc", false)
	nodeTemp := t.TempDir()
	t.Setenv("TMPDIR", nodeTemp)
	require.NoError(t, writer.Write(ctx, []types.RawRecord{
		nativeCOWRecord("1a", "u", 9007199254740993, "a", 10), nativeCOWRecord("1a", "u", 9007199254740993, "a", 11),
		nativeCOWRecord("2b", "u", 9007199254740994, "b", 40),
		nativeCOWRecord("", "u", 0, "", 50),
	}))
	require.NoError(t, writer.Write(ctx, []types.RawRecord{nativeCOWRecord("1a", "d", 9007199254740993, "a", 12)}))
	privateInputs := append([]string(nil), writer.cow.paths...)
	store, err := writer.table.FS(ctx)
	require.NoError(t, err)
	for _, path := range privateInputs {
		input, err := store.Open(path)
		require.NoError(t, err)
		require.NoError(t, input.Close())
	}
	nodeFiles, err := os.ReadDir(nodeTemp)
	require.NoError(t, err)
	require.Empty(t, nodeFiles, "COW payloads must not accumulate on the connector's temporary disk")
	require.Equal(t, map[string]int64{"1a": 1, "1b": 2, "2a": 3, "2b": 4, "": 5}, nativeVisibleValues(t, writer.table), "Write is private until Close")
	require.NoError(t, writer.Close(ctx, map[string]any{"state": "end", "dedup_inserts": false}))
	require.Equal(t, map[string]int64{"1a": 12, "1b": 2, "2a": 3, "2b": 40, "": 50}, nativeVisibleValues(t, writer.table))
	hasDeletes, err := nativeCOWHasDeletes(ctx, writer.table)
	require.NoError(t, err)
	require.False(t, hasDeletes)
	for _, path := range privateInputs {
		_, err := store.Open(path)
		require.ErrorIs(t, err, os.ErrNotExist)
	}
	before := nativeSnapshotID(writer.table)
	quiet := openNativeTestWriter(ctx, t, cfg, stream, cat, nil, "quiet", false)
	require.NoError(t, quiet.Close(ctx, map[string]any{"state": "quiet-end", "dedup_inserts": true}))
	require.Equal(t, before, nativeSnapshotID(quiet.table), "quiet checkpoint uses the common property-only commit")
	require.Contains(t, quiet.table.Properties()[nativeRecoveryProperty], "quiet-end")
	terminal := openNativeTestWriter(ctx, t, cfg, stream, cat, nil, "terminal", false)
	require.NoError(t, terminal.Close(ctx, map[string]any{"state": "quiet-end", "dedup_inserts": false}))
	var metadata types.MetadataState
	require.NoError(t, json.Unmarshal([]byte(terminal.table.Properties()[nativeRecoveryProperty]), &metadata))
	require.NotNil(t, metadata.DedupInserts)
	require.False(t, *metadata.DedupInserts, "same-LSN terminal close must retire the overlap flag")
}

func TestNativeCopyOnWriteOverlappingSegmentsPublishOneRewrite(t *testing.T) {
	ctx := context.Background()
	cfg, stream, cat := nativeCOWFixture(t, types.UpdateTypeEquality)
	seed := openNativeTestWriter(ctx, t, cfg, stream, cat, nil, "seed", true)
	require.NoError(t, seed.Write(ctx, []types.RawRecord{
		nativeCOWRecord("updated", "r", 1, "a", 1),
		nativeCOWRecord("deleted", "r", 2, "a", 2),
		nativeCOWRecord("bystander", "r", 1, "b", 3),
	}))
	require.NoError(t, seed.Close(ctx, map[string]any{"state": "start"}))

	writer := openNativeTestWriter(ctx, t, cfg, stream, cat, nil, "batch", false)
	const segments = 24
	for segment := int64(1); segment <= segments; segment++ {
		updateOp, deleteOp := "u", "u"
		if segment == segments-1 {
			updateOp = "d"
		}
		if segment == segments {
			deleteOp = "d"
		}
		require.NoError(t, writer.Write(ctx, []types.RawRecord{
			nativeCOWRecord("updated", updateOp, 1, "a", 100+segment),
			nativeCOWRecord("deleted", deleteOp, 2, "a", 200+segment),
		}), "segment %d", segment)
	}
	require.Len(t, writer.cow.paths, segments, "each Write must stage a separate overlapping segment")
	before, err := cat.LoadTable(ctx, seed.table.Identifier())
	require.NoError(t, err)
	require.Equal(t, nativeSnapshotID(seed.table), nativeSnapshotID(before), "staged segments must not publish snapshots")
	require.Equal(t, map[string]int64{"updated": 1, "deleted": 2, "bystander": 3}, nativeVisibleValues(t, before))

	require.NoError(t, writer.Close(ctx, map[string]any{"state": "end"}))
	after, err := cat.LoadTable(ctx, seed.table.Identifier())
	require.NoError(t, err)
	require.Len(t, after.Metadata().Snapshots(), len(before.Metadata().Snapshots())+1)
	snapshot := after.CurrentSnapshot()
	require.NotNil(t, snapshot.ParentSnapshotID)
	require.Equal(t, nativeSnapshotID(before), *snapshot.ParentSnapshotID)
	require.Equal(t, table.OpOverwrite, snapshot.Summary.Operation)
	require.Equal(t, map[string]int64{"updated": 124, "deleted": 224, "bystander": 3}, nativeVisibleValues(t, after))

	// COW keeps the winning delete as a row tombstone, not an Iceberg delete file.
	scan := after.Scan(table.WithSelectedFields(constants.OlakeID, constants.OpType))
	defer scan.Close()
	schema, batches, err := scan.ToArrowRecords(ctx)
	require.NoError(t, err)
	idColumn := schema.FieldIndices(constants.OlakeID)[0]
	opColumn := schema.FieldIndices(constants.OpType)[0]
	ops := make(map[string]string)
	for batch, err := range batches {
		require.NoError(t, err)
		ids, operations := batch.Column(idColumn).(*array.String), batch.Column(opColumn).(*array.String)
		for row := range int(batch.NumRows()) {
			ops[ids.Value(row)] = operations.Value(row)
		}
		batch.Release()
	}
	require.Equal(t, map[string]string{"updated": "u", "deleted": "d", "bystander": "r"}, ops)
	hasDeletes, err := nativeCOWHasDeletes(ctx, after)
	require.NoError(t, err)
	require.False(t, hasDeletes)
}

func TestNativeCopyOnWritePreservesNullBystanders(t *testing.T) {
	ctx := context.Background()
	cfg, stream, cat := nativeCOWFixture(t, types.UpdateTypeEquality)
	stream.StreamMetadata.MergeStrategy = types.MergeOnRead
	seed := openNativeTestWriter(ctx, t, cfg, stream, cat, nil, "seed", true)
	missing := nativeCOWRecord("null-bystander", "r", 0, "a", 8)
	missing.Data["id"] = nil
	require.NoError(t, seed.Write(ctx, []types.RawRecord{nativeCOWRecord("old", "r", 1, "a", 1), missing}))
	require.NoError(t, seed.Close(ctx, map[string]any{"state": "start"}))
	stream.StreamMetadata.MergeStrategy = types.CopyOnWrite
	writer := openNativeTestWriter(ctx, t, cfg, stream, cat, nil, "cow", false)
	require.NoError(t, writer.Write(ctx, []types.RawRecord{nativeCOWRecord("old", "u", 1, "a", 2)}))
	require.NoError(t, writer.Close(ctx, map[string]any{"state": "end"}))
	require.Equal(t, map[string]int64{"old": 2, "null-bystander": 8}, nativeVisibleValues(t, writer.table))
}

func TestNativeCopyOnWriteRejectsEveryLiveDeleteKindWithoutMutation(t *testing.T) {
	for _, mode := range []types.UpdateType{types.UpdateTypeEquality, types.UpdateTypePosition, types.UpdateTypeDeletionVector} {
		t.Run(string(mode), func(t *testing.T) {
			ctx := context.Background()
			cfg, stream, cat := nativeCOWFixture(t, mode)
			stream.StreamMetadata.MergeStrategy = types.MergeOnRead
			var index *nativeMemoryIndex
			if mode.NeedsTableIndex(types.Iceberg) {
				index = &nativeMemoryIndex{rows: make(map[string]types.RowLocation)}
			}
			seed := openNativeTestWriter(ctx, t, cfg, stream, cat, index, "seed", true)
			require.NoError(t, seed.Write(ctx, []types.RawRecord{nativeCOWRecord("a", "r", 1, "a", 1), nativeCOWRecord("b", "r", 2, "b", 2)}))
			require.NoError(t, seed.Close(ctx, map[string]any{"state": "keep-checkpoint"}))
			old := openNativeTestWriter(ctx, t, cfg, stream, cat, index, "mor", false)
			require.NoError(t, old.Write(ctx, []types.RawRecord{nativeCOWRecord("a", "u", 1, "a", 3)}))
			require.NoError(t, old.Close(ctx, map[string]any{"state": "keep-checkpoint"}))
			originalCheckpoint := old.table.Properties()[nativeRecoveryProperty]
			originalShape, err := ordinaryRecoveryShape(old.table.Metadata())
			require.NoError(t, err)
			stream.StreamMetadata.MergeStrategy = types.CopyOnWrite
			before := nativeSnapshotID(old.table)
			guard := nativeCOWManifestScope(t, old.table)
			guardedCatalog := &nativeCommitFaultCatalog{Catalog: cat, fs: guard}
			_, _, err = newNativeWriter(ctx, cfg, stream, &destination.Options{ThreadID: "requires-cleanup"}, guardedCatalog, nil)
			require.ErrorContains(t, err, "separate explicit maintenance rewrite")
			fresh, err := cat.LoadTable(ctx, old.table.Identifier())
			require.NoError(t, err)
			require.Equal(t, before, nativeSnapshotID(fresh))
			require.Equal(t, old.table.MetadataLocation(), fresh.MetadataLocation())
			require.Equal(t, originalCheckpoint, fresh.Properties()[nativeRecoveryProperty])
			require.Equal(t, old.table.Metadata().TableUUID(), fresh.Metadata().TableUUID())
			shape, err := ordinaryRecoveryShape(fresh.Metadata())
			require.NoError(t, err)
			require.Equal(t, originalShape, shape)
			require.Equal(t, map[string]int64{"a": 3, "b": 2}, nativeVisibleValues(t, fresh))
			hasDeletes, err := nativeCOWHasDeletes(ctx, fresh)
			require.NoError(t, err)
			require.True(t, hasDeletes)
		})
	}
}

func TestNativeCopyOnWriteRejectsConcurrentLiveDeletes(t *testing.T) {
	ctx := context.Background()
	cfg, stream, cat := nativeCOWFixture(t, types.UpdateTypeEquality)
	seed := openNativeTestWriter(ctx, t, cfg, stream, cat, nil, "seed", true)
	require.NoError(t, seed.Write(ctx, []types.RawRecord{nativeCOWRecord("a", "r", 1, "a", 1)}))
	require.NoError(t, seed.Close(ctx, map[string]any{"state": "seed"}))
	cow := openNativeTestWriter(ctx, t, cfg, stream, cat, nil, "cow", false)
	require.NoError(t, cow.Write(ctx, []types.RawRecord{nativeCOWRecord("a", "u", 1, "a", 9)}))
	privateInputs := append([]string(nil), cow.cow.paths...)
	stream.StreamMetadata.MergeStrategy = types.MergeOnRead
	mor := openNativeTestWriter(ctx, t, cfg, stream, cat, nil, "mor", false)
	require.NoError(t, mor.Write(ctx, []types.RawRecord{nativeCOWRecord("a", "u", 1, "a", 2)}))
	require.NoError(t, mor.Close(ctx, map[string]any{"state": "mor"}))
	require.ErrorContains(t, cow.Close(ctx, map[string]any{"state": "must-not-land"}), "separate explicit maintenance rewrite")
	fresh, err := cat.LoadTable(ctx, mor.table.Identifier())
	require.NoError(t, err)
	require.Equal(t, mor.table.MetadataLocation(), fresh.MetadataLocation())
	require.Equal(t, mor.table.Properties()[nativeRecoveryProperty], fresh.Properties()[nativeRecoveryProperty])
	require.Equal(t, map[string]int64{"a": 2}, nativeVisibleValues(t, fresh))
	store, err := cow.table.FS(ctx)
	require.NoError(t, err)
	for _, path := range privateInputs {
		_, err := store.Open(path)
		require.ErrorIs(t, err, os.ErrNotExist, "known rejection removes private replay input")
	}
}

func TestNativeCopyOnWriteRefreshesAfterSameFileCompaction(t *testing.T) {
	ctx := context.Background()
	cfg, stream, realCatalog := nativeCOWFixture(t, types.UpdateTypePosition)
	seed := openNativeTestWriter(ctx, t, cfg, stream, realCatalog, nil, "seed", true)
	require.NoError(t, seed.Write(ctx, []types.RawRecord{nativeCOWRecord("a", "r", 1, "a", 1), nativeCOWRecord("b", "r", 2, "b", 2)}))
	require.NoError(t, seed.Close(ctx, map[string]any{"state": "start"}))
	fault := &nativeCommitFaultCatalog{Catalog: realCatalog}
	writer := openNativeTestWriter(ctx, t, cfg, stream, fault, nil, "cow", false)
	require.NoError(t, writer.Write(ctx, []types.RawRecord{nativeCOWRecord("a", "u", 1, "a", 9)}))
	fault.fault = func(ctx context.Context, id table.Identifier, _ []table.Requirement, _ []table.Update) (bool, error) {
		fault.fault = nil
		fresh, err := realCatalog.LoadTable(ctx, id)
		require.NoError(t, err)
		scan := fresh.Scan()
		tasks, err := scan.PlanFiles(ctx)
		require.NoError(t, err)
		require.NoError(t, scan.Close())
		tx := fresh.NewTransaction()
		for _, task := range tasks {
			result, err := table.ExecuteCompactionGroup(ctx, fresh, table.CompactionTaskGroup{Tasks: []table.FileScanTask{task}}, table.WithCompactionArrowBatchSize(1), table.WithCompactionRecordBatchBufferSize(1))
			require.NoError(t, err)
			require.NoError(t, tx.ReplaceFiles(ctx, result.OldDataFiles, result.NewDataFiles, nil, nil))
		}
		_, err = tx.Commit(ctx)
		require.NoError(t, err)
		return true, table.ErrCommitFailed
	}
	require.NoError(t, writer.Close(ctx, map[string]any{"state": "end"}))
	require.Equal(t, map[string]int64{"a": 9, "b": 2}, nativeVisibleValues(t, writer.table))
}

func TestNativeCopyOnWriteUnknownCommitRetainsReplay(t *testing.T) {
	ctx := context.Background()
	cfg, stream, realCatalog := nativeCOWFixture(t, types.UpdateTypePosition)
	create := openNativeTestWriter(ctx, t, cfg, stream, realCatalog, nil, "create", true)
	create.Abort()
	fault := &nativeCommitFaultCatalog{Catalog: realCatalog}
	writer := openNativeTestWriter(ctx, t, cfg, stream, fault, nil, "unknown", false)
	require.NoError(t, writer.Write(ctx, []types.RawRecord{nativeCOWRecord("landed", "c", 1, "a", 7)}))
	privateInputs := append([]string(nil), writer.cow.paths...)
	store, err := writer.table.FS(ctx)
	require.NoError(t, err)
	t.Cleanup(func() {
		for _, path := range privateInputs {
			_ = store.Remove(path)
		}
	})
	fault.fault = func(ctx context.Context, id table.Identifier, requirements []table.Requirement, updates []table.Update) (bool, error) {
		_, _, err := realCatalog.CommitTable(ctx, id, requirements, updates)
		require.NoError(t, err)
		return true, errors.New("response lost after publication")
	}
	require.Error(t, writer.Close(ctx, map[string]any{"state": "landed-boundary"}))
	require.True(t, writer.uncertain)
	for _, path := range privateInputs {
		input, err := store.Open(path)
		require.NoError(t, err)
		require.NoError(t, input.Close())
	}
	fresh, err := realCatalog.LoadTable(ctx, writer.table.Identifier())
	require.NoError(t, err)
	require.Equal(t, map[string]int64{"landed": 7}, nativeVisibleValues(t, fresh))
	require.Contains(t, fresh.Properties()[nativeRecoveryProperty], "landed-boundary")
}

func TestNativeCopyOnWriteTypedSpillFidelity(t *testing.T) {
	schema := apacheiceberg.NewSchema(0,
		apacheiceberg.NestedField{ID: 1, Name: "id", Type: apacheiceberg.Int64Type{}, Required: true},
		apacheiceberg.NestedField{ID: 2, Name: "money", Type: apacheiceberg.DecimalTypeOf(30, 4)},
		apacheiceberg.NestedField{ID: 3, Name: "payload", Type: apacheiceberg.BinaryType{}},
		apacheiceberg.NestedField{ID: 4, Name: "at", Type: apacheiceberg.TimestampTzType{}},
	)
	payload := bytes.Repeat([]byte{0, 255, 1, 128}, 131072)
	at := time.Date(2026, 11, 19, 17, 3, 12, 123456000, time.UTC)
	batch, err := nativeArrowRecord(schema, []map[string]any{{"id": int64(9007199254740993), "money": "123456789012345678901234.5678", "payload": payload, "at": at}})
	require.NoError(t, err)
	defer batch.Release()
	ctx := context.Background()
	cfg, stream, cat := nativeCOWFixture(t, types.UpdateTypePosition)
	writer := openNativeTestWriter(ctx, t, cfg, stream, cat, nil, "typed-replay", false)
	defer writer.Abort()
	require.NoError(t, writer.cow.spill(ctx, writer, batch))
	store, err := writer.table.FS(ctx)
	require.NoError(t, err)
	file, err := store.Open(writer.cow.paths[0])
	require.NoError(t, err)
	defer file.Close()
	reader, err := ipc.NewReader(file)
	require.NoError(t, err)
	defer reader.Release()
	require.True(t, reader.Next())
	replayed := reader.RecordBatch()
	require.Equal(t, int64(9007199254740993), replayed.Column(0).(*array.Int64).Value(0))
	require.Equal(t, "123456789012345678901234.5678", replayed.Column(1).(*array.Decimal128).Value(0).ToString(4))
	require.Equal(t, payload, replayed.Column(2).(*array.Binary).Value(0))
	require.Equal(t, at.UnixMicro(), int64(replayed.Column(3).(*array.Timestamp).Value(0)))
}

type nativeCOWClearingAllocator struct {
	memory.Allocator
}

func (a nativeCOWClearingAllocator) Free(data []byte) {
	clear(data)
	a.Allocator.Free(data)
}

func TestNativeCopyOnWriteFilterOwnsIPCIdentities(t *testing.T) {
	for _, test := range []struct {
		name  string
		ids   []string
		start int64
		end   int64
	}{
		{name: "empty"},
		{name: "single", ids: []string{"identity-to-retain"}, end: 1},
		{name: "multiple", ids: []string{"first", "second", "third"}, end: 3},
		{name: "sliced", ids: []string{"exclude-before", "", "identity-to-retain", "other-你好", "exclude-after"}, start: 1, end: 4},
		{name: "duplicates", ids: []string{"same", "same", "other"}, end: 3},
	} {
		t.Run(test.name, func(t *testing.T) {
			schema := apacheiceberg.NewSchema(0,
				apacheiceberg.NestedField{ID: 1, Name: constants.OlakeID, Type: apacheiceberg.StringType{}, Required: true},
				apacheiceberg.NestedField{ID: 2, Name: "wide", Type: apacheiceberg.StringType{}},
			)
			rows := make([]map[string]any, len(test.ids))
			for row, id := range test.ids {
				rows[row] = map[string]any{constants.OlakeID: id, "wide": strings.Repeat("wide", 1024)}
			}
			batch, err := nativeArrowRecord(schema, rows)
			require.NoError(t, err)
			var serialized bytes.Buffer
			writer := ipc.NewWriter(&serialized, ipc.WithSchema(batch.Schema()))
			require.NoError(t, writer.Write(batch))
			require.NoError(t, writer.Close())
			batch.Release()
			reader, err := ipc.NewReader(bytes.NewReader(serialized.Bytes()), ipc.WithAllocator(nativeCOWClearingAllocator{Allocator: memory.DefaultAllocator}))
			require.NoError(t, err)
			require.True(t, reader.Next())
			sliced := reader.RecordBatch().NewSlice(test.start, test.end)
			filter, err := nativeCOWFilter(sliced)
			require.NoError(t, err)
			sliced.Release()
			reader.Release()
			values := make([]apacheiceberg.Literal, 0, test.end-test.start)
			for _, id := range test.ids[test.start:test.end] {
				values = append(values, apacheiceberg.NewLiteral(id))
			}
			term := apacheiceberg.Reference(constants.OlakeID)
			expected := apacheiceberg.NewAnd(apacheiceberg.NotNull(term), apacheiceberg.SetPredicate(apacheiceberg.OpIn, term, values))
			require.True(t, expected.Equals(filter), "the transaction retains the filter after the IPC reader releases its body")
		})
	}
}

func TestNativeCopyOnWritePreservesRowsAcrossPartitionEvolution(t *testing.T) {
	ctx := context.Background()
	cfg, stream, cat := nativeCOWFixture(t, types.UpdateTypePosition)
	seed := openNativeTestWriter(ctx, t, cfg, stream, cat, nil, "seed", true)
	require.NoError(t, seed.Write(ctx, []types.RawRecord{
		nativeCOWRecord("a", "r", 1, "a", 1), nativeCOWRecord("b", "r", 2, "b", 2),
		nativeCOWRecord("c", "r", 3, "c", 3), nativeCOWRecord("d", "r", 4, "d", 4),
	}))
	require.NoError(t, seed.Close(ctx, nil))
	tx := seed.table.NewTransaction()
	require.NoError(t, tx.UpdateSpec(true).AddIdentity("tenant").Commit())
	evolved, err := tx.Commit(ctx)
	require.NoError(t, err)
	writer := openNativeTestWriter(ctx, t, cfg, stream, cat, nil, "evolved", false)
	require.NoError(t, writer.Write(ctx, []types.RawRecord{nativeCOWRecord("a", "u", 1, "a", 10)}))
	require.NoError(t, writer.Close(ctx, map[string]any{"state": "after-evolution"}))
	require.Equal(t, map[string]int64{"a": 10, "b": 2, "c": 3, "d": 4}, nativeVisibleValues(t, writer.table))
	require.Equal(t, evolved.Metadata().PartitionSpec(), writer.table.Metadata().PartitionSpec())
	hasDeletes, err := nativeCOWHasDeletes(ctx, writer.table)
	require.NoError(t, err)
	require.False(t, hasDeletes)
}

func TestNativeCopyOnWriteBackfillPreservesEveryChunk(t *testing.T) {
	ctx := context.Background()
	cfg, stream, cat := nativeCOWFixture(t, types.UpdateTypeDeletionVector)
	first := openNativeTestWriter(ctx, t, cfg, stream, cat, nil, "first-chunk", true)
	second := openNativeTestWriter(ctx, t, cfg, stream, cat, nil, "second-chunk", true)
	require.NoError(t, first.Write(ctx, []types.RawRecord{nativeCOWRecord("a", "r", 1, "a", 1)}))
	require.NoError(t, second.Write(ctx, []types.RawRecord{nativeCOWRecord("b", "r", 2, "b", 2)}))
	require.NoError(t, first.Close(ctx, nil))
	require.NoError(t, second.Close(ctx, nil))
	require.Equal(t, map[string]int64{"a": 1, "b": 2}, nativeVisibleValues(t, second.table))
	var metadata types.MetadataState
	require.NoError(t, json.Unmarshal([]byte(second.table.Properties()[nativeRecoveryProperty]), &metadata))
	require.ElementsMatch(t, []string{"first-chunk", "second-chunk"}, metadata.FullRefreshCommittedIDs)
	guard := nativeCOWManifestScope(t, second.table)
	guardedCatalog := &nativeCommitFaultCatalog{Catalog: cat, fs: guard}
	validation := openNativeTestWriter(ctx, t, cfg, stream, guardedCatalog, nil, "validate-data-only", false)
	require.NoError(t, validation.Close(ctx, nil))
	require.Equal(t, nativeSnapshotID(second.table), nativeSnapshotID(validation.table))
	fresh, err := cat.LoadTable(ctx, second.table.Identifier())
	require.NoError(t, err)
	require.Equal(t, map[string]int64{"a": 1, "b": 2}, nativeVisibleValues(t, fresh))
}

func TestNativeCopyOnWriteEmptyTablePublishesCommonCheckpoint(t *testing.T) {
	ctx := context.Background()
	cfg, stream, cat := nativeCOWFixture(t, types.UpdateTypeDeletionVector)
	backfill := openNativeTestWriter(ctx, t, cfg, stream, cat, nil, "empty-backfill", true)
	require.NoError(t, backfill.Close(ctx, nil))
	writer := openNativeTestWriter(ctx, t, cfg, stream, cat, nil, "first-checkpoint", false)
	before := nativeSnapshotID(writer.table)
	require.NoError(t, writer.Close(ctx, map[string]any{"state": `{"lsn":"0/100"}`, "dedup_inserts": false}))
	require.Equal(t, before, nativeSnapshotID(writer.table), "empty checkpoint uses the common property-only commit")
	var metadata types.MetadataState
	require.NoError(t, json.Unmarshal([]byte(writer.table.Properties()[nativeRecoveryProperty]), &metadata))
	require.Equal(t, `{"lsn":"0/100"}`, metadata.State)
	require.Empty(t, nativeVisibleValues(t, writer.table))
}
