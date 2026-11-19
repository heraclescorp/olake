package iceberg

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	apacheiceberg "github.com/apache/iceberg-go"

	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/table"
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
	options := types.DefaultCOWOptions()
	options.MaxBatchRows, options.MaxBatchBytes = 2, 2048
	stream.StreamMetadata.CowOptions = &options
	return cfg, stream, cat
}

func nativeCOWRecord(label, operation string, key int64, tenant string, value int64) types.RawRecord {
	record := nativeTestRecord(label, operation, value, "one")
	record.Data["id"], record.Data["tenant"] = key, tenant
	return record
}

// Tiny, real local-file/Hadoop-catalog smoke; no services or production access.
func TestNativeCopyOnWriteBoundedExactTuples(t *testing.T) {
	ctx := context.Background()
	cfg, stream, cat := nativeCOWFixture(t, types.UpdateTypeDeletionVector)
	seed := openNativeTestWriter(ctx, t, cfg, stream, cat, nil, "seed", true)
	require.Equal(t, 2, seed.table.Metadata().Version(), "inactive saved dv must not create v3")
	require.NoError(t, seed.Write(ctx, []types.RawRecord{
		nativeCOWRecord("1a", "r", 9007199254740993, "a", 1), nativeCOWRecord("1b", "r", 9007199254740993, "b", 2),
		nativeCOWRecord("2a", "r", 9007199254740994, "a", 3), nativeCOWRecord("2b", "r", 9007199254740994, "b", 4),
	}))
	require.NoError(t, seed.Close(ctx, map[string]any{"state": "start"}))
	writer := openNativeTestWriter(ctx, t, cfg, stream, cat, nil, "cdc", false)
	require.NoError(t, writer.Write(ctx, []types.RawRecord{
		nativeCOWRecord("first", "u", 9007199254740993, "a", 10), nativeCOWRecord("latest", "u", 9007199254740993, "a", 11),
		nativeCOWRecord("new2b", "u", 9007199254740994, "b", 40), nativeCOWRecord("tombstone", "d", 9007199254740993, "a", 12),
	}))
	privateDirectory := writer.cow.directory
	info, err := os.Stat(privateDirectory)
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0700), info.Mode().Perm())
	info, err = os.Stat(filepath.Join(privateDirectory, "0.arrow"))
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0600), info.Mode().Perm())
	require.Equal(t, map[string]int64{"1a": 1, "1b": 2, "2a": 3, "2b": 4}, nativeVisibleValues(t, writer.table), "Write is private until Close")
	require.NoError(t, writer.Close(ctx, map[string]any{"state": "end", "dedup_inserts": false}))
	require.Equal(t, map[string]int64{"tombstone": 12, "1b": 2, "2a": 3, "new2b": 40}, nativeVisibleValues(t, writer.table))
	deletes, _, err := nativeCOWLiveFiles(ctx, writer.table)
	require.NoError(t, err)
	require.Empty(t, deletes)
	_, err = os.Stat(privateDirectory)
	require.True(t, os.IsNotExist(err))
	before := nativeSnapshotID(writer.table)
	quiet := openNativeTestWriter(ctx, t, cfg, stream, cat, nil, "quiet", false)
	require.NoError(t, quiet.Close(ctx, map[string]any{"state": "quiet-end", "dedup_inserts": true}))
	require.NotEqual(t, before, nativeSnapshotID(quiet.table), "quiet checkpoint is snapshot-fenced")
	require.Contains(t, quiet.table.Properties()[nativeRecoveryProperty], "quiet-end")
	terminal := openNativeTestWriter(ctx, t, cfg, stream, cat, nil, "terminal", false)
	require.NoError(t, terminal.Close(ctx, map[string]any{"state": "quiet-end", "dedup_inserts": false}))
	var metadata types.MetadataState
	require.NoError(t, json.Unmarshal([]byte(terminal.table.Properties()[nativeRecoveryProperty]), &metadata))
	require.NotNil(t, metadata.DedupInserts)
	require.False(t, *metadata.DedupInserts, "same-LSN terminal close must retire the overlap flag")
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
	require.NoError(t, writer.Write(ctx, []types.RawRecord{nativeCOWRecord("replacement", "u", 1, "a", 2)}))
	require.NoError(t, writer.Close(ctx, map[string]any{"state": "end"}))
	require.Equal(t, map[string]int64{"replacement": 2, "null-bystander": 8}, nativeVisibleValues(t, writer.table))
}

func TestNativeCopyOnWriteMaterializesEveryDeleteKind(t *testing.T) {
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
			_, _, err = newNativeWriter(ctx, cfg, stream, &destination.Options{ThreadID: "not-authorized"}, cat, nil)
			require.ErrorContains(t, err, "materialize_existing_deletes=true")
			stream.StreamMetadata.CowOptions.MaterializeExistingDeletes = true
			cow := openNativeTestWriter(ctx, t, cfg, stream, cat, nil, "authorized", false)
			defer cow.Abort()
			require.Equal(t, originalCheckpoint, cow.table.Properties()[nativeRecoveryProperty])
			require.Equal(t, old.table.Metadata().TableUUID(), cow.table.Metadata().TableUUID())
			shape, err := ordinaryRecoveryShape(cow.table.Metadata())
			require.NoError(t, err)
			require.Equal(t, originalShape, shape)
			require.Equal(t, map[string]int64{"a": 3, "b": 2}, nativeVisibleValues(t, cow.table))
			deletes, _, err := nativeCOWLiveFiles(ctx, cow.table)
			require.NoError(t, err)
			require.Empty(t, deletes)
		})
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
	require.NoError(t, writer.Write(ctx, []types.RawRecord{nativeCOWRecord("updated", "u", 1, "a", 9)}))
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
	require.Equal(t, map[string]int64{"updated": 9, "b": 2}, nativeVisibleValues(t, writer.table))
}

func TestNativeCopyOnWriteUnknownCommitRetainsReplay(t *testing.T) {
	ctx := context.Background()
	cfg, stream, realCatalog := nativeCOWFixture(t, types.UpdateTypePosition)
	create := openNativeTestWriter(ctx, t, cfg, stream, realCatalog, nil, "create", true)
	create.Abort()
	fault := &nativeCommitFaultCatalog{Catalog: realCatalog}
	writer := openNativeTestWriter(ctx, t, cfg, stream, fault, nil, "unknown", false)
	require.NoError(t, writer.Write(ctx, []types.RawRecord{nativeCOWRecord("landed", "c", 1, "a", 7)}))
	directory := writer.cow.directory
	t.Cleanup(func() { _ = os.RemoveAll(directory) })
	fault.fault = func(ctx context.Context, id table.Identifier, requirements []table.Requirement, updates []table.Update) (bool, error) {
		_, _, err := realCatalog.CommitTable(ctx, id, requirements, updates)
		require.NoError(t, err)
		return true, errors.New("response lost after publication")
	}
	require.Error(t, writer.Close(ctx, map[string]any{"state": "landed-boundary"}))
	require.True(t, writer.uncertain)
	_, err := os.Stat(filepath.Join(directory, "0.arrow"))
	require.NoError(t, err)
	fresh, err := realCatalog.LoadTable(ctx, writer.table.Identifier())
	require.NoError(t, err)
	require.Equal(t, map[string]int64{"landed": 7}, nativeVisibleValues(t, fresh))
	require.Contains(t, fresh.Properties()[nativeRecoveryProperty], "landed-boundary")
	retry := openNativeTestWriter(ctx, t, cfg, stream, realCatalog, nil, "restarted-thread", false)
	require.NoError(t, retry.Write(ctx, []types.RawRecord{nativeCOWRecord("must-not-reapply", "u", 1, "a", 99)}))
	require.NoError(t, retry.Close(ctx, map[string]any{"state": "landed-boundary"}))
	require.Equal(t, nativeSnapshotID(fresh), nativeSnapshotID(retry.table))
	require.Equal(t, map[string]int64{"landed": 7}, nativeVisibleValues(t, retry.table))
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
	_, stream, _ := nativeCOWFixture(t, types.UpdateTypeEquality)
	spool, err := newNativeCOW(stream)
	require.NoError(t, err)
	defer spool.cleanup(false)
	require.NoError(t, spool.spill(batch))
	file, err := os.Open(filepath.Join(spool.directory, "0.arrow"))
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
	_, err = nativeCOWKey(schema, []string{"id"}, map[string]any{"id": float64(9007199254740992)})
	require.ErrorContains(t, err, "exact integer")
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
	require.NoError(t, writer.Write(ctx, []types.RawRecord{nativeCOWRecord("new-a", "u", 1, "a", 10)}))
	require.NoError(t, writer.Close(ctx, map[string]any{"state": "after-evolution"}))
	require.Equal(t, map[string]int64{"new-a": 10, "b": 2, "c": 3, "d": 4}, nativeVisibleValues(t, writer.table))
	require.Equal(t, evolved.Metadata().PartitionSpec(), writer.table.Metadata().PartitionSpec())
	deletes, _, err := nativeCOWLiveFiles(ctx, writer.table)
	require.NoError(t, err)
	require.Empty(t, deletes)
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
}

func TestNativeCopyOnWriteEmptyTablePublishesFencedCheckpoint(t *testing.T) {
	ctx := context.Background()
	cfg, stream, cat := nativeCOWFixture(t, types.UpdateTypeDeletionVector)
	backfill := openNativeTestWriter(ctx, t, cfg, stream, cat, nil, "empty-backfill", true)
	require.NoError(t, backfill.Close(ctx, nil))
	writer := openNativeTestWriter(ctx, t, cfg, stream, cat, nil, "first-checkpoint", false)
	before := nativeSnapshotID(writer.table)
	require.NoError(t, writer.Close(ctx, map[string]any{"state": `{"lsn":"0/100"}`, "dedup_inserts": false}))
	require.NotEqual(t, before, nativeSnapshotID(writer.table))
	var metadata types.MetadataState
	require.NoError(t, json.Unmarshal([]byte(writer.table.Properties()[nativeRecoveryProperty]), &metadata))
	require.Equal(t, `{"lsn":"0/100"}`, metadata.State)
	require.Empty(t, nativeVisibleValues(t, writer.table))
}
