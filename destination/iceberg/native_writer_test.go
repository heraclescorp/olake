package iceberg

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow/array"
	apacheiceberg "github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/catalog/hadoop"
	"github.com/apache/iceberg-go/table"
	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/destination"
	"github.com/datazip-inc/olake/types"
	"github.com/stretchr/testify/require"
)

type nativeMemoryIndex struct {
	rows      map[string]types.RowLocation
	snapshot  int64
	commitErr error
}

func (i *nativeMemoryIndex) Lookup(key string) (types.RowLocation, bool, error) {
	loc, ok := i.rows[key]
	return loc, ok, nil
}
func (i *nativeMemoryIndex) Commit(batch *types.StreamIndexThread, snapshot *int64) error {
	if i.commitErr != nil {
		return i.commitErr
	}
	if err := batch.Range(func(key string, loc types.RowLocation) error { i.rows[key] = loc; return nil }); err != nil {
		return err
	}
	if snapshot != nil {
		i.snapshot = *snapshot
	}
	return nil
}
func (i *nativeMemoryIndex) LastCommittedSnapshot() (int64, error) { return i.snapshot, nil }
func (i *nativeMemoryIndex) Truncate() error                       { clear(i.rows); i.snapshot = 0; return nil }
func (i *nativeMemoryIndex) Close() error                          { return nil }

func nativeWriterFixture(t *testing.T, mode types.UpdateType, normalized bool, partition string) (*Config, *types.ConfiguredStream, catalog.Catalog) {
	t.Helper()
	root := t.TempDir()
	cat, err := hadoop.NewCatalog("native-writer-test", root, nil)
	require.NoError(t, err)
	stream := types.NewStream("records", "db", nil)
	stream.DestinationDatabase, stream.DestinationTable = "db", "records"
	stream.UpsertField(constants.OlakeID, types.String, false, true)
	stream.UpsertField(constants.OpType, types.String, false, true)
	stream.UpsertField(constants.OlakeTimestamp, types.TimestampMicro, false, true)
	stream.UpsertField("value", types.Int64, true, false)
	stream.UpsertField("part", types.String, true, false)
	stream.UpsertField("blob", types.FixedBinaryOf(4), true, false)
	updateMode := string(mode)
	configured := &types.ConfiguredStream{Stream: stream, StreamMetadata: types.StreamMetadata{Normalization: &normalized, UpdateType: &updateMode, PartitionRegex: partition, UseSourceColumnNames: true}}
	return &Config{IcebergS3Path: root, IcebergDatabase: "db"}, configured, cat
}

func openNativeTestWriter(t *testing.T, ctx context.Context, cfg *Config, stream *types.ConfiguredStream, cat catalog.Catalog, index *nativeMemoryIndex, thread string, backfill bool) *nativeWriter {
	t.Helper()
	opts := &destination.Options{ThreadID: thread, Backfill: backfill}
	if index != nil {
		opts.TableIndex = index
	}
	writer, _, err := newNativeWriter(ctx, cfg, stream, opts, cat, types.NewStreamIndexThread(opts.TableIndex))
	require.NoError(t, err)
	return writer
}

func nativeTestRecord(id, operation string, value int64, partition string) types.RawRecord {
	return types.RawRecord{Data: map[string]any{"value": value, "part": partition, "blob": []byte{0, 1, 2, 3}}, OlakeColumns: map[string]any{constants.OlakeID: id, constants.OpType: operation, constants.OlakeTimestamp: time.Date(2025, 3, 8, 12, 0, 0, 123456000, time.UTC)}}
}

func nativeVisibleValues(t *testing.T, tbl *table.Table) map[string]int64 {
	t.Helper()
	scan := tbl.Scan()
	defer scan.Close()
	schema, records, err := scan.ToArrowRecords(context.Background())
	require.NoError(t, err)
	idColumn := schema.FieldIndices(constants.OlakeID)[0]
	valueColumn := schema.FieldIndices("value")[0]
	result := make(map[string]int64)
	for batch, err := range records {
		require.NoError(t, err)
		ids, values := batch.Column(idColumn).(*array.String), batch.Column(valueColumn).(*array.Int64)
		for row := range int(batch.NumRows()) {
			id := ids.Value(row)
			_, duplicate := result[id]
			require.False(t, duplicate, "duplicate visible key %s", id)
			result[id] = values.Value(row)
		}
		batch.Release()
	}
	return result
}

func TestNativeWriterEqualityRepeatsAndSoftDelete(t *testing.T) {
	ctx := context.Background()
	cfg, stream, cat := nativeWriterFixture(t, types.UpdateTypeEquality, true, "/{part, identity}")
	seed := openNativeTestWriter(t, ctx, cfg, stream, cat, nil, "snapshot-1", true)
	require.NoError(t, seed.Write(ctx, []types.RawRecord{nativeTestRecord("a", "r", 1, "one"), nativeTestRecord("b", "r", 7, "one")}))
	require.NoError(t, seed.Close(ctx, nil))
	writer := openNativeTestWriter(t, ctx, cfg, stream, cat, nil, "cdc-1", false)
	require.NoError(t, writer.Write(ctx, []types.RawRecord{nativeTestRecord("a", "u", 2, "one"), nativeTestRecord("a", "u", 3, "two")}))
	require.NoError(t, writer.Write(ctx, []types.RawRecord{nativeTestRecord("a", "d", 4, "two")}))
	require.NoError(t, writer.Close(ctx, map[string]any{"state": `{"captured_cdc_pos":"0/ABC"}`, "dedup_inserts": false}))
	require.Equal(t, map[string]int64{"a": 4, "b": 7}, nativeVisibleValues(t, writer.table))
	var state types.MetadataState
	require.NoError(t, json.Unmarshal([]byte(writer.table.Properties()[nativeRecoveryProperty]), &state))
	require.Equal(t, "cdc-1", state.ID)
	require.Equal(t, []string{"snapshot-1"}, state.FullRefreshCommittedIDs)
	require.NotNil(t, state.DedupInserts)
	require.False(t, *state.DedupInserts)
}

func TestNativeWriterIndexedDeletesAndRecoveryReads(t *testing.T) {
	for _, mode := range []types.UpdateType{types.UpdateTypePosition, types.UpdateTypeDeletionVector} {
		t.Run(string(mode), func(t *testing.T) {
			ctx := context.Background()
			cfg, stream, cat := nativeWriterFixture(t, mode, true, "/{part, identity}")
			index := &nativeMemoryIndex{rows: make(map[string]types.RowLocation)}
			seed := openNativeTestWriter(t, ctx, cfg, stream, cat, index, "snapshot", true)
			require.NoError(t, seed.Write(ctx, []types.RawRecord{nativeTestRecord("a", "r", 1, "one"), nativeTestRecord("b", "r", 2, "one")}))
			original := seed.dataFiles[0].FilePath()
			require.NoError(t, seed.Close(ctx, nil))
			writer := openNativeTestWriter(t, ctx, cfg, stream, cat, index, "cdc-a", false)
			require.NoError(t, writer.Write(ctx, []types.RawRecord{nativeTestRecord("a", "u", 3, "two"), nativeTestRecord("a", "u", 4, "one")}))
			loc, found, err := writer.indexThread.Lookup("a")
			require.NoError(t, err)
			require.True(t, found)
			require.NoError(t, writer.EnsureReadable(ctx, []string{loc.FilePath}))
			rows, err := writer.ReadRows(ctx, loc.FilePath, []int64{loc.Position}, []string{"value", "blob"})
			require.NoError(t, err)
			require.Equal(t, int64(4), rows[loc.Position]["value"])
			require.Equal(t, []byte{0, 1, 2, 3}, rows[loc.Position]["blob"])
			require.NoError(t, writer.Close(ctx, map[string]any{"state": "a"}))
			next := openNativeTestWriter(t, ctx, cfg, stream, cat, index, "cdc-b", false)
			require.NoError(t, next.Write(ctx, []types.RawRecord{nativeTestRecord("b", "d", 5, "two")}))
			require.NoError(t, next.Close(ctx, map[string]any{"state": "b"}))
			require.Equal(t, map[string]int64{"a": 4, "b": 5}, nativeVisibleValues(t, next.table))
			require.Equal(t, nativeSnapshotID(next.table), index.snapshot)
			positions, vectors, err := nativeDeletedPositions(ctx, next.table, original)
			require.NoError(t, err)
			require.Equal(t, []int64{0, 1}, positions)
			if mode == types.UpdateTypeDeletionVector {
				require.Len(t, vectors, 1)
			} else {
				require.Empty(t, vectors)
			}
		})
	}
}

func TestNativeWriterEqualityMigration(t *testing.T) {
	for _, mode := range []types.UpdateType{types.UpdateTypePosition, types.UpdateTypeDeletionVector} {
		t.Run(string(mode), func(t *testing.T) {
			ctx := context.Background()
			cfg, stream, cat := nativeWriterFixture(t, types.UpdateTypeEquality, true, "")
			seed := openNativeTestWriter(t, ctx, cfg, stream, cat, nil, "snapshot", true)
			require.NoError(t, seed.Write(ctx, []types.RawRecord{nativeTestRecord("a", "r", 1, "one"), nativeTestRecord("b", "r", 2, "one")}))
			require.NoError(t, seed.Close(ctx, nil))
			cdc := openNativeTestWriter(t, ctx, cfg, stream, cat, nil, "eq-cdc", false)
			require.NoError(t, cdc.Write(ctx, []types.RawRecord{nativeTestRecord("a", "u", 3, "one")}))
			require.NoError(t, cdc.Close(ctx, map[string]any{"state": "checkpoint"}))
			update := string(mode)
			stream.StreamMetadata.UpdateType = &update
			index := &nativeMemoryIndex{rows: make(map[string]types.RowLocation)}
			migrated := openNativeTestWriter(t, ctx, cfg, stream, cat, index, "migrate", false)
			entries, err := nativeLiveEntries(ctx, migrated.table)
			require.NoError(t, err)
			for _, entry := range entries {
				require.NotEqual(t, apacheiceberg.EntryContentEqDeletes, entry.DataFile().ContentType())
			}
			require.Equal(t, map[string]int64{"a": 3, "b": 2}, nativeVisibleValues(t, migrated.table))
			require.NoError(t, migrated.Write(ctx, []types.RawRecord{nativeTestRecord("a", "u", 4, "two")}))
			require.NoError(t, migrated.Close(ctx, map[string]any{"state": "next"}))
			require.Equal(t, map[string]int64{"a": 4, "b": 2}, nativeVisibleValues(t, migrated.table))
		})
	}
}

func TestNativeWriterSchemaEvolutionKeepsFieldIDs(t *testing.T) {
	ctx := context.Background()
	cfg, stream, cat := nativeWriterFixture(t, types.UpdateTypeEquality, true, "")
	writer := openNativeTestWriter(t, ctx, cfg, stream, cat, nil, "schema", true)
	before := make(map[string]int)
	for _, field := range writer.table.Schema().Fields() {
		before[field.Name] = field.ID
	}
	require.NoError(t, writer.Write(ctx, []types.RawRecord{nativeTestRecord("a", "r", 1, "one")}))
	columns := writer.schemaMap()
	columns["new_column"] = "string"
	require.NoError(t, writer.EvolveSchema(ctx, columns))
	for _, field := range writer.table.Schema().Fields() {
		if id, existing := before[field.Name]; existing {
			require.Equal(t, id, field.ID)
		}
	}
	record := nativeTestRecord("b", "r", 2, "one")
	record.Data["new_column"] = "new value"
	require.NoError(t, writer.Write(ctx, []types.RawRecord{record}))
	require.NoError(t, writer.Close(ctx, nil))
	require.Equal(t, map[string]int64{"a": 1, "b": 2}, nativeVisibleValues(t, writer.table))
}

func TestNativeWriterRawAppendAndAbort(t *testing.T) {
	ctx := context.Background()
	cfg, stream, cat := nativeWriterFixture(t, types.UpdateTypeEquality, false, "/{part, identity}")
	appendMode := true
	stream.StreamMetadata.AppendMode = &appendMode
	writer := openNativeTestWriter(t, ctx, cfg, stream, cat, nil, "raw", false)
	row := nativeTestRecord("a", "u", 1, "null")
	row.Data[constants.StringifiedData] = `{"nested":{"text":"raw"}}`
	require.NoError(t, writer.Write(ctx, []types.RawRecord{row}))
	require.NoError(t, writer.Close(ctx, map[string]any{"state": "raw"}))
	field, ok := writer.table.Schema().FindFieldByName(constants.StringifiedData)
	require.True(t, ok)
	require.Equal(t, "string", field.Type.String())
	_, hasSourceColumn := writer.table.Schema().FindFieldByName("value")
	require.False(t, hasSourceColumn)
	abort := openNativeTestWriter(t, ctx, cfg, stream, cat, nil, "abort", false)
	require.NoError(t, abort.Write(ctx, []types.RawRecord{row}))
	paths := slices.Collect(maps.Keys(abort.paths))
	require.NotEmpty(t, paths)
	abort.Abort()
	fs, err := writer.table.FS(ctx)
	require.NoError(t, err)
	for _, path := range paths {
		_, err := fs.Open(path)
		require.Error(t, err)
	}
	fresh, err := cat.LoadTable(ctx, writer.table.Identifier())
	require.NoError(t, err)
	require.Equal(t, nativeSnapshotID(writer.table), nativeSnapshotID(fresh))
}

type nativeCommitFaultCatalog struct {
	catalog.Catalog
	fault func(context.Context, table.Identifier, []table.Requirement, []table.Update) (bool, error)
	calls int
	loads int
}

func (c *nativeCommitFaultCatalog) bind(tbl *table.Table) *table.Table {
	return table.New(tbl.Identifier(), tbl.Metadata(), tbl.MetadataLocation(), tbl.FS, c)
}
func (c *nativeCommitFaultCatalog) LoadTable(ctx context.Context, id table.Identifier) (*table.Table, error) {
	c.loads++
	tbl, err := c.Catalog.LoadTable(ctx, id)
	if err != nil {
		return nil, err
	}
	return c.bind(tbl), nil
}
func (c *nativeCommitFaultCatalog) CommitTable(ctx context.Context, id table.Identifier, reqs []table.Requirement, updates []table.Update) (table.Metadata, string, error) {
	c.calls++
	if c.fault != nil {
		handled, err := c.fault(ctx, id, reqs, updates)
		if handled {
			return nil, "", err
		}
	}
	return c.Catalog.CommitTable(ctx, id, reqs, updates)
}

func TestNativeWriterRetriesMergeFreshRecoveryState(t *testing.T) {
	ctx := context.Background()
	cfg, stream, realCatalog := nativeWriterFixture(t, types.UpdateTypeEquality, true, "")
	seed := openNativeTestWriter(t, ctx, cfg, stream, realCatalog, nil, "snapshot", true)
	require.NoError(t, seed.Write(ctx, []types.RawRecord{nativeTestRecord("a", "r", 1, "one")}))
	require.NoError(t, seed.Close(ctx, nil))
	cat := &nativeCommitFaultCatalog{Catalog: realCatalog}
	writer := openNativeTestWriter(t, ctx, cfg, stream, cat, nil, "retry", false)
	require.NoError(t, writer.Write(ctx, []types.RawRecord{nativeTestRecord("a", "u", 2, "one")}))
	cat.fault = func(ctx context.Context, id table.Identifier, _ []table.Requirement, _ []table.Update) (bool, error) {
		cat.fault = nil
		fresh, err := realCatalog.LoadTable(ctx, id)
		require.NoError(t, err)
		tx := fresh.NewTransaction()
		require.NoError(t, tx.SetProperties(apacheiceberg.Properties{nativeRecoveryProperty: `{"state":"{\"other_partition\":17}"}`}))
		_, err = tx.Commit(ctx)
		require.NoError(t, err)
		return true, table.ErrCommitFailed
	}
	require.NoError(t, writer.Close(ctx, map[string]any{"state": `{"this_partition":18}`}))
	var state struct {
		State string `json:"state"`
	}
	require.NoError(t, json.Unmarshal([]byte(writer.table.Properties()[nativeRecoveryProperty]), &state))
	var offsets map[string]int
	require.NoError(t, json.Unmarshal([]byte(state.State), &offsets))
	require.Equal(t, map[string]int{"other_partition": 17, "this_partition": 18}, offsets)
	require.Equal(t, map[string]int64{"a": 2}, nativeVisibleValues(t, writer.table))
}

func TestNativeWriterUnknownCommitRetainsFilesAndRecovery(t *testing.T) {
	ctx := context.Background()
	cfg, stream, realCatalog := nativeWriterFixture(t, types.UpdateTypeEquality, true, "")
	seed := openNativeTestWriter(t, ctx, cfg, stream, realCatalog, nil, "create", true)
	seed.Abort()
	cat := &nativeCommitFaultCatalog{Catalog: realCatalog}
	writer := openNativeTestWriter(t, ctx, cfg, stream, cat, nil, "unknown", false)
	require.NoError(t, writer.Write(ctx, []types.RawRecord{nativeTestRecord("a", "i", 1, "one")}))
	paths := slices.Collect(maps.Keys(writer.paths))
	cat.fault = func(ctx context.Context, id table.Identifier, reqs []table.Requirement, updates []table.Update) (bool, error) {
		_, _, err := realCatalog.CommitTable(ctx, id, reqs, updates)
		require.NoError(t, err)
		return true, errors.New("connection reset after commit; token=do-not-leak")
	}
	err := writer.Close(ctx, map[string]any{"state": "committed"})
	require.Error(t, err)
	require.NotContains(t, err.Error(), "do-not-leak")
	require.True(t, writer.uncertain)
	require.Error(t, writer.Close(ctx, map[string]any{"state": "retry"}))
	writer.Abort()
	fs, err := writer.table.FS(ctx)
	require.NoError(t, err)
	for _, path := range paths {
		file, err := fs.Open(path)
		require.NoError(t, err)
		require.NoError(t, file.Close())
	}
	fresh, err := realCatalog.LoadTable(ctx, writer.table.Identifier())
	require.NoError(t, err)
	require.Equal(t, map[string]int64{"a": 1}, nativeVisibleValues(t, fresh))
	require.Contains(t, fresh.Properties()[nativeRecoveryProperty], "committed")
}

func TestNativeWriterRejectsStaleIndexWithoutPublishing(t *testing.T) {
	ctx := context.Background()
	cfg, stream, cat := nativeWriterFixture(t, types.UpdateTypePosition, true, "")
	index := &nativeMemoryIndex{rows: make(map[string]types.RowLocation)}
	seed := openNativeTestWriter(t, ctx, cfg, stream, cat, index, "snapshot", true)
	require.NoError(t, seed.Write(ctx, []types.RawRecord{nativeTestRecord("a", "r", 1, "one")}))
	require.NoError(t, seed.Close(ctx, nil))
	stale := openNativeTestWriter(t, ctx, cfg, stream, cat, index, "stale", false)
	require.NoError(t, stale.Write(ctx, []types.RawRecord{nativeTestRecord("a", "u", 2, "one")}))
	external := openNativeTestWriter(t, ctx, cfg, stream, cat, nil, "external", true)
	require.NoError(t, external.Write(ctx, []types.RawRecord{nativeTestRecord("b", "r", 3, "one")}))
	require.NoError(t, external.Close(ctx, nil))
	err := stale.Close(ctx, map[string]any{"state": "unsafe"})
	require.ErrorIs(t, err, table.ErrCommitFailed)
	fresh, err := cat.LoadTable(ctx, external.table.Identifier())
	require.NoError(t, err)
	require.Equal(t, map[string]int64{"a": 1, "b": 3}, nativeVisibleValues(t, fresh))
	require.NotContains(t, fresh.Properties()[nativeRecoveryProperty], "unsafe")
}

func TestNativeRecoveryMergeAndConflictClasses(t *testing.T) {
	state, changed, err := mergeNativeRecovery(`{"state":"{\"a\":1}","full_refresh_committed_ids":["one"]}`, "two", map[string]any{"state": `{"b":2}`, "dedup_inserts": false}, true)
	require.NoError(t, err)
	require.True(t, changed)
	require.Contains(t, state, `\"a\":1`)
	require.Contains(t, state, `\"b\":2`)
	state, _, err = mergeNativeRecovery(state, "three", nil, true)
	require.NoError(t, err)
	require.Contains(t, state, `"full_refresh_committed_ids":["one","three"]`)
	unchanged, changed, err := mergeNativeRecovery(state, "empty", nil, false)
	require.NoError(t, err)
	require.False(t, changed)
	require.Equal(t, state, unchanged)
	for sentinel, class := range map[error]string{table.ErrConflictingDataFiles: "concurrent_data", table.ErrConflictingDeleteFiles: "concurrent_deletes", table.ErrDataFilesMissing: "referenced_data_missing", table.ErrCommitFailed: "catalog_compare_and_swap"} {
		require.Equal(t, class, nativeConflictClass(fmt.Errorf("wrapped: %w", sentinel)))
	}
	require.Equal(t, "commit_state_unknown", nativeConflictClass(errors.New("transport")))
}

func TestNativePartitionGroupingDistinguishesNullString(t *testing.T) {
	schema, err := nativeSchema(map[string]string{"part": "string"}, false)
	require.NoError(t, err)
	field := schema.Field(0)
	spec := apacheiceberg.NewPartitionSpec(apacheiceberg.PartitionField{SourceIDs: []int{field.ID}, FieldID: 1000, Name: "part", Transform: apacheiceberg.IdentityTransform{}})
	missing, _, err := nativeRecordPartition(schema, spec, map[string]any{})
	require.NoError(t, err)
	text, _, err := nativeRecordPartition(schema, spec, map[string]any{"part": "null"})
	require.NoError(t, err)
	require.NotEqual(t, missing, text)
	require.False(t, strings.Contains(text, "<nil>"))
}

func TestNativeWriterStateOnlyCommitPreservesSnapshot(t *testing.T) {
	ctx := context.Background()
	cfg, stream, cat := nativeWriterFixture(t, types.UpdateTypeEquality, true, "")
	seed := openNativeTestWriter(t, ctx, cfg, stream, cat, nil, "snapshot", true)
	require.NoError(t, seed.Write(ctx, []types.RawRecord{nativeTestRecord("a", "r", 1, "one")}))
	require.NoError(t, seed.Close(ctx, nil))
	snapshot := nativeSnapshotID(seed.table)
	writer := openNativeTestWriter(t, ctx, cfg, stream, cat, nil, "heartbeat", false)
	require.NoError(t, writer.Close(ctx, map[string]any{"state": `{"captured_cdc_pos":"0/123"}`}))
	require.Equal(t, snapshot, nativeSnapshotID(writer.table))
	require.Contains(t, writer.table.Properties()[nativeRecoveryProperty], "0/123")
	require.Equal(t, map[string]int64{"a": 1}, nativeVisibleValues(t, writer.table))
}

func TestNativeWriterConcurrentIndexKeyFence(t *testing.T) {
	ctx := context.Background()
	cfg, stream, cat := nativeWriterFixture(t, types.UpdateTypePosition, true, "")
	index := &nativeMemoryIndex{rows: make(map[string]types.RowLocation)}
	seed := openNativeTestWriter(t, ctx, cfg, stream, cat, index, "snapshot", true)
	require.NoError(t, seed.Write(ctx, []types.RawRecord{nativeTestRecord("a", "r", 1, "one"), nativeTestRecord("b", "r", 2, "one")}))
	require.NoError(t, seed.Close(ctx, nil))
	first := openNativeTestWriter(t, ctx, cfg, stream, cat, index, "first", false)
	stale := openNativeTestWriter(t, ctx, cfg, stream, cat, index, "same-key", false)
	disjoint := openNativeTestWriter(t, ctx, cfg, stream, cat, index, "different-key", false)
	require.NoError(t, first.Write(ctx, []types.RawRecord{nativeTestRecord("a", "u", 3, "one")}))
	require.NoError(t, stale.Write(ctx, []types.RawRecord{nativeTestRecord("a", "u", 4, "one")}))
	require.NoError(t, disjoint.Write(ctx, []types.RawRecord{nativeTestRecord("b", "u", 5, "one")}))
	require.NoError(t, first.Close(ctx, map[string]any{"state": "first"}))
	require.ErrorIs(t, stale.Close(ctx, map[string]any{"state": "stale"}), table.ErrConflictingDataFiles)
	require.NoError(t, disjoint.Close(ctx, map[string]any{"state": "disjoint"}))
	require.Equal(t, map[string]int64{"a": 3, "b": 5}, nativeVisibleValues(t, disjoint.table))
}

func TestNativeWriterTimestampPartitionAndPromotion(t *testing.T) {
	ctx := context.Background()
	cfg, stream, cat := nativeWriterFixture(t, types.UpdateTypeEquality, true, "/{_olake_timestamp, day}/{blob, bucket[8]}")
	stream.Stream.UpsertField("promoted", types.Int32, true, false)
	writer := openNativeTestWriter(t, ctx, cfg, stream, cat, nil, "partitioned", false)
	before, ok := writer.table.Schema().FindFieldByName("promoted")
	require.True(t, ok)
	columns := writer.schemaMap()
	columns["promoted"] = "long"
	require.NoError(t, writer.EvolveSchema(ctx, columns))
	after, ok := writer.table.Schema().FindFieldByName("promoted")
	require.True(t, ok)
	require.Equal(t, before.ID, after.ID)
	require.Equal(t, "long", after.Type.String())
	record := nativeTestRecord("a", "i", 1, "one")
	record.Data["promoted"] = int64(1) << 40
	require.NoError(t, writer.Write(ctx, []types.RawRecord{record}))
	require.NoError(t, writer.Close(ctx, map[string]any{"state": "partitioned"}))
	entries, err := nativeLiveEntries(ctx, writer.table)
	require.NoError(t, err)
	require.NotEmpty(t, entries)
	for _, entry := range entries {
		require.Len(t, entry.DataFile().Partition(), 2)
	}
	require.Equal(t, map[string]int64{"a": 1}, nativeVisibleValues(t, writer.table))
}

func TestNativeWriterPartialWriteFailureCleansFiles(t *testing.T) {
	ctx := context.Background()
	cfg, stream, cat := nativeWriterFixture(t, types.UpdateTypeEquality, true, "/{part, identity}")
	writer := openNativeTestWriter(t, ctx, cfg, stream, cat, nil, "bad", true)
	bad := nativeTestRecord("b", "r", 2, "two")
	bad.Data["blob"] = []byte{1}
	require.Error(t, writer.Write(ctx, []types.RawRecord{nativeTestRecord("a", "r", 1, "one"), bad}))
	fs, err := writer.table.FS(ctx)
	require.NoError(t, err)
	for _, file := range writer.dataFiles {
		_, err := fs.Open(file.FilePath())
		require.Error(t, err)
	}
	require.Nil(t, writer.table.CurrentSnapshot())
	require.Error(t, writer.Close(ctx, nil))
}

func TestNativeWriterConstructorReusesLoadedMetadata(t *testing.T) {
	for _, mode := range []types.UpdateType{types.UpdateTypeEquality, types.UpdateTypePosition, types.UpdateTypeDeletionVector} {
		t.Run(string(mode), func(t *testing.T) {
			ctx := context.Background()
			cfg, stream, realCatalog := nativeWriterFixture(t, mode, true, "")
			var index *nativeMemoryIndex
			if mode != types.UpdateTypeEquality {
				index = &nativeMemoryIndex{rows: make(map[string]types.RowLocation)}
			}
			seed := openNativeTestWriter(t, ctx, cfg, stream, realCatalog, index, "seed", true)
			require.NoError(t, seed.Write(ctx, []types.RawRecord{nativeTestRecord("a", "r", 1, "one")}))
			require.NoError(t, seed.Close(ctx, nil))
			cat := &nativeCommitFaultCatalog{Catalog: realCatalog}
			writer := openNativeTestWriter(t, ctx, cfg, stream, cat, index, "no-change", false)
			require.Equal(t, 1, cat.loads, "unchanged setup must use its initial catalog load")
			require.Zero(t, cat.calls, "unchanged setup must not commit metadata")
			require.NoError(t, writer.EvolveSchema(ctx, writer.schemaMap()))
			require.Equal(t, 2, cat.loads, "explicit schema evolution must refresh even when unchanged")
			writer.Abort()
		})
	}
}

func TestNativeWriterCreationKeepsCatalogLocationAndCredentialsPrivate(t *testing.T) {
	ctx := context.Background()
	cfg, stream, cat := nativeWriterFixture(t, types.UpdateTypeEquality, true, "")
	warehouse := cfg.IcebergS3Path
	// Managed REST catalogs interpret this as a warehouse identifier, not a filesystem path.
	cfg.IcebergS3Path = "arn:aws:s3tables:us-east-2:123456789012:bucket/example"
	cfg.AccessKey, cfg.SecretKey, cfg.SessionToken = "private-access", "private-secret", "private-session"
	cfg.GCPServiceAccountJSON = "private-google-key"
	writer := openNativeTestWriter(t, ctx, cfg, stream, cat, nil, "creation", true)
	defer writer.Abort()
	persisted, err := cat.LoadTable(ctx, writer.table.Identifier())
	require.NoError(t, err)
	require.True(t, strings.HasPrefix(persisted.Location(), warehouse), "the catalog must own the physical location")
	metadata, err := json.Marshal(persisted.Metadata())
	require.NoError(t, err)
	for _, secret := range []string{cfg.AccessKey, cfg.SecretKey, cfg.SessionToken, cfg.GCPServiceAccountJSON} {
		require.NotContains(t, string(metadata), secret)
	}
	require.Equal(t, "parquet", persisted.Properties()["write.format.default"])
}
