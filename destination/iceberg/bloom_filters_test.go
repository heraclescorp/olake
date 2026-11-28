package iceberg

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"maps"
	"strconv"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/compress"
	parquetfile "github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	apacheiceberg "github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/destination"
	"github.com/datazip-inc/olake/types"
	"github.com/stretchr/testify/require"
)

func nativeBloomTestProperty(column string) string {
	return table.ParquetBloomFilterColumnEnabledKeyPrefix + "." + column
}

func nativeBloomTestInt(value int64) []byte {
	return binary.LittleEndian.AppendUint64(nil, uint64(value))
}

func nativeBloomTestDataPaths(t *testing.T, tbl *table.Table) []string {
	t.Helper()
	entries, err := nativeLiveEntries(context.Background(), tbl)
	require.NoError(t, err)
	var paths []string
	for _, entry := range entries {
		if entry.DataFile().ContentType() == apacheiceberg.EntryContentData {
			paths = append(paths, entry.DataFile().FilePath())
		}
	}
	return paths
}

func nativeBloomTestFileBytes(t *testing.T, tbl *table.Table, path string) []byte {
	t.Helper()
	fs, err := tbl.FS(context.Background())
	require.NoError(t, err)
	input, err := fs.Open(path)
	require.NoError(t, err)
	defer input.Close()
	data, err := io.ReadAll(input)
	require.NoError(t, err)
	return data
}

func nativeBloomTestFile(t *testing.T, tbl *table.Table, path string, enabled map[string][][]byte, disabled []string, maxBytes int64) map[string][]int64 {
	t.Helper()
	fs, err := tbl.FS(context.Background())
	require.NoError(t, err)
	input, err := fs.Open(path)
	require.NoError(t, err)
	reader, err := parquetfile.NewParquetReader(input)
	require.NoError(t, err)
	defer reader.Close()
	require.Positive(t, reader.NumRowGroups())
	arrowReader, err := pqarrow.NewFileReader(reader, pqarrow.ArrowReadProperties{}, memory.DefaultAllocator)
	require.NoError(t, err)
	columns := make(map[string]int)
	for index := range reader.MetaData().Schema.NumColumns() {
		columns[reader.MetaData().Schema.Column(index).SchemaNode().Name()] = index
	}
	sizes := make(map[string][]int64)
	for group := range reader.NumRowGroups() {
		metadata := reader.MetaData().RowGroup(group)
		for _, column := range disabled {
			index, exists := columns[column]
			require.True(t, exists, "missing Parquet column %q", column)
			chunk, err := metadata.ColumnChunk(index)
			require.NoError(t, err)
			require.Zero(t, chunk.BloomFilterOffset(), "unexpected Bloom filter for %q", column)
		}
		if len(enabled) == 0 {
			continue
		}
		filters, err := reader.GetBloomFilterReader().RowGroup(group)
		require.NoError(t, err)
		require.NotNil(t, filters)
		for column, values := range enabled {
			index, exists := columns[column]
			require.True(t, exists, "missing Parquet column %q", column)
			chunk, err := metadata.ColumnChunk(index)
			require.NoError(t, err)
			require.Positive(t, chunk.BloomFilterOffset(), "missing Bloom offset for %q", column)
			require.Equal(t, compress.Codecs.Zstd, chunk.Compression())
			statsSet, err := chunk.StatsSet()
			require.NoError(t, err)
			require.True(t, statsSet, "Bloom-enabled columns must retain statistics")
			filter, err := filters.GetColumnBloomFilter(index)
			require.NoError(t, err)
			require.NotNil(t, filter, "missing serialized Bloom filter for %q", column)
			var header [11]byte
			_, err = input.ReadAt(header[:], chunk.BloomFilterOffset())
			require.NoError(t, err)
			require.Equal(t, byte(0x15), header[0])
			encodedBytes, consumed := binary.Uvarint(header[1:])
			require.Positive(t, consumed)
			require.Zero(t, encodedBytes&1, "Bloom bitset size must not be negative")
			numBytes := int64(encodedBytes >> 1)
			require.Positive(t, numBytes)
			require.LessOrEqual(t, numBytes, maxBytes, "Bloom bitset exceeds configured cap for %q", column)
			require.GreaterOrEqual(t, numBytes, int64(32))
			require.Zero(t, numBytes&(numBytes-1), "Bloom bitset must have a power-of-two size")
			sizes[column] = append(sizes[column], numBytes)
			expected := make(map[string]struct{}, len(values))
			for _, value := range values {
				expected[string(value)] = struct{}{}
			}
			groupData, err := arrowReader.ReadRowGroups(context.Background(), []int{index}, []int{group})
			require.NoError(t, err)
			defer groupData.Release()
			for _, chunk := range groupData.Column(0).Data().Chunks() {
				for row := range chunk.Len() {
					if chunk.IsNull(row) {
						continue
					}
					var value []byte
					switch chunk := chunk.(type) {
					case *array.String:
						value = []byte(chunk.Value(row))
					case *array.Int64:
						value = nativeBloomTestInt(chunk.Value(row))
					default:
						t.Fatalf("unsupported Bloom test column %q type %T", column, chunk)
					}
					require.Contains(t, expected, string(value), "unexpected stored value for %q", column)
					require.True(t, filter.CheckHash(filter.Hasher().Sum64(value)), "Bloom false negative for %q value %x in row group %d", column, value, group)
				}
			}
			var negatives int
			for probe := range 64 {
				if !filter.CheckHash(filter.Hasher().Sum64([]byte(fmt.Sprintf("absent-bloom-probe-%d", probe)))) {
					negatives++
				}
			}
			// Saturated caps can legitimately accept every absent probe.
			if int64(len(expected))*2 <= numBytes {
				require.Positive(t, negatives, "Bloom filter for %q accepts every absent probe", column)
			}
		}
	}
	return sizes
}

// All paths use the same nullable value column and distinct composite keys, even
// when id repeats. The COW path updates one seed row so survivors are SDK output.
func nativeBloomTestAdaptiveOutput(t *testing.T, path string, count, distinct, maxBytes int) map[string]int64 {
	t.Helper()
	ctx := context.Background()
	cfg, stream, cat := nativeCOWFixture(t, types.UpdateTypeEquality)
	if path != "copy-on-write" {
		stream.StreamMetadata.MergeStrategy = types.MergeOnRead
	}
	created := openNativeTestWriter(ctx, t, cfg, stream, cat, nil, "adaptive-create", true)
	require.NoError(t, created.Close(ctx, nil))
	props := apacheiceberg.Properties{
		nativeBloomTestProperty(constants.OlakeID): "false",
		nativeBloomTestProperty("tenant"):          "false",
		nativeBloomTestProperty("value"):           "true",
	}
	if maxBytes != 0 {
		props[table.ParquetBloomFilterMaxBytesKey] = strconv.Itoa(maxBytes)
	}
	tx := created.table.NewTransaction()
	require.NoError(t, tx.SetProperties(props))
	_, err := tx.Commit(ctx)
	require.NoError(t, err)
	rows := make([]types.RawRecord, count)
	values := make([][]byte, distinct)
	for index := range distinct {
		values[index] = nativeBloomTestInt(int64(index))
	}
	for index := range rows {
		label := fmt.Sprintf("adaptive-%05d", index)
		rows[index] = nativeCOWRecord(label, "r", int64(index%distinct), label, 0)
		if index%3 == 1 {
			rows[index].Data["value"] = nil
		}
	}
	var seedPaths []string
	if path == "copy-on-write" {
		seed := openNativeTestWriter(ctx, t, cfg, stream, cat, nil, "adaptive-seed", true)
		require.NoError(t, seed.Write(ctx, rows))
		require.NoError(t, seed.Close(ctx, nil))
		seedPaths = nativeBloomTestDataPaths(t, seed.table)
		require.NotEmpty(t, seedPaths)
	}
	writer := openNativeTestWriter(ctx, t, cfg, stream, cat, nil, "adaptive-write", path == "backfill")
	t.Cleanup(func() { writer.Abort() })
	if path == "copy-on-write" {
		update := rows[0]
		update.OlakeColumns = maps.Clone(update.OlakeColumns)
		update.OlakeColumns[constants.OpType] = "u"
		require.NoError(t, writer.Write(ctx, []types.RawRecord{update}))
	} else {
		require.NoError(t, writer.Write(ctx, rows))
	}
	require.NoError(t, writer.Close(ctx, nil))
	paths := nativeBloomTestDataPaths(t, writer.table)
	require.NotEmpty(t, paths)
	capBytes := int64(maxBytes)
	if capBytes == 0 {
		capBytes = 1024 * 1024
	}
	sizes := make(map[string]int64)
	fs, err := writer.table.FS(ctx)
	require.NoError(t, err)
	var rowCount int64
	var nulls int
	for _, filePath := range paths {
		require.NotContains(t, seedPaths, filePath, "inspect SDK output, not an unchanged native seed")
		fileSizes := nativeBloomTestFile(t, writer.table, filePath, map[string][][]byte{
			"id": values, "value": {nativeBloomTestInt(0)},
		}, []string{constants.OlakeID, "tenant", "part", "blob", constants.OpType, constants.OlakeTimestamp}, capBytes)
		for column, groupSizes := range fileSizes {
			for _, size := range groupSizes {
				sizes[column] = max(sizes[column], size)
			}
		}
		input, err := fs.Open(filePath)
		require.NoError(t, err)
		reader, err := parquetfile.NewParquetReader(input)
		require.NoError(t, err)
		rowCount += reader.NumRows()
		arrowReader, err := pqarrow.NewFileReader(reader, pqarrow.ArrowReadProperties{}, memory.DefaultAllocator)
		require.NoError(t, err)
		valueColumn := -1
		for index := range reader.MetaData().Schema.NumColumns() {
			if reader.MetaData().Schema.Column(index).SchemaNode().Name() == "value" {
				valueColumn = index
			}
		}
		require.NotEqual(t, -1, valueColumn)
		for group := range reader.NumRowGroups() {
			data, err := arrowReader.ReadRowGroups(ctx, []int{valueColumn}, []int{group})
			require.NoError(t, err)
			for _, chunk := range data.Column(0).Data().Chunks() {
				nulls += chunk.NullN()
			}
			data.Release()
		}
		require.NoError(t, reader.Close())
	}
	require.Equal(t, int64(count), rowCount, "repeated IDs must not shrink the test dataset")
	require.Equal(t, (count+1)/3, nulls, "the membership regression must retain its null fixture rows")
	return sizes
}

func TestNativeBloomAdaptiveDistinctValues(t *testing.T) {
	for _, path := range []string{"ordinary", "backfill", "copy-on-write"} {
		t.Run(path, func(t *testing.T) {
			tiny := nativeBloomTestAdaptiveOutput(t, path, 1, 1, 0)
			large := nativeBloomTestAdaptiveOutput(t, path, 8192, 8192, 0)
			repeated := nativeBloomTestAdaptiveOutput(t, path, 8192, 3, 0)
			require.Less(t, tiny["id"], large["id"], "higher NDV must grow the serialized filter")
			require.Equal(t, tiny["id"], repeated["id"], "equal row counts must not size repeated IDs like distinct IDs")
			require.Less(t, large["id"], int64(1024*1024), "the default cap is not a fixed allocation on disk")
			require.Equal(t, tiny["value"], large["value"], "nulls and repeated zero values must not inflate NDV")
		})
	}
}

func TestNativeBloomAdaptiveByteCaps(t *testing.T) {
	for _, path := range []string{"ordinary", "backfill", "copy-on-write"} {
		for _, capBytes := range []int{32, 64, 1000, 1024} {
			t.Run(fmt.Sprintf("%s/%d", path, capBytes), func(t *testing.T) {
				sizes := nativeBloomTestAdaptiveOutput(t, path, 2048, 2048, capBytes)
				effectiveCap := int64(32)
				for effectiveCap*2 <= int64(capBytes) {
					effectiveCap *= 2
				}
				require.Equal(t, effectiveCap, sizes["id"], "high NDV must saturate without rounding above the configured byte cap")
			})
		}
	}
}

func TestNativeBloomDefaultsForRenamedCompositeKeys(t *testing.T) {
	for _, backfill := range []bool{false, true} {
		t.Run(fmt.Sprintf("backfill=%t", backfill), func(t *testing.T) {
			ctx := context.Background()
			cfg, stream, cat := nativeWriterFixture(t, types.UpdateTypeEquality, true, "")
			stream.StreamMetadata.UseSourceColumnNames = false
			stream.Stream.WithPrimaryKey("Account ID", "Tenant Code", "Missing Key")
			stream.Stream.UpsertField("Account ID", types.Int64, false, false)
			stream.Stream.UpsertField("Tenant Code", types.String, false, false)
			writer := openNativeTestWriter(ctx, t, cfg, stream, cat, nil, "composite-bloom", backfill)
			defer writer.Abort()
			persisted, err := cat.LoadTable(ctx, writer.table.Identifier())
			require.NoError(t, err)
			for _, key := range []string{constants.OlakeID, "account_id", "tenant_code"} {
				require.Equal(t, "true", persisted.Properties()[nativeBloomTestProperty(key)])
			}
			require.Equal(t, "1048576", persisted.Properties()[table.ParquetBloomFilterMaxBytesKey])
			for _, key := range []string{"Account ID", "Tenant Code", "missing_key", "value", "part", "blob", constants.OpType, constants.OlakeTimestamp} {
				require.NotContains(t, persisted.Properties(), nativeBloomTestProperty(key))
			}
			rows := []types.RawRecord{nativeTestRecord("first", "r", 10, "one"), nativeTestRecord("second", "r", 20, "one")}
			for index := range rows {
				rows[index].Data["account_id"] = int64(index + 1)
				rows[index].Data["tenant_code"] = fmt.Sprintf("tenant-%d", index+1)
			}
			if backfill {
				require.NoError(t, writer.Write(ctx, rows[:1]))
				require.NoError(t, writer.Write(ctx, rows[1:]))
				require.Len(t, writer.backfillFiles, 1, "exercise the rolling streaming writer")
			} else {
				require.NoError(t, writer.Write(ctx, rows))
			}
			require.NoError(t, writer.Close(ctx, nil))
			paths := nativeBloomTestDataPaths(t, writer.table)
			require.Len(t, paths, 1)
			nativeBloomTestFile(t, writer.table, paths[0], map[string][][]byte{
				constants.OlakeID: {[]byte("first"), []byte("second")},
				"account_id":      {nativeBloomTestInt(1), nativeBloomTestInt(2)},
				"tenant_code":     {[]byte("tenant-1"), []byte("tenant-2")},
			}, []string{"value", "part", "blob", constants.OpType, constants.OlakeTimestamp}, 1024*1024)
		})
	}
}

func TestNativeBloomExplicitColumnProperties(t *testing.T) {
	for _, path := range []string{"ordinary", "backfill", "copy-on-write"} {
		t.Run(path, func(t *testing.T) {
			ctx := context.Background()
			cfg, stream, cat := nativeCOWFixture(t, types.UpdateTypeEquality)
			if path != "copy-on-write" {
				stream.StreamMetadata.MergeStrategy = types.MergeOnRead
			}
			created := openNativeTestWriter(ctx, t, cfg, stream, cat, nil, "create-overrides", true)
			require.NoError(t, created.Close(ctx, nil))
			tx := created.table.NewTransaction()
			require.NoError(t, tx.SetProperties(apacheiceberg.Properties{
				nativeBloomTestProperty(constants.OlakeID): "false",
				nativeBloomTestProperty("id"):              "false",
				nativeBloomTestProperty("value"):           "true",
				table.ParquetBloomFilterMaxBytesKey:        "64",
			}))
			_, err := tx.Commit(ctx)
			require.NoError(t, err)
			if path == "copy-on-write" {
				seed := openNativeTestWriter(ctx, t, cfg, stream, cat, nil, "override-seed", true)
				require.NoError(t, seed.Write(ctx, []types.RawRecord{nativeCOWRecord("first", "r", 1, "one", 1), nativeCOWRecord("second", "r", 2, "two", 20)}))
				require.NoError(t, seed.Close(ctx, nil))
			}
			writer := openNativeTestWriter(ctx, t, cfg, stream, cat, nil, "explicit-bloom", path == "backfill")
			defer writer.Abort()
			rows := []types.RawRecord{nativeCOWRecord("first", "r", 1, "one", 10), nativeCOWRecord("second", "r", 2, "two", 20)}
			if path == "copy-on-write" {
				rows = rows[:1]
				rows[0].OlakeColumns[constants.OpType] = "u"
			}
			require.NoError(t, writer.Write(ctx, rows))
			require.NoError(t, writer.Close(ctx, nil))
			require.Equal(t, map[string]int64{"first": 10, "second": 20}, nativeVisibleValues(t, writer.table))
			for _, file := range nativeBloomTestDataPaths(t, writer.table) {
				nativeBloomTestFile(t, writer.table, file, map[string][][]byte{
					"tenant": {[]byte("one"), []byte("two")},
					"value":  {nativeBloomTestInt(10), nativeBloomTestInt(20)},
				}, []string{constants.OlakeID, "id", "part", "blob", constants.OpType, constants.OlakeTimestamp}, 64)
			}
			for column, expected := range map[string]string{constants.OlakeID: "false", "id": "false", "value": "true", "tenant": "true"} {
				require.Equal(t, expected, writer.table.Properties()[nativeBloomTestProperty(column)])
			}
			require.Equal(t, "64", writer.table.Properties()[table.ParquetBloomFilterMaxBytesKey])
		})
	}
}

func TestNativeBloomExistingDefaultsJoinOnlyDataCommit(t *testing.T) {
	for _, copyOnWrite := range []bool{false, true} {
		t.Run(fmt.Sprintf("copy-on-write=%t", copyOnWrite), func(t *testing.T) {
			ctx := context.Background()
			cfg, stream, cat := nativeCOWFixture(t, types.UpdateTypeEquality)
			if !copyOnWrite {
				stream.StreamMetadata.MergeStrategy = types.MergeOnRead
			}
			created := openNativeTestWriter(ctx, t, cfg, stream, cat, nil, "create-legacy", true)
			require.NoError(t, created.Close(ctx, nil))
			keys := []string{nativeBloomTestProperty(constants.OlakeID), nativeBloomTestProperty("id"), nativeBloomTestProperty("tenant"), table.ParquetBloomFilterMaxBytesKey}
			tx := created.table.NewTransaction()
			require.NoError(t, tx.SetProperties(apacheiceberg.Properties{keys[0]: "false", keys[1]: "false", keys[2]: "false"}))
			_, err := tx.Commit(ctx)
			require.NoError(t, err)
			seed := openNativeTestWriter(ctx, t, cfg, stream, cat, nil, "legacy-seed", true)
			require.NoError(t, seed.Write(ctx, []types.RawRecord{nativeCOWRecord("old", "r", 1, "one", 1)}))
			require.NoError(t, seed.Close(ctx, nil))
			oldPaths := nativeBloomTestDataPaths(t, seed.table)
			require.Len(t, oldPaths, 1)
			oldBytes := nativeBloomTestFileBytes(t, seed.table, oldPaths[0])
			nativeBloomTestFile(t, seed.table, oldPaths[0], nil, []string{constants.OlakeID, "id", "tenant", "value"}, 1024*1024)
			tx = seed.table.NewTransaction()
			require.NoError(t, tx.RemoveProperties(keys))
			legacy, err := tx.Commit(ctx)
			require.NoError(t, err)
			before := legacy.MetadataLocation()
			counted := &nativeCommitFaultCatalog{Catalog: cat}
			quiet := openNativeTestWriter(ctx, t, cfg, stream, counted, nil, "quiet-legacy", false)
			require.NoError(t, quiet.Close(ctx, nil))
			require.Zero(t, counted.calls, "setup/empty close must not commit Bloom-only metadata")
			persisted, err := cat.LoadTable(ctx, legacy.Identifier())
			require.NoError(t, err)
			require.Equal(t, before, persisted.MetadataLocation())
			require.Equal(t, oldPaths, nativeBloomTestDataPaths(t, persisted))
			require.Equal(t, oldBytes, nativeBloomTestFileBytes(t, persisted, oldPaths[0]))
			for _, key := range keys {
				require.NotContains(t, persisted.Properties(), key)
			}
			writer := openNativeTestWriter(ctx, t, cfg, stream, counted, nil, "upgrade-with-data", false)
			defer writer.Abort()
			row := nativeCOWRecord("new", "r", 2, "two", 2)
			if copyOnWrite {
				row = nativeCOWRecord("old", "u", 1, "one", 2)
			}
			require.NoError(t, writer.Write(ctx, []types.RawRecord{row}))
			require.Zero(t, counted.calls, "staging rows must not persist defaults")
			persisted, err = cat.LoadTable(ctx, legacy.Identifier())
			require.NoError(t, err)
			require.Equal(t, before, persisted.MetadataLocation())
			for _, key := range keys {
				require.NotContains(t, persisted.Properties(), key)
				require.NotContains(t, legacy.Properties(), key, "borrowed metadata must not be mutated")
			}
			require.NoError(t, writer.Close(ctx, map[string]any{"state": "with-bloom-defaults"}))
			require.Equal(t, 1, counted.calls, "defaults and snapshot must share one catalog commit")
			persisted, err = cat.LoadTable(ctx, legacy.Identifier())
			require.NoError(t, err)
			require.NotEqual(t, nativeSnapshotID(legacy), nativeSnapshotID(persisted))
			for _, key := range keys[:3] {
				require.Equal(t, "true", persisted.Properties()[key])
			}
			require.Equal(t, "1048576", persisted.Properties()[keys[3]])
			require.Contains(t, persisted.Properties()[nativeRecoveryProperty], "with-bloom-defaults")
			require.Equal(t, oldBytes, nativeBloomTestFileBytes(t, persisted, oldPaths[0]), "enabling defaults must never modify old Parquet bytes")
			paths := nativeBloomTestDataPaths(t, persisted)
			if copyOnWrite {
				require.Len(t, paths, 1)
				require.NotEqual(t, oldPaths[0], paths[0])
			} else {
				require.Len(t, paths, 2)
				require.Contains(t, paths, oldPaths[0])
			}
			var inspected int
			for _, path := range paths {
				if path == oldPaths[0] {
					continue
				}
				label, key, tenant := "new", int64(2), "two"
				if copyOnWrite {
					label, key, tenant = "old", 1, "one"
				}
				nativeBloomTestFile(t, persisted, path, map[string][][]byte{
					constants.OlakeID: {[]byte(label)}, "id": {nativeBloomTestInt(key)}, "tenant": {[]byte(tenant)},
				}, []string{"value", "part", "blob", constants.OpType, constants.OlakeTimestamp}, 1024*1024)
				inspected++
			}
			require.Equal(t, 1, inspected)
		})
	}
}

func TestNativeBloomDeclaredIdentifierGetsFilter(t *testing.T) {
	ctx := context.Background()
	cfg, stream, cat := nativeWriterFixture(t, types.UpdateTypeEquality, true, "")
	created := openNativeTestWriter(ctx, t, cfg, stream, cat, nil, "create-identifier", true)
	require.NoError(t, created.Close(ctx, nil))
	tx := created.table.NewTransaction()
	require.NoError(t, tx.UpgradeFormatVersion(3))
	require.NoError(t, tx.UpdateSchema(true, false).
		AddColumn([]string{"declared_identifier"}, apacheiceberg.PrimitiveTypes.String, "", true, apacheiceberg.NewLiteral("initial")).
		SetIdentifierField([][]string{{constants.OlakeID}, {"declared_identifier"}}).Commit())
	_, err := tx.Commit(ctx)
	require.NoError(t, err)
	writer := openNativeTestWriter(ctx, t, cfg, stream, cat, nil, "declared-identifier", false)
	defer writer.Abort()
	row := nativeTestRecord("identifier-row", "r", 1, "one")
	row.Data["declared_identifier"] = "declared-key"
	require.NoError(t, writer.Write(ctx, []types.RawRecord{row}))
	require.NoError(t, writer.Close(ctx, nil))
	paths := nativeBloomTestDataPaths(t, writer.table)
	require.Len(t, paths, 1)
	nativeBloomTestFile(t, writer.table, paths[0], map[string][][]byte{
		constants.OlakeID: {[]byte("identifier-row")}, "declared_identifier": {[]byte("declared-key")},
	}, []string{"value", "part", "blob", constants.OpType, constants.OlakeTimestamp}, 1024*1024)
	require.Equal(t, "true", writer.table.Properties()[nativeBloomTestProperty("declared_identifier")])
}

func TestNativeBloomInvalidMaxBytesReturnsErrorWithoutPanic(t *testing.T) {
	for _, path := range []string{"ordinary", "backfill", "copy-on-write"} {
		for _, value := range []string{"", "not-an-integer", "0", "31", "-1", "134217729", "9223372036854775808"} {
			t.Run(path+"/"+strconv.Quote(value), func(t *testing.T) {
				ctx := context.Background()
				cfg, stream, cat := nativeCOWFixture(t, types.UpdateTypeEquality)
				if path != "copy-on-write" {
					stream.StreamMetadata.MergeStrategy = types.MergeOnRead
				}
				created := openNativeTestWriter(ctx, t, cfg, stream, cat, nil, "create-invalid", true)
				require.NoError(t, created.Close(ctx, nil))
				tx := created.table.NewTransaction()
				require.NoError(t, tx.SetProperties(apacheiceberg.Properties{table.ParquetBloomFilterMaxBytesKey: value}))
				invalid, err := tx.Commit(ctx)
				require.NoError(t, err)
				before := invalid.MetadataLocation()
				var writeErr error
				require.NotPanics(t, func() {
					opts := &destination.Options{ThreadID: "invalid-bloom", Backfill: path == "backfill"}
					writer, _, err := newNativeWriter(ctx, cfg, stream, opts, cat, types.NewStreamIndexThread(nil))
					if err != nil {
						writeErr = err
						return
					}
					defer writer.Abort()
					writeErr = writer.Write(ctx, []types.RawRecord{nativeCOWRecord("invalid", "r", 1, "one", 1)})
					if writeErr == nil {
						writeErr = writer.Close(ctx, nil)
					}
				})
				require.ErrorIs(t, writeErr, apacheiceberg.ErrInvalidArgument)
				require.ErrorContains(t, writeErr, table.ParquetBloomFilterMaxBytesKey)
				persisted, err := cat.LoadTable(ctx, invalid.Identifier())
				require.NoError(t, err)
				require.Equal(t, before, persisted.MetadataLocation())
				require.Empty(t, nativeBloomTestDataPaths(t, persisted))
			})
		}
	}
}

func TestNativeBloomPolicyDoesNotMutateBorrowedProperties(t *testing.T) {
	_, stream, _ := nativeWriterFixture(t, types.UpdateTypeEquality, true, "")
	stream.Stream.WithPrimaryKey("id", "missing", "nested")
	schema := apacheiceberg.NewSchemaWithIdentifiers(0, []int{4},
		apacheiceberg.NestedField{ID: 1, Name: constants.OlakeID, Type: apacheiceberg.PrimitiveTypes.String, Required: true},
		apacheiceberg.NestedField{ID: 2, Name: "id", Type: apacheiceberg.PrimitiveTypes.Int64},
		apacheiceberg.NestedField{ID: 3, Name: "value", Type: apacheiceberg.PrimitiveTypes.String},
		apacheiceberg.NestedField{ID: 4, Name: "declared", Type: apacheiceberg.PrimitiveTypes.String, Required: true},
		apacheiceberg.NestedField{ID: 5, Name: "nested", Type: &apacheiceberg.StructType{FieldList: []apacheiceberg.NestedField{{ID: 6, Name: "child", Type: apacheiceberg.PrimitiveTypes.String}}}},
	)
	borrowed := apacheiceberg.Properties{nativeBloomTestProperty("id"): "false", nativeBloomTestProperty("value"): "true", "unrelated": "preserved"}
	before := maps.Clone(borrowed)
	effective, err := nativeBloomFilterProperties(schema, stream, borrowed)
	require.NoError(t, err)
	require.Equal(t, before, borrowed)
	require.Equal(t, "true", effective[nativeBloomTestProperty(constants.OlakeID)])
	require.Equal(t, "true", effective[nativeBloomTestProperty("declared")])
	require.Equal(t, "false", effective[nativeBloomTestProperty("id")])
	require.Equal(t, "true", effective[nativeBloomTestProperty("value")])
	require.NotContains(t, effective, nativeBloomTestProperty("missing"))
	require.NotContains(t, effective, nativeBloomTestProperty("nested"))
	require.Equal(t, "preserved", effective["unrelated"])
	effective["unrelated"] = "changed"
	require.Equal(t, before, borrowed)
}
