package iceberg

import (
	"context"
	"testing"

	apacheiceberg "github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
	"github.com/datazip-inc/olake/types"
	"github.com/stretchr/testify/require"
)

// Switching to equality must not reset an existing managed v3 table or need its
// physical index. Existing DVs and same-write duplicates must remain effective.
func TestNativeWriterSwitchDVToEqualityV3(t *testing.T) {
	ctx := context.Background()
	cfg, stream, cat := nativeWriterFixture(t, types.UpdateTypeDeletionVector, true, "/{part, identity}")
	index := &nativeMemoryIndex{rows: make(map[string]types.RowLocation)}
	seed := openNativeTestWriter(ctx, t, cfg, stream, cat, index, "seed-dv", true)
	require.NoError(t, seed.Write(ctx, []types.RawRecord{nativeTestRecord("a", "r", 1, "one"), nativeTestRecord("b", "r", 2, "one")}))
	require.NoError(t, seed.Close(ctx, nil))
	original := nativeSnapshotID(seed.table)
	update := openNativeTestWriter(ctx, t, cfg, stream, cat, index, "update-dv", false)
	require.NoError(t, update.Write(ctx, []types.RawRecord{nativeTestRecord("a", "u", 3, "one")}))
	require.NoError(t, update.Close(ctx, nil))
	mode := string(types.UpdateTypeEquality)
	stream.StreamMetadata.UpdateType = &mode
	eq := openNativeTestWriter(ctx, t, cfg, stream, cat, nil, "eq-v3", false)
	require.Nil(t, eq.indexThread)
	require.Equal(t, 3, eq.table.Metadata().Version())
	require.NotNil(t, eq.table.SnapshotByID(original))
	require.NoError(t, eq.Write(ctx, []types.RawRecord{nativeTestRecord("a", "u", 4, "two"), nativeTestRecord("a", "u", 5, "three"), nativeTestRecord("c", "c", 6, "one")}))
	require.NoError(t, eq.Close(ctx, nil))
	require.Equal(t, map[string]int64{"a": 5, "b": 2, "c": 6}, nativeVisibleValues(t, eq.table))
	entries, err := nativeLiveEntries(ctx, eq.table)
	require.NoError(t, err)
	var eqDeletes, dvs int
	for _, entry := range entries {
		f := entry.DataFile()
		if f.ContentType() == apacheiceberg.EntryContentEqDeletes {
			eqDeletes++
		}
		if f.ContentType() == apacheiceberg.EntryContentPosDeletes {
			require.Equal(t, apacheiceberg.PuffinFile, f.FileFormat(), "v3 must not add Parquet position deletes")
			dvs++
		}
	}
	require.Positive(t, eqDeletes)
	require.Positive(t, dvs)
	// Snapshot time travel remains readable after switching formats.
	scan := eq.table.Scan(table.WithSnapshotID(original))
	defer scan.Close()
	_, records, err := scan.ToArrowRecords(ctx)
	require.NoError(t, err)
	var rows int64
	for batch, err := range records {
		require.NoError(t, err)
		rows += batch.NumRows()
		batch.Release()
	}
	require.Equal(t, int64(2), rows)
}

// An external owner's file rewrite must not require a row index on the equality path.
func TestNativeEqualityAfterExternalCompaction(t *testing.T) {
	ctx := context.Background()
	cfg, stream, cat, boundary := nativeRESTWriterFixture(t, types.UpdateTypeEquality, nativeTestRecord("a", "r", 1, "one"))
	stale := openNativeTestWriter(ctx, t, cfg, stream, cat, nil, "staged-equality", false)
	require.NoError(t, stale.Write(ctx, []types.RawRecord{nativeTestRecord("a", "u", 3, "one")}))
	compactor := openNativeTestWriter(ctx, t, cfg, stream, boundary.cat, nil, "external-rewrite", true)
	require.NoError(t, compactor.Write(ctx, []types.RawRecord{nativeTestRecord("a", "r", 1, "one")}))
	require.NoError(t, compactor.finishBackfillFiles(ctx))
	entries, err := nativeLiveEntries(ctx, compactor.table)
	require.NoError(t, err)
	var removed []apacheiceberg.DataFile
	for _, e := range entries {
		if e.DataFile().ContentType() == apacheiceberg.EntryContentData {
			removed = append(removed, e.DataFile())
		}
	}
	tx := compactor.table.NewTransaction()
	require.NoError(t, tx.ReplaceFiles(ctx, removed, compactor.dataFiles, nil, nil))
	_, err = tx.Commit(ctx)
	require.NoError(t, err)
	clear(compactor.paths)
	compactor.Abort()
	require.NoError(t, stale.Close(ctx, nil))
	require.Nil(t, stale.indexThread)
	require.Equal(t, map[string]int64{"a": 3}, nativeVisibleValues(t, stale.table))
}
