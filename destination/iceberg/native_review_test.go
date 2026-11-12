package iceberg

import (
	"context"
	"fmt"
	"testing"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/types"
	"github.com/stretchr/testify/require"
)

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
				seed := openNativeTestWriter(t, ctx, cfg, stream, cat, index, "seed", true)
				require.NoError(t, seed.Write(ctx, []types.RawRecord{nativeTestRecord("seed", "r", 0, "seed")}))
				require.NoError(t, seed.Close(ctx, nil))
				writer := openNativeTestWriter(t, ctx, cfg, stream, cat, index, "pending-versions", false)
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
			seed := openNativeTestWriter(t, ctx, cfg, stream, cat, index, "seed", true)
			require.NoError(t, seed.Write(ctx, []types.RawRecord{nativeTestRecord("seed", "r", 1, "one")}))
			require.NoError(t, seed.Close(ctx, nil))
			checkpoint := index.snapshot
			heartbeat := openNativeTestWriter(t, ctx, cfg, stream, cat, index, "heartbeat", false)
			external := openNativeTestWriter(t, ctx, cfg, stream, cat, nil, "external", true)
			require.NoError(t, external.Write(ctx, []types.RawRecord{nativeTestRecord("external", "r", 2, "one")}))
			require.NoError(t, external.Close(ctx, nil))
			require.NotEqual(t, checkpoint, nativeSnapshotID(external.table))
			require.NoError(t, heartbeat.Close(ctx, map[string]any{"state": `{"captured_cdc_pos":"0/456"}`}))
			require.Equal(t, nativeSnapshotID(external.table), nativeSnapshotID(heartbeat.table))
			require.Contains(t, heartbeat.table.Properties()[nativeRecoveryProperty], "0/456")
			require.Equal(t, checkpoint, index.snapshot, "state-only commits cannot checkpoint rows they never indexed")
			_, indexedExternal := index.rows["external"]
			require.False(t, indexedExternal)
			recovered := openNativeTestWriter(t, ctx, cfg, stream, cat, index, "next-sync", false)
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
			backfill := openNativeTestWriter(t, ctx, cfg, stream, cat, index, "backfill", true)
			records := make([]types.RawRecord, count)
			for row := range records {
				records[row] = nativeTestRecord(fmt.Sprint(row), "r", int64(row), "one")
			}
			require.NoError(t, backfill.Write(ctx, records))
			require.Nil(t, backfill.indexReads)
			require.Nil(t, backfill.indexTouched)
			backfill.Abort()
			creates := openNativeTestWriter(t, ctx, cfg, stream, cat, index, "creates", false)
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
