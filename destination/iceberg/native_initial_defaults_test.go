package iceberg

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"

	apacheiceberg "github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table/dv"
	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/destination"
	"github.com/datazip-inc/olake/types"
	"github.com/stretchr/testify/require"
)

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
				deleted := writeNativeIndexDelete(t, tbl, apacheiceberg.PositionalDeleteSchema, fmt.Sprintf(`[{"file_path":%q,"pos":1}]`, original.FilePath), false, 1)
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

			// Every requested field is absent from the old file. Projection must
			// supply the initial default (NULL for v2) without losing row groups
			// or renumbering the directly addressable deleted physical row.
			rows, err := readNativeRows(ctx, tbl, original.FilePath, []int64{4, 1, 2, 0}, []string{"added"})
			require.NoError(t, err)
			require.Len(t, rows, 4)
			for _, position := range []int64{0, 1, 2, 4} {
				require.Equal(t, map[string]any{"added": initialValue}, rows[position])
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
			rows, err = readNativeRows(ctx, tbl, valueBefore.FilePath, []int64{valueBefore.Position}, []string{"payload", "added"})
			require.NoError(t, err)
			require.Empty(t, rows[valueBefore.Position], "dropped fields must be absent even while their bytes remain in the file")

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

			rows, err = readNativeRows(ctx, tbl, original.FilePath, []int64{1, 4}, []string{"payload", "added"})
			require.NoError(t, err)
			for _, position := range []int64{1, 4} {
				require.Equal(t, map[string]any{"payload": nil, "added": replacementValue}, rows[position], "same-name re-adds must not resurrect dropped field IDs")
			}
			rows, err = readNativeRows(ctx, tbl, nullBefore.FilePath, []int64{nullBefore.Position, valueBefore.Position}, []string{"payload", "added"})
			require.NoError(t, err)
			for _, position := range []int64{nullBefore.Position, valueBefore.Position} {
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
			seed := openNativeTestWriter(t, ctx, cfg, stream, cat, index, "defaults-seed", true)
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

			next := nativeTestRecord("row", "u", 5, placeholder)
			next.Data["stable"], next.Data["new_toast"] = placeholder, placeholder
			next.UnavailableColumns = []string{"part", "stable", "new_toast"}
			beforeReads := resolver.fromTable
			_, records, _, err = dest.FlattenAndCleanData(ctx, []types.RawRecord{next})
			if mode == types.UpdateTypeDeletionVector {
				require.NoError(t, err)
				require.Equal(t, "replacement-default", records[0].Data["part"], "a same-name re-add must not reuse the dropped field's carry")
				require.Equal(t, "added-default", records[0].Data["new_toast"], "an old pending file must expose the added column's initial default")
				require.Equal(t, beforeReads+2, resolver.fromTable, "only changed columns should require native file reads")
				require.NoError(t, dest.Write(ctx, records))
			} else {
				// v2 cannot persist non-NULL initial defaults. Native reads expose
				// NULL, so recovery must reject the missing values rather than
				// resurrect either the dropped field or its stale carried value.
				require.ErrorContains(t, err, "could not be recovered from the destination")
				require.Equal(t, placeholder, next.Data["part"])
				require.Equal(t, placeholder, next.Data["new_toast"])
				require.Nil(t, newField.InitialDefault)
				physical, readErr := writer.ReadRows(ctx, location.FilePath, []int64{location.Position}, []string{"part", "new_toast"})
				require.NoError(t, readErr)
				require.Equal(t, map[string]any{"part": nil, "new_toast": nil}, physical[location.Position])
				require.Equal(t, beforeReads, resolver.fromTable)
			}
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
