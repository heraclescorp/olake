package iceberg

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	apacheiceberg "github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/destination"
	"github.com/datazip-inc/olake/types"
	"github.com/stretchr/testify/require"
)

func TestNativeRecoveryPreservesOmittedDestinationColumns(t *testing.T) {
	for _, test := range []struct {
		strategy types.MergeStrategy
		mode     types.UpdateType
	}{
		{types.CopyOnWrite, types.UpdateTypeEquality},
		{types.MergeOnRead, types.UpdateTypeEquality},
		{types.MergeOnRead, types.UpdateTypePosition},
		{types.MergeOnRead, types.UpdateTypeDeletionVector},
	} {
		t.Run(fmt.Sprintf("%s/%s", test.strategy, test.mode), func(t *testing.T) {
			ctx := context.Background()
			cfg, stream, cat := nativeCOWFixture(t, test.mode)
			stream.StreamMetadata.MergeStrategy = test.strategy
			var index *nativeMemoryIndex
			if test.strategy == types.MergeOnRead && test.mode != types.UpdateTypeEquality {
				index = &nativeMemoryIndex{rows: make(map[string]types.RowLocation)}
			}
			labels := []string{"kept", "cleared", "overwritten", "same-batch", "across-batch", "deleted", "bystander"}
			seed := openNativeTestWriter(ctx, t, cfg, stream, cat, index, "seed", true)
			var initial []types.RawRecord
			for key, label := range labels {
				initial = append(initial, nativeCOWRecord(label, "r", int64(key+1), "one", 1))
			}
			require.NoError(t, seed.Write(ctx, initial))
			require.NoError(t, seed.Close(ctx, map[string]any{"state": `{"lsn":"0/10"}`}))
			originalSchema := seed.table.Schema()
			tableUUID := seed.table.Metadata().TableUUID()
			at := time.Date(2025, 3, 8, 12, 0, 0, 123456000, time.UTC)
			next := at.Add(24 * time.Hour)
			extend := openNativeTestWriter(ctx, t, cfg, stream, cat, index, "extend", false)
			require.Empty(t, extend.recoveryColumns)
			require.NoError(t, extend.AlterSourceColumns(ctx, []*nativeSourceColumn{
				{Key: "added_optional", IceType: "long"},
				{Key: "tombstoned_at", IceType: "timestamptz"},
			}, nil))
			var populated []types.RawRecord
			for key, label := range labels {
				record := nativeCOWRecord(label, "u", int64(key+1), "one", 2)
				record.Data["added_optional"], record.Data["tombstoned_at"] = int64(17), at
				populated = append(populated, record)
			}
			require.NoError(t, extend.Write(ctx, populated))
			require.NoError(t, extend.Close(ctx, map[string]any{"state": `{"lsn":"0/10"}`}))
			_, present := stream.Schema().Properties.Load("added_optional")
			require.False(t, present)
			_, present = stream.Schema().Properties.Load("tombstoned_at")
			require.False(t, present)
			second := ordinarySecondStream(stream)
			targetSeed := openNativeTestWriter(ctx, t, cfg, second, cat, nil, "target-seed", true)
			require.NoError(t, targetSeed.Write(ctx, []types.RawRecord{nativeCOWRecord("anchor", "r", 1, "one", 1)}))
			require.NoError(t, targetSeed.Close(ctx, map[string]any{"state": `{"lsn":"0/20"}`}))
			cfg.CatalogType = RestCatalog
			recoveryCtx, options := ordinaryTestContext(t, "preserve-destination-columns")
			adapter := &Iceberg{config: cfg, catalog: cat}
			streams := []types.StreamInterface{stream, second}
			proofs, err := adapter.PrepareOrdinaryRecovery(recoveryCtx, streams, "0/10")
			require.NoError(t, err)
			ordinaryTestPlan(options, proofs, "0/10", "0/20")
			writer := openNativeTestWriter(recoveryCtx, t, cfg, stream, cat, index, "replay", false)
			cleared := nativeCOWRecord("cleared", "u", 2, "one", 3)
			cleared.Data["added_optional"], cleared.Data["tombstoned_at"] = nil, nil
			overwritten := nativeCOWRecord("overwritten", "u", 3, "one", 3)
			overwritten.Data["added_optional"], overwritten.Data["tombstoned_at"] = int64(99), next
			sameBatch := nativeCOWRecord("same-batch", "u", 4, "one", 3)
			sameBatch.Data["added_optional"], sameBatch.Data["tombstoned_at"] = int64(55), next
			acrossBatch := nativeCOWRecord("across-batch", "u", 5, "one", 3)
			acrossBatch.Data["added_optional"], acrossBatch.Data["tombstoned_at"] = int64(66), next
			require.NoError(t, writer.Write(recoveryCtx, []types.RawRecord{
				nativeCOWRecord("kept", "u", 1, "one", 3), cleared, overwritten,
				sameBatch, nativeCOWRecord("same-batch", "u", 4, "one", 4), acrossBatch,
				nativeCOWRecord("deleted", "d", 6, "one", 3), nativeCOWRecord("new", "c", 8, "one", 3),
			}))
			require.NoError(t, writer.Write(recoveryCtx, []types.RawRecord{
				nativeCOWRecord("across-batch", "u", 5, "one", 4),
				nativeCOWRecord("cleared", "u", 2, "one", 4),
			}))
			require.NoError(t, writer.Close(recoveryCtx, map[string]any{"state": `{"lsn":"0/20"}`}))
			_, err = adapter.ConfirmOrdinaryRecovery(recoveryCtx, streams, proofs, "0/20")
			require.NoError(t, err)
			persisted, err := cat.LoadTable(recoveryCtx, writer.table.Identifier())
			require.NoError(t, err)
			require.Equal(t, tableUUID, persisted.Metadata().TableUUID())
			require.Equal(t, originalSchema.NumFields()+2, persisted.Schema().NumFields())
			for _, field := range originalSchema.Fields() {
				current, found := persisted.Schema().FindFieldByName(field.Name)
				require.True(t, found)
				require.True(t, field.Equals(current))
			}
			require.Equal(t, map[string]any{
				"kept": int64(17), "cleared": nil, "overwritten": int64(99), "same-batch": int64(55),
				"across-batch": int64(66), "deleted": int64(17), "bystander": int64(17), "new": nil,
			}, nativeSourceValues(t, persisted, "added_optional"))
			scan := persisted.Scan(table.WithSelectedFields(constants.OlakeID, "tombstoned_at"))
			defer scan.Close()
			schema, batches, err := scan.ToArrowRecords(recoveryCtx)
			require.NoError(t, err)
			actual := make(map[string]any)
			for batch, err := range batches {
				require.NoError(t, err)
				ids := batch.Column(schema.FieldIndices(constants.OlakeID)[0]).(*array.String)
				timestamps := batch.Column(schema.FieldIndices("tombstoned_at")[0]).(*array.Timestamp)
				for row := range int(batch.NumRows()) {
					if timestamps.IsNull(row) {
						actual[ids.Value(row)] = nil
					} else {
						actual[ids.Value(row)] = int64(timestamps.Value(row))
					}
				}
				batch.Release()
			}
			require.Equal(t, map[string]any{
				"kept": at.UnixMicro(), "cleared": nil, "overwritten": next.UnixMicro(), "same-batch": next.UnixMicro(),
				"across-batch": next.UnixMicro(), "deleted": at.UnixMicro(), "bystander": at.UnixMicro(), "new": nil,
			}, actual)
			if test.strategy == types.CopyOnWrite {
				hasDeletes, err := nativeCOWHasDeletes(recoveryCtx, persisted)
				require.NoError(t, err)
				require.False(t, hasDeletes)
			}
		})
	}
}

func TestNativeRecoverySourceColumnsRetainsDestinationOnlyFields(t *testing.T) {
	ctx := context.Background()
	cfg, stream, cat := nativeCOWFixture(t, types.UpdateTypeEquality)
	stream.StreamMetadata.DropRemovedColumns = true
	seed := openNativeTestWriter(ctx, t, cfg, stream, cat, nil, "seed", true)
	require.NoError(t, seed.Write(ctx, []types.RawRecord{nativeCOWRecord("old", "r", 1, "one", 1)}))
	require.NoError(t, seed.Close(ctx, map[string]any{"state": `{"lsn":"0/10"}`}))
	extend := openNativeTestWriter(ctx, t, cfg, stream, cat, nil, "extend", false)
	require.NoError(t, extend.AlterSourceColumns(ctx, []*nativeSourceColumn{{Key: "added_optional", IceType: "long"}}, nil))
	require.NoError(t, extend.Close(ctx, map[string]any{"state": `{"lsn":"0/10"}`}))
	cfg.CatalogType = RestCatalog
	recoveryCtx, options := ordinaryTestContext(t, "source-columns-recovery")
	adapter := &Iceberg{config: cfg, catalog: cat}
	proofs, err := adapter.PrepareOrdinaryRecovery(recoveryCtx, []types.StreamInterface{stream}, "0/10")
	require.NoError(t, err)
	ordinaryTestPlan(options, proofs, "0/10", "0/10")
	_, _, err = adapter.Setup(recoveryCtx, stream, nil, &destination.Options{ThreadID: "replay"})
	require.NoError(t, err)
	columns := []types.SourceColumn{}
	for name := range adapter.schema {
		if name != "added_optional" {
			columns = append(columns, types.SourceColumn{Name: name, Type: types.Int64})
		}
	}
	_, err = adapter.ApplySourceColumns(recoveryCtx, adapter.schema, types.SourceColumnsChange{Columns: columns})
	require.NoError(t, err)
	_, exists := adapter.writer.(*nativeWriter).table.Schema().FindFieldByName("added_optional")
	require.True(t, exists)
	require.Error(t, adapter.writer.AlterSourceColumns(recoveryCtx, nil, []string{"value"}))
	adapter.writer.Abort()
}

func TestNativeRecoveryColumnLogicalValueFidelity(t *testing.T) {
	schema := apacheiceberg.NewSchema(0,
		apacheiceberg.NestedField{ID: 1, Name: constants.OlakeID, Type: apacheiceberg.StringType{}, Required: true},
		apacheiceberg.NestedField{ID: 2, Name: "nested", Type: &apacheiceberg.StructType{FieldList: []apacheiceberg.NestedField{
			{ID: 3, Name: "child", Type: apacheiceberg.StringType{}},
			{ID: 4, Name: "large", Type: apacheiceberg.Int64Type{}},
		}}},
		apacheiceberg.NestedField{ID: 5, Name: "event_date", Type: apacheiceberg.DateType{}},
		apacheiceberg.NestedField{ID: 6, Name: "sentinel", Type: apacheiceberg.StringType{}},
	)
	arrowSchema, err := table.SchemaToArrowSchema(schema, nil, true, false)
	require.NoError(t, err)
	builder := array.NewRecordBuilder(memory.DefaultAllocator, arrowSchema)
	defer builder.Release()
	builder.Field(0).(*array.StringBuilder).Append("matched")
	require.NoError(t, builder.Field(1).AppendValueFromString(`{"child":"value","large":9007199254740993}`))
	require.NoError(t, builder.Field(2).AppendValueFromString("2025-03-08"))
	builder.Field(3).(*array.StringBuilder).Append(array.NullValueStr)
	builder.Field(0).(*array.StringBuilder).Append("new")
	for index := 1; index < len(arrowSchema.Fields()); index++ {
		builder.Field(index).AppendNull()
	}
	original := builder.NewRecordBatch()
	defer original.Release()
	rows := make([]map[string]any, int(original.NumRows()))
	for row := range rows {
		rows[row] = make(map[string]any)
		for index, field := range original.Schema().Fields() {
			rows[row][field.Name] = nativeRecoveryValue(original.Column(index), row)
		}
	}
	reencoded, err := nativeArrowRecord(schema, rows)
	require.NoError(t, err)
	defer reencoded.Release()
	require.True(t, array.RecordEqual(original, reencoded))
}
