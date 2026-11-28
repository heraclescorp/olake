package iceberg

import (
	"context"
	"encoding/json"
	"testing"

	apacheiceberg "github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
	"github.com/datazip-inc/olake/types"
	"github.com/stretchr/testify/require"
)

func backfillBindingTestBase(t *testing.T) table.Metadata {
	t.Helper()
	schema := apacheiceberg.NewSchemaWithIdentifiers(0, []int{1},
		apacheiceberg.NestedField{ID: 1, Name: "id", Type: apacheiceberg.PrimitiveTypes.String, Required: true},
		apacheiceberg.NestedField{ID: 2, Name: "value", Type: apacheiceberg.PrimitiveTypes.Int64},
		apacheiceberg.NestedField{ID: 3, Name: "nested", Type: &apacheiceberg.StructType{FieldList: []apacheiceberg.NestedField{
			{ID: 4, Name: "child", Type: apacheiceberg.PrimitiveTypes.String},
		}}},
	)
	metadata, err := table.NewMetadata(schema, apacheiceberg.UnpartitionedSpec, table.UnsortedSortOrder, "s3://bucket/table", apacheiceberg.Properties{table.PropertyFormatVersion: "2"})
	require.NoError(t, err)
	return metadata
}

func backfillBindingTestExpected(t *testing.T, metadata table.Metadata) types.BackfillTableBinding {
	t.Helper()
	shape, err := ordinaryRecoveryShape(metadata)
	require.NoError(t, err)
	return types.BackfillTableBinding{TableUUID: metadata.TableUUID().String(), ShapeSHA256: shape}
}

func backfillBindingTestRewrite(t *testing.T, metadata table.Metadata, updates map[string]any) table.Metadata {
	t.Helper()
	encoded, err := json.Marshal(metadata)
	require.NoError(t, err)
	var raw map[string]any
	require.NoError(t, json.Unmarshal(encoded, &raw))
	for key, value := range updates {
		raw[key] = value
	}
	encoded, err = json.Marshal(raw)
	require.NoError(t, err)
	changed, err := table.ParseMetadataBytes(encoded)
	require.NoError(t, err)
	return changed
}

func TestBackfillBindingHistoricalNullableAdditions(t *testing.T) {
	base := backfillBindingTestBase(t)
	original := base.CurrentSchema()
	fields := append(original.Fields(), apacheiceberg.NestedField{ID: 5, Name: "tombstoned_at", Type: apacheiceberg.PrimitiveTypes.TimestampTz})
	first := apacheiceberg.NewSchemaWithIdentifiers(1, original.IdentifierFieldIDs, fields...)
	fields = append(first.Fields(), apacheiceberg.NestedField{ID: 6, Name: "note", Type: apacheiceberg.PrimitiveTypes.String})
	second := apacheiceberg.NewSchemaWithIdentifiers(2, original.IdentifierFieldIDs, fields...)
	current := backfillHistoricalMetadata{Metadata: base, schemas: []*apacheiceberg.Schema{original, first, second}, current: second, lastColumn: 6}
	frozen := backfillBindingTestExpected(t, base)
	require.NotEqual(t, frozen.ShapeSHA256, backfillBindingTestExpected(t, current).ShapeSHA256)
	require.NoError(t, validateBackfillTableBinding(current, frozen))
	require.NoError(t, validateBackfillTableBinding(current, backfillBindingTestExpected(t, current)))
	prefix := backfillHistoricalMetadata{Metadata: base, schemas: []*apacheiceberg.Schema{original, first}, current: first, lastColumn: 5}
	require.NoError(t, validateBackfillTableBinding(current, backfillBindingTestExpected(t, prefix)))
}

func TestBackfillBindingReconstructsOriginalCurrentAndHighestIDs(t *testing.T) {
	base := backfillBindingTestBase(t)
	original := base.CurrentSchema()
	retired := apacheiceberg.NewSchemaWithIdentifiers(1, original.IdentifierFieldIDs,
		append(original.Fields(), apacheiceberg.NestedField{ID: 5, Name: "retired", Type: apacheiceberg.PrimitiveTypes.String})...)
	frozen := backfillHistoricalMetadata{Metadata: base, schemas: []*apacheiceberg.Schema{original, retired}, current: original, lastColumn: 5}
	current := apacheiceberg.NewSchemaWithIdentifiers(2, original.IdentifierFieldIDs,
		append(original.Fields(), apacheiceberg.NestedField{ID: 6, Name: "tombstoned_at", Type: apacheiceberg.PrimitiveTypes.TimestampTz})...)
	live := backfillHistoricalMetadata{Metadata: base, schemas: []*apacheiceberg.Schema{original, retired, current}, current: current, lastColumn: 6}
	require.NoError(t, validateBackfillTableBinding(live, backfillBindingTestExpected(t, frozen)))
}

func TestBackfillBindingRejectsUnsafeHistoricalChanges(t *testing.T) {
	cases := []string{"rename", "field_id", "type", "requiredness", "identifier_ids", "reorder", "drop", "nested_addition", "required_addition", "initial_default", "write_default", "intermediate_type", "intermediate_added_then_dropped", "missing_original", "missing_intermediate", "current_rollback", "last_column_low", "last_column_high", "location", "format", "partition_history", "sort_history", "saved_shape", "uuid"}
	for _, name := range cases {
		t.Run(name, func(t *testing.T) {
			base := backfillBindingTestBase(t)
			original := base.CurrentSchema()
			frozen := backfillBindingTestExpected(t, base)
			fields := append(original.Fields(), apacheiceberg.NestedField{ID: 5, Name: "tombstoned_at", Type: apacheiceberg.PrimitiveTypes.TimestampTz})
			ids := original.IdentifierFieldIDs
			lastColumn := 5
			var intermediate *apacheiceberg.Schema
			schemaID := 1
			switch name {
			case "rename":
				fields[1].Name = "renamed"
			case "field_id":
				fields[1].ID = 6
				lastColumn = 6
			case "type":
				fields[1].Type = apacheiceberg.PrimitiveTypes.String
			case "requiredness":
				fields[1].Required = true
			case "identifier_ids":
				ids = nil
			case "reorder":
				fields[0], fields[1] = fields[1], fields[0]
			case "drop":
				fields = append(fields[:1], fields[2:]...)
			case "nested_addition":
				fields[2].Type = &apacheiceberg.StructType{FieldList: []apacheiceberg.NestedField{
					{ID: 4, Name: "child", Type: apacheiceberg.PrimitiveTypes.String},
					{ID: 6, Name: "extra", Type: apacheiceberg.PrimitiveTypes.String},
				}}
				lastColumn = 6
			case "required_addition":
				fields[3].Required = true
			case "initial_default":
				fields[3].InitialDefault = apacheiceberg.Timestamp(1)
			case "write_default":
				fields[3].WriteDefault = apacheiceberg.Timestamp(1)
			case "intermediate_type":
				changed := original.Fields()
				changed[1].Type = apacheiceberg.PrimitiveTypes.String
				intermediate = apacheiceberg.NewSchemaWithIdentifiers(1, ids, changed...)
				schemaID = 2
			case "intermediate_added_then_dropped":
				intermediate = apacheiceberg.NewSchemaWithIdentifiers(1, ids,
					append(original.Fields(), apacheiceberg.NestedField{ID: 5, Name: "temporary", Type: apacheiceberg.PrimitiveTypes.String})...)
				fields[3].ID = 6
				lastColumn, schemaID = 6, 2
			case "missing_intermediate":
				schemaID = 2
			case "last_column_low":
				lastColumn = 4
			case "last_column_high":
				lastColumn = 6
			case "location":
				base = backfillBindingTestRewrite(t, base, map[string]any{"location": "s3://different/table"})
			case "format":
				var err error
				base, err = table.NewMetadataWithUUID(original, apacheiceberg.UnpartitionedSpec, table.UnsortedSortOrder, base.Location(), apacheiceberg.Properties{table.PropertyFormatVersion: "3"}, base.TableUUID())
				require.NoError(t, err)
			case "partition_history":
				base = backfillBindingTestRewrite(t, base, map[string]any{"partition-specs": []any{map[string]any{"spec-id": 0, "fields": []any{}}, map[string]any{"spec-id": 1, "fields": []any{}}}})
			case "sort_history":
				base = backfillBindingTestRewrite(t, base, map[string]any{"sort-orders": []any{map[string]any{"order-id": 0, "fields": []any{}}, map[string]any{"order-id": 1, "fields": []any{map[string]any{"source-id": 1, "transform": "identity", "direction": "asc", "null-order": "nulls-first"}}}}})
			case "saved_shape":
				frozen.ShapeSHA256 = "not-the-saved-shape"
			case "uuid":
				frozen.TableUUID = "not-the-saved-uuid"
			}
			next := apacheiceberg.NewSchemaWithIdentifiers(schemaID, ids, fields...)
			history := []*apacheiceberg.Schema{original}
			if intermediate != nil {
				history = append(history, intermediate)
			}
			history = append(history, next)
			if name == "missing_original" {
				history = history[1:]
			}
			current := next
			if name == "current_rollback" {
				current = original
			}
			live := backfillHistoricalMetadata{Metadata: base, schemas: history, current: current, lastColumn: lastColumn}
			require.Error(t, validateBackfillTableBinding(live, frozen))
		})
	}
}

func TestNativeBackfillRecoveryPreservesFrozenBindingAfterNullableAddition(t *testing.T) {
	adapter, ctx, stream, plan, frozen := nativeCommittedIDsFixture(t, false)
	first := types.Chunk{Min: nil, Max: "m"}
	writer := openNativeTestWriter(ctx, t, adapter.config, stream, adapter.catalog, nil, nativeBackfillChunkID(stream, first), true)
	require.NoError(t, writer.Write(ctx, []types.RawRecord{nativeTestRecord("a", "r", 1, "one")}))
	require.NoError(t, writer.Close(ctx, nil))
	options, ok := types.OrdinaryRecoveryFromContext(ctx)
	require.True(t, ok)
	options.BackfillState.SetBackfillTable(stream, &frozen)
	require.NoError(t, options.BackfillState.SetBackfillPlan(stream, plan))
	journal := nativeBackfillTestJournal(t, options.BackfillState)
	originalJournal, err := journal.Read(stream.ID())
	require.NoError(t, err)
	originalBinding := writer.table.Properties()[nativeBackfillBindingProperty]
	underlying := adapter.catalog.(*nativeCatalog).Catalog
	tbl, err := underlying.LoadTable(context.Background(), writer.table.Identifier())
	require.NoError(t, err)
	tx := tbl.NewTransaction()
	require.NoError(t, tx.UpdateSchema(true, false).AddColumn([]string{"tombstoned_at"}, apacheiceberg.PrimitiveTypes.TimestampTz, "", false, nil).Commit())
	evolved, err := tx.Commit(context.Background())
	require.NoError(t, err)
	actual := backfillBindingTestExpected(t, evolved.Metadata())
	require.NotEqual(t, frozen.ShapeSHA256, actual.ShapeSHA256)
	nextCtx, _ := nativeBackfillRecoveryContext(t, "nullable-addition-recovery", options.BackfillState)
	next := &Iceberg{config: adapter.config, catalog: &nativeCatalog{Catalog: underlying}}
	require.NoError(t, next.PrepareBackfillRecovery(nextCtx, []types.StreamInterface{stream}, "0/10"))
	require.Equal(t, actual.ShapeSHA256, next.backfillRecovery[stream.ID()].marker.ShapeSHA256)
	prepared, err := next.PrepareBackfillTable(nextCtx, stream)
	require.NoError(t, err)
	require.Equal(t, frozen, *prepared)
	committed, err := next.GetBackfillCommittedIDs(nextCtx, stream, plan, frozen)
	require.NoError(t, err)
	require.Equal(t, map[string]struct{}{nativeBackfillChunkID(stream, first): {}}, committed)
	confirmedJournal, err := journal.Read(stream.ID())
	require.NoError(t, err)
	require.Equal(t, originalJournal, confirmedJournal)
	confirmed, err := underlying.LoadTable(context.Background(), tbl.Identifier())
	require.NoError(t, err)
	require.Equal(t, originalBinding, confirmed.Properties()[nativeBackfillBindingProperty])
	require.Equal(t, evolved.MetadataLocation(), confirmed.MetadataLocation())
	require.Equal(t, evolved.CurrentSnapshot().SnapshotID, confirmed.CurrentSnapshot().SnapshotID)
	racingTx := confirmed.NewTransaction()
	require.NoError(t, racingTx.UpdateSchema(true, false).AddColumn([]string{"raced"}, apacheiceberg.PrimitiveTypes.String, "", false, nil).Commit())
	_, err = racingTx.Commit(context.Background())
	require.NoError(t, err)
	_, err = next.PrepareBackfillTable(nextCtx, stream)
	require.ErrorContains(t, err, "shape changed")
	_, err = next.GetBackfillCommittedIDs(nextCtx, stream, plan, frozen)
	require.ErrorContains(t, err, "shape changed")
}

func TestNativeBackfillRecoveryRejectsIncompatibleIntermediateSchema(t *testing.T) {
	adapter, ctx, stream, plan, frozen := nativeCommittedIDsFixture(t, false)
	options, ok := types.OrdinaryRecoveryFromContext(ctx)
	require.True(t, ok)
	options.BackfillState.SetBackfillTable(stream, &frozen)
	require.NoError(t, options.BackfillState.SetBackfillPlan(stream, plan))
	underlying := adapter.catalog.(*nativeCatalog).Catalog
	tbl, err := underlying.LoadTable(context.Background(), table.Identifier{"db", "records"})
	require.NoError(t, err)
	originalName := tbl.Schema().Field(0).Name
	tx := tbl.NewTransaction()
	require.NoError(t, tx.UpdateSchema(true, false).RenameColumn([]string{originalName}, "temporary_name").Commit())
	tbl, err = tx.Commit(context.Background())
	require.NoError(t, err)
	tx = tbl.NewTransaction()
	require.NoError(t, tx.UpdateSchema(true, false).RenameColumn([]string{"temporary_name"}, originalName).AddColumn([]string{"tombstoned_at"}, apacheiceberg.PrimitiveTypes.TimestampTz, "", false, nil).Commit())
	tbl, err = tx.Commit(context.Background())
	require.NoError(t, err)
	nextCtx, _ := nativeBackfillRecoveryContext(t, "unsafe-intermediate-recovery", options.BackfillState)
	spy := &nativeCommitFaultCatalog{Catalog: underlying}
	adapter.config.CatalogType = RestCatalog
	next := &Iceberg{config: adapter.config, catalog: &nativeCatalog{Catalog: spy}}
	require.Error(t, next.PrepareBackfillRecovery(nextCtx, []types.StreamInterface{stream}, "0/10"))
	require.Zero(t, spy.calls)
	confirmed, err := underlying.LoadTable(context.Background(), tbl.Identifier())
	require.NoError(t, err)
	require.Equal(t, tbl.MetadataLocation(), confirmed.MetadataLocation())
}

func TestNativeBackfillBindingEnrollmentRetainsHistoricalHash(t *testing.T) {
	for _, mode := range []string{"fresh_journal", "missing_server_binding", "different_compatible_candidate"} {
		t.Run(mode, func(t *testing.T) {
			adapter, ctx, stream, plan, frozen := nativeCommittedIDsFixture(t, false)
			underlying := adapter.catalog.(*nativeCatalog).Catalog
			tbl, err := underlying.LoadTable(context.Background(), table.Identifier{"db", "records"})
			require.NoError(t, err)
			tx := tbl.NewTransaction()
			require.NoError(t, tx.UpdateSchema(true, false).AddColumn([]string{"tombstoned_at"}, apacheiceberg.PrimitiveTypes.TimestampTz, "", false, nil).Commit())
			tbl, err = tx.Commit(context.Background())
			require.NoError(t, err)
			options, ok := types.OrdinaryRecoveryFromContext(ctx)
			require.True(t, ok)
			saved := options.BackfillState
			if mode == "fresh_journal" {
				saved = nativeBackfillState()
			}
			saved.SetBackfillTable(stream, &frozen)
			require.NoError(t, saved.SetBackfillPlan(stream, plan))
			if mode == "missing_server_binding" {
				_, _, err = underlying.CommitTable(context.Background(), tbl.Identifier(), nil, []table.Update{table.NewRemovePropertiesUpdate([]string{nativeBackfillBindingProperty})})
				require.NoError(t, err)
			}
			if mode == "different_compatible_candidate" {
				actual := backfillBindingTestExpected(t, tbl.Metadata())
				saved.SetBackfillTable(stream, &actual)
			}
			nextCtx, _ := nativeBackfillRecoveryContext(t, "historical-enrollment-"+mode, saved)
			spy := &nativeCommitFaultCatalog{Catalog: underlying}
			adapter.config.CatalogType = RestCatalog
			next := &Iceberg{config: adapter.config, catalog: &nativeCatalog{Catalog: spy}}
			err = next.PrepareBackfillRecovery(nextCtx, []types.StreamInterface{stream}, "0/10")
			if mode == "different_compatible_candidate" {
				require.ErrorContains(t, err, "candidate table binding differs from the durable binding journal")
				require.Zero(t, spy.calls)
				return
			}
			require.NoError(t, err)
			prepared, err := next.PrepareBackfillTable(nextCtx, stream)
			require.NoError(t, err)
			require.Equal(t, frozen, *prepared)
			journal := nativeBackfillTestJournal(t, saved)
			entry, err := journal.Read(stream.ID())
			require.NoError(t, err)
			require.NotNil(t, entry)
			require.Equal(t, frozen.TableUUID, entry.TableUUID)
			require.Equal(t, frozen.ShapeSHA256, entry.ShapeSHA256)
		})
	}
}
