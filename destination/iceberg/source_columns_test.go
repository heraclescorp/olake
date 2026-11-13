package iceberg

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow/array"
	apacheiceberg "github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func sourceColumnsStream(dropRemoved bool, selected ...string) *types.ConfiguredStream {
	stream := &types.ConfiguredStream{
		Stream: types.NewStream("orders", "public", nil),
		StreamMetadata: types.StreamMetadata{
			Normalization:      new(true),
			DropRemovedColumns: dropRemoved,
		},
	}
	stream.Stream.UpsertField("_cdc_lsn", types.String, true, true)
	if len(selected) > 0 {
		stream.StreamMetadata.SelectedColumns = &types.SelectedColumns{Columns: selected}
	}
	return stream
}

func tableSchema(columns ...string) map[string]string {
	schema := map[string]string{
		constants.OlakeID:        "string",
		constants.OpType:         "string",
		constants.CdcTimestamp:   "timestamptz",
		constants.OlakeTimestamp: "timestamptz",
		"_cdc_lsn":               "string",
	}
	for _, column := range columns {
		schema[column] = "string"
	}
	return schema
}

func addedKeys(added []*nativeSourceColumn) []string {
	keys := make([]string, 0, len(added))
	for _, column := range added {
		keys = append(keys, column.Key)
	}
	return keys
}

func TestPlanSourceColumnsAdds(t *testing.T) {
	stream := sourceColumnsStream(false)
	change := types.SourceColumnsChange{Columns: []types.SourceColumn{
		{Name: "id", Type: types.Int64},
		{Name: "Qty", Type: types.Int32, ExistingRowValue: int32(7)},
		{Name: "note", Type: types.String},
		{Name: "created", Type: types.TimestampMicro, ExistingRowValueUnknown: true},
		{Name: "bad", Type: types.Int32, ExistingRowValue: "not a number"},
	}}

	added, dropped := planSourceColumns(stream, tableSchema("id"), change, map[string]struct{}{})

	assert.Empty(t, dropped)
	assert.Equal(t, []string{"qty", "note", "created", "bad"}, addedKeys(added))
	assert.Equal(t, "int", added[0].IceType)
	assert.Equal(t, int32(7), added[0].InitialDefault.Any())
	for _, column := range added[1:] {
		assert.Nil(t, column.InitialDefault, column.Key)
	}
}

func TestPlanSourceColumnsSkipsUnselected(t *testing.T) {
	stream := sourceColumnsStream(true, "id", "qty")
	change := types.SourceColumnsChange{Columns: []types.SourceColumn{
		{Name: "id", Type: types.Int64},
		{Name: "qty", Type: types.Int32},
		{Name: "secret", Type: types.String},
	}}

	added, dropped := planSourceColumns(stream, tableSchema("id"), change, map[string]struct{}{})

	assert.Equal(t, []string{"qty"}, addedKeys(added))
	assert.Empty(t, dropped)
}

func TestPlanSourceColumnsDrops(t *testing.T) {
	change := types.SourceColumnsChange{
		Columns:     []types.SourceColumn{{Name: "id", Type: types.Int64}},
		KeepColumns: []string{"total"},
	}
	schema := tableSchema("id", "gone", "total", constants.StringifiedData)

	t.Run("option off keeps removed columns", func(t *testing.T) {
		added, dropped := planSourceColumns(sourceColumnsStream(false), schema, change, map[string]struct{}{})
		assert.Empty(t, added)
		assert.Empty(t, dropped)
	})

	t.Run("option on drops only source columns", func(t *testing.T) {
		added, dropped := planSourceColumns(sourceColumnsStream(true), schema, change, map[string]struct{}{})
		assert.Empty(t, added)
		assert.Equal(t, []string{"gone"}, dropped)
	})
}

func TestPlanSourceColumnsSyncStart(t *testing.T) {
	stream := sourceColumnsStream(true)
	syncStartColumns := map[string]struct{}{}
	syncStart := types.SourceColumnsChange{SyncStart: true, Columns: []types.SourceColumn{
		{Name: "id", Type: types.Int64},
		{Name: "late", Type: types.String},
		{Name: "unknown", Type: types.String, ExistingRowValueUnknown: true},
		{Name: "qty", Type: types.Int32, ExistingRowValue: int32(7)},
	}}

	added, dropped := planSourceColumns(stream, tableSchema("id", "gone"), syncStart, syncStartColumns)

	assert.Equal(t, []string{"late", "unknown"}, addedKeys(added), "a non-NULL existing-row value waits for the change stream")
	assert.Empty(t, dropped, "sync start never drops")

	schema := tableSchema("id", "gone", "late", "unknown")
	beforeAdd := types.SourceColumnsChange{Columns: []types.SourceColumn{{Name: "id", Type: types.Int64}, {Name: "unknown", Type: types.String}}}
	added, dropped = planSourceColumns(stream, schema, beforeAdd, syncStartColumns)
	assert.Empty(t, added)
	assert.Equal(t, []string{"gone"}, dropped, "a change from before the add keeps the column added at sync start")

	afterAdd := types.SourceColumnsChange{Columns: []types.SourceColumn{{Name: "id", Type: types.Int64}, {Name: "late", Type: types.String}}}
	_, dropped = planSourceColumns(stream, schema, afterAdd, syncStartColumns)
	assert.Equal(t, []string{"gone", "unknown"}, dropped)

	_, dropped = planSourceColumns(stream, schema, beforeAdd, syncStartColumns)
	assert.Equal(t, []string{"gone", "late"}, dropped, "once the change stream listed it, the column is dropped like any other")
}

func nativeSourceValues(t *testing.T, tbl *table.Table, column string) map[string]any {
	t.Helper()
	scan := tbl.Scan()
	defer scan.Close()
	schema, records, err := scan.ToArrowRecords(context.Background())
	require.NoError(t, err)
	idIndex := schema.FieldIndices(constants.OlakeID)[0]
	columnIndex := schema.FieldIndices(column)[0]
	values := make(map[string]any)
	for batch, err := range records {
		require.NoError(t, err)
		ids := batch.Column(idIndex).(*array.String)
		column := batch.Column(columnIndex).(*array.Int64)
		for row := range int(batch.NumRows()) {
			if column.IsNull(row) {
				values[ids.Value(row)] = nil
			} else {
				values[ids.Value(row)] = column.Value(row)
			}
		}
		batch.Release()
	}
	return values
}

func TestNativeSourceColumnsInitialDefaultsAndMissingValues(t *testing.T) {
	for _, mode := range []types.UpdateType{types.UpdateTypeEquality, types.UpdateTypeDeletionVector} {
		t.Run(string(mode), func(t *testing.T) {
			ctx := context.Background()
			cfg, stream, cat := nativeWriterFixture(t, mode, true, "")
			seed := openNativeTestWriter(ctx, t, cfg, stream, cat, nil, "seed", true)
			require.NoError(t, seed.Write(ctx, []types.RawRecord{nativeTestRecord("old", "r", 1, "one")}))
			oldPath := seed.dataFiles[0].FilePath()
			require.NoError(t, seed.Close(ctx, map[string]any{"state": "preserve-recovery"}))
			originalSchema := seed.table.Schema()
			originalSnapshot := nativeSnapshotID(seed.table)
			recovery := seed.table.Properties()[nativeRecoveryProperty]
			var wantOld any
			if mode == types.UpdateTypeDeletionVector {
				wantOld = int64(42)
			}

			// Discovery before SyncStart must not add the field and consume the
			// opportunity to attach PostgreSQL's authoritative initial default.
			stream.Stream.UpsertField("added", types.Int32, true, false)
			writer := openNativeTestWriter(ctx, t, cfg, stream, cat, nil, "alter", true)
			_, exists := writer.table.Schema().FindFieldByName("added")
			require.False(t, exists)
			initial, err := nativeSourceColumnDefault("int", "42")
			require.NoError(t, err)
			require.NoError(t, writer.AlterSourceColumns(ctx, []*nativeSourceColumn{
				{Key: "added", IceType: "int", InitialDefault: initial.Val},
				{Key: "nullable", IceType: "string"},
			}, nil))
			persisted, err := cat.LoadTable(ctx, writer.table.Identifier())
			require.NoError(t, err)
			field, exists := persisted.Schema().FindFieldByName("added")
			require.True(t, exists)
			require.False(t, field.Required)
			require.EqualValues(t, wantOld, field.InitialDefault)
			require.Equal(t, seed.table.Metadata().Version(), persisted.Metadata().Version())
			require.Nil(t, field.WriteDefault)
			require.Greater(t, field.ID, originalSchema.HighestFieldID())
			require.Equal(t, originalSchema.IdentifierFieldIDs, persisted.Schema().IdentifierFieldIDs)
			for _, original := range originalSchema.Fields() {
				current, found := persisted.Schema().FindFieldByName(original.Name)
				require.True(t, found)
				require.Equal(t, original.ID, current.ID)
			}
			require.Equal(t, recovery, persisted.Properties()[nativeRecoveryProperty])
			require.Equal(t, originalSnapshot, nativeSnapshotID(persisted))
			// Upstream v0.7.0 cannot scan a projection with no physical columns.
			rows, err := writer.ReadRows(ctx, oldPath, []int64{0}, []string{constants.OlakeID, "added", "nullable"})
			require.NoError(t, err)
			require.EqualValues(t, wantOld, rows[0]["added"])
			require.Nil(t, rows[0]["nullable"])

			columns := writer.schemaMap()
			columns["added"] = "long"
			columns["record_discovered"] = "string"
			require.NoError(t, writer.EvolveSchema(ctx, columns))
			promoted, found := writer.table.Schema().FindFieldByName("added")
			require.True(t, found)
			require.Equal(t, field.ID, promoted.ID)
			require.EqualValues(t, wantOld, promoted.InitialDefault)
			require.Nil(t, promoted.WriteDefault)
			require.Equal(t, recovery, writer.table.Properties()[nativeRecoveryProperty])
			require.NoError(t, writer.Write(ctx, []types.RawRecord{nativeTestRecord("new", "r", 2, "one")}))
			require.NoError(t, writer.Close(ctx, nil))
			persisted, err = cat.LoadTable(ctx, writer.table.Identifier())
			require.NoError(t, err)
			require.Equal(t, map[string]any{"old": wantOld, "new": nil}, nativeSourceValues(t, persisted, "added"))
		})
	}
}

func TestNativeSourceColumnsDropAndReAddUsesFreshIDs(t *testing.T) {
	ctx := context.Background()
	cfg, stream, cat := nativeWriterFixture(t, types.UpdateTypeDeletionVector, true, "")
	seed := openNativeTestWriter(ctx, t, cfg, stream, cat, nil, "seed", true)
	require.NoError(t, seed.Write(ctx, []types.RawRecord{nativeTestRecord("old", "r", 7, "one")}))
	require.NoError(t, seed.Close(ctx, map[string]any{"state": "keep"}))
	writer := openNativeTestWriter(ctx, t, cfg, stream, cat, nil, "alter", true)
	defer writer.Abort()
	old, found := writer.table.Schema().FindFieldByName("value")
	require.True(t, found)
	recovery := writer.table.Properties()[nativeRecoveryProperty]
	// Merely adding a source column must never remove unmentioned columns.
	require.NoError(t, writer.AlterSourceColumns(ctx, []*nativeSourceColumn{{Key: "extra", IceType: "string"}}, nil))
	_, found = writer.table.Schema().FindFieldByName("value")
	require.True(t, found)
	require.NoError(t, writer.AlterSourceColumns(ctx, nil, []string{"value"}))
	persisted, err := cat.LoadTable(ctx, writer.table.Identifier())
	require.NoError(t, err)
	_, found = persisted.Schema().FindFieldByName("value")
	require.False(t, found)
	initial, err := nativeSourceColumnDefault("long", 99)
	require.NoError(t, err)
	require.NoError(t, writer.AlterSourceColumns(ctx, []*nativeSourceColumn{{Key: "value", IceType: "long", InitialDefault: initial.Val}}, nil))
	persisted, err = cat.LoadTable(ctx, writer.table.Identifier())
	require.NoError(t, err)
	readded, found := persisted.Schema().FindFieldByName("value")
	require.True(t, found)
	require.Greater(t, readded.ID, old.ID)
	require.Nil(t, readded.WriteDefault)
	require.Equal(t, recovery, persisted.Properties()[nativeRecoveryProperty])
	// Name reuse must not expose the old physical field's value (7).
	require.Equal(t, map[string]any{"old": int64(99)}, nativeSourceValues(t, persisted, "value"))
}

func TestNativeSourceColumnsKeepIdentifiersHistoricalPartitionsAndSortSources(t *testing.T) {
	ctx := context.Background()
	cfg, stream, cat := nativeWriterFixture(t, types.UpdateTypeEquality, true, "/{part, identity}")
	writer := openNativeTestWriter(ctx, t, cfg, stream, cat, nil, "protected", true)
	defer writer.Abort()
	original := writer.table.Schema()
	value, found := original.FindFieldByName("value")
	require.True(t, found)
	order, err := table.NewSortOrder(1, []table.SortField{{SourceIDs: []int{value.ID}, Transform: apacheiceberg.IdentityTransform{}, Direction: table.SortASC, NullOrder: table.NullsFirst}})
	require.NoError(t, err)
	tx := writer.table.NewTransaction()
	require.NoError(t, tx.UpdateSpec(true).RemoveField("part").Commit())
	require.NoError(t, tx.ReplaceSortOrder(order))
	writer.table, err = tx.Commit(ctx)
	require.NoError(t, err)
	spec := writer.table.Spec()
	require.Zero(t, spec.NumFields(), "part is now only a historical partition source")
	metadataLocation := writer.table.MetadataLocation()
	require.NoError(t, writer.AlterSourceColumns(ctx, nil, []string{constants.OlakeID}))
	require.NoError(t, writer.AlterSourceColumns(ctx, nil, []string{"part"}))
	require.NoError(t, writer.AlterSourceColumns(ctx, nil, []string{"value"}))
	persisted, err := cat.LoadTable(ctx, writer.table.Identifier())
	require.NoError(t, err)
	require.True(t, original.Equals(persisted.Schema()))
	require.Equal(t, metadataLocation, persisted.MetadataLocation(), "keeping protected columns must not create metadata")
}

func TestNativeSourceColumnDefaultConversions(t *testing.T) {
	stamp := time.Date(2025, 3, 8, 12, 0, 0, 123456000, time.UTC)
	for _, tc := range []struct {
		iceType string
		value   any
		want    any
	}{
		{"boolean", "true", true},
		{"int", "42", int32(42)},
		{"long", "42", int64(42)},
		{"float", "1.5", float32(1.5)},
		{"double", "1.5", float64(1.5)},
		{"string", 42, "42"},
		{"string", map[string]any{"a": 1}, `{"a":1}`},
		{"string", []any{1, "two"}, `[1,"two"]`},
		{"timestamp", stamp, apacheiceberg.Timestamp(stamp.UnixMicro())},
		{"timestamptz", stamp, apacheiceberg.Timestamp(stamp.UnixMicro())},
		{"binary", []byte{1, 2}, []byte{1, 2}},
		{"fixed[2]", []byte{1, 2}, []byte{1, 2}},
	} {
		t.Run(tc.iceType, func(t *testing.T) {
			literal, err := nativeSourceColumnDefault(tc.iceType, tc.value)
			require.NoError(t, err)
			require.True(t, literal.Valid)
			require.Equal(t, tc.want, literal.Val.Any())
			schema, err := nativeSchema(map[string]string{"default": tc.iceType}, false)
			require.NoError(t, err)
			require.True(t, literal.Val.Type().Equals(schema.Field(0).Type))
		})
	}
	for _, tc := range []struct {
		iceType string
		value   any
	}{
		{"boolean", "not-a-bool"}, {"int", "not-an-int"},
		{"long", "not-an-int"}, {"float", "not-a-number"},
		{"double", "not-a-number"}, {"timestamp", "not-a-date"},
		{"fixed[2]", []byte{1}}, {"not-a-type", "value"},
		{"int", int64(1 << 40)}, {"int", uint32(1 << 31)},
		{"int", 1.5}, {"int", float64(1 << 40)},
		{"long", uint64(1 << 63)}, {"long", float64(1 << 63)},
		{"long", 1.5}, {"float", float64(1e100)},
		{"double", "1e1000"}, {"timestamp", "2025-02-30T12:00:00Z"},
		{"timestamptz", "2025-03-08Tnot-a-time"},
	} {
		_, err := nativeSourceColumnDefault(tc.iceType, tc.value)
		require.Error(t, err)
	}
	var nilPointer *any
	var null any
	for _, value := range []any{nil, nilPointer, &null} {
		literal, err := nativeSourceColumnDefault("long", value)
		require.NoError(t, err)
		require.False(t, literal.Valid)
		require.Nil(t, literal.Val)
	}
}

func TestNativeSchemaEvolutionPreservesExistingDefaultsAndIdentifiers(t *testing.T) {
	ctx := context.Background()
	cfg, stream, cat := nativeWriterFixture(t, types.UpdateTypeDeletionVector, true, "")
	seed := openNativeTestWriter(ctx, t, cfg, stream, cat, nil, "seed", true)
	require.NoError(t, seed.Write(ctx, []types.RawRecord{nativeTestRecord("old", "r", 7, "one")}))
	require.NoError(t, seed.Close(ctx, map[string]any{"state": "keep-existing"}))
	tx := seed.table.NewTransaction()
	require.NoError(t, tx.UpdateSchema(true, false).
		AddColumn([]string{"external_default"}, apacheiceberg.PrimitiveTypes.Int64, "", false, apacheiceberg.NewLiteral(int64(17))).
		AddColumn([]string{"external_identifier"}, apacheiceberg.PrimitiveTypes.String, "", true, apacheiceberg.NewLiteral("identifier")).
		SetIdentifierField([][]string{{constants.OlakeID}, {"external_identifier"}}).Commit())
	committed, err := tx.Commit(ctx)
	require.NoError(t, err)
	original, found := committed.Schema().FindFieldByName("external_default")
	require.True(t, found)
	recovery := committed.Properties()[nativeRecoveryProperty]
	writer := openNativeTestWriter(ctx, t, cfg, stream, cat, nil, "evolve", true)
	defer writer.Abort()
	columns := writer.schemaMap()
	columns["record_discovered"] = "string"
	require.NoError(t, writer.EvolveSchema(ctx, columns))
	persisted, err := cat.LoadTable(ctx, writer.table.Identifier())
	require.NoError(t, err)
	current, found := persisted.Schema().FindFieldByName("external_default")
	require.True(t, found)
	require.Equal(t, original.ID, current.ID)
	require.EqualValues(t, original.InitialDefault, current.InitialDefault)
	require.EqualValues(t, original.WriteDefault, current.WriteDefault)
	require.Equal(t, committed.Schema().IdentifierFieldIDs, persisted.Schema().IdentifierFieldIDs)
	identifier, found := persisted.Schema().FindFieldByName("external_identifier")
	require.True(t, found)
	require.True(t, identifier.Required)
	require.Equal(t, recovery, persisted.Properties()[nativeRecoveryProperty])
	require.Equal(t, map[string]any{"old": int64(17)}, nativeSourceValues(t, persisted, "external_default"))
}

func TestNativeSourceColumnsCommitRecovery(t *testing.T) {
	for _, unknown := range []bool{false, true} {
		name := "conflict"
		if unknown {
			name = "unknown-outcome"
		}
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			cfg, stream, realCatalog := nativeWriterFixture(t, types.UpdateTypeDeletionVector, true, "")
			seed := openNativeTestWriter(ctx, t, cfg, stream, realCatalog, nil, "seed", true)
			require.NoError(t, seed.Write(ctx, []types.RawRecord{nativeTestRecord("old", "r", 1, "one")}))
			require.NoError(t, seed.Close(ctx, map[string]any{"state": "original"}))
			cat := &nativeCommitFaultCatalog{Catalog: realCatalog}
			writer := openNativeTestWriter(ctx, t, cfg, stream, cat, nil, "alter", true)
			defer writer.Abort()
			cat.fault = func(ctx context.Context, id table.Identifier, reqs []table.Requirement, updates []table.Update) (bool, error) {
				cat.fault = nil
				if unknown {
					_, _, err := realCatalog.CommitTable(ctx, id, reqs, updates)
					require.NoError(t, err)
					return true, context.DeadlineExceeded
				}
				fresh, err := realCatalog.LoadTable(ctx, id)
				require.NoError(t, err)
				tx := fresh.NewTransaction()
				require.NoError(t, tx.UpdateSchema(true, false).
					AddColumn([]string{"concurrent"}, apacheiceberg.PrimitiveTypes.String, "", false, nil).Commit())
				require.NoError(t, tx.SetProperties(apacheiceberg.Properties{nativeRecoveryProperty: `{"state":"concurrent-recovery"}`}))
				_, err = tx.Commit(ctx)
				require.NoError(t, err)
				return true, table.ErrCommitFailed
			}
			initial, err := nativeSourceColumnDefault("long", 42)
			require.NoError(t, err)
			err = writer.AlterSourceColumns(ctx, []*nativeSourceColumn{{Key: "added", IceType: "long", InitialDefault: initial.Val}}, nil)
			if unknown {
				require.ErrorContains(t, err, "outcome is unknown")
				require.ErrorContains(t, writer.Write(ctx, []types.RawRecord{nativeTestRecord("unsafe", "r", 2, "one")}), "outcome is unknown")
			} else {
				require.NoError(t, err)
			}
			persisted, err := realCatalog.LoadTable(ctx, writer.table.Identifier())
			require.NoError(t, err)
			added, found := persisted.Schema().FindFieldByName("added")
			require.True(t, found)
			require.Nil(t, added.WriteDefault)
			require.Equal(t, map[string]any{"old": int64(42)}, nativeSourceValues(t, persisted, "added"))
			if unknown {
				require.Contains(t, persisted.Properties()[nativeRecoveryProperty], "original")
			} else {
				concurrent, found := persisted.Schema().FindFieldByName("concurrent")
				require.True(t, found)
				require.Greater(t, added.ID, concurrent.ID)
				require.Contains(t, persisted.Properties()[nativeRecoveryProperty], "concurrent-recovery")
			}
		})
	}
}

func TestNativeSourcePrimitiveDefaultsPersistAndReadOldRows(t *testing.T) {
	for _, mode := range []types.UpdateType{types.UpdateTypeEquality, types.UpdateTypeDeletionVector} {
		t.Run(string(mode), func(t *testing.T) {
			ctx := context.Background()
			cfg, stream, cat := nativeWriterFixture(t, mode, true, "")
			seed := openNativeTestWriter(ctx, t, cfg, stream, cat, nil, "seed", true)
			require.NoError(t, seed.Write(ctx, []types.RawRecord{nativeTestRecord("old", "r", 1, "one")}))
			oldPath := seed.dataFiles[0].FilePath()
			require.NoError(t, seed.Close(ctx, nil))
			defaults := []struct {
				name    string
				iceType string
				value   any
				want    any
			}{
				{"new_int", "int", "41", int32(41)},
				{"new_long", "long", "42", int64(42)},
				{"new_float", "float", "1.25", float32(1.25)},
				{"new_double", "double", "2.5", float64(2.5)},
				{"new_bool", "boolean", "false", false},
				{"new_string", "string", "", ""},
			}
			// Upstream v0.7.0 rounds integer defaults above 2^53 and cannot
			// round-trip temporal defaults through its metadata codec.
			var added []*nativeSourceColumn
			// A stored ID avoids upstream's zero-physical-column projection bug.
			names := []string{constants.OlakeID}
			for _, column := range defaults {
				initial, err := nativeSourceColumnDefault(column.iceType, column.value)
				require.NoError(t, err)
				added = append(added, &nativeSourceColumn{Key: column.name, IceType: column.iceType, InitialDefault: initial.Val})
				names = append(names, column.name)
			}
			writer := openNativeTestWriter(ctx, t, cfg, stream, cat, nil, "alter", true)
			require.NoError(t, writer.AlterSourceColumns(ctx, added, nil))
			writer.Abort()
			// Reopen from catalog JSON rather than retaining transaction metadata.
			reader := openNativeTestWriter(ctx, t, cfg, stream, cat, nil, "read", true)
			defer reader.Abort()
			rows, err := reader.ReadRows(ctx, oldPath, []int64{0}, names)
			require.NoError(t, err)
			for _, column := range defaults {
				field, found := reader.table.Schema().FindFieldByName(column.name)
				require.True(t, found)
				require.Nil(t, field.WriteDefault)
				if mode != types.UpdateTypeDeletionVector {
					require.Nil(t, field.InitialDefault)
					require.Nil(t, rows[0][column.name], column.name)
					continue
				}
				require.Equal(t, column.want, rows[0][column.name], column.name)
			}
			scan := reader.table.Scan()
			defer scan.Close()
			_, records, err := scan.ToArrowRecords(ctx)
			require.NoError(t, err)
			count := 0
			for batch, err := range records {
				require.NoError(t, err)
				count += int(batch.NumRows())
				batch.Release()
			}
			require.Equal(t, 1, count)
		})
	}
}

func TestNativeRESTCoordinatorSchemaMutationsPublishLatestView(t *testing.T) {
	ctx := context.Background()
	cfg, stream, cat, boundary := nativeRESTWriterFixture(t, types.UpdateTypeDeletionVector, nativeTestRecord("old", "r", 1, "one"))
	discoverer := openNativeTestWriter(ctx, t, cfg, stream, cat, nil, "record-schema", true)
	source := openNativeTestWriter(ctx, t, cfg, stream, cat, nil, "source-schema", true)
	initialSchema := source.table.Schema().ID
	columns := discoverer.schemaMap()
	columns["discovered"] = "string"
	require.NoError(t, discoverer.EvolveSchema(ctx, columns))
	require.Equal(t, []string{http.MethodGet, http.MethodGet, http.MethodPost}, boundary.tableRequests())
	discovered, found := discoverer.table.Schema().FindFieldByName("discovered")
	require.True(t, found)
	initial, err := nativeSourceColumnDefault("long", 42)
	require.NoError(t, err)
	// This writer predates discovery; its mutation must use the shared result,
	// not replace it with its own older table view or issue a second metadata GET.
	require.Equal(t, initialSchema, source.table.Schema().ID)
	require.NoError(t, source.AlterSourceColumns(ctx, []*nativeSourceColumn{{Key: "source_added", IceType: "long", InitialDefault: initial.Val}}, nil))
	require.Equal(t, []string{http.MethodGet, http.MethodGet, http.MethodPost, http.MethodPost}, boundary.tableRequests())
	current, found := source.table.Schema().FindFieldByName("discovered")
	require.True(t, found)
	require.Equal(t, discovered.ID, current.ID)
	added, found := source.table.Schema().FindFieldByName("source_added")
	require.True(t, found)
	require.Greater(t, added.ID, discovered.ID)
	require.Nil(t, added.WriteDefault)
	require.Equal(t, map[string]any{"old": int64(42)}, nativeSourceValues(t, source.table, "source_added"))

	reader := openNativeTestWriter(ctx, t, cfg, stream, cat, nil, "after-schema", true)
	defer reader.Abort()
	require.Equal(t, source.table.MetadataLocation(), reader.table.MetadataLocation())
	require.Equal(t, source.table.Schema().ID, reader.table.Schema().ID)
	require.Equal(t, []string{http.MethodGet, http.MethodGet, http.MethodPost, http.MethodPost, http.MethodGet}, boundary.tableRequests())
	require.Equal(t, "string", reader.schemaMap()["discovered"])
	require.Equal(t, "long", reader.schemaMap()["source_added"])
	require.NoError(t, discoverer.Close(ctx, map[string]any{"state": `{"schema_seen":1}`}))
	require.Equal(t, source.table.Schema().ID, discoverer.table.Schema().ID, "state-only commit must not roll back a later schema mutation")
	require.Equal(t, []string{http.MethodGet, http.MethodGet, http.MethodPost, http.MethodPost, http.MethodGet, http.MethodPost}, boundary.tableRequests())
	require.Equal(t, map[string]any{"old": int64(42)}, nativeSourceValues(t, discoverer.table, "source_added"))
	source.Abort()
}

func TestNativeCoordinatorSetupObservesExternalSchemaReplacement(t *testing.T) {
	ctx := context.Background()
	cfg, stream, cat := nativeWriterFixture(t, types.UpdateTypeEquality, true, "")
	seed := openNativeTestWriter(ctx, t, cfg, stream, cat, nil, "before-admin-ddl", true)
	require.NoError(t, seed.Write(ctx, []types.RawRecord{nativeTestRecord("seed", "r", 1, "one")}))
	require.NoError(t, seed.Close(ctx, nil))
	original, found := seed.table.Schema().FindFieldByName("value")
	require.True(t, found)
	// A separate engine changes only schema; the main snapshot does not advance.
	admin := cat.(*nativeCatalog).Catalog
	current, err := admin.LoadTable(ctx, seed.table.Identifier())
	require.NoError(t, err)
	tx := current.NewTransaction()
	require.NoError(t, tx.UpdateSchema(true, false).DeleteColumn([]string{"value"}).Commit())
	dropped, err := tx.Commit(ctx)
	require.NoError(t, err)
	tx = dropped.NewTransaction()
	require.NoError(t, tx.UpdateSchema(true, false).AddColumn([]string{"value"}, apacheiceberg.PrimitiveTypes.Int64, "", false, nil).Commit())
	replaced, err := tx.Commit(ctx)
	require.NoError(t, err)
	field, found := replaced.Schema().FindFieldByName("value")
	require.True(t, found)
	require.Greater(t, field.ID, original.ID)
	require.Equal(t, nativeSnapshotID(seed.table), nativeSnapshotID(replaced))
	after := openNativeTestWriter(ctx, t, cfg, stream, cat, nil, "after-admin-ddl", true)
	require.NoError(t, after.Write(ctx, []types.RawRecord{nativeTestRecord("after", "r", 42, "one")}))
	require.NoError(t, after.Close(ctx, nil))
	persisted, err := admin.LoadTable(ctx, seed.table.Identifier())
	require.NoError(t, err)
	require.Equal(t, map[string]any{"seed": nil, "after": int64(42)}, nativeSourceValues(t, persisted, "value"))
}
