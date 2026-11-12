package iceberg

import (
	"context"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow/array"
	apacheiceberg "github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/types"
	"github.com/stretchr/testify/require"
)

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
			seed := openNativeTestWriter(t, ctx, cfg, stream, cat, nil, "seed", true)
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
			writer := openNativeTestWriter(t, ctx, cfg, stream, cat, nil, "alter", true)
			_, exists := writer.table.Schema().FindFieldByName("added")
			require.False(t, exists)
			initial, err := nativeSourceColumnDefault("int", "42")
			require.NoError(t, err)
			require.NoError(t, writer.AlterSourceColumns(ctx, []*nativeSourceColumn{
				{Key: "added", IceType: "int", InitialDefault: initial},
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
			rows, err := writer.ReadRows(ctx, oldPath, []int64{0}, []string{"added", "nullable"})
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
	seed := openNativeTestWriter(t, ctx, cfg, stream, cat, nil, "seed", true)
	require.NoError(t, seed.Write(ctx, []types.RawRecord{nativeTestRecord("old", "r", 7, "one")}))
	require.NoError(t, seed.Close(ctx, map[string]any{"state": "keep"}))
	writer := openNativeTestWriter(t, ctx, cfg, stream, cat, nil, "alter", true)
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
	require.NoError(t, writer.AlterSourceColumns(ctx, []*nativeSourceColumn{{Key: "value", IceType: "long", InitialDefault: initial}}, nil))
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
	writer := openNativeTestWriter(t, ctx, cfg, stream, cat, nil, "protected", true)
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
			require.Equal(t, tc.want, literal.Any())
			schema, err := nativeSchema(map[string]string{"default": tc.iceType}, false)
			require.NoError(t, err)
			require.True(t, literal.Type().Equals(schema.Field(0).Type))
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
	literal, err := nativeSourceColumnDefault("long", nil)
	require.NoError(t, err)
	require.Nil(t, literal)
}

func TestNativeSchemaEvolutionPreservesExistingDefaultsAndIdentifiers(t *testing.T) {
	ctx := context.Background()
	cfg, stream, cat := nativeWriterFixture(t, types.UpdateTypeDeletionVector, true, "")
	seed := openNativeTestWriter(t, ctx, cfg, stream, cat, nil, "seed", true)
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
	writer := openNativeTestWriter(t, ctx, cfg, stream, cat, nil, "evolve", true)
	defer writer.Abort()
	columns := writer.schemaMap()
	columns["record_discovered"] = "string"
	require.NoError(t, writer.EvolveSchema(ctx, columns))
	persisted, err := cat.LoadTable(ctx, writer.table.Identifier())
	require.NoError(t, err)
	current, found := persisted.Schema().FindFieldByName("external_default")
	require.True(t, found)
	require.Equal(t, original.ID, current.ID)
	require.Equal(t, original.InitialDefault, current.InitialDefault)
	require.Equal(t, original.WriteDefault, current.WriteDefault)
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
			seed := openNativeTestWriter(t, ctx, cfg, stream, realCatalog, nil, "seed", true)
			require.NoError(t, seed.Write(ctx, []types.RawRecord{nativeTestRecord("old", "r", 1, "one")}))
			require.NoError(t, seed.Close(ctx, map[string]any{"state": "original"}))
			cat := &nativeCommitFaultCatalog{Catalog: realCatalog}
			writer := openNativeTestWriter(t, ctx, cfg, stream, cat, nil, "alter", true)
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
			err = writer.AlterSourceColumns(ctx, []*nativeSourceColumn{{Key: "added", IceType: "long", InitialDefault: initial}}, nil)
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
			seed := openNativeTestWriter(t, ctx, cfg, stream, cat, nil, "seed", true)
			require.NoError(t, seed.Write(ctx, []types.RawRecord{nativeTestRecord("old", "r", 1, "one")}))
			oldPath := seed.dataFiles[0].FilePath()
			require.NoError(t, seed.Close(ctx, nil))
			stamp := time.Date(2025, 3, 8, 14, 30, 1, 123456000, time.FixedZone("source", 2*60*60))
			defaults := []struct {
				name    string
				iceType string
				value   any
				want    any
			}{
				{"new_int", "int", "41", int32(41)},
				{"new_long", "long", "9223372036854775807", int64(9223372036854775807)},
				{"new_long_min", "long", "-9223372036854775808", int64(-9223372036854775808)},
				{"new_float", "float", "1.25", float32(1.25)},
				{"new_double", "double", "2.5", float64(2.5)},
				{"new_bool", "boolean", "false", false},
				{"new_string", "string", "", ""},
				{"new_timestamp", "timestamp", stamp, nil},
				{"new_timestamptz", "timestamptz", stamp.Format(time.RFC3339Nano), nil},
			}
			var added []*nativeSourceColumn
			var names []string
			for _, column := range defaults {
				initial, err := nativeSourceColumnDefault(column.iceType, column.value)
				require.NoError(t, err)
				added = append(added, &nativeSourceColumn{Key: column.name, IceType: column.iceType, InitialDefault: initial})
				names = append(names, column.name)
			}
			writer := openNativeTestWriter(t, ctx, cfg, stream, cat, nil, "alter", true)
			require.NoError(t, writer.AlterSourceColumns(ctx, added, nil))
			writer.Abort()
			// Reopen from catalog JSON rather than retaining transaction metadata.
			reader := openNativeTestWriter(t, ctx, cfg, stream, cat, nil, "read", true)
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
				if column.want != nil {
					require.Equal(t, column.want, rows[0][column.name], column.name)
				} else {
					require.NotNil(t, rows[0][column.name], column.name)
				}
			}
			scan := reader.table.Scan()
			defer scan.Close()
			schema, records, err := scan.ToArrowRecords(ctx)
			require.NoError(t, err)
			count := 0
			for batch, err := range records {
				require.NoError(t, err)
				for _, name := range []string{"new_timestamp", "new_timestamptz"} {
					column := batch.Column(schema.FieldIndices(name)[0]).(*array.Timestamp)
					if mode == types.UpdateTypeDeletionVector {
						require.False(t, column.IsNull(0))
						require.Equal(t, stamp.UnixMicro(), int64(column.Value(0)), name)
					} else {
						require.True(t, column.IsNull(0))
					}
				}
				count += int(batch.NumRows())
				batch.Release()
			}
			require.Equal(t, 1, count)
		})
	}
}
