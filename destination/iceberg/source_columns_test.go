package iceberg

import (
	"testing"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/types"
	"github.com/stretchr/testify/assert"
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
