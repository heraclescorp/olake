package waljs

import (
	"math"
	"testing"

	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils/typeutils"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func testSourceColumnsFilter() ChangeFilter {
	typeMapping := map[string]types.DataType{"int4": types.Int32, "text": types.String, "timestamptz": types.TimestampMicro}
	columnType := func(typeName string) types.DataType { return typeutils.ExtractAndMapColumnType(typeName, typeMapping) }
	converter := func(value any, typeName string) (any, error) {
		if value == nil {
			return nil, typeutils.ErrNullValue
		}
		return typeutils.ReformatValue(columnType(typeName), value)
	}
	return NewChangeFilter(converter, columnType)
}

func TestRelationColumnsChanged(t *testing.T) {
	relation := func(columns ...*pglogrepl.RelationMessageColumn) *pglogrepl.RelationMessage {
		return &pglogrepl.RelationMessage{Columns: columns}
	}
	id := &pglogrepl.RelationMessageColumn{Name: "id", DataType: pgtype.Int4OID}
	note := &pglogrepl.RelationMessageColumn{Name: "note", DataType: pgtype.TextOID}

	assert.True(t, relationColumnsChanged(nil, relation(id)), "first message for the relation")
	assert.False(t, relationColumnsChanged(relation(id, note), relation(id, note)), "resent after an unrelated invalidation")
	assert.True(t, relationColumnsChanged(relation(id), relation(id, note)), "column added")
	assert.True(t, relationColumnsChanged(relation(id, note), relation(id)), "column dropped")
	assert.True(t, relationColumnsChanged(relation(id, note), relation(id, &pglogrepl.RelationMessageColumn{Name: "note", DataType: pgtype.Int4OID})), "type changed")
}

func TestSourceColumnExistingRowValue(t *testing.T) {
	filter := testSourceColumnsFilter()
	testCases := []struct {
		name        string
		catalog     *catalogColumn
		wantValue   any
		wantUnknown bool
	}{
		{name: "stored constant default", catalog: &catalogColumn{HasMissing: true, HasDefault: true, MissingValue: new("7")}, wantValue: int32(7)},
		{name: "stored NULL", catalog: &catalogColumn{HasMissing: true}},
		{name: "no default", catalog: &catalogColumn{}},
		{name: "volatile default or rewritten table", catalog: &catalogColumn{HasDefault: true}, wantUnknown: true},
		{name: "unconvertible stored value", catalog: &catalogColumn{HasMissing: true, MissingValue: new("x")}, wantUnknown: true},
		{name: "column no longer in the catalog", wantUnknown: true},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			column := filter.sourceColumn("qty", pgtype.Int4OID, -1, tc.catalog)
			assert.Equal(t, types.SourceColumn{Name: "qty", Type: types.Int32, ExistingRowValue: tc.wantValue, ExistingRowValueUnknown: tc.wantUnknown}, column)
		})
	}
}

// numericTypmodOf encodes a numeric(precision,scale) type modifier as PostgreSQL does.
func numericTypmodOf(precision, scale int32) int32 {
	return precision<<16 | scale + 4
}

func TestNumericTypmodTypeNames(t *testing.T) {
	assert.Equal(t, "decimal(10,2)", columnTypeName(pgtype.NumericOID, numericTypmodOf(10, 2)))
	assert.Equal(t, "decimal(6,4)", columnTypeName(pgtype.NumericOID, numericTypmodOf(6, 4)))
	assert.Equal(t, "decimal(16,2)", columnTypeName(pgtype.NumericOID, numericTypmodOf(16, 2)))
	assert.Equal(t, "numeric", columnTypeName(pgtype.NumericOID, -1), "unconstrained numeric")
	assert.Equal(t, "numeric", columnTypeName(pgtype.NumericOID, numericTypmodOf(50, 2)), "wider than an Iceberg decimal")
	assert.Equal(t, "int4", columnTypeName(pgtype.Int4OID, -1))
	assert.Equal(t, "timestamp", columnTypeName(pgtype.TimestampOID, -1))
	assert.Equal(t, "date", columnTypeName(pgtype.DateOID, -1))
	precision, scale, constrained := numericTypmod(numericTypmodOf(10, 2))
	assert.Equal(t, []any{10, 2, true}, []any{precision, scale, constrained})
	_, _, constrained = numericTypmod(-1)
	assert.False(t, constrained)

	changed := &pglogrepl.RelationMessage{Columns: []*pglogrepl.RelationMessageColumn{{Name: "amount", DataType: pgtype.NumericOID, TypeModifier: numericTypmodOf(16, 2)}}}
	before := &pglogrepl.RelationMessage{Columns: []*pglogrepl.RelationMessageColumn{{Name: "amount", DataType: pgtype.NumericOID, TypeModifier: numericTypmodOf(10, 2)}}}
	assert.True(t, relationColumnsChanged(before, changed), "a numeric's precision changed")
}

func TestRelationSourceColumns(t *testing.T) {
	filter := testSourceColumnsFilter()
	msg := &pglogrepl.RelationMessage{Columns: []*pglogrepl.RelationMessageColumn{
		{Name: "id", DataType: pgtype.Int4OID},
		{Name: "qty", DataType: pgtype.Int4OID},
		{Name: "note", DataType: pgtype.TextOID},
	}}
	catalog := []catalogColumn{
		{Name: "id", TypeOID: pgtype.Int4OID},
		// dropped and re-added with another type since this relation message was written
		{Name: "qty", TypeOID: pgtype.TextOID, HasMissing: true, MissingValue: new("a")},
		{Name: "total", TypeOID: pgtype.Int4OID, Generated: true},
	}

	change, err := filter.relationSourceColumns(msg, catalog)
	require.NoError(t, err)

	assert.Equal(t, types.SourceColumnsChange{
		Columns: []types.SourceColumn{
			{Name: "id", Type: types.Int32},
			{Name: "qty", Type: types.Int32, ExistingRowValueUnknown: true},
			{Name: "note", Type: types.String, ExistingRowValueUnknown: true},
		},
		KeepColumns: []string{"total"},
	}, change)
}

func TestCatalogSourceColumns(t *testing.T) {
	filter := testSourceColumnsFilter()
	change, err := filter.catalogSourceColumns([]catalogColumn{
		{Name: "id", TypeOID: pgtype.Int4OID},
		{Name: "label", TypeOID: pgtype.TextOID, HasMissing: true, HasDefault: true, MissingValue: new(`a "quoted" \ value`)},
		{Name: "total", TypeOID: pgtype.Int4OID, Generated: true},
	})
	require.NoError(t, err)

	assert.Equal(t, types.SourceColumnsChange{
		SyncStart: true,
		Columns: []types.SourceColumn{
			{Name: "id", Type: types.Int32},
			{Name: "label", Type: types.String, ExistingRowValue: `a "quoted" \ value`},
		},
		KeepColumns: []string{"total"},
	}, change)
}

func TestCatalogTypeOIDBounds(t *testing.T) {
	filter := testSourceColumnsFilter()
	for _, oid := range []int64{-1, math.MaxUint32 + 1, math.MaxInt64} {
		catalog := []catalogColumn{{Name: "invalid", TypeOID: oid}}
		_, err := filter.catalogSourceColumns(catalog)
		require.ErrorContains(t, err, "outside the PostgreSQL OID range")
		_, err = filter.relationSourceColumns(&pglogrepl.RelationMessage{}, catalog)
		require.ErrorContains(t, err, "outside the PostgreSQL OID range")
	}
	for _, oid := range []int64{0, math.MaxUint32} {
		got, err := (&catalogColumn{Name: "boundary", TypeOID: oid}).typeOID()
		require.NoError(t, err)
		assert.Equal(t, oid, int64(got))
	}
}
