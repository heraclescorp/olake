package waljs

import (
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils/typeutils"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgtype"
	"reflect"
	"testing"
)

func TestPGOutputVarcharArray(t *testing.T) {
	cases := []struct {
		name string
		text string
		want []any
	}{
		{"escaped and null elements", `{"a,b","quote\"","slash\\",NULL,"NULL",""}`, []any{"a,b", `quote"`, `slash\`, nil, "NULL", ""}},
		{"empty", `{}`, []any{}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			for _, format := range []int16{pgtype.TextFormatCode, pgtype.BinaryFormatCode} {
				data := []byte(tc.text)
				wireFormat := uint8(pglogrepl.TupleDataTypeText)
				if format == pgtype.BinaryFormatCode {
					var err error
					data, err = pgtype.NewMap().Encode(pgtype.VarcharArrayOID, format, tc.want, nil)
					if err != nil {
						t.Fatal(err)
					}
					wireFormat = pglogrepl.TupleDataTypeBinary
				}
				p := &pgoutputReplicator{socket: &Socket{changeFilter: NewChangeFilter(func(v any, name string) (any, error) {
					typ := types.String
					if name == "varchar[]" {
						typ = types.Array
					}
					return typeutils.ReformatValue(typ, v)
				}, nil)}}
				rel := &pglogrepl.RelationMessage{Columns: []*pglogrepl.RelationMessageColumn{{Name: "values", DataType: pgtype.VarcharArrayOID}}}
				tuple := &pglogrepl.TupleData{Columns: []*pglogrepl.TupleDataColumn{{DataType: wireFormat, Data: data}}}
				got, _, _, err := p.tupleValuesToMap(rel, tuple, nil)
				if err != nil {
					t.Fatal(err)
				}
				if !reflect.DeepEqual(got["values"], tc.want) {
					t.Fatalf("format=%d got %#v want %#v", format, got["values"], tc.want)
				}
				tuple.Columns[0] = &pglogrepl.TupleDataColumn{DataType: pglogrepl.TupleDataTypeNull}
				got, _, _, err = p.tupleValuesToMap(rel, tuple, nil)
				if err != nil || got["values"] != nil {
					t.Fatalf("NULL array got %#v err %v", got, err)
				}
			}
		})
	}
}
