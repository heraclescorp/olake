package waljs

import (
	"context"
	"reflect"
	"testing"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/drivers/abstract"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
	"github.com/datazip-inc/olake/utils/typeutils"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgtype"
)

func TestPGOutputBytea(t *testing.T) {
	old := constants.LoadedStateVersion
	t.Cleanup(func() { constants.LoadedStateVersion = old })
	tests := []struct {
		name    string
		version int
		format  uint8
		data    []byte
		want    any
		wantErr bool
	}{
		{name: "hex output", version: 8, format: pglogrepl.TupleDataTypeText, data: []byte(`\x00ff805c`), want: []byte{0, 0xff, 0x80, 0x5c}},
		{name: "printable hex bytes", version: 8, format: pglogrepl.TupleDataTypeText, data: []byte(`\x34313432`), want: []byte("4142")},
		{name: "binary output", version: 8, format: pglogrepl.TupleDataTypeBinary, data: []byte{0, 0xff, 0x80, 0x5c}, want: []byte{0, 0xff, 0x80, 0x5c}},
		{name: "empty hex", version: 8, format: pglogrepl.TupleDataTypeText, data: []byte(`\x`), want: []byte{}},
		{name: "null", version: 8, format: pglogrepl.TupleDataTypeNull, data: nil, want: nil},
		{name: "invalid hex", version: 8, format: pglogrepl.TupleDataTypeText, data: []byte(`\xzz`), wantErr: true},
		{name: "legacy text", version: 7, format: pglogrepl.TupleDataTypeText, data: []byte(`\x00ff`), want: `\x00ff`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			constants.LoadedStateVersion = tt.version
			stream := &types.ConfiguredStream{Stream: types.NewStream("binary_smoke", "public", nil)}
			converter := func(value any, _ string) (any, error) {
				datatype := types.String
				if utils.IsBinarySupported() {
					datatype = types.Binary
				}
				return typeutils.ReformatValue(datatype, value)
			}
			p := &pgoutputReplicator{
				socket: &Socket{changeFilter: NewChangeFilter(converter, stream)},
				relationIDToMsgMap: map[uint32]*pglogrepl.RelationMessage{
					1: {Namespace: "public", RelationName: "binary_smoke", Columns: []*pglogrepl.RelationMessageColumn{{Name: "payload", DataType: pgtype.ByteaOID}}},
				},
			}
			message := &pglogrepl.InsertMessage{RelationID: 1, Tuple: &pglogrepl.TupleData{Columns: []*pglogrepl.TupleDataColumn{{DataType: tt.format, Data: tt.data}}}}
			var changes []abstract.CDCChange
			err := p.emitInsert(context.Background(), message, func(_ context.Context, change abstract.CDCChange) error {
				changes = append(changes, change)
				return nil
			})
			if tt.wantErr {
				if err == nil || len(changes) != 0 {
					t.Fatalf("invalid bytea emitted changes %v with error %v", changes, err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if len(changes) != 1 || !reflect.DeepEqual(changes[0].Data["payload"], tt.want) {
				t.Fatalf("got changes %#v, want payload %#v", changes, tt.want)
			}
		})
	}
}
