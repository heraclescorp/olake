package waljs

import (
	"context"
	"errors"
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
				socket: &Socket{changeFilter: NewChangeFilter(converter, nil, stream)},
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

func TestPGOutputUnchangedToast(t *testing.T) {
	unchanged := &pglogrepl.TupleDataColumn{DataType: pglogrepl.TupleDataTypeToast}
	newTuple := &pglogrepl.TupleData{Columns: []*pglogrepl.TupleDataColumn{
		{DataType: pglogrepl.TupleDataTypeText, Data: []byte("1")},
		{DataType: pglogrepl.TupleDataTypeText, Data: []byte("approved")},
		unchanged,
	}}
	fullOldTuple := &pglogrepl.TupleData{Columns: []*pglogrepl.TupleDataColumn{
		{DataType: pglogrepl.TupleDataTypeText, Data: []byte("1")},
		{DataType: pglogrepl.TupleDataTypeText, Data: []byte("pending")},
		{DataType: pglogrepl.TupleDataTypeText, Data: []byte(`{"large":"document"}`)},
	}}

	tests := []struct {
		name            string
		preserve        bool
		oldTuple        *pglogrepl.TupleData
		wantErr         bool
		wantDoc         any
		wantUnavailable []string
	}{
		{name: "default identity without preserve fails instead of writing the placeholder", wantErr: true},
		{name: "default identity with preserve marks the column for the destination", preserve: true,
			wantDoc: constants.UnavailableValue, wantUnavailable: []string{"doc"}},
		{name: "full identity recovers the value from the old tuple", oldTuple: fullOldTuple,
			wantDoc: `{"large":"document"}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stream := &types.ConfiguredStream{Stream: types.NewStream("toast_table", "public", nil)}
			p := &pgoutputReplicator{
				socket:              &Socket{changeFilter: NewChangeFilter(func(value any, _ string) (any, error) { return value, nil }, nil, stream)},
				preserveToastValues: tt.preserve,
				relationIDToMsgMap: map[uint32]*pglogrepl.RelationMessage{
					1: {Namespace: "public", RelationName: "toast_table", ReplicaIdentity: 'd', Columns: []*pglogrepl.RelationMessageColumn{
						{Name: "id", DataType: pgtype.Int4OID},
						{Name: "status", DataType: pgtype.TextOID},
						{Name: "doc", DataType: pgtype.JSONBOID},
					}},
				},
			}
			var changes []abstract.CDCChange
			err := p.emitUpdate(context.Background(), &pglogrepl.UpdateMessage{RelationID: 1, NewTuple: newTuple, OldTuple: tt.oldTuple},
				func(_ context.Context, change abstract.CDCChange) error {
					changes = append(changes, change)
					return nil
				})
			if tt.wantErr {
				if !errors.Is(err, constants.ErrNonRetryable) || len(changes) != 0 {
					t.Fatalf("got changes %v with error %v, want a non-retryable error and no change", changes, err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if len(changes) != 1 || changes[0].Data["doc"] != tt.wantDoc || !reflect.DeepEqual(changes[0].UnavailableColumns, tt.wantUnavailable) {
				t.Fatalf("got changes %#v, want doc %v and unavailable columns %v", changes, tt.wantDoc, tt.wantUnavailable)
			}
		})
	}
}
