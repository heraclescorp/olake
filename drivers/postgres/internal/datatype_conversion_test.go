package driver

import (
	"errors"
	"reflect"
	"testing"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/utils/typeutils"
)

func TestPostgresByteaConversion(t *testing.T) {
	old := constants.LoadedStateVersion
	t.Cleanup(func() { constants.LoadedStateVersion = old })

	tests := []struct {
		name    string
		version int
		value   any
		want    any
		wantErr bool
	}{
		{name: "snapshot preserves non UTF8 bytes", version: 8, value: []byte{0, 0xff, 0x80, 0x5c}, want: []byte{0, 0xff, 0x80, 0x5c}},
		{name: "snapshot does not decode raw hex text", version: 8, value: []byte(`\x4142`), want: []byte(`\x4142`)},
		{name: "pgoutput hex", version: 8, value: `\x00ff805c`, want: []byte{0, 0xff, 0x80, 0x5c}},
		{name: "wal2json hex", version: 8, value: "00ff805c", want: []byte{0, 0xff, 0x80, 0x5c}},
		{name: "empty snapshot", version: 8, value: []byte{}, want: []byte{}},
		{name: "empty pgoutput", version: 8, value: `\x`, want: []byte{}},
		{name: "empty wal2json", version: 8, value: "", want: []byte{}},
		{name: "invalid hex", version: 8, value: `\xzz`, wantErr: true},
		{name: "odd hex", version: 8, value: "abc", wantErr: true},
		{name: "legacy snapshot", version: 7, value: []byte{0, 0xff}, want: string([]byte{0, 0xff})},
		{name: "legacy pgoutput", version: 7, value: `\x00ff`, want: `\x00ff`},
		{name: "legacy wal2json", version: 0, value: "00ff", want: "00ff"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			constants.LoadedStateVersion = tt.version
			p := &Postgres{typeMapping: pgTypeToDataTypes()}
			got, err := p.dataTypeConverter(tt.value, "BYTEA")
			if tt.wantErr {
				if err == nil {
					t.Fatalf("expected invalid bytea error, got %#v", got)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("got %#v (%T), want %#v (%T)", got, got, tt.want, tt.want)
			}
		})
	}
}

func TestPostgresNullBytea(t *testing.T) {
	p := &Postgres{}
	got, err := p.dataTypeConverter(nil, "bytea")
	if got != nil || !errors.Is(err, typeutils.ErrNullValue) {
		t.Fatalf("got %#v, %v; want nil, ErrNullValue", got, err)
	}
}
