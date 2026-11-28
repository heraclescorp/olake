package driver

import (
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/pkg/jdbc"
	"github.com/datazip-inc/olake/types"
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

// Constrained numerics convert exactly to their decimal, unconstrained ones to a double, dates and
// timestamps without time zone keep their wall clock, and timestamptz is unchanged.
func TestPostgresSourceTypeFidelity(t *testing.T) {
	p := &Postgres{typeMapping: pgTypeToDataTypes()}
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	mock.ExpectQuery("SELECT").WillReturnRows(sqlmock.NewRowsWithColumnDefinition(
		sqlmock.NewColumn("amount").OfType("NUMERIC", "").WithPrecisionAndScale(16, 2),
		sqlmock.NewColumn("ratio").OfType("NUMERIC", ""),
		sqlmock.NewColumn("expires_on").OfType("DATE", time.Time{}),
		sqlmock.NewColumn("created_at").OfType("TIMESTAMP", time.Time{}),
		sqlmock.NewColumn("seen_at").OfType("TIMESTAMPTZ", time.Time{}),
	).AddRow("48.10", "48.10", time.Date(2026, 1, 31, 0, 0, 0, 0, time.UTC), time.Date(2026, 1, 31, 23, 30, 0, 0, time.UTC), time.Date(2026, 1, 31, 18, 0, 0, 0, time.UTC)))
	rows, err := db.Query("SELECT")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	if !rows.Next() {
		t.Fatal("no row")
	}
	got := map[string]any{}
	if _, err := jdbc.MapScan(rows, got, p.dataTypeConverter, pgColumnSizer); err != nil {
		t.Fatal(err)
	}
	amount, ok := got["amount"].(typeutils.Decimal)
	if !ok || amount.String() != "48.10" || amount.Type() != types.Decimal.Of(16, 2) {
		t.Fatalf("amount = %#v, want exact decimal(16,2) 48.10", got["amount"])
	}
	if got["ratio"] != 48.1 {
		t.Fatalf("unconstrained numeric = %#v, want double 48.1", got["ratio"])
	}
	if got["expires_on"] != (typeutils.Date{Time: time.Date(2026, 1, 31, 0, 0, 0, 0, time.UTC)}) {
		t.Fatalf("date = %#v", got["expires_on"])
	}
	if got["created_at"] != (typeutils.TimestampNTZ{Time: time.Date(2026, 1, 31, 23, 30, 0, 0, time.UTC)}) {
		t.Fatalf("timestamp = %#v", got["created_at"])
	}
	if got["seen_at"] != time.Date(2026, 1, 31, 18, 0, 0, 0, time.UTC) {
		t.Fatalf("timestamptz = %#v", got["seen_at"])
	}

	// pgoutput sends text, named by the relation's type modifier
	for _, tc := range []struct {
		typeName string
		value    string
		want     any
	}{
		{"decimal(6,4)", "0.0125", "0.0125"},
		{"numeric", "0.30", 0.3},
		{"date", "2026-01-31", typeutils.Date{Time: time.Date(2026, 1, 31, 0, 0, 0, 0, time.UTC)}},
		{"timestamp", "2026-01-31 23:30:00", typeutils.TimestampNTZ{Time: time.Date(2026, 1, 31, 23, 30, 0, 0, time.UTC)}},
	} {
		value, err := p.dataTypeConverter(tc.value, tc.typeName)
		if err != nil {
			t.Fatal(err)
		}
		if d, isDecimal := value.(typeutils.Decimal); isDecimal {
			value = d.String()
		}
		if value != tc.want {
			t.Fatalf("%s %q = %#v, want %#v", tc.typeName, tc.value, value, tc.want)
		}
	}

	precision, scale, numeric, text := int64(16), int64(2), "numeric", "text"
	if dataType, ok := (ColumnDetails{DataType: &numeric, NumericPrecision: &precision, NumericScale: &scale}).decimalType(); !ok || dataType != types.Decimal.Of(16, 2) {
		t.Fatalf("discovered %s, %v; want decimal(16,2)", dataType, ok)
	}
	if _, ok := (ColumnDetails{DataType: &numeric}).decimalType(); ok {
		t.Fatal("an unconstrained numeric has no decimal type")
	}
	if _, ok := (ColumnDetails{DataType: &text, NumericPrecision: &precision, NumericScale: &scale}).decimalType(); ok {
		t.Fatal("only numeric columns are decimals")
	}
	for name, want := range map[string]types.DataType{"date": types.Date, "timestamp": types.TimestampNTZ, "timestamp without time zone": types.TimestampNTZ, "timestamptz": types.Timestamp, "timestamp with time zone": types.Timestamp, "numeric": types.Float64} {
		if got := p.typeMapping[name]; got != want {
			t.Fatalf("%s maps to %s, want %s", name, got, want)
		}
	}
}
