// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package iceberg_test

import (
	"encoding/json"
	"fmt"
	"math"
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/stretchr/testify/require"
)

func TestSchemaIntegerDefaultsPersistExactly(t *testing.T) {
	const beyondFloatPrecision int64 = 1<<53 + 1
	tests := []struct {
		name    string
		typ     iceberg.Type
		initial any
		write   any
	}{
		{"long beyond float precision", iceberg.Int64Type{}, beyondFloatPrecision, -beyondFloatPrecision},
		{"long maximum", iceberg.Int64Type{}, int64(math.MaxInt64), int64(math.MinInt64)},
		{"long minimum", iceberg.Int64Type{}, int64(math.MinInt64), int64(math.MaxInt64)},
		{"small int", iceberg.Int32Type{}, int32(7), int32(-9)},
		{"int limits", iceberg.Int32Type{}, int32(math.MaxInt32), int32(math.MinInt32)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			schema := iceberg.NewSchema(7, iceberg.NestedField{
				ID: 11, Name: "value", Type: tt.typ, Required: true,
				InitialDefault: tt.initial, WriteDefault: tt.write,
			})

			for range 2 {
				persisted, err := json.Marshal(schema)
				require.NoError(t, err)
				var wire struct {
					Fields []struct {
						Initial json.RawMessage `json:"initial-default"`
						Write   json.RawMessage `json:"write-default"`
					} `json:"fields"`
				}
				require.NoError(t, json.Unmarshal(persisted, &wire))
				require.Len(t, wire.Fields, 1)
				require.Equal(t, fmt.Sprint(tt.initial), string(wire.Fields[0].Initial))
				require.Equal(t, fmt.Sprint(tt.write), string(wire.Fields[0].Write))

				var restored iceberg.Schema
				require.NoError(t, json.Unmarshal(persisted, &restored))
				field := restored.Field(0)
				require.Equal(t, 11, field.ID)
				require.Equal(t, "value", field.Name)
				require.True(t, field.Required)
				require.True(t, tt.typ.Equals(field.Type))
				require.Equal(t, tt.initial, field.InitialDefault)
				require.Equal(t, tt.write, field.WriteDefault)
				schema = &restored
			}
		})
	}
}

func TestSchemaTemporalDefaultsUseCanonicalISOJSON(t *testing.T) {
	tests := []struct {
		name      string
		typ       string
		input     string
		canonical string
		expected  any
	}{
		{"date", "date", "2024-01-02", "2024-01-02", iceberg.Date(19724)},
		{"negative date", "date", "1969-12-31", "1969-12-31", iceberg.Date(-1)},
		{"time", "time", "12:34:56.123456", "12:34:56.123456", iceberg.Time(45296123456)},
		{"midnight", "time", "00:00:00", "00:00:00", iceberg.Time(0)},
		{"timestamp", "timestamp", "2024-01-02T12:34:56.123456", "2024-01-02T12:34:56.123456", iceberg.Timestamp(1704198896123456)},
		{"negative timestamp", "timestamp", "1969-12-31T23:59:59.999999", "1969-12-31T23:59:59.999999", iceberg.Timestamp(-1)},
		{"timestamptz offset", "timestamptz", "2024-01-02T12:34:56.123456+05:30", "2024-01-02T07:04:56.123456+00:00", iceberg.Timestamp(1704179096123456)},
		{"negative timestamptz", "timestamptz", "1969-12-31T23:59:59.999999Z", "1969-12-31T23:59:59.999999+00:00", iceberg.Timestamp(-1)},
		{"timestamp ns", "timestamp_ns", "2024-01-02T12:34:56.123456789", "2024-01-02T12:34:56.123456789", iceberg.TimestampNano(1704198896123456789)},
		{"negative timestamp ns", "timestamp_ns", "1969-12-31T23:59:59.999999999", "1969-12-31T23:59:59.999999999", iceberg.TimestampNano(-1)},
		{"timestamp ns maximum", "timestamp_ns", "2262-04-11T23:47:16.854775807", "2262-04-11T23:47:16.854775807", iceberg.TimestampNano(math.MaxInt64)},
		{"timestamp ns minimum", "timestamp_ns", "1677-09-21T00:12:43.145224192", "1677-09-21T00:12:43.145224192", iceberg.TimestampNano(math.MinInt64)},
		{"timestamptz ns offset", "timestamptz_ns", "2024-01-02T12:34:56.123456789+05:30", "2024-01-02T07:04:56.123456789+00:00", iceberg.TimestampNano(1704179096123456789)},
		{"negative timestamptz ns", "timestamptz_ns", "1969-12-31T23:59:59.999999999Z", "1969-12-31T23:59:59.999999999+00:00", iceberg.TimestampNano(-1)},
	}

	for _, tt := range tests {
		for _, representation := range []string{"ISO", "legacy epoch"} {
			t.Run(tt.name+"/"+representation, func(t *testing.T) {
				token := fmt.Sprintf("%q", tt.input)
				if representation == "legacy epoch" {
					token = fmt.Sprint(tt.expected)
				}
				persisted := []byte(fmt.Sprintf(`{"type":"struct","schema-id":0,"fields":[{"id":1,"name":"value","type":%q,"required":false,"initial-default":%s,"write-default":%s}]}`, tt.typ, token, token))
				for range 2 {
					var schema iceberg.Schema
					require.NoError(t, json.Unmarshal(persisted, &schema))
					require.Equal(t, tt.expected, schema.Field(0).InitialDefault)
					require.Equal(t, tt.expected, schema.Field(0).WriteDefault)
					var err error
					persisted, err = json.Marshal(&schema)
					require.NoError(t, err)
					var wire struct {
						Fields []struct {
							Initial string `json:"initial-default"`
							Write   string `json:"write-default"`
						} `json:"fields"`
					}
					require.NoError(t, json.Unmarshal(persisted, &wire))
					require.Equal(t, tt.canonical, wire.Fields[0].Initial)
					require.Equal(t, tt.canonical, wire.Fields[0].Write)
				}
			})
		}
	}
}

func TestSchemaNestedIntegerDefaultsPersistExactly(t *testing.T) {
	const beyondFloatPrecision int64 = 1<<53 + 1
	schema := iceberg.NewSchema(3,
		iceberg.NestedField{ID: 1, Name: "record", Type: &iceberg.StructType{FieldList: []iceberg.NestedField{
			{ID: 2, Name: "positive", Type: iceberg.Int64Type{}, InitialDefault: beyondFloatPrecision, WriteDefault: int64(math.MaxInt64)},
			{ID: 3, Name: "negative", Type: iceberg.Int64Type{}, InitialDefault: int64(math.MinInt64), WriteDefault: -beyondFloatPrecision},
		}}},
		iceberg.NestedField{ID: 4, Name: "records", Type: &iceberg.ListType{
			ElementID: 5, ElementRequired: true,
			Element: &iceberg.StructType{FieldList: []iceberg.NestedField{
				{ID: 6, Name: "value", Type: iceberg.Int64Type{}, InitialDefault: int64(math.MaxInt64), WriteDefault: int64(math.MinInt64)},
			}},
		}},
		iceberg.NestedField{ID: 7, Name: "lookup", Type: &iceberg.MapType{
			KeyID: 8, KeyType: iceberg.StringType{}, ValueID: 9, ValueRequired: true,
			ValueType: &iceberg.StructType{FieldList: []iceberg.NestedField{
				{ID: 10, Name: "value", Type: iceberg.Int64Type{}, InitialDefault: -beyondFloatPrecision, WriteDefault: beyondFloatPrecision},
			}},
		}},
	)
	original, err := json.Marshal(schema)
	require.NoError(t, err)

	for range 2 {
		var restored iceberg.Schema
		require.NoError(t, json.Unmarshal(original, &restored))
		for _, id := range []int{2, 3, 6, 10} {
			expected, found := schema.FindFieldByID(id)
			require.True(t, found)
			actual, found := restored.FindFieldByID(id)
			require.True(t, found)
			require.Equal(t, expected, actual)
		}
		persisted, err := json.Marshal(&restored)
		require.NoError(t, err)
		// Compare actual JSON bytes, not JSONEq's float64 interpretation of numbers.
		require.Equal(t, string(original), string(persisted))
		original = persisted
		schema = &restored
	}
}

func TestSchemaDefaultsPreservePrimitiveAndNullValues(t *testing.T) {
	tests := []struct {
		name     string
		typ      string
		token    string
		expected any
	}{
		{"false", "boolean", "false", false},
		{"true", "boolean", "true", true},
		{"empty string", "string", `""`, ""},
		{"string", "string", `"unchanged"`, "unchanged"},
		{"float", "float", "1.25", float64(1.25)},
		{"double", "double", "-2.5", float64(-2.5)},
		{"float zero", "float", "0", float64(0)},
		{"int zero", "int", "0", int32(0)},
		{"long zero", "long", "0", int64(0)},
		{"decimal", "decimal(9, 2)", `"12.34"`, "12.34"},
		{"uuid", "uuid", `"f79c3e09-677c-4bbd-a479-512f87f77acf"`, "f79c3e09-677c-4bbd-a479-512f87f77acf"},
		{"binary", "binary", `"dead"`, "dead"},
		{"empty binary", "binary", `""`, ""},
		{"fixed", "fixed[2]", `"0001"`, "0001"},
		{"null", "long", "null", nil},
		{"omitted", "long", "", nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defaults := ""
			if tt.token != "" {
				defaults = fmt.Sprintf(`,"initial-default":%s,"write-default":%s`, tt.token, tt.token)
			}
			persisted := []byte(fmt.Sprintf(`{"type":"struct","schema-id":0,"fields":[{"id":1,"name":"value","type":%q,"required":false%s}]}`, tt.typ, defaults))
			for range 2 {
				var schema iceberg.Schema
				require.NoError(t, json.Unmarshal(persisted, &schema))
				require.Equal(t, tt.expected, schema.Field(0).InitialDefault)
				require.Equal(t, tt.expected, schema.Field(0).WriteDefault)
				var err error
				persisted, err = json.Marshal(&schema)
				require.NoError(t, err)
				var wire struct {
					Fields []map[string]json.RawMessage `json:"fields"`
				}
				require.NoError(t, json.Unmarshal(persisted, &wire))
				if tt.expected == nil {
					require.NotContains(t, wire.Fields[0], "initial-default")
					require.NotContains(t, wire.Fields[0], "write-default")
				} else {
					require.Equal(t, tt.token, string(wire.Fields[0]["initial-default"]))
					require.Equal(t, tt.token, string(wire.Fields[0]["write-default"]))
				}
			}
		})
	}
}

func TestSchemaIntegerDefaultsRejectOverflowAndFractions(t *testing.T) {
	tests := []struct {
		typ   string
		token string
	}{
		{"long", "9223372036854775808"},
		{"long", "-9223372036854775809"},
		{"long", "1.5"},
		{"long", "-1.5"},
		{"long", "9007199254740993.1"},
		{"int", "2147483648"},
		{"int", "-2147483649"},
		{"int", "1.5"},
		{"date", "2147483648"},
		{"date", "-2147483649"},
		{"date", "1.5"},
		{"time", "9223372036854775808"},
		{"time", "0.5"},
		{"timestamp", "9223372036854775808"},
		{"timestamp", "-9223372036854775809"},
		{"timestamp", "1.5"},
		{"timestamptz", "9223372036854775808"},
		{"timestamptz", "1.5"},
		{"timestamp_ns", "-9223372036854775809"},
		{"timestamp_ns", "0.5"},
		{"timestamptz_ns", "9223372036854775808"},
		{"timestamptz_ns", "0.5"},
	}

	for _, tt := range tests {
		for _, key := range []string{"initial-default", "write-default"} {
			t.Run(tt.typ+"/"+key+"/"+tt.token, func(t *testing.T) {
				persisted := []byte(fmt.Sprintf(`{"type":"struct","schema-id":0,"fields":[{"id":1,"name":"value","type":%q,"required":false,%q:%s}]}`, tt.typ, key, tt.token))
				var schema iceberg.Schema
				err := json.Unmarshal(persisted, &schema)
				require.ErrorIs(t, err, iceberg.ErrInvalidSchema)
				require.ErrorContains(t, err, key)
			})
		}
	}
}

func TestNestedFieldInvalidDefaultPreservesState(t *testing.T) {
	for _, key := range []string{"initial-default", "write-default"} {
		t.Run(key, func(t *testing.T) {
			field := iceberg.NestedField{
				ID: 7, Name: "old", Type: iceberg.Int64Type{}, Required: true,
				InitialDefault: int64(math.MaxInt64), WriteDefault: int64(math.MinInt64),
			}
			original := field
			persisted := []byte(fmt.Sprintf(`{"id":1,"name":"new","type":"long","required":false,%q:9223372036854775808}`, key))
			require.Error(t, json.Unmarshal(persisted, &field))
			require.Equal(t, original, field)
		})
	}
}

func TestSchemaTemporalDefaultsRejectMalformedStrings(t *testing.T) {
	tests := []struct {
		typ   string
		value string
	}{
		{"date", "2024-02-30"},
		{"date", ""},
		{"time", "25:00:00"},
		{"time", "not-a-time"},
		{"timestamp", "2024-01-02 12:34:56"},
		{"timestamp", "2024-01-02T12:34:56Z"},
		{"timestamp", "2024-01-02T12:34:56.1234567"},
		{"timestamptz", "2024-01-02T12:34:56"},
		{"timestamptz", "2024-01-02T12:34:56.1234567+00:00"},
		{"timestamp_ns", "3000-01-01T00:00:00"},
		{"timestamp_ns", "2024-01-02T12:34:56.1234567891"},
		{"timestamp_ns", "2262-04-11T23:47:16.854775808"},
		{"timestamp_ns", "1677-09-21T00:12:43.145224191"},
		{"timestamptz_ns", "2024-01-02T12:34:56.123456789"},
		{"timestamptz_ns", "3000-01-01T00:00:00+00:00"},
		{"timestamptz_ns", "2024-01-02T12:34:56.1234567891+00:00"},
	}
	for _, tt := range tests {
		for _, key := range []string{"initial-default", "write-default"} {
			t.Run(tt.typ+"/"+key+"/"+tt.value, func(t *testing.T) {
				persisted := []byte(fmt.Sprintf(`{"type":"struct","schema-id":0,"fields":[{"id":1,"name":"value","type":%q,"required":false,%q:%q}]}`, tt.typ, key, tt.value))
				var schema iceberg.Schema
				err := json.Unmarshal(persisted, &schema)
				require.ErrorIs(t, err, iceberg.ErrInvalidSchema)
				require.ErrorContains(t, err, key)
			})
		}
	}
}

func TestSchemaTemporalDefaultsRejectUnrepresentableValues(t *testing.T) {
	tests := []struct {
		name  string
		typ   iceberg.Type
		value any
	}{
		{"date beyond ISO years", iceberg.DateType{}, iceberg.Date(math.MaxInt32)},
		{"date overflow", iceberg.DateType{}, int64(math.MaxInt64)},
		{"date literal overflow", iceberg.DateType{}, iceberg.Int64Literal(math.MaxInt64)},
		{"negative time", iceberg.TimeType{}, iceberg.Time(-1)},
		{"time outside day", iceberg.TimeType{}, iceberg.Time(86400000000)},
		{"timestamp beyond ISO years", iceberg.TimestampType{}, iceberg.Timestamp(math.MaxInt64)},
		{"fractional epoch", iceberg.TimestampType{}, json.Number("1.5")},
		{"floating fractional epoch", iceberg.TimestampType{}, float64(1.5)},
		{"floating overflowing epoch", iceberg.TimestampType{}, float64(math.MaxInt64)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			schema := iceberg.NewSchema(0, iceberg.NestedField{
				ID: 1, Name: "value", Type: tt.typ, InitialDefault: tt.value,
			})
			_, err := json.Marshal(schema)
			require.ErrorIs(t, err, iceberg.ErrInvalidSchema)
			require.ErrorContains(t, err, "initial-default")
		})
	}
}
