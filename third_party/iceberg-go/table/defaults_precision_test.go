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

package table

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/arrow/scalar"
	"github.com/apache/iceberg-go"
	"github.com/stretchr/testify/require"
)

func TestIntegerDefaultScalarAndArrayPrecision(t *testing.T) {
	cases := []struct {
		name   string
		typ    iceberg.Type
		dt     arrow.DataType
		values []int64
		typed  func(int64) any
		want   func(int64) scalar.Scalar
	}{
		{
			name: "int", typ: iceberg.PrimitiveTypes.Int32, dt: arrow.PrimitiveTypes.Int32,
			values: []int64{math.MaxInt32, math.MinInt32},
			typed:  func(n int64) any { return int32(n) },
			want:   func(n int64) scalar.Scalar { return scalar.NewInt32Scalar(int32(n)) },
		},
		{
			name: "long", typ: iceberg.PrimitiveTypes.Int64, dt: arrow.PrimitiveTypes.Int64,
			typed: func(n int64) any { return n },
			want:  func(n int64) scalar.Scalar { return scalar.NewInt64Scalar(n) },
		},
		{
			name: "date", typ: iceberg.PrimitiveTypes.Date, dt: arrow.FixedWidthTypes.Date32,
			values: []int64{math.MaxInt32, math.MinInt32},
			typed:  func(n int64) any { return iceberg.Date(n) },
			want:   func(n int64) scalar.Scalar { return scalar.NewDate32Scalar(arrow.Date32(n)) },
		},
		{
			name: "time", typ: iceberg.PrimitiveTypes.Time, dt: arrow.FixedWidthTypes.Time64us,
			typed: func(n int64) any { return iceberg.Time(n) },
			want: func(n int64) scalar.Scalar {
				return scalar.NewTime64Scalar(arrow.Time64(n), arrow.FixedWidthTypes.Time64us)
			},
		},
		{
			name: "timestamp", typ: iceberg.PrimitiveTypes.Timestamp, dt: &arrow.TimestampType{Unit: arrow.Microsecond},
			typed: func(n int64) any { return iceberg.Timestamp(n) },
			want: func(n int64) scalar.Scalar {
				return scalar.NewTimestampScalar(arrow.Timestamp(n), &arrow.TimestampType{Unit: arrow.Microsecond})
			},
		},
		{
			name: "timestamptz", typ: iceberg.PrimitiveTypes.TimestampTz, dt: arrow.FixedWidthTypes.Timestamp_us,
			typed: func(n int64) any { return iceberg.Timestamp(n) },
			want: func(n int64) scalar.Scalar {
				return scalar.NewTimestampScalar(arrow.Timestamp(n), arrow.FixedWidthTypes.Timestamp_us)
			},
		},
		{
			name: "timestamp_ns", typ: iceberg.PrimitiveTypes.TimestampNs, dt: &arrow.TimestampType{Unit: arrow.Nanosecond},
			typed: func(n int64) any { return iceberg.TimestampNano(n) },
			want: func(n int64) scalar.Scalar {
				return scalar.NewTimestampScalar(arrow.Timestamp(n), &arrow.TimestampType{Unit: arrow.Nanosecond})
			},
		},
		{
			name: "timestamptz_ns", typ: iceberg.PrimitiveTypes.TimestampTzNs, dt: arrow.FixedWidthTypes.Timestamp_ns,
			typed: func(n int64) any { return iceberg.TimestampNano(n) },
			want: func(n int64) scalar.Scalar {
				return scalar.NewTimestampScalar(arrow.Timestamp(n), arrow.FixedWidthTypes.Timestamp_ns)
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
			defer mem.AssertSize(t, 0)
			values := tc.values
			if values == nil {
				values = []int64{1<<53 + 1, math.MaxInt64, math.MinInt64}
			}
			for _, n := range values {
				for _, input := range []any{tc.typed(n), json.Number(strconv.FormatInt(n, 10)), n} {
					t.Run(fmt.Sprintf("%d/%T", n, input), func(t *testing.T) {
						require.Equal(t, tc.want(n), defaultToScalar(input, tc.typ, tc.dt))
						arr := defaultToArray(input, tc.typ, tc.dt, 3, mem)
						defer arr.Release()
						require.True(t, arrow.TypeEqual(tc.dt, arr.DataType()))
						for i := range arr.Len() {
							got, err := scalar.GetScalar(arr, i)
							require.NoError(t, err)
							require.Equal(t, tc.want(n), got)
						}
					})
				}
			}
		})
	}
}

func TestNumericDefaultRejectsInvalidIntegers(t *testing.T) {
	cases := []struct {
		name string
		typ  iceberg.Type
		dt   arrow.DataType
		bits int
	}{
		{"int", iceberg.PrimitiveTypes.Int32, arrow.PrimitiveTypes.Int32, 32},
		{"long", iceberg.PrimitiveTypes.Int64, arrow.PrimitiveTypes.Int64, 64},
		{"date", iceberg.PrimitiveTypes.Date, arrow.FixedWidthTypes.Date32, 32},
		{"time", iceberg.PrimitiveTypes.Time, arrow.FixedWidthTypes.Time64us, 64},
		{"timestamp", iceberg.PrimitiveTypes.Timestamp, &arrow.TimestampType{Unit: arrow.Microsecond}, 64},
		{"timestamp_ns", iceberg.PrimitiveTypes.TimestampNs, &arrow.TimestampType{Unit: arrow.Nanosecond}, 64},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			over, under := json.Number("9223372036854775808"), json.Number("-9223372036854775809")
			if tc.bits == 32 {
				over, under = json.Number("2147483648"), json.Number("-2147483649")
			}
			invalid := []any{
				over, under, json.Number("1.5"), json.Number("1e3"), json.Number("invalid"),
				float64(1.5), float32(1.5), math.NaN(), math.Inf(1), math.Inf(-1),
				math.Ldexp(1, tc.bits-1), -math.Ldexp(1, tc.bits),
			}
			if tc.bits == 32 {
				invalid = append(invalid, int64(math.MaxInt32)+1, int64(math.MinInt32)-1)
			}
			for _, input := range invalid {
				require.Panics(t, func() { defaultToScalar(input, tc.typ, tc.dt) }, "%T(%v)", input, input)
			}
			// Legacy float64 defaults remain supported when exactly integral and in range.
			require.NotPanics(t, func() { defaultToScalar(float64(42), tc.typ, tc.dt) })
			require.NotPanics(t, func() { defaultToScalar(-math.Ldexp(1, tc.bits-1), tc.typ, tc.dt) })
		})
	}
}

func TestFloatingDefaultScalarAndArray(t *testing.T) {
	for _, input := range []any{json.Number("1.25"), float32(1.25), float64(1.25)} {
		for _, typ := range []iceberg.Type{iceberg.PrimitiveTypes.Float32, iceberg.PrimitiveTypes.Float64} {
			t.Run(fmt.Sprintf("%s/%T", typ, input), func(t *testing.T) {
				var dt arrow.DataType = arrow.PrimitiveTypes.Float64
				var want scalar.Scalar = scalar.NewFloat64Scalar(1.25)
				if typ.Equals(iceberg.PrimitiveTypes.Float32) {
					dt, want = arrow.PrimitiveTypes.Float32, scalar.NewFloat32Scalar(1.25)
				}
				require.Equal(t, want, defaultToScalar(input, typ, dt))
				mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
				defer mem.AssertSize(t, 0)
				arr := defaultToArray(input, typ, dt, 2, mem)
				defer arr.Release()
				for i := range arr.Len() {
					got, err := scalar.GetScalar(arr, i)
					require.NoError(t, err)
					require.Equal(t, want, got)
				}
			})
		}
	}
	require.Equal(t, float64(125), numericDefault[float64](json.Number("1.25e2")))
	require.Equal(t, float32(7), numericDefault[float32](int64(7)))
	require.Panics(t, func() { numericDefault[float32](json.Number("1e40")) })
	require.Panics(t, func() { numericDefault[float64](json.Number("1e400")) })
}

func TestHistoricalLongDefaultsProjectExactly(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)
	ctx := compute.WithAllocator(context.Background(), mem)
	fileSchema := iceberg.NewSchema(0, iceberg.NestedField{
		ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int32, Required: true,
	})
	inputSchema := arrow.NewSchema([]arrow.Field{{
		Name: "id", Type: arrow.PrimitiveTypes.Int32,
		Metadata: arrow.MetadataFrom(map[string]string{ArrowParquetFieldIDKey: "1"}),
	}}, nil)
	builder := array.NewRecordBuilder(mem, inputSchema)
	defer builder.Release()
	builder.Field(0).(*array.Int32Builder).AppendValues([]int32{1, 2, 3}, nil)
	batch := builder.NewRecordBatch()
	defer batch.Release()

	for _, n := range []int64{1<<53 + 1, math.MaxInt64, math.MinInt64} {
		t.Run(strconv.FormatInt(n, 10), func(t *testing.T) {
			// Persist and reload the schema, as a scan of historical files does.
			requested := iceberg.NewSchema(1,
				fileSchema.Fields()[0],
				iceberg.NestedField{
					ID: 2, Name: "historical_default", Type: iceberg.PrimitiveTypes.Int64, Required: true,
					InitialDefault: n, WriteDefault: -1,
				},
				iceberg.NestedField{
					ID: 3, Name: "write_default", Type: iceberg.PrimitiveTypes.Int64, Required: true,
					InitialDefault: -1, WriteDefault: n,
				},
			)
			encoded, err := json.Marshal(requested)
			require.NoError(t, err)
			var persisted iceberg.Schema
			require.NoError(t, json.Unmarshal(encoded, &persisted))
			for _, useWrite := range []bool{false, true} {
				out, err := ToRequestedSchema(ctx, &persisted, fileSchema, batch, SchemaOptions{
					UseWriteDefault: useWrite, IncludeFieldIDs: true,
				})
				require.NoError(t, err)
				defer out.Release()
				column := 1
				if useWrite {
					column = 2
				}
				got := out.Column(column).(*array.Int64)
				require.Equal(t, 3, got.Len())
				require.Equal(t, 0, got.NullN())
				for i := range got.Len() {
					require.Equal(t, n, got.Value(i))
				}
				field := out.Schema().Field(column)
				require.Equal(t, strconv.Itoa(column+1), field.Metadata.ToMap()[ArrowParquetFieldIDKey])
				require.Equal(t, arrow.INT64, field.Type.ID())
			}
		})
	}
}

func TestCanonicalTemporalDefaultsProjectFromSchemaJSON(t *testing.T) {
	// These are the Appendix D representations a Java writer persists, not
	// numeric defaults produced by older iceberg-go releases.
	const schemaJSON = `{
		"type": "struct", "schema-id": 1, "fields": [
			{"id": 1, "name": "id", "required": true, "type": "int"},
			{"id": 2, "name": "date_default", "required": true, "type": "date", "initial-default": "2024-01-02"},
			{"id": 3, "name": "time_default", "required": true, "type": "time", "initial-default": "12:34:56.123456"},
			{"id": 4, "name": "timestamp_default", "required": true, "type": "timestamp", "initial-default": "2024-01-02T12:34:56.123456"},
			{"id": 5, "name": "timestamptz_default", "required": true, "type": "timestamptz", "initial-default": "2024-01-02T12:34:56.123456+05:30"},
			{"id": 6, "name": "timestamp_ns_default", "required": true, "type": "timestamp_ns", "initial-default": "2024-01-02T12:34:56.123456789"},
			{"id": 7, "name": "timestamptz_ns_default", "required": true, "type": "timestamptz_ns", "initial-default": "2024-01-02T12:34:56.123456789+05:30"}
		]
	}`
	var requested iceberg.Schema
	require.NoError(t, json.Unmarshal([]byte(schemaJSON), &requested))
	local := time.Date(2024, time.January, 2, 12, 34, 56, 123456789, time.UTC)
	normalized := time.Date(2024, time.January, 2, 7, 4, 56, 123456789, time.UTC)
	expected := []struct {
		value any
		dt    arrow.DataType
	}{
		{iceberg.Date(time.Date(2024, time.January, 2, 0, 0, 0, 0, time.UTC).Unix() / 86400), arrow.FixedWidthTypes.Date32},
		{iceberg.Time((12*3600+34*60+56)*1000000 + 123456), arrow.FixedWidthTypes.Time64us},
		{iceberg.Timestamp(local.UnixMicro()), &arrow.TimestampType{Unit: arrow.Microsecond}},
		{iceberg.Timestamp(normalized.UnixMicro()), arrow.FixedWidthTypes.Timestamp_us},
		{iceberg.TimestampNano(local.UnixNano()), &arrow.TimestampType{Unit: arrow.Nanosecond}},
		{iceberg.TimestampNano(normalized.UnixNano()), arrow.FixedWidthTypes.Timestamp_ns},
	}
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)
	ctx := compute.WithAllocator(context.Background(), mem)
	fileSchema := iceberg.NewSchema(0, iceberg.NestedField{
		ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int32, Required: true,
	})
	inputSchema := arrow.NewSchema([]arrow.Field{{
		Name: "id", Type: arrow.PrimitiveTypes.Int32,
		Metadata: arrow.MetadataFrom(map[string]string{ArrowParquetFieldIDKey: "1"}),
	}}, nil)
	builder := array.NewRecordBuilder(mem, inputSchema)
	defer builder.Release()
	builder.Field(0).(*array.Int32Builder).AppendValues([]int32{1, 2, 3}, nil)
	batch := builder.NewRecordBatch()
	defer batch.Release()
	out, err := ToRequestedSchema(ctx, &requested, fileSchema, batch, SchemaOptions{IncludeFieldIDs: true})
	require.NoError(t, err)
	defer out.Release()
	require.EqualValues(t, 7, out.NumCols())
	for i, want := range expected {
		column := i + 1
		require.Equal(t, want.value, requested.Field(column).InitialDefault)
		col := out.Column(column)
		require.True(t, arrow.TypeEqual(want.dt, col.DataType()), "column %d: %s", column, col.DataType())
		require.Equal(t, 3, col.Len())
		require.Equal(t, 0, col.NullN())
		require.Equal(t, requested.Field(column).Name, out.Schema().Field(column).Name)
		require.Equal(t, strconv.Itoa(column+1), out.Schema().Field(column).Metadata.ToMap()[ArrowParquetFieldIDKey])
		for row := range col.Len() {
			switch value := want.value.(type) {
			case iceberg.Date:
				require.Equal(t, arrow.Date32(value), col.(*array.Date32).Value(row))
			case iceberg.Time:
				require.Equal(t, arrow.Time64(value), col.(*array.Time64).Value(row))
			case iceberg.Timestamp:
				require.Equal(t, arrow.Timestamp(value), col.(*array.Timestamp).Value(row))
			case iceberg.TimestampNano:
				require.Equal(t, arrow.Timestamp(value), col.(*array.Timestamp).Value(row))
			default:
				t.Fatalf("unexpected temporal default type %T", value)
			}
		}
	}
}

func TestDefaultOnlyProjectionPreservesRowCount(t *testing.T) {
	const exactDefault int64 = 1<<53 + 1
	for _, rows := range []int{0, 3} {
		t.Run(strconv.Itoa(rows), func(t *testing.T) {
			mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
			defer mem.AssertSize(t, 0)
			ctx := compute.WithAllocator(context.Background(), mem)
			fileSchema := iceberg.NewSchema(0, iceberg.NestedField{
				ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64,
			})
			inputSchema := arrow.NewSchema([]arrow.Field{{
				Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: true,
				Metadata: arrow.MetadataFrom(map[string]string{ArrowParquetFieldIDKey: "1"}),
			}}, nil)
			builder := array.NewRecordBuilder(mem, inputSchema)
			defer builder.Release()
			for i := range rows {
				builder.Field(0).(*array.Int64Builder).Append(int64(i))
			}
			batch := builder.NewRecordBatch()
			defer batch.Release()

			// A scan that needs only added fields first selects no physical columns.
			// Keep the count on this zero-column batch without reading an anchor.
			empty, err := ToRequestedSchema(ctx, iceberg.NewSchema(1), fileSchema, batch, SchemaOptions{})
			require.NoError(t, err)
			defer empty.Release()
			require.EqualValues(t, 0, empty.NumCols())
			require.EqualValues(t, rows, empty.NumRows())

			requested := iceberg.NewSchema(2,
				iceberg.NestedField{
					ID: 2, Name: "added", Type: iceberg.PrimitiveTypes.Int64, Required: true,
					InitialDefault: exactDefault, WriteDefault: int64(7),
				},
				iceberg.NestedField{
					ID: 3, Name: "added_null", Type: iceberg.PrimitiveTypes.Int64,
					WriteDefault: int64(9),
				},
			)
			out, err := ToRequestedSchema(ctx, requested, fileSchema, empty, SchemaOptions{IncludeFieldIDs: true})
			require.NoError(t, err)
			defer out.Release()
			require.EqualValues(t, rows, out.NumRows())
			require.EqualValues(t, 2, out.NumCols())
			got := out.Column(0).(*array.Int64)
			require.Equal(t, 0, got.NullN())
			require.Equal(t, rows, out.Column(1).NullN(), "a write default must not fill historical NULL")
			for row := range rows {
				require.Equal(t, exactDefault, got.Value(row))
				require.True(t, out.Column(1).IsNull(row))
			}
			// NULL-only projections must also retain a count with no physical input.
			nullOnly, err := ToRequestedSchema(ctx, iceberg.NewSchema(3, requested.Field(1)), fileSchema, empty, SchemaOptions{})
			require.NoError(t, err)
			defer nullOnly.Release()
			require.EqualValues(t, rows, nullOnly.NumRows())
			require.Equal(t, rows, nullOnly.Column(0).NullN())
		})
	}
}

func TestProjectionPreservesExplicitNullAndPhysicalOffsets(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)
	ctx := compute.WithAllocator(context.Background(), mem)
	fileSchema := iceberg.NewSchema(0, iceberg.NestedField{
		ID: 1, Name: "value", Type: iceberg.PrimitiveTypes.Int64,
	})
	requested := iceberg.NewSchema(1, iceberg.NestedField{
		ID: 1, Name: "value", Type: iceberg.PrimitiveTypes.Int64,
		InitialDefault: int64(1<<53 + 1), WriteDefault: int64(7),
	})
	inputSchema := arrow.NewSchema([]arrow.Field{{
		Name: "value", Type: arrow.PrimitiveTypes.Int64, Nullable: true,
		Metadata: arrow.MetadataFrom(map[string]string{ArrowParquetFieldIDKey: "1"}),
	}}, nil)
	builder := array.NewRecordBuilder(mem, inputSchema)
	builder.Field(0).(*array.Int64Builder).AppendValues([]int64{111, 0, 333, 444, 555}, []bool{true, false, true, true, true})
	batch := builder.NewRecordBatch()
	builder.Release()
	sliced := batch.NewSlice(1, 4)
	batch.Release()
	readOut, err := ToRequestedSchema(ctx, requested, fileSchema, sliced, SchemaOptions{})
	require.NoError(t, err)
	defer readOut.Release()
	writeOut, err := ToRequestedSchema(ctx, requested, fileSchema, sliced, SchemaOptions{UseWriteDefault: true})
	require.NoError(t, err)
	defer writeOut.Release()
	sliced.Release()
	for _, out := range []arrow.RecordBatch{readOut, writeOut} {
		require.EqualValues(t, 3, out.NumRows())
		got := out.Column(0).(*array.Int64)
		require.Equal(t, 1, got.NullN())
		require.True(t, got.IsNull(0), "an explicit NULL must not be replaced by either default")
		require.Equal(t, int64(333), got.Value(1))
		require.Equal(t, int64(444), got.Value(2))
	}
}
