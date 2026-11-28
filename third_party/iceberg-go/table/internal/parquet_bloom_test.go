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

package internal_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
	"github.com/apache/iceberg-go/table/internal"
	"github.com/stretchr/testify/require"
)

func TestParquetAdaptiveBloomFilters(t *testing.T) {
	for _, source := range []struct {
		name    string
		options func(iceberg.Properties) []parquet.WriterProperty
	}{
		{"sdk-writer", func(props iceberg.Properties) []parquet.WriterProperty {
			return internal.GetFileFormat(iceberg.ParquetFile).GetWriteProperties(props).([]parquet.WriterProperty)
		}},
		{"shared-policy", table.ParquetBloomFilterWriteOptions},
	} {
		t.Run(source.name, func(t *testing.T) {
			sizes := make(map[string]int64)
			for _, tc := range []struct {
				name          string
				cap           string
				rows          int
				distinct      int
				expectedLimit int64
			}{
				{"tiny", "", 8, 4, 4096},
				{"repeated", "", 8192, 4, 4096},
				{"distinct", "", 8192, 8192, table.ParquetBloomFilterMaxBytesDefault},
				{"non-power-cap", "6000", 8192, 8192, 4096},
				{"tiny-non-power-cap", "1000", 8192, 8192, 512},
				{"tiny-power-cap", "1024", 8192, 8192, 1024},
				{"minimum-cap", "32", 64, 4, 32},
			} {
				t.Run(tc.name, func(t *testing.T) {
					props := iceberg.Properties{
						table.ParquetBloomFilterColumnEnabledKeyPrefix + ".key":      "true",
						table.ParquetBloomFilterColumnEnabledKeyPrefix + ".disabled": "false",
					}
					if tc.cap != "" {
						props[table.ParquetBloomFilterMaxBytesKey] = tc.cap
					}
					require.NoError(t, internal.ValidateParquetWriteProperties(props))
					sizes[tc.name] = serializedAdaptiveBloomSize(t, source.options(props), tc.rows, tc.distinct)
					require.LessOrEqual(t, sizes[tc.name], tc.expectedLimit)
					if tc.cap != "" {
						require.Equal(t, tc.expectedLimit, sizes[tc.name])
					}
				})
			}
			// Equal distinct counts should retain the same small filter even when
			// the row count grows; distinct-heavy data must select a larger one.
			require.Equal(t, sizes["tiny"], sizes["repeated"])
			require.Greater(t, sizes["distinct"], sizes["repeated"])
		})
	}
}

func serializedAdaptiveBloomSize(t *testing.T, options []parquet.WriterProperty, rows, distinct int) int64 {
	t.Helper()

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "key", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: "disabled", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: "unselected", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
	}, nil)
	builder := array.NewInt64Builder(memory.DefaultAllocator)
	defer builder.Release()
	for row := range rows {
		if row%31 == 0 {
			builder.AppendNull()
		} else {
			builder.Append(int64(row % distinct))
		}
	}
	values := builder.NewInt64Array()
	defer values.Release()
	record := array.NewRecordBatch(schema, []arrow.Array{values, values, values}, int64(rows))
	defer record.Release()

	var output bytes.Buffer
	writer, err := pqarrow.NewFileWriter(schema, &output, parquet.NewWriterProperties(options...), pqarrow.DefaultWriterProps())
	require.NoError(t, err)
	require.NoError(t, writer.Write(record))
	require.NoError(t, writer.Close())

	input := bytes.NewReader(output.Bytes())
	reader, err := file.NewParquetReader(input)
	require.NoError(t, err)
	defer reader.Close()
	require.Equal(t, 1, reader.NumRowGroups())
	group := reader.MetaData().RowGroup(0)
	for _, column := range []int{1, 2} {
		chunk, err := group.ColumnChunk(column)
		require.NoError(t, err)
		require.Zero(t, chunk.BloomFilterOffset(), "disabled and unselected columns must not gain filters")
	}
	chunk, err := group.ColumnChunk(0)
	require.NoError(t, err)
	require.Positive(t, chunk.BloomFilterOffset())

	// Read the serialized Thrift header's first field (numBytes), rather than
	// inspecting writer properties or an in-memory candidate's allocation.
	var header [11]byte
	_, err = input.ReadAt(header[:], chunk.BloomFilterOffset())
	require.NoError(t, err)
	require.Equal(t, byte(0x15), header[0])
	encodedSize, consumed := binary.Uvarint(header[1:])
	require.Positive(t, consumed)
	require.Zero(t, encodedSize&1)
	size := int64(encodedSize >> 1)
	require.GreaterOrEqual(t, size, int64(32))
	require.Zero(t, size&(size-1), "serialized bitset must be a power of two")

	filters, err := reader.GetBloomFilterReader().RowGroup(0)
	require.NoError(t, err)
	require.NotNil(t, filters)
	filter, err := filters.GetColumnBloomFilter(0)
	require.NoError(t, err)
	require.NotNil(t, filter)
	arrowReader, err := pqarrow.NewFileReader(reader, pqarrow.ArrowReadProperties{}, memory.DefaultAllocator)
	require.NoError(t, err)
	data, err := arrowReader.ReadTable(context.Background())
	require.NoError(t, err)
	defer data.Release()
	require.Equal(t, int64(rows), data.NumRows())
	var value [8]byte
	for _, chunk := range data.Column(0).Data().Chunks() {
		values := chunk.(*array.Int64)
		for row := range values.Len() {
			if values.IsNull(row) {
				continue
			}
			binary.LittleEndian.PutUint64(value[:], uint64(values.Value(row)))
			require.True(t, filter.CheckHash(filter.Hasher().Sum64(value[:])), "Bloom false negative for stored value %d", values.Value(row))
		}
	}

	return size
}
