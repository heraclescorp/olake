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

package file_test

import (
	"bytes"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/metadata"
	"github.com/apache/arrow-go/v18/parquet/schema"
	"github.com/stretchr/testify/require"
)

func TestRowGroupAdaptiveBloomCompaction(t *testing.T) {
	const (
		groups        = 24
		maxBloomBytes = 64 << 10
	)
	for _, mode := range []string{"serial", "buffered"} {
		for _, dictionary := range []bool{false, true} {
			name := mode + "/plain"
			if dictionary {
				name = mode + "/dictionary"
			}
			t.Run(name, func(t *testing.T) {
				mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
				defer mem.AssertSize(t, 0)
				root, err := schema.NewGroupNode("schema", parquet.Repetitions.Required, schema.FieldList{
					schema.NewInt32Node("value", parquet.Repetitions.Required, -1),
				}, -1)
				require.NoError(t, err)
				props := parquet.NewWriterProperties(
					parquet.WithAllocator(mem),
					parquet.WithDictionaryDefault(dictionary),
					parquet.WithBloomFilterEnabled(true),
					parquet.WithAdaptiveBloomFilterEnabled(true),
					parquet.WithMaxBloomFilterBytes(maxBloomBytes),
					parquet.WithBloomFilterCandidates(16),
				)
				var sink bytes.Buffer
				writer := file.NewParquetWriter(&sink, root, file.WithWriterProps(props))
				defer writer.Close()
				var rowGroups []file.RowGroupWriter
				var selectedBytes int64
				valuesFor := func(group int) []int32 {
					values := make([]int32, 32)
					for i := range values {
						values[i] = int32(group*100 + i%8)
					}
					return values
				}
				for group := range groups {
					var rg file.RowGroupWriter
					var column file.ColumnChunkWriter
					if mode == "buffered" {
						buffered := writer.AppendBufferedRowGroup()
						rg = buffered
						column, err = buffered.Column(0)
					} else {
						serial := writer.AppendRowGroup()
						rg = serial
						column, err = serial.NextColumn()
					}
					require.NoError(t, err)
					values := valuesFor(group)
					written, err := column.(*file.Int32ColumnChunkWriter).WriteBatch(values, nil, nil)
					require.NoError(t, err)
					require.EqualValues(t, len(values), written)
					// Close must flush the dictionary's final hashes before compacting.
					filter := column.GetBloomFilter()
					require.NotNil(t, filter)
					require.NoError(t, rg.Close())
					selectedBytes += filter.Size()
					require.NoError(t, rg.Close())
					// Allow one maximum-sized working buffer plus alignment overhead
					// on the selected bitsets, not all the discarded candidates.
					require.LessOrEqual(t, int64(mem.CurrentAlloc()), int64(maxBloomBytes)+2*selectedBytes,
						"closed row group %d retains unused Bloom candidates", group)
					rowGroups = append(rowGroups, rg)
				}
				require.NoError(t, writer.Close())
				require.NoError(t, writer.Close())
				for _, rg := range rowGroups {
					require.NoError(t, rg.Close())
				}
				mem.AssertSize(t, 0)

				reader, err := file.NewParquetReader(bytes.NewReader(sink.Bytes()))
				require.NoError(t, err)
				defer reader.Close()
				require.Equal(t, groups, reader.NumRowGroups())
				require.EqualValues(t, groups*32, reader.NumRows())
				var previousBloomEnd int64
				for group := range groups {
					rg := reader.RowGroup(group)
					require.EqualValues(t, 32, rg.NumRows())
					column, err := rg.Column(0)
					require.NoError(t, err)
					values := make([]int32, 32)
					rows, count, err := column.(*file.Int32ColumnChunkReader).ReadBatch(32, values, nil, nil)
					require.NoError(t, err)
					require.EqualValues(t, 32, rows)
					require.Equal(t, 32, count)
					require.Equal(t, valuesFor(group), values)

					chunk, err := rg.MetaData().ColumnChunk(0)
					require.NoError(t, err)
					require.Positive(t, chunk.BloomFilterOffset())
					require.Positive(t, chunk.BloomFilterLength())
					require.GreaterOrEqual(t, chunk.BloomFilterOffset(), previousBloomEnd)
					previousBloomEnd = chunk.BloomFilterOffset() + int64(chunk.BloomFilterLength())
					require.Less(t, previousBloomEnd, int64(sink.Len()))
					blooms, err := reader.GetBloomFilterReader().RowGroup(group)
					require.NoError(t, err)
					bloom, err := blooms.GetColumnBloomFilter(0)
					require.NoError(t, err)
					require.NotNil(t, bloom)
					typed := metadata.TypedBloomFilter[int32]{BloomFilter: bloom}
					for _, value := range values {
						require.True(t, typed.Check(value), "row group %d lost value %d", group, value)
					}
				}
			})
		}
	}
}

func TestRowGroupAdaptiveBloomFailedCloseRemainsFailed(t *testing.T) {
	for _, mode := range []string{"serial", "buffered"} {
		t.Run(mode, func(t *testing.T) {
			root, err := schema.NewGroupNode("schema", parquet.Repetitions.Required, schema.FieldList{
				schema.NewInt32Node("first", parquet.Repetitions.Required, -1),
				schema.NewInt32Node("second", parquet.Repetitions.Required, -1),
			}, -1)
			require.NoError(t, err)
			props := parquet.NewWriterProperties(
				parquet.WithBloomFilterEnabled(true),
				parquet.WithAdaptiveBloomFilterEnabled(true),
				parquet.WithMaxBloomFilterBytes(64<<10),
				parquet.WithBloomFilterCandidates(16),
			)
			var sink bytes.Buffer
			writer := file.NewParquetWriter(&sink, root, file.WithWriterProps(props))
			var rg file.RowGroupWriter
			var column file.ColumnChunkWriter
			if mode == "serial" {
				serial := writer.AppendRowGroup()
				rg = serial
				column, err = serial.NextColumn()
				// Leaving the second column uninitialized makes metadata.Finish fail
				// after the first column has successfully closed.
			} else {
				buffered := writer.AppendBufferedRowGroup()
				rg = buffered
				column, err = buffered.Column(0)
				second, err := buffered.Column(1)
				require.NoError(t, err)
				defer second.GetBloomFilter().Release()
				// Unequal column lengths fail before column finalization.
			}
			require.NoError(t, err)
			filter := column.GetBloomFilter()
			defer filter.Release()
			_, err = column.(*file.Int32ColumnChunkWriter).WriteBatch([]int32{17}, nil, nil)
			require.NoError(t, err)
			closeErr := rg.Close()
			require.Error(t, closeErr)
			// Failed groups must not be marked compact/finalized.
			filter.InsertHash(123)
			require.True(t, filter.(metadata.BloomFilter).CheckHash(123))
			require.ErrorIs(t, rg.Close(), closeErr)
			require.ErrorIs(t, writer.Close(), closeErr)
			require.ErrorIs(t, writer.Close(), closeErr)
			_, err = file.NewParquetReader(bytes.NewReader(sink.Bytes()))
			require.Error(t, err, "a failed row group must not publish a valid footer")
		})
	}
}
