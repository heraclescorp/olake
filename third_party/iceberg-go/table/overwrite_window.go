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
	"iter"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go"
	"github.com/google/uuid"
)

func windowedOverwriteFiles(ctx context.Context, location string, meta *MetadataBuilder, args recordWritingArgs, maxRows int, maxBytes int64) iter.Seq2[iceberg.DataFile, error] {
	return func(yield func(iceberg.DataFile, error) bool) {
		var window []arrow.RecordBatch
		var rows, bytes int64
		release := func() {
			for _, record := range window {
				record.Release()
			}
			clear(window)
			window, rows, bytes = window[:0], 0, 0
		}
		defer release()
		flush := func() bool {
			if len(window) == 0 {
				return true
			}
			record, err := concatenateOverwriteWindow(args.sc, window, rows)
			if err != nil {
				yield(nil, err)
				return false
			}
			defer record.Release()
			release()
			windowArgs := args
			// Each writer has its own file counters, so windows need unique names.
			id := uuid.New()
			windowArgs.writeUUID = &id
			windowArgs.clustered = true
			windowArgs.maxWriteWorkers = 0
			windowArgs.itr = func(emit func(arrow.RecordBatch, error) bool) { emit(record, nil) }
			for file, err := range recordsToDataFiles(ctx, location, meta, windowArgs) {
				if err != nil {
					yield(nil, err)
					return false
				}
				if !yield(file, nil) {
					return false
				}
			}
			return true
		}
		for record, err := range args.itr {
			if err != nil {
				yield(nil, err)
				return
			}
			if record.NumRows() == 0 {
				continue
			}
			var size int64
			for _, column := range record.Columns() {
				size += EstimateArrayDataBytes(column.Data())
			}
			if rows > 0 && (record.NumRows() > int64(maxRows)-rows || size > maxBytes-bytes) && !flush() {
				return
			}
			record.Retain()
			window = append(window, record)
			rows += record.NumRows()
			bytes += size
			if (rows >= int64(maxRows) || bytes >= maxBytes) && !flush() {
				return
			}
		}
		flush()
	}
}

func concatenateOverwriteWindow(schema *arrow.Schema, records []arrow.RecordBatch, rows int64) (arrow.RecordBatch, error) {
	if len(records) == 1 {
		records[0].Retain()
		return records[0], nil
	}
	columns := make([]arrow.Array, len(schema.Fields()))
	defer func() {
		for _, column := range columns {
			if column != nil {
				column.Release()
			}
		}
	}()
	chunks := make([]arrow.Array, len(records))
	for column := range columns {
		for i, record := range records {
			chunks[i] = record.Column(column)
		}
		var err error
		columns[column], err = array.Concatenate(chunks, memory.DefaultAllocator)
		if err != nil {
			return nil, err
		}
	}
	return array.NewRecordBatch(schema, columns, rows), nil
}

// EstimateArrayDataBytes counts retained buffers, including nested/dictionary
// data. Shared buffers may be counted repeatedly, making budgets conservative.
func EstimateArrayDataBytes(data arrow.ArrayData) int64 {
	if data == nil {
		return 0
	}
	if concrete, ok := data.(*array.Data); ok && concrete == nil {
		return 0
	}
	var size int64
	for _, buffer := range data.Buffers() {
		if buffer != nil {
			size += int64(buffer.Len())
		}
	}
	for _, child := range data.Children() {
		size += EstimateArrayDataBytes(child)
	}
	if dictionary := data.Dictionary(); dictionary != nil {
		size += EstimateArrayDataBytes(dictionary)
	}
	return size
}
