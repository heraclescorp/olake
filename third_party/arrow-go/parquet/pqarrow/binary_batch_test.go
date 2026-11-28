package pqarrow_test

import (
	"bytes"
	"context"
	"encoding/base64"
	"fmt"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/stretchr/testify/require"
)

func appendWideBinaryValue(builder array.Builder, value []byte) {
	switch builder := builder.(type) {
	case *array.StringBuilder:
		builder.Append(string(value))
	case *array.LargeStringBuilder:
		builder.Append(string(value))
	case *array.BinaryBuilder:
		builder.Append(value)
	case *array.StructBuilder:
		builder.Append(true)
		appendWideBinaryValue(builder.FieldBuilder(0), value)
	case *array.ListBuilder:
		builder.Append(true)
		appendWideBinaryValue(builder.ValueBuilder(), value)
	case *array.LargeListBuilder:
		builder.Append(true)
		appendWideBinaryValue(builder.ValueBuilder(), value)
	case *array.FixedSizeListBuilder:
		builder.Append(true)
		appendWideBinaryValue(builder.ValueBuilder(), value)
	case *array.MapBuilder:
		builder.Append(true)
		builder.KeyBuilder().(*array.StringBuilder).Append("key")
		appendWideBinaryValue(builder.ItemBuilder(), value)
	default:
		panic(fmt.Sprintf("unexpected builder %T", builder))
	}
}

func wideBinaryValue(values arrow.Array, row int) string {
	switch values := values.(type) {
	case *array.String:
		return values.Value(row)
	case *array.LargeString:
		return values.Value(row)
	case *array.Binary:
		return string(values.Value(row))
	case *array.LargeBinary:
		return string(values.Value(row))
	case *array.Struct:
		return wideBinaryValue(values.Field(0), row)
	case *array.List:
		start, _ := values.ValueOffsets(row)
		return wideBinaryValue(values.ListValues(), int(start))
	case *array.LargeList:
		start, _ := values.ValueOffsets(row)
		return wideBinaryValue(values.ListValues(), int(start))
	case *array.FixedSizeList:
		start, _ := values.ValueOffsets(row)
		return wideBinaryValue(values.ListValues(), int(start))
	case *array.Map:
		start, _ := values.ValueOffsets(row)
		return wideBinaryValue(values.Items(), int(start))
	default:
		panic(fmt.Sprintf("unexpected array %T", values))
	}
}

func TestBinaryRecordBatchesPreserveBoundedChunks(t *testing.T) {
	const rows = 1100
	value := bytes.Repeat([]byte("v"), 65537)
	value[len(value)-1] = '!'
	expected := string(value)
	binaryField := arrow.Field{Name: "payload", Type: arrow.BinaryTypes.String, Nullable: true}
	listField := binaryField
	listField.Name = "element"
	listField.Metadata = arrow.NewMetadata([]string{"PARQUET:field_id"}, []string{"-1"})
	types := []arrow.DataType{
		arrow.BinaryTypes.String,
		arrow.BinaryTypes.Binary,
		arrow.BinaryTypes.LargeString,
		arrow.BinaryTypes.LargeBinary,
		arrow.StructOf(binaryField),
		arrow.ListOfField(listField),
		arrow.LargeListOfField(listField),
		arrow.FixedSizeListOfField(1, listField),
		arrow.MapOf(arrow.BinaryTypes.String, arrow.BinaryTypes.String),
	}
	for _, dtype := range types {
		for _, parallel := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/parallel=%t", dtype, parallel), func(t *testing.T) {
				mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
				defer mem.AssertSize(t, 0)
				storageType := dtype
				if dtype.ID() == arrow.FIXED_SIZE_LIST {
					storageType = arrow.ListOfField(dtype.(*arrow.FixedSizeListType).ElemField())
				}
				builder := array.NewBuilder(mem, storageType)
				ids := array.NewInt64Builder(mem)
				for row := range rows {
					ids.Append(int64(row))
					if row%23 == 0 {
						builder.AppendNull()
					} else {
						appendWideBinaryValue(builder, value)
					}
				}
				values, indices := builder.NewArray(), ids.NewArray()
				builder.Release()
				ids.Release()
				fields := []arrow.Field{
					{Name: "id", Type: arrow.PrimitiveTypes.Int64},
					{Name: "wide", Type: dtype, Nullable: true},
				}
				sc := arrow.NewSchema(fields, nil)
				writerProps := pqarrow.NewArrowWriterProperties(pqarrow.WithAllocator(mem), pqarrow.WithStoreSchema())
				if dtype.ID() == arrow.FIXED_SIZE_LIST {
					storedSchema := base64.StdEncoding.EncodeToString(flight.SerializeSchema(sc, mem))
					meta := arrow.NewMetadata([]string{"ARROW:schema"}, []string{storedSchema})
					fields[1].Type = storageType
					sc = arrow.NewSchema(fields, &meta)
					writerProps = pqarrow.NewArrowWriterProperties(pqarrow.WithAllocator(mem))
				}
				record := array.NewRecordBatch(sc, []arrow.Array{indices, values}, rows)
				defer record.Release()
				indices.Release()
				values.Release()
				var sink bytes.Buffer
				writer, err := pqarrow.NewFileWriter(sc, &sink,
					parquet.NewWriterProperties(parquet.WithAllocator(mem), parquet.WithDictionaryDefault(true)),
					writerProps)
				require.NoError(t, err)
				defer writer.Close()
				require.NoError(t, writer.Write(record))
				require.NoError(t, writer.Close())
				parquetReader, err := file.NewParquetReader(bytes.NewReader(sink.Bytes()), file.WithReadProps(parquet.NewReaderProperties(mem)))
				require.NoError(t, err)
				defer parquetReader.Close()
				reader, err := pqarrow.NewFileReader(parquetReader, pqarrow.ArrowReadProperties{
					BatchSize: rows, Parallel: parallel, PreAllocBinaryData: true,
				}, mem)
				require.NoError(t, err)
				batches, err := reader.GetRecordReader(context.Background(), nil, nil)
				require.NoError(t, err)
				defer batches.Release()
				require.True(t, arrow.TypeEqual(dtype, batches.Schema().Field(1).Type))
				seen, count := 0, 0
				var retained arrow.RecordBatch
				defer func() {
					if retained != nil {
						retained.Release()
					}
				}()
				for batches.Next() {
					batch := batches.RecordBatch()
					if retained == nil {
						retained = batch
						retained.Retain()
					}
					maskBuilder := array.NewBooleanBuilder(mem)
					selected := 0
					for row := range int(batch.NumRows()) {
						id := int(batch.Column(0).(*array.Int64).Value(row))
						require.Equal(t, seen+row, id)
						maskBuilder.Append(id%3 != 0)
						if id%3 != 0 {
							selected++
						}
						if id%23 == 0 {
							require.True(t, batch.Column(1).IsNull(row))
						} else {
							require.Equal(t, expected, wideBinaryValue(batch.Column(1), row))
						}
					}
					mask := maskBuilder.NewBooleanArray()
					maskBuilder.Release()
					filtered, err := compute.FilterRecordBatch(context.Background(), batch, mask, compute.DefaultFilterOptions())
					mask.Release()
					require.NoError(t, err)
					require.EqualValues(t, selected, filtered.NumRows())
					for row := range int(filtered.NumRows()) {
						id := int(filtered.Column(0).(*array.Int64).Value(row))
						require.NotZero(t, id%3)
						if id%23 == 0 {
							require.True(t, filtered.Column(1).IsNull(row))
						} else {
							require.Equal(t, expected, wideBinaryValue(filtered.Column(1), row))
						}
					}
					filtered.Release()
					seen += int(batch.NumRows())
					count++
				}
				require.NoError(t, batches.Err())
				require.Equal(t, rows, seen)
				require.Greater(t, count, 1)
				require.Equal(t, expected, wideBinaryValue(retained.Column(1), 1))
			})
		}
	}
}
