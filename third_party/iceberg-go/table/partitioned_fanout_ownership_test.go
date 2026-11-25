package table

import (
	"bytes"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go"
	"github.com/stretchr/testify/require"
)

type clearingPartitionAllocator struct {
	memory.Allocator
}

func (a clearingPartitionAllocator) Free(data []byte) {
	clear(data)
	a.Allocator.Free(data)
}

func TestPartitionedFanoutOwnsIPCPartitionValues(t *testing.T) {
	for _, transform := range []iceberg.Transform{iceberg.IdentityTransform{}, iceberg.TruncateTransform{Width: 4}} {
		t.Run(transform.String(), func(t *testing.T) {
			schema := iceberg.NewSchema(0,
				iceberg.NestedField{ID: 1, Name: "part", Type: iceberg.StringType{}, Required: true},
				iceberg.NestedField{ID: 2, Name: "wide", Type: iceberg.StringType{}},
			)
			arrowSchema, err := SchemaToArrowSchema(schema, nil, true, false)
			require.NoError(t, err)
			builder := array.NewRecordBuilder(memory.DefaultAllocator, arrowSchema)
			builder.Field(0).(*array.StringBuilder).Append("partition-value")
			builder.Field(1).(*array.StringBuilder).Append(strings.Repeat("wide", 1024))
			batch := builder.NewRecordBatch()
			builder.Release()
			var serialized bytes.Buffer
			writer := ipc.NewWriter(&serialized, ipc.WithSchema(arrowSchema))
			require.NoError(t, writer.Write(batch))
			require.NoError(t, writer.Close())
			batch.Release()
			reader, err := ipc.NewReader(bytes.NewReader(serialized.Bytes()), ipc.WithAllocator(clearingPartitionAllocator{Allocator: memory.DefaultAllocator}))
			require.NoError(t, err)
			require.True(t, reader.Next())
			spec := iceberg.NewPartitionSpec(iceberg.PartitionField{SourceIDs: []int{1}, FieldID: 1000, Name: "part", Transform: transform})
			partitions, err := getRecordPartitions(spec, schema, reader.RecordBatch())
			require.NoError(t, err)
			require.Len(t, partitions, 1)
			reader.Release()
			expected := "partition-value"
			if _, truncated := transform.(iceberg.TruncateTransform); truncated {
				expected = "part"
			}
			require.Equal(t, expected, partitions[0].partitionValues[1000])
			require.Equal(t, expected, partitions[0].partitionRec[0])
		})
	}
}
