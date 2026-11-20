package iceberg

import (
	"fmt"
	"maps"
	"strconv"

	apacheiceberg "github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/types"
)

const (
	nativeBloomFilterColumnPrefix = table.ParquetBloomFilterColumnEnabledKeyPrefix + "."
	nativeBloomFilterMaxBytesKey  = table.ParquetBloomFilterMaxBytesKey
	nativeBloomFilterMaxBytes     = table.ParquetBloomFilterMaxBytesDefault
)

func nativeBloomFilterProperties(schema *apacheiceberg.Schema, stream types.StreamInterface, properties apacheiceberg.Properties) (apacheiceberg.Properties, error) {
	if value, exists := properties[nativeBloomFilterMaxBytesKey]; exists {
		maxBytes, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("%w: invalid %s value %q: %v", apacheiceberg.ErrInvalidArgument, nativeBloomFilterMaxBytesKey, value, err)
		}
		if maxBytes < 32 || maxBytes > 128*1024*1024 {
			return nil, fmt.Errorf("%w: %s must be between 32 and 134217728 bytes, got %d", apacheiceberg.ErrInvalidArgument, nativeBloomFilterMaxBytesKey, maxBytes)
		}
	}
	props := maps.Clone(properties)
	if props == nil {
		props = make(apacheiceberg.Properties)
	}
	if _, exists := props[nativeBloomFilterMaxBytesKey]; !exists {
		props[nativeBloomFilterMaxBytesKey] = strconv.Itoa(nativeBloomFilterMaxBytes)
	}
	addColumn := func(name string) {
		field, exists := schema.FindFieldByName(name)
		if !exists {
			return
		}
		if _, scalar := field.Type.(apacheiceberg.PrimitiveType); !scalar {
			return
		}
		key := nativeBloomFilterColumnPrefix + name
		if _, explicit := props[key]; !explicit {
			props[key] = "true"
		}
	}
	addColumn(constants.OlakeID)
	if stream != nil {
		if source := stream.GetStream(); source != nil && source.SourceDefinedPrimaryKey != nil {
			source.SourceDefinedPrimaryKey.Range(func(name string) {
				addColumn(stream.ResolveColumnName(name))
			})
		}
	}
	for _, id := range schema.IdentifierFieldIDs {
		if name, exists := schema.FindColumnName(id); exists {
			addColumn(name)
		}
	}
	return props, nil
}
