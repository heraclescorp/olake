package iceberg

import (
	"context"
	"fmt"
	"maps"
	"regexp"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"errors"

	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/table"
	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/destination"
	"github.com/datazip-inc/olake/destination/iceberg/internal"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
	"github.com/datazip-inc/olake/utils/errs"
	"github.com/datazip-inc/olake/utils/typeutils"
	"github.com/spf13/viper"
)

type Iceberg struct {
	options       *destination.Options
	config        *Config
	stream        types.StreamInterface
	partitionInfo []internal.PartitionInfo // ordered slice to preserve partition column order
	catalog       catalog.Catalog
	schema        map[string]string
	writer        Writer
	// unavailableValues recovers values the source could not send; nil unless the stream keeps a row index (pos or dv)
	unavailableValues *unavailableValuesResolver
	// syncStartColumns are columns added from the source catalog at sync start (see planSourceColumns).
	syncStartColumns map[string]struct{}
	// Schema is refreshed at explicit evolution and commit boundaries.
}

// Pre-computed lookup tables for type validation
var validTypeTransitions = map[string]map[string]bool{
	"int":         {"int": true, "long": true},
	"long":        {"long": true, "int": true},
	"float":       {"float": true, "double": true},
	"double":      {"double": true, "float": true},
	"boolean":     {"boolean": true},
	"string":      {"string": true},
	"timestamptz": {"timestamptz": true},
}

// promotionTransitions tracks when type promotion is required
var promotionTransitions = map[string]map[string]bool{
	"int":   {"long": true},
	"float": {"double": true},
}

func (i *Iceberg) Type() string {
	return string(types.Iceberg)
}

func (i *Iceberg) Spec() any {
	return Config{}
}

func (i *Iceberg) Setup(ctx context.Context, stream types.StreamInterface, _ any, options *destination.Options) (any, *types.MetadataState, error) {
	i.options, i.stream = options, stream
	var err error
	i.partitionInfo, err = parsePartitionRegex(stream.GetPartitionRegex(), stream.ResolveColumnName)
	if err != nil {
		return nil, nil, err
	}
	indexThread := types.NewStreamIndexThread(options.TableIndex)
	writer, state, err := newNativeWriter(ctx, i.config, stream, options, i.catalog, indexThread)
	if err != nil {
		return nil, nil, err
	}
	i.writer = writer
	i.schema = writer.schemaMap()
	if indexThread != nil {
		i.unavailableValues = newUnavailableValuesResolver(options.ThreadID, stream, writer, indexThread, writer)
	}
	return i.schema, state, nil
}

// Write bounds batches after TOAST recovery expands their in-memory size.
func (i *Iceberg) Write(ctx context.Context, records []types.RawRecord) error {
	if i.unavailableValues == nil || i.unavailableValues.filledBytes == 0 {
		return i.writer.Write(ctx, records)
	}

	start, size := 0, int64(0)
	for idx := range records {
		// a fixed overhead for the record, plus the size of every value it holds
		recordSize := carryEntryOverhead
		for _, value := range records[idx].Data {
			recordSize += valueSize(value)
		}
		if idx > start && size+recordSize > constants.MaxDestinationBatchBytes {
			if err := i.writer.Write(ctx, records[start:idx]); err != nil {
				return err
			}
			start, size = idx, 0
		}
		size += recordSize
	}

	return i.writer.Write(ctx, records[start:])
}

func (i *Iceberg) Close(ctx context.Context, finalMetadataState any) error {
	if i.unavailableValues != nil {
		i.unavailableValues.Close()
	}
	if err := ctx.Err(); err != nil {
		i.writer.Abort()
		return err
	}
	return i.writer.Close(ctx, finalMetadataState)
}

func (i *Iceberg) Check(ctx context.Context) error {
	destinationDB := "test_olake"
	if prefix := viper.GetString(constants.DestinationDatabasePrefix); prefix != "" {
		destinationDB = fmt.Sprintf("%s_%s", utils.Reformat(prefix), destinationDB)
	}
	ctx, cancel := context.WithTimeout(ctx, 300*time.Second)
	defer cancel()
	stream := types.NewStream(destinationDB, destinationDB, nil)
	stream.DestinationDatabase, stream.DestinationTable = destinationDB, destinationDB
	for name, dataType := range types.RawSchema {
		if name != constants.StringifiedData {
			stream.Schema.AddTypes(name, true, dataType)
		}
	}
	stream.Schema.AddTypes("name", false, types.String)
	appendMode, normalized := true, false
	configured := &types.ConfiguredStream{Stream: stream, StreamMetadata: types.StreamMetadata{
		AppendMode: &appendMode, Normalization: &normalized,
	}}
	checker := &Iceberg{config: i.config, catalog: i.catalog}
	if _, _, err := checker.Setup(ctx, configured, nil, &destination.Options{
		ThreadID: "olake-check", Backfill: true,
	}); err != nil {
		return fmt.Errorf("initialize native Iceberg check: %w", err)
	}
	now := time.Now().UTC()
	record := types.CreateRawRecord(map[string]any{"name": "olake"}, map[string]any{
		constants.OlakeID: "olake", constants.OpType: "r", constants.OlakeTimestamp: now,
		constants.CdcTimestamp: now,
	}, nil)
	_, records, _, err := checker.FlattenAndCleanData(ctx, []types.RawRecord{record})
	if err != nil {
		checker.writer.Abort()
		return err
	}
	if err := checker.Write(ctx, records); err != nil {
		checker.writer.Abort()
		return err
	}
	return checker.Close(ctx, nil)
}

// validate schema change & evolution and removes null records
func (i *Iceberg) FlattenAndCleanData(ctx context.Context, records []types.RawRecord) (bool, []types.RawRecord, any, error) {
	// Resolve first, so schema detection, filters, partition values and the JSON row
	// (normalization off) all see the recovered values.
	if i.unavailableValues != nil {
		if err := i.unavailableValues.Resolve(ctx, records); err != nil {
			return false, nil, nil, fmt.Errorf("failed to recover unavailable column values: %w", err)
		}
	}
	if err := destination.RejectUnavailableValues(i.stream, records); err != nil {
		return false, nil, nil, err
	}

	// extractSchemaFromRecords detects difference in current thread schema and the batch that being received
	// Also extracts current batch schema
	extractSchemaFromRecords := func(ctx context.Context, records []types.RawRecord) (bool, map[string]string, error) {
		// detectOrUpdateSchema detects difference in current thread schema and the batch that being received when detectChange is true
		// else updates the schemaMap with the new schema
		detectOrUpdateSchema := func(record types.RawRecord, detectChange bool, threadSchema, finalSchema map[string]string) (bool, error) {
			for key, value := range record.Data {
				detectedType := typeutils.TypeFromValue(value)

				if detectedType == types.Null {
					// remove element from data if it is null
					delete(record.Data, key)
					continue
				}

				detectedIcebergType := detectedType.ToIceberg()
				if _, existInIceberg := threadSchema[key]; existInIceberg {
					// Column exists in iceberg table: restrict to valid promotions only
					valid := isValidTypeForColumn(finalSchema[key], detectedIcebergType)
					if !valid {
						return false, fmt.Errorf(
							"failed to validate schema for field[%s] (detected two different types in batch), expected type: %s, detected type: %s",
							key, finalSchema[key], detectedIcebergType,
						)
					}

					if isPromotionRequired(finalSchema[key], detectedIcebergType) {
						if detectChange {
							return true, nil
						}

						// evolve schema
						finalSchema[key] = detectedIcebergType
					}
				} else {
					// New column: converge to common ancestor across concurrent updates
					if detectChange {
						return true, nil
					}

					// evolve schema
					if existingType, exists := finalSchema[key]; exists {
						finalSchema[key] = getCommonAncestorType(existingType, detectedIcebergType)
					} else {
						finalSchema[key] = detectedIcebergType
					}
				}
			}
			return false, nil
		}

		// parallel flatten data and detect schema difference
		// One flattener per batch: the internal cache amortizes resolve calls
		// across all records so each column name is resolved only once.
		batchFlattener := typeutils.NewFlattener(i.stream.ResolveColumnName)
		diffThreadSchema := atomic.Bool{}
		err := utils.Concurrent(ctx, records, runtime.GOMAXPROCS(0)*16, func(_ context.Context, record types.RawRecord, idx int) error {
			flattenRecord, err := batchFlattener.Flatten(record.Data)
			if err != nil {
				return fmt.Errorf("failed to flatten record, iceberg writer: %w", err)
			}
			records[idx].Data = flattenRecord
			maps.Copy(records[idx].Data, record.OlakeColumns)
			// if schema difference is not detected, detect schema difference
			if !diffThreadSchema.Load() {
				// when detectChange is true, the function does not modify schema parameter
				if changeDetected, err := detectOrUpdateSchema(records[idx], true, i.schema, i.schema); err != nil {
					return fmt.Errorf("failed to detect schema: %w", err)
				} else if changeDetected {
					diffThreadSchema.Store(true)
				}
			}

			return nil
		})
		if err != nil {
			return false, nil, fmt.Errorf("failed to flatten schema concurrently and detect change in records: %w", err)
		}

		// if schema difference is detected, update schemaMap with the new schema
		schemaMap := copySchema(i.schema)
		if diffThreadSchema.Load() {
			for _, record := range records {
				_, err := detectOrUpdateSchema(record, false, i.schema, schemaMap)
				if err != nil {
					return false, nil, fmt.Errorf("failed to update schema: %w", err)
				}
			}
		}

		return diffThreadSchema.Load(), schemaMap, err
	}

	if !i.stream.NormalizationEnabled() {
		err := utils.Concurrent(ctx, records, runtime.GOMAXPROCS(0)*16, func(_ context.Context, record types.RawRecord, idx int) error {
			// JSON-encode original source data before overwriting the map
			dataBytes, err := internal.MarshalRawData(record.Data)
			if err != nil {
				return destination.WriteFailure(fmt.Errorf("failed to marshal raw data: %w", err))
			}
			records[idx].Data = make(map[string]any, len(record.OlakeColumns)+1+len(i.partitionInfo))
			records[idx].Data[constants.StringifiedData] = string(dataBytes)
			maps.Copy(records[idx].Data, record.OlakeColumns)
			// include partition column values from the original source data so writers
			// can apply the Iceberg partition spec without a separate data buffer.
			// Read with Field (original source key), write with SchemaField (reformatted
			// destination key) so downstream lookups against the Iceberg schema succeed.
			for _, pInfo := range i.partitionInfo {
				if pInfo.Field == constants.OlakeTimestamp {
					continue // _olake_timestamp is already copied via OlakeColumns above
				}
				if v, ok := record.Data[pInfo.Field]; ok {
					records[idx].Data[pInfo.SchemaField] = v
				}
			}
			return nil
		})
		if err != nil {
			return false, nil, nil, fmt.Errorf("failed to pre-shape raw records: %w", err)
		}
		return false, records, i.schema, nil
	}

	schemaDifference, recordsSchema, err := extractSchemaFromRecords(ctx, records)
	if err != nil {
		return false, nil, nil, fmt.Errorf("failed to extract schema from records: %w", err)
	}

	if i.options.ApplyFilter {
		filter, isLegacy, filterErr := i.stream.GetFilter()
		if filterErr != nil {
			return false, nil, nil, fmt.Errorf("failed to parse stream filter: %w", filterErr)
		}
		records, err = typeutils.FilterRecords(ctx, records, filter, isLegacy, recordsSchema, i.stream.ResolveColumnName, i.options.KeepDeletesThroughFilter)
		if err != nil {
			return false, nil, nil, fmt.Errorf("failed to filter records: %w", err)
		}
	}

	return schemaDifference, records, recordsSchema, nil
}

// EvolveSchema reloads the current table and applies compatible source schema changes.
func (i *Iceberg) EvolveSchema(ctx context.Context, globalSchema, recordsRawSchema any) (any, error) {
	if !i.stream.NormalizationEnabled() {
		return i.schema, nil
	}
	global, ok := globalSchema.(map[string]string)
	if !ok {
		return nil, fmt.Errorf("invalid global schema type %T", globalSchema)
	}
	records, ok := recordsRawSchema.(map[string]string)
	if !ok {
		return nil, fmt.Errorf("invalid record schema type %T", recordsRawSchema)
	}
	columns := copySchema(global)
	for name, fieldType := range records {
		if previous, found := columns[name]; !found || isPromotionRequired(previous, fieldType) {
			columns[name] = fieldType
		}
	}
	if err := i.writer.EvolveSchema(ctx, columns); err != nil {
		return nil, destination.WriteFailure(fmt.Errorf("evolve native Iceberg schema: %w", err))
	}
	i.schema = i.writer.(*nativeWriter).schemaMap()
	return i.schema, nil
}

// parsePartitionRegex parses the partition regex and populates the partitionInfo slice
func parsePartitionRegex(pattern string, resolveColumnName func(string) string) ([]internal.PartitionInfo, error) {
	// path pattern example: /{col_name, partition_transform}/{col_name, partition_transform}
	// This strictly identifies column name and partition transform entries
	var partitionInfo []internal.PartitionInfo
	if pattern == "" {
		return partitionInfo, nil
	}

	patternRegex := regexp.MustCompile(constants.PartitionRegexIceberg)
	matches := patternRegex.FindAllStringSubmatch(pattern, -1)
	if len(matches) == 0 {
		return nil, errs.Precondition(errs.ConfigInvalid, codePartitionRegexNoMatch,
			fmt.Errorf("no matches found for partition regex: %s", pattern))
	}

	for _, match := range matches {
		if len(match) < 3 {
			continue // We need at least 3 matches: full match, column name, transform
		}

		colName := strings.Replace(strings.TrimSpace(strings.Trim(match[1], `'"`)), "now()", constants.OlakeTimestamp, 1)
		transform := strings.TrimSpace(strings.Trim(match[2], `'"`))

		// Append to ordered slice to preserve partition order.
		// SchemaField is reformatted once here so all consumers use the consistent
		// destination column name without scattering utils.Reformat() calls.
		partitionInfo = append(partitionInfo, internal.PartitionInfo{
			Field:       colName,
			SchemaField: resolveColumnName(colName),
			Transform:   transform,
		})
	}

	return partitionInfo, nil
}

// isValidTypeForColumn reports whether a column of iceberg type columnType can take a value of
// iceberg type valueType, as its value type or through an iceberg promotion. A fixed[n] value of
// the wrong length fails on write.
func isValidTypeForColumn(columnType, valueType string) bool {
	if types.IcebergValueType(columnType) == valueType || validTypeTransitions[columnType][valueType] {
		return true
	}
	return getCommonAncestorType(columnType, valueType) == columnType
}

// isPromotionRequired checks if promotion is needed using lookup table
func isPromotionRequired(oldType, newType string) bool {
	if transitions, ok := promotionTransitions[oldType]; ok {
		return transitions[newType]
	}

	return false
}

// DropStreams purges only the selected destination tables.
func (i *Iceberg) DropStreams(ctx context.Context, streams []types.StreamInterface) error {
	purger, ok := i.catalog.(catalog.PurgeableTable)
	if !ok {
		return fmt.Errorf("Iceberg catalog %s does not support purging tables", i.config.CatalogType)
	}
	for _, stream := range streams {
		identifier := table.Identifier{stream.GetDestinationDatabase(&i.config.IcebergDatabase), stream.GetDestinationTable()}
		if err := purger.PurgeTable(nativeCatalogContext(ctx, i.catalog), identifier); err != nil && !errors.Is(err, catalog.ErrNoSuchTable) {
			return fmt.Errorf("purge Iceberg table %v: %w", identifier, err)
		}
	}
	return nil
}

// returns a new copy of schema
func copySchema(schema map[string]string) map[string]string {
	return maps.Clone(schema)
}

func getCommonAncestorType(d1, d2 string) string {
	// check for cases:
	// d1: string d2: int  -> return string
	// d1: float d2: int  -> return float
	// d1: string d2: float  -> return string
	// d1: string d2: timestamp  -> return string

	oldDT := types.IcebergTypeToDatatype(d1)
	newDT := types.IcebergTypeToDatatype(d2)
	return types.GetCommonAncestorType(oldDT, newDT).ToIceberg()
}

func isUpsertMode(stream types.StreamInterface, backfill bool) bool {
	return utils.Ternary(stream.AppendModeEnabled(), false, !backfill).(bool)
}

func init() {
	var mu sync.Mutex
	var sharedCatalog catalog.Catalog
	var sharedConfig *Config
	destination.RegisteredWriters[types.Iceberg] = func(raw any) (destination.Writer, func(context.Context), error) {
		mu.Lock()
		defer mu.Unlock()
		if sharedCatalog != nil {
			return &Iceberg{config: sharedConfig, catalog: sharedCatalog}, nil, nil
		}
		config := &Config{}
		if err := utils.Unmarshal(raw, config); err != nil {
			return nil, nil, fmt.Errorf("decode native Iceberg config: %w", err)
		}
		if err := config.Validate(); err != nil {
			return nil, nil, err
		}
		ctx, cancel := context.WithCancel(context.Background())
		cat, closeCatalog, err := openNativeCatalog(ctx, config)
		if err != nil {
			cancel()
			return nil, nil, err
		}
		sharedCatalog, sharedConfig = cat, config
		cleanup := func(ctx context.Context) {
			mu.Lock()
			defer mu.Unlock()
			defer cancel()
			closeCatalog(ctx)
			sharedCatalog, sharedConfig = nil, nil
		}
		return &Iceberg{config: config, catalog: cat}, cleanup, nil
	}
}
