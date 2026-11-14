package iceberg

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"math"
	"regexp"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	apacheiceberg "github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/catalog/rest"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
	"github.com/apache/iceberg-go/table/dv"
	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/destination"
	"github.com/datazip-inc/olake/destination/iceberg/internal"
	"github.com/datazip-inc/olake/pkg/indexdb"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
	"github.com/datazip-inc/olake/utils/errs"
	"github.com/datazip-inc/olake/utils/logger"
	"github.com/datazip-inc/olake/utils/typeutils"
	"github.com/google/uuid"
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
	resyncMu      sync.Mutex
	resync        *resyncControl
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

// EvolveSchema applies compatible source changes to the coordinated table view.
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
	destination.RegisteredWriters[types.Iceberg] = func(ctx context.Context, raw any) (destination.Writer, func(context.Context), error) {
		if err := utils.CheckExecutionFence(ctx); err != nil {
			return nil, nil, err
		}
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
		catalogCtx, cancel := context.WithCancel(ctx)
		cat, closeCatalog, err := openNativeCatalog(catalogCtx, config)
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

const nativeCommitAttempts = 4
const nativeRecoveryProperty = "olake_2pc"

// The coordinator belongs to one catalog lifetime, never a process-global cache.
// OLake must be the sole OLake process owning a destination table: this mutex
// serializes local recovery-property merges, not distributed property writers.
// Ref and default-shape assertions remain enabled; schema mutations retain the
// SDK's schema requirements. None is a CAS on arbitrary table properties.
type nativeTableCoordinator struct {
	mu     sync.Mutex
	tables map[string]*nativeTableState
	closed bool
}

type nativeTableState struct {
	mu         sync.Mutex
	latest     *table.Table
	unknown    bool
	closed     bool
	generation uint64
	resync     *resyncControl
	sessions   int
}

type nativeTableLockKey struct{}

func (c *nativeTableCoordinator) entry(identifier table.Identifier) *nativeTableState {
	// Length-prefixed components distinguish identifiers containing separators.
	var key strings.Builder
	for _, component := range identifier {
		key.WriteString(strconv.Itoa(len(component)))
		key.WriteByte(':')
		key.WriteString(component)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.tables == nil {
		c.tables = make(map[string]*nativeTableState)
	}
	state := c.tables[key.String()]
	if state == nil {
		state = &nativeTableState{closed: c.closed}
		c.tables[key.String()] = state
	}
	return state
}

func nativeTableEntry(cat catalog.Catalog, identifier table.Identifier) *nativeTableState {
	if stage, ok := cat.(*nativeResyncCatalog); ok {
		return stage.tables.entry(identifier)
	}
	if native, ok := cat.(*nativeCatalog); ok {
		return native.tables.entry(identifier)
	}
	// Callers supplying their own catalog can opt into sharing by wrapping it in
	// nativeCatalog. Never retain arbitrary catalog instances in a global map.
	return &nativeTableState{}
}

func (s *nativeTableState) usable() error {
	if s.closed {
		return errors.New("Iceberg catalog is closed")
	}
	if s.unknown {
		return errors.New("Iceberg table commit outcome is unknown; reopen the catalog before recovery")
	}
	return nil
}

func (s *nativeTableState) invalidate(unknown bool) {
	s.latest = nil
	s.unknown = s.unknown || unknown
}

func (w *nativeWriter) lockTable(ctx context.Context) (context.Context, *nativeTableState, error) {
	state := w.coordinator
	if state == nil {
		state = nativeTableEntry(w.cat, w.table.Identifier())
		w.coordinator = state
	}
	state.mu.Lock()
	if err := state.usable(); err != nil {
		if state.unknown {
			w.uncertain = true
		}
		state.mu.Unlock()
		return ctx, nil, err
	}
	if state.generation != w.generation {
		state.mu.Unlock()
		return ctx, nil, errors.New("Iceberg table was replaced or removed while this writer was active")
	}
	return context.WithValue(ctx, nativeTableLockKey{}, state), state, nil
}

func (w *nativeWriter) currentTable(ctx context.Context, state *nativeTableState) (*table.Table, error) {
	if state.latest == nil {
		fresh, err := w.cat.LoadTable(ctx, w.table.Identifier())
		if err != nil {
			return nil, err
		}
		state.latest = fresh
	}
	if state.latest.Metadata().TableUUID() != w.table.Metadata().TableUUID() {
		return nil, errors.New("Iceberg table identity changed while this writer was active")
	}
	return state.latest, nil
}

type nativeIndexRead struct {
	location types.RowLocation
	found    bool
}

type nativeWriter struct {
	table                  *table.Table
	cat                    catalog.Catalog
	coordinator            *nativeTableState
	generation             uint64
	config                 *Config
	stream                 types.StreamInterface
	options                *destination.Options
	indexThread            *types.StreamIndexThread
	indexReads             map[string]nativeIndexRead
	indexTouched           map[string]struct{}
	upsert                 bool
	deleteMode             types.UpdateType
	dataFiles              []apacheiceberg.DataFile
	dataSchemas            map[string]*apacheiceberg.Schema
	positions              map[string][]int64
	latest                 map[string]types.RowLocation
	paths                  map[string]struct{}
	backfillFiles          map[string]*nativeBackfillFile
	backfillClock          uint64
	backfillTargetBytes    int64
	backfillPartitionLimit int
	closed                 bool
	uncertain              bool
	failed                 error
	releaseSession         func(bool)
}

func newNativeWriter(ctx context.Context, config *Config, stream types.StreamInterface, options *destination.Options, cat catalog.Catalog, indexThread *types.StreamIndexThread) (_ *nativeWriter, _ *types.MetadataState, err error) {
	ctx, finish, err := beginNativeWriterWork(ctx, config)
	if err != nil {
		return nil, nil, err
	}
	defer finish()
	identifier := catalog.ToIdentifier(stream.GetDestinationDatabase(&config.IcebergDatabase), stream.GetDestinationTable())
	cat, release, err := routeNativeResyncWriter(ctx, cat, identifier)
	if err != nil {
		return nil, nil, err
	}
	var w *nativeWriter
	defer func() {
		if err != nil {
			if w != nil {
				w.Abort()
			} else {
				release(false)
			}
		}
	}()
	ctx = nativeCatalogContext(ctx, cat)
	state := nativeTableEntry(cat, identifier)
	state.mu.Lock()
	defer state.mu.Unlock()
	if err := state.usable(); err != nil {
		return nil, nil, err
	}
	ctx = context.WithValue(ctx, nativeTableLockKey{}, state)
	// Setup is the freshness boundary for external schema-only changes and
	// response-config credentials. Hold the table lock so an older load cannot
	// overwrite a newer local commit; Close reuses coordinated metadata.
	tbl, err := loadOrCreateNativeTable(ctx, config, stream, cat)
	if err != nil {
		return nil, nil, fmt.Errorf("load or create Iceberg table: %w", err)
	}
	state.latest = tbl
	w = &nativeWriter{table: tbl, cat: cat, coordinator: state, generation: state.generation, config: config, stream: stream, options: options, indexThread: indexThread, upsert: isUpsertMode(stream, options.Backfill), deleteMode: stream.GetUpdateType(), positions: make(map[string][]int64), latest: make(map[string]types.RowLocation), paths: make(map[string]struct{})}
	w.releaseSession = release
	if err := w.validateFormat(ctx); err != nil {
		return nil, nil, err
	}
	// Existing source columns are reconciled by ApplySourceColumns after the
	// SyncStart phase, when PostgreSQL defaults have become authoritative. Eager
	// union here would consume additions before their initial defaults arrive.
	if options.TableIndex != nil {
		if err := w.migrateEqualityDeletes(ctx); err != nil {
			return nil, nil, err
		}
	}
	// Reconcile and return recovery from the loaded metadata, or from the
	// successful format/migration commit that replaced it. A no-op
	// initialization must not issue another catalog GET.
	if err := indexdb.ReconcileTable(options.TableIndex, w.table.Properties()[resyncGenerationProperty], w.table.Metadata().TableUUID().String()); err != nil {
		return nil, nil, fmt.Errorf("reconcile stream index generation: %w", err)
	}
	if err := reconcileNativeTableIndex(ctx, options.TableIndex, w.table, constants.OlakeID); err != nil {
		return nil, nil, err
	}
	metadataState := &types.MetadataState{}
	if value := w.table.Properties()[nativeRecoveryProperty]; value != "" {
		if err := json.Unmarshal([]byte(value), metadataState); err != nil {
			return nil, nil, fmt.Errorf("decode Iceberg recovery state: %w", err)
		}
	}
	indexThread.SetObservabilityContext(ctx)
	return w, metadataState, nil
}

func (w *nativeWriter) writable() error {
	if w.uncertain {
		return errors.New("Iceberg commit outcome is unknown; writer cannot be reused or cleaned up")
	}
	if w.options != nil && w.options.Backfill && w.failed != nil {
		return fmt.Errorf("Iceberg writer has failed: %w", w.failed)
	}
	if w.closed {
		return errors.New("Iceberg writer is closed")
	}
	if w.failed != nil {
		return fmt.Errorf("Iceberg writer has failed: %w", w.failed)
	}
	return nil
}

func (w *nativeWriter) mutationError(err error) {
	if _, private := w.cat.(*nativeResyncCatalog); !private {
		nativeExecutionMutationError(w.config, err)
	}
}

func (w *nativeWriter) validateFormat(ctx context.Context) error {
	// Initialization holds the entry lock through format changes, equality
	// migration, index reconciliation and recovery-state publication.
	state := w.coordinator
	version := w.table.Metadata().Version()
	if version > 3 {
		return fmt.Errorf("unsupported Iceberg format version %d", version)
	}
	if w.deleteMode != types.UpdateTypeDeletionVector {
		if version >= 3 {
			return fmt.Errorf("Iceberg format v%d requires deletion vector update mode; reset the table before changing update mode", version)
		}
		if w.upsert && version < 2 {
			return errors.New("Iceberg upserts require table format version 2 or later")
		}
		return nil
	}
	if version >= 3 {
		return nil
	}
	for attempt := range nativeCommitAttempts {
		fresh, err := w.currentTable(ctx, state)
		if err != nil {
			return err
		}
		tx := fresh.NewTransaction()
		if err := tx.AssertRefSnapshotID(table.MainBranch); err != nil {
			return err
		}
		if err := tx.UpgradeFormatVersion(3); err != nil {
			return err
		}
		updated, err := tx.Commit(ctx)
		if err == nil {
			w.table, state.latest = updated, updated
			return nil
		}
		w.mutationError(err)
		err = nativeCatalogError("upgrade Iceberg format", err)
		w.logConflict("format-upgrade", attempt+1, fresh, err)
		unknown := !nativeMutationOutcomeKnown(err)
		state.invalidate(unknown)
		if unknown {
			w.uncertain = true
			return fmt.Errorf("Iceberg format upgrade outcome is unknown: %w", err)
		}
		if !errors.Is(err, table.ErrCommitFailed) {
			return err
		}
	}
	return fmt.Errorf("upgrade Iceberg format: %w", table.ErrCommitFailed)
}

type nativeWriteGroup struct {
	rows      []map[string]any
	indices   []int
	partition map[int]any
}

func (w *nativeWriter) Write(ctx context.Context, records []types.RawRecord) (err error) {
	ctx, finish, err := beginNativeWriterWork(ctx, w.config)
	if err != nil {
		if w.options.Backfill && ctx.Err() != nil && !w.closed {
			w.failed = err
			w.Abort()
		}
		return err
	}
	defer finish()
	if err := w.writable(); err != nil {
		return err
	}
	defer func() {
		if err != nil {
			w.failed = err
			w.Abort()
		}
	}()
	ctx = nativeCatalogContext(ctx, w.cat)
	w.indexThread.SetObservabilityContext(ctx)
	if w.options.Backfill {
		return w.writeBackfill(ctx, records)
	}
	groups := make(map[string]*nativeWriteGroup)
	rows := make([]map[string]any, len(records))
	operations := make([]string, len(records))
	keys := make([]string, len(records))
	var batchSeen map[string]struct{}
	if w.indexThread != nil && w.upsert {
		batchSeen = make(map[string]struct{})
	}
	for idx, record := range records {
		if err := ctx.Err(); err != nil {
			return err
		}
		if record.Data == nil {
			continue
		}
		row := record.Data
		maps.Copy(row, record.OlakeColumns)
		id, ok := row[constants.OlakeID].(string)
		if !ok {
			return errors.New("Iceberg row is missing its string _olake_id")
		}
		op, ok := row[constants.OpType].(string)
		if !ok {
			return errors.New("Iceberg row is missing its string _op_type")
		}
		operations[idx], keys[idx] = op, id
		if w.indexThread != nil && w.upsert && op != "r" && op != "c" {
			_, touched := w.indexTouched[id]
			_, seen := batchSeen[id]
			if !touched && !seen {
				previous, found, err := w.options.TableIndex.Lookup(id)
				if err != nil {
					return fmt.Errorf("look up Iceberg row identity: %w", err)
				}
				if w.indexReads == nil {
					w.indexReads = make(map[string]nativeIndexRead)
				}
				w.indexReads[id] = nativeIndexRead{location: previous, found: found}
				if op == "u" && w.stream.ResolveUpsertOp() && !found {
					row[constants.OpType] = "c"
				}
			}
		}
		if op == "i" {
			row[constants.OpType] = "c"
		}
		key, partition, err := nativeRecordPartition(w.table.Schema(), w.table.Spec(), row)
		if err != nil {
			return err
		}
		group := groups[key]
		if group == nil {
			group = &nativeWriteGroup{partition: partition}
			groups[key] = group
		}
		group.rows = append(group.rows, row)
		group.indices = append(group.indices, idx)
		rows[idx] = row
		if batchSeen != nil {
			batchSeen[id] = struct{}{}
		}
	}
	locations := make([]types.RowLocation, len(records))
	for _, key := range slices.Sorted(maps.Keys(groups)) {
		group := groups[key]
		batch, err := nativeArrowRecord(w.table.Schema(), group.rows)
		if err != nil {
			return err
		}
		file, err := w.writeNativeParquet(ctx, w.table.Schema(), w.table.Spec(), group.partition, batch, apacheiceberg.EntryContentData, nil, nil)
		batch.Release()
		if err != nil {
			return err
		}
		w.dataFiles = append(w.dataFiles, file)
		if w.dataSchemas == nil {
			w.dataSchemas = make(map[string]*apacheiceberg.Schema)
		}
		w.dataSchemas[file.FilePath()] = w.table.Schema()
		for position, idx := range group.indices {
			locations[idx] = types.RowLocation{FilePath: file.FilePath(), Position: int64(position)}
		}
	}
	// Resolve superseded locations in SOURCE order, not partition iteration
	// order. The pending index and latest maps therefore see earlier records in
	// this same batch, even when a key changes partition.
	for idx, row := range rows {
		if row == nil {
			continue
		}
		id, op := keys[idx], operations[idx]
		if w.upsert {
			if w.indexThread != nil {
				_, touched := w.indexTouched[id]
				if touched {
					// A touched key is guaranteed to be pending: this lookup never
					// reaches the committed store, including for later c/r events.
					previous, found, err := w.indexThread.Lookup(id)
					if err != nil {
						return err
					}
					if !found {
						return errors.New("touched Iceberg row is missing from the pending index")
					}
					w.positions[previous.FilePath] = append(w.positions[previous.FilePath], previous.Position)
				} else if op != "r" && op != "c" {
					previous := w.indexReads[id]
					if previous.found {
						w.positions[previous.location.FilePath] = append(w.positions[previous.location.FilePath], previous.location.Position)
					}
				}
				if w.indexTouched == nil {
					w.indexTouched = make(map[string]struct{})
				}
				w.indexTouched[id] = struct{}{}
			} else if op != "r" {
				if previous, found := w.latest[id]; found {
					w.positions[previous.FilePath] = append(w.positions[previous.FilePath], previous.Position)
				}
				w.latest[id] = locations[idx]
			}
		}
		if w.indexThread != nil {
			w.indexThread.Put(id, locations[idx])
		}
	}
	return nil
}

func (w *nativeWriter) EnsureReadable(ctx context.Context, paths []string) error {
	if err := w.writable(); err != nil {
		return err
	}
	if len(w.backfillFiles) != 0 {
		ctx, finish, err := beginNativeWriterWork(ctx, w.config)
		if err != nil {
			if ctx.Err() != nil {
				w.failed = err
				w.Abort()
			}
			return err
		}
		defer finish()
		for _, key := range slices.Sorted(maps.Keys(w.backfillFiles)) {
			if slices.Contains(paths, w.backfillFiles[key].path) {
				if err := w.finishBackfillFile(ctx, key); err != nil {
					w.failed = err
					w.Abort()
					return err
				}
			}
		}
	}
	fs, err := w.table.FS(nativeCatalogContext(ctx, w.cat))
	if err != nil {
		return err
	}
	for _, path := range paths {
		if _, ours := w.paths[path]; !ours {
			continue
		}
		file, err := fs.Open(path)
		if err != nil {
			return fmt.Errorf("open staged Iceberg recovery file: %w", err)
		}
		if err := file.Close(); err != nil {
			return err
		}
	}
	return ctx.Err()
}

func mergeNativeRecovery(current, threadID string, payload any, hasFiles bool) (string, bool, error) {
	root := make(map[string]json.RawMessage)
	if current != "" {
		if err := json.Unmarshal([]byte(current), &root); err != nil {
			return "", false, fmt.Errorf("decode stored Iceberg recovery document: %w", err)
		}
	}
	if root == nil {
		return "", false, errors.New("stored Iceberg recovery document is not an object")
	}
	if payload == nil {
		if !hasFiles {
			return current, false, nil
		}
		var ids []string
		if raw := root["full_refresh_committed_ids"]; len(raw) > 0 {
			if err := json.Unmarshal(raw, &ids); err != nil {
				return "", false, err
			}
		}
		if !slices.Contains(ids, threadID) {
			ids = append(ids, threadID)
		}
		root["full_refresh_committed_ids"], _ = json.Marshal(ids)
		root["dedup_inserts"] = json.RawMessage("true")
	} else {
		encoded, err := json.Marshal(payload)
		if err != nil {
			return "", false, fmt.Errorf("encode Iceberg recovery payload: %w", err)
		}
		incoming := make(map[string]json.RawMessage)
		if err := json.Unmarshal(encoded, &incoming); err != nil {
			return "", false, fmt.Errorf("Iceberg recovery payload must be an object: %w", err)
		}
		root["id"], _ = json.Marshal(threadID)
		for key, value := range incoming {
			oldObject, oldOK := nativeJSONStringObject(root[key])
			newObject, newOK := nativeJSONStringObject(value)
			if oldOK && newOK {
				maps.Copy(oldObject, newObject)
				merged, err := json.Marshal(oldObject)
				if err != nil {
					return "", false, err
				}
				value, _ = json.Marshal(string(merged))
			}
			root[key] = value
		}
	}
	encoded, err := json.Marshal(root)
	return string(encoded), string(encoded) != current, err
}

func nativeJSONStringObject(raw json.RawMessage) (map[string]json.RawMessage, bool) {
	var value string
	if json.Unmarshal(raw, &value) != nil {
		return nil, false
	}
	var object map[string]json.RawMessage
	if json.Unmarshal([]byte(value), &object) != nil || object == nil {
		return nil, false
	}
	return object, true
}

func nativeSnapshotID(tbl *table.Table) int64 {
	if snapshot := tbl.CurrentSnapshot(); snapshot != nil {
		return snapshot.SnapshotID
	}
	return 0
}

func (w *nativeWriter) Close(ctx context.Context, finalMetadataState any) (err error) {
	ctx, finish, err := beginNativeWriterWork(ctx, w.config)
	if err != nil {
		if w.options.Backfill && ctx.Err() != nil && !w.closed {
			w.failed = err
			w.Abort()
		}
		return err
	}
	defer finish()
	if err := w.writable(); err != nil {
		return err
	}
	defer func() {
		if err == nil && w.releaseSession != nil {
			w.releaseSession(true)
		}
		w.indexThread.ReleaseObservability()
		if err != nil {
			if w.options.Backfill {
				w.failed = err
			}
			w.Abort()
		}
	}()
	ctx = nativeCatalogContext(ctx, w.cat)
	if err := w.finishBackfillFiles(ctx); err != nil {
		return fmt.Errorf("finish backfill Iceberg files: %w", err)
	}
	if err := w.compactStagedDataFiles(ctx); err != nil {
		return fmt.Errorf("compact superseded staged Iceberg rows: %w", err)
	}
	ctx, coordinated, err := w.lockTable(ctx)
	if err != nil {
		return err
	}
	defer coordinated.mu.Unlock()
	hasFiles := len(w.dataFiles) > 0 || len(w.positions) > 0
	var lastConflict error
	for attempt := range nativeCommitAttempts {
		if err := ctx.Err(); err != nil {
			return err
		}
		fresh, err := w.currentTable(ctx, coordinated)
		if err != nil {
			return err
		}
		var indexBase int64
		if hasFiles && w.indexThread != nil {
			indexBase, err = w.options.TableIndex.LastCommittedSnapshot()
			if err != nil {
				return err
			}
			if nativeSnapshotID(fresh) != indexBase {
				w.logConflict("commit-index-base", attempt+1, fresh, table.ErrConflictingDeleteFiles)
				return fmt.Errorf("Iceberg index checkpoint %d differs from current snapshot %d; refusing stale positional deletes: %w", indexBase, nativeSnapshotID(fresh), table.ErrCommitFailed)
			}
			for id, original := range w.indexReads {
				location, found, err := w.options.TableIndex.Lookup(id)
				if err != nil {
					return err
				}
				if found != original.found || found && location != original.location {
					w.logConflict("commit-index-key", attempt+1, fresh, table.ErrConflictingDataFiles)
					return fmt.Errorf("indexed row changed while this thread was writing; refusing stale positional deletes: %w", table.ErrConflictingDataFiles)
				}
			}
		}
		state, changed, err := mergeNativeRecovery(fresh.Properties()[nativeRecoveryProperty], w.options.ThreadID, finalMetadataState, hasFiles)
		if err != nil {
			return err
		}
		if !hasFiles && !changed {
			w.table = fresh
			w.closed = true
			return nil
		}
		tx := fresh.NewTransaction()
		if err := tx.AssertRefSnapshotID(table.MainBranch); err != nil {
			return err
		}
		if err := tx.AssertDefaultShape(); err != nil {
			return err
		}
		if changed {
			if err := tx.SetProperties(apacheiceberg.Properties{nativeRecoveryProperty: state}); err != nil {
				return err
			}
		}
		// Rebuild position deletes against the fresh table on every retry. A
		// replacement DV must retain every prior vector bit and retire exactly
		// the vector currently live on this base snapshot.
		positionFiles, replaced, err := w.writePositionDeletes(ctx, w.positions, fresh)
		if err != nil {
			return err
		}
		equalityFiles, err := w.writeEqualityDeletes(ctx, fresh)
		if err != nil {
			return err
		}
		if hasFiles {
			delta := tx.NewRowDelta(apacheiceberg.Properties{"olake.thread-id": w.options.ThreadID})
			delta.AddRows(w.dataFiles...).AddDeletes(equalityFiles...).AddDeletes(positionFiles...).RemoveDeletes(replaced...)
			if err := delta.Commit(ctx); err != nil {
				return err
			}
		}
		committed, commitErr := tx.Commit(ctx)
		if commitErr != nil {
			w.mutationError(commitErr)
			commitErr = nativeCatalogError("commit Iceberg table", commitErr)
			lastConflict = commitErr
			w.logConflict("commit", attempt+1, fresh, commitErr)
			unknown := !nativeMutationOutcomeKnown(commitErr)
			coordinated.invalidate(unknown)
			if unknown {
				w.uncertain = true
				logger.Warnf("Thread[%s]: native Iceberg commit outcome unknown; retaining staged work data_files=%d position_deletes=%d staged_paths=%d ctx_err=%v", w.options.ThreadID, len(w.dataFiles), len(w.positions), len(w.paths), ctx.Err())
				return fmt.Errorf("Iceberg commit outcome is unknown; staged files retained for recovery: %w", commitErr)
			}
			// Delete files belong to this rejected attempt. Only optimistic
			// conflicts retry here; other known rejections return and Abort
			// cleans the unpublished staged data without advancing recovery.
			w.removeFiles(ctx, positionFiles)
			w.removeFiles(ctx, equalityFiles)
			if !errors.Is(commitErr, table.ErrCommitFailed) {
				return commitErr
			}
			continue
		}
		w.table, w.closed = committed, true
		coordinated.latest = committed
		// A successful metadata commit owns all staged files from now on.
		// Never delete them, including when the local index checkpoint fails.
		clear(w.paths)
		if hasFiles && w.indexThread != nil {
			snapshot := committed.CurrentSnapshot()
			parent := int64(0)
			if snapshot.ParentSnapshotID != nil {
				parent = *snapshot.ParentSnapshotID
			}
			if parent == indexBase {
				if err := w.options.TableIndex.Commit(w.indexThread, &snapshot.SnapshotID); err != nil {
					return fmt.Errorf("Iceberg committed but local index checkpoint failed: %w", err)
				}
			}
		}
		w.indexThread.ReleaseObservability()
		w.indexThread = nil
		return nil
	}
	return fmt.Errorf("Iceberg commit retries exhausted: %w", lastConflict)
}

// Summary counters are authoritative when present; absent/unknown counters
// require manifest inspection rather than guessing that the table is clean.
func nativeMayHaveEqualityDeletes(tbl *table.Table) bool {
	snapshot := tbl.CurrentSnapshot()
	if snapshot == nil {
		return false
	}
	if snapshot.Summary != nil {
		properties := snapshot.Summary.Properties
		if properties["total-equality-deletes"] == "0" || properties["total-delete-files"] == "0" {
			return false
		}
	}
	return true
}

func (w *nativeWriter) migrateEqualityDeletes(ctx context.Context) error {
	// Called only during initialization, with the coordinator entry locked.
	state := w.coordinator
	for attempt := range nativeCommitAttempts {
		fresh, err := w.currentTable(ctx, state)
		if err != nil {
			return err
		}
		w.table = fresh
		if !nativeMayHaveEqualityDeletes(fresh) {
			return nil
		}
		positions, equalityFiles, err := collectNativeEqualityDeletePositions(ctx, fresh)
		if err != nil {
			return err
		}
		if len(equalityFiles) == 0 {
			return nil
		}
		files, replaced, err := w.writePositionDeletes(ctx, positions, fresh)
		if err != nil {
			return err
		}
		tx := fresh.NewTransaction()
		if err := tx.AssertRefSnapshotID(table.MainBranch); err != nil {
			return err
		}
		// iceberg-go's delete-file rewrite API does not allow changing content
		// kinds. Two staged snapshots in one transaction provide the same atomic
		// conversion without publishing an equality-free intermediate table.
		if err := tx.ReplaceFiles(ctx, nil, nil, equalityFiles, nil); err != nil {
			return err
		}
		if len(files) > 0 {
			if err := tx.NewRowDelta(nil).AddDeletes(files...).RemoveDeletes(replaced...).Commit(ctx); err != nil {
				return err
			}
		}
		updated, commitErr := tx.Commit(ctx)
		if commitErr == nil {
			w.table, state.latest = updated, updated
			clear(w.paths)
			return nil
		}
		w.mutationError(commitErr)
		commitErr = nativeCatalogError("migrate Iceberg equality deletes", commitErr)
		w.logConflict("equality-migration", attempt+1, fresh, commitErr)
		unknown := !nativeMutationOutcomeKnown(commitErr)
		state.invalidate(unknown)
		if unknown {
			w.uncertain = true
			return fmt.Errorf("Iceberg equality migration outcome is unknown: %w", commitErr)
		}
		w.removeFiles(ctx, files)
		if !errors.Is(commitErr, table.ErrCommitFailed) {
			return commitErr
		}
	}
	return fmt.Errorf("Iceberg equality migration retries exhausted: %w", table.ErrCommitFailed)
}

func nativeConflictClass(err error) string {
	if errors.Is(err, rest.ErrCommitStateUnknown) {
		return "commit_state_unknown"
	}
	var throttled *nativeRESTThrottleRejection
	if errors.As(err, &throttled) {
		return "catalog_throttled"
	}
	switch {
	case errors.Is(err, table.ErrConflictingDataFiles):
		return "concurrent_data"
	case errors.Is(err, table.ErrConflictingDeleteFiles):
		return "concurrent_deletes"
	case errors.Is(err, table.ErrDataFilesMissing):
		return "referenced_data_missing"
	case errors.Is(err, table.ErrCommitFailed):
		return "catalog_compare_and_swap"
	default:
		return "commit_state_unknown"
	}
}

func (w *nativeWriter) logConflict(operation string, attempt int, tbl *table.Table, err error) {
	// Never log the raw catalog error: REST errors may embed response bodies,
	// signed URLs or credentials. The classification and snapshot IDs suffice.
	logger.Warnf("Thread[%s]: native Iceberg %s attempt=%d base_snapshot=%d classification=%s", w.options.ThreadID, operation, attempt, nativeSnapshotID(tbl), nativeConflictClass(err))
}

// S3 Tables reserves the --table-s3 bucket suffix and removes unreferenced
// objects through managed maintenance; its object API does not support DELETE.
// Only writer-owned, uncommitted files may be handed off to that cleanup.
func (w *nativeWriter) discardStagedFile(fs iceio.IO, path string) error {
	if _, ours := w.paths[path]; !ours {
		return errors.New("refusing to discard a file not staged by this writer")
	}
	scheme, object, hasScheme := strings.Cut(path, "://")
	bucket, _, _ := strings.Cut(object, "/")
	managed := hasScheme && (scheme == "s3" || scheme == "s3a" || scheme == "s3n") && strings.HasSuffix(bucket, "--table-s3")
	if !managed {
		if err := fs.Remove(path); err != nil {
			return err
		}
	}
	delete(w.paths, path)
	return nil
}

func (w *nativeWriter) removeFiles(ctx context.Context, files []apacheiceberg.DataFile) {
	fs, err := w.table.FS(nativeCatalogContext(ctx, w.cat))
	if err != nil {
		return
	}
	for _, file := range files {
		_ = w.discardStagedFile(fs, file.FilePath())
	}
}

func (w *nativeWriter) Abort() {
	for _, f := range w.backfillFiles {
		f.abort()
	}
	clear(w.backfillFiles)
	if w.releaseSession != nil {
		w.releaseSession(false)
	}
	w.indexThread.ReleaseObservability()
	w.indexThread = nil
	w.closed = true
	if w.uncertain || len(w.paths) == 0 {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	fs, err := w.table.FS(nativeCatalogContext(ctx, w.cat))
	if err != nil {
		return
	}
	for path := range w.paths {
		_ = w.discardStagedFile(fs, path)
	}
}

type nativePartition []any

func (p nativePartition) Size() int              { return len(p) }
func (p nativePartition) Get(pos int) any        { return p[pos] }
func (p nativePartition) Set(pos int, value any) { p[pos] = value }

type nativeCountingWriter struct {
	writer io.Writer
	size   int64
}

func (w *nativeCountingWriter) Write(data []byte) (int, error) {
	n, err := w.writer.Write(data)
	w.size += int64(n)
	return n, err
}

func nativeArrowRecord(schema *apacheiceberg.Schema, rows []map[string]any) (arrow.RecordBatch, error) {
	arrowSchema, err := table.SchemaToArrowSchema(schema, nil, true, false)
	if err != nil {
		return nil, err
	}
	builder := array.NewRecordBuilder(memory.DefaultAllocator, arrowSchema)
	defer builder.Release()
	builder.Reserve(len(rows))
	for _, row := range rows {
		for idx, field := range arrowSchema.Fields() {
			value := row[field.Name]
			if value == nil {
				if !field.Nullable {
					return nil, fmt.Errorf("required Iceberg column %q is null", field.Name)
				}
				builder.Field(idx).AppendNull()
				continue
			}
			if err := appendNativeValue(builder.Field(idx), value); err != nil {
				return nil, fmt.Errorf("encode Iceberg column %q: %w", field.Name, err)
			}
		}
	}
	return builder.NewRecordBatch(), nil
}

func appendNativeValue(builder array.Builder, value any) error {
	switch b := builder.(type) {
	case *array.BooleanBuilder:
		v, err := typeutils.ReformatBool(value)
		if err != nil {
			return err
		}
		b.Append(v)
	case *array.Int32Builder:
		v, err := typeutils.ReformatInt32(value)
		if err != nil {
			return err
		}
		b.Append(v)
	case *array.Int64Builder:
		v, err := typeutils.ReformatInt64(value)
		if err != nil {
			return err
		}
		b.Append(v)
	case *array.Float32Builder:
		v, err := typeutils.ReformatFloat32(value)
		if err != nil {
			return err
		}
		b.Append(v)
	case *array.Float64Builder:
		v, err := typeutils.ReformatFloat64(value)
		if err != nil {
			return err
		}
		b.Append(v)
	case *array.TimestampBuilder:
		v, err := typeutils.ReformatDate(value, true)
		if err != nil {
			return err
		}
		ts, err := arrow.TimestampFromTime(v, b.Type().(*arrow.TimestampType).Unit)
		if err != nil {
			return err
		}
		b.Append(ts)
	case *array.BinaryBuilder:
		v, err := typeutils.ReformatBytes(value)
		if err != nil {
			return err
		}
		b.Append(v)
	case *array.FixedSizeBinaryBuilder:
		v, err := typeutils.ReformatBytes(value)
		if err != nil {
			return err
		}
		if len(v) != b.Type().(*arrow.FixedSizeBinaryType).ByteWidth {
			return fmt.Errorf("fixed binary width mismatch: expected %d bytes, got %d", b.Type().(*arrow.FixedSizeBinaryType).ByteWidth, len(v))
		}
		b.Append(v)
	case *array.StringBuilder:
		switch v := value.(type) {
		case map[string]any, []any:
			encoded, err := json.Marshal(v)
			if err != nil {
				return err
			}
			b.Append(string(encoded))
		default:
			b.Append(utils.ConvertToString(v))
		}
	default:
		return fmt.Errorf("unsupported Iceberg Arrow column type %s", builder.Type())
	}
	return nil
}

// nativePartitionLiteral represents SQL NULL as an invalid optional, not an error.
func nativePartitionLiteral(field apacheiceberg.NestedField, value any) (apacheiceberg.Optional[apacheiceberg.Literal], error) {
	if value == nil {
		return apacheiceberg.Optional[apacheiceberg.Literal]{}, nil
	}
	switch field.Type.(type) {
	case apacheiceberg.BooleanType:
		v, err := typeutils.ReformatBool(value)
		if err != nil {
			return apacheiceberg.Optional[apacheiceberg.Literal]{}, err
		}
		return apacheiceberg.Optional[apacheiceberg.Literal]{Val: apacheiceberg.NewLiteral(v), Valid: true}, nil
	case apacheiceberg.Int32Type:
		v, err := typeutils.ReformatInt32(value)
		if err != nil {
			return apacheiceberg.Optional[apacheiceberg.Literal]{}, err
		}
		return apacheiceberg.Optional[apacheiceberg.Literal]{Val: apacheiceberg.NewLiteral(v), Valid: true}, nil
	case apacheiceberg.Int64Type:
		v, err := typeutils.ReformatInt64(value)
		if err != nil {
			return apacheiceberg.Optional[apacheiceberg.Literal]{}, err
		}
		return apacheiceberg.Optional[apacheiceberg.Literal]{Val: apacheiceberg.NewLiteral(v), Valid: true}, nil
	case apacheiceberg.Float32Type:
		v, err := typeutils.ReformatFloat32(value)
		if err != nil {
			return apacheiceberg.Optional[apacheiceberg.Literal]{}, err
		}
		return apacheiceberg.Optional[apacheiceberg.Literal]{Val: apacheiceberg.NewLiteral(v), Valid: true}, nil
	case apacheiceberg.Float64Type:
		v, err := typeutils.ReformatFloat64(value)
		if err != nil {
			return apacheiceberg.Optional[apacheiceberg.Literal]{}, err
		}
		return apacheiceberg.Optional[apacheiceberg.Literal]{Val: apacheiceberg.NewLiteral(v), Valid: true}, nil
	case apacheiceberg.TimestampType, apacheiceberg.TimestampTzType:
		v, err := typeutils.ReformatDate(value, true)
		if err != nil {
			return apacheiceberg.Optional[apacheiceberg.Literal]{}, err
		}
		return apacheiceberg.Optional[apacheiceberg.Literal]{Val: apacheiceberg.NewLiteral(apacheiceberg.Timestamp(v.UnixMicro())), Valid: true}, nil
	case apacheiceberg.BinaryType, apacheiceberg.FixedType:
		v, err := typeutils.ReformatBytes(value)
		if err != nil {
			return apacheiceberg.Optional[apacheiceberg.Literal]{}, err
		}
		return apacheiceberg.Optional[apacheiceberg.Literal]{Val: apacheiceberg.NewLiteral(v), Valid: true}, nil
	case apacheiceberg.StringType:
		return apacheiceberg.Optional[apacheiceberg.Literal]{Val: apacheiceberg.NewLiteral(utils.ConvertToString(value)), Valid: true}, nil
	default:
		return apacheiceberg.Optional[apacheiceberg.Literal]{}, fmt.Errorf("unsupported partition source type %s", field.Type)
	}
}

func nativeRecordPartition(schema *apacheiceberg.Schema, spec apacheiceberg.PartitionSpec, row map[string]any) (string, map[int]any, error) {
	if spec.NumFields() == 0 {
		return "", nil, nil
	}
	partition := make(nativePartition, spec.NumFields())
	values := make(map[int]any, spec.NumFields())
	for idx, part := range spec.Fields() {
		field, ok := schema.FindFieldByID(part.SourceID())
		if !ok {
			return "", nil, fmt.Errorf("partition source field ID %d is missing", part.SourceID())
		}
		lit, err := nativePartitionLiteral(field, row[field.Name])
		if err != nil {
			return "", nil, fmt.Errorf("partition column %q: %w", field.Name, err)
		}
		transformed := part.Transform.Apply(lit)
		if transformed.Valid {
			partition[idx] = transformed.Val.Any()
		}
		values[part.FieldID] = partition[idx]
	}
	// Human partition paths intentionally conflate null and the string "null".
	return fmt.Sprintf("%#v", partition), values, nil
}

// Files are streamed directly through the catalog's native FileIO. The Parquet
// footer and the object itself are closed before a RowLocation is made visible.
func (w *nativeWriter) writeNativeParquet(ctx context.Context, schema *apacheiceberg.Schema, spec apacheiceberg.PartitionSpec, partition map[int]any, batch arrow.RecordBatch, content apacheiceberg.ManifestEntryContent, equalityIDs []int, referenced *string) (_ apacheiceberg.DataFile, err error) {
	return w.writeNativeParquetStream(ctx, schema, spec, partition, batch.Schema(), func(writer *pqarrow.FileWriter) error { return writer.Write(batch) }, content, equalityIDs, referenced)
}

// Streaming callers borrow each input batch only for the duration of write.
// The shared lifecycle always closes the footer and FileIO upload before return.
func (w *nativeWriter) writeNativeParquetStream(ctx context.Context, schema *apacheiceberg.Schema, spec apacheiceberg.PartitionSpec, partition map[int]any, arrowSchema *arrow.Schema, write func(*pqarrow.FileWriter) error, content apacheiceberg.ManifestEntryContent, equalityIDs []int, referenced *string) (_ apacheiceberg.DataFile, err error) {
	ctx = nativeCatalogContext(ctx, w.cat)
	fs, err := w.table.FS(ctx)
	if err != nil {
		return nil, err
	}
	writeFS, ok := fs.(iceio.WriteFileIO)
	if !ok {
		return nil, table.ErrWriteIORequired
	}
	provider, err := w.table.LocationProvider()
	if err != nil {
		return nil, err
	}
	name := uuid.NewString() + ".parquet"
	if spec.NumFields() > 0 {
		values := make(nativePartition, spec.NumFields())
		for idx, field := range spec.Fields() {
			values[idx] = partition[field.FieldID]
		}
		name = spec.PartitionToPath(values, w.table.Schema()) + "/" + name
	}
	path := provider.NewDataLocation(name)
	w.paths[path] = struct{}{}
	file, err := writeFS.Create(path)
	if err != nil {
		return nil, err
	}
	counter := &nativeCountingWriter{writer: file}
	props := parquet.NewWriterProperties(parquet.WithCompression(compress.Codecs.Zstd), parquet.WithCompressionLevel(3), parquet.WithStats(true), parquet.WithMaxRowGroupLength(64*1024), parquet.WithRootName("table"), parquet.WithPageIndexEnabled(true))
	writer, err := pqarrow.NewFileWriter(arrowSchema, counter, props, pqarrow.DefaultWriterProps())
	if err != nil {
		return nil, errors.Join(err, file.Close())
	}
	schemaJSON, err := json.Marshal(schema)
	if err == nil {
		err = writer.AppendKeyValueMetadata("iceberg.schema", string(schemaJSON))
	}
	if err == nil && content == apacheiceberg.EntryContentEqDeletes {
		err = writer.AppendKeyValueMetadata("delete-type", "equality")
		if err == nil {
			ids := make([]string, len(equalityIDs))
			for idx, id := range equalityIDs {
				ids[idx] = strconv.Itoa(id)
			}
			err = writer.AppendKeyValueMetadata("delete-field-ids", strings.Join(ids, ","))
		}
	}
	if err == nil {
		err = write(writer)
	}
	err = errors.Join(err, writer.Close(), file.Close())
	if err != nil {
		return nil, err
	}
	metadata, err := writer.FileMetadata()
	if err != nil {
		return nil, err
	}
	// Delete files project away partition sources, but the metadata builder
	// still needs their types (notably timestamp/date logical types).
	metadataSchema := schema
	if content != apacheiceberg.EntryContentData && spec.NumFields() > 0 {
		fields := schema.Fields()
		for _, part := range spec.Fields() {
			if slices.ContainsFunc(fields, func(field apacheiceberg.NestedField) bool { return field.ID == part.SourceID() }) {
				continue
			}
			for _, tableSchema := range w.table.Schemas() {
				if source, present := tableSchema.FindFieldByID(part.SourceID()); present {
					source.Name = fmt.Sprintf("__olake_partition_source_%d", source.ID)
					fields = append(fields, source)
					break
				}
			}
		}
		metadataSchema = apacheiceberg.NewSchema(schema.ID, fields...)
	}
	return table.DataFileFromMetadata(table.DataFileArgs{Schema: metadataSchema, Spec: spec, Format: apacheiceberg.ParquetFile, Metadata: metadata, FilePath: path, FileSize: counter.size, Content: content, PartitionValues: partition, Properties: w.table.Properties(), EqualityFieldIDs: equalityIDs, FormatVersion: w.table.Metadata().Version(), ReferencedDataFile: referenced})
}

func nativeLiveEntries(ctx context.Context, tbl *table.Table) ([]apacheiceberg.ManifestEntry, error) {
	if tbl.CurrentSnapshot() == nil {
		return nil, nil
	}
	fs, err := tbl.FS(ctx)
	if err != nil {
		return nil, err
	}
	manifests, err := tbl.CurrentSnapshot().Manifests(fs)
	if err != nil {
		return nil, err
	}
	var entries []apacheiceberg.ManifestEntry
	for _, manifest := range manifests {
		for entry, err := range manifest.Entries(fs, true) {
			if err != nil {
				return nil, err
			}
			entries = append(entries, entry)
		}
	}
	return entries, nil
}

// All prior position deletes are folded into a replacement DV, not just its
// prior vector. This also supports a v2 table upgraded after positional writes.
func nativeDeletedPositions(ctx context.Context, tbl *table.Table, path string) ([]int64, []apacheiceberg.DataFile, error) {
	entries, err := nativeLiveEntries(ctx, tbl)
	if err != nil {
		return nil, nil, err
	}
	bitmaps, replaced, err := nativePriorPositionDeletes(ctx, tbl, entries, map[string][]int64{path: nil})
	if err != nil {
		return nil, nil, err
	}
	var result []int64
	if bitmap := bitmaps[path]; bitmap != nil {
		for position := range bitmap.Positions() {
			if position > math.MaxInt64 {
				return nil, nil, errors.New("native delete position exceeds signed row position range")
			}
			result = append(result, int64(position))
		}
	}
	return result, replaced[path], nil
}

func nativePriorPositionDeletes(ctx context.Context, tbl *table.Table, entries []apacheiceberg.ManifestEntry, targets map[string][]int64) (map[string]*dv.RoaringPositionBitmap, map[string][]apacheiceberg.DataFile, error) {
	fs, err := tbl.FS(ctx)
	if err != nil {
		return nil, nil, err
	}
	bitmaps := make(map[string]*dv.RoaringPositionBitmap)
	replaced := make(map[string][]apacheiceberg.DataFile)
	for _, entry := range entries {
		file := entry.DataFile()
		if file.ContentType() != apacheiceberg.EntryContentPosDeletes {
			continue
		}
		if ref := file.ReferencedDataFile(); ref != nil {
			if _, wanted := targets[*ref]; !wanted {
				continue
			}
		}
		if table.IsDeletionVector(file) {
			ref := file.ReferencedDataFile()
			if ref == nil {
				return nil, nil, errors.New("deletion vector has no referenced data file")
			}
			bitmap, err := dv.ReadDV(fs, file)
			if err != nil {
				return nil, nil, err
			}
			if bitmaps[*ref] == nil {
				bitmaps[*ref] = dv.NewRoaringPositionBitmap()
			}
			bitmaps[*ref].Or(bitmap)
			replaced[*ref] = append(replaced[*ref], file)
			continue
		}
		rows, err := readNativePositionDeletes(ctx, tbl, file.FilePath())
		if err != nil {
			return nil, nil, err
		}
		for path, positions := range rows {
			if _, wanted := targets[path]; !wanted {
				continue
			}
			if bitmaps[path] == nil {
				bitmaps[path] = dv.NewRoaringPositionBitmap()
			}
			for _, position := range positions {
				if position < 0 {
					return nil, nil, errors.New("negative position in existing delete file")
				}
				bitmaps[path].Set(uint64(position))
			}
		}
	}
	return bitmaps, replaced, nil
}

func (w *nativeWriter) writePositionDeletes(ctx context.Context, positions map[string][]int64, tbl *table.Table) ([]apacheiceberg.DataFile, []apacheiceberg.DataFile, error) {
	if len(positions) == 0 {
		return nil, nil, nil
	}
	entries, err := nativeLiveEntries(ctx, tbl)
	if err != nil {
		return nil, nil, err
	}
	dataFiles := make(map[string]apacheiceberg.DataFile)
	for _, entry := range entries {
		file := entry.DataFile()
		if file.ContentType() == apacheiceberg.EntryContentData {
			dataFiles[file.FilePath()] = file
		}
	}
	for _, file := range w.dataFiles {
		dataFiles[file.FilePath()] = file
	}
	fs, err := tbl.FS(ctx)
	if err != nil {
		return nil, nil, err
	}
	writeFS, ok := fs.(iceio.WriteFileIO)
	if !ok {
		return nil, nil, table.ErrWriteIORequired
	}
	var prior map[string]*dv.RoaringPositionBitmap
	var oldVectors map[string][]apacheiceberg.DataFile
	if w.deleteMode == types.UpdateTypeDeletionVector {
		prior, oldVectors, err = nativePriorPositionDeletes(ctx, tbl, entries, positions)
		if err != nil {
			return nil, nil, err
		}
	}
	var files, replaced []apacheiceberg.DataFile
	for _, path := range slices.Sorted(maps.Keys(positions)) {
		file, found := dataFiles[path]
		if !found {
			return nil, nil, fmt.Errorf("%w: a positional delete references an unavailable data file", table.ErrDataFilesMissing)
		}
		spec := tbl.Metadata().PartitionSpecByID(int(file.SpecID()))
		if spec == nil {
			return nil, nil, fmt.Errorf("missing partition spec %d", file.SpecID())
		}
		pending := slices.Clone(positions[path])
		slices.Sort(pending)
		pending = slices.Compact(pending)
		if len(pending) == 0 {
			continue
		}
		for _, position := range pending {
			if position < 0 || position >= file.Count() {
				return nil, nil, errors.New("positional delete ordinal is outside its data file")
			}
		}
		if w.deleteMode == types.UpdateTypeDeletionVector {
			vector := dv.NewDVWriter(writeFS, func(id int32) *apacheiceberg.PartitionSpec { return tbl.Metadata().PartitionSpecByID(int(id)) })
			if err := vector.Add(path, pending, file.SpecID(), file.Partition()); err != nil {
				return nil, nil, err
			}
			vector.Load(path, prior[path], file.SpecID(), file.Partition())
			provider, err := tbl.LocationProvider()
			if err != nil {
				return nil, nil, err
			}
			vectorPath := provider.NewMetadataLocation(uuid.NewString() + ".puffin")
			w.paths[vectorPath] = struct{}{}
			created, err := vector.Flush(ctx, vectorPath)
			if err != nil {
				return nil, nil, err
			}
			files = append(files, created...)
			replaced = append(replaced, oldVectors[path]...)
		} else {
			rows := make([]map[string]any, len(pending))
			for idx, position := range pending {
				rows[idx] = map[string]any{"file_path": path, "pos": position}
			}
			batch, err := nativeArrowRecord(apacheiceberg.PositionalDeleteSchema, rows)
			if err != nil {
				return nil, nil, err
			}
			created, err := w.writeNativeParquet(ctx, apacheiceberg.PositionalDeleteSchema, *spec, file.Partition(), batch, apacheiceberg.EntryContentPosDeletes, nil, &path)
			batch.Release()
			if err != nil {
				return nil, nil, err
			}
			files = append(files, created)
		}
	}
	return files, replaced, nil
}

// Equality mode has no row index. A changed partition column cannot tell us
// the old row's partition, so scope the key deletes to every live partition
// (including historical specs). Files added in this commit have the same data
// sequence as these deletes and are therefore not equality-deleted.
func (w *nativeWriter) writeEqualityDeletes(ctx context.Context, tbl *table.Table) ([]apacheiceberg.DataFile, error) {
	if !w.upsert || w.indexThread != nil || len(w.latest) == 0 {
		return nil, nil
	}
	entries, err := nativeLiveEntries(ctx, tbl)
	if err != nil {
		return nil, err
	}
	partitions := make(map[string]apacheiceberg.DataFile)
	for _, entry := range entries {
		file := entry.DataFile()
		if file.ContentType() == apacheiceberg.EntryContentData {
			partitions[fmt.Sprintf("%d:%#v", file.SpecID(), file.Partition())] = file
		}
	}
	if len(partitions) == 0 {
		return nil, nil
	}
	field, ok := tbl.Schema().FindFieldByName(constants.OlakeID)
	if !ok {
		return nil, errors.New("Iceberg identifier column missing")
	}
	schema := apacheiceberg.NewSchema(tbl.Schema().ID, field)
	rows := make([]map[string]any, 0, len(w.latest))
	for _, id := range slices.Sorted(maps.Keys(w.latest)) {
		rows = append(rows, map[string]any{constants.OlakeID: id})
	}
	batch, err := nativeArrowRecord(schema, rows)
	if err != nil {
		return nil, err
	}
	defer batch.Release()
	files := make([]apacheiceberg.DataFile, 0, len(partitions))
	for _, key := range slices.Sorted(maps.Keys(partitions)) {
		file := partitions[key]
		spec := tbl.Metadata().PartitionSpecByID(int(file.SpecID()))
		if spec == nil {
			return nil, fmt.Errorf("missing equality delete partition spec %d", file.SpecID())
		}
		created, err := w.writeNativeParquet(ctx, schema, *spec, file.Partition(), batch, apacheiceberg.EntryContentEqDeletes, []int{field.ID}, nil)
		if err != nil {
			return nil, err
		}
		files = append(files, created)
	}
	return files, nil
}

type Writer interface {
	Write(ctx context.Context, records []types.RawRecord) error
	EvolveSchema(ctx context.Context, newSchema map[string]string) error
	AlterSourceColumns(ctx context.Context, added []*nativeSourceColumn, dropped []string) error
	schemaMap() map[string]string
	Close(ctx context.Context, finalMetadataState any) error
	// Abort discards whatever the writer staged outside Iceberg, for the paths
	// that give up before Close can commit.
	Abort()
}
