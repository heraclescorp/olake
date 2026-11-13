package iceberg

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"math"
	"slices"
	"strconv"
	"strings"
	"time"

	apacheiceberg "github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/table"
	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/destination"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils/logger"
	"github.com/datazip-inc/olake/utils/typeutils"
)

type nativeSourceColumn struct {
	Key            string
	IceType        string
	InitialDefault apacheiceberg.Literal
}

// ApplySourceColumns adds selected source columns missing from the table and, when the stream
// enables drop_removed_columns, deletes table columns the source no longer has.
func (i *Iceberg) ApplySourceColumns(ctx context.Context, globalSchema any, change types.SourceColumnsChange) (any, error) {
	if !i.stream.NormalizationEnabled() {
		return globalSchema, nil
	}

	if i.syncStartColumns == nil {
		i.syncStartColumns = make(map[string]struct{})
	}
	added, dropped := planSourceColumns(i.stream, i.schema, change, i.syncStartColumns)
	if len(added) == 0 && len(dropped) == 0 {
		return globalSchema, nil
	}

	if err := i.writer.AlterSourceColumns(ctx, added, dropped); err != nil {
		return nil, destination.WriteFailure(fmt.Errorf("failed to alter source columns: %w", err))
	}
	if i.unavailableValues != nil {
		changed := dropped
		for _, column := range added {
			changed = append(changed, column.Key)
		}
		i.unavailableValues.InvalidateColumns(changed)
	}
	schemaAfterAlter := i.writer.schemaMap()
	i.schema = copySchema(schemaAfterAlter)
	return schemaAfterAlter, nil
}

// planSourceColumns returns the columns to add to and drop from a table whose current schema is
// schema. A sync-start change only adds columns whose existing rows read NULL (or an unknown
// value): it is not ordered with the pending changes, which may still carry rows from before an
// add with a default, or values of a column dropped since. syncStartColumns tracks the columns a
// sync-start change added: the change stream may still describe the table from before they
// existed, so they are not dropped until a change-stream change lists them.
func planSourceColumns(stream types.StreamInterface, schema map[string]string, change types.SourceColumnsChange, syncStartColumns map[string]struct{}) ([]*nativeSourceColumn, []string) {
	isSelected := stream.IsSelectedColumn()
	sourceColumns := types.NewSet[string]()
	for _, name := range change.KeepColumns {
		sourceColumns.Insert(stream.ResolveColumnName(name))
	}

	var added []*nativeSourceColumn
	for _, column := range change.Columns {
		key := stream.ResolveColumnName(column.Name)
		sourceColumns.Insert(key)
		if !change.SyncStart {
			delete(syncStartColumns, key)
		}
		if _, exists := schema[key]; exists || !isSelected(key) {
			continue
		}

		iceType := column.Type.ToIceberg()
		switch {
		case column.ExistingRowValueUnknown:
			logger.Warnf("stream[%s]: adding column[%s] type[%s] with NULL for existing rows: the source's value for rows stored before the column was added is unknown (volatile default, table rewritten, or column no longer exists)", stream.ID(), key, iceType)
			added = append(added, &nativeSourceColumn{Key: key, IceType: iceType})
		case column.ExistingRowValue == nil:
			logger.Infof("stream[%s]: adding column[%s] type[%s], existing rows read NULL", stream.ID(), key, iceType)
			added = append(added, &nativeSourceColumn{Key: key, IceType: iceType})
		case change.SyncStart:
			logger.Infof("stream[%s]: column[%s] type[%s] with existing-row value[%v] will be added when its change arrives in the change stream", stream.ID(), key, iceType, column.ExistingRowValue)
		default:
			initialDefault, err := nativeSourceColumnDefault(iceType, column.ExistingRowValue)
			if err != nil {
				logger.Warnf("stream[%s]: adding column[%s] type[%s] with NULL for existing rows: failed to encode existing-row value[%v]: %s", stream.ID(), key, iceType, column.ExistingRowValue, err)
				added = append(added, &nativeSourceColumn{Key: key, IceType: iceType})
				continue
			}
			logger.Infof("stream[%s]: adding column[%s] type[%s], existing rows read [%v]", stream.ID(), key, iceType, column.ExistingRowValue)
			added = append(added, &nativeSourceColumn{Key: key, IceType: iceType, InitialDefault: initialDefault.Val})
		}
	}

	if change.SyncStart {
		for _, column := range added {
			syncStartColumns[column.Key] = struct{}{}
		}
		return added, nil
	}

	olakeColumns := olakeColumnNames(stream)
	var dropped []string
	for _, key := range slices.Sorted(maps.Keys(schema)) {
		if _, addedAtSyncStart := syncStartColumns[key]; addedAtSyncStart || sourceColumns.Exists(key) || olakeColumns.Exists(key) {
			continue
		}
		if !stream.DropRemovedColumnsEnabled() {
			logger.Infof("stream[%s]: column[%s] no longer exists at the source; keeping it, new rows hold NULL (drop_removed_columns is off)", stream.ID(), key)
			continue
		}
		logger.Infof("stream[%s]: dropping column[%s] removed at the source", stream.ID(), key)
		dropped = append(dropped, key)
	}
	return added, dropped
}

// olakeColumnNames returns the table columns OLake writes itself, which no source has.
func olakeColumnNames(stream types.StreamInterface) *types.Set[string] {
	names := types.NewSet[string](constants.OlakeID, constants.StringifiedData)
	for name := range types.RawSchema {
		names.Insert(name)
	}
	stream.Schema().Properties.Range(func(key, value any) bool {
		if property, ok := value.(*types.Property); ok && property.OlakeColumn {
			names.Insert(stream.ResolveColumnName(key.(string)))
		}
		return true
	})
	return names
}

// Native schemas are built in a stable order only at creation. Once a table
// exists, its catalog-assigned IDs are authoritative, including after evolution.
func nativeSchema(columns map[string]string, identifiers bool) (*apacheiceberg.Schema, error) {
	fields := make([]apacheiceberg.NestedField, 0, len(columns))
	var identifierIDs []int
	for _, name := range slices.Sorted(maps.Keys(columns)) {
		fieldJSON, err := json.Marshal(map[string]any{"id": len(fields) + 1, "name": name, "type": columns[name], "required": name == constants.OlakeID})
		if err != nil {
			return nil, err
		}
		var field apacheiceberg.NestedField
		if err := json.Unmarshal(fieldJSON, &field); err != nil {
			return nil, fmt.Errorf("column %q has invalid Iceberg type: %w", name, err)
		}
		fields = append(fields, field)
		if identifiers && name == constants.OlakeID {
			identifierIDs = append(identifierIDs, field.ID)
		}
	}
	return apacheiceberg.NewSchemaWithIdentifiers(0, identifierIDs, fields...), nil
}

func nativeStreamColumns(stream types.StreamInterface) (map[string]string, error) {
	partitionInfo, err := parsePartitionRegex(stream.GetPartitionRegex(), stream.ResolveColumnName)
	if err != nil {
		return nil, err
	}
	var extra []string
	if !stream.NormalizationEnabled() {
		for _, part := range partitionInfo {
			extra = append(extra, part.Field)
		}
	}
	columns := make(map[string]string)
	for _, field := range stream.Schema().ToIceberg(!stream.NormalizationEnabled(), stream, extra...) {
		columns[field.Key] = field.IceType
	}
	return columns, nil
}

func nativePartitionSpec(stream types.StreamInterface, schema *apacheiceberg.Schema) (apacheiceberg.PartitionSpec, error) {
	parts, err := parsePartitionRegex(stream.GetPartitionRegex(), stream.ResolveColumnName)
	if err != nil {
		return apacheiceberg.PartitionSpec{}, err
	}
	fields := make([]apacheiceberg.PartitionField, 0, len(parts))
	for idx, part := range parts {
		field, found := schema.FindFieldByName(part.SchemaField)
		if !found {
			return apacheiceberg.PartitionSpec{}, fmt.Errorf("partition column %q is absent from table schema", part.SchemaField)
		}
		transformName := strings.ToLower(part.Transform)
		switch transformName {
		case "year", "years":
			transformName = "year"
		case "month", "months":
			transformName = "month"
		case "day", "days":
			transformName = "day"
		case "hour", "hours":
			transformName = "hour"
		}
		transform, err := apacheiceberg.ParseTransform(transformName)
		if err != nil {
			return apacheiceberg.PartitionSpec{}, fmt.Errorf("partition column %q: %w", part.SchemaField, err)
		}
		if !transform.CanTransform(field.Type) {
			return apacheiceberg.PartitionSpec{}, fmt.Errorf("transform %s cannot partition column %q of type %s", transform, field.Name, field.Type)
		}
		name := field.Name
		if transform.String() != "identity" {
			name += "_" + strings.NewReplacer("[", "_", "]", "").Replace(transform.String())
		}
		fields = append(fields, apacheiceberg.PartitionField{SourceIDs: []int{field.ID}, FieldID: 1000 + idx, Name: name, Transform: transform})
	}
	return apacheiceberg.NewPartitionSpec(fields...), nil
}

func (w *nativeWriter) schemaMap() map[string]string {
	columns := make(map[string]string, w.table.Schema().NumFields())
	for _, field := range w.table.Schema().Fields() {
		columns[field.Name] = field.Type.String()
	}
	return columns
}

// nativeSourceLiteral retains the declared timezone type: iceberg-go represents
// both timestamp and timestamptz values with TimestampLiteral, whose Type is
// always timestamp, while AddColumn requires the exact declared type.
type nativeSourceLiteral struct {
	apacheiceberg.Literal
	fieldType apacheiceberg.Type
}

func (l nativeSourceLiteral) Type() apacheiceberg.Type { return l.fieldType }

// nativeSourceColumnDefault distinguishes a NULL default (invalid optional) from
// a value that cannot be encoded (error), preserving NULL and typed defaults.
func nativeSourceColumnDefault(iceType string, value any) (apacheiceberg.Optional[apacheiceberg.Literal], error) {
	if value == nil {
		return apacheiceberg.Optional[apacheiceberg.Literal]{}, nil
	}
	if pointer, ok := value.(*any); ok {
		if pointer == nil {
			return apacheiceberg.Optional[apacheiceberg.Literal]{}, nil
		}
		return nativeSourceColumnDefault(iceType, *pointer)
	}
	schema, err := nativeSchema(map[string]string{"default": iceType}, false)
	if err != nil {
		return apacheiceberg.Optional[apacheiceberg.Literal]{}, err
	}
	field := schema.Field(0)
	// Row conversions deliberately tolerate truncation and timestamp fallback;
	// source defaults must instead fail so the planner can choose NULL.
	switch field.Type.(type) {
	case apacheiceberg.Int32Type, apacheiceberg.Int64Type:
		bits := 64
		if _, ok := field.Type.(apacheiceberg.Int32Type); ok {
			bits = 32
		}
		switch number := value.(type) {
		case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, json.Number:
			parsed, err := strconv.ParseInt(fmt.Sprint(number), 10, bits)
			if err != nil {
				return apacheiceberg.Optional[apacheiceberg.Literal]{}, err
			}
			value = parsed
		case float32, float64:
			parsed, err := typeutils.ReformatFloat64(number)
			if err != nil {
				return apacheiceberg.Optional[apacheiceberg.Literal]{}, err
			}
			outside := parsed < -0x1p63 || parsed >= 0x1p63
			if bits == 32 {
				outside = parsed < math.MinInt32 || parsed > math.MaxInt32
			}
			if math.IsNaN(parsed) || math.IsInf(parsed, 0) || math.Trunc(parsed) != parsed || outside {
				return apacheiceberg.Optional[apacheiceberg.Literal]{}, fmt.Errorf("source default %v is not a valid %s", value, iceType)
			}
		}
	case apacheiceberg.TimestampType, apacheiceberg.TimestampTzType:
		if bytes, ok := value.([]byte); ok {
			value = string(bytes)
		}
		if text, ok := value.(string); ok {
			var parsed time.Time
			var parseErr error
			for _, layout := range typeutils.DateTimeFormats {
				parsed, parseErr = time.Parse(layout, text)
				if parseErr == nil {
					break
				}
			}
			if parseErr != nil {
				return apacheiceberg.Optional[apacheiceberg.Literal]{}, parseErr
			}
			value = parsed
		}
		if stamp, ok := value.(time.Time); ok && (stamp.Year() < 1 || stamp.Year() > 9999) {
			return apacheiceberg.Optional[apacheiceberg.Literal]{}, fmt.Errorf("source timestamp default is outside years 1 through 9999")
		}
	}
	if _, ok := field.Type.(apacheiceberg.StringType); ok {
		switch value.(type) {
		case map[string]any, []any:
			encoded, err := json.Marshal(value)
			if err != nil {
				return apacheiceberg.Optional[apacheiceberg.Literal]{}, err
			}
			value = string(encoded)
		}
	}
	optional, err := nativePartitionLiteral(field, value)
	if err != nil {
		return apacheiceberg.Optional[apacheiceberg.Literal]{}, err
	}
	literal := optional.Val
	switch number := literal.Any().(type) {
	case float32:
		if math.IsInf(float64(number), 0) || math.IsNaN(float64(number)) {
			return apacheiceberg.Optional[apacheiceberg.Literal]{}, fmt.Errorf("source default %v is not a finite %s", value, iceType)
		}
	case float64:
		if math.IsInf(number, 0) || math.IsNaN(number) {
			return apacheiceberg.Optional[apacheiceberg.Literal]{}, fmt.Errorf("source default %v is not a finite %s", value, iceType)
		}
	}
	literal, err = literal.To(field.Type)
	if err != nil {
		return apacheiceberg.Optional[apacheiceberg.Literal]{}, err
	}
	if !literal.Type().Equals(field.Type) {
		literal = nativeSourceLiteral{Literal: literal, fieldType: field.Type}
	}
	return apacheiceberg.Optional[apacheiceberg.Literal]{Val: literal, Valid: true}, nil
}

// AlterSourceColumns applies source additions and opt-in drops atomically.
// UnionByNameWith accepts initial-only defaults and assigns authoritative IDs;
// unlike AddColumn, it does not manufacture a write default that must be cleared.
func (w *nativeWriter) AlterSourceColumns(ctx context.Context, added []*nativeSourceColumn, dropped []string) error {
	if err := w.writable(); err != nil {
		return err
	}
	if len(added) == 0 && len(dropped) == 0 {
		return nil
	}
	ctx = nativeCatalogContext(ctx, w.cat)
	ctx, state, err := w.lockTable(ctx)
	if err != nil {
		return err
	}
	defer state.mu.Unlock()
	for attempt := range nativeCommitAttempts {
		fresh, err := w.currentTable(ctx, state)
		if err != nil {
			return err
		}
		tx := fresh.NewTransaction()
		if err := tx.AssertRefSnapshotID(table.MainBranch); err != nil {
			return err
		}
		if err := tx.AssertDefaultShape(); err != nil {
			return err
		}
		update := tx.UpdateSchema(true, false)
		changed := false
		var additions []apacheiceberg.NestedField
		for _, column := range added {
			if _, found := fresh.Schema().FindFieldByName(column.Key); found {
				continue
			}
			schema, err := nativeSchema(map[string]string{column.Key: column.IceType}, false)
			if err != nil {
				return err
			}
			field := schema.Field(0)
			field.ID = len(additions) + 1
			field.Required = false
			if column.InitialDefault != nil {
				if fresh.Metadata().Version() < 3 {
					logger.Warnf("Iceberg table %s: adding column %s with NULL for existing rows: format version %d does not support column defaults", fresh.Identifier(), column.Key, fresh.Metadata().Version())
				} else {
					// Upstream iceberg-go v0.7.0 remains unpatched: default codecs can
					// round integers above 2^53 and mishandle temporal defaults.
					field.InitialDefault = column.InitialDefault.Any()
					// TimestampLiteral represents both timezone variants, but the
					// SDK's default validator treats its native type as timestamp.
					// Its supported JSON numeric form retains exact epoch micros.
					if _, timezone := field.Type.(apacheiceberg.TimestampTzType); timezone {
						field.InitialDefault = json.Number(strconv.FormatInt(int64(field.InitialDefault.(apacheiceberg.Timestamp)), 10))
					}
				}
			}
			additions = append(additions, field)
		}
		if len(additions) != 0 {
			update.UnionByNameWith(apacheiceberg.NewSchema(0, additions...))
			changed = true
		}
		protected := make(map[int]struct{})
		for _, id := range fresh.Schema().IdentifierFieldIDs {
			protected[id] = struct{}{}
		}
		for _, spec := range fresh.Metadata().PartitionSpecs() {
			for _, part := range spec.Fields() {
				for _, id := range part.SourceIDs {
					protected[id] = struct{}{}
				}
			}
		}
		for _, sort := range fresh.SortOrder().Fields() {
			for _, id := range sort.SourceIDs {
				protected[id] = struct{}{}
			}
		}
		for _, name := range dropped {
			field, found := fresh.Schema().FindFieldByName(name)
			if !found {
				continue
			}
			if _, keep := protected[field.ID]; keep {
				logger.Warnf("Iceberg table %s: keeping column %s removed at the source: it is an identifier, partition or sort source column", fresh.Identifier(), name)
				continue
			}
			update.DeleteColumn([]string{name})
			changed = true
		}
		if !changed {
			w.table = fresh
			return nil
		}
		if err := update.Commit(); err != nil {
			return fmt.Errorf("alter Iceberg source columns: %w", err)
		}
		updated, err := tx.Commit(ctx)
		if err == nil {
			w.table, state.latest = updated, updated
			return nil
		}
		err = nativeCatalogError("alter Iceberg source columns", err)
		w.logConflict("source columns", attempt+1, fresh, err)
		state.invalidate(!errors.Is(err, table.ErrCommitFailed))
		if !errors.Is(err, table.ErrCommitFailed) {
			w.uncertain = true
			return fmt.Errorf("source schema commit outcome is unknown; refresh before recovery: %w", err)
		}
	}
	return fmt.Errorf("alter Iceberg source columns: %w", table.ErrCommitFailed)
}

func (w *nativeWriter) EvolveSchema(ctx context.Context, columns map[string]string) error {
	if err := w.writable(); err != nil {
		return err
	}
	ctx = nativeCatalogContext(ctx, w.cat)
	ctx, state, err := w.lockTable(ctx)
	if err != nil {
		return err
	}
	defer state.mu.Unlock()
	for attempt := range nativeCommitAttempts {
		fresh, err := w.currentTable(ctx, state)
		if err != nil {
			return err
		}
		incoming, err := nativeSchema(columns, !w.config.NoIdentifierFields)
		if err != nil {
			return err
		}
		fields := incoming.Fields()
		for idx := range fields {
			if existing, found := fresh.Schema().FindFieldByName(fields[idx].Name); found {
				if slices.Contains(fresh.Schema().IdentifierFieldIDs, existing.ID) {
					fields[idx].Required = existing.Required
				}
				fields[idx].InitialDefault = existing.InitialDefault
				fields[idx].WriteDefault = existing.WriteDefault
			}
		}
		incoming = apacheiceberg.NewSchema(0, fields...)
		tx := fresh.NewTransaction()
		if err := tx.AssertRefSnapshotID(table.MainBranch); err != nil {
			return err
		}
		if err := tx.AssertDefaultShape(); err != nil {
			return err
		}
		update := tx.UpdateSchema(true, false).UnionByNameWith(incoming)
		if !w.config.NoIdentifierFields {
			identifiers := make([][]string, 0, len(fresh.Schema().IdentifierFieldIDs)+1)
			for _, id := range fresh.Schema().IdentifierFieldIDs {
				name, _ := fresh.Schema().FindColumnName(id)
				if name != constants.OlakeID {
					identifiers = append(identifiers, []string{name})
				}
			}
			update.SetIdentifierField(append(identifiers, []string{constants.OlakeID}))
		}
		if err := update.Commit(); err != nil {
			return fmt.Errorf("evolve Iceberg schema: %w", err)
		}
		updated, err := tx.Commit(ctx)
		if err == nil {
			w.table, state.latest = updated, updated
			return nil
		}
		err = nativeCatalogError("evolve Iceberg schema", err)
		w.logConflict("schema", attempt+1, fresh, err)
		state.invalidate(!errors.Is(err, table.ErrCommitFailed))
		if !errors.Is(err, table.ErrCommitFailed) {
			w.uncertain = true
			return fmt.Errorf("schema commit outcome is unknown; refresh before recovery: %w", err)
		}
	}
	return fmt.Errorf("evolve Iceberg schema: %w", table.ErrCommitFailed)
}

func loadOrCreateNativeTable(ctx context.Context, config *Config, stream types.StreamInterface, cat catalog.Catalog) (*table.Table, error) {
	identifier := catalog.ToIdentifier(stream.GetDestinationDatabase(&config.IcebergDatabase), stream.GetDestinationTable())
	tbl, err := cat.LoadTable(ctx, identifier)
	if err == nil {
		return tbl, nil
	}
	if !errors.Is(err, catalog.ErrNoSuchTable) {
		return nil, err
	}
	columns, err := nativeStreamColumns(stream)
	if err != nil {
		return nil, err
	}
	schema, err := nativeSchema(columns, !config.NoIdentifierFields)
	if err != nil {
		return nil, err
	}
	spec, err := nativePartitionSpec(stream, schema)
	if err != nil {
		return nil, err
	}
	if err := cat.CreateNamespace(ctx, identifier[:len(identifier)-1], nil); err != nil && !errors.Is(err, catalog.ErrNamespaceAlreadyExists) {
		return nil, err
	}
	version := "2"
	if stream.GetUpdateType() == types.UpdateTypeDeletionVector {
		version = "3"
	}
	// Storage credentials belong only to catalog/FileIO configuration, never table metadata.
	props := apacheiceberg.Properties{
		table.PropertyFormatVersion:       version,
		"write.format.default":            "parquet",
		"write.parquet.compression-codec": "zstd",
	}
	// Pin each native transaction to its original branch head. Retrying stale
	// properties inside iceberg-go would overwrite another thread's 2PC state.
	props["commit.retry.num-retries"] = "0"
	// The catalog owns table locations, including managed S3 Tables/Unity warehouses.
	tbl, err = cat.CreateTable(ctx, identifier, schema, catalog.WithPartitionSpec(&spec), catalog.WithProperties(props))
	if errors.Is(err, catalog.ErrTableAlreadyExists) {
		return cat.LoadTable(ctx, identifier)
	}
	return tbl, err
}
