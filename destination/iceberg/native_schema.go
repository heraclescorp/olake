package iceberg

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"

	apacheiceberg "github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/table"
	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/types"
)

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

func (w *nativeWriter) EvolveSchema(ctx context.Context, columns map[string]string) error {
	return w.evolveNativeSchema(ctx, columns, true)
}

// Constructor metadata was just loaded. Explicit evolution refreshes first;
// initialization can use that loaded view and let catalog CAS detect races.
func (w *nativeWriter) evolveNativeSchema(ctx context.Context, columns map[string]string, refresh bool) error {
	if err := w.writable(); err != nil {
		return err
	}
	ctx = nativeCatalogContext(ctx, w.cat)
	incoming, err := nativeSchema(columns, !w.config.NoIdentifierFields)
	if err != nil {
		return err
	}
	for attempt := range nativeCommitAttempts {
		fresh := w.table
		if refresh || attempt > 0 {
			fresh, err = w.cat.LoadTable(ctx, w.table.Identifier())
			if err != nil {
				return err
			}
		}
		tx := fresh.NewTransaction()
		update := tx.UpdateSchema(true, false).UnionByNameWith(incoming)
		if !w.config.NoIdentifierFields {
			update.SetIdentifierField([][]string{{constants.OlakeID}})
		}
		if err := update.Commit(); err != nil {
			return fmt.Errorf("evolve Iceberg schema: %w", err)
		}
		updated, err := tx.Commit(ctx)
		if err == nil {
			w.table = updated
			return nil
		}
		err = nativeCatalogError("evolve Iceberg schema", err)
		w.logConflict("schema", attempt+1, fresh, err)
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
