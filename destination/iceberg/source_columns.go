package iceberg

import (
	"context"
	"fmt"
	"maps"
	"slices"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/destination"
	legacywriter "github.com/datazip-inc/olake/destination/iceberg/legacy-writer"
	"github.com/datazip-inc/olake/destination/iceberg/proto"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils/logger"
)

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

	request := proto.IcebergPayload{
		Type: proto.IcebergPayload_ALTER_COLUMNS,
		Metadata: &proto.IcebergPayload_Metadata{
			ThreadId:       i.options.ThreadID,
			AddedColumns:   added,
			DroppedColumns: dropped,
		},
	}
	resp, err := i.server.SendClientRequest(ctx, &request)
	if err != nil {
		return nil, fmt.Errorf("failed to %s: %w", request.Type.String(), err)
	}

	result := resp.(*proto.RecordIngestResponse).GetResult()
	schemaAfterAlter, err := parseSchema(result)
	if err != nil {
		return nil, fmt.Errorf("failed to parse schema from resp[%s]: %w", result, err)
	}

	i.schema = copySchema(schemaAfterAlter)
	if err := i.writer.EvolveSchema(ctx, schemaAfterAlter); err != nil {
		return nil, destination.WriteFailure(fmt.Errorf("failed to evolve writer schema: %w", err))
	}
	return schemaAfterAlter, nil
}

// planSourceColumns returns the columns to add to and drop from a table whose current schema is
// schema. A sync-start change only adds columns whose existing rows read NULL (or an unknown
// value): it is not ordered with the pending changes, which may still carry rows from before an
// add with a default, or values of a column dropped since. syncStartColumns tracks the columns a
// sync-start change added: the change stream may still describe the table from before they
// existed, so they are not dropped until a change-stream change lists them.
func planSourceColumns(stream types.StreamInterface, schema map[string]string, change types.SourceColumnsChange, syncStartColumns map[string]struct{}) ([]*proto.IcebergPayload_AddedColumn, []string) {
	isSelected := stream.IsSelectedColumn()
	sourceColumns := types.NewSet[string]()
	for _, name := range change.KeepColumns {
		sourceColumns.Insert(stream.ResolveColumnName(name))
	}

	var added []*proto.IcebergPayload_AddedColumn
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
			added = append(added, &proto.IcebergPayload_AddedColumn{Key: key, IceType: iceType})
		case column.ExistingRowValue == nil:
			logger.Infof("stream[%s]: adding column[%s] type[%s], existing rows read NULL", stream.ID(), key, iceType)
			added = append(added, &proto.IcebergPayload_AddedColumn{Key: key, IceType: iceType})
		case change.SyncStart:
			logger.Infof("stream[%s]: column[%s] type[%s] with existing-row value[%v] will be added when its change arrives in the change stream", stream.ID(), key, iceType, column.ExistingRowValue)
		default:
			initialDefault, err := legacywriter.ToProtoFieldValue(iceType, column.ExistingRowValue)
			if err != nil {
				logger.Warnf("stream[%s]: adding column[%s] type[%s] with NULL for existing rows: failed to encode existing-row value[%v]: %s", stream.ID(), key, iceType, column.ExistingRowValue, err)
				added = append(added, &proto.IcebergPayload_AddedColumn{Key: key, IceType: iceType})
				continue
			}
			logger.Infof("stream[%s]: adding column[%s] type[%s], existing rows read [%v]", stream.ID(), key, iceType, column.ExistingRowValue)
			added = append(added, &proto.IcebergPayload_AddedColumn{Key: key, IceType: iceType, InitialDefault: initialDefault})
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
