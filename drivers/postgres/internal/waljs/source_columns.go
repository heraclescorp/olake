package waljs

import (
	"context"
	"errors"
	"fmt"

	"github.com/datazip-inc/olake/drivers/abstract"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils/logger"
	"github.com/datazip-inc/olake/utils/typeutils"
	"github.com/jackc/pglogrepl"
	"github.com/jmoiron/sqlx"
)

// attmissingval holds the value rows stored before an ADD COLUMN read for it, when Postgres
// could record it without rewriting the table (a non-volatile default). attgenerated is read
// through to_jsonb so the query also runs on versions without generated columns.
const sourceColumnsQuery = `SELECT a.attname, a.atttypid::int8 AS atttypid, a.atthasmissing,
	(a.attmissingval::text::text[])[1] AS missing_value, d.adbin IS NOT NULL AS has_default,
	COALESCE(to_jsonb(a)->>'attgenerated', '') <> '' AS generated
FROM pg_attribute a LEFT JOIN pg_attrdef d ON d.adrelid = a.attrelid AND d.adnum = a.attnum
WHERE a.attrelid = %s AND a.attnum > 0 AND NOT a.attisdropped ORDER BY a.attnum`

type catalogColumn struct {
	Name         string  `db:"attname"`
	TypeOID      int64   `db:"atttypid"`
	HasMissing   bool    `db:"atthasmissing"`
	MissingValue *string `db:"missing_value"`
	HasDefault   bool    `db:"has_default"`
	Generated    bool    `db:"generated"`
}

func fetchCatalogColumns(ctx context.Context, db *sqlx.DB, relation string, args ...any) ([]catalogColumn, error) {
	var columns []catalogColumn
	if err := db.SelectContext(ctx, &columns, fmt.Sprintf(sourceColumnsQuery, relation), args...); err != nil {
		return nil, err
	}
	return columns, nil
}

// relationColumnsChanged reports whether a Relation message changes the column list Postgres
// last sent for the relation. pgoutput resends Relation messages after unrelated cache
// invalidations too, which must not count as schema changes.
func relationColumnsChanged(prev, next *pglogrepl.RelationMessage) bool {
	if prev == nil || len(prev.Columns) != len(next.Columns) {
		return true
	}
	for idx, column := range next.Columns {
		if column.Name != prev.Columns[idx].Name || column.DataType != prev.Columns[idx].DataType {
			return true
		}
	}
	return false
}

// sourceColumn describes a column whose values are typed by typeOID. catalog is the column's
// current catalog entry, nil when the column (with that type) is no longer in the catalog.
func (c ChangeFilter) sourceColumn(name string, typeOID uint32, catalog *catalogColumn) types.SourceColumn {
	typeName := oidToString(typeOID)
	column := types.SourceColumn{Name: name, Type: c.columnType(typeName)}
	switch {
	case catalog == nil:
		column.ExistingRowValueUnknown = true
	case catalog.HasMissing:
		if catalog.MissingValue == nil {
			break
		}
		value, err := c.converter(*catalog.MissingValue, typeName)
		if err != nil && !errors.Is(err, typeutils.ErrNullValue) {
			logger.Warnf("failed to convert stored value[%s] of column[%s] type[%s]: %s", *catalog.MissingValue, name, typeName, err)
			column.ExistingRowValueUnknown = true
			break
		}
		column.ExistingRowValue = value
	case catalog.HasDefault:
		// the default was evaluated per row while rewriting the table, or the table was
		// rewritten since the add: the value older rows hold is not recorded anywhere
		column.ExistingRowValueUnknown = true
	}
	return column
}

// relationSourceColumns lists the columns of a Relation message. catalog is the table's current
// catalog, nil when it could not be read.
func (c ChangeFilter) relationSourceColumns(msg *pglogrepl.RelationMessage, catalog []catalogColumn) types.SourceColumnsChange {
	byName := make(map[string]*catalogColumn, len(catalog))
	var change types.SourceColumnsChange
	for idx := range catalog {
		if catalog[idx].Generated {
			change.KeepColumns = append(change.KeepColumns, catalog[idx].Name)
			continue
		}
		byName[catalog[idx].Name] = &catalog[idx]
	}
	for _, column := range msg.Columns {
		entry := byName[column.Name]
		if entry != nil && uint32(entry.TypeOID) != column.DataType {
			entry = nil
		}
		change.Columns = append(change.Columns, c.sourceColumn(column.Name, column.DataType, entry))
	}
	return change
}

// catalogSourceColumns lists the columns of a table's current catalog.
func (c ChangeFilter) catalogSourceColumns(catalog []catalogColumn) types.SourceColumnsChange {
	change := types.SourceColumnsChange{SyncStart: true}
	for idx := range catalog {
		if catalog[idx].Generated {
			change.KeepColumns = append(change.KeepColumns, catalog[idx].Name)
			continue
		}
		change.Columns = append(change.Columns, c.sourceColumn(catalog[idx].Name, uint32(catalog[idx].TypeOID), &catalog[idx]))
	}
	return change
}

func (p *pgoutputReplicator) emitRelationColumns(ctx context.Context, msg *pglogrepl.RelationMessage, insertFn abstract.CDCMsgFn) error {
	stream := p.socket.changeFilter.tables[fmt.Sprintf("%s.%s", msg.Namespace, msg.RelationName)]
	if stream == nil {
		return nil
	}
	if p.socket.changeFilter.skipTransaction(stream.ID(), p.txnFinalLSN) {
		return nil
	}

	catalog, err := fetchCatalogColumns(ctx, p.db, "$1::oid", msg.RelationID)
	if err != nil {
		logger.Warnf("table[%s.%s]: failed to read column catalog, values of existing rows for added columns are unknown: %s", msg.Namespace, msg.RelationName, err)
		catalog = nil
	}
	return insertFn(ctx, abstract.NewSourceColumnsChange(stream, p.socket.changeFilter.relationSourceColumns(msg, catalog)))
}

// emitSyncStartColumns reports every stream's current columns, so columns added to a table
// without later row changes still reach the destination.
func (p *pgoutputReplicator) emitSyncStartColumns(ctx context.Context, insertFn abstract.CDCMsgFn) error {
	for _, stream := range p.socket.changeFilter.tables {
		catalog, err := fetchCatalogColumns(ctx, p.db, "(quote_ident($1) || '.' || quote_ident($2))::regclass", stream.Namespace(), stream.Name())
		if err != nil {
			logger.Warnf("stream[%s]: failed to read column catalog, skipping column check at sync start: %s", stream.ID(), err)
			continue
		}
		if err := insertFn(ctx, abstract.NewSourceColumnsChange(stream, p.socket.changeFilter.catalogSourceColumns(catalog))); err != nil {
			return err
		}
	}
	return nil
}
