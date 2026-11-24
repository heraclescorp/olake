package iceberg

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	apacheiceberg "github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/types"
)

type nativeRecoveryScanner func(...table.ScanOption) (*table.Scan, error)

type nativeRecoveryEncodedValue string

func nativeRecoveryValue(column arrow.Array, row int) any {
	if column.IsNull(row) {
		return nil
	}
	switch value := column.(type) {
	case *array.String, *array.LargeString, *array.StringView, *array.Binary, *array.LargeBinary, *array.FixedSizeBinary, *array.BinaryView:
		return nativeIndexValue(column, row)
	case *array.Dictionary:
		return nativeRecoveryValue(value.Dictionary(), value.GetValueIndex(row))
	default:
		return nativeRecoveryEncodedValue(column.ValueStr(row))
	}
}

func readNativeRecoveryColumns(ctx context.Context, scanRows nativeRecoveryScanner, rows []map[string]any, columns []string, previous map[string]map[string]any) error {
	ids := make(map[string]struct{})
	for _, row := range rows {
		id, ok := row[constants.OlakeID].(string)
		if !ok {
			return errors.New("recovery row is missing its string _olake_id")
		}
		if _, found := previous[id]; found {
			continue
		}
		for _, column := range columns {
			if _, present := row[column]; !present {
				ids[id] = struct{}{}
				break
			}
		}
	}
	if len(ids) == 0 {
		return nil
	}
	literals := make([]apacheiceberg.Literal, 0, len(ids))
	for id := range ids {
		literals = append(literals, apacheiceberg.NewLiteral(id))
	}
	selected := append([]string{constants.OlakeID}, columns...)
	scan, err := scanRows(table.WithSelectedFields(selected...), table.WithRowFilter(apacheiceberg.SetPredicate(apacheiceberg.OpIn, apacheiceberg.Reference(constants.OlakeID), literals)))
	if err != nil {
		return err
	}
	defer scan.Close()
	schema, batches, err := scan.ToArrowRecords(ctx)
	if err != nil {
		return err
	}
	idIndex := schema.FieldIndices(constants.OlakeID)[0]
	indices := make([]int, len(columns))
	for index, column := range columns {
		indices[index] = schema.FieldIndices(column)[0]
	}
	for batch, err := range batches {
		if err != nil {
			return err
		}
		for row := range int(batch.NumRows()) {
			id := nativeIndexValue(batch.Column(idIndex), row).(string)
			if _, duplicate := previous[id]; duplicate {
				batch.Release()
				return fmt.Errorf("recovery cannot preserve columns for duplicate canonical row identity %q", id)
			}
			values := make(map[string]any, len(columns))
			for index, column := range columns {
				values[column] = nativeRecoveryValue(batch.Column(indices[index]), row)
			}
			previous[id] = values
		}
		batch.Release()
	}
	return nil
}

func fillNativeRecoveryColumns(rows []map[string]any, columns []string, previous map[string]map[string]any) {
	for _, row := range rows {
		id := row[constants.OlakeID].(string)
		values := previous[id]
		if values == nil {
			values = make(map[string]any, len(columns))
			previous[id] = values
		}
		for _, column := range columns {
			value, present := row[column]
			if !present {
				value = values[column]
				row[column] = value
			}
			values[column] = value
		}
	}
}

func (w *nativeWriter) restoreRecoveryColumns(ctx context.Context, records []types.RawRecord) error {
	rows := make([]map[string]any, 0, len(records))
	reads := make(map[string]map[int64]string)
	for _, record := range records {
		if record.Data == nil {
			continue
		}
		maps.Copy(record.Data, record.OlakeColumns)
		rows = append(rows, record.Data)
		id, ok := record.Data[constants.OlakeID].(string)
		if !ok {
			return errors.New("recovery row is missing its string _olake_id")
		}
		missing := false
		for _, column := range w.recoveryColumns {
			if _, present := record.Data[column]; !present {
				missing = true
				break
			}
		}
		if !missing {
			continue
		}
		location, found := w.latest[id]
		if w.indexThread != nil {
			var err error
			location, found, err = w.indexThread.Lookup(id)
			if err != nil {
				return err
			}
		}
		if found {
			if reads[location.FilePath] == nil {
				reads[location.FilePath] = make(map[int64]string)
			}
			reads[location.FilePath][location.Position] = id
		}
	}
	previous := make(map[string]map[string]any)
	for path, positions := range reads {
		stored, err := readNativeRowsWithValue(ctx, w.table, path, slices.Sorted(maps.Keys(positions)), w.recoveryColumns, nativeRecoveryValue)
		if err != nil {
			return err
		}
		for position, values := range stored {
			previous[positions[position]] = values
		}
	}
	if err := readNativeRecoveryColumns(ctx, func(opts ...table.ScanOption) (*table.Scan, error) {
		return w.table.Scan(opts...), nil
	}, rows, w.recoveryColumns, previous); err != nil {
		return err
	}
	fillNativeRecoveryColumns(rows, w.recoveryColumns, previous)
	return nil
}

func (w *nativeWriter) spillRecoveryCopyOnWrite(ctx context.Context, rows []map[string]any) error {
	for start := 0; start < len(rows); {
		fields := make([]apacheiceberg.NestedField, 0, w.table.Schema().NumFields())
		omitted := make([]string, 0, len(w.recoveryColumns))
		for _, column := range w.recoveryColumns {
			if _, present := rows[start][column]; !present {
				omitted = append(omitted, column)
			}
		}
		for _, field := range w.table.Schema().Fields() {
			if !slices.Contains(omitted, field.Name) {
				fields = append(fields, field)
			}
		}
		end := start + 1
		for ; end < len(rows); end++ {
			same := true
			for _, column := range w.recoveryColumns {
				_, present := rows[end][column]
				if present == slices.Contains(omitted, column) {
					same = false
					break
				}
			}
			if !same {
				break
			}
		}
		schema := apacheiceberg.NewSchemaWithIdentifiers(w.table.Schema().ID, w.table.Schema().IdentifierFieldIDs, fields...)
		batch, err := nativeArrowRecord(schema, rows[start:end])
		if err != nil {
			return err
		}
		err = w.cow.spill(ctx, w, batch)
		batch.Release()
		if err != nil {
			return err
		}
		start = end
	}
	return nil
}

func (w *nativeWriter) restoreRecoveryCopyOnWrite(ctx context.Context, tx *table.Transaction, batch arrow.RecordBatch) (arrow.RecordBatch, error) {
	var omitted []string
	for _, column := range w.recoveryColumns {
		if len(batch.Schema().FieldIndices(column)) == 0 {
			omitted = append(omitted, column)
		}
	}
	if len(omitted) == 0 {
		batch.Retain()
		return batch, nil
	}
	rows := make([]map[string]any, int(batch.NumRows()))
	for row := range rows {
		rows[row] = make(map[string]any, w.table.Schema().NumFields())
		for index, field := range batch.Schema().Fields() {
			rows[row][field.Name] = nativeRecoveryValue(batch.Column(index), row)
		}
	}
	previous := make(map[string]map[string]any)
	if err := readNativeRecoveryColumns(ctx, tx.Scan, rows, omitted, previous); err != nil {
		return nil, err
	}
	fillNativeRecoveryColumns(rows, omitted, previous)
	return nativeArrowRecord(w.table.Schema(), rows)
}
