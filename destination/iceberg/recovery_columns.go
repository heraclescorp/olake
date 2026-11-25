package iceberg

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
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
	return scanNativeRecoveryColumns(ctx, scanRows, ids, columns, func(id string, batch arrow.RecordBatch, row int, indices []int) error {
		if _, duplicate := previous[id]; duplicate {
			return fmt.Errorf("recovery cannot preserve columns for duplicate canonical row identity %q", id)
		}
		values := make(map[string]any, len(columns))
		for index, column := range columns {
			values[column] = nativeRecoveryValue(batch.Column(indices[index]), row)
		}
		previous[id] = values
		return nil
	})
}

func scanNativeRecoveryColumns(ctx context.Context, scanRows nativeRecoveryScanner, ids map[string]struct{}, columns []string, consume func(string, arrow.RecordBatch, int, []int) error) error {
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
	idIndices := schema.FieldIndices(constants.OlakeID)
	if len(idIndices) != 1 {
		return errors.New("recovery scan requires one canonical _olake_id column")
	}
	indices := make([]int, len(columns))
	for index, column := range columns {
		fields := schema.FieldIndices(column)
		if len(fields) != 1 {
			return fmt.Errorf("recovery scan requires one column named %q", column)
		}
		indices[index] = fields[0]
	}
	for batch, err := range batches {
		if err != nil {
			return err
		}
		for row := range int(batch.NumRows()) {
			id, ok := nativeIndexValue(batch.Column(idIndices[0]), row).(string)
			if !ok {
				batch.Release()
				return errors.New("recovery scan row is missing its string _olake_id")
			}
			if err := consume(id, batch, row, indices); err != nil {
				batch.Release()
				return err
			}
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

// A supplied null is distinct from a value that still needs hydration.
type nativeCOWRecoveryValue struct {
	value   any
	present bool
}

func nativeCOWRecoveryFields(schema *arrow.Schema, columns []string) (present map[string]int, omitted []string) {
	for _, column := range columns {
		indices := schema.FieldIndices(column)
		if len(indices) == 0 {
			omitted = append(omitted, column)
			continue
		}
		if present == nil {
			present = make(map[string]int, len(columns))
		}
		present[column] = indices[0]
	}
	return present, omitted
}

func (winner nativeCOWRow) captureRecovery(batch arrow.RecordBatch, row int, present map[string]int) {
	for name, value := range winner.recovery {
		if index, found := present[name]; found && !value.present {
			winner.recovery[name] = nativeCOWRecoveryValue{nativeRecoveryValue(batch.Column(index), row), true}
		}
	}
}

func hydrateNativeCOWRecovery(ctx context.Context, scanRows nativeRecoveryScanner, latest map[string]nativeCOWRow, columns []string) error {
	if len(columns) == 0 {
		return nil
	}
	ids := make(map[string]struct{})
	missing := make(map[string]struct{})
	for id, winner := range latest {
		for name, value := range winner.recovery {
			if !value.present {
				ids[id] = struct{}{}
				missing[name] = struct{}{}
			}
		}
	}
	var projection []string
	for _, name := range columns {
		if _, needed := missing[name]; needed {
			projection = append(projection, name)
		}
	}
	return scanNativeRecoveryColumns(ctx, scanRows, ids, projection, func(id string, batch arrow.RecordBatch, row int, indices []int) error {
		if _, found := ids[id]; !found {
			return fmt.Errorf("recovery cannot preserve columns for duplicate canonical row identity %q", id)
		}
		delete(ids, id)
		values := latest[id].recovery
		for index, name := range projection {
			if value, needed := values[name]; needed && !value.present {
				values[name] = nativeCOWRecoveryValue{nativeRecoveryValue(batch.Column(indices[index]), row), true}
			}
		}
		return nil
	})
}

func restoreNativeCOWBatch(schema *arrow.Schema, batch arrow.RecordBatch, ids *array.String, start, end int, latest map[string]nativeCOWRow) (arrow.RecordBatch, error) {
	if batch.Schema().Equal(schema) {
		if start == 0 && end == ids.Len() {
			batch.Retain()
			return batch, nil
		}
		return batch.NewSlice(int64(start), int64(end)), nil
	}
	columns := make([]arrow.Array, 0, schema.NumFields())
	defer func() {
		for _, column := range columns {
			column.Release()
		}
	}()
	for _, field := range schema.Fields() {
		if indices := batch.Schema().FieldIndices(field.Name); len(indices) == 1 {
			columns = append(columns, array.NewSlice(batch.Column(indices[0]), int64(start), int64(end)))
			continue
		}
		column, err := buildNativeCOWRecoveryColumn(field, ids, start, end, latest)
		if err != nil {
			return nil, err
		}
		columns = append(columns, column)
	}
	return array.NewRecordBatch(schema, columns, int64(end-start)), nil
}

func buildNativeCOWRecoveryColumn(field arrow.Field, ids *array.String, start, end int, latest map[string]nativeCOWRow) (arrow.Array, error) {
	builder := array.NewBuilder(memory.DefaultAllocator, field.Type)
	defer builder.Release()
	builder.Reserve(end - start)
	for row := start; row < end; row++ {
		recovered, needed := latest[ids.Value(row)].recovery[field.Name]
		if !needed {
			return nil, fmt.Errorf("COW replay input is missing column %q", field.Name)
		}
		if recovered.value == nil {
			if !field.Nullable {
				return nil, fmt.Errorf("required Iceberg column %q is null", field.Name)
			}
			builder.AppendNull()
			continue
		}
		if err := appendNativeValue(builder, recovered.value); err != nil {
			return nil, fmt.Errorf("encode Iceberg column %q: %w", field.Name, err)
		}
	}
	return builder.NewArray(), nil
}
