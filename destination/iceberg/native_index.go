package iceberg

import (
	"cmp"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"slices"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	apacheiceberg "github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
	"github.com/apache/iceberg-go/table/dv"
	"github.com/datazip-inc/olake/types"
)

const indexScanFlushRows = 200_000

// reconcileNativeTableIndex publishes the target checkpoint only after every row
// is durable. Bounded partial flushes invalidate the old checkpoint atomically so
// a crash followed by an external rewrite cannot leave stale partial-scan keys.
func reconcileNativeTableIndex(ctx context.Context, index types.StreamIndex, tbl *table.Table, identifierField string) error {
	if index == nil {
		return nil
	}
	indexed, err := index.LastCommittedSnapshot()
	if err != nil {
		return fmt.Errorf("read native stream index checkpoint: %w", err)
	}
	var target int64
	if snapshot := tbl.CurrentSnapshot(); snapshot != nil {
		target = snapshot.SnapshotID
	}
	if indexed == target {
		return nil
	}
	if indexed != 0 && !table.IsAncestorOf(target, indexed, tbl.SnapshotByID) {
		return fmt.Errorf("cannot catch up stream index: snapshot %d is not an ancestor of destination snapshot %d; a full index reset is required", indexed, target)
	}
	current, err := planNativeIndexFiles(ctx, tbl, target)
	if err != nil {
		return err
	}
	selected := current
	rebuild := indexed == 0
	if !rebuild {
		previous, err := planNativeIndexFiles(ctx, tbl, indexed)
		if err != nil {
			return fmt.Errorf("read checkpoint snapshot %d: %w", indexed, err)
		}
		selected, rebuild = nativeIndexChanges(previous, current)
	}
	if rebuild {
		// Clearing also resets the checkpoint, making an interrupted rebuild restart
		// from scratch rather than expose a partially replaced index on recovery.
		if err := index.Truncate(); err != nil {
			return fmt.Errorf("truncate native stream index: %w", err)
		}
		selected = current
	}
	pending := types.NewStreamIndexThread(index)
	buffered := 0
	checkpointInvalidated := rebuild
	flush := func(checkpoint *int64) error {
		var invalid int64
		if checkpoint == nil && !checkpointInvalidated {
			checkpoint = &invalid
		}
		if err := index.Commit(pending, checkpoint); err != nil {
			return fmt.Errorf("persist native stream index: %w", err)
		}
		if checkpoint != nil && *checkpoint == 0 {
			checkpointInvalidated = true
		}
		pending = types.NewStreamIndexThread(index)
		buffered = 0
		return nil
	}
	fs, err := tbl.FS(ctx)
	if err != nil {
		return fmt.Errorf("open native index file IO: %w", err)
	}
	projection, err := tbl.Schema().Select(true, identifierField)
	if err != nil {
		return fmt.Errorf("select native index identifier: %w", err)
	}
	cache := newNativeDeleteCache()
	for _, task := range selected {
		deleted, err := cache.positions(ctx, fs, tbl, task, true)
		if err != nil {
			return fmt.Errorf("read native index deletes: %w", err)
		}
		err = readNativePhysicalBatches(ctx, fs, tbl, task.File.FilePath(), projection, nil, func(base int64, batch arrow.RecordBatch) (bool, error) {
			for row := range int(batch.NumRows()) {
				position := base + int64(row)
				if deleted.Contains(uint64(position)) || batch.Column(0).IsNull(row) {
					continue
				}
				identifier, err := nativeIndexValue(batch.Column(0), row)
				if err != nil {
					return false, err
				}
				key, ok := identifier.(string)
				if !ok {
					return false, errors.New("native stream index identifier must be a string")
				}
				pending.Put(key, types.RowLocation{FilePath: task.File.FilePath(), Position: position})
				buffered++
				if buffered >= indexScanFlushRows {
					if err := flush(nil); err != nil {
						return false, err
					}
				}
			}
			return true, nil
		})
		if err != nil {
			return fmt.Errorf("scan native stream index data: %w", err)
		}
	}
	return flush(&target)
}

func planNativeIndexFiles(ctx context.Context, tbl *table.Table, snapshot int64) ([]table.FileScanTask, error) {
	if snapshot == 0 {
		return nil, nil
	}
	scan := tbl.Scan(table.WithSnapshotID(snapshot), table.WithMaxConcurrency(1))
	defer scan.Close()
	tasks, err := scan.PlanFiles(ctx)
	if err != nil {
		return nil, fmt.Errorf("plan native index snapshot %d: %w", snapshot, err)
	}
	unique := make(map[string]table.FileScanTask, len(tasks))
	for _, task := range tasks {
		// PlanFiles can split files into row-group tasks. Physical readers below
		// always read each full file exactly once.
		task.Start, task.Length = 0, task.File.FileSizeBytes()
		unique[task.File.FilePath()] = task
	}
	tasks = make([]table.FileScanTask, 0, len(unique))
	for _, task := range unique {
		tasks = append(tasks, task)
	}
	slices.SortFunc(tasks, func(a, b table.FileScanTask) int {
		var aSeq, bSeq int64
		if a.DataSequenceNumber != nil {
			aSeq = *a.DataSequenceNumber
		}
		if b.DataSequenceNumber != nil {
			bSeq = *b.DataSequenceNumber
		}
		if order := cmp.Compare(aSeq, bSeq); order != 0 {
			return order
		}
		return strings.Compare(a.File.FilePath(), b.File.FilePath())
	})
	return tasks, nil
}

// The index has no key deletion operation. File removal or a changed delete set
// therefore requires a full rebuild; only pure additions are safe incrementally.
func nativeIndexChanges(previous, current []table.FileScanTask) ([]table.FileScanTask, bool) {
	old := make(map[string]table.FileScanTask, len(previous))
	for _, task := range previous {
		old[task.File.FilePath()] = task
	}
	var added []table.FileScanTask
	for _, task := range current {
		before, exists := old[task.File.FilePath()]
		if !exists {
			added = append(added, task)
			continue
		}
		if nativeTaskDeleteSignature(before) != nativeTaskDeleteSignature(task) {
			return current, true
		}
		delete(old, task.File.FilePath())
	}
	if len(old) != 0 {
		return current, true
	}
	return added, false
}

func nativeTaskDeleteSignature(task table.FileScanTask) string {
	var names []string
	for _, files := range [][]apacheiceberg.DataFile{task.DeleteFiles, task.EqualityDeleteFiles, task.DeletionVectorFiles} {
		for _, f := range files {
			name := fmt.Sprintf("%d:%s", f.ContentType(), f.FilePath())
			if offset := f.ContentOffset(); offset != nil {
				name += fmt.Sprintf(":%d", *offset)
			}
			if size := f.ContentSizeInBytes(); size != nil {
				name += fmt.Sprintf(":%d", *size)
			}
			names = append(names, name)
		}
	}
	slices.Sort(names)
	return strings.Join(names, "\x00")
}

func (w *nativeWriter) ReadRows(ctx context.Context, path string, positions []int64, columns []string) (map[int64]map[string]any, error) {
	return readNativeRows(nativeCatalogContext(ctx, w.cat), w.table, path, positions, columns)
}

// readNativeRows addresses physical file ordinals, not ordinals after Iceberg
// deletes. It also reads pending uploaded files not yet present in a manifest.
func readNativeRows(ctx context.Context, tbl *table.Table, path string, positions []int64, columns []string) (map[int64]map[string]any, error) {
	result := make(map[int64]map[string]any, len(positions))
	if len(positions) == 0 {
		return result, nil
	}
	positions = slices.Clone(positions)
	slices.Sort(positions)
	positions = slices.Compact(positions)
	if positions[0] < 0 {
		return nil, errors.New("native row position must not be negative")
	}
	// A column dropped or deselected since the old file was written is absent,
	// not a fabricated value. Match current column names to stable field IDs.
	var selected []string
	for _, column := range columns {
		if _, ok := tbl.Schema().FindFieldByName(column); ok {
			selected = append(selected, column)
		}
	}
	projection, err := tbl.Schema().Select(true, selected...)
	if err != nil {
		return nil, err
	}
	fs, err := tbl.FS(ctx)
	if err != nil {
		return nil, fmt.Errorf("open native row file IO: %w", err)
	}
	next := 0
	err = readNativePhysicalBatches(ctx, fs, tbl, path, projection, positions, func(base int64, batch arrow.RecordBatch) (bool, error) {
		end := base + batch.NumRows()
		for next < len(positions) && positions[next] < end {
			position := positions[next]
			if position < base {
				return false, errors.New("native reader skipped a requested physical position")
			}
			row := make(map[string]any, len(selected))
			for col, field := range batch.Schema().Fields() {
				value, err := nativeIndexValue(batch.Column(col), int(position-base))
				if err != nil {
					return false, err
				}
				row[field.Name] = value
			}
			result[position] = row
			next++
		}
		return next < len(positions), nil
	})
	if err != nil {
		return nil, fmt.Errorf("read native physical rows: %w", err)
	}
	if next != len(positions) {
		return nil, errors.New("native row index references a position beyond the data file")
	}
	return result, nil
}

// readNativePhysicalBatches projects Iceberg field IDs while keeping original
// row-group offsets. A projected/missing column must never change row ordinals.
func readNativePhysicalBatches(ctx context.Context, fs iceio.IO, tbl *table.Table, path string, projection *apacheiceberg.Schema, positions []int64, consume func(int64, arrow.RecordBatch) (bool, error)) (err error) {
	input, err := fs.Open(path)
	if err != nil {
		return err
	}
	parquetReader, err := file.NewParquetReader(input)
	if err != nil {
		return errors.Join(err, input.Close())
	}
	defer func() { err = errors.Join(err, parquetReader.Close()) }()
	reader, err := pqarrow.NewFileReader(parquetReader, pqarrow.ArrowReadProperties{BatchSize: 8192}, memory.DefaultAllocator)
	if err != nil {
		return err
	}
	arrowSchema, err := reader.Schema()
	if err != nil {
		return err
	}
	mapping := tbl.NameMapping()
	if mapping == nil {
		mapping = tbl.Schema().NameMapping()
	}
	fileSchema, err := table.ArrowSchemaToIcebergWithOptions(arrowSchema, table.ArrowToIcebergOptions{NameMapping: mapping, TableSchema: tbl.Schema()})
	if err != nil {
		return fmt.Errorf("decode physical Iceberg schema: %w", err)
	}
	// Select only physical leaves under requested top-level fields. Nested field
	// projection is completed by ToRequestedSchema using stable Iceberg IDs.
	var indices []int
	var appendLeaves func(pqarrow.SchemaField)
	appendLeaves = func(field pqarrow.SchemaField) {
		if field.IsLeaf() {
			indices = append(indices, field.ColIndex)
			return
		}
		for _, child := range field.Children {
			appendLeaves(child)
		}
	}
	for i, field := range fileSchema.Fields() {
		wanted := false
		var hasRequested func(apacheiceberg.NestedField)
		hasRequested = func(field apacheiceberg.NestedField) {
			if _, ok := projection.FindFieldByID(field.ID); ok {
				wanted = true
			}
			if nested, ok := field.Type.(apacheiceberg.NestedType); ok {
				for _, child := range nested.Fields() {
					hasRequested(child)
				}
			}
		}
		hasRequested(field)
		if wanted {
			appendLeaves(reader.Manifest.Fields[i])
		}
	}
	// Arrow cannot iterate a zero-column record reader. Read one physical leaf
	// solely to preserve the row count when every requested column is absent.
	if len(indices) == 0 {
		if len(reader.Manifest.ColIndexToField) == 0 {
			return errors.New("physical data file has no columns")
		}
		indices = []int{0}
	}
	var base int64
	for group := range parquetReader.NumRowGroups() {
		if err := ctx.Err(); err != nil {
			return err
		}
		count := parquetReader.MetaData().RowGroup(group).NumRows()
		if positions != nil {
			idx, _ := slices.BinarySearch(positions, base)
			if idx == len(positions) || positions[idx] >= base+count {
				base += count
				continue
			}
		}
		records, err := reader.GetRecordReader(ctx, indices, []int{group})
		if err != nil {
			return err
		}
		projectedFileSchema, err := table.ArrowSchemaToIcebergWithOptions(records.Schema(), table.ArrowToIcebergOptions{NameMapping: mapping, TableSchema: tbl.Schema()})
		if err != nil {
			records.Release()
			return err
		}
		offset := base
		keepReading := true
		for records.Next() {
			if err := ctx.Err(); err != nil {
				records.Release()
				return err
			}
			batch := records.RecordBatch()
			projected, err := table.ToRequestedSchema(ctx, projection, projectedFileSchema, batch, table.SchemaOptions{FormatVersion: tbl.Metadata().Version(), IncludeFieldIDs: true, AllowMissingRequired: true})
			if err != nil {
				records.Release()
				return err
			}
			keepReading, err = consume(offset, projected)
			projected.Release()
			offset += batch.NumRows()
			if err != nil {
				records.Release()
				return err
			}
			if !keepReading {
				break
			}
		}
		err = records.Err()
		records.Release()
		if err != nil {
			return err
		}
		if !keepReading {
			return nil
		}
		base += count
	}
	return nil
}

func nativeIndexValue(column arrow.Array, row int) (any, error) {
	if column.IsNull(row) {
		return nil, nil
	}
	switch value := column.(type) {
	case *array.String:
		return strings.Clone(value.Value(row)), nil
	case *array.LargeString:
		return strings.Clone(value.Value(row)), nil
	case *array.StringView:
		return strings.Clone(value.Value(row)), nil
	case *array.Binary:
		return slices.Clone(value.Value(row)), nil
	case *array.LargeBinary:
		return slices.Clone(value.Value(row)), nil
	case *array.FixedSizeBinary:
		return slices.Clone(value.Value(row)), nil
	case *array.BinaryView:
		return slices.Clone(value.Value(row)), nil
	case *array.Boolean:
		return value.Value(row), nil
	case *array.Int32:
		return value.Value(row), nil
	case *array.Int64:
		return value.Value(row), nil
	case *array.Float32:
		return value.Value(row), nil
	case *array.Float64:
		return value.Value(row), nil
	case *array.Dictionary:
		return nativeIndexValue(value.Dictionary(), value.GetValueIndex(row))
	default:
		// TOAST-bearing source values (text, JSON, arrays) are stored as strings;
		// the other primitive values retain their Arrow logical representation.
		return column.GetOneForMarshal(row), nil
	}
}

type nativeEqualitySet struct {
	projection *apacheiceberg.Schema
	fieldIDs   []int
	keys       map[string]struct{}
}

type nativeDeleteCache struct {
	positionsByFile map[string]*dv.RoaringPositionBitmap
	positionalFiles map[string]bool
	equalities      map[string]*nativeEqualitySet
}

func newNativeDeleteCache() *nativeDeleteCache {
	return &nativeDeleteCache{positionsByFile: make(map[string]*dv.RoaringPositionBitmap), positionalFiles: make(map[string]bool), equalities: make(map[string]*nativeEqualitySet)}
}

func (cache *nativeDeleteCache) positions(ctx context.Context, fs iceio.IO, tbl *table.Table, task table.FileScanTask, includeEquality bool) (*dv.RoaringPositionBitmap, error) {
	deleted := dv.NewRoaringPositionBitmap()
	for _, deleteFile := range task.DeleteFiles {
		byFile, err := cache.positional(ctx, fs, tbl, deleteFile, task.File.FilePath())
		if err != nil {
			return nil, err
		}
		deleted.Or(byFile)
	}
	for _, deleteFile := range task.DeletionVectorFiles {
		bitmap, err := dv.ReadDV(fs, deleteFile)
		if err != nil {
			return nil, err
		}
		deleted.Or(bitmap)
	}
	if includeEquality {
		for _, deleteFile := range task.EqualityDeleteFiles {
			set, err := cache.equality(ctx, fs, tbl, deleteFile)
			if err != nil {
				return nil, err
			}
			err = readNativePhysicalBatches(ctx, fs, tbl, task.File.FilePath(), set.projection, nil, func(base int64, batch arrow.RecordBatch) (bool, error) {
				columns, err := nativeEqualityColumns(batch, set.fieldIDs)
				if err != nil {
					return false, err
				}
				var key []byte
				for row := range int(batch.NumRows()) {
					key = nativeEqualityKey(key[:0], columns, row)
					if _, matches := set.keys[string(key)]; matches {
						deleted.Set(uint64(base + int64(row)))
					}
				}
				return true, nil
			})
			if err != nil {
				return nil, err
			}
		}
	}
	return deleted, nil
}

func (cache *nativeDeleteCache) positional(ctx context.Context, fs iceio.IO, tbl *table.Table, deleteFile apacheiceberg.DataFile, target string) (*dv.RoaringPositionBitmap, error) {
	key := deleteFile.FilePath() + "\x00" + target
	if cache.positionalFiles[deleteFile.FilePath()] {
		return cache.positionsByFile[key], nil
	}
	err := readNativePhysicalBatches(ctx, fs, tbl, deleteFile.FilePath(), apacheiceberg.PositionalDeleteSchema, nil, func(_ int64, batch arrow.RecordBatch) (bool, error) {
		for row := range int(batch.NumRows()) {
			path, err := nativeIndexValue(batch.Column(0), row)
			if err != nil {
				return false, err
			}
			filePath, ok := path.(string)
			if !ok {
				return false, errors.New("native positional delete has a null or invalid file path")
			}
			position, ok := batch.Column(1).(*array.Int64)
			if !ok || position.IsNull(row) || position.Value(row) < 0 {
				return false, errors.New("invalid native positional delete row")
			}
			fileKey := deleteFile.FilePath() + "\x00" + filePath
			bitmap := cache.positionsByFile[fileKey]
			if bitmap == nil {
				bitmap = dv.NewRoaringPositionBitmap()
				cache.positionsByFile[fileKey] = bitmap
			}
			bitmap.Set(uint64(position.Value(row)))
		}
		return true, nil
	})
	if err != nil {
		return nil, err
	}
	cache.positionalFiles[deleteFile.FilePath()] = true
	return cache.positionsByFile[key], nil
}

func (cache *nativeDeleteCache) equality(ctx context.Context, fs iceio.IO, tbl *table.Table, deleteFile apacheiceberg.DataFile) (*nativeEqualitySet, error) {
	if set, ok := cache.equalities[deleteFile.FilePath()]; ok {
		return set, nil
	}
	ids := deleteFile.EqualityFieldIDs()
	if len(ids) == 0 {
		return nil, errors.New("native equality delete has no equality field IDs")
	}
	selected := make(map[int]apacheiceberg.Void, len(ids))
	for _, id := range ids {
		selected[id] = apacheiceberg.Void{}
	}
	source := tbl.Schema()
	contains := func(schema *apacheiceberg.Schema) bool {
		for _, id := range ids {
			if _, ok := schema.FindFieldByID(id); !ok {
				return false
			}
		}
		return true
	}
	if !contains(source) {
		source = nil
		for _, schema := range tbl.Metadata().Schemas() {
			if contains(schema) && (source == nil || schema.ID > source.ID) {
				source = schema
			}
		}
		if source == nil {
			return nil, errors.New("native equality delete fields are absent from all table schemas")
		}
	}
	for _, id := range ids {
		field, _ := source.FindFieldByID(id)
		if _, primitive := field.Type.(apacheiceberg.PrimitiveType); !primitive {
			return nil, errors.New("native equality delete field must be primitive")
		}
	}
	projection, err := apacheiceberg.PruneColumns(source, selected, false)
	if err != nil {
		return nil, err
	}
	set := &nativeEqualitySet{projection: projection, fieldIDs: slices.Clone(ids), keys: make(map[string]struct{})}
	err = readNativePhysicalBatches(ctx, fs, tbl, deleteFile.FilePath(), projection, nil, func(_ int64, batch arrow.RecordBatch) (bool, error) {
		columns, err := nativeEqualityColumns(batch, set.fieldIDs)
		if err != nil {
			return false, err
		}
		var key []byte
		for row := range int(batch.NumRows()) {
			key = nativeEqualityKey(key[:0], columns, row)
			set.keys[string(key)] = struct{}{}
		}
		return true, nil
	})
	if err != nil {
		return nil, err
	}
	cache.equalities[deleteFile.FilePath()] = set
	return set, nil
}

// Projection normalizes promotions before encoding. Length prefixes distinguish
// composite keys; nulls, NaNs, and signed zero follow Iceberg equality semantics.
type nativeEqualityColumn struct {
	value   arrow.Array
	parents []arrow.Array
}

func nativeEqualityColumns(batch arrow.RecordBatch, ids []int) ([]nativeEqualityColumn, error) {
	byID := make(map[int]nativeEqualityColumn, len(ids))
	var visit func(arrow.Field, arrow.Array, []arrow.Array)
	visit = func(field arrow.Field, value arrow.Array, parents []arrow.Array) {
		if text, ok := field.Metadata.GetValue(table.ArrowParquetFieldIDKey); ok {
			for _, id := range ids {
				if text == fmt.Sprint(id) {
					byID[id] = nativeEqualityColumn{value: value, parents: slices.Clone(parents)}
				}
			}
		}
		if nested, ok := value.(*array.Struct); ok {
			fields := field.Type.(*arrow.StructType).Fields()
			for i, child := range fields {
				visit(child, nested.Field(i), append(parents, value))
			}
		}
	}
	for i, field := range batch.Schema().Fields() {
		visit(field, batch.Column(i), nil)
	}
	columns := make([]nativeEqualityColumn, 0, len(ids))
	for _, id := range ids {
		column, ok := byID[id]
		if !ok {
			return nil, fmt.Errorf("native equality field ID %d is missing from projected data", id)
		}
		columns = append(columns, column)
	}
	return columns, nil
}

func nativeEqualityKey(key []byte, columns []nativeEqualityColumn, row int) []byte {
	for _, column := range columns {
		null := false
		for _, parent := range column.parents {
			if parent.IsNull(row) {
				null = true
				break
			}
		}
		if null {
			key = append(key, 0)
		} else {
			key = nativeEqualityValue(key, column.value, row)
		}
	}
	return key
}

func nativeEqualityValue(key []byte, column arrow.Array, row int) []byte {
	if column.IsNull(row) {
		return append(key, 0)
	}
	key = append(key, 1)
	if dictionary, ok := column.(*array.Dictionary); ok {
		return nativeEqualityValue(key[:len(key)-1], dictionary.Dictionary(), dictionary.GetValueIndex(row))
	}
	if nested, ok := column.(*array.Struct); ok {
		for i := range nested.NumField() {
			key = nativeEqualityValue(key, nested.Field(i), row)
		}
		return key
	}
	var value string
	switch typed := column.(type) {
	case *array.Float32:
		v := typed.Value(row)
		if v == 0 {
			value = "0"
		} else if math.IsNaN(float64(v)) {
			value = "NaN"
		} else {
			value = typed.ValueStr(row)
		}
	case *array.Float64:
		v := typed.Value(row)
		if v == 0 {
			value = "0"
		} else if math.IsNaN(v) {
			value = "NaN"
		} else {
			value = typed.ValueStr(row)
		}
	default:
		value = column.ValueStr(row)
	}
	key = binary.AppendUvarint(key, uint64(len(value)))
	return append(key, value...)
}

// collectNativeEqualityDeletePositions translates only delete files the native
// planner says apply to each data file (including partition and sequence rules).
// Existing pos/DV deletions are excluded: the replacement commit retains them.
func collectNativeEqualityDeletePositions(ctx context.Context, tbl *table.Table) (map[string][]int64, []apacheiceberg.DataFile, error) {
	positions := make(map[string][]int64)
	snapshot := tbl.CurrentSnapshot()
	if snapshot == nil {
		return positions, nil, nil
	}
	fs, err := tbl.FS(ctx)
	if err != nil {
		return nil, nil, err
	}
	// Include dead equality files too, so migration removes the old encoding in
	// the same commit even when it currently matches no live data file.
	manifests, err := snapshot.Manifests(fs)
	if err != nil {
		return nil, nil, err
	}
	var old []apacheiceberg.DataFile
	for _, manifest := range manifests {
		if manifest.ManifestContent() != apacheiceberg.ManifestContentDeletes {
			continue
		}
		for entry, err := range manifest.Entries(fs, true) {
			if err != nil {
				return nil, nil, err
			}
			if entry.DataFile().ContentType() == apacheiceberg.EntryContentEqDeletes {
				old = append(old, entry.DataFile())
			}
		}
	}
	if len(old) == 0 {
		return positions, nil, nil
	}
	tasks, err := planNativeIndexFiles(ctx, tbl, snapshot.SnapshotID)
	if err != nil {
		return nil, nil, err
	}
	cache := newNativeDeleteCache()
	for _, task := range tasks {
		if len(task.EqualityDeleteFiles) == 0 {
			continue
		}
		before, err := cache.positions(ctx, fs, tbl, task, false)
		if err != nil {
			return nil, nil, err
		}
		after, err := cache.positions(ctx, fs, tbl, task, true)
		if err != nil {
			return nil, nil, err
		}
		for position := range after.Positions() {
			if position >= uint64(task.File.Count()) {
				return nil, nil, errors.New("native equality delete position exceeds data file row count")
			}
			if !before.Contains(position) {
				positions[task.File.FilePath()] = append(positions[task.File.FilePath()], int64(position))
			}
		}
	}
	return positions, old, nil
}

// readNativePositionDeletes is shared by DV union/replacement and index reads.
func readNativePositionDeletes(ctx context.Context, tbl *table.Table, path string) (map[string][]int64, error) {
	fs, err := tbl.FS(ctx)
	if err != nil {
		return nil, err
	}
	positions := make(map[string][]int64)
	err = readNativePhysicalBatches(ctx, fs, tbl, path, apacheiceberg.PositionalDeleteSchema, nil, func(_ int64, batch arrow.RecordBatch) (bool, error) {
		for row := range int(batch.NumRows()) {
			path, err := nativeIndexValue(batch.Column(0), row)
			if err != nil {
				return false, err
			}
			filePath, ok := path.(string)
			if !ok {
				return false, errors.New("native positional delete has a null or invalid file path")
			}
			column, ok := batch.Column(1).(*array.Int64)
			if !ok || column.IsNull(row) || column.Value(row) < 0 {
				return false, errors.New("native positional delete has an invalid physical position")
			}
			positions[filePath] = append(positions[filePath], column.Value(row))
		}
		return true, nil
	})
	if err != nil {
		return nil, err
	}
	for path, values := range positions {
		slices.Sort(values)
		positions[path] = slices.Compact(values)
	}
	return positions, nil
}
