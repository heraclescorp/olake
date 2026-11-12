package iceberg

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"slices"
	"strconv"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	apacheiceberg "github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
	"github.com/apache/iceberg-go/table/dv"
	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
	"github.com/datazip-inc/olake/utils/typeutils"
	"github.com/google/uuid"
)

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

func nativePartitionLiteral(field apacheiceberg.NestedField, value any) (apacheiceberg.Literal, error) {
	if value == nil {
		return nil, nil
	}
	switch field.Type.(type) {
	case apacheiceberg.BooleanType:
		v, err := typeutils.ReformatBool(value)
		if err != nil {
			return nil, err
		}
		return apacheiceberg.NewLiteral(v), nil
	case apacheiceberg.Int32Type:
		v, err := typeutils.ReformatInt32(value)
		if err != nil {
			return nil, err
		}
		return apacheiceberg.NewLiteral(v), nil
	case apacheiceberg.Int64Type:
		v, err := typeutils.ReformatInt64(value)
		if err != nil {
			return nil, err
		}
		return apacheiceberg.NewLiteral(v), nil
	case apacheiceberg.Float32Type:
		v, err := typeutils.ReformatFloat32(value)
		if err != nil {
			return nil, err
		}
		return apacheiceberg.NewLiteral(v), nil
	case apacheiceberg.Float64Type:
		v, err := typeutils.ReformatFloat64(value)
		if err != nil {
			return nil, err
		}
		return apacheiceberg.NewLiteral(v), nil
	case apacheiceberg.TimestampType, apacheiceberg.TimestampTzType:
		v, err := typeutils.ReformatDate(value, true)
		if err != nil {
			return nil, err
		}
		return apacheiceberg.NewLiteral(apacheiceberg.Timestamp(v.UnixMicro())), nil
	case apacheiceberg.BinaryType, apacheiceberg.FixedType:
		v, err := typeutils.ReformatBytes(value)
		if err != nil {
			return nil, err
		}
		return apacheiceberg.NewLiteral(v), nil
	case apacheiceberg.StringType:
		return apacheiceberg.NewLiteral(utils.ConvertToString(value)), nil
	default:
		return nil, fmt.Errorf("unsupported partition source type %s", field.Type)
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
		transformed := part.Transform.Apply(apacheiceberg.Optional[apacheiceberg.Literal]{Val: lit, Valid: lit != nil})
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
		files, err := manifest.FetchEntries(fs, true)
		if err != nil {
			return nil, err
		}
		entries = append(entries, files...)
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
