package iceberg

import (
	"cmp"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"maps"
	"math"
	"slices"
	"strings"
	"sync/atomic"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	apacheiceberg "github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
	"github.com/apache/iceberg-go/table/dv"
	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils/logger"
	"github.com/goccy/go-json"
)

// unavailableValuesResolver fills columns the source could not send (Postgres unchanged TOAST)
// with the value from the row's previous version: from carry if this sync wrote the row,
// else read from the destination at the location the row index (pos or dv) gives.
type unavailableValuesResolver struct {
	// openFiles uploads data files the writer still has open so their rows can be read.
	openFiles openFileWriter
	// index locates a row's newest version: this thread's uncommitted writes first, then
	// the committed index.
	index *types.StreamIndexThread
	// reader reads stored values directly from native data files.
	reader   nativeRowReader
	threadID string
	stream   types.StreamInterface

	tracked    map[string]struct{}  // columns seen marked in this thread
	carry      map[string]*carryRow // olake id -> values this thread wrote
	carryBytes int64
	batch      int64 // batches resolved, to tell recently written rows from old ones

	recovered  int64 // values filled into records
	fromTable  int64 // values read from the destination
	unresolved int64 // values left as the placeholder

	// filledBytes is what the last Resolve added to its batch, which the buffer did not count
	filledBytes int64
}

const (
	// maxCarryBytes caps the memory, across all streams, used to keep values written in this
	// sync. Past it they are dropped and read back from the destination if needed.
	maxCarryBytes = int64(128) * 1024 * 1024 // 128 MB
	// carryEntryOverhead approximates the memory cost of one carried row.
	carryEntryOverhead = int64(96)
)

// unavailableJSON is the placeholder as the stored JSON row (normalization off) holds it.
const unavailableJSON = `"` + constants.UnavailableValue + `"`

// carryUsage is the carry memory held by all threads of this process. The budget is shared
// so that N streams cannot use N times maxCarryBytes.
var carryUsage atomic.Int64

// carryRow is what this thread last wrote for one row. deleted means nothing may be
// carried forward from it.
type carryRow struct {
	deleted bool
	values  map[string]any
	batch   int64 // last batch that wrote the row
}

// size is the carry memory the row is accounted for.
func (row *carryRow) size(olakeID string) int64 {
	size := int64(len(olakeID)) + carryEntryOverhead
	for _, value := range row.values {
		size += valueSize(value)
	}
	return size
}

// pendingRead is one stored row to read: the row it is the previous version of, and the
// source columns wanted from it.
type pendingRead struct {
	olakeID string
	columns []string
}

// openFileWriter is a writer that holds data files open on the Go side.
type openFileWriter interface {
	// EnsureReadable closes any file in paths the writer still has open, without
	// committing it, so its rows can be read (an open file has no footer).
	EnsureReadable(ctx context.Context, paths []string) error
}

type nativeRowReader interface {
	ReadRows(ctx context.Context, path string, positions []int64, columns []string) (map[int64]map[string]any, error)
}

func newUnavailableValuesResolver(threadID string, stream types.StreamInterface, writer Writer, index *types.StreamIndexThread, reader nativeRowReader) *unavailableValuesResolver {
	openFiles, _ := writer.(openFileWriter)
	return &unavailableValuesResolver{
		openFiles: openFiles,
		index:     index,
		reader:    reader,
		threadID:  threadID,
		stream:    stream,
		tracked:   make(map[string]struct{}),
		carry:     make(map[string]*carryRow),
	}
}

// InvalidateColumns forgets cached source values whose destination columns were
// added or dropped. Keep other values and deletion tombstones: neither may be
// reconstructed safely from an older indexed row during this thread.
func (r *unavailableValuesResolver) InvalidateColumns(columns []string) {
	if r == nil || len(columns) == 0 {
		return
	}
	changed := make(map[string]struct{}, len(columns))
	for _, column := range columns {
		changed[column] = struct{}{}
	}
	var sources []string
	for column := range r.tracked {
		if _, invalidated := changed[r.stream.ResolveColumnName(column)]; invalidated {
			sources = append(sources, column)
			delete(r.tracked, column)
		}
	}
	if len(sources) == 0 {
		return
	}
	for olakeID, row := range r.carry {
		if row.deleted {
			continue
		}
		for _, column := range sources {
			r.account(-valueSize(row.values[column]))
			delete(row.values, column)
		}
		if len(row.values) == 0 {
			r.account(-row.size(olakeID))
			delete(r.carry, olakeID)
		}
	}
}

// Resolve fills every unavailable value in the batch it can recover. The rest keep
// constants.UnavailableValue, as before this feature.
func (r *unavailableValuesResolver) Resolve(ctx context.Context, records []types.RawRecord) error {
	r.filledBytes = 0
	marked := r.track(records)
	// Once a column is tracked, every later batch is processed so carry stays up to date, even batches with no marker.
	if len(r.tracked) == 0 {
		return nil
	}
	r.batch++

	if marked {
		if err := r.readMissing(ctx, records); err != nil {
			return err
		}
	}

	// Records are applied in order on top of carry, which now holds each row's state from
	// before the batch, so every record sees exactly what the changes before it wrote.
	for idx := range records {
		record := &records[idx]
		olakeID := record.OlakeColumns[constants.OlakeID].(string)

		if record.OlakeColumns[constants.OpType].(string) == "d" {
			r.tombstone(olakeID)
			continue
		}

		for _, column := range record.UnavailableColumns {
			if _, selected := record.Data[column]; !selected {
				continue // column not synced
			}
			if row := r.carry[olakeID]; row != nil {
				if value, carried := row.values[column]; carried {
					record.Data[column] = value
					r.recovered++
					r.filledBytes += valueSize(value)
					continue
				}
			}
			r.unresolved++
		}

		// Remember the values this record does carry, for the next change to the row.
		for column := range r.tracked {
			if value, present := record.Data[column]; present && !isUnavailable(value) {
				r.remember(olakeID, column, value)
			}
		}
	}

	r.trim()

	return nil
}

// readMissing reads from the destination the values carry cannot answer and adds them to
// carry. Only a row's first record in the batch can need a read: every later record of the
// row is answered by what the earlier ones wrote.
func (r *unavailableValuesResolver) readMissing(ctx context.Context, records []types.RawRecord) error {
	seen := make(map[string]struct{})
	reads := make(map[types.RowLocation]*pendingRead)

	for idx := range records {
		record := &records[idx]
		olakeID := record.OlakeColumns[constants.OlakeID].(string)
		if _, done := seen[olakeID]; done {
			continue
		}
		seen[olakeID] = struct{}{}

		row := r.carry[olakeID]
		if row != nil && row.deleted {
			continue // deleted earlier in this sync: old values must not come back
		}

		var missing []string
		for _, column := range record.UnavailableColumns {
			if _, selected := record.Data[column]; !selected {
				continue
			}
			if row != nil {
				if _, carried := row.values[column]; carried {
					continue
				}
			}
			missing = append(missing, column)
		}
		if len(missing) == 0 {
			continue
		}

		location, found, err := r.index.Lookup(olakeID)
		if err != nil {
			return fmt.Errorf("failed to look up row[%s] in index: %w", olakeID, err)
		}
		if !found {
			continue // no previous version: a filtered row, or the UPDATE changed the primary key
		}
		reads[location] = &pendingRead{olakeID: olakeID, columns: missing}
	}

	if len(reads) == 0 {
		return nil
	}

	return r.read(ctx, reads)
}

// track adds this batch's marked columns to r.tracked and reports whether the batch has any.
func (r *unavailableValuesResolver) track(records []types.RawRecord) bool {
	marked := false
	for idx := range records {
		for _, column := range records[idx].UnavailableColumns {
			r.tracked[column] = struct{}{}
			marked = true
		}
	}

	return marked
}

// readColumns is what to read of one stored row: _op_type, to spot a soft-deleted row (an
// UPDATE can move a row onto a key deleted in an earlier sync), then the destination columns
// holding the wanted source columns. Without normalization that is the one column holding
// the whole row as JSON.
func (r *unavailableValuesResolver) readColumns(columns []string) []string {
	if !r.stream.NormalizationEnabled() {
		return []string{constants.OpType, constants.StringifiedData}
	}

	read := make([]string, 0, len(columns)+1)
	read = append(read, constants.OpType)
	for _, column := range columns {
		read = append(read, r.stream.ResolveColumnName(column))
	}
	return read
}

// read groups physical positions and projected columns by file. All open files are
// flushed before any reads, without committing their pending table changes.
func (r *unavailableValuesResolver) read(ctx context.Context, reads map[types.RowLocation]*pendingRead) error {
	rows := make(map[string][]int64)
	columns := make(map[string]map[string]struct{})
	for location, pending := range reads {
		rows[location.FilePath] = append(rows[location.FilePath], location.Position)
		if columns[location.FilePath] == nil {
			columns[location.FilePath] = make(map[string]struct{})
		}
		for _, column := range r.readColumns(pending.columns) {
			columns[location.FilePath][column] = struct{}{}
		}
	}
	paths := slices.Sorted(maps.Keys(rows))
	if r.openFiles != nil {
		if err := r.openFiles.EnsureReadable(ctx, paths); err != nil {
			return err
		}
	}
	for _, path := range paths {
		slices.Sort(rows[path])
		stored, err := r.reader.ReadRows(ctx, path, rows[path], slices.Sorted(maps.Keys(columns[path])))
		if err != nil {
			return fmt.Errorf("failed to read unavailable column values: %w", err)
		}
		for position, row := range stored {
			pending := reads[types.RowLocation{FilePath: path, Position: position}]
			if pending == nil {
				return fmt.Errorf("native row reader returned an unrequested position %d", position)
			}
			if opType, _ := row[constants.OpType].(string); opType == "d" {
				continue
			}
			values, err := r.rowValues(row, pending.columns)
			if err != nil {
				return fmt.Errorf("failed to decode stored row at position %d: %w", position, err)
			}
			for column, value := range values {
				r.remember(pending.olakeID, column, value)
				r.fromTable++
			}
		}
	}
	return nil
}

// rowValues picks the wanted source columns out of one stored row, keyed by destination
// column. A column the data file does not carry, that is NULL there, or that holds the
// placeholder is left out, so the record keeps the placeholder: a value the source could not
// send is never NULL (Postgres keeps only non-NULL values out of line), so a stored NULL was
// written without the value, for example while the column was deselected. Without
// normalization the row is one JSON document, parsed once, and each column's raw JSON is
// returned.
func (r *unavailableValuesResolver) rowValues(stored map[string]any, columns []string) (map[string]any, error) {
	values := make(map[string]any, len(columns))
	if r.stream.NormalizationEnabled() {
		for _, column := range columns {
			if value := stored[r.stream.ResolveColumnName(column)]; value != nil && !isUnavailable(value) {
				values[column] = value
			}
		}
		return values, nil
	}

	encoded, _ := stored[constants.StringifiedData].(string)
	var row map[string]json.RawMessage
	if err := json.Unmarshal([]byte(encoded), &row); err != nil {
		return nil, fmt.Errorf("failed to parse stored row: %s", err)
	}
	for _, column := range columns {
		if raw, present := row[column]; present && string(raw) != "null" && string(raw) != unavailableJSON {
			values[column] = raw
		}
	}

	return values, nil
}

// remember stores the newest value this thread wrote for a row's column.
func (r *unavailableValuesResolver) remember(olakeID, column string, value any) {
	row := r.carry[olakeID]
	if row == nil || row.deleted {
		if row == nil {
			// a tombstone already counted this entry
			r.account(int64(len(olakeID)) + carryEntryOverhead)
		}
		row = &carryRow{values: make(map[string]any, len(r.tracked))}
		r.carry[olakeID] = row
	}

	r.account(valueSize(value) - valueSize(row.values[column]))
	row.values[column] = value
	row.batch = r.batch
}

// tombstone marks a row deleted and releases the values it carried.
func (r *unavailableValuesResolver) tombstone(olakeID string) {
	if row := r.carry[olakeID]; row != nil {
		for _, value := range row.values {
			r.account(-valueSize(value))
		}
	} else {
		r.account(int64(len(olakeID)) + carryEntryOverhead)
	}

	r.carry[olakeID] = &carryRow{deleted: true, batch: r.batch}
}

// account adds delta carried bytes to this thread and to the process total.
func (r *unavailableValuesResolver) account(delta int64) {
	r.carryBytes += delta
	carryUsage.Add(delta)
}

// trim brings the process back to half the budget once it is over, dropping this thread's
// least recently written rows first. Nothing is lost: the index still locates every dropped
// row, so its next change reads it back. Old rows go first because they sit in data files
// already closed, which costs just a read; a recent row may sit in a file still open, which
// the read would have to close early. Halving keeps trims rare.
func (r *unavailableValuesResolver) trim() {
	if carryUsage.Load() <= maxCarryBytes {
		return
	}

	olakeIDs := slices.SortedFunc(maps.Keys(r.carry), func(a, b string) int {
		return cmp.Compare(r.carry[a].batch, r.carry[b].batch)
	})
	dropped := 0
	for _, olakeID := range olakeIDs {
		if carryUsage.Load() <= maxCarryBytes/2 {
			break
		}
		r.account(-r.carry[olakeID].size(olakeID))
		delete(r.carry, olakeID)
		dropped++
	}

	logger.Debugf("Thread[%s]: dropped %d of %d carried row(s), %d bytes left", r.threadID, dropped, dropped+len(r.carry), r.carryBytes)
}

// Close releases this thread's carry and logs what it recovered.
func (r *unavailableValuesResolver) Close() {
	carryUsage.Add(-r.carryBytes)
	r.carryBytes = 0
	r.carry = make(map[string]*carryRow)

	if r.recovered+r.unresolved == 0 {
		return
	}

	logger.Infof("Thread[%s]: recovered %d unavailable column value(s), read %d from the destination table, %d could not be recovered",
		r.threadID, r.recovered, r.fromTable, r.unresolved)
}

func valueSize(value any) int64 {
	switch typed := value.(type) {
	case nil:
		return 0
	case string:
		return int64(len(typed))
	case json.RawMessage:
		return int64(len(typed))
	case []byte:
		return int64(len(typed))
	case []any: // Postgres arrays
		size := int64(0)
		for _, element := range typed {
			size += valueSize(element)
		}
		return size
	default:
		return 16
	}
}

func isUnavailable(value any) bool {
	text, ok := value.(string)
	return ok && text == constants.UnavailableValue
}

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
				if position < 0 {
					return false, errors.New("native index row position is negative or overflows int64")
				}
				if deleted.Contains(uint64(position)) || batch.Column(0).IsNull(row) {
					continue
				}
				key, ok := nativeIndexValue(batch.Column(0), row).(string)
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
				row[field.Name] = nativeIndexValue(batch.Column(col), int(position-base))
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
			// Upstream iceberg-go v0.7.0 remains unpatched: missing-column,
			// default-only projections can fail with zero-column record errors.
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

// nativeIndexValue returns nil for SQL NULL; Arrow conversions cannot fail.
func nativeIndexValue(column arrow.Array, row int) any {
	if column.IsNull(row) {
		return nil
	}
	switch value := column.(type) {
	case *array.String:
		return strings.Clone(value.Value(row))
	case *array.LargeString:
		return strings.Clone(value.Value(row))
	case *array.StringView:
		return strings.Clone(value.Value(row))
	case *array.Binary:
		return slices.Clone(value.Value(row))
	case *array.LargeBinary:
		return slices.Clone(value.Value(row))
	case *array.FixedSizeBinary:
		return slices.Clone(value.Value(row))
	case *array.BinaryView:
		return slices.Clone(value.Value(row))
	case *array.Boolean:
		return value.Value(row)
	case *array.Int32:
		return value.Value(row)
	case *array.Int64:
		return value.Value(row)
	case *array.Float32:
		return value.Value(row)
	case *array.Float64:
		return value.Value(row)
	case *array.Dictionary:
		return nativeIndexValue(value.Dictionary(), value.GetValueIndex(row))
	default:
		// TOAST-bearing source values (text, JSON, arrays) are stored as strings;
		// the other primitive values retain their Arrow logical representation.
		return column.GetOneForMarshal(row)
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
						position := base + int64(row)
						if position < 0 {
							return false, errors.New("native equality delete row position is negative or overflows int64")
						}
						deleted.Set(uint64(position))
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
			filePath, ok := nativeIndexValue(batch.Column(0), row).(string)
			if !ok {
				return false, errors.New("native positional delete has a null or invalid file path")
			}
			position, ok := batch.Column(1).(*array.Int64)
			if !ok || position.IsNull(row) {
				return false, errors.New("invalid native positional delete row")
			}
			signedPosition := position.Value(row)
			if signedPosition < 0 {
				return false, errors.New("invalid native positional delete row")
			}
			fileKey := deleteFile.FilePath() + "\x00" + filePath
			bitmap := cache.positionsByFile[fileKey]
			if bitmap == nil {
				bitmap = dv.NewRoaringPositionBitmap()
				cache.positionsByFile[fileKey] = bitmap
			}
			bitmap.Set(uint64(signedPosition))
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
		count := task.File.Count()
		if count < 0 {
			return nil, nil, errors.New("native data file has a negative row count")
		}
		for position := range after.Positions() {
			if position > math.MaxInt64 {
				return nil, nil, errors.New("native equality delete position exceeds signed row position range")
			}
			rowPosition := int64(position)
			if rowPosition >= count {
				return nil, nil, errors.New("native equality delete position exceeds data file row count")
			}
			if !before.Contains(position) {
				positions[task.File.FilePath()] = append(positions[task.File.FilePath()], rowPosition)
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
			filePath, ok := nativeIndexValue(batch.Column(0), row).(string)
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

type nativeStagedRewrite struct {
	original    apacheiceberg.DataFile
	replacement apacheiceberg.DataFile
	removed     []int64
}

func (r nativeStagedRewrite) remap(location types.RowLocation) (types.RowLocation, error) {
	if location.Position < 0 || location.Position >= r.original.Count() {
		return types.RowLocation{}, errors.New("pending index ordinal is outside its staged data file")
	}
	before, deleted := slices.BinarySearch(r.removed, location.Position)
	if deleted || r.replacement == nil {
		return types.RowLocation{}, errors.New("pending index points at a superseded staged row")
	}
	return types.RowLocation{FilePath: r.replacement.FilePath(), Position: location.Position - int64(before)}, nil
}

// Earlier versions written by this thread have never been published. Remove
// those versions from OUR staged files instead of publishing position deletes
// referencing same-delta additions: iceberg-go v0.7.0 correctly validates old
// references against the current branch, but also rejects its own new paths.
// Published files are never rewritten here and retain normal RowDelta conflict
// validation. Files stay readable at their original ordinals until Close.
func (w *nativeWriter) compactStagedDataFiles(ctx context.Context) error {
	if len(w.positions) == 0 {
		return nil
	}
	needed := false
	for _, file := range w.dataFiles {
		if len(w.positions[file.FilePath()]) > 0 {
			needed = true
			break
		}
	}
	if !needed {
		return nil
	}
	fs, err := w.table.FS(nativeCatalogContext(ctx, w.cat))
	if err != nil {
		return err
	}
	rewrites := make(map[string]nativeStagedRewrite)
	files := make([]apacheiceberg.DataFile, 0, len(w.dataFiles))
	for _, file := range w.dataFiles {
		path := file.FilePath()
		positions := w.positions[path]
		if len(positions) == 0 {
			files = append(files, file)
			continue
		}
		// Membership in dataFiles is necessary but not sufficient: never rewrite
		// any path that was not allocated by this writer's native FileIO path.
		if _, ours := w.paths[path]; !ours {
			return errors.New("refusing to rewrite a data file not staged by this writer")
		}
		removed := slices.Clone(positions)
		slices.Sort(removed)
		removed = slices.Compact(removed)
		if removed[0] < 0 || removed[len(removed)-1] >= file.Count() {
			return errors.New("superseded staged ordinal is outside its data file")
		}
		rewrite := nativeStagedRewrite{original: file, removed: removed}
		if int64(len(removed)) < file.Count() {
			schema := w.dataSchemas[path]
			if schema == nil {
				return errors.New("staged data file has no recorded Iceberg schema")
			}
			spec := w.table.Metadata().PartitionSpecByID(int(file.SpecID()))
			if spec == nil {
				return fmt.Errorf("staged data file partition spec %d is missing", file.SpecID())
			}
			arrowSchema, err := table.SchemaToArrowSchema(schema, nil, true, false)
			if err != nil {
				return err
			}
			var readRows int64
			replacement, err := w.writeNativeParquetStream(ctx, schema, *spec, file.Partition(), arrowSchema, func(writer *pqarrow.FileWriter) error {
				return readNativePhysicalBatches(ctx, fs, w.table, path, schema, nil, func(base int64, batch arrow.RecordBatch) (bool, error) {
					readRows += batch.NumRows()
					first, _ := slices.BinarySearch(removed, base)
					end, _ := slices.BinarySearch(removed, base+batch.NumRows())
					if first == end {
						return true, writer.WriteBuffered(batch)
					}
					if int64(end-first) == batch.NumRows() {
						return true, nil
					}
					filterBuilder := array.NewBooleanBuilder(memory.DefaultAllocator)
					filterBuilder.Reserve(int(batch.NumRows()))
					position := first
					for row := range batch.NumRows() {
						keep := position == end || removed[position] != base+row
						filterBuilder.Append(keep)
						if !keep {
							position++
						}
					}
					filter := filterBuilder.NewBooleanArray()
					filterBuilder.Release()
					filtered, err := compute.FilterRecordBatch(ctx, batch, filter, &compute.FilterOptions{})
					filter.Release()
					if err != nil {
						return false, err
					}
					err = writer.WriteBuffered(filtered)
					filtered.Release()
					return err == nil, err
				})
			}, apacheiceberg.EntryContentData, nil, nil)
			if err != nil {
				return fmt.Errorf("rewrite superseded staged data: %w", err)
			}
			if readRows != file.Count() || replacement.Count() != file.Count()-int64(len(removed)) {
				return errors.New("staged rewrite row counts do not match its physical source")
			}
			rewrite.replacement = replacement
			files = append(files, replacement)
		}
		rewrites[path] = rewrite
	}

	// Compute every pending remap before mutating the index or deleting any
	// original. A partial read/write failure leaves all originals available for
	// Abort, and no local-index mutations are ever made durable before commit.
	type pendingRemap struct {
		key      string
		location types.RowLocation
	}
	var pending []pendingRemap
	if w.indexThread != nil {
		if err := w.indexThread.Range(func(key string, location types.RowLocation) error {
			rewrite, affected := rewrites[location.FilePath]
			if !affected {
				return nil
			}
			mapped, err := rewrite.remap(location)
			if err != nil {
				return err
			}
			pending = append(pending, pendingRemap{key: key, location: mapped})
			return nil
		}); err != nil {
			return err
		}
	}
	for _, location := range w.latest {
		if rewrite, affected := rewrites[location.FilePath]; affected {
			if _, err := rewrite.remap(location); err != nil {
				return err
			}
		}
	}
	for _, file := range w.dataFiles {
		path := file.FilePath()
		if _, rewritten := rewrites[path]; !rewritten {
			continue
		}
		if err := w.discardStagedFile(fs, path); err != nil {
			return fmt.Errorf("remove superseded staged data file: %w", err)
		}
	}
	for _, change := range pending {
		w.indexThread.Put(change.key, change.location)
	}
	for key, location := range w.latest {
		if rewrite, affected := rewrites[location.FilePath]; affected {
			mapped, _ := rewrite.remap(location)
			w.latest[key] = mapped
		}
	}
	for path, rewrite := range rewrites {
		if rewrite.replacement != nil {
			w.dataSchemas[rewrite.replacement.FilePath()] = w.dataSchemas[path]
		}
		delete(w.dataSchemas, path)
		delete(w.positions, path)
	}
	w.dataFiles = files
	return nil
}
