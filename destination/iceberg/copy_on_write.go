package iceberg

import (
	"context"
	"errors"
	"fmt"
	"maps"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	apacheiceberg "github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils/logger"
	"github.com/google/uuid"
)

// Replay inputs contain source rows. The table FileIO supplies the same access
// controls and encryption as data files without accumulating payloads on /tmp.
type nativeCOW struct {
	paths []string
}

func (c *nativeCOW) cleanup(ctx context.Context, w *nativeWriter) {
	if len(c.paths) == 0 {
		return
	}
	store, err := w.table.FS(nativeCatalogContext(ctx, w.cat))
	if err != nil {
		logger.Warnf("COW replay input cleanup unavailable table=%q inputs=%d", w.table.Identifier(), len(c.paths))
		return
	}
	retained := c.paths[:0]
	for _, path := range c.paths {
		if err := w.discardStagedFile(store, path); err != nil {
			retained = append(retained, path)
		}
	}
	if len(retained) != 0 {
		logger.Warnf("COW replay input cleanup incomplete table=%q retained_inputs=%d first_input=%q", w.table.Identifier(), len(retained), retained[0])
	}
	clear(c.paths[len(retained):])
	c.paths = retained
}

func (w *nativeWriter) writeCopyOnWrite(ctx context.Context, records []types.RawRecord) error {
	rows := make([]map[string]any, 0, len(records))
	last := make(map[string]int, len(records))
	for _, record := range records {
		if err := ctx.Err(); err != nil {
			return err
		}
		if record.Data == nil {
			continue
		}
		if len(record.UnavailableColumns) > 0 {
			return errors.New("copy-on-write requires source TOAST hydration; destination index hydration is unsupported")
		}
		row := record.Data
		maps.Copy(row, record.OlakeColumns)
		id, ok := row[constants.OlakeID].(string)
		if !ok {
			return errors.New("copy-on-write requires the canonical string _olake_id")
		}
		op, ok := row[constants.OpType].(string)
		if !ok || (op != "r" && op != "c" && op != "i" && op != "u" && op != "d") {
			return fmt.Errorf("copy-on-write requires a supported source operation, got %q", op)
		}
		if op == "i" {
			row[constants.OpType] = "c"
		}
		// Deletes remain OLake soft-delete tombstone after-images, as with MOR.
		if previous, found := last[id]; found {
			rows[previous] = nil
		}
		last[id] = len(rows)
		rows = append(rows, row)
	}
	latest := rows[:0]
	for _, row := range rows {
		if row != nil {
			latest = append(latest, row)
		}
	}
	if len(latest) == 0 {
		return nil
	}
	batch, err := nativeArrowRecord(w.table.Schema(), latest)
	if err != nil {
		return err
	}
	defer batch.Release()
	return w.cow.spill(ctx, w, batch)
}

func (c *nativeCOW) spill(ctx context.Context, w *nativeWriter, batch arrow.RecordBatch) error {
	store, err := w.table.FS(nativeCatalogContext(ctx, w.cat))
	if err != nil {
		return err
	}
	writable, ok := store.(iceio.WriteFileIO)
	if !ok {
		return table.ErrWriteIORequired
	}
	provider, err := w.table.LocationProvider()
	if err != nil {
		return err
	}
	path := provider.NewDataLocation("olake-cow-" + uuid.NewString() + ".arrow")
	w.paths[path] = struct{}{}
	file, err := writable.Create(path)
	if err != nil {
		return err
	}
	writer := ipc.NewWriter(file, ipc.WithSchema(batch.Schema()))
	err = writer.Write(batch)
	err = errors.Join(err, writer.Close(), file.Close())
	if err != nil {
		return fmt.Errorf("stage COW replay input: %w", err)
	}
	c.paths = append(c.paths, path)
	return nil
}

func nativeCOWFilter(batch arrow.RecordBatch) (apacheiceberg.BooleanExpression, error) {
	indices := batch.Schema().FieldIndices(constants.OlakeID)
	if len(indices) != 1 {
		return nil, errors.New("copy-on-write requires one canonical _olake_id column")
	}
	ids, ok := batch.Column(indices[0]).(*array.String)
	if !ok || ids.NullN() != 0 {
		return nil, errors.New("copy-on-write requires nonnull string _olake_id values")
	}
	values := make([]apacheiceberg.Literal, ids.Len())
	for row := range values {
		values[row] = apacheiceberg.NewLiteral(ids.Value(row))
	}
	term := apacheiceberg.Reference(constants.OlakeID)
	return apacheiceberg.NewAnd(apacheiceberg.NotNull(term), apacheiceberg.SetPredicate(apacheiceberg.OpIn, term, values)), nil
}

func (w *nativeWriter) replayCopyOnWrite(ctx context.Context, tx *table.Transaction) error {
	if len(w.cow.paths) == 0 {
		return nil
	}
	store, err := w.table.FS(nativeCatalogContext(ctx, w.cat))
	if err != nil {
		return err
	}
	for _, path := range w.cow.paths {
		err := func() error {
			file, err := store.Open(path)
			if err != nil {
				return err
			}
			defer file.Close()
			reader, err := ipc.NewReader(file)
			if err != nil {
				return err
			}
			defer reader.Release()
			if !reader.Next() {
				if err := reader.Err(); err != nil {
					return err
				}
				return errors.New("empty COW replay segment")
			}
			batch := reader.RecordBatch()
			filter, err := nativeCOWFilter(batch)
			if err != nil {
				return err
			}
			records, err := array.NewRecordReader(batch.Schema(), []arrow.RecordBatch{batch})
			if err != nil {
				return err
			}
			// Overwrite consumes but does not release its reader. NewRecordReader
			// retains the batch; IPC keeps its own reference until this call ends.
			defer records.Release()
			return tx.Overwrite(ctx, records, apacheiceberg.Properties{"olake.thread-id": w.options.ThreadID}, table.WithOverwriteFilter(filter))
		}()
		if err != nil {
			return err
		}
	}
	// Stock Overwrite owns its output paths. They are deliberately not guessed
	// or deleted on rejection; normal Iceberg orphan maintenance owns cleanup.
	return nil
}

func nativeCOWHasDeletes(ctx context.Context, tbl *table.Table) (bool, error) {
	snapshot := tbl.CurrentSnapshot()
	if snapshot == nil {
		return false, nil
	}
	fs, err := tbl.FS(ctx)
	if err != nil {
		return false, err
	}
	manifests, err := snapshot.Manifests(fs)
	if err != nil {
		return false, err
	}
	for _, manifest := range manifests {
		if err := ctx.Err(); err != nil {
			return false, err
		}
		switch manifest.ManifestContent() {
		case apacheiceberg.ManifestContentData:
			continue
		case apacheiceberg.ManifestContentDeletes:
		default:
			return false, fmt.Errorf("unknown Iceberg manifest content %d", manifest.ManifestContent())
		}
		for _, err := range manifest.Entries(fs, true) {
			if err != nil {
				return false, err
			}
			return true, nil
		}
	}
	return false, nil
}

func validateNativeCOWTable(ctx context.Context, tbl *table.Table) error {
	if tbl.Metadata().Version() < 2 || tbl.Metadata().Version() > 3 {
		return errors.New("copy-on-write requires Iceberg format v2 or v3")
	}
	field, found := tbl.Schema().FindFieldByName(constants.OlakeID)
	if !found || !field.Type.Equals(apacheiceberg.StringType{}) {
		return errors.New("copy-on-write requires the canonical string _olake_id column")
	}
	hasDeletes, err := nativeCOWHasDeletes(ctx, tbl)
	if err != nil {
		return err
	}
	if hasDeletes {
		return errors.New("copy-on-write requires a data-only current snapshot; perform a separate explicit maintenance rewrite to clean up existing live delete files before enabling copy-on-write")
	}
	return nil
}
