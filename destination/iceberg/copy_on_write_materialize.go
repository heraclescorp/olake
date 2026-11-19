package iceberg

import (
	"context"
	"errors"
	"iter"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	arrowutil "github.com/apache/arrow-go/v18/arrow/util"
	apacheiceberg "github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
)

// Materialization scans the entire logical view with all delete kinds applied.
// Bounded windows are concatenated without scalar/JSON conversion, then a single
// clustered writer groups that window under the CURRENT partition spec. Restarting
// the writer per window bounds open partitions even after partition-spec evolution.
// Concatenation temporarily holds both input/output; a single large row can exceed
// the soft byte budget, but is never truncated or silently omitted.
func (w *nativeWriter) materializeCOWView(ctx context.Context, fresh *table.Table) ([]apacheiceberg.DataFile, error) {
	planner := fresh.Scan(table.WithMaxConcurrency(w.cow.options.RewriteConcurrency))
	tasks, err := planner.PlanFiles(ctx)
	err = errors.Join(err, planner.Close())
	if err != nil {
		return nil, err
	}
	var rewritten []apacheiceberg.DataFile
	for _, task := range tasks {
		lineage := fresh.Metadata().Version() >= 3 && task.FirstRowID != nil
		opts := []table.ScanOption{table.WithMaxConcurrency(1), table.WithArrowBatchSize(1)}
		if lineage {
			opts = append(opts, table.WithRowLineage())
		}
		scan := fresh.Scan(opts...)
		arrowSchema, records, err := scan.ReadTasks(ctx, []table.FileScanTask{task})
		if err != nil {
			_ = scan.Close()
			return nil, err
		}
		if lineage {
			arrowSchema, err = table.SchemaToArrowSchemaWithOptions(apacheiceberg.SchemaWithRowLineage(fresh.Schema()), table.ArrowSchemaOptions{IncludeFieldIDs: true, TableProperties: fresh.Metadata().Properties()})
			if err != nil {
				_ = scan.Close()
				return nil, err
			}
		}
		var pending []arrow.RecordBatch
		var rows, bytes int64
		flush := func() error {
			if len(pending) == 0 {
				return nil
			}
			defer func() {
				for _, record := range pending {
					record.Release()
				}
				pending = nil
				rows, bytes = 0, 0
			}()
			columns := make([]arrow.Array, len(arrowSchema.Fields()))
			defer func() {
				for _, column := range columns {
					if column != nil {
						column.Release()
					}
				}
			}()
			for column := range columns {
				parts := make([]arrow.Array, len(pending))
				for index, record := range pending {
					parts[index] = record.Column(column)
				}
				joined, err := array.Concatenate(parts, memory.DefaultAllocator)
				if err != nil {
					return err
				}
				columns[column] = joined
			}
			combined := array.NewRecordBatch(arrowSchema, columns, rows)
			// WriteRecords owns each yielded batch; validation can fail before
			// iteration, so retain our own reference through the whole operation.
			defer combined.Release()
			one := iter.Seq2[arrow.RecordBatch, error](func(yield func(arrow.RecordBatch, error) bool) { combined.Retain(); yield(combined, nil) })
			writeOptions := []table.WriteRecordOption{table.WithClusteredWrite(), table.WithRecordBatchBufferSize(1), table.WithParquetRowGroupLimit(min(w.cow.options.MaxBatchRows, 1024)), table.WithTargetFileSize(int64(w.cow.options.MaxBatchBytes))}
			if lineage {
				writeOptions = append(writeOptions, table.WithPreserveRowLineage(apacheiceberg.SchemaWithRowLineage(fresh.Schema())))
			}
			for file, writeErr := range table.WriteRecords(ctx, fresh, arrowSchema, one, writeOptions...) {
				if writeErr != nil {
					return writeErr
				}
				rewritten = append(rewritten, file)
				w.paths[file.FilePath()] = struct{}{}
			}
			return nil
		}
		err = func() error {
			defer func() {
				for _, record := range pending {
					record.Release()
				}
			}()
			for batch, readErr := range records {
				if readErr != nil {
					return readErr
				}
				size := arrowutil.TotalRecordSize(batch)
				if len(pending) > 0 && (rows+batch.NumRows() > int64(w.cow.options.MaxBatchRows) || bytes+size > int64(w.cow.options.MaxBatchBytes)) {
					if err := flush(); err != nil {
						batch.Release()
						return err
					}
				}
				pending = append(pending, batch)
				rows += batch.NumRows()
				bytes += size
				if rows >= int64(w.cow.options.MaxBatchRows) || bytes >= int64(w.cow.options.MaxBatchBytes) {
					if err := flush(); err != nil {
						return err
					}
				}
			}
			return flush()
		}()
		err = errors.Join(err, scan.Close())
		if err != nil {
			return nil, err
		}
	}
	return rewritten, nil
}
