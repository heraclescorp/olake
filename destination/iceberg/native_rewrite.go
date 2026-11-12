package iceberg

import (
	"context"
	"errors"
	"fmt"
	"slices"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	apacheiceberg "github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
	"github.com/datazip-inc/olake/types"
)

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
		if err := fs.Remove(path); err != nil {
			return fmt.Errorf("remove superseded staged data file: %w", err)
		}
		delete(w.paths, path)
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
