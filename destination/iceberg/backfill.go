package iceberg

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"maps"
	"slices"
	"sync"
	"sync/atomic"

	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	apacheiceberg "github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/types"
	"github.com/google/uuid"
)

const (
	nativeBackfillTargetBytes   = int64(512 * 1024 * 1024)
	nativeBackfillMaxPartitions = 8
	nativeBackfillBatchBytes    = int64(4 * 1024 * 1024)
	nativeBackfillBatchRows     = 8192
	nativeBackfillFragmentBytes = 256 * 1024
)

type nativeAsyncFragment struct {
	data    []byte
	barrier bool
}

type nativeAsyncOutput struct {
	file    iceio.FileWriter
	ctx     context.Context
	queue   chan nativeAsyncFragment
	free    chan []byte
	ack     chan struct{}
	stopped chan struct{}
	buffer  []byte
	mu      sync.Mutex
	err     error
	size    atomic.Int64
}

func newNativeAsyncOutput(ctx context.Context, file iceio.FileWriter) *nativeAsyncOutput {
	out := &nativeAsyncOutput{file: file, ctx: ctx, queue: make(chan nativeAsyncFragment, 1), free: make(chan []byte, 2), ack: make(chan struct{}, 1), stopped: make(chan struct{})}
	go out.run()
	return out
}

func (out *nativeAsyncOutput) run() {
	defer close(out.stopped)
	for fragment := range out.queue {
		if fragment.data != nil && out.failure() == nil {
			n, err := out.file.Write(fragment.data)
			out.size.Add(int64(n))
			if err == nil && n != len(fragment.data) {
				err = io.ErrShortWrite
			}
			if err != nil {
				out.mu.Lock()
				out.err = err
				out.mu.Unlock()
			}
		}
		if fragment.data != nil {
			select {
			case out.free <- fragment.data[:0]:
			default:
			}
		}
		if fragment.barrier {
			out.ack <- struct{}{}
		}
	}
}

func (out *nativeAsyncOutput) failure() error {
	out.mu.Lock()
	defer out.mu.Unlock()
	return out.err
}

func (out *nativeAsyncOutput) send(fragment nativeAsyncFragment) error {
	if err := out.failure(); err != nil {
		return err
	}
	select {
	case <-out.ctx.Done():
		return out.ctx.Err()
	case out.queue <- fragment:
		return nil
	}
}

func (out *nativeAsyncOutput) Write(data []byte) (int, error) {
	written := 0
	for len(data) != 0 {
		if out.buffer == nil {
			select {
			case out.buffer = <-out.free:
			default:
				out.buffer = make([]byte, 0, nativeBackfillFragmentBytes)
			}
		}
		n := min(len(data), cap(out.buffer)-len(out.buffer))
		out.buffer = append(out.buffer, data[:n]...)
		data = data[n:]
		written += n
		if len(out.buffer) == cap(out.buffer) {
			if err := out.send(nativeAsyncFragment{data: out.buffer}); err != nil {
				return written, err
			}
			out.buffer = nil
		}
	}
	return written, out.failure()
}

func (out *nativeAsyncOutput) flush() error {
	if len(out.buffer) != 0 {
		if err := out.send(nativeAsyncFragment{data: out.buffer}); err != nil {
			return err
		}
		out.buffer = nil
	}
	if err := out.send(nativeAsyncFragment{barrier: true}); err != nil {
		return err
	}
	select {
	case <-out.ctx.Done():
		return out.ctx.Err()
	case <-out.ack:
		return out.failure()
	}
}

func (out *nativeAsyncOutput) stop() {
	close(out.queue)
	<-out.stopped
	out.buffer = nil
}

type nativeBackfillFile struct {
	path          string
	schema        *apacheiceberg.Schema
	spec          apacheiceberg.PartitionSpec
	partition     map[int]any
	parquet       *pqarrow.FileWriter
	output        *nativeAsyncOutput
	cancel        context.CancelFunc
	stopExecution func() bool
	rows          int64
	lastUsed      uint64
	finished      bool
}

func (w *nativeWriter) openBackfillFile(ctx context.Context, partition map[int]any) (_ *nativeBackfillFile, err error) {
	fileCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	var stopExecution func() bool
	if w.config != nil && w.config.executionContext != nil {
		stopExecution = context.AfterFunc(w.config.executionContext, cancel)
	}
	stopOperation := context.AfterFunc(ctx, cancel)
	defer stopOperation()
	defer func() {
		if err != nil {
			cancel()
			if stopExecution != nil {
				stopExecution()
			}
		}
	}()
	fs, err := w.table.FS(nativeCatalogContext(fileCtx, w.cat))
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
	schema, spec := w.table.Schema(), w.table.Spec()
	name := uuid.NewString() + ".parquet"
	if spec.NumFields() > 0 {
		values := make(nativePartition, spec.NumFields())
		for idx, field := range spec.Fields() {
			values[idx] = partition[field.FieldID]
		}
		name = spec.PartitionToPath(values, schema) + "/" + name
	}
	path := provider.NewDataLocation(name)
	w.paths[path] = struct{}{}
	file, err := writeFS.Create(path)
	if err != nil {
		return nil, err
	}
	out := newNativeAsyncOutput(fileCtx, file)
	fileState := &nativeBackfillFile{path: path, schema: schema, spec: spec, partition: partition, output: out, cancel: cancel, stopExecution: stopExecution}
	defer func() {
		if err != nil {
			fileState.abort()
		}
	}()
	arrowSchema, err := table.SchemaToArrowSchema(schema, nil, true, false)
	if err != nil {
		return nil, err
	}
	props := parquet.NewWriterProperties(parquet.WithCompression(compress.Codecs.Zstd), parquet.WithCompressionLevel(3), parquet.WithStats(true), parquet.WithMaxRowGroupLength(64*1024), parquet.WithRootName("table"), parquet.WithPageIndexEnabled(true))
	fileState.parquet, err = pqarrow.NewFileWriter(arrowSchema, out, props, pqarrow.DefaultWriterProps())
	if err != nil {
		return nil, err
	}
	schemaJSON, err := json.Marshal(schema)
	if err == nil {
		err = fileState.parquet.AppendKeyValueMetadata("iceberg.schema", string(schemaJSON))
	}
	if err == nil {
		err = out.flush()
	}
	if err == nil {
		err = ctx.Err()
	}
	if err != nil {
		return nil, err
	}
	return fileState, nil
}

func (f *nativeBackfillFile) abort() {
	if f.finished {
		return
	}
	f.finished = true
	f.cancel()
	if f.parquet != nil {
		_ = f.parquet.Close()
	}
	f.output.stop()
	if aborter, ok := f.output.file.(interface{ abort() error }); ok {
		_ = aborter.abort()
	} else {
		_ = f.output.file.Close()
	}
	if f.stopExecution != nil {
		f.stopExecution()
	}
}

func (w *nativeWriter) finishBackfillFile(ctx context.Context, key string) (err error) {
	f := w.backfillFiles[key]
	if f == nil {
		return nil
	}
	stop := context.AfterFunc(ctx, f.cancel)
	defer stop()
	if err = ctx.Err(); err != nil {
		return err
	}
	if err = f.parquet.Close(); err == nil {
		err = f.output.flush()
	}
	if err == nil {
		err = ctx.Err()
	}
	if err != nil {
		return err
	}
	f.output.stop()
	err = f.output.file.Close()
	f.finished = true
	f.cancel()
	if f.stopExecution != nil {
		f.stopExecution()
	}
	if err != nil {
		return err
	}
	if err = ctx.Err(); err != nil {
		return err
	}
	metadata, err := f.parquet.FileMetadata()
	if err != nil {
		return err
	}
	file, err := table.DataFileFromMetadata(table.DataFileArgs{Schema: f.schema, Spec: f.spec, Format: apacheiceberg.ParquetFile, Metadata: metadata, FilePath: f.path, FileSize: f.output.size.Load(), Content: apacheiceberg.EntryContentData, PartitionValues: f.partition, Properties: w.table.Properties(), FormatVersion: w.table.Metadata().Version()})
	if err != nil {
		return err
	}
	w.dataFiles = append(w.dataFiles, file)
	if w.dataSchemas == nil {
		w.dataSchemas = make(map[string]*apacheiceberg.Schema)
	}
	w.dataSchemas[f.path] = f.schema
	delete(w.backfillFiles, key)
	return nil
}

func (w *nativeWriter) finishBackfillFiles(ctx context.Context) error {
	for _, key := range slices.Sorted(maps.Keys(w.backfillFiles)) {
		if err := w.finishBackfillFile(ctx, key); err != nil {
			return err
		}
	}
	return nil
}

func (w *nativeWriter) backfillFile(ctx context.Context, key string, partition map[int]any) (*nativeBackfillFile, error) {
	if f := w.backfillFiles[key]; f != nil {
		w.backfillClock++
		f.lastUsed = w.backfillClock
		return f, nil
	}
	limit := w.backfillPartitionLimit
	if limit <= 0 {
		limit = nativeBackfillMaxPartitions
	}
	if len(w.backfillFiles) >= limit {
		var victim string
		var oldest uint64
		first := true
		for k, f := range w.backfillFiles {
			if first || f.lastUsed < oldest || f.lastUsed == oldest && k < victim {
				victim, oldest, first = k, f.lastUsed, false
			}
		}
		if err := w.finishBackfillFile(ctx, victim); err != nil {
			return nil, err
		}
	}
	f, err := w.openBackfillFile(ctx, partition)
	if err != nil {
		return nil, err
	}
	if w.backfillFiles == nil {
		w.backfillFiles = make(map[string]*nativeBackfillFile)
	}
	w.backfillClock++
	f.lastUsed = w.backfillClock
	w.backfillFiles[key] = f
	return f, nil
}

func (w *nativeWriter) writeBackfill(ctx context.Context, records []types.RawRecord) error {
	for start := 0; start < len(records); {
		end, bytes := start, int64(0)
		for end < len(records) && end-start < nativeBackfillBatchRows {
			size := carryEntryOverhead
			for _, value := range records[end].Data {
				size += valueSize(value)
			}
			for _, value := range records[end].OlakeColumns {
				size += valueSize(value)
			}
			if end > start && bytes+size > nativeBackfillBatchBytes {
				break
			}
			bytes += size
			end++
		}
		groups := make(map[string]*nativeWriteGroup)
		for idx := start; idx < end; idx++ {
			if err := ctx.Err(); err != nil {
				return err
			}
			record := records[idx]
			if record.Data == nil {
				continue
			}
			row := record.Data
			maps.Copy(row, record.OlakeColumns)
			if _, ok := row[constants.OlakeID].(string); !ok {
				return errors.New("Iceberg row is missing its string _olake_id")
			}
			op, ok := row[constants.OpType].(string)
			if !ok {
				return errors.New("Iceberg row is missing its string _op_type")
			}
			if op == "i" {
				row[constants.OpType] = "c"
			}
			key, partition, err := nativeRecordPartition(w.table.Schema(), w.table.Spec(), row)
			if err != nil {
				return err
			}
			group := groups[key]
			if group == nil {
				group = &nativeWriteGroup{partition: partition}
				groups[key] = group
			}
			group.rows = append(group.rows, row)
			group.indices = append(group.indices, idx-start)
		}
		locations := make([]types.RowLocation, end-start)
		for _, key := range slices.Sorted(maps.Keys(groups)) {
			group := groups[key]
			f, err := w.backfillFile(ctx, key, group.partition)
			if err != nil {
				return err
			}
			batch, err := nativeArrowRecord(f.schema, group.rows)
			if err != nil {
				return err
			}
			stop := context.AfterFunc(ctx, f.cancel)
			err = f.parquet.Write(batch)
			batch.Release()
			if err == nil {
				err = f.output.flush()
			}
			if err == nil {
				err = ctx.Err()
			}
			stop()
			if err != nil {
				return err
			}
			for position, idx := range group.indices {
				locations[idx] = types.RowLocation{FilePath: f.path, Position: f.rows + int64(position)}
			}
			f.rows += int64(len(group.rows))
			target := w.backfillTargetBytes
			if target <= 0 {
				target = nativeBackfillTargetBytes
			}
			if f.output.size.Load() >= target {
				if err := w.finishBackfillFile(ctx, key); err != nil {
					return err
				}
			}
		}
		if w.indexThread != nil {
			for idx := start; idx < end; idx++ {
				if records[idx].Data != nil {
					w.indexThread.Put(records[idx].Data[constants.OlakeID].(string), locations[idx-start])
				}
			}
		}
		start = end
	}
	return nil
}
