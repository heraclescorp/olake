package iceberg

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"sync"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	apacheiceberg "github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/types"
	"github.com/goccy/go-json"
)

const nativeCOWBatchProperty = "olake.cow.batch-id"

type nativeCOW struct {
	options        types.COWOptions
	keys           []string
	directory      string
	segments       int
	digest         hash.Hash
	baseCheckpoint string
}

func newNativeCOW(stream types.StreamInterface) (*nativeCOW, error) {
	options, err := stream.Self().EffectiveCOWOptions()
	if err != nil {
		return nil, err
	}
	keys := stream.GetStream().SourceDefinedPrimaryKey.Array()
	for index := range keys {
		keys[index] = stream.ResolveColumnName(keys[index])
	}
	slices.Sort(keys)
	return &nativeCOW{options: options, keys: keys, digest: sha256.New()}, nil
}

func (c *nativeCOW) cleanup(uncertain bool) {
	// Unknown publication retains the typed input as well as destination files.
	// It can contain unredacted financial/TOAST values: directory 0700, files 0600.
	if !uncertain && c.directory != "" {
		_ = os.RemoveAll(c.directory)
		c.directory = ""
	}
}

func nativeCOWCheckpoint(document string) (string, error) {
	if document == "" {
		return "", nil
	}
	var root map[string]json.RawMessage
	if err := json.Unmarshal([]byte(document), &root); err != nil {
		return "", err
	}
	return string(root["state"]), nil
}

func nativeCOWKey(schema *apacheiceberg.Schema, keys []string, row map[string]any) (string, error) {
	var encoded []byte
	for _, name := range keys {
		field, found := schema.FindFieldByName(name)
		if !found {
			return "", fmt.Errorf("copy-on-write primary key column %q is unavailable", name)
		}
		switch field.Type.(type) {
		case apacheiceberg.Int32Type, apacheiceberg.Int64Type:
			if row[name] != nil {
				// Never route key identity through float64 or narrowing casts.
				switch row[name].(type) {
				case float32, float64:
					return "", fmt.Errorf("copy-on-write integer key %q requires exact integer source encoding", name)
				}
				text := fmt.Sprint(row[name])
				if value, ok := row[name].([]byte); ok {
					text = string(value)
				}
				bits := 64
				if _, small := field.Type.(apacheiceberg.Int32Type); small {
					bits = 32
				}
				value, err := strconv.ParseInt(text, 10, bits)
				if err != nil {
					return "", fmt.Errorf("copy-on-write integer key %q is not exactly representable: %w", name, err)
				}
				if bits == 32 {
					row[name] = int32(value)
				} else {
					row[name] = value
				}
			}
		}
		literal, err := nativePartitionLiteral(field, row[name])
		if err != nil {
			return "", err
		}
		if !literal.Valid {
			return "", fmt.Errorf("copy-on-write primary key %q is null/unavailable; key changes require complete old and new key events", name)
		}
		value, err := literal.Val.MarshalBinary()
		if err != nil {
			return "", err
		}
		encoded = binary.AppendUvarint(encoded, uint64(len(value)))
		encoded = append(encoded, value...)
	}
	return string(encoded), nil
}

func (w *nativeWriter) writeCopyOnWrite(ctx context.Context, records []types.RawRecord) error {
	c := w.cow
	for first := 0; first < len(records); {
		rows := make([]map[string]any, 0, min(len(records)-first, c.options.MaxBatchRows))
		last := make(map[string]int)
		var payload int64
		end := first
		for end < len(records) && len(rows) < c.options.MaxBatchRows {
			record := records[end]
			if err := ctx.Err(); err != nil {
				return err
			}
			if record.Data == nil {
				end++
				continue
			}
			if len(record.UnavailableColumns) > 0 {
				return errors.New("copy-on-write requires source TOAST hydration; destination index hydration is unsupported")
			}
			size := record.PayloadSize()
			if end > first && payload+size > int64(c.options.MaxBatchBytes) {
				break
			}
			row := record.Data
			maps.Copy(row, record.OlakeColumns)
			op, ok := row[constants.OpType].(string)
			if !ok || (op != "r" && op != "c" && op != "i" && op != "u" && op != "d") {
				return fmt.Errorf("copy-on-write requires a supported source operation, got %q", op)
			}
			if op == "i" {
				row[constants.OpType] = "c"
			}
			// Deletes deliberately remain OLake tombstone after-images, just as MOR.
			key, err := nativeCOWKey(w.table.Schema(), c.keys, row)
			if err != nil {
				return err
			}
			if previous, found := last[key]; found {
				rows[previous] = nil
			}
			last[key] = len(rows)
			rows = append(rows, row)
			payload += size
			end++
			if payload >= int64(c.options.MaxBatchBytes) {
				break
			}
		}
		if len(rows) > 0 {
			latest := rows[:0]
			for _, row := range rows {
				if row != nil {
					latest = append(latest, row)
				}
			}
			batch, err := nativeArrowRecord(w.table.Schema(), latest)
			if err != nil {
				return err
			}
			err = c.spill(batch)
			batch.Release()
			if err != nil {
				return err
			}
		}
		first = end
	}
	return nil
}

func (c *nativeCOW) spill(batch arrow.RecordBatch) error {
	if c.directory == "" {
		directory, err := os.MkdirTemp("", "olake-cow-")
		if err != nil {
			return err
		}
		c.directory = directory
	}
	file, err := os.OpenFile(filepath.Join(c.directory, strconv.Itoa(c.segments)+".arrow"), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return err
	}
	writer := ipc.NewWriter(io.MultiWriter(file, c.digest), ipc.WithSchema(batch.Schema()))
	err = writer.Write(batch)
	err = errors.Join(err, writer.Close(), file.Close())
	if err != nil {
		return err
	}
	c.segments++
	return nil
}

func nativeCOWArrayLiteral(column arrow.Array, row int) (apacheiceberg.Literal, error) {
	if column.IsNull(row) {
		return nil, errors.New("copy-on-write primary key is null")
	}
	switch values := column.(type) {
	case *array.Boolean:
		return apacheiceberg.NewLiteral(values.Value(row)), nil
	case *array.Int32:
		return apacheiceberg.NewLiteral(values.Value(row)), nil
	case *array.Int64:
		return apacheiceberg.NewLiteral(values.Value(row)), nil
	case *array.String:
		return apacheiceberg.NewLiteral(values.Value(row)), nil
	case *array.Binary:
		return apacheiceberg.NewLiteral(values.Value(row)), nil
	case *array.FixedSizeBinary:
		return apacheiceberg.NewLiteral(values.Value(row)), nil
	case *array.Timestamp:
		unit := values.DataType().(*arrow.TimestampType).Unit
		value := values.Value(row).ToTime(unit)
		return apacheiceberg.NewLiteral(apacheiceberg.Timestamp(value.UnixMicro())), nil
	default:
		return nil, fmt.Errorf("unsupported copy-on-write key Arrow type %s", column.DataType())
	}
}

// Balanced tuple AND / tuple-set OR avoids linear expression recursion for large batches.
func nativeCOWCombine(expressions []apacheiceberg.BooleanExpression, and bool) apacheiceberg.BooleanExpression {
	if len(expressions) == 0 {
		if and {
			return apacheiceberg.AlwaysTrue{}
		}
		return apacheiceberg.AlwaysFalse{}
	}
	if len(expressions) == 1 {
		return expressions[0]
	}
	middle := len(expressions) / 2
	left, right := nativeCOWCombine(expressions[:middle], and), nativeCOWCombine(expressions[middle:], and)
	if and {
		return apacheiceberg.NewAnd(left, right)
	}
	return apacheiceberg.NewOr(left, right)
}

func (c *nativeCOW) filter(batch arrow.RecordBatch) (apacheiceberg.BooleanExpression, error) {
	indices := make([]int, len(c.keys))
	for index, name := range c.keys {
		found := batch.Schema().FieldIndices(name)
		if len(found) != 1 {
			return nil, fmt.Errorf("copy-on-write key %q absent or ambiguous", name)
		}
		indices[index] = found[0]
	}
	tuples := make([]apacheiceberg.BooleanExpression, batch.NumRows())
	for row := range int(batch.NumRows()) {
		fields := make([]apacheiceberg.BooleanExpression, len(indices))
		for index, column := range indices {
			literal, err := nativeCOWArrayLiteral(batch.Column(column), row)
			if err != nil {
				return nil, err
			}
			term := apacheiceberg.Reference(c.keys[index])
			// Explicit null exclusion makes the survivor predicate include NULL
			// bystanders even under SQL/Kleene Arrow comparison semantics.
			fields[index] = apacheiceberg.NewAnd(apacheiceberg.NotNull(term), apacheiceberg.LiteralPredicate(apacheiceberg.OpEQ, term, literal))
		}
		tuples[row] = nativeCOWCombine(fields, true)
	}
	return nativeCOWCombine(tuples, false), nil
}

func (c *nativeCOW) overwriteOptions(filter apacheiceberg.BooleanExpression, track func(string)) []table.OverwriteOption {
	return []table.OverwriteOption{
		table.WithOverwriteFilter(filter),
		table.WithOverwriteConcurrency(c.options.RewriteConcurrency),
		table.WithOverwriteReadBatchSize(1),
		table.WithOverwriteWriteLimits(c.options.RewriteConcurrency, 1, min(c.options.MaxBatchRows, 1024), int64(c.options.MaxBatchBytes)),
		// Every incoming reader contains exactly one bounded typed record.
		table.WithOverwriteClusteredInput(),
		table.WithOverwriteRewriteWindow(c.options.MaxBatchRows, int64(c.options.MaxBatchBytes)),
		table.WithOverwriteFileTracking(track),
	}
}

func (w *nativeWriter) replayCopyOnWrite(ctx context.Context, tx *table.Transaction, track func(string)) error {
	for segment := range w.cow.segments {
		file, err := os.Open(filepath.Join(w.cow.directory, strconv.Itoa(segment)+".arrow"))
		if err != nil {
			return err
		}
		reader, err := ipc.NewReader(file)
		if err != nil {
			_ = file.Close()
			return err
		}
		if !reader.Next() {
			err = reader.Err()
			if err == nil {
				err = errors.New("empty COW replay segment")
			}
			reader.Release()
			_ = file.Close()
			return err
		}
		batch := reader.RecordBatch()
		filter, filterErr := w.cow.filter(batch)
		if filterErr == nil {
			var records array.RecordReader
			records, filterErr = array.NewRecordReader(batch.Schema(), []arrow.RecordBatch{batch})
			if filterErr == nil {
				filterErr = tx.Overwrite(ctx, records, apacheiceberg.Properties{"olake.thread-id": w.options.ThreadID}, w.cow.overwriteOptions(filter, track)...)
				records.Release()
			}
		}
		reader.Release()
		err = errors.Join(filterErr, file.Close())
		if err != nil {
			return err
		}
	}
	return nil
}

func (w *nativeWriter) closeCopyOnWrite(ctx context.Context, finalMetadataState any) error {
	c := w.cow
	encoded, err := json.Marshal(finalMetadataState)
	if err != nil {
		return err
	}
	finalCheckpoint, err := nativeCOWCheckpoint(string(encoded))
	if err != nil {
		return err
	}
	identity := sha256.New()
	_, _ = identity.Write([]byte(w.table.Metadata().TableUUID().String()))
	if finalCheckpoint != "" {
		// Checkpoint identity is stable across restart, source receive timestamps,
		// and writer-thread IDs. Recovery skips landed-but-unacknowledged batches.
		_, _ = identity.Write([]byte(finalCheckpoint))
	} else {
		_, _ = identity.Write(c.digest.Sum(nil))
		_, _ = identity.Write(encoded)
	}
	batchID := hex.EncodeToString(identity.Sum(nil))
	baseShape, err := ordinaryRecoveryShape(w.table.Metadata())
	if err != nil {
		return err
	}
	ctx, coordinated, err := w.lockTable(ctx)
	if err != nil {
		return err
	}
	defer coordinated.mu.Unlock()
	for attempt := range nativeCommitAttempts {
		// Always refresh at the publication boundary, including external compaction.
		fresh, err := w.cat.LoadTable(ctx, w.table.Identifier())
		if err != nil {
			return err
		}
		coordinated.latest = fresh
		shape, err := ordinaryRecoveryShape(fresh.Metadata())
		if err != nil {
			return err
		}
		if shape != baseShape || fresh.Metadata().TableUUID() != w.table.Metadata().TableUUID() {
			return errors.New("copy-on-write table shape/UUID changed; refresh source schema before retry")
		}
		if c.segments > 0 && fresh.Properties()[nativeCOWBatchProperty] == batchID {
			w.table, w.closed = fresh, true
			c.cleanup(false)
			return nil
		}
		checkpoint, err := nativeCOWCheckpoint(fresh.Properties()[nativeRecoveryProperty])
		if err != nil {
			return err
		}
		if c.segments > 0 && finalCheckpoint != "" && checkpoint == finalCheckpoint {
			w.table, w.closed = fresh, true
			c.cleanup(false)
			return nil
		}
		if checkpoint != c.baseCheckpoint {
			return errors.New("copy-on-write source checkpoint changed concurrently; reload authoritative recovery state before replay")
		}
		deletes, _, err := nativeCOWLiveFiles(ctx, fresh)
		if err != nil {
			return err
		}
		if len(deletes) != 0 {
			return errors.New("copy-on-write encountered concurrent live delete files; reopen with explicit materialize_existing_deletes permission")
		}
		state, changed, err := mergeNativeRecovery(fresh.Properties()[nativeRecoveryProperty], w.options.ThreadID, finalMetadataState, c.segments > 0)
		if err != nil {
			return err
		}
		if c.segments == 0 && !changed {
			w.table, w.closed = fresh, true
			c.cleanup(false)
			return nil
		}
		tx := fresh.NewTransaction()
		if err := tx.AssertRefSnapshotID(table.MainBranch); err != nil {
			return err
		}
		if err := tx.AssertDefaultShape(); err != nil {
			return err
		}
		if err := tx.SetProperties(apacheiceberg.Properties{nativeRecoveryProperty: state, nativeCOWBatchProperty: batchID}); err != nil {
			return err
		}
		var mu sync.Mutex
		paths := make(map[string]struct{})
		track := func(path string) { mu.Lock(); paths[path] = struct{}{}; mu.Unlock() }
		if err := w.replayCopyOnWrite(ctx, tx, track); err != nil {
			w.removeCOWPaths(ctx, fresh, paths)
			return err
		}
		if c.segments == 0 {
			// Quiet tables must advance the snapshot too: an unfenced property-only
			// checkpoint update cannot participate in source checkpoint CAS.
			arrowSchema, err := table.SchemaToArrowSchema(fresh.Schema(), nil, true, false)
			if err != nil {
				return err
			}
			reader, err := array.NewRecordReader(arrowSchema, nil)
			if err != nil {
				return err
			}
			err = tx.Overwrite(ctx, reader, nil, c.overwriteOptions(apacheiceberg.AlwaysFalse{}, track)...)
			reader.Release()
			if err != nil {
				w.removeCOWPaths(ctx, fresh, paths)
				return err
			}
		}
		committed, commitErr := tx.Commit(ctx)
		if commitErr == nil {
			w.table, w.closed, coordinated.latest = committed, true, committed
			c.cleanup(false)
			return nil
		}
		w.mutationError(commitErr)
		commitErr = nativeCatalogError("commit copy-on-write", commitErr)
		unknown := !nativeMutationOutcomeKnown(commitErr)
		coordinated.invalidate(unknown)
		if unknown {
			w.uncertain = true
			return fmt.Errorf("copy-on-write publication outcome unknown; staged destination files and private replay directory %q retained: %w", c.directory, commitErr)
		}
		w.removeCOWPaths(ctx, fresh, paths)
		w.logConflict("copy-on-write", attempt+1, fresh, commitErr)
		if !errors.Is(commitErr, table.ErrCommitFailed) && !errors.Is(commitErr, table.ErrConflictingDataFiles) && !errors.Is(commitErr, table.ErrConflictingDeleteFiles) {
			return commitErr
		}
	}
	return fmt.Errorf("copy-on-write refresh/rewrite retries exhausted: %w", table.ErrCommitFailed)
}

func (w *nativeWriter) removeCOWPaths(ctx context.Context, tbl *table.Table, paths map[string]struct{}) {
	fs, err := tbl.FS(ctx)
	if err != nil {
		return
	}
	for path := range paths {
		_ = fs.Remove(path)
	}
}

func nativeCOWLiveFiles(ctx context.Context, tbl *table.Table) (deletes, data []apacheiceberg.DataFile, err error) {
	entries, err := nativeLiveEntries(ctx, tbl)
	if err != nil {
		return nil, nil, err
	}
	for _, entry := range entries {
		file := entry.DataFile()
		if file.ContentType() == apacheiceberg.EntryContentData {
			data = append(data, file)
		} else {
			deletes = append(deletes, file)
		}
	}
	return deletes, data, nil
}

func (w *nativeWriter) prepareCopyOnWrite(ctx context.Context) error {
	if w.table.Metadata().Version() < 2 || w.table.Metadata().Version() > 3 {
		return errors.New("copy-on-write requires Iceberg format v2 or v3")
	}
	for _, name := range w.cow.keys {
		field, found := w.table.Schema().FindFieldByName(name)
		if !found {
			return fmt.Errorf("copy-on-write destination primary key %q unavailable", name)
		}
		switch field.Type.(type) {
		case apacheiceberg.BooleanType, apacheiceberg.Int32Type, apacheiceberg.Int64Type, apacheiceberg.StringType, apacheiceberg.BinaryType, apacheiceberg.FixedType, apacheiceberg.TimestampType, apacheiceberg.TimestampTzType:
		default:
			return fmt.Errorf("unsupported copy-on-write exact primary key type %s", field.Type)
		}
	}
	baseShape, err := ordinaryRecoveryShape(w.table.Metadata())
	if err != nil {
		return err
	}
	for attempt := range nativeCommitAttempts {
		fresh := w.table
		deletes, data, err := nativeCOWLiveFiles(ctx, fresh)
		if err != nil {
			return err
		}
		if len(deletes) == 0 {
			w.cow.baseCheckpoint, err = nativeCOWCheckpoint(fresh.Properties()[nativeRecoveryProperty])
			return err
		}
		if !w.cow.options.MaterializeExistingDeletes {
			return errors.New("copy-on-write requires a data-only current snapshot; set cow_options.materialize_existing_deletes=true to authorize a complete current-view rewrite preserving the source checkpoint")
		}
		if _, recovery := types.OrdinaryRecoveryFromContext(ctx); recovery || w.config.ordinaryRecovery {
			return errors.New("ordinary recovery cannot materialize existing deletes; complete an explicitly authorized normal sync first")
		}
		rewritten, err := w.materializeCOWView(ctx, fresh)
		if err != nil {
			return err
		}
		tx := fresh.NewTransaction()
		if err := tx.AssertRefSnapshotID(table.MainBranch); err != nil {
			return err
		}
		if err := tx.AssertDefaultShape(); err != nil {
			return err
		}
		rewrite := tx.NewRewrite(apacheiceberg.Properties{"olake.cow.materialize-deletes": "true"}).Apply(data, rewritten, deletes)
		if err := rewrite.Commit(ctx); err != nil {
			return err
		}
		committed, commitErr := tx.Commit(ctx)
		if commitErr == nil {
			w.table, w.coordinator.latest = committed, committed
			clear(w.paths)
			remaining, _, err := nativeCOWLiveFiles(ctx, committed)
			if err != nil {
				return err
			}
			if len(remaining) > 0 {
				return errors.New("copy-on-write materialization did not retire every live delete entry")
			}
			w.cow.baseCheckpoint, err = nativeCOWCheckpoint(committed.Properties()[nativeRecoveryProperty])
			return err
		}
		w.mutationError(commitErr)
		commitErr = nativeCatalogError("materialize existing deletes", commitErr)
		w.logConflict("copy-on-write-materialization", attempt+1, fresh, commitErr)
		unknown := !nativeMutationOutcomeKnown(commitErr)
		w.coordinator.invalidate(unknown)
		if unknown {
			w.uncertain = true
			return fmt.Errorf("existing-delete materialization outcome unknown; retaining all staged files: %w", commitErr)
		}
		w.removeFiles(ctx, rewritten)
		if !errors.Is(commitErr, table.ErrCommitFailed) {
			return commitErr
		}
		fresh, err = w.cat.LoadTable(ctx, fresh.Identifier())
		if err != nil {
			return err
		}
		shape, err := ordinaryRecoveryShape(fresh.Metadata())
		if err != nil {
			return err
		}
		if shape != baseShape || fresh.Metadata().TableUUID() != w.table.Metadata().TableUUID() {
			return errors.New("table shape/UUID changed during existing-delete materialization")
		}
		w.table, w.coordinator.latest = fresh, fresh
	}
	return fmt.Errorf("existing-delete materialization retries exhausted: %w", table.ErrCommitFailed)
}
