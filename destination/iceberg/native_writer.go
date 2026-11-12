package iceberg

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"slices"
	"sync"
	"time"

	apacheiceberg "github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/table"
	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/destination"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils/logger"
)

const nativeCommitAttempts = 4
const nativeRecoveryProperty = "olake_2pc"

// A table's chunks share a recovery document, even when their files were
// produced concurrently. Serialize the final refresh/merge/commit in-process;
// catalog branch assertions remain the cross-process correctness boundary.
var nativeCommitLocks sync.Map

func nativeCommitLock(tbl *table.Table) *sync.Mutex {
	lock, _ := nativeCommitLocks.LoadOrStore(tbl.Location(), &sync.Mutex{})
	return lock.(*sync.Mutex)
}

type nativeIndexRead struct {
	location types.RowLocation
	found    bool
}

type nativeWriter struct {
	table        *table.Table
	cat          catalog.Catalog
	config       *Config
	stream       types.StreamInterface
	options      *destination.Options
	indexThread  *types.StreamIndexThread
	indexReads   map[string]nativeIndexRead
	indexTouched map[string]struct{}
	upsert       bool
	deleteMode   types.UpdateType
	dataFiles    []apacheiceberg.DataFile
	dataSchemas  map[string]*apacheiceberg.Schema
	positions    map[string][]int64
	latest       map[string]types.RowLocation
	paths        map[string]struct{}
	closed       bool
	uncertain    bool
	failed       error
}

func newNativeWriter(ctx context.Context, config *Config, stream types.StreamInterface, options *destination.Options, cat catalog.Catalog, indexThread *types.StreamIndexThread) (_ *nativeWriter, _ *types.MetadataState, err error) {
	ctx = nativeCatalogContext(ctx, cat)
	tbl, err := loadOrCreateNativeTable(ctx, config, stream, cat)
	if err != nil {
		return nil, nil, fmt.Errorf("load or create Iceberg table: %w", err)
	}
	w := &nativeWriter{table: tbl, cat: cat, config: config, stream: stream, options: options, indexThread: indexThread, upsert: isUpsertMode(stream, options.Backfill), deleteMode: stream.GetUpdateType(), positions: make(map[string][]int64), latest: make(map[string]types.RowLocation), paths: make(map[string]struct{})}
	defer func() {
		if err != nil {
			w.Abort()
		}
	}()
	if err := w.validateFormat(ctx); err != nil {
		return nil, nil, err
	}
	columns, err := nativeStreamColumns(stream)
	if err != nil {
		return nil, nil, err
	}
	if err := w.evolveNativeSchema(ctx, columns, false); err != nil {
		return nil, nil, err
	}
	if options.TableIndex != nil {
		if err := w.migrateEqualityDeletes(ctx); err != nil {
			return nil, nil, err
		}
	}
	// Reconcile and return recovery from the loaded metadata, or from the
	// successful format/schema/migration commit that replaced it. A no-op
	// initialization must not issue another catalog GET.
	if err := reconcileNativeTableIndex(ctx, options.TableIndex, w.table, constants.OlakeID); err != nil {
		return nil, nil, err
	}
	state := &types.MetadataState{}
	if value := w.table.Properties()[nativeRecoveryProperty]; value != "" {
		if err := json.Unmarshal([]byte(value), state); err != nil {
			return nil, nil, fmt.Errorf("decode Iceberg recovery state: %w", err)
		}
	}
	indexThread.SetObservabilityContext(ctx)
	return w, state, nil
}

func (w *nativeWriter) writable() error {
	if w.uncertain {
		return errors.New("Iceberg commit outcome is unknown; writer cannot be reused or cleaned up")
	}
	if w.closed {
		return errors.New("Iceberg writer is closed")
	}
	if w.failed != nil {
		return fmt.Errorf("Iceberg writer has failed: %w", w.failed)
	}
	return nil
}

func (w *nativeWriter) validateFormat(ctx context.Context) error {
	version := w.table.Metadata().Version()
	if version > 3 {
		return fmt.Errorf("unsupported Iceberg format version %d", version)
	}
	if w.deleteMode != types.UpdateTypeDeletionVector {
		if version >= 3 {
			return fmt.Errorf("Iceberg format v%d requires deletion vector update mode; reset the table before changing update mode", version)
		}
		if w.upsert && version < 2 {
			return errors.New("Iceberg upserts require table format version 2 or later")
		}
		return nil
	}
	if version >= 3 {
		return nil
	}
	for attempt := range nativeCommitAttempts {
		fresh := w.table
		if attempt > 0 {
			var err error
			fresh, err = w.cat.LoadTable(ctx, w.table.Identifier())
			if err != nil {
				return err
			}
		}
		tx := fresh.NewTransaction()
		if err := tx.AssertRefSnapshotID(table.MainBranch); err != nil {
			return err
		}
		if err := tx.UpgradeFormatVersion(3); err != nil {
			return err
		}
		updated, err := tx.Commit(ctx)
		if err == nil {
			w.table = updated
			return nil
		}
		err = nativeCatalogError("upgrade Iceberg format", err)
		w.logConflict("format-upgrade", attempt+1, fresh, err)
		if !errors.Is(err, table.ErrCommitFailed) {
			w.uncertain = true
			return fmt.Errorf("Iceberg format upgrade outcome is unknown: %w", err)
		}
	}
	return fmt.Errorf("upgrade Iceberg format: %w", table.ErrCommitFailed)
}

type nativeWriteGroup struct {
	rows      []map[string]any
	indices   []int
	partition map[int]any
}

func (w *nativeWriter) Write(ctx context.Context, records []types.RawRecord) (err error) {
	if err := w.writable(); err != nil {
		return err
	}
	defer func() {
		if err != nil {
			w.failed = err
			w.Abort()
		}
	}()
	ctx = nativeCatalogContext(ctx, w.cat)
	w.indexThread.SetObservabilityContext(ctx)
	groups := make(map[string]*nativeWriteGroup)
	rows := make([]map[string]any, len(records))
	operations := make([]string, len(records))
	keys := make([]string, len(records))
	var batchSeen map[string]struct{}
	if w.indexThread != nil && w.upsert {
		batchSeen = make(map[string]struct{})
	}
	for idx, record := range records {
		if err := ctx.Err(); err != nil {
			return err
		}
		if record.Data == nil {
			continue
		}
		row := record.Data
		maps.Copy(row, record.OlakeColumns)
		id, ok := row[constants.OlakeID].(string)
		if !ok {
			return errors.New("Iceberg row is missing its string _olake_id")
		}
		op, ok := row[constants.OpType].(string)
		if !ok {
			return errors.New("Iceberg row is missing its string _op_type")
		}
		operations[idx], keys[idx] = op, id
		if w.indexThread != nil && w.upsert && op != "r" && op != "c" {
			_, touched := w.indexTouched[id]
			_, seen := batchSeen[id]
			if !touched && !seen {
				previous, found, err := w.options.TableIndex.Lookup(id)
				if err != nil {
					return fmt.Errorf("look up Iceberg row identity: %w", err)
				}
				if w.indexReads == nil {
					w.indexReads = make(map[string]nativeIndexRead)
				}
				w.indexReads[id] = nativeIndexRead{location: previous, found: found}
				if op == "u" && w.stream.ResolveUpsertOp() && !found {
					row[constants.OpType] = "c"
				}
			}
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
		group.indices = append(group.indices, idx)
		rows[idx] = row
		if batchSeen != nil {
			batchSeen[id] = struct{}{}
		}
	}
	locations := make([]types.RowLocation, len(records))
	for _, key := range slices.Sorted(maps.Keys(groups)) {
		group := groups[key]
		batch, err := nativeArrowRecord(w.table.Schema(), group.rows)
		if err != nil {
			return err
		}
		file, err := w.writeNativeParquet(ctx, w.table.Schema(), w.table.Spec(), group.partition, batch, apacheiceberg.EntryContentData, nil, nil)
		batch.Release()
		if err != nil {
			return err
		}
		w.dataFiles = append(w.dataFiles, file)
		if w.dataSchemas == nil {
			w.dataSchemas = make(map[string]*apacheiceberg.Schema)
		}
		w.dataSchemas[file.FilePath()] = w.table.Schema()
		for position, idx := range group.indices {
			locations[idx] = types.RowLocation{FilePath: file.FilePath(), Position: int64(position)}
		}
	}
	// Resolve superseded locations in SOURCE order, not partition iteration
	// order. The pending index and latest maps therefore see earlier records in
	// this same batch, even when a key changes partition.
	for idx, row := range rows {
		if row == nil {
			continue
		}
		id, op := keys[idx], operations[idx]
		if w.upsert {
			if w.indexThread != nil {
				_, touched := w.indexTouched[id]
				if touched {
					// A touched key is guaranteed to be pending: this lookup never
					// reaches the committed store, including for later c/r events.
					previous, found, err := w.indexThread.Lookup(id)
					if err != nil {
						return err
					}
					if !found {
						return errors.New("touched Iceberg row is missing from the pending index")
					}
					w.positions[previous.FilePath] = append(w.positions[previous.FilePath], previous.Position)
				} else if op != "r" && op != "c" {
					previous := w.indexReads[id]
					if previous.found {
						w.positions[previous.location.FilePath] = append(w.positions[previous.location.FilePath], previous.location.Position)
					}
				}
				if w.indexTouched == nil {
					w.indexTouched = make(map[string]struct{})
				}
				w.indexTouched[id] = struct{}{}
			} else if op != "r" {
				if previous, found := w.latest[id]; found {
					w.positions[previous.FilePath] = append(w.positions[previous.FilePath], previous.Position)
				}
				w.latest[id] = locations[idx]
			}
		}
		if w.indexThread != nil {
			w.indexThread.Put(id, locations[idx])
		}
	}
	return nil
}

func (w *nativeWriter) EnsureReadable(ctx context.Context, paths []string) error {
	if err := w.writable(); err != nil {
		return err
	}
	fs, err := w.table.FS(nativeCatalogContext(ctx, w.cat))
	if err != nil {
		return err
	}
	for _, path := range paths {
		if _, ours := w.paths[path]; !ours {
			continue
		}
		// Write has already closed every footer and cloud upload. Opening it here
		// verifies that a pending indexed location is actually recoverable.
		file, err := fs.Open(path)
		if err != nil {
			return fmt.Errorf("open staged Iceberg recovery file: %w", err)
		}
		if err := file.Close(); err != nil {
			return err
		}
	}
	return ctx.Err()
}

func mergeNativeRecovery(current, threadID string, payload any, hasFiles bool) (string, bool, error) {
	root := make(map[string]json.RawMessage)
	if current != "" {
		if err := json.Unmarshal([]byte(current), &root); err != nil {
			return "", false, fmt.Errorf("decode stored Iceberg recovery document: %w", err)
		}
	}
	if root == nil {
		return "", false, errors.New("stored Iceberg recovery document is not an object")
	}
	if payload == nil {
		if !hasFiles {
			return current, false, nil
		}
		var ids []string
		if raw := root["full_refresh_committed_ids"]; len(raw) > 0 {
			if err := json.Unmarshal(raw, &ids); err != nil {
				return "", false, err
			}
		}
		if !slices.Contains(ids, threadID) {
			ids = append(ids, threadID)
		}
		root["full_refresh_committed_ids"], _ = json.Marshal(ids)
		root["dedup_inserts"] = json.RawMessage("true")
	} else {
		encoded, err := json.Marshal(payload)
		if err != nil {
			return "", false, fmt.Errorf("encode Iceberg recovery payload: %w", err)
		}
		incoming := make(map[string]json.RawMessage)
		if err := json.Unmarshal(encoded, &incoming); err != nil {
			return "", false, fmt.Errorf("Iceberg recovery payload must be an object: %w", err)
		}
		root["id"], _ = json.Marshal(threadID)
		for key, value := range incoming {
			oldObject, oldOK := nativeJSONStringObject(root[key])
			newObject, newOK := nativeJSONStringObject(value)
			if oldOK && newOK {
				maps.Copy(oldObject, newObject)
				merged, err := json.Marshal(oldObject)
				if err != nil {
					return "", false, err
				}
				value, _ = json.Marshal(string(merged))
			}
			root[key] = value
		}
	}
	encoded, err := json.Marshal(root)
	return string(encoded), string(encoded) != current, err
}

func nativeJSONStringObject(raw json.RawMessage) (map[string]json.RawMessage, bool) {
	var value string
	if json.Unmarshal(raw, &value) != nil {
		return nil, false
	}
	var object map[string]json.RawMessage
	if json.Unmarshal([]byte(value), &object) != nil || object == nil {
		return nil, false
	}
	return object, true
}

func nativeSnapshotID(tbl *table.Table) int64 {
	if snapshot := tbl.CurrentSnapshot(); snapshot != nil {
		return snapshot.SnapshotID
	}
	return 0
}

func (w *nativeWriter) Close(ctx context.Context, finalMetadataState any) (err error) {
	if err := w.writable(); err != nil {
		return err
	}
	defer func() {
		w.indexThread.ReleaseObservability()
		if err != nil {
			w.Abort()
		}
	}()
	ctx = nativeCatalogContext(ctx, w.cat)
	lock := nativeCommitLock(w.table)
	lock.Lock()
	defer lock.Unlock()
	if err := w.compactStagedDataFiles(ctx); err != nil {
		return fmt.Errorf("compact superseded staged Iceberg rows: %w", err)
	}
	hasFiles := len(w.dataFiles) > 0 || len(w.positions) > 0
	var lastConflict error
	for attempt := range nativeCommitAttempts {
		if err := ctx.Err(); err != nil {
			return err
		}
		fresh, err := w.cat.LoadTable(ctx, w.table.Identifier())
		if err != nil {
			return err
		}
		var indexBase int64
		if hasFiles && w.indexThread != nil {
			indexBase, err = w.options.TableIndex.LastCommittedSnapshot()
			if err != nil {
				return err
			}
			if nativeSnapshotID(fresh) != indexBase {
				w.logConflict("commit-index-base", attempt+1, fresh, table.ErrConflictingDeleteFiles)
				return fmt.Errorf("Iceberg index checkpoint %d differs from current snapshot %d; refusing stale positional deletes: %w", indexBase, nativeSnapshotID(fresh), table.ErrCommitFailed)
			}
			for id, original := range w.indexReads {
				location, found, err := w.options.TableIndex.Lookup(id)
				if err != nil {
					return err
				}
				if found != original.found || found && location != original.location {
					w.logConflict("commit-index-key", attempt+1, fresh, table.ErrConflictingDataFiles)
					return fmt.Errorf("indexed row changed while this thread was writing; refusing stale positional deletes: %w", table.ErrConflictingDataFiles)
				}
			}
		}
		state, changed, err := mergeNativeRecovery(fresh.Properties()[nativeRecoveryProperty], w.options.ThreadID, finalMetadataState, hasFiles)
		if err != nil {
			return err
		}
		if !hasFiles && !changed {
			w.closed = true
			return nil
		}
		tx := fresh.NewTransaction()
		if err := tx.AssertRefSnapshotID(table.MainBranch); err != nil {
			return err
		}
		if err := tx.AssertDefaultShape(); err != nil {
			return err
		}
		if changed {
			if err := tx.SetProperties(apacheiceberg.Properties{nativeRecoveryProperty: state}); err != nil {
				return err
			}
		}
		// Rebuild position deletes against the fresh table on every retry. A
		// replacement DV must retain every prior vector bit and retire exactly
		// the vector currently live on this base snapshot.
		positionFiles, replaced, err := w.writePositionDeletes(ctx, w.positions, fresh)
		if err != nil {
			return err
		}
		equalityFiles, err := w.writeEqualityDeletes(ctx, fresh)
		if err != nil {
			return err
		}
		if hasFiles {
			delta := tx.NewRowDelta(apacheiceberg.Properties{"olake.thread-id": w.options.ThreadID})
			delta.AddRows(w.dataFiles...).AddDeletes(equalityFiles...).AddDeletes(positionFiles...).RemoveDeletes(replaced...)
			if err := delta.Commit(ctx); err != nil {
				return err
			}
		}
		committed, commitErr := tx.Commit(ctx)
		if commitErr != nil {
			commitErr = nativeCatalogError("commit Iceberg table", commitErr)
			lastConflict = commitErr
			w.logConflict("commit", attempt+1, fresh, commitErr)
			if !errors.Is(commitErr, table.ErrCommitFailed) {
				w.uncertain = true
				return fmt.Errorf("Iceberg commit outcome is unknown; staged files retained for recovery: %w", commitErr)
			}
			// The attempt was definitively rejected: only its newly written
			// delete files can be discarded; data is reused by the next attempt.
			w.removeFiles(ctx, positionFiles)
			w.removeFiles(ctx, equalityFiles)
			continue
		}
		w.table, w.closed = committed, true
		// A successful metadata commit owns all staged files from now on.
		// Never delete them, including when the local index checkpoint fails.
		clear(w.paths)
		if hasFiles && w.indexThread != nil {
			snapshot := committed.CurrentSnapshot()
			parent := int64(0)
			if snapshot.ParentSnapshotID != nil {
				parent = *snapshot.ParentSnapshotID
			}
			if parent == indexBase {
				if err := w.options.TableIndex.Commit(w.indexThread, &snapshot.SnapshotID); err != nil {
					return fmt.Errorf("Iceberg committed but local index checkpoint failed: %w", err)
				}
			}
		}
		w.indexThread.ReleaseObservability()
		w.indexThread = nil
		return nil
	}
	return fmt.Errorf("Iceberg commit retries exhausted: %w", lastConflict)
}

// Summary counters are authoritative when present; absent/unknown counters
// require manifest inspection rather than guessing that the table is clean.
func nativeMayHaveEqualityDeletes(tbl *table.Table) bool {
	snapshot := tbl.CurrentSnapshot()
	if snapshot == nil {
		return false
	}
	if snapshot.Summary != nil {
		properties := snapshot.Summary.Properties
		if properties["total-equality-deletes"] == "0" || properties["total-delete-files"] == "0" {
			return false
		}
	}
	return true
}

func (w *nativeWriter) migrateEqualityDeletes(ctx context.Context) error {
	lock := nativeCommitLock(w.table)
	lock.Lock()
	defer lock.Unlock()
	for attempt := range nativeCommitAttempts {
		fresh := w.table
		if attempt > 0 {
			var err error
			fresh, err = w.cat.LoadTable(ctx, w.table.Identifier())
			if err != nil {
				return err
			}
			w.table = fresh
		}
		if !nativeMayHaveEqualityDeletes(fresh) {
			return nil
		}
		positions, equalityFiles, err := collectNativeEqualityDeletePositions(ctx, fresh)
		if err != nil {
			return err
		}
		if len(equalityFiles) == 0 {
			return nil
		}
		files, replaced, err := w.writePositionDeletes(ctx, positions, fresh)
		if err != nil {
			return err
		}
		tx := fresh.NewTransaction()
		if err := tx.AssertRefSnapshotID(table.MainBranch); err != nil {
			return err
		}
		// iceberg-go's delete-file rewrite API does not allow changing content
		// kinds. Two staged snapshots in one transaction provide the same atomic
		// conversion without publishing an equality-free intermediate table.
		if err := tx.ReplaceFiles(ctx, nil, nil, equalityFiles, nil); err != nil {
			return err
		}
		if len(files) > 0 {
			if err := tx.NewRowDelta(nil).AddDeletes(files...).RemoveDeletes(replaced...).Commit(ctx); err != nil {
				return err
			}
		}
		updated, commitErr := tx.Commit(ctx)
		if commitErr == nil {
			w.table = updated
			clear(w.paths)
			return nil
		}
		commitErr = nativeCatalogError("migrate Iceberg equality deletes", commitErr)
		w.logConflict("equality-migration", attempt+1, fresh, commitErr)
		if !errors.Is(commitErr, table.ErrCommitFailed) {
			w.uncertain = true
			return fmt.Errorf("Iceberg equality migration outcome is unknown: %w", commitErr)
		}
		w.removeFiles(ctx, files)
	}
	return fmt.Errorf("Iceberg equality migration retries exhausted: %w", table.ErrCommitFailed)
}

func nativeConflictClass(err error) string {
	switch {
	case errors.Is(err, table.ErrConflictingDataFiles):
		return "concurrent_data"
	case errors.Is(err, table.ErrConflictingDeleteFiles):
		return "concurrent_deletes"
	case errors.Is(err, table.ErrDataFilesMissing):
		return "referenced_data_missing"
	case errors.Is(err, table.ErrCommitFailed):
		return "catalog_compare_and_swap"
	default:
		return "commit_state_unknown"
	}
}

func (w *nativeWriter) logConflict(operation string, attempt int, tbl *table.Table, err error) {
	// Never log the raw catalog error: REST errors may embed response bodies,
	// signed URLs or credentials. The classification and snapshot IDs suffice.
	logger.Warnf("Thread[%s]: native Iceberg %s attempt=%d base_snapshot=%d classification=%s", w.options.ThreadID, operation, attempt, nativeSnapshotID(tbl), nativeConflictClass(err))
}

func (w *nativeWriter) removeFiles(ctx context.Context, files []apacheiceberg.DataFile) {
	fs, err := w.table.FS(nativeCatalogContext(ctx, w.cat))
	if err != nil {
		return
	}
	for _, file := range files {
		path := file.FilePath()
		if _, ours := w.paths[path]; ours {
			if fs.Remove(path) == nil {
				delete(w.paths, path)
			}
		}
	}
}

func (w *nativeWriter) Abort() {
	w.indexThread.ReleaseObservability()
	w.indexThread = nil
	w.closed = true
	if w.uncertain || len(w.paths) == 0 {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	fs, err := w.table.FS(nativeCatalogContext(ctx, w.cat))
	if err != nil {
		return
	}
	for path := range w.paths {
		if err := fs.Remove(path); err == nil {
			delete(w.paths, path)
		}
	}
}
