package indexdb

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"sync"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/bloom"
	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils/logger"
)

var unsafeDirChars = regexp.MustCompile(`[^a-zA-Z0-9_.-]+`)

type StoreOptions struct {
	Dir          string
	CacheSize    int64
	MemTableSize uint64
	MaxOpenFiles int
}

func DefaultOptions() StoreOptions {
	dir := os.Getenv(constants.IndexDBDir)
	if dir == "" {
		wd, _ := os.Getwd()
		dir = filepath.Join(wd, constants.DefaultDirName)
	}

	cacheSize, err := strconv.Atoi(os.Getenv(constants.IndexDBCacheSizePerStream))
	if err != nil {
		logger.Warnf("failed to parse index db cache size (using default %d MB): %s", constants.DefaultCacheSize, err)
		cacheSize = constants.DefaultCacheSize
	}

	maxOpenFiles, err := strconv.Atoi(os.Getenv(constants.MaxOpenFilesPerStream))
	if err != nil {
		logger.Warnf("failed to parse index db max open files (using default %d): %s", constants.DefaultMaxOpenFiles, err)
		maxOpenFiles = constants.DefaultMaxOpenFiles
	}

	return StoreOptions{
		Dir:          dir,
		CacheSize:    int64(cacheSize),
		MemTableSize: constants.DefaultMemTableSize,
		MaxOpenFiles: maxOpenFiles,
	}
}

// Open opens or creates a TableIndex for the given streamID.
func Open(stream types.StreamInterface) (types.StreamIndex, error) {
	opts := DefaultOptions()
	dir := indexDir(opts.Dir, stream)
	lock, err := lockCanonical(dir)
	if err != nil {
		return nil, err
	}
	if err := recoverPromotion(dir, opts); err != nil {
		return nil, errors.Join(err, lock.Close())
	}
	index, err := openIndex(dir, opts)
	if err != nil {
		return nil, errors.Join(err, lock.Close())
	}
	if err := syncDirectory(filepath.Dir(dir)); err != nil {
		return nil, errors.Join(err, index.Close(), lock.Close())
	}
	index.lock = lock
	index.table = fmt.Sprintf("%s.%s", stream.GetDestinationDatabase(nil), stream.GetDestinationTable())
	return index, nil
}

// Drop removes the on-disk index directory for streamID.
func Drop(stream types.StreamInterface) error {
	opts := DefaultOptions()
	dir := indexDir(opts.Dir, stream)
	lock, err := lockCanonical(dir)
	if err != nil {
		return err
	}
	defer lock.Close()
	if err := recoverPromotion(dir, opts); err != nil {
		return err
	}
	indexLock, err := lockIndexDirectory(dir)
	if err != nil {
		return err
	}
	if indexLock != nil {
		defer indexLock.Close()
	}
	if err := os.RemoveAll(dir); err != nil {
		return fmt.Errorf("failed to remove stream index for stream[%s]: %w", stream.ID(), err)
	}
	if err := syncDirectory(filepath.Dir(dir)); err != nil {
		return err
	}
	return nil
}

func indexDir(baseDir string, stream types.StreamInterface) string {
	streamID := stream.ID()
	// new feature so direct use destination database name present in stream
	baseDir = filepath.Join(baseDir, stream.GetDestinationDatabase(nil))
	sum := sha256.Sum256([]byte(streamID))
	safe := unsafeDirChars.ReplaceAllString(streamID, "_")
	if len(safe) > 80 {
		safe = safe[:80]
	}
	return filepath.Join(baseDir, fmt.Sprintf("%s-%s", safe, hex.EncodeToString(sum[:6])))
}

const (
	prefixRow          byte = 0x01
	prefixIDToFilePath byte = 0x02
	prefixFileByPath   byte = 0x03
	prefixMeta         byte = 0x04

	bloomBitsPerKey        = 10
	maxCompactions         = 4
	formatVersion   uint64 = 1 // used for versioning stream index
)

var (
	metaSnapshot      = []byte("snapshot")
	metaNextFileID    = []byte("next_file_id")
	metaFormatVersion = []byte("format_version")
)

type pebbleIndex struct {
	dir   string
	table string
	db    *pebble.DB
	lock  io.Closer

	fileMu     sync.RWMutex
	pathToID   map[string]uint64
	idToPath   map[uint64]string
	nextFileID uint64
}

func openIndex(dir string, opts StoreOptions) (*pebbleIndex, error) {
	return openIndexWithLock(dir, opts, nil)
}

func openIndexWithLock(dir string, opts StoreOptions, lock *pebble.Lock) (*pebbleIndex, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("failed to create stream index directory %s: %s", dir, err)
	}

	pebbleOpts := &pebble.Options{
		MemTableSize:               opts.MemTableSize,
		MaxOpenFiles:               opts.MaxOpenFiles,
		Lock:                       lock,
		Logger:                     pebbleLogger{},
		CompactionConcurrencyRange: func() (int, int) { return 1, maxCompactions },
	}

	for level := range pebbleOpts.Levels {
		pebbleOpts.Levels[level].FilterPolicy = bloom.FilterPolicy(bloomBitsPerKey)
	}

	if opts.CacheSize > 0 {
		cache := pebble.NewCache(opts.CacheSize)
		defer cache.Unref()
		pebbleOpts.Cache = cache
	}

	db, err := pebble.Open(dir, pebbleOpts)
	if err != nil {
		return nil, fmt.Errorf("failed to open pebble db at %s: %s", dir, err)
	}

	index := &pebbleIndex{
		dir:      dir,
		db:       db,
		pathToID: make(map[string]uint64),
		idToPath: make(map[uint64]string),
	}

	if err := index.load(); err != nil {
		_ = db.Close()
		return nil, err
	}

	return index, nil
}

func (i *pebbleIndex) load() error {
	if err := i.checkFormatVersion(); err != nil {
		return err
	}

	var err error
	if i.nextFileID, err = i.readMetaKey(metaNextFileID); err != nil {
		return err
	}

	return nil
}

func (i *pebbleIndex) checkFormatVersion() error {
	stored, err := i.readMetaKey(metaFormatVersion)
	if err != nil {
		return err
	}

	if stored == formatVersion {
		return nil
	}

	if stored != 0 {
		logger.Warnf("stream index %s: on-disk format v%d is not v%d, discarding it for a rebuild",
			i.dir, stored, formatVersion)
	}

	return i.Truncate()
}

func (i *pebbleIndex) Lookup(key string) (types.RowLocation, bool, error) {
	value, closer, err := i.db.Get(rowKey(key))
	if errors.Is(err, pebble.ErrNotFound) {
		return types.RowLocation{}, false, nil
	}
	if err != nil {
		return types.RowLocation{}, false, fmt.Errorf("failed to read stream index for key[%s]: %s", key, err)
	}

	fileID, position, decodeErr := decodeRow(value)
	if err := closer.Close(); err != nil {
		return types.RowLocation{}, false, fmt.Errorf("failed to release stream index read for key[%s]: %s", key, err)
	}
	if decodeErr != nil {
		return types.RowLocation{}, false, fmt.Errorf("corrupt stream index entry for key[%s]: %s", key, decodeErr)
	}

	path, err := i.filePath(fileID)
	if err != nil {
		return types.RowLocation{}, false, err
	}

	return types.RowLocation{FilePath: path, Position: position}, true, nil
}

// RangeFiles streams matching rows from one snapshot without resolving paths for
// unrelated rows. No commit lock is held while invoking fn, which may Commit.
func (i *pebbleIndex) RangeFiles(paths map[string]struct{}, fn func(string, types.RowLocation) error) (err error) {
	if len(paths) == 0 {
		return nil
	}
	snapshot := i.db.NewSnapshot()
	defer snapshot.Close()
	files := make(map[uint64]string, len(paths))
	for path := range paths {
		value, closer, err := snapshot.Get(fileByPathKey(path))
		if errors.Is(err, pebble.ErrNotFound) {
			continue
		}
		if err != nil {
			return fmt.Errorf("failed to read file id for %s: %s", path, err)
		}
		if len(value) != 8 {
			_ = closer.Close()
			return fmt.Errorf("corrupt file id for %s: expected 8 bytes, got %d", path, len(value))
		}
		fileID := binary.BigEndian.Uint64(value)
		if err := closer.Close(); err != nil {
			return fmt.Errorf("failed to release file id for %s: %s", path, err)
		}
		files[fileID] = path
	}
	if len(files) == 0 {
		return nil
	}
	iter, err := snapshot.NewIter(&pebble.IterOptions{LowerBound: []byte{prefixRow}, UpperBound: []byte{prefixRow + 1}})
	if err != nil {
		return fmt.Errorf("failed to scan stream index %s: %s", i.dir, err)
	}
	defer func() {
		if closeErr := iter.Close(); err == nil && closeErr != nil {
			err = fmt.Errorf("failed to close stream index scan in %s: %s", i.dir, closeErr)
		}
	}()
	for valid := iter.First(); valid; valid = iter.Next() {
		value, err := iter.ValueAndErr()
		if err != nil {
			return fmt.Errorf("failed to read stream index scan in %s: %s", i.dir, err)
		}
		fileID, read := binary.Uvarint(value)
		if read <= 0 {
			return fmt.Errorf("corrupt stream index entry for key[%s]: unreadable file id", iter.Key()[1:])
		}
		path, matches := files[fileID]
		if !matches {
			continue
		}
		position, read := binary.Uvarint(value[read:])
		if read <= 0 || position > math.MaxInt64 {
			return fmt.Errorf("corrupt stream index position for key[%s]", iter.Key()[1:])
		}
		if err := fn(string(iter.Key()[1:]), types.RowLocation{FilePath: path, Position: int64(position)}); err != nil {
			return err
		}
	}
	if err := iter.Error(); err != nil {
		return fmt.Errorf("failed to scan stream index %s: %s", i.dir, err)
	}
	return nil
}

// pebbleCommitStats belongs to a single Commit. Plain counters in the existing
// staging pass avoid per-row timers, atomics, allocations or extra iteration.
// Staging time includes file interning and its separately reported Sync time.
type pebbleCommitStats struct {
	mutations          int64
	fileIDLookups      int64
	fileIDCacheHits    int64
	fileIDStoreLookups int64
	newFileAttempts    int64
	newFiles           int64
	fileInternDuration time.Duration
	fileSyncAttempts   int64
	fileSyncSuccesses  int64
	fileSyncDuration   time.Duration
}

func (i *pebbleIndex) Commit(batch *types.StreamIndexThread, snapshotID *int64) (err error) {
	started := time.Now()
	var stats pebbleCommitStats
	var stagingDuration, batchSyncDuration time.Duration
	var checkpoint int64
	if snapshotID != nil {
		checkpoint = *snapshotID
	}
	stage := "staging"
	batchSyncAttempted, batchSyncSuccess := false, false
	defer func() {
		if stage == "staging" {
			stagingDuration = time.Since(started)
		}
		outcome := "success"
		if err != nil {
			outcome = "failed"
		}
		logger.Infof("Table[%s]: stream index Pebble commit outcome=%s stage=%s checkpoint_requested=%t snapshot=%d mutations=%d staging_ms=%d batch_sync_attempted=%t batch_sync_success=%t batch_sync_ms=%d file_id_lookups=%d file_id_cache_hits=%d file_id_store_lookups=%d new_file_attempts=%d new_files=%d file_intern_ms=%d file_sync_attempts=%d file_sync_successes=%d file_sync_ms=%d duration_ms=%d",
			i.table, outcome, stage, snapshotID != nil, checkpoint, stats.mutations, stagingDuration.Milliseconds(),
			batchSyncAttempted, batchSyncSuccess, batchSyncDuration.Milliseconds(), stats.fileIDLookups, stats.fileIDCacheHits, stats.fileIDStoreLookups,
			stats.newFileAttempts, stats.newFiles, stats.fileInternDuration.Milliseconds(), stats.fileSyncAttempts, stats.fileSyncSuccesses, stats.fileSyncDuration.Milliseconds(), time.Since(started).Milliseconds())
	}()
	pending := i.db.NewBatch()
	defer func() {
		_ = pending.Close()
	}()

	if batch != nil {
		if err := batch.RangeDeletes(func(key string) error {
			if err := pending.Delete(rowKey(key), nil); err != nil {
				return fmt.Errorf("failed to stage stream index deletion for key[%s]: %s", key, err)
			}
			stats.mutations++
			return nil
		}); err != nil {
			return err
		}
		err := batch.Range(func(key string, loc types.RowLocation) error {
			if err := i.stageChange(pending, key, loc, &stats); err != nil {
				return err
			}
			stats.mutations++
			return nil
		})
		if err != nil {
			return err
		}
	}

	if snapshotID != nil {
		if err := pending.Set(metaKey(metaSnapshot), binary.AppendVarint(nil, *snapshotID), nil); err != nil {
			return fmt.Errorf("failed to stage stream index checkpoint[%d] in %s: %s", *snapshotID, i.dir, err)
		}
		stats.mutations++
	}

	if pending.Empty() {
		stagingDuration = time.Since(started)
		stage = "empty"
		return nil
	}

	stagingDuration = time.Since(started)
	stage = "batch_sync"
	batchSyncAttempted = true
	syncStarted := time.Now()
	err = pending.Commit(pebble.Sync)
	batchSyncDuration = time.Since(syncStarted)
	if err != nil {
		return fmt.Errorf("failed to commit stream index changes in %s: %s", i.dir, err)
	}
	batchSyncSuccess = true
	stage = "complete"

	return nil
}

func (i *pebbleIndex) stageChange(batch *pebble.Batch, key string, loc types.RowLocation, stats *pebbleCommitStats) error {
	if loc.Position < 0 {
		return fmt.Errorf("stream index position for key[%s] must not be negative, got %d", key, loc.Position)
	}

	fileID, err := i.fileID(loc.FilePath, stats)
	if err != nil {
		return err
	}
	if err := batch.Set(rowKey(key), encodeRow(fileID, uint64(loc.Position)), nil); err != nil {
		return fmt.Errorf("failed to stage stream index entry for key[%s]: %s", key, err)
	}

	return nil
}

func (i *pebbleIndex) LastCommittedSnapshot() (int64, error) {
	value, closer, err := i.db.Get(metaKey(metaSnapshot))
	if errors.Is(err, pebble.ErrNotFound) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("failed to read indexed snapshot from stream index %s: %s", i.dir, err)
	}
	defer closer.Close()

	snapshotID, read := binary.Varint(value)
	if read <= 0 {
		return 0, fmt.Errorf("corrupt indexed snapshot in stream index %s", i.dir)
	}

	return snapshotID, nil
}

func (i *pebbleIndex) Truncate() error {
	generation, err := i.resyncGeneration()
	if err != nil {
		return err
	}
	return i.truncateGeneration(generation)
}

func (i *pebbleIndex) truncateGeneration(generation string) error {
	batch := i.db.NewBatch()
	defer batch.Close()

	for _, prefix := range [][]byte{{prefixRow}, {prefixIDToFilePath}, {prefixFileByPath}, {prefixMeta}} {
		if err := batch.DeleteRange(prefix, prefixEnd(prefix), nil); err != nil {
			return fmt.Errorf("failed to clear stream index %s: %s", i.dir, err)
		}
	}

	if err := setCounter(batch, metaFormatVersion, formatVersion); err != nil {
		return err
	}
	if generation != "" {
		if err := batch.Set(metaKey(metaResyncGeneration), []byte(generation), nil); err != nil {
			return fmt.Errorf("failed to stage stream index resync generation: %w", err)
		}
	}

	if err := batch.Commit(pebble.Sync); err != nil {
		return fmt.Errorf("failed to commit stream index truncate for %s: %s", i.dir, err)
	}

	i.fileMu.Lock()
	defer i.fileMu.Unlock()

	i.pathToID = make(map[string]uint64)
	i.idToPath = make(map[uint64]string)
	i.nextFileID = 0

	return nil
}

func (i *pebbleIndex) Close() error {
	err := i.db.Close()
	if i.lock != nil {
		err = errors.Join(err, i.lock.Close())
		i.lock = nil
	}
	if err != nil {
		return fmt.Errorf("failed to close stream index %s: %w", i.dir, err)
	}
	return nil
}

func (i *pebbleIndex) fileID(path string, stats *pebbleCommitStats) (uint64, error) {
	stats.fileIDLookups++
	i.fileMu.RLock()
	id, cached := i.pathToID[path]
	i.fileMu.RUnlock()
	if cached {
		stats.fileIDCacheHits++
		return id, nil
	}

	i.fileMu.Lock()
	defer i.fileMu.Unlock()

	if id, cached := i.pathToID[path]; cached {
		stats.fileIDCacheHits++
		return id, nil
	}

	stats.fileIDStoreLookups++
	id, found, err := i.readFileID(path)
	if err != nil {
		return 0, err
	}

	if !found {
		stats.newFileAttempts++
		started := time.Now()
		defer func() {
			stats.fileInternDuration += time.Since(started)
		}()
		id = i.nextFileID

		batch := i.db.NewBatch()
		defer batch.Close()
		if err := batch.Set(fileByIDKey(id), []byte(path), nil); err != nil {
			return 0, fmt.Errorf("failed to stage file id[%d] for %s: %s", id, path, err)
		}
		if err := batch.Set(fileByPathKey(path), be64(id), nil); err != nil {
			return 0, fmt.Errorf("failed to stage file path %s: %s", path, err)
		}
		if err := setCounter(batch, metaNextFileID, id+1); err != nil {
			return 0, err
		}
		stats.fileSyncAttempts++
		syncStarted := time.Now()
		err := batch.Commit(pebble.Sync)
		stats.fileSyncDuration += time.Since(syncStarted)
		if err != nil {
			return 0, fmt.Errorf("failed to intern file path %s: %s", path, err)
		}
		stats.fileSyncSuccesses++
		stats.newFiles++

		i.nextFileID = id + 1
	}

	i.pathToID[path] = id
	i.idToPath[id] = path
	return id, nil
}

func (i *pebbleIndex) readFileID(path string) (uint64, bool, error) {
	value, closer, err := i.db.Get(fileByPathKey(path))
	if errors.Is(err, pebble.ErrNotFound) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, fmt.Errorf("failed to read file id for %s: %s", path, err)
	}
	defer closer.Close()

	if len(value) != 8 {
		return 0, false, fmt.Errorf("corrupt file id for %s: expected 8 bytes, got %d", path, len(value))
	}
	return binary.BigEndian.Uint64(value), true, nil
}

func (i *pebbleIndex) filePath(id uint64) (string, error) {
	i.fileMu.RLock()
	path, cached := i.idToPath[id]
	i.fileMu.RUnlock()
	if cached {
		return path, nil
	}

	path, err := i.readFilePath(id)
	if err != nil {
		return "", err
	}

	i.fileMu.Lock()
	i.idToPath[id] = path
	i.pathToID[path] = id
	i.fileMu.Unlock()

	return path, nil
}

func (i *pebbleIndex) readFilePath(id uint64) (string, error) {
	value, closer, err := i.db.Get(fileByIDKey(id))
	if err != nil {
		return "", fmt.Errorf("failed to resolve indexed file id[%d] in %s: %s", id, i.dir, err)
	}

	path := string(value)
	if err := closer.Close(); err != nil {
		return "", fmt.Errorf("failed to release file id[%d] read: %s", id, err)
	}

	return path, nil
}

func (i *pebbleIndex) readMetaKey(name []byte) (uint64, error) {
	value, closer, err := i.db.Get(metaKey(name))
	if errors.Is(err, pebble.ErrNotFound) {
		return 0, nil
	}

	if err != nil {
		return 0, fmt.Errorf("failed to read stream index counter %s in %s: %s", name, i.dir, err)
	}
	defer closer.Close()

	counter, read := binary.Uvarint(value)
	if read <= 0 {
		return 0, fmt.Errorf("corrupt stream index counter %s in %s", name, i.dir)
	}

	return counter, nil
}

func rowKey(id string) []byte {
	return append(append(make([]byte, 0, 1+len(id)), prefixRow), id...)
}

func fileByIDKey(id uint64) []byte {
	return append([]byte{prefixIDToFilePath}, be64(id)...)
}

func fileByPathKey(path string) []byte {
	return append(append(make([]byte, 0, 1+len(path)), prefixFileByPath), path...)
}

func metaKey(name []byte) []byte {
	return append(append(make([]byte, 0, 1+len(name)), prefixMeta), name...)
}

func be64(value uint64) []byte {
	encoded := make([]byte, 8)
	binary.BigEndian.PutUint64(encoded, value)
	return encoded
}

func encodeRow(fileID, position uint64) []byte {
	return binary.AppendUvarint(binary.AppendUvarint(make([]byte, 0, 2*binary.MaxVarintLen64), fileID), position)
}

func decodeRow(value []byte) (fileID uint64, position int64, err error) {
	fileID, read := binary.Uvarint(value)
	if read <= 0 {
		return 0, 0, fmt.Errorf("unreadable file id")
	}

	raw, readPosition := binary.Uvarint(value[read:])
	if readPosition <= 0 {
		return 0, 0, fmt.Errorf("unreadable position")
	}
	if raw > math.MaxInt64 {
		return 0, 0, fmt.Errorf("position %d out of range", raw)
	}

	return fileID, int64(raw), nil
}

func setCounter(batch *pebble.Batch, name []byte, value uint64) error {
	if err := batch.Set(metaKey(name), binary.AppendUvarint(nil, value), nil); err != nil {
		return fmt.Errorf("failed to stage stream index counter %s: %s", name, err)
	}
	return nil
}

func prefixEnd(prefix []byte) []byte {
	end := make([]byte, len(prefix))
	copy(end, prefix)

	for i := len(end) - 1; i >= 0; i-- {
		end[i]++
		if end[i] != 0 {
			return end[:i+1]
		}
	}

	return nil
}

type pebbleLogger struct{}

func (pebbleLogger) Infof(format string, args ...interface{}) {
	logger.Debugf("stream index: "+format, args...)
}

func (pebbleLogger) Errorf(format string, args ...interface{}) {
	logger.Errorf("stream index: "+format, args...)
}

func (pebbleLogger) Fatalf(format string, args ...interface{}) {
	logger.Fatalf("stream index: "+format, args...)
}
