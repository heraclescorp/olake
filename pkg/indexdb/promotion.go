package indexdb

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/vfs"
	"github.com/datazip-inc/olake/types"
	"github.com/google/uuid"
)

var ErrUnpromotable = errors.New("resync index cannot be promoted")

var (
	metaPromotionUUID     = []byte("promotion_table_uuid")
	metaPromotionSnapshot = []byte("promotion_snapshot")
)

func PromoteAttempt(index types.StreamIndex, generation, tableUUID string, snapshotID int64) error {
	attempt, ok := index.(*attemptIndex)
	if !ok || attempt == nil || attempt.closed {
		return fmt.Errorf("%w: index is not an open isolated attempt", ErrUnpromotable)
	}
	id, err := uuid.Parse(tableUUID)
	if strings.TrimSpace(generation) == "" || err != nil || id == uuid.Nil || snapshotID <= 0 {
		return fmt.Errorf("%w: invalid publication identity", ErrUnpromotable)
	}
	actualGeneration, err := attempt.resyncGeneration()
	if err != nil {
		return err
	}
	checkpoint, err := attempt.LastCommittedSnapshot()
	if err != nil {
		return err
	}
	if actualGeneration != generation || checkpoint != snapshotID {
		return fmt.Errorf("%w: attempt generation or snapshot does not match publication", ErrUnpromotable)
	}

	lock, err := lockCanonical(attempt.canonical)
	if err != nil {
		return err
	}
	defer lock.Close()
	if err := recoverPromotion(attempt.canonical, DefaultOptions()); err != nil {
		return err
	}
	oldLock, err := lockIndexDirectory(attempt.canonical)
	if err != nil {
		return err
	}
	if oldLock != nil {
		defer oldLock.Close()
	}

	batch := attempt.db.NewBatch()
	if err := batch.Set(metaKey(metaPromotionUUID), []byte(id.String()), nil); err != nil {
		batch.Close()
		return err
	}
	if err := batch.Set(metaKey(metaPromotionSnapshot), binary.AppendVarint(nil, snapshotID), nil); err != nil {
		batch.Close()
		return err
	}
	err = batch.Commit(pebble.Sync)
	closeErr := batch.Close()
	if err != nil || closeErr != nil {
		return errors.Join(err, closeErr)
	}
	attempt.closed = true
	if err := attempt.pebbleIndex.Close(); err != nil {
		return err
	}
	if err := syncDirectory(attempt.dir); err != nil {
		return err
	}
	parent := filepath.Dir(attempt.canonical)
	if err := syncDirectory(parent); err != nil {
		return err
	}

	if oldLock != nil {
		if err := os.Rename(attempt.canonical, promotionBackup(attempt.canonical)); err != nil {
			return fmt.Errorf("failed to preserve canonical stream index: %w", err)
		}
		if err := syncDirectory(parent); err != nil {
			return err
		}
	}
	if err := os.Rename(attempt.dir, attempt.canonical); err != nil {
		return fmt.Errorf("failed to publish promoted stream index: %w", err)
	}
	if err := syncDirectory(parent); err != nil {
		return err
	}
	if oldLock != nil {
		if err := os.RemoveAll(promotionBackup(attempt.canonical)); err != nil {
			return fmt.Errorf("failed to remove replaced stream index: %w", err)
		}
		if err := syncDirectory(parent); err != nil {
			return err
		}
	}
	return nil
}

func (i *pebbleIndex) promotionTableUUID() (string, error) {
	value, closer, err := i.db.Get(metaKey(metaPromotionUUID))
	if errors.Is(err, pebble.ErrNotFound) {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("failed to read promoted index table UUID: %w", err)
	}
	id := string(value)
	return id, closer.Close()
}

func (i *pebbleIndex) validPromotionSeal() (bool, error) {
	id, err := i.promotionTableUUID()
	if err != nil {
		return false, err
	}
	parsed, err := uuid.Parse(id)
	if err != nil || parsed == uuid.Nil {
		return false, nil
	}
	generation, err := i.resyncGeneration()
	if err != nil {
		return false, err
	}
	if strings.TrimSpace(generation) == "" {
		return false, nil
	}
	value, closer, err := i.db.Get(metaKey(metaPromotionSnapshot))
	if errors.Is(err, pebble.ErrNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	snapshot, n := binary.Varint(value)
	closeErr := closer.Close()
	if closeErr != nil {
		return false, closeErr
	}
	if n <= 0 || snapshot <= 0 {
		return false, nil
	}
	checkpoint, err := i.LastCommittedSnapshot()
	return checkpoint == snapshot, err
}

func lockCanonical(dir string) (io.Closer, error) {
	if err := ensureDirectory(filepath.Dir(dir)); err != nil {
		return nil, fmt.Errorf("failed to create index parent: %w", err)
	}
	lock, err := vfs.Default.Lock(dir + "-promotion-lock")
	if err != nil {
		return nil, fmt.Errorf("failed to lock canonical stream index %s: %w", dir, err)
	}
	return lock, nil
}

func lockIndexDirectory(dir string) (io.Closer, error) {
	_, err := os.Stat(dir)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	lock, err := pebble.LockDirectory(dir, vfs.Default)
	if err != nil {
		return nil, fmt.Errorf("failed to lock stream index directory %s: %w", dir, err)
	}
	return lock, nil
}

func promotionBackup(dir string) string {
	return dir + "-promotion-backup"
}

func ensureDirectory(dir string) error {
	info, err := os.Stat(dir)
	if err == nil {
		if !info.IsDir() {
			return fmt.Errorf("index parent %s is not a directory", dir)
		}
		return nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	parent := filepath.Dir(dir)
	if parent == dir {
		return fmt.Errorf("index parent %s does not exist", dir)
	}
	if err := ensureDirectory(parent); err != nil {
		return err
	}
	if err := os.Mkdir(dir, 0o755); err != nil && !errors.Is(err, os.ErrExist) {
		return err
	}
	info, err = os.Stat(dir)
	if err != nil {
		return err
	}
	if !info.IsDir() {
		return fmt.Errorf("index parent %s is not a directory", dir)
	}
	return syncDirectory(parent)
}

func syncDirectory(dir string) error {
	file, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("failed to open index directory for sync: %w", err)
	}
	err = file.Sync()
	closeErr := file.Close()
	if err != nil || closeErr != nil {
		return fmt.Errorf("failed to sync index directory %s: %w", dir, errors.Join(err, closeErr))
	}
	return nil
}

func recoverPromotion(dir string, opts StoreOptions) error {
	backup := promotionBackup(dir)
	backupLock, err := lockIndexDirectory(backup)
	if err != nil {
		return err
	}
	if backupLock == nil {
		return nil
	}
	defer backupLock.Close()
	parent := filepath.Dir(dir)
	_, err = os.Stat(dir)
	if errors.Is(err, os.ErrNotExist) {
		if err := os.Rename(backup, dir); err != nil {
			return fmt.Errorf("failed to restore stream index backup: %w", err)
		}
		return syncDirectory(parent)
	}
	if err != nil {
		return err
	}
	candidateLock, err := pebble.LockDirectory(dir, vfs.Default)
	if err != nil {
		return fmt.Errorf("failed to lock interrupted index promotion: %w", err)
	}
	defer candidateLock.Close()
	sealed := false
	candidate, err := openIndexWithLock(dir, opts, candidateLock)
	if err == nil {
		var sealErr error
		sealed, sealErr = candidate.validPromotionSeal()
		if err := candidate.Close(); err != nil {
			return err
		}
		if sealErr != nil {
			sealed = false
		}
	}
	if !sealed {
		quarantine, err := os.MkdirTemp(parent, filepath.Base(dir)+"-incomplete-")
		if err != nil {
			return err
		}
		if err := os.Remove(quarantine); err != nil {
			return err
		}
		if err := os.Rename(dir, quarantine); err != nil {
			return fmt.Errorf("failed to quarantine incomplete promoted index: %w", err)
		}
		if err := syncDirectory(parent); err != nil {
			return err
		}
		if err := os.Rename(backup, dir); err != nil {
			return fmt.Errorf("failed to restore stream index backup: %w", err)
		}
		return syncDirectory(parent)
	}
	if err := syncDirectory(parent); err != nil {
		return err
	}
	if err := os.RemoveAll(backup); err != nil {
		return fmt.Errorf("failed to remove replaced stream index: %w", err)
	}
	return syncDirectory(parent)
}
