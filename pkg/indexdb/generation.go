package indexdb

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/cockroachdb/pebble/v2"
	"github.com/datazip-inc/olake/types"
	"github.com/google/uuid"
)

var metaResyncGeneration = []byte("resync_generation")

type attemptIndex struct {
	*pebbleIndex
	canonical string
	closed    bool
}

// OpenAttempt never opens or truncates the canonical stream index, even on a same-ID retry.
func OpenAttempt(stream types.StreamInterface) (types.StreamIndex, error) {
	opts := DefaultOptions()
	canonical := indexDir(opts.Dir, stream)
	if err := ensureDirectory(filepath.Dir(canonical)); err != nil {
		return nil, fmt.Errorf("failed to create resync index parent: %w", err)
	}
	dir, err := os.MkdirTemp(filepath.Dir(canonical), filepath.Base(canonical)+"-resync-")
	if err != nil {
		return nil, fmt.Errorf("failed to create isolated resync index: %w", err)
	}
	index, err := openIndex(dir, opts)
	if err != nil {
		return nil, errors.Join(err, os.RemoveAll(dir))
	}
	index.table = fmt.Sprintf("%s.%s", stream.GetDestinationDatabase(nil), stream.GetDestinationTable())
	return &attemptIndex{pebbleIndex: index, canonical: canonical}, nil
}

func (i *attemptIndex) Close() error {
	if i.closed {
		return nil
	}
	i.closed = true
	if err := i.pebbleIndex.Close(); err != nil {
		return err
	}
	return os.RemoveAll(i.dir)
}

func ReconcileGeneration(index types.StreamIndex, generation string) error {
	if index == nil {
		return nil
	}
	store, ok := index.(interface{ ReconcileGeneration(string) error })
	if !ok {
		return fmt.Errorf("stream index does not support durable resync generations")
	}
	return store.ReconcileGeneration(generation)
}

func ReconcileTable(index types.StreamIndex, generation, tableUUID string) error {
	if index == nil {
		return nil
	}
	store, ok := index.(interface{ ReconcileTable(string, string) error })
	if !ok {
		return ReconcileGeneration(index, generation)
	}
	return store.ReconcileTable(generation, tableUUID)
}

func (i *pebbleIndex) ReconcileTable(generation, tableUUID string) error {
	previous, err := i.resyncGeneration()
	if err != nil {
		return err
	}
	if previous != "" && generation == "" {
		return fmt.Errorf("destination omitted the durable resync generation; refusing to use stale row locations")
	}
	storedUUID, err := i.promotionTableUUID()
	if err != nil {
		return err
	}
	if storedUUID != "" {
		id, err := uuid.Parse(tableUUID)
		if err != nil || id == uuid.Nil {
			return fmt.Errorf("destination omitted a valid table UUID; refusing to use promoted row locations")
		}
		tableUUID = id.String()
	}
	if previous != generation || storedUUID != "" && storedUUID != tableUUID {
		return i.truncateGeneration(generation)
	}
	return nil
}

// ReconcileGeneration binds this durable index to one public table lineage.
// attemptIndex inherits the same implementation without special-case dispatch.
func (i *pebbleIndex) ReconcileGeneration(generation string) error {
	previous, err := i.resyncGeneration()
	if err != nil {
		return err
	}
	if previous != "" && generation == "" {
		return fmt.Errorf("destination omitted the durable resync generation; refusing to use stale row locations")
	}
	if previous == generation {
		return nil
	}
	return i.truncateGeneration(generation)
}

func (i *pebbleIndex) resyncGeneration() (string, error) {
	value, closer, err := i.db.Get(metaKey(metaResyncGeneration))
	if errors.Is(err, pebble.ErrNotFound) {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("failed to read stream index resync generation: %w", err)
	}
	generation := string(value)
	return generation, closer.Close()
}
