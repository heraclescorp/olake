package indexdb

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/cockroachdb/pebble/v2"
	"github.com/datazip-inc/olake/types"
)

var metaResyncGeneration = []byte("resync_generation")

type attemptIndex struct {
	*pebbleIndex
}

// OpenAttempt never opens or truncates the canonical stream index, even on a same-ID retry.
func OpenAttempt(stream types.StreamInterface) (types.StreamIndex, error) {
	opts := DefaultOptions()
	canonical := indexDir(opts.Dir, stream)
	if err := os.MkdirAll(filepath.Dir(canonical), 0o755); err != nil {
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
	return &attemptIndex{pebbleIndex: index}, nil
}

func (i *attemptIndex) Close() error {
	if err := i.pebbleIndex.Close(); err != nil {
		return err
	}
	return os.RemoveAll(i.dir)
}

// ReconcileGeneration invalidates the checkpoint before any row in a replacement
// lineage can be read. The destination's durable marker, not an in-process index
// swap, makes a crash immediately after publication safe.
func ReconcileGeneration(index types.StreamIndex, generation string) error {
	if index == nil {
		return nil
	}
	var store *pebbleIndex
	switch index := index.(type) {
	case *pebbleIndex:
		store = index
	case *attemptIndex:
		store = index.pebbleIndex
	default:
		return fmt.Errorf("stream index does not support durable resync generations")
	}
	previous, err := store.resyncGeneration()
	if err != nil {
		return err
	}
	if previous != "" && generation == "" {
		return fmt.Errorf("destination omitted the durable resync generation; refusing to use stale row locations")
	}
	if previous == generation {
		return nil
	}
	return store.truncateGeneration(generation)
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
