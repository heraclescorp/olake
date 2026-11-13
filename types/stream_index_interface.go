package types

import (
	"context"

	"github.com/datazip-inc/olake/utils/observability"
)

// RowLocation addresses a single row within one data file of a destination table.
type RowLocation struct {
	FilePath string
	Position int64
}

// tableIndex is interface for index store
type StreamIndex interface {
	// lookup for keys in index store
	Lookup(key string) (loc RowLocation, found bool, err error)
	// RangeFiles visits a consistent snapshot of mappings to paths. Callbacks may Commit.
	RangeFiles(paths map[string]struct{}, fn func(key string, loc RowLocation) error) error
	// Commit atomically applies pending puts, deletes, and the optional snapshot checkpoint.
	Commit(batch *StreamIndexThread, snapshotID *int64) error
	// LastCommittedSnapshot returns the last committed snapshot ID
	LastCommittedSnapshot() (snapshotID int64, err error)
	// truncate index store
	Truncate() error
	Close() error
}

// streamIndexChange is one pending mutation.
type streamIndexChange struct {
	loc RowLocation
}

// StreamIndexThread buffers the index changes of a single writer thread until the destination commit happens
type StreamIndexThread struct {
	index           StreamIndex
	changes         map[string]streamIndexChange
	deletes         map[string]struct{}
	metricsContext  context.Context
	metricsEntries  int64
	metricsBytes    int64
	reportedEntries int64
	reportedBytes   int64
	metricUpdates   uint64
}

// NewStreamIndexThread returns an empty batch that resolves misses against index.
func NewStreamIndexThread(index StreamIndex) *StreamIndexThread {
	if index == nil {
		return nil
	}

	return &StreamIndexThread{index: index, changes: make(map[string]streamIndexChange)}
}

// Put records key as living at loc once this batch is applied.
func (b *StreamIndexThread) Put(key string, loc RowLocation) {
	if b.metricsContext != nil {
		previous, exists := b.changes[key]
		if !exists {
			if _, deleted := b.deletes[key]; deleted {
				b.metricsBytes += 32
			} else {
				b.metricsEntries++
				b.metricsBytes += int64(len(key) + 48)
			}
		} else {
			b.metricsBytes -= int64(len(previous.loc.FilePath))
		}
		b.metricsBytes += int64(len(loc.FilePath))
		b.metricUpdates++
		if b.metricUpdates%1024 == 0 {
			b.reportPendingIndex()
		}
	}
	if b.deletes != nil {
		delete(b.deletes, key)
	}
	b.changes[key] = streamIndexChange{loc: loc}
}

// Delete records key as absent once this batch is applied, replacing any pending put.
func (b *StreamIndexThread) Delete(key string) {
	if b.metricsContext != nil {
		if previous, exists := b.changes[key]; exists {
			b.metricsBytes -= int64(len(previous.loc.FilePath) + 32)
		} else if _, deleted := b.deletes[key]; !deleted {
			b.metricsEntries++
			b.metricsBytes += int64(len(key) + 16)
		}
		b.metricUpdates++
		if b.metricUpdates%1024 == 0 {
			b.reportPendingIndex()
		}
	}
	delete(b.changes, key)
	if b.deletes == nil {
		b.deletes = make(map[string]struct{})
	}
	b.deletes[key] = struct{}{}
}

// Lookup resolves key against this batch's pending changes first, so a row
// written moments ago by the same thread resolves to where it is about to land
// rather than to the stale location the last sync committed.
func (b *StreamIndexThread) Lookup(key string) (RowLocation, bool, error) {
	if change, pending := b.changes[key]; pending {
		return change.loc, true, nil
	}
	if _, deleted := b.deletes[key]; deleted {
		return RowLocation{}, false, nil
	}
	if b.index == nil {
		return RowLocation{}, false, nil
	}
	return b.index.Lookup(key)
}

// Range hands every pending put to fn, stopping at the first error.
func (b *StreamIndexThread) Range(fn func(key string, loc RowLocation) error) error {
	for key, change := range b.changes {
		if err := fn(key, change.loc); err != nil {
			return err
		}
	}
	return nil
}

// RangeDeletes hands every pending deletion key to fn, stopping at the first error.
func (b *StreamIndexThread) RangeDeletes(fn func(string) error) error {
	for key := range b.deletes {
		if err := fn(key); err != nil {
			return err
		}
	}
	return nil
}

// SetObservabilityContext enables estimated logical index payload accounting for this chunk.
// It deliberately does not claim to measure Go heap or shared-string ownership.
func (b *StreamIndexThread) SetObservabilityContext(ctx context.Context) {
	if b != nil && b.metricsContext == nil && observability.Enabled() {
		b.metricsContext = ctx
	}
}

func (b *StreamIndexThread) reportPendingIndex() {
	observability.AddPendingIndex(b.metricsContext, b.metricsEntries-b.reportedEntries, b.metricsBytes-b.reportedBytes)
	b.reportedEntries, b.reportedBytes = b.metricsEntries, b.metricsBytes
}

// ReleaseObservability removes this chunk's contribution after commit or abort.
func (b *StreamIndexThread) ReleaseObservability() {
	if b == nil || b.metricsContext == nil {
		return
	}
	observability.AddPendingIndex(b.metricsContext, -b.reportedEntries, -b.reportedBytes)
	b.reportedEntries, b.reportedBytes = 0, 0
	b.metricsContext = nil
}
