package indexdb

import (
	"errors"
	"testing"

	"github.com/datazip-inc/olake/types"
	"github.com/stretchr/testify/require"
)

func requireIndexRow(t *testing.T, index interface {
	Lookup(string) (types.RowLocation, bool, error)
}, key string, expected types.RowLocation, present bool) {
	t.Helper()
	location, found, err := index.Lookup(key)
	require.NoError(t, err)
	require.Equal(t, present, found, "key %q", key)
	require.Equal(t, expected, location, "key %q", key)
}

func requireIndexSnapshot(t *testing.T, index types.StreamIndex, expected int64) {
	t.Helper()
	snapshot, err := index.LastCommittedSnapshot()
	require.NoError(t, err)
	require.Equal(t, expected, snapshot)
}

func TestStreamIndexThreadDeletePutPrecedence(t *testing.T) {
	index, err := openIndex(t.TempDir(), StoreOptions{MemTableSize: 4 << 20, MaxOpenFiles: 64})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, index.Close()) })
	old := types.RowLocation{FilePath: "old.parquet", Position: 3}
	restored := types.RowLocation{FilePath: "new.parquet", Position: 7}
	seed := types.NewStreamIndexThread(index)
	seed.Put("delete-existing", old)
	seed.Put("delete-then-put", old)
	seed.Put("unchanged", old)
	checkpoint := int64(10)
	require.NoError(t, index.Commit(seed, &checkpoint))

	pending := types.NewStreamIndexThread(index)
	pending.Delete("delete-existing")
	pending.Delete("delete-existing")
	pending.Put("put-then-delete", restored)
	pending.Delete("put-then-delete")
	pending.Delete("delete-then-put")
	requireIndexRow(t, pending, "delete-then-put", types.RowLocation{}, false)
	pending.Put("delete-then-put", restored)
	pending.Delete("delete-missing")
	pending.Put("put-only", restored)
	for _, key := range []string{"delete-existing", "put-then-delete", "delete-missing"} {
		location, found, err := pending.Lookup(key)
		require.NoError(t, err)
		require.False(t, found, "key %q", key)
		require.Empty(t, location)
	}
	location, found, err := pending.Lookup("delete-then-put")
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, restored, location)
	location, found, err = pending.Lookup("unchanged")
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, old, location)
	requireIndexRow(t, index, "delete-existing", old, true)

	puts := make(map[string]types.RowLocation)
	require.NoError(t, pending.Range(func(key string, location types.RowLocation) error {
		puts[key] = location
		return nil
	}))
	require.Equal(t, map[string]types.RowLocation{"delete-then-put": restored, "put-only": restored}, puts)
	var deletes []string
	require.NoError(t, pending.RangeDeletes(func(key string) error {
		deletes = append(deletes, key)
		return nil
	}))
	require.ElementsMatch(t, []string{"delete-existing", "put-then-delete", "delete-missing"}, deletes)
	stop := errors.New("stop iteration")
	calls := 0
	require.ErrorIs(t, pending.RangeDeletes(func(string) error { calls++; return stop }), stop)
	require.Equal(t, 1, calls)

	require.NoError(t, index.Commit(pending, nil))
	for _, key := range deletes {
		requireIndexRow(t, index, key, types.RowLocation{}, false)
	}
	for key, expected := range puts {
		requireIndexRow(t, index, key, expected, true)
	}
	requireIndexRow(t, index, "unchanged", old, true)
	requireIndexSnapshot(t, index, checkpoint)
}

func TestPebbleDeletePersistsWithCheckpointAndRestores(t *testing.T) {
	dir := t.TempDir()
	opts := StoreOptions{MemTableSize: 4 << 20, MaxOpenFiles: 64}
	index, err := openIndex(dir, opts)
	require.NoError(t, err)
	t.Cleanup(func() {
		if index != nil {
			require.NoError(t, index.Close())
		}
	})
	old := types.RowLocation{FilePath: "old.parquet", Position: 2}
	current := types.RowLocation{FilePath: "replacement.parquet", Position: 5}
	seed := types.NewStreamIndexThread(index)
	seed.Put("removed", old)
	seed.Put("unchanged", old)
	checkpoint := int64(40)
	require.NoError(t, index.Commit(seed, &checkpoint))

	pending := types.NewStreamIndexThread(index)
	pending.Delete("removed")
	pending.Delete("absent")
	pending.Put("replacement", current)
	checkpoint++
	require.NoError(t, index.Commit(pending, &checkpoint))
	require.NoError(t, index.Close())
	index, err = openIndex(dir, opts)
	require.NoError(t, err)
	requireIndexRow(t, index, "removed", types.RowLocation{}, false)
	requireIndexRow(t, index, "absent", types.RowLocation{}, false)
	requireIndexRow(t, index, "replacement", current, true)
	requireIndexRow(t, index, "unchanged", old, true)
	requireIndexSnapshot(t, index, checkpoint)
	_, found, err := index.readFileID("")
	require.NoError(t, err)
	require.False(t, found, "deletions must not intern an empty file path")
	require.Equal(t, uint64(2), index.nextFileID, "puts still share interned file paths")

	pending = types.NewStreamIndexThread(index)
	pending.Put("removed", current)
	checkpoint++
	require.NoError(t, index.Commit(pending, &checkpoint))
	require.NoError(t, index.Close())
	index, err = openIndex(dir, opts)
	require.NoError(t, err)
	requireIndexRow(t, index, "removed", current, true)
	requireIndexRow(t, index, "replacement", current, true)
	requireIndexSnapshot(t, index, checkpoint)
	require.Equal(t, uint64(2), index.nextFileID)
}

func TestPebbleFailedCommitKeepsMutationsAndCheckpointUnpublished(t *testing.T) {
	for _, test := range []struct {
		name       string
		checkpoint int64
	}{
		{name: "valid checkpoint", checkpoint: 60},
		{name: "invalidated checkpoint", checkpoint: 0},
	} {
		t.Run(test.name, func(t *testing.T) {
			dir := t.TempDir()
			opts := StoreOptions{MemTableSize: 4 << 20, MaxOpenFiles: 64}
			index, err := openIndex(dir, opts)
			require.NoError(t, err)
			t.Cleanup(func() {
				if index != nil {
					require.NoError(t, index.Close())
				}
			})
			old := types.RowLocation{FilePath: "old.parquet", Position: 8}
			current := types.RowLocation{FilePath: "old.parquet", Position: 9}
			seed := types.NewStreamIndexThread(index)
			seed.Put("removed", old)
			seed.Put("updated", old)
			require.NoError(t, index.Commit(seed, &test.checkpoint))

			pending := types.NewStreamIndexThread(index)
			pending.Delete("removed")
			pending.Put("updated", current)
			pending.Put("new", current)
			pending.Put("invalid", types.RowLocation{FilePath: "old.parquet", Position: -1})
			nextCheckpoint := int64(61)
			require.ErrorContains(t, index.Commit(pending, &nextCheckpoint), "must not be negative")
			requireIndexRow(t, index, "removed", old, true)
			requireIndexRow(t, index, "updated", old, true)
			requireIndexRow(t, index, "new", types.RowLocation{}, false)
			requireIndexRow(t, index, "invalid", types.RowLocation{}, false)
			requireIndexSnapshot(t, index, test.checkpoint)

			require.NoError(t, index.Close())
			index, err = openIndex(dir, opts)
			require.NoError(t, err)
			requireIndexRow(t, index, "removed", old, true)
			requireIndexRow(t, index, "updated", old, true)
			requireIndexRow(t, index, "new", types.RowLocation{}, false)
			requireIndexRow(t, index, "invalid", types.RowLocation{}, false)
			requireIndexSnapshot(t, index, test.checkpoint)

			pending.Put("invalid", current)
			require.NoError(t, index.Commit(pending, &nextCheckpoint))
			requireIndexRow(t, index, "removed", types.RowLocation{}, false)
			for _, key := range []string{"updated", "new", "invalid"} {
				requireIndexRow(t, index, key, current, true)
			}
			requireIndexSnapshot(t, index, nextCheckpoint)
		})
	}
}

func TestPebbleRangeFilesSnapshotAllowsCallbackCommit(t *testing.T) {
	dir := t.TempDir()
	opts := StoreOptions{MemTableSize: 4 << 20, MaxOpenFiles: 64}
	index, err := openIndex(dir, opts)
	require.NoError(t, err)
	t.Cleanup(func() {
		if index != nil {
			require.NoError(t, index.Close())
		}
	})
	selected := map[string]types.RowLocation{
		"a": {FilePath: "removed.parquet", Position: 1},
		"b": {FilePath: "removed.parquet", Position: 2},
		"c": {FilePath: "changed-deletes.parquet", Position: 3},
	}
	unchanged := types.RowLocation{FilePath: "unchanged.parquet", Position: 4}
	seed := types.NewStreamIndexThread(index)
	for key, loc := range selected {
		seed.Put(key, loc)
	}
	seed.Put("unchanged", unchanged)
	checkpoint := int64(70)
	require.NoError(t, index.Commit(seed, &checkpoint))
	require.NoError(t, index.Close())
	index, err = openIndex(dir, opts)
	require.NoError(t, err)

	paths := map[string]struct{}{"removed.parquet": {}, "changed-deletes.parquet": {}, "unknown.parquet": {}}
	seen := make(map[string]types.RowLocation)
	inserted := types.RowLocation{FilePath: "removed.parquet", Position: 10}
	require.NoError(t, index.RangeFiles(paths, func(key string, loc types.RowLocation) error {
		seen[key] = loc
		if len(seen) == 1 {
			pending := types.NewStreamIndexThread(index)
			for removed := range selected {
				pending.Delete(removed)
			}
			pending.Put("inserted-during-scan", inserted)
			checkpoint++
			return index.Commit(pending, &checkpoint)
		}
		return nil
	}))
	require.Equal(t, selected, seen, "callbacks see the original mappings despite committed deletes and puts")
	for key := range selected {
		requireIndexRow(t, index, key, types.RowLocation{}, false)
	}
	requireIndexRow(t, index, "inserted-during-scan", inserted, true)
	requireIndexRow(t, index, "unchanged", unchanged, true)
	requireIndexSnapshot(t, index, checkpoint)

	require.NoError(t, index.Close())
	index, err = openIndex(dir, opts)
	require.NoError(t, err)
	seen = make(map[string]types.RowLocation)
	require.NoError(t, index.RangeFiles(paths, func(key string, loc types.RowLocation) error {
		seen[key] = loc
		return nil
	}))
	require.Equal(t, map[string]types.RowLocation{"inserted-during-scan": inserted}, seen)
	requireIndexSnapshot(t, index, checkpoint)
	for _, empty := range []map[string]struct{}{nil, {}, {"unknown.parquet": {}}} {
		require.NoError(t, index.RangeFiles(empty, func(string, types.RowLocation) error {
			t.Fatal("unmatched file paths must not invoke the callback")
			return nil
		}))
	}
	stop := errors.New("stop scan")
	calls := 0
	require.ErrorIs(t, index.RangeFiles(paths, func(string, types.RowLocation) error { calls++; return stop }), stop)
	require.Equal(t, 1, calls)
}
