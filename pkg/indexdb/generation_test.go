package indexdb

import (
	"path/filepath"
	"testing"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/types"
	"github.com/stretchr/testify/require"
)

func seedGenerationIndex(t *testing.T, index types.StreamIndex, key, path string, snapshot int64) {
	t.Helper()
	pending := types.NewStreamIndexThread(index)
	pending.Put(key, types.RowLocation{FilePath: path, Position: 7})
	require.NoError(t, index.Commit(pending, &snapshot))
}

func TestResyncGenerationInvalidatesOldLocationsAfterPublicationCrash(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "index")
	opts := StoreOptions{MemTableSize: 4 << 20, MaxOpenFiles: 128}
	index, err := openIndex(dir, opts)
	require.NoError(t, err)
	seedGenerationIndex(t, index, "old-row", "old.parquet", 11)
	require.NoError(t, index.Close())

	// The catalog commit survived, but the publishing process never touched the
	// canonical index. Its next opener must invalidate the old physical lineage.
	index, err = openIndex(dir, opts)
	require.NoError(t, err)
	require.NoError(t, ReconcileGeneration(index, "completed-resync"))
	_, found, err := index.Lookup("old-row")
	require.NoError(t, err)
	require.False(t, found)
	snapshot, err := index.LastCommittedSnapshot()
	require.NoError(t, err)
	require.Zero(t, snapshot)
	seedGenerationIndex(t, index, "new-row", "new.parquet", 22)
	require.NoError(t, index.Close())

	index, err = openIndex(dir, opts)
	require.NoError(t, err)
	defer func() { require.NoError(t, index.Close()) }()
	require.NoError(t, ReconcileGeneration(index, "completed-resync"))
	location, found, err := index.Lookup("new-row")
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, "new.parquet", location.FilePath)
	snapshot, err = index.LastCommittedSnapshot()
	require.NoError(t, err)
	require.Equal(t, int64(22), snapshot)

	require.NoError(t, ReconcileGeneration(index, "second-resync"))
	_, found, err = index.Lookup("new-row")
	require.NoError(t, err)
	require.False(t, found)
}

func TestFullIndexRebuildPreservesResyncGeneration(t *testing.T) {
	index, err := openIndex(filepath.Join(t.TempDir(), "index"), StoreOptions{MemTableSize: 4 << 20, MaxOpenFiles: 128})
	require.NoError(t, err)
	defer func() { require.NoError(t, index.Close()) }()
	require.NoError(t, ReconcileGeneration(index, "completed-resync"))
	require.NoError(t, index.Truncate())
	seedGenerationIndex(t, index, "live-row", "live.parquet", 33)
	require.NoError(t, ReconcileGeneration(index, "completed-resync"))
	_, found, err := index.Lookup("live-row")
	require.NoError(t, err)
	require.True(t, found)
}

func TestResyncAttemptsAreFreshAndDoNotTouchCanonicalIndex(t *testing.T) {
	t.Setenv(constants.IndexDBDir, t.TempDir())
	stream := types.NewStream("events", "public", nil).Wrap(0)
	canonical, err := Open(stream)
	require.NoError(t, err)
	defer func() { require.NoError(t, canonical.Close()) }()
	seedGenerationIndex(t, canonical, "old-row", "old.parquet", 11)

	first, err := OpenAttempt(stream)
	require.NoError(t, err)
	defer func() { require.NoError(t, first.Close()) }()
	seedGenerationIndex(t, first, "attempt-row", "first.parquet", 22)
	second, err := OpenAttempt(stream)
	require.NoError(t, err)
	defer func() { require.NoError(t, second.Close()) }()
	require.NotEqual(t, first.(*attemptIndex).dir, second.(*attemptIndex).dir)
	_, found, err := second.Lookup("attempt-row")
	require.NoError(t, err)
	require.False(t, found)
	location, found, err := canonical.Lookup("old-row")
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, "old.parquet", location.FilePath)
}

func TestMissingDestinationGenerationCannotDiscardReplacementIndex(t *testing.T) {
	index, err := openIndex(filepath.Join(t.TempDir(), "index"), StoreOptions{MemTableSize: 4 << 20, MaxOpenFiles: 128})
	require.NoError(t, err)
	defer func() { require.NoError(t, index.Close()) }()
	require.NoError(t, ReconcileGeneration(index, "completed-resync"))
	seedGenerationIndex(t, index, "live-row", "new.parquet", 42)
	require.ErrorContains(t, ReconcileGeneration(index, ""), "omitted the durable resync generation")
	location, found, err := index.Lookup("live-row")
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, "new.parquet", location.FilePath)
}
