package indexdb

import (
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/cockroachdb/pebble/v2"
	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/types"
	"github.com/stretchr/testify/require"
)

const promotionTestUUID = "2bc4db9a-11d0-46cc-a310-0276b83e038e"

func promotionFixture(t *testing.T) (types.StreamInterface, *attemptIndex) {
	t.Helper()
	t.Setenv(constants.IndexDBDir, t.TempDir())
	stream := types.NewStream("events", "public", nil).Wrap(0)
	canonical, err := Open(stream)
	require.NoError(t, err)
	seedGenerationIndex(t, canonical, "old", "old.parquet", 11)
	require.NoError(t, canonical.Close())
	index, err := OpenAttempt(stream)
	require.NoError(t, err)
	attempt := index.(*attemptIndex)
	require.NoError(t, ReconcileGeneration(attempt, "operation"))
	seedGenerationIndex(t, attempt, "new", "new.parquet", 22)
	t.Cleanup(func() { require.NoError(t, attempt.Close()) })
	return stream, attempt
}

func requirePromotedIndexRow(t *testing.T, index types.StreamIndex, key, path string, snapshot int64) {
	t.Helper()
	location, found, err := index.Lookup(key)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, path, location.FilePath)
	checkpoint, err := index.LastCommittedSnapshot()
	require.NoError(t, err)
	require.Equal(t, snapshot, checkpoint)
}

func sealTestAttempt(t *testing.T, attempt *attemptIndex) {
	t.Helper()
	batch := attempt.db.NewBatch()
	require.NoError(t, batch.Set(metaKey(metaPromotionUUID), []byte(promotionTestUUID), nil))
	require.NoError(t, batch.Set(metaKey(metaPromotionSnapshot), binary.AppendVarint(nil, 22), nil))
	require.NoError(t, batch.Commit(pebble.Sync))
	require.NoError(t, batch.Close())
}

func stopTestAttempt(t *testing.T, attempt *attemptIndex) {
	t.Helper()
	attempt.closed = true
	require.NoError(t, attempt.pebbleIndex.Close())
	require.NoError(t, syncDirectory(attempt.dir))
}

func TestPromoteAttemptPersistsVerifiedIndex(t *testing.T) {
	stream, attempt := promotionFixture(t)
	require.NoError(t, PromoteAttempt(attempt, "operation", promotionTestUUID, 22))
	require.NoError(t, attempt.Close())
	canonical, err := Open(stream)
	require.NoError(t, err)
	defer func() { require.NoError(t, canonical.Close()) }()
	require.NoError(t, ReconcileTable(canonical, "operation", promotionTestUUID))
	requirePromotedIndexRow(t, canonical, "new", "new.parquet", 22)
	_, found, err := canonical.Lookup("old")
	require.NoError(t, err)
	require.False(t, found)
	_, err = os.Stat(promotionBackup(attempt.canonical))
	require.ErrorIs(t, err, os.ErrNotExist)
}

func TestPromoteAttemptRejectsIncompleteOrWrongIdentity(t *testing.T) {
	stream, attempt := promotionFixture(t)
	for _, test := range []struct {
		generation string
		uuid       string
		snapshot   int64
	}{
		{"other-operation", promotionTestUUID, 22},
		{"operation", "", 22},
		{"operation", "00000000-0000-0000-0000-000000000000", 22},
		{"operation", promotionTestUUID, 0},
		{"operation", promotionTestUUID, 23},
	} {
		err := PromoteAttempt(attempt, test.generation, test.uuid, test.snapshot)
		require.ErrorIs(t, err, ErrUnpromotable)
		requirePromotedIndexRow(t, attempt, "new", "new.parquet", 22)
	}
	canonical, err := Open(stream)
	require.NoError(t, err)
	defer func() { require.NoError(t, canonical.Close()) }()
	requirePromotedIndexRow(t, canonical, "old", "old.parquet", 11)
}

func TestPromoteAttemptRequiresExclusiveCanonicalOwnership(t *testing.T) {
	stream, attempt := promotionFixture(t)
	canonical, err := Open(stream)
	require.NoError(t, err)
	require.Error(t, PromoteAttempt(attempt, "operation", promotionTestUUID, 22))
	requirePromotedIndexRow(t, canonical, "old", "old.parquet", 11)
	require.NoError(t, canonical.Close())
	require.NoError(t, PromoteAttempt(attempt, "operation", promotionTestUUID, 22))
}

func TestPromotionChecksPebbleLockOutsideCanonicalProtocol(t *testing.T) {
	_, attempt := promotionFixture(t)
	canonical, err := openIndex(attempt.canonical, DefaultOptions())
	require.NoError(t, err)
	require.Error(t, PromoteAttempt(attempt, "operation", promotionTestUUID, 22))
	requirePromotedIndexRow(t, canonical, "old", "old.parquet", 11)
	require.NoError(t, canonical.Close())
	require.NoError(t, PromoteAttempt(attempt, "operation", promotionTestUUID, 22))
}

func TestPromotionRecoveryRestoresOldIndexBeforePublication(t *testing.T) {
	stream, attempt := promotionFixture(t)
	stopTestAttempt(t, attempt)
	require.NoError(t, os.Rename(attempt.canonical, promotionBackup(attempt.canonical)))
	require.NoError(t, syncDirectory(filepath.Dir(attempt.canonical)))
	canonical, err := Open(stream)
	require.NoError(t, err)
	defer func() { require.NoError(t, canonical.Close()) }()
	requirePromotedIndexRow(t, canonical, "old", "old.parquet", 11)
	_, err = os.Stat(attempt.dir)
	require.NoError(t, err)
}

func TestPromotionRecoveryRejectsUnsealedCandidate(t *testing.T) {
	stream, attempt := promotionFixture(t)
	stopTestAttempt(t, attempt)
	require.NoError(t, os.Rename(attempt.canonical, promotionBackup(attempt.canonical)))
	require.NoError(t, os.Rename(attempt.dir, attempt.canonical))
	require.NoError(t, syncDirectory(filepath.Dir(attempt.canonical)))
	canonical, err := Open(stream)
	require.NoError(t, err)
	defer func() { require.NoError(t, canonical.Close()) }()
	requirePromotedIndexRow(t, canonical, "old", "old.parquet", 11)
	_, found, err := canonical.Lookup("new")
	require.NoError(t, err)
	require.False(t, found)
}

func TestPromotionRecoveryRejectsMismatchedSealedCheckpoint(t *testing.T) {
	stream, attempt := promotionFixture(t)
	sealTestAttempt(t, attempt)
	require.NoError(t, attempt.db.Set(metaKey(metaPromotionSnapshot), binary.AppendVarint(nil, 23), pebble.Sync))
	stopTestAttempt(t, attempt)
	require.NoError(t, os.Rename(attempt.canonical, promotionBackup(attempt.canonical)))
	require.NoError(t, os.Rename(attempt.dir, attempt.canonical))
	canonical, err := Open(stream)
	require.NoError(t, err)
	defer func() { require.NoError(t, canonical.Close()) }()
	requirePromotedIndexRow(t, canonical, "old", "old.parquet", 11)
}

func TestPromotionRecoveryQuarantinesCorruptCandidate(t *testing.T) {
	stream, attempt := promotionFixture(t)
	sealTestAttempt(t, attempt)
	stopTestAttempt(t, attempt)
	files, err := os.ReadDir(attempt.dir)
	require.NoError(t, err)
	removed := false
	for _, file := range files {
		if strings.HasPrefix(file.Name(), "MANIFEST-") {
			require.NoError(t, os.Remove(filepath.Join(attempt.dir, file.Name())))
			removed = true
		}
	}
	require.True(t, removed)
	require.NoError(t, os.Rename(attempt.canonical, promotionBackup(attempt.canonical)))
	require.NoError(t, os.Rename(attempt.dir, attempt.canonical))
	canonical, err := Open(stream)
	require.NoError(t, err)
	defer func() { require.NoError(t, canonical.Close()) }()
	requirePromotedIndexRow(t, canonical, "old", "old.parquet", 11)
}

func TestPromotionRecoveryAcceptsSealedCandidateAndRemovesBackup(t *testing.T) {
	stream, attempt := promotionFixture(t)
	sealTestAttempt(t, attempt)
	stopTestAttempt(t, attempt)
	require.NoError(t, os.Rename(attempt.canonical, promotionBackup(attempt.canonical)))
	require.NoError(t, syncDirectory(filepath.Dir(attempt.canonical)))
	require.NoError(t, os.Rename(attempt.dir, attempt.canonical))
	canonical, err := Open(stream)
	require.NoError(t, err)
	defer func() { require.NoError(t, canonical.Close()) }()
	require.NoError(t, ReconcileTable(canonical, "operation", promotionTestUUID))
	requirePromotedIndexRow(t, canonical, "new", "new.parquet", 22)
	_, err = os.Stat(promotionBackup(attempt.canonical))
	require.ErrorIs(t, err, os.ErrNotExist)
}

func TestPromotionRecoveryPreservesLockedBackup(t *testing.T) {
	stream, attempt := promotionFixture(t)
	stopTestAttempt(t, attempt)
	require.NoError(t, os.Rename(attempt.canonical, promotionBackup(attempt.canonical)))
	locked, err := lockIndexDirectory(promotionBackup(attempt.canonical))
	require.NoError(t, err)
	_, err = Open(stream)
	require.Error(t, err)
	require.NoError(t, locked.Close())
	canonical, err := Open(stream)
	require.NoError(t, err)
	defer func() { require.NoError(t, canonical.Close()) }()
	requirePromotedIndexRow(t, canonical, "old", "old.parquet", 11)
}

func TestPromotedIndexRejectsChangedTableIdentity(t *testing.T) {
	stream, attempt := promotionFixture(t)
	require.NoError(t, PromoteAttempt(attempt, "operation", promotionTestUUID, 22))
	canonical, err := Open(stream)
	require.NoError(t, err)
	defer func() { require.NoError(t, canonical.Close()) }()
	require.NoError(t, ReconcileTable(canonical, "operation", "a9402dc9-25c8-4af9-b72e-ef44109bef61"))
	_, found, err := canonical.Lookup("new")
	require.NoError(t, err)
	require.False(t, found)
	checkpoint, err := canonical.LastCommittedSnapshot()
	require.NoError(t, err)
	require.Zero(t, checkpoint)
}

func TestPromoteAttemptRejectsNonAttempt(t *testing.T) {
	stream, _ := promotionFixture(t)
	canonical, err := Open(stream)
	require.NoError(t, err)
	defer func() { require.NoError(t, canonical.Close()) }()
	require.True(t, errors.Is(PromoteAttempt(canonical, "operation", promotionTestUUID, 22), ErrUnpromotable))
}
