package iceberg

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"sync"
	"testing"

	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/catalog/hadoop"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/destination"
	"github.com/datazip-inc/olake/pkg/indexdb"
	"github.com/datazip-inc/olake/types"
	"github.com/stretchr/testify/require"
)

type promotionTestAdapter struct{ *Iceberg }

func (*promotionTestAdapter) Check(context.Context) error { return nil }

type promotionCountingIO struct {
	iceio.WriteFileIO
	mu    sync.Mutex
	opens map[string]int
}

func (fs *promotionCountingIO) Open(path string) (iceio.File, error) {
	fs.mu.Lock()
	fs.opens[path]++
	fs.mu.Unlock()
	return fs.WriteFileIO.Open(path)
}

func (fs *promotionCountingIO) dataOpens(paths []string) int {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	total := 0
	for _, path := range paths {
		total += fs.opens[path]
	}
	return total
}

func promotionTestPool(t *testing.T, ctx context.Context, cfg *Config, stream *types.ConfiguredStream, cat catalog.Catalog) *destination.WriterPool {
	t.Helper()
	previous := destination.RegisteredWriters[types.Iceberg]
	destination.RegisteredWriters[types.Iceberg] = func(context.Context, any) (destination.Writer, func(context.Context), error) {
		return &promotionTestAdapter{Iceberg: &Iceberg{config: cfg, catalog: cat}}, nil, nil
	}
	t.Cleanup(func() { destination.RegisteredWriters[types.Iceberg] = previous })
	pool, err := destination.NewWriterPool(ctx, &types.WriterConfig{Type: types.Iceberg}, []types.StreamInterface{stream}, 100)
	require.NoError(t, err)
	return pool
}

func promotionTestWrite(t *testing.T, ctx context.Context, pool *destination.WriterPool, stream *types.ConfiguredStream, thread string, backfill bool, records ...types.RawRecord) {
	t.Helper()
	writer, _, err := pool.NewWriter(ctx, stream, destination.WithThreadID(thread), destination.WithBackfill(backfill))
	require.NoError(t, err)
	for _, record := range records {
		require.NoError(t, writer.Push(ctx, record, 0))
	}
	require.NoError(t, writer.Close(ctx, nil))
}

func promotionTestSeed(t *testing.T, ctx context.Context, cfg *Config, stream *types.ConfiguredStream, cat catalog.Catalog) *table.Table {
	t.Helper()
	index, err := indexdb.Open(stream)
	require.NoError(t, err)
	writer := openResyncTestWriter(t, ctx, cfg, stream, cat, index, "old", true)
	require.NoError(t, writer.Write(ctx, []types.RawRecord{nativeTestRecord("old", "r", 1, "one")}))
	require.NoError(t, writer.Close(ctx, nil))
	require.NoError(t, index.Close())
	return writer.table
}

func promotionTestDataPaths(t *testing.T, ctx context.Context, tbl *table.Table) []string {
	t.Helper()
	files, err := planNativeIndexFiles(ctx, tbl, nativeSnapshotID(tbl))
	require.NoError(t, err)
	paths := make([]string, 0, len(files))
	for _, file := range files {
		paths = append(paths, file.File.FilePath())
	}
	return paths
}

func promotionTestIndexRows(t *testing.T, index types.StreamIndex, tbl *table.Table, keys ...string) {
	t.Helper()
	checkpoint, err := index.LastCommittedSnapshot()
	require.NoError(t, err)
	require.Equal(t, nativeSnapshotID(tbl), checkpoint)
	for _, key := range keys {
		location, found, err := index.Lookup(key)
		require.NoError(t, err)
		require.True(t, found, key)
		rows, err := readNativeRows(context.Background(), tbl, location.FilePath, []int64{location.Position}, []string{constants.OlakeID})
		require.NoError(t, err)
		require.Equal(t, key, rows[location.Position][constants.OlakeID])
	}
}

func TestNativeResyncPromotesCompleteIndexForNextSync(t *testing.T) {
	for _, mode := range []types.UpdateType{types.UpdateTypePosition, types.UpdateTypeDeletionVector} {
		t.Run(string(mode), func(t *testing.T) {
			t.Setenv(constants.IndexDBDir, t.TempDir())
			ctx := resyncTestContext(t)
			cfg, stream, cat := nativeWriterFixture(t, mode, true, "")
			original := promotionTestSeed(t, ctx, cfg, stream, cat)
			pool := promotionTestPool(t, ctx, cfg, stream, cat)
			recovered, err := pool.PrepareResync(ctx, stream, "operation-1", resyncTestState())
			require.NoError(t, err)
			require.Nil(t, recovered)
			promotionTestWrite(t, ctx, pool, stream, "backfill", true, nativeTestRecord("a", "r", 2, "one"), nativeTestRecord("b", "r", 3, "one"), nativeTestRecord("c", "r", 4, "one"))
			promotionTestWrite(t, ctx, pool, stream, "cdc", false, nativeTestRecord("a", "u", 5, "one"), nativeTestRecord("b", "d", 6, "one"))
			visible, err := cat.LoadTable(ctx, original.Identifier())
			require.NoError(t, err)
			require.Equal(t, map[string]int64{"old": 1}, nativeVisibleValues(t, visible))
			require.NoError(t, pool.PublishResync(ctx, resyncTestState()))
			published, err := cat.LoadTable(ctx, original.Identifier())
			require.NoError(t, err)
			require.Equal(t, original.Metadata().TableUUID(), published.Metadata().TableUUID())
			require.Equal(t, map[string]int64{"a": 5, "b": 6, "c": 4}, nativeVisibleValues(t, published))
			uuid, snapshot := pool.ResyncReceipt()
			require.Equal(t, published.Metadata().TableUUID().String(), uuid)
			require.Equal(t, nativeSnapshotID(published), snapshot)
			require.NoError(t, pool.FinishResync(ctx))

			canonical, err := indexdb.Open(stream)
			require.NoError(t, err)
			promotionTestIndexRows(t, canonical, published, "a", "b", "c")
			_, found, err := canonical.Lookup("old")
			require.NoError(t, err)
			require.False(t, found)
			require.NoError(t, canonical.Close())

			paths := promotionTestDataPaths(t, ctx, published)
			require.NotEmpty(t, paths)
			local, err := published.FS(ctx)
			require.NoError(t, err)
			writable, ok := local.(iceio.WriteFileIO)
			require.True(t, ok)
			counter := &promotionCountingIO{WriteFileIO: writable, opens: make(map[string]int)}
			scoped := &nativeCatalog{Catalog: &nativeCommitFaultCatalog{Catalog: cat.(*nativeCatalog).Catalog, fs: counter}}
			next := promotionTestPool(t, context.Background(), cfg, stream, scoped)
			writer, _, err := next.NewWriter(context.Background(), stream, destination.WithThreadID("next"))
			require.NoError(t, err)
			require.Zero(t, counter.dataOpens(paths), "opening the next sync must not scan replacement data files")
			require.NoError(t, writer.Push(context.Background(), nativeTestRecord("a", "u", 7, "one"), 0))
			require.NoError(t, writer.Push(context.Background(), nativeTestRecord("b", "u", 8, "one"), 0))
			require.NoError(t, writer.Close(context.Background(), nil))
			next.Shutdown(context.Background())
			updated, err := scoped.LoadTable(context.Background(), original.Identifier())
			require.NoError(t, err)
			require.Equal(t, map[string]int64{"a": 7, "b": 8, "c": 4}, nativeVisibleValues(t, updated))
			index, err := indexdb.Open(stream)
			require.NoError(t, err)
			promotionTestIndexRows(t, index, updated, "a", "b", "c")
			require.NoError(t, index.Close())
		})
	}
}

func TestNativeResyncIndexFallbackForUnpromotableCandidates(t *testing.T) {
	for _, scenario := range []string{"missing", "stale", "crash", "cancel-finish", "index-missing", "index-stale"} {
		t.Run(scenario, func(t *testing.T) {
			t.Setenv(constants.IndexDBDir, t.TempDir())
			ctx, cancel := context.WithCancel(resyncTestContext(t))
			defer cancel()
			cfg, stream, cat := nativeWriterFixture(t, types.UpdateTypePosition, true, "")
			original := promotionTestSeed(t, ctx, cfg, stream, cat)
			oldIndex, err := indexdb.Open(stream)
			require.NoError(t, err)
			oldLocation, found, err := oldIndex.Lookup("old")
			require.NoError(t, err)
			require.True(t, found)
			require.NoError(t, oldIndex.Close())
			pool := promotionTestPool(t, ctx, cfg, stream, cat)
			_, err = pool.PrepareResync(ctx, stream, "operation-1", resyncTestState())
			require.NoError(t, err)
			if scenario != "missing" {
				promotionTestWrite(t, ctx, pool, stream, "backfill", true, nativeTestRecord("new", "r", 2, "one"))
			}
			if scenario == "stale" {
				extra := openNativeTestWriter(ctx, t, cfg, stream, cat, nil, "unindexed-stage", true)
				require.NoError(t, extra.Write(ctx, []types.RawRecord{nativeTestRecord("extra", "r", 3, "one")}))
				require.NoError(t, extra.Close(ctx, nil))
			}
			require.NoError(t, pool.PublishResync(ctx, resyncTestState()))
			published, err := cat.LoadTable(ctx, original.Identifier())
			require.NoError(t, err)
			if scenario == "crash" {
				pool.Shutdown(ctx)
			} else {
				if scenario == "cancel-finish" {
					cancel()
				}
				require.NoError(t, pool.FinishResync(context.Background()))
			}
			if scenario == "index-missing" || scenario == "index-stale" {
				require.NoError(t, indexdb.Drop(stream))
				if scenario == "index-stale" {
					stale, err := indexdb.Open(stream)
					require.NoError(t, err)
					batch := types.NewStreamIndexThread(stale)
					batch.Put("old", oldLocation)
					oldSnapshot := nativeSnapshotID(original)
					require.NoError(t, stale.Commit(batch, &oldSnapshot))
					require.NoError(t, stale.Close())
				}
			}
			canonical, err := indexdb.Open(stream)
			require.NoError(t, err)
			checkpoint, err := canonical.LastCommittedSnapshot()
			require.NoError(t, err)
			require.NotEqual(t, nativeSnapshotID(published), checkpoint)
			require.NoError(t, canonical.Close())

			local, err := published.FS(context.Background())
			require.NoError(t, err)
			writable, ok := local.(iceio.WriteFileIO)
			require.True(t, ok)
			counter := &promotionCountingIO{WriteFileIO: writable, opens: make(map[string]int)}
			fresh := &nativeCatalog{Catalog: &nativeCommitFaultCatalog{Catalog: cat.(*nativeCatalog).Catalog, fs: counter}}
			next := promotionTestPool(t, context.Background(), cfg, stream, fresh)
			writer, _, err := next.NewWriter(context.Background(), stream, destination.WithThreadID("rebuild"))
			require.NoError(t, err)
			if scenario != "missing" {
				require.Positive(t, counter.dataOpens(promotionTestDataPaths(t, context.Background(), published)), "unusable index must scan replacement data")
			}
			require.NoError(t, writer.Push(context.Background(), nativeTestRecord("after", "u", 9, "one"), 0))
			require.NoError(t, writer.Close(context.Background(), nil))
			next.Shutdown(context.Background())
			updated, err := fresh.LoadTable(context.Background(), original.Identifier())
			require.NoError(t, err)
			expected := map[string]int64{"after": 9}
			if scenario != "missing" {
				expected["new"] = 2
			}
			if scenario == "stale" {
				expected["extra"] = 3
			}
			require.Equal(t, expected, nativeVisibleValues(t, updated))
			index, err := indexdb.Open(stream)
			require.NoError(t, err)
			keys := []string{"after"}
			if scenario != "missing" {
				keys = append(keys, "new")
			}
			if scenario == "stale" {
				keys = append(keys, "extra")
			}
			promotionTestIndexRows(t, index, updated, keys...)
			_, found, err = index.Lookup("old")
			require.NoError(t, err)
			require.False(t, found)
			require.NoError(t, index.Close())
		})
	}
}

func TestNativeResyncAbortAndUnknownNeverPromote(t *testing.T) {
	for _, scenario := range []string{"abort", "cancelled", "unknown"} {
		t.Run(scenario, func(t *testing.T) {
			t.Setenv(constants.IndexDBDir, t.TempDir())
			ctx := resyncTestContext(t)
			cfg, stream, base := nativeWriterFixture(t, types.UpdateTypePosition, true, "")
			original := promotionTestSeed(t, ctx, cfg, stream, base)
			fault := &nativeCommitFaultCatalog{Catalog: base.(*nativeCatalog).Catalog}
			cat := &nativeCatalog{Catalog: fault}
			pool := promotionTestPool(t, ctx, cfg, stream, cat)
			_, err := pool.PrepareResync(ctx, stream, "operation-1", resyncTestState())
			require.NoError(t, err)
			promotionTestWrite(t, ctx, pool, stream, "backfill", true, nativeTestRecord("new", "r", 2, "one"))
			if scenario == "abort" {
				require.NoError(t, pool.AbortResync(ctx))
			} else if scenario == "cancelled" {
				cancelled, cancel := context.WithCancel(ctx)
				cancel()
				require.Error(t, pool.PublishResync(cancelled, resyncTestState()))
				require.NoError(t, pool.AbortResync(ctx))
			} else {
				fault.fault = func(context.Context, table.Identifier, []table.Requirement, []table.Update) (bool, error) {
					return true, errors.New("unknown publication outcome")
				}
				require.ErrorContains(t, pool.PublishResync(ctx, resyncTestState()), "publication is unconfirmed")
				require.ErrorContains(t, pool.AbortResync(ctx), "unknown")
			}
			require.False(t, pool.ResyncPublished())
			require.NoError(t, pool.FinishResync(ctx))
			index, err := indexdb.Open(stream)
			require.NoError(t, err)
			promotionTestIndexRows(t, index, original, "old")
			_, found, err := index.Lookup("new")
			require.NoError(t, err)
			require.False(t, found)
			require.NoError(t, index.Close())
			visible, err := base.LoadTable(ctx, original.Identifier())
			require.NoError(t, err)
			require.Equal(t, map[string]int64{"old": 1}, nativeVisibleValues(t, visible))
		})
	}
}

func promotionCrashFixture(t *testing.T, root string) (*Config, *types.ConfiguredStream, catalog.Catalog) {
	t.Helper()
	cat, err := hadoop.NewCatalog("promotion-crash-test", root, nil)
	require.NoError(t, err)
	stream := types.NewStream("records", "db", nil)
	stream.DestinationDatabase, stream.DestinationTable = "db", "records"
	stream.UpsertField(constants.OlakeID, types.String, false, true)
	stream.UpsertField(constants.OpType, types.String, false, true)
	stream.UpsertField(constants.OlakeTimestamp, types.TimestampMicro, false, true)
	stream.UpsertField("value", types.Int64, true, false)
	stream.UpsertField("part", types.String, true, false)
	stream.UpsertField("blob", types.FixedBinaryOf(4), true, false)
	normalized, mode := true, string(types.UpdateTypePosition)
	configured := &types.ConfiguredStream{Stream: stream, StreamMetadata: types.StreamMetadata{Normalization: &normalized, UpdateType: &mode, UseSourceColumnNames: true}}
	return &Config{IcebergS3Path: root, IcebergDatabase: "db"}, configured, &nativeCatalog{Catalog: cat}
}

func TestNativeResyncCrashAfterPublicationBeforePromotion(t *testing.T) {
	const childRoot = "OLAKE_PROMOTION_CRASH_TEST_ROOT"
	if root := os.Getenv(childRoot); root != "" {
		ctx := resyncTestContext(t)
		cfg, stream, cat := promotionCrashFixture(t, root)
		promotionTestSeed(t, ctx, cfg, stream, cat)
		pool := promotionTestPool(t, ctx, cfg, stream, cat)
		_, err := pool.PrepareResync(ctx, stream, "operation-1", resyncTestState())
		require.NoError(t, err)
		promotionTestWrite(t, ctx, pool, stream, "replacement", true, nativeTestRecord("new", "r", 2, "one"))
		require.NoError(t, pool.PublishResync(ctx, resyncTestState()))
		os.Exit(0)
	}
	root := t.TempDir()
	indexRoot := t.TempDir()
	t.Setenv(constants.IndexDBDir, indexRoot)
	executable, err := os.Executable()
	require.NoError(t, err)
	child := exec.Command(executable, "-test.run=^TestNativeResyncCrashAfterPublicationBeforePromotion$")
	child.Env = append(os.Environ(), childRoot+"="+root)
	output, err := child.CombinedOutput()
	require.NoError(t, err, string(output))
	ctx := context.Background()
	cfg, stream, cat := promotionCrashFixture(t, root)
	published, err := cat.LoadTable(ctx, table.Identifier{"db", "records"})
	require.NoError(t, err)
	require.Equal(t, "operation-1", published.Properties()[resyncGenerationProperty])
	require.Equal(t, map[string]int64{"new": 2}, nativeVisibleValues(t, published))
	canonical, err := indexdb.Open(stream)
	require.NoError(t, err)
	checkpoint, err := canonical.LastCommittedSnapshot()
	require.NoError(t, err)
	require.NotEqual(t, nativeSnapshotID(published), checkpoint)
	require.NoError(t, canonical.Close())
	pool := promotionTestPool(t, ctx, cfg, stream, cat)
	promotionTestWrite(t, ctx, pool, stream, "after-crash", false, nativeTestRecord("new", "u", 3, "one"))
	pool.Shutdown(ctx)
	updated, err := cat.LoadTable(ctx, published.Identifier())
	require.NoError(t, err)
	require.Equal(t, map[string]int64{"new": 3}, nativeVisibleValues(t, updated))
	index, err := indexdb.Open(stream)
	require.NoError(t, err)
	promotionTestIndexRows(t, index, updated, "new")
	_, found, err := index.Lookup("old")
	require.NoError(t, err)
	require.False(t, found)
	require.NoError(t, index.Close())
}
