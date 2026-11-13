package iceberg

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"testing"
	"time"

	apacheiceberg "github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/table"
	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/destination"
	"github.com/datazip-inc/olake/pkg/indexdb"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
	"github.com/stretchr/testify/require"
)

func resyncTestState() *types.State {
	return &types.State{RWMutex: &sync.RWMutex{}, Type: types.GlobalType, Global: &types.GlobalState{State: map[string]any{"lsn": "0/ABC", "large": json.Number("9007199254740993")}, Streams: types.NewSet[string]("db.records", "db.unrelated")}}
}

func resyncTestContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
	t.Cleanup(cancel)
	return types.WithResync(ctx, &types.ResyncOptions{ID: "operation-1", LaunchID: "launch-1"})
}

func openResyncTestWriter(t *testing.T, ctx context.Context, cfg *Config, stream *types.ConfiguredStream, cat catalog.Catalog, index types.StreamIndex, thread string, backfill bool) *nativeWriter {
	t.Helper()
	options := &destination.Options{ThreadID: thread, Backfill: backfill, TableIndex: index}
	writer, _, err := newNativeWriter(ctx, cfg, stream, options, cat, types.NewStreamIndexThread(index))
	require.NoError(t, err)
	return writer
}

func resyncTestIndex(t *testing.T, stream types.StreamInterface) types.StreamIndex {
	t.Helper()
	index, err := indexdb.OpenAttempt(stream)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, index.Close()) })
	return index
}

func TestNativeAtomicResyncPreservesIdentityHistoryDeletesAndV3RowIDs(t *testing.T) {
	for _, mode := range []types.UpdateType{types.UpdateTypeEquality, types.UpdateTypePosition, types.UpdateTypeDeletionVector} {
		t.Run(string(mode), func(t *testing.T) {
			t.Setenv(constants.IndexDBDir, t.TempDir())
			ctx := resyncTestContext(t)
			cfg, stream, cat := nativeWriterFixture(t, mode, true, "/{part, identity}")
			seed := openResyncTestWriter(t, ctx, cfg, stream, cat, nil, "original", true)
			require.NoError(t, seed.Write(ctx, []types.RawRecord{nativeTestRecord("old", "r", 1, "old")}))
			require.NoError(t, seed.Close(ctx, map[string]any{"state": `{"original":1}`}))
			var originalIndex types.StreamIndex
			if mode != types.UpdateTypeEquality {
				originalIndex = resyncTestIndex(t, stream)
			}
			originalCDC := openResyncTestWriter(t, ctx, cfg, stream, cat, originalIndex, "original-cdc", false)
			require.NoError(t, originalCDC.Write(ctx, []types.RawRecord{nativeTestRecord("old", "u", 1, "old")}))
			require.NoError(t, originalCDC.Close(ctx, map[string]any{"state": `{"original-cdc":1}`}))
			original := originalCDC.table
			_, _, err := cat.CommitTable(ctx, original.Identifier(), nil, []table.Update{table.NewSetSnapshotRefUpdate("audit", nativeSnapshotID(original), table.TagRef, 3600000, 0, 0), table.NewSetStatisticsUpdate(table.StatisticsFile{SnapshotID: nativeSnapshotID(original), StatisticsPath: original.Location() + "/historical-statistics.puffin", FileSizeInBytes: 16, FileFooterSizeInBytes: 8, BlobMetadata: []table.BlobMetadata{}})})
			require.NoError(t, err)
			original, err = cat.LoadTable(ctx, original.Identifier())
			require.NoError(t, err)
			adapter := &Iceberg{config: cfg, catalog: cat}
			recovered, err := adapter.PrepareResync(ctx, stream, "operation-1", resyncTestState())
			require.NoError(t, err)
			require.Nil(t, recovered)
			var index types.StreamIndex
			if mode != types.UpdateTypeEquality {
				index = resyncTestIndex(t, stream)
			}
			first := openResyncTestWriter(t, ctx, cfg, stream, cat, index, "replacement-backfill", true)
			require.NoError(t, first.Write(ctx, []types.RawRecord{nativeTestRecord("a", "r", 2, "one"), nativeTestRecord("b", "r", 3, "two")}))
			evolved := first.schemaMap()
			evolved["resync_added"] = "string"
			require.NoError(t, first.EvolveSchema(ctx, evolved))
			require.NoError(t, first.Close(ctx, map[string]any{"full_refresh_committed_ids": []string{"replacement-backfill"}}))
			cdc := openResyncTestWriter(t, ctx, cfg, stream, cat, index, "replacement-cdc", false)
			require.NoError(t, cdc.Write(ctx, []types.RawRecord{nativeTestRecord("a", "u", 4, "changed")}))
			require.NoError(t, cdc.Close(ctx, map[string]any{"state": `{"cdc":4}`}))
			staged := cdc.table
			require.Equal(t, map[string]int64{"a": 4, "b": 3}, nativeVisibleValues(t, staged))
			visible, err := cat.LoadTable(ctx, original.Identifier())
			require.NoError(t, err)
			require.Equal(t, map[string]int64{"old": 1}, nativeVisibleValues(t, visible), "private backfill and CDC must never leak")
			require.NoError(t, adapter.PublishResync(ctx, resyncTestState()))
			published, err := cat.LoadTable(ctx, original.Identifier())
			require.NoError(t, err)
			require.Equal(t, original.Identifier(), published.Identifier())
			require.Equal(t, original.Location(), published.Location())
			require.Equal(t, original.Metadata().TableUUID(), published.Metadata().TableUUID())
			require.Equal(t, map[string]int64{"a": 4, "b": 3}, nativeVisibleValues(t, published))
			require.Equal(t, table.OpOverwrite, published.CurrentSnapshot().Summary.Operation)
			require.Equal(t, nativeSnapshotID(original), *published.CurrentSnapshot().ParentSnapshotID)
			require.Len(t, published.Metadata().Snapshots(), len(original.Metadata().Snapshots())+1, "private intermediate snapshots are not public history")
			require.NotNil(t, published.SnapshotByID(nativeSnapshotID(original)))
			require.Equal(t, nativeSnapshotID(original), published.SnapshotByName("audit").SnapshotID)
			require.Equal(t, slices.Collect(original.Metadata().Statistics()), slices.Collect(published.Metadata().Statistics()))
			_, addedColumn := published.Schema().FindFieldByName("resync_added")
			require.True(t, addedColumn)
			for _, field := range original.Schema().Fields() {
				current, ok := published.Schema().FindFieldByName(field.Name)
				require.True(t, ok)
				require.Equal(t, field.ID, current.ID)
			}
			offsets, chunks := nativeRecoveryOffsets(t, published)
			require.Equal(t, map[string]int{"cdc": 4}, offsets)
			require.Contains(t, chunks, "replacement-backfill")
			require.Equal(t, "operation-1", published.Properties()[resyncGenerationProperty])
			require.Equal(t, "launch-1", published.Properties()[resyncLaunchProperty])
			require.NotEmpty(t, published.Properties()[resyncBaselineProperty])
			uuid, snapshot := adapter.ResyncReceipt()
			require.Equal(t, published.Metadata().TableUUID().String(), uuid)
			require.Equal(t, nativeSnapshotID(published), snapshot)
			assertResyncManifestLineage(t, ctx, original, staged, published)
			if mode == types.UpdateTypeDeletionVector {
				require.Equal(t, staged.Metadata().NextRowID(), published.Metadata().NextRowID())
				require.Equal(t, original.Metadata().NextRowID(), *published.CurrentSnapshot().FirstRowID)
				require.Equal(t, staged.Metadata().NextRowID()-original.Metadata().NextRowID(), *published.CurrentSnapshot().AddedRows)
			}
			// Ordinary CDC can resume in this runtime, and reconciles canonical index
			// generation before using a single old row location.
			if index != nil {
				canonical, err := indexdb.Open(stream)
				require.NoError(t, err)
				defer canonical.Close()
				next := openResyncTestWriter(t, ctx, cfg, stream, cat, canonical, "ordinary-after-publish", false)
				require.NoError(t, next.Write(ctx, []types.RawRecord{nativeTestRecord("a", "u", 5, "changed")}))
				require.NoError(t, next.Close(ctx, nil))
				require.Equal(t, map[string]int64{"a": 5, "b": 3}, nativeVisibleValues(t, next.table))
			}
		})
	}
}

func assertResyncManifestLineage(t *testing.T, ctx context.Context, original, staged, published *table.Table) {
	t.Helper()
	type lineage struct {
		sequence     int64
		fileSequence int64
		firstRowID   int64
		hasRowID     bool
		content      apacheiceberg.ManifestEntryContent
		offset       int64
		length       int64
	}
	collect := func(tbl *table.Table) map[string]lineage {
		fs, err := tbl.FS(ctx)
		require.NoError(t, err)
		manifests, err := tbl.CurrentSnapshot().Manifests(fs)
		require.NoError(t, err)
		result := map[string]lineage{}
		for _, manifest := range manifests {
			for entry, err := range manifest.Entries(fs, true) {
				require.NoError(t, err)
				file := entry.DataFile()
				value := lineage{sequence: entry.SequenceNum(), content: file.ContentType()}
				if sequence := entry.FileSequenceNum(); sequence != nil {
					value.fileSequence = *sequence
				}
				if row := file.FirstRowID(); row != nil {
					value.firstRowID, value.hasRowID = *row, true
				}
				if offset := file.ContentOffset(); offset != nil {
					value.offset = *offset
				}
				if length := file.ContentSizeInBytes(); length != nil {
					value.length = *length
				}
				result[file.FilePath()] = value
			}
		}
		return result
	}
	require.Equal(t, collect(staged), collect(published), "all live data/delete paths, sequence numbers, Puffin offsets and row IDs must survive")
	fs, err := published.FS(ctx)
	require.NoError(t, err)
	manifests, err := published.CurrentSnapshot().Manifests(fs)
	require.NoError(t, err)
	removed := map[string]bool{}
	for _, manifest := range manifests {
		for entry, err := range manifest.Entries(fs, false) {
			require.NoError(t, err)
			if entry.Status() == apacheiceberg.EntryStatusDELETED {
				removed[entry.DataFile().FilePath()] = true
			}
		}
	}
	for path := range collect(original) {
		require.True(t, removed[path], "original live entry must be explicitly DELETED")
	}
}

func TestNativeAtomicResyncRoutesOtherCDCOrdinarilyAndRequiresClosedSessions(t *testing.T) {
	ctx := resyncTestContext(t)
	cfg, stream, cat := nativeWriterFixture(t, types.UpdateTypeEquality, true, "")
	seed := openNativeTestWriter(ctx, t, cfg, stream, cat, nil, "seed", true)
	require.NoError(t, seed.Close(ctx, nil))
	adapter := &Iceberg{config: cfg, catalog: cat}
	_, err := adapter.PrepareResync(ctx, stream, "operation-1", resyncTestState())
	require.NoError(t, err)
	target := openNativeTestWriter(ctx, t, cfg, stream, cat, nil, "target", true)
	require.ErrorContains(t, adapter.PublishResync(ctx, resyncTestState()), "close every target writer")
	other := *stream
	otherStream := *stream.Stream
	other.Stream = &otherStream
	otherStream.DestinationTable = "other"
	ordinary := openNativeTestWriter(ctx, t, cfg, &other, cat, nil, "other-cdc", true)
	require.NoError(t, ordinary.Write(ctx, []types.RawRecord{nativeTestRecord("other", "r", 8, "one")}))
	require.NoError(t, ordinary.Close(ctx, nil))
	publicOther, err := cat.LoadTable(ctx, ordinary.table.Identifier())
	require.NoError(t, err)
	require.Equal(t, map[string]int64{"other": 8}, nativeVisibleValues(t, publicOther))
	require.NoError(t, target.Close(ctx, nil))
	require.NoError(t, adapter.PublishResync(ctx, resyncTestState()))
}

func TestNativeAtomicResyncEmptyReplacementAndDurableRecovery(t *testing.T) {
	ctx := resyncTestContext(t)
	cfg, stream, cat := nativeWriterFixture(t, types.UpdateTypeEquality, true, "")
	seed := openNativeTestWriter(ctx, t, cfg, stream, cat, nil, "seed", true)
	require.NoError(t, seed.Write(ctx, []types.RawRecord{nativeTestRecord("old", "r", 1, "old")}))
	require.NoError(t, seed.Close(ctx, nil))
	original := seed.table
	adapter := &Iceberg{config: cfg, catalog: cat}
	_, err := adapter.PrepareResync(ctx, stream, "operation-1", resyncTestState())
	require.NoError(t, err)
	require.NoError(t, adapter.PublishResync(ctx, resyncTestState()))
	published, err := cat.LoadTable(ctx, original.Identifier())
	require.NoError(t, err)
	require.Positive(t, nativeSnapshotID(published))
	require.Empty(t, nativeVisibleValues(t, published))
	freshCatalog := &nativeCatalog{Catalog: cat.(*nativeCatalog).Catalog}
	recovered := &Iceberg{config: cfg, catalog: freshCatalog}
	recoverCtx := types.WithResync(ctx, &types.ResyncOptions{ID: "operation-1", LaunchID: "launch-1", RecoverOnly: true})
	checkpoint, err := recovered.PrepareResync(recoverCtx, stream, "operation-1", resyncTestState())
	require.NoError(t, err)
	require.Nil(t, recovered.resync.stage, "recover-only must not allocate a stage")
	require.Equal(t, json.Number("9007199254740993"), checkpoint.Global.State.(map[string]any)["large"])
	require.True(t, checkpoint.Global.Streams.Exists("db.unrelated"))
	require.Error(t, recovered.PublishResync(recoverCtx, resyncTestState()))
	require.Error(t, recovered.AbortResync(recoverCtx))
	require.Error(t, recovered.PublishResync(ctx, resyncTestState()), "recover-only admission cannot be bypassed with a different context")
	normal := &Iceberg{config: cfg, catalog: freshCatalog}
	_, err = normal.PrepareResync(ctx, stream, "operation-1", resyncTestState())
	require.NoError(t, err)
	require.NoError(t, normal.PublishResync(ctx, resyncTestState()), "same final checkpoint is idempotent")
	changed := resyncTestState()
	changed.Global.State = map[string]any{"lsn": "different"}
	require.Error(t, normal.PublishResync(ctx, changed))
	stale := &Iceberg{config: cfg, catalog: &nativeCatalog{Catalog: cat.(*nativeCatalog).Catalog}}
	_, err = stale.PrepareResync(types.WithResync(ctx, &types.ResyncOptions{LaunchID: "other-launch", RecoverOnly: true}), stream, "operation-1", resyncTestState())
	require.ErrorContains(t, err, "different admitted launch")
}

func TestNativeAtomicResyncRecoveryOnlyAbsentMarkerNeverStages(t *testing.T) {
	ctx := resyncTestContext(t)
	cfg, stream, cat := nativeWriterFixture(t, types.UpdateTypeEquality, true, "")
	seed := openNativeTestWriter(ctx, t, cfg, stream, cat, nil, "seed", true)
	require.NoError(t, seed.Close(ctx, nil))
	adapter := &Iceberg{config: cfg, catalog: cat}
	recoverCtx := types.WithResync(ctx, &types.ResyncOptions{ID: "operation-1", RecoverOnly: true})
	_, err := adapter.PrepareResync(recoverCtx, stream, "operation-1", resyncTestState())
	require.ErrorContains(t, err, "UNKNOWN")
	require.Nil(t, adapter.resync.stage)
	require.Nil(t, nativeTableEntry(cat, seed.table.Identifier()).resync)
	require.Error(t, adapter.PublishResync(recoverCtx, resyncTestState()))
}

func TestNativeAtomicResyncConflictAbortDeadlineAndClosedFence(t *testing.T) {
	for _, failure := range []string{"concurrent-properties", "concurrent-data", "aborted", "deadline", "closed-fence", "failed-writer"} {
		t.Run(failure, func(t *testing.T) {
			ctx := resyncTestContext(t)
			cfg, stream, cat := nativeWriterFixture(t, types.UpdateTypeEquality, true, "")
			seed := openNativeTestWriter(ctx, t, cfg, stream, cat, nil, "seed", true)
			require.NoError(t, seed.Write(ctx, []types.RawRecord{nativeTestRecord("old", "r", 1, "old")}))
			require.NoError(t, seed.Close(ctx, nil))
			adapter := &Iceberg{config: cfg, catalog: cat}
			fence := filepath.Join(t.TempDir(), "fence.json")
			if failure == "closed-fence" {
				require.NoError(t, os.WriteFile(fence, []byte(`{"version":1,"execution_id":"runtime","state":"open"}`), 0600))
				ctx = utils.WithExecution(ctx, &utils.ExecutionOptions{ID: "runtime", Fence: fence})
			}
			_, err := adapter.PrepareResync(ctx, stream, "operation-1", resyncTestState())
			require.NoError(t, err)
			switch failure {
			case "concurrent-properties":
				_, _, err = cat.CommitTable(ctx, seed.table.Identifier(), nil, []table.Update{table.NewSetPropertiesUpdate(apacheiceberg.Properties{"external": "changed"})})
				require.NoError(t, err)
			case "concurrent-data":
				external := &nativeCatalog{Catalog: cat.(*nativeCatalog).Catalog}
				writer := openNativeTestWriter(ctx, t, cfg, stream, external, nil, "external", true)
				require.NoError(t, writer.Write(ctx, []types.RawRecord{nativeTestRecord("external", "r", 2, "one")}))
				require.NoError(t, writer.Close(ctx, nil))
			case "aborted":
				require.NoError(t, adapter.AbortResync(ctx))
			case "deadline":
				adapter.resync.deadline = time.Now().Add(-time.Second)
			case "closed-fence":
				require.NoError(t, os.WriteFile(fence, []byte(`{"version":1,"execution_id":"runtime","state":"closed"}`), 0600))
			case "failed-writer":
				writer := openNativeTestWriter(ctx, t, cfg, stream, cat, nil, "failed", true)
				writer.Abort()
			}
			require.Error(t, adapter.PublishResync(ctx, resyncTestState()))
			require.False(t, adapter.ResyncPublished())
			visible, err := cat.LoadTable(context.Background(), seed.table.Identifier())
			require.NoError(t, err)
			require.Empty(t, visible.Properties()[resyncIDProperty])
			require.Equal(t, int64(1), nativeVisibleValues(t, visible)["old"])
			require.NoError(t, adapter.AbortResync(context.Background()))
		})
	}
}

// Uses the real native REST catalog and on-disk Hadoop backend, not a fake
// publication response. This is the runtime smoke route used by local/CI tests.
func TestNativeRESTAtomicResyncSmokeAndLostPublishResponse(t *testing.T) {
	for _, outcome := range []string{"confirmed", "lost-response", "throttle-rejected"} {
		t.Run(outcome, func(t *testing.T) {
			loseResponse := outcome == "lost-response"
			ctx := resyncTestContext(t)
			cfg, stream, cat, boundary := nativeRESTWriterFixture(t, types.UpdateTypeEquality, nativeTestRecord("old", "r", 1, "one"))
			controlled, _ := nativeExecutionTestContext(t)
			execution, ok := utils.ExecutionFromContext(controlled)
			require.True(t, ok)
			ctx = utils.WithExecution(ctx, execution)
			// The shared fixture opens an ordinary uncontrolled client. Reopen it
			// under this admitted runtime so its real REST transport captures the
			// execution whose sticky uncertainty this smoke route asserts.
			require.NoError(t, cat.(*nativeCatalog).Close())
			cat, cleanup, err := openNativeCatalog(ctx, cfg)
			require.NoError(t, err)
			t.Cleanup(func() {
				cleanup(context.Background())
				utils.SourceDrained(ctx, nil)
				result, ok := utils.OrdinaryExecutionResult(ctx)
				require.True(t, ok)
				outcome := "settled"
				if loseResponse {
					outcome = "unknown"
				}
				require.Equal(t, outcome, result.Outcome)
			})
			adapter := &Iceberg{config: cfg, catalog: cat}
			_, err = adapter.PrepareResync(ctx, stream, "operation-1", resyncTestState())
			require.NoError(t, err)
			writer := openNativeTestWriter(ctx, t, cfg, stream, cat, nil, "replacement", true)
			require.NoError(t, writer.Write(ctx, []types.RawRecord{nativeTestRecord("new", "r", 9, "one")}))
			require.NoError(t, writer.Close(ctx, nil))
			require.NotContains(t, boundary.tableRequests(), http.MethodPost, "private writer commits must not send public REST POSTs")
			boundary.mu.Lock()
			boundary.failAfterCommit = loseResponse
			if outcome == "throttle-rejected" {
				boundary.throttleResponses = nativeRESTThrottleMaxAttempts
			}
			boundary.mu.Unlock()
			publishErr := adapter.PublishResync(ctx, resyncTestState())
			if outcome == "throttle-rejected" {
				var throttled *nativeRESTThrottleRejection
				require.ErrorAs(t, publishErr, &throttled)
				require.True(t, nativeMutationOutcomeKnown(publishErr))
				require.False(t, adapter.resync.unknown)
				require.False(t, adapter.ResyncPublished())
				require.NoError(t, nativeExecutionWritable(ctx, cfg))
				require.NoError(t, adapter.AbortResync(ctx), "positive rejection permits confirmed abort")
				visible, err := boundary.cat.LoadTable(ctx, table.Identifier{"db", "records"})
				require.NoError(t, err)
				require.Equal(t, map[string]int64{"old": 1}, nativeVisibleValues(t, visible))
				return
			}
			require.NoError(t, publishErr)
			methods := boundary.tableRequests()
			posts := 0
			for _, method := range methods {
				if method == http.MethodPost {
					posts++
				}
			}
			require.Equal(t, 1, posts, "atomic publication is one actual POST, including lost responses")
			published, err := boundary.cat.LoadTable(ctx, table.Identifier{"db", "records"})
			require.NoError(t, err)
			require.Equal(t, map[string]int64{"new": 9}, nativeVisibleValues(t, published))
			require.True(t, adapter.ResyncPublished())
			require.Error(t, adapter.AbortResync(ctx))
			if loseResponse {
				require.Error(t, nativeExecutionWritable(ctx, cfg), "uncertain transport remains sticky even after durable reconciliation")
			}
		})
	}
}

func TestNativeAtomicResyncPrepareRejectsInvalidInputsBeforeStaging(t *testing.T) {
	for _, invalid := range []string{"unbounded", "too-long", "cancelled", "blank-id", "missing-baseline"} {
		t.Run(invalid, func(t *testing.T) {
			ctx := context.Background()
			id := "operation-1"
			state := resyncTestState()
			var cancel context.CancelFunc
			switch invalid {
			case "too-long":
				ctx, cancel = context.WithTimeout(ctx, 13*time.Hour)
			case "cancelled":
				ctx, cancel = context.WithTimeout(ctx, time.Hour)
				cancel()
			case "blank-id":
				ctx, cancel = context.WithTimeout(ctx, time.Hour)
				id = " "
			case "missing-baseline":
				ctx, cancel = context.WithTimeout(ctx, time.Hour)
				state = nil
			}
			if cancel != nil {
				defer cancel()
			}
			_, err := (&Iceberg{config: &Config{}}).PrepareResync(ctx, types.NewStream("records", "db", nil).Wrap(0), id, state)
			require.Error(t, err)
		})
	}
}

func TestNativeAtomicResyncRecoveryBindsOriginalBaselineAndFinalCheckpoint(t *testing.T) {
	ctx := resyncTestContext(t)
	cfg, stream, cat := nativeWriterFixture(t, types.UpdateTypeEquality, true, "")
	seed := openNativeTestWriter(ctx, t, cfg, stream, cat, nil, "seed", true)
	require.NoError(t, seed.Close(ctx, nil))
	baseline, final := resyncTestState(), resyncTestState()
	final.Global.State.(map[string]any)["lsn"] = "0/FINAL"
	adapter := &Iceberg{config: cfg, catalog: cat}
	_, err := adapter.PrepareResync(ctx, stream, "operation-1", baseline)
	require.NoError(t, err)
	require.NoError(t, adapter.PublishResync(ctx, final))
	for _, saved := range []*types.State{baseline, final} {
		recovered := &Iceberg{config: cfg, catalog: &nativeCatalog{Catalog: cat.(*nativeCatalog).Catalog}}
		checkpoint, err := recovered.PrepareResync(ctx, stream, "operation-1", saved)
		require.NoError(t, err)
		require.Equal(t, "0/FINAL", checkpoint.Global.State.(map[string]any)["lsn"])
		require.True(t, recovered.ResyncPublished())
		require.Nil(t, recovered.resync.stage)
	}
	wrong := resyncTestState()
	wrong.Global.State.(map[string]any)["lsn"] = "0/STALE"
	stale := &Iceberg{config: cfg, catalog: &nativeCatalog{Catalog: cat.(*nativeCatalog).Catalog}}
	_, err = stale.PrepareResync(ctx, stream, "operation-1", wrong)
	require.ErrorContains(t, err, "differs from the original baseline and published checkpoint")
	require.False(t, stale.ResyncPublished())
}

func TestNativeAtomicResyncUnknownPublicationCannotAbortOrRemoveAttemptFiles(t *testing.T) {
	ctx := resyncTestContext(t)
	cfg, stream, base := nativeWriterFixture(t, types.UpdateTypeEquality, true, "")
	seed := openNativeTestWriter(ctx, t, cfg, stream, base, nil, "seed", true)
	require.NoError(t, seed.Write(ctx, []types.RawRecord{nativeTestRecord("old", "r", 1, "one")}))
	require.NoError(t, seed.Close(ctx, nil))
	fault := &nativeCommitFaultCatalog{Catalog: base.(*nativeCatalog).Catalog, fault: func(context.Context, table.Identifier, []table.Requirement, []table.Update) (bool, error) {
		return true, errors.New("injected unknown publication response")
	}}
	cat := &nativeCatalog{Catalog: fault}
	adapter := &Iceberg{config: cfg, catalog: cat}
	_, err := adapter.PrepareResync(ctx, stream, "operation-1", resyncTestState())
	require.NoError(t, err)
	replacement := openNativeTestWriter(ctx, t, cfg, stream, cat, nil, "replacement", true)
	require.NoError(t, replacement.Write(ctx, []types.RawRecord{nativeTestRecord("new", "r", 2, "one")}))
	require.NoError(t, replacement.Close(ctx, nil))
	require.ErrorContains(t, adapter.PublishResync(ctx, resyncTestState()), "publication is unconfirmed")
	require.ErrorContains(t, adapter.AbortResync(ctx), "unknown")
	require.False(t, adapter.ResyncPublished())
	require.Error(t, adapter.PublishResync(ctx, resyncTestState()))
	fs, err := replacement.table.FS(ctx)
	require.NoError(t, err)
	for _, file := range replacement.dataFiles {
		input, err := fs.Open(file.FilePath())
		require.NoError(t, err)
		require.NoError(t, input.Close())
		require.Error(t, fs.Remove(file.FilePath()), "uncertain publication must retain every referenced file")
	}
	visible, err := base.LoadTable(ctx, seed.table.Identifier())
	require.NoError(t, err)
	require.Equal(t, map[string]int64{"old": 1}, nativeVisibleValues(t, visible))
	require.Equal(t, 1, fault.calls)
}

func TestNativeAtomicResyncV3PublicationRetainsPrivateAllocationGaps(t *testing.T) {
	ctx := resyncTestContext(t)
	cfg, stream, cat := nativeWriterFixture(t, types.UpdateTypeDeletionVector, true, "")
	seed := openNativeTestWriter(ctx, t, cfg, stream, cat, nil, "seed", true)
	require.NoError(t, seed.Write(ctx, []types.RawRecord{nativeTestRecord("old", "r", 1, "one")}))
	require.NoError(t, seed.Close(ctx, nil))
	adapter := &Iceberg{config: cfg, catalog: cat}
	_, err := adapter.PrepareResync(ctx, stream, "operation-1", resyncTestState())
	require.NoError(t, err)
	transient := openNativeTestWriter(ctx, t, cfg, stream, cat, nil, "transient", true)
	require.NoError(t, transient.Write(ctx, []types.RawRecord{nativeTestRecord("transient", "r", 2, "one")}))
	require.NoError(t, transient.Close(ctx, nil))
	tx := transient.table.NewTransaction()
	require.NoError(t, tx.ReplaceFiles(ctx, transient.dataFiles, nil, nil, nil))
	_, err = tx.Commit(ctx)
	require.NoError(t, err)
	replacement := openNativeTestWriter(ctx, t, cfg, stream, cat, nil, "replacement", true)
	require.NoError(t, replacement.Write(ctx, []types.RawRecord{nativeTestRecord("new", "r", 3, "one")}))
	require.NoError(t, replacement.Close(ctx, nil))
	nextRowID := replacement.table.Metadata().NextRowID()
	require.Greater(t, nextRowID-seed.table.Metadata().NextRowID(), int64(1), "discarded private rows must consume a permanent ID gap")
	require.NoError(t, adapter.PublishResync(ctx, resyncTestState()))
	published, err := cat.LoadTable(ctx, seed.table.Identifier())
	require.NoError(t, err)
	require.Equal(t, nextRowID, published.Metadata().NextRowID())
	require.Equal(t, nextRowID-seed.table.Metadata().NextRowID(), *published.CurrentSnapshot().AddedRows)
	require.Equal(t, map[string]int64{"new": 3}, nativeVisibleValues(t, published))
	assertResyncManifestLineage(t, ctx, seed.table, replacement.table, published)
}
