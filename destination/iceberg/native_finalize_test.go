package iceberg

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/destination"
	"github.com/datazip-inc/olake/types"
	"github.com/stretchr/testify/require"
)

func nativeFinalizeAwait[T any](t *testing.T, result <-chan T, operation string) T {
	t.Helper()
	select {
	case value := <-result:
		return value
	case <-time.After(5 * time.Second):
		t.Fatalf("%s did not complete independently of table coordination and another file's I/O", operation)
		var zero T
		return zero
	}
}

func TestNativeBackfillPrepareCloseFinalizesWithoutPublishing(t *testing.T) {
	for _, mode := range []types.UpdateType{types.UpdateTypeEquality, types.UpdateTypePosition, types.UpdateTypeDeletionVector} {
		t.Run(string(mode), func(t *testing.T) {
			ctx := context.Background()
			cfg, stream, cat := nativeWriterFixture(t, mode, true, "/{part, identity}")
			adapter := &Iceberg{config: cfg, catalog: cat}
			options := &destination.Options{ThreadID: "prepared-chunk", Backfill: true}
			var index *nativeMemoryIndex
			if mode != types.UpdateTypeEquality {
				index = &nativeMemoryIndex{rows: make(map[string]types.RowLocation)}
				options.TableIndex = index
			}
			_, _, err := adapter.Setup(ctx, stream, nil, options)
			require.NoError(t, err)
			writer := adapter.writer.(*nativeWriter)
			t.Cleanup(writer.Abort)
			before, err := cat.LoadTable(ctx, writer.table.Identifier())
			require.NoError(t, err)
			require.Nil(t, before.CurrentSnapshot())
			require.NoError(t, adapter.Write(ctx, []types.RawRecord{
				nativeTestRecord("a", "r", 1, "one"),
				nativeTestRecord("b", "r", 2, "two"),
			}))

			require.NoError(t, adapter.PrepareClose(ctx))
			require.NoError(t, adapter.PrepareClose(ctx), "preparation must be safe to repeat before Close")
			require.Empty(t, writer.backfillFiles)
			require.Len(t, writer.dataFiles, 2)
			prepared := make(map[string]int64)
			for _, file := range writer.dataFiles {
				require.EqualValues(t, 1, file.Count())
				rows, err := writer.ReadRows(ctx, file.FilePath(), []int64{0}, []string{constants.OlakeID, "value"})
				require.NoError(t, err, "preparation must leave readable, footer-complete Parquet files")
				id := rows[0][constants.OlakeID].(string)
				require.NotContains(t, prepared, id)
				prepared[id] = rows[0]["value"].(int64)
			}
			require.Equal(t, map[string]int64{"a": 1, "b": 2}, prepared)
			unpublished, err := cat.LoadTable(ctx, writer.table.Identifier())
			require.NoError(t, err)
			require.Equal(t, before.MetadataLocation(), unpublished.MetadataLocation())
			require.Nil(t, unpublished.CurrentSnapshot())
			require.Empty(t, nativeVisibleValues(t, unpublished))
			require.Equal(t, before.Properties()[nativeRecoveryProperty], unpublished.Properties()[nativeRecoveryProperty])
			if index != nil {
				require.Empty(t, index.rows)
				require.Zero(t, index.snapshot)
			}

			require.NoError(t, adapter.Close(ctx, map[string]any{"state": "prepared-state"}))
			published, err := cat.LoadTable(ctx, writer.table.Identifier())
			require.NoError(t, err)
			require.NotNil(t, published.CurrentSnapshot())
			require.Equal(t, prepared, nativeVisibleValues(t, published))
			var state types.MetadataState
			require.NoError(t, json.Unmarshal([]byte(published.Properties()[nativeRecoveryProperty]), &state))
			require.Equal(t, "prepared-chunk", state.ID)
			require.Contains(t, published.Properties()[nativeRecoveryProperty], "prepared-state")
			if index != nil {
				require.Len(t, index.rows, 2)
				require.Equal(t, nativeSnapshotID(published), index.snapshot)
			}
		})
	}
}

func TestNativeBackfillPrepareCloseFailureNeverPublishes(t *testing.T) {
	for _, failure := range []string{"upload-close", "canceled-before", "canceled-during-upload"} {
		t.Run(failure, func(t *testing.T) {
			ctx := context.Background()
			cfg, stream, cat := nativeWriterFixture(t, types.UpdateTypePosition, true, "")
			index := &nativeMemoryIndex{rows: make(map[string]types.RowLocation)}
			adapter := &Iceberg{config: cfg, catalog: cat}
			_, _, err := adapter.Setup(ctx, stream, nil, &destination.Options{ThreadID: "failed-preparation", Backfill: true, TableIndex: index})
			require.NoError(t, err)
			writer := adapter.writer.(*nativeWriter)
			t.Cleanup(writer.Abort)
			before, err := cat.LoadTable(ctx, writer.table.Identifier())
			require.NoError(t, err)
			fault := nativeBackfillInjectFS(t, writer)
			release := sync.OnceFunc(func() { close(fault.release) })
			t.Cleanup(release)
			require.NoError(t, adapter.Write(ctx, []types.RawRecord{nativeTestRecord("a", "r", 1, "p")}))
			location, found, err := writer.indexThread.Lookup("a")
			require.NoError(t, err)
			require.True(t, found)
			workCtx, cancel := context.WithCancel(ctx)
			t.Cleanup(cancel)

			switch failure {
			case "upload-close":
				fault.failClose.Store(true)
				err = adapter.PrepareClose(workCtx)
				require.ErrorContains(t, err, "injected upload close error")
			case "canceled-before":
				cancel()
				err = adapter.PrepareClose(workCtx)
				require.ErrorIs(t, err, context.Canceled)
			case "canceled-during-upload":
				fault.blockClose.Store(true)
				done := make(chan error, 1)
				go func() { done <- adapter.PrepareClose(workCtx) }()
				nativeFinalizeAwait(t, fault.entered, "entering the prepared file's upload close")
				cancel()
				release()
				err = nativeFinalizeAwait(t, done, "canceling preparation during upload close")
				require.ErrorIs(t, err, context.Canceled)
			}

			require.Error(t, adapter.Close(ctx, map[string]any{"state": "must-not-publish"}), "a fresh context must not revive failed preparation")
			cancel()
			require.ErrorIs(t, adapter.Close(workCtx, nil), context.Canceled)
			unpublished, err := cat.LoadTable(ctx, writer.table.Identifier())
			require.NoError(t, err)
			require.Equal(t, before.MetadataLocation(), unpublished.MetadataLocation())
			require.Nil(t, unpublished.CurrentSnapshot())
			require.Empty(t, nativeVisibleValues(t, unpublished))
			require.Equal(t, before.Properties()[nativeRecoveryProperty], unpublished.Properties()[nativeRecoveryProperty])
			require.Empty(t, index.rows)
			require.Zero(t, index.snapshot)
			_, err = fault.Open(location.FilePath)
			require.Error(t, err, "failed preparation must clean up its unpublished file")
		})
	}
}

func TestNativeBackfillPrepareCloseDoesNotWaitForTableCoordination(t *testing.T) {
	ctx := context.Background()
	cfg, stream, cat := nativeWriterFixture(t, types.UpdateTypeEquality, true, "")
	blocked := &Iceberg{config: cfg, catalog: cat}
	_, _, err := blocked.Setup(ctx, stream, nil, &destination.Options{ThreadID: "blocked-chunk", Backfill: true})
	require.NoError(t, err)
	blockedWriter := blocked.writer.(*nativeWriter)
	t.Cleanup(blockedWriter.Abort)
	peer := &Iceberg{config: cfg, catalog: cat}
	_, _, err = peer.Setup(ctx, stream, nil, &destination.Options{ThreadID: "independent-chunk", Backfill: true})
	require.NoError(t, err)
	peerWriter := peer.writer.(*nativeWriter)
	t.Cleanup(peerWriter.Abort)
	require.Same(t, blockedWriter.coordinator, peerWriter.coordinator)
	fault := nativeBackfillInjectFS(t, blockedWriter)
	release := sync.OnceFunc(func() { close(fault.release) })
	defer release()
	require.NoError(t, blocked.Write(ctx, []types.RawRecord{nativeTestRecord("a", "r", 1, "p")}))
	require.NoError(t, peer.Write(ctx, []types.RawRecord{nativeTestRecord("b", "r", 2, "p")}))
	fault.blockClose.Store(true)

	blockedWriter.coordinator.mu.Lock()
	unlock := sync.OnceFunc(blockedWriter.coordinator.mu.Unlock)
	defer unlock()
	blockedDone := make(chan error, 1)
	go func() { blockedDone <- blocked.PrepareClose(ctx) }()
	nativeFinalizeAwait(t, fault.entered, "finalizing a chunk while the table coordinator is held")
	peerDone := make(chan error, 1)
	go func() { peerDone <- peer.PrepareClose(ctx) }()
	require.NoError(t, nativeFinalizeAwait(t, peerDone, "finalizing an independent chunk while another upload is blocked"))
	require.Empty(t, peerWriter.backfillFiles)
	require.Len(t, peerWriter.dataFiles, 1)
	release()
	require.NoError(t, nativeFinalizeAwait(t, blockedDone, "finishing preparation while the table coordinator is still held"))
	unlock()

	unpublished, err := cat.LoadTable(ctx, blockedWriter.table.Identifier())
	require.NoError(t, err)
	require.Nil(t, unpublished.CurrentSnapshot())
	require.Empty(t, nativeVisibleValues(t, unpublished))
	require.NoError(t, blocked.Close(ctx, nil))
	require.NoError(t, peer.Close(ctx, nil))
	published, err := cat.LoadTable(ctx, peerWriter.table.Identifier())
	require.NoError(t, err)
	require.Equal(t, map[string]int64{"a": 1, "b": 2}, nativeVisibleValues(t, published))
	var state types.MetadataState
	require.NoError(t, json.Unmarshal([]byte(published.Properties()[nativeRecoveryProperty]), &state))
	require.ElementsMatch(t, []string{"blocked-chunk", "independent-chunk"}, state.FullRefreshCommittedIDs)
}
