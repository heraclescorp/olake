package iceberg

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	apacheiceberg "github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/table"
	"github.com/datazip-inc/olake/destination"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
	"github.com/stretchr/testify/require"
)

func nativeBackfillRecoveryContext(t *testing.T, epoch string, state *types.State) (context.Context, *types.OrdinaryRecoveryOptions) {
	t.Helper()
	ctx, options := ordinaryTestContext(t, epoch)
	options.Origin.Backfill = true
	options.Origin.BackfillCheckpointSHA256 = strings.Repeat("b", 64)
	options.Origin.SavedStateSHA256 = strings.Repeat("c", 64)
	options.BackfillState = state
	options.BackfillCheckpoint = &types.BackfillRecoveryCheckpoint{ServerIdentity: options.SourceIdentity, SlotName: "olake"}
	options.BackfillTargetLSN = "0/10"
	ctx = types.WithBackfillBindingJournal(ctx, nativeBackfillTestJournal(t, state))
	return ctx, options
}

var nativeBackfillTestJournals sync.Map

func nativeBackfillTestJournal(t *testing.T, state *types.State) *types.BackfillBindingJournal {
	t.Helper()
	if journal, ok := nativeBackfillTestJournals.Load(state); ok {
		return journal.(*types.BackfillBindingJournal)
	}
	root := t.TempDir()
	path := func(stream string) string {
		return filepath.Join(root, fmt.Sprintf("%x.json", sha256.Sum256([]byte(stream))))
	}
	journal := &types.BackfillBindingJournal{}
	journal.Read = func(stream string) (*types.BackfillBindingJournalEntry, error) {
		encoded, err := os.ReadFile(path(stream))
		if os.IsNotExist(err) {
			return nil, nil
		}
		if err != nil {
			return nil, err
		}
		var entry types.BackfillBindingJournalEntry
		if err := json.Unmarshal(encoded, &entry); err != nil {
			return nil, err
		}
		return &entry, nil
	}
	journal.Create = func(stream string, entry *types.BackfillBindingJournalEntry) error {
		encoded, err := json.Marshal(entry)
		if err != nil {
			return err
		}
		file, err := os.OpenFile(path(stream), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
		if os.IsExist(err) {
			previous, err := os.ReadFile(path(stream))
			if err != nil {
				return err
			}
			if !bytes.Equal(previous, encoded) {
				return fmt.Errorf("immutable test binding journal differs")
			}
			return nil
		}
		if err != nil {
			return err
		}
		_, err = file.Write(encoded)
		if err == nil {
			err = file.Sync()
		}
		closeErr := file.Close()
		if err != nil {
			return err
		}
		return closeErr
	}
	actual, _ := nativeBackfillTestJournals.LoadOrStore(state, journal)
	t.Cleanup(func() { nativeBackfillTestJournals.Delete(state) })
	return actual.(*types.BackfillBindingJournal)
}

func nativeBackfillState() *types.State {
	return &types.State{RWMutex: &sync.RWMutex{}, Type: types.GlobalType, Global: &types.GlobalState{State: map[string]any{"lsn": "0/10"}, Streams: types.NewSet[string]()}, SuppressPersistence: true}
}

func nativeBackfillChunkID(stream types.StreamInterface, chunk types.Chunk) string {
	return fmt.Sprintf("%s_min[%v]-max[%v]", stream.ID(), chunk.Min, chunk.Max)
}

func nativeBindBackfillPlan(t *testing.T, cfg *Config, stream types.StreamInterface, cat catalog.Catalog, state *types.State, plan *types.Set[types.Chunk]) {
	t.Helper()
	binding, err := (&Iceberg{config: cfg, catalog: cat}).PrepareBackfillTable(context.Background(), stream)
	require.NoError(t, err)
	state.SetBackfillTable(stream.Self(), binding)
	require.NoError(t, state.SetBackfillPlan(stream.Self(), plan))
}

func TestNativeBackfillBindingPrecedesWriterAndPreservesTable(t *testing.T) {
	cfg, stream, cat := nativeWriterFixture(t, types.UpdateTypeEquality, false, "")
	adapter := &Iceberg{config: cfg, catalog: cat}
	binding, err := adapter.PrepareBackfillTable(context.Background(), stream)
	require.NoError(t, err)
	tbl, err := cat.LoadTable(context.Background(), table.Identifier{"db", "records"})
	require.NoError(t, err)
	require.Nil(t, tbl.CurrentSnapshot())
	require.Equal(t, tbl.Metadata().TableUUID().String(), binding.TableUUID)
	shape, err := ordinaryRecoveryShape(tbl.Metadata())
	require.NoError(t, err)
	require.Equal(t, shape, binding.ShapeSHA256)
	writer := openNativeTestWriter(context.Background(), t, cfg, stream, cat, nil, "empty", true)
	require.NoError(t, writer.Close(context.Background(), nil))
	var metadata types.MetadataState
	require.NoError(t, json.Unmarshal([]byte(writer.table.Properties()[nativeRecoveryProperty]), &metadata))
	require.Equal(t, []string{"empty"}, metadata.FullRefreshCommittedIDs)
	require.Nil(t, writer.table.CurrentSnapshot())
	again, err := adapter.PrepareBackfillTable(context.Background(), stream)
	require.NoError(t, err)
	require.Equal(t, binding, again)
}

func TestNativeEmptyBackfillPlanRecoversWithoutCreatingDataSnapshot(t *testing.T) {
	cfg, stream, cat := nativeWriterFixture(t, types.UpdateTypeEquality, false, "")
	state := nativeBackfillState()
	chunk := types.Chunk{}
	plan := types.NewSet(chunk)
	nativeBindBackfillPlan(t, cfg, stream, cat, state, plan)
	chunkID := nativeBackfillChunkID(stream, chunk)
	writer := openNativeTestWriter(context.Background(), t, cfg, stream, cat, nil, chunkID, true)
	require.NoError(t, writer.Close(context.Background(), nil))
	require.Nil(t, writer.table.CurrentSnapshot())
	var metadata types.MetadataState
	require.NoError(t, json.Unmarshal([]byte(writer.table.Properties()[nativeRecoveryProperty]), &metadata))
	require.Equal(t, []string{chunkID}, metadata.FullRefreshCommittedIDs)
	require.Nil(t, metadata.State)
	require.NoError(t, state.CompleteBackfill(stream.ID()))
	require.Nil(t, state.GetResumePlan(stream))
	require.Equal(t, 1, state.GetBackfillPlan(stream).Len())
	ctx, options := nativeBackfillRecoveryContext(t, "empty-table-recovery", state)
	adapter := &Iceberg{config: cfg, catalog: &nativeCatalog{Catalog: cat.(*nativeCatalog).Catalog}}
	require.NoError(t, adapter.PrepareBackfillRecovery(ctx, []types.StreamInterface{stream}, "0/10"))
	options.BackfillComplete = true
	proofs, err := adapter.PrepareOrdinaryRecovery(ctx, []types.StreamInterface{stream}, "0/10")
	require.NoError(t, err)
	require.Len(t, proofs, 1)
	require.NotZero(t, proofs[0].FenceSnapshotID)
	require.Equal(t, "0/10", proofs[0].CommittedLSN)
	barrier, err := adapter.catalog.LoadTable(ctx, writer.table.Identifier())
	require.NoError(t, err)
	require.Equal(t, writer.table.Metadata().TableUUID(), barrier.Metadata().TableUUID())
	require.Len(t, barrier.Metadata().Snapshots(), 1)
	require.Equal(t, table.OpAppend, barrier.CurrentSnapshot().Summary.Operation)
	require.Empty(t, ordinaryLiveFiles(t, ctx, barrier))
	require.NoError(t, json.Unmarshal([]byte(barrier.Properties()[nativeRecoveryProperty]), &metadata))
	require.Equal(t, []string{chunkID}, metadata.FullRefreshCommittedIDs)
	require.Equal(t, `{"lsn":"0/10"}`, metadata.State)
	require.Equal(t, "0/10", state.GetGlobal().State.(map[string]any)["lsn"])
	ordinaryTestPlan(options, proofs, "0/10", "0/10")
	confirmed, err := adapter.ConfirmOrdinaryRecovery(ctx, []types.StreamInterface{stream}, proofs, "0/10")
	require.NoError(t, err)
	require.Equal(t, proofs, confirmed)
}

func TestNativeBackfillPreflightsAllBindingsBeforeCreatingUnstartedTable(t *testing.T) {
	for _, mutation := range []string{"missing", "uuid", "shape", "unexpected"} {
		t.Run(mutation, func(t *testing.T) {
			cfg, first, cat := nativeWriterFixture(t, types.UpdateTypeEquality, false, "")
			second := ordinarySecondStream(first)
			state := nativeBackfillState()
			plan := types.NewSet(types.Chunk{Min: nil, Max: nil})
			if mutation == "unexpected" {
				_, err := (&Iceberg{config: cfg, catalog: cat}).PrepareBackfillTable(context.Background(), second)
				require.NoError(t, err)
			} else {
				nativeBindBackfillPlan(t, cfg, second, cat, state, plan)
				binding := *state.GetBackfillTable(second)
				switch mutation {
				case "missing":
					require.NoError(t, cat.DropTable(context.Background(), table.Identifier{"db", second.GetDestinationTable()}))
				case "uuid":
					binding.TableUUID = "00000000-0000-0000-0000-000000000001"
				case "shape":
					binding.ShapeSHA256 = strings.Repeat("0", 64)
				}
				state.SetBackfillTable(second, &binding)
			}
			ctx, _ := nativeBackfillRecoveryContext(t, "retry", state)
			err := (&Iceberg{config: cfg, catalog: cat}).PrepareBackfillRecovery(ctx, []types.StreamInterface{first, second}, "0/10")
			require.Error(t, err)
			_, err = cat.LoadTable(ctx, table.Identifier{"db", "records"})
			require.ErrorIs(t, err, catalog.ErrNoSuchTable)
		})
	}
}

func TestNativeBackfillLowerRequiresEveryCommittedPlanChunkIncludingEmptyTail(t *testing.T) {
	cfg, stream, cat := nativeWriterFixture(t, types.UpdateTypeEquality, true, "")
	state := nativeBackfillState()
	first, tail := types.Chunk{Min: nil, Max: "m"}, types.Chunk{Min: "m", Max: nil}
	plan := types.NewSet(first, tail)
	nativeBindBackfillPlan(t, cfg, stream, cat, state, plan)
	writer := openNativeTestWriter(context.Background(), t, cfg, stream, cat, nil, nativeBackfillChunkID(stream, first), true)
	require.NoError(t, writer.Write(context.Background(), []types.RawRecord{nativeTestRecord("a", "r", 1, "one")}))
	require.NoError(t, writer.Close(context.Background(), nil))
	state.SetChunks(stream, types.NewSet[types.Chunk]())
	require.NoError(t, state.CompleteBackfill(stream.ID()))
	ctx, options := nativeBackfillRecoveryContext(t, "retry", state)
	adapter := &Iceberg{config: cfg, catalog: cat}
	require.NoError(t, adapter.PrepareBackfillRecovery(ctx, []types.StreamInterface{stream}, "0/10"))
	options.BackfillComplete = true
	_, err := adapter.PrepareOrdinaryRecovery(ctx, []types.StreamInterface{stream}, "0/10")
	require.ErrorContains(t, err, "not authoritatively committed")
	before := writer.table
	options.BackfillComplete = false
	tailWriter := openNativeTestWriter(ctx, t, cfg, stream, cat, nil, nativeBackfillChunkID(stream, tail), true)
	require.NoError(t, tailWriter.Close(ctx, nil))
	options.BackfillComplete = true
	proofs, err := adapter.PrepareOrdinaryRecovery(ctx, []types.StreamInterface{stream}, "0/10")
	require.NoError(t, err)
	require.Equal(t, "0/10", proofs[0].CommittedLSN)
	barrier, err := cat.LoadTable(ctx, before.Identifier())
	require.NoError(t, err)
	require.Equal(t, nativeSnapshotID(before), *barrier.CurrentSnapshot().ParentSnapshotID)
	require.Equal(t, table.OpAppend, barrier.CurrentSnapshot().Summary.Operation)
	require.Equal(t, map[string]int64{"a": 1}, nativeVisibleValues(t, barrier))
	var metadata types.MetadataState
	require.NoError(t, json.Unmarshal([]byte(barrier.Properties()[nativeRecoveryProperty]), &metadata))
	require.ElementsMatch(t, []string{nativeBackfillChunkID(stream, first), nativeBackfillChunkID(stream, tail)}, metadata.FullRefreshCommittedIDs)
	require.Equal(t, `{"lsn":"0/10"}`, metadata.State)
}

func TestNativeBackfillUnstartedOwnershipAndPlanSurviveNewEpoch(t *testing.T) {
	cfg, stream, cat := nativeWriterFixture(t, types.UpdateTypeEquality, false, "")
	baseline := nativeBackfillState()
	ctx, _ := nativeBackfillRecoveryContext(t, "first-retry", baseline)
	adapter := &Iceberg{config: cfg, catalog: cat}
	require.NoError(t, adapter.PrepareBackfillRecovery(ctx, []types.StreamInterface{stream}, "0/10"))
	plan := types.NewSet(types.Chunk{Min: nil, Max: json.Number("9007199254740993")}, types.Chunk{Min: json.Number("9007199254740993"), Max: nil})
	require.NoError(t, adapter.PrepareBackfillPlan(ctx, stream, plan))
	writer := openNativeTestWriter(ctx, t, cfg, stream, cat, nil, nativeBackfillChunkID(stream, plan.Array()[0]), true)
	require.NoError(t, writer.Write(ctx, []types.RawRecord{nativeTestRecord("a", "r", 1, "one")}))
	require.NoError(t, writer.Close(ctx, nil))
	tbl := writer.table
	require.Nil(t, baseline.GetBackfillTable(stream))
	require.Nil(t, baseline.GetBackfillPlan(stream))
	nextCtx, _ := nativeBackfillRecoveryContext(t, "second-retry", baseline)
	next := &Iceberg{config: cfg, catalog: &nativeCatalog{Catalog: cat.(*nativeCatalog).Catalog}}
	require.NoError(t, next.PrepareBackfillRecovery(nextCtx, []types.StreamInterface{stream}, "0/10"))
	restored, err := next.GetBackfillPlan(nextCtx, stream)
	require.NoError(t, err)
	expected, err := encodeBackfillPlan(stream, plan)
	require.NoError(t, err)
	actual, err := encodeBackfillPlan(stream, restored)
	require.NoError(t, err)
	require.Equal(t, expected, actual)
	require.NoError(t, next.PrepareBackfillPlan(nextCtx, stream, restored))
	err = next.PrepareBackfillPlan(nextCtx, stream, types.NewSet(types.Chunk{Min: nil, Max: nil}))
	require.ErrorContains(t, err, "immutable chunk plan changed")
	_, _, err = newNativeWriter(nextCtx, cfg, stream, &destination.Options{Backfill: true, ThreadID: "unplanned"}, next.catalog, nil)
	require.ErrorContains(t, err, "outside the exact durable plan")
	_, _, err = cat.CommitTable(nextCtx, tbl.Identifier(), nil, []table.Update{table.NewSetPropertiesUpdate(apacheiceberg.Properties{ordinaryRecoveryProperty: "foreign"})})
	require.NoError(t, err)
	_, err = next.PrepareBackfillTable(nextCtx, stream)
	require.ErrorContains(t, err, "malformed previous marker")
}

func TestNativeBackfillAcknowledgedRetryPreflightsEveryAuthoritativePosition(t *testing.T) {
	cfg, first, cat := nativeWriterFixture(t, types.UpdateTypeEquality, false, "")
	second := ordinarySecondStream(first)
	state := nativeBackfillState()
	plan := types.NewSet(types.Chunk{Min: nil, Max: nil})
	for _, stream := range []*types.ConfiguredStream{first, second} {
		nativeBindBackfillPlan(t, cfg, stream, cat, state, plan)
		position := "0/20"
		if stream == second {
			position = "0/10"
		}
		ordinaryTestSeed(t, cfg, stream, cat, position)
	}
	ctx, options := nativeBackfillRecoveryContext(t, "ack-retry", state)
	options.BackfillAcknowledgedLSN = "0/20"
	cfg.CatalogType = RestCatalog
	fault := &nativeCommitFaultCatalog{Catalog: cat}
	err := (&Iceberg{config: cfg, catalog: fault}).PrepareBackfillRecovery(ctx, []types.StreamInterface{first, second}, "0/10")
	require.ErrorContains(t, err, "cannot cover the acknowledged slot")
	require.Zero(t, fault.calls)
}

func TestNativeOriginalBackfillCreationOwnsTableBeforeCandidateBinding(t *testing.T) {
	cfg, stream, cat := nativeWriterFixture(t, types.UpdateTypeEquality, false, "")
	baseline := nativeBackfillState()
	recoveryCtx, options := nativeBackfillRecoveryContext(t, "recovery", baseline)
	encodedState, err := json.Marshal(baseline)
	require.NoError(t, err)
	checkpoint := &types.BackfillRecoveryCheckpoint{Version: 1, ExecutionID: options.Origin.OriginalExecutionID, LaunchID: options.Origin.OriginalLaunchID, SourceSHA256: options.Origin.SourceSHA256, DestinationSHA256: options.Origin.DestinationSHA256, CatalogSHA256: options.Origin.CatalogSHA256, State: encodedState, ServerIdentity: options.SourceIdentity, SlotName: options.BackfillCheckpoint.SlotName}
	originalCtx, _ := ordinaryTestContext(t, "original")
	execution, ok := utils.ExecutionFromContext(originalCtx)
	require.True(t, ok)
	originalCtx = types.WithBackfillCheckpoint(utils.WithExecution(context.Background(), execution), checkpoint)
	originalCtx = types.WithBackfillBindingJournal(originalCtx, nativeBackfillTestJournal(t, baseline))
	original := &Iceberg{config: cfg, catalog: cat}
	plan := types.NewSet(types.Chunk{Min: nil, Max: "m"}, types.Chunk{Min: "m", Max: nil})
	require.NoError(t, original.PrepareBackfillPlan(originalCtx, stream, plan))
	binding, err := original.PrepareBackfillTable(originalCtx, stream)
	require.NoError(t, err)
	require.NotEmpty(t, binding.TableUUID)
	require.Nil(t, baseline.GetBackfillTable(stream))
	require.Nil(t, baseline.GetBackfillPlan(stream))
	tbl, err := cat.LoadTable(recoveryCtx, table.Identifier{"db", "records"})
	require.NoError(t, err)
	require.NotEmpty(t, tbl.Properties()[nativeBackfillOwnerProperty])
	require.NotEmpty(t, tbl.Properties()[nativeBackfillBindingProperty])
	require.Nil(t, tbl.CurrentSnapshot())
	recovery := &Iceberg{config: cfg, catalog: &nativeCatalog{Catalog: cat.(*nativeCatalog).Catalog}}
	require.NoError(t, recovery.PrepareBackfillRecovery(recoveryCtx, []types.StreamInterface{stream}, "0/10"))
	restored, err := recovery.GetBackfillPlan(recoveryCtx, stream)
	require.NoError(t, err)
	want, err := encodeBackfillPlan(stream, plan)
	require.NoError(t, err)
	got, err := encodeBackfillPlan(stream, restored)
	require.NoError(t, err)
	require.Equal(t, want, got)
	require.NoError(t, recovery.PrepareBackfillPlan(recoveryCtx, stream, restored))
	first := restored.Array()[0]
	writer := openNativeTestWriter(recoveryCtx, t, cfg, stream, recovery.catalog, nil, nativeBackfillChunkID(stream, first), true)
	require.NoError(t, writer.Close(recoveryCtx, nil))
}

func TestNativeBackfillFixedTargetSurvivesPartialBarrierEpoch(t *testing.T) {
	cfg, first, cat := nativeWriterFixture(t, types.UpdateTypeEquality, false, "")
	second := ordinarySecondStream(first)
	state := nativeBackfillState()
	plan := types.NewSet(types.Chunk{})
	streams := []types.StreamInterface{first, second}
	for _, stream := range streams {
		nativeBindBackfillPlan(t, cfg, stream, cat, state, plan)
		writer := openNativeTestWriter(context.Background(), t, cfg, stream.Self(), cat, nil, nativeBackfillChunkID(stream, types.Chunk{}), true)
		require.NoError(t, writer.Close(context.Background(), nil))
		require.NoError(t, state.CompleteBackfill(stream.ID()))
	}
	boundCtx, _ := nativeBackfillRecoveryContext(t, "binding-epoch", state)
	require.NoError(t, (&Iceberg{config: cfg, catalog: cat}).PrepareBackfillRecovery(boundCtx, streams, "0/10"))
	cfg.CatalogType = RestCatalog
	underlying := cat.(*nativeCatalog).Catalog
	fault := &nativeCommitFaultCatalog{Catalog: underlying}
	fault.fault = func(_ context.Context, id table.Identifier, _ []table.Requirement, _ []table.Update) (bool, error) {
		if id[len(id)-1] == second.GetDestinationTable() {
			return true, table.ErrCommitFailed
		}
		return false, nil
	}
	ctx, options := nativeBackfillRecoveryContext(t, "first-target-epoch", state)
	adapter := &Iceberg{config: cfg, catalog: &nativeCatalog{Catalog: fault}}
	require.NoError(t, adapter.PrepareBackfillRecovery(ctx, streams, "0/10"))
	target, err := adapter.BackfillRecoveryTarget(ctx)
	require.NoError(t, err)
	require.Empty(t, target)
	options.BackfillComplete = true
	options.BackfillTargetLSN = "0/a0"
	_, err = adapter.PrepareOrdinaryRecovery(ctx, streams, "0/10")
	require.Error(t, err)
	firstBarrier, err := underlying.LoadTable(ctx, table.Identifier{"db", first.GetDestinationTable()})
	require.NoError(t, err)
	var marker ordinaryRecoveryMarker
	require.NoError(t, json.Unmarshal([]byte(firstBarrier.Properties()[ordinaryRecoveryProperty]), &marker))
	require.Equal(t, "0/a0", marker.BackfillTargetLSN)
	untouched, err := underlying.LoadTable(ctx, table.Identifier{"db", second.GetDestinationTable()})
	require.NoError(t, err)
	require.Empty(t, untouched.Properties()[ordinaryRecoveryProperty])
	nextCtx, nextOptions := nativeBackfillRecoveryContext(t, "second-target-epoch", state)
	nextFault := &nativeCommitFaultCatalog{Catalog: underlying}
	next := &Iceberg{config: cfg, catalog: &nativeCatalog{Catalog: nextFault}}
	require.NoError(t, next.PrepareBackfillRecovery(nextCtx, streams, "0/10"))
	target, err = next.BackfillRecoveryTarget(nextCtx)
	require.NoError(t, err)
	require.Equal(t, "0/a0", target)
	nextOptions.BackfillComplete = true
	nextOptions.BackfillTargetLSN = "0/B0"
	_, err = next.PrepareOrdinaryRecovery(nextCtx, streams, "0/10")
	require.ErrorContains(t, err, "differs from the persisted target")
	require.Zero(t, nextFault.calls)
	nextOptions.BackfillTargetLSN = "000/000A0"
	proofs, err := next.PrepareOrdinaryRecovery(nextCtx, streams, "0/10")
	require.NoError(t, err)
	ordinaryTestPlan(nextOptions, proofs, "000/00010", "0/A0")
	for _, stream := range streams {
		writer := openNativeTestWriter(nextCtx, t, cfg, stream.Self(), next.catalog, nil, "bounded-cdc", false)
		require.NoError(t, writer.Close(nextCtx, map[string]any{"state": `{"lsn":"0/A0"}`}))
	}
	confirmed, err := next.ConfirmOrdinaryRecovery(nextCtx, streams, proofs, target)
	require.NoError(t, err)
	for _, proof := range confirmed {
		position, err := ordinaryRecoveryLSN(proof.CommittedLSN)
		require.NoError(t, err)
		require.Equal(t, uint64(0xA0), position)
	}
	current, err := underlying.LoadTable(nextCtx, table.Identifier{"db", second.GetDestinationTable()})
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal([]byte(current.Properties()[ordinaryRecoveryProperty]), &marker))
	marker.BackfillTargetLSN = "0/a0"
	equivalent, err := json.Marshal(marker)
	require.NoError(t, err)
	_, _, err = underlying.CommitTable(nextCtx, current.Identifier(), nil, []table.Update{table.NewSetPropertiesUpdate(apacheiceberg.Properties{ordinaryRecoveryProperty: string(equivalent)})})
	require.NoError(t, err)
	equivalentCtx, _ := nativeBackfillRecoveryContext(t, "equivalent-target-epoch", state)
	equivalentAdapter := &Iceberg{config: cfg, catalog: cat}
	require.NoError(t, equivalentAdapter.PrepareBackfillRecovery(equivalentCtx, streams, "0/10"))
	marker.BackfillTargetLSN = "0/B0"
	encoded, err := json.Marshal(marker)
	require.NoError(t, err)
	_, _, err = underlying.CommitTable(nextCtx, current.Identifier(), nil, []table.Update{table.NewSetPropertiesUpdate(apacheiceberg.Properties{ordinaryRecoveryProperty: string(encoded)})})
	require.NoError(t, err)
	lastCtx, _ := nativeBackfillRecoveryContext(t, "third-target-epoch", state)
	lastFault := &nativeCommitFaultCatalog{Catalog: underlying}
	err = (&Iceberg{config: cfg, catalog: &nativeCatalog{Catalog: lastFault}}).PrepareBackfillRecovery(lastCtx, streams, "0/10")
	require.ErrorContains(t, err, "disagree on the persisted replay target")
	require.Zero(t, lastFault.calls)
}

func TestNativeBackfillPristineIntentCrashBindsBeforeAnyDataWriter(t *testing.T) {
	for _, condition := range []string{"pristine", "data", "metadata_history"} {
		t.Run(condition, func(t *testing.T) {
			cfg, stream, cat := nativeWriterFixture(t, types.UpdateTypeEquality, false, "")
			baseline := nativeBackfillState()
			recoveryCtx, options := nativeBackfillRecoveryContext(t, "intent-recovery", baseline)
			encodedState, err := json.Marshal(baseline)
			require.NoError(t, err)
			checkpoint := &types.BackfillRecoveryCheckpoint{Version: 1, ExecutionID: options.Origin.OriginalExecutionID, LaunchID: options.Origin.OriginalLaunchID, SourceSHA256: options.Origin.SourceSHA256, DestinationSHA256: options.Origin.DestinationSHA256, CatalogSHA256: options.Origin.CatalogSHA256, State: encodedState, ServerIdentity: options.SourceIdentity, SlotName: options.BackfillCheckpoint.SlotName}
			originalCtx, _ := ordinaryTestContext(t, "original")
			execution, ok := utils.ExecutionFromContext(originalCtx)
			require.True(t, ok)
			originalCtx = types.WithBackfillCheckpoint(utils.WithExecution(context.Background(), execution), checkpoint)
			originalCtx = types.WithBackfillBindingJournal(originalCtx, nativeBackfillTestJournal(t, baseline))
			underlying := cat.(*nativeCatalog).Catalog
			fault := &nativeCommitFaultCatalog{Catalog: underlying}
			fault.fault = func(context.Context, table.Identifier, []table.Requirement, []table.Update) (bool, error) {
				return true, table.ErrCommitFailed
			}
			original := &Iceberg{config: cfg, catalog: &nativeCatalog{Catalog: fault}}
			plan := types.NewSet(types.Chunk{})
			require.NoError(t, original.PrepareBackfillPlan(originalCtx, stream, plan))
			_, err = original.PrepareBackfillTable(originalCtx, stream)
			require.Error(t, err)
			tbl, err := underlying.LoadTable(recoveryCtx, table.Identifier{"db", "records"})
			require.NoError(t, err)
			require.NotEmpty(t, tbl.Properties()[nativeBackfillOwnerProperty])
			require.Empty(t, tbl.Properties()[nativeBackfillBindingProperty])
			_, _, err = newNativeWriter(originalCtx, cfg, stream, &destination.Options{Backfill: true, ThreadID: nativeBackfillChunkID(stream, types.Chunk{})}, &nativeCatalog{Catalog: underlying}, nil)
			require.ErrorContains(t, err, "immutable table binding before data work")
			if condition == "data" {
				writer := openNativeTestWriter(context.Background(), t, cfg, stream, &nativeCatalog{Catalog: underlying}, nil, "unbound-data", true)
				require.NoError(t, writer.Write(context.Background(), []types.RawRecord{nativeTestRecord("a", "r", 1, "one")}))
				require.NoError(t, writer.Close(context.Background(), nil))
			} else if condition == "metadata_history" {
				_, _, err := underlying.CommitTable(recoveryCtx, tbl.Identifier(), nil, []table.Update{table.NewSetPropertiesUpdate(apacheiceberg.Properties{"external": "change"})})
				require.NoError(t, err)
			}
			spy := &nativeCommitFaultCatalog{Catalog: underlying}
			cfg.CatalogType = RestCatalog
			recovery := &Iceberg{config: cfg, catalog: &nativeCatalog{Catalog: spy}}
			err = recovery.PrepareBackfillRecovery(recoveryCtx, []types.StreamInterface{stream}, "0/10")
			if condition != "pristine" {
				require.Error(t, err)
				require.Zero(t, spy.calls)
				return
			}
			require.NoError(t, err)
			bound, err := underlying.LoadTable(recoveryCtx, tbl.Identifier())
			require.NoError(t, err)
			require.NotEmpty(t, bound.Properties()[nativeBackfillBindingProperty])
			require.Nil(t, bound.CurrentSnapshot())
			restored, err := recovery.GetBackfillPlan(recoveryCtx, stream)
			require.NoError(t, err)
			require.Equal(t, 1, restored.Len())
			writer := openNativeTestWriter(recoveryCtx, t, cfg, stream, recovery.catalog, nil, nativeBackfillChunkID(stream, types.Chunk{}), true)
			require.NoError(t, writer.Close(recoveryCtx, nil))
			require.Nil(t, writer.table.CurrentSnapshot())
		})
	}
}

func TestNativeBackfillBindingRejectsCopiedPropertiesOnReplacement(t *testing.T) {
	cfg, stream, cat := nativeWriterFixture(t, types.UpdateTypeEquality, false, "")
	baseline := nativeBackfillState()
	ctx, _ := nativeBackfillRecoveryContext(t, "initial-binding", baseline)
	adapter := &Iceberg{config: cfg, catalog: cat}
	require.NoError(t, adapter.PrepareBackfillRecovery(ctx, []types.StreamInterface{stream}, "0/10"))
	require.NoError(t, adapter.PrepareBackfillPlan(ctx, stream, types.NewSet(types.Chunk{})))
	ident := table.Identifier{"db", "records"}
	original, err := cat.LoadTable(ctx, ident)
	require.NoError(t, err)
	properties := apacheiceberg.Properties{nativeBackfillOwnerProperty: original.Properties()[nativeBackfillOwnerProperty], nativeBackfillBindingProperty: original.Properties()[nativeBackfillBindingProperty]}
	underlying := cat.(*nativeCatalog).Catalog
	require.NoError(t, underlying.DropTable(ctx, ident))
	replacement, err := loadOrCreateNativeTable(context.Background(), cfg, stream, underlying)
	require.NoError(t, err)
	require.NotEqual(t, original.Metadata().TableUUID(), replacement.Metadata().TableUUID())
	_, _, err = underlying.CommitTable(ctx, ident, nil, []table.Update{table.NewSetPropertiesUpdate(properties)})
	require.NoError(t, err)
	nextCtx, _ := nativeBackfillRecoveryContext(t, "replacement-recovery", baseline)
	cfg.CatalogType = RestCatalog
	spy := &nativeCommitFaultCatalog{Catalog: underlying}
	err = (&Iceberg{config: cfg, catalog: &nativeCatalog{Catalog: spy}}).PrepareBackfillRecovery(nextCtx, []types.StreamInterface{stream}, "0/10")
	require.ErrorContains(t, err, "UUID or shape")
	require.Zero(t, spy.calls)
}

func TestNativeBackfillExternalJournalRejectsDroppedCandidateUnstartedTable(t *testing.T) {
	cfg, stream, cat := nativeWriterFixture(t, types.UpdateTypeEquality, false, "")
	baseline := nativeBackfillState()
	ctx, _ := nativeBackfillRecoveryContext(t, "created-and-committed", baseline)
	adapter := &Iceberg{config: cfg, catalog: cat}
	require.NoError(t, adapter.PrepareBackfillRecovery(ctx, []types.StreamInterface{stream}, "0/10"))
	require.NoError(t, adapter.PrepareBackfillPlan(ctx, stream, types.NewSet(types.Chunk{})))
	writer := openNativeTestWriter(ctx, t, cfg, stream, cat, nil, nativeBackfillChunkID(stream, types.Chunk{}), true)
	require.NoError(t, writer.Write(ctx, []types.RawRecord{nativeTestRecord("a", "r", 1, "one")}))
	require.NoError(t, writer.Close(ctx, nil))
	require.Nil(t, baseline.GetBackfillTable(stream))
	require.Nil(t, baseline.GetBackfillPlan(stream))
	underlying := cat.(*nativeCatalog).Catalog
	ident := table.Identifier{"db", "records"}
	require.NoError(t, underlying.DropTable(ctx, ident))
	nextCtx, _ := nativeBackfillRecoveryContext(t, "dropped-table-retry", baseline)
	cfg.CatalogType = RestCatalog
	spy := &nativeCommitFaultCatalog{Catalog: underlying}
	err := (&Iceberg{config: cfg, catalog: &nativeCatalog{Catalog: spy}}).PrepareBackfillRecovery(nextCtx, []types.StreamInterface{stream}, "0/10")
	require.ErrorIs(t, err, catalog.ErrNoSuchTable)
	require.Zero(t, spy.calls)
	_, err = underlying.LoadTable(nextCtx, ident)
	require.ErrorIs(t, err, catalog.ErrNoSuchTable)
}

func TestNativeBackfillJournalFailureBlocksWritersAndExistingBindingCanRetry(t *testing.T) {
	cfg, stream, cat := nativeWriterFixture(t, types.UpdateTypeEquality, false, "")
	baseline := nativeBackfillState()
	ctx, _ := nativeBackfillRecoveryContext(t, "journal-failure", baseline)
	journal := nativeBackfillTestJournal(t, baseline)
	ctx = types.WithBackfillBindingJournal(ctx, &types.BackfillBindingJournal{Read: journal.Read, Create: func(string, *types.BackfillBindingJournalEntry) error {
		return fmt.Errorf("journal publication failed")
	}})
	adapter := &Iceberg{config: cfg, catalog: cat}
	require.NoError(t, adapter.PrepareBackfillRecovery(ctx, []types.StreamInterface{stream}, "0/10"))
	err := adapter.PrepareBackfillPlan(ctx, stream, types.NewSet(types.Chunk{}))
	require.ErrorContains(t, err, "journal publication failed")
	_, _, err = newNativeWriter(ctx, cfg, stream, &destination.Options{Backfill: true, ThreadID: nativeBackfillChunkID(stream, types.Chunk{})}, cat, nil)
	require.ErrorContains(t, err, "durable plan and execution")
	tbl, err := cat.LoadTable(ctx, table.Identifier{"db", "records"})
	require.NoError(t, err)
	require.NotEmpty(t, tbl.Properties()[nativeBackfillBindingProperty])
	require.Nil(t, tbl.CurrentSnapshot())
	entry, err := journal.Read(stream.ID())
	require.NoError(t, err)
	require.Nil(t, entry)
	nextCtx, _ := nativeBackfillRecoveryContext(t, "journal-retry", baseline)
	cfg.CatalogType = RestCatalog
	spy := &nativeCommitFaultCatalog{Catalog: cat.(*nativeCatalog).Catalog}
	next := &Iceberg{config: cfg, catalog: &nativeCatalog{Catalog: spy}}
	require.NoError(t, next.PrepareBackfillRecovery(nextCtx, []types.StreamInterface{stream}, "0/10"))
	require.Zero(t, spy.calls)
	entry, err = journal.Read(stream.ID())
	require.NoError(t, err)
	require.NotNil(t, entry)
	require.Equal(t, tbl.Metadata().TableUUID().String(), entry.TableUUID)
	restored, err := next.GetBackfillPlan(nextCtx, stream)
	require.NoError(t, err)
	require.Equal(t, 1, restored.Len())
}

func TestNativeScheduledBackfillRetryTransfersStrongSavedOwnership(t *testing.T) {
	for _, mode := range []string{"bound", "unbound", "committed_tail", "foreign_source"} {
		t.Run(mode, func(t *testing.T) {
			cfg, stream, cat := nativeWriterFixture(t, types.UpdateTypeEquality, true, "")
			oldState := nativeBackfillState()
			oldCtx, oldOptions := nativeBackfillRecoveryContext(t, "old-partial-runtime", oldState)
			first, tail := types.Chunk{Min: nil, Max: "m"}, types.Chunk{Min: "m", Max: nil}
			plan := types.NewSet(first, tail)
			old := &Iceberg{config: cfg, catalog: cat}
			require.NoError(t, old.PrepareBackfillRecovery(oldCtx, []types.StreamInterface{stream}, "0/10"))
			require.NoError(t, old.PrepareBackfillPlan(oldCtx, stream, plan))
			writer := openNativeTestWriter(oldCtx, t, cfg, stream, cat, nil, nativeBackfillChunkID(stream, first), true)
			require.NoError(t, writer.Write(oldCtx, []types.RawRecord{nativeTestRecord("a", "r", 1, "one")}))
			require.NoError(t, writer.Close(oldCtx, nil))
			original := writer.table
			visible := map[string]int64{"a": 1}
			if mode == "committed_tail" {
				committedTail := openNativeTestWriter(oldCtx, t, cfg, stream, cat, nil, nativeBackfillChunkID(stream, tail), true)
				require.NoError(t, committedTail.Write(oldCtx, []types.RawRecord{nativeTestRecord("b", "r", 2, "two")}))
				require.NoError(t, committedTail.Close(oldCtx, nil))
				original = committedTail.table
				visible["b"] = 2
			}
			serverBinding := original.Properties()[nativeBackfillBindingProperty]
			binding, err := backfillTableBinding(original, stream)
			require.NoError(t, err)
			saved := nativeBackfillState()
			saved.BackfillSource = &types.BackfillSourceBinding{SourceSHA256: oldOptions.Origin.SourceSHA256, DestinationSHA256: oldOptions.Origin.DestinationSHA256, CatalogSHA256: oldOptions.Origin.CatalogSHA256, ServerIdentity: oldOptions.SourceIdentity, SlotName: oldOptions.BackfillCheckpoint.SlotName}
			require.NoError(t, saved.SetBackfillPlan(stream, plan))
			saved.SetChunks(stream, types.NewSet(tail))
			if mode != "unbound" {
				saved.SetBackfillTable(stream, binding)
			}
			if mode == "foreign_source" {
				saved.BackfillSource.SourceSHA256 = strings.Repeat("f", 64)
			}
			encodedState, err := json.Marshal(saved)
			require.NoError(t, err)
			checkpoint := &types.BackfillRecoveryCheckpoint{Version: 1, ExecutionID: "scheduled-new-runtime", LaunchID: "scheduled-new-runtime", SourceSHA256: oldOptions.Origin.SourceSHA256, DestinationSHA256: oldOptions.Origin.DestinationSHA256, CatalogSHA256: oldOptions.Origin.CatalogSHA256, State: encodedState, ServerIdentity: oldOptions.SourceIdentity, SlotName: oldOptions.BackfillCheckpoint.SlotName}
			fencedCtx, _ := ordinaryTestContext(t, checkpoint.ExecutionID)
			execution, ok := utils.ExecutionFromContext(fencedCtx)
			require.True(t, ok)
			currentExecution := *execution
			currentExecution.LaunchID = checkpoint.LaunchID
			ctx := types.WithBackfillCheckpoint(utils.WithExecution(context.Background(), &currentExecution), checkpoint)
			journal := nativeBackfillTestJournal(t, saved)
			ctx = types.WithBackfillBindingJournal(ctx, journal)
			entry, err := journal.Read(stream.ID())
			require.NoError(t, err)
			require.Nil(t, entry)
			spy := &nativeCommitFaultCatalog{Catalog: cat.(*nativeCatalog).Catalog}
			next := &Iceberg{config: cfg, catalog: &nativeCatalog{Catalog: spy}}
			require.NoError(t, next.PrepareBackfillPlan(ctx, stream, plan))
			resumedBinding, err := next.PrepareBackfillTable(ctx, stream)
			if mode == "foreign_source" {
				require.ErrorContains(t, err, "not bound to the current configuration and live source")
				require.Zero(t, spy.calls)
				return
			}
			require.NoError(t, err)
			require.Equal(t, binding, resumedBinding)
			require.Zero(t, spy.calls, "scheduled enrollment must not rewrite the immutable old server binding")
			entry, err = journal.Read(stream.ID())
			require.NoError(t, err)
			require.NotNil(t, entry)
			require.Equal(t, binding.TableUUID, entry.TableUUID)
			require.Equal(t, binding.ShapeSHA256, entry.ShapeSHA256)
			resumed, metadata, err := newNativeWriter(ctx, cfg, stream, &destination.Options{Backfill: true, ThreadID: nativeBackfillChunkID(stream, first)}, next.catalog, nil)
			require.NoError(t, err)
			require.Contains(t, metadata.FullRefreshCommittedIDs, nativeBackfillChunkID(stream, first))
			require.NoError(t, resumed.Close(ctx, nil))
			require.Equal(t, nativeSnapshotID(original), nativeSnapshotID(resumed.table))
			emptyTail, tailMetadata, err := newNativeWriter(ctx, cfg, stream, &destination.Options{Backfill: true, ThreadID: nativeBackfillChunkID(stream, tail)}, next.catalog, nil)
			require.NoError(t, err)
			if mode == "committed_tail" {
				require.Contains(t, tailMetadata.FullRefreshCommittedIDs, nativeBackfillChunkID(stream, tail))
			}
			require.NoError(t, emptyTail.Close(ctx, nil))
			require.Equal(t, nativeSnapshotID(original), nativeSnapshotID(emptyTail.table))
			require.Equal(t, serverBinding, emptyTail.table.Properties()[nativeBackfillBindingProperty])
			require.Equal(t, visible, nativeVisibleValues(t, emptyTail.table))
		})
	}
}
