package iceberg

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	apacheiceberg "github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/catalog/rest"
	"github.com/apache/iceberg-go/table"
	"github.com/datazip-inc/olake/destination"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
	"github.com/stretchr/testify/require"
)

func ordinaryTestContext(t *testing.T, epoch string) (context.Context, *types.OrdinaryRecoveryOptions) {
	t.Helper()
	fence := filepath.Join(t.TempDir(), "fence.json")
	encoded, err := json.Marshal(map[string]any{"version": 1, "execution_id": epoch, "state": "open"})
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(fence, encoded, 0600))
	digest := strings.Repeat("a", 64)
	options := &types.OrdinaryRecoveryOptions{Origin: types.OrdinaryRecoveryOrigin{
		Version: 1, OriginalExecutionID: "original", OriginalLaunchID: "original",
		OriginalReceiptSHA256: digest, SourceSHA256: digest, DestinationSHA256: digest,
		CatalogSHA256: digest, BaselineStateSHA256: digest,
	}, SourceIdentity: "postgres-system-identifier:123:slot:olake:db:16384"}
	ctx := utils.WithExecution(context.Background(), &utils.ExecutionOptions{ID: epoch, LaunchID: "original", Fence: fence, Command: "sync"})
	return types.WithOrdinaryRecovery(ctx, options), options
}

func ordinaryTestSeed(t *testing.T, cfg *Config, stream *types.ConfiguredStream, cat catalog.Catalog, position string) *table.Table {
	t.Helper()
	writer := openResyncTestWriter(t, context.Background(), cfg, stream, cat, nil, "seed", true)
	require.NoError(t, writer.Write(context.Background(), []types.RawRecord{nativeTestRecord("a", "r", 1, "one"), nativeTestRecord("b", "r", 2, "two")}))
	require.NoError(t, writer.Close(context.Background(), map[string]any{"state": `{"lsn":"` + position + `"}`, "full_refresh_committed_ids": []string{"seed"}}))
	return writer.table
}

func ordinaryTestPlan(options *types.OrdinaryRecoveryOptions, proofs []types.OrdinaryRecoveryTableProof, lower, target string) {
	options.Plan = &types.OrdinaryRecoveryPlan{LowerLSN: lower, TargetLSN: target, ReplayAfter: make(map[string]string, len(proofs))}
	for _, proof := range proofs {
		options.Plan.ReplayAfter[proof.Stream] = proof.CommittedLSN
	}
}

func TestOrdinaryRecoveryBarrierPreservesDataDeletesSchemaAndHistory(t *testing.T) {
	for _, mode := range []types.UpdateType{types.UpdateTypeEquality, types.UpdateTypePosition, types.UpdateTypeDeletionVector} {
		t.Run(string(mode), func(t *testing.T) {
			cfg, stream, cat := nativeWriterFixture(t, mode, true, "/{part, identity}")
			ordinaryTestSeed(t, cfg, stream, cat, "0/10")
			var index types.StreamIndex
			if mode != types.UpdateTypeEquality {
				index = &nativeMemoryIndex{rows: make(map[string]types.RowLocation)}
			}
			writer := openResyncTestWriter(t, context.Background(), cfg, stream, cat, index, "cdc", false)
			require.NoError(t, writer.Write(context.Background(), []types.RawRecord{nativeTestRecord("a", "u", 3, "one"), nativeTestRecord("b", "d", 2, "two")}))
			require.NoError(t, writer.Close(context.Background(), map[string]any{"state": `{"lsn":"0/20"}`}))
			original := writer.table
			_, _, err := cat.CommitTable(context.Background(), original.Identifier(), nil, []table.Update{
				table.NewSetSnapshotRefUpdate("audit", nativeSnapshotID(original), table.TagRef, 3600000, 0, 0),
				table.NewSetPropertiesUpdate(apacheiceberg.Properties{resyncGenerationProperty: "historical-generation"}),
			})
			require.NoError(t, err)
			original, err = cat.LoadTable(context.Background(), original.Identifier())
			require.NoError(t, err)
			cfg.CatalogType = RestCatalog
			ctx, options := ordinaryTestContext(t, "recovery-1")
			adapter := &Iceberg{config: cfg, catalog: cat}
			streams := []types.StreamInterface{stream}
			proofs, err := adapter.PrepareOrdinaryRecovery(ctx, streams, "0/10")
			require.NoError(t, err)
			require.Len(t, proofs, 1)
			barrier, err := cat.LoadTable(ctx, original.Identifier())
			require.NoError(t, err)
			require.NotEqual(t, nativeSnapshotID(original), nativeSnapshotID(barrier))
			require.Equal(t, nativeSnapshotID(original), *barrier.CurrentSnapshot().ParentSnapshotID)
			require.Equal(t, table.OpAppend, barrier.CurrentSnapshot().Summary.Operation)
			require.Equal(t, original.Metadata().TableUUID(), barrier.Metadata().TableUUID())
			require.Equal(t, original.Schema().ID, barrier.Schema().ID)
			require.True(t, original.Schema().Equals(barrier.Schema()), "barrier must preserve every schema field and identifier")
			require.True(t, original.Spec().Equals(barrier.Spec()))
			require.True(t, original.SortOrder().Equals(barrier.SortOrder()))
			require.Equal(t, original.Properties()[nativeRecoveryProperty], barrier.Properties()[nativeRecoveryProperty])
			require.Equal(t, original.Properties()[resyncGenerationProperty], barrier.Properties()[resyncGenerationProperty])
			require.Equal(t, original.Metadata().NextRowID(), barrier.Metadata().NextRowID())
			require.Equal(t, nativeVisibleValues(t, original), nativeVisibleValues(t, barrier))
			require.Len(t, barrier.Metadata().Snapshots(), len(original.Metadata().Snapshots())+1)
			for _, snapshot := range original.Metadata().Snapshots() {
				require.Equal(t, &snapshot, barrier.SnapshotByID(snapshot.SnapshotID))
			}
			require.Equal(t, nativeSnapshotID(original), barrier.SnapshotByName("audit").SnapshotID)
			require.Equal(t, slices.Collect(original.Metadata().Statistics()), slices.Collect(barrier.Metadata().Statistics()))
			require.Equal(t, ordinaryLiveFiles(t, ctx, original), ordinaryLiveFiles(t, ctx, barrier))
			ordinaryTestPlan(options, proofs, "0/10", "0/20")
			confirmed, err := adapter.ConfirmOrdinaryRecovery(ctx, streams, proofs, "0/20")
			require.NoError(t, err)
			require.Equal(t, proofs, confirmed)
		})
	}
}

func TestOrdinaryRecoveryEquivalentBookmarkSpellings(t *testing.T) {
	for _, lower := range []string{"0/ABC", "0/abc", "000/000aBc"} {
		t.Run(lower, func(t *testing.T) {
			cfg, stream, cat := nativeWriterFixture(t, types.UpdateTypeEquality, true, "")
			original := ordinaryTestSeed(t, cfg, stream, cat, "0/abc")
			cfg.CatalogType = RestCatalog
			ctx, options := ordinaryTestContext(t, "equivalent-bookmarks")
			adapter := &Iceberg{config: cfg, catalog: cat}
			streams := []types.StreamInterface{stream}
			proofs, err := adapter.PrepareOrdinaryRecovery(ctx, streams, lower)
			require.NoError(t, err)
			ordinaryTestPlan(options, proofs, "0/ABC", "000/000ABC")
			options.Plan.ReplayAfter[stream.ID()] = "000/000aBc"
			writer := openNativeTestWriter(ctx, t, cfg, stream, cat, nil, "bookmark-replay", false)
			require.NoError(t, writer.Close(ctx, map[string]any{"state": `{"lsn":"0/ABC"}`}))
			_, err = adapter.ConfirmOrdinaryRecovery(ctx, streams, proofs, "0/abc")
			require.NoError(t, err)
			current, err := cat.LoadTable(ctx, original.Identifier())
			require.NoError(t, err)
			require.Equal(t, nativeVisibleValues(t, original), nativeVisibleValues(t, current))
			for _, invalid := range []string{"", "0/ABD", "0/not-hex", "0/ABC/extra"} {
				options.Plan.LowerLSN = invalid
				_, err = adapter.ConfirmOrdinaryRecovery(ctx, streams, proofs, "0/ABC")
				require.Error(t, err)
				_, _, err = newNativeWriter(ctx, cfg, stream, &destination.Options{ThreadID: "invalid-bookmark"}, cat, types.NewStreamIndexThread(nil))
				require.Error(t, err)
			}
		})
	}
}

func TestOrdinaryRecoveryRepeatedEpochRebarriersAndBindsImmutableEvidence(t *testing.T) {
	cfg, stream, cat := nativeWriterFixture(t, types.UpdateTypeEquality, false, "")
	ordinaryTestSeed(t, cfg, stream, cat, "0/10")
	cfg.CatalogType = RestCatalog
	streams := []types.StreamInterface{stream}
	ctx, _ := ordinaryTestContext(t, "recovery-1")
	first, err := (&Iceberg{config: cfg, catalog: cat}).PrepareOrdinaryRecovery(ctx, streams, "0/10")
	require.NoError(t, err)
	// Even the same epoch must publish another real barrier, never adopt a marker.
	same, err := (&Iceberg{config: cfg, catalog: cat}).PrepareOrdinaryRecovery(ctx, streams, "0/10")
	require.NoError(t, err)
	require.NotEqual(t, first[0].FenceSnapshotID, same[0].FenceSnapshotID)
	require.Equal(t, first[0].CommittedLSN, same[0].CommittedLSN)
	nextCtx, options := ordinaryTestContext(t, "recovery-2")
	next, err := (&Iceberg{config: cfg, catalog: cat}).PrepareOrdinaryRecovery(nextCtx, streams, "0/10")
	require.NoError(t, err)
	require.NotEqual(t, same[0].FenceSnapshotID, next[0].FenceSnapshotID)
	for _, change := range []func(*types.OrdinaryRecoveryOptions){
		func(o *types.OrdinaryRecoveryOptions) { o.SourceIdentity += "other" },
		func(o *types.OrdinaryRecoveryOptions) { o.Origin.OriginalReceiptSHA256 = strings.Repeat("b", 64) },
		func(o *types.OrdinaryRecoveryOptions) { o.Origin.CatalogSHA256 = strings.Repeat("b", 64) },
	} {
		original := *options
		change(options)
		_, err := (&Iceberg{config: cfg, catalog: cat}).PrepareOrdinaryRecovery(nextCtx, streams, "0/10")
		require.ErrorContains(t, err, "immutable origin")
		*options = original
	}
}

func TestOrdinaryRecoveryOptionalOriginalReceiptRemainsImmutable(t *testing.T) {
	cfg, stream, cat := nativeWriterFixture(t, types.UpdateTypeEquality, false, "")
	ordinaryTestSeed(t, cfg, stream, cat, "0/10")
	cfg.CatalogType = RestCatalog
	streams := []types.StreamInterface{stream}
	ctx, options := ordinaryTestContext(t, "recovery-1")
	options.Origin.OriginalReceiptSHA256 = ""
	first, err := (&Iceberg{config: cfg, catalog: cat}).PrepareOrdinaryRecovery(ctx, streams, "0/10")
	require.NoError(t, err)
	nextCtx, nextOptions := ordinaryTestContext(t, "recovery-2")
	nextOptions.Origin = options.Origin
	next, err := (&Iceberg{config: cfg, catalog: cat}).PrepareOrdinaryRecovery(nextCtx, streams, "0/10")
	require.NoError(t, err)
	require.NotEqual(t, first[0].FenceSnapshotID, next[0].FenceSnapshotID)
	fault := &nativeCommitFaultCatalog{Catalog: cat}
	nextOptions.Origin.OriginalReceiptSHA256 = strings.Repeat("b", 64)
	_, err = (&Iceberg{config: cfg, catalog: fault}).PrepareOrdinaryRecovery(nextCtx, streams, "0/10")
	require.ErrorContains(t, err, "immutable origin")
	require.Zero(t, fault.calls, "a late receipt cannot rewrite an unbound immutable origin")
	nextOptions.Origin = options.Origin
	nextOptions.Origin.SourceSHA256 = ""
	_, err = (&Iceberg{config: cfg, catalog: fault}).PrepareOrdinaryRecovery(nextCtx, streams, "0/10")
	require.ErrorContains(t, err, "invalid artifact digest")
	require.Zero(t, fault.calls, "only the original receipt digest is optional")
}

func TestOrdinaryRecoveryPreflightsEveryMarkerBeforeAnyMutation(t *testing.T) {
	for _, mutation := range []string{"empty", "null", "malformed", "unknown_field", "trailing_json", "version", "missing_epoch", "original_epoch", "launch", "origin_launch", "origin_execution", "receipt", "source_config", "destination_config", "catalog", "baseline", "source_identity", "uuid", "shape", "stream", "lower"} {
		t.Run(mutation, func(t *testing.T) {
			cfg, first, cat := nativeWriterFixture(t, types.UpdateTypeEquality, false, "")
			second := ordinarySecondStream(first)
			ordinaryTestSeed(t, cfg, first, cat, "0/10")
			original := ordinaryTestSeed(t, cfg, second, cat, "0/20")
			cfg.CatalogType = RestCatalog
			ctx, _ := ordinaryTestContext(t, "recovery-1")
			streams := []types.StreamInterface{first, second}
			_, err := (&Iceberg{config: cfg, catalog: cat}).PrepareOrdinaryRecovery(ctx, streams, "0/10")
			require.NoError(t, err)
			current, err := cat.LoadTable(ctx, original.Identifier())
			require.NoError(t, err)
			var marker ordinaryRecoveryMarker
			require.NoError(t, json.Unmarshal([]byte(current.Properties()[ordinaryRecoveryProperty]), &marker))
			switch mutation {
			case "version":
				marker.Version++
			case "missing_epoch":
				marker.ExecutionID = ""
			case "original_epoch":
				marker.ExecutionID = marker.Origin.OriginalExecutionID
			case "launch":
				marker.LaunchID = "foreign-launch"
			case "origin_launch":
				marker.Origin.OriginalLaunchID = "foreign-launch"
			case "origin_execution":
				marker.Origin.OriginalExecutionID = "foreign-original"
			case "receipt":
				marker.Origin.OriginalReceiptSHA256 = ""
			case "source_config":
				marker.Origin.SourceSHA256 = strings.Repeat("b", 64)
			case "destination_config":
				marker.Origin.DestinationSHA256 = strings.Repeat("b", 64)
			case "catalog":
				marker.Origin.CatalogSHA256 = strings.Repeat("b", 64)
			case "baseline":
				marker.Origin.BaselineStateSHA256 = strings.Repeat("b", 64)
			case "source_identity":
				marker.SourceIdentity = "other-system"
			case "uuid":
				marker.TableUUID = "00000000-0000-0000-0000-000000000001"
			case "shape":
				marker.ShapeSHA256 = strings.Repeat("b", 64)
			case "stream":
				marker.Stream = first.ID()
			case "lower":
				marker.LowerLSN = "0/F"
			}
			encoded, err := json.Marshal(marker)
			require.NoError(t, err)
			stored := string(encoded)
			switch mutation {
			case "empty":
				stored = ""
			case "null":
				stored = "null"
			case "malformed":
				stored = "{"
			case "unknown_field":
				stored = strings.TrimSuffix(stored, "}") + `,"unexpected":true}`
			case "trailing_json":
				stored += " {}"
			}
			_, _, err = cat.CommitTable(ctx, current.Identifier(), nil, []table.Update{table.NewSetPropertiesUpdate(apacheiceberg.Properties{ordinaryRecoveryProperty: stored})})
			require.NoError(t, err)
			nextCtx, _ := ordinaryTestContext(t, "recovery-2")
			fault := &nativeCommitFaultCatalog{Catalog: cat}
			_, err = (&Iceberg{config: cfg, catalog: fault}).PrepareOrdinaryRecovery(nextCtx, streams, "0/10")
			require.Error(t, err)
			require.Zero(t, fault.calls, "the first selected table must not be rebarriered before rejecting the second")
		})
	}
}

func TestOrdinaryRecoveryMissingInvalidAndBehindMetadataNeverWrites(t *testing.T) {
	for _, metadata := range []string{"", "{", `{}`, `{"state":"{}"}`, `{"state":"{\"lsn\":\"0/F\"}"}`} {
		t.Run(metadata, func(t *testing.T) {
			cfg, stream, cat := nativeWriterFixture(t, types.UpdateTypeEquality, false, "")
			original := ordinaryTestSeed(t, cfg, stream, cat, "0/10")
			_, _, err := cat.CommitTable(context.Background(), original.Identifier(), nil, []table.Update{table.NewSetPropertiesUpdate(apacheiceberg.Properties{nativeRecoveryProperty: metadata})})
			require.NoError(t, err)
			cfg.CatalogType = RestCatalog
			fault := &nativeCommitFaultCatalog{Catalog: cat}
			ctx, _ := ordinaryTestContext(t, "recovery")
			_, err = (&Iceberg{config: cfg, catalog: &nativeCatalog{Catalog: fault}}).PrepareOrdinaryRecovery(ctx, []types.StreamInterface{stream}, "0/10")
			require.Error(t, err)
			require.Zero(t, fault.calls)
		})
	}
}

func TestOrdinaryRecoveryConflictReloadsAndAmbiguousCommitRemainsUnknown(t *testing.T) {
	for _, ambiguous := range []bool{false, true} {
		t.Run(map[bool]string{false: "conflict", true: "ambiguous"}[ambiguous], func(t *testing.T) {
			cfg, stream, cat := nativeWriterFixture(t, types.UpdateTypeEquality, false, "")
			original := ordinaryTestSeed(t, cfg, stream, cat, "0/10")
			cfg.CatalogType = RestCatalog
			fault := &nativeCommitFaultCatalog{Catalog: cat.(*nativeCatalog).Catalog}
			fault.fault = func(ctx context.Context, id table.Identifier, requirements []table.Requirement, updates []table.Update) (bool, error) {
				fault.fault = nil
				if ambiguous {
					_, _, err := fault.Catalog.CommitTable(ctx, id, requirements, updates)
					require.NoError(t, err)
					return true, rest.ErrCommitStateUnknown
				}
				_, _, err := fault.Catalog.CommitTable(ctx, id, nil, []table.Update{table.NewSetPropertiesUpdate(apacheiceberg.Properties{"concurrent": "preserved"})})
				require.NoError(t, err)
				return true, table.ErrCommitFailed
			}
			wrapped := &nativeCatalog{Catalog: fault}
			adapter := &Iceberg{config: cfg, catalog: wrapped}
			ctx, _ := ordinaryTestContext(t, "recovery-1")
			proofs, err := adapter.PrepareOrdinaryRecovery(ctx, []types.StreamInterface{stream}, "0/10")
			if ambiguous {
				require.ErrorContains(t, err, "UNKNOWN")
				require.Nil(t, proofs)
				require.Equal(t, 1, fault.calls)
				_, err = (&Iceberg{config: cfg, catalog: wrapped}).PrepareOrdinaryRecovery(ctx, []types.StreamInterface{stream}, "0/10")
				require.ErrorContains(t, err, "unknown")
				// A new catalog/new epoch fences the ambiguous earlier barrier anew.
				nextCtx, _ := ordinaryTestContext(t, "recovery-2")
				next, err := (&Iceberg{config: cfg, catalog: &nativeCatalog{Catalog: fault}}).PrepareOrdinaryRecovery(nextCtx, []types.StreamInterface{stream}, "0/10")
				require.NoError(t, err)
				require.NotZero(t, next[0].FenceSnapshotID)
			} else {
				require.NoError(t, err)
				require.Equal(t, 2, fault.calls)
				tbl, err := wrapped.LoadTable(ctx, original.Identifier())
				require.NoError(t, err)
				require.Equal(t, "preserved", tbl.Properties()["concurrent"])
			}
		})
	}
}

func TestOrdinaryRecoveryConflictsAreBoundedAndRejectShapeDrift(t *testing.T) {
	for _, drift := range []bool{false, true} {
		cfg, stream, cat := nativeWriterFixture(t, types.UpdateTypeEquality, false, "")
		ordinaryTestSeed(t, cfg, stream, cat, "0/10")
		cfg.CatalogType = RestCatalog
		fault := &nativeCommitFaultCatalog{Catalog: cat.(*nativeCatalog).Catalog}
		fault.fault = func(ctx context.Context, id table.Identifier, _ []table.Requirement, _ []table.Update) (bool, error) {
			if drift {
				fault.fault = nil
				fresh, err := fault.Catalog.LoadTable(ctx, id)
				require.NoError(t, err)
				tx := fresh.NewTransaction()
				require.NoError(t, tx.UpdateSchema(true, false).AddColumn([]string{"drift"}, apacheiceberg.PrimitiveTypes.String, "", false, nil).Commit())
				_, err = tx.Commit(ctx)
				require.NoError(t, err)
			}
			return true, table.ErrCommitFailed
		}
		ctx, _ := ordinaryTestContext(t, "recovery")
		_, err := (&Iceberg{config: cfg, catalog: &nativeCatalog{Catalog: fault}}).PrepareOrdinaryRecovery(ctx, []types.StreamInterface{stream}, "0/10")
		if drift {
			require.ErrorContains(t, err, "shape changed")
		} else {
			require.ErrorContains(t, err, "conflicts exhausted")
			require.Equal(t, nativeCommitAttempts, fault.calls)
		}
	}
}

func TestOrdinaryRecoveryMissingTableIsNeverCreated(t *testing.T) {
	cfg, stream, cat := nativeWriterFixture(t, types.UpdateTypeEquality, false, "")
	cfg.CatalogType, cfg.ordinaryRecovery = RestCatalog, true
	ctx, options := ordinaryTestContext(t, "recovery")
	_, err := (&Iceberg{config: cfg, catalog: cat}).PrepareOrdinaryRecovery(ctx, []types.StreamInterface{stream}, "0/10")
	require.ErrorIs(t, err, catalog.ErrNoSuchTable)
	options.Plan = &types.OrdinaryRecoveryPlan{LowerLSN: "0/10", TargetLSN: "0/10", ReplayAfter: map[string]string{stream.ID(): "0/10"}}
	_, _, err = newNativeWriter(ctx, cfg, stream, &destination.Options{ThreadID: "cdc"}, cat, nil)
	require.ErrorContains(t, err, "cannot create")
	_, err = cat.LoadTable(ctx, catalog.ToIdentifier("db", "records"))
	require.True(t, errors.Is(err, catalog.ErrNoSuchTable))
}

type ordinaryFileLineage struct {
	Sequence     int64
	FileSequence *int64
	FirstRowID   *int64
	Content      apacheiceberg.ManifestEntryContent
	Offset       *int64
	Length       *int64
}

func ordinaryLiveFiles(t *testing.T, ctx context.Context, tbl *table.Table) map[string]ordinaryFileLineage {
	t.Helper()
	fs, err := tbl.FS(ctx)
	require.NoError(t, err)
	manifests, err := tbl.CurrentSnapshot().Manifests(fs)
	require.NoError(t, err)
	result := make(map[string]ordinaryFileLineage)
	for _, manifest := range manifests {
		for entry, err := range manifest.Entries(fs, true) {
			require.NoError(t, err)
			file := entry.DataFile()
			result[file.FilePath()] = ordinaryFileLineage{entry.SequenceNum(), entry.FileSequenceNum(), file.FirstRowID(), file.ContentType(), file.ContentOffset(), file.ContentSizeInBytes()}
		}
	}
	return result
}

func ordinarySecondStream(stream *types.ConfiguredStream) *types.ConfiguredStream {
	second := *stream
	base := *stream.Stream
	base.Name, base.DestinationTable = "other", "other"
	second.Stream = &base
	return &second
}

func TestOrdinaryRecoveryRequiresExactCommonBoundary(t *testing.T) {
	cfg, first, cat := nativeWriterFixture(t, types.UpdateTypeEquality, false, "")
	second := ordinarySecondStream(first)
	ordinaryTestSeed(t, cfg, first, cat, "0/10")
	ordinaryTestSeed(t, cfg, second, cat, "0/20")
	cfg.CatalogType = RestCatalog
	ctx, options := ordinaryTestContext(t, "recovery")
	adapter := &Iceberg{config: cfg, catalog: cat}
	streams := []types.StreamInterface{first, second}
	proofs, err := adapter.PrepareOrdinaryRecovery(ctx, streams, "0/10")
	require.NoError(t, err)
	ordinaryTestPlan(options, proofs, "0/10", "0/20")
	_, err = adapter.ConfirmOrdinaryRecovery(ctx, streams, proofs, "0/20")
	require.ErrorContains(t, err, "behind")
	_, _, err = newNativeWriter(ctx, cfg, first, &destination.Options{ThreadID: "bad-backfill", Backfill: true}, cat, nil)
	require.Error(t, err)
	// The writer for the behind table commits only its missing prefix, with the
	// common target even if that table sees no rows in the final transaction.
	writer := openResyncTestWriter(t, ctx, cfg, first, cat, nil, "replay", false)
	require.NoError(t, writer.Write(ctx, []types.RawRecord{nativeTestRecord("a", "u", 9, "one")}))
	require.NoError(t, writer.Close(ctx, map[string]any{"state": `{"lsn":"0/20"}`}))
	confirmed, err := adapter.ConfirmOrdinaryRecovery(ctx, streams, proofs, "0/20")
	require.NoError(t, err)
	require.Len(t, confirmed, 2)
	for _, proof := range confirmed {
		require.Equal(t, "0/20", proof.CommittedLSN)
	}
	_, err = adapter.ConfirmOrdinaryRecovery(ctx, streams[:1], proofs[:1], "0/20")
	require.ErrorContains(t, err, "every selected")
	options.Plan.TargetLSN = "0/30"
	_, _, err = newNativeWriter(ctx, cfg, first, &destination.Options{ThreadID: "beyond-max"}, cat, nil)
	require.ErrorContains(t, err, "replay plan")
	_, err = adapter.ConfirmOrdinaryRecovery(ctx, streams, proofs, "0/30")
	require.ErrorContains(t, err, "binding changed")
}

func TestOrdinaryRecoveryInterruptedPreparationRebarriersEveryTable(t *testing.T) {
	cfg, first, cat := nativeWriterFixture(t, types.UpdateTypeEquality, false, "")
	second := ordinarySecondStream(first)
	originalFirst := ordinaryTestSeed(t, cfg, first, cat, "0/10")
	originalSecond := ordinaryTestSeed(t, cfg, second, cat, "0/20")
	cfg.CatalogType = RestCatalog
	fault := &nativeCommitFaultCatalog{Catalog: cat.(*nativeCatalog).Catalog}
	fault.fault = func(_ context.Context, ident table.Identifier, _ []table.Requirement, _ []table.Update) (bool, error) {
		if ident[len(ident)-1] == second.GetDestinationTable() {
			return true, rest.ErrForbidden
		}
		return false, nil
	}
	ctx, _ := ordinaryTestContext(t, "recovery-1")
	streams := []types.StreamInterface{first, second}
	_, err := (&Iceberg{config: cfg, catalog: &nativeCatalog{Catalog: fault}}).PrepareOrdinaryRecovery(ctx, streams, "0/10")
	require.Error(t, err)
	interrupted, err := cat.LoadTable(context.Background(), originalFirst.Identifier())
	require.NoError(t, err)
	require.NotEqual(t, nativeSnapshotID(originalFirst), nativeSnapshotID(interrupted))
	fault.fault = nil
	conflictingCtx, conflictingOptions := ordinaryTestContext(t, "recovery-conflicting")
	conflictingOptions.SourceIdentity = "changed-postgres-system"
	before := fault.calls
	_, err = (&Iceberg{config: cfg, catalog: &nativeCatalog{Catalog: fault}}).PrepareOrdinaryRecovery(conflictingCtx, streams, "0/10")
	require.ErrorContains(t, err, "source identity")
	require.Equal(t, before, fault.calls, "the interrupted barrier is durable source identity evidence without helper artifacts")
	nextCtx, _ := ordinaryTestContext(t, "recovery-2")
	proofs, err := (&Iceberg{config: cfg, catalog: &nativeCatalog{Catalog: fault}}).PrepareOrdinaryRecovery(nextCtx, streams, "0/10")
	require.NoError(t, err)
	require.NotEqual(t, nativeSnapshotID(interrupted), proofs[0].FenceSnapshotID)
	require.NotEqual(t, nativeSnapshotID(originalSecond), proofs[1].FenceSnapshotID)
}

func TestOrdinaryRecoveryFinalProofRejectsUUIDMarkerAndAncestryDrift(t *testing.T) {
	for _, mutation := range []string{"uuid", "marker", "ancestry", "ahead"} {
		t.Run(mutation, func(t *testing.T) {
			cfg, stream, cat := nativeWriterFixture(t, types.UpdateTypeEquality, false, "")
			original := ordinaryTestSeed(t, cfg, stream, cat, "0/10")
			cfg.CatalogType = RestCatalog
			ctx, options := ordinaryTestContext(t, "recovery")
			adapter := &Iceberg{config: cfg, catalog: cat}
			streams := []types.StreamInterface{stream}
			proofs, err := adapter.PrepareOrdinaryRecovery(ctx, streams, "0/10")
			require.NoError(t, err)
			ordinaryTestPlan(options, proofs, "0/10", "0/10")
			switch mutation {
			case "uuid":
				require.NoError(t, cat.DropTable(context.Background(), original.Identifier()))
				_, err = cat.CreateTable(context.Background(), original.Identifier(), original.Schema(), catalog.WithProperties(original.Properties()))
				require.NoError(t, err)
			case "marker":
				_, _, err = cat.CommitTable(ctx, original.Identifier(), nil, []table.Update{table.NewRemovePropertiesUpdate([]string{ordinaryRecoveryProperty})})
				require.NoError(t, err)
			case "ancestry":
				_, _, err = cat.CommitTable(ctx, original.Identifier(), nil, []table.Update{table.NewSetSnapshotRefUpdate(table.MainBranch, nativeSnapshotID(original), table.BranchRef, 0, 0, 0)})
				require.NoError(t, err)
			case "ahead":
				_, _, err = cat.CommitTable(ctx, original.Identifier(), nil, []table.Update{table.NewSetPropertiesUpdate(apacheiceberg.Properties{nativeRecoveryProperty: `{"state":"{\"lsn\":\"0/20\"}"}`})})
				require.NoError(t, err)
			}
			_, err = adapter.ConfirmOrdinaryRecovery(ctx, streams, proofs, "0/10")
			require.Error(t, err)
		})
	}
}

// Opt-in storage/SQL smoke: REST eligibility is separately enforced by catalog
// opening; this exercises the identical real transaction against a disposable
// PostgreSQL catalog with local files, without requiring cloud credentials.
func TestOrdinaryRecoverySQLCatalogSmoke(t *testing.T) {
	connection := os.Getenv("OLAKE_NATIVE_POSTGRES_URL")
	if connection == "" {
		t.Skip("requires disposable PostgreSQL via OLAKE_NATIVE_POSTGRES_URL")
	}
	cfg, stream, _ := nativeWriterFixture(t, types.UpdateTypeEquality, true, "")
	cfg.CatalogType, cfg.JDBCUrl, cfg.CatalogName = JDBCCatalog, connection, "ordinary_recovery_"+strings.ReplaceAll(t.Name(), "/", "_")
	cat, cleanup, err := openNativeCatalog(context.Background(), cfg)
	require.NoError(t, err)
	defer cleanup(context.Background())
	original := ordinaryTestSeed(t, cfg, stream, cat, "0/10")
	defer func() {
		_ = cat.DropTable(context.Background(), original.Identifier())
		_ = cat.DropNamespace(context.Background(), table.Identifier{"db"})
	}()
	cfg.CatalogType = RestCatalog
	ctx, options := ordinaryTestContext(t, "recovery")
	adapter := &Iceberg{config: cfg, catalog: cat}
	streams := []types.StreamInterface{stream}
	proofs, err := adapter.PrepareOrdinaryRecovery(ctx, streams, "0/10")
	require.NoError(t, err)
	ordinaryTestPlan(options, proofs, "0/10", "0/10")
	_, err = adapter.ConfirmOrdinaryRecovery(ctx, streams, proofs, "0/10")
	require.NoError(t, err)
	barrier, err := cat.LoadTable(ctx, original.Identifier())
	require.NoError(t, err)
	require.Equal(t, nativeVisibleValues(t, original), nativeVisibleValues(t, barrier))
	require.NotEqual(t, nativeSnapshotID(original), nativeSnapshotID(barrier))
}

func TestOrdinaryRecoveryEmptyBaselineCreatesFirstRealSnapshot(t *testing.T) {
	cfg, stream, cat := nativeWriterFixture(t, types.UpdateTypeEquality, false, "")
	writer := openResyncTestWriter(t, context.Background(), cfg, stream, cat, nil, "empty-baseline", false)
	require.NoError(t, writer.Close(context.Background(), map[string]any{"state": `{"lsn":"0/10"}`}))
	require.Nil(t, writer.table.CurrentSnapshot())
	cfg.CatalogType = RestCatalog
	ctx, options := ordinaryTestContext(t, "recovery")
	adapter := &Iceberg{config: cfg, catalog: cat}
	streams := []types.StreamInterface{stream}
	proofs, err := adapter.PrepareOrdinaryRecovery(ctx, streams, "0/10")
	require.NoError(t, err)
	require.NotZero(t, proofs[0].FenceSnapshotID)
	ordinaryTestPlan(options, proofs, "0/10", "0/10")
	_, err = adapter.ConfirmOrdinaryRecovery(ctx, streams, proofs, "0/10")
	require.NoError(t, err)
	tbl, err := cat.LoadTable(ctx, writer.table.Identifier())
	require.NoError(t, err)
	require.Nil(t, tbl.CurrentSnapshot().ParentSnapshotID)
	require.Empty(t, ordinaryLiveFiles(t, ctx, tbl), "an empty barrier must not introduce data or delete files")
	scan := tbl.Scan()
	defer scan.Close()
	_, records, err := scan.ToArrowRecords(ctx)
	require.NoError(t, err)
	var rows int64
	for batch, err := range records {
		require.NoError(t, err)
		rows += batch.NumRows()
		batch.Release()
	}
	require.Zero(t, rows, "scan the actual raw-table schema, not normalized-only value columns")
}

func TestOrdinaryRecoveryRejectsRetiredFenceAndUnvalidatedIdentity(t *testing.T) {
	for _, invalid := range []string{"closed", "original", "source"} {
		cfg, stream, cat := nativeWriterFixture(t, types.UpdateTypeEquality, false, "")
		ordinaryTestSeed(t, cfg, stream, cat, "0/10")
		cfg.CatalogType = RestCatalog
		ctx, options := ordinaryTestContext(t, "recovery")
		execution, _ := utils.ExecutionFromContext(ctx)
		switch invalid {
		case "closed":
			require.NoError(t, os.WriteFile(execution.Fence, []byte(`{"version":1,"execution_id":"recovery","state":"closed"}`), 0600))
		case "original":
			execution.ID = options.Origin.OriginalExecutionID
		case "source":
			options.SourceIdentity = ""
		}
		fault := &nativeCommitFaultCatalog{Catalog: cat}
		_, err := (&Iceberg{config: cfg, catalog: fault}).PrepareOrdinaryRecovery(ctx, []types.StreamInterface{stream}, "0/10")
		require.Error(t, err)
		require.Zero(t, fault.calls)
	}
}

func TestOrdinaryRecoveryRejectsOtherRESTCatalogFamiliesBeforeOpening(t *testing.T) {
	for _, family := range []string{"unity", "horizon", "biglake", "glue", "jdbc", "hive"} {
		ctx, _ := ordinaryTestContext(t, "recovery")
		_, _, err := destination.RegisteredWriters[types.Iceberg](ctx, map[string]any{"catalog_type": family, "iceberg_s3_path": t.TempDir()})
		require.ErrorContains(t, err, "only native REST/S3 Tables")
	}
}

func TestOrdinaryRecoveryMicrobatchesPreserveAheadAndQuietTables(t *testing.T) {
	for _, copyOnWrite := range []bool{false, true} {
		name := "merge-on-read"
		if copyOnWrite {
			name = "copy-on-write"
		}
		t.Run(name, func(t *testing.T) {
			cfg, first, cat := nativeWriterFixture(t, types.UpdateTypeEquality, true, "")
			first.Stream.WithPrimaryKey("part")
			first.StreamMetadata.AppendMode = new(false)
			if copyOnWrite {
				first.StreamMetadata.MergeStrategy = types.CopyOnWrite
			}
			ahead := ordinarySecondStream(first)
			quiet := ordinarySecondStream(first)
			quiet.Stream.Name, quiet.Stream.DestinationTable = "quiet", "quiet"
			ordinaryTestSeed(t, cfg, first, cat, "0/10")
			aheadOriginal := ordinaryTestSeed(t, cfg, ahead, cat, "0/40")
			quietOriginal := ordinaryTestSeed(t, cfg, quiet, cat, "0/10")
			cfg.CatalogType = RestCatalog
			ctx, options := ordinaryTestContext(t, "microbatch-epoch")
			streams := []types.StreamInterface{first, ahead, quiet}
			adapter := &Iceberg{config: cfg, catalog: cat}
			proofs, err := adapter.PrepareOrdinaryRecovery(ctx, streams, "0/10")
			require.NoError(t, err)
			ordinaryTestPlan(options, proofs, "0/10", "0/40")
			options.Plan.BatchTargetLSN = "0/20"
			writer := openNativeTestWriter(ctx, t, cfg, first, cat, nil, "first-batch", false)
			require.NoError(t, writer.Write(ctx, []types.RawRecord{nativeTestRecord("a", "u", 3, "one")}))
			require.NoError(t, writer.Close(ctx, map[string]any{"state": `{"lsn":"0/20"}`}))
			_, err = adapter.ConfirmOrdinaryRecovery(ctx, streams, proofs, "0/20")
			require.ErrorContains(t, err, "behind")
			aheadWriter := openNativeTestWriter(ctx, t, cfg, ahead, cat, nil, "ahead-batch", false)
			require.NoError(t, aheadWriter.Close(ctx, map[string]any{"state": `{"lsn":"0/20"}`}))
			require.Equal(t, aheadOriginal.Properties()[nativeRecoveryProperty], aheadWriter.table.Properties()[nativeRecoveryProperty])
			quietWriter := openNativeTestWriter(ctx, t, cfg, quiet, cat, nil, "quiet-batch", false)
			require.NoError(t, quietWriter.Close(ctx, map[string]any{"state": `{"lsn":"0/20"}`}))
			require.Equal(t, nativeVisibleValues(t, quietOriginal), nativeVisibleValues(t, quietWriter.table))
			confirmed, err := adapter.ConfirmOrdinaryRecovery(ctx, streams, proofs, "0/20")
			require.NoError(t, err)
			require.Equal(t, "0/20", confirmed[0].CommittedLSN)
			require.Equal(t, "0/40", confirmed[1].CommittedLSN)
			require.Equal(t, "0/20", confirmed[2].CommittedLSN)
			previous := proofs
			for _, batch := range []string{"0/30", "0/40"} {
				options.CheckpointLSN = confirmed[0].CommittedLSN
				options.Plan.BatchTargetLSN = ""
				adapter = &Iceberg{config: cfg, catalog: cat}
				proofs, err = adapter.PrepareOrdinaryRecovery(ctx, streams, "0/10")
				require.NoError(t, err)
				for index, proof := range proofs {
					require.NotEqual(t, previous[index].FenceSnapshotID, proof.FenceSnapshotID)
					require.Equal(t, previous[index].TableUUID, proof.TableUUID)
					require.Equal(t, confirmed[index].CommittedLSN, proof.CommittedLSN)
				}
				ordinaryTestPlan(options, proofs, "0/10", "0/40")
				options.Plan.BatchTargetLSN = batch
				for _, stream := range streams {
					writer := openNativeTestWriter(ctx, t, cfg, stream.Self(), cat, nil, "next-batch", false)
					require.NoError(t, writer.Close(ctx, map[string]any{"state": `{"lsn":"` + batch + `"}`}))
				}
				confirmed, err = adapter.ConfirmOrdinaryRecovery(ctx, streams, proofs, batch)
				require.NoError(t, err)
				require.Equal(t, "0/40", options.Plan.TargetLSN)
				require.Equal(t, "0/40", confirmed[1].CommittedLSN)
				previous = proofs
			}
			for _, proof := range confirmed {
				require.Equal(t, "0/40", proof.CommittedLSN)
			}
		})
	}
}

func TestOrdinaryRecoveryCheckpointRestartPreflightsAllTableCoverage(t *testing.T) {
	for _, invalid := range []string{"behind-checkpoint", "above-final", "changed-plan", "regressed-ahead"} {
		t.Run(invalid, func(t *testing.T) {
			cfg, first, cat := nativeWriterFixture(t, types.UpdateTypeEquality, false, "")
			second := ordinarySecondStream(first)
			ordinaryTestSeed(t, cfg, first, cat, "0/20")
			secondTable := ordinaryTestSeed(t, cfg, second, cat, "0/40")
			cfg.CatalogType = RestCatalog
			ctx, options := ordinaryTestContext(t, "checkpoint-restart")
			options.CheckpointLSN = "0/20"
			options.Plan = &types.OrdinaryRecoveryPlan{LowerLSN: "0/10", TargetLSN: "0/40"}
			position := "0/10"
			if invalid == "above-final" {
				position = "0/50"
			}
			if invalid == "changed-plan" {
				options.Plan.LowerLSN = "0/20"
			}
			if invalid == "regressed-ahead" {
				adapter := &Iceberg{config: cfg, catalog: cat}
				streams := []types.StreamInterface{first, second}
				proofs, err := adapter.PrepareOrdinaryRecovery(ctx, streams, "0/10")
				require.NoError(t, err)
				ordinaryTestPlan(options, proofs, "0/10", "0/40")
				options.Plan.BatchTargetLSN = "0/20"
				_, _, err = cat.CommitTable(ctx, secondTable.Identifier(), nil, []table.Update{table.NewSetPropertiesUpdate(apacheiceberg.Properties{nativeRecoveryProperty: `{"state":"{\"lsn\":\"0/30\"}"}`})})
				require.NoError(t, err)
				_, err = adapter.ConfirmOrdinaryRecovery(ctx, streams, proofs, "0/20")
				require.ErrorContains(t, err, "behind")
				return
			}
			if invalid != "changed-plan" {
				_, _, err := cat.CommitTable(ctx, secondTable.Identifier(), nil, []table.Update{table.NewSetPropertiesUpdate(apacheiceberg.Properties{nativeRecoveryProperty: `{"state":"{\"lsn\":\"` + position + `\"}"}`})})
				require.NoError(t, err)
			}
			fault := &nativeCommitFaultCatalog{Catalog: cat.(*nativeCatalog).Catalog}
			_, err := (&Iceberg{config: cfg, catalog: &nativeCatalog{Catalog: fault}}).PrepareOrdinaryRecovery(ctx, []types.StreamInterface{first, second}, "0/10")
			require.Error(t, err)
			require.Zero(t, fault.calls)
		})
	}
}

func TestOrdinaryRecoveryRejectsUncompletedOrOutOfRangeBatchMetadata(t *testing.T) {
	for _, batch := range []string{"0/F", "0/50", "invalid", "0/20"} {
		plan := &types.OrdinaryRecoveryPlan{LowerLSN: "0/10", TargetLSN: "0/40", BatchTargetLSN: batch}
		_, _, err := mergeOrdinaryRecovery(`{"state":"{\"lsn\":\"0/10\"}"}`, "batch", map[string]any{"state": `{"lsn":"0/30"}`}, false, plan)
		require.Error(t, err)
	}
}
