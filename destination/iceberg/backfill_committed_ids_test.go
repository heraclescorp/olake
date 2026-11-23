package iceberg

import (
	"context"
	"encoding/json"
	"os"
	"testing"

	apacheiceberg "github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/table"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
	"github.com/stretchr/testify/require"
)

func nativeCommittedIDsFixture(t *testing.T, checkpointMode bool) (*Iceberg, context.Context, *types.ConfiguredStream, *types.Set[types.Chunk], types.BackfillTableBinding) {
	t.Helper()
	cfg, stream, cat := nativeWriterFixture(t, types.UpdateTypeEquality, false, "")
	state := nativeBackfillState()
	ctx, options := nativeBackfillRecoveryContext(t, "committed-ids-recovery", state)
	adapter := &Iceberg{config: cfg, catalog: cat}
	plan := types.NewSet(types.Chunk{Min: nil, Max: "m"}, types.Chunk{Min: "m", Max: nil})
	if checkpointMode {
		encoded, err := json.Marshal(state)
		require.NoError(t, err)
		checkpoint := &types.BackfillRecoveryCheckpoint{Version: 1, ExecutionID: "original", LaunchID: "original", SourceSHA256: options.Origin.SourceSHA256, DestinationSHA256: options.Origin.DestinationSHA256, CatalogSHA256: options.Origin.CatalogSHA256, State: encoded, ServerIdentity: options.SourceIdentity, SlotName: options.BackfillCheckpoint.SlotName}
		controlled, _ := ordinaryTestContext(t, "original")
		execution, ok := utils.ExecutionFromContext(controlled)
		require.True(t, ok)
		execution.LaunchID = ""
		ctx = types.WithBackfillCheckpoint(utils.WithExecution(context.Background(), execution), checkpoint)
		ctx = types.WithBackfillBindingJournal(ctx, nativeBackfillTestJournal(t, state))
	} else {
		require.NoError(t, adapter.PrepareBackfillRecovery(ctx, []types.StreamInterface{stream}, "0/10"))
	}
	require.NoError(t, adapter.PrepareBackfillPlan(ctx, stream, plan))
	binding, err := adapter.PrepareBackfillTable(ctx, stream)
	require.NoError(t, err)
	return adapter, ctx, stream, plan, *binding
}

func TestNativeBackfillCommittedIDsUseOnlyFreshCommittedPlanEvidence(t *testing.T) {
	for _, checkpointMode := range []bool{false, true} {
		name := "recovery"
		if checkpointMode {
			name = "checkpoint"
		}
		t.Run(name, func(t *testing.T) {
			adapter, ctx, stream, plan, binding := nativeCommittedIDsFixture(t, checkpointMode)
			first, tail := types.Chunk{Min: nil, Max: "m"}, types.Chunk{Min: "m", Max: nil}
			ids, err := adapter.GetBackfillCommittedIDs(ctx, stream, plan, binding)
			require.NoError(t, err)
			require.NotNil(t, ids)
			require.Empty(t, ids)
			writer := openNativeTestWriter(ctx, t, adapter.config, stream, adapter.catalog, nil, nativeBackfillChunkID(stream, first), true)
			require.NoError(t, writer.Write(ctx, []types.RawRecord{nativeTestRecord("a", "r", 1, "one")}))
			require.NoError(t, writer.Close(ctx, nil))
			before := writer.table.MetadataLocation()
			if options, ok := types.OrdinaryRecoveryFromContext(ctx); ok {
				// Both chunks are pending in this stale local state, including the
				// chunk whose writer has already committed to the native catalog.
				options.BackfillState.SetChunks(stream, plan)
			}
			ids, err = adapter.GetBackfillCommittedIDs(ctx, stream, plan, binding)
			require.NoError(t, err)
			require.Equal(t, map[string]struct{}{nativeBackfillChunkID(stream, first): {}}, ids)
			if options, ok := types.OrdinaryRecoveryFromContext(ctx); ok {
				// Conversely, locally completed flags cannot invent a tail commit.
				require.NoError(t, options.BackfillState.CompleteBackfill(stream.ID()))
			}
			delete(ids, nativeBackfillChunkID(stream, first))
			ids["not-owned"] = struct{}{}
			fresh, err := adapter.GetBackfillCommittedIDs(ctx, stream, plan, binding)
			require.NoError(t, err)
			require.Equal(t, map[string]struct{}{nativeBackfillChunkID(stream, first): {}}, fresh)
			current, err := adapter.catalog.LoadTable(ctx, writer.table.Identifier())
			require.NoError(t, err)
			require.Equal(t, before, current.MetadataLocation(), "snapshot evidence reads must not publish table metadata")
			emptyTail := openNativeTestWriter(ctx, t, adapter.config, stream, adapter.catalog, nil, nativeBackfillChunkID(stream, tail), true)
			require.NoError(t, emptyTail.Close(ctx, nil))
			all, err := adapter.GetBackfillCommittedIDs(ctx, stream, plan, binding)
			require.NoError(t, err)
			require.Equal(t, backfillPlanIDs(stream, plan), all)
		})
	}
}

func TestNativeBackfillCommittedIDsIntersectFullPlan(t *testing.T) {
	adapter, ctx, stream, plan, binding := nativeCommittedIDsFixture(t, true)
	first := types.Chunk{Min: nil, Max: "m"}
	metadata, err := json.Marshal(types.MetadataState{FullRefreshCommittedIDs: []string{nativeBackfillChunkID(stream, first), "another-stream_min[<nil>]-max[<nil>]"}})
	require.NoError(t, err)
	underlying := adapter.catalog.(*nativeCatalog).Catalog
	_, _, err = underlying.CommitTable(context.Background(), table.Identifier{"db", "records"}, nil, []table.Update{table.NewSetPropertiesUpdate(apacheiceberg.Properties{nativeRecoveryProperty: string(metadata)})})
	require.NoError(t, err)
	ids, err := adapter.GetBackfillCommittedIDs(ctx, stream, plan, binding)
	require.NoError(t, err)
	require.Equal(t, map[string]struct{}{nativeBackfillChunkID(stream, first): {}}, ids)
}

func TestNativeBackfillCommittedIDsRejectUnboundOrChangedEvidence(t *testing.T) {
	for _, checkpointMode := range []bool{false, true} {
		name := "recovery"
		if checkpointMode {
			name = "checkpoint"
		}
		t.Run(name, func(t *testing.T) {
			mutations := []string{"expected_uuid", "expected_shape", "replaced_uuid", "changed_shape", "owner_source", "owner_plan", "malformed_binding", "missing_binding", "plan_subset", "unprepared", "foreign_source", "foreign_execution", "foreign_launch", "missing_fence", "closed_fence", "malformed_metadata", "null_metadata", "unknown_metadata", "invalid_committed_ids", "trailing_metadata", "empty_chunk_id"}
			if !checkpointMode {
				mutations = append(mutations, "malformed_previous_marker", "missing_launch")
			}
			for _, mutation := range mutations {
				t.Run(mutation, func(t *testing.T) {
					adapter, ctx, stream, plan, binding := nativeCommittedIDsFixture(t, checkpointMode)
					first := types.Chunk{Min: nil, Max: "m"}
					writer := openNativeTestWriter(ctx, t, adapter.config, stream, adapter.catalog, nil, nativeBackfillChunkID(stream, first), true)
					require.NoError(t, writer.Close(ctx, nil))
					underlying := adapter.catalog.(*nativeCatalog).Catalog
					ident := table.Identifier{"db", "records"}
					properties := apacheiceberg.Properties{}
					switch mutation {
					case "expected_uuid":
						binding.TableUUID = "foreign-uuid"
					case "expected_shape":
						binding.ShapeSHA256 = "foreign-shape"
					case "replaced_uuid":
						copied := apacheiceberg.Properties{nativeBackfillBindingProperty: writer.table.Properties()[nativeBackfillBindingProperty], nativeRecoveryProperty: writer.table.Properties()[nativeRecoveryProperty]}
						require.NoError(t, underlying.DropTable(context.Background(), ident))
						replacement, err := loadOrCreateNativeTable(context.Background(), adapter.config, stream, underlying)
						require.NoError(t, err)
						require.NotEqual(t, binding.TableUUID, replacement.Metadata().TableUUID().String())
						properties = copied
					case "changed_shape":
						current, err := underlying.LoadTable(context.Background(), ident)
						require.NoError(t, err)
						tx := current.NewTransaction()
						require.NoError(t, tx.UpgradeFormatVersion(3))
						_, err = tx.Commit(context.Background())
						require.NoError(t, err)
					case "owner_source", "owner_plan":
						var stored nativeBackfillBinding
						require.NoError(t, json.Unmarshal([]byte(writer.table.Properties()[nativeBackfillBindingProperty]), &stored))
						if mutation == "owner_source" {
							stored.Owner.SourceIdentity = "foreign-source"
						} else {
							stored.Owner.Plan, _ = encodeBackfillPlan(stream, types.NewSet(first))
						}
						encoded, err := json.Marshal(stored)
						require.NoError(t, err)
						properties[nativeBackfillBindingProperty] = string(encoded)
					case "malformed_binding":
						properties[nativeBackfillBindingProperty] = "{"
					case "missing_binding":
						_, _, err := underlying.CommitTable(context.Background(), ident, nil, []table.Update{table.NewRemovePropertiesUpdate([]string{nativeBackfillBindingProperty})})
						require.NoError(t, err)
					case "plan_subset":
						plan = types.NewSet(first)
					case "unprepared":
						adapter = &Iceberg{config: adapter.config, catalog: adapter.catalog}
					case "foreign_source":
						if options, ok := types.OrdinaryRecoveryFromContext(ctx); ok {
							options.SourceIdentity = "foreign-source"
						} else {
							checkpoint, ok := types.BackfillCheckpointFromContext(ctx)
							require.True(t, ok)
							checkpoint.ServerIdentity = "foreign-source"
						}
					case "foreign_execution":
						execution, ok := utils.ExecutionFromContext(ctx)
						require.True(t, ok)
						changed := *execution
						changed.ID = "foreign-execution"
						ctx = utils.WithExecution(ctx, &changed)
					case "foreign_launch", "missing_launch":
						execution, ok := utils.ExecutionFromContext(ctx)
						require.True(t, ok)
						changed := *execution
						changed.LaunchID = "foreign-launch"
						if mutation == "missing_launch" {
							changed.LaunchID = ""
						}
						ctx = utils.WithExecution(ctx, &changed)
					case "missing_fence":
						ctx = utils.WithExecution(ctx, &utils.ExecutionOptions{ID: "runtime", LaunchID: "original", Command: "sync"})
					case "closed_fence":
						execution, ok := utils.ExecutionFromContext(ctx)
						require.True(t, ok)
						encoded, err := json.Marshal(map[string]any{"version": 1, "execution_id": execution.ID, "state": "closed"})
						require.NoError(t, err)
						require.NoError(t, os.WriteFile(execution.Fence, encoded, 0600))
					case "malformed_metadata":
						properties[nativeRecoveryProperty] = "{"
					case "null_metadata":
						properties[nativeRecoveryProperty] = "null"
					case "unknown_metadata":
						properties[nativeRecoveryProperty] = `{"unknown":"field"}`
					case "invalid_committed_ids":
						properties[nativeRecoveryProperty] = `{"full_refresh_committed_ids":[42]}`
					case "trailing_metadata":
						properties[nativeRecoveryProperty] = writer.table.Properties()[nativeRecoveryProperty] + `{}`
					case "empty_chunk_id":
						properties[nativeRecoveryProperty] = `{"full_refresh_committed_ids":[""]}`
					case "malformed_previous_marker":
						properties[ordinaryRecoveryProperty] = "{"
					}
					if len(properties) != 0 {
						_, _, err := underlying.CommitTable(context.Background(), ident, nil, []table.Update{table.NewSetPropertiesUpdate(properties)})
						require.NoError(t, err)
					}
					ids, err := adapter.GetBackfillCommittedIDs(ctx, stream, plan, binding)
					require.Error(t, err)
					require.Nil(t, ids)
				})
			}
		})
	}
}

type nativeCommittedIDsFenceCatalog struct {
	catalog.Catalog
	afterLoad func()
}

func (c *nativeCommittedIDsFenceCatalog) LoadTable(ctx context.Context, ident table.Identifier) (*table.Table, error) {
	tbl, err := c.Catalog.LoadTable(ctx, ident)
	if err == nil {
		c.afterLoad()
	}
	return tbl, err
}

func TestNativeBackfillCommittedIDsRecheckFenceBeforeReturningEvidence(t *testing.T) {
	adapter, ctx, stream, plan, binding := nativeCommittedIDsFixture(t, true)
	writer := openNativeTestWriter(ctx, t, adapter.config, stream, adapter.catalog, nil, nativeBackfillChunkID(stream, types.Chunk{Min: nil, Max: "m"}), true)
	require.NoError(t, writer.Close(ctx, nil))
	execution, ok := utils.ExecutionFromContext(ctx)
	require.True(t, ok)
	adapter.catalog = &nativeCommittedIDsFenceCatalog{Catalog: adapter.catalog, afterLoad: func() {
		encoded, err := json.Marshal(map[string]any{"version": 1, "execution_id": execution.ID, "state": "closed"})
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(execution.Fence, encoded, 0600))
	}}
	ids, err := adapter.GetBackfillCommittedIDs(ctx, stream, plan, binding)
	require.ErrorContains(t, err, "fence")
	require.Nil(t, ids)
}

func TestNativeBackfillCommittedIDsRejectLostEvidenceOnDataBearingTable(t *testing.T) {
	for _, checkpointMode := range []bool{false, true} {
		name := "recovery"
		if checkpointMode {
			name = "checkpoint"
		}
		t.Run(name, func(t *testing.T) {
			for _, metadata := range []string{"missing", "{}", `{"full_refresh_committed_ids":[]}`, "null", "{"} {
				t.Run(metadata, func(t *testing.T) {
					adapter, ctx, stream, plan, binding := nativeCommittedIDsFixture(t, checkpointMode)
					writer := openNativeTestWriter(ctx, t, adapter.config, stream, adapter.catalog, nil, nativeBackfillChunkID(stream, types.Chunk{Min: nil, Max: "m"}), true)
					require.NoError(t, writer.Write(ctx, []types.RawRecord{nativeTestRecord("a", "r", 1, "one")}))
					require.NoError(t, writer.Close(ctx, nil))
					require.NotNil(t, writer.table.CurrentSnapshot())
					var update table.Update
					if metadata == "missing" {
						update = table.NewRemovePropertiesUpdate([]string{nativeRecoveryProperty})
					} else {
						update = table.NewSetPropertiesUpdate(apacheiceberg.Properties{nativeRecoveryProperty: metadata})
					}
					_, _, err := adapter.catalog.(*nativeCatalog).Catalog.CommitTable(context.Background(), writer.table.Identifier(), nil, []table.Update{update})
					require.NoError(t, err)
					ids, err := adapter.GetBackfillCommittedIDs(ctx, stream, plan, binding)
					require.Error(t, err)
					require.Nil(t, ids, "lost authoritative metadata must not be interpreted as permission to recopy committed rows")
				})
			}
		})
	}
}

func TestNativeBackfillCommittedIDsRejectUnsafeLocalCoordination(t *testing.T) {
	for _, checkpointMode := range []bool{false, true} {
		name := "recovery"
		if checkpointMode {
			name = "checkpoint"
		}
		t.Run(name, func(t *testing.T) {
			for _, condition := range []string{"unknown", "closed", "active_writer", "active_resync"} {
				t.Run(condition, func(t *testing.T) {
					adapter, ctx, stream, plan, binding := nativeCommittedIDsFixture(t, checkpointMode)
					state := nativeTableEntry(adapter.catalog, table.Identifier{"db", "records"})
					var writer *nativeWriter
					if condition == "active_writer" {
						writer = openNativeTestWriter(ctx, t, adapter.config, stream, adapter.catalog, nil, nativeBackfillChunkID(stream, types.Chunk{Min: nil, Max: "m"}), true)
					} else {
						state.mu.Lock()
						switch condition {
						case "unknown":
							state.invalidate(true)
						case "closed":
							state.closed = true
						case "active_resync":
							state.resync = &resyncControl{}
						}
						state.mu.Unlock()
					}
					ids, err := adapter.GetBackfillCommittedIDs(ctx, stream, plan, binding)
					require.Error(t, err)
					require.Nil(t, ids)
					if condition == "unknown" {
						require.ErrorContains(t, err, "commit outcome is unknown")
					} else if condition == "closed" {
						require.ErrorContains(t, err, "catalog is closed")
					} else {
						require.ErrorContains(t, err, "all local writers and resync work")
					}
					if writer != nil {
						require.NoError(t, writer.Close(ctx, nil))
						ids, err = adapter.GetBackfillCommittedIDs(ctx, stream, plan, binding)
						require.NoError(t, err)
						require.Contains(t, ids, nativeBackfillChunkID(stream, types.Chunk{Min: nil, Max: "m"}))
					}
				})
			}
		})
	}
}
