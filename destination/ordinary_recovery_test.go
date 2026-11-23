package destination

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
	"github.com/stretchr/testify/require"
)

type poolOrdinaryRecoveryAdapter struct {
	prepared  int
	confirmed int
	proofs    []types.OrdinaryRecoveryTableProof
}

func (a *poolOrdinaryRecoveryAdapter) PrepareOrdinaryRecovery(context.Context, []types.StreamInterface, string) ([]types.OrdinaryRecoveryTableProof, error) {
	a.prepared++
	return a.proofs, nil
}

func (a *poolOrdinaryRecoveryAdapter) ConfirmOrdinaryRecovery(context.Context, []types.StreamInterface, []types.OrdinaryRecoveryTableProof, string) ([]types.OrdinaryRecoveryTableProof, error) {
	a.confirmed++
	return a.proofs, nil
}

func TestPoolOrdinaryRecoveryRequiresCompleteUnopenedSavedCatalog(t *testing.T) {
	first := types.NewStream("first", "public", nil).Wrap(0)
	second := types.NewStream("second", "public", nil).Wrap(0)
	adapter := &poolOrdinaryRecoveryAdapter{}
	pool := &WriterPool{ordinaryRecovery: adapter}
	pool.writerSchema.Store(first.ID(), &writerSchema{})
	pool.writerSchema.Store(second.ID(), &writerSchema{})
	ctx := types.WithOrdinaryRecovery(context.Background(), &types.OrdinaryRecoveryOptions{})
	_, err := pool.PrepareOrdinaryRecovery(ctx, []types.StreamInterface{first}, "0/10")
	require.ErrorContains(t, err, "complete saved")
	_, err = pool.PrepareOrdinaryRecovery(ctx, []types.StreamInterface{first, first}, "0/10")
	require.ErrorContains(t, err, "duplicate")
	require.Zero(t, adapter.prepared)
	pool.writerSchema.Store(first.ID(), &writerSchema{schema: "already open"})
	_, err = pool.PrepareOrdinaryRecovery(ctx, []types.StreamInterface{first, second}, "0/10")
	require.ErrorContains(t, err, "before opening")
	pool.writerSchema.Store(first.ID(), &writerSchema{})
	_, err = pool.PrepareOrdinaryRecovery(ctx, []types.StreamInterface{first, second}, "0/10")
	require.NoError(t, err)
	require.Equal(t, 1, adapter.prepared)
	_, err = pool.PrepareOrdinaryRecovery(ctx, []types.StreamInterface{first, second}, "0/10")
	require.ErrorContains(t, err, "already been prepared")
	_, err = pool.ConfirmOrdinaryRecovery(ctx, []types.StreamInterface{first}, nil, "0/10")
	require.ErrorContains(t, err, "complete saved")
	require.Zero(t, adapter.confirmed)
	_, err = pool.ConfirmOrdinaryRecovery(ctx, []types.StreamInterface{first, second}, nil, "0/10")
	require.NoError(t, err)
	require.Equal(t, 1, adapter.confirmed)
}

type poolOrdinaryCheckAdapter struct {
	Writer
	poolOrdinaryRecoveryAdapter
	checks int
}

func (a *poolOrdinaryCheckAdapter) Check(context.Context) error {
	a.checks++
	return fmt.Errorf("connection-test table must not be created")
}

func TestPoolOrdinaryRecoverySkipsConnectionTestTable(t *testing.T) {
	adapter := &poolOrdinaryCheckAdapter{}
	original := RegisteredWriters[types.Iceberg]
	RegisteredWriters[types.Iceberg] = func(context.Context, any) (Writer, func(context.Context), error) { return adapter, nil, nil }
	t.Cleanup(func() { RegisteredWriters[types.Iceberg] = original })
	ctx := types.WithOrdinaryRecovery(context.Background(), &types.OrdinaryRecoveryOptions{})
	stream := types.NewStream("events", "public", nil).Wrap(0)
	pool, err := NewWriterPool(ctx, &types.WriterConfig{Type: types.Iceberg}, []types.StreamInterface{stream}, 100)
	require.NoError(t, err)
	defer pool.Shutdown(ctx)
	require.Zero(t, adapter.checks)
	require.NotNil(t, pool.ordinaryRecovery)
	_, _, err = pool.NewWriter(ctx, stream)
	require.ErrorContains(t, err, "prepared barriers")
}

type poolBackfillRecoveryAdapter struct {
	poolOrdinaryRecoveryAdapter
	backfills int
	plans     int
	plan      *types.Set[types.Chunk]
}

func (a *poolBackfillRecoveryAdapter) PrepareBackfillTable(context.Context, types.StreamInterface) (*types.BackfillTableBinding, error) {
	return &types.BackfillTableBinding{TableUUID: "bound", ShapeSHA256: "shape"}, nil
}

func (a *poolBackfillRecoveryAdapter) PrepareBackfillRecovery(context.Context, []types.StreamInterface, string) error {
	a.backfills++
	return nil
}

func (a *poolBackfillRecoveryAdapter) GetBackfillPlan(context.Context, types.StreamInterface) (*types.Set[types.Chunk], error) {
	return a.plan, nil
}

func (a *poolBackfillRecoveryAdapter) PrepareBackfillPlan(_ context.Context, _ types.StreamInterface, plan *types.Set[types.Chunk]) error {
	a.plans++
	a.plan = plan
	return nil
}

func (a *poolBackfillRecoveryAdapter) BackfillRecoveryTarget(context.Context) (string, error) {
	return "", nil
}

func TestPoolBackfillRecoveryRequiresWholeSelectionAndSeparateCompletion(t *testing.T) {
	first := types.NewStream("first", "public", nil).Wrap(0)
	second := types.NewStream("second", "public", nil).Wrap(0)
	adapter := &poolBackfillRecoveryAdapter{}
	pool := &WriterPool{ordinaryRecovery: adapter}
	pool.writerSchema.Store(first.ID(), &writerSchema{})
	pool.writerSchema.Store(second.ID(), &writerSchema{})
	options := &types.OrdinaryRecoveryOptions{Origin: types.OrdinaryRecoveryOrigin{Backfill: true}}
	ctx := types.WithOrdinaryRecovery(context.Background(), options)
	err := pool.PrepareBackfillRecovery(ctx, []types.StreamInterface{first}, "0/10")
	require.ErrorContains(t, err, "complete saved")
	require.Zero(t, adapter.backfills)
	_, err = pool.GetBackfillPlan(ctx, first)
	require.ErrorContains(t, err, "not been prepared")
	require.NoError(t, pool.PrepareBackfillRecovery(ctx, []types.StreamInterface{first, second}, "0/10"))
	require.Equal(t, 1, adapter.backfills)
	plan := types.NewSet(types.Chunk{Min: nil, Max: nil})
	require.NoError(t, pool.PrepareBackfillPlan(ctx, first, plan))
	restored, err := pool.GetBackfillPlan(ctx, first)
	require.NoError(t, err)
	require.Equal(t, plan, restored)
	_, _, err = pool.NewWriter(ctx, first)
	require.ErrorContains(t, err, "backfill writers")
	_, err = pool.PrepareOrdinaryRecovery(ctx, []types.StreamInterface{first, second}, "0/10")
	require.ErrorContains(t, err, "completed prepared backfill")
	require.Zero(t, adapter.prepared)
	options.BackfillComplete = true
	pool.writerSchema.Store(first.ID(), &writerSchema{schema: "backfill writer was opened"})
	_, err = pool.PrepareOrdinaryRecovery(ctx, []types.StreamInterface{first, second}, "0/10")
	require.NoError(t, err)
	require.Equal(t, 1, adapter.prepared)
	err = pool.PrepareBackfillPlan(ctx, first, plan)
	require.ErrorContains(t, err, "not been prepared")
}

func TestPoolLegacyBackfillPlanIsOptionalWithoutControlledCheckpoint(t *testing.T) {
	stream := types.NewStream("events", "public", nil).Wrap(0)
	pool := &WriterPool{}
	plan := types.NewSet(types.Chunk{})
	require.NoError(t, pool.PrepareBackfillPlan(context.Background(), stream, plan))
	require.NoError(t, pool.PrepareBackfillPlan(types.WithResync(context.Background(), &types.ResyncOptions{}), stream, plan))
	controlled := types.WithBackfillCheckpoint(context.Background(), &types.BackfillRecoveryCheckpoint{})
	require.ErrorContains(t, pool.PrepareBackfillPlan(controlled, stream, plan), "not been prepared")
	recovery := types.WithOrdinaryRecovery(context.Background(), &types.OrdinaryRecoveryOptions{})
	require.ErrorContains(t, pool.PrepareBackfillPlan(recovery, stream, plan), "not been prepared")
}

type poolCommittedIDsAdapter struct {
	poolBackfillRecoveryAdapter
	read func(context.Context, types.StreamInterface, *types.Set[types.Chunk], types.BackfillTableBinding) (map[string]struct{}, error)
}

func (a *poolCommittedIDsAdapter) GetBackfillCommittedIDs(ctx context.Context, stream types.StreamInterface, plan *types.Set[types.Chunk], expected types.BackfillTableBinding) (map[string]struct{}, error) {
	return a.read(ctx, stream, plan, expected)
}

func TestPoolBackfillCommittedIDsAreOptionalAndFenced(t *testing.T) {
	stream := types.NewStream("events", "public", nil).Wrap(0)
	plan := types.NewSet(types.Chunk{})
	binding := types.BackfillTableBinding{TableUUID: "bound", ShapeSHA256: "shape"}
	for _, adapter := range []ordinaryRecoveryDestination{nil, &poolBackfillRecoveryAdapter{}} {
		ids, err := (&WriterPool{ordinaryRecovery: adapter}).GetBackfillCommittedIDs(context.Background(), stream, plan, binding)
		require.NoError(t, err)
		require.Nil(t, ids)
	}
	for _, mode := range []string{"checkpoint", "recovery", "standalone", "uncontrolled_checkpoint", "completed", "unprepared", "ordinary", "resync", "closed", "closed_during_read"} {
		t.Run(mode, func(t *testing.T) {
			fence := filepath.Join(t.TempDir(), "fence.json")
			require.NoError(t, os.WriteFile(fence, []byte(`{"version":1,"execution_id":"runtime","state":"open"}`), 0600))
			ctx := utils.WithExecution(context.Background(), &utils.ExecutionOptions{ID: "runtime", LaunchID: "runtime", Fence: fence, Command: "sync"})
			pool := &WriterPool{backfillPrepared: true}
			calls := 0
			pool.ordinaryRecovery = &poolCommittedIDsAdapter{read: func(_ context.Context, gotStream types.StreamInterface, gotPlan *types.Set[types.Chunk], gotBinding types.BackfillTableBinding) (map[string]struct{}, error) {
				calls++
				require.Equal(t, stream, gotStream)
				require.Same(t, plan, gotPlan)
				require.Equal(t, binding, gotBinding)
				if mode == "closed_during_read" {
					require.NoError(t, os.WriteFile(fence, []byte(`{"version":1,"execution_id":"runtime","state":"closed"}`), 0600))
				}
				return map[string]struct{}{"committed": {}}, nil
			}}
			switch mode {
			case "checkpoint", "closed", "closed_during_read":
				ctx = types.WithBackfillCheckpoint(ctx, &types.BackfillRecoveryCheckpoint{})
			case "uncontrolled_checkpoint":
				ctx = types.WithBackfillCheckpoint(context.Background(), &types.BackfillRecoveryCheckpoint{})
			case "recovery", "completed", "unprepared", "ordinary":
				ctx = types.WithOrdinaryRecovery(ctx, &types.OrdinaryRecoveryOptions{Origin: types.OrdinaryRecoveryOrigin{Backfill: mode != "ordinary"}, BackfillComplete: mode == "completed"})
				pool.backfillPrepared = mode != "unprepared"
			case "resync":
				ctx = types.WithResync(types.WithBackfillCheckpoint(ctx, &types.BackfillRecoveryCheckpoint{}), &types.ResyncOptions{})
			}
			if mode == "closed" {
				require.NoError(t, os.WriteFile(fence, []byte(`{"version":1,"execution_id":"runtime","state":"closed"}`), 0600))
			}
			ids, err := pool.GetBackfillCommittedIDs(ctx, stream, plan, binding)
			if mode == "checkpoint" || mode == "recovery" {
				require.NoError(t, err)
				require.Equal(t, map[string]struct{}{"committed": {}}, ids)
				require.Equal(t, 1, calls)
			} else {
				require.Error(t, err)
				require.Nil(t, ids)
				if mode == "closed_during_read" {
					require.Equal(t, 1, calls)
				} else {
					require.Zero(t, calls)
				}
			}
		})
	}
}
