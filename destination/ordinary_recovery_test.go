package destination

import (
	"context"
	"fmt"
	"testing"

	"github.com/datazip-inc/olake/types"
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
