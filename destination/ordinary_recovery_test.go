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
