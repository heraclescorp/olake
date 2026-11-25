package destination

import (
	"context"
	"fmt"

	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
)

type ordinaryRecoveryDestination interface {
	PrepareOrdinaryRecovery(context.Context, []types.StreamInterface, string) ([]types.OrdinaryRecoveryTableProof, error)
	ConfirmOrdinaryRecovery(context.Context, []types.StreamInterface, []types.OrdinaryRecoveryTableProof, string) ([]types.OrdinaryRecoveryTableProof, error)
}

type backfillRecoveryDestination interface {
	PrepareBackfillTable(context.Context, types.StreamInterface) (*types.BackfillTableBinding, error)
	PrepareBackfillRecovery(context.Context, []types.StreamInterface, string) error
	GetBackfillPlan(context.Context, types.StreamInterface) (*types.Set[types.Chunk], error)
	PrepareBackfillPlan(context.Context, types.StreamInterface, *types.Set[types.Chunk]) error
	BackfillRecoveryTarget(context.Context) (string, error)
}

type backfillCommittedIDsDestination interface {
	GetBackfillCommittedIDs(context.Context, types.StreamInterface, *types.Set[types.Chunk], types.BackfillTableBinding) (map[string]struct{}, error)
}

// GetBackfillCommittedIDs returns only destination-authoritative evidence. A nil
// result preserves the per-writer checks for destinations without this capability.
func (w *WriterPool) GetBackfillCommittedIDs(ctx context.Context, stream types.StreamInterface, plan *types.Set[types.Chunk], expected types.BackfillTableBinding) (map[string]struct{}, error) {
	adapter, supported := w.ordinaryRecovery.(backfillCommittedIDsDestination)
	if !supported {
		return nil, nil
	}
	execution, controlled := utils.ExecutionFromContext(ctx)
	if !controlled || execution.ID == "" || execution.Fence == "" || execution.Command != "sync" {
		return nil, fmt.Errorf("backfill committed IDs require a controlled sync execution")
	}
	_, checkpoint := types.BackfillCheckpointFromContext(ctx)
	options, recovery := types.OrdinaryRecoveryFromContext(ctx)
	_, resync := types.ResyncFromContext(ctx)
	if resync || (!checkpoint && !recovery) || (recovery && (execution.LaunchID == "" || !options.Origin.Backfill || options.BackfillComplete || !w.backfillPrepared || w.ordinaryPrepared)) {
		return nil, fmt.Errorf("backfill committed IDs require a controlled checkpoint or prepared incomplete backfill recovery")
	}
	if err := utils.CheckExecutionFence(ctx); err != nil {
		return nil, err
	}
	committed, err := adapter.GetBackfillCommittedIDs(ctx, stream, plan, expected)
	if err != nil {
		return nil, err
	}
	if err := utils.CheckExecutionFence(ctx); err != nil {
		return nil, err
	}
	return committed, nil
}

func (w *WriterPool) PrepareBackfillTable(ctx context.Context, stream types.StreamInterface) (*types.BackfillTableBinding, error) {
	adapter, ok := w.ordinaryRecovery.(backfillRecoveryDestination)
	if !ok {
		return nil, fmt.Errorf("destination does not support durable backfill table bindings")
	}
	if err := utils.CheckExecutionFence(ctx); err != nil {
		return nil, err
	}
	return adapter.PrepareBackfillTable(ctx, stream)
}

func (w *WriterPool) PrepareBackfillRecovery(ctx context.Context, streams []types.StreamInterface, lower string) error {
	options, ok := types.OrdinaryRecoveryFromContext(ctx)
	adapter, supported := w.ordinaryRecovery.(backfillRecoveryDestination)
	if !ok || !supported || !options.Origin.Backfill || (options.BackfillComplete && options.CheckpointLSN == "") || w.backfillPrepared || w.ordinaryPrepared {
		return fmt.Errorf("destination has no unprepared backfill recovery")
	}
	if err := utils.CheckExecutionFence(ctx); err != nil {
		return err
	}
	if err := w.recoveryStreams(streams); err != nil {
		return err
	}
	if err := adapter.PrepareBackfillRecovery(ctx, streams, lower); err != nil {
		return err
	}
	w.backfillPrepared = true
	return nil
}

func (w *WriterPool) GetBackfillPlan(ctx context.Context, stream types.StreamInterface) (*types.Set[types.Chunk], error) {
	adapter, ok := w.ordinaryRecovery.(backfillRecoveryDestination)
	if !ok || !w.backfillPrepared || w.ordinaryPrepared {
		return nil, fmt.Errorf("backfill recovery tables have not been prepared")
	}
	if err := utils.CheckExecutionFence(ctx); err != nil {
		return nil, err
	}
	return adapter.GetBackfillPlan(ctx, stream)
}

func (w *WriterPool) PrepareBackfillPlan(ctx context.Context, stream types.StreamInterface, plan *types.Set[types.Chunk]) error {
	_, recovery := types.OrdinaryRecoveryFromContext(ctx)
	_, checkpoint := types.BackfillCheckpointFromContext(ctx)
	if !recovery && !checkpoint {
		return nil
	}
	adapter, ok := w.ordinaryRecovery.(backfillRecoveryDestination)
	if !ok || (recovery && (!w.backfillPrepared || w.ordinaryPrepared)) {
		return fmt.Errorf("backfill recovery tables have not been prepared")
	}
	if err := utils.CheckExecutionFence(ctx); err != nil {
		return err
	}
	return adapter.PrepareBackfillPlan(ctx, stream, plan)
}

func (w *WriterPool) BackfillRecoveryTarget(ctx context.Context) (string, error) {
	adapter, ok := w.ordinaryRecovery.(backfillRecoveryDestination)
	if !ok || !w.backfillPrepared || w.ordinaryPrepared {
		return "", fmt.Errorf("backfill recovery tables have not been prepared")
	}
	if err := utils.CheckExecutionFence(ctx); err != nil {
		return "", err
	}
	return adapter.BackfillRecoveryTarget(ctx)
}

// recoveryStreams requires the entire frozen selection, not a convenient subset.
func (w *WriterPool) recoveryStreams(streams []types.StreamInterface) error {
	selected := make(map[string]struct{}, len(streams))
	for _, stream := range streams {
		if stream == nil {
			return fmt.Errorf("ordinary recovery contains a nil stream")
		}
		if _, duplicate := selected[stream.ID()]; duplicate {
			return fmt.Errorf("ordinary recovery contains duplicate stream %q", stream.ID())
		}
		selected[stream.ID()] = struct{}{}
		value, ok := w.writerSchema.Load(stream.ID())
		if !ok {
			return fmt.Errorf("ordinary recovery stream %q is outside the saved catalog", stream.ID())
		}
		artifact := value.(*writerSchema)
		artifact.mu.RLock()
		started := artifact.schema != nil || artifact.streamIndex != nil
		artifact.mu.RUnlock()
		if !w.ordinaryPrepared && !w.backfillPrepared && started {
			return fmt.Errorf("ordinary recovery must be prepared before opening writers")
		}
	}
	count := 0
	w.writerSchema.Range(func(_, _ any) bool { count++; return true })
	if len(selected) == 0 || len(selected) != count {
		return fmt.Errorf("ordinary recovery requires the complete saved selected catalog")
	}
	return nil
}

func (w *WriterPool) PrepareOrdinaryRecovery(ctx context.Context, streams []types.StreamInterface, lower string) ([]types.OrdinaryRecoveryTableProof, error) {
	if _, ok := types.OrdinaryRecoveryFromContext(ctx); !ok || w.ordinaryRecovery == nil {
		return nil, fmt.Errorf("destination does not support this ordinary recovery")
	}
	if err := utils.CheckExecutionFence(ctx); err != nil {
		return nil, err
	}
	if w.ordinaryPrepared {
		return nil, fmt.Errorf("ordinary recovery has already been prepared on this writer pool")
	}
	if err := w.recoveryStreams(streams); err != nil {
		return nil, err
	}
	if options, _ := types.OrdinaryRecoveryFromContext(ctx); options.Origin.Backfill {
		if !options.BackfillComplete {
			return nil, fmt.Errorf("ordinary recovery requires completed prepared backfill")
		}
		if !w.backfillPrepared {
			if options.CheckpointLSN == "" {
				return nil, fmt.Errorf("ordinary recovery requires completed prepared backfill")
			}
			if err := w.PrepareBackfillRecovery(ctx, streams, lower); err != nil {
				return nil, err
			}
		}
	}
	proofs, err := w.ordinaryRecovery.PrepareOrdinaryRecovery(ctx, streams, lower)
	if err == nil {
		w.ordinaryPrepared = true
	}
	return proofs, err
}

func (w *WriterPool) ConfirmOrdinaryRecovery(ctx context.Context, streams []types.StreamInterface, proofs []types.OrdinaryRecoveryTableProof, target string) ([]types.OrdinaryRecoveryTableProof, error) {
	if _, ok := types.OrdinaryRecoveryFromContext(ctx); !ok || w.ordinaryRecovery == nil || !w.ordinaryPrepared {
		return nil, fmt.Errorf("ordinary recovery has not been prepared")
	}
	if err := utils.CheckExecutionFence(ctx); err != nil {
		return nil, err
	}
	if err := w.recoveryStreams(streams); err != nil {
		return nil, err
	}
	return w.ordinaryRecovery.ConfirmOrdinaryRecovery(ctx, streams, proofs, target)
}
