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
		if !w.ordinaryPrepared && started {
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
