package abstract

import (
	"context"
	"errors"
	"fmt"

	"github.com/datazip-inc/olake/destination"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
)

// OrdinaryRecoverySource replays only the missing prefixes of a frozen CDC catalog.
// Read deliberately does not acknowledge: the caller must first confirm every
// destination table has durably committed the exact common target.
type OrdinaryRecoverySource interface {
	ValidateOrdinaryRecovery(context.Context, []types.StreamInterface, string) (string, error)
	PrepareOrdinaryRecovery(context.Context, []types.StreamInterface, string, map[string]string) (*types.OrdinaryRecoveryPlan, error)
	AcknowledgeOrdinaryRecovery(context.Context, *types.OrdinaryRecoveryPlan) error
	CleanupOrdinaryRecovery(context.Context) error
}

func (a *AbstractDriver) ordinaryRecoverySource() (OrdinaryRecoverySource, error) {
	source, ok := a.driver.(OrdinaryRecoverySource)
	if !ok {
		return nil, fmt.Errorf("ordinary recovery is not supported by source %s", a.driver.Type())
	}
	return source, nil
}

func (a *AbstractDriver) ValidateOrdinaryRecovery(ctx context.Context, streams []types.StreamInterface, lower string) (string, error) {
	source, err := a.ordinaryRecoverySource()
	if err != nil {
		return "", err
	}
	return source.ValidateOrdinaryRecovery(ctx, streams, lower)
}

func (a *AbstractDriver) PrepareOrdinaryRecovery(ctx context.Context, streams []types.StreamInterface, lower string, positions map[string]string) (*types.OrdinaryRecoveryPlan, error) {
	if err := utils.CheckExecutionFence(ctx); err != nil {
		return nil, err
	}
	source, err := a.ordinaryRecoverySource()
	if err != nil {
		return nil, err
	}
	return source.PrepareOrdinaryRecovery(ctx, streams, lower, positions)
}

func (a *AbstractDriver) AcknowledgeOrdinaryRecovery(ctx context.Context, plan *types.OrdinaryRecoveryPlan) error {
	if err := utils.CheckExecutionFence(ctx); err != nil {
		return err
	}
	source, err := a.ordinaryRecoverySource()
	if err != nil {
		return err
	}
	return source.AcknowledgeOrdinaryRecovery(ctx, plan)
}

func (a *AbstractDriver) CleanupOrdinaryRecovery(ctx context.Context) error {
	source, err := a.ordinaryRecoverySource()
	if err != nil {
		return err
	}
	return source.CleanupOrdinaryRecovery(ctx)
}

func (a *AbstractDriver) ResumeOrdinaryBackfill(ctx context.Context, pool *destination.WriterPool, streams []types.StreamInterface) (err error) {
	options, ok := types.OrdinaryRecoveryFromContext(ctx)
	if !ok || !options.Origin.Backfill || options.BackfillState == nil || options.BackfillComplete {
		return fmt.Errorf("backfill recovery requires an immutable incomplete baseline")
	}
	if err := utils.CheckExecutionFence(ctx); err != nil {
		return err
	}
	ctx, cancel := context.WithCancel(ctx)
	a.GlobalCtxGroup = utils.NewCGroup(ctx)
	a.GlobalConnGroup = utils.NewCGroup(ctx)
	if a.driver.MaxConnections() > 0 {
		a.GlobalConnGroup = utils.NewCGroupWithLimit(ctx, a.driver.MaxConnections())
	}
	defer func() {
		cancel()
		err = errors.Join(err, a.GlobalCtxGroup.Block(), a.GlobalConnGroup.Block())
	}()
	completed := make(chan string, len(streams))
	for _, stream := range streams {
		if options.BackfillAcknowledgedLSN != "" {
			plan, err := pool.GetBackfillPlan(ctx, stream)
			if err != nil {
				return err
			}
			if err := a.state.SetBackfillPlan(stream.Self(), plan); err != nil {
				return err
			}
			binding, err := pool.PrepareBackfillTable(ctx, stream)
			if err != nil {
				return err
			}
			if binding == nil {
				return fmt.Errorf("acknowledged backfill table %s has no binding", stream.ID())
			}
			if saved := a.state.GetBackfillTable(stream.Self()); saved != nil && *saved != *binding {
				return fmt.Errorf("acknowledged backfill table %s identity or shape changed", stream.ID())
			}
			a.state.SetBackfillTable(stream.Self(), binding)
			if err := a.state.ErrBackfillPersistence(); err != nil {
				return err
			}
			if err := a.state.CompleteBackfill(stream.ID()); err != nil {
				return err
			}
			completed <- stream.ID()
			continue
		}
		if options.BackfillState.HasCompletedBackfill(stream.Self()) {
			completed <- stream.ID()
			continue
		}
		if err := a.Backfill(ctx, completed, pool, stream); err != nil {
			return err
		}
	}
	if err := a.waitForBackfillCompletion(ctx, completed, streams, func(id string) error {
		return a.state.CompleteBackfill(id)
	}); err != nil {
		return err
	}
	return errors.Join(a.state.ErrBackfillPersistence(), a.GlobalCtxGroup.Block(), a.GlobalConnGroup.Block())
}

func (a *AbstractDriver) CaptureBackfillRecoveryTarget(ctx context.Context, lower string) (string, error) {
	if err := utils.CheckExecutionFence(ctx); err != nil {
		return "", err
	}
	source, ok := a.driver.(interface {
		CaptureBackfillRecoveryTarget(context.Context, string) (string, error)
	})
	if !ok {
		return "", fmt.Errorf("backfill transaction boundary capture is not supported by source %s", a.driver.Type())
	}
	return source.CaptureBackfillRecoveryTarget(ctx, lower)
}
