package abstract

import (
	"context"
	"fmt"

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
