package abstract

import (
	"context"
	"fmt"

	"github.com/datazip-inc/olake/types"
)

// AtomicResyncSource must establish a source-consistent replacement without advancing
// its acknowledgement until the destination publishes the complete saved checkpoint.
type AtomicResyncSource interface {
	ValidateResync(context.Context, types.StreamInterface) error
	PrepareResync(context.Context, types.StreamInterface) error
	ResyncCheckpoint(context.Context, *types.State) error
	RecoverResync(context.Context, *types.State, types.StreamInterface) error
	CleanupResync(context.Context) error
}

func (a *AbstractDriver) resyncSource() (AtomicResyncSource, error) {
	source, ok := a.driver.(AtomicResyncSource)
	if !ok {
		return nil, fmt.Errorf("atomic table resync is not supported by source %s", a.driver.Type())
	}
	return source, nil
}

func (a *AbstractDriver) ValidateResync(ctx context.Context, target types.StreamInterface) error {
	source, err := a.resyncSource()
	if err != nil {
		return err
	}
	return source.ValidateResync(ctx, target)
}

func (a *AbstractDriver) PrepareResync(ctx context.Context, target types.StreamInterface) error {
	source, err := a.resyncSource()
	if err != nil {
		return err
	}
	return source.PrepareResync(ctx, target)
}

func (a *AbstractDriver) RecoverResync(ctx context.Context, checkpoint *types.State, target types.StreamInterface) error {
	source, err := a.resyncSource()
	if err != nil {
		return err
	}
	return source.RecoverResync(ctx, checkpoint, target)
}

func (a *AbstractDriver) CleanupResync(ctx context.Context) error {
	source, err := a.resyncSource()
	if err != nil {
		return err
	}
	return source.CleanupResync(ctx)
}

func (a *AbstractDriver) completeRead(ctx context.Context, publish func(context.Context, *types.State) error) error {
	if err := a.GlobalCtxGroup.Block(); err != nil {
		return fmt.Errorf("error occurred while waiting for context groups: %w", err)
	}
	if err := a.GlobalConnGroup.Block(); err != nil {
		return fmt.Errorf("error occurred while waiting for connections: %w", err)
	}
	if _, resync := types.ResyncFromContext(ctx); resync {
		return a.finishResync(ctx, publish)
	}
	return nil
}

// finishResync runs only after both read groups have joined, including every writer close.
func (a *AbstractDriver) finishResync(ctx context.Context, publish func(context.Context, *types.State) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	source, err := a.resyncSource()
	if err != nil {
		return err
	}
	checkpoint, err := a.state.Clone()
	if err != nil {
		return fmt.Errorf("failed to clone resync checkpoint: %w", err)
	}
	if err := source.ResyncCheckpoint(ctx, checkpoint); err != nil {
		return fmt.Errorf("failed to finalize resync checkpoint: %w", err)
	}
	if err := publish(ctx, checkpoint); err != nil {
		return fmt.Errorf("failed to publish atomic resync: %w", err)
	}
	for _, identifier := range a.resyncPostCDC {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := a.driver.PostCDC(ctx, identifier); err != nil {
			return fmt.Errorf("published resync source acknowledgement failed: %w", err)
		}
	}
	return nil
}
