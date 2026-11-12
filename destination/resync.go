package destination

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/datazip-inc/olake/pkg/indexdb"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
)

const ResyncMaxDuration = 12 * time.Hour

type resyncDestination interface {
	PrepareResync(context.Context, types.StreamInterface, string, *types.State) (*types.State, error)
	PublishResync(context.Context, *types.State) error
	AbortResync(context.Context) error
	ResyncPublished() bool
	ResyncReceipt() (string, int64)
}

// PrepareResync checks the durable completion marker before allocating a fresh attempt index.
func (w *WriterPool) PrepareResync(ctx context.Context, stream types.StreamInterface, operationID string, baseline *types.State) (*types.State, error) {
	if w.resync == nil {
		return nil, fmt.Errorf("destination does not support atomic table resync")
	}
	if strings.TrimSpace(operationID) == "" {
		return nil, fmt.Errorf("resync operation ID must not be blank")
	}
	if baseline == nil {
		return nil, fmt.Errorf("atomic resync requires the full baseline checkpoint")
	}
	if err := utils.CheckExecutionFence(ctx); err != nil {
		return nil, err
	}
	deadline, ok := ctx.Deadline()
	if !ok || time.Until(deadline) > ResyncMaxDuration {
		return nil, fmt.Errorf("atomic resync requires a deadline no more than 12 hours away")
	}
	value, ok := w.writerSchema.Load(stream.ID())
	if !ok {
		return nil, fmt.Errorf("resync target stream[%s] is not in the saved catalog", stream.ID())
	}
	artifact := value.(*writerSchema)
	artifact.mu.Lock()
	defer artifact.mu.Unlock()
	if artifact.schema != nil || artifact.streamIndex != nil {
		return nil, fmt.Errorf("resync must be prepared before opening target writers")
	}
	checkpoint, err := w.resync.PrepareResync(ctx, stream, operationID, baseline)
	if err != nil || checkpoint != nil {
		return checkpoint, err
	}
	if options, ok := types.ResyncFromContext(ctx); ok && options.RecoverOnly {
		return nil, fmt.Errorf("UNKNOWN: recovery-only resync has no confirmed durable publication checkpoint")
	}
	if artifact.needsIndex {
		artifact.streamIndex, err = indexdb.OpenAttempt(stream)
		if err != nil {
			cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
			defer cancel()
			return nil, errors.Join(err, w.resync.AbortResync(cleanupCtx))
		}
	}
	return nil, nil
}

func (w *WriterPool) PublishResync(ctx context.Context, checkpoint *types.State) error {
	if w.resync == nil {
		return fmt.Errorf("destination does not support atomic table resync")
	}
	if options, ok := types.ResyncFromContext(ctx); ok && options.RecoverOnly {
		return fmt.Errorf("recovery-only resync cannot publish replacement data")
	}
	if err := utils.CheckExecutionFence(ctx); err != nil {
		return err
	}
	return w.resync.PublishResync(ctx, checkpoint)
}

func (w *WriterPool) AbortResync(ctx context.Context) error {
	if w.resync == nil {
		return fmt.Errorf("atomic resync destination has not been initialized")
	}
	return w.resync.AbortResync(ctx)
}

func (w *WriterPool) ResyncPublished() bool {
	return w.resync != nil && w.resync.ResyncPublished()
}

func (w *WriterPool) ResyncReceipt() (string, int64) {
	if w.resync == nil {
		return "", 0
	}
	return w.resync.ResyncReceipt()
}

// FinishResync reports cleanup failures before the CLI emits a success receipt.
func (w *WriterPool) FinishResync(ctx context.Context) error {
	var cleanupErr error
	w.writerSchema.Range(func(_, value any) bool {
		artifact := value.(*writerSchema)
		artifact.mu.Lock()
		defer artifact.mu.Unlock()
		if artifact.streamIndex != nil {
			cleanupErr = errors.Join(cleanupErr, artifact.streamIndex.Close())
			artifact.streamIndex = nil
		}
		return true
	})
	if finisher, ok := w.resync.(interface{ FinishResync(context.Context) error }); ok {
		cleanupErr = errors.Join(cleanupErr, finisher.FinishResync(ctx))
	}
	if w.shutdown != nil {
		w.shutdown(ctx)
		w.shutdown = nil
	}
	return cleanupErr
}
