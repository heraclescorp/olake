package abstract

import (
	"context"
	"fmt"
	"time"

	"github.com/datazip-inc/olake/types"
)

// CopyOnWriteSource is implemented by sources with complete transaction controls
// and acknowledgement that does not release the active source connection.
type CopyOnWriteSource interface {
	ValidateCopyOnWrite(context.Context, []types.StreamInterface) error
	AcknowledgeCDCCheckpoint(context.Context, any) error
}

// ValidateCopyOnWrite deliberately checks this binary, not saved discovery flags.
// Protocol calls this before dropping tables or starting destination resources.
func (a *AbstractDriver) ValidateCopyOnWrite(ctx context.Context, streams []types.StreamInterface) error {
	active := false
	for _, stream := range streams {
		if !stream.Self().CopyOnWriteEnabled() {
			continue
		}
		active = true
		if _, err := stream.Self().EffectiveCOWOptions(); err != nil {
			return fmt.Errorf("stream %s: %w", stream.ID(), err)
		}
		if stream.GetSyncMode() != types.CDC && stream.GetSyncMode() != types.STRICTCDC {
			return fmt.Errorf("stream %s: copy-on-write requires CDC or strict_cdc", stream.ID())
		}
	}
	if !active {
		return nil
	}
	source, ok := a.driver.(CopyOnWriteSource)
	if !ok {
		return fmt.Errorf("source %s does not support complete-transaction copy-on-write checkpoints", a.driver.Type())
	}
	return source.ValidateCopyOnWrite(ctx, streams)
}

type cowBatch struct {
	options types.COWOptions
	rows    int64
	bytes   int64
	started time.Time
}

func (b *cowBatch) due(now time.Time) bool {
	return b.rows > 0 && (b.rows >= int64(b.options.MaxBatchRows) ||
		b.bytes >= int64(b.options.MaxBatchBytes) ||
		now.Sub(b.started) >= time.Duration(b.options.FlushIntervalSeconds)*time.Second)
}

func cowBatches(ctx context.Context, streams []types.StreamInterface) (map[string]*cowBatch, error) {
	// These lifecycles have their own terminal publication/ACK proof. Do not
	// publish an intermediate checkpoint into either ownership protocol.
	if _, recovery := types.OrdinaryRecoveryFromContext(ctx); recovery {
		return nil, nil
	}
	if _, resync := types.ResyncFromContext(ctx); resync {
		return nil, nil
	}
	var batches map[string]*cowBatch
	for _, stream := range streams {
		if !stream.Self().CopyOnWriteEnabled() {
			continue
		}
		options, err := stream.Self().EffectiveCOWOptions()
		if err != nil {
			return nil, err
		}
		if batches == nil {
			batches = make(map[string]*cowBatch)
		}
		batches[stream.ID()] = &cowBatch{options: options, started: time.Now()}
	}
	return batches, nil
}
