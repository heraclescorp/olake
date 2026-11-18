package destination

import (
	"context"
	"time"

	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils/logger"
)

// MetadataDiagnostics is optional and read-only: implementations must not create
// tables, open data writers, reconcile indexes, or change recovery checkpoints.
type MetadataDiagnostics interface {
	LogMetadataPosition(context.Context, types.StreamInterface)
}

// LogMetadataPositions runs before source PreCDC. Bound the entire diagnostic
// pass and each table read; an unavailable catalog must not hang source startup.
func (w *WriterPool) LogMetadataPositions(ctx context.Context, streams []types.StreamInterface) {
	if w.metadataDiagnostics == nil {
		return
	}
	diagnosticCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	for n, stream := range streams {
		if diagnosticCtx.Err() != nil {
			logger.Warnf("destination_metadata_diagnostics phase=pre_cdc status=incomplete remaining_tables=%d", len(streams)-n)
			return
		}
		tableCtx, cancelTable := context.WithTimeout(diagnosticCtx, 5*time.Second)
		w.metadataDiagnostics.LogMetadataPosition(tableCtx, stream)
		cancelTable()
	}
}
