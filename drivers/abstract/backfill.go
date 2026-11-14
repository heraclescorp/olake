package abstract

import (
	"context"
	"fmt"
	"slices"
	"sort"
	"time"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/destination"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
	"github.com/datazip-inc/olake/utils/logger"
	"github.com/datazip-inc/olake/utils/observability"
	"github.com/datazip-inc/olake/utils/typeutils"
)

func (a *AbstractDriver) Backfill(mainCtx context.Context, backfilledStreams chan string, pool *destination.WriterPool, stream types.StreamInterface) error {
	mainCtx = observability.WithTable(mainCtx, stream.Namespace(), stream.Name())
	var tableStart time.Time
	if observability.Enabled() {
		tableStart = time.Now()
	}
	chunksSet := a.state.GetResumePlan(stream.Self())
	var err error
	if chunksSet == nil || chunksSet.Len() == 0 {
		chunksSet, err = a.driver.GetOrSplitChunks(mainCtx, pool, stream)
		if err != nil {
			return fmt.Errorf("failed to get or split chunks: %w", err)
		}
		// The plan is immutable for the whole full refresh: a restarted connector reuses
		// these exact ranges so its chunk IDs still match the already committed ones.
		a.state.SetResumePlan(stream.Self(), chunksSet)
	}
	// Pending chunks shrink as chunks commit; they are what marks the full refresh complete
	// and retires the resume plan.
	if remaining := a.state.GetChunks(stream.Self()); remaining == nil || remaining.Len() == 0 {
		a.state.SetChunks(stream.Self(), types.NewSet(chunksSet.Array()...))
	}
	chunks := chunksSet.Array()
	observability.AddChunks(mainCtx, "planned", int64(len(chunks)))
	if len(chunks) == 0 {
		if backfilledStreams != nil {
			backfilledStreams <- stream.ID()
		}
		return nil
	}

	// Sort chunks by their minimum value
	sort.Slice(chunks, func(i, j int) bool {
		return typeutils.Compare(chunks[i].Min, chunks[j].Min) < 0
	})

	logger.Infof("Starting backfill for stream[%s] with %d chunks", stream.GetStream().Name, len(chunks))

	filterDataBySelectedColumnsFn := stream.RetainSelectedColumns()

	chunkProcessor := func(gCtx context.Context, chunkIndex int, chunk types.Chunk) (err error) {
		gCtx = observability.WithTable(gCtx, stream.Namespace(), stream.Name())
		observability.AddChunks(mainCtx, "active", 1)
		var chunkStart time.Time
		if observability.Enabled() {
			chunkStart = time.Now()
		}
		skipped := false
		defer func() {
			observability.AddChunks(mainCtx, "active", -1)
			if !chunkStart.IsZero() {
				observability.RecordDuration(mainCtx, "chunk", time.Since(chunkStart))
			}
			if err != nil || gCtx.Err() != nil {
				observability.AddChunks(mainCtx, "failed", 1)
			} else if skipped {
				observability.AddChunks(mainCtx, "skipped", 1)
			} else {
				observability.AddChunks(mainCtx, "completed", 1)
			}
		}()
		// create backfill context, so that main context not affected if backfill retries
		backfillCtx, backfillCtxCancel := context.WithCancel(gCtx)
		defer backfillCtxCancel()

		threadID := generateThreadID(stream.ID(), fmt.Sprintf("min[%v]-max[%v]", chunk.Min, chunk.Max))
		inserter, prevMetadataState, err := pool.NewWriter(backfillCtx, stream, destination.WithBackfill(true), destination.WithThreadID(threadID), destination.WithApplyFilter(slices.Contains(constants.FullRefreshPostReadFilterDrivers, constants.DriverType(a.driver.Type()))))
		if err != nil {
			return fmt.Errorf("failed to create new writer thread: %w", err)
		}

		defer func(ctx context.Context) {
			if ctx.Err() != nil {
				return
			}
			chunksLeft := a.state.RemoveChunk(stream.Self(), chunk)
			if chunksLeft == 0 && backfilledStreams != nil {
				if !tableStart.IsZero() {
					observability.RecordDuration(mainCtx, "table", time.Since(tableStart))
				}
				backfilledStreams <- stream.ID()
			}
			logger.Infof("finished chunk min[%v] and max[%v] of stream %s", chunk.Min, chunk.Max, stream.ID())
		}(backfillCtx)

		defer handleWriterCleanup(backfillCtx, backfillCtxCancel, &err, inserter, threadID, nil, nil)

		// The last chunk is open ended, so rows appended since the plan was cut land there.
		// It always re-runs; skipping it would leave that growth unbackfilled.
		isTailChunk := chunkIndex == len(chunks)-1
		if prevMetadataState != nil && !isTailChunk {
			if slices.Contains(prevMetadataState.FullRefreshCommittedIDs, threadID) {
				skipped = true
				logger.Infof("Thread[%s]: chunk min[%v] and max[%v] already committed, skipping", threadID, chunk.Min, chunk.Max)
				return nil
			}
		}

		logger.Infof("Thread[%s]: created writer for chunk min[%s] and max[%s] of stream %s", threadID, chunk.Min, chunk.Max, stream.ID())

		return a.driver.ChunkIterator(backfillCtx, stream, chunk, func(ctx context.Context, data map[string]any, sourceBytes int64) error {
			olakeID := utils.GetKeysHash(data, stream.GetStream().SourceDefinedPrimaryKey.Array()...)
			olakeColumns := map[string]any{
				constants.OlakeID:        olakeID,
				constants.OpType:         "r",
				constants.OlakeTimestamp: time.Now().UTC(),
			}

			// Add CDC specific columns only for CDC mode
			if stream.GetSyncMode() == types.CDC {
				olakeColumns[constants.CdcTimestamp] = time.Unix(0, 0)
			}

			filteredData := filterDataBySelectedColumnsFn(data)

			return inserter.Push(ctx, types.CreateRawRecord(filteredData, olakeColumns, nil), sourceBytes)
		})
	}
	utils.ConcurrentInGroupWithRetry(a.GlobalConnGroup, chunks, a.driver.MaxRetries(), chunkProcessor)
	return nil
}
