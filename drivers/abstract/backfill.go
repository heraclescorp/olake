package abstract

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/destination"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
	"github.com/datazip-inc/olake/utils/logger"
	"github.com/datazip-inc/olake/utils/observability"
	"github.com/datazip-inc/olake/utils/typeutils"
	"github.com/jackc/pglogrepl"
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
		if options, recovery := types.OrdinaryRecoveryFromContext(mainCtx); recovery && options.Origin.Backfill {
			chunksSet, err = pool.GetBackfillPlan(mainCtx, stream)
			if err != nil {
				return fmt.Errorf("load durable backfill plan: %w", err)
			}
		}
		if chunksSet == nil || chunksSet.Len() == 0 {
			chunksSet, err = a.driver.GetOrSplitChunks(mainCtx, pool, stream)
			if err != nil {
				return fmt.Errorf("failed to get or split chunks: %w", err)
			}
			options, recovery := types.OrdinaryRecoveryFromContext(mainCtx)
			if a.driver.Type() == string(constants.Postgres) && stream.GetSyncMode() == types.CDC && (a.state.BackfillCheckpoint || (recovery && options.Origin.Backfill)) {
				chunksSet, err = canonicalPostgresBackfillPlan(stream, chunksSet)
				if err != nil {
					return err
				}
			}
		}
		if stream.GetSyncMode() == types.CDC && (chunksSet == nil || chunksSet.Len() == 0) {
			chunksSet = types.NewSet(types.Chunk{})
		}
		if chunksSet != nil && chunksSet.Len() > 0 {
			if err := a.state.SetBackfillPlan(stream.Self(), chunksSet); err != nil {
				return err
			}
		}
	}
	if err := pool.PrepareBackfillPlan(mainCtx, stream, chunksSet); err != nil {
		return fmt.Errorf("persist durable backfill plan: %w", err)
	}
	// Pending chunks shrink as chunks commit; they are what marks the full refresh complete
	// and retires the resume plan.
	if remaining := a.state.GetChunks(stream.Self()); remaining == nil || remaining.Len() == 0 {
		a.state.SetChunks(stream.Self(), types.NewSet(chunksSet.Array()...))
	}
	options, recovery := types.OrdinaryRecoveryFromContext(mainCtx)
	var tableBinding *types.BackfillTableBinding
	if a.state.BackfillCheckpoint || (recovery && options.Origin.Backfill) {
		binding, err := pool.PrepareBackfillTable(mainCtx, stream)
		if err != nil {
			return err
		}
		if binding == nil {
			return fmt.Errorf("backfill table %s has no durable binding", stream.ID())
		}
		if saved := a.state.GetBackfillTable(stream.Self()); saved != nil && *saved != *binding {
			return fmt.Errorf("backfill table %s identity or shape changed", stream.ID())
		}
		tableBinding = binding
		a.state.SetBackfillTable(stream.Self(), binding)
		if err := a.state.ErrBackfillPersistence(); err != nil {
			return err
		}
	}
	chunks := chunksSet.Array()
	observability.AddChunks(mainCtx, "planned", int64(len(chunks)))
	if len(chunks) == 0 {
		if stream.GetSyncMode() == types.CDC {
			if err := a.state.CompleteBackfill(stream.ID()); err != nil {
				return err
			}
		}
		if backfilledStreams != nil {
			backfilledStreams <- stream.ID()
		}
		return nil
	}

	// Sort chunks by their minimum value
	sort.Slice(chunks, func(i, j int) bool {
		return typeutils.Compare(chunks[i].Min, chunks[j].Min) < 0
	})
	if tableBinding != nil && (canSkipCheckpointedTail(a.state, stream, a.driver.Type()) || (recovery && options.Origin.Backfill)) {
		committed, err := pool.GetBackfillCommittedIDs(mainCtx, stream, chunksSet, *tableBinding)
		if err != nil {
			return fmt.Errorf("validate committed backfill chunks: %w", err)
		}
		if committed != nil {
			pending := make([]types.Chunk, 0, len(chunks))
			planIDs := make(map[string]struct{}, len(chunks))
			for _, chunk := range chunks {
				id := generateThreadID(stream.ID(), fmt.Sprintf("min[%v]-max[%v]", chunk.Min, chunk.Max))
				planIDs[id] = struct{}{}
				if _, done := committed[id]; !done {
					pending = append(pending, chunk)
				}
			}
			for id := range committed {
				if _, exists := planIDs[id]; !exists {
					return fmt.Errorf("committed backfill evidence is outside the immutable plan")
				}
			}
			if err := mainCtx.Err(); err != nil {
				return err
			}
			if err := utils.CheckExecutionFence(mainCtx); err != nil {
				return err
			}
			if err := a.state.ErrBackfillPersistence(); err != nil {
				return err
			}
			pendingSet := types.NewSet(pending...)
			previous := a.state.GetChunks(stream.Self())
			if previous != nil && !previous.SubsetOf(chunksSet) {
				return fmt.Errorf("pending backfill chunks are outside the immutable plan")
			}
			if previous == nil || previous.Len() != pendingSet.Len() || !previous.SubsetOf(pendingSet) {
				a.state.SetChunks(stream.Self(), pendingSet)
				if err := a.state.ErrBackfillPersistence(); err != nil {
					return err
				}
			}
			skipped := len(chunks) - len(pending)
			observability.AddChunks(mainCtx, "skipped", int64(skipped))
			logger.Infof("stream[%s]: verified %d committed backfill chunks; scheduling %d pending chunks", stream.ID(), skipped, len(pending))
			chunks = pending
			if len(chunks) == 0 {
				if err := mainCtx.Err(); err != nil {
					return err
				}
				if err := utils.CheckExecutionFence(mainCtx); err != nil {
					return err
				}
				if stream.GetSyncMode() == types.CDC {
					if err := a.state.CompleteBackfill(stream.ID()); err != nil {
						return err
					}
				}
				if !tableStart.IsZero() {
					observability.RecordDuration(mainCtx, "table", time.Since(tableStart))
				}
				if backfilledStreams != nil {
					select {
					case backfilledStreams <- stream.ID():
					case <-mainCtx.Done():
						return mainCtx.Err()
					}
				}
				return nil
			}
		}
	}

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

		if err := a.state.ErrBackfillPersistence(); err != nil {
			return err
		}
		threadID := generateThreadID(stream.ID(), fmt.Sprintf("min[%v]-max[%v]", chunk.Min, chunk.Max))
		inserter, prevMetadataState, err := pool.NewWriter(backfillCtx, stream, destination.WithBackfill(true), destination.WithThreadID(threadID), destination.WithApplyFilter(slices.Contains(constants.FullRefreshPostReadFilterDrivers, constants.DriverType(a.driver.Type()))))
		if err != nil {
			return fmt.Errorf("failed to create new writer thread: %w", err)
		}

		defer func(ctx context.Context) {
			if ctx.Err() != nil {
				return
			}
			if persistErr := a.state.ErrBackfillPersistence(); persistErr != nil {
				err = persistErr
				backfillCtxCancel()
				return
			}
			chunksLeft := a.state.RemoveChunk(stream.Self(), chunk)
			if persistErr := a.state.ErrBackfillPersistence(); persistErr != nil {
				err = persistErr
				backfillCtxCancel()
				return
			}
			if chunksLeft == 0 && backfilledStreams != nil {
				if stream.GetSyncMode() == types.CDC {
					if completeErr := a.state.CompleteBackfill(stream.ID()); completeErr != nil {
						err = completeErr
						backfillCtxCancel()
						return
					}
				}
				if !tableStart.IsZero() {
					observability.RecordDuration(mainCtx, "table", time.Since(tableStart))
				}
				backfilledStreams <- stream.ID()
			}
			logger.Infof("finished backfill chunk for stream %s", stream.ID())
		}(backfillCtx)

		defer handleWriterCleanup(backfillCtx, backfillCtxCancel, &err, inserter, threadID, nil, nil)

		isTailChunk := chunkIndex == len(chunks)-1
		options, recovery := types.OrdinaryRecoveryFromContext(mainCtx)
		if prevMetadataState != nil && (!isTailChunk || (recovery && options.Origin.Backfill) || canSkipCheckpointedTail(a.state, stream, a.driver.Type())) {
			if slices.Contains(prevMetadataState.FullRefreshCommittedIDs, threadID) {
				skipped = true
				logger.Infof("backfill chunk already committed for stream %s, skipping", stream.ID())
				return nil
			}
		}

		logger.Infof("created backfill writer for stream %s", stream.ID())

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

func canSkipCheckpointedTail(state *types.State, stream types.StreamInterface, driver string) bool {
	if driver != string(constants.Postgres) || stream.GetSyncMode() != types.CDC || !state.BackfillCheckpoint || state.GetBackfillTable(stream.Self()) == nil {
		return false
	}
	plan := state.GetBackfillPlan(stream.Self())
	global := state.GetGlobal()
	if plan == nil || plan.Len() == 0 || global == nil || global.State == nil {
		return false
	}
	var checkpoint struct {
		LSN string `json:"lsn"`
	}
	if utils.Unmarshal(global.State, &checkpoint) != nil {
		return false
	}
	lower, err := pglogrepl.ParseLSN(checkpoint.LSN)
	return err == nil && lower != 0
}

func canonicalPostgresBackfillPlan(stream types.StreamInterface, plan *types.Set[types.Chunk]) (*types.Set[types.Chunk], error) {
	if plan == nil {
		return nil, nil
	}
	chunks := plan.Array()
	column := stream.Self().StreamMetadata.ChunkColumn
	columnType, _ := stream.Schema().GetType(column)
	numeric := columnType == types.Int64 || columnType == types.Float64 || types.BaseOf(columnType) == types.Decimal
	quote := func(value string) string { return "'" + strings.ReplaceAll(value, "'", "''") + "'" }
	boundary := func(value any) any {
		switch value := value.(type) {
		case time.Time:
			return quote(value.Format(time.RFC3339Nano))
		case []byte:
			if columnType == types.Binary {
				return "decode('" + hex.EncodeToString(value) + "','hex')"
			}
			if numeric {
				return json.Number(string(value))
			}
			return quote(string(value))
		case string:
			if column != "" {
				if numeric {
					return json.Number(value)
				}
				return quote(value)
			}
			return value
		default:
			return value
		}
	}
	for index := range chunks {
		chunks[index].Min = boundary(chunks[index].Min)
		chunks[index].Max = boundary(chunks[index].Max)
	}
	data, err := json.Marshal(chunks)
	if err != nil {
		return nil, fmt.Errorf("encode durable PostgreSQL backfill plan: %w", err)
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	if err := decoder.Decode(&chunks); err != nil {
		return nil, fmt.Errorf("decode durable PostgreSQL backfill plan: %w", err)
	}
	return types.NewSet(chunks...), nil
}
