package iceberg

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/datazip-inc/olake/destination/iceberg/proto"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils/logger"
	"google.golang.org/grpc"
)

// indexScanFlushRows is how many scanned rows are buffered in memory before being
// written to the index. Bounds the memory a full table rebuild can take.
const indexScanFlushRows = 200_000

const indexScanMaxRecvMsgSize = 16 * 1024 * 1024

// reconcileTableIndex makes the stream's index agree with the destination table before the first record is written
func (i *Iceberg) reconcileTableIndex(ctx context.Context, index types.StreamIndex, tableSnapshotID int64, hasEqualityDeletes bool) (err error) {
	if index == nil {
		// no stream index to reconcile
		return nil
	}

	table := fmt.Sprintf("%s.%s", i.stream.GetDestinationDatabase(&i.config.IcebergDatabase), i.stream.GetDestinationTable())
	started := time.Now()
	var indexedSnapshotID int64
	checkpointRead := false
	mode := "read_checkpoint"
	defer func() {
		outcome := "success"
		if err != nil {
			outcome = "failed"
		}
		logger.Infof("Table[%s]: stream index reconciliation thread_id=%s indexed_snapshot=%d checkpoint_read=%t destination_snapshot=%d equality_deletes=%t mode=%s outcome=%s duration_ms=%d",
			table, i.options.ThreadID, indexedSnapshotID, checkpointRead, tableSnapshotID, hasEqualityDeletes, mode, outcome, time.Since(started).Milliseconds())
	}()

	if hasEqualityDeletes {
		mode = "migrate_equality_deletes"
		// Rewrite straight into the stream's target encoding, one commit, not eq -> pos -> dv.
		migrated, err := i.server.tableIndexClient.MigrateEqualityDeletes(ctx, &proto.MigrateEqualityDeletesRequest{
			ThreadId:   i.options.ThreadID,
			TargetMode: protoDeleteMode(i.stream.GetUpdateType()),
		})
		if err != nil {
			return fmt.Errorf("failed to migrate equality deletes of table[%s]: %s", table, err)
		}

		logger.Infof("Table[%s]: rewrote %d equality delete file(s) as %d %s delete(s)",
			table, migrated.GetRewrittenDeleteFiles(), migrated.GetPositionalDeletesWritten(), i.stream.GetUpdateType())
	}

	mode = "read_checkpoint"
	indexedSnapshotID, err = index.LastCommittedSnapshot()
	if err != nil {
		return fmt.Errorf("failed to read stream index checkpoint of table[%s]: %s", table, err)
	}
	checkpointRead = true

	if indexedSnapshotID == tableSnapshotID && !hasEqualityDeletes {
		mode = "skip"
		logger.Infof("Table[%s]: stream index is already up to date at snapshot[%d]", table, tableSnapshotID)
		return nil
	}

	var fromSnapshotID *int64
	if indexedSnapshotID != 0 {
		fromSnapshotID = &indexedSnapshotID
	}

	mode = "rebuild"
	if fromSnapshotID != nil {
		mode = "incremental"
	}
	logger.Infof("Table[%s]: stream index reconciliation decision thread_id=%s indexed_snapshot=%d destination_snapshot=%d equality_deletes=%t mode=%s",
		table, i.options.ThreadID, indexedSnapshotID, tableSnapshotID, hasEqualityDeletes, mode)
	// reindex iceberg table
	return i.fillTableIndex(ctx, index, table, fromSnapshotID)
}

// fillTableIndex streams table locations from the destination into index
// if fromSnapshotID is provided it try to update indexes in incremental manner
func (i *Iceberg) fillTableIndex(ctx context.Context, index types.StreamIndex, table string, fromSnapshotID *int64) (err error) {
	started := time.Now()
	var stats tableIndexScanStats
	var fromSnapshot int64
	mode := "rebuild"
	if fromSnapshotID != nil {
		fromSnapshot = *fromSnapshotID
		mode = "incremental"
	}
	stage := "start_rpc"
	defer func() {
		if stats.stage != "" {
			stage = stats.stage
		}
		outcome := "success"
		if err != nil {
			outcome = "failed"
		}
		logger.Infof("Table[%s]: stream index scan thread_id=%s from_snapshot=%d to_snapshot=%d mode=%s outcome=%s stage=%s duration_ms=%d recv_calls=%d recv_batches=%d recv_ms=%d staged_rows=%d staging_ms=%d flush_attempts=%d flush_successes=%d flush_attempted_rows=%d flushed_rows=%d flush_ms=%d buffered_rows=%d checkpoint_attempted=%t checkpoint_success=%t checkpoint_rows=%d checkpoint_ms=%d",
			table, i.options.ThreadID, fromSnapshot, stats.snapshotID, mode, outcome, stage, time.Since(started).Milliseconds(),
			stats.recvCalls, stats.recvBatches, stats.recvDuration.Milliseconds(), stats.stagedRows, stats.stagingDuration.Milliseconds(),
			stats.flushAttempts, stats.flushSuccesses, stats.flushAttemptedRows, stats.flushedRows, stats.flushDuration.Milliseconds(), stats.bufferedRows,
			stats.checkpointAttempted, stats.checkpointSuccess, stats.checkpointRows, stats.checkpointDuration.Milliseconds())
	}()
	posIterator, err := i.server.tableIndexClient.ScanTableForIndexing(ctx, &proto.TableIndexScanRequest{
		ThreadId:       i.options.ThreadID,
		FromSnapshotId: fromSnapshotID,
	}, grpc.MaxCallRecvMsgSize(indexScanMaxRecvMsgSize))
	if err != nil {
		return fmt.Errorf("failed to start stream index scan: %s", err)
	}

	// if fromSnapshotID is not provided, we need to truncate the index before rebuilding it
	if fromSnapshotID == nil {
		stage = "truncate"
		logger.Infof("Table[%s]: not found starting index snapshot id, truncating stream index before rebuild", table)
		if err := index.Truncate(); err != nil {
			return fmt.Errorf("failed to empty stream index before rebuild: %s", err)
		}
	} else {
		logger.Infof("Table[%s]: starting stream index scan from snapshot[%d]", table, *fromSnapshotID)
	}

	stage = "drain"
	snapshotID, entries, err := drainTableIndexScan(posIterator, index, &stats)
	if err != nil {
		return fmt.Errorf("failed to drain table index scan of table[%s]: %s", table, err)
	}

	stage = "complete"
	logger.Infof("Table[%s]: indexed %d row(s) up to snapshot[%d] thread_id=%s from_snapshot=%d", table, entries, snapshotID, i.options.ThreadID, fromSnapshot)
	return nil
}

// tableIndexScanStats is local to one scan. Timers run once per RPC batch or
// commit, never per row. Recv includes server work, transfer and client decode;
// it is not a measurement of network transfer alone.
type tableIndexScanStats struct {
	stage               string
	snapshotID          int64
	recvCalls           int64
	recvBatches         int64
	recvDuration        time.Duration
	stagedRows          int64
	stagingDuration     time.Duration
	flushAttempts       int64
	flushSuccesses      int64
	flushAttemptedRows  int64
	flushedRows         int64
	flushDuration       time.Duration
	bufferedRows        int
	checkpointAttempted bool
	checkpointSuccess   bool
	checkpointRows      int
	checkpointDuration  time.Duration
}

func drainTableIndexScan(posIterator proto.TableIndexService_ScanTableForIndexingClient, index types.StreamIndex, stats *tableIndexScanStats) (snapshotID, entries int64, err error) {
	pending := types.NewStreamIndexThread(index)
	buffered := 0

	// flush writes the rows accumulated so far without advancing the checkpoint, so a
	// full table scan never has to hold every row of the table in memory at once.
	// Rows are safe to publish early because they are only reachable once the
	// checkpoint moves: a crash mid-scan leaves the old checkpoint in place and the
	// next sync rebuilds (truncating) or resumes from it.
	flush := func() error {
		if buffered == 0 {
			return nil
		}
		stats.stage = "flush"
		stats.flushAttempts++
		stats.flushAttemptedRows += int64(buffered)
		started := time.Now()
		err := index.Commit(pending, nil)
		stats.flushDuration += time.Since(started)
		if err != nil {
			return fmt.Errorf("failed to flush %d stream index row(s): %s", buffered, err)
		}
		stats.flushSuccesses++
		stats.flushedRows += int64(buffered)
		pending = types.NewStreamIndexThread(index)
		buffered = 0
		stats.bufferedRows = 0
		return nil
	}

	for {
		stats.stage = "recv"
		started := time.Now()
		batch, err := posIterator.Recv()
		stats.recvDuration += time.Since(started)
		stats.recvCalls++
		if errors.Is(err, io.EOF) {
			// The checkpoint rides with the final flush, written only after every
			// entry the scan produced is durable in the index.
			stats.stage = "checkpoint"
			stats.checkpointAttempted = true
			stats.checkpointRows = buffered
			started := time.Now()
			err := index.Commit(pending, &snapshotID)
			stats.checkpointDuration += time.Since(started)
			if err != nil {
				return 0, 0, fmt.Errorf("failed to checkpoint stream index at snapshot[%d]: %s", snapshotID, err)
			}
			stats.checkpointSuccess = true
			stats.bufferedRows = 0
			stats.stage = "complete"
			return snapshotID, entries, nil
		}

		if err != nil {
			return 0, 0, fmt.Errorf("stream index scan failed: %s", err)
		}

		snapshotID = batch.GetSnapshotId()
		stats.snapshotID = snapshotID
		stats.recvBatches++
		stats.stage = "staging"
		started = time.Now()
		for _, entry := range batch.GetEntries() {
			pending.Put(entry.GetOlakeId(), types.RowLocation{
				FilePath: entry.GetFilePath(),
				Position: entry.GetPosition(),
			})
		}
		stats.stagingDuration += time.Since(started)
		stats.stagedRows += int64(len(batch.GetEntries()))

		entries += int64(len(batch.GetEntries()))
		buffered += len(batch.GetEntries())
		stats.bufferedRows = buffered

		if buffered >= indexScanFlushRows {
			if err := flush(); err != nil {
				return 0, 0, fmt.Errorf("failed to flush stream index: %s", err)
			}
		}
	}
}
