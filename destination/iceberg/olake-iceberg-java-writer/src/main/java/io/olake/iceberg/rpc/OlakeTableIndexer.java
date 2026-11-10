package io.olake.iceberg.rpc;

import java.util.concurrent.ConcurrentMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.olake.iceberg.rpc.RecordIngest.IcebergPayload;
import io.olake.iceberg.rpc.RecordIngest.MigrateEqualityDeletesRequest;
import io.olake.iceberg.rpc.RecordIngest.MigrateEqualityDeletesResponse;
import io.olake.iceberg.rpc.RecordIngest.TableIndexScanBatch;
import io.olake.iceberg.rpc.RecordIngest.TableIndexScanRequest;
import io.olake.iceberg.tableIndex.EqualityDeleteMigrator;
import io.olake.iceberg.tableIndex.TableIndexScanner;
import io.olake.iceberg.tableoperator.DeleteMode;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;

/**
 * Serves the table index that the Go side keeps on local disk so it can express
 * deletes positionally.
 *
 * <p>Entries are streamed in batches rather than returned in one message because
 * a table can hold hundreds of millions of rows; batching keeps both sides at
 * flat memory. Like the arrow ingester, this service reuses the session created
 * by the GET_OR_CREATE_TABLE handshake instead of loading its own table handle.
 */
public class OlakeTableIndexer extends TableIndexServiceGrpc.TableIndexServiceImplBase {
  private static final Logger LOGGER = LoggerFactory.getLogger(OlakeTableIndexer.class);

  /** Entries per streamed message, a balance between round trips and message size. */
  private static final int BATCH_SIZE = 10_000;

  private final ConcurrentMap<String, IcebergSession> sessions;

  public OlakeTableIndexer(ConcurrentMap<String, IcebergSession> sessions) {
    this.sessions = sessions;
  }

  @Override
  public void scanTableForIndexing(TableIndexScanRequest request, StreamObserver<TableIndexScanBatch> responseObserver) {
    long startTime = System.currentTimeMillis();
    BatchEmitter emitter = null;
    String tableName = "unresolved";
    boolean completed = false;

    try {
      IcebergSession session = requireSession(request.getThreadId());
      tableName = session.icebergTable.name();
      Long fromSnapshotId = request.hasFromSnapshotId() ? request.getFromSnapshotId() : null;

      emitter = new BatchEmitter(responseObserver);
      TableIndexScanner.ScanResult result = TableIndexScanner.scan(
          session.icebergTable, session.identifierField, fromSnapshotId, emitter);

      emitter.finish();

      responseObserver.onCompleted();
      completed = true;
      LOGGER.info("streamed table index of {} entries for thread {} in {} ms",
          result.entries, request.getThreadId(), System.currentTimeMillis() - startTime);
    } catch (ScanAbandoned e) {
      LOGGER.warn("table index scan for thread {} abandoned by the caller after {} ms",
          request.getThreadId(), System.currentTimeMillis() - startTime);
    } catch (Exception e) {
      String message = String.format("failed to scan table index for thread %s: %s",
          request.getThreadId(), e.getMessage());
      LOGGER.error(message, e);
      responseObserver.onError(io.grpc.Status.INTERNAL.withDescription(message).asRuntimeException());
    } finally {
      if (emitter != null) {
        emitter.logSummary(tableName, request.getThreadId(),
            request.hasFromSnapshotId() ? request.getFromSnapshotId() : null, completed);
      }
    }
  }

  @Override
  public void migrateEqualityDeletes(MigrateEqualityDeletesRequest request,
      StreamObserver<MigrateEqualityDeletesResponse> responseObserver) {
    try {
      IcebergSession session = requireSession(request.getThreadId());
      // target_mode wins when the caller set it; UNSPECIFIED falls back to the
      // session's own mode, since deleteMode.addressesPositions() is what gated this RPC.
      IcebergPayload.DeleteMode requestedMode = request.getTargetMode();
      DeleteMode targetMode =
          requestedMode == IcebergPayload.DeleteMode.DELETE_MODE_UNSPECIFIED
              ? session.deleteMode
              : DeleteMode.resolve(requestedMode);
      EqualityDeleteMigrator.Result result = EqualityDeleteMigrator.migrate(
          session.icebergTable, session.identifierField, session.fileFactory, targetMode);

      responseObserver.onNext(MigrateEqualityDeletesResponse.newBuilder()
          .setRewrittenDeleteFiles(result.rewrittenDeleteFiles)
          .setPositionalDeletesWritten(result.positionalDeletesWritten)
          .build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      String message = String.format("failed to migrate equality deletes for thread %s: %s",
          request.getThreadId(), e.getMessage());
      LOGGER.error(message, e);
      responseObserver.onError(io.grpc.Status.INTERNAL.withDescription(message).asRuntimeException());
    }
  }

  private IcebergSession requireSession(String threadId) throws Exception {
    if (threadId == null || threadId.isEmpty()) {
      throw new Exception("Thread id not present in table index request");
    }

    IcebergSession session = sessions.get(threadId);
    if (session == null) {
      throw new Exception("No active session for thread " + threadId
          + "; GET_OR_CREATE_TABLE must be called before scanning the table index");
    }
    return session;
  }

  /** Raised to unwind a scan whose caller has gone away. */
  private static final class ScanAbandoned extends RuntimeException {
    private ScanAbandoned() {
      super("table index scan cancelled by the caller", null, false, false);
    }
  }

  /** Accumulates scan entries and ships them once a batch is full. */
  private static final class BatchEmitter implements TableIndexScanner.EntryConsumer {
    private static final long READY_WAIT_MILLIS = 500;
    private static final long STALL_REPORT_MILLIS = 30_000;

    private final StreamObserver<TableIndexScanBatch> responseObserver;
    private final ServerCallStreamObserver<TableIndexScanBatch> call;
    private final Object readyLock = new Object();
    private TableIndexScanBatch.Builder batch = TableIndexScanBatch.newBuilder();
    private long snapshotId;
    private boolean sentAnything;
    private long readyWaitNanos;
    private long waitCount;
    private long timeoutWakes;
    private long callbackWakes;
    private long callbacks;
    private long batches;
    private long entries;
    private long buildNanos;
    private long onNextNanos;

    @SuppressWarnings("unchecked")
    private BatchEmitter(StreamObserver<TableIndexScanBatch> responseObserver) {
      this.responseObserver = responseObserver;
      this.call = responseObserver instanceof ServerCallStreamObserver
          ? (ServerCallStreamObserver<TableIndexScanBatch>) responseObserver
          : null;

      if (call != null) {
        call.setOnReadyHandler(this::wakeUp);
        call.setOnCancelHandler(this::wakeUp);
      }
    }

    @Override
    public void begin(long snapshotId) {
      this.snapshotId = snapshotId;
      batch.setSnapshotId(snapshotId);
    }

    @Override
    public void accept(String identifier, String filePath, long position) {
      batch.addEntries(TableIndexScanBatch.Entry.newBuilder()
          .setOlakeId(identifier)
          .setFilePath(filePath)
          .setPosition(position));

      if (batch.getEntriesCount() >= BATCH_SIZE) {
        send();
      }
    }

    /**
     * Ships the trailing partial batch. An empty table still sends one batch so
     * the caller always learns which snapshot to checkpoint against.
     */
    private void finish() {
      if (batch.getEntriesCount() > 0 || !sentAnything) {
        send();
      }
    }

    private void send() {
      awaitReady();
      long started = System.nanoTime();
      TableIndexScanBatch message;
      try {
        message = batch.build();
      } finally {
        buildNanos += System.nanoTime() - started;
      }
      started = System.nanoTime();
      try {
        responseObserver.onNext(message);
        batches++;
        entries += message.getEntriesCount();
      } finally {
        onNextNanos += System.nanoTime() - started;
      }
      batch = TableIndexScanBatch.newBuilder().setSnapshotId(snapshotId);
      sentAnything = true;
    }

    private void awaitReady() {
      if (call == null) {
        return;
      }

      long waitBaseline = readyWaitNanos;
      long nextReportMillis = STALL_REPORT_MILLIS;
      synchronized (readyLock) {
        while (true) {
          if (call.isCancelled()) {
            throw new ScanAbandoned();
          }
          if (call.isReady()) {
            return;
          }
          long waitStarted = System.nanoTime();
          long callbacksBefore = callbacks;
          waitCount++;
          try {
            readyLock.wait(READY_WAIT_MILLIS);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new ScanAbandoned();
          } finally {
            long elapsed = System.nanoTime() - waitStarted;
            readyWaitNanos += elapsed;
            if (callbacks != callbacksBefore) {
              callbackWakes++;
            } else if (elapsed >= READY_WAIT_MILLIS * 1_000_000L) {
              timeoutWakes++;
            }
          }

          long waitedMillis = (readyWaitNanos - waitBaseline) / 1_000_000L;
          if (waitedMillis >= nextReportMillis) {
            LOGGER.warn("table index scan snapshot={} has waited {} ms for the caller to consume; still streaming, {} entries buffered",
                snapshotId, waitedMillis, batch.getEntriesCount());
            nextReportMillis = waitedMillis + STALL_REPORT_MILLIS;
          }
        }
      }
    }

    private void wakeUp() {
      synchronized (readyLock) {
        callbacks++;
        readyLock.notifyAll();
      }
    }

    private void logSummary(String table, String threadId, Long from, boolean completed) {
      synchronized (readyLock) {
        LOGGER.info("table index emitter table={} thread={} from={} to={} completed={} cancelled={} batches={} entries={} bufferedEntries={} readyWaitNanos={} waitCount={} timeoutWakes={} callbackWakes={} callbacks={} batchBuildNanos={} onNextNanos={}",
            table, threadId, from, snapshotId, completed, call != null && call.isCancelled(), batches,
            entries, batch.getEntriesCount(), readyWaitNanos, waitCount, timeoutWakes, callbackWakes,
            callbacks, buildNanos, onNextNanos);
      }
    }
  }
}
