package io.olake.iceberg.rpc;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Clock;
import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;

import org.apache.iceberg.BaseTable;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.ManifestReader;
import org.apache.iceberg.ResyncMetadata;
import org.apache.iceberg.RewriteFiles;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.LocationProvider;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.olake.iceberg.rpc.RecordIngest.IcebergPayload;
import io.olake.iceberg.rpc.RecordIngest.RecordIngestResponse;

/** A target's metadata stays private until all its ordinary writer transactions have closed. */
public final class ResyncCoordinator {
    public static final String ID_PROPERTY = "olake.resync.id";
    public static final String CHECKPOINT_PROPERTY = "olake.resync.checkpoint";
    public static final String BASELINE_SHA256_PROPERTY = "olake.resync.baseline-sha256";
    public static final String GENERATION_PROPERTY = "olake.resync.generation";
    public static final String SNAPSHOT_PROPERTY = "olake.resync.snapshot-id";
    public static final long MAX_ATTEMPT_MILLIS = Duration.ofHours(12).toMillis();
    private static final ObjectMapper JSON = new ObjectMapper().enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS);

    private final Catalog catalog;
    private final ConcurrentMap<String, IcebergSession> sessions;
    private final Clock clock;
    private final Map<TableIdentifier, ShadowOperations> attempts = new HashMap<>();

    public ResyncCoordinator(Catalog catalog, ConcurrentMap<String, IcebergSession> sessions) {
        this(catalog, sessions, Clock.systemUTC());
    }

    ResyncCoordinator(Catalog catalog, ConcurrentMap<String, IcebergSession> sessions, Clock clock) {
        this.catalog = catalog;
        this.sessions = sessions;
        this.clock = clock;
    }

    public synchronized RecordIngestResponse control(IcebergPayload.PayloadType type, IcebergPayload.Metadata request)
            throws Exception {
        String id = request.getResyncId();
        if (id.isBlank() || request.getNamespace().isBlank() || request.getDestTableName().isBlank()) {
            throw new IllegalArgumentException("Resync requires a nonblank operation id, namespace and destination table");
        }
        String baselineHash = null;
        if (type == IcebergPayload.PayloadType.PREPARE_RESYNC) {
            String checkpoint = request.getResyncCheckpoint();
            JsonNode state = checkpoint.isBlank() ? null : JSON.readTree(checkpoint);
            if (state == null || !state.isObject()) {
                throw new IllegalArgumentException("Resync preparation requires the complete job checkpoint object");
            }
            baselineHash = sha256(checkpoint);
        }
        TableIdentifier target = TableIdentifier.of(request.getNamespace(), request.getDestTableName());
        Table original = catalog.loadTable(target);
        TableOperations originalOps = operations(original);
        TableMetadata current = originalOps.refresh();
        RecordIngestResponse completed = published(current, id);
        if (completed != null) {
            if (type == IcebergPayload.PayloadType.PREPARE_RESYNC) {
                String originalHash = current.properties().get(BASELINE_SHA256_PROPERTY);
                if (originalHash == null || originalHash.isBlank()) {
                    throw new IllegalStateException("Completed resync marker has no original baseline binding");
                }
                if (!baselineHash.equals(originalHash)
                        && !baselineHash.equals(sha256(completed.getResyncCheckpoint()))) {
                    throw new IllegalStateException("Resync recovery checkpoint differs from the original baseline and published checkpoint");
                }
            }
            ShadowOperations existing = attempts.get(target);
            if (existing != null && existing.id.equals(id)) {
                existing.published = true;
                existing.unknown = false;
            }
            return completed;
        }

        ShadowOperations attempt = attempts.get(target);
        if (attempt != null && !attempt.id.equals(id)) {
            throw new IllegalStateException("Another resync operation owns " + target);
        }
        if (attempt != null && (attempt.unknown || attempt.published)) {
            throw new CommitStateUnknownException("Resync publication outcome is unknown; retain all attempt files",
                    new IllegalStateException("A previous publication has no confirmed durable completion marker"));
        }
        if (type == IcebergPayload.PayloadType.RESYNC_STATUS) {
            return RecordIngestResponse.newBuilder().setSuccess(true).setResyncPublished(false).build();
        }
        if (type == IcebergPayload.PayloadType.ABORT_RESYNC) {
            if (attempt == null) {
                attempt = new ShadowOperations(originalOps, current, id, null, clock.millis(), clock);
                attempts.put(target, attempt);
            }
            // Keep a tombstone: a queued or retried publication can never revive an aborted attempt.
            attempt.aborted = true;
            return RecordIngestResponse.newBuilder().setSuccess(true).setResyncPublished(false).build();
        }
        if (type == IcebergPayload.PayloadType.PREPARE_RESYNC) {
            if (attempt != null) {
                attempt.requireActive();
                if (!baselineHash.equals(attempt.baselineHash)) {
                    throw new IllegalStateException("Resync preparation checkpoint differs from the original baseline");
                }
                return RecordIngestResponse.newBuilder().setSuccess(true).build();
            }
            requireNoSessions(target, null);
            long now = clock.millis();
            long deadline = request.getResyncDeadlineMs();
            if (deadline <= now || deadline - now > MAX_ATTEMPT_MILLIS) {
                throw new IllegalArgumentException("Resync deadline must be in the future and within twelve hours");
            }
            attempt = new ShadowOperations(originalOps, current, id, baselineHash, deadline, clock);
            attempts.put(target, attempt);
            try {
                Table stage = new BaseTable(attempt, original.name());
                empty(stage);
                stage.updateProperties().remove("olake_2pc").remove(ID_PROPERTY).remove(CHECKPOINT_PROPERTY)
                        .remove(BASELINE_SHA256_PROPERTY).remove(GENERATION_PROPERTY).remove(SNAPSHOT_PROPERTY).commit();
                attempt.requireActive();
            } catch (Exception failure) {
                attempt.failed = true;
                throw failure;
            }
            return RecordIngestResponse.newBuilder().setSuccess(true).build();
        }
        if (type != IcebergPayload.PayloadType.PUBLISH_RESYNC || attempt == null) {
            throw new IllegalStateException("No prepared resync for " + target);
        }
        attempt.requireActive();
        requireNoSessions(target, attempt);
        String checkpoint = request.getResyncCheckpoint();
        JsonNode state = checkpoint.isBlank() ? null : JSON.readTree(checkpoint);
        if (state == null || !state.isObject()) {
            throw new IllegalArgumentException("Resync publication requires the complete job checkpoint object");
        }
        TableMetadata before = attempt.original.refresh();
        assertUnchanged(attempt.base, before);
        Snapshot tip = attempt.current().currentSnapshot();
        if (tip == null || tip.snapshotId() <= 0) {
            throw new IllegalStateException("Resync replacement must have a positive snapshot id, including empty results");
        }
        Table stage = new BaseTable(attempt, original.name());
        stage.updateProperties().set(ID_PROPERTY, id).set(CHECKPOINT_PROPERTY, checkpoint)
                .set(BASELINE_SHA256_PROPERTY, attempt.baselineHash)
                .set(GENERATION_PROPERTY, id).set(SNAPSHOT_PROPERTY, Long.toString(tip.snapshotId())).commit();
        TableMetadata staged = attempt.current();
        TableMetadata publication = ResyncMetadata.forPublication(attempt.base, staged, attempt,
                Math.max(clock.millis(), staged.lastUpdatedMillis()), attempt::requireActive);
        attempt.requireActive();
        try {
            // REST applies the same original-to-final delta as whole-metadata catalogs.
            attempt.original.commit(attempt.base, publication);
            attempt.published = true;
        } catch (CommitFailedException failure) {
            attempt.failed = true;
            throw failure;
        } catch (RuntimeException failure) {
            attempt.unknown = true;
            try {
                RecordIngestResponse recovered = published(attempt.original.refresh(), id);
                if (recovered != null) {
                    attempt.published = true;
                    attempt.unknown = false;
                    return recovered;
                }
            } catch (RuntimeException refreshFailure) {
                failure.addSuppressed(refreshFailure);
            }
            throw new CommitStateUnknownException("Resync publication outcome is unknown; retain all attempt files", failure);
        }
        attempt.unknown = true;
        RecordIngestResponse receipt = published(attempt.original.refresh(), id);
        if (receipt == null) {
            attempt.unknown = true;
            throw new CommitStateUnknownException("Resync publication did not return its durable marker",
                    new IllegalStateException("The destination did not expose the committed resync marker"));
        }
        attempt.unknown = false;
        return receipt;
    }

    synchronized Table route(TableIdentifier target) {
        ShadowOperations attempt = attempts.get(target);
        if (attempt == null || attempt.published) {
            return null;
        }
        attempt.requireActive();
        return new BaseTable(attempt, target.toString());
    }

    private void requireNoSessions(TableIdentifier target, ShadowOperations attempt) {
        for (IcebergSession session : sessions.values()) {
            if ((attempt != null && operations(session.icebergTable) == attempt)
                    || session.destination.equals(target)) {
                throw new IllegalStateException("Close every target writer session successfully before preparing or publishing resync");
            }
        }
    }

    private static String sha256(String checkpoint) throws NoSuchAlgorithmException {
        return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256")
                .digest(checkpoint.getBytes(StandardCharsets.UTF_8)));
    }

    private static RecordIngestResponse published(TableMetadata metadata, String id) {
        if (!id.equals(metadata.properties().get(ID_PROPERTY))) {
            return null;
        }
        String checkpoint = metadata.properties().get(CHECKPOINT_PROPERTY);
        String generation = metadata.properties().get(GENERATION_PROPERTY);
        long snapshot = Long.parseLong(metadata.properties().getOrDefault(SNAPSHOT_PROPERTY, "0"));
        if (checkpoint == null || checkpoint.isBlank() || !id.equals(generation) || snapshot <= 0
                || metadata.uuid() == null || metadata.uuid().isBlank()) {
            throw new IllegalStateException("Completed resync marker has an incomplete publication receipt");
        }
        return RecordIngestResponse.newBuilder().setSuccess(true).setResyncPublished(true)
                .setResyncCheckpoint(checkpoint).setResyncGeneration(generation)
                .setResyncTableUuid(metadata.uuid()).setSnapshotId(snapshot).build();
    }

    static TableOperations operations(Table table) {
        if (!(table instanceof HasTableOperations hasOperations)) {
            throw new UnsupportedOperationException("Atomic resync requires Iceberg TableOperations");
        }
        return hasOperations.operations();
    }

    private static void empty(Table stage) throws Exception {
        Snapshot snapshot = stage.currentSnapshot();
        RewriteFiles rewrite = stage.newRewrite();
        Set<String> dataPaths = new HashSet<>();
        boolean hasDeletes = false;
        try (CloseableIterable<org.apache.iceberg.FileScanTask> files = stage.newScan().planFiles()) {
            for (org.apache.iceberg.FileScanTask task : files) {
                DataFile data = task.file();
                if (dataPaths.add(data.location())) {
                    rewrite.deleteFile(data);
                }
            }
        }
        if (snapshot != null) {
            rewrite.validateFromSnapshot(snapshot.snapshotId());
            for (ManifestFile manifest : snapshot.deleteManifests(stage.io())) {
                try (ManifestReader<DeleteFile> deletes = ManifestFiles.readDeleteManifest(manifest, stage.io(), stage.specs())) {
                    for (DeleteFile delete : deletes) {
                        rewrite.deleteFile(delete.copy());
                        hasDeletes = true;
                    }
                }
            }
        }
        if (dataPaths.isEmpty() && !hasDeletes) {
            stage.newAppend().commit();
        } else {
            rewrite.commit();
        }
    }


    static void assertUnchanged(TableMetadata base, TableMetadata current) {
        if (!Objects.equals(base.uuid(), current.uuid()) || !Objects.equals(base.location(), current.location())
                || base.formatVersion() != current.formatVersion()
                || base.lastSequenceNumber() != current.lastSequenceNumber()
                || base.currentSchemaId() != current.currentSchemaId() || base.lastColumnId() != current.lastColumnId()
                || !base.schema().sameSchema(current.schema()) || base.defaultSpecId() != current.defaultSpecId()
                || base.lastAssignedPartitionId() != current.lastAssignedPartitionId()
                || base.defaultSortOrderId() != current.defaultSortOrderId() || !base.refs().equals(current.refs())) {
            throw new CommitFailedException("Concurrent data, schema, partition, sort or identity change during resync");
        }
        Map<String, String> correctness = new HashMap<>();
        base.properties().forEach((key, value) -> {
            if (key.equals("olake_2pc") || key.startsWith("olake.resync.")) {
                correctness.put(key, value);
            }
        });
        Map<String, String> now = new HashMap<>();
        current.properties().forEach((key, value) -> {
            if (key.equals("olake_2pc") || key.startsWith("olake.resync.")) {
                now.put(key, value);
            }
        });
        if (!correctness.equals(now)) {
            throw new CommitFailedException("Concurrent OLake checkpoint or resync marker change");
        }
    }

    static final class ShadowOperations implements TableOperations {
        final TableOperations original;
        final TableMetadata base;
        final String id;
        final String baselineHash;
        private final long deadline;
        private final Clock clock;
        private final FileIO stagedIO;
        private TableMetadata staged;
        volatile boolean failed;
        volatile boolean aborted;
        volatile boolean published;
        volatile boolean unknown;

        ShadowOperations(TableOperations original, TableMetadata base, String id, String baselineHash, long deadline, Clock clock) {
            this.original = original;
            this.base = base;
            this.staged = TableMetadata.buildFrom(base).discardChanges().build();
            this.id = id;
            this.baselineHash = baselineHash;
            this.deadline = deadline;
            this.clock = clock;
            FileIO delegate = original.io();
            this.stagedIO = new FileIO() {
                @Override public org.apache.iceberg.io.InputFile newInputFile(String path) {
                    return delegate.newInputFile(path);
                }
                @Override public org.apache.iceberg.io.OutputFile newOutputFile(String path) {
                    requireActive();
                    org.apache.iceberg.io.OutputFile output = delegate.newOutputFile(path);
                    return new org.apache.iceberg.io.OutputFile() {
                        @Override public org.apache.iceberg.io.PositionOutputStream create() {
                            requireActive();
                            return output.create();
                        }
                        @Override public org.apache.iceberg.io.PositionOutputStream createOrOverwrite() {
                            requireActive();
                            return output.createOrOverwrite();
                        }
                        @Override public String location() { return output.location(); }
                        @Override public org.apache.iceberg.io.InputFile toInputFile() { return output.toInputFile(); }
                    };
                }
                @Override public void deleteFile(String path) {
                    if (unknown || published) {
                        throw new IllegalStateException("Cannot delete files after a possibly successful resync publication");
                    }
                    delegate.deleteFile(path);
                }
                @Override public Map<String, String> properties() { return delegate.properties(); }
            };
        }

        void requireActive() {
            if (failed || aborted || published || unknown) {
                throw new IllegalStateException("Resync attempt is no longer writable; start a fresh process attempt");
            }
            if (io.grpc.Context.current().isCancelled()) {
                failed = true;
                throw new IllegalStateException("Resync RPC was cancelled before publication");
            }
            if (clock.millis() >= deadline) {
                failed = true;
                throw new IllegalStateException("Resync exceeded its twelve-hour staging retention deadline");
            }
        }

        @Override public synchronized TableMetadata current() { return staged; }
        @Override public synchronized TableMetadata refresh() { return staged; }
        @Override public synchronized void commit(TableMetadata expected, TableMetadata next) {
            requireActive();
            if (expected != staged) {
                throw new CommitFailedException("Stale resync shadow metadata");
            }
            if (!Objects.equals(base.uuid(), next.uuid()) || !Objects.equals(base.location(), next.location())) {
                throw new IllegalArgumentException("Resync cannot change the table UUID or location");
            }
            staged = next;
        }
        @Override public FileIO io() { return stagedIO; }
        @Override public EncryptionManager encryption() { return original.encryption(); }
        @Override public String metadataFileLocation(String file) { requireActive(); return original.metadataFileLocation(file); }
        @Override public LocationProvider locationProvider() { return original.locationProvider(); }
        @Override public TableOperations temp(TableMetadata metadata) {
            TableOperations delegate = original.temp(metadata);
            return new TableOperations() {
                @Override public TableMetadata current() { return delegate.current(); }
                @Override public TableMetadata refresh() { return delegate.refresh(); }
                @Override public void commit(TableMetadata expected, TableMetadata next) {
                    requireActive();
                    delegate.commit(expected, next);
                }
                @Override public FileIO io() { return stagedIO; }
                @Override public EncryptionManager encryption() { return delegate.encryption(); }
                @Override public String metadataFileLocation(String file) {
                    requireActive();
                    return delegate.metadataFileLocation(file);
                }
                @Override public LocationProvider locationProvider() { return delegate.locationProvider(); }
                @Override public long newSnapshotId() { requireActive(); return delegate.newSnapshotId(); }
                @Override public boolean requireStrictCleanup() { return delegate.requireStrictCleanup(); }
            };
        }
        @Override public long newSnapshotId() { requireActive(); return original.newSnapshotId(); }
        @Override public boolean requireStrictCleanup() { return original.requireStrictCleanup(); }
    }
}
