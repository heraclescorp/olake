package io.olake.iceberg.rpc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileMetadata;
import org.apache.iceberg.GenericStatisticsFile;
import org.apache.iceberg.HistoryEntry;
import org.apache.iceberg.ImmutableGenericPartitionStatisticsFile;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.ManifestReader;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.MetadataUpdate;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.ResyncMetadata;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.UpdateRequirement;
import org.apache.iceberg.UpdateRequirements;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericAppenderFactory;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.deletes.BaseDVFileWriter;
import org.apache.iceberg.deletes.DVFileWriter;
import org.apache.iceberg.deletes.EqualityDeleteWriter;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.deletes.PositionDeleteWriter;
import org.apache.iceberg.encryption.EncryptedFiles;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.rest.CatalogHandlers;
import org.apache.iceberg.rest.requests.UpdateTableRequest;
import org.apache.iceberg.rest.requests.UpdateTableRequestParser;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import com.fasterxml.jackson.core.JsonProcessingException;

import io.olake.iceberg.rpc.RecordIngest.IcebergPayload;
import io.olake.iceberg.rpc.RecordIngest.RecordIngestResponse;
import io.olake.iceberg.tableIndex.TableIndexScanner;
import io.olake.iceberg.tableoperator.DeleteMode;

class ResyncCoordinatorTest {
    private static final TableIdentifier TARGET = TableIdentifier.of("db", "target");
    private static final Schema SCHEMA = new Schema(Types.NestedField.required(1, "id", Types.StringType.get()),
            Types.NestedField.optional(2, "value", Types.StringType.get()));
    private static final String BASELINE = checkpoint("cdc", 1, 7);
    private static final String CHECKPOINT = checkpoint("cdc", 2, 7);

    @TempDir Path directory;
    private FaultCatalog catalog;
    private ConcurrentMap<String, IcebergSession> sessions;
    private MutableClock clock;
    private ResyncCoordinator coordinator;

    @BeforeEach
    void setUp() {
        catalog = new FaultCatalog(new Configuration(), directory.toString());
        sessions = new ConcurrentHashMap<>();
        clock = new MutableClock();
        coordinator = new ResyncCoordinator(catalog, sessions, clock);
    }

    @AfterEach
    void close() throws Exception {
        catalog.close();
    }

    @ParameterizedTest
    @ValueSource(ints = {2, 3})
    void replacementIsIsolatedAndRemovesAllOldDataAndDeleteFiles(int version) throws Exception {
        Table original = create(version);
        DataFile old = append(original, "old-a", "old-b", "old-c");
        DeleteFile delete = deletePosition(original, old, 1, version);
        original.newRowDelta().addDeletes(delete).commit();
        original.updateProperties().set("olake_2pc", "{\"id\":\"old\"}").set("owner", "untouched").commit();
        long oldSnapshot = original.currentSnapshot().snapshotId();
        String uuid = metadata(original).uuid();
        String location = original.location();
        List<Long> committedSnapshots = metadata(original).snapshots().stream()
                .map(org.apache.iceberg.Snapshot::snapshotId).toList();
        int originalCommits = catalog.commits;

        prepare();
        Table stage = coordinator.route(TARGET);
        assertEquals(List.of(), ids(stage));
        assertNull(stage.properties().get("olake_2pc"));
        assertEquals("{\"id\":\"old\"}", catalog.loadTable(TARGET).properties().get("olake_2pc"));
        stage.updateSchema().addColumn("new_field", Types.LongType.get()).commit();
        transactionAppend(stage, "new-a", "new-b");
        assertEquals(List.of("old-a", "old-c"), ids(catalog.loadTable(TARGET)));
        assertNull(catalog.loadTable(TARGET).schema().findField("new_field"));
        transactionAppend(stage, "new-c");
        assertEquals(oldSnapshot, catalog.loadTable(TARGET).currentSnapshot().snapshotId());
        assertEquals(originalCommits, catalog.commits);

        RecordIngestResponse receipt = publish();
        assertEquals(originalCommits + 1, catalog.commits);
        Table replaced = catalog.loadTable(TARGET);
        assertEquals(List.of("new-a", "new-b", "new-c"), ids(replaced));
        assertEquals(uuid, metadata(replaced).uuid());
        assertEquals(location, replaced.location());
        assertEquals("untouched", replaced.properties().get("owner"));
        assertNotNull(replaced.schema().findField("new_field"));
        assertNotNull(replaced.snapshot(oldSnapshot));
        assertEquals(List.of("old-a", "old-c"), idsAt(replaced, oldSnapshot));
        assertTrue(receipt.getResyncPublished());
        assertEquals(CHECKPOINT, receipt.getResyncCheckpoint());
        assertEquals(HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256")
                .digest(BASELINE.getBytes(StandardCharsets.UTF_8))),
                replaced.properties().get(ResyncCoordinator.BASELINE_SHA256_PROPERTY));
        assertEquals(uuid, receipt.getResyncTableUuid());
        assertEquals(replaced.currentSnapshot().snapshotId(), receipt.getSnapshotId());
        java.util.Set<Long> expectedHistory = new java.util.HashSet<>(committedSnapshots);
        expectedHistory.add(receipt.getSnapshotId());
        assertEquals(expectedHistory, new java.util.HashSet<>(metadata(replaced).snapshots().stream()
                .map(org.apache.iceberg.Snapshot::snapshotId).toList()));
        assertTrue(replaced.history().stream().allMatch(entry -> expectedHistory.contains(entry.snapshotId())));
        assertTrue(receipt.getSnapshotId() > 0);
        try (CloseableIterable<org.apache.iceberg.FileScanTask> tasks = replaced.newScan().planFiles()) {
            for (org.apache.iceberg.FileScanTask task : tasks) {
                assertNotEquals(old.location(), task.file().location());
                assertTrue(task.deletes().isEmpty());
            }
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void emptyResultsPublishWithPositiveSnapshotAndSameIdentity(boolean initiallyEmpty) throws Exception {
        Table original = create(2);
        if (!initiallyEmpty) {
            append(original, "old");
        }
        String uuid = metadata(original).uuid();
        prepare();
        RecordIngestResponse receipt = publish();
        assertTrue(receipt.getSnapshotId() > 0);
        assertEquals(uuid, receipt.getResyncTableUuid());
        assertEquals(List.of(), ids(catalog.loadTable(TARGET)));
    }

    @ParameterizedTest
    @ValueSource(strings = {"cdc", "full_refresh", "incremental"})
    void sameIdRecoveryRequiresOriginalOrPublishedFullCheckpoint(String mode) throws Exception {
        create(2);
        String baseline = checkpoint(mode, 1, 7);
        String published = checkpoint(mode, 2, 7);
        coordinator.control(IcebergPayload.PayloadType.PREPARE_RESYNC,
                request().setResyncCheckpoint(baseline)
                        .setResyncDeadlineMs(clock.millis() + ResyncCoordinator.MAX_ATTEMPT_MILLIS).build());
        transactionAppend(coordinator.route(TARGET), "new");
        RecordIngestResponse receipt = coordinator.control(IcebergPayload.PayloadType.PUBLISH_RESYNC,
                request().setResyncCheckpoint(published).build());
        int publishedCommits = catalog.commits;
        ResyncCoordinator restarted = new ResyncCoordinator(catalog, new ConcurrentHashMap<>(), clock);
        clock.now += ResyncCoordinator.MAX_ATTEMPT_MILLIS * 2;
        assertEquals(receipt, restarted.control(IcebergPayload.PayloadType.PREPARE_RESYNC,
                request().setResyncCheckpoint(baseline).setResyncDeadlineMs(1).build()));
        assertEquals(receipt, restarted.control(IcebergPayload.PayloadType.PREPARE_RESYNC,
                request().setResyncCheckpoint(published).setResyncDeadlineMs(1).build()));
        for (String newer : List.of(checkpoint(mode, 3, 7), checkpoint(mode, 2, 8), checkpoint(mode, 1, 8))) {
            assertThrows(IllegalStateException.class, () -> restarted.control(IcebergPayload.PayloadType.PREPARE_RESYNC,
                    request().setResyncCheckpoint(newer).setResyncDeadlineMs(1).build()));
        }
        assertNull(restarted.route(TARGET));
        assertEquals(publishedCommits, catalog.commits);
        assertEquals(List.of("new"), ids(catalog.loadTable(TARGET)));
        assertEquals(receipt, restarted.control(IcebergPayload.PayloadType.RESYNC_STATUS, request().build()));
        assertEquals(receipt, restarted.control(IcebergPayload.PayloadType.ABORT_RESYNC, request().build()));
        assertEquals(receipt, restarted.control(IcebergPayload.PayloadType.PUBLISH_RESYNC, request().build()));
    }

    @ParameterizedTest
    @ValueSource(strings = {"cdc", "full_refresh", "incremental"})
    void repeatedPrepareRequiresTheSameFullBaseline(String mode) throws Exception {
        Table original = create(2);
        append(original, "old");
        String baseline = checkpoint(mode, 1, 7);
        IcebergPayload.Metadata.Builder initial = request().setResyncCheckpoint(baseline)
                .setResyncDeadlineMs(clock.millis() + ResyncCoordinator.MAX_ATTEMPT_MILLIS);
        coordinator.control(IcebergPayload.PayloadType.PREPARE_RESYNC, initial.build());
        Table stage = coordinator.route(TARGET);
        transactionAppend(stage, "new");
        long stagedSnapshot = stage.currentSnapshot().snapshotId();
        assertTrue(coordinator.control(IcebergPayload.PayloadType.PREPARE_RESYNC, initial.build()).getSuccess());
        for (String changed : List.of(checkpoint(mode, 2, 7), checkpoint(mode, 1, 8))) {
            assertThrows(IllegalStateException.class, () -> coordinator.control(IcebergPayload.PayloadType.PREPARE_RESYNC,
                    initial.clone().setResyncCheckpoint(changed).build()));
        }
        assertEquals(stagedSnapshot, coordinator.route(TARGET).currentSnapshot().snapshotId());
        assertEquals(List.of("new"), ids(coordinator.route(TARGET)));
        assertEquals(List.of("old"), ids(catalog.loadTable(TARGET)));
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void publishedRecoveryRejectsMissingOrBlankOriginalBaseline(boolean blank) throws Exception {
        create(2);
        prepare();
        RecordIngestResponse receipt = publish();
        Table published = catalog.loadTable(TARGET);
        if (blank) {
            published.updateProperties().set(ResyncCoordinator.BASELINE_SHA256_PROPERTY, " ").commit();
        } else {
            published.updateProperties().remove(ResyncCoordinator.BASELINE_SHA256_PROPERTY).commit();
        }
        int publishedCommits = catalog.commits;
        ResyncCoordinator restarted = new ResyncCoordinator(catalog, new ConcurrentHashMap<>(), clock);
        for (String checkpoint : List.of(BASELINE, CHECKPOINT)) {
            assertThrows(IllegalStateException.class, () -> restarted.control(IcebergPayload.PayloadType.PREPARE_RESYNC,
                    request().setResyncCheckpoint(checkpoint)
                            .setResyncDeadlineMs(clock.millis() + ResyncCoordinator.MAX_ATTEMPT_MILLIS).build()));
        }
        assertNull(restarted.route(TARGET));
        assertEquals(publishedCommits, catalog.commits);
        assertEquals(receipt, restarted.control(IcebergPayload.PayloadType.RESYNC_STATUS, request().build()));
        assertEquals(receipt, restarted.control(IcebergPayload.PayloadType.ABORT_RESYNC, request().build()));
        assertEquals(receipt, restarted.control(IcebergPayload.PayloadType.PUBLISH_RESYNC, request().build()));
    }

    @Test
    void freshStageClearsAllPreviousPublicationMarkersPrivately() throws Exception {
        create(2);
        prepare();
        RecordIngestResponse receipt = publish();
        ResyncCoordinator restarted = new ResyncCoordinator(catalog, new ConcurrentHashMap<>(), clock);
        restarted.control(IcebergPayload.PayloadType.PREPARE_RESYNC,
                request().setResyncId("next-operation").setResyncCheckpoint(CHECKPOINT)
                        .setResyncDeadlineMs(clock.millis() + ResyncCoordinator.MAX_ATTEMPT_MILLIS).build());
        Table stage = restarted.route(TARGET);
        Table visible = catalog.loadTable(TARGET);
        for (String property : List.of(ResyncCoordinator.ID_PROPERTY, ResyncCoordinator.CHECKPOINT_PROPERTY,
                ResyncCoordinator.BASELINE_SHA256_PROPERTY, ResyncCoordinator.GENERATION_PROPERTY,
                ResyncCoordinator.SNAPSHOT_PROPERTY)) {
            assertNull(stage.properties().get(property));
            assertNotNull(visible.properties().get(property));
        }
        assertEquals(receipt.getSnapshotId(), visible.currentSnapshot().snapshotId());
    }

    @Test
    void abortIsTerminalAndNeverChangesVisibleDataOrSchema() throws Exception {
        Table original = create(2);
        append(original, "old");
        long snapshot = original.currentSnapshot().snapshotId();
        prepare();
        Table stage = coordinator.route(TARGET);
        stage.updateSchema().addColumn("hidden", Types.StringType.get()).commit();
        transactionAppend(stage, "new");
        assertFalse(coordinator.control(IcebergPayload.PayloadType.ABORT_RESYNC, request().build()).getResyncPublished());
        assertThrows(IllegalStateException.class, this::publish);
        assertThrows(IllegalStateException.class, this::prepare);
        assertThrows(IllegalStateException.class, () -> stage.newAppend().commit());
        assertEquals(snapshot, catalog.loadTable(TARGET).currentSnapshot().snapshotId());
        assertEquals(List.of("old"), ids(catalog.loadTable(TARGET)));
        assertNull(catalog.loadTable(TARGET).schema().findField("hidden"));
    }

    @Test
    void deadlineIsValidatedBeforeStageAndAgainBeforeWritesAndPublish() throws Exception {
        Table original = create(2);
        append(original, "old");
        long snapshot = original.currentSnapshot().snapshotId();
        assertThrows(IllegalArgumentException.class, () -> coordinator.control(IcebergPayload.PayloadType.PREPARE_RESYNC,
                request().setResyncDeadlineMs(clock.millis()).build()));
        assertThrows(IllegalArgumentException.class, () -> coordinator.control(IcebergPayload.PayloadType.PREPARE_RESYNC,
                request().setResyncDeadlineMs(clock.millis() + ResyncCoordinator.MAX_ATTEMPT_MILLIS + 1).build()));
        assertNull(coordinator.route(TARGET));
        prepare();
        Table stage = coordinator.route(TARGET);
        clock.now += ResyncCoordinator.MAX_ATTEMPT_MILLIS;
        assertThrows(IllegalStateException.class,
                () -> stage.io().newOutputFile(stage.location() + "/expired.parquet").create());
        assertThrows(IllegalStateException.class, () -> stage.newAppend().commit());
        assertThrows(IllegalStateException.class, this::publish);
        assertEquals(snapshot, catalog.loadTable(TARGET).currentSnapshot().snapshotId());
    }

    @Test
    void requiresClosedSuccessfullyCommittedTargetSessionsButNotUnrelatedSessions() throws Exception {
        create(2);
        Table other = catalog.createTable(TableIdentifier.of("db", "other"), SCHEMA);
        sessions.put("other", new IcebergSession(other, false, "id", DeleteMode.POSITION,
                TableIdentifier.of("db", "other")));
        prepare();
        IcebergSession target = new IcebergSession(coordinator.route(TARGET), false, "id", DeleteMode.POSITION, TARGET);
        sessions.put("target", target);
        assertThrows(IllegalStateException.class, this::publish);
        target.close();
        sessions.remove("target");
        assertTrue(publish().getResyncPublished());
        assertTrue(sessions.containsKey("other"));
    }

    @Test
    void uncommittedWriterCannotCloseOrPublishItsPartialFiles() throws Exception {
        Table original = create(2);
        append(original, "old");
        prepare();
        Table stage = coordinator.route(TARGET);
        IcebergSession session = new IcebergSession(stage, false, "id", DeleteMode.POSITION, TARGET);
        sessions.put("pending", session);
        GenericRecord row = GenericRecord.create(stage.schema());
        row.setField("id", "partial");
        row.setField("value", "partial");
        session.op.addToTablePerSchema("pending", stage, List.of(new io.olake.iceberg.tableoperator.RecordWrapper(
                row, io.olake.iceberg.tableoperator.Operation.READ)));
        session.op.completeWriter();
        assertThrows(IllegalStateException.class, session::close);
        assertThrows(IllegalStateException.class, this::publish);
        assertFalse(coordinator.control(IcebergPayload.PayloadType.ABORT_RESYNC, request().build()).getResyncPublished());
        assertEquals(List.of("old"), ids(catalog.loadTable(TARGET)));
    }

    @ParameterizedTest
    @ValueSource(strings = {"data", "schema", "spec", "sort", "checkpoint", "marker"})
    void concurrentChangesAreRejectedWithoutRebasing(String change) throws Exception {
        Table table = create(2);
        append(table, "old");
        prepare();
        transactionAppend(coordinator.route(TARGET), "new");
        Table concurrent = catalog.loadTable(TARGET);
        switch (change) {
            case "data" -> append(concurrent, "concurrent");
            case "schema" -> concurrent.updateSchema().addColumn("concurrent", Types.StringType.get()).commit();
            case "spec" -> concurrent.updateSpec().addField("id").commit();
            case "sort" -> concurrent.replaceSortOrder().asc("id").commit();
            case "checkpoint" -> concurrent.updateProperties().set("olake_2pc", "concurrent").commit();
            case "marker" -> concurrent.updateProperties().set(ResyncCoordinator.GENERATION_PROPERTY, "concurrent").commit();
            default -> throw new AssertionError(change);
        }
        assertThrows(CommitFailedException.class, this::publish);
        assertFalse(ids(catalog.loadTable(TARGET)).contains("new"));
        assertFalse(coordinator.control(IcebergPayload.PayloadType.ABORT_RESYNC, request().build()).getResyncPublished());
    }

    @ParameterizedTest
    @ValueSource(ints = {2, 3})
    void restDeltaRoundTripContainsClearSchemaEveryChunkAndIdentityRequirements(int version) throws Exception {
        Table table = create(version);
        append(table, "old-a");
        long retainedSnapshot = table.currentSnapshot().snapshotId();
        append(table, "old-b");
        table.manageSnapshots().createBranch("audit", retainedSnapshot).createTag("pinned", retainedSnapshot)
                .setMinSnapshotsToKeep("audit", 3).setMaxSnapshotAgeMs("audit", 86_400_000L)
                .setMaxRefAgeMs("pinned", 172_800_000L).setMinSnapshotsToKeep("main", 2)
                .setMaxSnapshotAgeMs("main", 259_200_000L).commit();
        table.updateStatistics().setStatistics(new GenericStatisticsFile(retainedSnapshot,
                table.location() + "/old-statistics.puffin", 100, 20, List.of())).commit();
        table.updatePartitionStatistics().setPartitionStatistics(ImmutableGenericPartitionStatisticsFile.builder()
                .snapshotId(retainedSnapshot).path(table.location() + "/old-partition-statistics.parquet")
                .fileSizeInBytes(200).build()).commit();
        table.updateProperties().set("olake_2pc", "old").commit();
        TableMetadata base = metadata(table);
        prepare();
        Table stage = coordinator.route(TARGET);
        stage.updateSchema().addColumn("new_field", Types.LongType.get()).commit();
        transactionAppend(stage, "new-a");
        stage.manageSnapshots().removeBranch("audit").removeTag("pinned")
                .createBranch("private-stage", stage.currentSnapshot().snapshotId()).commit();
        stage.updateStatistics().removeStatistics(retainedSnapshot).commit();
        stage.updatePartitionStatistics().removePartitionStatistics(retainedSnapshot).commit();
        transactionAppend(stage, "new-b");
        stage.updateSpec().addField("id").commit();
        stage.replaceSortOrder().asc("id").commit();
        String baselineHash = HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256")
                .digest(BASELINE.getBytes(StandardCharsets.UTF_8)));
        stage.updateProperties().set(ResyncCoordinator.ID_PROPERTY, "operation")
                .set(ResyncCoordinator.CHECKPOINT_PROPERTY, CHECKPOINT)
                .set(ResyncCoordinator.BASELINE_SHA256_PROPERTY, baselineHash).commit();
        long publicationMillis = Math.max(base.lastUpdatedMillis(), metadata(stage).lastUpdatedMillis()) + 1_000;
        TableMetadata publication = ResyncMetadata.forPublication(base, metadata(stage),
                ResyncCoordinator.operations(stage), publicationMillis, () -> { });
        assertPublicLineage(base, metadata(stage), publication);
        assertPublicationChanges(base, publication);
        UpdateTableRequest request = new UpdateTableRequest(UpdateRequirements.forUpdateTable(base, publication.changes()),
                publication.changes());
        String json = UpdateTableRequestParser.toJson(request);
        UpdateTableRequest wire = UpdateTableRequestParser.fromJson(json);
        assertTrue(wire.requirements().stream().anyMatch(UpdateRequirement.AssertTableUUID.class::isInstance));
        assertTrue(wire.requirements().stream().anyMatch(UpdateRequirement.AssertRefSnapshotID.class::isInstance));
        assertTrue(wire.requirements().stream().anyMatch(UpdateRequirement.AssertCurrentSchemaID.class::isInstance));
        assertTrue(wire.requirements().stream().anyMatch(UpdateRequirement.AssertLastAssignedFieldId.class::isInstance));
        assertTrue(wire.requirements().stream().anyMatch(UpdateRequirement.AssertDefaultSpecID.class::isInstance));
        assertTrue(wire.requirements().stream().anyMatch(UpdateRequirement.AssertLastAssignedPartitionId.class::isInstance));
        assertTrue(wire.requirements().stream().anyMatch(UpdateRequirement.AssertDefaultSortOrderID.class::isInstance));
        // A REST delta merges harmless concurrent properties rather than replacing the whole metadata file.
        catalog.loadTable(TARGET).updateProperties().set("external.description", "preserved").commit();
        CatalogHandlers.updateTable(catalog, TARGET, wire);
        Table applied = catalog.loadTable(TARGET);
        assertEquals(List.of("new-a", "new-b"), ids(applied));
        assertNotNull(applied.schema().findField("new_field"));
        assertNull(applied.properties().get("olake_2pc"));
        assertEquals(CHECKPOINT, applied.properties().get(ResyncCoordinator.CHECKPOINT_PROPERTY));
        assertEquals(baselineHash, applied.properties().get(ResyncCoordinator.BASELINE_SHA256_PROPERTY));
        assertEquals("preserved", applied.properties().get("external.description"));
        java.util.Set<Long> publishedSnapshots = new java.util.HashSet<>(base.snapshots().stream()
                .map(org.apache.iceberg.Snapshot::snapshotId).toList());
        publishedSnapshots.add(applied.currentSnapshot().snapshotId());
        assertEquals(publishedSnapshots, new java.util.HashSet<>(metadata(applied).snapshots().stream()
                .map(org.apache.iceberg.Snapshot::snapshotId).toList()));
        assertEquals(base.uuid(), metadata(applied).uuid());
        TableMetadata actual = metadata(applied);
        assertEquals(publication.snapshotLog(), actual.snapshotLog());
        assertEquals(publication.currentSnapshot().timestampMillis(), applied.currentSnapshot().timestampMillis());
        assertEquals(publication.currentSnapshot().parentId(), applied.currentSnapshot().parentId());
        assertEquals(publication.currentSnapshot().sequenceNumber(), applied.currentSnapshot().sequenceNumber());
        assertEquals(publication.currentSnapshot().summary(), applied.currentSnapshot().summary());
        assertEquals(publication.refs(), actual.refs());
        assertEquals(publication.statisticsFiles(), actual.statisticsFiles());
        assertEquals(publication.partitionStatisticsFiles(), actual.partitionStatisticsFiles());
        assertEquals(publication.currentSchemaId(), actual.currentSchemaId());
        assertEquals(publication.lastColumnId(), actual.lastColumnId());
        assertEquals(publication.defaultSpecId(), actual.defaultSpecId());
        assertEquals(publication.lastAssignedPartitionId(), actual.lastAssignedPartitionId());
        assertEquals(publication.defaultSortOrderId(), actual.defaultSortOrderId());
        assertEquals(publication.nextRowId(), actual.nextRowId());
        assertEquals(metadata(stage).nextRowId(), actual.nextRowId());
        assertEquals(List.of("old-a"), idsAt(applied, retainedSnapshot));
        assertEquals(List.of("old-a", "old-b"), idsAsOf(applied,
                base.snapshotLog().get(base.snapshotLog().size() - 1).timestampMillis()));
        assertEquals(List.of("new-a", "new-b"), idsAsOf(applied, publicationMillis));
        assertEquals(liveFileState(stage), liveFileState(applied));
        if (version == 3) {
            assertEquals(rowIds(stage), rowIds(applied));
        }
        com.fasterxml.jackson.databind.ObjectMapper jsonMapper = new com.fasterxml.jackson.databind.ObjectMapper();
        com.fasterxml.jackson.databind.node.ObjectNode expectedMetadata = (com.fasterxml.jackson.databind.node.ObjectNode)
                jsonMapper.readTree(org.apache.iceberg.TableMetadataParser.toJson(publication));
        com.fasterxml.jackson.databind.node.ObjectNode actualMetadata = (com.fasterxml.jackson.databind.node.ObjectNode)
                jsonMapper.readTree(org.apache.iceberg.TableMetadataParser.toJson(actual));
        // The harmless concurrent property commit adds a physical metadata-file log entry, not table state.
        expectedMetadata.remove("metadata-log");
        actualMetadata.remove("metadata-log");
        ((com.fasterxml.jackson.databind.node.ObjectNode) expectedMetadata.get("properties"))
                .put("external.description", "preserved");
        assertEquals(expectedMetadata, actualMetadata);
    }

    @ParameterizedTest
    @ValueSource(strings = {"data", "schema", "spec", "sort"})
    void restAssertionsRejectChangesAfterPublicationPreflight(String change) throws Exception {
        Table table = create(2);
        append(table, "old");
        TableMetadata base = metadata(table);
        prepare();
        TableMetadata publication = publication(table, coordinator.route(TARGET));
        UpdateTableRequest wire = UpdateTableRequestParser.fromJson(UpdateTableRequestParser.toJson(
                new UpdateTableRequest(UpdateRequirements.forUpdateTable(base, publication.changes()), publication.changes())));
        Table concurrent = catalog.loadTable(TARGET);
        switch (change) {
            case "data" -> append(concurrent, "concurrent");
            case "schema" -> concurrent.updateSchema().addColumn("concurrent", Types.LongType.get()).commit();
            case "spec" -> concurrent.updateSpec().addField("id").commit();
            case "sort" -> concurrent.replaceSortOrder().asc("id").commit();
            default -> throw new AssertionError(change);
        }
        assertThrows(CommitFailedException.class, () -> CatalogHandlers.updateTable(catalog, TARGET, wire));
    }

    @ParameterizedTest
    @ValueSource(ints = {2, 3})
    void fullIndexScanAppliesDeletesAndKeepsPhysicalPositions(int version) throws Exception {
        Table table = create(version);
        DataFile data = append(table, "a", "deleted", "c");
        DataFile second = append(table, "x", "also-deleted", "z");
        table.newRowDelta().addDeletes(deletePosition(table, data, 1, version))
                .addDeletes(deletePosition(table, second, 1, version)).commit();
        Map<String, Long> positions = new HashMap<>();
        Map<String, String> paths = new HashMap<>();
        TableIndexScanner.ScanResult scan = TableIndexScanner.scan(table, "id", null, new TableIndexScanner.EntryConsumer() {
            @Override public void begin(long snapshotId) { assertEquals(table.currentSnapshot().snapshotId(), snapshotId); }
            @Override public void accept(String id, String path, long position) {
                paths.put(id, path);
                positions.put(id, position);
            }
        });
        assertEquals(Map.of("a", 0L, "c", 2L, "x", 0L, "z", 2L), positions);
        assertEquals(Map.of("a", data.location(), "c", data.location(), "x", second.location(), "z", second.location()), paths);
        assertEquals(4, scan.entries);
    }

    @ParameterizedTest
    @ValueSource(strings = {"id", "value"})
    void equalityDeleteRecoveryDoesNotResurrectDeletedIndexEntries(String equalityField) throws Exception {
        Table table = create(2);
        DataFile data = append(table, "a", "deleted", "c");
        Schema keys = new Schema(SCHEMA.findField(equalityField));
        GenericAppenderFactory factory = new GenericAppenderFactory(table.schema(), table.spec(),
                new int[] {keys.columns().get(0).fieldId()}, keys, null);
        String path = table.locationProvider().newDataLocation(UUID.randomUUID() + ".parquet");
        EqualityDeleteWriter<Record> writer = factory.newEqDeleteWriter(
                EncryptedFiles.plainAsEncryptedOutput(table.io().newOutputFile(path)), FileFormat.PARQUET, null);
        try (writer) {
            GenericRecord deleted = GenericRecord.create(keys);
            deleted.setField(equalityField, "deleted");
            writer.write(deleted);
        }
        table.newRowDelta().addDeletes(writer.toDeleteFile()).commit();
        Map<String, Long> positions = new HashMap<>();
        TableIndexScanner.scan(table, "id", null, new TableIndexScanner.EntryConsumer() {
            @Override public void begin(long snapshot) { }
            @Override public void accept(String id, String file, long position) {
                assertEquals(data.location(), file);
                positions.put(id, position);
            }
        });
        assertEquals(Map.of("a", 0L, "c", 2L), positions);
        prepare();
        assertEquals(List.of(), ids(coordinator.route(TARGET)));
        publish();
        assertEquals(List.of(), ids(catalog.loadTable(TARGET)));
    }

    @Test
    void committedButLostResponseRecoversReceiptWithoutRebuilding() throws Exception {
        create(2);
        prepare();
        transactionAppend(coordinator.route(TARGET), "new");
        catalog.failAfterCommit = true;
        RecordIngestResponse recovered = publish();
        assertTrue(recovered.getResyncPublished());
        assertEquals(CHECKPOINT, recovered.getResyncCheckpoint());
        assertEquals(List.of("new"), ids(catalog.loadTable(TARGET)));
        assertEquals(recovered, coordinator.control(IcebergPayload.PayloadType.ABORT_RESYNC, request().build()));
    }

    @Test
    void unknownOutcomeCannotAbortOrStartAnotherPublication() throws Exception {
        Table original = create(2);
        append(original, "old");
        prepare();
        transactionAppend(coordinator.route(TARGET), "new");
        catalog.failBeforeCommit = true;
        assertThrows(CommitStateUnknownException.class, this::publish);
        CommitStateUnknownException statusFailure = assertThrows(CommitStateUnknownException.class,
                () -> coordinator.control(IcebergPayload.PayloadType.RESYNC_STATUS, request().build()));
        assertNotNull(statusFailure.getCause());
        assertThrows(CommitStateUnknownException.class,
                () -> coordinator.control(IcebergPayload.PayloadType.ABORT_RESYNC, request().build()));
        assertThrows(CommitStateUnknownException.class, this::publish);
        assertEquals(List.of("old"), ids(catalog.loadTable(TARGET)));
    }

    @ParameterizedTest
    @ValueSource(strings = {"", " ", "null", "[]", "42", "\"cursor\""})
    void preparationRequiresACompleteCheckpointObjectBeforeStaging(String checkpoint) throws Exception {
        create(2);
        int originalCommits = catalog.commits;
        assertThrows(IllegalArgumentException.class, () -> coordinator.control(IcebergPayload.PayloadType.PREPARE_RESYNC,
                request().setResyncCheckpoint(checkpoint)
                        .setResyncDeadlineMs(clock.millis() + ResyncCoordinator.MAX_ATTEMPT_MILLIS).build()));
        assertNull(coordinator.route(TARGET));
        assertEquals(originalCommits, catalog.commits);
    }

    @ParameterizedTest
    @ValueSource(strings = {"{", "{} {}"})
    void preparationRejectsInvalidOrTrailingJsonBeforeStaging(String checkpoint) throws Exception {
        create(2);
        int originalCommits = catalog.commits;
        assertThrows(JsonProcessingException.class, () -> coordinator.control(IcebergPayload.PayloadType.PREPARE_RESYNC,
                request().setResyncCheckpoint(checkpoint)
                        .setResyncDeadlineMs(clock.millis() + ResyncCoordinator.MAX_ATTEMPT_MILLIS).build()));
        assertNull(coordinator.route(TARGET));
        assertEquals(originalCommits, catalog.commits);
    }

    @Test
    void incompleteCheckpointAndBlankIdFailWithoutPublishing() throws Exception {
        create(2);
        assertThrows(IllegalArgumentException.class, () -> coordinator.control(IcebergPayload.PayloadType.PREPARE_RESYNC,
                request().setResyncId(" ").build()));
        prepare();
        assertThrows(IllegalArgumentException.class, () -> coordinator.control(IcebergPayload.PayloadType.PUBLISH_RESYNC,
                request().clearResyncCheckpoint().build()));
        assertFalse(coordinator.control(IcebergPayload.PayloadType.RESYNC_STATUS, request().build()).getResyncPublished());
    }

    @ParameterizedTest
    @ValueSource(ints = {2, 3})
    void publicationPreservesDeleteApplicabilityAndReportsTheCompleteReplacement(int version) throws Exception {
        Table original = create(version);
        DataFile old = append(original, "old", "old-deleted");
        original.newRowDelta().addDeletes(deletePosition(original, old, 1, version))
                .addDeletes(deleteEquality(original, "id", "old")).commit();
        TableMetadata base = metadata(original);
        // Iceberg's removed-file APIs intentionally return copyWithoutStats, unlike live/added files.
        Map<String, List<Object>> oldFiles = liveFileState(original, true);
        prepare();
        Table stage = coordinator.route(TARGET);
        DataFile first = append(stage, "first", "deleted", "same");
        stage.newRowDelta().addDeletes(deleteEquality(stage, "id", "same")).commit();
        // This occurrence must survive: equality deletes apply only to earlier data sequences.
        DataFile second = append(stage, "same", "later", "second-deleted");
        List<DeleteFile> positionDeletes = deletePositions(stage, first, 1, second, 2, version);
        org.apache.iceberg.RowDelta delta = stage.newRowDelta();
        positionDeletes.forEach(delta::addDeletes);
        delta.commit();
        if (version == 3) {
            assertEquals(2, positionDeletes.size());
            assertEquals(positionDeletes.get(0).location(), positionDeletes.get(1).location());
            assertNotEquals(positionDeletes.get(0).referencedDataFile(), positionDeletes.get(1).referencedDataFile());
            assertNotEquals(positionDeletes.get(0).contentOffset(), positionDeletes.get(1).contentOffset());
        }
        assertEquals(List.of("first", "later", "same"), ids(stage));
        TableMetadata staged = metadata(stage);
        Map<String, List<Object>> stagedFiles = liveFileState(stage);
        Map<String, Long> stagedRowIds = version == 3 ? rowIds(stage) : Map.of();
        int beforePublication = catalog.commits;

        publish();

        Table published = catalog.loadTable(TARGET);
        Snapshot snapshot = published.currentSnapshot();
        assertEquals(beforePublication + 1, catalog.commits);
        assertEquals(List.of("first", "later", "same"), ids(published));
        assertEquals(stagedFiles, liveFileState(published));
        assertEquals(Set.of(first.location(), second.location()), fileState(snapshot.addedDataFiles(published.io())).keySet());
        Map<String, List<Object>> added = fileState(snapshot.addedDataFiles(published.io()));
        added.putAll(fileState(snapshot.addedDeleteFiles(published.io())));
        assertEquals(stagedFiles, added);
        Map<String, List<Object>> removed = fileState(snapshot.removedDataFiles(published.io()));
        removed.putAll(fileState(snapshot.removedDeleteFiles(published.io())));
        assertEquals(oldFiles, removed);
        assertEquals("overwrite", snapshot.operation());
        for (Map.Entry<String, String> expected : Map.of("added-data-files", "2", "deleted-data-files", "1",
                "added-delete-files", "3", "removed-delete-files", "2", "total-data-files", "2",
                "total-delete-files", "3", "added-records", "6", "deleted-records", "2", "total-records", "6").entrySet()) {
            assertEquals(expected.getValue(), snapshot.summary().get(expected.getKey()), expected.getKey());
        }
        assertPublicLineage(base, staged, metadata(published));
        Map<String, Long> positions = new HashMap<>();
        TableIndexScanner.scan(published, "id", null, new TableIndexScanner.EntryConsumer() {
            @Override public void begin(long snapshotId) { assertEquals(snapshot.snapshotId(), snapshotId); }
            @Override public void accept(String id, String path, long position) { positions.put(id, position); }
        });
        assertEquals(Map.of("first", 0L, "same", 0L, "later", 1L), positions);
        if (version == 3) {
            assertEquals(stagedRowIds, rowIds(published));
        }
    }

    @Test
    void publicationPreservesReservedRowIdsIncludingGapsAndTheNextAppend() throws Exception {
        Table original = create(3);
        append(original, "old-a", "old-b");
        long originalNextRowId = metadata(original).nextRowId();
        prepare();
        Table stage = coordinator.route(TARGET);
        DataFile discarded = append(stage, "discarded-a", "discarded-b");
        DataFile retained = append(stage, "kept-a", "kept-b");
        stage.newDelete().deleteFile(discarded).commit();
        append(stage, "kept-c");
        stage.newRowDelta().addDeletes(deletePosition(stage, retained, 1, 3)).commit();
        Map<String, Long> stagedIds = rowIds(stage);
        long stagedNextRowId = metadata(stage).nextRowId();
        assertEquals(originalNextRowId + 5, stagedNextRowId);
        assertEquals(Map.of("kept-a", originalNextRowId + 2, "kept-c", originalNextRowId + 4), stagedIds);

        publish();

        Table published = catalog.loadTable(TARGET);
        assertEquals(stagedIds, rowIds(published));
        assertEquals(stagedNextRowId, metadata(published).nextRowId());
        assertEquals(Long.valueOf(originalNextRowId), published.currentSnapshot().firstRowId());
        assertEquals(Long.valueOf(5), published.currentSnapshot().addedRows());
        append(published, "after-publication");
        Map<String, Long> appendedIds = rowIds(published);
        assertEquals(Long.valueOf(stagedNextRowId), appendedIds.remove("after-publication"));
        assertEquals(stagedIds, appendedIds);
        assertEquals(stagedNextRowId + 1, metadata(published).nextRowId());
    }

    @Test
    void metadataRewriteChecksDeadlineDuringEnumerationWithoutPublishing() throws Exception {
        Table original = create(2);
        append(original, "old-a");
        append(original, "old-b");
        TableMetadata base = metadata(original);
        prepare();
        Table stage = coordinator.route(TARGET);
        append(stage, "new-a");
        append(stage, "new-b");
        int beforePublication = catalog.commits;
        int beforeMetadataWrites = catalog.metadataWrites;
        AtomicInteger callbacks = new AtomicInteger();
        ResyncCoordinator.ShadowOperations operations = (ResyncCoordinator.ShadowOperations) ResyncCoordinator.operations(stage);
        IllegalStateException failure = assertThrows(IllegalStateException.class,
                () -> ResyncMetadata.forPublication(base, metadata(stage), operations,
                        Math.max(System.currentTimeMillis(), metadata(stage).lastUpdatedMillis()), () -> {
                            callbacks.incrementAndGet();
                            // The list and first rewritten manifest exist: cancellation is not just a preflight check.
                            if (catalog.metadataWrites >= beforeMetadataWrites + 2) {
                                clock.now += ResyncCoordinator.MAX_ATTEMPT_MILLIS;
                            }
                            operations.requireActive();
                        }));
        assertTrue(callbacks.get() > 1);
        assertEquals(beforeMetadataWrites + 2, catalog.metadataWrites);
        assertTrue(failure.getMessage().contains("deadline"));
        assertEquals(beforePublication, catalog.commits);
        Table visible = catalog.loadTable(TARGET);
        assertEquals(base.currentSnapshot().snapshotId(), visible.currentSnapshot().snapshotId());
        assertEquals(base.snapshotLog(), visible.history());
        assertEquals(List.of("old-a", "old-b"), ids(visible));
        assertThrows(IllegalStateException.class, this::publish);
    }

    @Test
    void cancelledGrpcContextFailsPublicationAndPermanentlyClosesTheStage() throws Exception {
        Table original = create(2);
        append(original, "old");
        long originalSnapshot = original.currentSnapshot().snapshotId();
        prepare();
        Table stage = coordinator.route(TARGET);
        append(stage, "new");
        int beforePublication = catalog.commits;
        io.grpc.Context.CancellableContext context = io.grpc.Context.current().withCancellation();
        context.cancel(new IOException("client cancelled publication"));
        context.run(() -> assertThrows(IllegalStateException.class, this::publish));
        assertEquals(beforePublication, catalog.commits);
        assertEquals(originalSnapshot, catalog.loadTable(TARGET).currentSnapshot().snapshotId());
        assertEquals(List.of("old"), ids(catalog.loadTable(TARGET)));
        assertThrows(IllegalStateException.class,
                () -> stage.io().newOutputFile(stage.location() + "/cancelled.parquet"));
        assertThrows(IllegalStateException.class, () -> append(stage, "too-late"));
        assertFalse(coordinator.control(IcebergPayload.PayloadType.ABORT_RESYNC, request().build()).getResyncPublished());
        assertThrows(IllegalStateException.class, this::publish);
    }

    @Test
    void publicationRewritesOnlyMetadataAndPreservesPartitionSpecsAndEncryptionKeys() throws Exception {
        Table original = create(2);
        original.updateSpec().addField("id").commit();
        DataFile old = metadataOnlyData(original, "old");
        original.newAppend().appendFile(old).commit();
        original.newRowDelta().addDeletes(metadataOnlyDelete(original, old)).commit();
        Map<String, List<Object>> oldFiles = liveFileState(original);
        prepare();
        Table stage = coordinator.route(TARGET);
        DataFile first = metadataOnlyData(stage, "first");
        stage.newAppend().appendFile(first).commit();
        stage.newRowDelta().addDeletes(metadataOnlyDelete(stage, first)).commit();
        stage.updateSpec().removeField("id").addField(org.apache.iceberg.expressions.Expressions.bucket("id", 8)).commit();
        DataFile second = metadataOnlyData(stage, "second");
        stage.newAppend().appendFile(second).commit();
        stage.newRowDelta().addDeletes(metadataOnlyDelete(stage, second)).commit();
        assertNotEquals(first.specId(), second.specId());
        assertFalse(stage.io().newInputFile(first.location()).exists());
        assertFalse(stage.io().newInputFile(second.location()).exists());
        Map<String, List<Object>> expectedFiles = liveFileState(stage);

        publish();

        Table published = catalog.loadTable(TARGET);
        assertEquals(expectedFiles, liveFileState(published));
        Map<String, List<Object>> added = fileState(published.currentSnapshot().addedDataFiles(published.io()));
        added.putAll(fileState(published.currentSnapshot().addedDeleteFiles(published.io())));
        assertEquals(expectedFiles, added);
        Map<String, List<Object>> removed = fileState(published.currentSnapshot().removedDataFiles(published.io()));
        removed.putAll(fileState(published.currentSnapshot().removedDeleteFiles(published.io())));
        assertEquals(oldFiles, removed);
    }

    private Table create(int version) {
        return catalog.createTable(TARGET, SCHEMA, PartitionSpec.unpartitioned(),
                Map.of("format-version", Integer.toString(version)));
    }

    private void prepare() throws Exception {
        coordinator.control(IcebergPayload.PayloadType.PREPARE_RESYNC,
                request().setResyncDeadlineMs(clock.millis() + ResyncCoordinator.MAX_ATTEMPT_MILLIS).build());
    }

    private RecordIngestResponse publish() throws Exception {
        return coordinator.control(IcebergPayload.PayloadType.PUBLISH_RESYNC,
                request().setResyncCheckpoint(CHECKPOINT).build());
    }

    private IcebergPayload.Metadata.Builder request() {
        return IcebergPayload.Metadata.newBuilder().setNamespace("db").setDestTableName("target").setResyncId("operation")
                .setResyncCheckpoint(BASELINE);
    }

    private static String checkpoint(String mode, int targetCursor, int unrelatedCursor) {
        String global = mode.equals("cdc")
                ? "\"global\":{\"state\":{\"lsn\":" + targetCursor + "},\"streams\":[\"db.target\",\"db.unrelated\"]},"
                : "";
        return "{" + global + "\"streams\":[{\"namespace\":\"db\",\"state\":{\"cursor\":" + targetCursor
                + "},\"stream\":\"target\",\"sync_mode\":\"" + mode
                + "\"},{\"namespace\":\"db\",\"state\":{\"cursor\":" + unrelatedCursor
                + "},\"stream\":\"unrelated\",\"sync_mode\":\"incremental\"}],\"type\":\""
                + (mode.equals("cdc") ? "GLOBAL" : "STREAM") + "\"}";
    }

    private static TableMetadata metadata(Table table) {
        return ResyncCoordinator.operations(table).current();
    }

    private static TableMetadata publication(Table original, Table stage) throws IOException {
        return ResyncMetadata.forPublication(metadata(original), metadata(stage), ResyncCoordinator.operations(stage),
                Math.max(System.currentTimeMillis(), metadata(stage).lastUpdatedMillis()), () -> { });
    }

    private static void assertPublicLineage(TableMetadata original, TableMetadata staged, TableMetadata published) {
        Snapshot snapshot = published.currentSnapshot();
        assertEquals(staged.currentSnapshot().snapshotId(), snapshot.snapshotId());
        assertEquals(staged.currentSnapshot().sequenceNumber(), snapshot.sequenceNumber());
        assertEquals(original.currentSnapshot() == null ? null : original.currentSnapshot().snapshotId(), snapshot.parentId());
        assertTrue(snapshot.timestampMillis() > original.lastUpdatedMillis());
        assertTrue(snapshot.timestampMillis() >= staged.lastUpdatedMillis());
        assertEquals(original.snapshotLog(), published.snapshotLog().subList(0, original.snapshotLog().size()));
        assertEquals(original.snapshotLog().size() + 1, published.snapshotLog().size());
        HistoryEntry finalEntry = published.snapshotLog().get(original.snapshotLog().size());
        assertEquals(snapshot.snapshotId(), finalEntry.snapshotId());
        assertEquals(snapshot.timestampMillis(), finalEntry.timestampMillis());
        Set<Long> expectedSnapshots = new java.util.HashSet<>(original.snapshots().stream().map(Snapshot::snapshotId).toList());
        expectedSnapshots.add(snapshot.snapshotId());
        assertEquals(expectedSnapshots, new java.util.HashSet<>(published.snapshots().stream().map(Snapshot::snapshotId).toList()));
        for (Snapshot retained : original.snapshots()) {
            Snapshot actual = published.snapshot(retained.snapshotId());
            assertEquals(retained.parentId(), actual.parentId());
            assertEquals(retained.timestampMillis(), actual.timestampMillis());
            assertEquals(retained.manifestListLocation(), actual.manifestListLocation());
        }
        original.refs().forEach((name, reference) -> {
            SnapshotRef actual = published.ref(name);
            assertNotNull(actual, name);
            assertEquals(reference.type(), actual.type());
            assertEquals(reference.minSnapshotsToKeep(), actual.minSnapshotsToKeep());
            assertEquals(reference.maxSnapshotAgeMs(), actual.maxSnapshotAgeMs());
            assertEquals(reference.maxRefAgeMs(), actual.maxRefAgeMs());
            assertEquals(name.equals("main") ? snapshot.snapshotId() : reference.snapshotId(), actual.snapshotId());
        });
        assertEquals(original.refs().keySet(), published.refs().keySet());
        assertEquals(original.statisticsFiles(), published.statisticsFiles());
        assertEquals(original.partitionStatisticsFiles(), published.partitionStatisticsFiles());
        assertEquals(staged.nextRowId(), published.nextRowId());
    }

    private static void assertPublicationChanges(TableMetadata original, TableMetadata publication) {
        assertEquals(1, publication.changes().stream().filter(MetadataUpdate.AddSnapshot.class::isInstance).count());
        assertEquals(1, publication.changes().stream().filter(MetadataUpdate.SetSnapshotRef.class::isInstance)
                .map(MetadataUpdate.SetSnapshotRef.class::cast).filter(update -> update.name().equals("main")).count());
        assertFalse(publication.changes().stream().anyMatch(MetadataUpdate.RemoveSnapshots.class::isInstance));
        for (MetadataUpdate update : publication.changes()) {
            if (update instanceof MetadataUpdate.AddSnapshot added) {
                assertEquals(publication.currentSnapshot().snapshotId(), added.snapshot().snapshotId());
                assertEquals(original.currentSnapshot().snapshotId(), added.snapshot().parentId());
            } else if (update instanceof MetadataUpdate.SetSnapshotRef reference) {
                assertEquals(publication.ref(reference.name()).snapshotId(), reference.snapshotId());
            }
        }
    }

    private static Map<String, List<Object>> liveFileState(Table table) throws IOException {
        return liveFileState(table, false);
    }

    private static Map<String, List<Object>> liveFileState(Table table, boolean withoutStats) throws IOException {
        Map<String, List<Object>> files = new HashMap<>();
        if (table.currentSnapshot() == null) {
            return files;
        }
        for (ManifestFile manifest : table.currentSnapshot().dataManifests(table.io())) {
            try (ManifestReader<DataFile> reader = ManifestFiles.read(manifest, table.io(), table.specs())) {
                files.putAll(fileState(reader, withoutStats));
            }
        }
        for (ManifestFile manifest : table.currentSnapshot().deleteManifests(table.io())) {
            try (ManifestReader<DeleteFile> reader = ManifestFiles.readDeleteManifest(manifest, table.io(), table.specs())) {
                files.putAll(fileState(reader, withoutStats));
            }
        }
        return files;
    }

    private static Map<String, List<Object>> fileState(Iterable<? extends ContentFile<?>> files) {
        return fileState(files, false);
    }

    private static Map<String, List<Object>> fileState(Iterable<? extends ContentFile<?>> files, boolean withoutStats) {
        Map<String, List<Object>> states = new HashMap<>();
        for (ContentFile<?> file : files) {
            if (withoutStats) {
                file = (ContentFile<?>) file.copyWithoutStats();
            }
            DeleteFile delete = file instanceof DeleteFile ? (DeleteFile) file : null;
            String key = delete == null ? file.location() : file.location() + "#" + delete.referencedDataFile()
                    + ":" + delete.contentOffset() + ":" + delete.contentSizeInBytes();
            states.put(key, Arrays.asList(file.content(), file.specId(), file.partition().toString(),
                    file.dataSequenceNumber(), file.fileSequenceNumber(), file.firstRowId(), file.equalityFieldIds(),
                    file.keyMetadata(), file.recordCount(), file.fileSizeInBytes(), file.format(), file.sortOrderId(),
                    file.splitOffsets(), file.columnSizes(), file.valueCounts(), file.nullValueCounts(), file.nanValueCounts(),
                    file.lowerBounds(), file.upperBounds(), delete == null ? null : delete.referencedDataFile(),
                    delete == null ? null : delete.contentOffset(), delete == null ? null : delete.contentSizeInBytes()));
        }
        return states;
    }

    private static Map<String, Long> rowIds(Table table) throws IOException {
        Map<String, Long> ids = new HashMap<>();
        Schema projection = new Schema(table.schema().findField("id"), MetadataColumns.ROW_ID);
        try (CloseableIterable<Record> rows = IcebergGenerics.read(table).project(projection).build()) {
            for (Record row : rows) {
                ids.put(row.getField("id").toString(), (Long) row.getField(MetadataColumns.ROW_ID.name()));
            }
        }
        return ids;
    }

    private static List<String> idsAsOf(Table table, long timestampMillis) throws IOException {
        List<String> ids = new ArrayList<>();
        try (CloseableIterable<Record> rows = IcebergGenerics.read(table).asOfTime(timestampMillis).build()) {
            for (Record row : rows) {
                ids.add(row.getField("id").toString());
            }
        }
        ids.sort(String::compareTo);
        return ids;
    }

    private static DeleteFile deleteEquality(Table table, String field, String value) throws IOException {
        Schema keys = new Schema(table.schema().findField(field));
        GenericAppenderFactory factory = new GenericAppenderFactory(table.schema(), table.spec(),
                new int[] {keys.columns().get(0).fieldId()}, keys, null);
        String path = table.locationProvider().newDataLocation(UUID.randomUUID() + ".parquet");
        EqualityDeleteWriter<Record> writer = factory.newEqDeleteWriter(
                EncryptedFiles.plainAsEncryptedOutput(table.io().newOutputFile(path)), FileFormat.PARQUET, null);
        try (writer) {
            GenericRecord deleted = GenericRecord.create(keys);
            deleted.setField(field, value);
            writer.write(deleted);
        }
        return writer.toDeleteFile();
    }

    private static DataFile metadataOnlyData(Table table, String id) {
        GenericRecord row = GenericRecord.create(table.schema());
        row.setField("id", id);
        org.apache.iceberg.PartitionKey partition = new org.apache.iceberg.PartitionKey(table.spec(), table.schema());
        partition.partition(row);
        return DataFiles.builder(table.spec()).withPath(table.location() + "/" + id + "-metadata-only.parquet")
                .withFormat(FileFormat.PARQUET).withPartition(partition).withRecordCount(4).withFileSizeInBytes(256)
                .withEncryptionKeyMetadata(ByteBuffer.wrap(("data-key-" + id).getBytes(StandardCharsets.UTF_8))).build();
    }

    private static DeleteFile metadataOnlyDelete(Table table, DataFile data) {
        return FileMetadata.deleteFileBuilder(table.specs().get(data.specId())).ofEqualityDeletes(1)
                .withPath(data.location() + ".delete.parquet").withFormat(FileFormat.PARQUET).withPartition(data.partition())
                .withRecordCount(1).withFileSizeInBytes(64)
                .withEncryptionKeyMetadata(ByteBuffer.wrap("delete-key".getBytes(StandardCharsets.UTF_8))).build();
    }

    private static DataFile append(Table table, String... ids) throws Exception {
        DataFile file = write(table, ids);
        table.newAppend().appendFile(file).commit();
        return file;
    }

    private static void transactionAppend(Table table, String... ids) throws Exception {
        DataFile data = write(table, ids);
        Transaction chunk = table.newTransaction();
        chunk.newAppend().appendFile(data).commit();
        chunk.commitTransaction();
    }

    private static DataFile write(Table table, String... ids) throws Exception {
        String path = table.locationProvider().newDataLocation(UUID.randomUUID() + ".parquet");
        DataWriter<Record> writer = new GenericAppenderFactory(table.schema(), table.spec()).newDataWriter(
                EncryptedFiles.plainAsEncryptedOutput(table.io().newOutputFile(path)), FileFormat.PARQUET, null);
        try (writer) {
            for (String id : ids) {
                GenericRecord row = GenericRecord.create(table.schema());
                row.setField("id", id);
                row.setField("value", id);
                writer.write(row);
            }
        }
        return writer.toDataFile();
    }

    private static List<DeleteFile> deletePositions(Table table, DataFile first, long firstPosition,
                                                   DataFile second, long secondPosition, int version) throws Exception {
        if (version == 2) {
            return List.of(deletePosition(table, first, firstPosition, version),
                    deletePosition(table, second, secondPosition, version));
        }
        DVFileWriter writer = new BaseDVFileWriter(OutputFileFactory.builderFor(table, 0, 0).build(), path -> null);
        try (writer) {
            writer.delete(first.location(), firstPosition, table.specs().get(first.specId()), first.partition());
            writer.delete(second.location(), secondPosition, table.specs().get(second.specId()), second.partition());
        }
        return writer.result().deleteFiles();
    }

    private static DeleteFile deletePosition(Table table, DataFile data, long position, int version) throws Exception {
        if (version == 3) {
            DVFileWriter writer = new BaseDVFileWriter(OutputFileFactory.builderFor(table, 0, 0).build(), path -> null);
            try (writer) {
                writer.delete(data.location(), position, table.spec(), data.partition());
            }
            return writer.result().deleteFiles().get(0);
        }
        String path = table.locationProvider().newDataLocation(UUID.randomUUID() + ".parquet");
        PositionDeleteWriter<Record> writer = new GenericAppenderFactory(table.schema(), table.spec()).newPosDeleteWriter(
                EncryptedFiles.plainAsEncryptedOutput(table.io().newOutputFile(path)), FileFormat.PARQUET, null);
        try (writer) {
            writer.write(PositionDelete.<Record>create().set(data.location(), position));
        }
        return writer.toDeleteFile();
    }

    private static List<String> ids(Table table) throws Exception {
        return idsAt(table, table.currentSnapshot() == null ? 0 : table.currentSnapshot().snapshotId());
    }

    private static List<String> idsAt(Table table, long snapshot) throws Exception {
        if (snapshot == 0) {
            return List.of();
        }
        List<String> ids = new ArrayList<>();
        try (CloseableIterable<Record> rows = IcebergGenerics.read(table).useSnapshot(snapshot).build()) {
            for (Record row : rows) {
                ids.add(row.getField("id").toString());
            }
        }
        ids.sort(String::compareTo);
        return ids;
    }

    private static final class FaultCatalog extends HadoopCatalog {
        boolean failBeforeCommit;
        boolean failAfterCommit;
        int commits;
        int metadataWrites;

        FaultCatalog(Configuration configuration, String warehouse) {
            super(configuration, warehouse);
        }

        @Override protected TableOperations newTableOps(TableIdentifier identifier) {
            TableOperations delegate = super.newTableOps(identifier);
            return new TableOperations() {
                @Override public TableMetadata current() { return delegate.current(); }
                @Override public TableMetadata refresh() { return delegate.refresh(); }
                @Override public void commit(TableMetadata base, TableMetadata metadata) {
                    if (failBeforeCommit) {
                        throw new CommitStateUnknownException("Injected lost catalog response", new IOException("catalog unavailable"));
                    }
                    delegate.commit(base, metadata);
                    commits++;
                    if (failAfterCommit) {
                        throw new CommitStateUnknownException("Injected lost successful response", new IOException("response lost"));
                    }
                }
                @Override public org.apache.iceberg.io.FileIO io() {
                    org.apache.iceberg.io.FileIO files = delegate.io();
                    return new org.apache.iceberg.io.FileIO() {
                        @Override public org.apache.iceberg.io.InputFile newInputFile(String path) {
                            return files.newInputFile(path);
                        }
                        @Override public org.apache.iceberg.io.OutputFile newOutputFile(String path) {
                            if (path.endsWith(".avro")) {
                                metadataWrites++;
                            }
                            return files.newOutputFile(path);
                        }
                        @Override public void deleteFile(String path) { files.deleteFile(path); }
                        @Override public Map<String, String> properties() { return files.properties(); }
                    };
                }
                @Override public org.apache.iceberg.encryption.EncryptionManager encryption() { return delegate.encryption(); }
                @Override public String metadataFileLocation(String file) { return delegate.metadataFileLocation(file); }
                @Override public org.apache.iceberg.io.LocationProvider locationProvider() { return delegate.locationProvider(); }
                @Override public TableOperations temp(TableMetadata metadata) { return delegate.temp(metadata); }
                @Override public long newSnapshotId() { return delegate.newSnapshotId(); }
                @Override public boolean requireStrictCleanup() { return delegate.requireStrictCleanup(); }
            };
        }
    }

    private static final class MutableClock extends Clock {
        long now = Instant.parse("2026-11-01T00:00:00Z").toEpochMilli();
        @Override public ZoneId getZone() { return ZoneOffset.UTC; }
        @Override public Clock withZone(ZoneId zone) { return this; }
        @Override public Instant instant() { return Instant.ofEpochMilli(now); }
        @Override public long millis() { return now; }
    }
}
