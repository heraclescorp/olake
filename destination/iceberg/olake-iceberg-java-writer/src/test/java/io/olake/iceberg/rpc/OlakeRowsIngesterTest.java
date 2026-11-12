package io.olake.iceberg.rpc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.olake.iceberg.ExecutionFence;

import io.grpc.stub.StreamObserver;
import io.olake.iceberg.rpc.RecordIngest.IcebergPayload;
import io.olake.iceberg.rpc.RecordIngest.RecordIngestResponse;

class OlakeRowsIngesterTest {
    private static final TableIdentifier TABLE_ID = TableIdentifier.of("db", "stream");
    private InMemoryCatalog catalog;
    private OlakeRowsIngester ingester;
    @TempDir Path directory;

    @BeforeEach
    void setUp() {
        ExecutionFence.configure(Map.of());
        catalog = new InMemoryCatalog();
        catalog.initialize("test", Map.of());
        catalog.createNamespace(Namespace.of("db"));
        ingester = new OlakeRowsIngester(catalog, new ConcurrentHashMap<>());
    }

    @AfterEach
    void closeCatalog() throws Exception {
        ExecutionFence.configure(Map.of());
        catalog.close();
    }

    private RecordIngestResponse initialize(IcebergPayload.DeleteMode mode) throws Exception {
        IcebergPayload request = IcebergPayload.newBuilder()
                .setType(IcebergPayload.PayloadType.GET_OR_CREATE_TABLE)
                .setMetadata(IcebergPayload.Metadata.newBuilder().setNamespace("db").setDestTableName("stream")
                        .setThreadId("chunk-thread").setIdentifierField("id").setDeleteMode(mode)
                        .addSchema(IcebergPayload.SchemaField.newBuilder().setKey("id").setIceType("long")))
                .build();
        return call(request);
    }

    private RecordIngestResponse call(IcebergPayload request) throws Exception {
        CompletableFuture<RecordIngestResponse> response = new CompletableFuture<>();
        ingester.sendRecords(request, new StreamObserver<>() {
            @Override public void onNext(RecordIngestResponse value) { response.complete(value); }
            @Override public void onError(Throwable error) { response.completeExceptionally(error); }
            @Override public void onCompleted() { }
        });
        return response.get(10, TimeUnit.SECONDS);
    }

    @Test
    void capabilityReportsStartupAuthorityWithoutTableOrSessionMutation() throws Exception {
        Path fence = directory.resolve("fence.json");
        Files.writeString(fence, "{\"version\":1,\"execution_id\":\"configured-runtime\",\"state\":\"open\"}");
        ExecutionFence.configure(Map.of("execution-id", "configured-runtime", "execution-fence", fence.toString(),
                "resync-launch-id", "original-launch", "resync-recover-only", "true"));
        IcebergPayload request = IcebergPayload.newBuilder().setType(IcebergPayload.PayloadType.EXECUTION_CAPABILITY).build();
        RecordIngestResponse response = call(request);
        assertTrue(response.getSuccess());
        JsonNode capability = new ObjectMapper().readTree(response.getResult());
        assertEquals(1, capability.path("version").asInt());
        assertEquals("configured-runtime", capability.path("execution_id").asText());
        assertEquals("original-launch", capability.path("launch_id").asText());
        assertTrue(capability.path("recover_only").asBoolean());
        assertEquals("open", capability.path("fence_state").asText());
        assertTrue(catalog.listTables(Namespace.of("db")).isEmpty());
        Files.writeString(fence, "{\"version\":1,\"execution_id\":\"configured-runtime\",\"state\":\"closed\"}");
        assertThrows(java.util.concurrent.ExecutionException.class, () -> call(request));
        assertTrue(catalog.listTables(Namespace.of("db")).isEmpty());
    }

    @Test
    void terminalRpcUsesConfiguredIdentityNotRequestMetadataAndAcceptsClosedFence() throws Exception {
        Path fence = directory.resolve("retirement.json");
        Files.writeString(fence, "{\"version\":1,\"execution_id\":\"runtime\",\"state\":\"open\"}");
        ExecutionFence.configure(Map.of("execution-id", "runtime", "execution-fence", fence.toString(),
                "execution-launch-id", "runtime", "execution-command", "clear-destination"));
        Files.writeString(fence, "{\"version\":1,\"execution_id\":\"runtime\",\"state\":\"closed\"}");
        IcebergPayload request = IcebergPayload.newBuilder().setType(IcebergPayload.PayloadType.EXECUTION_RETIRE)
                .setMetadata(IcebergPayload.Metadata.newBuilder().setResyncId("forged-request-identity")).build();
        RecordIngestResponse response = call(request);
        assertTrue(response.getSuccess());
        JsonNode proof = new ObjectMapper().readTree(response.getResult());
        assertEquals(1, proof.path("version").asInt());
        assertEquals("runtime", proof.path("execution_id").asText());
        assertEquals("runtime", proof.path("launch_id").asText());
        assertEquals("clear-destination", proof.path("command").asText());
        assertEquals("not_started", proof.path("outcome").asText());
        assertTrue(catalog.listTables(Namespace.of("db")).isEmpty());
        Files.writeString(fence, "{\"version\":1,\"execution_id\":\"other\",\"state\":\"closed\"}");
        assertThrows(java.util.concurrent.ExecutionException.class, () -> call(request));
    }

    @Test
    void standaloneRecoveryCapabilityDoesNotInventRuntimeIdentity() throws Exception {
        ExecutionFence.configure(Map.of("resync-launch-id", "original-launch", "resync-recover-only", "true"));
        JsonNode capability = new ObjectMapper().readTree(call(IcebergPayload.newBuilder()
                .setType(IcebergPayload.PayloadType.EXECUTION_CAPABILITY).build()).getResult());
        assertEquals("", capability.path("execution_id").asText());
        assertEquals("original-launch", capability.path("launch_id").asText());
        assertTrue(capability.path("recover_only").asBoolean());
        assertTrue(catalog.listTables(Namespace.of("db")).isEmpty());
    }

    @Test
    void retryInitializationReadsNewlyCommittedChunkRecoveryState() throws Exception {
        catalog.createTable(TABLE_ID, new Schema(Types.NestedField.required(1, "id", Types.LongType.get())));
        initialize(IcebergPayload.DeleteMode.DELETE_MODE_POSITION);
        String recovery = "{\"full_refresh_committed_ids\":[\"chunk-thread\"]}";
        catalog.loadTable(TABLE_ID).updateProperties().set("olake_2pc", recovery).commit();

        RecordIngestResponse response = initialize(IcebergPayload.DeleteMode.DELETE_MODE_POSITION);

        assertEquals(recovery, response.getOlake2PcState());
        assertEquals(0, response.getSnapshotId());
        assertFalse(response.getHasEqualityDeletes());
    }

    @Test
    void initializationCreatesMissingTableAtRequestedFormatVersion() throws Exception {
        RecordIngestResponse response = initialize(IcebergPayload.DeleteMode.DELETE_MODE_DELETION_VECTOR);

        Table table = catalog.loadTable(TABLE_ID);
        assertNotNull(table.schema().findField("id"));
        assertEquals(3, ((HasTableOperations) table).operations().current().formatVersion());
        assertEquals("", response.getOlake2PcState());
        assertEquals(0, response.getSnapshotId());
    }

    @Test
    void initializationPreservesRecoveryStateWhenUpgradingExistingTable() throws Exception {
        String recovery = "{\"full_refresh_committed_ids\":[\"previous-chunk\"]}";
        catalog.createTable(TABLE_ID, new Schema(Types.NestedField.required(1, "id", Types.LongType.get())))
                .updateProperties().set(TableProperties.FORMAT_VERSION, "2").set("olake_2pc", recovery).commit();

        RecordIngestResponse response = initialize(IcebergPayload.DeleteMode.DELETE_MODE_DELETION_VECTOR);

        assertEquals(3, ((HasTableOperations) catalog.loadTable(TABLE_ID)).operations().current().formatVersion());
        assertEquals(recovery, response.getOlake2PcState());
    }

    @Test
    void controlRpcRoutesBeforeFormatAndSchemaMutationsAndWaitsForClose() throws Exception {
        Table original = catalog.createTable(TABLE_ID,
                new Schema(Types.NestedField.required(1, "id", Types.LongType.get())),
                org.apache.iceberg.PartitionSpec.unpartitioned(), Map.of("format-version", "2"));
        String uuid = ((HasTableOperations) original).operations().current().uuid();
        IcebergPayload.Metadata.Builder control = IcebergPayload.Metadata.newBuilder()
                .setNamespace("db").setDestTableName("stream").setResyncId("stable-operation")
                .setResyncCheckpoint("{\"streams\":[{\"name\":\"unrelated\"}]}")
                .setResyncDeadlineMs(System.currentTimeMillis() + ResyncCoordinator.MAX_ATTEMPT_MILLIS);
        call(IcebergPayload.newBuilder().setType(IcebergPayload.PayloadType.PREPARE_RESYNC)
                .setMetadata(control).build());
        initialize(IcebergPayload.DeleteMode.DELETE_MODE_DELETION_VECTOR);
        assertEquals(2, ((HasTableOperations) catalog.loadTable(TABLE_ID)).operations().current().formatVersion());
        call(IcebergPayload.newBuilder().setType(IcebergPayload.PayloadType.EVOLVE_SCHEMA)
                .setMetadata(IcebergPayload.Metadata.newBuilder().setThreadId("chunk-thread")
                        .addSchema(IcebergPayload.SchemaField.newBuilder().setKey("id").setIceType("long"))
                        .addSchema(IcebergPayload.SchemaField.newBuilder().setKey("staged").setIceType("string")))
                .build());
        assertNull(catalog.loadTable(TABLE_ID).schema().findField("staged"));
        IcebergPayload publish = IcebergPayload.newBuilder().setType(IcebergPayload.PayloadType.PUBLISH_RESYNC)
                .setMetadata(control).build();
        assertThrows(java.util.concurrent.ExecutionException.class, () -> call(publish));
        call(IcebergPayload.newBuilder().setType(IcebergPayload.PayloadType.CLOSE_SESSION)
                .setMetadata(IcebergPayload.Metadata.newBuilder().setThreadId("chunk-thread")).build());
        RecordIngestResponse receipt = call(publish);
        assertTrue(receipt.getResyncPublished());
        assertEquals(uuid, receipt.getResyncTableUuid());
        assertTrue(receipt.getSnapshotId() > 0);
        assertEquals(3, ((HasTableOperations) catalog.loadTable(TABLE_ID)).operations().current().formatVersion());
        assertNotNull(catalog.loadTable(TABLE_ID).schema().findField("staged"));
        // A fresh JVM recognizes the stable marker without resetting the table or opening a writer.
        ingester = new OlakeRowsIngester(catalog, new ConcurrentHashMap<>());
        RecordIngestResponse recovered = call(IcebergPayload.newBuilder().setType(IcebergPayload.PayloadType.PREPARE_RESYNC)
                .setMetadata(control.setResyncDeadlineMs(0)).build());
        assertEquals(receipt, recovered);
        RecordIngestResponse handshake = initialize(IcebergPayload.DeleteMode.DELETE_MODE_DELETION_VECTOR);
        assertEquals("stable-operation", handshake.getResyncGeneration());
    }

    @Test
    void prepareMissingTableFailsWithoutCreatingOrDroppingAnything() {
        IcebergPayload request = IcebergPayload.newBuilder().setType(IcebergPayload.PayloadType.PREPARE_RESYNC)
                .setMetadata(IcebergPayload.Metadata.newBuilder().setNamespace("db").setDestTableName("stream")
                        .setResyncId("operation").setResyncCheckpoint("{\"type\":\"STREAM\"}")
                        .setResyncDeadlineMs(System.currentTimeMillis() + 60_000)).build();
        assertThrows(java.util.concurrent.ExecutionException.class, () -> call(request));
        assertFalse(catalog.tableExists(TABLE_ID));
    }
    @Test
    void closedRuntimeAllowsOnlyReadOnlyRetirementControlsAfterPrepare() throws Exception {
        catalog.createTable(TABLE_ID, new Schema(Types.NestedField.required(1, "id", Types.LongType.get())));
        Path fence = directory.resolve("retirement-fence.json");
        Files.writeString(fence, "{\"version\":1,\"execution_id\":\"runtime\",\"state\":\"open\"}");
        ExecutionFence.configure(Map.of("execution-id", "runtime", "execution-fence", fence.toString(),
                "resync-launch-id", "launch"));
        IcebergPayload.Metadata metadata = IcebergPayload.Metadata.newBuilder().setNamespace("db")
                .setDestTableName("stream").setResyncId("operation").setResyncCheckpoint("{\"type\":\"STREAM\"}")
                .setResyncDeadlineMs(System.currentTimeMillis() + 60_000).build();
        call(IcebergPayload.newBuilder().setType(IcebergPayload.PayloadType.PREPARE_RESYNC).setMetadata(metadata).build());
        Files.writeString(fence, "{\"version\":1,\"execution_id\":\"runtime\",\"state\":\"closed\"}");
        for (IcebergPayload.PayloadType type : new IcebergPayload.PayloadType[] {
                IcebergPayload.PayloadType.RESYNC_STATUS, IcebergPayload.PayloadType.ABORT_RESYNC,
                IcebergPayload.PayloadType.RESYNC_STATUS}) {
            RecordIngestResponse response = call(IcebergPayload.newBuilder().setType(type).setMetadata(metadata).build());
            assertTrue(response.getSuccess());
            assertFalse(response.getResyncPublished());
        }
        assertThrows(java.util.concurrent.ExecutionException.class, () -> call(IcebergPayload.newBuilder()
                .setType(IcebergPayload.PayloadType.PUBLISH_RESYNC).setMetadata(metadata).build()));
        assertThrows(java.util.concurrent.ExecutionException.class, () -> call(IcebergPayload.newBuilder()
                .setType(IcebergPayload.PayloadType.EXECUTION_CAPABILITY).build()));
    }
}
