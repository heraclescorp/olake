package io.olake.iceberg.rpc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

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

import io.grpc.stub.StreamObserver;
import io.olake.iceberg.rpc.RecordIngest.IcebergPayload;
import io.olake.iceberg.rpc.RecordIngest.RecordIngestResponse;

class OlakeRowsIngesterTest {
    private static final TableIdentifier TABLE_ID = TableIdentifier.of("db", "stream");
    private InMemoryCatalog catalog;
    private OlakeRowsIngester ingester;

    @BeforeEach
    void setUp() {
        catalog = new InMemoryCatalog();
        catalog.initialize("test", Map.of());
        catalog.createNamespace(Namespace.of("db"));
        ingester = new OlakeRowsIngester(catalog, new ConcurrentHashMap<>());
    }

    @AfterEach
    void closeCatalog() throws Exception {
        catalog.close();
    }

    private RecordIngestResponse initialize(IcebergPayload.DeleteMode mode) throws Exception {
        IcebergPayload request = IcebergPayload.newBuilder()
                .setType(IcebergPayload.PayloadType.GET_OR_CREATE_TABLE)
                .setMetadata(IcebergPayload.Metadata.newBuilder().setNamespace("db").setDestTableName("stream")
                        .setThreadId("chunk-thread").setIdentifierField("id").setDeleteMode(mode)
                        .addSchema(IcebergPayload.SchemaField.newBuilder().setKey("id").setIceType("long")))
                .build();
        CompletableFuture<RecordIngestResponse> response = new CompletableFuture<>();
        ingester.sendRecords(request, new StreamObserver<>() {
            @Override public void onNext(RecordIngestResponse value) { response.complete(value); }
            @Override public void onError(Throwable error) { response.completeExceptionally(error); }
            @Override public void onCompleted() { }
        });
        return response.get(10, TimeUnit.SECONDS);
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
}
