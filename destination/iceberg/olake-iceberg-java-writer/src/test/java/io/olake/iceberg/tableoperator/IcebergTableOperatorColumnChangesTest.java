package io.olake.iceberg.tableoperator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.olake.iceberg.rpc.RecordIngest.IcebergPayload.AddedColumn;
import io.olake.iceberg.rpc.RecordIngest.IcebergPayload.IceRecord.FieldValue;

class IcebergTableOperatorColumnChangesTest {

    private InMemoryCatalog catalog;

    @BeforeEach
    void setUp() {
        catalog = new InMemoryCatalog();
        catalog.initialize("test", Map.of());
        catalog.createNamespace(Namespace.of("db"));
    }

    private Table createTable(String name, int formatVersion) {
        Schema schema = new Schema(List.of(
                Types.NestedField.required(1, "_olake_id", Types.StringType.get()),
                Types.NestedField.optional(2, "region", Types.StringType.get()),
                Types.NestedField.optional(3, "note", Types.StringType.get())),
                Set.of(1));
        return catalog.buildTable(TableIdentifier.of("db", name), schema)
                .withPartitionSpec(PartitionSpec.builderFor(schema).identity("region").build())
                .withProperty("format-version", String.valueOf(formatVersion))
                .create();
    }

    private static AddedColumn added(String key, String iceType, FieldValue initialDefault) {
        AddedColumn.Builder builder = AddedColumn.newBuilder().setKey(key).setIceType(iceType);
        if (initialDefault != null) {
            builder.setInitialDefault(initialDefault);
        }
        return builder.build();
    }

    @Test
    void addsColumnWithInitialDefaultOnV3() {
        Table table = createTable("v3", 3);
        boolean changed = new IcebergTableOperator(false).applyColumnChanges(table, List.of(
                added("qty", "int", FieldValue.newBuilder().setIntValue(7).build()),
                added("ts", "timestamptz", FieldValue.newBuilder().setLongValue(1_700_000_000_000_000L).build()),
                added("empty", "string", null)), List.of());

        assertTrue(changed);
        table.refresh();
        assertEquals(7, table.schema().findField("qty").initialDefault());
        assertEquals(1_700_000_000_000_000L, table.schema().findField("ts").initialDefault());
        assertNull(table.schema().findField("empty").initialDefault());
    }

    @Test
    void addsColumnWithoutDefaultOnV2() {
        Table table = createTable("v2", 2);
        new IcebergTableOperator(false).applyColumnChanges(table, List.of(
                added("qty", "int", FieldValue.newBuilder().setIntValue(7).build())), List.of());

        table.refresh();
        assertNotNull(table.schema().findField("qty"));
        assertNull(table.schema().findField("qty").initialDefault());
    }

    @Test
    void dropsColumnButKeepsIdentifierAndPartitionSources() {
        Table table = createTable("drop", 3);
        boolean changed = new IcebergTableOperator(false).applyColumnChanges(table, List.of(),
                List.of("note", "region", "_olake_id", "missing"));

        assertTrue(changed);
        table.refresh();
        assertNull(table.schema().findField("note"));
        assertNotNull(table.schema().findField("region"));
        assertNotNull(table.schema().findField("_olake_id"));
    }

    @Test
    void skipsCommitWhenNothingChanges() {
        Table table = createTable("noop", 3);
        long schemaId = table.schema().schemaId();
        boolean changed = new IcebergTableOperator(false).applyColumnChanges(table,
                List.of(added("note", "string", null)), List.of("region"));

        assertFalse(changed);
        table.refresh();
        assertEquals(schemaId, table.schema().schemaId());
    }
}
