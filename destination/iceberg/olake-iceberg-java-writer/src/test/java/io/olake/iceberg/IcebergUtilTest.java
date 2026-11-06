package io.olake.iceberg;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class IcebergUtilTest {

    private static final TableIdentifier TABLE_ID = TableIdentifier.of("db", "stream");

    private Catalog catalog;

    @BeforeEach
    void setUp() {
        catalog = mock(Catalog.class);
    }

    @Test
    @DisplayName("clear destination drops an existing table with purge enabled")
    void dropsExistingTableWithPurge() {
        when(catalog.tableExists(TABLE_ID)).thenReturn(true);
        when(catalog.dropTable(TABLE_ID, true)).thenReturn(true);

        assertTrue(IcebergUtil.dropIcebergTable("db", "stream", catalog));

        verify(catalog).dropTable(TABLE_ID, true);
    }

    @Test
    @DisplayName("clear destination skips a missing table without attempting a drop")
    void skipsMissingTable() {
        when(catalog.tableExists(TABLE_ID)).thenReturn(false);

        assertFalse(IcebergUtil.dropIcebergTable("db", "stream", catalog));

        verify(catalog, never()).dropTable(any(TableIdentifier.class), anyBoolean());
    }

    @Test
    @DisplayName("clear destination tolerates a table already removed at the drop boundary")
    void preservesMissingTableResultFromDrop() {
        when(catalog.tableExists(TABLE_ID)).thenReturn(true);
        when(catalog.dropTable(TABLE_ID, true)).thenReturn(false);

        assertFalse(IcebergUtil.dropIcebergTable("db", "stream", catalog));

        verify(catalog).dropTable(TABLE_ID, true);
    }
}
