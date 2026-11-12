package io.olake.iceberg;

import org.apache.iceberg.BaseTable;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.LocationProvider;

/** Rechecks authority at the actual catalog commit, including Iceberg retries. */
public final class FencedTableOperations implements TableOperations {
    private final TableOperations delegate;

    private FencedTableOperations(TableOperations delegate) { this.delegate = delegate; }

    public static Table wrap(Table table) {
        if (!(table instanceof HasTableOperations operations)) {
            throw new IllegalArgumentException("Execution fencing requires TableOperations");
        }
        return new BaseTable(new FencedTableOperations(operations.operations()), table.name());
    }

    @Override public TableMetadata current() { return delegate.current(); }
    @Override public TableMetadata refresh() { return delegate.refresh(); }
    @Override public void commit(TableMetadata base, TableMetadata metadata) {
        ExecutionFence.requireCatalogMutation(delegate);
        delegate.commit(base, metadata);
    }
    @Override public FileIO io() { return delegate.io(); }
    @Override public EncryptionManager encryption() { return delegate.encryption(); }
    @Override public String metadataFileLocation(String file) { return delegate.metadataFileLocation(file); }
    @Override public LocationProvider locationProvider() { return delegate.locationProvider(); }
    @Override public TableOperations temp(TableMetadata metadata) {
        return new FencedTableOperations(delegate.temp(metadata));
    }
    @Override public long newSnapshotId() { return delegate.newSnapshotId(); }
    @Override public boolean requireStrictCleanup() { return delegate.requireStrictCleanup(); }
}
