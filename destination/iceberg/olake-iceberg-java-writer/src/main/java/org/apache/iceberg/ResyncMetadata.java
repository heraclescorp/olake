package org.apache.iceberg;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.encryption.EncryptingFileIO;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.OutputFile;

/** Publishes a private resync as one complete overwrite of the original table's current snapshot. */
public final class ResyncMetadata extends TableMetadata {
    private ResyncMetadata(TableMetadata metadata, List<MetadataUpdate> changes) {
        super(metadata.metadataFileLocation(), metadata.formatVersion(), metadata.uuid(), metadata.location(),
                metadata.lastSequenceNumber(), metadata.lastUpdatedMillis(), metadata.lastColumnId(),
                metadata.currentSchemaId(), metadata.schemas(), metadata.defaultSpecId(), metadata.specs(),
                metadata.lastAssignedPartitionId(), metadata.defaultSortOrderId(), metadata.sortOrders(),
                metadata.properties(), metadata.currentSnapshot() == null ? -1L : metadata.currentSnapshot().snapshotId(),
                metadata.snapshots(), null, metadata.snapshotLog(), metadata.previousFiles(), metadata.refs(),
                metadata.statisticsFiles(), metadata.partitionStatisticsFiles(), metadata.nextRowId(),
                metadata.encryptionKeys(), changes);
    }

    public static TableMetadata forPublication(TableMetadata original, TableMetadata staged,
                                              TableOperations operations, long publicationMillis,
                                              Runnable checkActive) throws IOException {
        checkActive.run();
        Snapshot tip = staged.currentSnapshot();
        if (tip == null) {
            throw new IllegalArgumentException("Resync publication requires a staged snapshot");
        }
        long timestamp = Math.max(publicationMillis,
                Math.max(Math.incrementExact(original.lastUpdatedMillis()), staged.lastUpdatedMillis()));
        Long parentId = original.currentSnapshot() == null ? null : original.currentSnapshot().snapshotId();
        boolean rowLineage = staged.formatVersion() >= MIN_FORMAT_VERSION_ROW_LINEAGE;
        Long firstRowId = rowLineage ? original.nextRowId() : null;
        Long addedRows = rowLineage ? Math.subtractExact(staged.nextRowId(), original.nextRowId()) : null;

        // Clear only change tracking, not original snapshots, history, refs or statistics. The official
        // builder must produce every persisted field so that replaying the REST updates is equivalent.
        TableMetadata.Builder builder = TableMetadata.buildFrom(new ResyncMetadata(original, List.of()));
        for (MetadataUpdate update : staged.changes()) {
            checkActive.run();
            if (isTableDefinitionUpdate(update)) {
                update.applyTo(builder);
            }
        }

        String prefix = UUID.randomUUID() + "-resync";
        checkActive.run();
        OutputFile manifestList = operations.io().newOutputFile(
                operations.metadataFileLocation(prefix + "-snap.avro"));
        SnapshotSummary.Builder summary = SnapshotSummary.builder();
        try (ManifestListWriter writer = ManifestLists.write(staged.formatVersion(), manifestList,
                tip.snapshotId(), parentId, tip.sequenceNumber(), firstRowId)) {
            PublicationManifests manifests = new PublicationManifests(operations, staged.formatVersion(),
                    tip.snapshotId(), original.nextRowId(), prefix, writer, summary, checkActive);
            manifests.writeSnapshot(original.currentSnapshot(), original, false);
            manifests.writeSnapshot(tip, staged, true);
            checkActive.run();
        }
        checkActive.run();
        Snapshot published = new BaseSnapshot(tip.sequenceNumber(), tip.snapshotId(), parentId, timestamp,
                DataOperations.OVERWRITE, completeSummary(summary), staged.currentSchemaId(),
                manifestList.location(), firstRowId, addedRows, null);
        builder.setBranchSnapshot(published, SnapshotRef.MAIN_BRANCH);
        TableMetadata metadata = builder.build();

        List<MetadataUpdate> changes = new ArrayList<>(metadata.changes());
        // Idempotent updates retain REST's schema/spec/sort/field-ID assertions even when unchanged.
        // The actual final set-main above provides the main-ref assertion, and must occur only once.
        changes.add(new MetadataUpdate.AddSchema(metadata.schema()));
        changes.add(new MetadataUpdate.UpgradeFormatVersion(metadata.formatVersion()));
        changes.add(new MetadataUpdate.SetCurrentSchema(metadata.currentSchemaId()));
        changes.add(new MetadataUpdate.AddPartitionSpec(metadata.spec()));
        changes.add(new MetadataUpdate.SetDefaultPartitionSpec(metadata.defaultSpecId()));
        changes.add(new MetadataUpdate.SetDefaultSortOrder(metadata.defaultSortOrderId()));
        for (Map.Entry<String, SnapshotRef> entry : metadata.refs().entrySet()) {
            checkActive.run();
            if (!SnapshotRef.MAIN_BRANCH.equals(entry.getKey())) {
                SnapshotRef ref = entry.getValue();
                changes.add(new MetadataUpdate.SetSnapshotRef(entry.getKey(), ref.snapshotId(), ref.type(),
                        ref.minSnapshotsToKeep(), ref.maxSnapshotAgeMs(), ref.maxRefAgeMs()));
            }
        }
        TableMetadata publication = new ResyncMetadata(metadata, List.copyOf(changes));
        checkActive.run();
        return publication;
    }

    private static boolean isTableDefinitionUpdate(MetadataUpdate update) {
        return update instanceof MetadataUpdate.UpgradeFormatVersion
                || update instanceof MetadataUpdate.AddSchema
                || update instanceof MetadataUpdate.SetCurrentSchema
                || update instanceof MetadataUpdate.RemoveSchemas
                || update instanceof MetadataUpdate.AddPartitionSpec
                || update instanceof MetadataUpdate.SetDefaultPartitionSpec
                || update instanceof MetadataUpdate.RemovePartitionSpecs
                || update instanceof MetadataUpdate.AddSortOrder
                || update instanceof MetadataUpdate.SetDefaultSortOrder
                || update instanceof MetadataUpdate.SetProperties
                || update instanceof MetadataUpdate.RemoveProperties
                || update instanceof MetadataUpdate.AddEncryptionKey
                || update instanceof MetadataUpdate.RemoveEncryptionKey;
    }

    private static Map<String, String> completeSummary(SnapshotSummary.Builder builder) {
        Map<String, String> summary = new HashMap<>(builder.build());
        summary.put(SnapshotSummary.TOTAL_DATA_FILES_PROP,
                summary.getOrDefault(SnapshotSummary.ADDED_FILES_PROP, "0"));
        summary.put(SnapshotSummary.TOTAL_DELETE_FILES_PROP,
                summary.getOrDefault(SnapshotSummary.ADDED_DELETE_FILES_PROP, "0"));
        summary.put(SnapshotSummary.TOTAL_RECORDS_PROP,
                summary.getOrDefault(SnapshotSummary.ADDED_RECORDS_PROP, "0"));
        summary.put(SnapshotSummary.TOTAL_FILE_SIZE_PROP,
                summary.getOrDefault(SnapshotSummary.ADDED_FILE_SIZE_PROP, "0"));
        summary.put(SnapshotSummary.TOTAL_POS_DELETES_PROP,
                summary.getOrDefault(SnapshotSummary.ADDED_POS_DELETES_PROP, "0"));
        summary.put(SnapshotSummary.TOTAL_EQ_DELETES_PROP,
                summary.getOrDefault(SnapshotSummary.ADDED_EQ_DELETES_PROP, "0"));
        return Map.copyOf(summary);
    }

    private static final class PublicationManifests {
        private final TableOperations operations;
        private final EncryptingFileIO io;
        private final int formatVersion;
        private final long snapshotId;
        private final long originalNextRowId;
        private final String prefix;
        private final ManifestListWriter listWriter;
        private final SnapshotSummary.Builder summary;
        private final Runnable checkActive;
        private int manifestNumber;

        private PublicationManifests(TableOperations operations, int formatVersion, long snapshotId,
                                     long originalNextRowId, String prefix, ManifestListWriter listWriter,
                                     SnapshotSummary.Builder summary, Runnable checkActive) {
            this.operations = operations;
            this.io = EncryptingFileIO.combine(operations.io(), operations.encryption());
            this.formatVersion = formatVersion;
            this.snapshotId = snapshotId;
            this.originalNextRowId = originalNextRowId;
            this.prefix = prefix;
            this.listWriter = listWriter;
            this.summary = summary;
            this.checkActive = checkActive;
        }

        private void writeSnapshot(Snapshot snapshot, TableMetadata metadata, boolean added) throws IOException {
            checkActive.run();
            if (snapshot == null) {
                return;
            }
            if (snapshot.manifestListLocation() == null) {
                // Iceberg v1 also permits snapshots containing inline manifest paths.
                for (ManifestFile manifest : snapshot.allManifests(io)) {
                    writeManifest(manifest, metadata.specsById(), added);
                }
            } else {
                // Stream the list instead of Snapshot.allManifests: large lists must remain cancellable.
                try (CloseableIterable<ManifestFile> manifests = InternalData.read(FileFormat.AVRO,
                        io.newInputFile(snapshot.manifestListLocation()))
                        .setRootType(GenericManifestFile.class)
                        .setCustomType(ManifestFile.PARTITION_SUMMARIES_ELEMENT_ID, GenericPartitionFieldSummary.class)
                        .project(ManifestFile.schema()).build()) {
                    for (ManifestFile manifest : manifests) {
                        writeManifest(manifest, metadata.specsById(), added);
                    }
                    checkActive.run();
                }
            }
            checkActive.run();
        }

        private void writeManifest(ManifestFile manifest, Map<Integer, PartitionSpec> specs, boolean added)
                throws IOException {
            checkActive.run();
            if (!manifest.hasAddedFiles() && !manifest.hasExistingFiles()) {
                return;
            }
            EncryptedOutputFile output = io.newEncryptingOutputFile(operations.metadataFileLocation(
                    prefix + "-m" + manifestNumber++ + ".avro"));
            if (manifest.content() == ManifestContent.DATA) {
                // An explicit manifest first-row-id prevents list-writing from allocating a second
                // range. Each resolved file row ID is written explicitly, including private gaps.
                Long firstRowId = formatVersion >= MIN_FORMAT_VERSION_ROW_LINEAGE
                        ? (manifest.firstRowId() != null ? manifest.firstRowId() : originalNextRowId) : null;
                try (ManifestReader<DataFile> reader = ManifestFiles.read(manifest, io, specs)) {
                    ManifestWriter<DataFile> writer = ManifestFiles.newWriter(formatVersion, reader.spec(),
                            output, snapshotId, firstRowId);
                    rewriteEntries(reader, writer, added);
                }
            } else {
                try (ManifestReader<DeleteFile> reader = ManifestFiles.readDeleteManifest(manifest, io, specs)) {
                    ManifestWriter<DeleteFile> writer = ManifestFiles.writeDeleteManifest(formatVersion,
                            reader.spec(), output, snapshotId);
                    rewriteEntries(reader, writer, added);
                }
            }
            checkActive.run();
        }

        private <F extends ContentFile<F>> void rewriteEntries(ManifestReader<F> reader,
                                                               ManifestWriter<F> writer, boolean added)
                throws IOException {
            GenericManifestEntry<F> rewritten = new GenericManifestEntry<>(
                    ManifestEntry.getSchema(reader.spec().partitionType()).asStruct());
            try (writer) {
                for (ManifestEntry<F> entry : reader.liveEntries()) {
                    checkActive.run();
                    // wrapAppend suppresses firstRowId and clears fileSequenceNumber. Start from
                    // wrapExisting instead, then change only status and snapshot ID for this delta.
                    rewritten.wrapExisting(snapshotId, entry.dataSequenceNumber(), entry.fileSequenceNumber(),
                            entry.file());
                    rewritten.set(0, (added ? ManifestEntry.Status.ADDED : ManifestEntry.Status.DELETED).id());
                    writer.addEntry(rewritten);
                    if (!added) {
                        summary.deletedFile(reader.spec(), entry.file());
                    } else if (entry.file() instanceof DataFile) {
                        summary.addedFile(reader.spec(), (DataFile) entry.file());
                    } else {
                        summary.addedFile(reader.spec(), (DeleteFile) entry.file());
                    }
                    checkActive.run();
                }
                checkActive.run();
            }
            checkActive.run();
            listWriter.add(writer.toManifestFile());
            checkActive.run();
        }
    }
}
