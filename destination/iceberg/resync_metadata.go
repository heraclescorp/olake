package iceberg

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"maps"
	"math"
	"strconv"
	"time"

	apacheiceberg "github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
	"github.com/google/uuid"
	"github.com/twmb/avro/ocf"
)

// Publication rewrites only manifest metadata, never data or delete payloads.
// Generic Avro records retain every file attribute (including Puffin offsets,
// equality IDs and encryption metadata) and preserve both sequence numbers.
// SDK ManifestWriter.Add deliberately clears file_sequence_number; using it
// here would make replacement data incorrectly interact with staged deletes.
func newNativeResyncSnapshot(ctx context.Context, r *resyncControl, tip *table.Snapshot) (*table.Snapshot, error) {
	if err := r.active(ctx); err != nil {
		return nil, err
	}
	metadata := r.base.Metadata()
	version := metadata.Version()
	id := int64(0)
	if tip == nil {
		for id == 0 || metadata.SnapshotByID(id) != nil {
			value := uuid.New()
			id = int64(binary.LittleEndian.Uint64(value[:8]) & math.MaxInt64)
		}
	}
	sequence := metadata.LastSequenceNumber() + 1
	if version == 1 {
		sequence = 0
	}
	if tip != nil {
		id, sequence, version = tip.SnapshotID, tip.SequenceNumber, r.stage.metadata.Version()
	}
	schemaID := metadata.CurrentSchema().ID
	if tip != nil {
		schemaID = r.stage.metadata.CurrentSchema().ID
	}
	snapshot := &table.Snapshot{SnapshotID: id, SequenceNumber: sequence, TimestampMs: max(time.Now().UnixMilli(), metadata.LastUpdatedMillis()+1), SchemaID: &schemaID, Summary: &table.Summary{Operation: table.OpOverwrite, Properties: apacheiceberg.Properties{}}}
	if tip != nil {
		snapshot.TimestampMs = max(snapshot.TimestampMs, r.stage.metadata.LastUpdatedMillis())
	}
	if previous := metadata.CurrentSnapshot(); previous != nil {
		parent := previous.SnapshotID
		snapshot.ParentSnapshotID = &parent
	}
	firstRowID := metadata.NextRowID()
	if version >= 3 {
		added := int64(0)
		if tip != nil {
			added = r.stage.metadata.NextRowID() - firstRowID
		}
		if added < 0 {
			return nil, errors.New("resync V3 next-row-id moved backwards")
		}
		snapshot.FirstRowID, snapshot.AddedRows = &firstRowID, &added
	}
	fs, err := r.base.FS(ctx)
	if err != nil {
		return nil, err
	}
	writable, ok := fs.(iceio.WriteFileIO)
	if !ok {
		return nil, errors.New("atomic resync requires writable native table FileIO")
	}
	locations, err := r.base.LocationProvider()
	if err != nil {
		return nil, err
	}
	prefix := locations.NewMetadataLocation(uuid.NewString() + "-resync")
	snapshot.ManifestList = prefix + "-snap.avro"
	// Obtain the SDK's canonical list schema rather than maintaining a second
	// copy of the Iceberg Avro schema. Only this small header is buffered.
	var template bytes.Buffer
	if err := apacheiceberg.WriteManifestList(version, &template, id, snapshot.ParentSnapshotID, &sequence, firstRowID, nil); err != nil {
		return nil, err
	}
	header, err := ocf.NewReader(bytes.NewReader(template.Bytes()))
	if err != nil {
		return nil, err
	}
	defer header.Close()
	out, err := writable.Create(snapshot.ManifestList)
	if err != nil {
		return nil, err
	}
	list, err := ocf.NewWriter(out, header.Schema(), ocf.WithSchema(string(header.Metadata()["avro.schema"])), ocf.WithMetadata(nativeResyncAvroMetadata(header.Metadata())), ocf.WithCodec(ocf.DeflateCodec(-1)))
	if err != nil {
		_ = out.Close()
		return nil, err
	}
	summary := map[string]int64{}
	manifestNumber := 0
	writeSnapshot := func(source *table.Snapshot, added bool) error {
		if source == nil {
			return nil
		}
		manifests, err := source.Manifests(fs)
		if err != nil {
			return err
		}
		for _, manifest := range manifests {
			if err := r.active(ctx); err != nil {
				return err
			}
			if !manifest.HasAddedFiles() && !manifest.HasExistingFiles() {
				continue
			}
			path := prefix + "-m" + strconv.Itoa(manifestNumber) + ".avro"
			manifestNumber++
			record, err := rewriteNativeResyncManifest(ctx, r, fs, writable, manifest, path, id, sequence, firstRowID, added, summary)
			if err != nil {
				return err
			}
			if err := list.Encode(record); err != nil {
				return err
			}
		}
		return nil
	}
	if tip != nil {
		err = writeSnapshot(metadata.CurrentSnapshot(), false)
		if err == nil {
			err = writeSnapshot(tip, true)
		}
	}
	err = errors.Join(err, list.Close(), out.Close())
	if err != nil {
		return nil, err
	}
	if err := r.active(ctx); err != nil {
		return nil, err
	}
	for key, value := range summary {
		snapshot.Summary.Properties[key] = strconv.FormatInt(value, 10)
	}
	for total, added := range map[string]string{"total-data-files": "added-data-files", "total-delete-files": "added-delete-files", "total-records": "added-records", "total-files-size": "added-files-size", "total-position-deletes": "added-position-deletes", "total-equality-deletes": "added-equality-deletes"} {
		snapshot.Summary.Properties[total] = strconv.FormatInt(summary[added], 10)
	}
	return snapshot, nil
}

func rewriteNativeResyncManifest(ctx context.Context, r *resyncControl, fs iceio.IO, writable iceio.WriteFileIO, manifest apacheiceberg.ManifestFile, path string, snapshotID, sequence, originalNextRowID int64, added bool, summary map[string]int64) (_ map[string]any, err error) {
	input, err := fs.Open(manifest.FilePath())
	if err != nil {
		return nil, err
	}
	defer func() { err = errors.Join(err, input.Close()) }()
	reader, err := ocf.NewReader(input)
	if err != nil {
		return nil, err
	}
	defer func() { err = errors.Join(err, reader.Close()) }()
	out, err := writable.Create(path)
	if err != nil {
		return nil, err
	}
	outputClosed := false
	defer func() {
		if !outputClosed {
			err = errors.Join(err, out.Close())
		}
	}()
	metadata := nativeResyncAvroMetadata(reader.Metadata())
	counted := &nativeCountingWriter{writer: out}
	writer, err := ocf.NewWriter(counted, reader.Schema(), ocf.WithSchema(string(reader.Metadata()["avro.schema"])), ocf.WithMetadata(metadata), ocf.WithCodec(ocf.DeflateCodec(-1)))
	if err != nil {
		return nil, err
	}
	defer func() { err = errors.Join(err, writer.Close()) }()
	var record map[string]any
	var files int32
	var rows int64
	minSequence := sequence
	nextRowID := originalNextRowID
	if value := manifest.FirstRowID(); value != nil {
		nextRowID = *value
	}
	for {
		if err := r.active(ctx); err != nil {
			return nil, err
		}
		err := reader.Decode(&record)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, err
		}
		status, err := resyncAvroInt(record["status"])
		if err != nil {
			return nil, err
		}
		if status == int64(apacheiceberg.EntryStatusDELETED) {
			continue
		}
		if status != int64(apacheiceberg.EntryStatusEXISTING) && status != int64(apacheiceberg.EntryStatusADDED) {
			return nil, errors.New("invalid source manifest entry status")
		}
		data, ok := record["data_file"].(map[string]any)
		if !ok {
			return nil, errors.New("invalid source manifest data file")
		}
		count, err := resyncAvroInt(data["record_count"])
		if err != nil || count < 0 {
			return nil, errors.New("invalid source manifest record count")
		}
		size, err := resyncAvroInt(data["file_size_in_bytes"])
		if err != nil || size < 0 {
			return nil, errors.New("invalid source manifest file size")
		}
		// Resolve inherited IDs before changing status or the manifest snapshot.
		// Explicit IDs advance no allocation cursor, matching Iceberg inheritance.
		if _, has := data["first_row_id"]; has && data["first_row_id"] == nil && manifest.FirstRowID() != nil {
			if count > math.MaxInt64-nextRowID {
				return nil, errors.New("resync row ID overflow")
			}
			data["first_row_id"] = nextRowID
			nextRowID += count
		}
		if _, has := record["sequence_number"]; has {
			if record["sequence_number"] == nil {
				record["sequence_number"] = manifest.SequenceNum()
			}
			if record["file_sequence_number"] == nil {
				record["file_sequence_number"] = manifest.SequenceNum()
			}
			seq, err := resyncAvroInt(record["sequence_number"])
			if err != nil || seq < 0 {
				return nil, errors.New("invalid source manifest sequence number")
			}
			minSequence = min(minSequence, seq)
		} else {
			minSequence = 0
		}
		record["snapshot_id"] = snapshotID
		record["status"] = int32(apacheiceberg.EntryStatusDELETED)
		if added {
			record["status"] = int32(apacheiceberg.EntryStatusADDED)
		}
		if err := writer.Encode(record); err != nil {
			return nil, err
		}
		files++
		rows += count
		content := int64(0)
		if value, exists := data["content"]; exists {
			content, err = resyncAvroInt(value)
			if err != nil {
				return nil, err
			}
		}
		updateNativeResyncSummary(summary, added, content, count, size)
	}
	if err := writer.Close(); err != nil {
		return nil, err
	}
	// Closing the upload is part of success; don't publish references to an
	// unfinished object even when the Avro encoder itself flushed successfully.
	outputClosed = true
	if err := out.Close(); err != nil {
		return nil, err
	}
	result := map[string]any{"manifest_path": path, "manifest_length": counted.size, "partition_spec_id": manifest.PartitionSpecID(), "content": int32(manifest.ManifestContent()), "sequence_number": sequence, "min_sequence_number": minSequence, "added_snapshot_id": snapshotID, "added_files_count": int32(0), "existing_files_count": int32(0), "deleted_files_count": int32(0), "added_rows_count": int64(0), "existing_rows_count": int64(0), "deleted_rows_count": int64(0), "partitions": manifest.Partitions(), "key_metadata": nil, "first_row_id": nil}
	if added {
		result["added_files_count"], result["added_rows_count"] = files, rows
	} else {
		result["deleted_files_count"], result["deleted_rows_count"] = files, rows
	}
	if manifest.ManifestContent() == apacheiceberg.ManifestContentData && r.stage.metadata.Version() >= 3 {
		first := originalNextRowID
		if value := manifest.FirstRowID(); value != nil {
			first = *value
		}
		result["first_row_id"] = first
	}
	return result, nil
}

func resyncAvroInt(value any) (int64, error) {
	switch value := value.(type) {
	case int32:
		return int64(value), nil
	case int64:
		return value, nil
	case int:
		return int64(value), nil
	}
	return 0, fmt.Errorf("invalid manifest integer %T", value)
}

func updateNativeResyncSummary(summary map[string]int64, added bool, content, count, size int64) {
	if added {
		summary["added-files-size"] += size
		if content == 0 {
			summary["added-data-files"]++
			summary["added-records"] += count
		} else {
			summary["added-delete-files"]++
		}
		if content == 1 {
			summary["added-position-deletes"] += count
		}
		if content == 2 {
			summary["added-equality-deletes"] += count
		}
	} else {
		summary["removed-files-size"] += size
		if content == 0 {
			summary["deleted-data-files"]++
			summary["deleted-records"] += count
		} else {
			summary["removed-delete-files"]++
		}
		if content == 1 {
			summary["removed-position-deletes"] += count
		}
		if content == 2 {
			summary["removed-equality-deletes"] += count
		}
	}
}

func nativeResyncAvroMetadata(metadata map[string][]byte) map[string][]byte {
	result := maps.Clone(metadata)
	delete(result, "avro.schema")
	delete(result, "avro.codec")
	return result
}
