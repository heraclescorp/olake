package iceberg

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	apacheiceberg "github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
	"github.com/datazip-inc/olake/constants"
	"github.com/stretchr/testify/require"
)

func nativeCOWTableWithManifests(t *testing.T, manifests []apacheiceberg.ManifestFile) *table.Table {
	t.Helper()
	root := t.TempDir()
	schema := apacheiceberg.NewSchema(0, apacheiceberg.NestedField{ID: 1, Name: constants.OlakeID, Type: apacheiceberg.PrimitiveTypes.String, Required: true})
	spec := *apacheiceberg.UnpartitionedSpec
	metadata, err := table.NewMetadata(schema, &spec, table.UnsortedSortOrder, root, apacheiceberg.Properties{table.PropertyFormatVersion: "2"})
	require.NoError(t, err)
	const snapshotID, sequence = int64(2), int64(2)
	var list bytes.Buffer
	seq := sequence
	require.NoError(t, apacheiceberg.WriteManifestList(2, &list, snapshotID, nil, &seq, 0, manifests))
	listPath := filepath.Join(root, "manifest-list.avro")
	require.NoError(t, os.WriteFile(listPath, list.Bytes(), 0600))
	builder, err := table.MetadataBuilderFromBase(metadata, "")
	require.NoError(t, err)
	require.NoError(t, builder.AddSnapshot(&table.Snapshot{SnapshotID: snapshotID, SequenceNumber: sequence, TimestampMs: time.Now().UnixMilli(), ManifestList: listPath, Summary: &table.Summary{Operation: table.OpDelete}}))
	require.NoError(t, builder.SetSnapshotRef(table.MainBranch, snapshotID, table.BranchRef))
	metadata, err = builder.Build()
	require.NoError(t, err)
	return table.New(table.Identifier{"db", "records"}, metadata, "", func(context.Context) (iceio.IO, error) { return iceio.LocalFS{}, nil }, nil)
}

func TestNativeCopyOnWriteIgnoresRetiredDeleteEntries(t *testing.T) {
	root := t.TempDir()
	schema := apacheiceberg.NewSchema(0, apacheiceberg.NestedField{ID: 1, Name: constants.OlakeID, Type: apacheiceberg.PrimitiveTypes.String, Required: true})
	spec := *apacheiceberg.UnpartitionedSpec
	file, err := apacheiceberg.NewDataFileBuilder(spec, apacheiceberg.EntryContentEqDeletes, filepath.Join(root, "removed-delete.parquet"), apacheiceberg.ParquetFile, nil, nil, nil, 1, 1)
	require.NoError(t, err)
	removed := file.EqualityFieldIDs([]int{1}).Build()
	var manifest bytes.Buffer
	writer, err := apacheiceberg.NewManifestWriter(2, &manifest, spec, schema, 2, apacheiceberg.WithManifestWriterContent(apacheiceberg.ManifestContentDeletes))
	require.NoError(t, err)
	previousSnapshot, sequence := int64(1), int64(1)
	entry := apacheiceberg.NewManifestEntry(apacheiceberg.EntryStatusDELETED, &previousSnapshot, &sequence, &sequence, removed)
	require.NoError(t, writer.Delete(entry))
	require.NoError(t, writer.Close())
	manifestPath := filepath.Join(root, "retired-delete.avro")
	metadata, err := writer.ToManifestFile(manifestPath, int64(manifest.Len()), apacheiceberg.WithManifestFileContent(apacheiceberg.ManifestContentDeletes))
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(manifestPath, manifest.Bytes(), 0600))
	tbl := nativeCOWTableWithManifests(t, []apacheiceberg.ManifestFile{metadata})
	hasDeletes, err := nativeCOWHasDeletes(context.Background(), tbl)
	require.NoError(t, err)
	require.False(t, hasDeletes)
	require.NoError(t, validateNativeCOWTable(context.Background(), tbl))
}

func TestNativeCopyOnWriteFailsClosedForUnreadableDeleteManifest(t *testing.T) {
	path := filepath.Join(t.TempDir(), "unavailable-delete.avro")
	manifest := apacheiceberg.NewManifestFile(2, path, 1, 0, 2).Content(apacheiceberg.ManifestContentDeletes).AddedFiles(1).SequenceNum(2, 2).Build()
	tbl := nativeCOWTableWithManifests(t, []apacheiceberg.ManifestFile{manifest})
	hasDeletes, err := nativeCOWHasDeletes(context.Background(), tbl)
	require.ErrorIs(t, err, os.ErrNotExist)
	require.False(t, hasDeletes)
	require.Error(t, validateNativeCOWTable(context.Background(), tbl))
}

func TestNativeCopyOnWriteFailsClosedForUnknownManifestContent(t *testing.T) {
	manifest := apacheiceberg.NewManifestFile(2, filepath.Join(t.TempDir(), "unknown.avro"), 1, 0, 2).Content(apacheiceberg.ManifestContent(99)).SequenceNum(2, 2).Build()
	tbl := nativeCOWTableWithManifests(t, []apacheiceberg.ManifestFile{manifest})
	hasDeletes, err := nativeCOWHasDeletes(context.Background(), tbl)
	require.Error(t, err)
	require.False(t, hasDeletes)
}
