package iceberg

import (
	"context"
	"errors"
	"io/fs"
	"strings"
	"testing"

	apacheiceberg "github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
	"github.com/datazip-inc/olake/types"
	"github.com/stretchr/testify/require"
)

type nativeCOWStagingFaultIO struct {
	iceio.WriteFileIO
	phase string
	err   error
}

func (f *nativeCOWStagingFaultIO) Create(path string) (iceio.FileWriter, error) {
	if !strings.HasSuffix(path, ".arrow") {
		return f.WriteFileIO.Create(path)
	}
	if f.phase == "create" {
		return nil, f.err
	}
	file, err := f.WriteFileIO.Create(path)
	if err != nil {
		return nil, err
	}
	return &nativeCOWStagingFaultWriter{FileWriter: file, owner: f}, nil
}

func (f *nativeCOWStagingFaultIO) Open(path string) (iceio.File, error) {
	if f.phase == "replay" && strings.HasSuffix(path, ".arrow") {
		return nil, f.err
	}
	return f.WriteFileIO.Open(path)
}

func (f *nativeCOWStagingFaultIO) Remove(path string) error {
	if f.phase == "cleanup" && strings.HasSuffix(path, ".arrow") {
		return f.err
	}
	return f.WriteFileIO.Remove(path)
}

type nativeCOWStagingFaultWriter struct {
	iceio.FileWriter
	owner *nativeCOWStagingFaultIO
}

func (f *nativeCOWStagingFaultWriter) Write(data []byte) (int, error) {
	if f.owner.phase == "write" {
		return 0, f.owner.err
	}
	return f.FileWriter.Write(data)
}
func (f *nativeCOWStagingFaultWriter) Close() error {
	err := f.FileWriter.Close()
	if f.owner.phase == "close" {
		return errors.Join(err, f.owner.err)
	}
	return err
}

func TestNativeCOWStagingFailureNeverPublishesOrAdvances(t *testing.T) {
	for _, phase := range []string{"create", "write", "close", "replay"} {
		t.Run(phase, func(t *testing.T) {
			ctx := context.Background()
			cfg, stream, cat := nativeCOWFixture(t, types.UpdateTypePosition)
			seed := openNativeTestWriter(ctx, t, cfg, stream, cat, nil, "seed", true)
			require.NoError(t, seed.Write(ctx, []types.RawRecord{nativeCOWRecord("id", "r", 1, "a", 7)}))
			require.NoError(t, seed.Close(ctx, map[string]any{"state": "baseline"}))
			beforeMetadata := seed.table.MetadataLocation()
			beforePosition := seed.table.Properties()[nativeRecoveryProperty]
			writer := openNativeTestWriter(ctx, t, cfg, stream, cat, nil, "staging-fault", false)
			store, err := writer.table.FS(ctx)
			require.NoError(t, err)
			writable, ok := store.(iceio.WriteFileIO)
			require.True(t, ok)
			injected := errors.New("injected COW staging " + phase + " failure")
			fault := &nativeCOWStagingFaultIO{WriteFileIO: writable, phase: phase, err: injected}
			writer.table = table.New(writer.table.Identifier(), writer.table.Metadata(), writer.table.MetadataLocation(), func(context.Context) (iceio.IO, error) { return fault, nil }, writer.cat)
			err = writer.Write(ctx, []types.RawRecord{nativeCOWRecord("id", "u", 1, "a", 99)})
			if phase == "replay" {
				require.NoError(t, err)
				require.ErrorIs(t, writer.Close(ctx, map[string]any{"state": "must-not-land"}), injected)
			} else {
				require.ErrorIs(t, err, injected)
				require.Error(t, writer.Close(ctx, map[string]any{"state": "must-not-land"}))
			}
			fresh, err := cat.LoadTable(ctx, seed.table.Identifier())
			require.NoError(t, err)
			require.Equal(t, beforeMetadata, fresh.MetadataLocation())
			require.Equal(t, beforePosition, fresh.Properties()[nativeRecoveryProperty])
			require.Equal(t, map[string]int64{"id": 7}, nativeVisibleValues(t, fresh))
			entries, err := nativeLiveEntries(ctx, fresh)
			require.NoError(t, err)
			for _, entry := range entries {
				require.Equal(t, apacheiceberg.ParquetFile, entry.DataFile().FileFormat())
			}
		})
	}
}

func TestNativeCOWCleanupFailureRetainsOnlyPrivateInputs(t *testing.T) {
	ctx := context.Background()
	cfg, stream, realCatalog := nativeCOWFixture(t, types.UpdateTypePosition)
	seed := openNativeTestWriter(ctx, t, cfg, stream, realCatalog, nil, "seed", true)
	require.NoError(t, seed.Write(ctx, []types.RawRecord{nativeCOWRecord("id", "r", 1, "a", 7)}))
	require.NoError(t, seed.Close(ctx, map[string]any{"state": "baseline"}))
	store, err := seed.table.FS(ctx)
	require.NoError(t, err)
	writable, ok := store.(iceio.WriteFileIO)
	require.True(t, ok)
	fault := &nativeCOWStagingFaultIO{WriteFileIO: writable, phase: "cleanup", err: errors.New("injected private input cleanup failure")}
	cat := &nativeCommitFaultCatalog{Catalog: realCatalog, fs: fault}
	writer := openNativeTestWriter(ctx, t, cfg, stream, cat, nil, "cleanup-retry", false)
	require.NoError(t, writer.Write(ctx, []types.RawRecord{nativeCOWRecord("id", "u", 1, "a", 99)}))
	privateInputs := append([]string(nil), writer.cow.paths...)
	require.NoError(t, writer.Close(ctx, map[string]any{"state": "committed"}))
	for _, path := range privateInputs {
		input, err := store.Open(path)
		require.NoError(t, err)
		require.NoError(t, input.Close())
	}
	fault.phase = ""
	writer.Abort()
	for _, path := range privateInputs {
		_, err := store.Open(path)
		require.ErrorIs(t, err, fs.ErrNotExist)
	}
	fresh, err := realCatalog.LoadTable(ctx, writer.table.Identifier())
	require.NoError(t, err)
	require.Equal(t, map[string]int64{"id": 99}, nativeVisibleValues(t, fresh))
	require.Contains(t, fresh.Properties()[nativeRecoveryProperty], "committed")
}
