package types

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/datazip-inc/olake/constants"
	"github.com/goccy/go-json"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
)

func TestResyncContext(t *testing.T) {
	_, found := ResyncFromContext(context.Background())
	require.False(t, found)
	options := &ResyncOptions{Namespace: "public", Table: "items", ID: "stable-operation"}
	ctx := WithResync(context.Background(), options)
	got, found := ResyncFromContext(ctx)
	require.True(t, found)
	require.Same(t, options, got)
}

func TestUnpublishedStateNeverTouchesSavedCheckpoint(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	original := []byte(`{"saved":"authoritative"}`)
	require.NoError(t, os.WriteFile(path, original, 0600))
	previous := viper.Get(constants.StatePath)
	viper.Set(constants.StatePath, path)
	t.Cleanup(func() { viper.Set(constants.StatePath, previous) })

	state := &State{RWMutex: &sync.RWMutex{}, Type: GlobalType, SuppressPersistence: true}
	stream := NewStream("items", "public", nil).Wrap(0)
	state.SetGlobal(map[string]any{"lsn": "0/100"}, stream.ID())
	state.SetCursor(stream, "offset", int64(9007199254740993))
	stream.Stream.SyncMode = CDC
	chunk := Chunk{Min: "a", Max: "z"}
	state.SetChunks(stream, NewSet(chunk))
	state.RemoveChunk(stream, chunk)
	state.ResetCursor(stream)
	state.LogWithLock()
	state.ResetStreams()
	state.LogState()
	actual, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, original, actual)
}

func TestResyncCheckpointClonePreservesAllStreamsAndNumbers(t *testing.T) {
	state := &State{RWMutex: &sync.RWMutex{}, Type: GlobalType, Version: 8, SuppressPersistence: true}
	target := NewStream("items", "public", nil).Wrap(0)
	other := NewStream("unrelated", "public", nil).Wrap(0)
	state.SetGlobal(map[string]any{"lsn": "0/100", "position": int64(9007199254740993)}, target.ID(), other.ID())
	state.SetCursor(target, "offset", int64(9007199254740993))
	state.SetCursor(other, "offset", int64(9223372036854775807))
	other.Stream.SyncMode = CDC
	state.SetChunks(other, NewSet(Chunk{Min: int64(9007199254740993), Max: int64(9223372036854775807)}))

	checkpoint, err := state.Clone()
	require.NoError(t, err)
	require.False(t, checkpoint.SuppressPersistence)
	require.True(t, state.SuppressPersistence)
	require.Equal(t, state.Version, checkpoint.Version)
	require.Equal(t, "9007199254740993", checkpoint.GetCursor(target, "offset").(json.Number).String())
	require.Equal(t, "9223372036854775807", checkpoint.GetCursor(other, "offset").(json.Number).String())
	require.Equal(t, "9007199254740993", checkpoint.GetGlobal().State.(map[string]any)["position"].(json.Number).String())
	require.Equal(t, "9007199254740993", checkpoint.GetChunks(other).Array()[0].Min.(json.Number).String())
	require.True(t, checkpoint.HasCompletedBackfill(target))
	require.True(t, checkpoint.HasCompletedBackfill(other))
	checkpoint.Global.Streams.Remove(target.ID())
	require.True(t, state.HasCompletedBackfill(target), "clone must not share stream membership with the original")
}
