package protocol

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/datazip-inc/olake/types"
	"github.com/stretchr/testify/require"
)

func TestResyncRequiredFlags(t *testing.T) {
	previousNamespace, previousTable, previousID, previousState, previousNoSave := resyncNamespace, resyncTable, resyncID, statePath, noSave
	t.Cleanup(func() {
		resyncNamespace, resyncTable, resyncID, statePath, noSave = previousNamespace, previousTable, previousID, previousState, previousNoSave
	})
	cases := []struct {
		name      string
		namespace string
		table     string
		id        string
		state     string
		noSave    bool
		want      string
	}{
		{"namespace", "", "t", "operation", "state.json", false, "--resync-namespace"},
		{"table", "public", " ", "operation", "state.json", false, "--resync-table"},
		{"operation", "public", "t", "\t", "state.json", false, "--resync-id"},
		{"saved state", "public", "t", "operation", "", false, "--state"},
		{"receipt required", "public", "t", "operation", "state.json", true, "--no-save"},
		{"complete", "public", "t", "operation", "state.json", false, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resyncNamespace, resyncTable, resyncID, statePath, noSave = tc.namespace, tc.table, tc.id, tc.state, tc.noSave
			err := validateResyncFlags()
			if tc.want == "" {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, tc.want)
			}
		})
	}
}

func TestResyncRequiresUniqueSavedSelectedBackfillTarget(t *testing.T) {
	for _, mode := range []types.SyncMode{types.CDC, types.FULLREFRESH, types.INCREMENTAL} {
		t.Run(string(mode), func(t *testing.T) {
			saved := catalogOf(stream{name: "target", mode: mode}, stream{name: "other", mode: types.CDC})
			classification := &StreamClassification{SelectedStreams: []types.StreamInterface{saved.Streams[0], saved.Streams[1]}}
			target, err := selectedResyncTarget(saved, classification, "public", "target")
			require.NoError(t, err)
			require.Same(t, saved.Streams[0], target)
			require.Len(t, saved.Streams, 2)
			require.Len(t, classification.SelectedStreams, 2)
		})
	}
	cases := []struct {
		name   string
		mutate func(*types.Catalog, *StreamClassification)
		want   string
	}{
		{"not saved", func(c *types.Catalog, _ *StreamClassification) { c.Streams = nil }, "exactly one saved"},
		{"duplicate saved", func(c *types.Catalog, _ *StreamClassification) { c.Streams = append(c.Streams, c.Streams[0]) }, "exactly one saved"},
		{"not selected", func(c *types.Catalog, _ *StreamClassification) { c.SelectedStreams["public"] = nil }, "selected exactly once"},
		{"duplicate selection", func(c *types.Catalog, _ *StreamClassification) {
			c.SelectedStreams["public"] = append(c.SelectedStreams["public"], c.SelectedStreams["public"][0])
		}, "selected exactly once"},
		{"invalid target", func(_ *types.Catalog, c *StreamClassification) { c.SelectedStreams = nil }, "not a valid selected"},
		{"strict CDC", func(c *types.Catalog, _ *StreamClassification) { c.Streams[0].Stream.SyncMode = types.STRICTCDC }, "not backfill-capable"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			saved := catalogOf(stream{name: "target", mode: types.CDC})
			classification := &StreamClassification{SelectedStreams: []types.StreamInterface{saved.Streams[0]}}
			tc.mutate(saved, classification)
			_, err := selectedResyncTarget(saved, classification, "public", "target")
			require.ErrorContains(t, err, tc.want)
		})
	}
}

func TestResyncCanonicalStatePreservesNumbers(t *testing.T) {
	got, err := canonicalResyncState([]byte(` {"z":9007199254740993,"nested":{"b":null,"a":18446744073709551615},"a":1} `))
	require.NoError(t, err)
	require.Equal(t, `{"a":1,"nested":{"a":18446744073709551615,"b":null},"z":9007199254740993}`, string(got))
	for _, input := range []string{"null", "[]", "{} {}", "{} garbage"} {
		_, err := canonicalResyncState([]byte(input))
		require.Error(t, err, input)
	}
}

func TestResyncArtifactsBindPublishedIdentityAndFullCheckpoint(t *testing.T) {
	dir := t.TempDir()
	stateFile, receiptFile := filepath.Join(dir, "state.json"), filepath.Join(dir, "resync-result.json")
	checkpoint := &types.State{
		RWMutex: &sync.RWMutex{}, Type: types.GlobalType, Version: 1,
		Global:              &types.GlobalState{State: map[string]any{"lsn": "0/1234"}, Streams: types.NewSet("public.target", "public.unrelated")},
		SuppressPersistence: true,
	}
	require.NoError(t, writeResyncArtifacts(stateFile, receiptFile, checkpoint, resyncReceipt{
		Version: 1, ResyncID: "workflow-stable-id", Namespace: "public", StreamName: "target", TableUUID: "original-uuid", SnapshotID: 123,
		ExecutionID: "recovery-runtime-2", LaunchID: "admitted-launch-1",
	}))
	stateBytes, err := os.ReadFile(stateFile)
	require.NoError(t, err)
	canonical, err := canonicalResyncState(stateBytes)
	require.NoError(t, err)
	require.Equal(t, canonical, stateBytes)
	require.Contains(t, string(stateBytes), "public.unrelated")
	require.NotContains(t, string(stateBytes), "SuppressPersistence")
	receiptBytes, err := os.ReadFile(receiptFile)
	require.NoError(t, err)
	var receipt resyncReceipt
	require.NoError(t, json.Unmarshal(receiptBytes, &receipt))
	digest := sha256.Sum256(canonical)
	require.Equal(t, hex.EncodeToString(digest[:]), receipt.StateSHA256)
	require.Equal(t, "workflow-stable-id", receipt.ResyncID)
	require.Equal(t, "recovery-runtime-2", receipt.ExecutionID)
	require.Equal(t, "admitted-launch-1", receipt.LaunchID)
	require.Equal(t, "original-uuid", receipt.TableUUID)
	require.EqualValues(t, 123, receipt.SnapshotID)
}

func TestResyncIncompletePublicationCannotReplaceStateOrEmitReceipt(t *testing.T) {
	for _, receipt := range []resyncReceipt{
		{Version: 1, ResyncID: "stable", SnapshotID: 1},
		{Version: 1, ResyncID: "stable", TableUUID: "uuid"},
		{Version: 1, TableUUID: "uuid", SnapshotID: 1},
		{Version: 2, ResyncID: "stable", TableUUID: "uuid", SnapshotID: 1},
		{Version: 1, ResyncID: "stable", TableUUID: "uuid", SnapshotID: 1, ExecutionID: "runtime"},
		{Version: 1, ResyncID: "stable", TableUUID: "uuid", SnapshotID: 1, ExecutionID: "runtime", LaunchID: " "},
	} {
		dir := t.TempDir()
		stateFile, receiptFile := filepath.Join(dir, "state.json"), filepath.Join(dir, "resync-result.json")
		require.NoError(t, os.WriteFile(stateFile, []byte("old-authoritative-checkpoint"), 0600))
		require.Error(t, writeResyncArtifacts(stateFile, receiptFile, &types.State{}, receipt))
		data, err := os.ReadFile(stateFile)
		require.NoError(t, err)
		require.Equal(t, "old-authoritative-checkpoint", string(data))
		_, err = os.Stat(receiptFile)
		require.True(t, os.IsNotExist(err))
	}
}

func TestResyncStateWriteFailureCannotEmitSuccessReceipt(t *testing.T) {
	dir := t.TempDir()
	receiptFile := filepath.Join(dir, "resync-result.json")
	err := writeResyncArtifacts(dir, receiptFile, &types.State{}, resyncReceipt{Version: 1, ResyncID: "stable", TableUUID: "uuid", SnapshotID: 1})
	require.ErrorContains(t, err, "emit committed resync state")
	_, err = os.Stat(receiptFile)
	require.True(t, os.IsNotExist(err))
}

func TestResyncDefiniteFailureArtifactDoesNotTouchCheckpoint(t *testing.T) {
	dir := t.TempDir()
	stateFile := filepath.Join(dir, "state.json")
	require.NoError(t, os.WriteFile(stateFile, []byte("original"), 0600))
	failureFile := filepath.Join(dir, "resync-failure.json")
	require.NoError(t, writeResyncFailure(failureFile, "", "stable", "public", "target", "runtime-1", "launch-1"))
	data, err := os.ReadFile(failureFile)
	require.NoError(t, err)
	var evidence map[string]any
	require.NoError(t, json.Unmarshal(data, &evidence))
	require.Equal(t, "not_published", evidence["publication"])
	require.Equal(t, "stable", evidence["resync_id"])
	require.Equal(t, "runtime-1", evidence["execution_id"])
	require.Equal(t, "launch-1", evidence["launch_id"])
	data, err = os.ReadFile(stateFile)
	require.NoError(t, err)
	require.Equal(t, "original", string(data))
}

func TestResyncDeadlineAndCommandContract(t *testing.T) {
	require.Less(t, resyncMaximumDuration, 24*time.Hour)
	require.Equal(t, 12*time.Hour, resyncMaximumDuration)
	require.Equal(t, "resync", resyncCmd.Use)
	require.Contains(t, strings.ToLower(resyncCmd.Long), "12 hours")
	for _, name := range []string{"resync-namespace", "resync-table", "resync-id", "resync-launch-id", "resync-recover-only"} {
		require.NotNil(t, resyncCmd.Flags().Lookup(name))
	}
	require.NotSame(t, clearCmd, resyncCmd)
}

func TestResyncRetryInvalidatesPreviousAbortProof(t *testing.T) {
	path := filepath.Join(t.TempDir(), "resync-failure.json")
	require.NoError(t, writeResyncFailure(path, "", "same-id", "public", "target", "runtime", "launch"))
	require.NoError(t, writeResyncEvidence(context.Background(), path, "", "same-id", "public", "target", "in_progress", "runtime", "launch"))
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	var evidence map[string]any
	require.NoError(t, json.Unmarshal(data, &evidence))
	require.Equal(t, "same-id", evidence["resync_id"])
	require.Equal(t, "in_progress", evidence["publication"])
	require.NotEqual(t, "not_published", evidence["publication"])
}

func TestResyncMixedModeCatalogNeverSchedulesUnrelatedRebuilds(t *testing.T) {
	for _, mode := range []types.SyncMode{types.CDC, types.FULLREFRESH, types.INCREMENTAL} {
		t.Run(string(mode), func(t *testing.T) {
			saved := catalogOf(
				stream{name: "target", mode: mode},
				stream{name: "unrelated-full", mode: types.FULLREFRESH},
				stream{name: "unrelated-incremental", mode: types.INCREMENTAL},
				stream{name: "unrelated-cdc", mode: types.CDC},
			)
			target := saved.Streams[0]
			classification := &StreamClassification{
				SelectedStreams:    []types.StreamInterface{target, saved.Streams[1], saved.Streams[2], saved.Streams[3]},
				CDCStreams:         []types.StreamInterface{saved.Streams[3]},
				FullLoadStreams:    []types.StreamInterface{saved.Streams[1]},
				IncrementalStreams: []types.StreamInterface{saved.Streams[2]},
			}
			if mode == types.CDC {
				classification.CDCStreams = append(classification.CDCStreams, target)
			}
			full, cdc, incremental := resyncReadStreams(target, classification)
			require.Equal(t, classification.CDCStreams, cdc)
			require.Contains(t, cdc, saved.Streams[3])
			require.NotContains(t, full, saved.Streams[1])
			require.NotContains(t, incremental, saved.Streams[2])
			if mode == types.FULLREFRESH {
				require.Equal(t, []types.StreamInterface{target}, full)
			} else {
				require.Empty(t, full)
			}
			if mode == types.INCREMENTAL {
				require.Equal(t, []types.StreamInterface{target}, incremental)
			} else {
				require.Empty(t, incremental)
			}
			require.Len(t, saved.Streams, 4)
			require.Equal(t, []types.StreamInterface{saved.Streams[1]}, classification.FullLoadStreams)
			require.Equal(t, []types.StreamInterface{saved.Streams[2]}, classification.IncrementalStreams)
		})
	}
}

func TestControlledResyncRequiresAdmittedLaunchIdentity(t *testing.T) {
	namespace, table, operation, savedState, savedNoSave := resyncNamespace, resyncTable, resyncID, statePath, noSave
	runtime, fence, launch, recoverOnly := executionID, executionFence, resyncLaunchID, resyncRecoverOnly
	t.Cleanup(func() {
		resyncNamespace, resyncTable, resyncID, statePath, noSave = namespace, table, operation, savedState, savedNoSave
		executionID, executionFence, resyncLaunchID, resyncRecoverOnly = runtime, fence, launch, recoverOnly
	})
	resyncNamespace, resyncTable, resyncID, statePath, noSave = "public", "target", "stable", "state.json", false
	executionID, executionFence, resyncLaunchID, resyncRecoverOnly = "runtime", "fence.json", "", false
	require.ErrorContains(t, validateResyncFlags(), "--resync-launch-id")
	resyncLaunchID = "original-launch"
	require.NoError(t, validateResyncFlags())
	resyncRecoverOnly = true
	require.NoError(t, validateResyncFlags())
	executionID, executionFence, resyncLaunchID = "", "", ""
	require.ErrorContains(t, validateResyncFlags(), "recovery-only")
	resyncRecoverOnly = false
	require.NoError(t, validateResyncFlags(), "explicit standalone CLI remains supported")
}
