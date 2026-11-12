package utils

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestExecutionFenceRequiresPositiveMatchingOpenDocument(t *testing.T) {
	for _, tc := range []struct {
		name, document string
		allowed        bool
	}{
		{"open", `{"version":1,"execution_id":"runtime-1","state":"open"}`, true},
		{"closed", `{"version":1,"execution_id":"runtime-1","state":"closed"}`, false},
		{"wrong-runtime", `{"version":1,"execution_id":"runtime-2","state":"open"}`, false},
		{"wrong-version", `{"version":2,"execution_id":"runtime-1","state":"open"}`, false},
		{"missing-state", `{"version":1,"execution_id":"runtime-1"}`, false},
		{"unknown-state", `{"version":1,"execution_id":"runtime-1","state":"pending"}`, false},
		{"malformed", `{"version":`, false},
		{"trailing", `{"version":1,"execution_id":"runtime-1","state":"open"} {}`, false},
		{"oversize", string(make([]byte, 4097)), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "execution-fence.json")
			require.NoError(t, os.WriteFile(path, []byte(tc.document), 0600))
			ctx := WithExecution(context.Background(), &ExecutionOptions{ID: "runtime-1", Fence: path})
			err := CheckExecutionFence(ctx)
			if tc.allowed {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
		})
	}
}

func TestExecutionFenceClosureAndDisappearanceRejectNewMutations(t *testing.T) {
	path := filepath.Join(t.TempDir(), "execution-fence.json")
	ctx := WithExecution(context.Background(), &ExecutionOptions{ID: "runtime-1", Fence: path})
	require.Error(t, CheckExecutionFence(ctx), "missing is never authorization")
	require.NoError(t, os.WriteFile(path, []byte(`{"version":1,"execution_id":"runtime-1","state":"open"}`), 0600))
	require.NoError(t, CheckExecutionFence(ctx))
	// Deterministically close between admission and the next mutation boundary.
	closed := filepath.Join(filepath.Dir(path), "closed.json")
	require.NoError(t, os.WriteFile(closed, []byte(`{"version":1,"execution_id":"runtime-1","state":"closed"}`), 0600))
	require.NoError(t, os.Rename(closed, path))
	require.Error(t, CheckExecutionFence(ctx))
	require.NoError(t, os.Remove(path))
	require.Error(t, CheckExecutionFence(ctx))
}

func TestExecutionFenceStandaloneAndIncompleteFlags(t *testing.T) {
	require.NoError(t, CheckExecutionFence(context.Background()))
	for _, execution := range []*ExecutionOptions{{}, {ID: "runtime"}, {Fence: "/missing"}} {
		require.ErrorContains(t, CheckExecutionFence(WithExecution(context.Background(), execution)), "supplied together")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, CheckExecutionFence(ctx), context.Canceled)
}
