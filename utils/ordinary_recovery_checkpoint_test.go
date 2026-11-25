package utils

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/datazip-inc/olake/constants"
	s3util "github.com/datazip-inc/olake/utils/s3"
	"github.com/stretchr/testify/require"
)

func TestOrdinaryRecoveryCheckpointPathsPreserveRemoteDirectory(t *testing.T) {
	local, uri, err := OrdinaryRecoveryCheckpointPaths("/tmp/runtime/ordinary-recovery-result.json", "s3://bucket/runtime/ordinary-recovery-result.json", "recovery-2", "1A/BC")
	require.NoError(t, err)
	require.Equal(t, "/tmp/runtime/ordinary-recovery-checkpoints/recovery-2-1A-BC.json", local)
	require.Equal(t, "s3://bucket/runtime/ordinary-recovery-checkpoints/recovery-2-1A-BC.json", uri)
	for _, input := range [][2]string{{"../foreign", "0/100"}, {"runtime", "../state"}, {"runtime", "0/100/1"}, {"runtime", "0/xyz"}, {"runtime", "0/"}} {
		_, _, err := OrdinaryRecoveryCheckpointPaths("result.json", "", input[0], input[1])
		require.Error(t, err)
	}
}

func TestPublishVerifiedRecoveryCheckpointIsImmutableAndFenced(t *testing.T) {
	dir := t.TempDir()
	local := filepath.Join(dir, "ordinary-recovery-checkpoints", "runtime-0-100.json")
	data := []byte(`{"checkpoint":"0/100"}`)
	digest, err := PublishVerifiedRecoveryCheckpoint(context.Background(), local, "", data)
	require.NoError(t, err)
	want := sha256.Sum256(data)
	require.Equal(t, hex.EncodeToString(want[:]), digest)
	_, err = PublishVerifiedRecoveryCheckpoint(context.Background(), local, "", []byte(`{"changed":true}`))
	require.Error(t, err)
	observed, err := os.ReadFile(local)
	require.NoError(t, err)
	require.Equal(t, data, observed)
	fence := filepath.Join(dir, "execution-fence.json")
	require.NoError(t, os.WriteFile(fence, []byte(`{"version":1,"execution_id":"runtime","state":"closed"}`), 0600))
	ctx := WithExecution(context.Background(), &ExecutionOptions{ID: "runtime", Fence: fence})
	blocked := filepath.Join(dir, "ordinary-recovery-checkpoints", "runtime-0-200.json")
	_, err = PublishVerifiedRecoveryCheckpoint(ctx, blocked, "", data)
	require.Error(t, err)
	_, err = os.Stat(blocked)
	require.True(t, os.IsNotExist(err))
}

func TestRecoveryCheckpointRemoteReadbackMismatchFailsClosed(t *testing.T) {
	if os.Getenv("OLAKE_TEST_CHECKPOINT_READBACK") != "1" {
		child := exec.Command(os.Args[0], "-test.run=^TestRecoveryCheckpointRemoteReadbackMismatchFailsClosed$", "-test.count=1")
		child.Env = append(os.Environ(), "OLAKE_TEST_CHECKPOINT_READBACK=1")
		output, err := child.CombinedOutput()
		require.NoError(t, err, "%s", output)
		return
	}
	puts, gets := 0, 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPut:
			puts++
			if r.Header.Get("If-None-Match") != "*" {
				http.Error(w, "missing immutable create precondition", http.StatusBadRequest)
				return
			}
			w.WriteHeader(http.StatusOK)
		case http.MethodGet:
			gets++
			fmt.Fprint(w, `{"tampered":true}`)
		default:
			http.Error(w, "unexpected request", http.StatusBadRequest)
		}
	}))
	defer server.Close()
	t.Setenv(constants.EnvStorageMode, constants.StorageModeS3)
	t.Setenv(constants.EnvS3Endpoint, server.URL)
	t.Setenv(constants.EnvS3Region, "us-east-2")
	t.Setenv(constants.EnvS3AccessKeyID, "test")
	t.Setenv(constants.EnvS3SecretAccessKey, "test")
	require.NoError(t, s3util.Init(context.Background()))
	_, err := PublishVerifiedRecoveryCheckpoint(context.Background(), filepath.Join(t.TempDir(), "not-written.json"), "s3://bucket/runtime/ordinary-recovery-checkpoints/runtime-0-100.json", []byte(`{"checkpoint":"0/100"}`))
	require.ErrorContains(t, err, "read-after-create mismatch")
	require.Equal(t, 1, puts)
	require.Equal(t, 1, gets)
}
