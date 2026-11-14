package utils

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"

	s3util "github.com/datazip-inc/olake/utils/s3"
)

// ExecutionOptions binds a controlled runtime to its positive, never-reopened fence.
// The fence is read fresh at every mutation boundary, never downloaded as config.
type ExecutionOptions struct {
	ID         string
	Fence      string
	Command    string
	retirement *executionRetirement
}

type executionContextKey struct{}

func WithExecution(ctx context.Context, execution *ExecutionOptions) context.Context {
	return context.WithValue(ctx, executionContextKey{}, execution)
}

func ExecutionFromContext(ctx context.Context) (*ExecutionOptions, bool) {
	execution, ok := ctx.Value(executionContextKey{}).(*ExecutionOptions)
	return execution, ok && execution != nil
}

// CheckExecutionFence leaves explicit standalone (unfenced) CLI usage unchanged.
// A controlled runtime requires a positively matching OPEN document; absence is
// never permission to mutate. Storage errors and cancellation fail closed.
func CheckExecutionFence(ctx context.Context) error {
	execution, controlled := ExecutionFromContext(ctx)
	if !controlled {
		return ctx.Err()
	}
	if strings.TrimSpace(execution.ID) == "" || strings.TrimSpace(execution.Fence) == "" {
		return fmt.Errorf("--execution-id and --execution-fence must be supplied together and nonblank")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	var input io.ReadCloser
	if isS3Path(execution.Fence) {
		bucket, key, err := parseS3URI(execution.Fence)
		if err != nil {
			return err
		}
		response, err := s3util.GetObject(ctx, bucket, key)
		if err != nil {
			return fmt.Errorf("runtime execution fence is unreadable: %w", err)
		}
		input = response.Body
	} else {
		file, err := os.Open(execution.Fence)
		if err != nil {
			return fmt.Errorf("runtime execution fence is unreadable: %w", err)
		}
		input = file
	}
	defer input.Close()
	data, err := io.ReadAll(io.LimitReader(input, 4097))
	if err != nil {
		return fmt.Errorf("read runtime execution fence: %w", err)
	}
	if len(data) > 4096 {
		return fmt.Errorf("runtime execution fence exceeds maximum document size")
	}
	return validateExecutionFence(data, execution.ID)
}

func validateExecutionFence(data []byte, executionID string) error {
	var fence struct {
		Version     int    `json:"version"`
		ExecutionID string `json:"execution_id"`
		State       string `json:"state"`
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&fence); err != nil {
		return fmt.Errorf("runtime execution fence is malformed: %w", err)
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return fmt.Errorf("runtime execution fence has trailing content")
	}
	if fence.Version != 1 || fence.ExecutionID != executionID || fence.State != "open" {
		return fmt.Errorf("runtime execution fence is not open for execution %q", executionID)
	}
	return nil
}
