package utils

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	s3util "github.com/datazip-inc/olake/utils/s3"
	"io"
	"os"
	"path/filepath"
	"sync"
)

type executionRetirement struct {
	mu           sync.Mutex
	participants int
	pending      int
	unknown      bool
	drained      bool
}

type ExecutionResult struct {
	Version     int    `json:"version"`
	ExecutionID string `json:"execution_id"`
	LaunchID    string `json:"launch_id"`
	Command     string `json:"command"`
	Outcome     string `json:"outcome"`
	StateSHA256 string `json:"state_sha256,omitempty"`
}

func BeginOrdinaryExecution(ctx context.Context, command string) {
	if e, ok := ExecutionFromContext(ctx); ok && (command == "sync" || command == "clear-destination") {
		e.Command = command
		e.retirement = &executionRetirement{}
	}
}

// BeginExecutionParticipant must precede a destination launch attempt, not its
// capability response. Each participant must positively retire; uncertainty sticks.
func BeginExecutionParticipant(ctx context.Context) func(bool) {
	e, ok := ExecutionFromContext(ctx)
	if !ok || e.retirement == nil {
		return func(bool) {}
	}
	t := e.retirement
	t.mu.Lock()
	t.participants++
	t.pending++
	t.mu.Unlock()
	var once sync.Once
	return func(settled bool) {
		once.Do(func() {
			t.mu.Lock()
			defer t.mu.Unlock()
			t.pending--
			t.unknown = t.unknown || !settled
		})
	}
}

// SourceDrained is called only after source and writer goroutines have joined.
// A cleanup failure is not the command's local data-processing error.
func SourceDrained(ctx context.Context, err error) {
	if e, ok := ExecutionFromContext(ctx); ok && e.retirement != nil {
		t := e.retirement
		t.mu.Lock()
		defer t.mu.Unlock()
		t.drained = true
		t.unknown = t.unknown || err != nil
	}
}

func OrdinaryExecutionResult(ctx context.Context) (ExecutionResult, bool) {
	e, ok := ExecutionFromContext(ctx)
	if !ok || e.retirement == nil {
		return ExecutionResult{}, false
	}
	t := e.retirement
	t.mu.Lock()
	defer t.mu.Unlock()
	outcome := "unknown"
	if t.participants == 0 {
		outcome = "not_started"
	} else if t.pending == 0 && !t.unknown && t.drained {
		outcome = "settled"
	}
	return ExecutionResult{Version: 1, ExecutionID: e.ID, LaunchID: e.ID, Command: e.Command, Outcome: outcome}, true
}

// CanonicalStateSHA256 hashes the complete saved JSON object without float64
// roundoff of checkpoint numbers.
func CanonicalStateSHA256(data []byte) (string, error) {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	var state map[string]any
	if err := decoder.Decode(&state); err != nil {
		return "", err
	}
	if state == nil {
		return "", fmt.Errorf("state must be a JSON object")
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return "", fmt.Errorf("state has trailing content")
	}
	canonical, err := json.Marshal(state)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(canonical)
	return hex.EncodeToString(sum[:]), nil
}

// PublishExecutionResult never replaces evidence published by an earlier runtime.
func PublishExecutionResult(ctx context.Context, localPath, uri string, result ExecutionResult) error {
	data, err := json.Marshal(result)
	if err != nil {
		return err
	}
	if uri != "" {
		bucket, key, err := parseS3URI(uri)
		if err != nil {
			return err
		}
		return s3util.PutObjectIfAbsent(ctx, bucket, key, data)
	}
	file, err := os.CreateTemp(filepath.Dir(localPath), ".execution-result-*")
	if err != nil {
		return err
	}
	defer os.Remove(file.Name())
	if err := file.Chmod(0600); err != nil {
		file.Close()
		return err
	}
	if _, err := file.Write(data); err != nil {
		file.Close()
		return err
	}
	if err := file.Sync(); err != nil {
		file.Close()
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	return os.Link(file.Name(), localPath)
}
