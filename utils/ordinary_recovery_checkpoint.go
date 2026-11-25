package utils

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

const OrdinaryRecoveryCheckpointName = "ordinary-recovery-checkpoint.json"

func OrdinaryRecoveryCheckpointPaths(resultPath, resultURI, executionID, lsn string) (string, string, error) {
	if executionID == "" || executionID == "." || executionID == ".." || strings.ContainsAny(executionID, "/\\\x00") {
		return "", "", fmt.Errorf("invalid checkpoint execution identity")
	}
	parts := strings.Split(lsn, "/")
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return "", "", fmt.Errorf("invalid checkpoint LSN")
	}
	for _, part := range parts {
		for _, char := range part {
			if !strings.ContainsRune("0123456789ABCDEFabcdef", char) {
				return "", "", fmt.Errorf("invalid checkpoint LSN")
			}
		}
	}
	name := executionID + "-" + strings.Join(parts, "-") + ".json"
	local := filepath.Join(filepath.Dir(resultPath), "ordinary-recovery-checkpoints", name)
	if resultURI == "" {
		return local, "", nil
	}
	index := strings.LastIndex(resultURI, "/")
	if index < 0 {
		return "", "", fmt.Errorf("invalid checkpoint artifact URI")
	}
	return local, resultURI[:index] + "/ordinary-recovery-checkpoints/" + name, nil
}

func PublishVerifiedRecoveryCheckpoint(ctx context.Context, local, uri string, data []byte) (string, error) {
	if err := CheckExecutionFence(ctx); err != nil {
		return "", err
	}
	if uri == "" {
		if err := os.MkdirAll(filepath.Dir(local), 0700); err != nil {
			return "", err
		}
	}
	if err := PublishImmutableArtifact(ctx, local, uri, data); err != nil {
		return "", err
	}
	observed, err := ReadImmutableArtifact(ctx, local, uri)
	if err != nil {
		return "", err
	}
	if !bytes.Equal(data, observed) {
		return "", fmt.Errorf("ordinary recovery checkpoint read-after-create mismatch")
	}
	if err := CheckExecutionFence(ctx); err != nil {
		return "", err
	}
	sum := sha256.Sum256(observed)
	return hex.EncodeToString(sum[:]), nil
}
