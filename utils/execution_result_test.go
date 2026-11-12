package utils

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

func retirementContext() context.Context {
	ctx := WithExecution(context.Background(), &ExecutionOptions{ID: "runtime", Fence: "fence"})
	BeginOrdinaryExecution(ctx, "sync")
	return ctx
}

func TestOrdinaryPreDestinationFailureNotStarted(t *testing.T) {
	// Source authentication fails before any destination JVM or durable ACK.
	result, ok := OrdinaryExecutionResult(retirementContext())
	if !ok || result.Outcome != "not_started" {
		t.Fatalf("unexpected evidence: %+v", result)
	}
}

func TestOrdinaryLocalFailureCanSettleOnlyAfterDrainAndTerminals(t *testing.T) {
	ctx := retirementContext()
	drop := BeginExecutionParticipant(ctx)
	drop(true)
	pool := BeginExecutionParticipant(ctx)
	SourceDrained(ctx, nil)
	result, _ := OrdinaryExecutionResult(ctx)
	if result.Outcome != "unknown" {
		t.Fatal(result)
	}
	pool(true)
	result, _ = OrdinaryExecutionResult(ctx)
	if result.Outcome != "settled" {
		t.Fatal(result)
	}
}

func TestOrdinaryUnknownSticksAcrossJVMs(t *testing.T) {
	ctx := retirementContext()
	BeginExecutionParticipant(ctx)(false)
	BeginExecutionParticipant(ctx)(true)
	SourceDrained(ctx, nil)
	result, _ := OrdinaryExecutionResult(ctx)
	if result.Outcome != "unknown" {
		t.Fatal(result)
	}
}

func TestOrdinaryDrainFailureCannotSettle(t *testing.T) {
	ctx := retirementContext()
	BeginExecutionParticipant(ctx)(true)
	SourceDrained(ctx, context.Canceled)
	SourceDrained(ctx, nil)
	result, _ := OrdinaryExecutionResult(ctx)
	if result.Outcome != "unknown" {
		t.Fatal(result)
	}
}

func TestCanonicalCompleteSavedState(t *testing.T) {
	a, err := CanonicalStateSHA256([]byte(`{"z":9007199254740993,"a":{"lsn":"0/ABC"}}`))
	if err != nil {
		t.Fatal(err)
	}
	b, err := CanonicalStateSHA256([]byte(`{"a":{"lsn":"0/ABC"},"z":9007199254740993}`))
	if err != nil || a != b {
		t.Fatalf("canonical hashes differ: %s %s %v", a, b, err)
	}
	c, _ := CanonicalStateSHA256([]byte(`{"z":9007199254740992,"a":{"lsn":"0/ABC"}}`))
	if a == c {
		t.Fatal("checkpoint number lost precision")
	}
	for _, bad := range []string{`null`, `[]`, `{} {}`} {
		if _, err := CanonicalStateSHA256([]byte(bad)); err == nil {
			t.Fatalf("accepted %s", bad)
		}
	}
}

func TestExecutionResultImmutableAgainstLateClosedWriter(t *testing.T) {
	file := filepath.Join(t.TempDir(), "execution-result.json")
	first := ExecutionResult{Version: 1, ExecutionID: "runtime", LaunchID: "runtime", Command: "sync", Outcome: "unknown"}
	if err := PublishExecutionResult(context.Background(), file, "", first); err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(file)
	if err != nil {
		t.Fatal(err)
	}
	first.Outcome = "settled"
	if err := PublishExecutionResult(context.Background(), file, "", first); err == nil {
		t.Fatal("overwrote immutable evidence")
	}
	after, err := os.ReadFile(file)
	if err != nil || string(before) != string(after) {
		t.Fatal("late writer replaced evidence")
	}
}
