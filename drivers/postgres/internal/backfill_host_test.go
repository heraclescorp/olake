package driver

import (
	"context"
	"errors"
	"testing"
	"time"
)

func readerAppliedAt(positions ...int64) (func(context.Context) (int64, error), *int) {
	calls := 0
	return func(context.Context) (int64, error) {
		position := positions[min(calls, len(positions)-1)]
		calls++
		return position, nil
	}, &calls
}

func TestWaitForAppliedLSN(t *testing.T) {
	t.Run("returns at the first applied position reaching the target", func(t *testing.T) {
		readApplied, calls := readerAppliedAt(98, 99, 100, 101)
		applied, _, err := waitForAppliedLSN(context.Background(), readApplied, 100, time.Second, time.Millisecond)
		if err != nil {
			t.Fatalf("expected the reader to be caught up, got: %v", err)
		}
		if applied != 100 || *calls != 3 {
			t.Fatalf("expected to stop at applied=100 after 3 reads, got applied=%d after %d reads", applied, *calls)
		}
	})

	t.Run("never succeeds while the reader stays behind", func(t *testing.T) {
		readApplied, _ := readerAppliedAt(99)
		timeout := 20 * time.Millisecond
		applied, waited, err := waitForAppliedLSN(context.Background(), readApplied, 100, timeout, time.Millisecond)
		if err == nil {
			t.Fatalf("expected an error while the reader is one position behind")
		}
		if applied != 99 || waited < timeout {
			t.Fatalf("expected applied=99 after at least %s, got applied=%d after %s", timeout, applied, waited)
		}
	})

	t.Run("stops when the context is canceled", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		readApplied := func(context.Context) (int64, error) {
			cancel()
			return 1, nil
		}
		_, _, err := waitForAppliedLSN(ctx, readApplied, 100, time.Minute, time.Minute)
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected context.Canceled, got: %v", err)
		}
	})

	t.Run("returns a failed position read without retrying", func(t *testing.T) {
		readFailure := errors.New("aurora_replica_status unavailable")
		calls := 0
		readApplied := func(context.Context) (int64, error) {
			calls++
			return 0, readFailure
		}
		_, _, err := waitForAppliedLSN(context.Background(), readApplied, 100, time.Minute, time.Millisecond)
		if !errors.Is(err, readFailure) || calls != 1 {
			t.Fatalf("expected the read failure after one read, got %v after %d reads", err, calls)
		}
	})
}
