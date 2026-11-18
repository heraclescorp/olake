package waljs

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/stretchr/testify/require"
)

func TestReplicationProgressSeparatesReadCommitAndServerEnd(t *testing.T) {
	now := time.Now()
	p := newReplicationProgress("pgoutput", "slot", 100, 1000, now)
	p.received(200, 2000, 50)
	p.transactionOpen.Store(true)
	s := p.snapshot(now.Add(time.Second))
	require.Equal(t, pglogrepl.LSN(200), s.Read)
	require.Equal(t, pglogrepl.LSN(100), s.DecodedCommit)
	require.Equal(t, uint64(800), s.RemainingBytes)
	require.True(t, s.TransactionOpen)
	p.committed(250)
	p.serverEnd.Store(5000)
	s = p.snapshot(now.Add(10 * time.Second))
	require.Equal(t, pglogrepl.LSN(250), s.DecodedCommit)
	require.Equal(t, pglogrepl.LSN(200), s.Read)
	require.Equal(t, uint64(1), s.Transactions)
	require.False(t, s.TransactionOpen)
	require.Equal(t, pglogrepl.LSN(100), p.start)
	p.received(1100, 6000, 2)
	require.Zero(t, p.snapshot(now).RemainingBytes)
}
func TestProgressSnapshotConcurrentWithDecoder(t *testing.T) {
	p := newReplicationProgress("pgoutput", "slot", 0, 1000, time.Now())
	var wg sync.WaitGroup
	wg.Go(func() {
		for i := 0; i < 1000; i++ {
			p.received(pglogrepl.LSN(i), 2000, 20)
			p.committed(pglogrepl.LSN(i))
		}
	})
	for i := 0; i < 1000; i++ {
		_ = p.snapshot(time.Now())
	}
	wg.Wait()
	require.Equal(t, uint64(1000), p.messages.Load())
}
func TestProgressReporterJoinsOnExit(t *testing.T) {
	socket := &Socket{ReplicationSlot: "slot", ConfirmedFlushLSN: 100, CurrentWalPosition: 200}
	p, stop := startReplicationProgress(socket, "pgoutput")
	p.received(150, 200, 10)
	stop()
	require.Zero(t, socket.ClientXLogPos) // logging must not mutate socket
}

func TestPGOutputProgressUpdatesOnlyOnDecodedCommit(t *testing.T) {
	p := &pgoutputReplicator{socket: &Socket{}, progress: newReplicationProgress("pgoutput", "slot", 100, 1000, time.Now())}
	require.NoError(t, p.processPgoutputWAL(context.Background(), resyncBegin(250), nil))
	require.True(t, p.progress.snapshot(time.Now()).TransactionOpen)
	require.Equal(t, pglogrepl.LSN(100), p.progress.snapshot(time.Now()).DecodedCommit)
	require.NoError(t, p.processPgoutputWAL(context.Background(), resyncCommit(250, 260), nil))
	s := p.progress.snapshot(time.Now())
	require.False(t, s.TransactionOpen)
	require.Equal(t, pglogrepl.LSN(260), s.DecodedCommit)
}

func TestProgressTimerEmitsWithoutDecoderActivity(t *testing.T) {
	events := make(chan string, 100)
	stop := runProgressReporter(time.Millisecond, func(event string) { events <- event })
	require.Equal(t, "start", <-events)
	select {
	case event := <-events:
		require.Equal(t, "periodic", event)
	case <-time.After(time.Second):
		t.Fatal("progress timer blocked on decoder")
	}
	stop()
	last := ""
	for len(events) > 0 {
		last = <-events
	}
	require.Equal(t, "stream_exit", last)
}
