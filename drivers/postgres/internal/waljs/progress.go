package waljs

import (
	"sync/atomic"
	"time"

	"github.com/datazip-inc/olake/utils/logger"
	"github.com/jackc/pglogrepl"
)

const progressLogInterval = 30 * time.Second

// Independent atomics let the timer report while ReceiveMessage, hydration or
// destination backpressure blocks. It never reads the replication Socket, whose
// fields belong to the decoder goroutine. These are diagnostics, not checkpoints.
type replicationProgress struct {
	start, target                        pglogrepl.LSN
	plugin, slot                         string
	started                              time.Time
	read, serverEnd, decodedCommit       atomic.Uint64
	messages, payloadBytes, transactions atomic.Uint64
	transactionOpen                      atomic.Bool
	lastReceived                         atomic.Int64
}

type progressSnapshot struct {
	Read, ServerEnd, DecodedCommit                       pglogrepl.LSN
	Messages, PayloadBytes, Transactions, RemainingBytes uint64
	TransactionOpen                                      bool
	Elapsed, Idle                                        time.Duration
}

func (p *replicationProgress) snapshot(now time.Time) progressSnapshot {
	s := progressSnapshot{Read: pglogrepl.LSN(p.read.Load()), ServerEnd: pglogrepl.LSN(p.serverEnd.Load()), DecodedCommit: pglogrepl.LSN(p.decodedCommit.Load()), Messages: p.messages.Load(), PayloadBytes: p.payloadBytes.Load(), Transactions: p.transactions.Load(), TransactionOpen: p.transactionOpen.Load(), Elapsed: now.Sub(p.started), Idle: now.Sub(time.Unix(0, p.lastReceived.Load()))}
	if s.Read < p.target {
		s.RemainingBytes = uint64(p.target - s.Read)
	}
	return s
}

func (p *replicationProgress) emit(event string) {
	s := p.snapshot(time.Now())
	logger.Infof("postgres_cdc_progress event=%s plugin=%q slot=%q start_lsn=%s read_lsn=%s decoded_commit_lsn=%s server_wal_end_lsn=%s target_lsn=%s durable_ack_baseline_lsn=%s transaction_open=%t wal_messages=%d wal_payload_bytes=%d decoded_transactions=%d wal_address_bytes_remaining=%d elapsed_seconds=%.0f wal_data_idle_seconds=%.0f destination_checkpoint=not_advanced_by_progress_log", event, p.plugin, p.slot, p.start, s.Read, s.DecodedCommit, s.ServerEnd, p.target, p.start, s.TransactionOpen, s.Messages, s.PayloadBytes, s.Transactions, s.RemainingBytes, s.Elapsed.Seconds(), s.Idle.Seconds())
}

func newReplicationProgress(plugin, slot string, start, target pglogrepl.LSN, now time.Time) *replicationProgress {
	p := &replicationProgress{plugin: plugin, slot: slot, start: start, target: target, started: now}
	p.read.Store(uint64(start))
	p.decodedCommit.Store(uint64(start))
	p.lastReceived.Store(now.UnixNano())
	return p
}

func startReplicationProgress(socket *Socket, plugin string) (*replicationProgress, func()) {
	p := newReplicationProgress(plugin, socket.ReplicationSlot, socket.ConfirmedFlushLSN, socket.CurrentWalPosition, time.Now())
	return p, runProgressReporter(progressLogInterval, p.emit)
}

func runProgressReporter(interval time.Duration, emit func(string)) func() {
	stop, done := make(chan struct{}), make(chan struct{})
	emit("start")
	go func() {
		defer close(done)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
				emit("periodic")
			}
		}
	}()
	return func() { close(stop); <-done; emit("stream_exit") }
}

func (p *replicationProgress) received(read, serverEnd pglogrepl.LSN, bytes int) {
	p.read.Store(uint64(read))
	p.serverEnd.Store(uint64(serverEnd))
	p.messages.Add(1)
	p.payloadBytes.Add(uint64(bytes))
	p.lastReceived.Store(time.Now().UnixNano())
}

// A PostgreSQL COMMIT decoded is not a durable Iceberg commit. Hydration and
// asynchronous writer buffers may still hold some of its rows.
func (p *replicationProgress) committed(lsn pglogrepl.LSN) {
	p.decodedCommit.Store(uint64(lsn))
	p.transactions.Add(1)
	p.transactionOpen.Store(false)
}
