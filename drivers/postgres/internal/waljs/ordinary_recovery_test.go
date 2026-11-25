package waljs

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/datazip-inc/olake/drivers/abstract"
	"github.com/datazip-inc/olake/types"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/require"
)

func TestOrdinaryRecoverySourceIdentityIsDeterministicAndUnambiguous(t *testing.T) {
	first, err := SourceIdentity(pglogrepl.IdentifySystemResult{SystemID: "123", Timeline: 1, DBName: "db|slot"}, "name", "pgoutput")
	require.NoError(t, err)
	second, err := SourceIdentity(pglogrepl.IdentifySystemResult{SystemID: "123", Timeline: 1, DBName: "db"}, "slot|name", "pgoutput")
	require.NoError(t, err)
	require.NotEqual(t, first, second)
	again, err := SourceIdentity(pglogrepl.IdentifySystemResult{SystemID: "123", Timeline: 1, DBName: "db|slot"}, "name", "pgoutput")
	require.NoError(t, err)
	require.Equal(t, first, again)
	_, err = SourceIdentity(pglogrepl.IdentifySystemResult{}, "name", "pgoutput")
	require.Error(t, err)
}

func strictRecoveryDecoder() (*pgoutputReplicator, []types.StreamInterface) {
	first := types.NewStream("first", "public", nil).Wrap(0)
	second := types.NewStream("second", "public", nil).Wrap(0)
	filter := NewChangeFilter(func(value any, _ string) (any, error) { return value, nil }, nil, first, second)
	filter.replayAfter = map[string]pglogrepl.LSN{first.ID(): 0x100, second.ID(): 0x500}
	return &pgoutputReplicator{socket: &Socket{StrictRecovery: true, CurrentWalPosition: 0x500, ClientXLogPos: 0x100, changeFilter: filter},
		relationIDToMsgMap: map[uint32]*pglogrepl.RelationMessage{
			1: {Namespace: "public", RelationName: "first", Columns: []*pglogrepl.RelationMessageColumn{{Name: "id", DataType: pgtype.Int4OID}}},
			2: {Namespace: "public", RelationName: "second", Columns: []*pglogrepl.RelationMessageColumn{{Name: "id", DataType: pgtype.Int4OID}}},
		}}, []types.StreamInterface{first, second}
}

func strictRecoveryInsert(relation byte) []byte {
	// INSERT, relation OID, new tuple, one text column containing 1.
	return []byte{'I', 0, 0, 0, relation, 'N', 0, 1, 't', 0, 0, 0, 1, '1'}
}

func TestOrdinaryRecoveryPGOutputReplaysEachMissingPrefixOnlyThroughTarget(t *testing.T) {
	p, streams := strictRecoveryDecoder()
	var got []abstract.CDCChange
	callback := func(_ context.Context, change abstract.CDCChange) error { got = append(got, change); return nil }
	for _, boundary := range []struct{ commit, end pglogrepl.LSN }{{0x200, 0x210}, {0x4F0, 0x500}} {
		require.NoError(t, p.processPgoutputWAL(context.Background(), resyncBegin(boundary.commit), callback))
		require.NoError(t, p.processPgoutputWAL(context.Background(), strictRecoveryInsert(1), callback))
		require.NoError(t, p.processPgoutputWAL(context.Background(), strictRecoveryInsert(2), callback))
		before := len(got)
		require.NoError(t, p.processPgoutputWAL(context.Background(), resyncCommit(boundary.commit, boundary.end), callback))
		require.Equal(t, before+1, len(got), "table two is already covered; table one still needs every missing transaction")
	}
	require.Len(t, got, 2)
	for _, change := range got {
		require.Equal(t, streams[0].ID(), change.Stream.ID())
	}
	require.Equal(t, pglogrepl.LSN(0x500), p.socket.ClientXLogPos)
	require.False(t, p.socket.TransactionOpen)
}

func TestOrdinaryRecoveryPGOutputWithholdsCrossingAndFutureTransactions(t *testing.T) {
	for _, scenario := range []string{"crossing_commit", "future_transaction", "at_commit_start", "callback_failure", "canceled_commit"} {
		t.Run(scenario, func(t *testing.T) {
			p, _ := strictRecoveryDecoder()
			var emitted int
			callback := func(context.Context, abstract.CDCChange) error {
				emitted++
				if scenario == "callback_failure" {
					return errors.New("writer failed")
				}
				return nil
			}
			if scenario == "future_transaction" {
				require.NoError(t, p.processPgoutputWAL(context.Background(), resyncBegin(0x600), callback))
				require.False(t, p.socket.TransactionOpen)
				require.Equal(t, pglogrepl.LSN(0x500), p.socket.ClientXLogPos, "outer decoder stops before the future transaction's first event")
				require.Zero(t, emitted)
				return
			}
			if scenario == "at_commit_start" {
				require.ErrorContains(t, p.processPgoutputWAL(context.Background(), resyncBegin(0x500), callback), "inside")
				require.Zero(t, emitted)
				return
			}
			require.NoError(t, p.processPgoutputWAL(context.Background(), resyncBegin(0x4F0), callback))
			require.NoError(t, p.processPgoutputWAL(context.Background(), strictRecoveryInsert(1), callback))
			require.Zero(t, emitted, "rows must wait for COMMIT's end LSN")
			end := pglogrepl.LSN(0x500)
			if scenario == "crossing_commit" {
				end = 0x510
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if scenario == "canceled_commit" {
				cancel()
			}
			require.Error(t, p.processPgoutputWAL(ctx, resyncCommit(0x4F0, end), callback))
			require.True(t, p.socket.TransactionOpen)
			require.Equal(t, pglogrepl.LSN(0x100), p.socket.ClientXLogPos)
			if scenario != "callback_failure" {
				require.Zero(t, emitted)
			}
		})
	}
}

func TestOrdinaryRecoveryWal2JSONStopsBeforeFutureTransactionCallbacks(t *testing.T) {
	p, streams := strictRecoveryDecoder()
	filter := p.socket.changeFilter
	target := pglogrepl.LSN(0x500)
	filter.replayThrough = &target
	for _, next := range []string{"0/200", "0/500", "0/600"} {
		var emitted []string
		message := fmt.Sprintf(`{"nextlsn":%q,"change":[{"kind":"insert","schema":"public","table":"first","columnnames":["id"],"columntypes":["integer"],"columnvalues":[1]},{"kind":"insert","schema":"public","table":"second","columnnames":["id"],"columntypes":["integer"],"columnvalues":[1]}]}`, next)
		_, _, err := filter.FilterWalJsChange(context.Background(), []byte(message), func(_ context.Context, change abstract.CDCChange) error {
			emitted = append(emitted, change.Stream.ID())
			return nil
		})
		if next == "0/600" {
			require.ErrorIs(t, err, errOrdinaryRecoveryTargetReached)
			require.Empty(t, emitted)
		} else {
			require.NoError(t, err)
			require.Equal(t, []string{streams[0].ID()}, emitted)
		}
	}
}

func TestOrdinaryRecoveryReplicatorRequiresExplicitBoundAndIdentity(t *testing.T) {
	_, err := NewReplicator(context.Background(), &Config{StrictRecovery: true}, ReplicationSlot{}, nil, nil, nil)
	require.ErrorContains(t, err, "explicit target")
	target := pglogrepl.LSN(0x500)
	_, err = NewReplicator(context.Background(), &Config{StrictRecovery: true}, ReplicationSlot{}, &target, nil, nil)
	require.ErrorContains(t, err, "pinned source identity")
}

func TestOrdinaryRecoveryLogicalKeepaliveNeverCrossesOpenTransactionOrTarget(t *testing.T) {
	socket := &Socket{StrictRecovery: true, ClientXLogPos: 0x100, CurrentWalPosition: 0x500, TransactionOpen: true}
	socket.observeKeepalive(0x900)
	require.Equal(t, pglogrepl.LSN(0x100), socket.ClientXLogPos, "keepalive cannot complete an open transaction")
	socket.TransactionOpen = false
	socket.observeKeepalive(0x300)
	require.Equal(t, pglogrepl.LSN(0x300), socket.ClientXLogPos)
	socket.observeKeepalive(0x200)
	require.Equal(t, pglogrepl.LSN(0x300), socket.ClientXLogPos, "old keepalive cannot regress decoded progress")
	socket.observeKeepalive(0x900)
	require.Equal(t, pglogrepl.LSN(0x500), socket.ClientXLogPos, "later source activity cannot widen recovery checkpoint")
}

func TestOrdinaryRecoveryCheckpointDeadlineNeverSplitsTransaction(t *testing.T) {
	p, _ := strictRecoveryDecoder()
	now := time.Unix(100, 0)
	p.socket.RecoveryStartLSN = 0x100
	p.socket.RecoveryBatchDeadline = now
	var emitted int
	callback := func(context.Context, abstract.CDCChange) error { emitted++; return nil }
	require.False(t, p.socket.recoveryBatchComplete(now))
	require.NoError(t, p.processPgoutputWAL(context.Background(), resyncBegin(0x200), callback))
	require.NoError(t, p.processPgoutputWAL(context.Background(), strictRecoveryInsert(1), callback))
	p.socket.observeKeepalive(0x900)
	require.False(t, p.socket.recoveryBatchComplete(now.Add(time.Hour)))
	require.Zero(t, emitted)
	require.NoError(t, p.processPgoutputWAL(context.Background(), resyncCommit(0x200, 0x210), callback))
	require.Equal(t, 1, emitted)
	require.True(t, p.socket.recoveryBatchComplete(now.Add(time.Hour)))
	require.Equal(t, pglogrepl.LSN(0x210), p.socket.RecoveryBatchLSN)
	require.Equal(t, pglogrepl.LSN(0x500), p.socket.CurrentWalPosition)
}

func TestOrdinaryRecoveryCheckpointWaitsForDeadlineOrFinalTarget(t *testing.T) {
	now := time.Unix(100, 0)
	socket := &Socket{StrictRecovery: true, ConfirmedFlushLSN: 0x100, RecoveryStartLSN: 0x300,
		ClientXLogPos: 0x400, CurrentWalPosition: 0x500, RecoveryBatchDeadline: now.Add(time.Minute)}
	require.False(t, socket.recoveryBatchComplete(now))
	require.True(t, socket.recoveryBatchComplete(now.Add(time.Minute)))
	require.Equal(t, pglogrepl.LSN(0x400), socket.RecoveryBatchLSN)
	require.Equal(t, pglogrepl.LSN(0x100), socket.ConfirmedFlushLSN)
	socket.ClientXLogPos = 0x500
	require.True(t, socket.recoveryBatchComplete(now))
	require.Equal(t, pglogrepl.LSN(0x500), socket.RecoveryBatchLSN)
}

func recoveryWireConnection(t *testing.T, start string, serve func(*pgproto3.Backend) error) (*pgconn.PgConn, <-chan error) {
	t.Helper()
	client, server := net.Pipe()
	t.Cleanup(func() { _ = client.Close(); _ = server.Close() })
	require.NoError(t, server.SetDeadline(time.Now().Add(10*time.Second)))
	done := make(chan error, 1)
	go func() {
		defer server.Close()
		done <- func() error {
			backend := pgproto3.NewBackend(server, server)
			if _, err := backend.ReceiveStartupMessage(); err != nil {
				return err
			}
			backend.Send(&pgproto3.AuthenticationOk{})
			backend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
			if err := backend.Flush(); err != nil {
				return err
			}
			message, err := backend.Receive()
			if err != nil {
				return err
			}
			query, ok := message.(*pgproto3.Query)
			if !ok || !strings.Contains(query.String, "LOGICAL "+start) {
				return fmt.Errorf("expected replication from %s, got %#v", start, message)
			}
			backend.Send(&pgproto3.CopyBothResponse{})
			if err := backend.Flush(); err != nil {
				return err
			}
			return serve(backend)
		}()
	}()
	config, err := pgconn.ParseConfig("postgres://test@localhost/test?sslmode=disable")
	require.NoError(t, err)
	config.DialFunc = func(context.Context, string, string) (net.Conn, error) { return client, nil }
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, err := pgconn.ConnectConfig(ctx, config)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close(context.Background()) })
	return conn, done
}

func recoveryWireWAL(start, advertised pglogrepl.LSN, payload []byte) *pgproto3.CopyData {
	data := make([]byte, 25, 25+len(payload))
	data[0] = pglogrepl.XLogDataByteID
	binary.BigEndian.PutUint64(data[1:9], uint64(start))
	binary.BigEndian.PutUint64(data[9:17], uint64(advertised))
	return &pgproto3.CopyData{Data: append(data, payload...)}
}

func recoveryWireKeepalive(processed pglogrepl.LSN, reply bool) *pgproto3.CopyData {
	data := make([]byte, 18)
	data[0] = pglogrepl.PrimaryKeepaliveMessageByteID
	binary.BigEndian.PutUint64(data[1:9], uint64(processed))
	if reply {
		data[17] = 1
	}
	return &pgproto3.CopyData{Data: data}
}

func recoveryWireReplicator(plugin string, socket *Socket) Replicator {
	decoder, _ := strictRecoveryDecoder()
	socket.changeFilter = decoder.socket.changeFilter
	socket.changeFilter.replayThrough = &socket.CurrentWalPosition
	socket.changeFilter.replayFrom = socket.RecoveryStartLSN
	if plugin == "pgoutput" {
		decoder.socket = socket
		decoder.publication = "publication"
		return decoder
	}
	return &wal2jsonReplicator{socket: socket}
}

func TestOrdinaryRecoverySparseKeepaliveCutsOnlyProcessedBoundaryAndKeepsRealAck(t *testing.T) {
	for _, plugin := range []string{"pgoutput", "wal2json"} {
		t.Run(plugin, func(t *testing.T) {
			conn, done := recoveryWireConnection(t, "0/300", func(backend *pgproto3.Backend) error {
				backend.Send(recoveryWireKeepalive(0x400, true))
				if err := backend.Flush(); err != nil {
					return err
				}
				message, err := backend.Receive()
				if err != nil {
					return err
				}
				ack, ok := message.(*pgproto3.CopyData)
				if !ok || len(ack.Data) != 34 || ack.Data[0] != 'r' {
					return fmt.Errorf("expected keepalive status, got %#v", message)
				}
				for _, offset := range []int{1, 9, 17} {
					if got := binary.BigEndian.Uint64(ack.Data[offset : offset+8]); got != 0x100 {
						return fmt.Errorf("keepalive acknowledged unconfirmed checkpoint: %X", got)
					}
				}
				return nil
			})
			socket := &Socket{pgConn: conn, StrictRecovery: true, ReplicationSlot: "slot", ConfirmedFlushLSN: 0x100,
				RecoveryStartLSN: 0x300, ClientXLogPos: 0x300, CurrentWalPosition: 0x500,
				RecoveryBatchDeadline: time.Unix(1, 0), initialWaitTime: time.Nanosecond}
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			err := recoveryWireReplicator(plugin, socket).StreamChanges(ctx, nil, func(context.Context, abstract.CDCChange) error {
				return errors.New("sparse keepalive emitted a row")
			})
			require.NoError(t, err)
			require.NoError(t, <-done)
			require.Equal(t, pglogrepl.LSN(0x400), socket.RecoveryBatchLSN)
			require.Equal(t, pglogrepl.LSN(0x500), socket.CurrentWalPosition)
			require.Equal(t, pglogrepl.LSN(0x100), socket.ConfirmedFlushLSN)
		})
	}
}

func TestOrdinaryRecoveryWireDoesNotUseAdvertisedTipBeforeCommit(t *testing.T) {
	for _, plugin := range []string{"pgoutput", "wal2json"} {
		t.Run(plugin, func(t *testing.T) {
			conn, done := recoveryWireConnection(t, "0/100", func(backend *pgproto3.Backend) error {
				if plugin == "pgoutput" {
					backend.Send(recoveryWireWAL(0x110, 0x900, resyncBegin(0x200)))
					backend.Send(recoveryWireWAL(0x120, 0x900, strictRecoveryInsert(1)))
					backend.Send(recoveryWireKeepalive(0x900, false))
					backend.Send(recoveryWireWAL(0x200, 0x900, resyncCommit(0x200, 0x210)))
				} else {
					backend.Send(recoveryWireWAL(0x110, 0x900, []byte(`{"nextlsn":"0/210","change":[{"kind":"insert","schema":"public","table":"first","columnnames":["id"],"columntypes":["integer"],"columnvalues":[1]}]}`)))
				}
				return backend.Flush()
			})
			socket := &Socket{pgConn: conn, StrictRecovery: true, ReplicationSlot: "slot", ConfirmedFlushLSN: 0x100,
				RecoveryStartLSN: 0x100, ClientXLogPos: 0x100, CurrentWalPosition: 0x500, RecoveryBatchDeadline: time.Unix(1, 0)}
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			var emitted int
			err := recoveryWireReplicator(plugin, socket).StreamChanges(ctx, nil, func(context.Context, abstract.CDCChange) error { emitted++; return nil })
			require.NoError(t, err)
			require.NoError(t, <-done)
			require.Equal(t, 1, emitted)
			require.False(t, socket.TransactionOpen)
			require.Equal(t, pglogrepl.LSN(0x210), socket.RecoveryBatchLSN)
			require.Equal(t, pglogrepl.LSN(0x500), socket.CurrentWalPosition)
		})
	}
}

func TestOrdinaryRecoveryWireCancellationAndEOFNeverCompleteBatch(t *testing.T) {
	for _, plugin := range []string{"pgoutput", "wal2json"} {
		for _, scenario := range []string{"eof", "cancellation", "open_transaction_eof"} {
			t.Run(plugin+"/"+scenario, func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				conn, done := recoveryWireConnection(t, "0/100", func(backend *pgproto3.Backend) error {
					if scenario == "cancellation" {
						cancel()
					}
					if scenario == "open_transaction_eof" && plugin == "pgoutput" {
						backend.Send(recoveryWireWAL(0x110, 0x900, resyncBegin(0x200)))
						backend.Send(recoveryWireWAL(0x120, 0x900, strictRecoveryInsert(1)))
						return backend.Flush()
					}
					return nil
				})
				socket := &Socket{pgConn: conn, StrictRecovery: true, ReplicationSlot: "slot", ConfirmedFlushLSN: 0x100,
					RecoveryStartLSN: 0x100, ClientXLogPos: 0x100, CurrentWalPosition: 0x500, RecoveryBatchDeadline: time.Unix(1, 0)}
				err := recoveryWireReplicator(plugin, socket).StreamChanges(ctx, nil, func(context.Context, abstract.CDCChange) error {
					t.Fatal("an incomplete transaction emitted a row")
					return nil
				})
				require.Error(t, err)
				require.NoError(t, <-done)
				require.Zero(t, socket.RecoveryBatchLSN)
				require.Equal(t, pglogrepl.LSN(0x100), socket.ClientXLogPos)
			})
		}
	}
}

func TestOrdinaryRecoveryWal2JSONFutureTransactionCompletesFixedTargetWithoutEmitting(t *testing.T) {
	for _, priorTransaction := range []bool{false, true} {
		t.Run(fmt.Sprintf("prior_transaction=%t", priorTransaction), func(t *testing.T) {
			const committedTime = "2026-11-25 10:00:00+00"
			const futureTime = "2026-11-25 11:00:00+00"
			conn, done := recoveryWireConnection(t, "0/100", func(backend *pgproto3.Backend) error {
				if priorTransaction {
					backend.Send(recoveryWireWAL(0x110, 0x900, []byte(fmt.Sprintf(`{"nextlsn":"0/210","timestamp":%q,"change":[{"kind":"insert","schema":"public","table":"first","columnnames":["id"],"columntypes":["integer"],"columnvalues":[1]}]}`, committedTime))))
				}
				backend.Send(recoveryWireWAL(0x510, 0x900, []byte(fmt.Sprintf(`{"nextlsn":"0/610","timestamp":%q,"change":[{"kind":"insert","schema":"public","table":"first","columnnames":["id"],"columntypes":["integer"],"columnvalues":[2]}]}`, futureTime))))
				return backend.Flush()
			})
			socket := &Socket{pgConn: conn, StrictRecovery: true, ReplicationSlot: "slot", ConfirmedFlushLSN: 0x100,
				RecoveryStartLSN: 0x100, ClientXLogPos: 0x100, CurrentWalPosition: 0x500, RecoveryBatchDeadline: time.Now().Add(time.Hour)}
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			var emitted []abstract.CDCChange
			err := recoveryWireReplicator("wal2json", socket).StreamChanges(ctx, nil, func(_ context.Context, change abstract.CDCChange) error {
				emitted = append(emitted, change)
				return nil
			})
			require.NoError(t, err)
			require.NoError(t, <-done)
			require.Equal(t, pglogrepl.LSN(0x500), socket.RecoveryBatchLSN)
			require.Equal(t, pglogrepl.LSN(0x500), socket.ClientXLogPos)
			require.Equal(t, pglogrepl.LSN(0x100), socket.ConfirmedFlushLSN)
			if priorTransaction {
				require.Len(t, emitted, 1)
				require.EqualValues(t, 1, emitted[0].Data["id"])
				require.True(t, socket.RecoverySourceTime.Equal(time.Date(2026, 11, 25, 10, 0, 0, 0, time.UTC)))
				require.Equal(t, pglogrepl.LSN(0x210), socket.changeFilter.replayFrom)
			} else {
				require.Empty(t, emitted)
				require.True(t, socket.RecoverySourceTime.IsZero())
				require.Equal(t, pglogrepl.LSN(0x100), socket.changeFilter.replayFrom)
			}
		})
	}
}

func TestOrdinaryRecoveryWal2JSONStillRejectsMalformedAndRegressingTransactions(t *testing.T) {
	target := pglogrepl.LSN(0x500)
	for _, next := range []string{"", "not-an-lsn", "0/0", "0/100", "0/200"} {
		t.Run(next, func(t *testing.T) {
			decoder, _ := strictRecoveryDecoder()
			filter := decoder.socket.changeFilter
			filter.replayThrough = &target
			filter.replayFrom = 0x200
			_, _, err := filter.FilterWalJsChange(context.Background(), []byte(fmt.Sprintf(`{"nextlsn":%q,"change":[]}`, next)), func(context.Context, abstract.CDCChange) error {
				t.Fatal("invalid transaction emitted a row")
				return nil
			})
			require.Error(t, err)
			require.NotErrorIs(t, err, errOrdinaryRecoveryTargetReached)
		})
	}
}
