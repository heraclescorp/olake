package waljs

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/datazip-inc/olake/drivers/abstract"
	"github.com/datazip-inc/olake/types"
	"github.com/jackc/pglogrepl"
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

func TestOrdinaryRecoveryWal2JSONRejectsFutureBeforeAnyCallback(t *testing.T) {
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
			require.ErrorContains(t, err, "exceeds target")
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
