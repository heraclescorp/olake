package waljs

import (
	"context"
	"errors"
	"testing"

	"github.com/datazip-inc/olake/drivers/abstract"
	"github.com/datazip-inc/olake/types"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/require"
)

func TestCopyOnWriteCommitControlIsOptInAndAfterCompleteBoundary(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		t.Run(map[bool]string{false: "legacy", true: "cow"}[enabled], func(t *testing.T) {
			p := &pgoutputReplicator{socket: &Socket{CommitAligned: enabled, EmitCommitEvents: enabled, ClientXLogPos: 0x100}}
			calls := 0
			callback := func(_ context.Context, change abstract.CDCChange) error {
				calls++
				require.Nil(t, change.Stream)
				require.Equal(t, WALState{LSN: "0/210"}, change.CommitState)
				require.False(t, p.socket.TransactionOpen)
				require.Equal(t, pglogrepl.LSN(0x210), p.socket.ClientXLogPos)
				return nil
			}
			require.NoError(t, p.processPgoutputWAL(context.Background(), resyncBegin(0x200), callback))
			require.Zero(t, calls, "BEGIN cannot checkpoint an incomplete transaction")
			require.NoError(t, p.processPgoutputWAL(context.Background(), resyncCommit(0x200, 0x210), callback))
			if enabled {
				require.Equal(t, 1, calls)
			} else {
				require.Zero(t, calls, "ordinary MOR must not receive controls")
			}
		})
	}
}

func TestCopyOnWriteCommitControlPropagatesBarrierFailure(t *testing.T) {
	p := &pgoutputReplicator{socket: &Socket{CommitAligned: true, EmitCommitEvents: true, ClientXLogPos: 0x100}}
	failure := errors.New("quiet table publication uncertain")
	callback := func(context.Context, abstract.CDCChange) error { return failure }
	require.NoError(t, p.processPgoutputWAL(context.Background(), resyncBegin(0x200), callback))
	require.ErrorIs(t, p.processPgoutputWAL(context.Background(), resyncCommit(0x200, 0x210), callback), failure)
	require.Equal(t, pglogrepl.LSN(0), p.socket.ConfirmedFlushLSN, "decoding is not an ACK")
}

func TestCopyOnWriteRejectsIncompleteCommitAndAdvertisedProgress(t *testing.T) {
	p := &pgoutputReplicator{socket: &Socket{CommitAligned: true, EmitCommitEvents: true, ClientXLogPos: 0x100, CurrentWalPosition: 0x500}}
	callback := func(context.Context, abstract.CDCChange) error {
		t.Fatal("invalid transaction emitted a checkpoint")
		return nil
	}
	require.Error(t, p.processPgoutputWAL(context.Background(), resyncCommit(0x200, 0x210), callback))
	require.NoError(t, p.processPgoutputWAL(context.Background(), resyncBegin(0x200), callback))
	p.socket.observeKeepalive(0x900)
	require.Equal(t, pglogrepl.LSN(0x100), p.socket.ClientXLogPos)
	require.Zero(t, p.socket.decodedThrough, "keepalive cannot complete an open transaction")
	require.Error(t, p.processPgoutputWAL(context.Background(), resyncCommit(0x300, 0x310), callback))
	p.socket.TransactionOpen = false
	p.socket.observeKeepalive(0x900)
	require.Equal(t, pglogrepl.LSN(0x900), p.socket.decodedThrough, "idle decoding may finish the fixed run target")
	require.Equal(t, pglogrepl.LSN(0x100), p.socket.ClientXLogPos, "advertised WAL end must never become persisted/ACK progress")
}

func TestCopyOnWriteRejectsChangedSourcePrimaryKey(t *testing.T) {
	stream := types.NewStream("keyed", "public", nil).WithPrimaryKey("id").Wrap(0)
	stream.StreamMetadata.MergeStrategy = types.CopyOnWrite
	appendMode := false
	stream.StreamMetadata.AppendMode = &appendMode
	p := &pgoutputReplicator{socket: &Socket{changeFilter: NewChangeFilter(func(value any, _ string) (any, error) { return value, nil }, nil, stream)}, relationIDToMsgMap: map[uint32]*pglogrepl.RelationMessage{
		1: {Namespace: "public", RelationName: "keyed", Columns: []*pglogrepl.RelationMessageColumn{{Name: "id", DataType: pgtype.Int4OID}}},
	}}
	tuple := func(value string) *pglogrepl.TupleData {
		return &pglogrepl.TupleData{Columns: []*pglogrepl.TupleDataColumn{{DataType: pglogrepl.TupleDataTypeText, Data: []byte(value)}}}
	}
	called := false
	callback := func(context.Context, abstract.CDCChange) error { called = true; return nil }
	require.ErrorContains(t, p.emitUpdate(context.Background(), &pglogrepl.UpdateMessage{RelationID: 1, OldTuple: tuple("1"), NewTuple: tuple("2")}, callback), "does not support changes")
	require.False(t, called)
	require.NoError(t, p.emitUpdate(context.Background(), &pglogrepl.UpdateMessage{RelationID: 1, OldTuple: tuple("1"), NewTuple: tuple("1")}, callback))
	require.True(t, called)
}
