package waljs

import (
	"context"
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/datazip-inc/olake/drivers/abstract"
	"github.com/datazip-inc/olake/types"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/require"
)

func resyncBegin(finalLSN pglogrepl.LSN) []byte {
	message := make([]byte, 21)
	message[0] = 'B'
	binary.BigEndian.PutUint64(message[1:9], uint64(finalLSN))
	binary.BigEndian.PutUint32(message[17:21], 42)
	return message
}

func resyncCommit(commitLSN, endLSN pglogrepl.LSN) []byte {
	message := make([]byte, 26)
	message[0] = 'C'
	binary.BigEndian.PutUint64(message[2:10], uint64(commitLSN))
	binary.BigEndian.PutUint64(message[10:18], uint64(endLSN))
	return message
}

func TestPGOutputResyncUsesCommitBoundaryForEveryChangeKind(t *testing.T) {
	for _, kind := range []string{"insert", "update", "delete"} {
		t.Run(kind, func(t *testing.T) {
			target := types.NewStream("target", "public", nil).Wrap(0)
			other := types.NewStream("other", "public", nil).Wrap(0)
			filter := NewChangeFilter(func(value any, _ string) (any, error) { return value, nil }, nil, target, other)
			filter.replayAfter = map[string]pglogrepl.LSN{target.ID(): 0x800, other.ID(): 0x500}
			p := &pgoutputReplicator{socket: &Socket{changeFilter: filter}, relationIDToMsgMap: map[uint32]*pglogrepl.RelationMessage{
				1: {Namespace: "public", RelationName: "target", Columns: []*pglogrepl.RelationMessageColumn{{Name: "id", DataType: pgtype.Int4OID}}},
				2: {Namespace: "public", RelationName: "other", Columns: []*pglogrepl.RelationMessageColumn{{Name: "id", DataType: pgtype.Int4OID}}},
			}}
			var changes []abstract.CDCChange
			callback := func(_ context.Context, change abstract.CDCChange) error {
				changes = append(changes, change)
				return nil
			}
			tuple := &pglogrepl.TupleData{Columns: []*pglogrepl.TupleDataColumn{{DataType: pglogrepl.TupleDataTypeText, Data: []byte("1")}}}
			emit := func(relation uint32) error {
				switch kind {
				case "insert":
					return p.emitInsert(context.Background(), &pglogrepl.InsertMessage{RelationID: relation, Tuple: tuple}, callback)
				case "update":
					return p.emitUpdate(context.Background(), &pglogrepl.UpdateMessage{RelationID: relation, NewTuple: tuple}, callback)
				default:
					return p.emitDelete(context.Background(), &pglogrepl.DeleteMessage{RelationID: relation, OldTuple: tuple}, callback)
				}
			}

			// This transaction is already visible in the target's fresh snapshot, but
			// is new to the unrelated stream. Old target updates/deletes must not replay.
			require.NoError(t, p.processPgoutputWAL(context.Background(), resyncBegin(0x600), callback))
			require.True(t, p.socket.TransactionOpen)
			require.NoError(t, emit(1))
			require.NoError(t, emit(2))
			require.Len(t, changes, 1)
			require.Equal(t, other.ID(), changes[0].Stream.ID())
			require.NoError(t, p.processPgoutputWAL(context.Background(), resyncCommit(0x600, 0x610), callback))
			require.False(t, p.socket.TransactionOpen)
			require.Equal(t, pglogrepl.LSN(0x610), p.socket.ClientXLogPos)

			// A long-running transaction's rows can have WALStart before the snapshot,
			// yet commit after it: those rows must be applied to the fresh target.
			require.NoError(t, p.processPgoutputWAL(context.Background(), resyncBegin(0x900), callback))
			p.socket.ClientXLogPos = 0x100
			require.NoError(t, emit(1))
			require.Len(t, changes, 2)
			require.Equal(t, target.ID(), changes[1].Stream.ID())
			require.Equal(t, kind, changes[1].Kind)
			require.NoError(t, p.processPgoutputWAL(context.Background(), resyncCommit(0x900, 0x910), callback))
			require.False(t, p.socket.TransactionOpen)
			require.Equal(t, pglogrepl.LSN(0x910), p.socket.ClientXLogPos, "checkpoint must use complete COMMIT end, not row WALStart")
		})
	}
}

func TestPGOutputResyncSkipsHistoricalSchema(t *testing.T) {
	target := types.NewStream("target", "public", nil).Wrap(0)
	filter := NewChangeFilter(nil, nil, target)
	filter.replayAfter = map[string]pglogrepl.LSN{target.ID(): 0x800}
	p := &pgoutputReplicator{socket: &Socket{changeFilter: filter}, txnFinalLSN: 0x600}
	// No catalog connection is necessary for a historical relation that must not
	// replace the current snapshot's schema.
	require.NoError(t, p.emitRelationColumns(context.Background(), &pglogrepl.RelationMessage{Namespace: "public", RelationName: "target"}, func(context.Context, abstract.CDCChange) error {
		t.Fatal("historical target schema was emitted")
		return nil
	}))
}

func TestWal2JSONResyncPreservesUnrelatedReplay(t *testing.T) {
	target := types.NewStream("target", "public", nil).Wrap(0)
	other := types.NewStream("other", "public", nil).Wrap(0)
	filter := NewChangeFilter(func(value any, _ string) (any, error) { return value, nil }, nil, target, other)
	filter.replayAfter = map[string]pglogrepl.LSN{target.ID(): 0x800, other.ID(): 0x500}
	for _, next := range []string{"0/600", "0/900"} {
		message := fmt.Sprintf(`{"nextlsn":%q,"change":[{"kind":"delete","schema":"public","table":"target","oldkeys":{"keynames":["id"],"keytypes":["integer"],"keyvalues":[1]}},{"kind":"insert","schema":"public","table":"other","columnnames":["id"],"columntypes":["integer"],"columnvalues":[2]}]}`, next)
		var streams []string
		_, _, err := filter.FilterWalJsChange(context.Background(), []byte(message), func(_ context.Context, change abstract.CDCChange) error {
			streams = append(streams, change.Stream.ID())
			return nil
		})
		require.NoError(t, err)
		if next == "0/600" {
			require.Equal(t, []string{other.ID()}, streams)
		} else {
			require.Equal(t, []string{target.ID(), other.ID()}, streams)
		}
	}
}
