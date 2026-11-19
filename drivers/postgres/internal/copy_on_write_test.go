package driver

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/drivers/abstract"
	"github.com/datazip-inc/olake/drivers/postgres/internal/waljs"
	"github.com/datazip-inc/olake/types"
	s3util "github.com/datazip-inc/olake/utils/s3"
	"github.com/jackc/pglogrepl"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
)

func TestCopyOnWriteCommitWaitsForSourceToastHydration(t *testing.T) {
	p, mock := newMockPostgres(t)
	p.typeMapping = pgTypeToDataTypes()
	mock.ExpectQuery(`SELECT "id", "doc" FROM "public"."users" WHERE ("id" = $1)`).WithArgs(int64(1)).WillReturnRows(sqlmock.NewRowsWithColumnDefinition(sqlmock.NewColumn("id").OfType("INT8", int64(0)), sqlmock.NewColumn("doc").OfType("TEXT", "")).AddRow(int64(1), "hydrated"))
	var got []abstract.CDCChange
	batch := &sourceToastBatch{postgres: p, callback: func(_ context.Context, change abstract.CDCChange) error {
		got = append(got, change)
		return nil
	}}
	require.NoError(t, batch.push(context.Background(), toastChange(1)))
	require.Empty(t, got)
	require.NoError(t, batch.push(context.Background(), abstract.CDCChange{CommitState: waljs.WALState{LSN: "0/210"}}))
	require.Len(t, got, 2)
	require.Equal(t, "hydrated", got[0].Data["doc"])
	require.Empty(t, got[0].UnavailableColumns)
	require.Nil(t, got[1].Stream)
	require.Equal(t, waljs.WALState{LSN: "0/210"}, got[1].CommitState)
}

func TestCopyOnWriteDurableCheckpointPreservesCompleteGlobalState(t *testing.T) {
	previousPath, previousBucket := viper.GetString(constants.StatePath), s3util.JobBucket
	t.Cleanup(func() { viper.Set(constants.StatePath, previousPath); s3util.JobBucket = previousBucket })
	s3util.JobBucket = ""
	name := filepath.Join(t.TempDir(), "state.json")
	viper.Set(constants.StatePath, name)
	stream := types.NewStream("users", "public", nil).Wrap(0)
	other := &types.StreamState{Stream: "other", Namespace: "archive"}
	other.HoldsValue.Store(true)
	other.State.Store("large_cursor", int64(9007199254740993))
	state := &types.State{RWMutex: &sync.RWMutex{}, Type: types.GlobalType, Version: 8,
		Global:  &types.GlobalState{State: waljs.WALState{LSN: "0/100"}, Streams: types.NewSet(stream.ID(), "public.quiet")},
		Streams: []*types.StreamState{other},
	}
	p := &Postgres{state: state, streams: []types.StreamInterface{stream}}
	require.NoError(t, p.persistCopyOnWriteCheckpoint(context.Background(), waljs.WALState{LSN: "0/210"}))
	data, err := os.ReadFile(name)
	require.NoError(t, err)
	require.Contains(t, string(data), `"lsn":"0/210"`)
	require.Contains(t, string(data), `"large_cursor":9007199254740993`)
	require.Contains(t, string(data), `public.quiet`)
	require.Equal(t, waljs.WALState{LSN: "0/210"}, state.Global.State)
	info, err := os.Stat(name)
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0600), info.Mode().Perm())
}

func TestCopyOnWriteCheckpointWriteFailureCannotAdvanceInMemoryState(t *testing.T) {
	previous := viper.GetString(constants.StatePath)
	t.Cleanup(func() { viper.Set(constants.StatePath, previous) })
	viper.Set(constants.StatePath, filepath.Join(t.TempDir(), "missing", "state.json"))
	p := &Postgres{state: &types.State{RWMutex: &sync.RWMutex{}, Type: types.GlobalType,
		Global: &types.GlobalState{State: waljs.WALState{LSN: "0/100"}, Streams: types.NewSet[string]()},
	}}
	require.Error(t, p.persistCopyOnWriteCheckpoint(context.Background(), waljs.WALState{LSN: "0/210"}))
	require.Equal(t, waljs.WALState{LSN: "0/100"}, p.state.Global.State)
}

func TestCopyOnWriteSourceStateRetainsOpaqueLargeFields(t *testing.T) {
	previous := map[string]any{"lsn": "0/100", "opaque": json.Number("9007199254740993"), "nested": map[string]any{"maximum": json.Number("18446744073709551615")}}
	updated, err := copyOnWriteSourceState(previous, waljs.WALState{LSN: "0/210"})
	require.NoError(t, err)
	data, err := json.Marshal(updated)
	require.NoError(t, err)
	require.Contains(t, string(data), `"lsn":"0/210"`)
	require.Contains(t, string(data), `"opaque":9007199254740993`)
	require.Contains(t, string(data), `"maximum":18446744073709551615`)
	require.Equal(t, "0/100", previous["lsn"], "candidate assembly must not mutate the old checkpoint")
}

func TestCopyOnWriteTablePositionsCoverInitialBackfillAndSavedState(t *testing.T) {
	for _, scenario := range []struct {
		name  string
		value any
		want  pglogrepl.LSN
		valid bool
	}{
		{"no metadata", nil, 0x100, true},
		{"completed backfill without CDC", "", 0x100, true},
		{"common checkpoint", `{"lsn":"0/100"}`, 0x100, true},
		{"table ahead", `{"lsn":"0/210"}`, 0x210, true},
		{"table behind", `{"lsn":"0/FF"}`, 0, false},
		{"missing LSN", `{}`, 0, false},
		{"malformed state", `{`, 0, false},
		{"unsupported encoding", 123, 0, false},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			position, err := copyOnWriteTablePosition(scenario.value, 0x100)
			if !scenario.valid {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, scenario.want, position)
		})
	}
}
