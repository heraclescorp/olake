package driver

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/datazip-inc/olake/drivers/abstract"
	"github.com/datazip-inc/olake/drivers/postgres/internal/waljs"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
	"github.com/datazip-inc/olake/utils/errs"
	"github.com/jmoiron/sqlx"
)

func TestValidateToastStreams(t *testing.T) {
	stream := func(updateType string, appendMode bool) types.StreamInterface {
		return &types.ConfiguredStream{
			Stream:         &types.Stream{Name: "users", Namespace: "public"},
			StreamMetadata: types.StreamMetadata{StreamName: "users", UpdateType: &updateType, AppendMode: &appendMode},
		}
	}
	cow := stream("pos", false).Self()
	cow.StreamMetadata.MergeStrategy = types.CopyOnWrite

	tests := []struct {
		name     string
		stream   types.StreamInterface
		wantCode string
	}{
		{name: "pos keeps the index", stream: stream("pos", false)},
		{name: "eq keeps no index", stream: stream("eq", false), wantCode: codeUpdateTypeWithoutIndex},
		{name: "append mode keeps no index", stream: stream("pos", true), wantCode: codeAppendModeWithToast},
		{name: "COW keeps no index", stream: cow, wantCode: codeUpdateTypeWithoutIndex},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := validateToastStreams([]types.StreamInterface{tc.stream})
			if tc.wantCode == "" {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			assert.Equal(t, tc.wantCode, errs.From(errs.Classify(err)).Code)
		})
	}
}

func TestSupportedUpdateTypes(t *testing.T) {
	tests := []struct {
		name         string
		updateMethod any
		expected     []types.UpdateType
	}{
		{name: "no update method has no restriction"},
		{name: "standard replication has no restriction", updateMethod: map[string]any{"type": "standard"}},
		{name: "preserve_toast_values without cdc has no restriction", updateMethod: map[string]any{"type": "standard", "preserve_toast_values": true}},
		{name: "cdc defaults to mandatory source recovery", updateMethod: map[string]any{"replication_slot": "olake_slot"}},
		{name: "legacy false cannot disable preservation", updateMethod: map[string]any{"replication_slot": "olake_slot", "preserve_toast_values": false}},
		{name: "source recovery permits equality", updateMethod: map[string]any{"replication_slot": "olake_slot", "toast_value_source": "source"}},
		{
			name:         "explicit destination recovery offers only the indexed formats",
			updateMethod: map[string]any{"replication_slot": "olake_slot", "toast_value_source": "destination"},
			expected:     []types.UpdateType{types.UpdateTypePosition, types.UpdateTypeDeletionVector},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			p := &Postgres{config: &Config{UpdateMethod: tc.updateMethod}}
			assert.Equal(t, tc.expected, p.SupportedUpdateTypes())
		})
	}
}

func TestPostCDCCanceledSyncPreservesCheckpointAndResyncOwnership(t *testing.T) {
	for _, resync := range []bool{false, true} {
		t.Run(fmt.Sprintf("resync=%t", resync), func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			if resync {
				ctx = types.WithResync(ctx, &types.ResyncOptions{ID: "resync"})
			}
			cancel()
			replicator := resyncCheckpointReplicator{socket: &waljs.Socket{ClientXLogPos: 0x300, ConfirmedFlushLSN: 0x100}}
			p := &Postgres{
				replicator: replicator,
				state:      &types.State{RWMutex: &sync.RWMutex{}, Global: &types.GlobalState{State: waljs.WALState{LSN: "0/100"}}},
			}
			require.ErrorIs(t, p.PostCDC(ctx, 0), context.Canceled)
			require.Equal(t, waljs.WALState{LSN: "0/100"}, p.state.GetGlobal().State)
			if resync {
				require.Equal(t, replicator, p.replicator, "CleanupResync retains ownership until publication or abort")
				return
			}
			require.Nil(t, p.replicator, "failed ordinary sync must release its replication connection")
		})
	}
}

// Requires an explicitly supplied disposable database with logical replication enabled.
func TestPostCDCCanceledSyncReleasesReplicationSlotIntegration(t *testing.T) {
	dsn := os.Getenv("OLAKE_POSTGRES_TEST_DSN")
	if dsn == "" {
		t.Skip("set OLAKE_POSTGRES_TEST_DSN to a disposable PostgreSQL database with wal_level=logical")
	}
	for _, resync := range []bool{false, true} {
		t.Run(fmt.Sprintf("resync=%t", resync), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			db, err := sqlx.ConnectContext(ctx, "pgx", dsn)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, db.Close()) })
			name := fmt.Sprintf("olake_cleanup_%d", time.Now().UnixNano())
			_, err = db.ExecContext(ctx, "CREATE PUBLICATION "+name)
			require.NoError(t, err)
			t.Cleanup(func() {
				cleanupCtx, stop := context.WithTimeout(context.Background(), 10*time.Second)
				defer stop()
				_, err := db.ExecContext(cleanupCtx, "DROP PUBLICATION "+name)
				require.NoError(t, err)
			})
			var startLSN string
			require.NoError(t, db.GetContext(ctx, &startLSN, "SELECT lsn::text FROM pg_create_logical_replication_slot($1, 'pgoutput')", name))
			t.Cleanup(func() {
				cleanupCtx, stop := context.WithTimeout(context.Background(), 10*time.Second)
				defer stop()
				_, err := db.ExecContext(cleanupCtx, "SELECT pg_drop_replication_slot($1)", name)
				require.NoError(t, err)
			})
			slot, err := waljs.GetSlotPosition(ctx, db, name)
			require.NoError(t, err)
			connection, err := url.Parse(dsn)
			require.NoError(t, err)
			config := &waljs.Config{Connection: *connection, ReplicationSlotName: name, Publication: name, InitialWaitTime: time.Second, Tables: types.NewSet[types.StreamInterface]()}
			p := &Postgres{client: db, state: &types.State{RWMutex: &sync.RWMutex{}, SuppressPersistence: true, Global: &types.GlobalState{State: waljs.WALState{LSN: startLSN}}}}
			startReplication := func() waljs.Replicator {
				replicator, err := waljs.NewReplicator(ctx, config, slot, &slot.LSN, p.dataTypeConverter, p.columnType)
				require.NoError(t, err)
				t.Cleanup(func() { require.NoError(t, waljs.Cleanup(context.Background(), replicator.Socket())) })
				// Start a real WAL sender, then stop reading at the already-confirmed boundary.
				// The connection must stay active until PostCDC or CleanupResync releases it.
				require.NoError(t, replicator.StreamChanges(ctx, db, func(context.Context, abstract.CDCChange) error { return nil }))
				var active bool
				require.NoError(t, db.GetContext(ctx, &active, "SELECT active FROM pg_replication_slots WHERE slot_name = $1", name))
				require.True(t, active)
				return replicator
			}
			p.replicator = startReplication()
			p.replicator.Socket().ClientXLogPos = slot.LSN + 16
			syncCtx, cancelSync := context.WithCancel(ctx)
			if resync {
				syncCtx = types.WithResync(syncCtx, &types.ResyncOptions{ID: name})
			} else {
				// Controlled runs must still release their source after writer Close cancels CDC.
				syncCtx = utils.WithExecution(syncCtx, &utils.ExecutionOptions{ID: name, Fence: "unused-after-cancellation"})
			}
			cancelSync()
			require.ErrorIs(t, p.PostCDC(syncCtx, 0), context.Canceled)
			require.Equal(t, waljs.WALState{LSN: startLSN}, p.state.GetGlobal().State)
			if resync {
				require.NotNil(t, p.replicator, "resync owns its WAL sender until explicit cleanup")
				require.NoError(t, p.CleanupResync(syncCtx))
			}
			require.Nil(t, p.replicator)
			waitInactive := func() {
				require.Eventually(t, func() bool {
					var active bool
					return db.GetContext(ctx, &active, "SELECT active FROM pg_replication_slots WHERE slot_name = $1", name) == nil && !active
				}, 5*time.Second, 10*time.Millisecond, "canceled sync left the replication slot active")
			}
			waitInactive()
			after, err := waljs.GetSlotPosition(ctx, db, name)
			require.NoError(t, err)
			require.Equal(t, slot.LSN, after.LSN, "failed writer must not acknowledge or advance the slot")
			// Starting the same slot again reproduces the retry that previously hit slot-active.
			retry := startReplication()
			require.NoError(t, waljs.Cleanup(ctx, retry.Socket()))
			waitInactive()
		})
	}
}
