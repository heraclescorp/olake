package driver

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/datazip-inc/olake/drivers/postgres/internal/waljs"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
	"github.com/stretchr/testify/require"
)

func TestSourceFenceRejectsStartupPreparationAndAcknowledgement(t *testing.T) {
	for _, fence := range []string{"missing", `{"version":1,"execution_id":"runtime","state":"closed"}`, `{"version":1,"execution_id":"other","state":"open"}`} {
		t.Run(fence, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "fence.json")
			if fence != "missing" {
				require.NoError(t, os.WriteFile(path, []byte(fence), 0600))
			}
			ctx := utils.WithExecution(context.Background(), &utils.ExecutionOptions{ID: "runtime", Fence: path})
			p := &Postgres{CDCSupport: true, replicator: resyncCheckpointReplicator{socket: &waljs.Socket{ClientXLogPos: 0x300}}}
			target := types.NewStream("items", "public", nil).Wrap(0)
			require.Error(t, p.Setup(ctx))
			require.Error(t, p.PrepareResync(ctx, target))
			require.Nil(t, p.resync)
			require.Error(t, p.PostCDC(ctx, 0))
			checkpoint := &types.State{Global: &types.GlobalState{State: waljs.WALState{LSN: "0/300"}}}
			require.Error(t, p.RecoverResync(ctx, checkpoint, target))
		})
	}
}

func TestRecoveryOnlyCannotExportSnapshot(t *testing.T) {
	ctx := types.WithResync(context.Background(), &types.ResyncOptions{RecoverOnly: true})
	for _, mode := range []types.SyncMode{types.CDC, types.FULLREFRESH} {
		target := types.NewStream("items", "public", nil).Wrap(0)
		target.Stream.SyncMode = mode
		p := &Postgres{CDCSupport: true}
		require.ErrorContains(t, p.PrepareResync(ctx, target), "recovery-only")
		require.Nil(t, p.resync)
	}
}

func TestRecoverResyncWithoutSelectedCDCDoesNotAcknowledge(t *testing.T) {
	p := &Postgres{CDCSupport: true}
	ctx := types.WithResync(context.Background(), &types.ResyncOptions{RecoverOnly: true})
	for _, checkpoint := range []*types.State{nil, {}, {Global: &types.GlobalState{}}} {
		require.NoError(t, p.RecoverResync(ctx, checkpoint, nil))
	}
}
