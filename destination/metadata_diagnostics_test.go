package destination

import (
	"context"
	"github.com/datazip-inc/olake/types"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

type diagnosticProbe struct {
	calls  []string
	cancel context.CancelFunc
	t      *testing.T
}

func (p *diagnosticProbe) LogMetadataPosition(ctx context.Context, stream types.StreamInterface) {
	deadline, ok := ctx.Deadline()
	require.True(p.t, ok)
	require.LessOrEqual(p.t, time.Until(deadline), 5*time.Second)
	p.calls = append(p.calls, stream.ID())
	if p.cancel != nil {
		p.cancel()
	}
}
func TestMetadataDiagnosticsBoundedAndOptional(t *testing.T) {
	stream := &types.ConfiguredStream{Stream: types.NewStream("items", "public", nil)}
	var empty WriterPool
	empty.LogMetadataPositions(context.Background(), []types.StreamInterface{stream})
	probe := &diagnosticProbe{t: t}
	pool := &WriterPool{metadataDiagnostics: probe}
	pool.LogMetadataPositions(context.Background(), []types.StreamInterface{stream})
	require.Equal(t, []string{"public.items"}, probe.calls)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	probe.calls = nil
	probe.cancel = cancel
	pool.LogMetadataPositions(ctx, []types.StreamInterface{stream, stream})
	require.Len(t, probe.calls, 1)
}
