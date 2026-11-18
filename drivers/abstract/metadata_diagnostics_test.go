package abstract

import (
	"context"
	"errors"
	"testing"

	"github.com/datazip-inc/olake/destination"
	"github.com/datazip-inc/olake/types"
	"github.com/stretchr/testify/require"
)

type preCDCTimeoutDriver struct {
	stubDriver
	pre func() error
}

func (d preCDCTimeoutDriver) PreCDC(context.Context, []types.StreamInterface) error { return d.pre() }

func TestMetadataDiagnosticsPrecedePreCDCTimeout(t *testing.T) {
	streams := metadataStreams(2)
	var read []string
	pool := newMetadataWriterPool(t, streams, metadataWriterHooks{
		Diagnostics: func(_ context.Context, s types.StreamInterface) { read = append(read, s.ID()) },
		Setup: func(context.Context, types.StreamInterface, *destination.Options) (*types.MetadataState, error) {
			t.Fatal("diagnostics must not open a data writer")
			return nil, nil
		},
	})
	driver := preCDCTimeoutDriver{stubDriver: stubDriver{typ: "postgres"}, pre: func() error {
		require.Equal(t, []string{streams[0].ID(), streams[1].ID()}, read)
		return context.DeadlineExceeded
	}}
	err := NewAbstractDriver(context.Background(), driver).RunChangeStream(context.Background(), pool, streams...)
	require.True(t, errors.Is(err, context.DeadlineExceeded))
}
