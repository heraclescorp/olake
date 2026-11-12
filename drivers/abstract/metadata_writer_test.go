package abstract

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/datazip-inc/olake/destination"
	"github.com/datazip-inc/olake/types"
	"github.com/stretchr/testify/require"
)

type metadataWriterHooks struct {
	Setup func(context.Context, types.StreamInterface, *destination.Options) (*types.MetadataState, error)
	Write func(context.Context, types.StreamInterface, *destination.Options, []types.RawRecord) error
	Close func(context.Context, types.StreamInterface, *destination.Options, any) error
}

var metadataWriterPoolID atomic.Uint64

func newMetadataWriterPool(t *testing.T, streams []types.StreamInterface, hooks metadataWriterHooks) *destination.WriterPool {
	t.Helper()
	writerType := types.DestinationType(fmt.Sprintf("metadata-test-%d", metadataWriterPoolID.Add(1)))
	previous, existed := destination.RegisteredWriters[writerType]
	destination.RegisteredWriters[writerType] = func(any) (destination.Writer, func(context.Context), error) {
		return &metadataTestWriter{hooks: hooks}, nil, nil
	}
	t.Cleanup(func() {
		if existed {
			destination.RegisteredWriters[writerType] = previous
		} else {
			delete(destination.RegisteredWriters, writerType)
		}
	})
	pool, err := destination.NewWriterPool(context.Background(), &types.WriterConfig{Type: writerType}, streams, 1)
	require.NoError(t, err)
	t.Cleanup(func() { pool.Shutdown(context.Background()) })
	return pool
}

type metadataTestWriter struct {
	hooks  metadataWriterHooks
	stream types.StreamInterface
	opts   *destination.Options
}

func (*metadataTestWriter) Spec() any                   { return nil }
func (*metadataTestWriter) Type() string                { return "metadata-test" }
func (*metadataTestWriter) Check(context.Context) error { return nil }

func (w *metadataTestWriter) Setup(ctx context.Context, stream types.StreamInterface, _ any, opts *destination.Options) (any, *types.MetadataState, error) {
	w.stream, w.opts = stream, opts
	if w.hooks.Setup != nil {
		metadata, err := w.hooks.Setup(ctx, stream, opts)
		return struct{}{}, metadata, err
	}
	return struct{}{}, nil, nil
}

func (w *metadataTestWriter) Write(ctx context.Context, records []types.RawRecord) error {
	if w.hooks.Write != nil {
		return w.hooks.Write(ctx, w.stream, w.opts, records)
	}
	return nil
}

func (*metadataTestWriter) FlattenAndCleanData(_ context.Context, records []types.RawRecord) (bool, []types.RawRecord, any, error) {
	return false, records, nil, nil
}

func (*metadataTestWriter) EvolveSchema(_ context.Context, schema, _ any) (any, error) {
	return schema, nil
}

func (*metadataTestWriter) ApplySourceColumns(_ context.Context, schema any, _ types.SourceColumnsChange) (any, error) {
	return schema, nil
}

func (*metadataTestWriter) DropStreams(context.Context, []types.StreamInterface) error { return nil }

func (w *metadataTestWriter) Close(ctx context.Context, metadata any) error {
	if w.hooks.Close != nil {
		return w.hooks.Close(ctx, w.stream, w.opts, metadata)
	}
	return nil
}
