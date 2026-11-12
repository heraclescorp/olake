package types

import "context"

// ResyncOptions identifies one atomic replacement within the full saved catalog.
type ResyncOptions struct {
	Namespace   string
	Table       string
	ID          string
	LaunchID    string
	RecoverOnly bool
}

type resyncContextKey struct{}

func WithResync(ctx context.Context, options *ResyncOptions) context.Context {
	return context.WithValue(ctx, resyncContextKey{}, options)
}

func ResyncFromContext(ctx context.Context) (*ResyncOptions, bool) {
	options, ok := ctx.Value(resyncContextKey{}).(*ResyncOptions)
	return options, ok && options != nil
}
