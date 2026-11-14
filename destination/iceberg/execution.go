package iceberg

import (
	"context"
	"errors"
	"sync"

	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/catalog/rest"
	"github.com/apache/iceberg-go/table"
	"github.com/datazip-inc/olake/utils"
)

var errNativeExecutionRetired = errors.New("native Iceberg execution is retiring")

// A catalog lifetime owns every admitted native operation, including REST table
// transactions which retain the SDK's catalog. Retirement is read-only: CLOSED
// fences prohibit writes, but must not prevent draining and terminal evidence.
type nativeExecution struct {
	mu          sync.Mutex
	cond        *sync.Cond
	context     context.Context
	identity    utils.ExecutionOptions
	active      int
	retiring    bool
	unknown     bool
	recoverOnly bool
	retire      func(bool)
}

type nativeExecutionWorkKey struct{}

type nativeExecutionWork struct {
	execution *nativeExecution
	active    bool // guarded by execution.mu
}

func (e *nativeExecution) ownsWorkLocked(ctx context.Context) bool {
	work, ok := ctx.Value(nativeExecutionWorkKey{}).(*nativeExecutionWork)
	return ok && work.execution == e && work.active
}

func newNativeExecution(ctx context.Context, config *Config) *nativeExecution {
	if execution, ok := utils.ExecutionFromContext(ctx); ok {
		config.executionID, config.executionFence, config.executionCommand = execution.ID, execution.Fence, execution.Command
	} else if config.executionID != "" || config.executionFence != "" {
		ctx = utils.WithExecution(ctx, &utils.ExecutionOptions{ID: config.executionID, Fence: config.executionFence, Command: config.executionCommand})
	} else {
		return nil
	}
	config.executionContext = ctx
	e := &nativeExecution{context: ctx, recoverOnly: config.resyncRecoverOnly, retire: utils.BeginExecutionParticipant(ctx)}
	e.identity = utils.ExecutionOptions{ID: config.executionID, Fence: config.executionFence, Command: config.executionCommand}
	e.cond = sync.NewCond(&e.mu)
	return e
}

func (e *nativeExecution) ctx(ctx context.Context) context.Context {
	if e == nil {
		return ctx
	}
	// The runtime identity is fixed at startup, not supplied by a later caller.
	if current, ok := utils.ExecutionFromContext(ctx); ok && current == &e.identity {
		return ctx
	}
	return utils.WithExecution(ctx, &e.identity)
}

func (e *nativeExecution) writable(ctx context.Context) error {
	ctx = e.ctx(ctx)
	fenceErr := utils.CheckExecutionFence(ctx)
	if e == nil {
		return fenceErr
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	// A closed fence or later positive rejection cannot downgrade an earlier
	// dispatched mutation whose outcome is still unknown.
	if e.unknown {
		return rest.ErrCommitStateUnknown
	}
	if fenceErr != nil {
		return fenceErr
	}
	if err := e.context.Err(); err != nil {
		return err
	}
	if e.recoverOnly {
		return errors.New("recover-only native Iceberg execution cannot mutate tables")
	}
	if e.retiring && !e.ownsWorkLocked(ctx) {
		return errNativeExecutionRetired
	}
	return nil
}

func (e *nativeExecution) begin(ctx context.Context) (context.Context, func(), error) {
	ctx = e.ctx(ctx)
	if err := e.writable(ctx); err != nil {
		return ctx, nil, err
	}
	return e.admit(ctx, true)
}

func (e *nativeExecution) beginRead(ctx context.Context) (context.Context, func(), error) {
	ctx = e.ctx(ctx)
	if err := ctx.Err(); err != nil {
		return ctx, nil, err
	}
	return e.admit(ctx, false)
}

func (e *nativeExecution) admit(ctx context.Context, mutation bool) (context.Context, func(), error) {
	if e == nil {
		return ctx, func() {}, nil
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if mutation && e.unknown {
		return ctx, nil, rest.ErrCommitStateUnknown
	}
	if e.retiring && !e.ownsWorkLocked(ctx) {
		return ctx, nil, errNativeExecutionRetired
	}
	e.active++
	work := &nativeExecutionWork{execution: e, active: true}
	var once sync.Once
	finish := func() {
		once.Do(func() {
			e.mu.Lock()
			work.active = false
			e.active--
			e.cond.Broadcast()
			e.mu.Unlock()
		})
	}
	return context.WithValue(ctx, nativeExecutionWorkKey{}, work), finish, nil
}

func (e *nativeExecution) markUnknown() {
	if e != nil {
		e.mu.Lock()
		e.unknown = true
		e.mu.Unlock()
	}
}

// drain prevents new admissions before waiting. It deliberately does not consult
// the execution fence or a canceled command context.
func (e *nativeExecution) drain() {
	if e == nil {
		return
	}
	e.mu.Lock()
	e.retiring = true
	for e.active != 0 {
		e.cond.Wait()
	}
	e.mu.Unlock()
}

func (e *nativeExecution) retired(closeErr error) {
	if e == nil {
		return
	}
	e.mu.Lock()
	settled := !e.unknown && closeErr == nil && e.active == 0 && e.retiring
	e.mu.Unlock()
	e.retire(settled)
}

func nativeExecutionWritable(ctx context.Context, config *Config) error {
	if config == nil {
		return utils.CheckExecutionFence(ctx)
	}
	if config.resyncRecoverOnly {
		return errors.New("recover-only native Iceberg execution cannot mutate tables")
	}
	return config.execution.writable(ctx)
}

func beginNativeWriterWork(ctx context.Context, config *Config) (context.Context, func(), error) {
	if config == nil {
		if err := utils.CheckExecutionFence(ctx); err != nil {
			return ctx, nil, err
		}
		return ctx, func() {}, nil
	}
	if config.resyncRecoverOnly {
		return ctx, nil, errors.New("recover-only native Iceberg execution cannot mutate tables")
	}
	return config.execution.begin(ctx)
}

func nativeExecutionUnknown(config *Config) {
	if config != nil {
		config.execution.markUnknown()
	}
}

// Only positive rejection is proof of no publication. A read, successful retry,
// or later successful operation must never clear an earlier ambiguous mutation.
func nativeMutationOutcomeKnown(err error) bool {
	if errors.Is(err, rest.ErrCommitStateUnknown) {
		return false
	}
	var rejected *nativeMutationNotDispatched
	var throttled *nativeRESTThrottleRejection
	if errors.As(err, &rejected) || errors.As(err, &throttled) {
		return true
	}
	return err == nil || errors.Is(err, table.ErrCommitFailed) || errors.Is(err, rest.ErrCommitFailed) ||
		errors.Is(err, catalog.ErrNoSuchTable) || errors.Is(err, catalog.ErrNoSuchNamespace) ||
		errors.Is(err, catalog.ErrTableAlreadyExists) || errors.Is(err, catalog.ErrNamespaceAlreadyExists) ||
		errors.Is(err, rest.ErrBadRequest) || errors.Is(err, rest.ErrUnauthorized) ||
		errors.Is(err, rest.ErrForbidden) || errors.Is(err, rest.ErrAuthorizationExpired)
}

func nativeExecutionMutationError(config *Config, err error) {
	if !nativeMutationOutcomeKnown(err) {
		nativeExecutionUnknown(config)
	}
}

func (c *nativeCatalog) mutationResult(err error) {
	if !nativeMutationOutcomeKnown(err) {
		c.execution.markUnknown()
	}
}

// The transport uses this only when it refused dispatch, never for a failed
// network attempt. It keeps a late fence closure from inventing ambiguity.
type nativeMutationNotDispatched struct{ cause error }

func (e *nativeMutationNotDispatched) Error() string { return e.cause.Error() }
func (e *nativeMutationNotDispatched) Unwrap() error { return e.cause }

// The SDK exposes 429 only as ErrRESTError, without a public status accessor.
// Preserve the wire's positive rejection rather than inferring it from a body,
// generic REST error, or a later request. A 429 refused this mutation; failures
// preparing a replay or closing its response cannot make it publish retroactively.
type nativeRESTThrottleRejection struct{ cause error }

func (e *nativeRESTThrottleRejection) Error() string {
	if errors.Is(e.cause, rest.ErrCommitStateUnknown) {
		return "catalog mutation outcome unknown despite a later HTTP 429 rejection"
	}
	return "catalog mutation rejected (HTTP 429); throttle retry stopped"
}
func (e *nativeRESTThrottleRejection) HTTPStatusCode() int  { return 429 }
func (e *nativeRESTThrottleRejection) Is(target error) bool { return target == rest.ErrRESTError }
func (e *nativeRESTThrottleRejection) Unwrap() error        { return e.cause }
