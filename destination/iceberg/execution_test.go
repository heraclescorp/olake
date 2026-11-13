package iceberg

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	apacheiceberg "github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/catalog/rest"
	"github.com/apache/iceberg-go/table"
	"github.com/apache/thrift/lib/go/thrift"
	"github.com/datazip-inc/olake/utils"
	"github.com/stretchr/testify/require"
)

func nativeExecutionTestContext(t *testing.T) (context.Context, string) {
	t.Helper()
	fence := filepath.Join(t.TempDir(), "fence.json")
	require.NoError(t, os.WriteFile(fence, []byte(`{"version":1,"execution_id":"runtime","state":"open"}`), 0600))
	ctx := utils.WithExecution(context.Background(), &utils.ExecutionOptions{ID: "runtime", Fence: fence})
	utils.BeginOrdinaryExecution(ctx, "sync")
	return ctx, fence
}

func nativeExecutionFixture(t *testing.T) (context.Context, *Config, string) {
	t.Helper()
	ctx, fence := nativeExecutionTestContext(t)
	config := &Config{}
	config.execution = newNativeExecution(ctx, config)
	return ctx, config, fence
}

func closeNativeTestFence(t *testing.T, fence string) {
	t.Helper()
	require.NoError(t, os.WriteFile(fence, []byte(`{"version":1,"execution_id":"runtime","state":"closed"}`), 0600))
}

type nativeExecutionTestCatalog struct {
	catalog.Catalog
	mutations int
	failure   error
}

func (c *nativeExecutionTestCatalog) CreateNamespace(context.Context, table.Identifier, apacheiceberg.Properties) error {
	c.mutations++
	return c.failure
}

func (c *nativeExecutionTestCatalog) CheckNamespaceExists(context.Context, table.Identifier) (bool, error) {
	return true, nil
}

func TestNativeCatalogRequiresPositiveFenceAtMutation(t *testing.T) {
	for _, document := range []string{
		`{"version":1,"execution_id":"runtime","state":"closed"}`,
		`{"version":1,"execution_id":"different","state":"open"}`,
		`{"version":2,"execution_id":"runtime","state":"open"}`,
		`{"version":1,"execution_id":"runtime","state":"open","extra":true}`,
		`{"version":1,"execution_id":"runtime","state":"open"} {}`,
		`not-json`, "",
	} {
		t.Run(document, func(t *testing.T) {
			ctx, config, fence := nativeExecutionFixture(t)
			require.NoError(t, os.WriteFile(fence, []byte(document), 0600))
			backend := &nativeExecutionTestCatalog{}
			cat := &nativeCatalog{Catalog: backend, execution: config.execution}
			require.Error(t, cat.CreateNamespace(ctx, table.Identifier{"db"}, nil))
			require.Zero(t, backend.mutations)
			require.NoError(t, cat.Close())
			utils.SourceDrained(ctx, nil)
			result, ok := utils.OrdinaryExecutionResult(ctx)
			require.True(t, ok)
			require.Equal(t, "settled", result.Outcome)
		})
	}
}

func TestNativeFenceRemovalDeniesAlreadyAdmittedMutation(t *testing.T) {
	ctx, config, fence := nativeExecutionFixture(t)
	work, finish, err := beginNativeWriterWork(ctx, config)
	require.NoError(t, err)
	require.NoError(t, os.Remove(fence))
	calls := 0
	transport := newNativeRESTTransport(nativeRoundTripFunc(func(*http.Request) (*http.Response, error) {
		calls++
		return nil, errors.New("must not dispatch")
	})).(*nativeRESTThrottleTransport)
	transport.execution = config.execution
	request, err := http.NewRequestWithContext(work, http.MethodPost, "https://catalog.example/v1/namespaces/db/tables/t", strings.NewReader(`{}`))
	require.NoError(t, err)
	_, err = transport.RoundTrip(request)
	var rejected *nativeMutationNotDispatched
	require.ErrorAs(t, err, &rejected)
	require.True(t, nativeMutationOutcomeKnown(err))
	require.Zero(t, calls)
	finish()
	cat := &nativeCatalog{Catalog: &nativeExecutionTestCatalog{}, execution: config.execution}
	require.NoError(t, cat.Close())
	utils.SourceDrained(ctx, nil)
	result, _ := utils.OrdinaryExecutionResult(ctx)
	require.Equal(t, "settled", result.Outcome)
}

func TestNativeRuntimeIdentityCannotBeReplacedByCaller(t *testing.T) {
	_, config, fence := nativeExecutionFixture(t)
	closeNativeTestFence(t, fence)
	other, _, _ := nativeExecutionFixture(t)
	for _, ctx := range []context.Context{context.Background(), other} {
		require.Error(t, nativeExecutionWritable(ctx, config))
	}
}

func TestNativeUnknownMutationIsStickyAcrossReconciliation(t *testing.T) {
	ctx, config, fence := nativeExecutionFixture(t)
	backend := &nativeExecutionTestCatalog{failure: errors.New("lost publication response")}
	cat := &nativeCatalog{Catalog: backend, execution: config.execution}
	require.Error(t, cat.CreateNamespace(ctx, table.Identifier{"db"}, nil))
	backend.failure = nil
	closeNativeTestFence(t, fence)
	exists, err := cat.CheckNamespaceExists(ctx, table.Identifier{"db"})
	require.NoError(t, err)
	require.True(t, exists)
	// Even a positively OPEN document cannot erase this catalog lifetime's
	// uncertainty. Only a separately launched recovery may settle its outcome.
	require.NoError(t, os.WriteFile(fence, []byte(`{"version":1,"execution_id":"runtime","state":"open"}`), 0600))
	require.ErrorIs(t, cat.CreateNamespace(ctx, table.Identifier{"other"}, nil), rest.ErrCommitStateUnknown)
	require.Equal(t, 1, backend.mutations)
	require.NoError(t, cat.Close())
	utils.SourceDrained(ctx, nil)
	result, _ := utils.OrdinaryExecutionResult(ctx)
	require.Equal(t, "unknown", result.Outcome)
}

func TestNativePositiveMutationRejectionCanRetireSettled(t *testing.T) {
	for _, rejection := range []error{table.ErrCommitFailed, catalog.ErrNamespaceAlreadyExists, rest.ErrForbidden} {
		t.Run(rejection.Error(), func(t *testing.T) {
			ctx, config, fence := nativeExecutionFixture(t)
			backend := &nativeExecutionTestCatalog{failure: rejection}
			cat := &nativeCatalog{Catalog: backend, execution: config.execution}
			require.ErrorIs(t, cat.CreateNamespace(ctx, table.Identifier{"db"}, nil), rejection)
			backend.failure = nil
			require.NoError(t, cat.CreateNamespace(ctx, table.Identifier{"next"}, nil))
			closeNativeTestFence(t, fence)
			require.NoError(t, cat.Close())
			utils.SourceDrained(ctx, nil)
			result, _ := utils.OrdinaryExecutionResult(ctx)
			require.Equal(t, "settled", result.Outcome)
		})
	}
}

func TestNativeRetirementDrainsAdmittedWriterWork(t *testing.T) {
	ctx, config, fence := nativeExecutionFixture(t)
	_, finish, err := beginNativeWriterWork(ctx, config)
	require.NoError(t, err)
	closeNativeTestFence(t, fence)
	cat := &nativeCatalog{Catalog: &nativeExecutionTestCatalog{}, execution: config.execution}
	done := make(chan error, 1)
	go func() { done <- cat.Close() }()
	select {
	case <-done:
		t.Fatal("retirement overtook admitted native work")
	case <-time.After(20 * time.Millisecond):
	}
	utils.SourceDrained(ctx, nil)
	result, _ := utils.OrdinaryExecutionResult(ctx)
	require.Equal(t, "unknown", result.Outcome)
	finish()
	finish() // Finishing an admission is idempotent.
	require.NoError(t, <-done)
	result, _ = utils.OrdinaryExecutionResult(ctx)
	require.Equal(t, "settled", result.Outcome)
}

func TestNativeRESTMutatingResponsesAreNeverReplayed(t *testing.T) {
	for _, status := range []int{http.StatusTooManyRequests, http.StatusTemporaryRedirect, http.StatusPermanentRedirect, http.StatusInternalServerError, http.StatusServiceUnavailable} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			ctx, config, _ := nativeExecutionFixture(t)
			calls := 0
			transport := newNativeRESTTransport(nativeRoundTripFunc(func(request *http.Request) (*http.Response, error) {
				calls++
				require.Nil(t, request.GetBody, "wire transport must not permit connection replay")
				return &http.Response{StatusCode: status, Header: http.Header{"Retry-After": {"0"}, "Location": {"https://catalog.example/elsewhere"}}, Body: http.NoBody}, nil
			})).(*nativeRESTThrottleTransport)
			transport.execution = config.execution
			request, err := http.NewRequestWithContext(ctx, http.MethodPost, "https://catalog.example/v1/namespaces/db/tables/t", strings.NewReader(`{"updates":[]}`))
			require.NoError(t, err)
			response, err := (&http.Client{Transport: transport}).Do(request)
			if response != nil {
				_ = response.Body.Close()
			}
			if status >= 300 && status < 400 {
				require.ErrorIs(t, err, rest.ErrCommitStateUnknown)
			} else {
				require.NoError(t, err)
				require.Equal(t, status, response.StatusCode)
			}
			require.Equal(t, 1, calls)
			if status != http.StatusTooManyRequests {
				require.ErrorIs(t, nativeExecutionWritable(ctx, config), rest.ErrCommitStateUnknown)
			}
		})
	}
}

func TestNativeRESTTransportFailureMakesRuntimeUnknown(t *testing.T) {
	ctx, config, _ := nativeExecutionFixture(t)
	calls := 0
	cause := io.ErrUnexpectedEOF
	transport := newNativeRESTTransport(nativeRoundTripFunc(func(*http.Request) (*http.Response, error) {
		calls++
		return nil, cause
	})).(*nativeRESTThrottleTransport)
	transport.execution = config.execution
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, "https://catalog.example/v1/namespaces/db/tables/t", strings.NewReader(`{}`))
	require.NoError(t, err)
	_, err = transport.RoundTrip(request)
	require.ErrorIs(t, err, cause)
	require.ErrorIs(t, err, rest.ErrCommitStateUnknown)
	_, err = transport.RoundTrip(request)
	require.ErrorIs(t, err, rest.ErrCommitStateUnknown)
	require.Equal(t, 1, calls)
}

func TestNativeRESTResponseBodyParticipatesInRetirement(t *testing.T) {
	ctx, config, _ := nativeExecutionFixture(t)
	transport := newNativeRESTTransport(nativeRoundTripFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(`{}`))}, nil
	})).(*nativeRESTThrottleTransport)
	transport.execution = config.execution
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, "https://catalog.example/v1/namespaces/db/tables/t", strings.NewReader(`{}`))
	require.NoError(t, err)
	response, err := transport.RoundTrip(request)
	require.NoError(t, err)
	config.execution.mu.Lock()
	require.Equal(t, 1, config.execution.active)
	config.execution.mu.Unlock()
	// Abandoning even a 200 body is not proof that the commit response decoded.
	require.NoError(t, response.Body.Close())
	require.ErrorIs(t, nativeExecutionWritable(ctx, config), rest.ErrCommitStateUnknown)
	config.execution.mu.Lock()
	require.Zero(t, config.execution.active)
	config.execution.mu.Unlock()
}

func TestNativeRESTReadAndAuthRemainAvailableAfterFenceClosed(t *testing.T) {
	ctx, config, fence := nativeExecutionFixture(t)
	closeNativeTestFence(t, fence)
	calls := 0
	transport := newNativeRESTTransport(nativeRoundTripFunc(func(*http.Request) (*http.Response, error) {
		calls++
		return &http.Response{StatusCode: http.StatusOK, Body: http.NoBody}, nil
	})).(*nativeRESTThrottleTransport)
	transport.execution = config.execution
	for _, test := range []struct{ method, path string }{
		{http.MethodGet, "/v1/namespaces/db/tables/t"},
		{http.MethodPost, "/v1/oauth/tokens"},
		{http.MethodPost, "/v1/namespaces/db/tables/t/plan"},
		{http.MethodPost, "/v1/namespaces/db/tables/t/tasks"},
	} {
		request, err := http.NewRequestWithContext(ctx, test.method, "https://catalog.example"+test.path, nil)
		require.NoError(t, err)
		response, err := transport.RoundTrip(request)
		require.NoError(t, err)
		_ = response.Body.Close()
	}
	require.Equal(t, 4, calls)
}

func TestNativeRecoverOnlyBlocksActualMutationWithoutBlockingReads(t *testing.T) {
	ctx, config, _ := nativeExecutionFixture(t)
	config.resyncRecoverOnly = true
	config.execution.recoverOnly = true
	backend := &nativeExecutionTestCatalog{}
	cat := &nativeCatalog{Catalog: backend, execution: config.execution}
	require.ErrorContains(t, cat.CreateNamespace(ctx, table.Identifier{"db"}, nil), "recover-only")
	_, err := cat.CheckNamespaceExists(ctx, table.Identifier{"db"})
	require.NoError(t, err)
	require.Zero(t, backend.mutations)
}

func TestNativeRESTOrdinaryRetirementAndMalformedSuccess(t *testing.T) {
	for _, malformed := range []bool{false, true} {
		t.Run(map[bool]string{false: "successful-job", true: "malformed-success"}[malformed], func(t *testing.T) {
			ctx, fence := nativeExecutionTestContext(t)
			// This fixture exercises the real native REST SDK, not a retirement
			// assertion supplied by a sidecar or a mocked successful terminal.
			mutations := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				switch {
				case r.Method == http.MethodGet && r.URL.Path == "/v1/config":
					_, _ = io.WriteString(w, `{"defaults":{},"overrides":{}}`)
				case r.Method == http.MethodPost && r.URL.Path == "/v1/namespaces":
					mutations++
					if malformed {
						_, _ = io.WriteString(w, `{"namespace":`)
					} else {
						_, _ = io.WriteString(w, `{"namespace":["db"],"properties":{}}`)
					}
				default:
					w.WriteHeader(http.StatusNoContent)
				}
			}))
			defer server.Close()
			config := &Config{CatalogType: RestCatalog, CatalogName: "execution", RestCatalogURL: server.URL, RestAuthType: "none", IcebergS3Path: t.TempDir(), Region: "us-east-1", AccessKey: "test", SecretKey: "test"}
			cat, cleanup, err := openNativeCatalog(ctx, config)
			require.NoError(t, err)
			err = cat.CreateNamespace(ctx, table.Identifier{"db"}, nil)
			if malformed {
				require.Error(t, err)
				require.ErrorIs(t, nativeExecutionWritable(ctx, config), rest.ErrCommitStateUnknown)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, 1, mutations)
			closeNativeTestFence(t, fence)
			cleanup(context.Background())
			utils.SourceDrained(ctx, nil)
			result, _ := utils.OrdinaryExecutionResult(ctx)
			require.Equal(t, map[bool]string{false: "settled", true: "unknown"}[malformed], result.Outcome)
		})
	}
}

func TestNativeAWSMutationFenceIsCheckedAtWireDispatch(t *testing.T) {
	for _, test := range []struct {
		name, method, endpoint, target, authorization string
		blocked                                       bool
	}{
		{"glue-update", http.MethodPost, "https://glue.example/", "AWSGlue.UpdateTable", "", true},
		{"glue-create", http.MethodPost, "https://glue.example/", "AWSGlue.CreateTable", "", true},
		{"glue-read", http.MethodPost, "https://glue.example/", "AWSGlue.GetTable", "", false},
		{"s3-upload", http.MethodPut, "https://bucket.s3.example/data", "", "AWS4-HMAC-SHA256 Credential=test/date/region/s3/aws4_request", true},
		{"s3-delete", http.MethodDelete, "https://bucket.s3.example/data", "", "AWS4-HMAC-SHA256 Credential=test/date/region/s3/aws4_request", true},
		{"s3-read", http.MethodGet, "https://bucket.s3.example/data", "", "AWS4-HMAC-SHA256 Credential=test/date/region/s3/aws4_request", false},
		{"credential-refresh", http.MethodPost, "https://sts.example/", "", "", false},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx, config, fence := nativeExecutionFixture(t)
			closeNativeTestFence(t, fence)
			calls := 0
			transport := newNativeAWSExecutionTransport(nativeRoundTripFunc(func(*http.Request) (*http.Response, error) {
				calls++
				return &http.Response{StatusCode: http.StatusOK, Body: http.NoBody}, nil
			}), config.execution, config)
			request, err := http.NewRequestWithContext(ctx, test.method, test.endpoint, nil)
			require.NoError(t, err)
			request.Header.Set("X-Amz-Target", test.target)
			request.Header.Set("Authorization", test.authorization)
			response, err := transport.RoundTrip(request)
			if test.blocked {
				require.Error(t, err)
				require.Zero(t, calls)
			} else {
				require.NoError(t, err)
				require.Equal(t, 1, calls)
				_ = response.Body.Close()
			}
		})
	}
}

func TestNativeRESTReadBodyMustDrainBeforeRetirement(t *testing.T) {
	ctx, config, fence := nativeExecutionFixture(t)
	closeNativeTestFence(t, fence)
	transport := newNativeRESTTransport(nativeRoundTripFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(`{}`))}, nil
	})).(*nativeRESTThrottleTransport)
	transport.execution = config.execution
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, "https://catalog.example/v1/namespaces/db/tables/t", nil)
	require.NoError(t, err)
	response, err := transport.RoundTrip(request)
	require.NoError(t, err)
	cat := &nativeCatalog{Catalog: &nativeExecutionTestCatalog{}, execution: config.execution}
	done := make(chan error, 1)
	go func() { done <- cat.Close() }()
	select {
	case <-done:
		t.Fatal("retirement overtook a live native read")
	case <-time.After(20 * time.Millisecond):
	}
	require.NoError(t, response.Body.Close())
	require.NoError(t, <-done)
	utils.SourceDrained(ctx, nil)
	result, _ := utils.OrdinaryExecutionResult(ctx)
	require.Equal(t, "settled", result.Outcome)
}

func TestNativeCanceledCommandCannotWriteThroughFreshCallerContext(t *testing.T) {
	ctx, _ := nativeExecutionTestContext(t)
	config := &Config{}
	commandCtx, cancel := context.WithCancel(ctx)
	config.execution = newNativeExecution(commandCtx, config)
	cancel()
	require.ErrorIs(t, nativeExecutionWritable(context.Background(), config), context.Canceled)
}

func TestNativeExpiredWorkContextCannotDispatchAfterRetirement(t *testing.T) {
	ctx, config, _ := nativeExecutionFixture(t)
	work, finish, err := beginNativeWriterWork(ctx, config)
	require.NoError(t, err)
	finish()
	cat := &nativeCatalog{Catalog: &nativeExecutionTestCatalog{}, execution: config.execution}
	require.NoError(t, cat.Close())
	calls := 0
	transport := newNativeRESTTransport(nativeRoundTripFunc(func(*http.Request) (*http.Response, error) {
		calls++
		return &http.Response{StatusCode: http.StatusOK, Body: http.NoBody}, nil
	})).(*nativeRESTThrottleTransport)
	transport.execution = config.execution
	request, err := http.NewRequestWithContext(work, http.MethodPost, "https://catalog.example/v1/namespaces/db/tables/t", nil)
	require.NoError(t, err)
	_, err = transport.RoundTrip(request)
	require.ErrorIs(t, err, errNativeExecutionRetired)
	require.Zero(t, calls)
	utils.SourceDrained(ctx, nil)
	result, _ := utils.OrdinaryExecutionResult(ctx)
	require.Equal(t, "settled", result.Outcome)
}

func TestNativeEscapedTableNamesCannotBypassMutationFence(t *testing.T) {
	ctx, config, fence := nativeExecutionFixture(t)
	closeNativeTestFence(t, fence)
	calls := 0
	transport := newNativeRESTTransport(nativeRoundTripFunc(func(*http.Request) (*http.Response, error) {
		calls++
		return &http.Response{StatusCode: http.StatusOK, Body: http.NoBody}, nil
	})).(*nativeRESTThrottleTransport)
	transport.execution = config.execution
	for _, name := range []string{"a%2Fplan", "a%2Ftasks", "a%2Fmetrics", "a%2Foauth%2Ftokens", "plan", "tasks"} {
		request, err := http.NewRequestWithContext(ctx, http.MethodPost, "https://catalog.example/v1/namespaces/db/tables/"+name, strings.NewReader(`{}`))
		require.NoError(t, err)
		_, err = transport.RoundTrip(request)
		require.Error(t, err, name)
	}
	require.Zero(t, calls)
}

func TestNativeUncontrolledRuntimeKeepsExistingRESTBehavior(t *testing.T) {
	require.Nil(t, newNativeExecution(context.Background(), &Config{}))
	calls := 0
	transport := nativeThrottleTestTransport(nativeRoundTripFunc(func(request *http.Request) (*http.Response, error) {
		calls++
		body, err := io.ReadAll(request.Body)
		require.NoError(t, err)
		require.Equal(t, `{}`, string(body))
		_ = request.Body.Close()
		status := http.StatusTooManyRequests
		if calls == 2 {
			status = http.StatusOK
		}
		return &http.Response{StatusCode: status, Header: http.Header{"Retry-After": {"0"}}, Body: http.NoBody}, nil
	}))
	request, err := http.NewRequest(http.MethodPost, "https://catalog.example/v1/namespaces/db/tables/t", strings.NewReader(`{}`))
	require.NoError(t, err)
	response, err := transport.RoundTrip(request)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, response.StatusCode)
	require.Equal(t, 2, calls)
	require.NoError(t, response.Body.Close())
}

func TestNativeHiveMutationRechecksFenceAfterWaitingForClient(t *testing.T) {
	ctx, config, fence := nativeExecutionFixture(t)
	work, finish, err := beginNativeWriterWork(ctx, config)
	require.NoError(t, err)
	transport := &nativeHiveTrackedTransport{TTransport: thrift.NewTMemoryBuffer()}
	pool := &nativeHivePool{clients: make(chan *nativeHiveClient, 1), done: make(chan struct{}), execution: config.execution}
	calls := 0
	done := make(chan error, 1)
	go func() {
		done <- nativeHiveExec(work, pool, func(*nativeHiveClient) error { calls++; return nil })
	}()
	closeNativeTestFence(t, fence)
	pool.clients <- &nativeHiveClient{transport: transport}
	err = <-done
	var rejected *nativeMutationNotDispatched
	require.ErrorAs(t, err, &rejected)
	require.Zero(t, calls)
	require.Zero(t, transport.closes, "fence rejection must not mark a healthy connection broken")
	finish()
	cat := &nativeCatalog{Catalog: &nativeExecutionTestCatalog{}, execution: config.execution}
	require.NoError(t, cat.Close())
	utils.SourceDrained(ctx, nil)
	result, _ := utils.OrdinaryExecutionResult(ctx)
	require.Equal(t, "settled", result.Outcome)
}

func TestNativeStartupFenceIdentityIsNotMutableCallerState(t *testing.T) {
	ctx, config, fence := nativeExecutionFixture(t)
	closeNativeTestFence(t, fence)
	other, _, otherFence := nativeExecutionFixture(t)
	execution, ok := utils.ExecutionFromContext(ctx)
	require.True(t, ok)
	execution.Fence = otherFence
	require.Error(t, nativeExecutionWritable(ctx, config))
	require.Error(t, nativeExecutionWritable(other, config))
}

func TestNativeControlledHTTPWireDisablesHiddenMutationReplay(t *testing.T) {
	client, closeClient := newNativeHTTPClient(true)
	defer closeClient()
	wire, ok := client.Transport.(*nativeDiagnosticTransport).base.(*nativeExecutionWireTransport)
	require.True(t, ok)
	mutations := wire.mutations.(*http.Transport)
	require.True(t, mutations.DisableKeepAlives)
	require.False(t, mutations.ForceAttemptHTTP2)
	require.NotNil(t, mutations.TLSNextProto)
	require.Empty(t, mutations.TLSNextProto)
	require.True(t, wire.ordinary.(*http.Transport).ForceAttemptHTTP2)

	ordinary, closeOrdinary := newNativeHTTPClient(false)
	defer closeOrdinary()
	_, isStandard := ordinary.Transport.(*nativeDiagnosticTransport).base.(*http.Transport)
	require.True(t, isStandard, "standalone sessions must retain the native pooled transport")
}

func TestNativeMutationUsesIsolatedWireWithoutChangingSignedRequest(t *testing.T) {
	ctx, config, _ := nativeExecutionFixture(t)
	reads, writes := 0, 0
	wire := &nativeExecutionWireTransport{
		ordinary: nativeRoundTripFunc(func(*http.Request) (*http.Response, error) {
			reads++
			return &http.Response{StatusCode: http.StatusOK, Body: http.NoBody}, nil
		}),
		mutations: nativeRoundTripFunc(func(request *http.Request) (*http.Response, error) {
			writes++
			require.Equal(t, "signed-request", request.Header.Get("Authorization"))
			require.Equal(t, "replay-key", request.Header.Get("Idempotency-Key"))
			require.Nil(t, request.Body, "bodyless signed requests must remain bodyless")
			return &http.Response{StatusCode: http.StatusNoContent, Body: http.NoBody}, nil
		}),
	}
	transport := newNativeRESTTransport(wire).(*nativeRESTThrottleTransport)
	transport.execution = config.execution
	request, err := http.NewRequestWithContext(ctx, http.MethodDelete, "https://catalog.example/v1/namespaces/db/tables/t", nil)
	require.NoError(t, err)
	request.Header.Set("Authorization", "signed-request")
	request.Header.Set("Idempotency-Key", "replay-key")
	response, err := transport.RoundTrip(request)
	require.NoError(t, err)
	require.NoError(t, response.Body.Close())
	request, err = http.NewRequestWithContext(ctx, http.MethodGet, "https://catalog.example/v1/namespaces/db/tables/t", nil)
	require.NoError(t, err)
	response, err = transport.RoundTrip(request)
	require.NoError(t, err)
	require.NoError(t, response.Body.Close())
	require.Equal(t, 1, writes)
	require.Equal(t, 1, reads)
	require.NoError(t, nativeExecutionWritable(ctx, config))
}
