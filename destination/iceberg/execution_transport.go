package iceberg

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"

	"github.com/apache/iceberg-go/catalog/rest"
)

type nativeMutationWireKey struct{}

type nativeExecutionWireTransport struct {
	ordinary  http.RoundTripper
	mutations http.RoundTripper
}

func (t *nativeExecutionWireTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	if mutation, _ := request.Context().Value(nativeMutationWireKey{}).(bool); mutation {
		return t.mutations.RoundTrip(request)
	}
	return t.ordinary.RoundTrip(request)
}

// AWS clients may wait for credentials or retry after catalog admission. Check
// the same fixed runtime fence at their actual metadata/storage dispatch, not
// just before calling the SDK. Credential-chain STS and read APIs are untouched.
type nativeAWSExecutionTransport struct {
	guard  nativeRESTThrottleTransport
	s3Host string
}

func newNativeAWSExecutionTransport(base http.RoundTripper, execution *nativeExecution, config *Config) http.RoundTripper {
	t := &nativeAWSExecutionTransport{guard: nativeRESTThrottleTransport{base: base, execution: execution}}
	if endpoint, err := url.Parse(config.S3Endpoint); err == nil {
		t.s3Host = endpoint.Host
	}
	return t
}

func (t *nativeAWSExecutionTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	target := request.Header.Get("X-Amz-Target")
	if strings.HasPrefix(target, "AWSGlue.") {
		operation := strings.TrimPrefix(target, "AWSGlue.")
		if !strings.HasPrefix(operation, "Get") && !strings.HasPrefix(operation, "BatchGet") && !strings.HasPrefix(operation, "Search") {
			return t.guard.mutate(request)
		}
	}
	if request.Method != http.MethodGet && request.Method != http.MethodHead && request.Method != http.MethodOptions &&
		(strings.Contains(request.Header.Get("Authorization"), "/s3/aws4_request") ||
			(t.s3Host != "" && request.URL.Host == t.s3Host)) {
		return t.guard.mutate(request)
	}
	return t.guard.base.RoundTrip(request)
}

func (t *nativeRESTThrottleTransport) mutates(request *http.Request) bool {
	switch request.Method {
	case http.MethodGet, http.MethodHead, http.MethodOptions:
		return false
	}
	// Authentication has a separate lifecycle and may use an arbitrary configured
	// endpoint. Never mistake an OAuth refresh for an Iceberg metadata commit.
	if t.catalogURL != nil && (request.URL.Host != t.catalogURL.Host ||
		!strings.HasPrefix(request.URL.EscapedPath(), strings.TrimSuffix(t.catalogURL.EscapedPath(), "/")+"/")) {
		return false
	}
	if t.authURL != nil && request.URL.Scheme == t.authURL.Scheme && request.URL.Host == t.authURL.Host && request.URL.EscapedPath() == t.authURL.EscapedPath() {
		return false
	}
	path := strings.TrimSuffix(request.URL.EscapedPath(), "/")
	if strings.HasSuffix(path, "/oauth/tokens") && !strings.Contains(path, "/tables/") {
		return false
	}
	// Scan planning, task retrieval, metrics, and plan cancellation do not alter
	// table metadata. Keep SDK read planning and credential refresh operational
	// after CLOSED so recovery can inspect a publication's actual outcome.
	// EscapedPath keeps a table named "a/plan" a single identifier instead of
	// accidentally treating its metadata commit as a read-only planning call.
	if index := strings.LastIndex(path, "/tables/"); index >= 0 {
		_, action, hasAction := strings.Cut(path[index+len("/tables/"):], "/")
		if hasAction && request.Method == http.MethodPost && (action == "plan" || action == "tasks" || action == "metrics") {
			return false
		}
		if hasAction && request.Method == http.MethodDelete && strings.HasPrefix(action, "plan/") && !strings.Contains(strings.TrimPrefix(action, "plan/"), "/") {
			return false
		}
	}
	return true
}

func (t *nativeRESTThrottleTransport) read(request *http.Request) (*http.Response, error) {
	if t.execution == nil {
		return t.readWithThrottle(request)
	}
	ctx, finish, err := t.execution.beginRead(request.Context())
	if err != nil {
		if request.Body != nil {
			_ = request.Body.Close()
		}
		return nil, err
	}
	response, err := t.readWithThrottle(request.Clone(ctx))
	if err != nil || response == nil || response.Body == nil {
		finish()
	} else {
		response.Body = &nativeMutationResponseBody{ReadCloser: response.Body, finish: finish}
	}
	return response, err
}

func (t *nativeRESTThrottleTransport) mutate(request *http.Request) (*http.Response, error) {
	ctx, finish, err := t.execution.begin(request.Context())
	if err != nil {
		if request.Body != nil {
			_ = request.Body.Close()
		}
		return nil, &nativeMutationNotDispatched{cause: err}
	}
	ctx = context.WithValue(ctx, nativeMutationWireKey{}, true)
	// The SDK has already signed the body. Removing GetBody here prevents
	// net/http's reused-connection replay without changing the signed bytes,
	// method, URL, or headers. A 307/308 never reaches http.Client redirect logic.
	wire := request.Clone(ctx)
	wire.GetBody = nil
	response, err := t.base.RoundTrip(wire)
	if err != nil || response == nil {
		t.execution.markUnknown()
		finish()
		if response != nil && response.Body != nil {
			_ = response.Body.Close()
		}
		return nil, errors.Join(rest.ErrCommitStateUnknown, err)
	}
	if response.StatusCode >= 300 && response.StatusCode < 400 {
		t.execution.markUnknown()
		if response.Body != nil {
			_ = response.Body.Close()
		}
		finish()
		return nil, rest.ErrCommitStateUnknown
	}
	if response.StatusCode >= 500 || response.StatusCode == http.StatusRequestTimeout {
		t.execution.markUnknown()
	}
	if response.Body == nil {
		finish()
	} else {
		response.Body = &nativeMutationResponseBody{
			ReadCloser: response.Body,
			execution:  t.execution,
			finish:     finish,
			success:    response.StatusCode >= 200 && response.StatusCode < 300,
			eof:        response.Body == http.NoBody,
		}
	}
	return response, nil
}

// RoundTrip returning headers does not mean native work has drained. The SDK
// still must read and close the mutation response; a lost success body is unknown.
type nativeMutationResponseBody struct {
	io.ReadCloser
	execution *nativeExecution
	finish    func()
	once      sync.Once
	success   bool
	eof       bool
}

func (b *nativeMutationResponseBody) Read(data []byte) (int, error) {
	n, err := b.ReadCloser.Read(data)
	if errors.Is(err, io.EOF) {
		b.eof = true
	} else if err != nil && b.success {
		b.execution.markUnknown()
	}
	return n, err
}

func (b *nativeMutationResponseBody) Close() error {
	err := b.ReadCloser.Close()
	b.once.Do(func() {
		if b.success && (!b.eof || err != nil) {
			b.execution.markUnknown()
		}
		b.finish()
	})
	return err
}
