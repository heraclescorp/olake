package iceberg

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"syscall"
	"testing"
	"time"

	awsconfig "github.com/aws/aws-sdk-go-v2/config"
)

type nativeRoundTripFunc func(*http.Request) (*http.Response, error)

func (f nativeRoundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

type nativeUnreadBody struct{ reads, closes int }

func (b *nativeUnreadBody) Read([]byte) (int, error) { b.reads++; return 0, io.EOF }
func (b *nativeUnreadBody) Close() error             { b.closes++; return nil }

func TestNativeHTTPDiagnosticsPreserveBodiesAndRedactSecrets(t *testing.T) {
	requestBody, responseBody := &nativeUnreadBody{}, &nativeUnreadBody{}
	request, err := http.NewRequest(http.MethodPost, "https://user:userinfo-secret@catalog.example/v1/namespaces/ns/tables/t?token=query-secret", requestBody)
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Authorization", "Bearer auth-secret")
	request.Header.Set("Cookie", "session=cookie-secret")
	var events []nativeHTTPAttempt
	transport := newNativeHTTPTransport(nativeRoundTripFunc(func(got *http.Request) (*http.Response, error) {
		if got != request || got.Body != requestBody {
			t.Fatal("transport changed the signed request")
		}
		if got.Header.Get("Authorization") != "Bearer auth-secret" {
			t.Fatal("transport changed authorization")
		}
		return &http.Response{StatusCode: 409, Header: http.Header{"X-Amz-Request-Id": {"aws-request"}, "X-Amz-Id-2": {"aws-extended"}, "Set-Cookie": {"response-secret"}}, Body: responseBody}, nil
	}), func(a nativeHTTPAttempt) { events = append(events, a) })
	response, err := transport.RoundTrip(request)
	if err != nil {
		t.Fatal(err)
	}
	if response.Body != responseBody || requestBody.reads != 0 || requestBody.closes != 0 || responseBody.reads != 0 || responseBody.closes != 0 {
		t.Fatal("diagnostics consumed or closed a body")
	}
	if len(events) != 1 {
		t.Fatalf("expected one event, got %d", len(events))
	}
	a := events[0]
	if a.Method != "POST" || a.Host != "catalog.example" || a.Path != "/v1/namespaces/ns/tables/t" || a.Status != 409 || a.RequestID != "aws-request" || a.ExtendedRequestID != "aws-extended" || a.ID == 0 || a.Duration < 0 {
		t.Fatalf("incomplete diagnostic: %s", a.String())
	}
	for _, secret := range []string{"userinfo-secret", "query-secret", "auth-secret", "cookie-secret", "response-secret"} {
		if strings.Contains(a.String(), secret) {
			t.Fatalf("diagnostic leaked %s", secret)
		}
	}
}

func TestNativeHTTPDiagnosticsRecordRecoveredAttempts(t *testing.T) {
	request, err := http.NewRequest(http.MethodGet, "https://catalog.example/v1/config", nil)
	if err != nil {
		t.Fatal(err)
	}
	statuses := []int{409, 429, 200}
	var events []nativeHTTPAttempt
	transport := newNativeHTTPTransport(nativeRoundTripFunc(func(*http.Request) (*http.Response, error) {
		status := statuses[len(events)]
		return &http.Response{StatusCode: status, Header: http.Header{"X-Amzn-Requestid": {"request"}}, Body: http.NoBody}, nil
	}), func(a nativeHTTPAttempt) { events = append(events, a) })
	for range statuses {
		if _, err := transport.RoundTrip(request); err != nil {
			t.Fatal(err)
		}
	}
	if len(events) != len(statuses) {
		t.Fatalf("lost recovered attempts: %v", events)
	}
	for i, event := range events {
		if event.Status != statuses[i] || event.RequestID != "request" {
			t.Fatalf("wrong attempt %d: %s", i, event.String())
		}
		if i > 0 && event.ID <= events[i-1].ID {
			t.Fatal("attempt IDs are not increasing")
		}
	}
}

func TestNativeHTTPDiagnosticsDoNotLogTransportErrors(t *testing.T) {
	request, _ := http.NewRequest(http.MethodGet, "https://catalog.example/v1/config", nil)
	cause := errors.New("https://catalog.example/?secret=transport-secret")
	var event nativeHTTPAttempt
	transport := newNativeHTTPTransport(nativeRoundTripFunc(func(*http.Request) (*http.Response, error) { return nil, cause }), func(a nativeHTTPAttempt) { event = a })
	_, err := transport.RoundTrip(request)
	if !errors.Is(err, cause) || !event.TransportError || event.Status != 0 {
		t.Fatal("transport error changed or missing")
	}
	if strings.Contains(event.String(), "transport-secret") {
		t.Fatal("logged transport error URL")
	}
}

func TestNativeHTTPDiagnosticsEscapeHeaderAndPathControls(t *testing.T) {
	event := nativeHTTPAttempt{Path: "/path\nforged_log", RequestID: "id\r\nAuthorization: secret"}
	if strings.ContainsAny(event.String(), "\r\n") {
		t.Fatal("diagnostic permits log injection")
	}
}

func TestNativeCatalogErrorsPreserveCausesWithoutSecrets(t *testing.T) {
	cause := &url.Error{Op: "POST", URL: "https://catalog/?token=secret", Err: errors.New("response-body-secret")}
	err := nativeCatalogError("open catalog", cause)
	if !errors.Is(err, cause) {
		t.Fatal("lost machine-readable cause")
	}
	var urlError *url.Error
	if !errors.As(err, &urlError) {
		t.Fatal("lost error type")
	}
	if strings.Contains(err.Error(), "secret") {
		t.Fatal("error leaked request or response secrets")
	}
	if nativeCatalogError("operation", nil) != nil {
		t.Fatal("wrapped a nil error")
	}
	for _, cause := range []error{context.Canceled, context.DeadlineExceeded} {
		err := nativeCatalogError("catalog", cause)
		if !errors.Is(err, cause) {
			t.Fatal("lost cancellation cause")
		}
	}
}

func nativeThrottleTestTransport(base http.RoundTripper) *nativeRESTThrottleTransport {
	transport := newNativeRESTTransport(base).(*nativeRESTThrottleTransport)
	transport.now = func() time.Time { return time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC) }
	transport.wait = func(ctx context.Context, _ time.Duration) error { return ctx.Err() }
	return transport
}

func TestNativeRESTThrottleRetriesRemainVisible(t *testing.T) {
	var attempts []nativeHTTPAttempt
	bodies := []*nativeUnreadBody{{}, {}, {}}
	calls := 0
	diagnostics := newNativeHTTPTransport(nativeRoundTripFunc(func(*http.Request) (*http.Response, error) {
		index := calls
		calls++
		status := http.StatusTooManyRequests
		if calls == 3 {
			status = http.StatusOK
		}
		return &http.Response{StatusCode: status, Header: http.Header{"Retry-After": {"0"}, "X-Amz-Request-Id": {"request-id"}}, Body: bodies[index]}, nil
	}), func(attempt nativeHTTPAttempt) { attempts = append(attempts, attempt) })
	transport := nativeThrottleTestTransport(diagnostics)
	request, err := http.NewRequest(http.MethodGet, "https://catalog.example/v1/config?token=secret", nil)
	if err != nil {
		t.Fatal(err)
	}
	response, err := transport.RoundTrip(request)
	if err != nil || response.StatusCode != http.StatusOK || calls != 3 || len(attempts) != 3 {
		t.Fatalf("throttle retry failed: calls=%d events=%d err=%v", calls, len(attempts), err)
	}
	for index, body := range bodies {
		wantCloses := 1
		if index == 2 {
			wantCloses = 0
		}
		if body.reads != 0 || body.closes != wantCloses {
			t.Fatal("discarded response was consumed or not closed, or final response was changed")
		}
		if strings.Contains(attempts[index].String(), "secret") {
			t.Fatal("retried attempt leaked its query")
		}
		if index < 2 && attempts[index].Status != 429 {
			t.Fatal("recovered throttle was not logged")
		}
	}
	_ = response.Body.Close()
}

func TestNativeRESTThrottleStopsAtAttemptLimit(t *testing.T) {
	calls := 0
	var responses []*nativeUnreadBody
	var waits []time.Duration
	transport := nativeThrottleTestTransport(nativeRoundTripFunc(func(*http.Request) (*http.Response, error) {
		calls++
		body := &nativeUnreadBody{}
		responses = append(responses, body)
		return &http.Response{StatusCode: 429, Header: http.Header{}, Body: body}, nil
	}))
	transport.wait = func(_ context.Context, delay time.Duration) error { waits = append(waits, delay); return nil }
	request, _ := http.NewRequest(http.MethodGet, "https://catalog.example/v1/config", nil)
	response, err := transport.RoundTrip(request)
	if err != nil || response.StatusCode != 429 || calls != nativeRESTThrottleMaxAttempts || len(waits) != calls-1 {
		t.Fatal("REST retries are not attempt-bounded")
	}
	for index, delay := range waits {
		minimum := time.Second << index
		if delay < minimum || delay >= minimum+minimum/10 {
			t.Fatal("unexpected throttle backoff")
		}
		if responses[index].closes != 1 || responses[index].reads != 0 {
			t.Fatal("discarded throttle response not closed intact")
		}
	}
	if responses[len(responses)-1].closes != 0 {
		t.Fatal("final throttle response was closed before caller could inspect it")
	}
	_ = response.Body.Close()
}

func TestNativeRESTThrottleHonorsRetryAfterAndWaitBudget(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	for _, test := range []struct {
		header   string
		expected time.Duration
	}{
		{"0", 0},
		{" 3 ", 3 * time.Second},
		{now.Add(5 * time.Second).Format(http.TimeFormat), 5 * time.Second},
		{now.Add(-5 * time.Second).Format(http.TimeFormat), 0},
	} {
		if actual := nativeHTTPThrottleDelay(test.header, 1, now); actual != test.expected {
			t.Fatalf("Retry-After %q: got %v want %v", test.header, actual, test.expected)
		}
	}
	for _, header := range []string{"3600", "9999999999999999999999999999999999999", now.Add(time.Hour).Format(http.TimeFormat)} {
		if delay := nativeHTTPThrottleDelay(header, 1, now); delay <= nativeRESTThrottleMaxWait {
			t.Fatal("oversized Retry-After became an earlier retry")
		}
	}
	calls, waits := 0, 0
	var finalBody *nativeUnreadBody
	transport := nativeThrottleTestTransport(nativeRoundTripFunc(func(*http.Request) (*http.Response, error) {
		calls++
		finalBody = &nativeUnreadBody{}
		return &http.Response{StatusCode: 429, Header: http.Header{"Retry-After": {"40"}}, Body: finalBody}, nil
	}))
	transport.wait = func(_ context.Context, delay time.Duration) error {
		waits++
		if delay != 40*time.Second {
			t.Fatal("ignored Retry-After")
		}
		return nil
	}
	request, _ := http.NewRequest(http.MethodGet, "https://catalog.example/v1/config", nil)
	response, err := transport.RoundTrip(request)
	if err != nil || response.StatusCode != 429 || calls != 2 || waits != 1 || finalBody.closes != 0 {
		t.Fatal("wait budget did not stop retries while retaining the final response")
	}
	_ = response.Body.Close()
}

func TestNativeRESTThrottleCancellationInterruptsWait(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	body := &nativeUnreadBody{}
	calls := 0
	transport := nativeThrottleTestTransport(nativeRoundTripFunc(func(*http.Request) (*http.Response, error) {
		calls++
		return &http.Response{StatusCode: 429, Header: http.Header{"Retry-After": {"30"}}, Body: body}, nil
	}))
	waiting := make(chan struct{})
	transport.wait = func(ctx context.Context, delay time.Duration) error {
		close(waiting)
		return nativeHTTPRetryWait(ctx, delay)
	}
	request, _ := http.NewRequestWithContext(ctx, http.MethodGet, "https://catalog.example/v1/config", nil)
	finished := make(chan error, 1)
	go func() { _, err := transport.RoundTrip(request); finished <- err }()
	<-waiting
	cancel()
	select {
	case err := <-finished:
		if !errors.Is(err, context.Canceled) || calls != 1 || body.closes != 1 || body.reads != 0 {
			t.Fatal("canceled throttle wait retried or leaked its response")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("throttle retry ignored cancellation")
	}
}

func TestNativeRESTThrottleReplaysPOSTOnlyThroughGetBody(t *testing.T) {
	request, _ := http.NewRequest(http.MethodPost, "https://catalog.example/v1/namespaces/db/tables/t", strings.NewReader(`{"updates":["exact-payload"]}`))
	request.Header.Set("Authorization", "signed-authorization")
	originalGetBody := request.GetBody
	replays, calls := 0, 0
	request.GetBody = func() (io.ReadCloser, error) { replays++; return originalGetBody() }
	transport := nativeThrottleTestTransport(nativeRoundTripFunc(func(attempt *http.Request) (*http.Response, error) {
		calls++
		payload, err := io.ReadAll(attempt.Body)
		_ = attempt.Body.Close()
		if err != nil || string(payload) != `{"updates":["exact-payload"]}` || attempt.ContentLength != request.ContentLength || attempt.Method != http.MethodPost || attempt.Header.Get("Authorization") != "signed-authorization" {
			t.Fatal("POST replay changed its payload or signed headers")
		}
		status := http.StatusTooManyRequests
		if calls == 2 {
			status = http.StatusOK
		}
		return &http.Response{StatusCode: status, Header: http.Header{"Retry-After": {"0"}}, Body: http.NoBody}, nil
	}))
	response, err := transport.RoundTrip(request)
	if err != nil || response.StatusCode != 200 || calls != 2 || replays != 1 {
		t.Fatal("POST was not replayed via GetBody")
	}
}

func TestNativeRESTThrottleLeavesUnreplayablePOSTResponseIntact(t *testing.T) {
	for _, getBodyFails := range []bool{false, true} {
		request, _ := http.NewRequest(http.MethodPost, "https://catalog.example/v1/commit", io.NopCloser(strings.NewReader("payload")))
		if getBodyFails {
			request.GetBody = func() (io.ReadCloser, error) { return nil, errors.New("unreplayable") }
		}
		calls := 0
		body := &nativeUnreadBody{}
		transport := nativeThrottleTestTransport(nativeRoundTripFunc(func(attempt *http.Request) (*http.Response, error) {
			calls++
			_ = attempt.Body.Close()
			return &http.Response{StatusCode: 429, Header: http.Header{"Retry-After": {"0"}}, Body: body}, nil
		}))
		response, err := transport.RoundTrip(request)
		if err != nil || response.StatusCode != 429 || calls != 1 || body.closes != 0 || body.reads != 0 {
			t.Fatal("unreplayable POST was retried or its final response was discarded")
		}
		_ = response.Body.Close()
	}
}

func TestNativeRESTThrottleDoesNotRetryConflictsOrAmbiguousFailures(t *testing.T) {
	for _, method := range []string{http.MethodGet, http.MethodPost} {
		for _, status := range []int{409, 500, 502, 503, 504} {
			calls := 0
			transport := nativeThrottleTestTransport(nativeRoundTripFunc(func(attempt *http.Request) (*http.Response, error) {
				calls++
				if attempt.Body != nil {
					_ = attempt.Body.Close()
				}
				return &http.Response{StatusCode: status, Header: http.Header{"Retry-After": {"0"}}, Body: http.NoBody}, nil
			}))
			request, _ := http.NewRequest(method, "https://catalog.example/v1/commit", strings.NewReader("payload"))
			response, err := transport.RoundTrip(request)
			if err != nil || response.StatusCode != status || calls != 1 {
				t.Fatalf("retried %s HTTP %d", method, status)
			}
		}
	}
	cause := errors.New("ambiguous network failure")
	calls := 0
	transport := nativeThrottleTestTransport(nativeRoundTripFunc(func(*http.Request) (*http.Response, error) { calls++; return nil, cause }))
	request, _ := http.NewRequest(http.MethodPost, "https://catalog.example/v1/commit", nil)
	response, err := transport.RoundTrip(request)
	if response != nil || !errors.Is(err, cause) || calls != 1 {
		t.Fatal("retried an ambiguous transport failure")
	}
}

type nativeTestSQLStateError struct{ state string }

func (e nativeTestSQLStateError) Error() string    { return "private-secret" }
func (e nativeTestSQLStateError) SQLState() string { return e.state }

func TestNativeCatalogErrorsUseSafeBackendCategories(t *testing.T) {
	for _, test := range []struct {
		cause    error
		category string
	}{
		{nativeTestSQLStateError{state: "28P01"}, "PostgreSQL authentication rejected (SQLSTATE 28P01)"},
		{nativeTestSQLStateError{state: "40001"}, "PostgreSQL transaction conflict (SQLSTATE 40001)"},
		{nativeTestSQLStateError{state: "private-secret"}, "operation failed"},
		{awsconfig.SharedConfigProfileNotExistError{Profile: "private-secret", Filename: []string{"private-secret"}}, "AWS shared profile not found"},
		{awsconfig.SharedConfigLoadError{Filename: "private-secret", Err: errors.New("private-secret")}, "AWS shared configuration could not be loaded"},
		{&net.OpError{Op: "dial", Net: "tcp", Addr: &net.TCPAddr{IP: net.ParseIP("192.0.2.1"), Port: 5432}, Err: syscall.ECONNREFUSED}, "network connection refused"},
		{&net.DNSError{Name: "private-secret", Err: "private-secret"}, "DNS lookup failed"},
		{errors.New("private-secret"), "operation failed"},
	} {
		err := nativeCatalogError("open catalog", test.cause)
		if !strings.Contains(err.Error(), test.category) || strings.Contains(err.Error(), "private-secret") || strings.Contains(err.Error(), "192.0.2.1") || strings.Contains(err.Error(), "iceberg_http") {
			t.Fatalf("incorrect backend diagnostic: %s", err.Error())
		}
	}
}

func TestNativeRESTThrottleCancellationClosesPendingPOSTReplay(t *testing.T) {
	requestBody, replayBody, responseBody := &nativeUnreadBody{}, &nativeUnreadBody{}, &nativeUnreadBody{}
	request, _ := http.NewRequest(http.MethodPost, "https://catalog.example/v1/commit", requestBody)
	request.GetBody = func() (io.ReadCloser, error) { return replayBody, nil }
	calls := 0
	transport := nativeThrottleTestTransport(nativeRoundTripFunc(func(attempt *http.Request) (*http.Response, error) {
		calls++
		_ = attempt.Body.Close()
		return &http.Response{StatusCode: 429, Header: http.Header{"Retry-After": {"1"}}, Body: responseBody}, nil
	}))
	transport.wait = func(context.Context, time.Duration) error { return context.Canceled }
	response, err := transport.RoundTrip(request)
	if response != nil || !errors.Is(err, context.Canceled) || calls != 1 || replayBody.closes != 1 || replayBody.reads != 0 || responseBody.closes != 1 {
		t.Fatal("canceled POST retry leaked a replay or discarded response body")
	}
}
