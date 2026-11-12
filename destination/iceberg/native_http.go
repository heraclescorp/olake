package iceberg

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/catalog/rest"
	"github.com/apache/iceberg-go/table"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/datazip-inc/olake/utils/logger"
)

// nativeHTTPAttempt describes one wire attempt, not one logical catalog operation.
// Deliberately absent: query strings, userinfo, bodies, authorization, cookies,
// arbitrary headers, and error messages (which frequently contain complete URLs).
type nativeHTTPAttempt struct {
	ID                uint64
	Method            string
	Host              string
	Path              string
	Status            int
	Duration          time.Duration
	RequestID         string
	ExtendedRequestID string
	RetryAfter        string
	TransportError    bool
}

func (a nativeHTTPAttempt) String() string {
	return fmt.Sprintf("iceberg_http attempt_id=%d method=%q host=%q path=%q status=%d duration_ms=%.3f aws_request_id=%q aws_extended_request_id=%q retry_after=%q transport_error=%t",
		a.ID, a.Method, a.Host, a.Path, a.Status, float64(a.Duration)/float64(time.Millisecond), a.RequestID, a.ExtendedRequestID, a.RetryAfter, a.TransportError)
}

var nativeHTTPAttemptID atomic.Uint64

type nativeDiagnosticTransport struct {
	base http.RoundTripper
	emit func(nativeHTTPAttempt)
}

// newNativeHTTPTransport belongs below authentication and SDK retry middleware.
// Each SDK retry therefore produces its own event, including recovered 409/429s.
// It never reads or replaces either body and never changes a signed request.
func newNativeHTTPTransport(base http.RoundTripper, emit func(nativeHTTPAttempt)) http.RoundTripper {
	if base == nil {
		base = http.DefaultTransport
	}
	if emit == nil {
		emit = func(a nativeHTTPAttempt) {
			if a.TransportError || a.Status >= http.StatusBadRequest {
				logger.Warn(a.String())
			} else {
				logger.Info(a.String())
			}
		}
	}
	return &nativeDiagnosticTransport{base: base, emit: emit}
}

func (t *nativeDiagnosticTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	attempt := nativeHTTPAttempt{ID: nativeHTTPAttemptID.Add(1), Method: req.Method, Host: req.URL.Host, Path: req.URL.EscapedPath()}
	started := time.Now()
	response, err := t.base.RoundTrip(req)
	attempt.Duration = time.Since(started)
	attempt.TransportError = err != nil
	if response != nil {
		attempt.Status = response.StatusCode
		attempt.RequestID = response.Header.Get("X-Amz-Request-Id")
		if attempt.RequestID == "" {
			attempt.RequestID = response.Header.Get("X-Amzn-Requestid")
		}
		attempt.ExtendedRequestID = response.Header.Get("X-Amz-Id-2")
		attempt.RetryAfter = response.Header.Get("Retry-After")
	}
	t.emit(attempt)
	return response, err
}

// newNativeHTTPClient supplies the same diagnostic layer to REST, OAuth, Google
// token refresh, Glue, and AWS's default credential chain. The caller owns it.
func newNativeHTTPClient() (*http.Client, func()) {
	transport := &http.Transport{
		Proxy:               http.ProxyFromEnvironment,
		DialContext:         (&net.Dialer{Timeout: 30 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
		ForceAttemptHTTP2:   true,
		MaxIdleConns:        256,
		MaxIdleConnsPerHost: 256,
		IdleConnTimeout:     90 * time.Second,
		TLSHandshakeTimeout: 10 * time.Second,
		// Starts after request upload completes; large S3 bodies are not given
		// a blanket client timeout by this catalog diagnostic transport.
		ResponseHeaderTimeout: 30 * time.Second,
		ExpectContinueTimeout: time.Second,
	}
	return &http.Client{Transport: newNativeHTTPTransport(transport, nil)}, transport.CloseIdleConnections
}

const (
	// The previous Java REST client allowed five retries. Only explicit
	// throttling responses are retried here: 409 needs a metadata reload and
	// an ambiguous POST failure must remain visible to commit recovery.
	nativeRESTThrottleMaxAttempts = 6
	nativeRESTThrottleMaxWait     = time.Minute
)

type nativeRESTThrottleTransport struct {
	base http.RoundTripper
	now  func() time.Time
	wait func(context.Context, time.Duration) error
}

// newNativeRESTTransport wraps an already-instrumented transport. It is used
// only by REST sessions and their authentication clients, never by AWS SDK
// clients, whose retry middleware already owns their wire attempts.
func newNativeRESTTransport(base http.RoundTripper) http.RoundTripper {
	return &nativeRESTThrottleTransport{base: base, now: time.Now, wait: nativeHTTPRetryWait}
}

func (t *nativeRESTThrottleTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	ctx := request.Context()
	current := request
	var waited time.Duration
	for attempt := 1; ; attempt++ {
		if err := ctx.Err(); err != nil {
			if current.Body != nil {
				_ = current.Body.Close()
			}
			return nil, err
		}
		response, err := t.base.RoundTrip(current)
		if err != nil || response == nil || response.StatusCode != http.StatusTooManyRequests || attempt >= nativeRESTThrottleMaxAttempts {
			return response, err
		}
		delay := nativeHTTPThrottleDelay(response.Header.Get("Retry-After"), attempt, t.now())
		if delay > nativeRESTThrottleMaxWait-waited {
			// Never retry before Retry-After. Return the intact final response
			// instead of allowing a server header to create an unbounded wait.
			return response, nil
		}
		var body io.ReadCloser
		if request.Body != nil && request.Body != http.NoBody {
			if request.GetBody == nil {
				return response, nil
			}
			body, err = request.GetBody()
			if err != nil || body == nil {
				// The original throttle is still actionable; do not discard it
				// if the caller cannot provide an exact replay of the request.
				if body != nil {
					_ = body.Close()
				}
				return response, nil
			}
		}
		if response.Body != nil {
			_ = response.Body.Close()
		}
		waited += delay
		if err := t.wait(ctx, delay); err != nil {
			if body != nil {
				_ = body.Close()
			}
			return nil, err
		}
		current = request.Clone(ctx)
		current.Body = body
	}
}

func nativeHTTPRetryWait(ctx context.Context, delay time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if delay <= 0 {
		return nil
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func nativeHTTPThrottleDelay(header string, attempt int, now time.Time) time.Duration {
	header = strings.TrimSpace(header)
	if header != "" {
		if seconds, err := strconv.ParseUint(header, 10, 64); err == nil {
			if seconds > uint64(nativeRESTThrottleMaxWait/time.Second) {
				return nativeRESTThrottleMaxWait + time.Nanosecond
			}
			return time.Duration(seconds) * time.Second
		} else if errors.Is(err, strconv.ErrRange) {
			return nativeRESTThrottleMaxWait + time.Nanosecond
		}
		if deadline, err := http.ParseTime(header); err == nil {
			return max(0, deadline.Sub(now))
		}
	}
	// Match the former client's exponential backoff and sub-10% jitter.
	delay := time.Second << min(max(attempt-1, 0), 6)
	return delay + time.Duration(rand.Int64N(int64(delay/10)))
}

// Only fixed application-authored messages may use this type, never server
// responses, credentials, or user-provided connection strings.
type nativeCatalogConfigError string

func (e nativeCatalogConfigError) Error() string { return string(e) }

// nativeCatalogFailure preserves machine-readable causes without publishing a
// catalog response body, database DSN, OAuth description, or signed URL.
type nativeCatalogFailure struct {
	operation string
	cause     error
}

func (e *nativeCatalogFailure) Error() string {
	category := nativeBackendErrorCategory(e.cause)
	var configError nativeCatalogConfigError
	switch {
	case errors.As(e.cause, &configError):
		category = configError.Error()
	case errors.Is(e.cause, context.Canceled):
		category = "operation canceled"
	case errors.Is(e.cause, context.DeadlineExceeded):
		category = "deadline exceeded"
	case errors.Is(e.cause, table.ErrCommitFailed), errors.Is(e.cause, rest.ErrCommitFailed):
		category = "optimistic commit conflict"
	case errors.Is(e.cause, rest.ErrCommitStateUnknown):
		category = "commit state unknown; inspect the table before retrying"
	case errors.Is(e.cause, rest.ErrUnauthorized):
		category = "catalog authentication rejected (HTTP 401)"
	case errors.Is(e.cause, rest.ErrForbidden):
		category = "catalog access forbidden (HTTP 403)"
	case errors.Is(e.cause, catalog.ErrNoSuchTable):
		category = "table does not exist"
	case errors.Is(e.cause, catalog.ErrNoSuchNamespace):
		category = "namespace does not exist"
	case errors.Is(e.cause, catalog.ErrTableAlreadyExists):
		category = "table already exists"
	case errors.Is(e.cause, catalog.ErrNamespaceAlreadyExists):
		category = "namespace already exists"
	}
	return e.operation + ": " + category
}

func nativeBackendErrorCategory(err error) string {
	var profile awsconfig.SharedConfigProfileNotExistError
	var shared awsconfig.SharedConfigLoadError
	var role awsconfig.SharedConfigAssumeRoleError
	switch {
	case errors.As(err, &profile):
		return "AWS shared profile not found; check aws_profile and shared AWS configuration"
	case errors.As(err, &shared):
		return "AWS shared configuration could not be loaded"
	case errors.As(err, &role):
		return "AWS shared-profile role configuration is invalid"
	}
	var postgres interface{ SQLState() string }
	if errors.As(err, &postgres) {
		state := postgres.SQLState()
		valid := len(state) == 5
		for _, character := range state {
			if (character < '0' || character > '9') && (character < 'A' || character > 'Z') {
				valid = false
				break
			}
		}
		if valid {
			category := "PostgreSQL operation failed"
			switch state[:2] {
			case "28":
				category = "PostgreSQL authentication rejected"
			case "08":
				category = "PostgreSQL connection failed"
			case "3D":
				category = "PostgreSQL database does not exist"
			case "40":
				category = "PostgreSQL transaction conflict"
			}
			return category + " (SQLSTATE " + state + ")"
		}
	}
	var dns *net.DNSError
	if errors.As(err, &dns) {
		return "DNS lookup failed"
	}
	var network net.Error
	if errors.As(err, &network) && network.Timeout() {
		return "network timeout"
	}
	switch {
	case errors.Is(err, syscall.ECONNREFUSED):
		return "network connection refused"
	case errors.Is(err, syscall.ECONNRESET):
		return "network connection reset"
	case errors.Is(err, io.EOF), errors.Is(err, io.ErrUnexpectedEOF):
		return "connection closed unexpectedly"
	}
	var operation *net.OpError
	if errors.As(err, &operation) {
		switch operation.Op {
		case "dial":
			return "network connection failed"
		case "read":
			return "network read failed"
		case "write":
			return "network write failed"
		default:
			return "network operation failed"
		}
	}
	return "operation failed (details redacted)"
}

func (e *nativeCatalogFailure) Unwrap() error { return e.cause }

func nativeCatalogError(operation string, err error) error {
	if err == nil {
		return nil
	}
	return &nativeCatalogFailure{operation: operation, cause: err}
}
