package iceberg

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
	"net"
	"net/http"
	"strings"
	"syscall"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNativeTransportErrorClass(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
		want string
	}{
		{name: "nil", err: nil, want: ""},
		{name: "canceled", err: context.Canceled, want: "context_canceled"},
		{name: "deadline", err: context.DeadlineExceeded, want: "context_deadline"},
		{name: "reset", err: syscall.ECONNRESET, want: "connection_reset"},
		{name: "refused", err: syscall.ECONNREFUSED, want: "connection_refused"},
		{name: "eof", err: io.EOF, want: "eof"},
		{name: "unexpected eof", err: io.ErrUnexpectedEOF, want: "unexpected_eof"},
		{name: "dns", err: &net.DNSError{Err: "no such host", Name: "example.invalid"}, want: "dns"},
		{name: "tls record", err: &tls.RecordHeaderError{Msg: "bad record"}, want: "tls"},
		{name: "timeout", err: &net.OpError{Op: "read", Err: timeoutError{}}, want: "timeout"},
		{name: "unknown", err: errors.New("boom"), want: "unknown"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, nativeTransportErrorClass(tc.err))
		})
	}
}

type timeoutError struct{}

func (timeoutError) Error() string   { return "i/o timeout" }
func (timeoutError) Timeout() bool   { return true }
func (timeoutError) Temporary() bool { return true }

func TestNativeResponseErrorCodeUsesHeadersOnly(t *testing.T) {
	resp := &http.Response{
		StatusCode: http.StatusServiceUnavailable,
		Header:     http.Header{"X-Amzn-Errortype": {"ServiceUnavailableException"}},
		Body:       io.NopCloser(strings.NewReader("<Error><Code>ignored</Code></Error>")),
	}
	require.Equal(t, "ServiceUnavailableException", nativeResponseErrorCode(resp))

	restored, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Equal(t, "<Error><Code>ignored</Code></Error>", string(restored), "the transport must not consume the body")

	success := &http.Response{StatusCode: http.StatusOK, Header: http.Header{"X-Amzn-Errortype": {"stale"}}}
	require.Empty(t, nativeResponseErrorCode(success))
}

func TestNativeDiagnosticTransportRecordsFailureDetail(t *testing.T) {
	var attempts []nativeHTTPAttempt
	transport := newNativeHTTPTransport(nativeRoundTripFunc(func(req *http.Request) (*http.Response, error) {
		if strings.Contains(req.URL.Path, "transport") {
			return nil, &net.OpError{Op: "dial", Net: "tcp", Err: syscall.ECONNRESET}
		}
		return &http.Response{
			StatusCode: http.StatusServiceUnavailable,
			Header:     http.Header{"X-Amz-Request-Id": {"req-1"}, "X-Amz-Error-Code": {"SlowDown"}},
			Body:       io.NopCloser(strings.NewReader(`<Error><Code>SlowDown</Code></Error>`)),
		}, nil
	}), func(a nativeHTTPAttempt) { attempts = append(attempts, a) })

	_, err := transport.RoundTrip(mustRequest(t, "https://s3tables.us-east-2.amazonaws.com/iceberg/v1/transport"))
	require.Error(t, err)

	resp, err := transport.RoundTrip(mustRequest(t, "https://s3tables.us-east-2.amazonaws.com/iceberg/v1/commit"))
	require.NoError(t, err)
	require.Equal(t, http.StatusServiceUnavailable, resp.StatusCode)

	require.Len(t, attempts, 2)
	require.True(t, attempts[0].TransportError)
	require.Equal(t, "connection_reset", attempts[0].ErrorClass)
	require.Equal(t, "SlowDown", attempts[1].ResponseCode)
	require.Contains(t, attempts[0].String(), `error_class="connection_reset"`)
	require.Contains(t, attempts[1].String(), `resp_code="SlowDown"`)
}

func mustRequest(t *testing.T, url string) *http.Request {
	t.Helper()
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, url, nil)
	require.NoError(t, err)
	return req
}
