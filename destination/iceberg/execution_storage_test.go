package iceberg

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/require"
)

func TestNativeS3UploadRetryPreservesCatalogAdmission(t *testing.T) {
	for _, failure := range []string{"lost-response", "service-unavailable", "fence-closed"} {
		t.Run(failure, func(t *testing.T) {
			ctx, config, fence := nativeExecutionFixture(t)
			var uploads, commits atomic.Int32
			var stored atomic.Value
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method == http.MethodPost && r.URL.Path == "/v1/namespaces/db/tables/t" {
					commits.Add(1)
					w.WriteHeader(http.StatusOK)
					_, _ = io.WriteString(w, "{}")
					return
				}
				if r.Method == http.MethodGet {
					value, _ := stored.Load().(string)
					_, _ = io.WriteString(w, value)
					return
				}
				data, err := io.ReadAll(r.Body)
				if err != nil {
					t.Errorf("read upload body: %v", err)
					return
				}
				stored.Store(string(data))
				if uploads.Add(1) == 1 {
					if failure == "service-unavailable" {
						w.WriteHeader(http.StatusServiceUnavailable)
						_, _ = io.WriteString(w, `<Error><Code>ServiceUnavailable</Code></Error>`)
						return
					}
					if failure == "fence-closed" {
						closeNativeTestFence(t, fence)
					}
					connection, _, err := w.(http.Hijacker).Hijack()
					if err != nil {
						t.Errorf("hijack upload response: %v", err)
						return
					}
					_ = connection.Close()
					return
				}
				w.WriteHeader(http.StatusOK)
			}))
			defer server.Close()
			client, closeClient := newNativeHTTPClient(true)
			defer closeClient()
			client.Transport = newNativeAWSExecutionTransport(client.Transport, config.execution, config)
			storage := s3.NewFromConfig(aws.Config{
				Region: "us-east-2", HTTPClient: client,
				Credentials: credentials.NewStaticCredentialsProvider("local-test", "local-test", ""),
				Retryer: func() aws.Retryer {
					return retry.NewStandard(func(options *retry.StandardOptions) {
						options.MaxAttempts = 2
						options.Backoff = retry.BackoffDelayerFunc(func(int, error) (time.Duration, error) { return 0, nil })
					})
				},
			}, func(options *s3.Options) { options.BaseEndpoint = aws.String(server.URL); options.UsePathStyle = true })
			const payload = "complete-immutable-test-object"
			_, err := storage.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String("local-bucket"), Key: aws.String("data/test.parquet"), Body: strings.NewReader(payload)})
			if failure == "fence-closed" {
				require.Error(t, err)
				require.EqualValues(t, 1, uploads.Load(), "closed fence must reject the SDK retry before network dispatch")
				require.Zero(t, commits.Load())
				return
			}
			require.NoError(t, err)
			require.EqualValues(t, 2, uploads.Load())
			object, err := storage.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String("local-bucket"), Key: aws.String("data/test.parquet")})
			require.NoError(t, err)
			data, err := io.ReadAll(object.Body)
			require.NoError(t, err)
			require.NoError(t, object.Body.Close())
			require.Equal(t, payload, string(data))
			catalogTransport := newNativeRESTTransport(client.Transport).(*nativeRESTThrottleTransport)
			catalogTransport.execution = config.execution
			catalogClient := &http.Client{Transport: catalogTransport}
			request, err := http.NewRequestWithContext(ctx, http.MethodPost, server.URL+"/v1/namespaces/db/tables/t", strings.NewReader(`{"updates":[]}`))
			require.NoError(t, err)
			response, err := catalogClient.Do(request)
			require.NoError(t, err)
			_, err = io.Copy(io.Discard, response.Body)
			require.NoError(t, err)
			require.NoError(t, response.Body.Close())
			require.Equal(t, http.StatusOK, response.StatusCode)
			require.EqualValues(t, 1, commits.Load())
		})
	}
}

func TestNativeStorageResponseMustDrainBeforeRetirement(t *testing.T) {
	ctx, config, _ := nativeExecutionFixture(t)
	started := make(chan struct{})
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.WriteHeader(http.StatusOK)
		w.(http.Flusher).Flush()
		close(started)
		<-release
		_, _ = io.WriteString(w, "complete")
	}))
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
		server.Close()
	}()
	client, closeClient := newNativeHTTPClient(true)
	defer closeClient()
	config.S3Endpoint = server.URL
	client.Transport = newNativeAWSExecutionTransport(client.Transport, config.execution, config)
	request, err := http.NewRequestWithContext(ctx, http.MethodPut, server.URL+"/data/immutable", strings.NewReader("payload"))
	require.NoError(t, err)
	response, err := client.Do(request)
	require.NoError(t, err)
	<-started
	retired := make(chan error, 1)
	catalog := &nativeCatalog{Catalog: &nativeExecutionTestCatalog{}, execution: config.execution}
	go func() { retired <- catalog.Close() }()
	select {
	case err := <-retired:
		t.Fatalf("retirement completed before storage response drained: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	data, err := io.ReadAll(response.Body)
	require.NoError(t, err)
	require.Equal(t, "complete", string(data))
	require.NoError(t, response.Body.Close())
	select {
	case err := <-retired:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("retirement did not finish after storage response drained")
	}
}
