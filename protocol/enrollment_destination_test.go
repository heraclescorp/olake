package protocol

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/s3tables"
	"github.com/datazip-inc/olake/types"
)

const enrollmentTestBucketARN = "arn:aws:s3tables:us-east-1:111122223333:bucket/enrollment-smoke"
const enrollmentNativeExistingResponse = `{"createdAt":"2000-01-01T00:00:00Z","createdBy":"111122223333","format":"ICEBERG","modifiedAt":"2000-01-01T00:00:00Z","modifiedBy":"111122223333","name":"new_table","namespace":["enrollment_smoke"],"ownerAccountId":"111122223333","tableARN":"arn:aws:s3tables:us-east-1:111122223333:bucket/enrollment-smoke/table/unrelated","type":"customer","versionToken":"fixture-version","warehouseLocation":"s3://fixture/warehouse"}`

func nativeEnrollmentTestWriter() enrollmentWriterConfig {
	return enrollmentWriterConfig{
		CatalogType: "s3tables", IcebergS3Path: enrollmentTestBucketARN,
		AccessKey: "fixture-access", SecretKey: "fixture-secret", SessionToken: "fixture-session",
	}
}

func TestNativeEnrollmentDestinationHTTPBoundary(t *testing.T) {
	for _, tc := range []struct {
		name       string
		status     int
		body, want string
	}{
		{name: "absent", status: http.StatusNotFound, body: `{"__type":"NotFoundException","message":"not found"}`},
		{name: "unrelated table exists", status: http.StatusOK, body: enrollmentNativeExistingResponse, want: "already exists"},
		{name: "denied", status: http.StatusForbidden, body: `{"__type":"AccessDeniedException","message":"sensitive service details"}`, want: "access denied"},
		{name: "unauthenticated", status: http.StatusUnauthorized, body: `{"message":"credential details"}`, want: "access denied"},
		{name: "unavailable", status: http.StatusInternalServerError, body: `{"message":"internal service details"}`, want: "HTTP 500"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("AWS_PROFILE", "")
			var calls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				if r.Method != http.MethodGet || r.URL.Path != "/get-table" {
					t.Errorf("metadata lookup must use only native GetTable GET: %s %s", r.Method, r.URL.Path)
				}
				if r.URL.Query().Get("tableBucketARN") != enrollmentTestBucketARN || r.URL.Query().Get("namespace") != "enrollment_smoke" || r.URL.Query().Get("name") != "new_table" {
					t.Errorf("wrong effective destination lookup: %s", r.URL.RawQuery)
				}
				if !strings.Contains(r.Header.Get("Authorization"), "/us-east-1/s3tables/aws4_request") || r.Header.Get("X-Amz-Security-Token") != "fixture-session" {
					t.Error("lookup must use SDK SigV4 and configured session credentials")
				}
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(tc.status)
				_, _ = w.Write([]byte(tc.body))
			}))
			defer server.Close()
			guard := newNativeEnrollmentDestination(context.Background(), nativeEnrollmentTestWriter(), func(options *s3tables.Options) { options.BaseEndpoint = new(server.URL); options.RetryMaxAttempts = 1 })
			reason := guard.skipReason(context.Background(), "enrollment_smoke", "new_table")
			if tc.want == "" {
				if reason != "" {
					t.Fatalf("404 must approve absent table: %s", reason)
				}
			} else if !strings.Contains(reason, tc.want) {
				t.Fatalf("wanted %q, got %q", tc.want, reason)
			}
			if calls.Load() != 1 {
				t.Fatalf("expected exactly one metadata GET, got %d", calls.Load())
			}
			if strings.Contains(reason, "sensitive") || strings.Contains(reason, "credential details") || strings.Contains(reason, "internal service") {
				t.Fatal("skip reason must not disclose AWS response bodies")
			}
		})
	}
}

type enrollmentFailedGetTable struct{}

func (enrollmentFailedGetTable) GetTable(context.Context, *s3tables.GetTableInput, ...func(*s3tables.Options)) (*s3tables.GetTableOutput, error) {
	return nil, errors.New("transport error containing private details")
}

func TestNativeEnrollmentDestinationUnavailableFailsClosed(t *testing.T) {
	guard := &nativeEnrollmentDestination{client: enrollmentFailedGetTable{}, bucketARN: enrollmentTestBucketARN}
	reason := guard.skipReason(context.Background(), "enrollment_smoke", "new_table")
	if reason == "" || strings.Contains(reason, "private details") {
		t.Fatalf("unknown lookup error must skip without leaking details: %q", reason)
	}
	writer := nativeEnrollmentTestWriter()
	writer.IcebergS3Path = "s3://not-a-table-bucket-arn/warehouse"
	invalid := newNativeEnrollmentDestination(context.Background(), writer)
	if reason := invalid.skipReason(context.Background(), "enrollment_smoke", "new_table"); !strings.Contains(reason, "warehouse ARN") {
		t.Fatalf("unresolvable bucket must fail closed: %q", reason)
	}
	writer = nativeEnrollmentTestWriter()
	writer.RestSigningRegion = "us-west-2"
	if reason := newNativeEnrollmentDestination(context.Background(), writer).skipReason(context.Background(), "enrollment_smoke", "new_table"); !strings.Contains(reason, "signing region") {
		t.Fatalf("wrong signing region must fail closed: %q", reason)
	}
}

func TestNativeEnrollmentDestinationDetection(t *testing.T) {
	for _, writer := range []enrollmentWriterConfig{
		{CatalogType: "s3tables"},
		{RestSigningName: "s3tables"},
		{IcebergS3Path: enrollmentTestBucketARN},
		{IcebergS3Path: "arn:aws-cn:s3tables:cn-north-1:111122223333:bucket/enrollment-smoke"},
		{RestCatalogURL: "https://s3tables.us-east-1.amazonaws.com/iceberg"},
		{RestCatalogURL: "https://s3tables-fips.us-east-1.amazonaws.com/iceberg"},
	} {
		if !writer.nativeS3Tables() {
			t.Fatalf("native service must not bypass collision checks: %+v", writer)
		}
	}
	if (enrollmentWriterConfig{CatalogType: "rest", RestCatalogURL: "http://iceberg-fixture.invalid", IcebergS3Path: "s3://enrollment-smoke/warehouse"}).nativeS3Tables() {
		t.Fatal("generic REST should retain the documented reserved-namespace prerequisite")
	}
}

func TestEnrollmentNativeDestinationLookupBeforeCatalogPersistence(t *testing.T) {
	for _, tc := range []struct {
		name         string
		status       int
		wantEnrolled int
	}{
		{name: "absent", status: http.StatusNotFound, wantEnrolled: 1},
		{name: "existing unrelated table", status: http.StatusOK},
		{name: "permission denied", status: http.StatusForbidden},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodGet || r.URL.Path != "/get-table" || r.URL.Query().Get("namespace") != "warehouse_public" || r.URL.Query().Get("name") != "native_new" {
					t.Errorf("lookup did not use actual runtime destination: %s", r.URL)
				}
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(tc.status)
				if tc.status == http.StatusNotFound {
					_, _ = w.Write([]byte(`{"__type":"NotFoundException","message":"absent"}`))
				} else if tc.status == http.StatusForbidden {
					_, _ = w.Write([]byte(`{"__type":"AccessDeniedException","message":"denied"}`))
				} else {
					_, _ = w.Write([]byte(enrollmentNativeExistingResponse))
				}
			}))
			defer server.Close()
			t.Setenv("AWS_ENDPOINT_URL_S3TABLES", server.URL)
			t.Setenv("AWS_IGNORE_CONFIGURED_ENDPOINT_URLS", "false")
			t.Setenv("AWS_PROFILE", "")
			destination := &types.WriterConfig{Type: types.Iceberg, WriterConfig: nativeEnrollmentTestWriter()}
			saved := enrollmentTestCatalog(t, false, enrollmentSavedFixture)
			source := &enrollmentTestSource{names: []types.StreamID{{Namespace: "public", Name: "native_new"}}}
			result, err := discoverNewTables(context.Background(), source, saved, enrollmentTestPolicy(), destination, nil)
			if err != nil {
				t.Fatal(err)
			}
			if len(result.Enrolled) != tc.wantEnrolled {
				t.Fatalf("unexpected native enrollment: %+v", result)
			}
			if tc.wantEnrolled == 0 && (len(result.Skipped) != 1 || result.StreamsConfig != enrollmentSavedFixture || len(saved.streams) != 2 || len(saved.selections["public"]) != 1) {
				t.Fatal("existence/permission failure must never add a definition or selection")
			}
		})
	}
}
