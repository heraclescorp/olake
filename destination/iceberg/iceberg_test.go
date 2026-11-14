package iceberg

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"maps"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	parquetfile "github.com/apache/arrow-go/v18/parquet/file"
	apacheiceberg "github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/catalog/hadoop"
	"github.com/apache/iceberg-go/catalog/rest"
	icesql "github.com/apache/iceberg-go/catalog/sql"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
	iceutils "github.com/apache/iceberg-go/utils"
	"github.com/apache/thrift/lib/go/thrift"
	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awsglue "github.com/aws/aws-sdk-go-v2/service/glue"
	"github.com/beltran/gohive/hive_metastore"
	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/destination"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/oauth2"
)

func TestIsValidTypeForColumn(t *testing.T) {
	testCases := []struct {
		name       string
		columnType string
		valueType  string
		valid      bool
	}{
		{
			name:       "binary column accepts bytes",
			columnType: "binary",
			valueType:  "binary",
			valid:      true,
		},
		{
			name:       "fixed column accepts fixed bytes of the same width",
			columnType: "fixed[16]",
			valueType:  "fixed[16]",
			valid:      true,
		},
		{
			name:       "fixed column accepts width-less bytes, since a value cannot reveal a width",
			columnType: "fixed[16]",
			valueType:  "binary",
			valid:      true,
		},
		{
			name:       "binary column accepts fixed bytes of any width",
			columnType: "binary",
			valueType:  "fixed[16]",
			valid:      true,
		},
		{
			name:       "fixed column rejects a different width",
			columnType: "fixed[16]",
			valueType:  "fixed[32]",
			valid:      false,
		},
		{
			name:       "string column rejects bytes",
			columnType: "string",
			valueType:  "binary",
			valid:      false,
		},
		{
			name:       "binary column accepts text as its bytes",
			columnType: "binary",
			valueType:  "string",
			valid:      true,
		},
		{
			name:       "fixed column rejects text",
			columnType: "fixed[16]",
			valueType:  "string",
			valid:      false,
		},
		{
			name:       "iceberg promotes int to long",
			columnType: "int",
			valueType:  "long",
			valid:      true,
		},
		{
			name:       "an int fits a long column as it is",
			columnType: "long",
			valueType:  "int",
			valid:      true,
		},
		{
			name:       "float column accepts double",
			columnType: "float",
			valueType:  "double",
			valid:      true,
		},
		{
			name:       "double column accepts float",
			columnType: "double",
			valueType:  "float",
			valid:      true,
		},
		{
			name:       "anything fits a string column",
			columnType: "string",
			valueType:  "int",
			valid:      true,
		},
		{
			name:       "int column rejects string",
			columnType: "int",
			valueType:  "string",
			valid:      false,
		},
		{
			name:       "boolean column rejects int",
			columnType: "boolean",
			valueType:  "int",
			valid:      false,
		},
		{
			name:       "timestamptz column accepts timestamptz",
			columnType: "timestamptz",
			valueType:  "timestamptz",
			valid:      true,
		},
		{
			name:       "timestamptz column rejects string",
			columnType: "timestamptz",
			valueType:  "string",
			valid:      false,
		},
		{
			name:       "a column type olake does not map keeps its own value type and rejects string",
			columnType: "date",
			valueType:  "string",
			valid:      false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.valid, isValidTypeForColumn(tc.columnType, tc.valueType))
		})
	}
}

// chunkWriter keeps the ids of the records each Write call received.
type chunkWriter struct {
	Writer
	calls [][]int
}

func (w *chunkWriter) Write(_ context.Context, records []types.RawRecord) error {
	ids := []int{}
	for _, record := range records {
		ids = append(ids, record.Data["id"].(int))
	}
	w.calls = append(w.calls, ids)
	return nil
}

// A batch grown past one message by recovered values must reach the writer in order, in
// chunks under MaxDestinationBatchBytes, with a record larger than that sent on its own. A
// batch nothing was recovered into goes in one call, as before.
func TestWriteChunksBySize(t *testing.T) {
	// counted as mb megabytes while holding one: valueSize sums every element of an array
	mb := strings.Repeat("x", 1024*1024)
	record := func(id, megabytes int) types.RawRecord {
		value := make([]any, megabytes)
		for idx := range value {
			value[idx] = mb
		}
		return types.RawRecord{Data: map[string]any{"id": id, "payload": value}}
	}
	// sized from the limit: two parts fit in one chunk, three do not, and a large record exceeds it alone
	limitMB := int(constants.MaxDestinationBatchBytes / (1024 * 1024))
	part := limitMB * 2 / 5
	large := limitMB + limitMB/2

	overLimit := []types.RawRecord{record(0, part), record(1, part), record(2, part), record(3, part), record(4, part)}

	testCases := []struct {
		name     string
		resolver *unavailableValuesResolver // nil: the stream recovers nothing
		records  []types.RawRecord
		expected [][]int
	}{
		{
			name:     "an empty batch is one call",
			resolver: &unavailableValuesResolver{filledBytes: 1},
			records:  []types.RawRecord{},
			expected: [][]int{{}},
		},
		{
			name:     "a batch under the limit is one call",
			resolver: &unavailableValuesResolver{filledBytes: 1},
			records:  []types.RawRecord{record(0, part), record(1, part)},
			expected: [][]int{{0, 1}},
		},
		{
			name:     "a batch over the limit is split in order",
			resolver: &unavailableValuesResolver{filledBytes: 1},
			records:  overLimit,
			expected: [][]int{{0, 1}, {2, 3}, {4}},
		},
		{
			name:     "a record over the limit goes alone",
			resolver: &unavailableValuesResolver{filledBytes: 1},
			records:  []types.RawRecord{record(0, 1), record(1, large), record(2, 1)},
			expected: [][]int{{0}, {1}, {2}},
		},
		{
			name:     "a batch nothing was recovered into is one call",
			resolver: &unavailableValuesResolver{},
			records:  overLimit,
			expected: [][]int{{0, 1, 2, 3, 4}},
		},
		{
			name:     "a stream without a resolver is one call",
			records:  overLimit,
			expected: [][]int{{0, 1, 2, 3, 4}},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			writer := &chunkWriter{}
			require.NoError(t, (&Iceberg{writer: writer, unavailableValues: tc.resolver}).Write(context.Background(), tc.records))
			assert.Equal(t, tc.expected, writer.calls)
		})
	}
}

// A value the source could not send must never reach the writer as the placeholder: neither
// on a stream without a row index (eq) nor when the index has no previous version of the row.
func TestFlattenAndCleanDataRejectsUnrecoveredValues(t *testing.T) {
	testCases := []struct {
		name     string
		resolver func(t *testing.T) *unavailableValuesResolver
	}{
		{
			name:     "a stream without a row index",
			resolver: func(*testing.T) *unavailableValuesResolver { return nil },
		},
		{
			name: "a row the index has no previous version of",
			resolver: func(t *testing.T) *unavailableValuesResolver {
				return testResolver(t, true, &testWriter{locations: map[string]types.RowLocation{}}, &testReader{table: testTable{}})
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			stream := testConfiguredStream(true)
			stream.Stream = types.NewStream("toast_table", "public", nil)
			iceberg := &Iceberg{stream: stream, unavailableValues: tc.resolver(t)}
			records := []types.RawRecord{testRecord("42", "u", map[string]any{"id": 42, "payload": placeholder}, "payload")}

			_, written, _, err := iceberg.FlattenAndCleanData(context.Background(), records)

			require.ErrorContains(t, err, `unchanged TOAST column "payload"`)
			assert.Nil(t, written)
		})
	}
}

func nativeTestConfig(t *testing.T) *Config {
	t.Helper()
	t.Setenv("AWS_EC2_METADATA_DISABLED", "true")
	t.Setenv("AWS_PROFILE", "")
	t.Setenv("AWS_DEFAULT_PROFILE", "")
	t.Setenv("AWS_ENDPOINT_URL", "")
	t.Setenv("AWS_ENDPOINT_URL_GLUE", "")
	return &Config{CatalogType: RestCatalog, CatalogName: "olake_iceberg", Region: "us-east-1", AccessKey: "test-access", SecretKey: "test-secret", IcebergS3Path: "s3://warehouse/prefix"}
}

func TestNativeStorageProperties(t *testing.T) {
	config := &Config{Region: "storage-region", AccessKey: "access", SecretKey: "secret", SessionToken: "session", S3Endpoint: "minio.example:9000", S3UseSSL: true, GCPServiceAccountJSON: "inline-json"}
	props, err := nativeStorageProperties(config)
	if err != nil {
		t.Fatal(err)
	}
	for key, expected := range map[string]string{iceio.S3Region: "storage-region", iceio.S3AccessKeyID: "access", iceio.S3SecretAccessKey: "secret", iceio.S3SessionToken: "session", iceio.S3EndpointURL: "https://minio.example:9000", iceio.S3ForceVirtualAddressing: "false", iceio.S3CompatMode: "true", iceio.GCSJSONKey: "inline-json"} {
		if props[key] != expected {
			t.Fatalf("property %s: got %q, expected %q", key, props[key], expected)
		}
	}
	config.S3Endpoint = "http://minio.example:9000"
	props, err = nativeStorageProperties(config)
	if err != nil || props[iceio.S3EndpointURL] != config.S3Endpoint {
		t.Fatal("overrode explicit endpoint scheme")
	}
	for _, endpoint := range []string{"file:///tmp/secret", "://secret", "https://"} {
		config.S3Endpoint = endpoint
		_, err := nativeStorageProperties(config)
		if err == nil || strings.Contains(err.Error(), "secret") {
			t.Fatal("accepted invalid endpoint or exposed it")
		}
	}
	endpoint := "https://user:secret@catalog.example/oauth/token?audience=private"
	parsed, err := nativeHTTPURL(endpoint)
	if err != nil || parsed.String() != endpoint {
		t.Fatal("changed configured endpoint authentication or query parameters")
	}
}

func TestNativeStorageRejectsPartialCredentials(t *testing.T) {
	for _, config := range []*Config{{AccessKey: "access-secret"}, {SecretKey: "secret-value"}, {SessionToken: "session-secret"}} {
		_, err := nativeStorageProperties(config)
		if err == nil || strings.Contains(err.Error(), "secret-value") || strings.Contains(err.Error(), "access-secret") || strings.Contains(err.Error(), "session-secret") {
			t.Fatal("accepted partial credentials or leaked them")
		}
	}
}

func TestNativeAWSDefaultChainAndExplicitCredentials(t *testing.T) {
	config := nativeTestConfig(t)
	t.Setenv("AWS_ACCESS_KEY_ID", "environment-key")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "environment-secret")
	t.Setenv("AWS_SESSION_TOKEN", "environment-token")
	t.Setenv("AWS_REGION", "environment-region")
	config.Region, config.AccessKey, config.SecretKey = "", "", ""
	cfg, err := nativeAWSConfig(context.Background(), config, false, http.DefaultClient)
	if err != nil {
		t.Fatal(err)
	}
	creds, err := cfg.Credentials.Retrieve(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Region != "environment-region" || creds.AccessKeyID != "environment-key" || creds.SessionToken != "environment-token" {
		t.Fatal("lost AWS default chain")
	}
	config.AccessKey, config.SecretKey, config.SessionToken, config.Region = "explicit-key", "explicit-secret", "explicit-token", "explicit-region"
	cfg, err = nativeAWSConfig(context.Background(), config, false, http.DefaultClient)
	if err != nil {
		t.Fatal(err)
	}
	creds, err = cfg.Credentials.Retrieve(context.Background())
	if err != nil || cfg.Region != "explicit-region" || creds.AccessKeyID != "explicit-key" || creds.SessionToken != "explicit-token" {
		t.Fatal("explicit credentials did not override chain")
	}
}

func TestNativeAWSNamedProfile(t *testing.T) {
	config := nativeTestConfig(t)
	dir := t.TempDir()
	credentialsFile := filepath.Join(dir, "credentials")
	configFile := filepath.Join(dir, "config")
	if err := os.WriteFile(credentialsFile, []byte("[native-profile]\naws_access_key_id = profile-key\naws_secret_access_key = profile-secret\naws_session_token = profile-token\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(configFile, []byte("[profile native-profile]\nregion = profile-region\n"), 0600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("AWS_SHARED_CREDENTIALS_FILE", credentialsFile)
	t.Setenv("AWS_CONFIG_FILE", configFile)
	t.Setenv("AWS_ACCESS_KEY_ID", "")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "")
	t.Setenv("AWS_SESSION_TOKEN", "")
	t.Setenv("AWS_REGION", "")
	t.Setenv("AWS_DEFAULT_REGION", "")
	config.ProfileName, config.Region, config.AccessKey, config.SecretKey = "native-profile", "", "", ""
	cfg, err := nativeAWSConfig(context.Background(), config, false, http.DefaultClient)
	if err != nil {
		t.Fatal(err)
	}
	creds, err := cfg.Credentials.Retrieve(context.Background())
	if err != nil || cfg.Region != "profile-region" || creds.AccessKeyID != "profile-key" || creds.SessionToken != "profile-token" {
		t.Fatal("named profile not used")
	}
}

func TestNativeGlueOverridesDoNotChangeStorageConfig(t *testing.T) {
	config := nativeTestConfig(t)
	config.CatalogType, config.UseGlueAdditionalConfig = GlueCatalog, true
	config.GlueAccessKey, config.GlueSecretKey, config.GlueRegion = "glue-key", "glue-secret", "eu-west-1"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.Contains(r.Header.Get("Authorization"), "Credential=glue-key/") || !strings.Contains(r.Header.Get("Authorization"), "/eu-west-1/glue/aws4_request") {
			t.Error("Glue overrides were not scoped to the Glue client")
		}
		w.Header().Set("Content-Type", "application/x-amz-json-1.1")
		_, _ = w.Write([]byte(`{"DatabaseList":[]}`))
	}))
	defer server.Close()
	config.GlueEndpoint = server.URL
	cat, cleanup, err := openNativeCatalog(context.Background(), config)
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup(context.Background())
	if _, err := cat.ListNamespaces(context.Background(), nil); err != nil {
		t.Fatal(err)
	}
	storage := iceutils.GetAwsConfig(nativeCatalogContext(context.Background(), cat))
	if storage == nil {
		t.Fatal("missing storage context")
	}
	creds, err := storage.Credentials.Retrieve(context.Background())
	if err != nil || creds.AccessKeyID != "test-access" || storage.Region != "us-east-1" || storage.BaseEndpoint != nil {
		t.Fatal("Glue override contaminated storage AWS configuration")
	}
	glueCfg, err := nativeAWSConfig(context.Background(), config, true, http.DefaultClient)
	if err != nil {
		t.Fatal(err)
	}
	creds, err = glueCfg.Credentials.Retrieve(context.Background())
	if err != nil || creds.AccessKeyID != "glue-key" || glueCfg.Region != "eu-west-1" || aws.ToString(glueCfg.BaseEndpoint) != config.GlueEndpoint {
		t.Fatal("Glue override was not applied")
	}
}

func TestNativeRESTFactoryTokenAndDelegation(t *testing.T) {
	config := nativeTestConfig(t)
	config.RestToken, config.RestAuthType = "catalog-token", "oauth2"
	config.RESTAccessDelegation, config.GCPProjectID = "vended-credentials", "billing-project"
	var paths []string
	var pathsMu sync.Mutex
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		pathsMu.Lock()
		paths = append(paths, r.URL.Path)
		pathsMu.Unlock()
		if r.Header.Get("Authorization") != "Bearer catalog-token" || r.Header.Get("X-Iceberg-Access-Delegation") != "vended-credentials" || r.Header.Get("X-Goog-User-Project") != "billing-project" {
			t.Error("catalog authentication or routing headers missing")
		}
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/catalog/v1/config":
			if r.URL.Query().Get("warehouse") != config.IcebergS3Path {
				t.Error("lost warehouse")
			}
			_, _ = w.Write([]byte(`{"defaults":{},"overrides":{}}`))
		case "/catalog/v1/namespaces":
			_, _ = w.Write([]byte(`{"namespaces":[["test"]]}`))
		default:
			t.Errorf("unexpected catalog path: %s", r.URL.Path)
			w.WriteHeader(404)
		}
	}))
	defer server.Close()
	config.RestCatalogURL = server.URL + "/catalog"
	cat, cleanup, err := openNativeCatalog(context.Background(), config)
	if err != nil {
		t.Fatal(err)
	}
	namespaces, err := cat.ListNamespaces(context.Background(), nil)
	if err != nil || len(namespaces) != 1 || namespaces[0][0] != "test" {
		t.Fatalf("catalog not usable: %v, %v", namespaces, err)
	}
	cleanup(context.Background())
	cleanup(context.Background())
	pathsMu.Lock()
	defer pathsMu.Unlock()
	if len(paths) != 2 {
		t.Fatalf("unexpected requests: %v", paths)
	}
}

func TestNativeRESTOAuthUsesConfiguredEndpointAndScope(t *testing.T) {
	config := nativeTestConfig(t)
	config.RestAuthType, config.RestCredential, config.RestScope = "oauth2", "client:client-secret", "catalog offline_access"
	var mu sync.Mutex
	tokenRequests, catalogRequests := 0, 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path == "/oauth/token" {
			if err := r.ParseForm(); err != nil {
				t.Error(err)
			}
			if r.Form.Get("grant_type") != "client_credentials" || r.Form.Get("scope") != config.RestScope || r.Form.Get("client_id") != "client" || r.Form.Get("client_secret") != "client-secret" {
				t.Error("wrong OAuth request")
			}
			mu.Lock()
			tokenRequests++
			mu.Unlock()
			// oauth2 considers this expired immediately (its refresh safety
			// margin exceeds one second), so refresh tests require no sleep.
			_, _ = w.Write([]byte(`{"access_token":"issued-token","token_type":"Bearer","expires_in":1}`))
			return
		}
		if r.Header.Get("Authorization") != "Bearer issued-token" {
			t.Error("OAuth token missing from catalog request")
		}
		mu.Lock()
		catalogRequests++
		mu.Unlock()
		if strings.HasSuffix(r.URL.Path, "/config") {
			_, _ = w.Write([]byte(`{"defaults":{},"overrides":{}}`))
		} else {
			_, _ = w.Write([]byte(`{"namespaces":[]}`))
		}
	}))
	defer server.Close()
	config.RestCatalogURL, config.RestOAuthURI = server.URL, server.URL+"/oauth/token"
	initCtx, cancelInit := context.WithCancel(context.Background())
	defer cancelInit()
	cat, cleanup, err := openNativeCatalog(initCtx, config)
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup(context.Background())
	cancelInit()
	for range 2 {
		if _, err := cat.ListNamespaces(context.Background(), nil); err != nil {
			t.Fatal(err)
		}
	}
	mu.Lock()
	defer mu.Unlock()
	if tokenRequests < 3 || catalogRequests != 3 {
		t.Fatal("OAuth refresh did not survive initialization-context cancellation")
	}
}

func TestNativeRESTSigV4SigningConfiguration(t *testing.T) {
	config := nativeTestConfig(t)
	config.RestSigningV4, config.RestSigningName, config.RestSigningRegion = true, "s3tables", "eu-west-1"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		auth := r.Header.Get("Authorization")
		if !strings.HasPrefix(auth, "AWS4-HMAC-SHA256 ") || !strings.Contains(auth, "/eu-west-1/s3tables/aws4_request") || !strings.Contains(auth, "Credential=test-access/") {
			t.Error("wrong SigV4 configuration")
		}
		if r.Header.Get("X-Amz-Content-Sha256") == "" {
			t.Error("request is not payload-signed")
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"defaults":{},"overrides":{}}`))
	}))
	defer server.Close()
	config.RestCatalogURL = server.URL
	_, cleanup, err := openNativeCatalog(context.Background(), config)
	if err != nil {
		t.Fatal(err)
	}
	cleanup(context.Background())
}

func TestNativeGoogleAuthRefreshAndCancellation(t *testing.T) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	keyBytes, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		if err := r.ParseForm(); err != nil {
			t.Error(err)
		}
		if r.Form.Get("grant_type") != "urn:ietf:params:oauth:grant-type:jwt-bearer" || r.Form.Get("assertion") == "" {
			t.Error("Google service account exchange not used")
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"access_token":"google-token","token_type":"Bearer","expires_in":3600}`))
	}))
	defer server.Close()
	credentialJSON, err := json.Marshal(map[string]string{"type": "service_account", "client_email": "writer@example.iam.gserviceaccount.com", "private_key_id": "key-id", "private_key": string(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyBytes})), "token_uri": server.URL})
	if err != nil {
		t.Fatal(err)
	}
	auth := &nativeGoogleAuth{json: string(credentialJSON), scopes: []string{"https://www.googleapis.com/auth/cloud-platform"}, client: server.Client()}
	initCtx, cancelInit := context.WithCancel(context.Background())
	defer cancelInit()
	for index := range 2 {
		ctx := context.Background()
		if index == 0 {
			ctx = initCtx
		}
		key, value, err := auth.AuthHeaderWithContext(ctx)
		if err != nil || key != "Authorization" || value != "Bearer google-token" {
			t.Fatalf("Google auth failed: %v", err)
		}
		if index == 0 {
			cancelInit()
		}
	}
	if requests.Load() != 1 {
		t.Fatal("Google access token was not cached")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, _, err := auth.AuthHeaderWithContext(ctx); !errors.Is(err, context.Canceled) {
		t.Fatal("Google auth ignored canceled request")
	}
	auth.token = &oauth2.Token{AccessToken: "expired", Expiry: time.Now().Add(-time.Hour)}
	if _, _, err := auth.AuthHeaderWithContext(context.Background()); err != nil {
		t.Fatal(err)
	}
	if requests.Load() != 2 {
		t.Fatal("expired Google access token was not refreshed")
	}
}

func TestNativeSQLConnectionTranslation(t *testing.T) {
	config := &Config{JDBCUrl: "jdbc:postgresql://db.example:5432/iceberg?sslmode=require&currentSchema=iceberg_meta&connectTimeout=5", JDBCUsername: "user@tenant", JDBCPassword: "password:/secret"}
	driver, dsn, dialect, err := nativeSQLConnection(config)
	if err != nil {
		t.Fatal(err)
	}
	u, err := url.Parse(dsn)
	if err != nil {
		t.Fatal(err)
	}
	password, _ := u.User.Password()
	if driver != "pgx" || dialect != icesql.Postgres || u.User.Username() != config.JDBCUsername || password != config.JDBCPassword || u.Query().Get("search_path") != "iceberg_meta" || u.Query().Get("connect_timeout") != "5" || u.Query().Get("currentSchema") != "" {
		t.Fatal("JDBC PostgreSQL config not preserved")
	}
	for _, connection := range []string{"jdbc:mysql://db.example/iceberg", "jdbc:sqlite::memory:", "jdbc:unknown://db.example/secret"} {
		config.JDBCUrl = connection
		if _, _, _, err := nativeSQLConnection(config); err == nil || strings.Contains(err.Error(), "secret") {
			t.Fatal("accepted an unbundled JDBC dialect or leaked its URL")
		}
	}
}

// Set OLAKE_NATIVE_POSTGRES_URL to a disposable PostgreSQL database to exercise
// the same JDBC conversion and metadata commit path used in production.
func TestNativeSQLCatalogFactoryPostgres(t *testing.T) {
	connection := os.Getenv("OLAKE_NATIVE_POSTGRES_URL")
	if connection == "" {
		t.Skip("requires disposable PostgreSQL via OLAKE_NATIVE_POSTGRES_URL")
	}
	config := nativeTestConfig(t)
	config.CatalogType, config.JDBCUrl = JDBCCatalog, connection
	config.CatalogName = "olake_native_test_" + strconv.FormatInt(time.Now().UnixNano(), 10)
	config.IcebergS3Path = t.TempDir()
	cat, cleanup, err := openNativeCatalog(context.Background(), config)
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup(context.Background())
	ctx := nativeCatalogContext(context.Background(), cat)
	namespace, ident := table.Identifier{"db"}, table.Identifier{"db", "records"}
	if err := cat.CreateNamespace(ctx, namespace, apacheiceberg.Properties{}); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cat.DropNamespace(ctx, namespace) }()
	schema := apacheiceberg.NewSchema(0, apacheiceberg.NestedField{ID: 1, Name: "id", Type: apacheiceberg.PrimitiveTypes.Int64, Required: true})
	if _, err := cat.CreateTable(ctx, ident, schema); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cat.DropTable(ctx, ident) }()
	if _, err := cat.LoadTable(ctx, ident); err != nil {
		t.Fatal(err)
	}
}

func TestNativeGlueSDKRetriesRemainVisible(t *testing.T) {
	attempts := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		attempts++
		w.Header().Set("Content-Type", "application/x-amz-json-1.1")
		w.Header().Set("X-Amzn-Requestid", "request-id")
		if attempts == 1 {
			w.WriteHeader(429)
			_, _ = w.Write([]byte(`{"__type":"ThrottlingException","message":"retry"}`))
		} else {
			_, _ = w.Write([]byte(`{"DatabaseList":[]}`))
		}
	}))
	defer server.Close()
	var events []nativeHTTPAttempt
	client := server.Client()
	client.Transport = newNativeHTTPTransport(client.Transport, func(a nativeHTTPAttempt) { events = append(events, a) })
	cfg := aws.Config{Region: "us-east-1", Credentials: credentials.NewStaticCredentialsProvider("key", "secret", ""), HTTPClient: client, BaseEndpoint: aws.String(server.URL), RetryMaxAttempts: 2}
	_, err := awsglue.NewFromConfig(cfg).GetDatabases(context.Background(), &awsglue.GetDatabasesInput{})
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 2 || events[0].Status != 429 || events[1].Status != 200 {
		t.Fatalf("lost retried wire attempts: %v", events)
	}
}

func TestNativeCatalogFailureKeepsConflictIdentity(t *testing.T) {
	err := nativeCatalogError("commit", rest.ErrCommitFailed)
	if !errors.Is(err, rest.ErrCommitFailed) || !strings.Contains(err.Error(), "conflict") {
		t.Fatal("commit conflict identity or diagnosis lost")
	}
	err = nativeCatalogError("load", catalog.ErrNoSuchTable)
	if !errors.Is(err, catalog.ErrNoSuchTable) {
		t.Fatal("table-not-found identity lost")
	}
}

type nativeTestHiveHandler struct {
	hive_metastore.ThriftHiveMetastore
}

func (*nativeTestHiveHandler) GetAllDatabases(context.Context) ([]string, error) {
	return []string{"native_database"}, nil
}

func TestNativeHiveFactoryUsesThriftAndConfiguredPool(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	var connections sync.WaitGroup
	acceptDone := make(chan struct{})
	go func() {
		defer close(acceptDone)
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			connections.Add(1)
			go func() {
				defer connections.Done()
				socket := thrift.NewTSocketFromConnConf(conn, &thrift.TConfiguration{SocketTimeout: 5 * time.Second})
				defer socket.Close()
				factory := thrift.NewTBinaryProtocolFactoryConf(nil)
				input, output := factory.GetProtocol(socket), factory.GetProtocol(socket)
				processor := hive_metastore.NewThriftHiveMetastoreProcessor(&nativeTestHiveHandler{})
				for {
					if _, err := processor.Process(context.Background(), input, output); err != nil {
						return
					}
				}
			}()
		}
	}()
	defer func() { _ = listener.Close(); <-acceptDone; connections.Wait() }()
	config := nativeTestConfig(t)
	config.CatalogType, config.HiveURI, config.HiveClients = HiveCatalog, "thrift://"+listener.Addr().String(), 3
	cat, cleanup, err := openNativeCatalog(context.Background(), config)
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup(context.Background())
	native := cat.(*nativeCatalog).Catalog
	if native.CatalogType() != catalog.Hive {
		t.Fatal("factory did not create a native Hive catalog")
	}
	for range config.HiveClients * 2 {
		namespaces, err := cat.ListNamespaces(context.Background(), nil)
		if err != nil || len(namespaces) != 1 || namespaces[0][0] != "native_database" {
			t.Fatalf("native Thrift request failed: %v", err)
		}
	}
	cleanup(context.Background())
}

func TestNativeHivePoolHonorsWaitingCancellation(t *testing.T) {
	pool := &nativeHivePool{clients: make(chan *nativeHiveClient), done: make(chan struct{})}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := pool.GetAllDatabases(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatal("waiting for a Hive client ignored cancellation")
	}
	if err := pool.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.GetAllDatabases(context.Background()); err == nil {
		t.Fatal("closed Hive pool accepted an operation")
	}
}

// A real Kerberos metastore and an ambient GSSAPI ticket cache are required.
func TestNativeHiveSASLIntegration(t *testing.T) {
	uri := os.Getenv("OLAKE_NATIVE_HIVE_SASL_URI")
	if uri == "" {
		t.Skip("requires Kerberos metastore via OLAKE_NATIVE_HIVE_SASL_URI")
	}
	config := nativeTestConfig(t)
	config.CatalogType, config.HiveURI, config.HiveSaslEnabled, config.HiveClients = HiveCatalog, uri, true, 1
	cat, cleanup, err := openNativeCatalog(context.Background(), config)
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup(context.Background())
	if _, err := cat.ListNamespaces(context.Background(), nil); err != nil {
		t.Fatal(err)
	}
}

func TestNativePostgresJDBCTLSPolicy(t *testing.T) {
	for _, test := range []struct{ connection, mode string }{
		{"jdbc:postgresql://db.example/db?ssl=true", "verify-full"},
		{"jdbc:postgresql://db.example/db?ssl", "verify-full"},
		{"jdbc:postgresql://db.example/db?ssl=false", "prefer"},
		{"jdbc:postgresql://db.example/db?ssl=true&sslmode=verify-ca", "verify-ca"},
	} {
		_, dsn, _, err := nativeSQLConnection(&Config{JDBCUrl: test.connection})
		if err != nil {
			t.Fatal(err)
		}
		parsed, err := url.Parse(dsn)
		if err != nil || parsed.Query().Get("sslmode") != test.mode || parsed.Query().Has("ssl") {
			t.Fatal("JDBC TLS policy changed during migration")
		}
	}
	_, dsn, _, err := nativeSQLConnection(&Config{JDBCUrl: "jdbc:postgresql:iceberg"})
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := url.Parse(dsn)
	if err != nil || parsed.Host != "localhost" || parsed.Path != "/iceberg" {
		t.Fatal("JDBC short-form database URL not translated")
	}
}

func TestNativeGlueInheritsConfiguredKeysAndFallsBackToDefaultChain(t *testing.T) {
	config := nativeTestConfig(t)
	config.SessionToken = "configured-session"
	t.Setenv("AWS_ACCESS_KEY_ID", "glue-environment-key")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "glue-environment-secret")
	t.Setenv("AWS_SESSION_TOKEN", "glue-environment-session")
	for _, additional := range []bool{false, true} {
		config.UseGlueAdditionalConfig = additional
		cfg, err := nativeAWSConfig(context.Background(), config, true, http.DefaultClient)
		if err != nil {
			t.Fatal(err)
		}
		creds, err := cfg.Credentials.Retrieve(context.Background())
		if err != nil || creds.AccessKeyID != config.AccessKey || creds.SessionToken != config.SessionToken {
			t.Fatal("Glue did not inherit configured AWS credentials")
		}
	}
	config.AccessKey, config.SecretKey, config.SessionToken = "", "", ""
	cfg, err := nativeAWSConfig(context.Background(), config, true, http.DefaultClient)
	if err != nil {
		t.Fatal(err)
	}
	creds, err := cfg.Credentials.Retrieve(context.Background())
	if err != nil || creds.AccessKeyID != "glue-environment-key" || creds.SessionToken != "glue-environment-session" {
		t.Fatal("Glue did not fall back to the default credential chain")
	}
}

func TestNativeCatalogConfigurationErrorsRemainActionableAndSafe(t *testing.T) {
	config := nativeTestConfig(t)
	config.RestCatalogURL = "file:///private-secret"
	_, _, err := openNativeCatalog(context.Background(), config)
	if err == nil || !strings.Contains(err.Error(), "rest_catalog_url") || strings.Contains(err.Error(), "private-secret") {
		t.Fatal("configuration error lost its field or leaked its value")
	}
	config.RestCatalogURL, config.RestAuthType = "https://catalog.example", "private-secret"
	_, _, err = openNativeCatalog(context.Background(), config)
	if err == nil || !strings.Contains(err.Error(), "rest_auth_type") || strings.Contains(err.Error(), "private-secret") {
		t.Fatal("authentication configuration error lost its field or leaked its value")
	}
}

type nativeOptionalTestCatalog struct {
	catalog.Catalog
	purgeContext context.Context
	purged       table.Identifier
	purgeErr     error
	closeErr     error
	closeCalls   int
}

func (c *nativeOptionalTestCatalog) PurgeTable(ctx context.Context, ident table.Identifier) error {
	c.purgeContext, c.purged = ctx, ident
	return c.purgeErr
}

func (c *nativeOptionalTestCatalog) Close() error { c.closeCalls++; return c.closeErr }

func TestNativeCatalogForwardsPurgeWithStorageContext(t *testing.T) {
	cause := errors.New("private-server-message")
	underlying := &nativeOptionalTestCatalog{purgeErr: cause}
	wrapped := &nativeCatalog{Catalog: underlying, storageAWS: aws.Config{Region: "storage-region"}}
	purger, ok := any(wrapped).(catalog.PurgeableTable)
	if !ok {
		t.Fatal("wrapper hides purge capability")
	}
	err := purger.PurgeTable(context.Background(), table.Identifier{"db", "table"})
	if !errors.Is(err, cause) || strings.Contains(err.Error(), "private-server-message") {
		t.Fatal("purge lost its cause or leaked the response")
	}
	if len(underlying.purged) != 2 || underlying.purged[1] != "table" {
		t.Fatal("purge was not forwarded")
	}
	config := iceutils.GetAwsConfig(underlying.purgeContext)
	if config == nil || config.Region != "storage-region" {
		t.Fatal("purge lost storage credentials")
	}
}

func TestNativeCatalogCloseForwardsExactlyOnce(t *testing.T) {
	cause := errors.New("private-close-message")
	underlying := &nativeOptionalTestCatalog{closeErr: cause}
	wrapped := &nativeCatalog{Catalog: underlying}
	closer, ok := any(wrapped).(catalog.Closer)
	if !ok {
		t.Fatal("wrapper hides close capability")
	}
	for range 2 {
		err := closer.Close()
		if !errors.Is(err, cause) || strings.Contains(err.Error(), "private-close-message") {
			t.Fatal("close lost its cause or leaked its message")
		}
	}
	if underlying.closeCalls != 1 {
		t.Fatal("underlying catalog closed more than once")
	}
	ownedCloses := 0
	wrapped = &nativeCatalog{Catalog: underlying, closeOwned: func() error { ownedCloses++; return nil }}
	if err := wrapped.Close(); err != nil {
		t.Fatal(err)
	}
	if err := wrapped.Close(); err != nil {
		t.Fatal(err)
	}
	if ownedCloses != 1 || underlying.closeCalls != 1 {
		t.Fatal("owned-resource cleanup was duplicated or bypassed")
	}
}

type nativeHiveTrackedTransport struct {
	thrift.TTransport
	closes int
}

func (t *nativeHiveTrackedTransport) Close() error { t.closes++; return t.TTransport.Close() }

func TestNativeHivePoolRedialsBrokenConnectionsWithoutReplayingRPC(t *testing.T) {
	for _, cause := range []error{io.EOF, context.Canceled, thrift.NewTApplicationException(thrift.BAD_SEQUENCE_ID, "late reply"), thrift.NewTTransportException(thrift.TIMED_OUT, "timeout")} {
		original := &nativeHiveTrackedTransport{TTransport: thrift.NewTMemoryBuffer()}
		replacement := &nativeHiveTrackedTransport{TTransport: thrift.NewTMemoryBuffer()}
		dials, calls := 0, 0
		client := &nativeHiveClient{transport: original, reconnect: func(context.Context) (*nativeHiveClient, error) {
			dials++
			return &nativeHiveClient{transport: replacement}, nil
		}}
		pool := &nativeHivePool{clients: make(chan *nativeHiveClient, 1), all: []*nativeHiveClient{client}, done: make(chan struct{})}
		pool.clients <- client
		_, err := nativeHiveCall(context.Background(), pool, func(*nativeHiveClient) (string, error) { calls++; return "", cause })
		if !errors.Is(err, cause) || calls != 1 || dials != 0 || original.closes != 1 {
			t.Fatal("failed Hive RPC was replayed or its broken connection was retained")
		}
		value, err := nativeHiveCall(context.Background(), pool, func(c *nativeHiveClient) (string, error) {
			calls++
			if c.transport != replacement {
				t.Fatal("redial did not replace pool transport")
			}
			return "healthy", nil
		})
		if err != nil || value != "healthy" || dials != 1 || calls != 2 {
			t.Fatal("later Hive RPC did not recover through a fresh connection")
		}
		if err := pool.Close(); err != nil {
			t.Fatal(err)
		}
		if replacement.closes != 1 || original.closes != 1 {
			t.Fatal("Hive recovery leaked or double-closed a transport")
		}
	}
}

func TestNativeHivePoolRetainsDecodedApplicationExceptions(t *testing.T) {
	for _, cause := range []error{&hive_metastore.NoSuchObjectException{Message: "missing"}, &hive_metastore.MetaException{Message: "application failure"}, thrift.NewTApplicationException(thrift.INTERNAL_ERROR, "server rejected request")} {
		transport := &nativeHiveTrackedTransport{TTransport: thrift.NewTMemoryBuffer()}
		client := &nativeHiveClient{transport: transport, reconnect: func(context.Context) (*nativeHiveClient, error) {
			t.Fatal("decoded application exception discarded a healthy transport")
			return nil, errors.New("unexpected reconnect after decoded application exception")
		}}
		pool := &nativeHivePool{clients: make(chan *nativeHiveClient, 1), all: []*nativeHiveClient{client}, done: make(chan struct{})}
		pool.clients <- client
		_, err := nativeHiveCall(context.Background(), pool, func(*nativeHiveClient) (bool, error) { return false, cause })
		if !errors.Is(err, cause) || transport.closes != 0 {
			t.Fatal("application error changed or connection was discarded")
		}
		if _, err := nativeHiveCall(context.Background(), pool, func(*nativeHiveClient) (bool, error) { return true, nil }); err != nil {
			t.Fatal(err)
		}
		_ = pool.Close()
	}
}

func TestNativeHiveSASLWithoutBuildTagReturnsConfigurationError(t *testing.T) {
	if nativeKerberosAvailable {
		t.Skip("Kerberos implementation is enabled in this build")
	}
	_, err := newNativeHiveClient(context.Background(), "thrift://not-contacted.example:9083", true)
	var configError nativeCatalogConfigError
	if err == nil || !errors.As(err, &configError) || !strings.Contains(err.Error(), "kerberos build tag") {
		t.Fatal("untagged SASL configuration did not fail safely before dialing")
	}
}

func TestNativePostgresJDBCOptionsAreTranslatedOrRejected(t *testing.T) {
	for _, test := range []struct{ query, timeout string }{
		{"ApplicationName=olake&loginTimeout=7", "7"},
		{"ApplicationName=olake&loginTimeout=7&connectTimeout=3", "3"},
		{"ApplicationName=olake&loginTimeout=0&connectTimeout=3", "3"},
	} {
		_, dsn, _, err := nativeSQLConnection(&Config{JDBCUrl: "jdbc:postgresql://db.example/db?" + test.query})
		if err != nil {
			t.Fatal(err)
		}
		parsed, err := url.Parse(dsn)
		if err != nil || parsed.Query().Get("application_name") != "olake" || parsed.Query().Get("connect_timeout") != test.timeout || parsed.Query().Has("ApplicationName") || parsed.Query().Has("loginTimeout") {
			t.Fatal("JDBC-only options escaped into PostgreSQL startup parameters")
		}
	}
	for _, option := range []string{"sslfactory", "sslfactoryarg", "sslhostnameverifier", "sslpasswordcallback", "socketTimeout", "private-secret"} {
		_, _, _, err := nativeSQLConnection(&Config{JDBCUrl: "jdbc:postgresql://db.example/db?" + option + "=private-secret"})
		if err == nil || strings.Contains(err.Error(), "private-secret") {
			t.Fatal("unsupported Java option reached pgx or leaked its configured value")
		}
	}
}

func TestNativeGlueStaleBranchRequirementsAreKnownPrewriteConflicts(t *testing.T) {
	var mu sync.Mutex
	var parameters map[string]string
	updates := 0
	serviceFailure := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		w.Header().Set("Content-Type", "application/x-amz-json-1.1")
		switch target := r.Header.Get("X-Amz-Target"); {
		case strings.HasSuffix(target, ".CreateTable"), strings.HasSuffix(target, ".UpdateTable"):
			if strings.HasSuffix(target, ".UpdateTable") && serviceFailure {
				w.WriteHeader(http.StatusBadRequest)
				_, _ = w.Write([]byte(`{"__type":"InvalidInputException","message":"Requirement failed: not a client validation failure"}`))
				return
			}
			var request struct {
				TableInput struct{ Parameters map[string]string }
			}
			if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
				t.Error(err)
				w.WriteHeader(400)
				return
			}
			parameters = request.TableInput.Parameters
			if strings.HasSuffix(target, ".UpdateTable") {
				updates++
			}
			_, _ = w.Write([]byte(`{}`))
		case strings.HasSuffix(target, ".GetTable"):
			if err := json.NewEncoder(w).Encode(map[string]any{"Table": map[string]any{"Name": "records", "DatabaseName": "db", "TableType": "EXTERNAL_TABLE", "VersionId": strconv.Itoa(updates + 1), "Parameters": parameters}}); err != nil {
				t.Error(err)
			}
		default:
			t.Errorf("unexpected Glue RPC %s", target)
			w.WriteHeader(400)
		}
	}))
	defer server.Close()
	config := nativeTestConfig(t)
	config.CatalogType, config.UseGlueAdditionalConfig, config.GlueEndpoint = GlueCatalog, true, server.URL
	config.IcebergS3Path = t.TempDir()
	cat, cleanup, err := openNativeCatalog(context.Background(), config)
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup(context.Background())
	ctx := nativeCatalogContext(context.Background(), cat)
	ident := table.Identifier{"db", "records"}
	schema := apacheiceberg.NewSchema(0, apacheiceberg.NestedField{ID: 1, Name: "id", Type: apacheiceberg.PrimitiveTypes.Int64, Required: true})
	initial, err := cat.CreateTable(ctx, ident, schema, catalog.WithLocation(config.IcebergS3Path))
	if err != nil {
		t.Fatal(err)
	}
	stale := []table.Requirement{table.AssertTableUUID(initial.Metadata().TableUUID()), table.AssertRefSnapshotID(table.MainBranch, nil)}
	arrowSchema, err := table.SchemaToArrowSchema(initial.Schema(), nil, true, false)
	if err != nil {
		t.Fatal(err)
	}
	builder := array.NewRecordBuilder(memory.DefaultAllocator, arrowSchema)
	defer builder.Release()
	builder.Field(0).(*array.Int64Builder).Append(1)
	record := builder.NewRecordBatch()
	defer record.Release()
	reader, err := array.NewRecordReader(arrowSchema, []arrow.RecordBatch{record})
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Release()
	advanced, err := initial.Append(ctx, reader, nil)
	if err != nil {
		t.Fatal(err)
	}
	if advanced.CurrentSnapshot() == nil {
		t.Fatal("real native Glue append did not advance main")
	}
	mu.Lock()
	beforeUpdates := updates
	mu.Unlock()
	_, _, err = cat.CommitTable(ctx, ident, stale, []table.Update{table.NewSetPropertiesUpdate(apacheiceberg.Properties{"losing-update": "not-committed"})})
	if !errors.Is(err, table.ErrCommitFailed) {
		t.Fatalf("stale main was not a known rejection: %v", err)
	}
	mu.Lock()
	afterUpdates := updates
	serviceFailure = true
	mu.Unlock()
	if beforeUpdates != afterUpdates {
		t.Fatal("stale requirement was rejected only after a Glue write")
	}
	_, _, err = cat.CommitTable(ctx, ident, []table.Requirement{table.AssertTableUUID(initial.Metadata().TableUUID())}, []table.Update{table.NewSetPropertiesUpdate(apacheiceberg.Properties{"service-error": "unknown"})})
	if err == nil || errors.Is(err, table.ErrCommitFailed) {
		t.Fatal("service error text was incorrectly normalized as a prewrite requirement failure")
	}
}

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
	if response != nil {
		defer func() { _ = response.Body.Close() }()
	}
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
		response, err := transport.RoundTrip(request)
		if response != nil {
			if closeErr := response.Body.Close(); closeErr != nil {
				t.Fatal(closeErr)
			}
		}
		if err != nil {
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
	response, err := transport.RoundTrip(request)
	if response != nil {
		defer func() { _ = response.Body.Close() }()
	}
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
	if response != nil {
		defer func() { _ = response.Body.Close() }()
	}
	if err != nil || response.StatusCode != 429 || calls != 2 || waits != 1 || finalBody.closes != 0 {
		t.Fatal("wait budget did not stop retries while retaining the final response")
	}
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
	go func() {
		response, err := transport.RoundTrip(request)
		if response != nil {
			_ = response.Body.Close()
		}
		finished <- err
	}()
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

func TestNativeRESTThrottleReplaysOnlyRejectedControlledMutatingPOST(t *testing.T) {
	ctx, config, _ := nativeExecutionFixture(t)
	request, _ := http.NewRequestWithContext(ctx, http.MethodPost, "https://catalog.example/v1/namespaces/db/tables/t", strings.NewReader(`{"updates":["exact-payload"]}`))
	request.Header.Set("Authorization", "signed-authorization")
	originalGetBody := request.GetBody
	replays, calls := 0, 0
	request.GetBody = func() (io.ReadCloser, error) { replays++; return originalGetBody() }
	transport := nativeThrottleTestTransport(nativeRoundTripFunc(func(attempt *http.Request) (*http.Response, error) {
		calls++
		payload, err := io.ReadAll(attempt.Body)
		_ = attempt.Body.Close()
		if err != nil || string(payload) != `{"updates":["exact-payload"]}` || attempt.ContentLength != request.ContentLength || attempt.Method != http.MethodPost || attempt.Header.Get("Authorization") != "signed-authorization" || attempt.GetBody != nil {
			t.Fatal("POST dispatch changed its payload, signed headers, or allowed hidden replay")
		}
		status := http.StatusTooManyRequests
		if calls == 3 {
			status = http.StatusNoContent
		}
		return &http.Response{StatusCode: status, Header: http.Header{"Retry-After": {"0"}}, Body: http.NoBody}, nil
	}))
	transport.execution = config.execution
	response, err := transport.RoundTrip(request)
	require.NoError(t, err)
	require.Equal(t, http.StatusNoContent, response.StatusCode)
	require.Equal(t, 3, calls)
	require.Equal(t, 2, replays)
	require.NoError(t, response.Body.Close())
	require.NoError(t, nativeExecutionWritable(ctx, config))
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
		if response != nil {
			defer func() { _ = response.Body.Close() }()
		}
		if err != nil || response.StatusCode != 429 || calls != 1 || body.closes != 0 || body.reads != 0 {
			t.Fatal("unreplayable POST was retried or its final response was discarded")
		}
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
			if response != nil {
				_ = response.Body.Close()
			}
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
	if response != nil {
		defer func() { _ = response.Body.Close() }()
	}
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
	if response != nil && response.Body != nil {
		defer func() { require.NoError(t, response.Body.Close()) }()
	}
	if response != nil || !errors.Is(err, context.Canceled) || calls != 1 || replayBody.closes != 1 || replayBody.reads != 0 || responseBody.closes != 1 {
		t.Fatal("canceled POST retry leaked a replay or discarded response body")
	}
}

type nativeMemoryIndex struct {
	rows       map[string]types.RowLocation
	snapshot   int64
	commitErr  error
	generation string
}

func (i *nativeMemoryIndex) Lookup(key string) (types.RowLocation, bool, error) {
	loc, ok := i.rows[key]
	return loc, ok, nil
}
func (i *nativeMemoryIndex) RangeFiles(paths map[string]struct{}, fn func(string, types.RowLocation) error) error {
	if len(paths) == 0 {
		return nil
	}
	rows := make(map[string]types.RowLocation)
	for key, loc := range i.rows {
		if _, matches := paths[loc.FilePath]; matches {
			rows[key] = loc
		}
	}
	for key, loc := range rows {
		if err := fn(key, loc); err != nil {
			return err
		}
	}
	return nil
}
func (i *nativeMemoryIndex) Commit(batch *types.StreamIndexThread, snapshot *int64) error {
	if i.commitErr != nil {
		return i.commitErr
	}
	if batch != nil {
		if err := batch.RangeDeletes(func(key string) error { delete(i.rows, key); return nil }); err != nil {
			return err
		}
		if err := batch.Range(func(key string, loc types.RowLocation) error { i.rows[key] = loc; return nil }); err != nil {
			return err
		}
	}
	if snapshot != nil {
		i.snapshot = *snapshot
	}
	return nil
}
func (i *nativeMemoryIndex) LastCommittedSnapshot() (int64, error) { return i.snapshot, nil }
func (i *nativeMemoryIndex) Truncate() error                       { clear(i.rows); i.snapshot = 0; return nil }
func (i *nativeMemoryIndex) Close() error                          { return nil }

func (i *nativeMemoryIndex) ReconcileGeneration(generation string) error {
	if i.generation != "" && generation == "" {
		return errors.New("destination omitted the durable resync generation; refusing to use stale row locations")
	}
	if i.generation == generation {
		return nil
	}
	if err := i.Truncate(); err != nil {
		return err
	}
	i.generation = generation
	return nil
}

func nativeWriterFixture(t *testing.T, mode types.UpdateType, normalized bool, partition string) (*Config, *types.ConfiguredStream, catalog.Catalog) {
	t.Helper()
	root := t.TempDir()
	cat, err := hadoop.NewCatalog("native-writer-test", root, nil)
	require.NoError(t, err)
	stream := types.NewStream("records", "db", nil)
	stream.DestinationDatabase, stream.DestinationTable = "db", "records"
	stream.UpsertField(constants.OlakeID, types.String, false, true)
	stream.UpsertField(constants.OpType, types.String, false, true)
	stream.UpsertField(constants.OlakeTimestamp, types.TimestampMicro, false, true)
	stream.UpsertField("value", types.Int64, true, false)
	stream.UpsertField("part", types.String, true, false)
	stream.UpsertField("blob", types.FixedBinaryOf(4), true, false)
	updateMode := string(mode)
	configured := &types.ConfiguredStream{Stream: stream, StreamMetadata: types.StreamMetadata{Normalization: &normalized, UpdateType: &updateMode, PartitionRegex: partition, UseSourceColumnNames: true}}
	return &Config{IcebergS3Path: root, IcebergDatabase: "db"}, configured, &nativeCatalog{Catalog: cat}
}

func openNativeTestWriter(ctx context.Context, t *testing.T, cfg *Config, stream *types.ConfiguredStream, cat catalog.Catalog, index *nativeMemoryIndex, thread string, backfill bool) *nativeWriter {
	t.Helper()
	if fault, ok := cat.(*nativeCommitFaultCatalog); ok {
		if fault.coordinated == nil {
			fault.coordinated = &nativeCatalog{Catalog: fault}
		}
		cat = fault.coordinated
	}
	opts := &destination.Options{ThreadID: thread, Backfill: backfill}
	if index != nil {
		opts.TableIndex = index
	}
	writer, _, err := newNativeWriter(ctx, cfg, stream, opts, cat, types.NewStreamIndexThread(opts.TableIndex))
	require.NoError(t, err)
	return writer
}

func nativeTestRecord(id, operation string, value int64, partition string) types.RawRecord {
	return types.RawRecord{Data: map[string]any{"value": value, "part": partition, "blob": []byte{0, 1, 2, 3}}, OlakeColumns: map[string]any{constants.OlakeID: id, constants.OpType: operation, constants.OlakeTimestamp: time.Date(2025, 3, 8, 12, 0, 0, 123456000, time.UTC)}}
}

func nativeVisibleValues(t *testing.T, tbl *table.Table) map[string]int64 {
	t.Helper()
	scan := tbl.Scan()
	defer scan.Close()
	schema, records, err := scan.ToArrowRecords(context.Background())
	require.NoError(t, err)
	idColumn := schema.FieldIndices(constants.OlakeID)[0]
	valueColumn := schema.FieldIndices("value")[0]
	result := make(map[string]int64)
	for batch, err := range records {
		require.NoError(t, err)
		ids, values := batch.Column(idColumn).(*array.String), batch.Column(valueColumn).(*array.Int64)
		for row := range int(batch.NumRows()) {
			id := ids.Value(row)
			_, duplicate := result[id]
			require.False(t, duplicate, "duplicate visible key %s", id)
			result[id] = values.Value(row)
		}
		batch.Release()
	}
	return result
}

func TestNativeWriterEqualityRepeatsAndSoftDelete(t *testing.T) {
	ctx := context.Background()
	cfg, stream, cat := nativeWriterFixture(t, types.UpdateTypeEquality, true, "/{part, identity}")
	seed := openNativeTestWriter(ctx, t, cfg, stream, cat, nil, "snapshot-1", true)
	require.NoError(t, seed.Write(ctx, []types.RawRecord{nativeTestRecord("a", "r", 1, "one"), nativeTestRecord("b", "r", 7, "one")}))
	require.NoError(t, seed.Close(ctx, nil))
	writer := openNativeTestWriter(ctx, t, cfg, stream, cat, nil, "cdc-1", false)
	require.NoError(t, writer.Write(ctx, []types.RawRecord{nativeTestRecord("a", "u", 2, "one"), nativeTestRecord("a", "u", 3, "two")}))
	require.NoError(t, writer.Write(ctx, []types.RawRecord{nativeTestRecord("a", "d", 4, "two")}))
	require.NoError(t, writer.Close(ctx, map[string]any{"state": `{"captured_cdc_pos":"0/ABC"}`, "dedup_inserts": false}))
	require.Equal(t, map[string]int64{"a": 4, "b": 7}, nativeVisibleValues(t, writer.table))
	var state types.MetadataState
	require.NoError(t, json.Unmarshal([]byte(writer.table.Properties()[nativeRecoveryProperty]), &state))
	require.Equal(t, "cdc-1", state.ID)
	require.Equal(t, []string{"snapshot-1"}, state.FullRefreshCommittedIDs)
	require.NotNil(t, state.DedupInserts)
	require.False(t, *state.DedupInserts)
}

func TestNativeWriterIndexedDeletesAndRecoveryReads(t *testing.T) {
	for _, mode := range []types.UpdateType{types.UpdateTypePosition, types.UpdateTypeDeletionVector} {
		t.Run(string(mode), func(t *testing.T) {
			ctx := context.Background()
			cfg, stream, cat := nativeWriterFixture(t, mode, true, "/{part, identity}")
			index := &nativeMemoryIndex{rows: make(map[string]types.RowLocation)}
			seed := openNativeTestWriter(ctx, t, cfg, stream, cat, index, "snapshot", true)
			require.NoError(t, seed.Write(ctx, []types.RawRecord{nativeTestRecord("a", "r", 1, "one"), nativeTestRecord("b", "r", 2, "one")}))
			require.NoError(t, seed.Close(ctx, nil))
			original := seed.dataFiles[0].FilePath()
			writer := openNativeTestWriter(ctx, t, cfg, stream, cat, index, "cdc-a", false)
			require.NoError(t, writer.Write(ctx, []types.RawRecord{nativeTestRecord("a", "u", 3, "two"), nativeTestRecord("a", "u", 4, "one")}))
			loc, found, err := writer.indexThread.Lookup("a")
			require.NoError(t, err)
			require.True(t, found)
			require.NoError(t, writer.EnsureReadable(ctx, []string{loc.FilePath}))
			rows, err := writer.ReadRows(ctx, loc.FilePath, []int64{loc.Position}, []string{"value", "blob"})
			require.NoError(t, err)
			require.Equal(t, int64(4), rows[loc.Position]["value"])
			require.Equal(t, []byte{0, 1, 2, 3}, rows[loc.Position]["blob"])
			require.NoError(t, writer.Close(ctx, map[string]any{"state": "a"}))
			next := openNativeTestWriter(ctx, t, cfg, stream, cat, index, "cdc-b", false)
			require.NoError(t, next.Write(ctx, []types.RawRecord{nativeTestRecord("b", "d", 5, "two")}))
			require.NoError(t, next.Close(ctx, map[string]any{"state": "b"}))
			require.Equal(t, map[string]int64{"a": 4, "b": 5}, nativeVisibleValues(t, next.table))
			require.Equal(t, nativeSnapshotID(next.table), index.snapshot)
			positions, vectors, err := nativeDeletedPositions(ctx, next.table, original)
			require.NoError(t, err)
			require.Equal(t, []int64{0, 1}, positions)
			if mode == types.UpdateTypeDeletionVector {
				require.Len(t, vectors, 1)
			} else {
				require.Empty(t, vectors)
			}
		})
	}
}

func TestNativeWriterEqualityMigration(t *testing.T) {
	for _, mode := range []types.UpdateType{types.UpdateTypePosition, types.UpdateTypeDeletionVector} {
		t.Run(string(mode), func(t *testing.T) {
			ctx := context.Background()
			cfg, stream, cat := nativeWriterFixture(t, types.UpdateTypeEquality, true, "")
			seed := openNativeTestWriter(ctx, t, cfg, stream, cat, nil, "snapshot", true)
			require.NoError(t, seed.Write(ctx, []types.RawRecord{nativeTestRecord("a", "r", 1, "one"), nativeTestRecord("b", "r", 2, "one")}))
			require.NoError(t, seed.Close(ctx, nil))
			cdc := openNativeTestWriter(ctx, t, cfg, stream, cat, nil, "eq-cdc", false)
			require.NoError(t, cdc.Write(ctx, []types.RawRecord{nativeTestRecord("a", "u", 3, "one")}))
			require.NoError(t, cdc.Close(ctx, map[string]any{"state": "checkpoint"}))
			update := string(mode)
			stream.StreamMetadata.UpdateType = &update
			index := &nativeMemoryIndex{rows: make(map[string]types.RowLocation)}
			migrated := openNativeTestWriter(ctx, t, cfg, stream, cat, index, "migrate", false)
			entries, err := nativeLiveEntries(ctx, migrated.table)
			require.NoError(t, err)
			for _, entry := range entries {
				require.NotEqual(t, apacheiceberg.EntryContentEqDeletes, entry.DataFile().ContentType())
			}
			require.Equal(t, map[string]int64{"a": 3, "b": 2}, nativeVisibleValues(t, migrated.table))
			require.NoError(t, migrated.Write(ctx, []types.RawRecord{nativeTestRecord("a", "u", 4, "two")}))
			require.NoError(t, migrated.Close(ctx, map[string]any{"state": "next"}))
			require.Equal(t, map[string]int64{"a": 4, "b": 2}, nativeVisibleValues(t, migrated.table))
		})
	}
}

func TestNativeWriterSchemaEvolutionKeepsFieldIDs(t *testing.T) {
	ctx := context.Background()
	cfg, stream, cat := nativeWriterFixture(t, types.UpdateTypeEquality, true, "")
	writer := openNativeTestWriter(ctx, t, cfg, stream, cat, nil, "schema", true)
	before := make(map[string]int)
	for _, field := range writer.table.Schema().Fields() {
		before[field.Name] = field.ID
	}
	require.NoError(t, writer.Write(ctx, []types.RawRecord{nativeTestRecord("a", "r", 1, "one")}))
	columns := writer.schemaMap()
	columns["new_column"] = "string"
	require.NoError(t, writer.EvolveSchema(ctx, columns))
	for _, field := range writer.table.Schema().Fields() {
		if id, existing := before[field.Name]; existing {
			require.Equal(t, id, field.ID)
		}
	}
	record := nativeTestRecord("b", "r", 2, "one")
	record.Data["new_column"] = "new value"
	require.NoError(t, writer.Write(ctx, []types.RawRecord{record}))
	require.NoError(t, writer.Close(ctx, nil))
	require.Equal(t, map[string]int64{"a": 1, "b": 2}, nativeVisibleValues(t, writer.table))
}

func TestNativeKnownThrottleDoesNotPoisonSchemaOrMigration(t *testing.T) {
	for _, operation := range []string{"source-columns", "schema", "format-upgrade", "equality-migration"} {
		t.Run(operation, func(t *testing.T) {
			ctx := context.Background()
			cfg, stream, realCatalog := nativeWriterFixture(t, types.UpdateTypeEquality, true, "")
			seed := openNativeTestWriter(ctx, t, cfg, stream, realCatalog, nil, "seed", true)
			require.NoError(t, seed.Write(ctx, []types.RawRecord{nativeTestRecord("a", "r", 1, "one"), nativeTestRecord("b", "r", 2, "one")}))
			require.NoError(t, seed.Close(ctx, map[string]any{"state": "original"}))
			cdc := openNativeTestWriter(ctx, t, cfg, stream, realCatalog, nil, "equality-delete", false)
			require.NoError(t, cdc.Write(ctx, []types.RawRecord{nativeTestRecord("a", "u", 3, "one")}))
			require.NoError(t, cdc.Close(ctx, map[string]any{"state": "original"}))
			fault := &nativeCommitFaultCatalog{Catalog: realCatalog}
			cat := &nativeCatalog{Catalog: fault}
			reject := func(context.Context, table.Identifier, []table.Requirement, []table.Update) (bool, error) {
				return true, &nativeRESTThrottleRejection{}
			}
			if operation == "format-upgrade" || operation == "equality-migration" {
				mode := string(types.UpdateTypePosition)
				if operation == "format-upgrade" {
					mode = string(types.UpdateTypeDeletionVector)
				}
				stream.StreamMetadata.UpdateType = &mode
				index := &nativeMemoryIndex{rows: make(map[string]types.RowLocation)}
				options := &destination.Options{ThreadID: "rejected-initialization", TableIndex: index}
				fault.fault = reject
				_, _, err := newNativeWriter(ctx, cfg, stream, options, cat, types.NewStreamIndexThread(index))
				require.True(t, nativeMutationOutcomeKnown(err))
				var throttled *nativeRESTThrottleRejection
				require.ErrorAs(t, err, &throttled)
				require.Equal(t, 1, fault.calls, "known throttling is not another optimistic-conflict retry loop")
				require.False(t, cat.tables.entry(cdc.table.Identifier()).unknown)
				fault.fault = nil
				recovered := openNativeTestWriter(ctx, t, cfg, stream, cat, index, "retry-initialization", false)
				require.Equal(t, map[string]int64{"a": 3, "b": 2}, nativeVisibleValues(t, recovered.table))
				require.Contains(t, recovered.table.Properties()[nativeRecoveryProperty], "original")
				recovered.Abort()
				return
			}
			writer := openNativeTestWriter(ctx, t, cfg, stream, cat, nil, "schema-rejection", false)
			defer writer.Abort()
			fault.fault = reject
			mutate := func() error {
				if operation == "source-columns" {
					return writer.AlterSourceColumns(ctx, []*nativeSourceColumn{{Key: "added", IceType: "long"}}, nil)
				}
				columns := writer.schemaMap()
				columns["added"] = "long"
				return writer.EvolveSchema(ctx, columns)
			}
			err := mutate()
			var throttled *nativeRESTThrottleRejection
			require.ErrorAs(t, err, &throttled)
			require.True(t, nativeMutationOutcomeKnown(err))
			require.False(t, writer.uncertain)
			require.False(t, writer.coordinator.unknown)
			require.Equal(t, 1, fault.calls)
			fault.fault = nil
			require.NoError(t, mutate(), "the same writer may retry a definite schema rejection")
			_, found := writer.table.Schema().FindFieldByName("added")
			require.True(t, found)
			require.Contains(t, writer.table.Properties()[nativeRecoveryProperty], "original")
		})
	}
}

func TestNativeWriterRawAppendAndAbort(t *testing.T) {
	ctx := context.Background()
	cfg, stream, cat := nativeWriterFixture(t, types.UpdateTypeEquality, false, "/{part, identity}")
	appendMode := true
	stream.StreamMetadata.AppendMode = &appendMode
	writer := openNativeTestWriter(ctx, t, cfg, stream, cat, nil, "raw", false)
	row := nativeTestRecord("a", "u", 1, "null")
	row.Data[constants.StringifiedData] = `{"nested":{"text":"raw"}}`
	require.NoError(t, writer.Write(ctx, []types.RawRecord{row}))
	require.NoError(t, writer.Close(ctx, map[string]any{"state": "raw"}))
	field, ok := writer.table.Schema().FindFieldByName(constants.StringifiedData)
	require.True(t, ok)
	require.Equal(t, "string", field.Type.String())
	_, hasSourceColumn := writer.table.Schema().FindFieldByName("value")
	require.False(t, hasSourceColumn)
	abort := openNativeTestWriter(ctx, t, cfg, stream, cat, nil, "abort", false)
	require.NoError(t, abort.Write(ctx, []types.RawRecord{row}))
	paths := slices.Collect(maps.Keys(abort.paths))
	require.NotEmpty(t, paths)
	abort.Abort()
	fs, err := writer.table.FS(ctx)
	require.NoError(t, err)
	for _, path := range paths {
		_, err := fs.Open(path)
		require.Error(t, err)
	}
	fresh, err := cat.LoadTable(ctx, writer.table.Identifier())
	require.NoError(t, err)
	require.Equal(t, nativeSnapshotID(writer.table), nativeSnapshotID(fresh))
}

type nativeCommitFaultCatalog struct {
	catalog.Catalog
	fault       func(context.Context, table.Identifier, []table.Requirement, []table.Update) (bool, error)
	calls       int
	loads       int
	fs          iceio.IO
	coordinated *nativeCatalog
}

func (c *nativeCommitFaultCatalog) bind(tbl *table.Table) *table.Table {
	factory := tbl.FS
	if c.fs != nil {
		factory = func(context.Context) (iceio.IO, error) { return c.fs, nil }
	}
	return table.New(tbl.Identifier(), tbl.Metadata(), tbl.MetadataLocation(), factory, c)
}
func (c *nativeCommitFaultCatalog) LoadTable(ctx context.Context, id table.Identifier) (*table.Table, error) {
	c.loads++
	tbl, err := c.Catalog.LoadTable(ctx, id)
	if err != nil {
		return nil, err
	}
	return c.bind(tbl), nil
}
func (c *nativeCommitFaultCatalog) CommitTable(ctx context.Context, id table.Identifier, reqs []table.Requirement, updates []table.Update) (table.Metadata, string, error) {
	c.calls++
	if c.fault != nil {
		handled, err := c.fault(ctx, id, reqs, updates)
		if handled {
			return nil, "", err
		}
	}
	return c.Catalog.CommitTable(ctx, id, reqs, updates)
}

func TestNativeWriterRetriesMergeFreshRecoveryState(t *testing.T) {
	ctx := context.Background()
	cfg, stream, realCatalog := nativeWriterFixture(t, types.UpdateTypeEquality, true, "")
	seed := openNativeTestWriter(ctx, t, cfg, stream, realCatalog, nil, "snapshot", true)
	require.NoError(t, seed.Write(ctx, []types.RawRecord{nativeTestRecord("a", "r", 1, "one")}))
	require.NoError(t, seed.Close(ctx, nil))
	cat := &nativeCommitFaultCatalog{Catalog: realCatalog}
	writer := openNativeTestWriter(ctx, t, cfg, stream, cat, nil, "retry", false)
	require.NoError(t, writer.Write(ctx, []types.RawRecord{nativeTestRecord("a", "u", 2, "one")}))
	cat.fault = func(ctx context.Context, id table.Identifier, _ []table.Requirement, _ []table.Update) (bool, error) {
		cat.fault = nil
		fresh, err := realCatalog.LoadTable(ctx, id)
		require.NoError(t, err)
		tx := fresh.NewTransaction()
		require.NoError(t, tx.SetProperties(apacheiceberg.Properties{nativeRecoveryProperty: `{"state":"{\"other_partition\":17}"}`}))
		_, err = tx.Commit(ctx)
		require.NoError(t, err)
		// This injects a definite catalog rejection; snapshot assertions alone
		// do not detect an external property-only update. Single-process table
		// ownership is still required, rather than a distributed merge guarantee.
		return true, table.ErrCommitFailed
	}
	require.NoError(t, writer.Close(ctx, map[string]any{"state": `{"this_partition":18}`}))
	var state struct {
		State string `json:"state"`
	}
	require.NoError(t, json.Unmarshal([]byte(writer.table.Properties()[nativeRecoveryProperty]), &state))
	var offsets map[string]int
	require.NoError(t, json.Unmarshal([]byte(state.State), &offsets))
	require.Equal(t, map[string]int{"other_partition": 17, "this_partition": 18}, offsets)
	require.Equal(t, map[string]int64{"a": 2}, nativeVisibleValues(t, writer.table))
}

func TestNativeWriterUnknownCommitRetainsFilesAndRecovery(t *testing.T) {
	ctx := context.Background()
	cfg, stream, realCatalog := nativeWriterFixture(t, types.UpdateTypeEquality, true, "")
	seed := openNativeTestWriter(ctx, t, cfg, stream, realCatalog, nil, "create", true)
	seed.Abort()
	cat := &nativeCommitFaultCatalog{Catalog: realCatalog}
	writer := openNativeTestWriter(ctx, t, cfg, stream, cat, nil, "unknown", false)
	require.NoError(t, writer.Write(ctx, []types.RawRecord{nativeTestRecord("a", "i", 1, "one")}))
	paths := slices.Collect(maps.Keys(writer.paths))
	cat.fault = func(ctx context.Context, id table.Identifier, reqs []table.Requirement, updates []table.Update) (bool, error) {
		_, _, err := realCatalog.CommitTable(ctx, id, reqs, updates)
		require.NoError(t, err)
		return true, errors.New("connection reset after commit; token=do-not-leak")
	}
	err := writer.Close(ctx, map[string]any{"state": "committed"})
	require.Error(t, err)
	require.NotContains(t, err.Error(), "do-not-leak")
	require.True(t, writer.uncertain)
	require.Error(t, writer.Close(ctx, map[string]any{"state": "retry"}))
	writer.Abort()
	fs, err := writer.table.FS(ctx)
	require.NoError(t, err)
	for _, path := range paths {
		file, err := fs.Open(path)
		require.NoError(t, err)
		require.NoError(t, file.Close())
	}
	fresh, err := realCatalog.LoadTable(ctx, writer.table.Identifier())
	require.NoError(t, err)
	require.Equal(t, map[string]int64{"a": 1}, nativeVisibleValues(t, fresh))
	require.Contains(t, fresh.Properties()[nativeRecoveryProperty], "committed")
}

func TestNativeWriterRejectsStaleIndexWithoutPublishing(t *testing.T) {
	ctx := context.Background()
	cfg, stream, cat := nativeWriterFixture(t, types.UpdateTypePosition, true, "")
	index := &nativeMemoryIndex{rows: make(map[string]types.RowLocation)}
	seed := openNativeTestWriter(ctx, t, cfg, stream, cat, index, "snapshot", true)
	require.NoError(t, seed.Write(ctx, []types.RawRecord{nativeTestRecord("a", "r", 1, "one")}))
	require.NoError(t, seed.Close(ctx, nil))
	stale := openNativeTestWriter(ctx, t, cfg, stream, cat, index, "stale", false)
	require.NoError(t, stale.Write(ctx, []types.RawRecord{nativeTestRecord("a", "u", 2, "one")}))
	external := openNativeTestWriter(ctx, t, cfg, stream, cat, nil, "external", true)
	require.NoError(t, external.Write(ctx, []types.RawRecord{nativeTestRecord("b", "r", 3, "one")}))
	require.NoError(t, external.Close(ctx, nil))
	err := stale.Close(ctx, map[string]any{"state": "unsafe"})
	require.ErrorIs(t, err, table.ErrCommitFailed)
	fresh, err := cat.LoadTable(ctx, external.table.Identifier())
	require.NoError(t, err)
	require.Equal(t, map[string]int64{"a": 1, "b": 3}, nativeVisibleValues(t, fresh))
	require.NotContains(t, fresh.Properties()[nativeRecoveryProperty], "unsafe")
}

func TestNativeRecoveryMergeAndConflictClasses(t *testing.T) {
	state, changed, err := mergeNativeRecovery(`{"state":"{\"a\":1}","full_refresh_committed_ids":["one"]}`, "two", map[string]any{"state": `{"b":2}`, "dedup_inserts": false}, true)
	require.NoError(t, err)
	require.True(t, changed)
	require.Contains(t, state, `\"a\":1`)
	require.Contains(t, state, `\"b\":2`)
	state, _, err = mergeNativeRecovery(state, "three", nil, true)
	require.NoError(t, err)
	require.Contains(t, state, `"full_refresh_committed_ids":["one","three"]`)
	unchanged, changed, err := mergeNativeRecovery(state, "empty", nil, false)
	require.NoError(t, err)
	require.False(t, changed)
	require.Equal(t, state, unchanged)
	for sentinel, class := range map[error]string{table.ErrConflictingDataFiles: "concurrent_data", table.ErrConflictingDeleteFiles: "concurrent_deletes", table.ErrDataFilesMissing: "referenced_data_missing", table.ErrCommitFailed: "catalog_compare_and_swap"} {
		require.Equal(t, class, nativeConflictClass(fmt.Errorf("wrapped: %w", sentinel)))
	}
	require.Equal(t, "commit_state_unknown", nativeConflictClass(errors.New("transport")))
}

func TestNativePartitionGroupingDistinguishesNullString(t *testing.T) {
	schema, err := nativeSchema(map[string]string{"part": "string"}, false)
	require.NoError(t, err)
	field := schema.Field(0)
	spec := apacheiceberg.NewPartitionSpec(apacheiceberg.PartitionField{SourceIDs: []int{field.ID}, FieldID: 1000, Name: "part", Transform: apacheiceberg.IdentityTransform{}})
	missing, _, err := nativeRecordPartition(schema, spec, map[string]any{})
	require.NoError(t, err)
	text, _, err := nativeRecordPartition(schema, spec, map[string]any{"part": "null"})
	require.NoError(t, err)
	require.NotEqual(t, missing, text)
	require.False(t, strings.Contains(text, "<nil>"))
}

func TestNativeWriterStateOnlyCommitPreservesSnapshot(t *testing.T) {
	ctx := context.Background()
	cfg, stream, cat := nativeWriterFixture(t, types.UpdateTypeEquality, true, "")
	seed := openNativeTestWriter(ctx, t, cfg, stream, cat, nil, "snapshot", true)
	require.NoError(t, seed.Write(ctx, []types.RawRecord{nativeTestRecord("a", "r", 1, "one")}))
	require.NoError(t, seed.Close(ctx, nil))
	snapshot := nativeSnapshotID(seed.table)
	writer := openNativeTestWriter(ctx, t, cfg, stream, cat, nil, "heartbeat", false)
	require.NoError(t, writer.Close(ctx, map[string]any{"state": `{"captured_cdc_pos":"0/123"}`}))
	require.Equal(t, snapshot, nativeSnapshotID(writer.table))
	require.Contains(t, writer.table.Properties()[nativeRecoveryProperty], "0/123")
	require.Equal(t, map[string]int64{"a": 1}, nativeVisibleValues(t, writer.table))
}

func TestNativeWriterConcurrentIndexKeyFence(t *testing.T) {
	ctx := context.Background()
	cfg, stream, cat := nativeWriterFixture(t, types.UpdateTypePosition, true, "")
	index := &nativeMemoryIndex{rows: make(map[string]types.RowLocation)}
	seed := openNativeTestWriter(ctx, t, cfg, stream, cat, index, "snapshot", true)
	require.NoError(t, seed.Write(ctx, []types.RawRecord{nativeTestRecord("a", "r", 1, "one"), nativeTestRecord("b", "r", 2, "one")}))
	require.NoError(t, seed.Close(ctx, nil))
	first := openNativeTestWriter(ctx, t, cfg, stream, cat, index, "first", false)
	stale := openNativeTestWriter(ctx, t, cfg, stream, cat, index, "same-key", false)
	disjoint := openNativeTestWriter(ctx, t, cfg, stream, cat, index, "different-key", false)
	require.NoError(t, first.Write(ctx, []types.RawRecord{nativeTestRecord("a", "u", 3, "one")}))
	require.NoError(t, stale.Write(ctx, []types.RawRecord{nativeTestRecord("a", "u", 4, "one")}))
	require.NoError(t, disjoint.Write(ctx, []types.RawRecord{nativeTestRecord("b", "u", 5, "one")}))
	require.NoError(t, first.Close(ctx, map[string]any{"state": "first"}))
	require.ErrorIs(t, stale.Close(ctx, map[string]any{"state": "stale"}), table.ErrConflictingDataFiles)
	require.NoError(t, disjoint.Close(ctx, map[string]any{"state": "disjoint"}))
	require.Equal(t, map[string]int64{"a": 3, "b": 5}, nativeVisibleValues(t, disjoint.table))
}

func TestNativeWriterTimestampPartitionAndPromotion(t *testing.T) {
	ctx := context.Background()
	cfg, stream, cat := nativeWriterFixture(t, types.UpdateTypeEquality, true, "/{_olake_timestamp, day}/{blob, bucket[8]}")
	stream.Stream.UpsertField("promoted", types.Int32, true, false)
	writer := openNativeTestWriter(ctx, t, cfg, stream, cat, nil, "partitioned", false)
	before, ok := writer.table.Schema().FindFieldByName("promoted")
	require.True(t, ok)
	columns := writer.schemaMap()
	columns["promoted"] = "long"
	require.NoError(t, writer.EvolveSchema(ctx, columns))
	after, ok := writer.table.Schema().FindFieldByName("promoted")
	require.True(t, ok)
	require.Equal(t, before.ID, after.ID)
	require.Equal(t, "long", after.Type.String())
	record := nativeTestRecord("a", "i", 1, "one")
	record.Data["promoted"] = int64(1) << 40
	require.NoError(t, writer.Write(ctx, []types.RawRecord{record}))
	require.NoError(t, writer.Close(ctx, map[string]any{"state": "partitioned"}))
	entries, err := nativeLiveEntries(ctx, writer.table)
	require.NoError(t, err)
	require.NotEmpty(t, entries)
	for _, entry := range entries {
		require.Len(t, entry.DataFile().Partition(), 2)
	}
	require.Equal(t, map[string]int64{"a": 1}, nativeVisibleValues(t, writer.table))
}

func TestNativeWriterPartialWriteFailureCleansFiles(t *testing.T) {
	ctx := context.Background()
	cfg, stream, cat := nativeWriterFixture(t, types.UpdateTypeEquality, true, "/{part, identity}")
	writer := openNativeTestWriter(ctx, t, cfg, stream, cat, nil, "bad", true)
	bad := nativeTestRecord("b", "r", 2, "two")
	bad.Data["blob"] = []byte{1}
	require.Error(t, writer.Write(ctx, []types.RawRecord{nativeTestRecord("a", "r", 1, "one"), bad}))
	fs, err := writer.table.FS(ctx)
	require.NoError(t, err)
	for _, file := range writer.dataFiles {
		_, err := fs.Open(file.FilePath())
		require.Error(t, err)
	}
	require.Nil(t, writer.table.CurrentSnapshot())
	require.Error(t, writer.Close(ctx, nil))
}

func TestNativeWriterCreationKeepsCatalogLocationAndCredentialsPrivate(t *testing.T) {
	ctx := context.Background()
	cfg, stream, cat := nativeWriterFixture(t, types.UpdateTypeEquality, true, "")
	warehouse := cfg.IcebergS3Path
	// Managed REST catalogs interpret this as a warehouse identifier, not a filesystem path.
	cfg.IcebergS3Path = "arn:aws:s3tables:us-east-2:123456789012:bucket/example"
	cfg.AccessKey, cfg.SecretKey, cfg.SessionToken = "private-access", "private-secret", "private-session"
	cfg.GCPServiceAccountJSON = "private-google-key"
	writer := openNativeTestWriter(ctx, t, cfg, stream, cat, nil, "creation", true)
	defer writer.Abort()
	persisted, err := cat.LoadTable(ctx, writer.table.Identifier())
	require.NoError(t, err)
	require.True(t, strings.HasPrefix(persisted.Location(), warehouse), "the catalog must own the physical location")
	metadata, err := json.Marshal(persisted.Metadata())
	require.NoError(t, err)
	for _, secret := range []string{cfg.AccessKey, cfg.SecretKey, cfg.SessionToken, cfg.GCPServiceAccountJSON} {
		require.NotContains(t, string(metadata), secret)
	}
	require.Equal(t, "parquet", persisted.Properties()["write.format.default"])
}

func nativePendingLocation(t *testing.T, writer *nativeWriter, id string) types.RowLocation {
	t.Helper()
	if writer.indexThread == nil {
		location, found := writer.latest[id]
		require.True(t, found)
		return location
	}
	location, found, err := writer.indexThread.Lookup(id)
	require.NoError(t, err)
	require.True(t, found)
	return location
}

func TestNativeStagedRewritePreservesSchemaPartitionAndIndex(t *testing.T) {
	for _, mode := range []types.UpdateType{types.UpdateTypeEquality, types.UpdateTypePosition, types.UpdateTypeDeletionVector} {
		t.Run(string(mode), func(t *testing.T) {
			ctx := context.Background()
			cfg, stream, cat := nativeWriterFixture(t, mode, true, "/{part, identity}")
			var index *nativeMemoryIndex
			if mode != types.UpdateTypeEquality {
				index = &nativeMemoryIndex{rows: make(map[string]types.RowLocation)}
			}
			writer := openNativeTestWriter(ctx, t, cfg, stream, cat, index, "staged-rewrite", false)
			require.NoError(t, writer.Write(ctx, []types.RawRecord{nativeTestRecord("a", "u", 1, "one"), nativeTestRecord("b", "u", 2, "one"), nativeTestRecord("c", "u", 3, "two")}))
			originalB := nativePendingLocation(t, writer, "b")
			originalC := nativePendingLocation(t, writer, "c")
			require.Equal(t, int64(1), originalB.Position)
			// TOAST recovery can read an earlier write before Close. Compaction
			// must not alter that path or its ordinals during Write/evolution.
			require.NoError(t, writer.EnsureReadable(ctx, []string{originalB.FilePath, originalC.FilePath}))
			rows, err := writer.ReadRows(ctx, originalB.FilePath, []int64{0, originalB.Position}, []string{"value", "blob"})
			require.NoError(t, err)
			require.Equal(t, int64(1), rows[0]["value"])
			require.Equal(t, int64(2), rows[1]["value"])
			columns := writer.schemaMap()
			columns["later"] = "string"
			require.NoError(t, writer.EvolveSchema(ctx, columns))
			a := nativeTestRecord("a", "u", 4, "one")
			a.Data["later"] = "new-schema"
			require.NoError(t, writer.Write(ctx, []types.RawRecord{a, nativeTestRecord("c", "u", 5, "three")}))
			require.NoError(t, writer.EnsureReadable(ctx, []string{originalB.FilePath}))
			require.NoError(t, writer.Close(ctx, map[string]any{"state": "compacted"}))
			require.Equal(t, map[string]int64{"a": 4, "b": 2, "c": 5}, nativeVisibleValues(t, writer.table))
			finalB := writer.latest["b"]
			if index != nil {
				finalB = index.rows["b"]
				require.Equal(t, nativeSnapshotID(writer.table), index.snapshot)
			}
			require.NotEqual(t, originalB.FilePath, finalB.FilePath)
			require.Zero(t, finalB.Position)
			rows, err = writer.ReadRows(ctx, finalB.FilePath, []int64{finalB.Position}, []string{"value", "blob", "later"})
			require.NoError(t, err)
			require.Equal(t, int64(2), rows[0]["value"])
			require.Equal(t, []byte{0, 1, 2, 3}, rows[0]["blob"])
			require.Nil(t, rows[0]["later"])
			fs, err := writer.table.FS(ctx)
			require.NoError(t, err)
			for _, path := range []string{originalB.FilePath, originalC.FilePath} {
				_, err := fs.Open(path)
				require.Error(t, err)
			}
			entries, err := nativeLiveEntries(ctx, writer.table)
			require.NoError(t, err)
			for _, entry := range entries {
				require.Equal(t, apacheiceberg.EntryContentData, entry.DataFile().ContentType(), "own superseded rows require no delete file")
			}
		})
	}
}

type nativeRewriteFaultFS struct {
	iceio.WriteFileIO
	failAt  int
	creates int
	created []string
}

func (fs *nativeRewriteFaultFS) Create(path string) (iceio.FileWriter, error) {
	fs.creates++
	if fs.creates == fs.failAt {
		return nil, errors.New("injected staged rewrite upload failure")
	}
	writer, err := fs.WriteFileIO.Create(path)
	if err == nil {
		fs.created = append(fs.created, path)
	}
	return writer, err
}

func TestNativeStagedRewriteFailureKeepsPendingIndexAtomic(t *testing.T) {
	ctx := context.Background()
	cfg, stream, cat := nativeWriterFixture(t, types.UpdateTypePosition, true, "/{part, identity}")
	index := &nativeMemoryIndex{rows: make(map[string]types.RowLocation)}
	writer := openNativeTestWriter(ctx, t, cfg, stream, cat, index, "rewrite-fails", false)
	require.NoError(t, writer.Write(ctx, []types.RawRecord{nativeTestRecord("a", "u", 1, "one"), nativeTestRecord("x", "u", 2, "one"), nativeTestRecord("b", "u", 3, "two"), nativeTestRecord("y", "u", 4, "two")}))
	require.NoError(t, writer.Write(ctx, []types.RawRecord{nativeTestRecord("a", "u", 5, "three"), nativeTestRecord("b", "u", 6, "three")}))
	pending := writer.indexThread
	beforeX := nativePendingLocation(t, writer, "x")
	beforeY := nativePendingLocation(t, writer, "y")
	originalPaths := slices.Collect(maps.Keys(writer.paths))
	baseFS, err := writer.table.FS(ctx)
	require.NoError(t, err)
	writeFS, ok := baseFS.(iceio.WriteFileIO)
	require.True(t, ok)
	fault := &nativeRewriteFaultFS{WriteFileIO: writeFS, failAt: 2}
	writer.table = table.New(writer.table.Identifier(), writer.table.Metadata(), writer.table.MetadataLocation(), func(context.Context) (iceio.IO, error) { return fault, nil }, cat)
	err = writer.Close(ctx, map[string]any{"state": "must-not-commit"})
	require.ErrorContains(t, err, "injected staged rewrite upload failure")
	require.Len(t, fault.created, 1, "first replacement must succeed before the injected failure")
	afterX, found, err := pending.Lookup("x")
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, beforeX, afterX)
	afterY, found, err := pending.Lookup("y")
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, beforeY, afterY)
	require.Empty(t, index.rows)
	require.Zero(t, index.snapshot)
	for _, path := range append(originalPaths, fault.created...) {
		_, err := baseFS.Open(path)
		require.Error(t, err)
	}
	fresh, err := cat.LoadTable(ctx, writer.table.Identifier())
	require.NoError(t, err)
	require.Nil(t, fresh.CurrentSnapshot())
	require.NotContains(t, fresh.Properties()[nativeRecoveryProperty], "must-not-commit")
}

func TestNativeStagedRewriteAbortCleansReplacementFiles(t *testing.T) {
	ctx := context.Background()
	cfg, stream, cat := nativeWriterFixture(t, types.UpdateTypePosition, true, "")
	index := &nativeMemoryIndex{rows: make(map[string]types.RowLocation)}
	writer := openNativeTestWriter(ctx, t, cfg, stream, cat, index, "rewrite-abort", false)
	require.NoError(t, writer.Write(ctx, []types.RawRecord{nativeTestRecord("a", "u", 1, "one"), nativeTestRecord("b", "u", 2, "one"), nativeTestRecord("a", "u", 3, "one")}))
	original := nativePendingLocation(t, writer, "b")
	require.NoError(t, writer.compactStagedDataFiles(ctx))
	location := nativePendingLocation(t, writer, "b")
	require.NotEqual(t, original.FilePath, location.FilePath)
	require.Zero(t, location.Position)
	paths := slices.Collect(maps.Keys(writer.paths))
	require.NotEmpty(t, paths)
	writer.Abort()
	fs, err := writer.table.FS(ctx)
	require.NoError(t, err)
	for _, path := range paths {
		_, err := fs.Open(path)
		require.Error(t, err)
	}
	require.Empty(t, index.rows)
	require.Zero(t, index.snapshot)
}

func TestNativeStagedRewriteStreamsAcrossPhysicalBatches(t *testing.T) {
	ctx := context.Background()
	cfg, stream, cat := nativeWriterFixture(t, types.UpdateTypePosition, true, "")
	index := &nativeMemoryIndex{rows: make(map[string]types.RowLocation)}
	writer := openNativeTestWriter(ctx, t, cfg, stream, cat, index, "rewrite-batches", false)
	const count = 9000
	records := make([]types.RawRecord, count)
	for row := range records {
		records[row] = nativeTestRecord(fmt.Sprint(row), "u", int64(row), "one")
	}
	require.NoError(t, writer.Write(ctx, records))
	var updates []types.RawRecord
	for _, position := range []int{0, 8191, 8192, count - 1} {
		updates = append(updates, nativeTestRecord(fmt.Sprint(position), "u", int64(count+position), "one"))
	}
	require.NoError(t, writer.Write(ctx, updates))
	require.NoError(t, writer.Close(ctx, map[string]any{"state": "streamed"}))
	for _, position := range []int{1, 8190, 8193, count - 2} {
		location := index.rows[fmt.Sprint(position)]
		rows, err := writer.ReadRows(ctx, location.FilePath, []int64{location.Position}, []string{"value"})
		require.NoError(t, err)
		require.Equal(t, int64(position), rows[location.Position]["value"])
	}
	entries, err := nativeLiveEntries(ctx, writer.table)
	require.NoError(t, err)
	var total int64
	for _, entry := range entries {
		require.Equal(t, apacheiceberg.EntryContentData, entry.DataFile().ContentType())
		total += entry.DataFile().Count()
	}
	fs, err := writer.table.FS(ctx)
	require.NoError(t, err)
	for _, dataFile := range writer.dataFiles {
		input, err := fs.Open(dataFile.FilePath())
		require.NoError(t, err)
		reader, err := parquetfile.NewParquetReader(input)
		require.NoError(t, err)
		require.Equal(t, 1, reader.NumRowGroups(), "9,000-row rewrites must not inherit 8,192-row scan batch boundaries")
		require.NoError(t, reader.Close())
	}
	require.Equal(t, int64(count), total)
	require.Len(t, index.rows, count)
}

func TestNativeStagedRewriteUnknownCommitKeepsPublishedReplacements(t *testing.T) {
	ctx := context.Background()
	cfg, stream, realCatalog := nativeWriterFixture(t, types.UpdateTypeDeletionVector, true, "")
	index := &nativeMemoryIndex{rows: make(map[string]types.RowLocation)}
	seed := openNativeTestWriter(ctx, t, cfg, stream, realCatalog, index, "create", true)
	seed.Abort()
	cat := &nativeCommitFaultCatalog{Catalog: realCatalog}
	writer := openNativeTestWriter(ctx, t, cfg, stream, cat, index, "rewrite-unknown", false)
	require.NoError(t, writer.Write(ctx, []types.RawRecord{nativeTestRecord("a", "u", 1, "one"), nativeTestRecord("b", "u", 2, "one"), nativeTestRecord("a", "u", 3, "one")}))
	original := nativePendingLocation(t, writer, "b")
	cat.fault = func(ctx context.Context, id table.Identifier, reqs []table.Requirement, updates []table.Update) (bool, error) {
		_, _, err := realCatalog.CommitTable(ctx, id, reqs, updates)
		require.NoError(t, err)
		return true, errors.New("connection reset after publishing compacted data")
	}
	require.Error(t, writer.Close(ctx, map[string]any{"state": "published"}))
	require.True(t, writer.uncertain)
	writer.Abort()
	fs, err := writer.table.FS(ctx)
	require.NoError(t, err)
	for _, file := range writer.dataFiles {
		require.NotEqual(t, original.FilePath, file.FilePath())
		input, err := fs.Open(file.FilePath())
		require.NoError(t, err)
		require.NoError(t, input.Close())
	}
	require.Empty(t, index.rows)
	require.Zero(t, index.snapshot, "unknown outcomes must not checkpoint the local index")
	recovered := openNativeTestWriter(ctx, t, cfg, stream, realCatalog, index, "recover", false)
	require.Equal(t, map[string]int64{"a": 3, "b": 2}, nativeVisibleValues(t, recovered.table))
	require.Equal(t, int64(0), index.rows["b"].Position)
	require.Equal(t, nativeSnapshotID(recovered.table), index.snapshot)
	require.Contains(t, recovered.table.Properties()[nativeRecoveryProperty], "published")
	recovered.Abort()
}

type nativeDeleteDeniedFS struct {
	iceio.WriteFileIO
	prefix  string
	root    string
	deletes atomic.Int32
}

func (fs *nativeDeleteDeniedFS) local(path string) string {
	if relative, found := strings.CutPrefix(path, fs.prefix+"/"); found {
		return filepath.Join(fs.root, relative)
	}
	return path
}

func (fs *nativeDeleteDeniedFS) Open(path string) (iceio.File, error) {
	return fs.WriteFileIO.Open(fs.local(path))
}

func (fs *nativeDeleteDeniedFS) Create(path string) (iceio.FileWriter, error) {
	return fs.WriteFileIO.Create(fs.local(path))
}

func (fs *nativeDeleteDeniedFS) WriteFile(path string, data []byte) error {
	return fs.WriteFileIO.WriteFile(fs.local(path), data)
}

func (fs *nativeDeleteDeniedFS) Remove(path string) error {
	if !strings.HasPrefix(path, fs.prefix+"/") {
		return fs.WriteFileIO.Remove(path)
	}
	fs.deletes.Add(1)
	return &os.PathError{Op: "remove", Path: path, Err: os.ErrPermission}
}

func nativeDeleteDeniedStorage(ctx context.Context, t *testing.T, mode types.UpdateType, bucket string) (*Config, *types.ConfiguredStream, *nativeCommitFaultCatalog, *nativeDeleteDeniedFS) {
	t.Helper()
	cfg, stream, base := nativeWriterFixture(t, mode, true, "/{part, identity}")
	initial := openNativeTestWriter(ctx, t, cfg, stream, base, nil, "initialize-cleanup", true)
	tx := initial.table.NewTransaction()
	prefix := "s3://" + bucket
	require.NoError(t, tx.SetProperties(apacheiceberg.Properties{
		table.WriteDataPathKey:     prefix + "/data",
		table.WriteMetadataPathKey: prefix + "/metadata",
	}))
	_, err := tx.Commit(ctx)
	require.NoError(t, err)
	initial.Abort()
	storage := &nativeDeleteDeniedFS{WriteFileIO: iceio.LocalFS{}, prefix: prefix, root: t.TempDir()}
	return cfg, stream, &nativeCommitFaultCatalog{Catalog: base, fs: storage}, storage
}

func TestNativeManagedStorageRewriteRetryAndAbort(t *testing.T) {
	for _, mode := range []types.UpdateType{types.UpdateTypeEquality, types.UpdateTypePosition, types.UpdateTypeDeletionVector} {
		t.Run(string(mode), func(t *testing.T) {
			ctx := context.Background()
			cfg, stream, cat, storage := nativeDeleteDeniedStorage(ctx, t, mode, "example--table-s3")
			var index *nativeMemoryIndex
			if mode != types.UpdateTypeEquality {
				index = &nativeMemoryIndex{rows: make(map[string]types.RowLocation)}
			}
			writer := openNativeTestWriter(ctx, t, cfg, stream, cat, index, "managed-rewrite", false)
			require.NoError(t, writer.Write(ctx, []types.RawRecord{nativeTestRecord("a", "u", 1, "one"), nativeTestRecord("b", "u", 2, "one")}))
			originalB := nativePendingLocation(t, writer, "b")
			require.NoError(t, writer.Write(ctx, []types.RawRecord{nativeTestRecord("a", "u", 3, "two"), nativeTestRecord("a", "d", 4, "two"), nativeTestRecord("c", "c", 5, "one")}))
			require.NoError(t, writer.Close(ctx, map[string]any{"state": "managed-committed"}))
			require.Equal(t, map[string]int64{"a": 4, "b": 2, "c": 5}, nativeVisibleValues(t, writer.table))
			require.Contains(t, writer.table.Properties()[nativeRecoveryProperty], "managed-committed")
			_, err := os.Stat(storage.local(originalB.FilePath))
			require.NoError(t, err, "obsolete bytes are retained for managed maintenance")
			entries, err := nativeLiveEntries(ctx, writer.table)
			require.NoError(t, err)
			for _, entry := range entries {
				require.NotEqual(t, originalB.FilePath, entry.DataFile().FilePath(), "obsolete staged files must not be committed")
			}
			if index != nil {
				location := index.rows["b"]
				require.NotEqual(t, originalB.FilePath, location.FilePath)
				require.Zero(t, location.Position)
				rows, err := writer.ReadRows(ctx, location.FilePath, []int64{location.Position}, []string{"value"})
				require.NoError(t, err)
				require.Equal(t, int64(2), rows[location.Position]["value"])
				require.Equal(t, nativeSnapshotID(writer.table), index.snapshot)
			}
			updater := openNativeTestWriter(ctx, t, cfg, stream, cat, index, "managed-conflict", false)
			require.NoError(t, updater.Write(ctx, []types.RawRecord{nativeTestRecord("b", "u", 6, "two")}))
			cat.fault = func(context.Context, table.Identifier, []table.Requirement, []table.Update) (bool, error) {
				cat.fault = nil
				return true, table.ErrCommitFailed
			}
			require.NoError(t, updater.Close(ctx, map[string]any{"state": "managed-retried"}))
			require.Equal(t, map[string]int64{"a": 4, "b": 6, "c": 5}, nativeVisibleValues(t, updater.table))
			require.Contains(t, updater.table.Properties()[nativeRecoveryProperty], "managed-retried")
			aborted := openNativeTestWriter(ctx, t, cfg, stream, cat, index, "managed-abort", false)
			require.NoError(t, aborted.Write(ctx, []types.RawRecord{nativeTestRecord("unpublished", "c", 7, "one")}))
			unpublished := nativePendingLocation(t, aborted, "unpublished")
			aborted.Abort()
			_, err = os.Stat(storage.local(unpublished.FilePath))
			require.NoError(t, err)
			fresh, err := cat.LoadTable(ctx, updater.table.Identifier())
			require.NoError(t, err)
			require.Equal(t, map[string]int64{"a": 4, "b": 6, "c": 5}, nativeVisibleValues(t, fresh))
			require.Zero(t, storage.deletes.Load(), "S3 Tables must never receive object DELETE requests")
		})
	}
}

func TestNativeOrdinaryStorageDeleteFailurePreventsCommit(t *testing.T) {
	ctx := context.Background()
	cfg, stream, cat, storage := nativeDeleteDeniedStorage(ctx, t, types.UpdateTypePosition, "ordinary-bucket")
	index := &nativeMemoryIndex{rows: make(map[string]types.RowLocation)}
	writer := openNativeTestWriter(ctx, t, cfg, stream, cat, index, "ordinary-denied", false)
	require.NoError(t, writer.Write(ctx, []types.RawRecord{nativeTestRecord("a", "u", 1, "one"), nativeTestRecord("a", "u", 2, "one")}))
	err := writer.Close(ctx, map[string]any{"state": "must-not-commit"})
	require.ErrorIs(t, err, os.ErrPermission)
	require.Positive(t, storage.deletes.Load())
	fresh, err := cat.LoadTable(ctx, writer.table.Identifier())
	require.NoError(t, err)
	require.Nil(t, fresh.CurrentSnapshot())
	require.NotContains(t, fresh.Properties()[nativeRecoveryProperty], "must-not-commit")
	require.Empty(t, index.rows)
	require.Zero(t, index.snapshot)
}

// This boundary executes real REST requests and real Hadoop metadata commits.
// The coordinator protects one catalog lifetime in one OLake process; it is not
// distributed ownership or a fence against another property-only writer.
type nativeRESTCommitBoundary struct {
	cat                catalog.Catalog
	mu                 sync.Mutex
	requests           []string
	requirements       [][]table.Requirement
	throttleResponses  int
	postBodies         [][]byte
	postHeaders        []http.Header
	failAfterCommit    bool
	failLoad           bool
	initialCredentials []rest.StorageCredential
	freshCredentials   []rest.StorageCredential
	credentialRequests int
}

func (b *nativeRESTCommitBoundary) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if r.Method == http.MethodGet && r.URL.Path == "/v1/config" {
		_, _ = w.Write([]byte(`{"defaults":{},"overrides":{},"endpoints":["GET /v1/{prefix}/namespaces/{namespace}/tables/{table}","POST /v1/{prefix}/namespaces/{namespace}/tables/{table}","GET /v1/{prefix}/namespaces/{namespace}/tables/{table}/credentials"]}`))
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if r.Method == http.MethodGet && r.URL.Path == "/v1/namespaces/db/tables/records" && b.failLoad {
		http.Error(w, "injected setup metadata failure", http.StatusServiceUnavailable)
		return
	}
	if r.Method == http.MethodGet && r.URL.Path == "/v1/namespaces/db/tables/records/credentials" {
		b.credentialRequests++
		_ = json.NewEncoder(w).Encode(map[string]any{"storage-credentials": b.freshCredentials})
		return
	}
	if r.URL.Path != "/v1/namespaces/db/tables/records" {
		http.Error(w, "unexpected catalog path", http.StatusNotFound)
		return
	}
	b.requests = append(b.requests, r.Method)
	ident := table.Identifier{"db", "records"}
	current, err := b.cat.LoadTable(r.Context(), ident)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	metadata, location := current.Metadata(), current.MetadataLocation()
	switch r.Method {
	case http.MethodGet:
	case http.MethodPost:
		payload, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		b.postBodies = append(b.postBodies, payload)
		b.postHeaders = append(b.postHeaders, r.Header.Clone())
		if b.throttleResponses > 0 {
			b.throttleResponses--
			w.Header().Set("Retry-After", "0")
			w.WriteHeader(http.StatusTooManyRequests)
			_, _ = io.WriteString(w, "proxy rejected mutation; deliberately not JSON")
			return
		}
		var request struct {
			Requirements table.Requirements `json:"requirements"`
			Updates      table.Updates      `json:"updates"`
		}
		if err := json.Unmarshal(payload, &request); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		b.requirements = append(b.requirements, request.Requirements)
		for _, requirement := range request.Requirements {
			if err := requirement.Validate(metadata); err != nil {
				w.WriteHeader(http.StatusConflict)
				_ = json.NewEncoder(w).Encode(map[string]any{"error": map[string]any{"message": err.Error(), "type": "CommitFailedException", "code": http.StatusConflict}})
				return
			}
		}
		metadata, location, err = b.cat.CommitTable(r.Context(), ident, request.Requirements, request.Updates)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		if b.failAfterCommit {
			b.failAfterCommit = false
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte(`{"error":{"message":"response lost after publication","type":"CommitStateUnknownException","code":503}}`))
			return
		}
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	_ = json.NewEncoder(w).Encode(map[string]any{"metadata": metadata, "metadata-location": location, "config": map[string]string{}, "storage-credentials": b.initialCredentials})
}

func (b *nativeRESTCommitBoundary) tableRequests() []string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return slices.Clone(b.requests)
}

func nativeRESTWriterFixture(t *testing.T, mode types.UpdateType, rows ...types.RawRecord) (*Config, *types.ConfiguredStream, catalog.Catalog, *nativeRESTCommitBoundary) {
	t.Helper()
	ctx := context.Background()
	cfg, stream, base := nativeWriterFixture(t, mode, true, "")
	seed := openNativeTestWriter(ctx, t, cfg, stream, base, nil, "bootstrap", true)
	if len(rows) > 0 {
		require.NoError(t, seed.Write(ctx, rows))
		require.NoError(t, seed.Close(ctx, nil))
	} else {
		seed.Abort()
	}
	boundary := &nativeRESTCommitBoundary{cat: base}
	server := httptest.NewServer(boundary)
	t.Cleanup(server.Close)
	cfg.CatalogType, cfg.CatalogName, cfg.RestCatalogURL = RestCatalog, "coordinator-boundary", server.URL
	cfg.Region, cfg.AccessKey, cfg.SecretKey = "us-east-1", "test-access", "test-secret"
	cat, cleanup, err := openNativeCatalog(ctx, cfg)
	require.NoError(t, err)
	t.Cleanup(func() { cleanup(context.Background()) })
	return cfg, stream, cat, boundary
}

func TestNativeRESTControlledThrottleWriterRecovery(t *testing.T) {
	for _, exhausted := range []bool{false, true} {
		t.Run(map[bool]string{false: "recovered-429", true: "exhausted-429"}[exhausted], func(t *testing.T) {
			ctx, fence := nativeExecutionTestContext(t)
			cfg, stream, original, boundary := nativeRESTWriterFixture(t, types.UpdateTypeEquality, nativeTestRecord("old", "r", 1, "one"))
			require.NoError(t, original.(*nativeCatalog).Close())
			cfg.RestSigningV4, cfg.RestSigningName, cfg.RestSigningRegion = true, "s3tables", "us-east-1"
			cat, cleanup, err := openNativeCatalog(ctx, cfg)
			require.NoError(t, err)
			t.Cleanup(func() { cleanup(context.Background()) })
			writer := openNativeTestWriter(ctx, t, cfg, stream, cat, nil, "throttled", false)
			beforeMetadata := writer.table.MetadataLocation()
			beforeRecovery := writer.table.Properties()[nativeRecoveryProperty]
			require.NoError(t, writer.Write(ctx, []types.RawRecord{nativeTestRecord("new", "i", 2, "one")}))
			paths := slices.Collect(maps.Keys(writer.paths))
			boundary.mu.Lock()
			boundary.throttleResponses = 2
			if exhausted {
				boundary.throttleResponses = nativeRESTThrottleMaxAttempts
			}
			boundary.mu.Unlock()
			err = writer.Close(ctx, map[string]any{"state": `{"attempted":2}`})
			if exhausted {
				var rejected *nativeRESTThrottleRejection
				require.ErrorAs(t, err, &rejected, "the real SDK must preserve the typed wire rejection")
				require.True(t, nativeMutationOutcomeKnown(err))
				require.False(t, writer.uncertain)
				require.False(t, writer.coordinator.unknown)
				require.Equal(t, beforeMetadata, writer.table.MetadataLocation())
				require.Equal(t, beforeRecovery, writer.table.Properties()[nativeRecoveryProperty])
				persisted, err := boundary.cat.LoadTable(ctx, writer.table.Identifier())
				require.NoError(t, err)
				require.Equal(t, map[string]int64{"old": 1}, nativeVisibleValues(t, persisted))
				require.Equal(t, beforeRecovery, persisted.Properties()[nativeRecoveryProperty])
				fs, err := writer.table.FS(ctx)
				require.NoError(t, err)
				for _, path := range paths {
					_, err := fs.Open(path)
					require.Error(t, err, "definitively rejected staged files must be cleaned")
				}
			} else {
				require.NoError(t, err)
				require.Equal(t, map[string]int64{"old": 1, "new": 2}, nativeVisibleValues(t, writer.table))
			}
			boundary.mu.Lock()
			bodies, headers := slices.Clone(boundary.postBodies), slices.Clone(boundary.postHeaders)
			boundary.mu.Unlock()
			wantAttempts := 3
			if exhausted {
				wantAttempts = nativeRESTThrottleMaxAttempts
			}
			require.Len(t, bodies, wantAttempts)
			for index, body := range bodies {
				require.Equal(t, bodies[0], body, "wire replay must not rebuild the commit")
				require.Equal(t, headers[0], headers[index], "SigV4 and all signed headers must survive replay")
				require.Contains(t, headers[index].Get("Authorization"), "/us-east-1/s3tables/aws4_request")
				require.NotEmpty(t, headers[index].Get("X-Amz-Content-Sha256"))
			}
			require.NoError(t, nativeExecutionWritable(ctx, cfg))
			if exhausted {
				retry := openNativeTestWriter(ctx, t, cfg, stream, cat, nil, "retry-after-rejection", false)
				require.NoError(t, retry.Write(ctx, []types.RawRecord{nativeTestRecord("new", "i", 2, "one")}))
				require.NoError(t, retry.Close(ctx, map[string]any{"state": `{"retried":3}`}))
				require.Equal(t, map[string]int64{"old": 1, "new": 2}, nativeVisibleValues(t, retry.table))
				require.NotContains(t, retry.table.Properties()[nativeRecoveryProperty], "attempted")
				require.Contains(t, retry.table.Properties()[nativeRecoveryProperty], "retried")
			}
			closeNativeTestFence(t, fence)
			cleanup(context.Background())
			utils.SourceDrained(ctx, nil)
			result, ok := utils.OrdinaryExecutionResult(ctx)
			require.True(t, ok)
			require.Equal(t, "settled", result.Outcome)
		})
	}
}

func nativeRecoveryOffsets(t *testing.T, tbl *table.Table) (map[string]int, []string) {
	t.Helper()
	var recovery struct {
		State  string   `json:"state"`
		Chunks []string `json:"full_refresh_committed_ids"`
	}
	require.NoError(t, json.Unmarshal([]byte(tbl.Properties()[nativeRecoveryProperty]), &recovery))
	var offsets map[string]int
	require.NoError(t, json.Unmarshal([]byte(recovery.State), &offsets))
	return offsets, recovery.Chunks
}

func TestNativeRESTCoordinatorCDCUsesReturnedMetadata(t *testing.T) {
	for _, mode := range []types.UpdateType{types.UpdateTypeEquality, types.UpdateTypePosition, types.UpdateTypeDeletionVector} {
		t.Run(string(mode), func(t *testing.T) {
			ctx := context.Background()
			cfg, stream, cat, boundary := nativeRESTWriterFixture(t, mode, nativeTestRecord("a", "r", 1, "one"))
			var index *nativeMemoryIndex
			if mode != types.UpdateTypeEquality {
				index = &nativeMemoryIndex{rows: make(map[string]types.RowLocation)}
			}
			first := openNativeTestWriter(ctx, t, cfg, stream, cat, index, "cdc-first", false)
			require.Equal(t, []string{http.MethodGet}, boundary.tableRequests())
			require.NoError(t, first.Write(ctx, []types.RawRecord{nativeTestRecord("a", "u", 2, "one")}))
			require.NoError(t, first.Close(ctx, map[string]any{"state": `{"first":2}`}))
			require.Equal(t, []string{http.MethodGet, http.MethodPost}, boundary.tableRequests(), "normal CDC must not issue a precommit metadata GET")
			second := openNativeTestWriter(ctx, t, cfg, stream, cat, index, "cdc-second", false)
			require.Equal(t, first.table.MetadataLocation(), second.table.MetadataLocation())
			require.Equal(t, []string{http.MethodGet, http.MethodPost, http.MethodGet}, boundary.tableRequests(), "REST writer setup retains its credential-refresh GET")
			require.NoError(t, second.EvolveSchema(ctx, second.schemaMap()))
			require.Equal(t, []string{http.MethodGet, http.MethodPost, http.MethodGet}, boundary.tableRequests(), "unchanged schema evolution must not reload or recommit")
			require.NoError(t, second.Write(ctx, []types.RawRecord{nativeTestRecord("a", "u", 3, "one")}))
			require.NoError(t, second.Close(ctx, map[string]any{"state": `{"second":3}`}))
			require.Equal(t, []string{http.MethodGet, http.MethodPost, http.MethodGet, http.MethodPost}, boundary.tableRequests())
			require.Equal(t, map[string]int64{"a": 3}, nativeVisibleValues(t, second.table))
			offsets, _ := nativeRecoveryOffsets(t, second.table)
			require.Equal(t, map[string]int{"first": 2, "second": 3}, offsets)
			if index != nil {
				require.Equal(t, nativeSnapshotID(second.table), index.snapshot)
				require.Equal(t, second.dataFiles[0].FilePath(), index.rows["a"].FilePath)
			}
			boundary.mu.Lock()
			requirements := slices.Clone(boundary.requirements)
			boundary.mu.Unlock()
			for _, commit := range requirements {
				var kinds []string
				for _, requirement := range commit {
					kinds = append(kinds, requirement.GetType())
				}
				require.Subset(t, kinds, []string{"assert-table-uuid", "assert-ref-snapshot-id", "assert-default-spec-id", "assert-default-sort-order-id"})
			}
		})
	}
}

func TestNativeRESTCoordinatorConcurrentFullRefreshChunks(t *testing.T) {
	for _, mode := range []types.UpdateType{types.UpdateTypeEquality, types.UpdateTypePosition, types.UpdateTypeDeletionVector} {
		t.Run(string(mode), func(t *testing.T) {
			ctx := context.Background()
			cfg, stream, cat, boundary := nativeRESTWriterFixture(t, mode)
			initial, err := boundary.cat.LoadTable(ctx, table.Identifier{"db", "records"})
			require.NoError(t, err)
			tx := initial.NewTransaction()
			require.NoError(t, tx.SetProperties(apacheiceberg.Properties{nativeRecoveryProperty: `{"state":"{\"before_chunks\":17}","dedup_inserts":false,"retained":"keep"}`}))
			_, err = tx.Commit(ctx)
			require.NoError(t, err)
			var index *nativeMemoryIndex
			if mode != types.UpdateTypeEquality {
				index = &nativeMemoryIndex{rows: make(map[string]types.RowLocation)}
			}
			const chunks = 6
			writers := make([]*nativeWriter, chunks)
			expectedRows := make(map[string]int64)
			var expectedChunks []string
			for chunk := range chunks {
				id := fmt.Sprintf("chunk-%d", chunk)
				writers[chunk] = openNativeTestWriter(ctx, t, cfg, stream, cat, index, id, true)
				require.NoError(t, writers[chunk].Write(ctx, []types.RawRecord{nativeTestRecord(id, "r", int64(chunk), "one")}))
				expectedRows[id] = int64(chunk)
				expectedChunks = append(expectedChunks, id)
			}
			var expectedRequests []string
			for range chunks {
				expectedRequests = append(expectedRequests, http.MethodGet)
			}
			require.Equal(t, expectedRequests, boundary.tableRequests(), "REST chunk setup keeps one credential-refresh GET per writer")
			start := make(chan struct{})
			errors := make(chan error, chunks)
			for _, writer := range writers {
				go func() {
					<-start
					errors <- writer.Close(ctx, nil)
				}()
			}
			close(start)
			for range chunks {
				require.NoError(t, <-errors)
			}
			for range chunks {
				expectedRequests = append(expectedRequests, http.MethodPost)
			}
			require.Equal(t, expectedRequests, boundary.tableRequests())
			if index != nil {
				require.Len(t, index.rows, chunks, "serialized commits must publish every chunk to the shared index")
				for chunk, writer := range writers {
					id := fmt.Sprintf("chunk-%d", chunk)
					require.Equal(t, writer.dataFiles[0].FilePath(), index.rows[id].FilePath)
				}
			}
			reader := openNativeTestWriter(ctx, t, cfg, stream, cat, index, "after-chunks", true)
			defer reader.Abort()
			expectedRequests = append(expectedRequests, http.MethodGet)
			require.Equal(t, expectedRequests, boundary.tableRequests())
			require.Equal(t, expectedRows, nativeVisibleValues(t, reader.table))
			if index != nil {
				require.Equal(t, nativeSnapshotID(reader.table), index.snapshot)
			}
			offsets, committedChunks := nativeRecoveryOffsets(t, reader.table)
			require.Equal(t, map[string]int{"before_chunks": 17}, offsets)
			require.ElementsMatch(t, expectedChunks, committedChunks)
			var recovery struct {
				DedupInserts *bool  `json:"dedup_inserts"`
				Retained     string `json:"retained"`
			}
			require.NoError(t, json.Unmarshal([]byte(reader.table.Properties()[nativeRecoveryProperty]), &recovery))
			require.NotNil(t, recovery.DedupInserts)
			require.True(t, *recovery.DedupInserts)
			require.Equal(t, "keep", recovery.Retained, "chunk commits preserve unrelated recovery properties")
		})
	}
}

func TestNativeRESTCoordinatorPropertyOnlyCommitsMergeLocally(t *testing.T) {
	ctx := context.Background()
	cfg, stream, cat, boundary := nativeRESTWriterFixture(t, types.UpdateTypeEquality, nativeTestRecord("a", "r", 1, "one"))
	first := openNativeTestWriter(ctx, t, cfg, stream, cat, nil, "state-first", false)
	second := openNativeTestWriter(ctx, t, cfg, stream, cat, nil, "state-second", false)
	snapshot := nativeSnapshotID(first.table)
	start, results := make(chan struct{}), make(chan error, 2)
	go func() { <-start; results <- first.Close(ctx, map[string]any{"state": `{"partition_a":17}`}) }()
	go func() { <-start; results <- second.Close(ctx, map[string]any{"state": `{"partition_b":18}`}) }()
	close(start)
	for range 2 {
		require.NoError(t, <-results)
	}
	reader := openNativeTestWriter(ctx, t, cfg, stream, cat, nil, "read-state", false)
	defer reader.Abort()
	require.Equal(t, []string{http.MethodGet, http.MethodGet, http.MethodPost, http.MethodPost, http.MethodGet}, boundary.tableRequests())
	require.Equal(t, snapshot, nativeSnapshotID(reader.table))
	offsets, _ := nativeRecoveryOffsets(t, reader.table)
	require.Equal(t, map[string]int{"partition_a": 17, "partition_b": 18}, offsets)
	require.Equal(t, map[string]int64{"a": 1}, nativeVisibleValues(t, reader.table))
}

func TestNativeRESTCoordinatorUnknownCommitRequiresCatalogRecovery(t *testing.T) {
	for _, mode := range []types.UpdateType{types.UpdateTypeEquality, types.UpdateTypePosition, types.UpdateTypeDeletionVector} {
		t.Run(string(mode), func(t *testing.T) {
			ctx := context.Background()
			cfg, stream, cat, boundary := nativeRESTWriterFixture(t, mode, nativeTestRecord("a", "r", 1, "one"))
			var index *nativeMemoryIndex
			if mode != types.UpdateTypeEquality {
				index = &nativeMemoryIndex{rows: make(map[string]types.RowLocation)}
			}
			writer := openNativeTestWriter(ctx, t, cfg, stream, cat, index, "unknown", false)
			peer := openNativeTestWriter(ctx, t, cfg, stream, cat, index, "pending-peer", false)
			beforeMetadata := writer.table.MetadataLocation()
			beforeRecovery := writer.table.Properties()[nativeRecoveryProperty]
			var beforeRows map[string]types.RowLocation
			var beforeCheckpoint int64
			if index != nil {
				beforeRows, beforeCheckpoint = maps.Clone(index.rows), index.snapshot
			}
			require.NoError(t, writer.Write(ctx, []types.RawRecord{nativeTestRecord("a", "u", 2, "one")}))
			paths := slices.Collect(maps.Keys(writer.paths))
			boundary.mu.Lock()
			boundary.failAfterCommit = true
			boundary.mu.Unlock()
			require.Error(t, writer.Close(ctx, map[string]any{"state": `{"published":2}`}))
			require.True(t, writer.uncertain)
			require.Equal(t, beforeMetadata, writer.table.MetadataLocation(), "unacknowledged response must not become the writer view")
			require.Equal(t, beforeRecovery, writer.table.Properties()[nativeRecoveryProperty])
			require.Error(t, writer.Close(ctx, map[string]any{"state": `{"retry":3}`}))
			require.Error(t, peer.Close(ctx, map[string]any{"state": `{"unsafe_peer":4}`}))
			opts := &destination.Options{ThreadID: "unsafe-new"}
			if index != nil {
				opts.TableIndex = index
			}
			_, _, err := newNativeWriter(ctx, cfg, stream, opts, cat, types.NewStreamIndexThread(opts.TableIndex))
			require.Error(t, err, "new setup cannot clear an unknown catalog outcome")
			require.Equal(t, []string{http.MethodGet, http.MethodGet, http.MethodPost}, boundary.tableRequests(), "unknown outcome must not be retried or probed into success")
			writer.Abort()
			fs, err := writer.table.FS(ctx)
			require.NoError(t, err)
			for _, path := range paths {
				input, err := fs.Open(path)
				require.NoError(t, err, "possibly published staged data must survive abort")
				require.NoError(t, input.Close())
			}
			if index != nil {
				require.Equal(t, beforeRows, index.rows)
				require.Equal(t, beforeCheckpoint, index.snapshot)
			}
			// Reopen a catalog lifetime, then rebuild the index from authoritative
			// metadata before accepting more CDC. A GET alone is not recovery.
			reopened, cleanup, err := openNativeCatalog(ctx, cfg)
			require.NoError(t, err)
			defer cleanup(context.Background())
			recovered := openNativeTestWriter(ctx, t, cfg, stream, reopened, index, "recovery", false)
			defer recovered.Abort()
			require.Equal(t, []string{http.MethodGet, http.MethodGet, http.MethodPost, http.MethodGet}, boundary.tableRequests())
			require.Equal(t, map[string]int64{"a": 2}, nativeVisibleValues(t, recovered.table))
			offsets, _ := nativeRecoveryOffsets(t, recovered.table)
			require.Equal(t, map[string]int{"published": 2}, offsets)
			if index != nil {
				require.Equal(t, nativeSnapshotID(recovered.table), index.snapshot)
			}
		})
	}
}

func TestNativeCoordinatorTableLifecycleRejectsOldWriters(t *testing.T) {
	for _, purge := range []bool{false, true} {
		t.Run(fmt.Sprintf("purge=%t", purge), func(t *testing.T) {
			ctx := context.Background()
			cfg, stream, cat := nativeWriterFixture(t, types.UpdateTypeEquality, true, "")
			seed := openNativeTestWriter(ctx, t, cfg, stream, cat, nil, "original", true)
			require.NoError(t, seed.Write(ctx, []types.RawRecord{nativeTestRecord("old", "r", 1, "one")}))
			require.NoError(t, seed.Close(ctx, nil))
			oldUUID := seed.table.Metadata().TableUUID()
			stale := openNativeTestWriter(ctx, t, cfg, stream, cat, nil, "old-writer", false)
			require.NoError(t, stale.Write(ctx, []types.RawRecord{nativeTestRecord("old", "u", 2, "one")}))
			if purge {
				require.NoError(t, cat.(catalog.PurgeableTable).PurgeTable(ctx, stale.table.Identifier()))
			} else {
				require.NoError(t, cat.DropTable(ctx, stale.table.Identifier()))
			}
			replacement := openNativeTestWriter(ctx, t, cfg, stream, cat, nil, "replacement", true)
			require.NotEqual(t, oldUUID, replacement.table.Metadata().TableUUID())
			require.NoError(t, replacement.Write(ctx, []types.RawRecord{nativeTestRecord("new", "r", 3, "one")}))
			require.NoError(t, replacement.Close(ctx, map[string]any{"state": "replacement"}))
			require.Error(t, stale.Close(ctx, map[string]any{"state": "must-not-resurrect"}), "a writer from an old table generation cannot mutate its replacement")
			persisted, err := cat.LoadTable(ctx, replacement.table.Identifier())
			require.NoError(t, err)
			require.Equal(t, map[string]int64{"new": 3}, nativeVisibleValues(t, persisted))
			require.NotContains(t, persisted.Properties()[nativeRecoveryProperty], "must-not-resurrect")
		})
	}
}

func TestNativeRESTCoordinatorCatalogLifetimesStayIsolated(t *testing.T) {
	ctx := context.Background()
	firstCfg, firstStream, firstCat, firstBoundary := nativeRESTWriterFixture(t, types.UpdateTypeEquality, nativeTestRecord("first", "r", 1, "one"))
	secondCfg, secondStream, secondCat, secondBoundary := nativeRESTWriterFixture(t, types.UpdateTypeEquality, nativeTestRecord("second", "r", 2, "one"))
	first := openNativeTestWriter(ctx, t, firstCfg, firstStream, firstCat, nil, "first-owner", false)
	second := openNativeTestWriter(ctx, t, secondCfg, secondStream, secondCat, nil, "second-owner", false)
	require.NoError(t, first.Close(ctx, map[string]any{"state": `{"first_catalog":1}`}))
	require.NoError(t, second.Close(ctx, map[string]any{"state": `{"second_catalog":2}`}))
	require.Equal(t, []string{http.MethodGet, http.MethodPost}, firstBoundary.tableRequests())
	require.Equal(t, []string{http.MethodGet, http.MethodPost}, secondBoundary.tableRequests())
	require.Equal(t, map[string]int64{"first": 1}, nativeVisibleValues(t, first.table))
	require.Equal(t, map[string]int64{"second": 2}, nativeVisibleValues(t, second.table))
	firstOffsets, _ := nativeRecoveryOffsets(t, first.table)
	secondOffsets, _ := nativeRecoveryOffsets(t, second.table)
	require.Equal(t, map[string]int{"first_catalog": 1}, firstOffsets)
	require.Equal(t, map[string]int{"second_catalog": 2}, secondOffsets)
}

func TestNativeRESTCoordinatorSnapshotConflictRefreshesRecovery(t *testing.T) {
	ctx := context.Background()
	cfg, stream, cat, boundary := nativeRESTWriterFixture(t, types.UpdateTypeEquality, nativeTestRecord("a", "r", 1, "one"))
	writer := openNativeTestWriter(ctx, t, cfg, stream, cat, nil, "retry-after-snapshot", false)
	require.NoError(t, writer.Write(ctx, []types.RawRecord{nativeTestRecord("a", "u", 2, "one")}))
	external := openNativeTestWriter(ctx, t, cfg, stream, boundary.cat, nil, "external-chunk", true)
	require.NoError(t, external.Write(ctx, []types.RawRecord{nativeTestRecord("b", "r", 3, "one")}))
	require.NoError(t, external.Close(ctx, map[string]any{"state": `{"external_partition":17}`}))
	require.Equal(t, []string{http.MethodGet}, boundary.tableRequests())
	require.NoError(t, writer.Close(ctx, map[string]any{"state": `{"local_partition":18}`}))
	require.Equal(t, []string{http.MethodGet, http.MethodPost, http.MethodGet, http.MethodPost}, boundary.tableRequests())
	require.Equal(t, map[string]int64{"a": 2, "b": 3}, nativeVisibleValues(t, writer.table))
	offsets, chunks := nativeRecoveryOffsets(t, writer.table)
	require.Equal(t, map[string]int{"external_partition": 17, "local_partition": 18}, offsets)
	require.ElementsMatch(t, []string{"bootstrap"}, chunks)
}

func TestNativeRESTCoordinatorSerialCDCCommitsShareLatestResponse(t *testing.T) {
	for _, mode := range []types.UpdateType{types.UpdateTypeEquality, types.UpdateTypePosition, types.UpdateTypeDeletionVector} {
		t.Run(string(mode), func(t *testing.T) {
			ctx := context.Background()
			cfg, stream, cat, boundary := nativeRESTWriterFixture(t, mode, nativeTestRecord("a", "r", 1, "one"), nativeTestRecord("b", "r", 2, "one"))
			var index *nativeMemoryIndex
			if mode != types.UpdateTypeEquality {
				index = &nativeMemoryIndex{rows: make(map[string]types.RowLocation)}
			}
			first := openNativeTestWriter(ctx, t, cfg, stream, cat, index, "first-staged", false)
			second := openNativeTestWriter(ctx, t, cfg, stream, cat, index, "second-staged", false)
			require.NoError(t, first.Write(ctx, []types.RawRecord{nativeTestRecord("a", "u", 3, "one")}))
			require.NoError(t, second.Write(ctx, []types.RawRecord{nativeTestRecord("b", "u", 4, "one")}))
			require.NoError(t, first.Close(ctx, map[string]any{"state": `{"first":3}`}))
			require.NotEqual(t, first.table.MetadataLocation(), second.table.MetadataLocation())
			require.NoError(t, second.Close(ctx, map[string]any{"state": `{"second":4}`}))
			require.Equal(t, []string{http.MethodGet, http.MethodGet, http.MethodPost, http.MethodPost}, boundary.tableRequests(), "a writer staged before its peer committed must consume the latest successful POST response")
			require.Equal(t, map[string]int64{"a": 3, "b": 4}, nativeVisibleValues(t, second.table))
			offsets, _ := nativeRecoveryOffsets(t, second.table)
			require.Equal(t, map[string]int{"first": 3, "second": 4}, offsets)
			if index != nil {
				require.Equal(t, nativeSnapshotID(second.table), index.snapshot)
				require.Equal(t, first.dataFiles[0].FilePath(), index.rows["a"].FilePath)
				require.Equal(t, second.dataFiles[0].FilePath(), index.rows["b"].FilePath)
			}
		})
	}
}

func TestNativeRESTCoordinatorPreservesVendedCredentialRefresher(t *testing.T) {
	ctx := context.Background()
	cfg, stream, cat, boundary := nativeRESTWriterFixture(t, types.UpdateTypeEquality)
	boundary.mu.Lock()
	boundary.initialCredentials = []rest.StorageCredential{{Prefix: cfg.IcebergS3Path, Config: apacheiceberg.Properties{"s3.session-token-expires-at-ms": strconv.FormatInt(time.Now().Add(-time.Hour).UnixMilli(), 10)}}}
	boundary.freshCredentials = []rest.StorageCredential{{Prefix: cfg.IcebergS3Path, Config: apacheiceberg.Properties{"s3.session-token-expires-at-ms": strconv.FormatInt(time.Now().Add(time.Hour).UnixMilli(), 10)}}}
	boundary.mu.Unlock()
	writer := openNativeTestWriter(ctx, t, cfg, stream, cat, nil, "credential-refresh", false)
	// Commit without opening FileIO, then use the returned table twice: the
	// first access initializes the SDK closure and the second renews its expiry.
	require.NoError(t, writer.Close(ctx, map[string]any{"state": `{"before_io":1}`}))
	_, err := writer.table.FS(ctx)
	require.NoError(t, err)
	_, err = writer.table.FS(ctx)
	require.NoError(t, err)
	boundary.mu.Lock()
	credentialRequests := boundary.credentialRequests
	boundary.mu.Unlock()
	require.Equal(t, 1, credentialRequests, "successful commit metadata must retain the original SDK /credentials refresher")
	require.Equal(t, []string{http.MethodGet, http.MethodPost}, boundary.tableRequests(), "credential renewal must not require a metadata GET")
}

func TestNativeRESTCoordinatorRejectsOldIdentityAfterFailedSetup(t *testing.T) {
	ctx := context.Background()
	cfg, stream, cat, boundary := nativeRESTWriterFixture(t, types.UpdateTypeEquality)
	old := openNativeTestWriter(ctx, t, cfg, stream, cat, nil, "old-table", false)
	boundary.mu.Lock()
	boundary.failLoad = true
	boundary.mu.Unlock()
	_, _, err := newNativeWriter(ctx, cfg, stream, &destination.Options{ThreadID: "failed-setup"}, cat, nil)
	require.Error(t, err)
	boundary.mu.Lock()
	boundary.failLoad = false
	boundary.mu.Unlock()
	admin := boundary.cat.(*nativeCatalog).Catalog
	require.NoError(t, admin.DropTable(ctx, old.table.Identifier()))
	replacement, err := admin.CreateTable(ctx, old.table.Identifier(), old.table.Schema(), catalog.WithProperties(apacheiceberg.Properties{table.PropertyFormatVersion: "2"}))
	require.NoError(t, err)
	require.NotEqual(t, old.table.Metadata().TableUUID(), replacement.Metadata().TableUUID())
	current := openNativeTestWriter(ctx, t, cfg, stream, cat, nil, "new-table", false)
	require.NoError(t, current.Close(ctx, map[string]any{"state": "new-table-checkpoint"}))
	requests := boundary.tableRequests()
	require.Error(t, old.Close(ctx, map[string]any{"state": "must-not-resurrect"}))
	require.Equal(t, requests, boundary.tableRequests(), "old identities must be rejected before any catalog request")
	fresh, err := admin.LoadTable(ctx, replacement.Identifier())
	require.NoError(t, err)
	require.Contains(t, fresh.Properties()[nativeRecoveryProperty], "new-table-checkpoint")
	require.NotContains(t, fresh.Properties()[nativeRecoveryProperty], "must-not-resurrect")
}
