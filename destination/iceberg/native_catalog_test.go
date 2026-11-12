package iceberg

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	apacheiceberg "github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/catalog/rest"
	icesql "github.com/apache/iceberg-go/catalog/sql"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
	iceutils "github.com/apache/iceberg-go/utils"
	"github.com/apache/thrift/lib/go/thrift"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awsglue "github.com/aws/aws-sdk-go-v2/service/glue"
	"github.com/beltran/gohive/hive_metastore"
	"golang.org/x/oauth2"
)

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
	key, err := rsa.GenerateKey(rand.Reader, 1024)
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
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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
				factory := thrift.NewTBinaryProtocolFactoryDefault()
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
			return nil, nil
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
