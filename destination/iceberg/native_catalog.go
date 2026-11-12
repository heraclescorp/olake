package iceberg

import (
	"context"
	"database/sql"
	"errors"
	"iter"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	apacheiceberg "github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/catalog/glue"
	"github.com/apache/iceberg-go/catalog/hive"
	"github.com/apache/iceberg-go/catalog/rest"
	icesql "github.com/apache/iceberg-go/catalog/sql"
	iceio "github.com/apache/iceberg-go/io"
	_ "github.com/apache/iceberg-go/io/gocloud"
	"github.com/apache/iceberg-go/table"
	iceutils "github.com/apache/iceberg-go/utils"
	"github.com/apache/thrift/lib/go/thrift"
	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awsglue "github.com/aws/aws-sdk-go-v2/service/glue"
	"github.com/beltran/gohive"
	"github.com/beltran/gohive/hive_metastore"
	"github.com/datazip-inc/olake/utils/logger"
	_ "github.com/jackc/pgx/v5/stdlib"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
)

// openNativeCatalog uses only public iceberg-go APIs. SQL connection strings and
// authentication are application adapters; they are not iceberg-go features.
func openNativeCatalog(ctx context.Context, config *Config) (catalog.Catalog, func(context.Context), error) {
	if config == nil {
		return nil, nil, errors.New("Iceberg catalog configuration is required")
	}
	props, err := nativeStorageProperties(config)
	if err != nil {
		return nil, nil, err
	}
	client, closeHTTP := newNativeHTTPClient()
	storageAWS, err := nativeAWSConfig(ctx, config, false, client)
	if err != nil {
		closeHTTP()
		return nil, nil, nativeCatalogError("load AWS configuration", err)
	}
	var cat catalog.Catalog
	var closeDB func() error
	switch config.CatalogType {
	case "", GlueCatalog:
		glueAWS, loadErr := nativeAWSConfig(ctx, config, true, client)
		if loadErr != nil {
			err = loadErr
			break
		}
		if config.UseGlueAdditionalConfig && config.GlueCatalogID != "" {
			props[glue.CatalogIdKey] = config.GlueCatalogID
		}
		// Glue's catalog also uses its aws.Config to read table metadata.
		// ServiceOptions scopes overrides to Glue's client; its FileIO keeps
		// the storage credential chain, region, and S3 endpoint instead.
		glueConfig := storageAWS
		glueConfig.ServiceOptions = append(append([]func(string, any){}, storageAWS.ServiceOptions...), func(_ string, options any) {
			if options, ok := options.(*awsglue.Options); ok {
				options.Region, options.Credentials = glueAWS.Region, glueAWS.Credentials
				options.BaseEndpoint = glueAWS.BaseEndpoint
			}
		})
		cat, err = glue.NewCatalog(glue.WithAwsConfig(glueConfig), glue.WithAwsProperties(glue.AwsProperties(props)))
	case RestCatalog:
		cat, err = openNativeRESTCatalog(ctx, config, props, storageAWS, client)
	case HiveCatalog:
		var pool *nativeHivePool
		pool, err = newNativeHivePool(ctx, config)
		if err == nil {
			props[hive.URI], props[hive.Warehouse] = config.HiveURI, config.IcebergS3Path
			cat = hive.NewCatalogWithClient(pool, props)
		}
	case JDBCCatalog:
		var driver, dsn string
		var dialect icesql.SupportedDialect
		driver, dsn, dialect, err = nativeSQLConnection(config)
		if err == nil {
			var db *sql.DB
			db, err = sql.Open(driver, dsn)
			if err == nil {
				closeDB = db.Close
				pingCtx, cancelPing := context.WithTimeout(ctx, nativeCatalogInitTimeout)
				err = db.PingContext(pingCtx)
				cancelPing()
				if err == nil {
					cat, err = icesql.NewCatalog(config.CatalogName, db, dialect, props)
				}
			}
		}
	default:
		closeHTTP()
		return nil, nil, errors.New("unrecognized Iceberg catalog type")
	}
	if err != nil {
		if closeDB != nil {
			_ = closeDB()
		}
		closeHTTP()
		return nil, nil, nativeCatalogError("open native Iceberg catalog", err)
	}
	wrapped := &nativeCatalog{Catalog: cat, storageAWS: storageAWS, closeOwned: func() error {
		var closeErr error
		if closer, ok := cat.(catalog.Closer); ok {
			closeErr = closer.Close()
		}
		if closeDB != nil {
			closeErr = errors.Join(closeErr, closeDB())
		}
		closeHTTP()
		return closeErr
	}}
	cleanup := func(context.Context) {
		if err := wrapped.Close(); err != nil {
			logger.Warn(err.Error())
		}
	}
	return wrapped, cleanup, nil
}

func nativeStorageProperties(config *Config) (apacheiceberg.Properties, error) {
	if (config.AccessKey == "") != (config.SecretKey == "") || (config.SessionToken != "" && config.AccessKey == "") {
		return nil, errors.New("aws_access_key and aws_secret_key must be supplied together; aws_session_token requires both")
	}
	props := apacheiceberg.Properties{"warehouse": config.IcebergS3Path, "write.format.default": "parquet"}
	for key, value := range map[string]string{
		iceio.S3Region:          config.Region,
		iceio.S3AccessKeyID:     config.AccessKey,
		iceio.S3SecretAccessKey: config.SecretKey,
		iceio.S3SessionToken:    config.SessionToken,
		iceio.GCSJSONKey:        config.GCPServiceAccountJSON,
	} {
		if value != "" {
			props[key] = value
		}
	}
	props[iceio.S3ForceVirtualAddressing] = strconv.FormatBool(!config.S3PathStyle && config.S3Endpoint == "")
	if config.S3Endpoint != "" {
		endpoint := config.S3Endpoint
		if !strings.Contains(endpoint, "://") {
			scheme := "http://"
			if config.S3UseSSL {
				scheme = "https://"
			}
			endpoint = scheme + endpoint
		}
		parsed, err := nativeHTTPURL(endpoint)
		if err != nil {
			return nil, errors.New("s3_endpoint must be an absolute HTTP(S) URL")
		}
		props[iceio.S3EndpointURL] = parsed.String()
		// Native S3 compat mode handles GCS's S3 interoperability API as well as
		// other stores that do not implement AWS's optional checksum extensions.
		props[iceio.S3CompatMode] = "true"
	}
	if config.RESTAccessDelegation != "" && config.Region != "" {
		props[iceio.S3ClientRegion] = config.Region
	}
	return props, nil
}

func nativeAWSConfig(ctx context.Context, config *Config, forGlue bool, client *http.Client) (aws.Config, error) {
	opts := []func(*awsconfig.LoadOptions) error{awsconfig.WithHTTPClient(client)}
	if config.ProfileName != "" {
		opts = append(opts, awsconfig.WithSharedConfigProfile(config.ProfileName))
	}
	region, key, secret, token := config.Region, config.AccessKey, config.SecretKey, config.SessionToken
	endpoint := ""
	if forGlue && config.UseGlueAdditionalConfig {
		if config.GlueRegion != "" {
			region = config.GlueRegion
		}
		if (config.GlueAccessKey == "") != (config.GlueSecretKey == "") {
			return aws.Config{}, nativeCatalogConfigError("glue_access_key and glue_secret_key must be supplied together")
		}
		if config.GlueAccessKey != "" {
			key, secret, token = config.GlueAccessKey, config.GlueSecretKey, ""
		}
		endpoint = config.GlueEndpoint
	}
	if region != "" {
		opts = append(opts, awsconfig.WithRegion(region))
	}
	if key != "" {
		opts = append(opts, awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(key, secret, token)))
	}
	cfg, err := awsconfig.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return aws.Config{}, err
	}
	if endpoint != "" {
		parsed, err := nativeHTTPURL(endpoint)
		if err != nil {
			return aws.Config{}, nativeCatalogConfigError("glue_endpoint must be an absolute HTTP(S) URL")
		}
		cfg.BaseEndpoint = aws.String(parsed.String())
	}
	return cfg, nil
}

func nativeHTTPURL(value string) (*url.URL, error) {
	u, err := url.Parse(value)
	if err != nil || u == nil || (u.Scheme != "http" && u.Scheme != "https") || u.Hostname() == "" {
		return nil, errors.New("invalid HTTP endpoint")
	}
	return u, nil
}

const nativeCatalogInitTimeout = time.Minute

func openNativeRESTCatalog(ctx context.Context, config *Config, props apacheiceberg.Properties, cfg aws.Config, client *http.Client) (catalog.Catalog, error) {
	uri, err := nativeHTTPURL(config.RestCatalogURL)
	if err != nil {
		return nil, nativeCatalogConfigError("rest_catalog_url must be an absolute HTTP(S) URL")
	}
	restClient := *client
	restClient.Transport = newNativeRESTTransport(client.Transport)
	opts := []rest.Option{rest.WithWarehouseLocation(config.IcebergS3Path), rest.WithAdditionalProps(props), rest.WithCustomTransport(restClient.Transport), rest.WithAwsConfig(cfg)}
	headers := map[string]string{}
	if config.GCPProjectID != "" {
		headers["x-goog-user-project"] = config.GCPProjectID
	}
	if config.RESTAccessDelegation != "" {
		headers["X-Iceberg-Access-Delegation"] = config.RESTAccessDelegation
	}
	opts = append(opts, rest.WithHeaders(headers))
	auth := strings.ToLower(strings.TrimSpace(config.RestAuthType))
	googleAuth := auth == "google" || auth == "gcp" || auth == "org.apache.iceberg.gcp.auth.googleauthmanager"
	switch {
	case googleAuth:
		scopes := strings.FieldsFunc(config.GCPAuthScopes, func(r rune) bool { return r == ',' || r == ' ' || r == '\t' || r == '\n' })
		if len(scopes) == 0 {
			scopes = []string{"https://www.googleapis.com/auth/cloud-platform"}
		}
		opts = append(opts, rest.WithAuthManager(&nativeGoogleAuth{json: config.GCPServiceAccountJSON, scopes: scopes, client: &restClient}))
	case config.RestSigningV4 || auth == "sigv4" || auth == "org.apache.iceberg.aws.restsigv4authmanager":
		opts = append(opts, rest.WithSigV4RegionSvc(config.RestSigningRegion, config.RestSigningName))
	case auth == "none" || auth == "org.apache.iceberg.rest.auth.noopauthmanager":
		// An explicit manager prevents server defaults from enabling OAuth.
		opts = append(opts, rest.WithAuthManager(nativeNoAuth{}))
	case auth == "" || auth == "oauth2" || auth == "org.apache.iceberg.rest.auth.oauth2manager":
		if config.RestToken != "" {
			opts = append(opts, rest.WithOAuthToken(config.RestToken))
		}
		if config.RestCredential != "" {
			opts = append(opts, rest.WithCredential(config.RestCredential))
		}
		if config.RestScope != "" {
			opts = append(opts, rest.WithScope(config.RestScope))
		}
		if config.RestOAuthURI != "" {
			authURI, err := nativeHTTPURL(config.RestOAuthURI)
			if err != nil {
				return nil, nativeCatalogConfigError("oauth2_uri must be an absolute HTTP(S) URL")
			}
			opts = append(opts, rest.WithAuthURI(authURI))
		}
	default:
		return nil, nativeCatalogConfigError("rest_auth_type must select OAuth2, token, SigV4, Google, or none")
	}
	// Only bootstrap requests use this child context. iceberg-go's OAuth
	// refresh source uses its own bounded HTTP client, and nativeGoogleAuth
	// creates a source per request rather than retaining initialization ctx.
	initCtx, cancelInit := context.WithTimeout(ctx, nativeCatalogInitTimeout)
	defer cancelInit()
	return rest.NewCatalog(initCtx, config.CatalogName, uri.String(), opts...)
}

type nativeNoAuth struct{}

func (nativeNoAuth) AuthHeader() (string, string, error) { return "Authorization", "", nil }

type nativeGoogleAuth struct {
	mu     sync.Mutex
	json   string
	scopes []string
	client *http.Client
	token  *oauth2.Token
}

func (a *nativeGoogleAuth) AuthHeader() (string, string, error) {
	return a.AuthHeaderWithContext(context.Background())
}

func (a *nativeGoogleAuth) AuthHeaderWithContext(ctx context.Context) (string, string, error) {
	if err := ctx.Err(); err != nil {
		return "", "", err
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if !a.token.Valid() {
		// A fresh source binds a refresh to this request, not the catalog-open
		// context. The cached token remains shared across concurrent requests.
		ctx = context.WithValue(ctx, oauth2.HTTPClient, a.client)
		var creds *google.Credentials
		var err error
		if a.json != "" {
			creds, err = google.CredentialsFromJSONWithType(ctx, []byte(a.json), google.ServiceAccount, a.scopes...)
		} else {
			creds, err = google.FindDefaultCredentials(ctx, a.scopes...)
		}
		if err == nil {
			a.token, err = creds.TokenSource.Token()
		}
		if err != nil {
			return "", "", nativeCatalogError("obtain Google catalog access token", err)
		}
	}
	return "Authorization", "Bearer " + a.token.AccessToken, nil
}

// nativeSQLConnection translates the PostgreSQL JDBC configuration bundled by
// the Java writer into the native pgx driver's connection URL.
func nativeSQLConnection(config *Config) (string, string, icesql.SupportedDialect, error) {
	value := strings.TrimPrefix(config.JDBCUrl, "jdbc:")
	if strings.HasPrefix(value, "postgresql:") && !strings.HasPrefix(value, "postgresql://") {
		value = "postgresql://localhost/" + strings.TrimPrefix(value, "postgresql:")
	}
	u, err := url.Parse(value)
	if err != nil || u == nil || u.Host == "" {
		return "", "", "", nativeCatalogConfigError("jdbc_url must be a PostgreSQL connection URL")
	}
	query, err := url.ParseQuery(u.RawQuery)
	if err != nil {
		return "", "", "", nativeCatalogConfigError("jdbc_url contains malformed PostgreSQL connection options")
	}
	username, password := config.JDBCUsername, config.JDBCPassword
	if username == "" && u.User != nil {
		username = u.User.Username()
	}
	if password == "" && u.User != nil {
		password, _ = u.User.Password()
	}
	if username == "" {
		username = query.Get("user")
	}
	if password == "" {
		password = query.Get("password")
	}
	if username != "" || password != "" {
		u.User = url.UserPassword(username, password)
	} else {
		u.User = nil
	}
	query.Del("user")
	query.Del("password")
	switch u.Scheme {
	case "postgresql", "postgres":
		u.Scheme = "postgres"
		if schema := query.Get("currentSchema"); schema != "" {
			query.Set("search_path", schema)
			query.Del("currentSchema")
		}
		if query.Has("ApplicationName") {
			query.Set("application_name", query.Get("ApplicationName"))
			query.Del("ApplicationName")
		}
		// pgx's connect_timeout covers connection startup and authentication.
		// Retain the tighter nonzero bound when both JDBC timeouts are set.
		var timeoutSeconds int64
		hasTimeout := false
		for _, option := range []string{"connect_timeout", "connectTimeout", "loginTimeout"} {
			if !query.Has(option) {
				continue
			}
			seconds, err := strconv.ParseInt(query.Get(option), 10, 64)
			if err != nil || seconds < 0 || seconds > int64((1<<63-1)/time.Second) {
				return "", "", "", nativeCatalogConfigError("jdbc_url connection and login timeouts must be nonnegative integer seconds")
			}
			if !hasTimeout || (seconds > 0 && (timeoutSeconds == 0 || seconds < timeoutSeconds)) {
				timeoutSeconds = seconds
			}
			hasTimeout = true
			query.Del(option)
		}
		if hasTimeout {
			query.Set("connect_timeout", strconv.FormatInt(timeoutSeconds, 10))
		}
		if query.Has("ssl") {
			if query.Get("sslmode") == "" {
				// pgJDBC's ssl=true (including a bare ssl parameter) verifies
				// both certificate chain and hostname, not merely encryption.
				mode := "verify-full"
				if query.Get("ssl") == "false" {
					mode = "prefer"
				}
				query.Set("sslmode", mode)
			}
			query.Del("ssl")
		}
		for option := range query {
			switch option {
			case "sslfactory", "sslfactoryarg", "sslhostnameverifier", "sslpasswordcallback":
				return "", "", "", nativeCatalogConfigError("jdbc_url Java TLS factories, hostname verifiers, and callbacks are not native PostgreSQL options; configure sslmode, sslrootcert, sslcert, and sslkey instead")
			case "host", "port", "database", "dbname", "passfile", "service", "servicefile", "connect_timeout",
				"sslmode", "sslkey", "sslcert", "sslcertmode", "sslrootcert", "sslpassword", "sslsni", "sslnegotiation",
				"min_protocol_version", "max_protocol_version", "channel_binding", "require_auth", "target_session_attrs", "krbsrvname", "krbspn",
				"application_name", "search_path", "options", "TimeZone", "timezone", "client_encoding",
				"statement_timeout", "lock_timeout", "idle_in_transaction_session_timeout", "default_transaction_isolation",
				"default_transaction_read_only", "default_transaction_deferrable", "extra_float_digits":
			default:
				return "", "", "", nativeCatalogConfigError("jdbc_url contains an unsupported PostgreSQL driver option; use supported native connection options or the options startup parameter")
			}
		}
		u.RawQuery = query.Encode()
		return "pgx", u.String(), icesql.Postgres, nil
	default:
		return "", "", "", nativeCatalogConfigError("jdbc_url must select the existing PostgreSQL catalog dialect")
	}
}

// The context carries storage credentials separately from Glue overrides. Native
// FileIO honors explicit/vended table properties ahead of these ambient defaults.
type nativeCatalog struct {
	catalog.Catalog
	storageAWS aws.Config
	closeOnce  sync.Once
	closeOwned func() error
	closeErr   error
}

func nativeCatalogContext(ctx context.Context, cat catalog.Catalog) context.Context {
	if c, ok := cat.(*nativeCatalog); ok {
		return iceutils.WithAwsConfig(ctx, &c.storageAWS)
	}
	return ctx
}

func (c *nativeCatalog) ctx(ctx context.Context) context.Context {
	return iceutils.WithAwsConfig(ctx, &c.storageAWS)
}

// Every catalog produced by this factory implements PurgeableTable. Never fall
// back to metadata-only DropTable if a different catalog is supplied: that would
// report success while retaining the table's data.
func (c *nativeCatalog) PurgeTable(ctx context.Context, ident table.Identifier) error {
	purger, ok := c.Catalog.(catalog.PurgeableTable)
	if !ok {
		return errors.New("Iceberg catalog cannot purge tables; the table was not dropped")
	}
	return nativeCatalogError("purge Iceberg table", purger.PurgeTable(c.ctx(ctx), ident))
}

// Close and the factory cleanup callback share ownership, so callers may use
// either public lifecycle API without double-closing pools or HTTP transports.
func (c *nativeCatalog) Close() error {
	c.closeOnce.Do(func() {
		if c.closeOwned != nil {
			c.closeErr = c.closeOwned()
		} else if closer, ok := c.Catalog.(catalog.Closer); ok {
			c.closeErr = closer.Close()
		}
		c.closeErr = nativeCatalogError("close native Iceberg catalog", c.closeErr)
	})
	return c.closeErr
}

var _ catalog.PurgeableTable = (*nativeCatalog)(nil)
var _ catalog.Closer = (*nativeCatalog)(nil)

func nativeLocalRequirementValidation(kind catalog.Type) bool {
	return kind == catalog.Glue || kind == catalog.Hive || kind == catalog.SQL
}

type nativeValidatedRequirement struct{ table.Requirement }

func (r nativeValidatedRequirement) Validate(metadata table.Metadata) error {
	if err := r.Requirement.Validate(metadata); err != nil {
		// These catalogs call Validate before UpdateTableMetadata or any write.
		// Do not classify later service, metadata-write, or REST failures here.
		return errors.Join(table.ErrCommitFailed, err)
	}
	return nil
}

func (c *nativeCatalog) bindLocalTable(original *table.Table) *table.Table {
	if original == nil || !nativeLocalRequirementValidation(c.Catalog.CatalogType()) {
		return original
	}
	// Transactions retain the table's catalog. Rebind local catalogs so their
	// later commits pass through the pre-write requirement adapter too. REST
	// tables retain their original scan-planning and vended-credential state.
	return table.New(original.Identifier(), original.Metadata(), original.MetadataLocation(),
		func(ctx context.Context) (iceio.IO, error) { return original.FS(c.ctx(ctx)) }, c,
		table.WithMetricsReporter(original.MetricsReporter()))
}

func (c *nativeCatalog) CreateTable(ctx context.Context, ident table.Identifier, schema *apacheiceberg.Schema, opts ...catalog.CreateTableOpt) (*table.Table, error) {
	tbl, err := c.Catalog.CreateTable(c.ctx(ctx), ident, schema, opts...)
	return c.bindLocalTable(tbl), nativeCatalogError("create Iceberg table", err)
}
func (c *nativeCatalog) CommitTable(ctx context.Context, ident table.Identifier, requirements []table.Requirement, updates []table.Update) (table.Metadata, string, error) {
	if nativeLocalRequirementValidation(c.Catalog.CatalogType()) && len(requirements) > 0 {
		validated := make([]table.Requirement, len(requirements))
		for index, requirement := range requirements {
			validated[index] = nativeValidatedRequirement{Requirement: requirement}
		}
		requirements = validated
	}
	metadata, location, err := c.Catalog.CommitTable(c.ctx(ctx), ident, requirements, updates)
	return metadata, location, nativeCatalogError("commit Iceberg table", err)
}
func (c *nativeCatalog) LoadTable(ctx context.Context, ident table.Identifier) (*table.Table, error) {
	tbl, err := c.Catalog.LoadTable(c.ctx(ctx), ident)
	return c.bindLocalTable(tbl), nativeCatalogError("load Iceberg table", err)
}
func (c *nativeCatalog) ListTables(ctx context.Context, namespace table.Identifier) iter.Seq2[table.Identifier, error] {
	return func(yield func(table.Identifier, error) bool) {
		for ident, err := range c.Catalog.ListTables(c.ctx(ctx), namespace) {
			if !yield(ident, nativeCatalogError("list Iceberg tables", err)) {
				return
			}
		}
	}
}
func (c *nativeCatalog) DropTable(ctx context.Context, ident table.Identifier) error {
	return nativeCatalogError("drop Iceberg table", c.Catalog.DropTable(c.ctx(ctx), ident))
}
func (c *nativeCatalog) RenameTable(ctx context.Context, from, to table.Identifier) (*table.Table, error) {
	tbl, err := c.Catalog.RenameTable(c.ctx(ctx), from, to)
	return c.bindLocalTable(tbl), nativeCatalogError("rename Iceberg table", err)
}
func (c *nativeCatalog) CheckTableExists(ctx context.Context, ident table.Identifier) (bool, error) {
	exists, err := c.Catalog.CheckTableExists(c.ctx(ctx), ident)
	return exists, nativeCatalogError("check Iceberg table", err)
}
func (c *nativeCatalog) ListNamespaces(ctx context.Context, parent table.Identifier) ([]table.Identifier, error) {
	ids, err := c.Catalog.ListNamespaces(c.ctx(ctx), parent)
	return ids, nativeCatalogError("list Iceberg namespaces", err)
}
func (c *nativeCatalog) CreateNamespace(ctx context.Context, namespace table.Identifier, props apacheiceberg.Properties) error {
	return nativeCatalogError("create Iceberg namespace", c.Catalog.CreateNamespace(c.ctx(ctx), namespace, props))
}
func (c *nativeCatalog) DropNamespace(ctx context.Context, namespace table.Identifier) error {
	return nativeCatalogError("drop Iceberg namespace", c.Catalog.DropNamespace(c.ctx(ctx), namespace))
}
func (c *nativeCatalog) CheckNamespaceExists(ctx context.Context, namespace table.Identifier) (bool, error) {
	exists, err := c.Catalog.CheckNamespaceExists(c.ctx(ctx), namespace)
	return exists, nativeCatalogError("check Iceberg namespace", err)
}
func (c *nativeCatalog) LoadNamespaceProperties(ctx context.Context, namespace table.Identifier) (apacheiceberg.Properties, error) {
	props, err := c.Catalog.LoadNamespaceProperties(c.ctx(ctx), namespace)
	return props, nativeCatalogError("load Iceberg namespace properties", err)
}
func (c *nativeCatalog) UpdateNamespaceProperties(ctx context.Context, namespace table.Identifier, removals []string, updates apacheiceberg.Properties) (catalog.PropertiesUpdateSummary, error) {
	summary, err := c.Catalog.UpdateNamespaceProperties(c.ctx(ctx), namespace, removals, updates)
	return summary, nativeCatalogError("update Iceberg namespace properties", err)
}

// Each Thrift client is single-flight; the pool preserves HiveClients rather
// than sharing a non-thread-safe protocol across simultaneous catalog requests.
type nativeHivePool struct {
	clients chan *nativeHiveClient
	all     []*nativeHiveClient
	done    chan struct{}
	once    sync.Once
	mu      sync.Mutex
}

type nativeHiveClient struct {
	*hive_metastore.ThriftHiveMetastoreClient
	transport thrift.TTransport
	reconnect func(context.Context) (*nativeHiveClient, error)
}

func newNativeHivePool(ctx context.Context, config *Config) (*nativeHivePool, error) {
	count := config.HiveClients
	if count <= 0 {
		count = 2
	}
	pool := &nativeHivePool{clients: make(chan *nativeHiveClient, count), done: make(chan struct{})}
	endpoints := strings.Split(config.HiveURI, ",")
	for index := range count {
		sasl := config.HiveSaslEnabled
		connect := func(ctx context.Context) (*nativeHiveClient, error) {
			var client *nativeHiveClient
			var err error
			for offset := range len(endpoints) {
				endpoint := strings.TrimSpace(endpoints[(index+offset)%len(endpoints)])
				client, err = newNativeHiveClient(ctx, endpoint, sasl)
				if err == nil || ctx.Err() != nil {
					break
				}
			}
			return client, err
		}
		client, err := connect(ctx)
		if err != nil {
			_ = pool.Close()
			return nil, err
		}
		client.reconnect = connect
		pool.all = append(pool.all, client)
		pool.clients <- client
	}
	return pool, nil
}

func newNativeHiveClient(ctx context.Context, uri string, sasl bool) (*nativeHiveClient, error) {
	if sasl && !nativeKerberosAvailable {
		return nil, nativeCatalogConfigError("hive_sasl_enabled requires a driver built with the kerberos build tag")
	}
	parsed, err := url.Parse(uri)
	if err != nil || parsed == nil || parsed.Scheme != "thrift" || parsed.Hostname() == "" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" {
		return nil, nativeCatalogConfigError("hive_uri must be a thrift://host:port endpoint without credentials, query, or fragment")
	}
	port := parsed.Port()
	if port == "" {
		port = "9083"
	}
	conn, err := (&net.Dialer{Timeout: 30 * time.Second}).DialContext(ctx, "tcp", net.JoinHostPort(parsed.Hostname(), port))
	if err != nil {
		return nil, err
	}
	socket := thrift.NewTSocketFromConnConf(conn, &thrift.TConfiguration{SocketTimeout: 60 * time.Second})
	var transport thrift.TTransport = thrift.NewTBufferedTransport(socket, 4096)
	if sasl {
		saslTransport, err := gohive.NewTSaslTransport(socket, parsed.Hostname(), "GSSAPI", map[string]string{"service": "hive"}, gohive.DEFAULT_MAX_LENGTH)
		if err != nil {
			_ = socket.Close()
			return nil, err
		}
		saslTransport.OpeningContext = ctx
		transport = saslTransport
	}
	if !transport.IsOpen() {
		if err := transport.Open(); err != nil {
			_ = transport.Close()
			return nil, err
		}
	}
	factory := thrift.NewTBinaryProtocolFactoryDefault()
	client := hive_metastore.NewThriftHiveMetastoreClient(thrift.NewTStandardClient(factory.GetProtocol(transport), factory.GetProtocol(transport)))
	return &nativeHiveClient{ThriftHiveMetastoreClient: client, transport: transport}, nil
}

func (p *nativeHivePool) Close() error {
	var err error
	p.once.Do(func() {
		p.mu.Lock()
		defer p.mu.Unlock()
		close(p.done)
		for _, client := range p.all {
			if client.transport != nil {
				err = errors.Join(err, client.transport.Close())
			}
		}
	})
	return err
}

func nativeHiveCall[T any](ctx context.Context, p *nativeHivePool, call func(*nativeHiveClient) (T, error)) (T, error) {
	var zero T
	if err := ctx.Err(); err != nil {
		return zero, err
	}
	select {
	case <-ctx.Done():
		return zero, ctx.Err()
	case <-p.done:
		return zero, errors.New("Hive catalog is closed")
	case client := <-p.clients:
		defer func() {
			select {
			case p.clients <- client:
			case <-p.done:
			}
		}()
		if err := p.ensureConnected(ctx, client); err != nil {
			return zero, err
		}
		value, err := call(client)
		if nativeHiveConnectionBroken(err) {
			// Do not replay the RPC: it may have committed before disconnecting.
			// A subsequent caller redials this slot with its own live context.
			p.mu.Lock()
			_ = client.transport.Close()
			client.transport, client.ThriftHiveMetastoreClient = nil, nil
			p.mu.Unlock()
		}
		return value, err
	}
}

func (p *nativeHivePool) ensureConnected(ctx context.Context, client *nativeHiveClient) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	p.mu.Lock()
	select {
	case <-p.done:
		p.mu.Unlock()
		return errors.New("Hive catalog is closed")
	default:
	}
	connected := client.transport != nil
	p.mu.Unlock()
	if connected {
		return nil
	}
	replacement, err := client.reconnect(ctx)
	if err != nil {
		return err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	select {
	case <-p.done:
		_ = replacement.transport.Close()
		return errors.New("Hive catalog is closed")
	default:
		client.transport, client.ThriftHiveMetastoreClient = replacement.transport, replacement.ThriftHiveMetastoreClient
		return nil
	}
}

func nativeHiveConnectionBroken(err error) bool {
	if err == nil {
		return false
	}
	var exception thrift.TException
	if errors.As(err, &exception) {
		if exception.TExceptionType() == thrift.TExceptionTypeCompiled {
			return false
		}
		if exception.TExceptionType() == thrift.TExceptionTypeApplication {
			var application thrift.TApplicationException
			if errors.As(err, &application) {
				switch application.TypeId() {
				case thrift.BAD_SEQUENCE_ID, thrift.WRONG_METHOD_NAME, thrift.INVALID_MESSAGE_TYPE_EXCEPTION, thrift.PROTOCOL_ERROR:
					return true
				default:
					return false
				}
			}
		}
	}
	return true
}

func nativeHiveExec(ctx context.Context, p *nativeHivePool, call func(*nativeHiveClient) error) error {
	_, err := nativeHiveCall(ctx, p, func(c *nativeHiveClient) (struct{}, error) { return struct{}{}, call(c) })
	return err
}

func (p *nativeHivePool) GetDatabase(ctx context.Context, name string) (*hive_metastore.Database, error) {
	return nativeHiveCall(ctx, p, func(c *nativeHiveClient) (*hive_metastore.Database, error) { return c.GetDatabase(ctx, name) })
}
func (p *nativeHivePool) CreateDatabase(ctx context.Context, db *hive_metastore.Database) error {
	return nativeHiveExec(ctx, p, func(c *nativeHiveClient) error { return c.CreateDatabase(ctx, db) })
}
func (p *nativeHivePool) AlterDatabase(ctx context.Context, name string, db *hive_metastore.Database) error {
	return nativeHiveExec(ctx, p, func(c *nativeHiveClient) error { return c.AlterDatabase(ctx, name, db) })
}
func (p *nativeHivePool) DropDatabase(ctx context.Context, name string, deleteData, cascade bool) error {
	return nativeHiveExec(ctx, p, func(c *nativeHiveClient) error { return c.DropDatabase(ctx, name, deleteData, cascade) })
}
func (p *nativeHivePool) GetAllDatabases(ctx context.Context) ([]string, error) {
	return nativeHiveCall(ctx, p, func(c *nativeHiveClient) ([]string, error) { return c.GetAllDatabases(ctx) })
}
func (p *nativeHivePool) GetTable(ctx context.Context, db, name string) (*hive_metastore.Table, error) {
	return nativeHiveCall(ctx, p, func(c *nativeHiveClient) (*hive_metastore.Table, error) { return c.GetTable(ctx, db, name) })
}
func (p *nativeHivePool) CreateTable(ctx context.Context, tbl *hive_metastore.Table) error {
	return nativeHiveExec(ctx, p, func(c *nativeHiveClient) error { return c.CreateTable(ctx, tbl) })
}
func (p *nativeHivePool) AlterTable(ctx context.Context, db, name string, tbl *hive_metastore.Table) error {
	return nativeHiveExec(ctx, p, func(c *nativeHiveClient) error { return c.AlterTable(ctx, db, name, tbl) })
}
func (p *nativeHivePool) DropTable(ctx context.Context, db, name string, deleteData bool) error {
	return nativeHiveExec(ctx, p, func(c *nativeHiveClient) error { return c.DropTable(ctx, db, name, deleteData) })
}
func (p *nativeHivePool) GetTables(ctx context.Context, db, pattern string) ([]string, error) {
	return nativeHiveCall(ctx, p, func(c *nativeHiveClient) ([]string, error) { return c.GetTables(ctx, db, pattern) })
}
func (p *nativeHivePool) Lock(ctx context.Context, request *hive_metastore.LockRequest) (*hive_metastore.LockResponse, error) {
	return nativeHiveCall(ctx, p, func(c *nativeHiveClient) (*hive_metastore.LockResponse, error) { return c.Lock(ctx, request) })
}
func (p *nativeHivePool) CheckLock(ctx context.Context, id int64) (*hive_metastore.LockResponse, error) {
	return nativeHiveCall(ctx, p, func(c *nativeHiveClient) (*hive_metastore.LockResponse, error) {
		return c.CheckLock(ctx, &hive_metastore.CheckLockRequest{Lockid: id})
	})
}
func (p *nativeHivePool) Unlock(ctx context.Context, id int64) error {
	return nativeHiveExec(ctx, p, func(c *nativeHiveClient) error { return c.Unlock(ctx, &hive_metastore.UnlockRequest{Lockid: id}) })
}

var _ hive.HiveClient = (*nativeHivePool)(nil)
var _ rest.ContextAuthManager = (*nativeGoogleAuth)(nil)
