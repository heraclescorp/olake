package iceberg

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"iter"
	"net"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	apacheiceberg "github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/catalog/glue"
	"github.com/apache/iceberg-go/catalog/hive"
	"github.com/apache/iceberg-go/catalog/rest"
	icesql "github.com/apache/iceberg-go/catalog/sql"
	iceio "github.com/apache/iceberg-go/io"
	_ "github.com/apache/iceberg-go/io/gocloud" // Register cloud-backed FileIO schemes.
	"github.com/apache/iceberg-go/table"
	iceutils "github.com/apache/iceberg-go/utils"
	"github.com/apache/thrift/lib/go/thrift"
	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awsglue "github.com/aws/aws-sdk-go-v2/service/glue"
	"github.com/beltran/gohive"
	"github.com/beltran/gohive/hive_metastore"
	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/destination"
	"github.com/datazip-inc/olake/pkg/objstorage"
	"github.com/datazip-inc/olake/utils"
	"github.com/datazip-inc/olake/utils/errs"
	"github.com/datazip-inc/olake/utils/logger"
	_ "github.com/jackc/pgx/v5/stdlib" // Register the pgx database/sql driver for SQL catalogs.
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
)

// CatalogType represents supported Iceberg catalog implementations
type CatalogType string

const (
	// GlueCatalog is the AWS Glue catalog implementation
	GlueCatalog CatalogType = "glue"
	// JDBCCatalog is the JDBC catalog implementation
	JDBCCatalog CatalogType = "jdbc"
	// HiveCatalog is the Hive catalog implementation
	HiveCatalog CatalogType = "hive"
	// RestCatalog is the REST catalog implementation
	RestCatalog CatalogType = "rest"
)

// TODO: add validation for each catalog properly
type Config struct {
	// S3-compatible Storage Configuration
	Region             string `json:"aws_region,omitempty"`
	AccessKey          string `json:"aws_access_key,omitempty"`
	SecretKey          string `json:"aws_secret_key,omitempty"`
	SessionToken       string `json:"aws_session_token,omitempty"`
	ProfileName        string `json:"aws_profile,omitempty"`
	NoIdentifierFields bool   `json:"no_identifier_fields"` // Needed to set true for Databricks Unity Catalog as it doesn't support identifier fields

	// S3 endpoint for custom S3-compatible services (like MinIO)
	S3Endpoint  string `json:"s3_endpoint,omitempty"`
	S3UseSSL    bool   `json:"s3_use_ssl,omitempty"`    // Use HTTPS if true
	S3PathStyle bool   `json:"s3_path_style,omitempty"` // Use path-style instead of virtual-hosted-style https://docs.aws.amazon.com/AmazonS3/latest/userguide/VirtualHosting.html

	// Catalog Configuration
	CatalogType CatalogType `json:"catalog_type,omitempty"`
	CatalogName string      `json:"catalog_name,omitempty"`

	// Glue catalog optional overrides
	UseGlueAdditionalConfig bool   `json:"glue_additional_config,omitempty"`
	GlueEndpoint            string `json:"glue_endpoint,omitempty"`
	GlueAccessKey           string `json:"glue_access_key,omitempty"`
	GlueSecretKey           string `json:"glue_secret_key,omitempty"`
	GlueRegion              string `json:"glue_region,omitempty"`
	GlueCatalogID           string `json:"glue_catalog_id,omitempty"`

	// JDBC specific configuration
	JDBCUrl      string `json:"jdbc_url,omitempty"`
	JDBCUsername string `json:"jdbc_username,omitempty"`
	JDBCPassword string `json:"jdbc_password,omitempty"`

	// Hive specific configuration
	HiveURI         string `json:"hive_uri,omitempty"`
	HiveClients     int    `json:"hive_clients,omitempty"`
	HiveSaslEnabled bool   `json:"hive_sasl_enabled,omitempty"`

	// Iceberg Configuration
	IcebergDatabase string `json:"iceberg_db,omitempty"`
	IcebergS3Path   string `json:"iceberg_s3_path"`

	// Rest Catalog Configuration
	RestCatalogURL    string `json:"rest_catalog_url,omitempty"`
	RestSigningName   string `json:"rest_signing_name,omitempty"`
	RestSigningRegion string `json:"rest_signing_region,omitempty"`
	RestSigningV4     bool   `json:"rest_signing_v_4,omitempty"`
	RestToken         string `json:"token,omitempty"`
	RestOAuthURI      string `json:"oauth2_uri,omitempty"`
	RestAuthType      string `json:"rest_auth_type,omitempty"`
	RestScope         string `json:"scope,omitempty"`
	RestCredential    string `json:"credential,omitempty"`

	// GCP auth for GoogleAuthManager (e.g. BigLake). Inline SA key JSON;
	GCPServiceAccountJSON string `json:"gcp_service_account_json,omitempty"`
	GCPAuthScopes         string `json:"gcp_auth_scopes,omitempty"`
	// Required by BigLake for request routing/billing (sent as the x-goog-user-project header).
	GCPProjectID string `json:"gcp_project_id,omitempty"`

	// Horizon
	RESTAccessDelegation string `json:"rest_access_delegation,omitempty"`
}

func (c *Config) Validate() error {
	if c.IcebergS3Path == "" {
		return errs.Precondition(errs.ConfigInvalid, codeCatalogConfigInvalid,
			fmt.Errorf("s3_path is required"))
	}

	// Set defaults for catalog type
	if c.CatalogType == "" {
		c.CatalogType = GlueCatalog
	}

	if c.CatalogName == "" {
		c.CatalogName = "olake_iceberg"
	}
	//S3 tables use SigV4 authentication and require signing name to be set to "s3tables"
	if c.CatalogType == "s3tables" {
		c.RestSigningV4 = true
		c.RestSigningName = "s3tables"
	}
	//Unity Catalog doesn't support identifier fields (disable them)
	if c.CatalogType == "unity" {
		c.NoIdentifierFields = true
	}
	// BigLake uses Google application/service-account credentials.
	if c.CatalogType == "biglake" {
		c.RestAuthType = "google"
	}
	// Horizon requires access delegation
	if c.CatalogType == "horizon" {
		c.RESTAccessDelegation = "vended-credentials"
	}
	c.RestAuthType = olakeAuthTypeToIcebergAuthType(c.RestAuthType)
	if slices.Contains(constants.RESTCatalogs, string(c.CatalogType)) {
		c.CatalogType = RestCatalog
	}

	// Default to path-style access for S3-compatible services
	if c.S3Endpoint != "" {
		c.S3PathStyle = true
	}

	// Validate S3 configuration
	// Region can be picked up from environment or credentials file for AWS S3
	if c.S3Endpoint == "" && c.Region == "" {
		logger.Warn("aws_region not explicitly provided, will attempt to use region from environment variables or AWS config/credentials file")
	}

	// Log information about credentials for all S3 configurations
	if c.AccessKey == "" && c.SecretKey == "" && c.ProfileName == "" {
		if c.S3Endpoint == "" {
			// AWS S3 scenario
			logger.Info("AWS credentials not explicitly provided, will use default credential chain (environment variables, AWS config/credentials file, or instance metadata service)")
		} else {
			// Custom S3 endpoint scenario
			logger.Info("S3 credentials not explicitly provided for custom endpoint. Ensure the service supports anonymous access or credentials are available through other means")
		}
	}

	// Validate based on catalog type
	switch c.CatalogType {
	case JDBCCatalog:
		if c.JDBCUrl == "" {
			return errs.Precondition(errs.ConfigInvalid, codeCatalogConfigInvalid,
				fmt.Errorf("jdbc_url is required when using JDBC catalog"))
		}
	case RestCatalog:
		if c.RestCatalogURL == "" {
			return errs.Precondition(errs.ConfigInvalid, codeCatalogConfigInvalid,
				fmt.Errorf("rest_catalog_url is required when using REST catalog"))
		}
	case HiveCatalog:
		if c.HiveURI == "" {
			return errs.Precondition(errs.ConfigInvalid, codeCatalogConfigInvalid,
				fmt.Errorf("hive_uri is required when using Hive catalog"))
		}
	case GlueCatalog:
		// No additional validation required for Glue catalog
	default:
		return errs.Precondition(errs.ConfigInvalid, codeUnsupportedCatalogType,
			fmt.Errorf("unsupported catalog_type: %s", c.CatalogType))
	}

	return utils.Validate(c)
}

func olakeAuthTypeToIcebergAuthType(authType string) string {
	switch strings.ToLower(strings.TrimSpace(authType)) {
	case "oauth2", "oauth2 u2m", "oauth2 m2m", "token", "token federation",
		"personal access token (pat)", "pat", "external oauth",
		"key-pair authentication", "programmatic access token (pat)":
		return "oauth2"
	case "none":
		return "none"
	case "sigv4":
		return "sigv4"
	case "google", "gcp":
		return "google"
	default:
		if strings.Contains(authType, ".") && !strings.Contains(authType, " ") {
			return authType
		}
		logger.Warnf("unmapped rest_auth_type %q. Iceberg rest.auth.type omitted", authType)
		return ""
	}
}

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
	factory := thrift.NewTBinaryProtocolFactoryConf(nil)
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
		return zero, errors.New("hive catalog is closed")
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
		return errors.New("hive catalog is closed")
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
		return errors.New("hive catalog is closed")
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
	backoff := min(max(attempt-1, 0), 6)
	delay := time.Second << backoff
	var jitter [4]byte
	if _, err := rand.Read(jitter[:]); err != nil {
		panic(err)
	}
	jitterNanos := binary.LittleEndian.Uint32(jitter[:]) % uint32(time.Second/10)
	return delay + (time.Duration(jitterNanos) << backoff)
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

const (
	codeCatalogConfigInvalid   = "iceberg.catalog_config_invalid"
	codeUnsupportedCatalogType = "iceberg.unsupported_catalog_type"
	codePartitionRegexNoMatch  = "iceberg.partition_regex_no_match"
)

func init() { errs.Register("iceberg", classify) }

func classify(err error) *errs.Failure {
	for _, rule := range []struct {
		cause    error
		category errs.Category
		code     string
	}{
		{table.ErrCommitFailed, errs.ConcurrencyConflict, "iceberg.commit_conflict"},
		{rest.ErrCommitStateUnknown, errs.CatalogError, "iceberg.commit_state_unknown"},
		{catalog.ErrNoSuchTable, errs.ObjectNotFound, "iceberg.table_not_found"},
		{catalog.ErrNoSuchNamespace, errs.ObjectNotFound, "iceberg.namespace_not_found"},
		{rest.ErrUnauthorized, errs.AuthFailed, "iceberg.unauthorized"},
		{rest.ErrAuthorizationExpired, errs.AuthFailed, "iceberg.authorization_expired"},
		{rest.ErrOAuthError, errs.AuthFailed, "iceberg.oauth_error"},
		{rest.ErrForbidden, errs.PermissionDenied, "iceberg.forbidden"},
		{rest.ErrServiceUnavailable, errs.NetworkUnreachable, "iceberg.service_unavailable"},
		{rest.ErrServerError, errs.CatalogError, "iceberg.server_error"},
		{rest.ErrBadRequest, errs.ConfigInvalid, "iceberg.bad_request"},
	} {
		if errors.Is(err, rule.cause) {
			return &errs.Failure{Category: rule.category, ClassifiedBy: errs.ClassifiedByVendor, Code: rule.code}
		}
	}
	var vendor interface{ ErrorCode() string }
	if errors.As(err, &vendor) {
		if category, found := objstorage.ServiceCodeCategories[vendor.ErrorCode()]; found {
			return &errs.Failure{Category: category, ClassifiedBy: errs.ClassifiedByVendor, Code: vendor.ErrorCode()}
		}
	}
	var sqlError interface{ SQLState() string }
	if errors.As(err, &sqlError) {
		state := sqlError.SQLState()
		if len(state) >= 2 {
			categories := map[string]errs.Category{"28": errs.AuthFailed, "08": errs.NetworkUnreachable,
				"40": errs.ConcurrencyConflict, "53": errs.ResourceExhausted, "57": errs.ResourceExhausted,
				"3D": errs.ObjectNotFound, "3F": errs.ObjectNotFound, "23": errs.CatalogError,
				"25": errs.CatalogError, "0A": errs.UnsupportedFeature}
			if category, found := categories[state[:2]]; found {
				return &errs.Failure{Category: category, ClassifiedBy: errs.ClassifiedByVendor, Code: state}
			}
		}
	}
	if destination.IsWriteFailure(err) {
		return &errs.Failure{Category: errs.DestinationWriteError, ClassifiedBy: errs.ClassifiedByPrecondition}
	}
	return nil
}
