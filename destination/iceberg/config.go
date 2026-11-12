package iceberg

import (
	"fmt"
	"slices"
	"strings"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/utils"
	"github.com/datazip-inc/olake/utils/errs"
	"github.com/datazip-inc/olake/utils/logger"
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
