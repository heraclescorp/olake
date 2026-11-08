package protocol

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws/arn"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3tables"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3tables/types"
	"github.com/aws/smithy-go"
	smithyhttp "github.com/aws/smithy-go/transport/http"
)

type enrollmentWriterConfig struct {
	CatalogType        string `json:"catalog_type"`
	RestSigningName    string `json:"rest_signing_name"`
	RestSigningRegion  string `json:"rest_signing_region"`
	RestCatalogURL     string `json:"rest_catalog_url"`
	IcebergS3Path      string `json:"iceberg_s3_path"`
	IcebergDatabase    string `json:"iceberg_db"`
	NoIdentifierFields bool   `json:"no_identifier_fields"`
	Region             string `json:"aws_region"`
	AccessKey          string `json:"aws_access_key"`
	SecretKey          string `json:"aws_secret_key"`
	SessionToken       string `json:"aws_session_token"`
	ProfileName        string `json:"aws_profile"`
}

func (w enrollmentWriterConfig) nativeS3Tables() bool {
	if strings.EqualFold(w.CatalogType, "s3tables") || strings.EqualFold(w.RestSigningName, "s3tables") {
		return true
	}
	if warehouse, err := arn.Parse(w.IcebergS3Path); err == nil && warehouse.Service == "s3tables" {
		return true
	}
	endpoint, err := url.Parse(w.RestCatalogURL)
	if err != nil {
		return false
	}
	host := strings.ToLower(endpoint.Hostname())
	return strings.HasPrefix(host, "s3tables.") || strings.Contains(host, ".s3tables.") || strings.HasPrefix(host, "s3tables-fips.")
}

type enrollmentGetTableAPI interface {
	GetTable(context.Context, *s3tables.GetTableInput, ...func(*s3tables.Options)) (*s3tables.GetTableOutput, error)
}

type nativeEnrollmentDestination struct {
	client      enrollmentGetTableAPI
	bucketARN   string
	unavailable string
}

// The normal AWS SDK credential chain includes IRSA/web identity. Client options
// are injectable by local tests only, never read from policy or user table names.
// No metadata file, source rows, or destination rows are read by this lookup.
func newNativeEnrollmentDestination(ctx context.Context, w enrollmentWriterConfig, options ...func(*s3tables.Options)) *nativeEnrollmentDestination {
	destination := &nativeEnrollmentDestination{bucketARN: w.IcebergS3Path}
	warehouse, err := arn.Parse(w.IcebergS3Path)
	if err != nil || warehouse.Service != "s3tables" || warehouse.Region == "" || warehouse.AccountID == "" || !strings.HasPrefix(warehouse.Resource, "bucket/") || strings.Count(warehouse.Resource, "/") != 1 || strings.TrimPrefix(warehouse.Resource, "bucket/") == "" {
		destination.unavailable = "native S3 Tables enrollment requires a valid table bucket warehouse ARN"
		return destination
	}
	if w.RestSigningRegion != "" && w.RestSigningRegion != warehouse.Region {
		destination.unavailable = "S3 Tables signing region does not match the warehouse ARN"
		return destination
	}
	if (w.AccessKey == "") != (w.SecretKey == "") {
		destination.unavailable = "S3 Tables credentials require both access key and secret key, or the default AWS credential chain"
		return destination
	}
	loadOptions := []func(*config.LoadOptions) error{config.WithRegion(warehouse.Region)}
	if w.ProfileName != "" {
		loadOptions = append(loadOptions, config.WithSharedConfigProfile(w.ProfileName))
	}
	if w.AccessKey != "" {
		loadOptions = append(loadOptions, config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(w.AccessKey, w.SecretKey, w.SessionToken)))
	}
	awsConfig, err := config.LoadDefaultConfig(ctx, loadOptions...)
	if err != nil {
		destination.unavailable = "S3 Tables AWS credential/region configuration is unavailable"
		return destination
	}
	destination.client = s3tables.NewFromConfig(awsConfig, options...)
	return destination
}

func (d *nativeEnrollmentDestination) skipReason(ctx context.Context, namespace, table string) string {
	if d.unavailable != "" {
		return d.unavailable
	}
	_, err := d.client.GetTable(ctx, &s3tables.GetTableInput{TableBucketARN: new(d.bucketARN), Namespace: new(namespace), Name: new(table)})
	if err == nil {
		return "destination S3 Tables table already exists; refusing to merge an unrelated table"
	}
	var response *smithyhttp.ResponseError
	if errors.As(err, &response) {
		if response.HTTPStatusCode() == http.StatusNotFound {
			return ""
		}
		if response.HTTPStatusCode() == http.StatusForbidden || response.HTTPStatusCode() == http.StatusUnauthorized {
			return "destination S3 Tables lookup access denied; require s3tables:GetTable permission"
		}
		return fmt.Sprintf("destination S3 Tables existence lookup failed (HTTP %d); candidate not enrolled", response.HTTPStatusCode())
	}
	var notFound *s3types.NotFoundException
	if errors.As(err, &notFound) {
		return ""
	}
	var apiError smithy.APIError
	if errors.As(err, &apiError) && (apiError.ErrorCode() == "AccessDeniedException" || apiError.ErrorCode() == "ForbiddenException") {
		return "destination S3 Tables lookup access denied; require s3tables:GetTable permission"
	}
	return "destination S3 Tables existence lookup is unavailable; candidate not enrolled"
}
