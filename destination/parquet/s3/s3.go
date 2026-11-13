package s3

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path"
	"runtime"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/s3/transfermanager"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	"github.com/datazip-inc/olake/utils"
	"github.com/datazip-inc/olake/utils/logger"
)

type Config struct {
	Bucket     string
	Region     string
	AccessKey  string
	SecretKey  string
	Prefix     string
	S3Endpoint string
}

// Client is the S3 operation boundary used by Store.
type Client interface {
	PutObject(context.Context, *awss3.PutObjectInput, ...func(*awss3.Options)) (*awss3.PutObjectOutput, error)
	GetObject(context.Context, *awss3.GetObjectInput, ...func(*awss3.Options)) (*awss3.GetObjectOutput, error)
	ListObjectsV2(context.Context, *awss3.ListObjectsV2Input, ...func(*awss3.Options)) (*awss3.ListObjectsV2Output, error)
	CopyObject(context.Context, *awss3.CopyObjectInput, ...func(*awss3.Options)) (*awss3.CopyObjectOutput, error)
	DeleteObject(context.Context, *awss3.DeleteObjectInput, ...func(*awss3.Options)) (*awss3.DeleteObjectOutput, error)
	DeleteObjects(context.Context, *awss3.DeleteObjectsInput, ...func(*awss3.Options)) (*awss3.DeleteObjectsOutput, error)
}

// Uploader supports streaming and multipart file uploads.
type Uploader interface {
	UploadObject(context.Context, *transfermanager.UploadObjectInput, ...func(*transfermanager.Options)) (*transfermanager.UploadObjectOutput, error)
}

// Store is the implementation of the ObjectStore interface for S3.
type Store struct {
	client   Client
	uploader Uploader
	bucket   string
	prefix   string
	gcs      bool
	Kind     string
}

// New creates a new AWS S3 client.
func New(cfg Config) (*Store, error) {
	options := []func(*config.LoadOptions) error{config.WithRegion(cfg.Region)}
	if cfg.AccessKey != "" && cfg.SecretKey != "" {
		options = append(options, config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(cfg.AccessKey, cfg.SecretKey, "")))
	}
	clientCfg, err := config.LoadDefaultConfig(context.Background(), options...)
	if err != nil {
		return nil, fmt.Errorf("failed to load AWS configuration: %w", err)
	}
	client := awss3.NewFromConfig(clientCfg, func(options *awss3.Options) {
		if cfg.S3Endpoint != "" {
			options.BaseEndpoint = aws.String(cfg.S3Endpoint)
			options.UsePathStyle = true
		}
		// S3-compatible endpoints may not implement optional checksum trailers.
		options.RequestChecksumCalculation = aws.RequestChecksumCalculationWhenRequired
		options.ResponseChecksumValidation = aws.ResponseChecksumValidationWhenRequired
	})
	store := NewWithClient(client, transfermanager.New(client, func(options *transfermanager.Options) {
		options.RequestChecksumCalculation = aws.RequestChecksumCalculationWhenRequired
		options.PartSizeBytes = 5 * 1024 * 1024
		options.MultipartUploadThreshold = 5 * 1024 * 1024
	}), cfg.Bucket, cfg.Prefix)
	store.gcs = strings.Contains(cfg.S3Endpoint, "googleapis.com")
	return store, nil
}

func NewWithClient(client Client, uploader Uploader, bucket, prefix string) *Store {
	return &Store{
		client:   client,
		uploader: uploader,
		bucket:   bucket,
		prefix:   strings.Trim(prefix, "/"),
		Kind:     "s3",
	}
}

// WithPrefix shares clients and bucket, retaining endpoint-specific delete behavior.
func (s *Store) WithPrefix(prefix string) *Store {
	next := NewWithClient(s.client, s.uploader, s.bucket, prefix)
	next.gcs = s.gcs
	next.Kind = s.Kind
	return next
}

func (s *Store) ObjectKey(relativePath string) string {
	return path.Join(s.prefix, relativePath)
}

func (s *Store) Put(ctx context.Context, key string, data []byte) error {
	_, err := s.client.PutObject(ctx, &awss3.PutObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
		Body:   bytes.NewReader(data),
	})
	return err
}

func (s *Store) UploadFile(ctx context.Context, key string, file *os.File) error {
	_, err := s.uploader.UploadObject(ctx, &transfermanager.UploadObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
		Body:   file,
	})
	return err
}

func (s *Store) DeletePrefix(ctx context.Context, prefix string) error {
	if s.gcs {
		return s.deletePrefixIndividually(ctx, prefix)
	}
	err := utils.RetryWithSkip(ctx, 3, time.Minute, s.IsRateLimitError, func(ctx context.Context) error {
		pages := awss3.NewListObjectsV2Paginator(s.client, &awss3.ListObjectsV2Input{
			Bucket: aws.String(s.bucket), Prefix: aws.String(prefix),
		})
		for pages.HasMorePages() {
			page, err := pages.NextPage(ctx)
			if err != nil {
				return err
			}
			if len(page.Contents) == 0 {
				continue
			}
			objects := make([]types.ObjectIdentifier, 0, len(page.Contents))
			for _, object := range page.Contents {
				objects = append(objects, types.ObjectIdentifier{Key: object.Key})
			}
			out, err := s.client.DeleteObjects(ctx, &awss3.DeleteObjectsInput{
				Bucket: aws.String(s.bucket), Delete: &types.Delete{Objects: objects, Quiet: aws.Bool(true)},
			})
			if err != nil {
				return err
			}
			// S3 may report individual failures in an otherwise successful response.
			if len(out.Errors) != 0 {
				failures := make([]error, 0, len(out.Errors))
				for _, failure := range out.Errors {
					failures = append(failures, &smithy.GenericAPIError{Code: aws.ToString(failure.Code), Message: aws.ToString(failure.Message)})
				}
				return errors.Join(failures...)
			}
		}
		return nil
	})
	if err != nil {
		logger.Warnf("batch delete failed for prefix %s, falling back to individual deletes: %v", prefix, err)
		if fallbackErr := s.deletePrefixIndividually(ctx, prefix); fallbackErr != nil {
			return fmt.Errorf("batch delete failed: %v, fallback individual delete also failed: %w", err, fallbackErr)
		}
	}
	return nil
}

// GCS has no DeleteObjects API; delete each page concurrently with throttling retries.
func (s *Store) deletePrefixIndividually(ctx context.Context, prefix string) error {
	return utils.RetryWithSkip(ctx, 3, time.Minute, s.IsRateLimitError, func(ctx context.Context) error {
		pages := awss3.NewListObjectsV2Paginator(s.client, &awss3.ListObjectsV2Input{
			Bucket: aws.String(s.bucket), Prefix: aws.String(prefix),
		})
		for pages.HasMorePages() {
			page, err := pages.NextPage(ctx)
			if err != nil {
				return fmt.Errorf("failed to list objects for prefix %s: %w", prefix, err)
			}
			keys := make([]string, 0, len(page.Contents))
			for _, object := range page.Contents {
				keys = append(keys, aws.ToString(object.Key))
			}
			if len(keys) == 0 {
				continue
			}
			concurrency := min(runtime.GOMAXPROCS(0)*4, len(keys))
			if err := utils.Concurrent(ctx, keys, concurrency, func(ctx context.Context, key string, _ int) error {
				return utils.RetryWithSkip(ctx, 3, time.Minute, s.IsRateLimitError, func(ctx context.Context) error {
					return s.Delete(ctx, key)
				})
			}); err != nil {
				return err
			}
		}
		return nil
	})
}

func (s *Store) Get(ctx context.Context, key string) ([]byte, error) {
	out, err := s.client.GetObject(ctx, &awss3.GetObjectInput{Bucket: aws.String(s.bucket), Key: aws.String(key)})
	if err != nil {
		return nil, err
	}
	defer out.Body.Close()
	return io.ReadAll(out.Body)
}

func (s *Store) List(ctx context.Context, prefix string) ([]string, error) {
	var keys []string
	pages := awss3.NewListObjectsV2Paginator(s.client, &awss3.ListObjectsV2Input{Bucket: aws.String(s.bucket), Prefix: aws.String(prefix)})
	for pages.HasMorePages() {
		page, err := pages.NextPage(ctx)
		if err != nil {
			return keys, err
		}
		for _, object := range page.Contents {
			keys = append(keys, aws.ToString(object.Key))
		}
	}
	return keys, nil
}

func (s *Store) Copy(ctx context.Context, srcKey, dstKey string) error {
	escaped := strings.ReplaceAll(url.PathEscape(srcKey), "%2F", "/")
	_, err := s.client.CopyObject(ctx, &awss3.CopyObjectInput{
		Bucket: aws.String(s.bucket), Key: aws.String(dstKey), CopySource: aws.String(s.bucket + "/" + escaped),
	})
	return err
}

func (s *Store) Delete(ctx context.Context, key string) error {
	_, err := s.client.DeleteObject(ctx, &awss3.DeleteObjectInput{Bucket: aws.String(s.bucket), Key: aws.String(key)})
	return err
}

func (s *Store) IsNotFound(err error) bool {
	var apiErr smithy.APIError
	return errors.As(err, &apiErr) && (apiErr.ErrorCode() == "NoSuchKey" || apiErr.ErrorCode() == "NotFound")
}

// IsRateLimitError recognizes wrapped HTTP throttle responses and batch item codes.
func (s *Store) IsRateLimitError(err error) bool {
	var responseErr *smithyhttp.ResponseError
	if errors.As(err, &responseErr) && (responseErr.HTTPStatusCode() == 429 || responseErr.HTTPStatusCode() == 503) {
		return true
	}
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		switch apiErr.ErrorCode() {
		case "SlowDown", "ServiceUnavailable", "Throttling", "ThrottlingException", "TooManyRequestsException":
			return true
		}
	}
	var joined interface{ Unwrap() []error }
	if errors.As(err, &joined) {
		for _, cause := range joined.Unwrap() {
			if s.IsRateLimitError(cause) {
				return true
			}
		}
	}
	return false
}
