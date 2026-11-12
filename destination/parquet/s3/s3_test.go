package s3

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/s3/transfermanager"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	"github.com/stretchr/testify/require"
)

func testMemoryS3Store() (*Store, *memoryS3) {
	inner := newMemoryS3()
	return NewWithClient(inner, &memoryUploader{store: inner}, "bucket", ""), inner
}

func TestS3ObjectKey(t *testing.T) {
	tests := []struct {
		name     string
		prefix   string
		relative string
		want     string
	}{
		{
			name:     "no prefix",
			relative: "namespace/table/data.parquet",
			want:     "namespace/table/data.parquet",
		},
		{
			name:     "joins s3 path",
			prefix:   "root",
			relative: "namespace/table/data.parquet",
			want:     "root/namespace/table/data.parquet",
		},
		{
			name:     "nested s3 path",
			prefix:   "allowed/path",
			relative: "namespace/table/_olake_2pc/finish.json",
			want:     "allowed/path/namespace/table/_olake_2pc/finish.json",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := &Store{prefix: tt.prefix}
			require.Equal(t, tt.want, store.ObjectKey(tt.relative))
		})
	}
}

func TestS3IsNotFound(t *testing.T) {
	store := &Store{}
	require.False(t, store.IsNotFound(nil))
	require.False(t, store.IsNotFound(errors.New("other")))
	require.True(t, store.IsNotFound(&s3types.NoSuchKey{Message: aws.String("missing")}))
	require.True(t, store.IsNotFound(&smithy.GenericAPIError{Code: "NotFound", Message: "missing"}))
	require.False(t, store.IsNotFound(&smithy.GenericAPIError{Code: "AccessDenied", Message: "denied"}))
	require.True(t, store.IsNotFound(fmt.Errorf("wrapped: %w", &s3types.NoSuchKey{})))
}

func TestNewS3Store(t *testing.T) {
	t.Run("trims s3 path", func(t *testing.T) {
		store, err := New(Config{Bucket: "bucket", Region: "us-east-1", Prefix: "/root/"})
		require.NoError(t, err)
		require.Equal(t, "s3", store.Kind)
		require.Equal(t, "bucket", store.bucket)
		require.Equal(t, "root", store.prefix)
		require.Equal(t, "root/namespace/table", store.ObjectKey("namespace/table"))
		require.False(t, store.gcs)
	})

	t.Run("custom endpoint uses path style", func(t *testing.T) {
		store, err := New(Config{
			Bucket:     "bucket",
			Region:     "us-east-1",
			S3Endpoint: "https://storage.googleapis.com",
		})
		require.NoError(t, err)
		client, ok := store.client.(*awss3.Client)
		require.True(t, ok)
		require.Equal(t, "https://storage.googleapis.com", aws.ToString(client.Options().BaseEndpoint))
		require.True(t, client.Options().UsePathStyle)
		require.True(t, store.gcs)
	})
}

func TestS3DeletePrefixGCSUsesIndividualDeletes(t *testing.T) {
	ctx := context.Background()
	store, inner := testMemoryS3Store()
	store.gcs = true
	inner.put("root/namespace/table/data.parquet", []byte("table-data"))
	inner.put("root/namespace/table/_olake_2pc/staged.parquet", []byte("staged"))
	inner.put("root/namespace/table_backup/data.parquet", []byte("sibling"))
	require.NoError(t, store.DeletePrefix(ctx, "root/namespace/table/"))
	require.Empty(t, inner.keys("root/namespace/table/"))
	require.Equal(t, []byte("sibling"), inner.get("root/namespace/table_backup/data.parquet"))
}

func TestS3PutGetListCopyDelete(t *testing.T) {
	ctx := context.Background()
	store, inner := testMemoryS3Store()

	require.NoError(t, store.Put(ctx, "root/namespace/table/data.parquet", []byte("table-data")))
	got, err := store.Get(ctx, "root/namespace/table/data.parquet")
	require.NoError(t, err)
	require.Equal(t, []byte("table-data"), got)

	_, err = store.Get(ctx, "root/namespace/table/missing.parquet")
	require.Error(t, err)
	require.True(t, store.IsNotFound(err))

	require.NoError(t, store.Copy(ctx, "root/namespace/table/data.parquet", "root/namespace/table/copy.parquet"))
	require.Equal(t, []byte("table-data"), inner.get("root/namespace/table/copy.parquet"))

	keys, err := store.List(ctx, "root/namespace/table/")
	require.NoError(t, err)
	require.Equal(t, []string{"root/namespace/table/copy.parquet", "root/namespace/table/data.parquet"}, keys)

	require.NoError(t, store.Delete(ctx, "root/namespace/table/copy.parquet"))
	require.Nil(t, inner.get("root/namespace/table/copy.parquet"))
}

func TestS3UploadFile(t *testing.T) {
	ctx := context.Background()
	store, inner := testMemoryS3Store()

	file, err := os.CreateTemp(t.TempDir(), "upload-*.parquet")
	require.NoError(t, err)
	_, err = file.Write([]byte("file-bytes"))
	require.NoError(t, err)
	_, err = file.Seek(0, 0)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, file.Close()) })

	require.NoError(t, store.UploadFile(ctx, "root/namespace/table/upload.parquet", file))
	require.Equal(t, []byte("file-bytes"), inner.get("root/namespace/table/upload.parquet"))
}

func TestS3DeletePrefix(t *testing.T) {
	ctx := context.Background()
	store, inner := testMemoryS3Store()
	inner.put("root/namespace/table/data.parquet", []byte("table-data"))
	inner.put("root/namespace/table/_olake_2pc/staged.parquet", []byte("staged"))
	inner.put("root/namespace/table_backup/data.parquet", []byte("sibling"))

	require.NoError(t, store.DeletePrefix(ctx, "root/namespace/table/"))
	require.Empty(t, inner.keys("root/namespace/table/"))
	require.Equal(t, []byte("sibling"), inner.get("root/namespace/table_backup/data.parquet"))
}

type memoryS3 struct {
	mu       sync.Mutex
	objects  map[string][]byte
	failures map[string]error
}

func newMemoryS3() *memoryS3 {
	return &memoryS3{
		objects:  make(map[string][]byte),
		failures: make(map[string]error),
	}
}

func (m *memoryS3) PutObject(_ context.Context, input *awss3.PutObjectInput, _ ...func(*awss3.Options)) (*awss3.PutObjectOutput, error) {
	key := aws.ToString(input.Key)
	if err := m.failure(memoryS3Put, key); err != nil {
		return nil, err
	}
	data, err := io.ReadAll(input.Body)
	if err != nil {
		return nil, err
	}
	m.put(key, data)
	return &awss3.PutObjectOutput{}, nil
}

func (m *memoryS3) GetObject(_ context.Context, input *awss3.GetObjectInput, _ ...func(*awss3.Options)) (*awss3.GetObjectOutput, error) {
	data := m.get(aws.ToString(input.Key))
	if data == nil {
		return nil, &s3types.NoSuchKey{Message: aws.String("object not found")}
	}
	return &awss3.GetObjectOutput{Body: io.NopCloser(strings.NewReader(string(data)))}, nil
}

func (m *memoryS3) CopyObject(_ context.Context, input *awss3.CopyObjectInput, _ ...func(*awss3.Options)) (*awss3.CopyObjectOutput, error) {
	source := strings.TrimPrefix(aws.ToString(input.CopySource), aws.ToString(input.Bucket)+"/")
	source, err := url.PathUnescape(source)
	if err != nil {
		return nil, err
	}
	if err := m.failure(memoryS3Copy, source); err != nil {
		return nil, err
	}
	data := m.get(source)
	if data == nil {
		return nil, &s3types.NoSuchKey{Message: aws.String("copy source not found")}
	}
	m.put(aws.ToString(input.Key), data)
	return &awss3.CopyObjectOutput{}, nil
}

func (m *memoryS3) DeleteObject(_ context.Context, input *awss3.DeleteObjectInput, _ ...func(*awss3.Options)) (*awss3.DeleteObjectOutput, error) {
	key := aws.ToString(input.Key)
	if err := m.failure(memoryS3Delete, key); err != nil {
		return nil, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.objects, key)
	return &awss3.DeleteObjectOutput{}, nil
}

func (m *memoryS3) listOutput(prefix string) *awss3.ListObjectsV2Output {
	keys := m.keys(prefix)
	objects := make([]s3types.Object, 0, len(keys))
	for _, key := range keys {
		objects = append(objects, s3types.Object{Key: aws.String(key)})
	}
	return &awss3.ListObjectsV2Output{Contents: objects, IsTruncated: aws.Bool(false)}
}

func (m *memoryS3) ListObjectsV2(_ context.Context, input *awss3.ListObjectsV2Input, _ ...func(*awss3.Options)) (*awss3.ListObjectsV2Output, error) {
	page := m.listOutput(aws.ToString(input.Prefix))
	if token := aws.ToString(input.ContinuationToken); token != "" {
		for len(page.Contents) > 0 && aws.ToString(page.Contents[0].Key) <= token {
			page.Contents = page.Contents[1:]
		}
	}
	if len(page.Contents) > 2 {
		page.Contents = page.Contents[:2]
		page.IsTruncated = aws.Bool(true)
		page.NextContinuationToken = page.Contents[1].Key
	}
	return page, nil
}

func (m *memoryS3) DeleteObjects(_ context.Context, input *awss3.DeleteObjectsInput, _ ...func(*awss3.Options)) (*awss3.DeleteObjectsOutput, error) {
	if err := m.failure("batch", aws.ToString(input.Bucket)); err != nil {
		return &awss3.DeleteObjectsOutput{Errors: []s3types.Error{{Code: aws.String("AccessDenied"), Message: aws.String(err.Error())}}}, nil
	}
	if input.Delete == nil {
		return &awss3.DeleteObjectsOutput{}, nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, obj := range input.Delete.Objects {
		delete(m.objects, aws.ToString(obj.Key))
	}
	return &awss3.DeleteObjectsOutput{}, nil
}

const (
	memoryS3Put    = "put"
	memoryS3Copy   = "copy"
	memoryS3Delete = "delete"
)

func (m *memoryS3) failure(operation, key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	failureKey := operation + ":" + key
	err := m.failures[failureKey]
	delete(m.failures, failureKey)
	return err
}

func (m *memoryS3) put(key string, data []byte) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.objects[key] = append([]byte(nil), data...)
}

func (m *memoryS3) get(key string) []byte {
	m.mu.Lock()
	defer m.mu.Unlock()
	data, exists := m.objects[key]
	if !exists {
		return nil
	}
	return append([]byte(nil), data...)
}

func (m *memoryS3) keys(prefix string) []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	var keys []string
	for key := range m.objects {
		if strings.HasPrefix(key, prefix) {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	return keys
}

type memoryUploader struct {
	store *memoryS3
}

func (u *memoryUploader) UploadObject(_ context.Context, input *transfermanager.UploadObjectInput, _ ...func(*transfermanager.Options)) (*transfermanager.UploadObjectOutput, error) {
	key := aws.ToString(input.Key)
	if err := u.store.failure(memoryS3Put, key); err != nil {
		return nil, err
	}
	data, err := io.ReadAll(input.Body)
	if err != nil {
		return nil, err
	}
	u.store.put(key, data)
	return &transfermanager.UploadObjectOutput{}, nil
}

func TestS3StaticCredentials(t *testing.T) {
	store, err := New(Config{Bucket: "bucket", Region: "us-east-1", AccessKey: "access", SecretKey: "secret"})
	require.NoError(t, err)
	client, ok := store.client.(*awss3.Client)
	require.True(t, ok)
	credentials, err := client.Options().Credentials.Retrieve(context.Background())
	require.NoError(t, err)
	require.Equal(t, "access", credentials.AccessKeyID)
	require.Equal(t, "secret", credentials.SecretAccessKey)
}

func TestS3RateLimitErrors(t *testing.T) {
	store := &Store{}
	for _, status := range []int{429, 503} {
		err := &smithyhttp.ResponseError{Response: &smithyhttp.Response{Response: &http.Response{StatusCode: status}}, Err: errors.New("throttled")}
		require.True(t, store.IsRateLimitError(fmt.Errorf("wrapped: %w", err)))
	}
	require.True(t, store.IsRateLimitError(errors.Join(errors.New("other"), &smithy.GenericAPIError{Code: "SlowDown"})))
	require.False(t, store.IsRateLimitError(&smithy.GenericAPIError{Code: "AccessDenied"}))
	require.False(t, store.IsRateLimitError(nil))
}

func TestS3PaginationAndDeleteFallback(t *testing.T) {
	ctx := context.Background()
	store, inner := testMemoryS3Store()
	for _, key := range []string{"root/a", "root/b", "root/c", "root/d", "sibling/a"} {
		inner.put(key, []byte(key))
	}
	keys, err := store.List(ctx, "root/")
	require.NoError(t, err)
	require.Equal(t, []string{"root/a", "root/b", "root/c", "root/d"}, keys)
	require.NoError(t, store.DeletePrefix(ctx, "root/"))
	require.Empty(t, inner.keys("root/"))
	for _, key := range keys {
		inner.put(key, []byte(key))
	}
	inner.failures["batch:bucket"] = errors.New("partial failure")
	require.NoError(t, store.DeletePrefix(ctx, "root/"))
	require.Empty(t, inner.keys("root/"))
	require.Equal(t, []byte("sibling/a"), inner.get("sibling/a"))
	inner.put("root/a", []byte("retained"))
	inner.failures["batch:bucket"] = errors.New("partial failure")
	inner.failures[memoryS3Delete+":root/a"] = errors.New("delete denied")
	require.ErrorContains(t, store.DeletePrefix(ctx, "root/"), "fallback individual delete also failed")
	require.Equal(t, []byte("retained"), inner.get("root/a"))
}
