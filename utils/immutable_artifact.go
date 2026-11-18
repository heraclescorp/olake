package utils

import (
	"context"
	"fmt"
	"io"
	"os"

	s3util "github.com/datazip-inc/olake/utils/s3"
)

// ReadImmutableArtifact reads remote evidence authoritatively when a URI exists.
func ReadImmutableArtifact(ctx context.Context, localPath, uri string) ([]byte, error) {
	if uri == "" {
		return os.ReadFile(localPath)
	}
	bucket, key, err := parseS3URI(uri)
	if err != nil {
		return nil, err
	}
	object, err := s3util.GetObject(ctx, bucket, key)
	if err != nil {
		return nil, err
	}
	defer object.Body.Close()
	data, err := io.ReadAll(io.LimitReader(object.Body, 1<<20+1))
	if err != nil {
		return nil, err
	}
	if len(data) > 1<<20 {
		return nil, fmt.Errorf("immutable artifact exceeds maximum document size")
	}
	return data, nil
}
