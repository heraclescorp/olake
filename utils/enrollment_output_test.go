package utils

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/datazip-inc/olake/constants"
	s3util "github.com/datazip-inc/olake/utils/s3"
)

func TestResolveEnrollmentArtifactOutput(t *testing.T) {
	oldBucket, oldPrefix := s3util.JobBucket, s3util.JobPrefix
	t.Cleanup(func() { s3util.JobBucket, s3util.JobPrefix = oldBucket, oldPrefix })
	for _, tc := range []struct{ name, mode, folder, output, source, bucket, prefix, wantLocal, wantURI string }{
		{name: "filesystem default", source: "/mnt/config/source.json", wantLocal: "/mnt/config/enrollment-result.json"},
		{name: "filesystem explicit output", output: "/mnt/results/proposal.json", source: "/mnt/config/source.json", wantLocal: "/mnt/results/proposal.json"},
		{name: "filesystem folder override", folder: "/mnt/config/current", output: "/old/hash/enrollment-result.json", source: "/mnt/config/source.json", wantLocal: "/mnt/config/current/enrollment-result.json"},
		{name: "S3 folder override", mode: constants.StorageModeS3, folder: "s3://config-bucket/current", output: "/old/hash/enrollment-result.json", source: "/tmp/source.json", bucket: "config-bucket", prefix: "current", wantLocal: filepath.Join(os.TempDir(), "enrollment-result.json"), wantURI: "s3://config-bucket/current/enrollment-result.json"},
		{name: "S3 explicit output", mode: constants.StorageModeS3, output: "s3://result-bucket/run/proposal.json", source: "/tmp/source.json", bucket: "config-bucket", prefix: "current", wantLocal: filepath.Join(os.TempDir(), "proposal.json"), wantURI: "s3://result-bucket/run/proposal.json"},
		{name: "S3 explicit output without config bucket", mode: constants.StorageModeS3, output: "s3://result-bucket/run/proposal.json", source: "/mnt/config/source.json", wantLocal: filepath.Join(os.TempDir(), "proposal.json"), wantURI: "s3://result-bucket/run/proposal.json"},
		{name: "S3 inferred default", mode: constants.StorageModeS3, source: filepath.Join(os.TempDir(), "source.json"), bucket: "config-bucket", prefix: "current", wantLocal: filepath.Join(os.TempDir(), "enrollment-result.json"), wantURI: "s3://config-bucket/current/enrollment-result.json"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv(constants.EnvS3ConfigFolder, tc.folder)
			t.Setenv(constants.EnvStorageMode, tc.mode)
			s3util.JobBucket, s3util.JobPrefix = tc.bucket, tc.prefix
			local, uri, err := ResolveArtifactOutput(tc.output, "enrollment-result.json", tc.source)
			if err != nil {
				t.Fatal(err)
			}
			if local != tc.wantLocal || uri != tc.wantURI {
				t.Fatalf("got local=%q uri=%q; expected local=%q uri=%q", local, uri, tc.wantLocal, tc.wantURI)
			}
		})
	}
}

func TestResolveEnrollmentOutputRejectsMalformedURIWithoutDownload(t *testing.T) {
	t.Setenv(constants.EnvS3ConfigFolder, "")
	t.Setenv(constants.EnvStorageMode, constants.StorageModeS3)
	if _, _, err := ResolveArtifactOutput("s3://bucket", "enrollment-result.json", "/mnt/config/source.json"); err == nil {
		t.Fatal("invalid S3 URI accepted")
	}
}

func TestResolveEnrollmentOutputRequiresS3StorageMode(t *testing.T) {
	oldBucket, oldPrefix := s3util.JobBucket, s3util.JobPrefix
	t.Cleanup(func() { s3util.JobBucket, s3util.JobPrefix = oldBucket, oldPrefix })
	s3util.JobBucket, s3util.JobPrefix = "", ""
	t.Setenv(constants.EnvS3ConfigFolder, "")
	for _, mode := range []string{"", "nfs", "S3"} {
		t.Run("mode="+mode, func(t *testing.T) {
			t.Setenv(constants.EnvStorageMode, mode)
			local, uri, err := ResolveArtifactOutput("s3://result-bucket/run/proposal.json", "enrollment-result.json", "/mnt/config/source.json")
			if err == nil || !strings.Contains(err.Error(), "OLAKE_STORAGE_MODE=s3") {
				t.Fatalf("expected S3 storage mode requirement, got %v", err)
			}
			if local != "" || uri != "" {
				t.Fatalf("rejected output returned local=%q uri=%q", local, uri)
			}
		})
	}
}
