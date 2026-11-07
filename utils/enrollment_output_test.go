package utils

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/datazip-inc/olake/constants"
	s3util "github.com/datazip-inc/olake/utils/s3"
)

func TestResolveEnrollmentArtifactOutput(t *testing.T) {
	oldBucket, oldPrefix := s3util.JobBucket, s3util.JobPrefix
	t.Cleanup(func() { s3util.JobBucket, s3util.JobPrefix = oldBucket, oldPrefix })
	for _, tc := range []struct{ name, folder, output, source, bucket, prefix, wantLocal, wantURI string }{
		{name: "filesystem default", source: "/mnt/config/source.json", wantLocal: "/mnt/config/enrollment-result.json"},
		{name: "filesystem folder override", folder: "/mnt/config/current", output: "/old/hash/enrollment-result.json", source: "/mnt/config/source.json", wantLocal: "/mnt/config/current/enrollment-result.json"},
		{name: "S3 folder override", folder: "s3://config-bucket/current", output: "/old/hash/enrollment-result.json", source: "/tmp/source.json", bucket: "config-bucket", prefix: "current", wantLocal: filepath.Join(os.TempDir(), "enrollment-result.json"), wantURI: "s3://config-bucket/current/enrollment-result.json"},
		{name: "S3 explicit output", output: "s3://result-bucket/run/proposal.json", source: "/tmp/source.json", bucket: "config-bucket", prefix: "current", wantLocal: filepath.Join(os.TempDir(), "proposal.json"), wantURI: "s3://result-bucket/run/proposal.json"},
		{name: "S3 inferred default", source: filepath.Join(os.TempDir(), "source.json"), bucket: "config-bucket", prefix: "current", wantLocal: filepath.Join(os.TempDir(), "enrollment-result.json"), wantURI: "s3://config-bucket/current/enrollment-result.json"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv(constants.EnvS3ConfigFolder, tc.folder)
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
	if _, _, err := ResolveArtifactOutput("s3://bucket", "enrollment-result.json", "/mnt/config/source.json"); err == nil {
		t.Fatal("invalid S3 URI accepted")
	}
}
