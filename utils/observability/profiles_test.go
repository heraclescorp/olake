package observability

import (
	"bytes"
	"compress/gzip"
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestCPUProfileDurationBounds(t *testing.T) {
	for _, tc := range []struct {
		input   string
		want    int
		invalid bool
	}{
		{"", 0, false}, {"1", 1, false}, {"300", 300, false}, {"301", 300, false},
		{"0", 0, true}, {"-1", 0, true}, {"1.5", 0, true}, {"garbage", 0, true},
	} {
		t.Run(tc.input, func(t *testing.T) {
			got, err := cpuProfileSeconds(tc.input)
			if (err != nil) != tc.invalid || got != tc.want {
				t.Fatalf("seconds=%d err=%v want=%d invalid=%t", got, err, tc.want, tc.invalid)
			}
		})
	}
}

func TestProfileWriterEnforcesByteLimit(t *testing.T) {
	var output bytes.Buffer
	writer := &boundedProfileWriter{writer: &output, remaining: 5}
	if n, err := writer.Write([]byte("abc")); n != 3 || err != nil {
		t.Fatalf("first write n=%d err=%v", n, err)
	}
	if n, err := writer.Write([]byte("def")); n != 2 || err == nil {
		t.Fatalf("limited write n=%d err=%v", n, err)
	}
	if n, err := writer.Write([]byte("more")); n != 0 || err == nil {
		t.Fatalf("exhausted write n=%d err=%v", n, err)
	}
	if output.String() != "abcde" || !writer.truncated {
		t.Fatalf("unexpected bounded output=%q truncated=%t", output.String(), writer.truncated)
	}
}

func TestProfilesRequireDirectoryOptIn(t *testing.T) {
	cleanEnvironment(t)
	t.Setenv("OLAKE_CPU_PROFILE_SECONDS", "1")
	p := startProfiles()
	if p.dir != "" {
		t.Fatal("profile enabled without directory")
	}
	p.stop()
}

func TestHeapProfileDoesNotRequireMetrics(t *testing.T) {
	cleanEnvironment(t)
	dir := t.TempDir()
	t.Setenv("OLAKE_PROFILE_DIR", dir)
	shutdown, err := Start(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if Enabled() {
		t.Fatal("profiles unexpectedly enabled metrics")
	}
	if err := shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	files, err := filepath.Glob(filepath.Join(dir, "*.pprof"))
	if err != nil || len(files) != 1 {
		t.Fatalf("heap files=%v err=%v", files, err)
	}
	if !strings.Contains(filepath.Base(files[0]), "heap") {
		t.Fatalf("unexpected profile %s", files[0])
	}
	assertLocalProfile(t, files[0])
}

func TestCPUProfileStopsAndWritesLocally(t *testing.T) {
	cleanEnvironment(t)
	dir := t.TempDir()
	t.Setenv("OLAKE_PROFILE_DIR", dir)
	t.Setenv("OLAKE_CPU_PROFILE_SECONDS", "301")
	p := startProfiles()
	p.stop()
	p.stop()
	files, err := filepath.Glob(filepath.Join(dir, "*.pprof"))
	if err != nil || len(files) != 2 {
		t.Fatalf("CPU and heap files=%v err=%v", files, err)
	}
	for _, file := range files {
		assertLocalProfile(t, file)
	}
}

func TestInvalidCPUProfileDurationLeavesExplicitHeapOptIn(t *testing.T) {
	cleanEnvironment(t)
	dir := t.TempDir()
	t.Setenv("OLAKE_PROFILE_DIR", dir)
	t.Setenv("OLAKE_CPU_PROFILE_SECONDS", "invalid")
	p := startProfiles()
	p.stop()
	files, err := filepath.Glob(filepath.Join(dir, "*.pprof"))
	if err != nil || len(files) != 1 || !strings.Contains(filepath.Base(files[0]), "heap") {
		t.Fatalf("unexpected profiles=%v err=%v", files, err)
	}
}

func assertLocalProfile(t *testing.T, path string) {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0600 {
		t.Fatalf("profile permissions=%o want=600", info.Mode().Perm())
	}
	if info.Size() == 0 || info.Size() > maxProfileBytes {
		t.Fatalf("profile bytes=%d", info.Size())
	}
	file, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	reader, err := gzip.NewReader(file)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	if _, err := io.Copy(io.Discard, reader); err != nil {
		t.Fatalf("invalid compressed profile: %v", err)
	}
}
