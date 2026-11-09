package observability

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"runtime/pprof"
	"strconv"
	"sync"
	"time"

	"github.com/datazip-inc/olake/utils/logger"
)

const maxProfileBytes int64 = 64 << 20
const maxCPUProfileSeconds = 300

// Profiles are local-only: no listener, upload, or automatic directory. Files
// are ephemeral job artifacts; operators opting in own their retention/deletion.
// OLAKE_PROFILE_DIR enables one shutdown heap snapshot (without forcing GC).
// OLAKE_CPU_PROFILE_SECONDS additionally enables CPU profiling, capped at 300s.
// Each output is capped at 64 MiB; oversized profiles may be truncated/unusable.
// Heap serialization is synchronous on shutdown and has a byte, not time, cap.
type profiles struct {
	dir     string
	stopCPU func()
	once    sync.Once
}

func startProfiles() *profiles {
	p := &profiles{stopCPU: func() {}}
	dir := os.Getenv("OLAKE_PROFILE_DIR")
	if dir == "" {
		return p
	}
	if err := os.MkdirAll(dir, 0700); err != nil {
		logger.Warnf("Go local profiling disabled: cannot create profile directory")
		return p
	}
	p.dir = dir
	seconds, err := cpuProfileSeconds(os.Getenv("OLAKE_CPU_PROFILE_SECONDS"))
	if err != nil {
		logger.Warnf("Go CPU profiling disabled: OLAKE_CPU_PROFILE_SECONDS must be a positive integer")
		return p
	}
	if seconds == 0 {
		return p
	}
	file, err := os.CreateTemp(dir, "olake-go-cpu-*.pprof")
	if err != nil {
		logger.Warnf("Go CPU profiling disabled: cannot create local profile")
		return p
	}
	writer := &boundedProfileWriter{writer: file, remaining: maxProfileBytes}
	if err := pprof.StartCPUProfile(writer); err != nil {
		_ = file.Close()
		_ = os.Remove(file.Name())
		logger.Warnf("Go CPU profiling disabled: a profile could not be started")
		return p
	}
	logger.Infof("Go CPU profile path=%q duration_seconds=%d max_bytes=%d", filepath.Clean(file.Name()), seconds, maxProfileBytes)
	var once sync.Once
	done := make(chan struct{})
	p.stopCPU = func() {
		once.Do(func() {
			pprof.StopCPUProfile()
			_ = file.Close()
			if writer.truncated {
				logger.Warnf("Go CPU profile reached its local size bound; file may be incomplete")
			}
			close(done)
		})
	}
	go func() {
		timer := time.NewTimer(time.Duration(seconds) * time.Second)
		defer timer.Stop()
		select {
		case <-timer.C:
			p.stopCPU()
		case <-done:
		}
	}()
	return p
}

func cpuProfileSeconds(value string) (int, error) {
	if value == "" {
		return 0, nil
	}
	seconds, err := strconv.Atoi(value)
	if err != nil || seconds <= 0 {
		return 0, errors.New("invalid CPU profile duration")
	}
	if seconds > maxCPUProfileSeconds {
		seconds = maxCPUProfileSeconds
	}
	return seconds, nil
}

func (p *profiles) stop() {
	p.once.Do(func() {
		p.stopCPU()
		if p.dir == "" {
			return
		}
		file, err := os.CreateTemp(p.dir, "olake-go-heap-*.pprof")
		if err != nil {
			logger.Warnf("Go heap profile skipped: cannot create local profile")
			return
		}
		defer file.Close()
		logger.Infof("Go heap profile path=%q max_bytes=%d", filepath.Clean(file.Name()), maxProfileBytes)
		if err := pprof.WriteHeapProfile(&boundedProfileWriter{writer: file, remaining: maxProfileBytes}); err != nil {
			logger.Warnf("Go heap profile incomplete: local write or size bound reached")
		}
	})
}

type boundedProfileWriter struct {
	writer    io.Writer
	remaining int64
	truncated bool
}

func (w *boundedProfileWriter) Write(data []byte) (int, error) {
	if int64(len(data)) <= w.remaining {
		n, err := w.writer.Write(data)
		w.remaining -= int64(n)
		return n, err
	}
	w.truncated = true
	n, err := w.writer.Write(data[:int(w.remaining)])
	w.remaining -= int64(n)
	if err != nil {
		return n, err
	}
	return n, errors.New("local profile size bound reached")
}
