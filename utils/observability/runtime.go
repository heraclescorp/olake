package observability

import (
	"context"
	"os"
	"runtime/metrics"
	"strconv"
	"sync"

	"github.com/shirou/gopsutil/v4/process"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// runtime/metrics reads are confined to the periodic SDK callback. No MemStats
// stop-the-world read, process I/O, or runtime sampling occurs on ingestion paths.
func registerRuntime(meter metric.Meter, base []attribute.KeyValue) error {
	heapLive, err := meter.Int64ObservableGauge("olake.go.heap.live", metric.WithUnit("By"), metric.WithDescription("Live heap bytes after the most recent GC"))
	if err != nil {
		return err
	}
	heapObjects, err := meter.Int64ObservableGauge("olake.go.heap.objects", metric.WithUnit("{object}"))
	if err != nil {
		return err
	}
	allocBytes, err := meter.Int64ObservableCounter("olake.go.heap.allocations", metric.WithUnit("By"), metric.WithDescription("Cumulative heap allocation bytes"))
	if err != nil {
		return err
	}
	gcCycles, err := meter.Int64ObservableCounter("olake.go.gc.cycles", metric.WithUnit("{cycle}"))
	if err != nil {
		return err
	}
	goroutines, err := meter.Int64ObservableGauge("olake.go.goroutines", metric.WithUnit("{goroutine}"))
	if err != nil {
		return err
	}
	memoryLimit, err := meter.Int64ObservableGauge("olake.go.memory.limit", metric.WithUnit("By"), metric.WithDescription("Effective runtime Go soft memory limit, not the container limit"))
	if err != nil {
		return err
	}
	runtimeCPU, err := meter.Float64ObservableCounter("olake.go.cpu.time", metric.WithUnit("s"), metric.WithDescription("Runtime CPU classes: total capacity includes idle; gc includes gc_assist"))
	if err != nil {
		return err
	}
	processCPU, err := meter.Float64ObservableCounter("process.cpu.time", metric.WithUnit("s"), metric.WithDescription("Actual cumulative Go process CPU time by user/system state"))
	if err != nil {
		return err
	}
	rss, err := meter.Int64ObservableGauge("process.memory.usage", metric.WithUnit("By"), metric.WithDescription("Go process resident set size, when supported by the OS"))
	if err != nil {
		return err
	}
	pauses, err := meter.Int64ObservableCounter("olake.go.gc.pause.bucket", metric.WithUnit("{pause}"), metric.WithDescription("Cumulative runtime GC stop-the-world pause counts <= le seconds; exact runtime histogram bounds, not sampled durations"))
	if err != nil {
		return err
	}

	samples := []metrics.Sample{
		{Name: "/gc/heap/live:bytes"}, {Name: "/gc/heap/objects:objects"},
		{Name: "/gc/heap/allocs:bytes"}, {Name: "/gc/cycles/total:gc-cycles"},
		{Name: "/sched/goroutines:goroutines"}, {Name: "/gc/gomemlimit:bytes"},
		{Name: "/cpu/classes/total:cpu-seconds"}, {Name: "/cpu/classes/gc/total:cpu-seconds"},
		{Name: "/cpu/classes/gc/mark/assist:cpu-seconds"}, {Name: "/sched/pauses/total/gc:seconds"},
	}
	metrics.Read(samples)
	baseOption := []metric.ObserveOption{measurement(base)}
	heapGauges := [...]metric.Int64ObservableGauge{heapLive, heapObjects}
	cpuOptions := [...][]metric.ObserveOption{
		{measurement(base, attribute.String("class", "total_capacity"))},
		{measurement(base, attribute.String("class", "gc"))},
		{measurement(base, attribute.String("class", "gc_assist"))},
	}
	userOption := []metric.ObserveOption{measurement(base, attribute.String("cpu.mode", "user"))}
	systemOption := []metric.ObserveOption{measurement(base, attribute.String("cpu.mode", "system"))}
	var pauseOptions [][]metric.ObserveOption
	if samples[9].Value.Kind() == metrics.KindFloat64Histogram {
		h := samples[9].Value.Float64Histogram()
		pauseOptions = make([][]metric.ObserveOption, len(h.Counts))
		for n := range h.Counts {
			pauseOptions[n] = []metric.ObserveOption{measurement(base, attribute.String("le", strconv.FormatFloat(h.Buckets[n+1], 'g', -1, 64)))}
		}
	}
	proc, _ := process.NewProcess(int32(os.Getpid()))
	var mu sync.Mutex
	_, err = meter.RegisterCallback(func(ctx context.Context, observer metric.Observer) error {
		mu.Lock()
		defer mu.Unlock()
		metrics.Read(samples)
		// Runtime metric kinds are checked so future or unsupported runtimes
		// omit unavailable values instead of panicking or exporting fake zeroes.
		for n, gauge := range heapGauges {
			if samples[n].Value.Kind() == metrics.KindUint64 {
				observer.ObserveInt64(gauge, signedValue(samples[n].Value.Uint64()), baseOption...)
			}
		}
		if samples[2].Value.Kind() == metrics.KindUint64 {
			observer.ObserveInt64(allocBytes, signedValue(samples[2].Value.Uint64()), baseOption...)
		}
		if samples[3].Value.Kind() == metrics.KindUint64 {
			observer.ObserveInt64(gcCycles, signedValue(samples[3].Value.Uint64()), baseOption...)
		}
		if samples[4].Value.Kind() == metrics.KindUint64 {
			observer.ObserveInt64(goroutines, signedValue(samples[4].Value.Uint64()), baseOption...)
		}
		if samples[5].Value.Kind() == metrics.KindUint64 {
			observer.ObserveInt64(memoryLimit, signedValue(samples[5].Value.Uint64()), baseOption...)
		}
		for n := range cpuOptions {
			if samples[n+6].Value.Kind() == metrics.KindFloat64 {
				observer.ObserveFloat64(runtimeCPU, samples[n+6].Value.Float64(), cpuOptions[n]...)
			}
		}
		if samples[9].Value.Kind() == metrics.KindFloat64Histogram {
			var count uint64
			for n, value := range samples[9].Value.Float64Histogram().Counts {
				count += value
				if n < len(pauseOptions) {
					observer.ObserveInt64(pauses, signedValue(count), pauseOptions[n]...)
				}
			}
		}
		if proc != nil {
			if times, err := proc.TimesWithContext(ctx); err == nil {
				observer.ObserveFloat64(processCPU, times.User, userOption...)
				observer.ObserveFloat64(processCPU, times.System, systemOption...)
			}
			if memory, err := proc.MemoryInfoWithContext(ctx); err == nil {
				observer.ObserveInt64(rss, signedValue(memory.RSS), baseOption...)
			}
		}
		return nil
	}, heapLive, heapObjects, allocBytes, gcCycles, goroutines, memoryLimit, runtimeCPU, processCPU, rss, pauses)
	return err
}

func signedValue(value uint64) int64 {
	const maxInt64 = uint64(1<<63 - 1)
	if value > maxInt64 {
		return int64(maxInt64)
	}
	return int64(value)
}
