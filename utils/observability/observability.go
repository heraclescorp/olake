// Package observability provides opt-in, bounded OTLP metrics and local profiles.
// It deliberately depends on neither the connector's types nor its utils package.
//
// An explicit OTEL_EXPORTER_OTLP_ENDPOINT or OTEL_EXPORTER_OTLP_METRICS_ENDPOINT
// enables HTTP/protobuf export every 15 seconds, with a five-second collection
// deadline and a final export on shutdown. Standard OTLP headers, TLS, compression,
// timeout, resource, and temporality environment settings are handled by the SDK.
// SDK failures are sanitized and nonfatal. No tracing or public listener is used.
//
// All datapoints carry job_id, component=go, and operation explicitly for AMP.
// Workflow and pod metadata are resource-only. Only row/byte, chunk, pending
// index, and last-progress metrics carry table labels: 2048 namespace/table pairs
// plus overflow, with 256-byte values. Durations, failures, and barrier attempts
// remain job-level.
// Chunk totals are monotonic; active chunks and pending entries/estimated bytes
// are signed outstanding-work measurements, not heap measurements.
//
// Runtime measurements include actual process CPU/RSS when the OS supports them,
// runtime CPU capacity/GC/assist, live heap/objects, cumulative allocations, GC
// cycles, exact cumulative GC pause buckets, goroutines, and effective GOMEMLIMIT.
// GC pause buckets are counters with le bounds, not a fabricated histogram sum.
//
// OLAKE_PROFILE_DIR explicitly enables one local shutdown heap snapshot, without
// forcing GC. OLAKE_CPU_PROFILE_SECONDS additionally enables a CPU profile capped
// at 300 seconds. Each private local file is capped at 64 MiB; truncation can make
// a profile unusable. Heap serialization is byte-bounded, not time-bounded.
// Nothing uploads profiles; the operator owns ephemeral file retention/deletion.
package observability

import (
	"context"
	"errors"
	"net/url"
	"os"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/datazip-inc/olake/utils/logger"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/exemplar"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.opentelemetry.io/otel/sdk/resource"
)

const (
	maxTables          = 2048
	collectionInterval = 15 * time.Second
	exportTimeout      = 5 * time.Second
)

var durationStages = [...]string{
	"read", "query_to_first_row", "source_backpressure", "normalize", "encode", "write", "rpc",
	"source_next_sample", "row_convert_sample", "row_handoff_sample", "flush_queue",
	"index_pending", "index_commit", "chunk", "table", "flush", "upload", "commit", "reader_barrier", "other",
}
var rowStages = [...]string{"read", "pushed", "committed"}
var chunkStates = [...]string{"planned", "active", "completed", "failed", "skipped"}

var active atomic.Pointer[instruments]
var lifecycleMu sync.Mutex

type tableKey struct{ namespace, table string }
type tableContextKey struct{}

type tableAttributes struct {
	owner           *instruments
	rows            [len(rowStages)][]metric.AddOption
	progress        [len(rowStages)]atomic.Int64
	progressOptions [len(rowStages)][]metric.ObserveOption
	chunks          [len(chunkStates)][]metric.AddOption
	pending         []metric.AddOption
}

type instruments struct {
	duration         metric.Float64Histogram
	failures         metric.Int64Counter
	failureOptions   [len(durationStages)][]metric.AddOption
	barrierAttempts  metric.Int64Histogram
	barrierDurations [2][]metric.RecordOption
	barrierOutcomes  [2][]metric.RecordOption
	rows             metric.Int64Counter
	bytes            metric.Int64Counter
	lastProgress     metric.Int64ObservableGauge
	chunks           metric.Int64Counter
	activeChunks     metric.Int64UpDownCounter
	pendingEntries   metric.Int64UpDownCounter
	pendingBytes     metric.Int64UpDownCounter
	base             []attribute.KeyValue
	durations        [len(durationStages)][]metric.RecordOption
	mu               sync.Mutex
	tables           map[tableKey]*tableAttributes
	overflow         *tableAttributes
	untabled         *tableAttributes
}

// Enabled reports whether metrics are active. Callers may use it to avoid even
// sampled clocks when diagnostics are disabled. Profiling is independent.
func Enabled() bool { return active.Load() != nil }

// Start enables metrics only when an OTLP endpoint is explicitly configured.
// The HTTP/protobuf exporter respects the standard OTLP environment settings
// (metrics-specific settings take precedence). Collection runs every 15 seconds,
// never on ingestion calls. The returned shutdown is always safe and idempotent;
// callers must invoke it with a fresh, bounded context even after cancellation.
// Initialization/export errors are diagnostics only, never ingestion failures.
// Explicit local profiling remains available without an OTLP endpoint.
func Start(ctx context.Context) (func(context.Context) error, error) {
	lifecycleMu.Lock()
	defer lifecycleMu.Unlock()
	if active.Load() != nil {
		return func(context.Context) error { return nil }, errors.New("Go metrics already started")
	}
	logger.Infof("runtime budget: Go effective memory limit=%d bytes, GOMEMLIMIT configured=%t", debug.SetMemoryLimit(-1), os.Getenv("GOMEMLIMIT") != "")
	if percent, err := strconv.Atoi(os.Getenv("OLAKE_JAVA_HEAP_PERCENT")); err == nil && percent >= 1 && percent <= 80 {
		logger.Infof("runtime budget: configured per-JVM MaxRAMPercentage=%d", percent)
	}
	profiles := startProfiles()
	var provider *sdkmetric.MeterProvider
	var inst *instruments
	var startErr error
	sdkDisabled, _ := strconv.ParseBool(os.Getenv("OTEL_SDK_DISABLED"))
	metricsDisabled := strings.EqualFold(strings.TrimSpace(os.Getenv("OTEL_METRICS_EXPORTER")), "none")
	if !sdkDisabled && !metricsDisabled && (os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT") != "" || os.Getenv("OTEL_EXPORTER_OTLP_METRICS_ENDPOINT") != "") {
		provider, inst, startErr = startMetrics(ctx)
		if inst != nil {
			active.Store(inst)
		}
	}
	var once sync.Once
	var shutdownErr error
	return func(ctx context.Context) error {
		once.Do(func() {
			if inst != nil {
				active.CompareAndSwap(inst, nil)
			}
			if provider != nil {
				// PeriodicReader.Shutdown performs the final collection and export.
				shutdownErr = diagnosticError(provider.Shutdown(ctx))
			}
			profiles.stop()
		})
		return shutdownErr
	}, startErr
}

func startMetrics(ctx context.Context) (*sdkmetric.MeterProvider, *instruments, error) {
	// Reject malformed endpoints rather than letting the exporter fall back to
	// localhost. Do not include endpoint/header values in errors or logs.
	endpoint := os.Getenv("OTEL_EXPORTER_OTLP_METRICS_ENDPOINT")
	if endpoint == "" {
		endpoint = os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT")
	}
	u, err := url.Parse(endpoint)
	if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") || u.User != nil {
		return nil, nil, errors.New("invalid OTLP HTTP metrics endpoint")
	}
	exporter, err := otlpmetrichttp.New(ctx, otlpmetrichttp.WithMaxRequestSize(4<<20))
	if err != nil {
		return nil, nil, errors.New("could not initialize OTLP HTTP metrics exporter")
	}
	base := []attribute.KeyValue{
		attribute.String("job_id", labelValue(os.Getenv("OLAKE_JOB_ID"), "unknown")),
		attribute.String("component", "go"),
		attribute.String("operation", labelValue(os.Getenv("OLAKE_OPERATION_TYPE"), "unknown")),
	}
	attrs := append([]attribute.KeyValue{attribute.String("service.name", "olake-connector-go")}, base...)
	host := os.Getenv("POD_NAME")
	if host == "" {
		host, _ = os.Hostname()
	}
	attrs = append(attrs,
		attribute.String("service.namespace", labelValue(os.Getenv("SERVICE_NAMESPACE"), labelValue(os.Getenv("POD_NAMESPACE"), "localhost"))),
		attribute.String("service.instance.id", labelValue(host, "localhost")+"-"+strconv.Itoa(os.Getpid())),
	)
	if version := os.Getenv("GIT_HASH"); version != "" {
		attrs = append(attrs, attribute.String("service.version", version))
	}
	for _, item := range []struct{ env, key string }{
		{"OLAKE_WORKFLOW_ID", "workflow_id"},
		{"POD_NAME", "k8s.pod.name"}, {"POD_NAMESPACE", "k8s.namespace.name"},
		{"POD_UID", "k8s.pod.uid"}, {"NODE_NAME", "k8s.node.name"},
	} {
		if value := os.Getenv(item.env); value != "" {
			attrs = append(attrs, attribute.String(item.key, labelValue(value, "unknown")))
		}
	}
	res, err := resource.New(ctx, resource.WithTelemetrySDK(), resource.WithAttributes(attrs...), resource.WithFromEnv())
	if err != nil {
		_ = exporter.Shutdown(ctx)
		return nil, nil, errors.New("could not initialize Go metric resource")
	}
	reader := sdkmetric.NewPeriodicReader(safeExporter{exporter}, sdkmetric.WithInterval(collectionInterval), sdkmetric.WithTimeout(exportTimeout))
	provider := sdkmetric.NewMeterProvider(
		sdkmetric.WithResource(res), sdkmetric.WithReader(reader),
		sdkmetric.WithCardinalityLimit((maxTables+2)*len(chunkStates)), sdkmetric.WithExemplarFilter(exemplar.AlwaysOffFilter),
	)
	inst, err := newInstruments(provider.Meter("github.com/datazip-inc/olake/utils/observability"), base)
	if err != nil {
		_ = provider.Shutdown(ctx)
		return nil, nil, errors.New("could not initialize Go metric instruments")
	}
	if err := registerRuntime(provider.Meter("github.com/datazip-inc/olake/utils/observability"), base); err != nil {
		_ = provider.Shutdown(ctx)
		return nil, nil, errors.New("could not initialize Go runtime metrics")
	}
	return provider, inst, nil
}

func newInstruments(meter metric.Meter, base []attribute.KeyValue) (*instruments, error) {
	i := &instruments{base: base, tables: make(map[tableKey]*tableAttributes, maxTables)}
	var err error
	i.duration, err = meter.Float64Histogram("olake.stage.duration", metric.WithUnit("s"), metric.WithDescription("Go stage duration; job-level, without table labels"), metric.WithExplicitBucketBoundaries(.001, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 30, 60, 300))
	if err != nil {
		return nil, err
	}
	i.failures, err = meter.Int64Counter("olake.stage.failures", metric.WithUnit("{failure}"), metric.WithDescription("Failures by bounded Go stage; no error messages or table labels"))
	if err != nil {
		return nil, err
	}
	i.rows, err = meter.Int64Counter("olake.rows", metric.WithUnit("{row}"), metric.WithDescription("Rows read, pushed to a destination, or successfully committed; separate stages"))
	if err != nil {
		return nil, err
	}
	i.bytes, err = meter.Int64Counter("olake.bytes", metric.WithUnit("By"), metric.WithDescription("Bytes associated with row stages; caller supplied"))
	if err != nil {
		return nil, err
	}
	i.lastProgress, err = meter.Int64ObservableGauge("olake.last_progress.time", metric.WithUnit("s"), metric.WithDescription("Last positive batched row/byte progress, Unix epoch seconds, by read/pushed/committed stage; retained while idle"))
	if err != nil {
		return nil, err
	}
	i.chunks, err = meter.Int64Counter("olake.chunks", metric.WithUnit("{chunk}"), metric.WithDescription("Chunk totals by planned/completed/failed/skipped state; never inferred from buffer pushes"))
	if err != nil {
		return nil, err
	}
	i.activeChunks, err = meter.Int64UpDownCounter("olake.chunks.active", metric.WithUnit("{chunk}"), metric.WithDescription("Active chunk concurrency; explicit signed deltas"))
	if err != nil {
		return nil, err
	}
	i.pendingEntries, err = meter.Int64UpDownCounter("olake.index.pending.entries", metric.WithUnit("{entry}"), metric.WithDescription("Entries pending index commit; signed deltas"))
	if err != nil {
		return nil, err
	}
	i.pendingBytes, err = meter.Int64UpDownCounter("olake.index.pending.estimated_bytes", metric.WithUnit("By"), metric.WithDescription("ESTIMATED serialized bytes pending index commit, NOT actual Go heap or RSS"))
	if err != nil {
		return nil, err
	}
	for n, stage := range durationStages {
		option := measurement(base, attribute.String("stage", stage))
		i.durations[n] = []metric.RecordOption{option}
		i.failureOptions[n] = []metric.AddOption{option}
	}
	i.barrierAttempts, err = meter.Int64Histogram("olake.reader_barrier.attempts", metric.WithUnit("{attempt}"), metric.WithDescription("Reader visibility barrier attempts per barrier, separated by success/failure"), metric.WithExplicitBucketBoundaries(1, 2, 3, 5, 10, 20, 50, 100))
	if err != nil {
		return nil, err
	}
	for n, outcome := range [...]string{"failure", "success"} {
		i.barrierOutcomes[n] = []metric.RecordOption{measurement(base, attribute.String("outcome", outcome))}
		i.barrierDurations[n] = []metric.RecordOption{measurement(base, attribute.String("stage", "reader_barrier"), attribute.String("outcome", outcome))}
	}
	i.untabled = i.newTableAttributes("", "")
	i.overflow = i.newTableAttributes("__overflow__", "__overflow__")
	_, err = meter.RegisterCallback(func(_ context.Context, observer metric.Observer) error {
		i.mu.Lock()
		defer i.mu.Unlock()
		i.untabled.observeProgress(observer, i.lastProgress)
		i.overflow.observeProgress(observer, i.lastProgress)
		for _, table := range i.tables {
			table.observeProgress(observer, i.lastProgress)
		}
		return nil
	}, i.lastProgress)
	if err != nil {
		return nil, err
	}
	return i, nil
}

func labelValue(value, fallback string) string {
	if value == "" {
		return fallback
	}
	// Bound retained label bytes as well as series count. Truncation occurs only
	// when creating contexts, never in row or duration recording.
	if len(value) > 256 {
		value = strings.Clone(value[:256])
	}
	return strings.ToValidUTF8(value, "?")
}

func measurement(base []attribute.KeyValue, extra ...attribute.KeyValue) metric.MeasurementOption {
	attrs := make([]attribute.KeyValue, 0, len(base)+len(extra))
	attrs = append(attrs, base...)
	attrs = append(attrs, extra...)
	return metric.WithAttributeSet(attribute.NewSet(attrs...))
}

func (i *instruments) newTableAttributes(namespace, table string) *tableAttributes {
	attrs := i.base
	if namespace != "" || table != "" {
		attrs = append(append([]attribute.KeyValue(nil), i.base...), attribute.String("namespace", namespace), attribute.String("table", table))
	}
	t := &tableAttributes{owner: i, pending: []metric.AddOption{measurement(attrs)}}
	for n, stage := range rowStages {
		option := measurement(attrs, attribute.String("stage", stage))
		t.rows[n] = []metric.AddOption{option}
		t.progressOptions[n] = []metric.ObserveOption{option}
	}
	for n, state := range chunkStates {
		t.chunks[n] = []metric.AddOption{measurement(attrs, attribute.String("state", state))}
	}
	return t
}

func (t *tableAttributes) observeProgress(observer metric.Observer, gauge metric.Int64ObservableGauge) {
	for n := range t.progress {
		if timestamp := t.progress[n].Load(); timestamp > 0 {
			observer.ObserveInt64(gauge, timestamp, t.progressOptions[n]...)
		}
	}
}

// WithTable caches all per-table attribute sets in the context. Only the first
// 2048 distinct namespace/table pairs are retained; subsequent pairs share an
// explicit __overflow__ series. Duration histograms never use these labels.
func WithTable(ctx context.Context, namespace, table string) context.Context {
	i := active.Load()
	if i == nil {
		return ctx
	}
	key := tableKey{labelValue(namespace, "unknown"), labelValue(table, "unknown")}
	i.mu.Lock()
	t := i.tables[key]
	if t == nil {
		t = i.overflow
		if len(i.tables) < maxTables {
			t = i.newTableAttributes(key.namespace, key.table)
			i.tables[key] = t
		}
	}
	i.mu.Unlock()
	return context.WithValue(ctx, tableContextKey{}, t)
}

func attributesFor(ctx context.Context, i *instruments) *tableAttributes {
	if t, ok := ctx.Value(tableContextKey{}).(*tableAttributes); ok && t.owner == i {
		return t
	}
	return i.untabled
}

// RecordDuration records a job-level stage histogram in seconds. Unknown stages
// share "other"; negative durations are discarded. No exports or I/O occur here.
func RecordDuration(ctx context.Context, stage string, duration time.Duration) {
	i := active.Load()
	if i == nil || duration < 0 {
		return
	}
	i.duration.Record(ctx, duration.Seconds(), i.durations[stageIndex(stage)]...)
}

func stageIndex(stage string) int {
	for n, candidate := range durationStages {
		if stage == candidate {
			return n
		}
	}
	return len(durationStages) - 1
}

// RecordFailure counts a failure by bounded stage. It never accepts or exports
// an error message, SQL, or row data, and never attaches table labels.
func RecordFailure(ctx context.Context, stage string) {
	i := active.Load()
	if i == nil {
		return
	}
	i.failures.Add(ctx, 1, i.failureOptions[stageIndex(stage)]...)
}

// AddRows records positive rows/bytes independently for read, pushed, or
// committed. Unknown stages and negative values are ignored; pushed does not
// imply committed. Callers supply commit counts only after actual success.
// Positive batched calls also update the last-progress Unix timestamp. Callers
// should aggregate row work rather than invoke this once for every source row.
func AddRows(ctx context.Context, stage string, rows, bytes int64) {
	i := active.Load()
	if i == nil || (rows <= 0 && bytes <= 0) {
		return
	}
	for n, candidate := range rowStages {
		if stage == candidate {
			t := attributesFor(ctx, i)
			if rows > 0 {
				i.rows.Add(ctx, rows, t.rows[n]...)
			}
			if bytes > 0 {
				i.bytes.Add(ctx, bytes, t.rows[n]...)
			}
			t.progress[n].Store(time.Now().Unix())
			return
		}
	}
}

// AddChunks records planned/completed/failed/skipped positive totals, or signed
// active concurrency. Finishing a chunk requires a separate active -1; terminal
// totals are not implicitly inferred from transitions or buffer pushes.
func AddChunks(ctx context.Context, state string, delta int64) {
	i := active.Load()
	if i == nil || delta == 0 || (state != "active" && delta < 0) {
		return
	}
	for n, candidate := range chunkStates {
		if state == candidate {
			options := attributesFor(ctx, i).chunks[n]
			if state == "active" {
				i.activeChunks.Add(ctx, delta, options...)
			} else {
				i.chunks.Add(ctx, delta, options...)
			}
			return
		}
	}
}

// AddPendingIndex records signed changes to outstanding entries and ESTIMATED
// serialized bytes. It is not a measurement of heap/RSS and must be decremented
// when entries are committed or discarded.
func AddPendingIndex(ctx context.Context, entriesDelta, bytesDelta int64) {
	i := active.Load()
	if i == nil {
		return
	}
	t := attributesFor(ctx, i)
	if entriesDelta != 0 {
		i.pendingEntries.Add(ctx, entriesDelta, t.pending...)
	}
	if bytesDelta != 0 {
		i.pendingBytes.Add(ctx, bytesDelta, t.pending...)
	}
}

// Keep error text from OTLP URLs/headers out of normal connector logs.
func diagnosticError(err error) error {
	if err == nil {
		return nil
	}
	return errors.New("Go diagnostic metrics export/shutdown failed")
}

// RecordReaderBarrier records visibility-barrier retry attempts and elapsed
// duration with a bounded success/failure outcome; no table labels are attached.
func RecordReaderBarrier(ctx context.Context, attempts int64, duration time.Duration, success bool) {
	i := active.Load()
	if i == nil || attempts < 0 || duration < 0 {
		return
	}
	n := 0
	if success {
		n = 1
	}
	i.barrierAttempts.Record(ctx, attempts, i.barrierOutcomes[n]...)
	i.duration.Record(ctx, duration.Seconds(), i.barrierDurations[n]...)
}

// Sanitize asynchronous SDK failures too: OTLP URLs can contain query tokens.
type safeExporter struct{ sdkmetric.Exporter }

func (e safeExporter) Export(ctx context.Context, data *metricdata.ResourceMetrics) error {
	return diagnosticError(e.Exporter.Export(ctx, data))
}
