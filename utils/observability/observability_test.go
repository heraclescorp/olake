package observability

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	collector "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	"google.golang.org/protobuf/proto"
)

func cleanEnvironment(t *testing.T) {
	t.Helper()
	for _, key := range []string{
		"OTEL_EXPORTER_OTLP_ENDPOINT", "OTEL_EXPORTER_OTLP_METRICS_ENDPOINT",
		"OTEL_EXPORTER_OTLP_HEADERS", "OTEL_EXPORTER_OTLP_METRICS_HEADERS",
		"OTEL_EXPORTER_OTLP_CERTIFICATE", "OTEL_EXPORTER_OTLP_METRICS_CERTIFICATE",
		"OTEL_EXPORTER_OTLP_CLIENT_CERTIFICATE", "OTEL_EXPORTER_OTLP_METRICS_CLIENT_CERTIFICATE",
		"OTEL_EXPORTER_OTLP_CLIENT_KEY", "OTEL_EXPORTER_OTLP_METRICS_CLIENT_KEY",
		"OTEL_EXPORTER_OTLP_COMPRESSION", "OTEL_EXPORTER_OTLP_METRICS_COMPRESSION",
		"OTEL_EXPORTER_OTLP_TIMEOUT", "OTEL_EXPORTER_OTLP_METRICS_TIMEOUT",
		"OTEL_RESOURCE_ATTRIBUTES", "OTEL_SERVICE_NAME", "OTEL_SDK_DISABLED", "OTEL_METRICS_EXPORTER",
		"OTEL_METRIC_EXPORT_INTERVAL", "OTEL_METRIC_EXPORT_TIMEOUT",
		"OLAKE_JOB_ID", "OLAKE_OPERATION_TYPE", "OLAKE_WORKFLOW_ID",
		"OLAKE_PROFILE_DIR", "OLAKE_CPU_PROFILE_SECONDS", "OLAKE_JAVA_HEAP_PERCENT",
		"POD_NAME", "POD_NAMESPACE", "POD_UID", "NODE_NAME",
		"SERVICE_NAMESPACE", "GIT_HASH",
	} {
		t.Setenv(key, "")
	}
}

func testInstruments(t *testing.T) (*sdkmetric.ManualReader, *instruments) {
	t.Helper()
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader), sdkmetric.WithCardinalityLimit((maxTables+2)*len(chunkStates)))
	i, err := newInstruments(provider.Meter("test"), []attribute.KeyValue{
		attribute.String("job_id", "job-test"), attribute.String("component", "go"), attribute.String("operation", "sync"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if !active.CompareAndSwap(nil, i) {
		t.Fatal("unexpected active metrics")
	}
	t.Cleanup(func() {
		active.CompareAndSwap(i, nil)
		if err := provider.Shutdown(context.Background()); err != nil {
			t.Error(err)
		}
	})
	return reader, i
}

func collectedMetric(t *testing.T, reader *sdkmetric.ManualReader, name string) metricdata.Metrics {
	t.Helper()
	var data metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &data); err != nil {
		t.Fatal(err)
	}
	for _, scope := range data.ScopeMetrics {
		for _, metric := range scope.Metrics {
			if metric.Name == name {
				return metric
			}
		}
	}
	t.Fatalf("metric %s missing", name)
	return metricdata.Metrics{}
}

func attr(t *testing.T, attrs attribute.Set, key string) string {
	t.Helper()
	value, ok := attrs.Value(attribute.Key(key))
	if !ok {
		t.Fatalf("attribute %q missing from %v", key, attrs.ToSlice())
	}
	return value.AsString()
}

func TestDisabledIsNoop(t *testing.T) {
	cleanEnvironment(t)
	ctx := context.Background()
	shutdown, err := Start(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if Enabled() {
		t.Fatal("metrics enabled without endpoint")
	}
	if WithTable(ctx, "public", "table") != ctx {
		t.Fatal("disabled WithTable allocated a context")
	}
	if allocations := testing.AllocsPerRun(100, func() {
		RecordDuration(ctx, "read", time.Second)
		RecordFailure(ctx, "read")
		AddRows(ctx, "read", 1, 10)
		AddChunks(ctx, "active", 1)
		AddPendingIndex(ctx, 1, 10)
		RecordReaderBarrier(ctx, 2, time.Second, true)
	}); allocations != 0 {
		t.Fatalf("disabled metrics allocated: %v", allocations)
	}
	if err := shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	if err := shutdown(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestDisabledSwitchesOverrideConfiguredEndpoint(t *testing.T) {
	for _, key := range []string{"OTEL_SDK_DISABLED", "OTEL_METRICS_EXPORTER"} {
		t.Run(key, func(t *testing.T) {
			cleanEnvironment(t)
			t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://127.0.0.1:1")
			value := "true"
			if key == "OTEL_METRICS_EXPORTER" {
				value = "none"
			}
			t.Setenv(key, value)
			shutdown, err := Start(context.Background())
			if err != nil || Enabled() {
				t.Fatalf("%s did not disable metrics: %v", key, err)
			}
			if err := shutdown(context.Background()); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestMalformedEndpointFailsOnlyDiagnostics(t *testing.T) {
	cleanEnvironment(t)
	for _, endpoint := range []string{"invalid", "grpc://localhost:4317", "http://user:secret@localhost:4318"} {
		t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", endpoint)
		shutdown, err := Start(context.Background())
		if err == nil {
			t.Fatalf("accepted endpoint %q", endpoint)
		}
		if strings.Contains(err.Error(), "secret") {
			t.Fatal("error leaked endpoint credentials")
		}
		if Enabled() {
			t.Fatal("invalid endpoint enabled metrics")
		}
		if err := shutdown(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
}

func TestRowsSeparateReadPushCommit(t *testing.T) {
	reader, _ := testInstruments(t)
	ctx := WithTable(context.Background(), "public", "events")
	AddRows(ctx, "read", 5, 50)
	AddRows(ctx, "pushed", 4, 40)
	AddRows(ctx, "committed", 3, 30)
	AddRows(ctx, "read", -1, -10)
	AddRows(ctx, "unknown", 100, 100)
	for name, expected := range map[string]map[string]int64{
		"olake.rows":  {"read": 5, "pushed": 4, "committed": 3},
		"olake.bytes": {"read": 50, "pushed": 40, "committed": 30},
	} {
		sum, ok := collectedMetric(t, reader, name).Data.(metricdata.Sum[int64])
		if !ok || !sum.IsMonotonic || len(sum.DataPoints) != 3 {
			t.Fatalf("unexpected %s data: %+v", name, sum)
		}
		for _, point := range sum.DataPoints {
			if attr(t, point.Attributes, "job_id") != "job-test" || attr(t, point.Attributes, "component") != "go" || attr(t, point.Attributes, "operation") != "sync" {
				t.Fatal("stable datapoint labels missing")
			}
			if attr(t, point.Attributes, "namespace") != "public" || attr(t, point.Attributes, "table") != "events" {
				t.Fatal("table labels missing")
			}
			if want := expected[attr(t, point.Attributes, "stage")]; point.Value != want {
				t.Fatalf("%s value=%d want=%d", name, point.Value, want)
			}
		}
	}
	for range 2 {
		progress := collectedMetric(t, reader, "olake.last_progress.time").Data.(metricdata.Gauge[int64])
		if len(progress.DataPoints) != 3 {
			t.Fatalf("last-progress points=%d want=3", len(progress.DataPoints))
		}
		for _, point := range progress.DataPoints {
			if point.Value <= 0 || point.Value > time.Now().Unix() {
				t.Fatalf("invalid last-progress timestamp=%d", point.Value)
			}
			if attr(t, point.Attributes, "table") != "events" {
				t.Fatal("last-progress table missing")
			}
		}
	}
}

func TestPendingAndChunkStateSemantics(t *testing.T) {
	reader, _ := testInstruments(t)
	ctx := WithTable(context.Background(), "public", "events")
	AddPendingIndex(ctx, 5, 500)
	AddPendingIndex(ctx, -3, -300)
	for name, expected := range map[string]int64{"olake.index.pending.entries": 2, "olake.index.pending.estimated_bytes": 200} {
		sum := collectedMetric(t, reader, name).Data.(metricdata.Sum[int64])
		if sum.IsMonotonic || len(sum.DataPoints) != 1 || sum.DataPoints[0].Value != expected {
			t.Fatalf("unexpected %s data: %+v", name, sum)
		}
	}
	AddChunks(ctx, "planned", 2)
	AddChunks(ctx, "active", 2)
	AddChunks(ctx, "active", -1)
	AddChunks(ctx, "completed", 1)
	AddChunks(ctx, "completed", -1)
	AddChunks(ctx, "skipped", 1)
	AddChunks(ctx, "not-a-state", 1)
	sum := collectedMetric(t, reader, "olake.chunks").Data.(metricdata.Sum[int64])
	if !sum.IsMonotonic || len(sum.DataPoints) != 3 {
		t.Fatalf("unexpected chunks: %+v", sum)
	}
	for _, point := range sum.DataPoints {
		want := int64(1)
		if attr(t, point.Attributes, "state") == "planned" {
			want = 2
		}
		if point.Value != want {
			t.Fatalf("chunk value=%d want=%d", point.Value, want)
		}
	}
	active := collectedMetric(t, reader, "olake.chunks.active").Data.(metricdata.Sum[int64])
	if active.IsMonotonic || len(active.DataPoints) != 1 || active.DataPoints[0].Value != 1 {
		t.Fatalf("unexpected active chunks: %+v", active)
	}
}

func TestDurationAndBarrierLabelsRemainJobLevel(t *testing.T) {
	reader, _ := testInstruments(t)
	ctx := WithTable(context.Background(), "private", "events")
	RecordDuration(ctx, "rpc", time.Second)
	RecordDuration(ctx, "unbounded-input", 2*time.Second)
	RecordDuration(ctx, "rpc", -time.Second)
	RecordReaderBarrier(ctx, 3, 3*time.Second, true)
	RecordReaderBarrier(ctx, 2, 2*time.Second, false)
	RecordReaderBarrier(ctx, -1, time.Second, true)
	histogram := collectedMetric(t, reader, "olake.stage.duration").Data.(metricdata.Histogram[float64])
	if len(histogram.DataPoints) != 4 {
		t.Fatalf("unexpected histogram points: %+v", histogram.DataPoints)
	}
	for _, point := range histogram.DataPoints {
		if point.Count != 1 {
			t.Fatalf("unexpected duration count: %d", point.Count)
		}
		if _, ok := point.Attributes.Value("table"); ok {
			t.Fatal("duration has table label")
		}
		if _, ok := point.Attributes.Value("namespace"); ok {
			t.Fatal("duration has namespace label")
		}
		if attr(t, point.Attributes, "stage") == "reader_barrier" {
			outcome := attr(t, point.Attributes, "outcome")
			if outcome != "success" && outcome != "failure" {
				t.Fatalf("invalid outcome %s", outcome)
			}
		}
	}
	attempts := collectedMetric(t, reader, "olake.reader_barrier.attempts").Data.(metricdata.Histogram[int64])
	if len(attempts.DataPoints) != 2 {
		t.Fatalf("unexpected barrier attempts: %+v", attempts)
	}
	for _, point := range attempts.DataPoints {
		want := int64(2)
		if attr(t, point.Attributes, "outcome") == "success" {
			want = 3
		}
		if point.Sum != want {
			t.Fatalf("attempt sum=%d want=%d", point.Sum, want)
		}
	}
}

func TestTableAttributesAreCachedAndBounded(t *testing.T) {
	reader, inst := testInstruments(t)
	ctx := context.Background()
	first := WithTable(ctx, "public", "events").Value(tableContextKey{})
	second := WithTable(ctx, "public", "events").Value(tableContextKey{})
	if first != second {
		t.Fatal("table attribute sets were not cached")
	}
	for n := range maxTables + 30 {
		AddRows(WithTable(ctx, "public", fmt.Sprint(n)), "read", 1, 1)
	}
	if len(inst.tables) != maxTables {
		t.Fatalf("table cache=%d want=%d", len(inst.tables), maxTables)
	}
	overflowA := WithTable(ctx, "new", "a").Value(tableContextKey{})
	overflowB := WithTable(ctx, "new", "b").Value(tableContextKey{})
	if overflowA != inst.overflow || overflowA != overflowB {
		t.Fatal("overflow sets were not reused")
	}
	sum := collectedMetric(t, reader, "olake.rows").Data.(metricdata.Sum[int64])
	if len(sum.DataPoints) > maxTables+1 {
		t.Fatalf("unbounded row series=%d", len(sum.DataPoints))
	}
	foundOverflow := false
	for _, point := range sum.DataPoints {
		if attr(t, point.Attributes, "table") == "__overflow__" {
			foundOverflow = true
			if point.Value <= 1 {
				t.Fatal("overflow rows were not aggregated")
			}
		}
	}
	if !foundOverflow {
		t.Fatal("explicit table overflow series missing")
	}
}

func TestFailureLabelsAreBoundedAndJobLevel(t *testing.T) {
	reader, _ := testInstruments(t)
	ctx := WithTable(context.Background(), "private", "events")
	RecordFailure(ctx, "write")
	RecordFailure(ctx, "rpc")
	RecordFailure(ctx, "raw error with private data")
	sum := collectedMetric(t, reader, "olake.stage.failures").Data.(metricdata.Sum[int64])
	if !sum.IsMonotonic || len(sum.DataPoints) != 3 {
		t.Fatalf("unexpected failures=%+v", sum)
	}
	for _, point := range sum.DataPoints {
		if _, ok := point.Attributes.Value("table"); ok {
			t.Fatal("failure has table label")
		}
		stage := attr(t, point.Attributes, "stage")
		if stage != "write" && stage != "rpc" && stage != "other" {
			t.Fatalf("unbounded failure stage=%s", stage)
		}
		if point.Value != 1 {
			t.Fatalf("failure count=%d", point.Value)
		}
	}
}

func TestRuntimeMetricsHaveStableAttributes(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	defer provider.Shutdown(context.Background())
	if err := registerRuntime(provider.Meter("runtime-test"), []attribute.KeyValue{
		attribute.String("job_id", "job-test"), attribute.String("component", "go"), attribute.String("operation", "sync"),
	}); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"olake.go.heap.live", "olake.go.heap.objects", "olake.go.goroutines", "olake.go.memory.limit"} {
		data := collectedMetric(t, reader, name).Data.(metricdata.Gauge[int64])
		if len(data.DataPoints) != 1 {
			t.Fatalf("unexpected %s gauge: %+v", name, data)
		}
		if attr(t, data.DataPoints[0].Attributes, "job_id") != "job-test" {
			t.Fatal("runtime labels missing")
		}
	}
	for _, name := range []string{"olake.go.heap.allocations", "olake.go.gc.cycles", "olake.go.gc.pause.bucket"} {
		data := collectedMetric(t, reader, name).Data.(metricdata.Sum[int64])
		if !data.IsMonotonic || len(data.DataPoints) == 0 {
			t.Fatalf("unexpected %s counter: %+v", name, data)
		}
	}
}

func TestHTTPProtobufExportAndShutdownAfterCancellation(t *testing.T) {
	cleanEnvironment(t)
	requests := make(chan *collector.ExportMetricsServiceRequest, 4)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/custom/metrics" || r.Header.Get("Content-Type") != "application/x-protobuf" || r.Header.Get("test-header") != "test-value" {
			t.Errorf("unexpected OTLP request path/headers")
		}
		body, err := io.ReadAll(io.LimitReader(r.Body, 4<<20))
		if err != nil {
			t.Error(err)
			w.WriteHeader(500)
			return
		}
		request := new(collector.ExportMetricsServiceRequest)
		if err := proto.Unmarshal(body, request); err != nil {
			t.Error(err)
			w.WriteHeader(400)
			return
		}
		requests <- request
		w.Header().Set("Content-Type", "application/x-protobuf")
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://127.0.0.1:1/ignored")
	t.Setenv("OTEL_EXPORTER_OTLP_METRICS_ENDPOINT", server.URL+"/custom/metrics")
	t.Setenv("OTEL_EXPORTER_OTLP_METRICS_HEADERS", "test-header=test-value")
	t.Setenv("OLAKE_JOB_ID", "job-http")
	t.Setenv("OLAKE_OPERATION_TYPE", "sync")
	t.Setenv("OLAKE_WORKFLOW_ID", "workflow-resource-only")
	t.Setenv("POD_NAME", "pod-test")
	ctx, cancel := context.WithCancel(context.Background())
	shutdown, err := Start(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = shutdown(context.Background()) })
	if !Enabled() {
		t.Fatal("metrics not enabled")
	}
	AddRows(WithTable(ctx, "public", "events"), "committed", 7, 70)
	cancel()
	flushCtx, stop := context.WithTimeout(context.Background(), 5*time.Second)
	defer stop()
	if err := shutdown(flushCtx); err != nil {
		t.Fatal(err)
	}
	if Enabled() {
		t.Fatal("metrics still enabled after shutdown")
	}
	if err := shutdown(flushCtx); err != nil {
		t.Fatal(err)
	}
	select {
	case request := <-requests:
		foundRows := false
		for _, resource := range request.ResourceMetrics {
			resourceAttrs := make(map[string]string)
			for _, value := range resource.Resource.Attributes {
				resourceAttrs[value.Key] = value.Value.GetStringValue()
			}
			if resourceAttrs["service.name"] != "olake-connector-go" || resourceAttrs["workflow_id"] != "workflow-resource-only" || resourceAttrs["k8s.pod.name"] != "pod-test" {
				t.Fatalf("unexpected resource attrs: %v", resourceAttrs)
			}
			for _, scope := range resource.ScopeMetrics {
				for _, metric := range scope.Metrics {
					if metric.Name != "olake.rows" {
						continue
					}
					for _, point := range metric.GetSum().DataPoints {
						labels := make(map[string]string)
						for _, value := range point.Attributes {
							labels[value.Key] = value.Value.GetStringValue()
						}
						if labels["job_id"] != "job-http" || labels["component"] != "go" || labels["operation"] != "sync" {
							t.Fatalf("explicit AMP labels missing: %v", labels)
						}
						if _, ok := labels["workflow_id"]; ok {
							t.Fatal("workflow became a metric label")
						}
						if point.GetAsInt() != 7 || labels["stage"] != "committed" {
							t.Fatalf("unexpected committed rows: %+v", point)
						}
						foundRows = true
					}
				}
			}
		}
		if !foundRows {
			t.Fatal("OTLP payload missing committed rows")
		}
	default:
		t.Fatal("shutdown did not export OTLP payload")
	}
}

func TestHTTPProtobufResourceDefaultsAndOverrides(t *testing.T) {
	host, err := os.Hostname()
	if err != nil || host == "" {
		host = "localhost"
	}
	instance := host + "-" + strconv.Itoa(os.Getpid())
	for _, test := range []struct {
		name string
		env  map[string]string
		want map[string]string
	}{
		{"local defaults", nil, map[string]string{"service.name": "olake-connector-go", "service.namespace": "localhost", "service.instance.id": instance}},
		{"Aven defaults", map[string]string{"SERVICE_NAMESPACE": "aven", "POD_NAMESPACE": "pods", "POD_NAME": "worker", "GIT_HASH": "revision"}, map[string]string{"service.namespace": "aven", "service.version": "revision", "service.instance.id": "worker-" + strconv.Itoa(os.Getpid())}},
		{"pod namespace fallback", map[string]string{"POD_NAMESPACE": "pods"}, map[string]string{"service.namespace": "pods"}},
		{"resource version without git hash", map[string]string{"OTEL_RESOURCE_ATTRIBUTES": "service.version=operator-version"}, map[string]string{"service.version": "operator-version"}},
		{"operator resource overrides", map[string]string{"SERVICE_NAMESPACE": "aven", "GIT_HASH": "revision", "POD_NAME": "worker", "OTEL_RESOURCE_ATTRIBUTES": "service.name=operator,service.namespace=operator-ns,service.version=operator-version,service.instance.id=operator-instance"}, map[string]string{"service.name": "operator", "service.namespace": "operator-ns", "service.version": "operator-version", "service.instance.id": "operator-instance"}},
		{"standard service name precedence", map[string]string{"OTEL_SERVICE_NAME": "explicit", "OTEL_RESOURCE_ATTRIBUTES": "service.name=resource-name"}, map[string]string{"service.name": "explicit"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			cleanEnvironment(t)
			requests := make(chan *collector.ExportMetricsServiceRequest, 4)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				body, err := io.ReadAll(io.LimitReader(r.Body, 4<<20))
				if err != nil {
					t.Error(err)
					w.WriteHeader(http.StatusBadRequest)
					return
				}
				request := new(collector.ExportMetricsServiceRequest)
				if err := proto.Unmarshal(body, request); err != nil {
					t.Error(err)
					w.WriteHeader(http.StatusBadRequest)
					return
				}
				requests <- request
				w.Header().Set("Content-Type", "application/x-protobuf")
			}))
			defer server.Close()
			t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", server.URL)
			for key, value := range test.env {
				t.Setenv(key, value)
			}
			provider, _, err := startMetrics(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := provider.Shutdown(ctx); err != nil {
				t.Fatal(err)
			}
			select {
			case request := <-requests:
				if len(request.ResourceMetrics) != 1 {
					t.Fatalf("resource count=%d want=1", len(request.ResourceMetrics))
				}
				attrs := make(map[string]string)
				for _, value := range request.ResourceMetrics[0].Resource.Attributes {
					attrs[value.Key] = value.Value.GetStringValue()
				}
				for key, want := range test.want {
					if attrs[key] != want {
						t.Errorf("resource %s=%q want=%q", key, attrs[key], want)
					}
				}
				if test.env["GIT_HASH"] == "" && test.env["OTEL_RESOURCE_ATTRIBUTES"] == "" {
					if _, ok := attrs["service.version"]; ok {
						t.Error("missing GIT_HASH produced a service version")
					}
				}
			default:
				t.Fatal("shutdown did not export a resource")
			}
		})
	}
}

func TestUnavailableExporterDoesNotBreakRecording(t *testing.T) {
	cleanEnvironment(t)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusBadRequest) }))
	defer server.Close()
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", server.URL)
	shutdown, err := Start(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	AddRows(context.Background(), "read", 1, 1)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := shutdown(ctx); err == nil {
		t.Fatal("missing diagnostic shutdown error")
	}
	if Enabled() {
		t.Fatal("failed shutdown left metrics enabled")
	}
}
