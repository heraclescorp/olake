package io.olake.iceberg;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Test;

import com.sun.net.httpserver.HttpServer;

import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.Status;
import io.olake.iceberg.RuntimeTelemetry.Stage;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;

class RuntimeTelemetryTest {
    private static final Map<String, String> JOB = Map.of(
            "OLAKE_JOB_ID", "job-test", "OLAKE_OPERATION_TYPE", "sync", "OLAKE_WORKFLOW_ID", "flow-resource-only");

    @Test
    void disabledWithoutEndpointAndInvalidConfigurationIsNonfatal() {
        try (RuntimeTelemetry disabled = RuntimeTelemetry.create(Map.of())) {
            assertTrue(disabled.flush());
            disabled.record(Stage.CONVERSION, System.nanoTime(), true);
        }
        assertDoesNotThrow(() -> {
            try (RuntimeTelemetry invalid = RuntimeTelemetry.create(Map.of(
                    "OTEL_EXPORTER_OTLP_METRICS_ENDPOINT", "not-a-url"))) {
                assertTrue(invalid.flush());
            }
        });
    }

    @Test
    void runtimeAndStageMeasurementsHaveExplicitBoundedJobAttributes() {
        InMemoryMetricReader reader = InMemoryMetricReader.create();
        SdkMeterProvider provider = SdkMeterProvider.builder().registerMetricReader(reader).build();
        try (RuntimeTelemetry telemetry = new RuntimeTelemetry(OpenTelemetrySdk.builder()
                .setMeterProvider(provider).build(), JOB)) {
            for (Stage stage : Stage.values()) {
                telemetry.record(stage, System.nanoTime() - 1_000_000, true);
                telemetry.record(stage, System.nanoTime() - 1_000_000, false);
            }
            var metrics = reader.collectAllMetrics();
            Set<String> names = metrics.stream().map(metric -> metric.getName()).collect(Collectors.toSet());
            assertTrue(names.containsAll(Set.of("olake.stage.duration", "olake.jvm.heap.used",
                    "olake.jvm.heap.committed", "olake.jvm.heap.max", "olake.jvm.nonheap.used",
                    "olake.jvm.buffer.used", "olake.jvm.gc.count", "olake.jvm.gc.time", "olake.jvm.threads")));
            for (var metric : metrics) {
                for (var point : metric.getData().getPoints()) {
                    var attributes = point.getAttributes();
                    assertEquals("job-test", attributes.get(AttributeKey.stringKey("job_id")));
                    assertEquals("sync", attributes.get(AttributeKey.stringKey("operation")));
                    assertEquals("iceberg-java", attributes.get(AttributeKey.stringKey("component")));
                    assertFalse(attributes.asMap().keySet().stream().anyMatch(key ->
                            Set.of("table", "namespace", "file", "thread_id", "workflow_id").contains(key.getKey())));
                }
            }
            var histogram = metrics.stream().filter(metric -> metric.getName().equals("olake.stage.duration"))
                    .findFirst().orElseThrow().getHistogramData();
            assertEquals(Stage.values().length * 2, histogram.getPoints().size());
            assertTrue(histogram.getPoints().stream().allMatch(point -> point.getCount() == 1 && point.getSum() >= 0));
            var heapMax = metrics.stream().filter(metric -> metric.getName().equals("olake.jvm.heap.max"))
                    .findFirst().orElseThrow().getLongGaugeData().getPoints().iterator().next().getValue();
            assertEquals(Runtime.getRuntime().maxMemory(), heapMax);
        }
        assertTrue(reader.collectAllMetrics().isEmpty());
    }

    @Test
    @SuppressWarnings("unchecked")
    void serverTimingRecordsFailedResponseWithoutStatusDescription() {
        InMemoryMetricReader reader = InMemoryMetricReader.create();
        SdkMeterProvider provider = SdkMeterProvider.builder().registerMetricReader(reader).build();
        try (RuntimeTelemetry telemetry = new RuntimeTelemetry(OpenTelemetrySdk.builder()
                .setMeterProvider(provider).build(), JOB)) {
            ServerCall<Object, Object> call = mock(ServerCall.class);
            ServerCallHandler<Object, Object> handler = (intercepted, headers) -> {
                intercepted.close(Status.INVALID_ARGUMENT.withDescription("private-request-data"), new Metadata());
                return new ServerCall.Listener<>() { };
            };
            telemetry.serverInterceptor().interceptCall(call, new Metadata(), handler);
            var point = reader.collectAllMetrics().stream().filter(metric -> metric.getName().equals("olake.stage.duration"))
                    .findFirst().orElseThrow().getHistogramData().getPoints().iterator().next();
            assertEquals("rpc", point.getAttributes().get(AttributeKey.stringKey("stage")));
            assertEquals("error", point.getAttributes().get(AttributeKey.stringKey("outcome")));
            assertFalse(point.getAttributes().toString().contains("private-request-data"));
        }
    }

    @Test
    void exportsHttpProtobufToMetricsEndpointWithHeadersAndFinalFlush() throws Exception {
        AtomicReference<byte[]> payload = new AtomicReference<>();
        AtomicReference<String> authorization = new AtomicReference<>();
        AtomicReference<String> contentType = new AtomicReference<>();
        AtomicInteger requests = new AtomicInteger();
        CountDownLatch received = new CountDownLatch(1);
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/custom/metrics", exchange -> {
            payload.set(exchange.getRequestBody().readAllBytes());
            authorization.set(exchange.getRequestHeaders().getFirst("Authorization"));
            contentType.set(exchange.getRequestHeaders().getFirst("Content-Type"));
            requests.incrementAndGet();
            exchange.sendResponseHeaders(200, -1);
            exchange.close();
            received.countDown();
        });
        server.start();
        RuntimeTelemetry telemetry = RuntimeTelemetry.create(Map.of(
                "OTEL_EXPORTER_OTLP_ENDPOINT", "http://127.0.0.1:1/unused",
                "OTEL_EXPORTER_OTLP_METRICS_ENDPOINT", "http://127.0.0.1:" + server.getAddress().getPort() + "/custom/metrics",
                "OTEL_EXPORTER_OTLP_METRICS_HEADERS", "Authorization=Bearer%20test-only",
                "OTEL_EXPORTER_OTLP_COMPRESSION", "none",
                "OLAKE_JOB_ID", "job-wire-test", "OLAKE_OPERATION_TYPE", "sync"));
        try {
            telemetry.record(Stage.UPLOAD, System.nanoTime() - 1_000_000, true);
            assertTrue(telemetry.flush());
            assertTrue(received.await(3, TimeUnit.SECONDS));
            assertEquals("Bearer test-only", authorization.get());
            assertEquals("application/x-protobuf", contentType.get());
            String wireStrings = new String(payload.get(), StandardCharsets.ISO_8859_1);
            assertTrue(wireStrings.contains("olake.stage.duration"));
            assertTrue(wireStrings.contains("olake.jvm.heap.max"));
            assertTrue(wireStrings.contains("job-wire-test"));
            assertTrue(wireStrings.contains("olake-connector-java"));
            int beforeClose = requests.get();
            telemetry.close();
            assertTrue(requests.get() > beforeClose, "shutdown must flush before exporter teardown");
            int afterClose = requests.get();
            telemetry.close();
            assertEquals(afterClose, requests.get());
        } finally {
            telemetry.close();
            server.stop(0);
        }
    }

    @Test
    void concurrentCloseWaitsForFinalExportAcknowledgement() throws Exception {
        CountDownLatch exportReceived = new CountDownLatch(1);
        CountDownLatch acknowledgeExport = new CountDownLatch(1);
        CountDownLatch secondCloseStarted = new CountDownLatch(1);
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/v1/metrics", exchange -> {
            exchange.getRequestBody().readAllBytes();
            exportReceived.countDown();
            try {
                exchange.sendResponseHeaders(acknowledgeExport.await(5, TimeUnit.SECONDS) ? 200 : 503, -1);
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                exchange.sendResponseHeaders(503, -1);
            } finally {
                exchange.close();
            }
        });
        server.start();
        var telemetry = RuntimeTelemetry.create(Map.of(
                "OTEL_EXPORTER_OTLP_ENDPOINT", "http://127.0.0.1:" + server.getAddress().getPort()));
        var callers = Executors.newFixedThreadPool(2);
        try {
            telemetry.record(Stage.COMMIT, System.nanoTime() - 1_000_000, true);
            var firstClose = callers.submit(telemetry::close);
            assertTrue(exportReceived.await(3, TimeUnit.SECONDS), "final export never reached collector");
            var secondClose = callers.submit(() -> {
                secondCloseStarted.countDown();
                telemetry.close();
            });
            assertTrue(secondCloseStarted.await(3, TimeUnit.SECONDS));
            assertThrows(TimeoutException.class, () -> secondClose.get(150, TimeUnit.MILLISECONDS),
                    "shutdown hook returned while another caller was still flushing metrics");
            acknowledgeExport.countDown();
            firstClose.get(5, TimeUnit.SECONDS);
            secondClose.get(5, TimeUnit.SECONDS);
        } finally {
            acknowledgeExport.countDown();
            callers.shutdown();
            assertTrue(callers.awaitTermination(8, TimeUnit.SECONDS));
            telemetry.close();
            server.stop(0);
        }
    }


    @Test
    void residentSetUsesLinuxKilobytesAndOmitsUnavailableValues() {
        assertEquals(42 * 1024, RuntimeTelemetry.parseResidentSetBytes("VmRSS:\t42 kB"));
        for (String value : Set.of("VmRSS: -1 kB", "VmRSS: 1 bytes", "VmRSS: 9999999999999999999999 kB", "unavailable")) {
            assertEquals(-1, RuntimeTelemetry.parseResidentSetBytes(value));
        }
    }
}
