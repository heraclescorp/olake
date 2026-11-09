package io.olake.iceberg;

import java.io.BufferedReader;
import java.io.IOException;
import java.lang.management.BufferPoolMXBean;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.grpc.ForwardingServerCall;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.DoubleHistogram;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.api.metrics.ObservableLongMeasurement;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.autoconfigure.AutoConfiguredOpenTelemetrySdk;
import io.opentelemetry.sdk.resources.Resource;

/** Job-scoped, bounded metrics only: no request bodies, table names, paths or spans. */
public final class RuntimeTelemetry implements AutoCloseable {
    private static final Logger LOGGER = LoggerFactory.getLogger(RuntimeTelemetry.class);
    private static final RuntimeTelemetry NOOP = new RuntimeTelemetry();
    private static volatile RuntimeTelemetry current = NOOP;
    private static final List<Double> DURATION_BUCKETS = List.of(
            0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 300.0);

    public enum Stage {
        CONVERSION("conversion"), PARQUET_WRITE("parquet_write"), FLUSH("flush"),
        UPLOAD("upload"), COMMIT("commit"), RPC("rpc");

        final String label;

        Stage(String label) {
            this.label = label;
        }
    }

    private final OpenTelemetrySdk sdk;
    private final DoubleHistogram duration;
    private final EnumMap<Stage, Attributes[]> stageAttributes = new EnumMap<>(Stage.class);
    private final AtomicBoolean closed = new AtomicBoolean();
    private final AtomicBoolean warned = new AtomicBoolean();

    private RuntimeTelemetry() {
        sdk = null;
        duration = null;
    }

    RuntimeTelemetry(OpenTelemetrySdk sdk, Map<String, String> env) {
        this.sdk = sdk;
        Attributes attributes = jobAttributes(env);
        Meter meter = sdk.getMeter("io.olake.iceberg");
        duration = meter.histogramBuilder("olake.stage.duration")
                .setDescription("JVM writer/server stage elapsed time, including failed attempts")
                .setUnit("s").setExplicitBucketBoundariesAdvice(DURATION_BUCKETS).build();
        for (Stage stage : Stage.values()) {
            stageAttributes.put(stage, new Attributes[] {
                    attributes.toBuilder().put("stage", stage.label).put("outcome", "error").build(),
                    attributes.toBuilder().put("stage", stage.label).put("outcome", "success").build()
            });
        }
        registerRuntime(meter, attributes);
    }

    public static synchronized RuntimeTelemetry start() {
        if (current == NOOP) {
            current = create(System.getenv());
        }
        return current;
    }

    static RuntimeTelemetry create(Map<String, String> env) {
        if (Boolean.parseBoolean(env.getOrDefault("OTEL_SDK_DISABLED", "false"))
                || env.getOrDefault("OTEL_METRICS_EXPORTER", "").equalsIgnoreCase("none")) {
            return NOOP;
        }
        if (env.getOrDefault("OTEL_EXPORTER_OTLP_ENDPOINT", "").isBlank()
                && env.getOrDefault("OTEL_EXPORTER_OTLP_METRICS_ENDPOINT", "").isBlank()) {
            return NOOP;
        }
        OpenTelemetrySdk sdk = null;
        try {
            sdk = AutoConfiguredOpenTelemetrySdk.builder().disableShutdownHook()
                    .addPropertiesCustomizer(config -> configuration(env))
                    .addResourceCustomizer((resource, config) -> resource.merge(Resource.create(
                            resourceAttributes(env))))
                    .build().getOpenTelemetrySdk();
            return new RuntimeTelemetry(sdk, env);
        } catch (RuntimeException | LinkageError error) {
            // Do not log exception messages: configuration errors may contain OTLP credentials.
            LOGGER.warn("Java metrics initialization disabled ({})", error.getClass().getSimpleName());
            if (sdk != null) {
                try {
                    sdk.getSdkMeterProvider().shutdown().join(2, TimeUnit.SECONDS);
                } catch (RuntimeException | LinkageError ignored) {
                    // Diagnostics initialization and cleanup must never fail ingestion.
                }
            }
            return NOOP;
        }
    }

    static Map<String, String> configuration(Map<String, String> env) {
        Map<String, String> settings = new HashMap<>();
        env.forEach((key, value) -> {
            if (key.startsWith("OTEL_")) {
                settings.put(key.toLowerCase(Locale.ROOT).replace('_', '.'), value);
            }
        });
        // Metrics only. One periodic reader has one export in flight; all dimensions
        // are finite, with an additional SDK cardinality cap per instrument.
        settings.put("otel.traces.exporter", "none");
        settings.put("otel.logs.exporter", "none");
        settings.putIfAbsent("otel.metrics.exporter", "otlp");
        settings.put("otel.exporter.otlp.metrics.protocol", "http/protobuf");
        settings.put("otel.exporter.otlp.metrics.timeout", "2000");
        settings.put("otel.metric.export.interval", "15000");
        settings.put("otel.java.metrics.cardinality.limit", "128");
        settings.put("otel.metrics.exemplar.filter", "always_off");
        settings.put("otel.service.name", "olake-connector-java");
        return settings;
    }

    static Attributes jobAttributes(Map<String, String> env) {
        return Attributes.builder().put("job_id", metadata(env, "OLAKE_JOB_ID"))
                .put("operation", metadata(env, "OLAKE_OPERATION_TYPE"))
                .put("component", "iceberg-java").build();
    }

    private static Attributes resourceAttributes(Map<String, String> env) {
        var attributes = jobAttributes(env).toBuilder().put("service.name", "olake-connector-java");
        Map.of("OLAKE_WORKFLOW_ID", "workflow_id", "POD_NAME", "k8s.pod.name",
                "POD_NAMESPACE", "k8s.namespace.name", "NODE_NAME", "k8s.node.name")
                .forEach((key, name) -> {
                    if (!env.getOrDefault(key, "").isBlank()) {
                        attributes.put(name, env.get(key));
                    }
                });
        return attributes.build();
    }

    private static String metadata(Map<String, String> env, String key) {
        String value = env.getOrDefault(key, "");
        return value.isBlank() ? "unknown" : value;
    }

    public static long startTimer() {
        return current == NOOP ? 0 : System.nanoTime();
    }

    public static void recordDuration(Stage stage, long started, boolean success) {
        current.record(stage, started, success);
    }

    void record(Stage stage, long started, boolean success) {
        if (started == 0 || duration == null || closed.get()) {
            return;
        }
        try {
            duration.record(Math.max(0, System.nanoTime() - started) / 1_000_000_000.0,
                    stageAttributes.get(stage)[success ? 1 : 0]);
        } catch (RuntimeException | LinkageError error) {
            if (warned.compareAndSet(false, true)) {
                LOGGER.warn("Java metrics recording disabled for an attempt ({})", error.getClass().getSimpleName());
            }
        }
    }

    /** Server-side end-to-end time through response close, including failed RPCs. */
    public ServerInterceptor serverInterceptor() {
        return new ServerInterceptor() {
            @Override
            public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(ServerCall<ReqT, RespT> call,
                    Metadata headers, ServerCallHandler<ReqT, RespT> next) {
                if (sdk == null) {
                    return next.startCall(call, headers);
                }
                long started = System.nanoTime();
                return next.startCall(new ForwardingServerCall.SimpleForwardingServerCall<>(call) {
                    @Override
                    public void close(Status status, Metadata trailers) {
                        try {
                            super.close(status, trailers);
                        } finally {
                            record(Stage.RPC, started, status.isOk());
                        }
                    }
                }, headers);
            }
        };
    }

    private static void registerRuntime(Meter meter, Attributes attributes) {
        MemoryMXBean memory = ManagementFactory.getMemoryMXBean();
        ObservableLongMeasurement heapUsed = gauge(meter, "olake.jvm.heap.used", "By");
        ObservableLongMeasurement heapCommitted = gauge(meter, "olake.jvm.heap.committed", "By");
        ObservableLongMeasurement heapMax = gauge(meter, "olake.jvm.heap.max", "By");
        ObservableLongMeasurement nonheapUsed = gauge(meter, "olake.jvm.nonheap.used", "By");
        ObservableLongMeasurement nonheapCommitted = gauge(meter, "olake.jvm.nonheap.committed", "By");
        meter.batchCallback(() -> {
            MemoryUsage heap = memory.getHeapMemoryUsage();
            MemoryUsage nonheap = memory.getNonHeapMemoryUsage();
            heapUsed.record(heap.getUsed(), attributes);
            heapCommitted.record(heap.getCommitted(), attributes);
            heapMax.record(Runtime.getRuntime().maxMemory(), attributes);
            nonheapUsed.record(nonheap.getUsed(), attributes);
            nonheapCommitted.record(nonheap.getCommitted(), attributes);
        }, heapUsed, heapCommitted, heapMax, nonheapUsed, nonheapCommitted);
        meter.gaugeBuilder("olake.jvm.threads").ofLongs().setUnit("{thread}")
                .buildWithCallback(value -> value.record(ManagementFactory.getThreadMXBean().getThreadCount(), attributes));
        List<BufferPoolMXBean> pools = ManagementFactory.getPlatformMXBeans(BufferPoolMXBean.class);
        Map<String, Attributes> poolAttributes = new HashMap<>();
        for (BufferPoolMXBean pool : pools) {
            poolAttributes.put(pool.getName(), attributes.toBuilder().put("pool", pool.getName()).build());
        }
        meter.gaugeBuilder("olake.jvm.buffer.used").ofLongs().setUnit("By").buildWithCallback(value -> {
            for (BufferPoolMXBean pool : pools) {
                if (pool.getMemoryUsed() >= 0) {
                    value.record(pool.getMemoryUsed(), poolAttributes.get(pool.getName()));
                }
            }
        });
        meter.gaugeBuilder("olake.jvm.buffer.count").ofLongs().setUnit("{buffer}").buildWithCallback(value -> {
            for (BufferPoolMXBean pool : pools) {
                value.record(pool.getCount(), poolAttributes.get(pool.getName()));
            }
        });
        List<GarbageCollectorMXBean> collectors = ManagementFactory.getGarbageCollectorMXBeans();
        Map<String, Attributes> gcAttributes = new HashMap<>();
        for (GarbageCollectorMXBean collector : collectors) {
            gcAttributes.put(collector.getName(), attributes.toBuilder().put("gc", collector.getName()).build());
        }
        meter.counterBuilder("olake.jvm.gc.count").setUnit("{collection}").buildWithCallback(value -> {
            for (GarbageCollectorMXBean collector : collectors) {
                if (collector.getCollectionCount() >= 0) {
                    value.record(collector.getCollectionCount(), gcAttributes.get(collector.getName()));
                }
            }
        });
        meter.counterBuilder("olake.jvm.gc.time").ofDoubles().setUnit("s").buildWithCallback(value -> {
            for (GarbageCollectorMXBean collector : collectors) {
                if (collector.getCollectionTime() >= 0) {
                    value.record(collector.getCollectionTime() / 1000.0, gcAttributes.get(collector.getName()));
                }
            }
        });
        if (ManagementFactory.getOperatingSystemMXBean() instanceof com.sun.management.OperatingSystemMXBean os) {
            meter.counterBuilder("olake.process.cpu.time").ofDoubles().setUnit("s")
                    .buildWithCallback(value -> {
                        long nanos = os.getProcessCpuTime();
                        if (nanos >= 0) {
                            value.record(nanos / 1_000_000_000.0, attributes);
                        }
                    });
        }
        if (Files.isReadable(Path.of("/proc/self/status"))) {
            meter.gaugeBuilder("olake.process.memory.rss").ofLongs().setUnit("By")
                    .buildWithCallback(value -> {
                        long bytes = residentSetBytes();
                        if (bytes >= 0) {
                            value.record(bytes, attributes);
                        }
                    });
        }
    }

    private static ObservableLongMeasurement gauge(Meter meter, String name, String unit) {
        return meter.gaugeBuilder(name).ofLongs().setUnit(unit).buildObserver();
    }

    static long residentSetBytes() {
        try (BufferedReader reader = Files.newBufferedReader(Path.of("/proc/self/status"))) {
            for (String line; (line = reader.readLine()) != null;) {
                if (line.startsWith("VmRSS:")) {
                    return parseResidentSetBytes(line);
                }
            }
        } catch (IOException | SecurityException ignored) {
            // This metric is optional outside Linux or under a restricted procfs.
        }
        return -1;
    }

    static long parseResidentSetBytes(String line) {
        try {
            String[] fields = line.trim().split("\\s+");
            if (fields.length == 3 && fields[0].equals("VmRSS:") && fields[2].equals("kB")) {
                long value = Long.parseLong(fields[1]);
                return value < 0 ? -1 : Math.multiplyExact(value, 1024);
            }
        } catch (NumberFormatException | ArithmeticException ignored) {
            // Malformed/unavailable procfs values are omitted, never reported as zero.
        }
        return -1;
    }

    boolean flush() {
        return sdk == null || sdk.getSdkMeterProvider().forceFlush().join(2, TimeUnit.SECONDS).isSuccess();
    }

    // The main thread and shutdown hook must both wait for an in-progress final flush.
    @Override
    public synchronized void close() {
        if (sdk == null || !closed.compareAndSet(false, true)) {
            return;
        }
        if (current == this) {
            current = NOOP;
        }
        try {
            if (!flush()) {
                LOGGER.warn("Java metrics final flush did not complete within 2s");
            }
            sdk.getSdkMeterProvider().shutdown().join(2, TimeUnit.SECONDS);
        } catch (RuntimeException | LinkageError error) {
            LOGGER.warn("Java metrics shutdown incomplete ({})", error.getClass().getSimpleName());
        }
    }
}
