package io.olake.iceberg;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.catalog.Catalog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.olake.iceberg.rpc.OlakeArrowIngester;
import io.olake.iceberg.rpc.OlakeFailures;
import io.olake.iceberg.rpc.OlakeTableIndexer;
import io.olake.iceberg.rpc.OlakeRowsIngester;
import io.olake.iceberg.rpc.IcebergSession;
import io.grpc.Server;
import io.grpc.ServerBuilder;

/**
 * Shared-JVM entry point. Catalog config is parsed once at startup; per-stream
 * context (namespace, upsert, partition-fields, identifier-fields) is now carried
 * on every gRPC request, so a single JVM can serve all streams and chunks of an
 * OLake sync.
 */
public class OlakeRpcServer {

    private static final Logger LOGGER = LoggerFactory.getLogger(OlakeRpcServer.class);
    final static Configuration hadoopConf = new Configuration();
    final static Map<String, String> icebergProperties = new ConcurrentHashMap<>();
    static Catalog icebergCatalog;

    public static void main(String[] args) throws InterruptedException {
        RuntimeTelemetry telemetry = RuntimeTelemetry.start();
        RuntimeRecording recording = RuntimeRecording.start();
        LOGGER.info("JVM effective memory: max_heap_bytes={} committed_heap_bytes={} available_processors={}",
                Runtime.getRuntime().maxMemory(), Runtime.getRuntime().totalMemory(),
                Runtime.getRuntime().availableProcessors());
        Server server;
        try {
            CatalogHttpLogging.install();
            if (args.length < 1) {
                LOGGER.error("Please provide a JSON config as an argument.");
                System.exit(1);
            }

            String jsonConfig = args[0];
            ObjectMapper objectMapper = new ObjectMapper();
            Map<String, Object> configMap = objectMapper.readValue(jsonConfig,
                    new TypeReference<Map<String, Object>>() {
                    });
            LOGGER.info("Logs will be output to console only");

            // Only catalog/storage-level config is consumed here. Stream-level context
            // (namespace, upsert, partition-fields, identifier-fields) comes per-request.
            Map<String, String> stringConfigMap = new ConcurrentHashMap<>();
            configMap.forEach((key, value) -> {
                if (value != null) {
                    stringConfigMap.put(key, value.toString());
                }
            });

            configureResyncCatalog(stringConfigMap);
            stringConfigMap.forEach(hadoopConf::set);
            icebergProperties.putAll(stringConfigMap);

            String catalogName = stringConfigMap.getOrDefault("catalog-name", "iceberg");

            icebergCatalog = CatalogUtil.buildIcebergCatalog(catalogName, icebergProperties, hadoopConf);

            boolean arrowWriterEnabled = Boolean.parseBoolean(
                stringConfigMap.getOrDefault("arrow-writer-enabled", "false"));

            int port = Integer.parseInt(stringConfigMap.getOrDefault("port", "50051"));
            // Set the gRPC message size to the maximum value supported by an int (2 GB - 1),
            // while the writer buffer is limited to 1 GB, allowing the server to handle the
            // entire contents of the writer buffer as a single message.
            int maxMessageSize = Integer.parseInt(
                stringConfigMap.getOrDefault("max-message-size", String.valueOf(Integer.MAX_VALUE)));

            ServerBuilder<?> serverBuilder = ServerBuilder.forPort(port)
                        .maxInboundMessageSize(maxMessageSize)
                        .intercept(telemetry.serverInterceptor());

            ConcurrentMap<String, IcebergSession> sharedSessions = new ConcurrentHashMap<>();

            if (arrowWriterEnabled) {
                 OlakeArrowIngester oai = new OlakeArrowIngester(sharedSessions);
                 serverBuilder.addService(oai);
                 LOGGER.info("Arrow writer enabled - registered OlakeArrowIngester service");
            }

        // Positional deletes are supported for both arrow and legacy paths, so the table
        // index that feeds them is always registered.
        serverBuilder.addService(new OlakeTableIndexer(sharedSessions));
        LOGGER.info("Registered OlakeTableIndexer service");

            // Legacy ingester is always registered (Check, GET_OR_CREATE_TABLE, DROP_TABLE
            // and the default RECORDS path all flow through it).
            OlakeRowsIngester ori = new OlakeRowsIngester(icebergCatalog, sharedSessions);
            serverBuilder.addService(ori);
            LOGGER.info("Legacy writer enabled - registered OlakeRowsIngester service");

            server = serverBuilder.build().start();

            // Drain requests before the bounded final metric export; no diagnostics listener is opened.
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                server.shutdown();
                try {
                    server.awaitTermination(3, TimeUnit.SECONDS);
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                } finally {
                    telemetry.close();
                    recording.close();
                }
            }, "olake-grpc-shutdown"));

            LOGGER.info("Server started on port {} with max message size: {}MB",
                        port, (maxMessageSize / (1024 * 1024)));
        } catch (Throwable t) {
            telemetry.close();
            recording.close();
            // stderr, not LOGGER: that is stdout, and Go only keeps stderr. Unwrap so Go sees
            // the real class, not Iceberg's IllegalArgumentException wrapper.
            Throwable cause = OlakeFailures.rootCause(t);
            String detail = cause.getMessage() == null ? cause.toString() : cause.getMessage();
            System.err.println("Iceberg writer failed to start [" + cause.getClass().getName() + "]: " + detail);
            t.printStackTrace();
            System.exit(1);
            return;
        }

        // Blocks until shutdown, which is what keeps the process alive. Outside the try above:
        // a failure while serving is not a failure to start.
        server.awaitTermination();
        telemetry.close();
        recording.close();
    }
    static void configureResyncCatalog(Map<String, String> properties) {
        if (!Boolean.parseBoolean(properties.remove("atomic-resync-enabled"))) {
            return;
        }
        boundTimeout(properties, "rest.client.connection-timeout-ms", 30_000);
        boundTimeout(properties, "rest.client.socket-timeout-ms", 60_000);
        int retries = Integer.parseInt(properties.getOrDefault("rest.client.max-retries", "5"));
        properties.put("rest.client.max-retries", Integer.toString(Math.max(0, Math.min(5, retries))));
    }

    private static void boundTimeout(Map<String, String> properties, String key, long maximum) {
        long configured = Long.parseLong(properties.getOrDefault(key, Long.toString(maximum)));
        properties.put(key, Long.toString(configured > 0 ? Math.min(configured, maximum) : maximum));
    }
}
