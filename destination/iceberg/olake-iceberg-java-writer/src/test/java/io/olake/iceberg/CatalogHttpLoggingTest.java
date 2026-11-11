package io.olake.iceberg;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.sun.net.httpserver.HttpServer;
import org.apache.iceberg.rest.HTTPClient;
import org.apache.iceberg.rest.auth.AuthSession;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.apache.logging.log4j.core.config.Property;
import org.junit.jupiter.api.Test;

class CatalogHttpLoggingTest {
    @Test
    void logsRetriedResponsesAndAwsIdsWithoutLeakingHeadersQueriesOrBodies() throws Exception {
        LoggerContext context = (LoggerContext) LogManager.getContext(false);
        LoggerConfig originalHeaders = context.getConfiguration().getLoggers().get("org.apache.hc.client5.http.headers");
        LoggerConfig originalDiagnostics = context.getConfiguration().getLoggers().get(CatalogHttpLogging.class.getName());
        Capture capture = new Capture();
        capture.start();
        LoggerConfig diagnostics = new LoggerConfig(CatalogHttpLogging.class.getName(), Level.INFO, false);
        diagnostics.addAppender(capture, Level.INFO, null);
        context.getConfiguration().addLogger(CatalogHttpLogging.class.getName(), diagnostics);
        // Even a preconfigured raw header logger must not bypass the allowlist.
        LoggerConfig rawHeaders = new LoggerConfig("org.apache.hc.client5.http.headers", Level.DEBUG, false);
        rawHeaders.addAppender(capture, Level.DEBUG, null);
        context.getConfiguration().removeLogger("org.apache.hc.client5.http.headers");
        context.getConfiguration().addLogger("org.apache.hc.client5.http.headers", rawHeaders);
        context.updateLoggers();

        AtomicInteger gets = new AtomicInteger();
        AtomicInteger posts = new AtomicInteger();
        List<String> received = new CopyOnWriteArrayList<>();
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", exchange -> {
            received.add(exchange.getRequestMethod() + " " + exchange.getRequestURI() + " "
                    + exchange.getRequestHeaders().getFirst("Authorization") + " "
                    + new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
            boolean throttled = exchange.getRequestMethod().equals("GET") && gets.incrementAndGet() == 1;
            if (exchange.getRequestMethod().equals("POST")) {
                posts.incrementAndGet();
            }
            exchange.getResponseHeaders().add("x-amzn-requestid", throttled ? "aws-throttle" : "aws-success");
            exchange.getResponseHeaders().add("x-amz-request-id", "aws-s3-id");
            exchange.getResponseHeaders().add("Set-Cookie", "response-cookie-secret");
            if (throttled) {
                exchange.getResponseHeaders().add("Retry-After", "1");
                byte[] body = "response-body-secret".getBytes(StandardCharsets.UTF_8);
                exchange.sendResponseHeaders(429, body.length);
                exchange.getResponseBody().write(body);
            } else {
                exchange.sendResponseHeaders(204, -1);
            }
            exchange.close();
        });
        server.start();
        try {
            CatalogHttpLogging.install();
            CatalogHttpLogging.install();
            try (HTTPClient client = HTTPClient.builder(Map.of("rest.client.max-retries", "1"))
                    .uri("http://127.0.0.1:" + server.getAddress().getPort())
                    .withAuthSession(AuthSession.EMPTY).build()) {
                Map<String, String> headers = Map.of("Authorization", "Bearer authorization-secret", "Cookie", "request-cookie-secret");
                client.get("v1/bucket/namespaces/public/tables/example",
                        Map.of("token", "query-secret"), null, headers, error -> {
                            throw new AssertionError("HTTP retry failed: " + error.code());
                        });
                client.postForm("v1/oauth/tokens", Map.of("client_secret", "request-body-secret"),
                        null, headers, error -> { throw new AssertionError("POST failed"); });
            }
            assertEquals(2, gets.get());
            assertEquals(1, posts.get());
            assertTrue(received.stream().anyMatch(line -> line.contains("query-secret") && line.contains("authorization-secret")));
            assertTrue(received.stream().anyMatch(line -> line.contains("request-body-secret")));
            List<String> responses = capture.messages.stream().filter(line -> line.startsWith("Catalog HTTP response attempt_id=")).toList();
            assertEquals(3, responses.size());
            assertTrue(responses.get(0).contains("method=GET host=127.0.0.1 path=/v1/bucket/namespaces/public/tables/example status=429"));
            assertTrue(responses.get(1).contains("status=204"));
            assertTrue(responses.get(2).contains("method=POST"));
            assertTrue(responses.get(2).contains("path=/v1/oauth/tokens status=204"));
            Matcher attempt = Pattern.compile("attempt_id=(\\d+)").matcher(responses.get(0));
            assertTrue(attempt.find());
            String throttleAttempt = "attempt_id=" + attempt.group(1);
            assertTrue(capture.messages.stream().anyMatch(line -> line.contains(throttleAttempt) && line.contains("name=x-amzn-requestid value=aws-throttle")));
            assertTrue(capture.messages.stream().anyMatch(line -> line.contains(throttleAttempt) && line.contains("name=retry-after value=1")));
            assertTrue(capture.messages.stream().anyMatch(line -> line.contains("name=x-amz-request-id value=aws-s3-id")));
            String logged = String.join("\n", capture.messages);
            for (String secret : List.of("authorization-secret", "request-cookie-secret", "response-cookie-secret",
                    "query-secret", "request-body-secret", "response-body-secret")) {
                assertFalse(logged.contains(secret), "leaked " + secret);
            }
        } finally {
            server.stop(0);
            context.getConfiguration().removeLogger("org.apache.hc.client5.http.headers");
            context.getConfiguration().removeLogger(CatalogHttpLogging.class.getName());
            if (originalHeaders != null) {
                context.getConfiguration().addLogger("org.apache.hc.client5.http.headers", originalHeaders);
            }
            if (originalDiagnostics != null) {
                context.getConfiguration().addLogger(CatalogHttpLogging.class.getName(), originalDiagnostics);
            }
            context.updateLoggers();
            capture.stop();
        }
    }

    private static final class Capture extends AbstractAppender {
        final List<String> messages = new CopyOnWriteArrayList<>();

        Capture() {
            super("CatalogHttpTestCapture", null, null, true, Property.EMPTY_ARRAY);
        }

        @Override
        public void append(LogEvent event) {
            messages.add(event.getMessage().getFormattedMessage());
        }
    }
}
