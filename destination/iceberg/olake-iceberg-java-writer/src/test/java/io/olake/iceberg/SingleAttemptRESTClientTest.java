package io.olake.iceberg;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.exceptions.RESTException;
import org.apache.iceberg.rest.ErrorHandlers;
import org.apache.iceberg.rest.HTTPHeaders;
import org.apache.iceberg.rest.HTTPRequest;
import org.apache.iceberg.rest.ImmutableHTTPRequest;
import org.apache.iceberg.rest.ParserContext;
import org.apache.iceberg.rest.RESTClient;
import org.apache.iceberg.rest.RESTResponse;
import org.apache.iceberg.rest.auth.AuthSession;
import org.apache.iceberg.rest.requests.UpdateTableRequest;
import org.apache.iceberg.rest.responses.ConfigResponse;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

class SingleAttemptRESTClientTest {
    @TempDir Path directory;
    private HttpServer server;

    @AfterEach
    void close() {
        if (server != null) { server.stop(0); }
        ExecutionFence.configure(Map.of());
    }

    private Map<String, String> start(com.sun.net.httpserver.HttpHandler handler) throws IOException {
        ExecutionFence.configure(Map.of());
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", handler);
        server.start();
        return Map.of("uri", "http://127.0.0.1:" + server.getAddress().getPort(), "rest.auth.type", "none");
    }

    private static void respond(HttpExchange exchange, int status, String body) throws IOException {
        if (body == null) {
            exchange.sendResponseHeaders(status, -1);
        } else {
            byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().add("Content-Type", "application/json");
            exchange.sendResponseHeaders(status, bytes.length);
            exchange.getResponseBody().write(bytes);
        }
        exchange.close();
    }

    @Test
    void post503CannotReplayTo409AndDelete503CannotReplayToFalse404Success() throws Exception {
        for (String method : List.of("POST", "DELETE")) {
            AtomicInteger calls = new AtomicInteger();
            Map<String, String> config = start(exchange -> {
                int attempt = calls.incrementAndGet();
                exchange.getRequestBody().readAllBytes();
                exchange.getResponseHeaders().add("Retry-After", "0");
                int status = attempt == 1 ? 503 : method.equals("POST") ? 409 : 404;
                respond(exchange, status, "{\"error\":{\"message\":\"held first mutation\",\"type\":\"ServiceFailureException\",\"code\":" + status + "}}");
            });
            try (SingleAttemptRESTClient root = new SingleAttemptRESTClient(config);
                    RESTClient client = root.withAuthSession(AuthSession.EMPTY)) {
                assertThrows(RuntimeException.class, () -> {
                    if (method.equals("POST")) {
                        client.post("v1/mutation", new UpdateTableRequest(List.of(), List.of()), null,
                                Map.of(), ErrorHandlers.tableErrorHandler());
                    } else {
                        client.delete("v1/mutation", null, Map.of(), ErrorHandlers.tableErrorHandler());
                    }
                });
                assertEquals(1, calls.get(), "mutating REST request was replayed after ambiguous 503");
            }
            server.stop(0);
        }
    }

    @Test
    void oauthFormAndReadsDoNotStartCatalogMutationAndRemainAllowedAfterSeal() throws Exception {
        Map<String, String> config = start(exchange -> {
            exchange.getRequestBody().readAllBytes();
            respond(exchange, 200, "{\"defaults\":{},\"overrides\":{}}");
        });
        try (SingleAttemptRESTClient root = new SingleAttemptRESTClient(config);
                RESTClient client = root.withAuthSession(AuthSession.EMPTY)) {
            client.postForm("oauth/tokens", Map.of("grant_type", "client_credentials"),
                    ConfigResponse.class, Map.of(), ErrorHandlers.defaultErrorHandler());
            assertEquals("not_started", ExecutionFence.runtime().retirement().seal());
            assertNotNull(client.get("v1/config", Map.of(), ConfigResponse.class, Map.of(), ErrorHandlers.defaultErrorHandler()));
            assertNotNull(client.postForm("oauth/tokens", Map.of("grant_type", "client_credentials"),
                    ConfigResponse.class, Map.of(), ErrorHandlers.defaultErrorHandler()));
            assertEquals("not_started", ExecutionFence.runtime().retirement().seal());
        }
    }

    @Test
    void ambiguityIsStickyAcrossAuthClonesAndIndependentRoots() throws Exception {
        AtomicInteger calls = new AtomicInteger();
        Map<String, String> config = start(exchange -> {
            exchange.getRequestBody().readAllBytes();
            int status = calls.incrementAndGet() == 1 ? 503 : 404;
            respond(exchange, status, "{\"error\":{\"message\":\"missing\",\"type\":\"NoSuchTableException\",\"code\":" + status + "}}");
        });
        try (SingleAttemptRESTClient first = new SingleAttemptRESTClient(config);
                SingleAttemptRESTClient second = new SingleAttemptRESTClient(config);
                RESTClient a = first.withAuthSession(AuthSession.EMPTY);
                RESTClient b = first.withAuthSession(AuthSession.EMPTY);
                RESTClient c = second.withAuthSession(AuthSession.EMPTY)) {
            assertThrows(RuntimeException.class, () -> a.delete("table", null, Map.of(), ErrorHandlers.tableErrorHandler()));
            assertThrows(RuntimeException.class, () -> b.delete("table", null, Map.of(), ErrorHandlers.tableErrorHandler()));
            assertThrows(RuntimeException.class, () -> c.delete("table", null, Map.of(), ErrorHandlers.tableErrorHandler()));
            assertEquals("unknown", ExecutionFence.runtime().retirement().seal());
            assertThrows(IllegalStateException.class, () -> c.delete("table", null, Map.of(), ErrorHandlers.tableErrorHandler()));
            assertEquals(3, calls.get());
        }
    }

    @Test
    void inFlightDispatchMakesTerminalUnknownEvenAfterResponseArrives() throws Exception {
        java.util.concurrent.CountDownLatch dispatched = new java.util.concurrent.CountDownLatch(1);
        java.util.concurrent.CountDownLatch release = new java.util.concurrent.CountDownLatch(1);
        Map<String, String> config = start(exchange -> {
            exchange.getRequestBody().readAllBytes();
            dispatched.countDown();
            try {
                if (!release.await(10, java.util.concurrent.TimeUnit.SECONDS)) { throw new IOException("barrier timed out"); }
                respond(exchange, 204, null);
            } catch (InterruptedException failure) {
                Thread.currentThread().interrupt();
                throw new IOException(failure);
            }
        });
        java.util.concurrent.ExecutorService executor = java.util.concurrent.Executors.newSingleThreadExecutor();
        try (SingleAttemptRESTClient root = new SingleAttemptRESTClient(config);
                RESTClient client = root.withAuthSession(AuthSession.EMPTY)) {
            java.util.concurrent.Future<?> pending = executor.submit(() -> client.delete("table", null, Map.of(), ErrorHandlers.tableErrorHandler()));
            try {
                assertTrue(dispatched.await(10, java.util.concurrent.TimeUnit.SECONDS));
                assertEquals("unknown", ExecutionFence.runtime().retirement().seal());
                assertThrows(IllegalStateException.class, () -> client.delete("late", null, Map.of(), ErrorHandlers.tableErrorHandler()));
            } finally {
                release.countDown();
            }
            pending.get(10, java.util.concurrent.TimeUnit.SECONDS);
            assertEquals("unknown", ExecutionFence.runtime().retirement().seal());
        } finally {
            release.countDown();
            executor.shutdownNow();
        }
    }

    @Test
    void completedMutationAndLaterLocalFailureRemainSettled() throws Exception {
        Map<String, String> config = start(exchange -> {
            exchange.getRequestBody().readAllBytes();
            respond(exchange, 204, null);
        });
        try (SingleAttemptRESTClient root = new SingleAttemptRESTClient(config);
                RESTClient client = root.withAuthSession(AuthSession.EMPTY)) {
            client.delete("table", null, Map.of(), ErrorHandlers.tableErrorHandler());
            assertThrows(IllegalArgumentException.class, () -> { throw new IllegalArgumentException("local TOAST conversion"); });
            assertEquals("settled", ExecutionFence.runtime().retirement().seal());
        }
    }

    @Test
    void singleAttemptMissingDeleteIsSettledWithoutEarlierAmbiguity() throws Exception {
        Map<String, String> config = start(exchange -> respond(exchange, 404,
                "{\"error\":{\"message\":\"missing\",\"type\":\"NoSuchTableException\",\"code\":404}}"));
        try (SingleAttemptRESTClient root = new SingleAttemptRESTClient(config);
                RESTClient client = root.withAuthSession(AuthSession.EMPTY)) {
            assertThrows(RuntimeException.class, () -> client.delete("table", null, Map.of(), ErrorHandlers.tableErrorHandler()));
            assertEquals("settled", ExecutionFence.runtime().retirement().seal());
        }
    }

    @Test
    void officialAuthSessionSeesCompleteRequestAndHeadersBodiesAndResponseHeadersSurvive() throws Exception {
        AtomicReference<String> body = new AtomicReference<>();
        AtomicReference<String> authorization = new AtomicReference<>();
        AtomicReference<String> catalogHeader = new AtomicReference<>();
        AtomicReference<String> explicitHeader = new AtomicReference<>();
        AtomicReference<String> contentType = new AtomicReference<>();
        Map<String, String> config = new java.util.HashMap<>(start(exchange -> {
            body.set(new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
            authorization.set(exchange.getRequestHeaders().getFirst("Authorization"));
            catalogHeader.set(exchange.getRequestHeaders().getFirst("X-Catalog"));
            explicitHeader.set(exchange.getRequestHeaders().getFirst("X-Explicit"));
            contentType.set(exchange.getRequestHeaders().getFirst("Content-Type"));
            exchange.getResponseHeaders().add("X-Response", "retained");
            respond(exchange, 200, "{\"defaults\":{\"retained\":\"yes\"},\"overrides\":{}}");
        }));
        config.put("header.X-Catalog", "configured");
        AtomicReference<HTTPRequest> authenticated = new AtomicReference<>();
        AuthSession auth = new AuthSession() {
            @Override public HTTPRequest authenticate(HTTPRequest request) {
                authenticated.set(request);
                return ImmutableHTTPRequest.builder().from(request).headers(request.headers().putIfAbsent(
                        HTTPHeaders.of(Map.of("Authorization", "test-auth-session")))).build();
            }
            @Override public void close() { }
        };
        AtomicReference<Map<String, String>> responseHeaders = new AtomicReference<>();
        try (SingleAttemptRESTClient root = new SingleAttemptRESTClient(config);
                RESTClient client = root.withAuthSession(auth)) {
            ConfigResponse response = client.post("v1/mutation", new UpdateTableRequest(List.of(), List.of()),
                    ConfigResponse.class, Map.of("X-Explicit", "caller"), ErrorHandlers.defaultErrorHandler(), responseHeaders::set);
            assertEquals("yes", response.defaults().get("retained"));
            assertEquals("POST", authenticated.get().method().name());
            assertEquals("/v1/mutation", authenticated.get().requestUri().getPath());
            assertTrue(new ObjectMapper().readTree(body.get()).path("updates").isArray());
            assertEquals("test-auth-session", authorization.get());
            assertEquals("configured", catalogHeader.get());
            assertEquals("caller", explicitHeader.get());
            assertTrue(contentType.get().startsWith("application/json"));
            assertTrue(responseHeaders.get().entrySet().stream().anyMatch(entry ->
                    entry.getKey().equalsIgnoreCase("X-Response") && entry.getValue().equals("retained")));
            client.postForm("oauth/tokens", Map.of("grant_type", "client_credentials", "scope", "a b"),
                    ConfigResponse.class, Map.of(), ErrorHandlers.defaultErrorHandler());
            assertTrue(contentType.get().startsWith("application/x-www-form-urlencoded"));
            assertTrue(body.get().contains("scope=a+b"));
        }
    }

    public static final class InjectedResponse implements RESTResponse {
        final String value;
        final String injected;
        @JsonCreator public InjectedResponse(@JsonProperty("value") String value,
                @JacksonInject("context") String injected) {
            this.value = value;
            this.injected = injected;
        }
        @Override public void validate() { }
    }

    @Test
    void parserContextAndHeadDeleteStatusSemanticsArePreserved() throws Exception {
        Map<String, String> config = start(exchange -> {
            if (exchange.getRequestMethod().equals("HEAD")) { respond(exchange, 200, null); }
            else if (exchange.getRequestMethod().equals("DELETE")) { respond(exchange, 204, null); }
            else { respond(exchange, 200, "{\"value\":\"parsed\"}"); }
        });
        try (SingleAttemptRESTClient root = new SingleAttemptRESTClient(config);
                RESTClient client = root.withAuthSession(AuthSession.EMPTY)) {
            client.head("v1/table", Map.of(), ErrorHandlers.tableErrorHandler());
            assertNull(client.delete("v1/table", Map.of("purgeRequested", "true"), null, Map.of(), ErrorHandlers.tableErrorHandler()));
            ParserContext context = ParserContext.builder().add("context", "injected-value").build();
            InjectedResponse response = client.get("v1/read", Map.of(), InjectedResponse.class,
                    Map.of(), ErrorHandlers.defaultErrorHandler(), context);
            assertEquals("parsed", response.value);
            assertEquals("injected-value", response.injected);
            assertEquals("injected-value", client.post("v1/post", new UpdateTableRequest(List.of(), List.of()),
                    InjectedResponse.class, Map.of(), ErrorHandlers.defaultErrorHandler(), ignored -> { }, context).injected);
        }
    }

    @Test
    void redirectsAreErrorsAndNeverReplayMutation() throws Exception {
        AtomicInteger redirects = new AtomicInteger();
        AtomicInteger replayed = new AtomicInteger();
        Map<String, String> config = start(exchange -> {
            exchange.getRequestBody().readAllBytes();
            if (exchange.getRequestURI().getPath().equals("/other")) {
                replayed.incrementAndGet(); respond(exchange, 204, null);
            } else {
                redirects.incrementAndGet();
                exchange.getResponseHeaders().add("Location", "/other");
                respond(exchange, 307, "{}");
            }
        });
        try (SingleAttemptRESTClient root = new SingleAttemptRESTClient(config);
                RESTClient client = root.withAuthSession(AuthSession.EMPTY)) {
            assertThrows(RESTException.class, () -> client.post("redirect", new UpdateTableRequest(List.of(), List.of()),
                    null, Map.of(), ErrorHandlers.defaultErrorHandler()));
            assertThrows(RESTException.class, () -> client.delete("redirect", null, Map.of(), ErrorHandlers.defaultErrorHandler()));
            assertEquals(2, redirects.get());
            assertEquals(0, replayed.get());
        }
    }

    @Test
    void authenticatedChildCloseDoesNotOwnTransportOrAuthSession() throws Exception {
        Map<String, String> config = start(exchange -> respond(exchange, 200, null));
        AtomicBoolean authClosed = new AtomicBoolean();
        AuthSession auth = new AuthSession() {
            @Override public HTTPRequest authenticate(HTTPRequest request) { return request; }
            @Override public void close() { authClosed.set(true); }
        };
        SingleAttemptRESTClient root = new SingleAttemptRESTClient(config);
        RESTClient first = root.withAuthSession(auth);
        first.close();
        try (RESTClient second = root.withAuthSession(auth)) {
            second.head("still-open", Map.of(), ErrorHandlers.defaultErrorHandler());
            assertFalse(authClosed.get());
            root.close();
            assertThrows(RuntimeException.class, () -> second.head("closed", Map.of(), ErrorHandlers.defaultErrorHandler()));
        }
        assertFalse(authClosed.get());
    }

    @Test
    void configuredTlsFailureNeverFallsBackSilently() throws Exception {
        Map<String, String> config = new java.util.HashMap<>(start(exchange -> respond(exchange, 200, null)));
        config.put("rest.client.tls.configurer-impl", "missing.TlsConfigurer");
        assertThrows(IllegalArgumentException.class, () -> new SingleAttemptRESTClient(config));
    }

    @Test
    void realRestCatalogInitializationUsesSingleAttemptPublicFactory() throws Exception {
        AtomicInteger requests = new AtomicInteger();
        Map<String, String> properties = new java.util.HashMap<>(start(exchange -> {
            requests.incrementAndGet();
            respond(exchange, 200, exchange.getRequestURI().getPath().equals("/v1/config")
                    ? "{\"defaults\":{},\"overrides\":{}}" : "{\"identifiers\":[]}");
        }));
        Path fence = directory.resolve("execution-fence.json");
        Files.writeString(fence, "{\"version\":1,\"execution_id\":\"runtime\",\"state\":\"open\"}");
        properties.put("execution-id", "runtime");
        properties.put("execution-fence", fence.toString());
        properties.put("catalog-impl", "org.apache.iceberg.rest.RESTCatalog");
        // The stock client rejects zero: this proves the actual catalog initializes
        // through our supported client factory rather than a property-only assertion.
        properties.put("rest.client.max-retries", "0");
        ExecutionFence.configure(properties);
        Catalog catalog = OlakeRpcServer.buildCatalog("test", properties, new Configuration());
        try {
            assertTrue(catalog.listTables(Namespace.of("db")).isEmpty());
            assertEquals(2, requests.get());
        } finally { ((java.io.Closeable) catalog).close(); }
    }
}
