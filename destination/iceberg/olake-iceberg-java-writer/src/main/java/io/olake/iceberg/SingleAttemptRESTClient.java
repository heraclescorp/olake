package io.olake.iceberg;

import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.apache.hc.client5.http.auth.AuthScope;
import org.apache.hc.client5.http.auth.UsernamePasswordCredentials;
import org.apache.hc.client5.http.classic.methods.HttpUriRequestBase;
import org.apache.hc.client5.http.config.ConnectionConfig;
import org.apache.hc.client5.http.impl.auth.BasicCredentialsProvider;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.CloseableHttpResponse;
import org.apache.hc.client5.http.impl.classic.HttpClientBuilder;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManagerBuilder;
import org.apache.hc.client5.http.ssl.DefaultClientTlsStrategy;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.Header;
import org.apache.hc.core5.http.HttpHeaders;
import org.apache.hc.core5.http.HttpHost;
import org.apache.hc.core5.http.ParseException;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.hc.core5.http.io.entity.StringEntity;
import org.apache.hc.core5.reactor.ssl.SSLBufferMode;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.IcebergBuild;
import org.apache.iceberg.exceptions.RESTException;
import org.apache.iceberg.rest.BaseHTTPClient;
import org.apache.iceberg.rest.ErrorHandler;
import org.apache.iceberg.rest.HTTPHeaders;
import org.apache.iceberg.rest.HTTPRequest;
import org.apache.iceberg.rest.HTTPRequest.HTTPMethod;
import org.apache.iceberg.rest.ImmutableHTTPRequest;
import org.apache.iceberg.rest.ParserContext;
import org.apache.iceberg.rest.RESTClient;
import org.apache.iceberg.rest.RESTSerializers;
import org.apache.iceberg.rest.RESTResponse;
import org.apache.iceberg.rest.RESTUtil;
import org.apache.iceberg.rest.auth.AuthSession;
import org.apache.iceberg.rest.auth.TLSConfigurer;
import org.apache.iceberg.rest.responses.ErrorResponse;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonFactoryBuilder;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;

/**
 * Iceberg 1.10.2's public RESTClient extension point with one HTTP dispatch per
 * call. Its stock client rejects max-retries=0 and retries mutating requests.
 * Reuse Iceberg's request/auth/parser machinery; only the transport policy differs.
 */
public final class SingleAttemptRESTClient extends BaseHTTPClient {
    private static final ObjectMapper MAPPER = createMapper();
    private final URI baseUri;
    private final Map<String, String> baseHeaders;
    private final CloseableHttpClient transport;
    private final AuthSession auth;
    private final boolean ownsTransport;
    private final ObjectMapper mapper = MAPPER;
    private final Map<Class<?>, ObjectReader> readers;

    private static ObjectMapper createMapper() {
        ObjectMapper mapper = new ObjectMapper(new JsonFactoryBuilder()
                .configure(JsonFactory.Feature.INTERN_FIELD_NAMES, false)
                .configure(JsonFactory.Feature.FAIL_ON_SYMBOL_HASH_OVERFLOW, false).build());
        mapper.setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY);
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        mapper.setPropertyNamingStrategy(new PropertyNamingStrategies.KebabCaseStrategy());
        RESTSerializers.registerAll(mapper);
        return mapper;
    }

    public SingleAttemptRESTClient(Map<String, String> properties) {
        baseUri = URI.create(properties.get(CatalogProperties.URI));
        baseHeaders = new HashMap<>(RESTUtil.configHeaders(properties));
        baseHeaders.put("X-Client-Version", IcebergBuild.fullVersion());
        baseHeaders.put("X-Client-Git-Commit-Short", IcebergBuild.gitCommitShortId());
        transport = transport(properties);
        auth = null;
        ownsTransport = true;
        readers = new ConcurrentHashMap<>();
    }

    private SingleAttemptRESTClient(SingleAttemptRESTClient parent, AuthSession session) {
        baseUri = parent.baseUri;
        baseHeaders = parent.baseHeaders;
        transport = parent.transport;
        auth = java.util.Objects.requireNonNull(session, "REST auth session");
        ownsTransport = false;
        readers = parent.readers;
    }

    @Override
    public RESTClient withAuthSession(AuthSession session) {
        return new SingleAttemptRESTClient(this, session);
    }

    @Override
    protected HTTPRequest buildRequest(HTTPMethod method, String path, Map<String, String> query,
            Map<String, String> headers, Object body) {
        Map<String, String> combined = new HashMap<>(headers == null ? Map.of() : headers);
        baseHeaders.forEach(combined::putIfAbsent);
        combined.putIfAbsent(HttpHeaders.ACCEPT, ContentType.APPLICATION_JSON.getMimeType());
        combined.putIfAbsent(HttpHeaders.CONTENT_TYPE, (body instanceof Map
                ? ContentType.APPLICATION_FORM_URLENCODED : ContentType.APPLICATION_JSON).getMimeType());
        HTTPRequest request = ImmutableHTTPRequest.builder().baseUri(baseUri).method(method).path(path)
                .queryParameters(query == null ? Map.of() : query).body(body).mapper(mapper)
                .headers(HTTPHeaders.of(combined)).build();
        if (auth == null) { throw new IllegalStateException("REST request requires an Iceberg auth session"); }
        return auth.authenticate(request);
    }

    @Override
    protected <T extends RESTResponse> T execute(HTTPRequest request, Class<T> responseType,
            Consumer<ErrorResponse> errorHandler, Consumer<Map<String, String>> responseHeaders) {
        return execute(request, responseType, errorHandler, responseHeaders, null);
    }

    @Override
    protected <T extends RESTResponse> T execute(HTTPRequest request, Class<T> responseType,
            Consumer<ErrorResponse> errorHandler, Consumer<Map<String, String>> responseHeaders,
            ParserContext parserContext) {
        HttpUriRequestBase wire = new HttpUriRequestBase(request.method().name(), request.requestUri());
        request.headers().entries().forEach(header -> wire.addHeader(header.name(), header.value()));
        String body = request.encodedBody();
        if (body != null) { wire.setEntity(new StringEntity(body, StandardCharsets.UTF_8)); }
        // OAuth form exchange is authentication, not a catalog-table mutation.
        boolean mutation = request.method() == HTTPMethod.DELETE
                || (request.method() == HTTPMethod.POST && !(request.body() instanceof Map));
        ExecutionRetirement retirement = ExecutionFence.runtime().retirement();
        if (mutation) {
            ExecutionFence.requireMutation();
            retirement.begin();
        }
        boolean definitive = false;
        try (CloseableHttpResponse response = transport.execute(wire)) {
            Map<String, String> received = new HashMap<>();
            for (Header header : response.getHeaders()) { received.put(header.getName(), header.getValue()); }
            responseHeaders.accept(received);
            int status = response.getCode();
            boolean success = status == 200 || status == 202 || status == 204;
            // A single DELETE 404 is the normal missing-table result. No other
            // error response proves that a dispatched mutation did not apply.
            definitive = success || (request.method() == HTTPMethod.DELETE && status == 404);
            // HEAD never requires a body; preserve its status/error semantics.
            if (success && (responseType == null || request.method() == HTTPMethod.HEAD || status == 204)) {
                return null;
            }
            String json = response.getEntity() == null ? null
                    : EntityUtils.toString(response.getEntity(), StandardCharsets.UTF_8);
            if (!success) { fail(response, json, errorHandler); }
            if (json == null) { throw new RESTException("REST success response has no body"); }
            ObjectReader reader = readers.computeIfAbsent(responseType, mapper::readerFor);
            if (parserContext != null && !parserContext.isEmpty()) {
                reader = reader.with(parserContext.toInjectableValues());
            }
            return reader.readValue(json);
        } catch (IOException | ParseException failure) {
            throw new RESTException(failure, "Single-attempt REST %s request failed", request.method());
        } finally {
            if (mutation) { retirement.complete(definitive); }
        }
    }

    private static void fail(CloseableHttpResponse response, String json, Consumer<ErrorResponse> handler) {
        ErrorResponse error = null;
        if (handler instanceof ErrorHandler parser && json != null) {
            try { error = parser.parseResponse(response.getCode(), json); }
            catch (RuntimeException malformed) { /* A gateway may return non-Iceberg HTML. */ }
        }
        if (error == null) {
            error = ErrorResponse.builder().responseCode(response.getCode()).withType("RESTException")
                    .withMessage("REST HTTP " + response.getCode()).build();
        }
        handler.accept(error);
        throw new RESTException("Unhandled REST error: %s", error);
    }

    @Override
    public void close() throws IOException {
        if (ownsTransport) { transport.close(); }
        // AuthManager owns AuthSession; authenticated children share this transport.
    }

    private static CloseableHttpClient transport(Map<String, String> properties) {
        PoolingHttpClientConnectionManagerBuilder pool = PoolingHttpClientConnectionManagerBuilder.create()
                .useSystemProperties()
                .setMaxConnTotal(Integer.getInteger("rest.client.max-connections",
                        integer(properties, "rest.client.max-connections", 100)))
                .setMaxConnPerRoute(integer(properties, "rest.client.connections-per-route", 100));
        ConnectionConfig.Builder connection = ConnectionConfig.custom();
        String connect = properties.get("rest.client.connection-timeout-ms");
        String socket = properties.get("rest.client.socket-timeout-ms");
        if (connect != null) { connection.setConnectTimeout(Long.parseLong(connect), TimeUnit.MILLISECONDS); }
        if (socket != null) { connection.setSocketTimeout(org.apache.hc.core5.util.Timeout.ofMilliseconds(Long.parseLong(socket))); }
        pool.setDefaultConnectionConfig(connection.build());
        String tlsClass = properties.get("rest.client.tls.configurer-impl");
        if (tlsClass != null) {
            TLSConfigurer tls;
            try {
                tls = Class.forName(tlsClass).asSubclass(TLSConfigurer.class).getConstructor().newInstance();
            } catch (ReflectiveOperationException | ClassCastException failure) {
                throw new IllegalArgumentException("Cannot initialize configured REST TLSConfigurer", failure);
            }
            tls.initialize(properties);
            pool.setTlsSocketStrategy(new DefaultClientTlsStrategy(tls.sslContext(), tls.supportedProtocols(),
                    tls.supportedCipherSuites(), SSLBufferMode.STATIC, tls.hostnameVerifier()));
        }
        HttpClientBuilder client = HttpClients.custom().setConnectionManager(pool.build())
                .disableAutomaticRetries().disableRedirectHandling();
        String proxyHost = properties.get("rest.client.proxy.hostname");
        String proxyPort = properties.get("rest.client.proxy.port");
        if (proxyHost != null && proxyPort != null) {
            HttpHost proxy = new HttpHost(proxyHost, Integer.parseInt(proxyPort));
            client.setProxy(proxy);
            String user = properties.get("rest.client.proxy.username");
            String password = properties.get("rest.client.proxy.password");
            if (user != null && password != null) {
                BasicCredentialsProvider credentials = new BasicCredentialsProvider();
                credentials.setCredentials(new AuthScope(proxy),
                        new UsernamePasswordCredentials(user, password.toCharArray()));
                client.setDefaultCredentialsProvider(credentials);
            }
        }
        String agent = properties.get("rest.client.user-agent");
        if (agent != null) { client.setUserAgent(agent); }
        return client.build();
    }

    private static int integer(Map<String, String> properties, String name, int fallback) {
        return Integer.parseInt(properties.getOrDefault(name, Integer.toString(fallback)));
    }
}
