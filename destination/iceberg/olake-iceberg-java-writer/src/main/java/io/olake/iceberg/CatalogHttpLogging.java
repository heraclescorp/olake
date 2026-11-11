package io.olake.iceberg;

import java.net.URI;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hc.core5.http.Header;
import org.apache.hc.core5.http.message.RequestLine;
import org.apache.hc.core5.http.message.StatusLine;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.apache.logging.log4j.core.config.Property;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Allowlisted transport diagnostics, including responses hidden by HTTP retries. */
public final class CatalogHttpLogging extends AbstractAppender {
    private static final Logger LOGGER = LoggerFactory.getLogger(CatalogHttpLogging.class);
    private static final String HEADER_LOGGER = "org.apache.hc.client5.http.headers";
    private static final String APPENDER_NAME = "CatalogHttpDiagnostics";
    private final AtomicLong nextAttempt = new AtomicLong();
    // Classic HttpClient submits a request and receives its headers on the same thread.
    private final ThreadLocal<Attempt> currentAttempt = new ThreadLocal<>();

    private CatalogHttpLogging() {
        super(APPENDER_NAME, null, null, true, Property.EMPTY_ARRAY);
    }

    public static synchronized void install() {
        LoggerContext context = (LoggerContext) LogManager.getContext(false);
        LoggerConfig existing = context.getConfiguration().getLoggers().get(HEADER_LOGGER);
        if (existing != null && existing.getAppenders().containsKey(APPENDER_NAME)) {
            return;
        }
        CatalogHttpLogging appender = new CatalogHttpLogging();
        appender.start();
        // Never propagate raw header events: they include Authorization and cookies.
        LoggerConfig logger = new LoggerConfig(HEADER_LOGGER, Level.DEBUG, false);
        logger.addAppender(appender, Level.DEBUG, null);
        context.getConfiguration().removeLogger(HEADER_LOGGER);
        context.getConfiguration().addLogger(HEADER_LOGGER, logger);
        context.updateLoggers();
    }

    @Override
    public void append(LogEvent event) {
        Object[] parameters = event.getMessage().getParameters();
        if (parameters == null || parameters.length != 2 || !(parameters[0] instanceof String connection)) {
            return;
        }
        Object detail = parameters[1];
        if ("{} >> {}".equals(event.getMessage().getFormat()) && detail instanceof RequestLine request) {
            Attempt attempt = new Attempt(nextAttempt.incrementAndGet(), connection, request);
            currentAttempt.set(attempt);
            LOGGER.info("Catalog HTTP request attempt_id={} connection={} method={} path={}",
                    attempt.id, connection, attempt.method, attempt.path);
            return;
        }
        Attempt attempt = currentAttempt.get();
        if (attempt == null || !attempt.connection.equals(connection)) {
            return;
        }
        if ("{} >> {}".equals(event.getMessage().getFormat()) && detail instanceof Header header) {
            if ("host".equalsIgnoreCase(header.getName())) {
                attempt.host = hostOnly(header.getValue());
            }
        } else if ("{} << {}".equals(event.getMessage().getFormat())) {
            if (detail instanceof StatusLine status) {
                attempt.status = status.getStatusCode();
                LOGGER.info("Catalog HTTP response attempt_id={} method={} host={} path={} status={} duration_ms={}",
                        attempt.id, attempt.method, attempt.host, attempt.path, attempt.status,
                        (System.nanoTime() - attempt.startedNanos) / 1_000_000);
            } else if (detail instanceof Header header) {
                String name = header.getName().toLowerCase(Locale.ROOT);
                if (name.equals("x-amzn-requestid") || name.equals("x-amz-request-id") || name.equals("retry-after")) {
                    LOGGER.info("Catalog HTTP response header attempt_id={} status={} name={} value={}",
                            attempt.id, attempt.status, name, safeValue(header.getValue()));
                }
            }
        }
    }

    private static String hostOnly(String authority) {
        try {
            String host = URI.create("https://" + authority).getHost();
            return host == null ? "invalid" : host;
        } catch (IllegalArgumentException e) {
            return "invalid";
        }
    }

    private static String safeValue(String value) {
        if (value == null) {
            return "";
        }
        StringBuilder safe = new StringBuilder(Math.min(value.length(), 512));
        for (int i = 0; i < value.length() && i < 512; i++) {
            char c = value.charAt(i);
            safe.append(c >= 32 && c <= 126 ? c : '_');
        }
        return safe.toString();
    }

    private static final class Attempt {
        final long id;
        final String connection;
        final String method;
        final String path;
        final long startedNanos = System.nanoTime();
        String host = "unknown";
        int status;

        Attempt(long id, String connection, RequestLine request) {
            this.id = id;
            this.connection = connection;
            this.method = safeValue(request.getMethod());
            String sanitizedPath;
            try {
                URI uri = URI.create(request.getUri());
                sanitizedPath = uri.getRawPath();
                if (uri.getHost() != null) {
                    host = uri.getHost();
                }
            } catch (IllegalArgumentException e) {
                sanitizedPath = "invalid";
            }
            this.path = safeValue(sanitizedPath);
        }
    }
}
