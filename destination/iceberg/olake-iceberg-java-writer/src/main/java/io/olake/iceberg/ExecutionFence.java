package io.olake.iceberg;

import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProviderChain;
import software.amazon.awssdk.auth.credentials.WebIdentityTokenFileCredentialsProvider;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.auth.credentials.ContainerCredentialsProvider;
import software.amazon.awssdk.auth.credentials.InstanceProfileCredentialsProvider;
import software.amazon.awssdk.profiles.ProfileFile;
import software.amazon.awssdk.regions.providers.InstanceProfileRegionProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;

/** Positive runtime authority, deliberately independent of catalog-vended credentials. */
public final class ExecutionFence {
    private static final ObjectMapper JSON = new ObjectMapper()
            .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS);
    private static volatile ExecutionFence runtime = new ExecutionFence(Map.of());
    private final String id;
    private final String location;
    private final boolean recoveryOnly;
    private final String launchId;
    private final String executionLaunchId;
    private final String command;
    private final ExecutionRetirement retirement = new ExecutionRetirement();
    private S3Client s3;

    public ExecutionFence(Map<String, String> config) {
        id = config.getOrDefault("execution-id", "");
        location = config.getOrDefault("execution-fence", "");
        recoveryOnly = Boolean.parseBoolean(config.getOrDefault("resync-recover-only", "false"));
        launchId = config.getOrDefault("resync-launch-id", "");
        executionLaunchId = config.getOrDefault("execution-launch-id", "");
        command = config.getOrDefault("execution-command", "");
        if (id.isBlank() != location.isBlank()) {
            throw new IllegalArgumentException("execution-id and execution-fence must be supplied together");
        }
        if (recoveryOnly && launchId.isBlank()) {
            throw new IllegalArgumentException("Recovery requires the original resync-launch-id");
        }
    }

    public static void configure(Map<String, String> config) {
        ExecutionFence next = new ExecutionFence(config);
        next.check();
        runtime = next;
    }

    public static ExecutionFence runtime() { return runtime; }
    public boolean recoveryOnly() { return recoveryOnly; }
    public String launchId() { return launchId; }
    public String id() { return id; }

    public String capabilityJson() throws com.fasterxml.jackson.core.JsonProcessingException {
        return JSON.writeValueAsString(Map.of("version", 1, "execution_id", id,
                "launch_id", launchId, "recover_only", recoveryOnly, "fence_state", "open"));
    }

    public ExecutionRetirement retirement() { return retirement; }

    public String retireJson() throws com.fasterxml.jackson.core.JsonProcessingException {
        checkReconciliation();
        if (id.isBlank() || executionLaunchId.isBlank()
                || !("sync".equals(command) || "clear-destination".equals(command))) {
            throw new IllegalStateException("Ordinary execution identity is missing from startup configuration");
        }
        return JSON.writeValueAsString(Map.of("version", 1, "execution_id", id,
                "launch_id", executionLaunchId, "command", command, "outcome", retirement.seal()));
    }

    public static void requireMutation() {
        runtime.retirement.requireAdmission();
        runtime.check();
        if (runtime.recoveryOnly) {
            throw new IllegalStateException("Recovery-only runtime cannot mutate Iceberg");
        }
    }

    public static void requireCatalogMutation(Object authority) {
        requireMutation();
        if (!(authority instanceof org.apache.iceberg.rest.RESTCatalog)
                && !authority.getClass().getName().equals("org.apache.iceberg.rest.RESTTableOperations")) {
            // Other transports may retry or return while remote work remains in flight.
            runtime.retirement.unsupportedMutation();
        }
    }

    public void check() {
        check(false);
    }

    // Terminal controls never publish data. CLOSED must still allow serialized
    // abort/status, but missing, unreadable or wrong-runtime authority cannot.
    public void checkReconciliation() {
        check(true);
    }

    private void check(boolean allowClosed) {
        if (id.isBlank()) { return; }
        try {
            byte[] bytes;
            if (location.startsWith("s3://")) {
                URI uri = URI.create(location);
                bytes = s3().getObjectAsBytes(GetObjectRequest.builder()
                        .bucket(uri.getHost()).key(uri.getPath().substring(1)).build()).asByteArray();
            } else {
                bytes = Files.readAllBytes(location.startsWith("file:")
                        ? Path.of(URI.create(location)) : Path.of(location));
            }
            JsonNode fence = JSON.readTree(bytes);
            if (fence == null || !fence.isObject() || !fence.path("version").isIntegralNumber()
                    || !fence.path("version").canConvertToInt()
                    || fence.path("version").asInt() != 1
                    || !id.equals(fence.path("execution_id").asText())
                    || !("open".equals(fence.path("state").asText())
                        || (allowClosed && "closed".equals(fence.path("state").asText())))) {
                throw new IllegalStateException("Execution fence does not authorize this runtime operation");
            }
        } catch (Exception failure) {
            throw new IllegalStateException("Execution fence rejected runtime " + id, failure);
        }
    }

    private synchronized S3Client s3() {
        if (s3 == null) {
            S3ClientBuilder builder = S3Client.builder();
            String region = System.getenv("OLAKE_S3_REGION");
            boolean captured = "true".equals(System.getenv("OLAKE_EXECUTION_AWS_CAPTURED"));
            String profile = capturedEnv("PROFILE", "default");
            if (captured && (region == null || region.isBlank())) {
                region = capturedEnv("REGION", capturedEnv("DEFAULT_REGION", ""));
                if (region.isBlank()) {
                    region = ProfileFile.defaultProfileFile().profile(profile)
                            .flatMap(value -> value.property("region")).orElse("");
                }
                if (region.isBlank()) { region = new InstanceProfileRegionProvider().getRegion().id(); }
            }
            if (region != null && !region.isBlank()) { builder.region(Region.of(region)); }
            String access = System.getenv("OLAKE_S3_ACCESS_KEY_ID");
            String secret = System.getenv("OLAKE_S3_SECRET_ACCESS_KEY");
            if (access != null && !access.isBlank() && secret != null && !secret.isBlank()) {
                builder.credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(access, secret)));
            } else if (captured) {
                access = capturedEnv("ACCESS_KEY_ID", "");
                secret = capturedEnv("SECRET_ACCESS_KEY", "");
                String token = capturedEnv("SESSION_TOKEN", "");
                if (!access.isBlank() && !secret.isBlank()) {
                    builder.credentialsProvider(StaticCredentialsProvider.create(token.isBlank()
                            ? AwsBasicCredentials.create(access, secret)
                            : AwsSessionCredentials.create(access, secret, token)));
                } else {
                    builder.credentialsProvider(AwsCredentialsProviderChain.builder().credentialsProviders(
                            WebIdentityTokenFileCredentialsProvider.create(),
                            ProfileCredentialsProvider.builder().profileName(profile).build(),
                            ContainerCredentialsProvider.builder().build(),
                            InstanceProfileCredentialsProvider.create()).build());
                }
            }
            String endpoint = System.getenv("OLAKE_S3_ENDPOINT");
            if (endpoint != null && !endpoint.isBlank()) {
                builder.endpointOverride(URI.create(endpoint)).forcePathStyle(true);
            }
            s3 = builder.build();
        }
        return s3;
    }
    private static String capturedEnv(String name, String fallback) {
        String value = System.getenv("OLAKE_EXECUTION_AWS_" + name);
        return value == null || value.isBlank() ? fallback : value;
    }
}
