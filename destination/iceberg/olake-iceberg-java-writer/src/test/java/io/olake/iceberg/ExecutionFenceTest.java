package io.olake.iceberg;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ExecutionFenceTest {
    @TempDir Path directory;

    @Test
    void freshPositiveReadIsRequiredEveryTime() throws Exception {
        Path path = directory.resolve("fence.json");
        ExecutionFence fence = new ExecutionFence(Map.of("execution-id", "runtime", "execution-fence", path.toString()));
        assertThrows(IllegalStateException.class, fence::check);
        Files.writeString(path, "{\"version\":1,\"execution_id\":\"runtime\",\"state\":\"open\"}");
        assertDoesNotThrow(fence::check);
        Files.writeString(path, "{\"version\":1,\"execution_id\":\"other\",\"state\":\"open\"}");
        assertThrows(IllegalStateException.class, fence::check);
        Files.writeString(path, "{\"version\":1,\"execution_id\":\"runtime\",\"state\":\"closed\"}");
        assertThrows(IllegalStateException.class, fence::check);
        assertDoesNotThrow(fence::checkReconciliation);
        Files.writeString(path, "not json");
        assertThrows(IllegalStateException.class, fence::check);
        Files.delete(path);
        assertThrows(IllegalStateException.class, fence::check);
        assertThrows(IllegalStateException.class, fence::checkReconciliation);
    }

    @Test
    void terminalProofUsesStartupIdentityAndAllowsOnlyMatchingClosedFence() throws Exception {
        Path path = directory.resolve("terminal-fence.json");
        for (String command : java.util.List.of("sync", "clear-destination")) {
            ExecutionFence fence = new ExecutionFence(Map.of("execution-id", "runtime", "execution-fence", path.toString(),
                    "execution-launch-id", "admitted-launch", "execution-command", command));
            Files.writeString(path, "{\"version\":1,\"execution_id\":\"runtime\",\"state\":\"closed\"}");
            com.fasterxml.jackson.databind.JsonNode proof = new com.fasterxml.jackson.databind.ObjectMapper().readTree(fence.retireJson());
            org.junit.jupiter.api.Assertions.assertEquals(1, proof.path("version").asInt());
            org.junit.jupiter.api.Assertions.assertEquals("runtime", proof.path("execution_id").asText());
            org.junit.jupiter.api.Assertions.assertEquals("admitted-launch", proof.path("launch_id").asText());
            org.junit.jupiter.api.Assertions.assertEquals(command, proof.path("command").asText());
            org.junit.jupiter.api.Assertions.assertEquals("not_started", proof.path("outcome").asText());
            assertThrows(IllegalStateException.class, () -> fence.retirement().begin());
            Files.writeString(path, "{\"version\":1,\"execution_id\":\"other\",\"state\":\"closed\"}");
            assertThrows(IllegalStateException.class, fence::retireJson);
            Files.delete(path);
            assertThrows(IllegalStateException.class, fence::retireJson);
        }
    }

    @Test
    void terminalSealPreventsMutationEvenWithOpenStorageFence() throws Exception {
        Path path = directory.resolve("open-fence.json");
        Files.writeString(path, "{\"version\":1,\"execution_id\":\"runtime\",\"state\":\"open\"}");
        try {
            ExecutionFence.configure(Map.of("execution-id", "runtime", "execution-fence", path.toString(),
                    "execution-launch-id", "runtime", "execution-command", "sync"));
            ExecutionFence.runtime().retireJson();
            assertThrows(IllegalStateException.class, ExecutionFence::requireMutation);
            assertDoesNotThrow(ExecutionFence.runtime()::checkReconciliation);
        } finally {
            ExecutionFence.configure(Map.of());
        }
    }

    @Test
    void unsupportedAuthorityCannotFalselySettle() {
        ExecutionRetirement retirement = new ExecutionRetirement();
        retirement.unsupportedMutation();
        org.junit.jupiter.api.Assertions.assertEquals("unknown", retirement.seal());
        assertThrows(IllegalStateException.class, retirement::unsupportedMutation);
    }

    @Test
    void terminalRejectsMissingOrNonordinaryStartupIdentity() throws Exception {
        assertThrows(IllegalStateException.class, () -> new ExecutionFence(Map.of()).retireJson());
        ExecutionFence fence = new ExecutionFence(Map.of("execution-launch-id", "launch", "execution-command", "resync"));
        assertThrows(IllegalStateException.class, fence::retireJson);
    }

    @Test
    void partialConfigurationFailsClosedAndStandaloneRemainsSupported() {
        assertThrows(IllegalArgumentException.class, () -> new ExecutionFence(Map.of("execution-id", "runtime")));
        assertThrows(IllegalArgumentException.class, () -> new ExecutionFence(Map.of("execution-fence", "path")));
        assertDoesNotThrow(() -> new ExecutionFence(Map.of()).check());
    }
}
