package io.olake.iceberg;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.time.Duration;
import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import jdk.jfr.RecordingState;
import jdk.jfr.consumer.RecordingFile;

class RuntimeRecordingTest {
    @TempDir
    Path directory;

    @Test
    void requiresExplicitDirectoryAndBoundedDuration() throws Exception {
        for (Map<String, String> env : Set.of(Map.<String, String>of(),
                Map.of("OLAKE_PROFILE_DIR", directory.toString()),
                Map.of("OLAKE_JFR_PROFILE_SECONDS", "1"))) {
            try (RuntimeRecording profile = RuntimeRecording.start(env)) {
                assertNull(profile.recording());
            }
        }
        for (String seconds : Set.of("0", "-1", "301", "1.5", "NaN", "private-value")) {
            try (RuntimeRecording profile = RuntimeRecording.start(Map.of(
                    "OLAKE_PROFILE_DIR", directory.toString(), "OLAKE_JFR_PROFILE_SECONDS", seconds))) {
                assertNull(profile.recording());
            }
        }
        try (var files = Files.list(directory)) {
            assertEquals(0, files.count());
        }
    }

    @Test
    void usesPrivateBoundedRuntimeOnlyRecordingAndDumpsAtShutdown() throws Exception {
        Path destination;
        Set<String> allowedEvents = Set.of("jdk.ExecutionSample", "jdk.NativeMethodSample", "jdk.CPULoad",
                "jdk.GarbageCollection", "jdk.GCHeapSummary");
        try (RuntimeRecording profile = RuntimeRecording.start(Map.of(
                "OLAKE_PROFILE_DIR", directory.toString(), "OLAKE_JFR_PROFILE_SECONDS", "300"))) {
            var recording = profile.recording();
            assertNotNull(recording);
            assertEquals(RecordingState.RUNNING, recording.getState());
            assertEquals(Duration.ofSeconds(300), recording.getDuration());
            assertEquals(Duration.ofSeconds(300), recording.getMaxAge());
            assertEquals(64L * 1024 * 1024, recording.getMaxSize());
            assertTrue(recording.isToDisk());
            assertTrue(recording.getDumpOnExit());
            recording.getSettings().forEach((key, value) -> {
                if (key.endsWith("#enabled") && value.equals("true")) {
                    assertTrue(allowedEvents.contains(key.substring(0, key.indexOf('#'))), key);
                }
            });
            destination = recording.getDestination();
            assertEquals(directory, destination.getParent());
            assertEquals(PosixFilePermissions.fromString("rw-------"), Files.getPosixFilePermissions(destination));
        }
        assertTrue(Files.size(destination) > 0);
        for (var event : RecordingFile.readAllEvents(destination)) {
            assertTrue(allowedEvents.contains(event.getEventType().getName()), event.getEventType().getName());
        }
        assertFalse(destination.toString().contains("private-value"));
    }

    @Test
    void inaccessibleProfileLocationIsNonfatal() throws Exception {
        Path notDirectory = Files.createFile(directory.resolve("file"));
        try (RuntimeRecording profile = RuntimeRecording.start(Map.of(
                "OLAKE_PROFILE_DIR", notDirectory.toString(), "OLAKE_JFR_PROFILE_SECONDS", "1"))) {
            assertNull(profile.recording());
        }
    }
}
