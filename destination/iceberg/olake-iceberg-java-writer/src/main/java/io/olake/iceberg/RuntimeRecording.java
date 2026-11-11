package io.olake.iceberg;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.time.Duration;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jdk.jfr.Recording;
import jdk.jfr.RecordingState;

/** Explicit local opt-in; allowlisted runtime events never capture source records or I/O paths. */
public final class RuntimeRecording implements AutoCloseable {
    private static final Logger LOGGER = LoggerFactory.getLogger(RuntimeRecording.class);
    private static final long MAX_BYTES = 64L * 1024 * 1024;
    private final Recording recording;

    private RuntimeRecording(Recording recording) {
        this.recording = recording;
    }

    public static RuntimeRecording start() {
        return start(System.getenv());
    }

    static RuntimeRecording start(Map<String, String> env) {
        String directory = env.getOrDefault("OLAKE_PROFILE_DIR", "");
        String seconds = env.getOrDefault("OLAKE_JFR_PROFILE_SECONDS", "");
        if (directory.isBlank() || seconds.isBlank()) {
            return new RuntimeRecording(null);
        }
        Recording recording = null;
        Path destination = null;
        try {
            int duration = Integer.parseInt(seconds);
            if (duration < 1 || duration > 300) {
                throw new IllegalArgumentException("JFR duration must be between 1 and 300 seconds");
            }
            Path parent = Path.of(directory);
            Files.createDirectories(parent);
            destination = Files.createTempFile(parent, "olake-jvm-", ".jfr",
                    PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rw-------")));
            recording = new Recording();
            recording.setName("olake-runtime");
            recording.setToDisk(true);
            recording.setMaxSize(MAX_BYTES);
            recording.setMaxAge(Duration.ofSeconds(duration));
            recording.setDuration(Duration.ofSeconds(duration));
            recording.setDestination(destination);
            recording.setDumpOnExit(true);
            // Deliberately do not load the JFR default/profile configuration: it includes
            // environment variables, JVM arguments, system properties and file/socket I/O.
            recording.enable("jdk.ExecutionSample").withPeriod(Duration.ofMillis(20)).withStackTrace();
            recording.enable("jdk.NativeMethodSample").withPeriod(Duration.ofMillis(20)).withStackTrace();
            recording.enable("jdk.CPULoad").withPeriod(Duration.ofSeconds(1));
            recording.enable("jdk.GarbageCollection");
            recording.enable("jdk.GCHeapSummary");
            recording.start();
            LOGGER.info("Bounded Java runtime recording enabled: file={} duration_seconds={} max_bytes={}",
                    destination, duration, MAX_BYTES);
            return new RuntimeRecording(recording);
        } catch (IOException | RuntimeException | LinkageError error) {
            if (recording != null) {
                try {
                    recording.close();
                } catch (RuntimeException | LinkageError ignored) {
                    // Failed optional recording cleanup must not fail ingestion.
                }
            }
            if (destination != null) {
                try {
                    Files.deleteIfExists(destination);
                } catch (IOException | SecurityException ignored) {
                    // An empty private file is harmless if profile initialization failed.
                }
            }
            LOGGER.warn("Java runtime recording disabled ({})", error.getClass().getSimpleName());
            return new RuntimeRecording(null);
        }
    }

    Recording recording() {
        return recording;
    }

    @Override
    public void close() {
        if (recording == null) {
            return;
        }
        try {
            if (recording.getState() == RecordingState.RUNNING) {
                recording.stop();
            }
            recording.close();
        } catch (RuntimeException | LinkageError error) {
            LOGGER.warn("Java runtime recording shutdown incomplete ({})", error.getClass().getSimpleName());
        }
    }
}
