package io.olake.iceberg;

/** JVM-wide catalog dispatch authority. Admission and terminal snapshot share one lock. */
public final class ExecutionRetirement {
    private boolean sealed;
    private boolean started;
    private boolean unknown;
    private int active;

    public synchronized void requireAdmission() {
        if (sealed) { throw new IllegalStateException("Execution mutation admission is sealed"); }
    }

    public synchronized void begin() {
        requireAdmission();
        started = true;
        active++;
    }

    public synchronized void complete(boolean definitive) {
        if (!definitive) { unknown = true; }
        active--;
    }

    /** Unsupported transports cannot prove remote completion, even on a local return. */
    public synchronized void unsupportedMutation() {
        requireAdmission();
        started = true;
        unknown = true;
    }

    public synchronized String seal() {
        sealed = true;
        if (active != 0) { unknown = true; }
        return unknown ? "unknown" : started ? "settled" : "not_started";
    }
}
