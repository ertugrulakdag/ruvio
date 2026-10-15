// SPDX-License-Identifier: MIT
package io.github.salihcantekin.ruvio.model;

/** Outcome of the LIMIT rate-limit helper. */
public final class RateLimitResult {
    private final boolean allowed;
    private final long remaining;

    public RateLimitResult(boolean allowed, long remaining) {
        this.allowed = allowed;
        this.remaining = remaining;
    }

    public boolean allowed() {
        return allowed;
    }

    /** Calls left in the current window after this one (0 when rejected). */
    public long remaining() {
        return remaining;
    }

    @Override
    public String toString() {
        return allowed ? "allowed, " + remaining + " left" : "rejected";
    }
}
