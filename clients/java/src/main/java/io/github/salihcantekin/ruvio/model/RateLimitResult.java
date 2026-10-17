// SPDX-License-Identifier: MIT
package io.github.salihcantekin.ruvio.model;

/** Outcome of the LIMIT rate-limit helper. */
public final class RateLimitResult {
    private final boolean allowed;
    private final long remaining;

    /**
     * Creates a result.
     *
     * @param allowed whether the call fits
     * @param remaining calls left in the window
     */
    public RateLimitResult(boolean allowed, long remaining) {
        this.allowed = allowed;
        this.remaining = remaining;
    }

    /**
     * Tells whether the call fit in the window.
     *
     * @return {@code true} when allowed, {@code false} when rejected
     */
    public boolean allowed() {
        return allowed;
    }

    /**
     * Returns the calls left in the current window after this one.
     *
     * @return the remaining calls; 0 when rejected
     */
    public long remaining() {
        return remaining;
    }

    /**
     * Returns a short readable form, for logs.
     *
     * @return the text
     */
    @Override
    public String toString() {
        return allowed ? "allowed, " + remaining + " left" : "rejected";
    }
}
