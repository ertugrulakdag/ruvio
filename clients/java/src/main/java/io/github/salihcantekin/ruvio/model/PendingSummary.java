// SPDX-License-Identifier: MIT
package io.github.salihcantekin.ruvio.model;

import java.util.Map;

/** The summary form of XPENDING. Smallest/greatest ids are null for an empty group. */
public final class PendingSummary {
    private final long count;
    private final String smallestId;
    private final String greatestId;
    private final Map<String, Long> consumers;

    /**
     * Creates a summary.
     *
     * @param count the number of pending entries
     * @param smallestId the smallest pending id, or {@code null}
     * @param greatestId the greatest pending id, or {@code null}
     * @param consumers pending count per consumer
     */
    public PendingSummary(long count, String smallestId, String greatestId, Map<String, Long> consumers) {
        this.count = count;
        this.smallestId = smallestId;
        this.greatestId = greatestId;
        this.consumers = consumers;
    }

    /**
     * Returns the number of pending entries in the group.
     *
     * @return the count
     */
    public long count() {
        return count;
    }

    /**
     * Returns the smallest pending id.
     *
     * @return the id, or {@code null} for an empty group
     */
    public String smallestId() {
        return smallestId;
    }

    /**
     * Returns the greatest pending id.
     *
     * @return the id, or {@code null} for an empty group
     */
    public String greatestId() {
        return greatestId;
    }

    /**
     * Returns the pending count per consumer.
     *
     * @return consumer name to pending count, sorted by name
     */
    public Map<String, Long> consumers() {
        return consumers;
    }

    /**
     * Returns a short readable form, for logs.
     *
     * @return the text
     */
    @Override
    public String toString() {
        return "PendingSummary{" + count + ", " + smallestId + ".." + greatestId + ", " + consumers + "}";
    }
}
