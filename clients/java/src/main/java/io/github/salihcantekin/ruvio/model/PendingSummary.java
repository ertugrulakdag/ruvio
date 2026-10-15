// SPDX-License-Identifier: MIT
package io.github.salihcantekin.ruvio.model;

import java.util.Map;

/** The summary form of XPENDING. Smallest/greatest ids are null for an empty group. */
public final class PendingSummary {
    private final long count;
    private final String smallestId;
    private final String greatestId;
    private final Map<String, Long> consumers;

    public PendingSummary(long count, String smallestId, String greatestId, Map<String, Long> consumers) {
        this.count = count;
        this.smallestId = smallestId;
        this.greatestId = greatestId;
        this.consumers = consumers;
    }

    public long count() {
        return count;
    }

    public String smallestId() {
        return smallestId;
    }

    public String greatestId() {
        return greatestId;
    }

    /** Consumer name to pending count, sorted by name. */
    public Map<String, Long> consumers() {
        return consumers;
    }

    @Override
    public String toString() {
        return "PendingSummary{" + count + ", " + smallestId + ".." + greatestId + ", " + consumers + "}";
    }
}
