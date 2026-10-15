// SPDX-License-Identifier: MIT
package io.github.salihcantekin.ruvio.model;

/** One row of the extended XPENDING form. */
public final class PendingEntry {
    private final String id;
    private final String consumer;
    private final long idleMillis;
    private final long deliveryCount;

    public PendingEntry(String id, String consumer, long idleMillis, long deliveryCount) {
        this.id = id;
        this.consumer = consumer;
        this.idleMillis = idleMillis;
        this.deliveryCount = deliveryCount;
    }

    public String id() {
        return id;
    }

    public String consumer() {
        return consumer;
    }

    public long idleMillis() {
        return idleMillis;
    }

    public long deliveryCount() {
        return deliveryCount;
    }

    @Override
    public String toString() {
        return "PendingEntry{" + id + ", " + consumer + ", idle=" + idleMillis + ", deliveries=" + deliveryCount + "}";
    }
}
