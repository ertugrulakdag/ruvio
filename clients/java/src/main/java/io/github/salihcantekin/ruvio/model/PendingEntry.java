// SPDX-License-Identifier: MIT
package io.github.salihcantekin.ruvio.model;

/** One row of the extended XPENDING form. */
public final class PendingEntry {
    private final String id;
    private final String consumer;
    private final long idleMillis;
    private final long deliveryCount;

    /**
     * Creates a row.
     *
     * @param id the entry id
     * @param consumer the consumer that owns the entry
     * @param idleMillis milliseconds since it was last delivered
     * @param deliveryCount how many times it was delivered
     */
    public PendingEntry(String id, String consumer, long idleMillis, long deliveryCount) {
        this.id = id;
        this.consumer = consumer;
        this.idleMillis = idleMillis;
        this.deliveryCount = deliveryCount;
    }

    /**
     * Returns the entry id.
     *
     * @return the id such as {@code 1700000000000-0}
     */
    public String id() {
        return id;
    }

    /**
     * Returns the consumer that currently owns the entry.
     *
     * @return the consumer name
     */
    public String consumer() {
        return consumer;
    }

    /**
     * Returns the time since the entry was last delivered.
     *
     * @return the idle time in milliseconds
     */
    public long idleMillis() {
        return idleMillis;
    }

    /**
     * Returns how often the entry was delivered.
     *
     * @return the delivery count
     */
    public long deliveryCount() {
        return deliveryCount;
    }

    /**
     * Returns a short readable form, for logs.
     *
     * @return the text
     */
    @Override
    public String toString() {
        return "PendingEntry{" + id + ", " + consumer + ", idle=" + idleMillis + ", deliveries=" + deliveryCount + "}";
    }
}
