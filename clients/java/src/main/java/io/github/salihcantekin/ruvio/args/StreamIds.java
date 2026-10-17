// SPDX-License-Identifier: MIT
package io.github.salihcantekin.ruvio.args;

/**
 * Special stream ids accepted by the stream commands. Each is the exact string the id parameters take.
 *
 * <p>Example: {@code client.xgroupCreate("jobs", "workers", StreamIds.LAST_DELIVERED)}
 */
public final class StreamIds {
    private StreamIds() {
    }

    /** {@code *}: let the server generate the id (XADD). */
    public static final String NEW_ENTRY = "*";
    /** {@code $}: only entries added after now (XGROUP CREATE, XREAD). */
    public static final String LAST_DELIVERED = "$";
    /** {@code >}: entries never delivered to another consumer (XREADGROUP). */
    public static final String NEW_MESSAGES = ">";
    /** {@code -}: the smallest possible id (XRANGE start). */
    public static final String MIN = "-";
    /** {@code +}: the largest possible id (XRANGE end). */
    public static final String MAX = "+";
}
