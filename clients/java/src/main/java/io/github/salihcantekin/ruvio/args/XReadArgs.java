// SPDX-License-Identifier: MIT
package io.github.salihcantekin.ruvio.args;

import io.github.salihcantekin.ruvio.internal.Args;
import io.github.salihcantekin.ruvio.internal.Cmd;

/**
 * Options of {@code XREAD} and {@code XREADGROUP}: {@code COUNT}, {@code BLOCK} and ({@code XREADGROUP} only) {@code NOACK}. Blocking reads should use a {@code dedicated()} client.
 *
 * <p>{@code XReadArgs.builder().count(10).block(5_000)}
 */
public final class XReadArgs {
    /** Creates an empty option set; the static factories are the usual way in. */
    public XReadArgs() {
    }

    private long count;
    private long blockMillis = -1;
    private boolean noAck;

    /**
     * Starts an empty option set (no limit, no blocking).
     *
     * @return a new option set
     */
    public static XReadArgs builder() {
        return new XReadArgs();
    }

    /**
     * Sets {@code COUNT}: the most entries to return.
     *
     * @param count the limit (above 0)
     * @return this builder
     */
    public XReadArgs count(long count) {
        this.count = count;
        return this;
    }

    /**
     * Sets {@code BLOCK}: wait for new entries when none are ready. It parks the whole connection; use {@code dedicated()}.
     *
     * @param millis how long to wait in milliseconds; 0 waits forever
     * @return this option set
     */
    public XReadArgs block(long millis) {
        this.blockMillis = millis;
        return this;
    }

    /**
     * Sets {@code NOACK} ({@code XREADGROUP} only): do not add the delivered entries to the pending list.
     *
     * @return this builder
     */
    public XReadArgs noAck() {
        noAck = true;
        return this;
    }

    /**
     * Appends these options to a command being encoded. Used by the client; you do not call it.
     *
     * @param a the argument writer
     * @param group {@code true} for {@code XREADGROUP}, which also allows {@code NOACK}
     */
    public void write(Args a, boolean group) {
        if (count > 0) a.w(Cmd.COUNT).l(count);
        if (blockMillis >= 0) a.s("BLOCK").l(blockMillis);
        if (group && noAck) a.s("NOACK");
    }
}
