// SPDX-License-Identifier: MIT
package io.github.salihcantekin.ruvio.args;

import io.github.salihcantekin.ruvio.internal.Args;

/**
 * Options of {@code XCLAIM}: how to set the claimed entries' delivery state.
 *
 * <p>{@code XClaimArgs.builder().idle(0).retryCount(1)}
 */
public final class XClaimArgs {
    /** Creates an empty option set; the static factories are the usual way in. */
    public XClaimArgs() {
    }

    private long idle = -1;
    private long time = -1;
    private long retryCount = -1;
    private boolean force;
    private String lastId;

    /**
     * Starts an empty option set.
     *
     * @return a new option set
     */
    public static XClaimArgs builder() {
        return new XClaimArgs();
    }

    /**
     * Sets {@code IDLE}: the idle time given to the claimed entries.
     *
     * @param millis the idle time in milliseconds
     * @return this builder
     */
    public XClaimArgs idle(long millis) {
        idle = millis;
        return this;
    }

    /**
     * Sets {@code TIME}: the last-delivery time given to the claimed entries.
     *
     * @param unixMillis an absolute Unix time in milliseconds
     * @return this builder
     */
    public XClaimArgs time(long unixMillis) {
        time = unixMillis;
        return this;
    }

    /**
     * Sets {@code RETRYCOUNT}: the delivery counter given to the claimed entries.
     *
     * @param count the delivery count
     * @return this builder
     */
    public XClaimArgs retryCount(long count) {
        retryCount = count;
        return this;
    }

    /**
     * Sets {@code FORCE}: claim ids that are not pending yet, if they exist in the stream.
     *
     * @return this builder
     */
    public XClaimArgs force() {
        force = true;
        return this;
    }

    /**
     * Sets {@code LASTID}: advance the group's last delivered id.
     *
     * @param id the stream id
     * @return this builder
     */
    public XClaimArgs lastId(String id) {
        lastId = id;
        return this;
    }

    /**
     * Appends these options to a command being encoded. Used by the client; you do not call it.
     *
     * @param a the argument writer
     */
    public void write(Args a) {
        if (idle >= 0) a.s("IDLE").l(idle);
        if (time >= 0) a.s("TIME").l(time);
        if (retryCount >= 0) a.s("RETRYCOUNT").l(retryCount);
        if (force) a.s("FORCE");
        if (lastId != null) a.s("LASTID").s(lastId);
    }
}
