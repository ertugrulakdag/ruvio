// SPDX-License-Identifier: MIT
package io.github.salihcantekin.ruvio.args;

import io.github.salihcantekin.ruvio.internal.Args;

/** Options of {@code RESTORE}. {@code IDLETIME} and {@code FREQ} are accepted and ignored by Ruvio. */
public final class RestoreArgs {
    /** Creates an empty option set; the static factories are the usual way in. */
    public RestoreArgs() {
    }

    private boolean replace;
    private boolean absTtl;
    private long idleTime = -1;
    private long freq = -1;

    /**
     * Starts an empty option set.
     *
     * @return a new option set
     */
    public static RestoreArgs builder() {
        return new RestoreArgs();
    }

    /**
     * Sets {@code REPLACE}: overwrite an existing key instead of failing with {@code BUSYKEY}.
     *
     * @return this builder
     */
    public RestoreArgs replace() {
        replace = true;
        return this;
    }

    /**
     * Sets {@code ABSTTL}: read the TTL argument as an absolute Unix time in milliseconds.
     *
     * @return this builder
     */
    public RestoreArgs absTtl() {
        absTtl = true;
        return this;
    }

    /**
     * Sets {@code IDLETIME} (accepted and ignored).
     *
     * @param seconds the idle time in seconds
     * @return this builder
     */
    public RestoreArgs idleTime(long seconds) {
        idleTime = seconds;
        return this;
    }

    /**
     * Sets {@code FREQ} (accepted and ignored).
     *
     * @param frequency the access frequency
     * @return this builder
     */
    public RestoreArgs freq(long frequency) {
        freq = frequency;
        return this;
    }

    /**
     * Appends these options to a command being encoded. Used by the client; you do not call it.
     *
     * @param a the argument writer
     */
    public void write(Args a) {
        if (replace) a.s("REPLACE");
        if (absTtl) a.s("ABSTTL");
        if (idleTime >= 0) a.s("IDLETIME").l(idleTime);
        if (freq >= 0) a.s("FREQ").l(freq);
    }
}
