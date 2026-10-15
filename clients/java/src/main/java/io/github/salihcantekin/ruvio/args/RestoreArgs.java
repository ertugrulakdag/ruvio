// SPDX-License-Identifier: MIT
package io.github.salihcantekin.ruvio.args;

import io.github.salihcantekin.ruvio.internal.Args;

/** REPLACE / ABSTTL / IDLETIME / FREQ of RESTORE (IDLETIME and FREQ are accepted and ignored). */
public final class RestoreArgs {
    private boolean replace;
    private boolean absTtl;
    private long idleTime = -1;
    private long freq = -1;

    public static RestoreArgs builder() {
        return new RestoreArgs();
    }

    public RestoreArgs replace() {
        replace = true;
        return this;
    }

    public RestoreArgs absTtl() {
        absTtl = true;
        return this;
    }

    public RestoreArgs idleTime(long seconds) {
        idleTime = seconds;
        return this;
    }

    public RestoreArgs freq(long frequency) {
        freq = frequency;
        return this;
    }

    public void write(Args a) {
        if (replace) a.s("REPLACE");
        if (absTtl) a.s("ABSTTL");
        if (idleTime >= 0) a.s("IDLETIME").l(idleTime);
        if (freq >= 0) a.s("FREQ").l(freq);
    }
}
