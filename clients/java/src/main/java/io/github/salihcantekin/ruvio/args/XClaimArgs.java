// SPDX-License-Identifier: MIT
package io.github.salihcantekin.ruvio.args;

import io.github.salihcantekin.ruvio.internal.Args;

/** IDLE / TIME / RETRYCOUNT / FORCE / LASTID options of XCLAIM. */
public final class XClaimArgs {
    private long idle = -1;
    private long time = -1;
    private long retryCount = -1;
    private boolean force;
    private String lastId;

    public static XClaimArgs builder() {
        return new XClaimArgs();
    }

    public XClaimArgs idle(long millis) {
        idle = millis;
        return this;
    }

    public XClaimArgs time(long unixMillis) {
        time = unixMillis;
        return this;
    }

    public XClaimArgs retryCount(long count) {
        retryCount = count;
        return this;
    }

    public XClaimArgs force() {
        force = true;
        return this;
    }

    public XClaimArgs lastId(String id) {
        lastId = id;
        return this;
    }

    public void write(Args a) {
        if (idle >= 0) a.s("IDLE").l(idle);
        if (time >= 0) a.s("TIME").l(time);
        if (retryCount >= 0) a.s("RETRYCOUNT").l(retryCount);
        if (force) a.s("FORCE");
        if (lastId != null) a.s("LASTID").s(lastId);
    }
}
