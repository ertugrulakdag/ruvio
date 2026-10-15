// SPDX-License-Identifier: MIT
package io.github.salihcantekin.ruvio.args;

import io.github.salihcantekin.ruvio.internal.Args;
import io.github.salihcantekin.ruvio.internal.Cmd;

/** COUNT / BLOCK (and NOACK for XREADGROUP). Blocking reads should use a dedicated client. */
public final class XReadArgs {
    private long count;
    private long blockMillis = -1;
    private boolean noAck;

    public static XReadArgs builder() {
        return new XReadArgs();
    }

    public XReadArgs count(long count) {
        this.count = count;
        return this;
    }

    /** BLOCK milliseconds (0 waits forever). It parks the whole connection; use {@code dedicated()}. */
    public XReadArgs block(long millis) {
        this.blockMillis = millis;
        return this;
    }

    public XReadArgs noAck() {
        noAck = true;
        return this;
    }

    public void write(Args a, boolean group) {
        if (count > 0) a.w(Cmd.COUNT).l(count);
        if (blockMillis >= 0) a.s("BLOCK").l(blockMillis);
        if (group && noAck) a.s("NOACK");
    }
}
