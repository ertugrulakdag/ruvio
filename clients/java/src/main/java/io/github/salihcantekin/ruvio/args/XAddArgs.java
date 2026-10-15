// SPDX-License-Identifier: MIT
package io.github.salihcantekin.ruvio.args;

import io.github.salihcantekin.ruvio.internal.Args;

/** XADD options: id, NOMKSTREAM, MAXLEN / MINID trimming and the Ruvio DELAY hold. */
public final class XAddArgs {
    private String id = "*";
    private boolean noMkStream;
    private long maxLen = -1;
    private String minId;
    private boolean approximate;
    private long delayMillis = -1;

    public static XAddArgs builder() {
        return new XAddArgs();
    }

    public XAddArgs id(String id) {
        this.id = id;
        return this;
    }

    public XAddArgs noMkStream() {
        noMkStream = true;
        return this;
    }

    /** MAXLEN [~] n. Ruvio trims exactly either way. */
    public XAddArgs maxLen(long maxLen, boolean approximate) {
        this.maxLen = maxLen;
        this.approximate = approximate;
        return this;
    }

    public XAddArgs maxLen(long maxLen) {
        return maxLen(maxLen, false);
    }

    public XAddArgs minId(String minId) {
        this.minId = minId;
        return this;
    }

    /** Ruvio DELAY: hide the entry from XREADGROUP for this many milliseconds. */
    public XAddArgs delay(long millis) {
        this.delayMillis = millis;
        return this;
    }

    public void write(Args a) {
        if (noMkStream) a.s("NOMKSTREAM");
        if (delayMillis >= 0) a.s("DELAY").l(delayMillis);
        if (maxLen >= 0) {
            a.s("MAXLEN");
            if (approximate) a.s("~");
            a.l(maxLen);
        } else if (minId != null) {
            a.s("MINID");
            if (approximate) a.s("~");
            a.s(minId);
        }
        a.s(id);
    }
}
