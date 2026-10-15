// SPDX-License-Identifier: MIT
package io.github.salihcantekin.ruvio.args;

import io.github.salihcantekin.ruvio.internal.Args;

/** NX / XX / GT / LT / CH flags of ZADD. */
public final class ZAddArgs {
    private boolean nx;
    private boolean xx;
    private boolean gt;
    private boolean lt;
    private boolean ch;

    public static ZAddArgs builder() {
        return new ZAddArgs();
    }

    public ZAddArgs nx() {
        nx = true;
        return this;
    }

    public ZAddArgs xx() {
        xx = true;
        return this;
    }

    public ZAddArgs gt() {
        gt = true;
        return this;
    }

    public ZAddArgs lt() {
        lt = true;
        return this;
    }

    /** CH: count changed members, not only new ones. */
    public ZAddArgs ch() {
        ch = true;
        return this;
    }

    public void write(Args a) {
        if (nx) a.s("NX");
        if (xx) a.s("XX");
        if (gt) a.s("GT");
        if (lt) a.s("LT");
        if (ch) a.s("CH");
    }
}
