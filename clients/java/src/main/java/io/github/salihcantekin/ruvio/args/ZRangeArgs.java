// SPDX-License-Identifier: MIT
package io.github.salihcantekin.ruvio.args;

import io.github.salihcantekin.ruvio.internal.Args;
import io.github.salihcantekin.ruvio.internal.Cmd;

/** BYSCORE / BYLEX / REV / LIMIT options of the unified ZRANGE. */
public final class ZRangeArgs {
    private boolean byScore;
    private boolean byLex;
    private boolean rev;
    private boolean limit;
    private long offset;
    private long count;

    public static ZRangeArgs builder() {
        return new ZRangeArgs();
    }

    public ZRangeArgs byScore() {
        byScore = true;
        return this;
    }

    public ZRangeArgs byLex() {
        byLex = true;
        return this;
    }

    public ZRangeArgs rev() {
        rev = true;
        return this;
    }

    /** LIMIT offset count (BYSCORE/BYLEX only). A negative count returns the rest of the range. */
    public ZRangeArgs limit(long offset, long count) {
        this.limit = true;
        this.offset = offset;
        this.count = count;
        return this;
    }

    public void write(Args a) {
        if (byScore) a.s("BYSCORE");
        if (byLex) a.s("BYLEX");
        if (rev) a.s("REV");
        if (limit) a.w(Cmd.LIMIT).l(offset).l(count);
    }
}
