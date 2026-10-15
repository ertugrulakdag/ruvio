// SPDX-License-Identifier: MIT
package io.github.salihcantekin.ruvio.args;

import io.github.salihcantekin.ruvio.internal.Args;

/** RANK and MAXLEN options of LPOS. */
public final class LPosArgs {
    private long rank;
    private long maxLen = -1;

    public static LPosArgs rank(long rank) {
        LPosArgs a = new LPosArgs();
        a.rank = rank;
        return a;
    }

    public static LPosArgs maxLen(long maxLen) {
        return new LPosArgs().withMaxLen(maxLen);
    }

    public LPosArgs withRank(long rank) {
        this.rank = rank;
        return this;
    }

    public LPosArgs withMaxLen(long maxLen) {
        this.maxLen = maxLen;
        return this;
    }

    public void write(Args a) {
        if (rank != 0) a.s("RANK").l(rank);
        if (maxLen >= 0) a.s("MAXLEN").l(maxLen);
    }
}
