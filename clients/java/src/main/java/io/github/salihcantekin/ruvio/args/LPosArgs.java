// SPDX-License-Identifier: MIT
package io.github.salihcantekin.ruvio.args;

import io.github.salihcantekin.ruvio.internal.Args;

/**
 * Options of {@code LPOS}: which match to return ({@code RANK}) and how far to scan ({@code MAXLEN}).
 *
 * <p>{@code LPosArgs.rank(-1)} finds the last match.
 */
public final class LPosArgs {
    /** Creates an empty option set; the static factories are the usual way in. */
    public LPosArgs() {
    }

    private long rank;
    private long maxLen = -1;

    /**
     * Starts with {@code RANK}.
     *
     * @param rank which match to take: 1 is the first, 2 the second; a negative value counts from the tail; 0 is invalid and the server answers {@code ERR}
     * @return a new option set
     */
    public static LPosArgs rank(long rank) {
        LPosArgs a = new LPosArgs();
        a.rank = rank;
        return a;
    }

    /**
     * Starts with {@code MAXLEN}.
     *
     * @param maxLen stop after comparing this many elements; {@code 0} means no limit
     * @return a new option set
     */
    public static LPosArgs maxLen(long maxLen) {
        return new LPosArgs().withMaxLen(maxLen);
    }

    /**
     * Sets {@code RANK}.
     *
     * @param rank which match to take: 1 is the first, 2 the second; a negative value counts from the tail
     * @return this builder
     */
    public LPosArgs withRank(long rank) {
        this.rank = rank;
        return this;
    }

    /**
     * Sets {@code MAXLEN}.
     *
     * @param maxLen stop after comparing this many elements; {@code 0} means no limit
     * @return this builder
     */
    public LPosArgs withMaxLen(long maxLen) {
        this.maxLen = maxLen;
        return this;
    }

    /**
     * Appends these options to a command being encoded. Used by the client; you do not call it.
     *
     * @param a the argument writer
     */
    public void write(Args a) {
        if (rank != 0) a.s("RANK").l(rank);
        if (maxLen >= 0) a.s("MAXLEN").l(maxLen);
    }
}
