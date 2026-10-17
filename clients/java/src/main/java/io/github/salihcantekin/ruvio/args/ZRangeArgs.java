// SPDX-License-Identifier: MIT
package io.github.salihcantekin.ruvio.args;

import io.github.salihcantekin.ruvio.internal.Args;
import io.github.salihcantekin.ruvio.internal.Cmd;

/**
 * Options of the unified {@code ZRANGE}: range by score or by lexicographic order, reversed order, and a {@code LIMIT} window. Without any option the range is by rank.
 *
 * <p>{@code ZRangeArgs.builder().byScore().rev().limit(0, 10)}
 */
public final class ZRangeArgs {
    /** Creates an empty option set; the static factories are the usual way in. */
    public ZRangeArgs() {
    }

    private boolean byScore;
    private boolean byLex;
    private boolean rev;
    private boolean limit;
    private long offset;
    private long count;

    /**
     * Starts an empty option set (range by rank, ascending).
     *
     * @return a new option set
     */
    public static ZRangeArgs builder() {
        return new ZRangeArgs();
    }

    /**
     * Sets {@code BYSCORE}: the bounds are scores ({@code 1.5}, {@code (1.5} exclusive, {@code -inf}, {@code +inf}).
     *
     * @return this builder
     */
    public ZRangeArgs byScore() {
        byScore = true;
        return this;
    }

    /**
     * Sets {@code BYLEX}: the bounds are lexicographic ({@code -}, {@code +}, {@code [a} inclusive, {@code (a} exclusive); meaningful when all scores are equal.
     *
     * @return this builder
     */
    public ZRangeArgs byLex() {
        byLex = true;
        return this;
    }

    /**
     * Sets {@code REV}: reverse the order. With {@code byScore()} or {@code byLex()} give the higher bound first.
     *
     * @return this builder
     */
    public ZRangeArgs rev() {
        rev = true;
        return this;
    }

    /**
     * Sets {@code LIMIT offset count} ({@code byScore()}/{@code byLex()} only).
     *
     * @param offset how many matching members to skip
     * @param count the most members to return; a negative value returns all the rest
     * @return this option set
     */
    public ZRangeArgs limit(long offset, long count) {
        this.limit = true;
        this.offset = offset;
        this.count = count;
        return this;
    }

    /**
     * Appends these options to a command being encoded. Used by the client; you do not call it.
     *
     * @param a the argument writer
     */
    public void write(Args a) {
        if (byScore) a.s("BYSCORE");
        if (byLex) a.s("BYLEX");
        if (rev) a.s("REV");
        if (limit) a.w(Cmd.LIMIT).l(offset).l(count);
    }
}
