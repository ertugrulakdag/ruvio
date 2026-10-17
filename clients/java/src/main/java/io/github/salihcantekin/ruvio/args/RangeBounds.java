// SPDX-License-Identifier: MIT
package io.github.salihcantekin.ruvio.args;

/**
 * Builders for the bound strings of score ranges ({@code ZRANGEBYSCORE}, {@code ZCOUNT}) and lexicographic ranges ({@code ZRANGEBYLEX}, {@code ZLEXCOUNT}).
 * Each returns the exact string the range methods take.
 *
 * <p>Example: {@code client.zrangebyscore("board", RangeBounds.NEGATIVE_INFINITY, RangeBounds.exclusive(100))}
 */
public final class RangeBounds {
    private RangeBounds() {
    }

    /** {@code -inf}: below every score. */
    public static final String NEGATIVE_INFINITY = "-inf";
    /** {@code +inf}: above every score. */
    public static final String POSITIVE_INFINITY = "+inf";
    /** {@code -}: before every member in a lexicographic range. */
    public static final String LEX_MIN = "-";
    /** {@code +}: after every member in a lexicographic range. */
    public static final String LEX_MAX = "+";

    /**
     * Returns an exclusive score bound.
     *
     * @param score the score that is not part of the range
     * @return {@code (score}, for example {@code (1.5}
     */
    public static String exclusive(double score) {
        return "(" + score;
    }

    /**
     * Returns an exclusive lexicographic bound.
     *
     * @param member the member that is not part of the range
     * @return {@code (member}
     */
    public static String exclusive(String member) {
        return "(" + member;
    }

    /**
     * Returns an inclusive lexicographic bound (a score bound is inclusive without a prefix).
     *
     * @param member the member that is part of the range
     * @return {@code [member}
     */
    public static String inclusive(String member) {
        return "[" + member;
    }
}
