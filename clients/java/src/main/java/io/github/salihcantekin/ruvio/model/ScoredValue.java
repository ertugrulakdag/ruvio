// SPDX-License-Identifier: MIT
package io.github.salihcantekin.ruvio.model;

import java.util.Objects;

/**
 * A sorted-set member with its score.
 *
 * @param <V> the member type
 */
public final class ScoredValue<V> {
    private final V value;
    private final double score;

    /**
     * Creates a pair.
     *
     * @param value the member
     * @param score the score
     */
    public ScoredValue(V value, double score) {
        this.value = value;
        this.score = score;
    }

    /**
     * Returns the member.
     *
     * @return the member
     */
    public V value() {
        return value;
    }

    /**
     * Returns the score.
     *
     * @return the score
     */
    public double score() {
        return score;
    }

    /**
     * Compares member and score; byte arrays compare by content.
     *
     * @param o the other object
     * @return {@code true} when both are equal
     */
    @Override
    public boolean equals(Object o) {
        if (!(o instanceof ScoredValue)) {
            return false;
        }
        ScoredValue<?> other = (ScoredValue<?>) o;
        return Double.compare(score, other.score) == 0 && Objects.deepEquals(value, other.value);
    }

    /**
     * Returns a hash of member and score.
     *
     * @return the hash code
     */
    @Override
    public int hashCode() {
        return Objects.hash(Objects.hashCode(value), score);
    }

    /**
     * Returns a short readable form, for logs.
     *
     * @return the text
     */
    @Override
    public String toString() {
        return value + "=" + score;
    }
}
