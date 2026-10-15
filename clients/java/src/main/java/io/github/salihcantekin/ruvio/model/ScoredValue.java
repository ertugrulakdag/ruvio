// SPDX-License-Identifier: MIT
package io.github.salihcantekin.ruvio.model;

import java.util.Objects;

/** A sorted-set member with its score. */
public final class ScoredValue<V> {
    private final V value;
    private final double score;

    public ScoredValue(V value, double score) {
        this.value = value;
        this.score = score;
    }

    public V value() {
        return value;
    }

    public double score() {
        return score;
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof ScoredValue)) {
            return false;
        }
        ScoredValue<?> other = (ScoredValue<?>) o;
        return Double.compare(score, other.score) == 0 && Objects.deepEquals(value, other.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(Objects.hashCode(value), score);
    }

    @Override
    public String toString() {
        return value + "=" + score;
    }
}
