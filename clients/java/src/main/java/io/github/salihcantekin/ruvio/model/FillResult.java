// SPDX-License-Identifier: MIT
package io.github.salihcantekin.ruvio.model;

import io.github.salihcantekin.ruvio.internal.Reply;

/**
 * Reply of {@code FILLGET}. {@code HIT}: {@link #value()} is fresh. {@code FILL}: this caller must
 * build the value and call FILLSET with {@link #token()}. {@code BUSY}: another caller is filling.
 * {@code STALE}: {@link #value()} is stale; a non-null {@link #token()} means this caller should
 * refresh it.
 */
public final class FillResult<V> {
    public enum State { HIT, FILL, BUSY, STALE }

    private final State state;
    private final V value;
    private final String token;

    public FillResult(State state, V value, String token) {
        this.state = state;
        this.value = value;
        this.token = token;
    }

    public State state() {
        return state;
    }

    public V value() {
        return value;
    }

    public String token() {
        return token;
    }

    @Override
    public String toString() {
        return "FillResult{" + state + ", value=" + value + ", token=" + token + "}";
    }
}
