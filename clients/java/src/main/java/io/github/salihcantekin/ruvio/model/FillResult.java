// SPDX-License-Identifier: MIT
package io.github.salihcantekin.ruvio.model;

import io.github.salihcantekin.ruvio.internal.Reply;

/**
 * Reply of {@code FILLGET}, the cache-aside command with stampede protection. {@code HIT}: {@link #value()} is fresh. {@code FILL}: this caller must
 * build the value and call {@code FILLSET} with {@link #token()}. {@code BUSY}: another caller is filling.
 * {@code STALE}: {@link #value()} is stale; a non-null {@link #token()} means this caller should
 * refresh it.
 *
 * @param <V> the value type
 */
public final class FillResult<V> {
    /** The outcome of a {@code FILLGET}. */
    public enum State {
        /** The value is cached and fresh. */
        HIT,
        /** There is no value and this caller is the one chosen to build it; finish with {@code FILLSET}. */
        FILL,
        /** There is no value and another caller is already building it. */
        BUSY,
        /** The value is past its TTL but still served; a token means this caller should refresh it. */
        STALE
    }

    private final State state;
    private final V value;
    private final String token;

    /**
     * Creates a result.
     *
     * @param state the outcome
     * @param value the cached value for {@code HIT} and {@code STALE}, otherwise {@code null}
     * @param token the fill token for {@code FILL} (and {@code STALE} when this caller refreshes), otherwise {@code null}
     */
    public FillResult(State state, V value, String token) {
        this.state = state;
        this.value = value;
        this.token = token;
    }

    /**
     * Returns the outcome.
     *
     * @return the state
     */
    public State state() {
        return state;
    }

    /**
     * Returns the cached value.
     *
     * @return the value for {@code HIT} and {@code STALE}, otherwise {@code null}
     */
    public V value() {
        return value;
    }

    /**
     * Returns the fill token to pass to {@code FILLSET}.
     *
     * @return the token, or {@code null} when this caller must not fill
     */
    public String token() {
        return token;
    }

    /**
     * Returns a short readable form, for logs.
     *
     * @return the text
     */
    @Override
    public String toString() {
        return "FillResult{" + state + ", value=" + value + ", token=" + token + "}";
    }
}
