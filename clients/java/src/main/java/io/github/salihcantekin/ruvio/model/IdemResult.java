// SPDX-License-Identifier: MIT
package io.github.salihcantekin.ruvio.model;

import io.github.salihcantekin.ruvio.internal.Reply;

import java.nio.charset.StandardCharsets;

/** Reply of {@code IDEM key BEGIN}: the reservation state and, when completed, the stored result. */
public final class IdemResult {
    /** The outcome of an {@code IDEM key BEGIN}. */
    public enum State {
        /** This caller owns the key and should run the action, then call {@code IDEM COMPLETE}. */
        ACQUIRED,
        /** Another caller owns the key and has not finished; poll again later. */
        PENDING,
        /** The key was used with a different fingerprint. */
        CONFLICT,
        /** The action already finished; {@link IdemResult#result()} holds its stored result. */
        COMPLETED
    }

    private final State state;
    private final byte[] result;

    /**
     * Creates a result.
     *
     * @param state the reservation state
     * @param result the stored result bytes for {@code COMPLETED}, otherwise {@code null}
     */
    public IdemResult(State state, byte[] result) {
        this.state = state;
        this.result = result;
    }

    /**
     * Returns the reservation state.
     *
     * @return the state
     */
    public State state() {
        return state;
    }

    /**
     * Returns the binary result stored by {@code IDEM COMPLETE}.
     *
     * @return the bytes, or {@code null} unless the state is {@link State#COMPLETED}
     */
    public byte[] result() {
        return result;
    }

    /**
     * Returns the stored result decoded as UTF-8 text.
     *
     * @return the text, or {@code null} unless the state is {@code COMPLETED}
     */
    public String resultString() {
        return result == null ? null : new String(result, StandardCharsets.UTF_8);
    }

    /**
     * Returns a short readable form, for logs.
     *
     * @return the text
     */
    @Override
    public String toString() {
        return "IdemResult{" + state + (result == null ? "" : ", result=" + resultString()) + "}";
    }
}
