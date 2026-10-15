// SPDX-License-Identifier: MIT
package io.github.salihcantekin.ruvio.model;

import io.github.salihcantekin.ruvio.internal.Reply;

import java.nio.charset.StandardCharsets;

/** Reply of {@code IDEM key BEGIN}: the reservation state and, when completed, the stored result. */
public final class IdemResult {
    public enum State { ACQUIRED, PENDING, CONFLICT, COMPLETED }

    private final State state;
    private final byte[] result;

    public IdemResult(State state, byte[] result) {
        this.state = state;
        this.result = result;
    }

    public State state() {
        return state;
    }

    /** The binary result stored by IDEM COMPLETE; null unless {@link State#COMPLETED}. */
    public byte[] result() {
        return result;
    }

    public String resultString() {
        return result == null ? null : new String(result, StandardCharsets.UTF_8);
    }

    @Override
    public String toString() {
        return "IdemResult{" + state + (result == null ? "" : ", result=" + resultString()) + "}";
    }
}
