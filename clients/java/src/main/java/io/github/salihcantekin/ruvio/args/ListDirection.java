// SPDX-License-Identifier: MIT
package io.github.salihcantekin.ruvio.args;

import io.github.salihcantekin.ruvio.internal.RespOut;

/** LEFT or RIGHT end of a list, for LMOVE, BLMOVE, LMPOP and BLMPOP. */
public enum ListDirection {
    LEFT, RIGHT;

    private final byte[] fragment;

    ListDirection() {
        this.fragment = RespOut.fragment(name());
    }

    /**
     * The keyword pre-encoded as a complete RESP bulk string, identical on the wire to {@code name()}.
     * The array is shared and must not be modified.
     */
    public byte[] fragment() {
        return fragment;
    }
}
