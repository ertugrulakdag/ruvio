// SPDX-License-Identifier: MIT
package io.github.salihcantekin.ruvio.args;

import io.github.salihcantekin.ruvio.internal.RespOut;

/** LEFT or RIGHT end of a list, for LMOVE, BLMOVE, LMPOP and BLMPOP. */
public enum ListDirection {
    /** The head of the list. */
    LEFT,
    /** The tail of the list. */
    RIGHT;

    private final byte[] fragment;

    ListDirection() {
        this.fragment = RespOut.fragment(name());
    }

    /**
     * Returns the keyword pre-encoded as a complete RESP bulk string, identical on the wire to {@code name()}.
     *
     * @return the shared encoded bytes; do not modify them
     */
    public byte[] fragment() {
        return fragment;
    }
}
