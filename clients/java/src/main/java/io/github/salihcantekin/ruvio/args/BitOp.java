// SPDX-License-Identifier: MIT
package io.github.salihcantekin.ruvio.args;

import io.github.salihcantekin.ruvio.internal.RespOut;

/** BITOP operations. */
public enum BitOp {
    AND, OR, XOR, NOT;

    private final byte[] fragment;

    BitOp() {
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
