// SPDX-License-Identifier: MIT
package io.github.salihcantekin.ruvio.args;

import io.github.salihcantekin.ruvio.internal.RespOut;

/** The operation of {@code BITOP}. */
public enum BitOp {
    /** Bitwise AND of all source keys. */
    AND,
    /** Bitwise OR of all source keys. */
    OR,
    /** Bitwise XOR of all source keys. */
    XOR,
    /** Bitwise NOT of exactly one source key. */
    NOT;

    private final byte[] fragment;

    BitOp() {
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
