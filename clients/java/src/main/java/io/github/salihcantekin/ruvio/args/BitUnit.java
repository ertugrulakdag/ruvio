// SPDX-License-Identifier: MIT
package io.github.salihcantekin.ruvio.args;

import io.github.salihcantekin.ruvio.internal.RespOut;

/**
 * Unit of the BITCOUNT / BITPOS range: bytes or single bits.
 *
 * <p>Example: {@code client.bitcount(key, 0, 7, BitUnit.BIT)}
 */
public enum BitUnit {
    /** The range bounds are byte offsets. */
    BYTE("BYTE"),
    /** The range bounds are bit offsets. */
    BIT("BIT");

    private final String word;
    private final byte[] fragment;

    BitUnit(String word) {
        this.word = word;
        this.fragment = RespOut.fragment(word);
    }

    /**
     * Returns the keyword as sent to the server.
     *
     * @return the keyword, for example {@code "BYTE"}
     */
    public String word() {
        return word;
    }

    /**
     * Returns the keyword pre-encoded as a complete RESP bulk string ({@code $len\r\nWORD\r\n}).
     *
     * @return the shared encoded bytes; do not modify them
     */
    public byte[] fragment() {
        return fragment;
    }
}
