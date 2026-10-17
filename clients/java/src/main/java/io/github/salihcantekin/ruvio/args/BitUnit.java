// SPDX-License-Identifier: MIT
package io.github.salihcantekin.ruvio.args;

import io.github.salihcantekin.ruvio.internal.RespOut;

/**
 * Unit of the BITCOUNT / BITPOS range: bytes or single bits.
 *
 * <p>Example: {@code client.bitcount(key, 0, 7, BitUnit.BIT)}
 */
public enum BitUnit {
    BYTE("BYTE"),
    BIT("BIT");

    private final String word;
    private final byte[] fragment;

    BitUnit(String word) {
        this.word = word;
        this.fragment = RespOut.fragment(word);
    }

    /** The wire keyword, for example {@code "BYTE"}. */
    public String word() {
        return word;
    }

    /**
     * The keyword pre-encoded as a complete RESP bulk string ({@code $len\r\nWORD\r\n}). The array is
     * shared and must not be modified.
     */
    public byte[] fragment() {
        return fragment;
    }
}
