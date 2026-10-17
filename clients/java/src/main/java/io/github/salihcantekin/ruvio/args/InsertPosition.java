// SPDX-License-Identifier: MIT
package io.github.salihcantekin.ruvio.args;

import io.github.salihcantekin.ruvio.internal.RespOut;

/**
 * Where LINSERT places the new element relative to the pivot.
 *
 * <p>Example: {@code list.linsert(key, InsertPosition.BEFORE, pivot, value)}
 */
public enum InsertPosition {
    BEFORE("BEFORE"),
    AFTER("AFTER");

    private final String word;
    private final byte[] fragment;

    InsertPosition(String word) {
        this.word = word;
        this.fragment = RespOut.fragment(word);
    }

    /** The wire keyword, for example {@code "BEFORE"}. */
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
