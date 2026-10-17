// SPDX-License-Identifier: MIT
package io.github.salihcantekin.ruvio.args;

import io.github.salihcantekin.ruvio.internal.RespOut;

/**
 * Where LINSERT places the new element relative to the pivot.
 *
 * <p>Example: {@code list.linsert(key, InsertPosition.BEFORE, pivot, value)}
 */
public enum InsertPosition {
    /** Insert in front of the pivot. */
    BEFORE("BEFORE"),
    /** Insert behind the pivot. */
    AFTER("AFTER");

    private final String word;
    private final byte[] fragment;

    InsertPosition(String word) {
        this.word = word;
        this.fragment = RespOut.fragment(word);
    }

    /**
     * Returns the keyword as sent to the server.
     *
     * @return the keyword, for example {@code "BEFORE"}
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
