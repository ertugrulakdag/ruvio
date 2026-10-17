// SPDX-License-Identifier: MIT
package io.github.salihcantekin.ruvio.args;

import io.github.salihcantekin.ruvio.internal.RespOut;

/**
 * Result order of GEORADIUS and GEOSEARCH, nearest first or farthest first.
 *
 * <p>Example: {@code GeoSearchArgs.options().order(GeoOrder.DESC)}
 */
public enum GeoOrder {
    ASC("ASC"),
    DESC("DESC");

    private final String word;
    private final byte[] fragment;

    GeoOrder(String word) {
        this.word = word;
        this.fragment = RespOut.fragment(word);
    }

    /** The wire keyword, for example {@code "ASC"}. */
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
