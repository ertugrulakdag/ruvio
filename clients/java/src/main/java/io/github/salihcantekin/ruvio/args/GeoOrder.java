// SPDX-License-Identifier: MIT
package io.github.salihcantekin.ruvio.args;

import io.github.salihcantekin.ruvio.internal.RespOut;

/**
 * Result order of GEORADIUS and GEOSEARCH, nearest first or farthest first.
 *
 * <p>Example: {@code GeoSearchArgs.options().order(GeoOrder.DESC)}
 */
public enum GeoOrder {
    /** Nearest first. */
    ASC("ASC"),
    /** Farthest first. */
    DESC("DESC");

    private final String word;
    private final byte[] fragment;

    GeoOrder(String word) {
        this.word = word;
        this.fragment = RespOut.fragment(word);
    }

    /**
     * Returns the keyword as sent to the server.
     *
     * @return the keyword, for example {@code "ASC"}
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
