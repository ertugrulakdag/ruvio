// SPDX-License-Identifier: MIT
package io.github.salihcantekin.ruvio.args;

import io.github.salihcantekin.ruvio.internal.RespOut;

/** Distance unit of the GEO commands. */
public enum GeoUnit {
    /** Meters. */
    M,
    /** Kilometers. */
    KM,
    /** Feet. */
    FT,
    /** Miles. */
    MI;

    private final byte[] fragment;

    GeoUnit() {
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
