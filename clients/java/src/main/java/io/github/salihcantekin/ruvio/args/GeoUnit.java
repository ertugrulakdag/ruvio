// SPDX-License-Identifier: MIT
package io.github.salihcantekin.ruvio.args;

import io.github.salihcantekin.ruvio.internal.RespOut;

/** Distance units for GEO commands. */
public enum GeoUnit {
    M, KM, FT, MI;

    private final byte[] fragment;

    GeoUnit() {
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
