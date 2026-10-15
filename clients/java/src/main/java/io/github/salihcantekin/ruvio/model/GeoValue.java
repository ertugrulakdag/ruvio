// SPDX-License-Identifier: MIT
package io.github.salihcantekin.ruvio.model;

/** A member with its position, for GEOADD. */
public final class GeoValue<V> {
    private final double longitude;
    private final double latitude;
    private final V member;

    public GeoValue(double longitude, double latitude, V member) {
        this.longitude = longitude;
        this.latitude = latitude;
        this.member = member;
    }

    public static <V> GeoValue<V> of(double longitude, double latitude, V member) {
        return new GeoValue<>(longitude, latitude, member);
    }

    public double longitude() {
        return longitude;
    }

    public double latitude() {
        return latitude;
    }

    public V member() {
        return member;
    }
}
