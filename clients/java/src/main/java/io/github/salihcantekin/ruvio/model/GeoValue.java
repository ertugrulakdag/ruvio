// SPDX-License-Identifier: MIT
package io.github.salihcantekin.ruvio.model;

/**
 * A member with its position, the argument of {@code GEOADD}.
 *
 * @param <V> the member type
 */
public final class GeoValue<V> {
    private final double longitude;
    private final double latitude;
    private final V member;

    /**
     * Creates a geo value.
     *
     * @param longitude degrees east, from -180 to 180
     * @param latitude degrees north, from -85.05112878 to 85.05112878
     * @param member the member name
     */
    public GeoValue(double longitude, double latitude, V member) {
        this.longitude = longitude;
        this.latitude = latitude;
        this.member = member;
    }

    /**
     * Creates a geo value; same as the constructor.
     *
     * @param longitude degrees east, from -180 to 180
     * @param latitude degrees north, from -85.05112878 to 85.05112878
     * @param member the member name
     * @param <V> the member type
     * @return the new value
     */
    public static <V> GeoValue<V> of(double longitude, double latitude, V member) {
        return new GeoValue<>(longitude, latitude, member);
    }

    /**
     * Returns the longitude.
     *
     * @return degrees east
     */
    public double longitude() {
        return longitude;
    }

    /**
     * Returns the latitude.
     *
     * @return degrees north
     */
    public double latitude() {
        return latitude;
    }

    /**
     * Returns the member.
     *
     * @return the member
     */
    public V member() {
        return member;
    }
}
