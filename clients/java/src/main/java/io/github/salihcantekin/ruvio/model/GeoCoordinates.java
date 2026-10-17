// SPDX-License-Identifier: MIT
package io.github.salihcantekin.ruvio.model;

/** Longitude and latitude in degrees. */
public final class GeoCoordinates {
    private final double longitude;
    private final double latitude;

    /**
     * Creates a position.
     *
     * @param longitude degrees east, from -180 to 180
     * @param latitude degrees north, from -85.05112878 to 85.05112878
     */
    public GeoCoordinates(double longitude, double latitude) {
        this.longitude = longitude;
        this.latitude = latitude;
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
     * Returns a short readable form, for logs.
     *
     * @return the text
     */
    @Override
    public String toString() {
        return "(" + longitude + ", " + latitude + ")";
    }
}
