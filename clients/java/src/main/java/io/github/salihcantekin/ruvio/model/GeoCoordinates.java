// SPDX-License-Identifier: MIT
package io.github.salihcantekin.ruvio.model;

/** Longitude and latitude in degrees. */
public final class GeoCoordinates {
    private final double longitude;
    private final double latitude;

    public GeoCoordinates(double longitude, double latitude) {
        this.longitude = longitude;
        this.latitude = latitude;
    }

    public double longitude() {
        return longitude;
    }

    public double latitude() {
        return latitude;
    }

    @Override
    public String toString() {
        return "(" + longitude + ", " + latitude + ")";
    }
}
