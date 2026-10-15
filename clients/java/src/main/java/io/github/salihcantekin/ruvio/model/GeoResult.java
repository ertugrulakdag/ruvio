// SPDX-License-Identifier: MIT
package io.github.salihcantekin.ruvio.model;

/** One GEOSEARCH / GEORADIUS match. Distance, hash and coordinates are null unless requested. */
public final class GeoResult<V> {
    private final V member;
    private final Double distance;
    private final Long hash;
    private final GeoCoordinates coordinates;

    public GeoResult(V member, Double distance, Long hash, GeoCoordinates coordinates) {
        this.member = member;
        this.distance = distance;
        this.hash = hash;
        this.coordinates = coordinates;
    }

    public V member() {
        return member;
    }

    public Double distance() {
        return distance;
    }

    public Long hash() {
        return hash;
    }

    public GeoCoordinates coordinates() {
        return coordinates;
    }

    @Override
    public String toString() {
        return "GeoResult{" + member + ", dist=" + distance + ", hash=" + hash + ", coord=" + coordinates + "}";
    }
}
