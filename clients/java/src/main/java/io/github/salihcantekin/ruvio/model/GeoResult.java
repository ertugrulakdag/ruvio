// SPDX-License-Identifier: MIT
package io.github.salihcantekin.ruvio.model;

/**
 * One match of {@code GEOSEARCH} or {@code GEORADIUS}.
 *
 * @param <V> the member type
 */
public final class GeoResult<V> {
    private final V member;
    private final Double distance;
    private final Long hash;
    private final GeoCoordinates coordinates;

    /**
     * Creates a result.
     *
     * @param member the matched member
     * @param distance the distance from the centre, or {@code null}
     * @param hash the raw geohash integer, or {@code null}
     * @param coordinates the member's position, or {@code null}
     */
    public GeoResult(V member, Double distance, Long hash, GeoCoordinates coordinates) {
        this.member = member;
        this.distance = distance;
        this.hash = hash;
        this.coordinates = coordinates;
    }

    /**
     * Returns the matched member.
     *
     * @return the member
     */
    public V member() {
        return member;
    }

    /**
     * Returns the distance from the search centre.
     *
     * @return the distance in the unit of the search, or {@code null} unless {@code WITHDIST} was requested
     */
    public Double distance() {
        return distance;
    }

    /**
     * Returns the member's raw geohash.
     *
     * @return the 52-bit geohash integer, or {@code null} unless {@code WITHHASH} was requested
     */
    public Long hash() {
        return hash;
    }

    /**
     * Returns the member's position.
     *
     * @return the position, or {@code null} unless {@code WITHCOORD} was requested
     */
    public GeoCoordinates coordinates() {
        return coordinates;
    }

    /**
     * Returns a short readable form, for logs.
     *
     * @return the text
     */
    @Override
    public String toString() {
        return "GeoResult{" + member + ", dist=" + distance + ", hash=" + hash + ", coord=" + coordinates + "}";
    }
}
