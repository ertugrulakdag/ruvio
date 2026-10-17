// SPDX-License-Identifier: MIT
package io.github.salihcantekin.ruvio.args;

import io.github.salihcantekin.ruvio.internal.Args;
import io.github.salihcantekin.ruvio.internal.Cmd;

/**
 * GEOSEARCH options: origin (FROMMEMBER or FROMLONLAT), shape (BYRADIUS or BYBOX), order, COUNT
 * [ANY] and the WITH* flags. GEORADIUS and GEORADIUSBYMEMBER use only the order, count and WITH*
 * parts. The member passed to {@link #fromMember(Object)} is encoded with the client's value codec.
 */
public final class GeoSearchArgs {
    /** Creates an empty option set; the static factories are the usual way in. */
    public GeoSearchArgs() {
    }

    private Object fromMember;
    private boolean fromLonLat;
    private double lon;
    private double lat;
    private boolean byBox;
    private double radius;
    private double width;
    private double height;
    private GeoUnit unit = GeoUnit.M;
    private Boolean asc;
    private long count;
    private boolean any;
    boolean withDist;
    boolean withHash;
    boolean withCoord;

    /**
     * Searches around an existing member's position ({@code FROMMEMBER}). Add a shape with {@link #byRadius} or {@link #byBox}.
     *
     * @param member the member; encoded with the client's value codec
     * @return a new option set
     */
    public static GeoSearchArgs fromMember(Object member) {
        GeoSearchArgs a = new GeoSearchArgs();
        a.fromMember = member;
        return a;
    }

    /**
     * Searches around a point ({@code FROMLONLAT}). Add a shape with {@link #byRadius} or {@link #byBox}.
     *
     * @param longitude degrees east, from -180 to 180
     * @param latitude degrees north, from -85.05112878 to 85.05112878
     * @return a new option set
     */
    public static GeoSearchArgs fromLonLat(double longitude, double latitude) {
        GeoSearchArgs a = new GeoSearchArgs();
        a.fromLonLat = true;
        a.lon = longitude;
        a.lat = latitude;
        return a;
    }

    /**
     * Starts an option set with no origin or shape, for {@code GEORADIUS} and {@code GEORADIUSBYMEMBER}, which take those as separate arguments.
     *
     * @return a new option set
     */
    public static GeoSearchArgs options() {
        return new GeoSearchArgs();
    }

    /**
     * Sets {@code BYRADIUS}: search inside a circle.
     *
     * @param radius the radius (0 or more)
     * @param unit the unit of the radius
     * @return this builder
     */
    public GeoSearchArgs byRadius(double radius, GeoUnit unit) {
        this.byBox = false;
        this.radius = radius;
        this.unit = unit;
        return this;
    }

    /**
     * Sets {@code BYBOX}: search inside a rectangle centred on the origin.
     *
     * @param width the box width (0 or more)
     * @param height the box height (0 or more)
     * @param unit the unit of both sizes
     * @return this builder
     */
    public GeoSearchArgs byBox(double width, double height, GeoUnit unit) {
        this.byBox = true;
        this.width = width;
        this.height = height;
        this.unit = unit;
        return this;
    }

    /**
     * Sorts the results nearest first ({@code ASC}).
     *
     * @return this builder
     */
    public GeoSearchArgs asc() {
        asc = Boolean.TRUE;
        return this;
    }

    /**
     * Sorts the results farthest first ({@code DESC}).
     *
     * @return this builder
     */
    public GeoSearchArgs desc() {
        asc = Boolean.FALSE;
        return this;
    }

    /**
     * Sets the result order from an enum, for example {@code order(GeoOrder.DESC)}; same as {@link #asc()} / {@link #desc()}.
     *
     * @param order nearest first or farthest first
     * @return this builder
     */
    public GeoSearchArgs order(GeoOrder order) {
        asc = order == GeoOrder.ASC ? Boolean.TRUE : Boolean.FALSE;
        return this;
    }

    /**
     * Sets {@code COUNT}: return only the closest matches.
     *
     * @param count the most results (above 0; 0 is an error)
     * @return this builder
     */
    public GeoSearchArgs count(long count) {
        this.count = count;
        return this;
    }

    /**
     * Sets {@code COUNT n [ANY]}.
     *
     * @param count the most results (above 0)
     * @param any {@code true} to return the first matches found instead of the closest; faster on large areas
     * @return this builder
     */
    public GeoSearchArgs count(long count, boolean any) {
        this.count = count;
        this.any = any;
        return this;
    }

    /**
     * Adds {@code WITHDIST}: return each member's distance from the origin, in the shape's unit.
     *
     * @return this builder
     */
    public GeoSearchArgs withDist() {
        withDist = true;
        return this;
    }

    /**
     * Adds {@code WITHHASH}: return each member's raw geohash integer.
     *
     * @return this builder
     */
    public GeoSearchArgs withHash() {
        withHash = true;
        return this;
    }

    /**
     * Adds {@code WITHCOORD}: return each member's longitude and latitude.
     *
     * @return this builder
     */
    public GeoSearchArgs withCoord() {
        withCoord = true;
        return this;
    }

    /**
     * Appends origin, shape and the remaining options to a {@code GEOSEARCH} being encoded. Used by the client; you do not call it.
     *
     * @param a the argument writer
     * @param includeWith whether to write the {@code WITH*} flags
     * @throws IllegalArgumentException when neither origin was set
     */
    public void writeSearch(Args a, boolean includeWith) {
        if (fromMember != null) {
            a.s("FROMMEMBER").v(fromMember);
        } else if (fromLonLat) {
            a.s("FROMLONLAT").d(lon).d(lat);
        } else {
            throw new IllegalArgumentException("GEOSEARCH needs fromMember or fromLonLat");
        }
        if (byBox) {
            a.s("BYBOX").d(width).d(height).w(unit.fragment());
        } else {
            a.s("BYRADIUS").d(radius).w(unit.fragment());
        }
        writeTail(a, includeWith);
    }

    /**
     * Appends order, count and the {@code WITH*} flags. Used by the client; you do not call it.
     *
     * @param a the argument writer
     * @param includeWith whether to write the {@code WITH*} flags
     */
    public void writeTail(Args a, boolean includeWith) {
        if (asc != null) {
            a.w(asc ? GeoOrder.ASC.fragment() : GeoOrder.DESC.fragment());
        }
        if (count > 0) {
            a.w(Cmd.COUNT).l(count);
            if (any) {
                a.s("ANY");
            }
        }
        if (includeWith) {
            if (withCoord) a.s("WITHCOORD");
            if (withDist) a.s("WITHDIST");
            if (withHash) a.s("WITHHASH");
        }
    }

    boolean plain() {
        return !withDist && !withHash && !withCoord;
    }
}
