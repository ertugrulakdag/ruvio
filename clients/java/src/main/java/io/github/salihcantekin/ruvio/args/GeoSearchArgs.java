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

    public static GeoSearchArgs fromMember(Object member) {
        GeoSearchArgs a = new GeoSearchArgs();
        a.fromMember = member;
        return a;
    }

    public static GeoSearchArgs fromLonLat(double longitude, double latitude) {
        GeoSearchArgs a = new GeoSearchArgs();
        a.fromLonLat = true;
        a.lon = longitude;
        a.lat = latitude;
        return a;
    }

    /** Options for GEORADIUS / GEORADIUSBYMEMBER (no origin or shape). */
    public static GeoSearchArgs options() {
        return new GeoSearchArgs();
    }

    public GeoSearchArgs byRadius(double radius, GeoUnit unit) {
        this.byBox = false;
        this.radius = radius;
        this.unit = unit;
        return this;
    }

    public GeoSearchArgs byBox(double width, double height, GeoUnit unit) {
        this.byBox = true;
        this.width = width;
        this.height = height;
        this.unit = unit;
        return this;
    }

    public GeoSearchArgs asc() {
        asc = Boolean.TRUE;
        return this;
    }

    public GeoSearchArgs desc() {
        asc = Boolean.FALSE;
        return this;
    }

    public GeoSearchArgs count(long count) {
        this.count = count;
        return this;
    }

    public GeoSearchArgs count(long count, boolean any) {
        this.count = count;
        this.any = any;
        return this;
    }

    public GeoSearchArgs withDist() {
        withDist = true;
        return this;
    }

    public GeoSearchArgs withHash() {
        withHash = true;
        return this;
    }

    public GeoSearchArgs withCoord() {
        withCoord = true;
        return this;
    }

    public void writeSearch(Args a, boolean includeWith) {
        if (fromMember != null) {
            a.s("FROMMEMBER").v(fromMember);
        } else if (fromLonLat) {
            a.s("FROMLONLAT").d(lon).d(lat);
        } else {
            throw new IllegalArgumentException("GEOSEARCH needs fromMember or fromLonLat");
        }
        if (byBox) {
            a.s("BYBOX").d(width).d(height).s(unit.name());
        } else {
            a.s("BYRADIUS").d(radius).s(unit.name());
        }
        writeTail(a, includeWith);
    }

    public void writeTail(Args a, boolean includeWith) {
        if (asc != null) {
            a.s(asc ? "ASC" : "DESC");
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
