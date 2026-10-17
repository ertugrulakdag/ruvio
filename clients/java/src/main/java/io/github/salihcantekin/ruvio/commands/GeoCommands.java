// SPDX-License-Identifier: MIT
package io.github.salihcantekin.ruvio.commands;

import io.github.salihcantekin.ruvio.RuvioCodec;
import io.github.salihcantekin.ruvio.args.GeoSearchArgs;
import io.github.salihcantekin.ruvio.args.GeoUnit;
import io.github.salihcantekin.ruvio.internal.Args;
import io.github.salihcantekin.ruvio.internal.Cmd;
import io.github.salihcantekin.ruvio.internal.Reply;
import io.github.salihcantekin.ruvio.model.GeoCoordinates;
import io.github.salihcantekin.ruvio.model.GeoResult;
import io.github.salihcantekin.ruvio.model.GeoValue;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/** GEO commands (a sorted set scored by a 52-bit geohash). */
public abstract class GeoCommands<K, V> extends SortedSetCommands<K, V> {
    protected GeoCommands(RuvioCodec<K> keys, RuvioCodec<V> values) {
        super(keys, values);
    }

    static final Reply<List<GeoCoordinates>> GEOPOS = Reply.of((tree, q) -> {
        List<Object> items = Reply.list(tree);
        List<GeoCoordinates> out = new ArrayList<>(items.size());
        for (Object item : items) {
            if (item == null) {
                out.add(null);
            } else {
                List<Object> p = Reply.list(item);
                out.add(new GeoCoordinates(Reply.dbl(p.get(0)), Reply.dbl(p.get(1))));
            }
        }
        return out;
    });

    /** Matches: plain members, or [member, dist?, hash?, [lon, lat]?] in that order. */
    static final Reply<List<GeoResult<Object>>> GEO_RESULTS = Reply.of((tree, q) -> {
        List<Object> items = Reply.list(tree);
        List<GeoResult<Object>> out = new ArrayList<>(items.size());
        for (Object item : items) {
            if (!(item instanceof List)) {
                out.add(new GeoResult<>(Reply.decodeWith(q.values, item), null, null, null));
                continue;
            }
            List<Object> parts = Reply.list(item);
            Object member = Reply.decodeWith(q.values, parts.get(0));
            Double dist = null;
            Long hash = null;
            GeoCoordinates coord = null;
            for (int i = 1; i < parts.size(); i++) {
                Object p = parts.get(i);
                if (p instanceof Long) {
                    hash = (Long) p;
                } else if (p instanceof List) {
                    List<Object> c = Reply.list(p);
                    coord = new GeoCoordinates(Reply.dbl(c.get(0)), Reply.dbl(c.get(1)));
                } else {
                    dist = Reply.dbl(p);
                }
            }
            out.add(new GeoResult<>(member, dist, hash, coord));
        }
        return out;
    });

    public CompletableFuture<Long> geoadd(K key, double longitude, double latitude, V member) {
        return send(args().w(Cmd.GEOADD).k(key).d(longitude).d(latitude).v(member), Reply.LONG);
    }

    @SafeVarargs
    public final CompletableFuture<Long> geoadd(K key, GeoValue<V>... members) {
        return geoadd(key, false, false, false, members);
    }

    /** GEOADD key [NX|XX] [CH] lon lat member ... */
    @SafeVarargs
    public final CompletableFuture<Long> geoadd(K key, boolean nx, boolean xx, boolean ch, GeoValue<V>... members) {
        Args a = args().w(Cmd.GEOADD).k(key);
        if (nx) a.s("NX");
        if (xx) a.s("XX");
        if (ch) a.s("CH");
        for (GeoValue<V> m : members) {
            a.d(m.longitude()).d(m.latitude()).v(m.member());
        }
        return send(a, Reply.LONG);
    }

    /** GEODIST in meters. Null when a member is missing. */
    public CompletableFuture<Double> geodist(K key, V member1, V member2) {
        return send(args().w(Cmd.GEODIST).k(key).v(member1).v(member2), Reply.DOUBLE);
    }

    public CompletableFuture<Double> geodist(K key, V member1, V member2, GeoUnit unit) {
        return send(args().w(Cmd.GEODIST).k(key).v(member1).v(member2).w(unit.fragment()), Reply.DOUBLE);
    }

    @SafeVarargs
    public final CompletableFuture<List<String>> geohash(K key, V... members) {
        return kvs(Cmd.GEOHASH, key, Reply.STRING_LIST, members);
    }

    /** GEOPOS: one entry per member, null for a missing member. */
    @SafeVarargs
    public final CompletableFuture<List<GeoCoordinates>> geopos(K key, V... members) {
        return kvs(Cmd.GEOPOS, key, GEOPOS, members);
    }

    /** GEOSEARCH key FROMMEMBER|FROMLONLAT BYRADIUS|BYBOX [ASC|DESC] [COUNT n [ANY]] [WITH...]. */
    public CompletableFuture<List<GeoResult<V>>> geosearch(K key, GeoSearchArgs search) {
        Args a = args().w(Cmd.GEOSEARCH).k(key);
        search.writeSearch(a, true);
        return send(a, r(GEO_RESULTS));
    }

    /** GEOSEARCHSTORE destination source ... [STOREDIST]. Returns the stored count. */
    public CompletableFuture<Long> geosearchstore(K destination, K source, GeoSearchArgs search, boolean storeDist) {
        Args a = args().w(Cmd.GEOSEARCHSTORE).k(destination).k(source);
        search.writeSearch(a, false);
        if (storeDist) {
            a.s("STOREDIST");
        }
        return send(a, Reply.LONG);
    }

    /** GEORADIUS key lon lat radius unit [WITH...] [COUNT] [ASC|DESC]. */
    public CompletableFuture<List<GeoResult<V>>> georadius(K key, double longitude, double latitude, double radius, GeoUnit unit) {
        return georadius(key, longitude, latitude, radius, unit, GeoSearchArgs.options());
    }

    public CompletableFuture<List<GeoResult<V>>> georadius(K key, double longitude, double latitude, double radius, GeoUnit unit, GeoSearchArgs options) {
        Args a = args().w(Cmd.GEORADIUS).k(key).d(longitude).d(latitude).d(radius).w(unit.fragment());
        options.writeTail(a, true);
        return send(a, r(GEO_RESULTS));
    }

    /** GEORADIUS ... STORE|STOREDIST destination. Returns the stored count. */
    public CompletableFuture<Long> georadiusStore(K key, double longitude, double latitude, double radius, GeoUnit unit, K destination, boolean storeDist) {
        Args a = args().w(Cmd.GEORADIUS).k(key).d(longitude).d(latitude).d(radius).w(unit.fragment());
        a.s(storeDist ? "STOREDIST" : "STORE").k(destination);
        return send(a, Reply.LONG);
    }

    public CompletableFuture<List<GeoResult<V>>> georadiusbymember(K key, V member, double radius, GeoUnit unit) {
        return georadiusbymember(key, member, radius, unit, GeoSearchArgs.options());
    }

    public CompletableFuture<List<GeoResult<V>>> georadiusbymember(K key, V member, double radius, GeoUnit unit, GeoSearchArgs options) {
        Args a = args().w(Cmd.GEORADIUSBYMEMBER).k(key).v(member).d(radius).w(unit.fragment());
        options.writeTail(a, true);
        return send(a, r(GEO_RESULTS));
    }

    public CompletableFuture<Long> georadiusbymemberStore(K key, V member, double radius, GeoUnit unit, K destination, boolean storeDist) {
        Args a = args().w(Cmd.GEORADIUSBYMEMBER).k(key).v(member).d(radius).w(unit.fragment());
        a.s(storeDist ? "STOREDIST" : "STORE").k(destination);
        return send(a, Reply.LONG);
    }
}
