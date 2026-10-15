// SPDX-License-Identifier: MIT
package io.github.salihcantekin.ruvio.commands;

import io.github.salihcantekin.ruvio.RuvioCodec;
import io.github.salihcantekin.ruvio.RuvioException;
import io.github.salihcantekin.ruvio.args.SetOptions;
import io.github.salihcantekin.ruvio.internal.Args;
import io.github.salihcantekin.ruvio.internal.Cmd;
import io.github.salihcantekin.ruvio.internal.Reply;
import io.github.salihcantekin.ruvio.model.FillResult;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/** String commands, including the Ruvio extensions SETV, TAKE, DELEX, GETEX, FILLGET/FILLSET. */
public abstract class StringCommands<K, V> extends BaseCommands<K, V> {
    protected StringCommands(RuvioCodec<K> keys, RuvioCodec<V> values) {
        super(keys, values);
    }

    static final Reply<FillResult<Object>> FILL = Reply.of((tree, q) -> {
        List<Object> items = Reply.list(tree);
        String state = Reply.str(items.get(0));
        switch (state) {
            case "HIT":
                return new FillResult<>(FillResult.State.HIT, Reply.decodeWith(q.values, items.get(1)), null);
            case "FILL":
                return new FillResult<>(FillResult.State.FILL, null, Reply.str(items.get(1)));
            case "BUSY":
                return new FillResult<>(FillResult.State.BUSY, null, null);
            case "STALE":
                return new FillResult<>(FillResult.State.STALE, Reply.decodeWith(q.values, items.get(1)),
                        items.size() > 2 ? Reply.str(items.get(2)) : null);
            default:
                throw new RuvioException("unexpected FILLGET state " + state);
        }
    });

    /** GET key. Null when missing. */
    public CompletableFuture<V> get(K key) {
        return k(Cmd.GET, key, r(Reply.VALUE));
    }

    /** SET key value. */
    public CompletableFuture<Void> set(K key, V value) {
        return kv(Cmd.SET, key, value, Reply.OK);
    }

    /** SET key value [EX|PX|EXAT|PXAT|KEEPTTL] [NX|XX]. False when NX/XX prevented the write. */
    public CompletableFuture<Boolean> set(K key, V value, SetOptions options) {
        Args a = args().w(Cmd.SET).k(key).v(value);
        options.write(a);
        return send(a, Reply.OK_BOOL);
    }

    /** SET key value GET: returns the previous string (null when missing). */
    public CompletableFuture<V> setGet(K key, V value) {
        return send(args().w(Cmd.SET).k(key).v(value).w(Cmd.GET), r(Reply.VALUE));
    }

    /** SET key value [options] GET: returns the previous string (null when missing). */
    public CompletableFuture<V> setGet(K key, V value, SetOptions options) {
        Args a = args().w(Cmd.SET).k(key).v(value);
        options.write(a);
        return send(a.w(Cmd.GET), r(Reply.VALUE));
    }

    /**
     * Ruvio {@code SET key value IFEQ expected}: writes only when the current string equals
     * {@code expected}. True when written.
     */
    public CompletableFuture<Boolean> setIfEq(K key, V value, V expected) {
        return send(args().w(Cmd.SET).k(key).v(value).w(Cmd.IFEQ).v(expected), Reply.OK_BOOL);
    }

    /** {@code SET key value IFEQ expected [EX|PX|EXAT|PXAT|KEEPTTL]} (the NX/XX part of options is ignored). */
    public CompletableFuture<Boolean> setIfEq(K key, V value, V expected, SetOptions expiry) {
        Args a = args().w(Cmd.SET).k(key).v(value).w(Cmd.IFEQ).v(expected);
        expiry.writeExpiry(a);
        return send(a, Reply.OK_BOOL);
    }

    /** SETV key value: creates version 1. Returns the new version. */
    public CompletableFuture<Long> setv(K key, V value) {
        return kv(Cmd.SETV, key, value, Reply.LONG);
    }

    /** SETV key value version: compare-and-set on the version; a mismatch fails with an error. */
    public CompletableFuture<Long> setv(K key, V value, long version) {
        return send(args().w(Cmd.SETV).k(key).v(value).l(version), Reply.LONG);
    }

    /** TAKE key count: the remainder, or null when the key holds less (nothing changes). */
    public CompletableFuture<Long> take(K key, long count) {
        return kl(Cmd.TAKE, key, count, Reply.LONG);
    }

    /** DELEX key IFEQ value: deletes only when the string equals {@code expected}. */
    public CompletableFuture<Boolean> delex(K key, V expected) {
        return send(args().w(Cmd.DELEX).k(key).w(Cmd.IFEQ).v(expected), Reply.BOOL);
    }

    /** GETEX key ttl_ms: returns the string and sets its TTL in milliseconds. */
    public CompletableFuture<V> getex(K key, long ttlMillis) {
        return kl(Cmd.GETEX, key, ttlMillis, r(Reply.VALUE));
    }

    @SafeVarargs
    public final CompletableFuture<List<V>> mget(K... keys) {
        return keys(Cmd.MGET, r(Reply.VALUE_LIST), keys);
    }

    public CompletableFuture<Void> mset(Map<K, V> pairs) {
        Args a = args().w(Cmd.MSET);
        for (Map.Entry<K, V> e : pairs.entrySet()) {
            a.k(e.getKey()).v(e.getValue());
        }
        return send(a, Reply.OK);
    }

    /** MSETNX: all or nothing. True when every key was set. */
    public CompletableFuture<Boolean> msetnx(Map<K, V> pairs) {
        Args a = args().w(Cmd.MSETNX);
        for (Map.Entry<K, V> e : pairs.entrySet()) {
            a.k(e.getKey()).v(e.getValue());
        }
        return send(a, Reply.BOOL);
    }

    public CompletableFuture<V> getset(K key, V value) {
        return kv(Cmd.GETSET, key, value, r(Reply.VALUE));
    }

    public CompletableFuture<Long> append(K key, V value) {
        return kv(Cmd.APPEND, key, value, Reply.LONG);
    }

    public CompletableFuture<Long> strlen(K key) {
        return k(Cmd.STRLEN, key, Reply.LONG);
    }

    public CompletableFuture<Long> incr(K key) {
        return k(Cmd.INCR, key, Reply.LONG);
    }

    public CompletableFuture<Long> decr(K key) {
        return k(Cmd.DECR, key, Reply.LONG);
    }

    public CompletableFuture<Long> incrby(K key, long delta) {
        return kl(Cmd.INCRBY, key, delta, Reply.LONG);
    }

    /** INCRBY key delta MAX limit: null (and no write) when the result would pass {@code limit}. */
    public CompletableFuture<Long> incrbyMax(K key, long delta, long limit) {
        return send(args().w(Cmd.INCRBY).k(key).l(delta).w(Cmd.MAX).l(limit), Reply.LONG);
    }

    public CompletableFuture<Long> decrby(K key, long delta) {
        return kl(Cmd.DECRBY, key, delta, Reply.LONG);
    }

    public CompletableFuture<Double> incrbyfloat(K key, double delta) {
        return send(args().w(Cmd.INCRBYFLOAT).k(key).d(delta), Reply.DOUBLE);
    }

    /**
     * FILLGET key fill_ms: HIT with the value, FILL with a token (this caller builds the value),
     * BUSY, or STALE.
     */
    public CompletableFuture<FillResult<V>> fillget(K key, long fillMillis) {
        return kl(Cmd.FILLGET, key, fillMillis, r(FILL));
    }

    /**
     * FILLGET key fill_ms WAIT wait_ms. Waits on the server, which parks this whole multiplexed
     * connection; prefer {@code fillget} with client-side polling, or a dedicated client.
     */
    public CompletableFuture<FillResult<V>> fillgetWait(K key, long fillMillis, long waitMillis) {
        return send(args().w(Cmd.FILLGET).k(key).l(fillMillis).w(Cmd.WAIT).l(waitMillis), r(FILL));
    }

    /** FILLSET key value ttl_ms token. True when the token was live and the value was written. */
    public CompletableFuture<Boolean> fillset(K key, V value, long ttlMillis, String token) {
        return send(args().w(Cmd.FILLSET).k(key).v(value).l(ttlMillis).s(token), Reply.BOOL);
    }

    /** FILLSET key value ttl_ms token SWR stale_ms (stale-while-revalidate). */
    public CompletableFuture<Boolean> fillset(K key, V value, long ttlMillis, String token, long staleMillis) {
        return send(args().w(Cmd.FILLSET).k(key).v(value).l(ttlMillis).s(token).w(Cmd.SWR).l(staleMillis), Reply.BOOL);
    }

    public CompletableFuture<Void> setex(K key, long seconds, V value) {
        return klv(Cmd.SETEX, key, seconds, value, Reply.OK);
    }

    public CompletableFuture<Void> psetex(K key, long millis, V value) {
        return klv(Cmd.PSETEX, key, millis, value, Reply.OK);
    }

    public CompletableFuture<Boolean> setnx(K key, V value) {
        return kv(Cmd.SETNX, key, value, Reply.BOOL);
    }

    public CompletableFuture<V> getdel(K key) {
        return k(Cmd.GETDEL, key, r(Reply.VALUE));
    }

    public CompletableFuture<V> getrange(K key, long start, long end) {
        return kll(Cmd.GETRANGE, key, start, end, r(Reply.VALUE));
    }

    /** SUBSTR: alias of GETRANGE. */
    public CompletableFuture<V> substr(K key, long start, long end) {
        return kll(Cmd.SUBSTR, key, start, end, r(Reply.VALUE));
    }

    public CompletableFuture<Long> setrange(K key, long offset, V value) {
        return klv(Cmd.SETRANGE, key, offset, value, Reply.LONG);
    }
}
