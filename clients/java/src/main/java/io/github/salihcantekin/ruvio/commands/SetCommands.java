// SPDX-License-Identifier: MIT
package io.github.salihcantekin.ruvio.commands;

import io.github.salihcantekin.ruvio.RuvioCodec;
import io.github.salihcantekin.ruvio.args.ScanArgs;
import io.github.salihcantekin.ruvio.internal.Args;
import io.github.salihcantekin.ruvio.internal.Cmd;
import io.github.salihcantekin.ruvio.internal.Reply;
import io.github.salihcantekin.ruvio.model.ScanResult;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/** Set commands. Ruvio replies with members sorted. */
public abstract class SetCommands<K, V> extends ListCommands<K, V> {
    protected SetCommands(RuvioCodec<K> keys, RuvioCodec<V> values) {
        super(keys, values);
    }

    static final Reply<ScanResult<Object>> SCAN_VALUES = Reply.of((tree, q) -> {
        List<Object> items = Reply.list(tree);
        List<Object> raw = Reply.list(items.get(1));
        List<Object> out = new ArrayList<>(raw.size());
        for (Object o : raw) {
            out.add(Reply.decodeWith(q.values, o));
        }
        return new ScanResult<>(Reply.str(items.get(0)), out);
    });

    @SafeVarargs
    public final CompletableFuture<Long> sadd(K key, V... members) {
        return kvs(Cmd.SADD, key, Reply.LONG, members);
    }

    @SafeVarargs
    public final CompletableFuture<Long> srem(K key, V... members) {
        return kvs(Cmd.SREM, key, Reply.LONG, members);
    }

    public CompletableFuture<Boolean> sismember(K key, V member) {
        return kv(Cmd.SISMEMBER, key, member, Reply.BOOL);
    }

    @SafeVarargs
    public final CompletableFuture<List<Boolean>> smismember(K key, V... members) {
        return kvs(Cmd.SMISMEMBER, key, Reply.BOOL_LIST, members);
    }

    public CompletableFuture<Long> scard(K key) {
        return k(Cmd.SCARD, key, Reply.LONG);
    }

    public CompletableFuture<List<V>> smembers(K key) {
        return k(Cmd.SMEMBERS, key, r(Reply.VALUE_LIST));
    }

    @SafeVarargs
    public final CompletableFuture<List<V>> sinter(K... keys) {
        return keys(Cmd.SINTER, r(Reply.VALUE_LIST), keys);
    }

    @SafeVarargs
    public final CompletableFuture<List<V>> sunion(K... keys) {
        return keys(Cmd.SUNION, r(Reply.VALUE_LIST), keys);
    }

    @SafeVarargs
    public final CompletableFuture<List<V>> sdiff(K... keys) {
        return keys(Cmd.SDIFF, r(Reply.VALUE_LIST), keys);
    }

    @SafeVarargs
    public final CompletableFuture<Long> sinterstore(K destination, K... keys) {
        return send(args().w(Cmd.SINTERSTORE).k(destination).keys(keys), Reply.LONG);
    }

    @SafeVarargs
    public final CompletableFuture<Long> sunionstore(K destination, K... keys) {
        return send(args().w(Cmd.SUNIONSTORE).k(destination).keys(keys), Reply.LONG);
    }

    @SafeVarargs
    public final CompletableFuture<Long> sdiffstore(K destination, K... keys) {
        return send(args().w(Cmd.SDIFFSTORE).k(destination).keys(keys), Reply.LONG);
    }

    /** SINTERCARD numkeys key... */
    @SafeVarargs
    public final CompletableFuture<Long> sintercard(K... keys) {
        return send(args().w(Cmd.SINTERCARD).l(keys.length).keys(keys), Reply.LONG);
    }

    /** SINTERCARD numkeys key... LIMIT limit */
    @SafeVarargs
    public final CompletableFuture<Long> sintercard(long limit, K... keys) {
        return send(args().w(Cmd.SINTERCARD).l(keys.length).keys(keys).w(Cmd.LIMIT).l(limit), Reply.LONG);
    }

    public CompletableFuture<Boolean> smove(K source, K destination, V member) {
        return send(args().w(Cmd.SMOVE).k(source).k(destination).v(member), Reply.BOOL);
    }

    public CompletableFuture<V> spop(K key) {
        return k(Cmd.SPOP, key, r(Reply.VALUE));
    }

    public CompletableFuture<List<V>> spop(K key, long count) {
        return kl(Cmd.SPOP, key, count, r(Reply.VALUE_LIST));
    }

    public CompletableFuture<V> srandmember(K key) {
        return k(Cmd.SRANDMEMBER, key, r(Reply.VALUE));
    }

    public CompletableFuture<List<V>> srandmember(K key, long count) {
        return kl(Cmd.SRANDMEMBER, key, count, r(Reply.VALUE_LIST));
    }

    public CompletableFuture<ScanResult<V>> sscan(K key, String cursor) {
        return send(args().w(Cmd.SSCAN).k(key).s(cursor), r(SCAN_VALUES));
    }

    public CompletableFuture<ScanResult<V>> sscan(K key, String cursor, ScanArgs options) {
        Args a = args().w(Cmd.SSCAN).k(key).s(cursor);
        options.write(a, false);
        return send(a, r(SCAN_VALUES));
    }
}
