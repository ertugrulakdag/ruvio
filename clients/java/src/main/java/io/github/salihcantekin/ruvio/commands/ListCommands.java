// SPDX-License-Identifier: MIT
package io.github.salihcantekin.ruvio.commands;

import io.github.salihcantekin.ruvio.RuvioCodec;
import io.github.salihcantekin.ruvio.args.LPosArgs;
import io.github.salihcantekin.ruvio.args.InsertPosition;
import io.github.salihcantekin.ruvio.args.ListDirection;
import io.github.salihcantekin.ruvio.internal.Args;
import io.github.salihcantekin.ruvio.internal.Cmd;
import io.github.salihcantekin.ruvio.internal.Reply;
import io.github.salihcantekin.ruvio.model.KeyValue;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * List commands, including Ruvio LPUSHCAP / RPUSHCAP. Blocking commands (BLPOP, BRPOP, BLMOVE,
 * BLMPOP) hold the connection while they wait: run them on {@code client.dedicated()}.
 */
public abstract class ListCommands<K, V> extends HashCommands<K, V> {
    protected ListCommands(RuvioCodec<K> keys, RuvioCodec<V> values) {
        super(keys, values);
    }

    static final Reply<KeyValue<Object, Object>> KEY_VALUE = Reply.of((tree, q) -> {
        if (tree == null) {
            return null;
        }
        List<Object> items = Reply.list(tree);
        return new KeyValue<>(Reply.decodeWith(q.keys, items.get(0)), Reply.decodeWith(q.values, items.get(1)));
    });

    static final Reply<KeyValue<Object, List<Object>>> KEY_VALUES = Reply.of((tree, q) -> {
        if (tree == null) {
            return null;
        }
        List<Object> items = Reply.list(tree);
        List<Object> raw = Reply.list(items.get(1));
        List<Object> values = new ArrayList<>(raw.size());
        for (Object o : raw) {
            values.add(Reply.decodeWith(q.values, o));
        }
        return new KeyValue<>(Reply.decodeWith(q.keys, items.get(0)), values);
    });

    @SafeVarargs
    public final CompletableFuture<Long> lpush(K key, V... values) {
        return kvs(Cmd.LPUSH, key, Reply.LONG, values);
    }

    @SafeVarargs
    public final CompletableFuture<Long> rpush(K key, V... values) {
        return kvs(Cmd.RPUSH, key, Reply.LONG, values);
    }

    @SafeVarargs
    public final CompletableFuture<Long> lpushx(K key, V... values) {
        return kvs(Cmd.LPUSHX, key, Reply.LONG, values);
    }

    @SafeVarargs
    public final CompletableFuture<Long> rpushx(K key, V... values) {
        return kvs(Cmd.RPUSHX, key, Reply.LONG, values);
    }

    /** LPUSHCAP key cap value...: LPUSH, then trim from the tail to at most {@code cap}. Returns the length. */
    @SafeVarargs
    public final CompletableFuture<Long> lpushcap(K key, long cap, V... values) {
        return send(args().w(Cmd.LPUSHCAP).k(key).l(cap).values(values), Reply.LONG);
    }

    /** RPUSHCAP key cap value...: RPUSH, then trim from the head to at most {@code cap}. */
    @SafeVarargs
    public final CompletableFuture<Long> rpushcap(K key, long cap, V... values) {
        return send(args().w(Cmd.RPUSHCAP).k(key).l(cap).values(values), Reply.LONG);
    }

    public CompletableFuture<V> lpop(K key) {
        return k(Cmd.LPOP, key, r(Reply.VALUE));
    }

    public CompletableFuture<List<V>> lpop(K key, long count) {
        return kl(Cmd.LPOP, key, count, r(Reply.VALUE_LIST));
    }

    public CompletableFuture<V> rpop(K key) {
        return k(Cmd.RPOP, key, r(Reply.VALUE));
    }

    public CompletableFuture<List<V>> rpop(K key, long count) {
        return kl(Cmd.RPOP, key, count, r(Reply.VALUE_LIST));
    }

    public CompletableFuture<Long> llen(K key) {
        return k(Cmd.LLEN, key, Reply.LONG);
    }

    public CompletableFuture<V> lindex(K key, long index) {
        return kl(Cmd.LINDEX, key, index, r(Reply.VALUE));
    }

    public CompletableFuture<List<V>> lrange(K key, long start, long stop) {
        return kll(Cmd.LRANGE, key, start, stop, r(Reply.VALUE_LIST));
    }

    public CompletableFuture<Void> ltrim(K key, long start, long stop) {
        return kll(Cmd.LTRIM, key, start, stop, Reply.OK);
    }

    /** BLPOP key... timeout_seconds. Null on timeout. */
    @SafeVarargs
    public final CompletableFuture<KeyValue<K, V>> blpop(double timeoutSeconds, K... keys) {
        return send(args().w(Cmd.BLPOP).keys(keys).d(timeoutSeconds), r(KEY_VALUE));
    }

    @SafeVarargs
    public final CompletableFuture<KeyValue<K, V>> brpop(double timeoutSeconds, K... keys) {
        return send(args().w(Cmd.BRPOP).keys(keys).d(timeoutSeconds), r(KEY_VALUE));
    }

    /** LINSERT key BEFORE|AFTER pivot value. -1 when the pivot is missing. */
    public CompletableFuture<Long> linsert(K key, boolean before, V pivot, V value) {
        return linsert(key, before ? InsertPosition.BEFORE : InsertPosition.AFTER, pivot, value);
    }

    /** LINSERT key BEFORE|AFTER pivot value with an enum position, for example {@code InsertPosition.AFTER}. */
    public CompletableFuture<Long> linsert(K key, InsertPosition position, V pivot, V value) {
        return send(args().w(Cmd.LINSERT).k(key).w(position.fragment()).v(pivot).v(value), Reply.LONG);
    }

    public CompletableFuture<Void> lset(K key, long index, V value) {
        return klv(Cmd.LSET, key, index, value, Reply.OK);
    }

    public CompletableFuture<Long> lrem(K key, long count, V value) {
        return klv(Cmd.LREM, key, count, value, Reply.LONG);
    }

    /** LPOS key value: index of the first match, or null. */
    public CompletableFuture<Long> lpos(K key, V value) {
        return kv(Cmd.LPOS, key, value, Reply.LONG);
    }

    public CompletableFuture<Long> lpos(K key, V value, LPosArgs options) {
        Args a = args().w(Cmd.LPOS).k(key).v(value);
        options.write(a);
        return send(a, Reply.LONG);
    }

    /** LPOS key value COUNT n (0 means all matches). */
    public CompletableFuture<List<Long>> lposAll(K key, V value, long count) {
        return send(args().w(Cmd.LPOS).k(key).v(value).w(Cmd.COUNT).l(count), Reply.LONG_LIST);
    }

    public CompletableFuture<List<Long>> lposAll(K key, V value, long count, LPosArgs options) {
        Args a = args().w(Cmd.LPOS).k(key).v(value);
        options.write(a);
        return send(a.w(Cmd.COUNT).l(count), Reply.LONG_LIST);
    }

    public CompletableFuture<V> lmove(K source, K destination, ListDirection from, ListDirection to) {
        return send(args().w(Cmd.LMOVE).k(source).k(destination).w(from.fragment()).w(to.fragment()), r(Reply.VALUE));
    }

    public CompletableFuture<V> rpoplpush(K source, K destination) {
        return kk(Cmd.RPOPLPUSH, source, destination, r(Reply.VALUE));
    }

    /** BLMOVE ... timeout_seconds. Null on timeout. Use a dedicated client. */
    public CompletableFuture<V> blmove(K source, K destination, ListDirection from, ListDirection to, double timeoutSeconds) {
        return send(args().w(Cmd.BLMOVE).k(source).k(destination).w(from.fragment()).w(to.fragment()).d(timeoutSeconds), r(Reply.VALUE));
    }

    /** LMPOP numkeys key... LEFT|RIGHT [COUNT n]. Null when every list is empty. */
    @SafeVarargs
    public final CompletableFuture<KeyValue<K, List<V>>> lmpop(ListDirection direction, long count, K... keys) {
        Args a = args().w(Cmd.LMPOP).l(keys.length).keys(keys).w(direction.fragment());
        if (count > 0) {
            a.w(Cmd.COUNT).l(count);
        }
        return send(a, r(KEY_VALUES));
    }

    /** BLMPOP timeout numkeys key... LEFT|RIGHT [COUNT n]. Null on timeout. Use a dedicated client. */
    @SafeVarargs
    public final CompletableFuture<KeyValue<K, List<V>>> blmpop(double timeoutSeconds, ListDirection direction, long count, K... keys) {
        Args a = args().w(Cmd.BLMPOP).d(timeoutSeconds).l(keys.length).keys(keys).w(direction.fragment());
        if (count > 0) {
            a.w(Cmd.COUNT).l(count);
        }
        return send(a, r(KEY_VALUES));
    }
}
