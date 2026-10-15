// SPDX-License-Identifier: MIT
package io.github.salihcantekin.ruvio.commands;

import io.github.salihcantekin.ruvio.RuvioCodec;
import io.github.salihcantekin.ruvio.args.ScanArgs;
import io.github.salihcantekin.ruvio.args.ZAddArgs;
import io.github.salihcantekin.ruvio.args.ZRangeArgs;
import io.github.salihcantekin.ruvio.args.ZStoreArgs;
import io.github.salihcantekin.ruvio.internal.Args;
import io.github.salihcantekin.ruvio.internal.Cmd;
import io.github.salihcantekin.ruvio.internal.Reply;
import io.github.salihcantekin.ruvio.model.KeyValue;
import io.github.salihcantekin.ruvio.model.ScanResult;
import io.github.salihcantekin.ruvio.model.ScoredValue;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Sorted-set commands. Score bounds are strings so that {@code -inf}, {@code +inf} and exclusive
 * {@code (1.5} bounds can be expressed; lex bounds use {@code -}, {@code +}, {@code [a}, {@code (a}.
 */
public abstract class SortedSetCommands<K, V> extends SetCommands<K, V> {
    protected SortedSetCommands(RuvioCodec<K> keys, RuvioCodec<V> values) {
        super(keys, values);
    }

    static final Reply<ScanResult<ScoredValue<Object>>> SCAN_SCORED = Reply.of((tree, q) -> {
        List<Object> items = Reply.list(tree);
        List<Object> raw = Reply.list(items.get(1));
        List<ScoredValue<Object>> out = new ArrayList<>(raw.size() / 2);
        for (int i = 0; i + 1 < raw.size(); i += 2) {
            out.add(new ScoredValue<>(Reply.decodeWith(q.values, raw.get(i)), Reply.dbl(raw.get(i + 1))));
        }
        return new ScanResult<>(Reply.str(items.get(0)), out);
    });

    static final Reply<KeyValue<Object, ScoredValue<Object>>> KEY_SCORED = Reply.of((tree, q) -> {
        if (tree == null) {
            return null;
        }
        List<Object> items = Reply.list(tree);
        return new KeyValue<>(Reply.decodeWith(q.keys, items.get(0)),
                new ScoredValue<>(Reply.decodeWith(q.values, items.get(1)), Reply.dbl(items.get(2))));
    });

    /** ZADD key score member. Returns the number of new members. */
    public CompletableFuture<Long> zadd(K key, double score, V member) {
        return send(args().w(Cmd.ZADD).k(key).d(score).v(member), Reply.LONG);
    }

    /** ZADD key score member [score member ...] from member -> score. */
    public CompletableFuture<Long> zadd(K key, Map<V, Double> members) {
        Args a = args().w(Cmd.ZADD).k(key);
        for (Map.Entry<V, Double> e : members.entrySet()) {
            a.d(e.getValue()).v(e.getKey());
        }
        return send(a, Reply.LONG);
    }

    /** ZADD key [NX|XX] [GT|LT] [CH] score member ... */
    public CompletableFuture<Long> zadd(K key, ZAddArgs options, Map<V, Double> members) {
        Args a = args().w(Cmd.ZADD).k(key);
        options.write(a);
        for (Map.Entry<V, Double> e : members.entrySet()) {
            a.d(e.getValue()).v(e.getKey());
        }
        return send(a, Reply.LONG);
    }

    /** ZADD key INCR increment member: the new score. */
    public CompletableFuture<Double> zaddIncr(K key, double increment, V member) {
        return send(args().w(Cmd.ZADD).k(key).s("INCR").d(increment).v(member), Reply.DOUBLE);
    }

    /** ZADD key [flags] INCR increment member: the new score, or null when a flag blocked it. */
    public CompletableFuture<Double> zaddIncr(K key, ZAddArgs options, double increment, V member) {
        Args a = args().w(Cmd.ZADD).k(key);
        options.write(a);
        return send(a.s("INCR").d(increment).v(member), Reply.DOUBLE);
    }

    public CompletableFuture<List<V>> zrange(K key, long start, long stop) {
        return kll(Cmd.ZRANGE, key, start, stop, r(Reply.VALUE_LIST));
    }

    public CompletableFuture<List<ScoredValue<V>>> zrangeWithScores(K key, long start, long stop) {
        return send(args().w(Cmd.ZRANGE).k(key).l(start).l(stop).w(Cmd.WITHSCORES), r(Reply.SCORED));
    }

    /** Unified ZRANGE key start stop [BYSCORE|BYLEX] [REV] [LIMIT offset count]. */
    public CompletableFuture<List<V>> zrange(K key, String start, String stop, ZRangeArgs options) {
        Args a = args().w(Cmd.ZRANGE).k(key).s(start).s(stop);
        options.write(a);
        return send(a, r(Reply.VALUE_LIST));
    }

    public CompletableFuture<List<ScoredValue<V>>> zrangeWithScores(K key, String start, String stop, ZRangeArgs options) {
        Args a = args().w(Cmd.ZRANGE).k(key).s(start).s(stop);
        options.write(a);
        return send(a.w(Cmd.WITHSCORES), r(Reply.SCORED));
    }

    public CompletableFuture<List<V>> zrevrange(K key, long start, long stop) {
        return kll(Cmd.ZREVRANGE, key, start, stop, r(Reply.VALUE_LIST));
    }

    public CompletableFuture<List<ScoredValue<V>>> zrevrangeWithScores(K key, long start, long stop) {
        return send(args().w(Cmd.ZREVRANGE).k(key).l(start).l(stop).w(Cmd.WITHSCORES), r(Reply.SCORED));
    }

    public CompletableFuture<List<V>> zrangebyscore(K key, String min, String max) {
        return send(args().w(Cmd.ZRANGEBYSCORE).k(key).s(min).s(max), r(Reply.VALUE_LIST));
    }

    public CompletableFuture<List<V>> zrangebyscore(K key, String min, String max, long offset, long count) {
        return send(args().w(Cmd.ZRANGEBYSCORE).k(key).s(min).s(max).w(Cmd.LIMIT).l(offset).l(count), r(Reply.VALUE_LIST));
    }

    public CompletableFuture<List<ScoredValue<V>>> zrangebyscoreWithScores(K key, String min, String max) {
        return send(args().w(Cmd.ZRANGEBYSCORE).k(key).s(min).s(max).w(Cmd.WITHSCORES), r(Reply.SCORED));
    }

    public CompletableFuture<List<ScoredValue<V>>> zrangebyscoreWithScores(K key, String min, String max, long offset, long count) {
        return send(args().w(Cmd.ZRANGEBYSCORE).k(key).s(min).s(max).w(Cmd.WITHSCORES).w(Cmd.LIMIT).l(offset).l(count), r(Reply.SCORED));
    }

    public CompletableFuture<List<V>> zrangebylex(K key, String min, String max) {
        return send(args().w(Cmd.ZRANGEBYLEX).k(key).s(min).s(max), r(Reply.VALUE_LIST));
    }

    public CompletableFuture<List<V>> zrangebylex(K key, String min, String max, long offset, long count) {
        return send(args().w(Cmd.ZRANGEBYLEX).k(key).s(min).s(max).w(Cmd.LIMIT).l(offset).l(count), r(Reply.VALUE_LIST));
    }

    /** ZREVRANGEBYLEX key max min: the high bound comes first. */
    public CompletableFuture<List<V>> zrevrangebylex(K key, String max, String min) {
        return send(args().w(Cmd.ZREVRANGEBYLEX).k(key).s(max).s(min), r(Reply.VALUE_LIST));
    }

    public CompletableFuture<List<V>> zrevrangebylex(K key, String max, String min, long offset, long count) {
        return send(args().w(Cmd.ZREVRANGEBYLEX).k(key).s(max).s(min).w(Cmd.LIMIT).l(offset).l(count), r(Reply.VALUE_LIST));
    }

    @SafeVarargs
    public final CompletableFuture<Long> zrem(K key, V... members) {
        return kvs(Cmd.ZREM, key, Reply.LONG, members);
    }

    public CompletableFuture<Double> zincrby(K key, double increment, V member) {
        return send(args().w(Cmd.ZINCRBY).k(key).d(increment).v(member), Reply.DOUBLE);
    }

    public CompletableFuture<Long> zcard(K key) {
        return k(Cmd.ZCARD, key, Reply.LONG);
    }

    public CompletableFuture<Double> zscore(K key, V member) {
        return kv(Cmd.ZSCORE, key, member, Reply.DOUBLE);
    }

    @SafeVarargs
    public final CompletableFuture<List<Double>> zmscore(K key, V... members) {
        return kvs(Cmd.ZMSCORE, key, Reply.DOUBLE_LIST, members);
    }

    public CompletableFuture<Long> zrank(K key, V member) {
        return kv(Cmd.ZRANK, key, member, Reply.LONG);
    }

    public CompletableFuture<Long> zrevrank(K key, V member) {
        return kv(Cmd.ZREVRANK, key, member, Reply.LONG);
    }

    public CompletableFuture<List<ScoredValue<V>>> zpopmin(K key) {
        return k(Cmd.ZPOPMIN, key, r(Reply.SCORED));
    }

    public CompletableFuture<List<ScoredValue<V>>> zpopmin(K key, long count) {
        return kl(Cmd.ZPOPMIN, key, count, r(Reply.SCORED));
    }

    public CompletableFuture<List<ScoredValue<V>>> zpopmax(K key) {
        return k(Cmd.ZPOPMAX, key, r(Reply.SCORED));
    }

    public CompletableFuture<List<ScoredValue<V>>> zpopmax(K key, long count) {
        return kl(Cmd.ZPOPMAX, key, count, r(Reply.SCORED));
    }

    /** BZPOPMIN key... timeout. Null on timeout. Use a dedicated client. */
    @SafeVarargs
    public final CompletableFuture<KeyValue<K, ScoredValue<V>>> bzpopmin(double timeoutSeconds, K... keys) {
        return send(args().w(Cmd.BZPOPMIN).keys(keys).d(timeoutSeconds), r(KEY_SCORED));
    }

    @SafeVarargs
    public final CompletableFuture<KeyValue<K, ScoredValue<V>>> bzpopmax(double timeoutSeconds, K... keys) {
        return send(args().w(Cmd.BZPOPMAX).keys(keys).d(timeoutSeconds), r(KEY_SCORED));
    }

    public CompletableFuture<Long> zremrangebyrank(K key, long start, long stop) {
        return kll(Cmd.ZREMRANGEBYRANK, key, start, stop, Reply.LONG);
    }

    public CompletableFuture<Long> zremrangebyscore(K key, String min, String max) {
        return send(args().w(Cmd.ZREMRANGEBYSCORE).k(key).s(min).s(max), Reply.LONG);
    }

    public CompletableFuture<Long> zremrangebylex(K key, String min, String max) {
        return send(args().w(Cmd.ZREMRANGEBYLEX).k(key).s(min).s(max), Reply.LONG);
    }

    public CompletableFuture<Long> zcount(K key, String min, String max) {
        return send(args().w(Cmd.ZCOUNT).k(key).s(min).s(max), Reply.LONG);
    }

    public CompletableFuture<Long> zlexcount(K key, String min, String max) {
        return send(args().w(Cmd.ZLEXCOUNT).k(key).s(min).s(max), Reply.LONG);
    }

    public CompletableFuture<ScanResult<ScoredValue<V>>> zscan(K key, String cursor) {
        return send(args().w(Cmd.ZSCAN).k(key).s(cursor), r(SCAN_SCORED));
    }

    public CompletableFuture<ScanResult<ScoredValue<V>>> zscan(K key, String cursor, ScanArgs options) {
        Args a = args().w(Cmd.ZSCAN).k(key).s(cursor);
        options.write(a, false);
        return send(a, r(SCAN_SCORED));
    }

    // ---- set algebra -----------------------------------------------------------------------

    private Args algebra(byte[] cmd, K destination, ZStoreArgs options, K[] keys) {
        Args a = args().w(cmd);
        if (destination != null) {
            a.k(destination);
        }
        a.l(keys.length).keys(keys);
        if (options != null) {
            options.write(a);
        }
        return a;
    }

    @SafeVarargs
    public final CompletableFuture<Long> zunionstore(K destination, K... keys) {
        return send(algebra(Cmd.ZUNIONSTORE, destination, null, keys), Reply.LONG);
    }

    @SafeVarargs
    public final CompletableFuture<Long> zunionstore(K destination, ZStoreArgs options, K... keys) {
        return send(algebra(Cmd.ZUNIONSTORE, destination, options, keys), Reply.LONG);
    }

    @SafeVarargs
    public final CompletableFuture<Long> zinterstore(K destination, K... keys) {
        return send(algebra(Cmd.ZINTERSTORE, destination, null, keys), Reply.LONG);
    }

    @SafeVarargs
    public final CompletableFuture<Long> zinterstore(K destination, ZStoreArgs options, K... keys) {
        return send(algebra(Cmd.ZINTERSTORE, destination, options, keys), Reply.LONG);
    }

    @SafeVarargs
    public final CompletableFuture<Long> zdiffstore(K destination, K... keys) {
        return send(algebra(Cmd.ZDIFFSTORE, destination, null, keys), Reply.LONG);
    }

    @SafeVarargs
    public final CompletableFuture<List<V>> zunion(K... keys) {
        return send(algebra(Cmd.ZUNION, null, null, keys), r(Reply.VALUE_LIST));
    }

    @SafeVarargs
    public final CompletableFuture<List<V>> zunion(ZStoreArgs options, K... keys) {
        return send(algebra(Cmd.ZUNION, null, options, keys), r(Reply.VALUE_LIST));
    }

    @SafeVarargs
    public final CompletableFuture<List<ScoredValue<V>>> zunionWithScores(ZStoreArgs options, K... keys) {
        return send(algebra(Cmd.ZUNION, null, options, keys).w(Cmd.WITHSCORES), r(Reply.SCORED));
    }

    @SafeVarargs
    public final CompletableFuture<List<V>> zinter(K... keys) {
        return send(algebra(Cmd.ZINTER, null, null, keys), r(Reply.VALUE_LIST));
    }

    @SafeVarargs
    public final CompletableFuture<List<V>> zinter(ZStoreArgs options, K... keys) {
        return send(algebra(Cmd.ZINTER, null, options, keys), r(Reply.VALUE_LIST));
    }

    @SafeVarargs
    public final CompletableFuture<List<ScoredValue<V>>> zinterWithScores(ZStoreArgs options, K... keys) {
        return send(algebra(Cmd.ZINTER, null, options, keys).w(Cmd.WITHSCORES), r(Reply.SCORED));
    }

    @SafeVarargs
    public final CompletableFuture<List<V>> zdiff(K... keys) {
        return send(algebra(Cmd.ZDIFF, null, null, keys), r(Reply.VALUE_LIST));
    }

    @SafeVarargs
    public final CompletableFuture<List<ScoredValue<V>>> zdiffWithScores(K... keys) {
        return send(algebra(Cmd.ZDIFF, null, null, keys).w(Cmd.WITHSCORES), r(Reply.SCORED));
    }

    public CompletableFuture<V> zrandmember(K key) {
        return k(Cmd.ZRANDMEMBER, key, r(Reply.VALUE));
    }

    public CompletableFuture<List<V>> zrandmember(K key, long count) {
        return kl(Cmd.ZRANDMEMBER, key, count, r(Reply.VALUE_LIST));
    }

    public CompletableFuture<List<ScoredValue<V>>> zrandmemberWithScores(K key, long count) {
        return send(args().w(Cmd.ZRANDMEMBER).k(key).l(count).w(Cmd.WITHSCORES), r(Reply.SCORED));
    }
}
