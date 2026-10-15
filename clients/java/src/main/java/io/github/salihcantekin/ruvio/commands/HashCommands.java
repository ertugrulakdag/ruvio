// SPDX-License-Identifier: MIT
package io.github.salihcantekin.ruvio.commands;

import io.github.salihcantekin.ruvio.RuvioCodec;
import io.github.salihcantekin.ruvio.args.ExpireCondition;
import io.github.salihcantekin.ruvio.args.ScanArgs;
import io.github.salihcantekin.ruvio.internal.Args;
import io.github.salihcantekin.ruvio.internal.Cmd;
import io.github.salihcantekin.ruvio.internal.Reply;
import io.github.salihcantekin.ruvio.model.KeyValue;
import io.github.salihcantekin.ruvio.model.ScanResult;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/** Hash commands, including per-field TTLs. Fields use the key codec, values the value codec. */
public abstract class HashCommands<K, V> extends KeyCommands<K, V> {
    protected HashCommands(RuvioCodec<K> keys, RuvioCodec<V> values) {
        super(keys, values);
    }

    static final Reply<ScanResult<KeyValue<Object, Object>>> SCAN_FIELDS = Reply.of((tree, q) -> {
        List<Object> items = Reply.list(tree);
        List<Object> raw = Reply.list(items.get(1));
        List<KeyValue<Object, Object>> out = new ArrayList<>(raw.size() / 2);
        for (int i = 0; i + 1 < raw.size(); i += 2) {
            out.add(new KeyValue<>(Reply.decodeWith(q.keys, raw.get(i)), Reply.decodeWith(q.values, raw.get(i + 1))));
        }
        return new ScanResult<>(Reply.str(items.get(0)), out);
    });

    static final Reply<List<KeyValue<Object, Object>>> FIELD_PAIRS = Reply.of((tree, q) -> {
        List<Object> raw = Reply.list(tree);
        List<KeyValue<Object, Object>> out = new ArrayList<>(raw.size() / 2);
        if (!raw.isEmpty() && raw.get(0) instanceof List) {
            for (Object pair : raw) {
                List<Object> p = Reply.list(pair);
                out.add(new KeyValue<>(Reply.decodeWith(q.keys, p.get(0)), Reply.decodeWith(q.values, p.get(1))));
            }
            return out;
        }
        for (int i = 0; i + 1 < raw.size(); i += 2) {
            out.add(new KeyValue<>(Reply.decodeWith(q.keys, raw.get(i)), Reply.decodeWith(q.values, raw.get(i + 1))));
        }
        return out;
    });

    /** HSET key field value. Returns the number of new fields. */
    public CompletableFuture<Long> hset(K key, K field, V value) {
        return kfv(Cmd.HSET, key, field, value, Reply.LONG);
    }

    /** HSET key field value [field value ...]. */
    public CompletableFuture<Long> hset(K key, Map<K, V> fields) {
        Args a = args().w(Cmd.HSET).k(key);
        for (Map.Entry<K, V> e : fields.entrySet()) {
            a.k(e.getKey()).v(e.getValue());
        }
        return send(a, Reply.LONG);
    }

    public CompletableFuture<V> hget(K key, K field) {
        return kf(Cmd.HGET, key, field, r(Reply.VALUE));
    }

    @SafeVarargs
    public final CompletableFuture<Long> hdel(K key, K... fields) {
        return kfs(Cmd.HDEL, key, Reply.LONG, fields);
    }

    public CompletableFuture<Long> hlen(K key) {
        return k(Cmd.HLEN, key, Reply.LONG);
    }

    /** HGETALL key, in field order. */
    public CompletableFuture<Map<K, V>> hgetall(K key) {
        return k(Cmd.HGETALL, key, r(Reply.KV_MAP));
    }

    @SafeVarargs
    public final CompletableFuture<List<V>> hmget(K key, K... fields) {
        return kfs(Cmd.HMGET, key, r(Reply.VALUE_LIST), fields);
    }

    public CompletableFuture<Boolean> hexists(K key, K field) {
        return kf(Cmd.HEXISTS, key, field, Reply.BOOL);
    }

    public CompletableFuture<List<K>> hkeys(K key) {
        return k(Cmd.HKEYS, key, r(Reply.KEY_LIST));
    }

    public CompletableFuture<List<V>> hvals(K key) {
        return k(Cmd.HVALS, key, r(Reply.VALUE_LIST));
    }

    public CompletableFuture<Long> hincrby(K key, K field, long delta) {
        return send(args().w(Cmd.HINCRBY).k(key).k(field).l(delta), Reply.LONG);
    }

    public CompletableFuture<Double> hincrbyfloat(K key, K field, double delta) {
        return send(args().w(Cmd.HINCRBYFLOAT).k(key).k(field).d(delta), Reply.DOUBLE);
    }

    public CompletableFuture<Boolean> hsetnx(K key, K field, V value) {
        return kfv(Cmd.HSETNX, key, field, value, Reply.BOOL);
    }

    public CompletableFuture<Long> hstrlen(K key, K field) {
        return kf(Cmd.HSTRLEN, key, field, Reply.LONG);
    }

    public CompletableFuture<ScanResult<KeyValue<K, V>>> hscan(K key, String cursor) {
        return send(args().w(Cmd.HSCAN).k(key).s(cursor), r(SCAN_FIELDS));
    }

    public CompletableFuture<ScanResult<KeyValue<K, V>>> hscan(K key, String cursor, ScanArgs options) {
        Args a = args().w(Cmd.HSCAN).k(key).s(cursor);
        options.write(a, false);
        return send(a, r(SCAN_FIELDS));
    }

    /** HRANDFIELD key: one random field, or null for a missing key. */
    public CompletableFuture<K> hrandfield(K key) {
        return k(Cmd.HRANDFIELD, key, r(Reply.KEY));
    }

    /** HRANDFIELD key count (negative count may repeat). */
    public CompletableFuture<List<K>> hrandfield(K key, long count) {
        return kl(Cmd.HRANDFIELD, key, count, r(Reply.KEY_LIST));
    }

    /** HRANDFIELD key count WITHVALUES. */
    public CompletableFuture<List<KeyValue<K, V>>> hrandfieldWithValues(K key, long count) {
        return send(args().w(Cmd.HRANDFIELD).k(key).l(count).s("WITHVALUES"), r(FIELD_PAIRS));
    }

    // ---- field TTLs ------------------------------------------------------------------------

    private CompletableFuture<List<Long>> fieldTtl(byte[] cmd, K key, long time, ExpireCondition condition, K[] fields) {
        Args a = args().w(cmd).k(key).l(time);
        if (condition != null && condition != ExpireCondition.ALWAYS) {
            a.s(condition.name());
        }
        a.w(Cmd.FIELDS).l(fields.length).keys(fields);
        return send(a, Reply.LONG_LIST);
    }

    private CompletableFuture<List<Long>> fieldQuery(byte[] cmd, K key, K[] fields) {
        return send(args().w(cmd).k(key).w(Cmd.FIELDS).l(fields.length).keys(fields), Reply.LONG_LIST);
    }

    /** HEXPIRE key seconds FIELDS n field...: per field -2 missing, 0 condition not met, 1 set, 2 deleted. */
    @SafeVarargs
    public final CompletableFuture<List<Long>> hexpire(K key, long seconds, K... fields) {
        return fieldTtl(Cmd.HEXPIRE, key, seconds, null, fields);
    }

    @SafeVarargs
    public final CompletableFuture<List<Long>> hexpire(K key, long seconds, ExpireCondition condition, K... fields) {
        return fieldTtl(Cmd.HEXPIRE, key, seconds, condition, fields);
    }

    @SafeVarargs
    public final CompletableFuture<List<Long>> hpexpire(K key, long millis, K... fields) {
        return fieldTtl(Cmd.HPEXPIRE, key, millis, null, fields);
    }

    @SafeVarargs
    public final CompletableFuture<List<Long>> hpexpire(K key, long millis, ExpireCondition condition, K... fields) {
        return fieldTtl(Cmd.HPEXPIRE, key, millis, condition, fields);
    }

    @SafeVarargs
    public final CompletableFuture<List<Long>> hexpireat(K key, long unixSeconds, K... fields) {
        return fieldTtl(Cmd.HEXPIREAT, key, unixSeconds, null, fields);
    }

    @SafeVarargs
    public final CompletableFuture<List<Long>> hexpireat(K key, long unixSeconds, ExpireCondition condition, K... fields) {
        return fieldTtl(Cmd.HEXPIREAT, key, unixSeconds, condition, fields);
    }

    @SafeVarargs
    public final CompletableFuture<List<Long>> hpexpireat(K key, long unixMillis, K... fields) {
        return fieldTtl(Cmd.HPEXPIREAT, key, unixMillis, null, fields);
    }

    @SafeVarargs
    public final CompletableFuture<List<Long>> hpexpireat(K key, long unixMillis, ExpireCondition condition, K... fields) {
        return fieldTtl(Cmd.HPEXPIREAT, key, unixMillis, condition, fields);
    }

    @SafeVarargs
    public final CompletableFuture<List<Long>> httl(K key, K... fields) {
        return fieldQuery(Cmd.HTTL, key, fields);
    }

    @SafeVarargs
    public final CompletableFuture<List<Long>> hpttl(K key, K... fields) {
        return fieldQuery(Cmd.HPTTL, key, fields);
    }

    @SafeVarargs
    public final CompletableFuture<List<Long>> hexpiretime(K key, K... fields) {
        return fieldQuery(Cmd.HEXPIRETIME, key, fields);
    }

    @SafeVarargs
    public final CompletableFuture<List<Long>> hpexpiretime(K key, K... fields) {
        return fieldQuery(Cmd.HPEXPIRETIME, key, fields);
    }

    /** HPERSIST: per field 1 removed a TTL, -1 had none, -2 missing. */
    @SafeVarargs
    public final CompletableFuture<List<Long>> hpersist(K key, K... fields) {
        return fieldQuery(Cmd.HPERSIST, key, fields);
    }
}
