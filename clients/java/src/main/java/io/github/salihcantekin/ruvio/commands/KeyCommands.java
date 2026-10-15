// SPDX-License-Identifier: MIT
package io.github.salihcantekin.ruvio.commands;

import io.github.salihcantekin.ruvio.RuvioCodec;
import io.github.salihcantekin.ruvio.args.RestoreArgs;
import io.github.salihcantekin.ruvio.args.ScanArgs;
import io.github.salihcantekin.ruvio.internal.Args;
import io.github.salihcantekin.ruvio.internal.Cmd;
import io.github.salihcantekin.ruvio.internal.Reply;
import io.github.salihcantekin.ruvio.model.ChangeNote;
import io.github.salihcantekin.ruvio.model.IdemResult;
import io.github.salihcantekin.ruvio.model.ScanResult;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/** Generic key commands plus the Ruvio coordination primitives (LIMIT, IDEM, ONCE, LEASE, SEMAPHORE, CHANGES). */
public abstract class KeyCommands<K, V> extends StringCommands<K, V> {
    protected KeyCommands(RuvioCodec<K> keys, RuvioCodec<V> values) {
        super(keys, values);
    }

    static final Reply<IdemResult> IDEM = Reply.of((tree, q) -> {
        List<Object> items = Reply.list(tree);
        String state = Reply.str(items.get(0));
        byte[] result = items.size() > 1 && items.get(1) instanceof byte[] ? (byte[]) items.get(1) : null;
        return new IdemResult(IdemResult.State.valueOf(state.toUpperCase(java.util.Locale.ROOT)), result);
    });

    static final Reply<List<ChangeNote>> CHANGES = Reply.of((tree, q) -> {
        List<Object> rows = Reply.list(tree);
        List<ChangeNote> out = new ArrayList<>(rows.size());
        for (Object row : rows) {
            List<Object> parts = Reply.list(row);
            out.add(new ChangeNote(Reply.lng(parts.get(0)), parts.size() > 1 ? Reply.str(parts.get(1)) : ""));
        }
        return out;
    });

    static final Reply<ScanResult<Object>> SCAN_KEYS = Reply.of((tree, q) -> {
        List<Object> items = Reply.list(tree);
        List<Object> raw = Reply.list(items.get(1));
        List<Object> out = new ArrayList<>(raw.size());
        for (Object o : raw) {
            out.add(Reply.decodeWith(q.keys, o));
        }
        return new ScanResult<>(Reply.str(items.get(0)), out);
    });

    // ---- Ruvio coordination ----------------------------------------------------------------

    /**
     * LIMIT key max window_ms: fixed-window rate limit. Returns the remaining allowance after this
     * call, or null when the window is full (the call is rejected and not counted).
     */
    public CompletableFuture<Long> limit(K key, long max, long windowMillis) {
        return kll(Cmd.LIMIT, key, max, windowMillis, Reply.LONG);
    }

    /** IDEM key BEGIN fingerprint owner ttl_ms. */
    public CompletableFuture<IdemResult> idemBegin(K key, String fingerprint, String owner, long ttlMillis) {
        return send(args().w(Cmd.IDEM).k(key).w(Cmd.BEGIN).s(fingerprint).s(owner).l(ttlMillis), IDEM);
    }

    /** IDEM key COMPLETE fingerprint owner result. Returns 1 when published (or identical). */
    public CompletableFuture<Long> idemComplete(K key, String fingerprint, String owner, byte[] result) {
        return send(args().w(Cmd.IDEM).k(key).w(Cmd.COMPLETE).s(fingerprint).s(owner).b(result), Reply.LONG);
    }

    /** IDEM key COMPLETE with a UTF-8 result. */
    public CompletableFuture<Long> idemComplete(K key, String fingerprint, String owner, String result) {
        return send(args().w(Cmd.IDEM).k(key).w(Cmd.COMPLETE).s(fingerprint).s(owner).s(result), Reply.LONG);
    }

    /** ONCE key ttl_ms: true for the first call within the TTL. */
    public CompletableFuture<Boolean> once(K key, long ttlMillis) {
        return kl(Cmd.ONCE, key, ttlMillis, Reply.BOOL);
    }

    /** LEASE key ttl_ms: a new fencing token, or null while another holder has the lease. */
    public CompletableFuture<Long> lease(K key, long ttlMillis) {
        return kl(Cmd.LEASE, key, ttlMillis, Reply.LONG);
    }

    /** SEMAPHORE key max ttl_ms: a holder token, or null when all {@code max} slots are taken. */
    public CompletableFuture<Long> semaphore(K key, long max, long ttlMillis) {
        return kll(Cmd.SEMAPHORE, key, max, ttlMillis, Reply.LONG);
    }

    /** RELEASE key token: frees a lease or one semaphore slot. True when the token matched. */
    public CompletableFuture<Boolean> release(K key, long token) {
        return kl(Cmd.RELEASE, key, token, Reply.BOOL);
    }

    /** CHANGES START: arms the change feed on this shard and returns the cursor. */
    public CompletableFuture<Long> changesStart() {
        return c00(Cmd.CHANGES, Cmd.START, Reply.LONG);
    }

    /** CHANGES cursor: notes after the cursor (at most 1024). */
    public CompletableFuture<List<ChangeNote>> changes(long cursor) {
        return send(args().w(Cmd.CHANGES).l(cursor), CHANGES);
    }

    // ---- keys ------------------------------------------------------------------------------

    @SafeVarargs
    public final CompletableFuture<Long> del(K... keys) {
        return keys(Cmd.DEL, Reply.LONG, keys);
    }

    @SafeVarargs
    public final CompletableFuture<Long> unlink(K... keys) {
        return keys(Cmd.UNLINK, Reply.LONG, keys);
    }

    @SafeVarargs
    public final CompletableFuture<Long> exists(K... keys) {
        return keys(Cmd.EXISTS, Reply.LONG, keys);
    }

    /** TYPE key: string, list, set, zset, hash, stream, none, ... */
    public CompletableFuture<String> type(K key) {
        return k(Cmd.TYPE, key, Reply.STRING);
    }

    public CompletableFuture<Void> rename(K key, K newKey) {
        return kk(Cmd.RENAME, key, newKey, Reply.OK);
    }

    public CompletableFuture<Boolean> renamenx(K key, K newKey) {
        return kk(Cmd.RENAMENX, key, newKey, Reply.BOOL);
    }

    /** SCAN cursor. Start with "0"; the iteration ends when the returned cursor is "0". */
    public CompletableFuture<ScanResult<K>> scan(String cursor) {
        return send(args().w(Cmd.SCAN).s(cursor), r(SCAN_KEYS));
    }

    public CompletableFuture<ScanResult<K>> scan(String cursor, ScanArgs options) {
        Args a = args().w(Cmd.SCAN).s(cursor);
        options.write(a, true);
        return send(a, r(SCAN_KEYS));
    }

    public CompletableFuture<Long> dbsize() {
        return c0(Cmd.DBSIZE, Reply.LONG);
    }

    public CompletableFuture<Void> flushdb() {
        return c0(Cmd.FLUSHDB, Reply.OK);
    }

    public CompletableFuture<Void> flushall() {
        return c0(Cmd.FLUSHALL, Reply.OK);
    }

    public CompletableFuture<Void> swapdb(int first, int second) {
        return send(args().w(Cmd.SWAPDB).l(first).l(second), Reply.OK);
    }

    public CompletableFuture<Boolean> move(K key, int database) {
        return kl(Cmd.MOVE, key, database, Reply.BOOL);
    }

    public CompletableFuture<Boolean> expire(K key, long seconds) {
        return kl(Cmd.EXPIRE, key, seconds, Reply.BOOL);
    }

    public CompletableFuture<Boolean> pexpire(K key, long millis) {
        return kl(Cmd.PEXPIRE, key, millis, Reply.BOOL);
    }

    public CompletableFuture<Boolean> expireat(K key, long unixSeconds) {
        return kl(Cmd.EXPIREAT, key, unixSeconds, Reply.BOOL);
    }

    public CompletableFuture<Boolean> pexpireat(K key, long unixMillis) {
        return kl(Cmd.PEXPIREAT, key, unixMillis, Reply.BOOL);
    }

    public CompletableFuture<Long> ttl(K key) {
        return k(Cmd.TTL, key, Reply.LONG);
    }

    public CompletableFuture<Long> pttl(K key) {
        return k(Cmd.PTTL, key, Reply.LONG);
    }

    public CompletableFuture<Boolean> persist(K key) {
        return k(Cmd.PERSIST, key, Reply.BOOL);
    }

    public CompletableFuture<Long> expiretime(K key) {
        return k(Cmd.EXPIRETIME, key, Reply.LONG);
    }

    public CompletableFuture<Long> pexpiretime(K key) {
        return k(Cmd.PEXPIRETIME, key, Reply.LONG);
    }

    // ---- keyspace --------------------------------------------------------------------------

    /** KEYS pattern. Refused above 100,000 keys per shard; prefer {@link #scan(String, ScanArgs)}. */
    public CompletableFuture<List<K>> keys(String pattern) {
        return send(args().w(Cmd.KEYS).s(pattern), r(Reply.KEY_LIST));
    }

    public CompletableFuture<K> randomkey() {
        return c0(Cmd.RANDOMKEY, r(Reply.KEY));
    }

    @SafeVarargs
    public final CompletableFuture<Long> touch(K... keys) {
        return keys(Cmd.TOUCH, Reply.LONG, keys);
    }

    public CompletableFuture<Boolean> copy(K source, K destination) {
        return kk(Cmd.COPY, source, destination, Reply.BOOL);
    }

    public CompletableFuture<Boolean> copy(K source, K destination, boolean replace) {
        Args a = args().w(Cmd.COPY).k(source).k(destination);
        if (replace) {
            a.s("REPLACE");
        }
        return send(a, Reply.BOOL);
    }

    /** OBJECT ENCODING key. */
    public CompletableFuture<String> objectEncoding(K key) {
        return send(args().w(Cmd.OBJECT).s("ENCODING").k(key), Reply.STRING);
    }

    /** OBJECT IDLETIME key (needs an LRU maxmemory-policy). */
    public CompletableFuture<Long> objectIdletime(K key) {
        return send(args().w(Cmd.OBJECT).s("IDLETIME").k(key), Reply.LONG);
    }

    /** OBJECT FREQ key (needs allkeys-lfu). */
    public CompletableFuture<Long> objectFreq(K key) {
        return send(args().w(Cmd.OBJECT).s("FREQ").k(key), Reply.LONG);
    }

    /** MEMORY USAGE key. Null when the key is missing. */
    public CompletableFuture<Long> memoryUsage(K key) {
        return send(args().w(Cmd.MEMORY).s("USAGE").k(key), Reply.LONG);
    }

    public CompletableFuture<Long> memoryUsage(K key, long samples) {
        return send(args().w(Cmd.MEMORY).s("USAGE").k(key).s("SAMPLES").l(samples), Reply.LONG);
    }

    /** DUMP key: Ruvio's own payload (only Ruvio's RESTORE reads it). Null when missing. */
    public CompletableFuture<byte[]> dump(K key) {
        return k(Cmd.DUMP, key, Reply.BYTES);
    }

    /** RESTORE key ttl_ms payload. */
    public CompletableFuture<Void> restore(K key, long ttlMillis, byte[] payload) {
        return send(args().w(Cmd.RESTORE).k(key).l(ttlMillis).b(payload), Reply.OK);
    }

    public CompletableFuture<Void> restore(K key, long ttlMillis, byte[] payload, RestoreArgs options) {
        Args a = args().w(Cmd.RESTORE).k(key).l(ttlMillis).b(payload);
        options.write(a);
        return send(a, Reply.OK);
    }
}
