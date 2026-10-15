// SPDX-License-Identifier: MIT
package io.github.salihcantekin.ruvio;

import io.github.salihcantekin.ruvio.args.ScanArgs;
import io.github.salihcantekin.ruvio.args.SetOptions;
import io.github.salihcantekin.ruvio.commands.RuvioSyncCommands;
import io.github.salihcantekin.ruvio.internal.ClusterNode;
import io.github.salihcantekin.ruvio.internal.ClusterRouter;
import io.github.salihcantekin.ruvio.internal.HashSlot;
import io.github.salihcantekin.ruvio.internal.Link;
import io.github.salihcantekin.ruvio.internal.ScanIterator;
import io.github.salihcantekin.ruvio.internal.TxPool;
import io.github.salihcantekin.ruvio.model.FillResult;
import io.github.salihcantekin.ruvio.model.IdemResult;
import io.github.salihcantekin.ruvio.model.KeyValue;
import io.github.salihcantekin.ruvio.model.PubSubMessage;
import io.github.salihcantekin.ruvio.model.RateLimitResult;
import io.github.salihcantekin.ruvio.model.ScoredValue;

import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Stream;

/**
 * Ruvio client: every typed command as a blocking method (inherited from
 * {@link RuvioSyncCommands}), {@link #async()} for the CompletableFuture API, explicit
 * {@link #pipeline()}, {@link #multi()} transactions, {@link #pubSub(Consumer)} and the
 * convenience helpers (locks, leases, semaphores, rate limits, cache-aside, idempotency, scan
 * iterators, typed JSON, capped lists, Bloom "seen before").
 *
 * <p>All views of one client share one multiplexed socket: any number of threads may call it at
 * once and their commands are pipelined. Blocking server commands (BLPOP, XREAD BLOCK, ...) hold
 * that socket while they wait, so run them on {@link #dedicated()}.
 *
 * @param <K> key type (also hash fields and stream field names)
 * @param <V> value type (also members and stream field values)
 */
public final class RuvioClient<K, V> extends RuvioSyncCommands<K, V> implements AutoCloseable {
    private static final SecureRandom RANDOM = new SecureRandom();

    private final Link link;
    private final TxPool txPool;
    private final long timeout;
    /** Non-null when cluster routing is active. */
    private final ClusterRouter router;
    /** A view onto one node (see {@link #onNode}): closing it leaves the shared connection open. */
    private final boolean view;
    private volatile JsonCodec json = JsonCodec.PASSTHROUGH;

    private RuvioClient(Link link, TxPool txPool, RuvioAsync<K, V> async, ClusterRouter router, boolean view) {
        super(async);
        this.link = link;
        this.txPool = txPool;
        this.timeout = link.options.commandTimeoutMillis;
        this.router = router;
        this.view = view;
    }

    /** Connects with UTF-8 String keys and values. */
    public static RuvioClient<String, String> connect(String host, int port) {
        return connect(RuvioOptions.builder().host(host).port(port).build());
    }

    /** Connects with UTF-8 String keys and values. */
    public static RuvioClient<String, String> connect(RuvioOptions options) {
        return connect(options, RuvioCodec.UTF8, RuvioCodec.UTF8);
    }

    /** Connects with the given codecs, for example {@code RuvioCodec.UTF8, RuvioCodec.BYTES}. */
    public static <K, V> RuvioClient<K, V> connect(RuvioOptions options, RuvioCodec<K> keys, RuvioCodec<V> values) {
        if (options.cluster) {
            // The server only has database 0 in cluster mode, so the seed connection never SELECTs.
            RuvioOptions seedOptions = options.database == 0 ? options : options.toBuilder().database(0).build();
            Link seed = new Link(seedOptions);
            ClusterRouter router;
            try {
                router = ClusterRouter.detect(seed, seedOptions);
            } catch (RuntimeException e) {
                seed.close();
                throw e;
            }
            if (router != null) {
                return new RuvioClient<>(seed, router.seed().txPool, new RuvioAsync<>(seed, router, keys, values), router, false);
            }
            seed.close(); // standalone server: reconnect below with the caller's database
        }
        Link link = new Link(options);
        return new RuvioClient<>(link, new TxPool(link), new RuvioAsync<>(link, keys, values), null, false);
    }

    /** The cluster router (null when standalone); package-private for tests. */
    ClusterRouter router() {
        return router;
    }

    /** True when commands are routed by key to a cluster's nodes (false: plain standalone client). */
    public boolean isCluster() {
        return router != null;
    }

    /** {@code host:port} of every node that owns slots, in slot order. Empty for a standalone client. */
    public List<String> nodeAddresses() {
        List<String> out = new ArrayList<>();
        if (router != null) {
            for (ClusterNode n : router.masters()) {
                out.add(n.address);
            }
        }
        return out;
    }

    /**
     * A client that sends everything to one cluster node, whatever the key (for per-node commands such
     * as DBSIZE, FLUSHDB, KEYS, INFO or SCAN). Shares that node's connection; closing the view does
     * not close it. Standalone clients return themselves.
     */
    public RuvioClient<K, V> onNode(String hostPort) {
        if (router == null) {
            return this;
        }
        ClusterNode n = router.node(hostPort);
        if (n == null) {
            throw new IllegalArgumentException("unknown cluster node " + hostPort);
        }
        return nodeView(n);
    }

    private RuvioClient<K, V> nodeView(ClusterNode n) {
        RuvioClient<K, V> c = new RuvioClient<>(n.link, n.txPool, new RuvioAsync<>(n.link, null, async.keyCodec(), async.valueCodec()), null, true);
        c.json = json;
        return c;
    }

    /** The asynchronous API on the same connection. */
    public RuvioAsync<K, V> async() {
        return async;
    }

    /** A view with other codecs on the same connection. Closing any view closes the connection. */
    public <K2, V2> RuvioClient<K2, V2> withCodecs(RuvioCodec<K2> keys, RuvioCodec<V2> values) {
        RuvioClient<K2, V2> c = new RuvioClient<>(link, txPool, async.withCodecs(keys, values), router, view);
        c.json = json;
        return c;
    }

    /** A view with byte[] values (binary safe) on the same connection. */
    public RuvioClient<K, byte[]> binaryValues() {
        return withCodecs(async.keyCodec(), RuvioCodec.BYTES);
    }

    /**
     * A new client with its own socket and the same settings, for blocking commands (BLPOP, BLMOVE,
     * XREAD BLOCK, FILLGET WAIT) that would otherwise hold the shared socket. Close it when done.
     * In cluster mode it is a cluster client of its own, so a blocking command runs on a connection
     * to the node that owns its key without holding the shared one.
     */
    public RuvioClient<K, V> dedicated() {
        RuvioOptions o = link.options.toBuilder().database(link.database).build();
        return connect(o, async.keyCodec(), async.valueCodec());
    }

    /** A new explicit pipeline (commands are sent by {@code flush()} in one write). */
    public Pipeline<K, V> pipeline() {
        return async.pipeline();
    }

    /** Starts a MULTI/EXEC transaction on a dedicated (pooled) connection. */
    public Transaction<K, V> multi() {
        if (router != null) {
            return new Transaction<>(router, timeout, async.keyCodec(), async.valueCodec());
        }
        return new Transaction<>(txPool, timeout, async.keyCodec(), async.valueCodec());
    }

    /** Opens a Pub/Sub subscriber on its own connection. */
    public RuvioPubSub pubSub(Consumer<PubSubMessage> listener) {
        RuvioOptions o = link.options;
        return new RuvioPubSub(o, listener, router);
    }

    /** Sets the codec used by the typed JSON helpers (default {@link JsonCodec#PASSTHROUGH}). */
    public void setJsonCodec(JsonCodec codec) {
        this.json = codec;
    }

    @Override
    protected <T> T await(CompletableFuture<T> future) {
        if (timeout > 0) {
            return Link.await(future, timeout);
        }
        try {
            return future.join();
        } catch (CompletionException e) {
            throw Link.rethrow(e.getCause());
        } catch (java.util.concurrent.CancellationException e) {
            throw new RuvioException("cancelled", e);
        }
    }

    @Override
    public void close() {
        if (view) {
            return;
        }
        txPool.close();
        link.close();
        if (router != null) {
            router.close();
        }
    }

    private static String newToken() {
        byte[] b = new byte[16];
        RANDOM.nextBytes(b);
        StringBuilder sb = new StringBuilder(32);
        for (byte x : b) {
            sb.append(Character.forDigit((x >> 4) & 0xF, 16)).append(Character.forDigit(x & 0xF, 16));
        }
        return sb.toString();
    }

    private static void backoff(int attempt) {
        long micros = Math.min(20_000, 500L << Math.min(attempt, 6));
        LockSupport.parkNanos(micros * 1000);
    }

    // ---- locks, leases, semaphores ---------------------------------------------------------

    /** Tries once to take the lock: {@code SET key token NX PX ttl}. Null when someone else holds it. */
    public RuvioLock<K> tryLock(K key, long ttlMillis) {
        RuvioAsync<K, String> c = async.withCodecs(async.keyCodec(), RuvioCodec.UTF8);
        String token = newToken();
        if (await(c.set(key, token, SetOptions.px(ttlMillis).ifAbsent()))) {
            return new RuvioLock<>(c, key, token, timeout);
        }
        return null;
    }

    /** Retries {@link #tryLock} with a short backoff for up to {@code waitMillis}. Null on timeout. */
    public RuvioLock<K> lock(K key, long ttlMillis, long waitMillis) {
        long deadline = System.nanoTime() + waitMillis * 1_000_000L;
        for (int attempt = 0; ; attempt++) {
            RuvioLock<K> lock = tryLock(key, ttlMillis);
            if (lock != null || System.nanoTime() >= deadline) {
                return lock;
            }
            backoff(attempt);
        }
    }

    /** LEASE: a held lease with its fencing token, or null while someone else holds it. */
    public RuvioToken<K> tryLease(K key, long ttlMillis) {
        Long token = await(async.lease(key, ttlMillis));
        return token == null ? null : new RuvioToken<>(async, key, token, timeout);
    }

    /** SEMAPHORE: one of {@code max} slots, or null when all are taken. */
    public RuvioToken<K> tryAcquirePermit(K key, long max, long ttlMillis) {
        Long token = await(async.semaphore(key, max, ttlMillis));
        return token == null ? null : new RuvioToken<>(async, key, token, timeout);
    }

    // ---- rate limit ------------------------------------------------------------------------

    /** LIMIT key max window: whether this call fits in the fixed window, and how many remain. */
    public RateLimitResult rateLimit(K key, long max, long windowMillis) {
        Long remaining = await(async.limit(key, max, windowMillis));
        return remaining == null ? new RateLimitResult(false, 0) : new RateLimitResult(true, remaining);
    }

    // ---- cache-aside -----------------------------------------------------------------------

    /**
     * Cache-aside with stampede protection on FILLGET/FILLSET: returns the cached value, or lets
     * exactly one caller run {@code factory} while the others poll plain FILLGET with a short
     * backoff (FILLGET WAIT would park the shared connection). {@code fillMillis} bounds how long
     * a filler may take before another caller takes over.
     */
    public V getOrCreate(K key, long ttlMillis, Supplier<V> factory) {
        return getOrCreate(key, ttlMillis, 0, Math.max(1_000, Math.min(ttlMillis, 30_000)), factory);
    }

    /**
     * Like {@link #getOrCreate(Object, long, Supplier)} with stale-while-revalidate: a value is
     * kept {@code staleMillis} past its TTL; one caller refreshes it while others get the stale value.
     */
    public V getOrCreate(K key, long ttlMillis, long staleMillis, long fillMillis, Supplier<V> factory) {
        long deadline = System.nanoTime() + (fillMillis + 1_000) * 1_000_000L;
        for (int attempt = 0; ; attempt++) {
            FillResult<V> r = await(async.fillget(key, fillMillis));
            switch (r.state()) {
                case HIT:
                    return r.value();
                case STALE:
                    if (r.token() == null) {
                        return r.value();
                    }
                    return fill(key, ttlMillis, staleMillis, r.token(), factory);
                case FILL:
                    return fill(key, ttlMillis, staleMillis, r.token(), factory);
                case BUSY:
                default:
                    if (System.nanoTime() >= deadline) {
                        throw new RuvioException("getOrCreate: value for key was not filled in time");
                    }
                    backoff(attempt);
            }
        }
    }

    private V fill(K key, long ttlMillis, long staleMillis, String token, Supplier<V> factory) {
        V value = factory.get();
        if (value == null) {
            throw new IllegalArgumentException("getOrCreate factory returned null");
        }
        if (staleMillis > 0) {
            await(async.fillset(key, value, ttlMillis, token, staleMillis));
        } else {
            await(async.fillset(key, value, ttlMillis, token));
        }
        return value;
    }

    // ---- idempotency -----------------------------------------------------------------------

    /**
     * Runs {@code action} at most once per (key, fingerprint) while the record lives: IDEM BEGIN
     * reserves the key; the winner runs the action and publishes its result with IDEM COMPLETE;
     * concurrent callers poll until the result is published. A different fingerprint on the same key
     * throws. Not exactly-once for external side effects if the winner crashes mid-way.
     */
    public String idempotent(K key, String fingerprint, long ttlMillis, long waitMillis, Supplier<String> action) {
        String owner = newToken();
        long deadline = System.nanoTime() + waitMillis * 1_000_000L;
        for (int attempt = 0; ; attempt++) {
            IdemResult r = await(async.idemBegin(key, fingerprint, owner, ttlMillis));
            switch (r.state()) {
                case ACQUIRED: {
                    String result = action.get();
                    await(async.idemComplete(key, fingerprint, owner, result.getBytes(StandardCharsets.UTF_8)));
                    return result;
                }
                case COMPLETED:
                    return r.resultString();
                case CONFLICT:
                    throw new RuvioException("idempotency key is reserved for a different fingerprint");
                case PENDING:
                default:
                    if (System.nanoTime() >= deadline) {
                        throw new RuvioException("idempotent: another caller is still running the action");
                    }
                    backoff(attempt);
            }
        }
    }

    // ---- scan iterators --------------------------------------------------------------------

    /**
     * Lazily iterates every key matching {@code options} (null for all) with SCAN. In cluster mode
     * the iterator walks the nodes one after the other (each with its own cursor), so it sees every
     * node's keys; a single {@code scan(cursor)} call only covers the node it lands on.
     */
    public Iterable<K> scanIterable(ScanArgs options) {
        if (router != null) {
            return () -> new NodeChain<>(router.masters(), n -> {
                RuvioClient<K, V> c = nodeView(n);
                return c.scanIterable(options).iterator();
            });
        }
        return ScanIterator.iterable(cursor -> await(options == null ? async.scan(cursor) : async.scan(cursor, options)));
    }

    public Stream<K> scanStream(ScanArgs options) {
        if (router != null) {
            return ScanIterator.streamOf(scanIterable(options));
        }
        return ScanIterator.stream(cursor -> await(options == null ? async.scan(cursor) : async.scan(cursor, options)));
    }

    /** Concatenates the iterators of several nodes, opening each one only when the previous is exhausted. */
    private static final class NodeChain<T> implements java.util.Iterator<T> {
        private final java.util.Iterator<ClusterNode> nodes;
        private final java.util.function.Function<ClusterNode, java.util.Iterator<T>> open;
        private java.util.Iterator<T> current = java.util.Collections.emptyIterator();

        NodeChain(List<ClusterNode> nodes, java.util.function.Function<ClusterNode, java.util.Iterator<T>> open) {
            this.nodes = nodes.iterator();
            this.open = open;
        }

        @Override
        public boolean hasNext() {
            while (!current.hasNext()) {
                if (!nodes.hasNext()) {
                    return false;
                }
                current = open.apply(nodes.next());
            }
            return true;
        }

        @Override
        public T next() {
            if (!hasNext()) {
                throw new java.util.NoSuchElementException();
            }
            return current.next();
        }
    }

    public Iterable<KeyValue<K, V>> hscanIterable(K key, ScanArgs options) {
        return ScanIterator.iterable(cursor -> await(options == null ? async.hscan(key, cursor) : async.hscan(key, cursor, options)));
    }

    public Stream<KeyValue<K, V>> hscanStream(K key, ScanArgs options) {
        return ScanIterator.stream(cursor -> await(options == null ? async.hscan(key, cursor) : async.hscan(key, cursor, options)));
    }

    public Iterable<V> sscanIterable(K key, ScanArgs options) {
        return ScanIterator.iterable(cursor -> await(options == null ? async.sscan(key, cursor) : async.sscan(key, cursor, options)));
    }

    public Stream<V> sscanStream(K key, ScanArgs options) {
        return ScanIterator.stream(cursor -> await(options == null ? async.sscan(key, cursor) : async.sscan(key, cursor, options)));
    }

    public Iterable<ScoredValue<V>> zscanIterable(K key, ScanArgs options) {
        return ScanIterator.iterable(cursor -> await(options == null ? async.zscan(key, cursor) : async.zscan(key, cursor, options)));
    }

    public Stream<ScoredValue<V>> zscanStream(K key, ScanArgs options) {
        return ScanIterator.stream(cursor -> await(options == null ? async.zscan(key, cursor) : async.zscan(key, cursor, options)));
    }

    // ---- typed JSON ------------------------------------------------------------------------

    /** JSON.SET key path with {@code value} serialized by the configured {@link JsonCodec}. */
    public boolean jsonSetObject(K key, String path, Object value) {
        return await(async.jsonSet(key, path, json.toJson(value)));
    }

    /** JSON.GET key path deserialized to {@code type}; null when missing. */
    public <T> T jsonGetObject(K key, String path, Class<T> type) {
        String text = await(async.jsonGet(key, path));
        return text == null ? null : json.fromJson(text, type);
    }

    /** JSON.MGET with each document deserialized to {@code type} (null entries stay null). */
    @SafeVarargs
    public final <T> List<T> jsonMgetObjects(String path, Class<T> type, K... keys) {
        List<String> texts = await(async.jsonMget(path, keys));
        List<T> out = new ArrayList<>(texts.size());
        for (String t : texts) {
            out.add(t == null ? null : json.fromJson(t, type));
        }
        return out;
    }

    // ---- capped lists and Bloom --------------------------------------------------------------

    /** "Recent items": LPUSHCAP key cap value... keeps the newest {@code cap} items. Returns the length. */
    @SafeVarargs
    public final long pushRecent(K key, long cap, V... values) {
        return await(async.lpushcap(key, cap, values));
    }

    /** The newest {@code count} items pushed with {@link #pushRecent}, newest first. */
    public List<V> recent(K key, long count) {
        return await(async.lrange(key, 0, count - 1));
    }

    /**
     * Bloom "seen before": BF.MADD each item and report, per item, whether it was (probably)
     * already present. False positives are possible; false negatives are not.
     */
    @SafeVarargs
    public final List<Boolean> seenBefore(K key, V... items) {
        List<Boolean> added = await(async.bfMadd(key, items));
        List<Boolean> out = new ArrayList<>(added.size());
        for (Boolean a : added) {
            out.add(!a);
        }
        return out;
    }

    /** Bloom membership check without adding (BF.MEXISTS). */
    @SafeVarargs
    public final List<Boolean> mightContain(K key, V... items) {
        return await(async.bfMexists(key, items));
    }

    /** The CRC16 cluster hash slot of a key ({@code {hashtag}} aware), computed locally. */
    public static int hashSlot(byte[] key) {
        return HashSlot.of(key);
    }

    public static int hashSlot(String key) {
        return HashSlot.of(key.getBytes(StandardCharsets.UTF_8));
    }
}
