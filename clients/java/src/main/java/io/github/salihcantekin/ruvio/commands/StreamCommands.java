// SPDX-License-Identifier: MIT
package io.github.salihcantekin.ruvio.commands;

import io.github.salihcantekin.ruvio.RuvioCodec;
import io.github.salihcantekin.ruvio.args.XAddArgs;
import io.github.salihcantekin.ruvio.args.XClaimArgs;
import io.github.salihcantekin.ruvio.args.XReadArgs;
import io.github.salihcantekin.ruvio.internal.Args;
import io.github.salihcantekin.ruvio.internal.Cmd;
import io.github.salihcantekin.ruvio.internal.Reply;
import io.github.salihcantekin.ruvio.internal.Request;
import io.github.salihcantekin.ruvio.model.AutoClaimResult;
import io.github.salihcantekin.ruvio.model.PendingEntry;
import io.github.salihcantekin.ruvio.model.PendingSummary;
import io.github.salihcantekin.ruvio.model.StreamEntry;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Stream commands. Field names use the key codec and field values the value codec. BLOCK reads
 * hold the connection while they wait: run them on {@code client.dedicated()}.
 */
public abstract class StreamCommands<K, V> extends GeoCommands<K, V> {
    protected StreamCommands(RuvioCodec<K> keys, RuvioCodec<V> values) {
        super(keys, values);
    }

    static StreamEntry<Object, Object> entry(Object tree, Request<?> q) {
        List<Object> parts = Reply.list(tree);
        String id = Reply.str(parts.get(0));
        Object rawFields = parts.size() > 1 ? parts.get(1) : null;
        Map<Object, Object> fields = new LinkedHashMap<>();
        if (rawFields != null) {
            List<Object> f = Reply.list(rawFields);
            for (int i = 0; i + 1 < f.size(); i += 2) {
                fields.put(Reply.decodeWith(q.keys, f.get(i)), Reply.decodeWith(q.values, f.get(i + 1)));
            }
        }
        return new StreamEntry<>(id, fields);
    }

    static List<StreamEntry<Object, Object>> entries(Object tree, Request<?> q) {
        if (tree == null) {
            return Collections.emptyList();
        }
        List<Object> items = Reply.list(tree);
        if (!items.isEmpty() && !(items.get(0) instanceof List) && items.get(0) != null) {
            // A single entry not wrapped in a list (observed on Ruvio's XREAD BLOCK wake-up reply).
            List<StreamEntry<Object, Object>> one = new ArrayList<>(1);
            one.add(entry(items, q));
            return one;
        }
        List<StreamEntry<Object, Object>> out = new ArrayList<>(items.size());
        for (Object item : items) {
            if (item != null) {
                out.add(entry(item, q));
            }
        }
        return out;
    }

    static final Reply<List<StreamEntry<Object, Object>>> ENTRIES = Reply.of(StreamCommands::entries);

    /** XREAD / XREADGROUP: key -> entries, in reply order. Empty on timeout. */
    @SuppressWarnings("unchecked")
    static final Reply<Map<Object, List<StreamEntry<Object, Object>>>> READ = Reply.of((tree, q) -> {
        Map<Object, List<StreamEntry<Object, Object>>> out = new LinkedHashMap<>();
        if (tree == null) {
            return out;
        }
        if (tree instanceof Map) {
            for (Map.Entry<Object, Object> e : ((Map<Object, Object>) tree).entrySet()) {
                out.put(Reply.decodeWith(q.keys, e.getKey()), entries(e.getValue(), q));
            }
            return out;
        }
        for (Object stream : Reply.list(tree)) {
            List<Object> pair = Reply.list(stream);
            out.put(Reply.decodeWith(q.keys, pair.get(0)), entries(pair.get(1), q));
        }
        return out;
    });

    static final Reply<PendingSummary> PENDING_SUMMARY = Reply.of((tree, q) -> {
        List<Object> items = Reply.list(tree);
        Map<String, Long> consumers = new LinkedHashMap<>();
        if (items.size() > 3 && items.get(3) != null) {
            for (Object row : Reply.list(items.get(3))) {
                List<Object> p = Reply.list(row);
                consumers.put(Reply.str(p.get(0)), Reply.lng(p.get(1)));
            }
        }
        return new PendingSummary(Reply.lng(items.get(0)), Reply.str(items.get(1)), Reply.str(items.get(2)), consumers);
    });

    static final Reply<List<PendingEntry>> PENDING_ROWS = Reply.of((tree, q) -> {
        List<Object> items = Reply.list(tree);
        List<PendingEntry> out = new ArrayList<>(items.size());
        for (Object row : items) {
            List<Object> p = Reply.list(row);
            out.add(new PendingEntry(Reply.str(p.get(0)), Reply.str(p.get(1)), Reply.lng(p.get(2)), Reply.lng(p.get(3))));
        }
        return out;
    });

    static final Reply<AutoClaimResult<Object, Object>> AUTOCLAIM = Reply.of((tree, q) -> {
        List<Object> items = Reply.list(tree);
        List<StreamEntry<Object, Object>> entries = new ArrayList<>();
        List<String> ids = new ArrayList<>();
        for (Object item : Reply.list(items.get(1))) {
            if (item instanceof List) {
                StreamEntry<Object, Object> e = entry(item, q);
                entries.add(e);
                ids.add(e.id());
            } else if (item != null) {
                ids.add(Reply.str(item));
            }
        }
        List<String> deleted = new ArrayList<>();
        if (items.size() > 2) {
            for (Object d : Reply.list(items.get(2))) {
                deleted.add(Reply.str(d));
            }
        }
        return new AutoClaimResult<>(Reply.str(items.get(0)), entries, ids, deleted);
    });

    @SuppressWarnings("unchecked")
    static final Reply<Map<String, Object>> INFO_MAP = Reply.of((tree, q) -> (Map<String, Object>) (Map<?, ?>) Reply.fieldMap(Reply.text(tree)));

    static final Reply<List<Map<String, Object>>> INFO_LIST = Reply.of((tree, q) -> {
        List<Map<String, Object>> out = new ArrayList<>();
        for (Object row : Reply.list(tree)) {
            out.add(Reply.fieldMap(Reply.text(row)));
        }
        return out;
    });

    private void fields(Args a, Map<K, V> fields) {
        if (fields.isEmpty()) {
            throw new IllegalArgumentException("a stream entry needs at least one field");
        }
        for (Map.Entry<K, V> e : fields.entrySet()) {
            a.k(e.getKey()).v(e.getValue());
        }
    }

    /** XADD key * field value ...: the new id. */
    public CompletableFuture<String> xadd(K key, Map<K, V> fields) {
        Args a = args().w(Cmd.XADD).k(key).s("*");
        fields(a, fields);
        return send(a, Reply.STRING);
    }

    /** XADD key id field value ... */
    public CompletableFuture<String> xadd(K key, String id, Map<K, V> fields) {
        Args a = args().w(Cmd.XADD).k(key).s(id);
        fields(a, fields);
        return send(a, Reply.STRING);
    }

    /** XADD with options (NOMKSTREAM, DELAY, MAXLEN/MINID, id). Null when NOMKSTREAM met a missing key. */
    public CompletableFuture<String> xadd(K key, XAddArgs options, Map<K, V> fields) {
        Args a = args().w(Cmd.XADD).k(key);
        options.write(a);
        fields(a, fields);
        return send(a, Reply.STRING);
    }

    public CompletableFuture<Long> xlen(K key) {
        return k(Cmd.XLEN, key, Reply.LONG);
    }

    public CompletableFuture<List<StreamEntry<K, V>>> xrange(K key, String start, String end) {
        return send(args().w(Cmd.XRANGE).k(key).s(start).s(end), r(ENTRIES));
    }

    public CompletableFuture<List<StreamEntry<K, V>>> xrange(K key, String start, String end, long count) {
        return send(args().w(Cmd.XRANGE).k(key).s(start).s(end).w(Cmd.COUNT).l(count), r(ENTRIES));
    }

    /** XREVRANGE key end start: the high id comes first. */
    public CompletableFuture<List<StreamEntry<K, V>>> xrevrange(K key, String end, String start) {
        return send(args().w(Cmd.XREVRANGE).k(key).s(end).s(start), r(ENTRIES));
    }

    public CompletableFuture<List<StreamEntry<K, V>>> xrevrange(K key, String end, String start, long count) {
        return send(args().w(Cmd.XREVRANGE).k(key).s(end).s(start).w(Cmd.COUNT).l(count), r(ENTRIES));
    }

    /** XREAD STREAMS key id: entries after {@code id} ("0", "$" or ms-seq). */
    public CompletableFuture<Map<K, List<StreamEntry<K, V>>>> xread(K key, String id) {
        return send(args().w(Cmd.XREAD).w(Cmd.STREAMS).k(key).s(id), r(READ));
    }

    /** XREAD [COUNT n] [BLOCK ms] STREAMS key... id... ({@code streams} maps key -> id, in order). */
    public CompletableFuture<Map<K, List<StreamEntry<K, V>>>> xread(XReadArgs options, Map<K, String> streams) {
        Args a = args().w(Cmd.XREAD);
        options.write(a, false);
        a.w(Cmd.STREAMS);
        for (K key : streams.keySet()) {
            a.k(key);
        }
        for (String id : streams.values()) {
            a.s(id);
        }
        return send(a, r(READ));
    }

    public CompletableFuture<Long> xdel(K key, String... ids) {
        return send(args().w(Cmd.XDEL).k(key).strings(ids), Reply.LONG);
    }

    public CompletableFuture<Void> xsetid(K key, String lastId) {
        return ks(Cmd.XSETID, key, lastId, Reply.OK);
    }

    /** XTRIM key MAXLEN [~] n: removed count (Ruvio trims exactly). */
    public CompletableFuture<Long> xtrimMaxlen(K key, long maxLen, boolean approximate) {
        Args a = args().w(Cmd.XTRIM).k(key).s("MAXLEN");
        if (approximate) a.s("~");
        return send(a.l(maxLen), Reply.LONG);
    }

    /** XTRIM key MINID [~] id: drops entries below the id. */
    public CompletableFuture<Long> xtrimMinid(K key, String minId, boolean approximate) {
        Args a = args().w(Cmd.XTRIM).k(key).s("MINID");
        if (approximate) a.s("~");
        return send(a.s(minId), Reply.LONG);
    }

    /** XGROUP CREATE key group id ("$", "0" or ms-seq). */
    public CompletableFuture<Void> xgroupCreate(K key, String group, String id) {
        return send(args().w(Cmd.XGROUP).s("CREATE").k(key).s(group).s(id), Reply.OK);
    }

    /** XGROUP CREATE key group id MKSTREAM. */
    public CompletableFuture<Void> xgroupCreate(K key, String group, String id, boolean mkStream) {
        Args a = args().w(Cmd.XGROUP).s("CREATE").k(key).s(group).s(id);
        if (mkStream) a.s("MKSTREAM");
        return send(a, Reply.OK);
    }

    public CompletableFuture<Boolean> xgroupCreateconsumer(K key, String group, String consumer) {
        return send(args().w(Cmd.XGROUP).s("CREATECONSUMER").k(key).s(group).s(consumer), Reply.BOOL);
    }

    public CompletableFuture<Boolean> xgroupDestroy(K key, String group) {
        return send(args().w(Cmd.XGROUP).s("DESTROY").k(key).s(group), Reply.BOOL);
    }

    public CompletableFuture<Void> xgroupSetid(K key, String group, String id) {
        return send(args().w(Cmd.XGROUP).s("SETID").k(key).s(group).s(id), Reply.OK);
    }

    /** XGROUP DELCONSUMER: how many pending entries the consumer held. */
    public CompletableFuture<Long> xgroupDelconsumer(K key, String group, String consumer) {
        return send(args().w(Cmd.XGROUP).s("DELCONSUMER").k(key).s(group).s(consumer), Reply.LONG);
    }

    /** XREADGROUP GROUP group consumer STREAMS key &gt;. Ruvio reads one stream and the {@code >} id. */
    public CompletableFuture<Map<K, List<StreamEntry<K, V>>>> xreadgroup(String group, String consumer, K key) {
        return send(args().w(Cmd.XREADGROUP).s("GROUP").s(group).s(consumer).w(Cmd.STREAMS).k(key).s(">"), r(READ));
    }

    /** XREADGROUP GROUP group consumer [COUNT n] [BLOCK ms] [NOACK] STREAMS key id. */
    public CompletableFuture<Map<K, List<StreamEntry<K, V>>>> xreadgroup(String group, String consumer, XReadArgs options, K key, String id) {
        Args a = args().w(Cmd.XREADGROUP).s("GROUP").s(group).s(consumer);
        options.write(a, true);
        return send(a.w(Cmd.STREAMS).k(key).s(id), r(READ));
    }

    public CompletableFuture<Long> xack(K key, String group, String... ids) {
        return send(args().w(Cmd.XACK).k(key).s(group).strings(ids), Reply.LONG);
    }

    /** XPENDING key group: the summary form. */
    public CompletableFuture<PendingSummary> xpending(K key, String group) {
        return send(args().w(Cmd.XPENDING).k(key).s(group), PENDING_SUMMARY);
    }

    /** XPENDING key group start end count. */
    public CompletableFuture<List<PendingEntry>> xpending(K key, String group, String start, String end, long count) {
        return send(args().w(Cmd.XPENDING).k(key).s(group).s(start).s(end).l(count), PENDING_ROWS);
    }

    /** XPENDING key group [IDLE min_idle] start end count [consumer]; idle &lt;= 0 and null consumer are omitted. */
    public CompletableFuture<List<PendingEntry>> xpending(K key, String group, long minIdleMillis, String start, String end, long count, String consumer) {
        Args a = args().w(Cmd.XPENDING).k(key).s(group);
        if (minIdleMillis > 0) a.s("IDLE").l(minIdleMillis);
        a.s(start).s(end).l(count);
        if (consumer != null) a.s(consumer);
        return send(a, PENDING_ROWS);
    }

    /** XAUTOCLAIM key group consumer min_idle start. */
    public CompletableFuture<AutoClaimResult<K, V>> xautoclaim(K key, String group, String consumer, long minIdleMillis, String start) {
        return send(args().w(Cmd.XAUTOCLAIM).k(key).s(group).s(consumer).l(minIdleMillis).s(start), r(AUTOCLAIM));
    }

    /** XAUTOCLAIM ... [COUNT n] [JUSTID]; count &lt;= 0 is omitted. */
    public CompletableFuture<AutoClaimResult<K, V>> xautoclaim(K key, String group, String consumer, long minIdleMillis, String start, long count, boolean justId) {
        Args a = args().w(Cmd.XAUTOCLAIM).k(key).s(group).s(consumer).l(minIdleMillis).s(start);
        if (count > 0) a.w(Cmd.COUNT).l(count);
        if (justId) a.s("JUSTID");
        return send(a, r(AUTOCLAIM));
    }

    /** XCLAIM key group consumer min_idle id...: the claimed entries. */
    public CompletableFuture<List<StreamEntry<K, V>>> xclaim(K key, String group, String consumer, long minIdleMillis, String... ids) {
        return send(args().w(Cmd.XCLAIM).k(key).s(group).s(consumer).l(minIdleMillis).strings(ids), r(ENTRIES));
    }

    public CompletableFuture<List<StreamEntry<K, V>>> xclaim(K key, String group, String consumer, long minIdleMillis, XClaimArgs options, String... ids) {
        Args a = args().w(Cmd.XCLAIM).k(key).s(group).s(consumer).l(minIdleMillis).strings(ids);
        options.write(a);
        return send(a, r(ENTRIES));
    }

    /** XCLAIM ... JUSTID: the claimed ids. */
    public CompletableFuture<List<String>> xclaimJustid(K key, String group, String consumer, long minIdleMillis, XClaimArgs options, String... ids) {
        Args a = args().w(Cmd.XCLAIM).k(key).s(group).s(consumer).l(minIdleMillis).strings(ids);
        options.write(a);
        return send(a.s("JUSTID"), Reply.STRING_LIST);
    }

    /** XINFO STREAM key: field -> value (entries as nested lists of strings). */
    public CompletableFuture<Map<String, Object>> xinfoStream(K key) {
        return send(args().w(Cmd.XINFO).s("STREAM").k(key), INFO_MAP);
    }

    public CompletableFuture<List<Map<String, Object>>> xinfoGroups(K key) {
        return send(args().w(Cmd.XINFO).s("GROUPS").k(key), INFO_LIST);
    }

    public CompletableFuture<List<Map<String, Object>>> xinfoConsumers(K key, String group) {
        return send(args().w(Cmd.XINFO).s("CONSUMERS").k(key).s(group), INFO_LIST);
    }
}
