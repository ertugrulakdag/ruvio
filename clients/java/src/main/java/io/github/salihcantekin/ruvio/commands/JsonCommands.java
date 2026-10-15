// SPDX-License-Identifier: MIT
package io.github.salihcantekin.ruvio.commands;

import io.github.salihcantekin.ruvio.RuvioCodec;
import io.github.salihcantekin.ruvio.args.BfInsertArgs;
import io.github.salihcantekin.ruvio.internal.Args;
import io.github.salihcantekin.ruvio.internal.Cmd;
import io.github.salihcantekin.ruvio.internal.Reply;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * JSON.* (simple {@code .field} and {@code [N]} paths, Ruvio JSON.INDEX / JSON.FIND) and BF.*
 * (RedisBloom wire shape). JSON documents and paths are UTF-8 text.
 */
public abstract class JsonCommands<K, V> extends StreamCommands<K, V> {
    protected JsonCommands(RuvioCodec<K> keys, RuvioCodec<V> values) {
        super(keys, values);
    }

    static final Reply<Map<String, Object>> BF_INFO = Reply.of((tree, q) -> Reply.fieldMap(Reply.text(tree)));

    static final Reply<Long> FIRST_LONG = Reply.of((tree, q) -> {
        Object v = tree instanceof List ? Reply.list(tree).get(0) : tree;
        return v == null ? null : Reply.lng(v);
    });

    // ---- JSON ------------------------------------------------------------------------------

    /** JSON.GET key (root). Null when the key is missing. */
    public CompletableFuture<String> jsonGet(K key) {
        return k(Cmd.JSON_GET, key, Reply.STRING);
    }

    public CompletableFuture<String> jsonGet(K key, String path) {
        return ks(Cmd.JSON_GET, key, path, Reply.STRING);
    }

    /** JSON.MGET key... path: one JSON text (or null) per key. */
    @SafeVarargs
    public final CompletableFuture<List<String>> jsonMget(String path, K... keys) {
        return send(args().w(Cmd.JSON_MGET).keys(keys).s(path), Reply.STRING_LIST);
    }

    /** JSON.SET key path json. */
    public CompletableFuture<Boolean> jsonSet(K key, String path, String json) {
        return send(args().w(Cmd.JSON_SET).k(key).s(path).s(json), Reply.OK_BOOL);
    }

    /** JSON.SET key path json NX: false when the path already exists. */
    public CompletableFuture<Boolean> jsonSetNx(K key, String path, String json) {
        return send(args().w(Cmd.JSON_SET).k(key).s(path).s(json).w(Cmd.NX), Reply.OK_BOOL);
    }

    /** JSON.SET key path json XX: false when the path is missing. */
    public CompletableFuture<Boolean> jsonSetXx(K key, String path, String json) {
        return send(args().w(Cmd.JSON_SET).k(key).s(path).s(json).w(Cmd.XX), Reply.OK_BOOL);
    }

    /** JSON.MERGE key path patch (RFC 7396). */
    public CompletableFuture<Void> jsonMerge(K key, String path, String patch) {
        return send(args().w(Cmd.JSON_MERGE).k(key).s(path).s(patch), Reply.OK);
    }

    /** JMERGE: alias of JSON.MERGE. */
    public CompletableFuture<Void> jmerge(K key, String path, String patch) {
        return send(args().w(Cmd.JMERGE).k(key).s(path).s(patch), Reply.OK);
    }

    public CompletableFuture<Long> jsonDel(K key) {
        return k(Cmd.JSON_DEL, key, Reply.LONG);
    }

    public CompletableFuture<Long> jsonDel(K key, String path) {
        return ks(Cmd.JSON_DEL, key, path, Reply.LONG);
    }

    /** JSON.FORGET: alias of JSON.DEL. */
    public CompletableFuture<Long> jsonForget(K key, String path) {
        return ks(Cmd.JSON_FORGET, key, path, Reply.LONG);
    }

    /** JSON.TYPE: object, array, string, integer, number, boolean or null; null when absent. */
    public CompletableFuture<String> jsonType(K key, String path) {
        return ks(Cmd.JSON_TYPE, key, path, Reply.STRING);
    }

    public CompletableFuture<Long> jsonStrlen(K key, String path) {
        return ks(Cmd.JSON_STRLEN, key, path, Reply.LONG);
    }

    public CompletableFuture<Long> jsonArrlen(K key, String path) {
        return ks(Cmd.JSON_ARRLEN, key, path, Reply.LONG);
    }

    public CompletableFuture<List<String>> jsonObjkeys(K key, String path) {
        return ks(Cmd.JSON_OBJKEYS, key, path, Reply.STRING_LIST);
    }

    public CompletableFuture<Long> jsonObjlen(K key, String path) {
        return ks(Cmd.JSON_OBJLEN, key, path, Reply.LONG);
    }

    /** JSON.NUMINCRBY: the new number as JSON text (exact, integer or double). */
    public CompletableFuture<String> jsonNumincrby(K key, String path, double delta) {
        return send(args().w(Cmd.JSON_NUMINCRBY).k(key).s(path).d(delta), Reply.STRING);
    }

    public CompletableFuture<String> jsonNummultby(K key, String path, double factor) {
        return send(args().w(Cmd.JSON_NUMMULTBY).k(key).s(path).d(factor), Reply.STRING);
    }

    /** JSON.TOGGLE: the new boolean. */
    public CompletableFuture<Boolean> jsonToggle(K key, String path) {
        return ks(Cmd.JSON_TOGGLE, key, path, Reply.BOOL);
    }

    /** JSON.ARRAPPEND key path json...: the new length. */
    public CompletableFuture<Long> jsonArrappend(K key, String path, String... jsonValues) {
        return send(args().w(Cmd.JSON_ARRAPPEND).k(key).s(path).strings(jsonValues), Reply.LONG);
    }

    public CompletableFuture<Long> jsonArrinsert(K key, String path, long index, String... jsonValues) {
        return send(args().w(Cmd.JSON_ARRINSERT).k(key).s(path).l(index).strings(jsonValues), Reply.LONG);
    }

    /** JSON.ARRPOP key: pops the last element of the root array (JSON text, null when empty). */
    public CompletableFuture<String> jsonArrpop(K key) {
        return k(Cmd.JSON_ARRPOP, key, Reply.STRING);
    }

    public CompletableFuture<String> jsonArrpop(K key, String path) {
        return ks(Cmd.JSON_ARRPOP, key, path, Reply.STRING);
    }

    public CompletableFuture<String> jsonArrpop(K key, String path, long index) {
        return send(args().w(Cmd.JSON_ARRPOP).k(key).s(path).l(index), Reply.STRING);
    }

    public CompletableFuture<Long> jsonArrtrim(K key, String path, long start, long stop) {
        return send(args().w(Cmd.JSON_ARRTRIM).k(key).s(path).l(start).l(stop), Reply.LONG);
    }

    /** JSON.ARRINDEX key path scalar: index or -1. {@code scalarJson} is JSON text, e.g. {@code "\"a\""} or {@code 3}. */
    public CompletableFuture<Long> jsonArrindex(K key, String path, String scalarJson) {
        return send(args().w(Cmd.JSON_ARRINDEX).k(key).s(path).s(scalarJson), Reply.LONG);
    }

    public CompletableFuture<Long> jsonArrindex(K key, String path, String scalarJson, long start, long stop) {
        return send(args().w(Cmd.JSON_ARRINDEX).k(key).s(path).s(scalarJson).l(start).l(stop), Reply.LONG);
    }

    public CompletableFuture<Long> jsonClear(K key, String path) {
        return ks(Cmd.JSON_CLEAR, key, path, Reply.LONG);
    }

    public CompletableFuture<Long> jsonStrappend(K key, String path, String jsonString) {
        return send(args().w(Cmd.JSON_STRAPPEND).k(key).s(path).s(jsonString), Reply.LONG);
    }

    /**
     * JSON.INDEX CREATE name PREFIX prefix PATH path [TYPE string|number|bool]. Database 0 only;
     * {@code type} may be null.
     */
    public CompletableFuture<Void> jsonIndexCreate(String name, String prefix, String path, String type) {
        Args a = args().w(Cmd.JSON_INDEX).s("CREATE").s(name).s("PREFIX").s(prefix).s("PATH").s(path);
        if (type != null) a.w(Cmd.TYPE).s(type);
        return send(a, Reply.OK);
    }

    /** JSON.INDEX DROP name: 1 when dropped. */
    public CompletableFuture<Long> jsonIndexDrop(String name) {
        return send(args().w(Cmd.JSON_INDEX).s("DROP").s(name), Reply.LONG);
    }

    /** JSON.INDEX LIST: one description line per index. */
    public CompletableFuture<List<String>> jsonIndexList() {
        return send(args().w(Cmd.JSON_INDEX).s("LIST"), Reply.STRING_LIST);
    }

    /** JSON.FIND name VALUE v: matching keys in ascending order (fails with "index is building" at first). */
    public CompletableFuture<List<K>> jsonFind(String index, String value) {
        return send(args().w(Cmd.JSON_FIND).s(index).s("VALUE").s(value), r(Reply.KEY_LIST));
    }

    /** JSON.FIND name VALUE v COUNT n [AFTER key]; {@code after} may be null. */
    public CompletableFuture<List<K>> jsonFind(String index, String value, long count, K after) {
        Args a = args().w(Cmd.JSON_FIND).s(index).s("VALUE").s(value).w(Cmd.COUNT).l(count);
        if (after != null) a.s("AFTER").k(after);
        return send(a, r(Reply.KEY_LIST));
    }

    // ---- Bloom -----------------------------------------------------------------------------

    public CompletableFuture<Void> bfReserve(K key, double errorRate, long capacity) {
        return send(args().w(Cmd.BF_RESERVE).k(key).d(errorRate).l(capacity), Reply.OK);
    }

    /** BF.RESERVE key error capacity [EXPANSION n | NONSCALING]; expansion &lt;= 0 is omitted. */
    public CompletableFuture<Void> bfReserve(K key, double errorRate, long capacity, long expansion, boolean nonScaling) {
        Args a = args().w(Cmd.BF_RESERVE).k(key).d(errorRate).l(capacity);
        if (nonScaling) {
            a.s("NONSCALING");
        } else if (expansion > 0) {
            a.s("EXPANSION").l(expansion);
        }
        return send(a, Reply.OK);
    }

    /** BF.ADD: true when the item was new. */
    public CompletableFuture<Boolean> bfAdd(K key, V item) {
        return kv(Cmd.BF_ADD, key, item, Reply.BOOL);
    }

    @SafeVarargs
    public final CompletableFuture<List<Boolean>> bfMadd(K key, V... items) {
        return kvs(Cmd.BF_MADD, key, Reply.BOOL_LIST, items);
    }

    /** BF.INSERT key [options] ITEMS item... */
    @SafeVarargs
    public final CompletableFuture<List<Boolean>> bfInsert(K key, BfInsertArgs options, V... items) {
        Args a = args().w(Cmd.BF_INSERT).k(key);
        if (options != null) {
            options.write(a);
        }
        return send(a.s("ITEMS").values(items), Reply.BOOL_LIST);
    }

    public CompletableFuture<Boolean> bfExists(K key, V item) {
        return kv(Cmd.BF_EXISTS, key, item, Reply.BOOL);
    }

    @SafeVarargs
    public final CompletableFuture<List<Boolean>> bfMexists(K key, V... items) {
        return kvs(Cmd.BF_MEXISTS, key, Reply.BOOL_LIST, items);
    }

    public CompletableFuture<Long> bfCard(K key) {
        return k(Cmd.BF_CARD, key, Reply.LONG);
    }

    /** BF.INFO key: Capacity, Size, Number of filters, Number of items inserted, Expansion rate. */
    public CompletableFuture<Map<String, Object>> bfInfo(K key) {
        return k(Cmd.BF_INFO, key, BF_INFO);
    }

    /** BF.INFO key CAPACITY|SIZE|FILTERS|ITEMS|EXPANSION. */
    public CompletableFuture<Long> bfInfo(K key, String field) {
        return ks(Cmd.BF_INFO, key, field, FIRST_LONG);
    }
}
