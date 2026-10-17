// SPDX-License-Identifier: MIT
package io.github.salihcantekin.ruvio.commands;

import io.github.salihcantekin.ruvio.RuvioCodec;
import io.github.salihcantekin.ruvio.args.BitFieldArgs;
import io.github.salihcantekin.ruvio.args.BitOp;
import io.github.salihcantekin.ruvio.args.BitUnit;
import io.github.salihcantekin.ruvio.internal.Args;
import io.github.salihcantekin.ruvio.internal.Cmd;
import io.github.salihcantekin.ruvio.internal.Reply;
import io.github.salihcantekin.ruvio.internal.Request;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/** Lua scripts and functions, bitmaps and HyperLogLog. */
public abstract class ScriptingCommands<K, V> extends JsonCommands<K, V> {
    protected ScriptingCommands(RuvioCodec<K> keys, RuvioCodec<V> values) {
        super(keys, values);
    }

    /** Script results: bulk strings decoded with the value codec, integers Long, arrays List. */
    static Object scriptValue(Object tree, Request<?> q) {
        if (tree instanceof byte[]) {
            return Reply.decodeWith(q.values, tree);
        }
        if (tree instanceof List) {
            List<Object> in = Reply.list(tree);
            List<Object> out = new ArrayList<>(in.size());
            for (Object o : in) {
                out.add(scriptValue(o, q));
            }
            return out;
        }
        return tree;
    }

    static final Reply<Object> SCRIPT = Reply.of(ScriptingCommands::scriptValue);

    static final Reply<List<String>> FUNCTION_NAMES = Reply.of((tree, q) -> {
        List<String> out = new ArrayList<>();
        collectNames(tree, out);
        return out;
    });

    private static void collectNames(Object tree, List<String> out) {
        if (tree instanceof List) {
            for (Object o : Reply.list(tree)) {
                collectNames(o, out);
            }
        } else if (tree != null) {
            out.add(Reply.str(tree));
        }
    }

    private Args script(byte[] cmd, String body, K[] keys, V[] arguments) {
        Args a = args().w(cmd).s(body).l(keys == null ? 0 : keys.length);
        if (keys != null) a.keys(keys);
        if (arguments != null) a.values(arguments);
        return a;
    }

    /** EVAL script numkeys key... arg...: result tree (bulk strings decoded with the value codec). */
    @SafeVarargs
    public final CompletableFuture<Object> eval(String script, K[] keys, V... arguments) {
        return send(script(Cmd.EVAL, script, keys, arguments), SCRIPT);
    }

    @SafeVarargs
    public final CompletableFuture<Object> evalsha(String sha1, K[] keys, V... arguments) {
        return send(script(Cmd.EVALSHA, sha1, keys, arguments), SCRIPT);
    }

    /** SCRIPT LOAD: the SHA1 (cache is per shard, memory only). */
    public CompletableFuture<String> scriptLoad(String script) {
        return send(args().w(Cmd.SCRIPT).s("LOAD").s(script), Reply.STRING);
    }

    public CompletableFuture<List<Boolean>> scriptExists(String... sha1s) {
        return send(args().w(Cmd.SCRIPT).s("EXISTS").strings(sha1s), Reply.BOOL_LIST);
    }

    public CompletableFuture<Void> scriptFlush() {
        return send(args().w(Cmd.SCRIPT).s("FLUSH"), Reply.OK);
    }

    /** SCRIPT KILL: fails with NOTBUSY when no script is running. */
    public CompletableFuture<Void> scriptKill() {
        return send(args().w(Cmd.SCRIPT).s("KILL"), Reply.OK);
    }

    /** FCALL function numkeys key... arg... */
    @SafeVarargs
    public final CompletableFuture<Object> fcall(String function, K[] keys, V... arguments) {
        return send(script(Cmd.FCALL, function, keys, arguments), SCRIPT);
    }

    /** FUNCTION LOAD name body: stores one named Lua body on the shard (memory only). */
    public CompletableFuture<Void> functionLoad(String name, String body) {
        return send(args().w(Cmd.FUNCTION).s("LOAD").s(name).s(body), Reply.OK);
    }

    /** FUNCTION LIST: loaded function names in byte order. */
    public CompletableFuture<List<String>> functionList() {
        return send(args().w(Cmd.FUNCTION).s("LIST"), FUNCTION_NAMES);
    }

    public CompletableFuture<Boolean> functionDelete(String name) {
        return send(args().w(Cmd.FUNCTION).s("DELETE").s(name), Reply.BOOL);
    }

    // ---- bitmaps ---------------------------------------------------------------------------

    /** SETBIT key offset 0|1: the previous bit. */
    public CompletableFuture<Long> setbit(K key, long offset, boolean value) {
        return kll(Cmd.SETBIT, key, offset, value ? 1 : 0, Reply.LONG);
    }

    public CompletableFuture<Long> getbit(K key, long offset) {
        return kl(Cmd.GETBIT, key, offset, Reply.LONG);
    }

    public CompletableFuture<Long> bitcount(K key) {
        return k(Cmd.BITCOUNT, key, Reply.LONG);
    }

    /** BITCOUNT key start end (byte offsets). */
    public CompletableFuture<Long> bitcount(K key, long start, long end) {
        return kll(Cmd.BITCOUNT, key, start, end, Reply.LONG);
    }

    /** BITCOUNT key start end BIT|BYTE. */
    public CompletableFuture<Long> bitcount(K key, long start, long end, boolean bitUnits) {
        return bitcount(key, start, end, bitUnits ? BitUnit.BIT : BitUnit.BYTE);
    }

    /** BITCOUNT key start end BYTE|BIT with an enum unit, for example {@code BitUnit.BIT}. */
    public CompletableFuture<Long> bitcount(K key, long start, long end, BitUnit unit) {
        return send(args().w(Cmd.BITCOUNT).k(key).l(start).l(end).w(unit.fragment()), Reply.LONG);
    }

    public CompletableFuture<Long> bitpos(K key, boolean bit) {
        return kl(Cmd.BITPOS, key, bit ? 1 : 0, Reply.LONG);
    }

    public CompletableFuture<Long> bitpos(K key, boolean bit, long start) {
        return kll(Cmd.BITPOS, key, bit ? 1 : 0, start, Reply.LONG);
    }

    public CompletableFuture<Long> bitpos(K key, boolean bit, long start, long end) {
        return send(args().w(Cmd.BITPOS).k(key).l(bit ? 1 : 0).l(start).l(end), Reply.LONG);
    }

    /** BITOP AND|OR|XOR|NOT destination key...: the result length. */
    @SafeVarargs
    public final CompletableFuture<Long> bitop(BitOp op, K destination, K... keys) {
        return send(args().w(Cmd.BITOP).w(op.fragment()).k(destination).keys(keys), Reply.LONG);
    }

    /** BITFIELD key ops...: one value per GET/SET/INCRBY (null when OVERFLOW FAIL stopped it). */
    public CompletableFuture<List<Long>> bitfield(K key, BitFieldArgs operations) {
        Args a = args().w(Cmd.BITFIELD).k(key);
        operations.write(a);
        return send(a, Reply.LONG_LIST);
    }

    /** BITFIELD_RO key GET ...: GET operations only. */
    public CompletableFuture<List<Long>> bitfieldRo(K key, BitFieldArgs getOperations) {
        Args a = args().w(Cmd.BITFIELD_RO).k(key);
        getOperations.write(a);
        return send(a, Reply.LONG_LIST);
    }

    // ---- HyperLogLog -----------------------------------------------------------------------

    /** PFADD: true when the estimate changed. */
    @SafeVarargs
    public final CompletableFuture<Boolean> pfadd(K key, V... elements) {
        if (elements.length == 0) {
            return k(Cmd.PFADD, key, Reply.BOOL);
        }
        return kvs(Cmd.PFADD, key, Reply.BOOL, elements);
    }

    @SafeVarargs
    public final CompletableFuture<Long> pfcount(K... keys) {
        return keys(Cmd.PFCOUNT, Reply.LONG, keys);
    }

    @SafeVarargs
    public final CompletableFuture<Void> pfmerge(K destination, K... sources) {
        return send(args().w(Cmd.PFMERGE).k(destination).keys(sources), Reply.OK);
    }
}
