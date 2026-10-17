// SPDX-License-Identifier: MIT
package io.github.salihcantekin.ruvio.commands;

import io.github.salihcantekin.ruvio.RuvioCodec;
import io.github.salihcantekin.ruvio.RuvioServerException;
import io.github.salihcantekin.ruvio.args.ScanArgs;
import io.github.salihcantekin.ruvio.internal.Args;
import io.github.salihcantekin.ruvio.internal.Cmd;
import io.github.salihcantekin.ruvio.internal.Reply;
import io.github.salihcantekin.ruvio.model.ScanResult;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Set commands. Ruvio replies with members sorted.
 *
 * @param <K> key type (also hash fields and stream field names)
 * @param <V> value type (also members, list elements and stream field values)
 */
public abstract class SetCommands<K, V> extends ListCommands<K, V> {
    /**
     * Creates the command family with the given codecs.
     *
     * @param keys the key codec
     * @param values the value codec
     */
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

    /**
     * Sends {@code SADD key member [member ...]}: adds members to a set, creating it when missing.
     *
     * <pre>{@code
     * long added = client.sadd("tags", "red", "blue");
     * }</pre>
     *
     * @param key the set key
     * @param members one or more members
     * @return how many members were new
     * @throws IllegalArgumentException when no member is given
     * @throws RuvioServerException {@code WRONGTYPE} when the key holds a non-set
     */
    @SafeVarargs
    public final CompletableFuture<Long> sadd(K key, V... members) {
        return kvs(Cmd.SADD, key, Reply.LONG, members);
    }

    /**
     * Sends {@code SREM key member [member ...]}: removes members. A set left empty disappears.
     *
     * @param key the set key
     * @param members one or more members
     * @return how many members existed and were removed
     * @throws RuvioServerException {@code WRONGTYPE} when the key holds a non-set
     */
    @SafeVarargs
    public final CompletableFuture<Long> srem(K key, V... members) {
        return kvs(Cmd.SREM, key, Reply.LONG, members);
    }

    /**
     * Sends {@code SISMEMBER key member}.
     *
     * @param key the set key
     * @param member the member to test
     * @return {@code true} when it is a member, {@code false} otherwise (also for a missing key)
     * @throws RuvioServerException {@code WRONGTYPE} when the key holds a non-set
     */
    public CompletableFuture<Boolean> sismember(K key, V member) {
        return kv(Cmd.SISMEMBER, key, member, Reply.BOOL);
    }

    /**
     * Sends {@code SMISMEMBER key member [member ...]}: tests several members at once.
     *
     * @param key the set key
     * @param members one or more members
     * @return one flag per member in the same order
     * @throws RuvioServerException {@code WRONGTYPE} when the key holds a non-set
     */
    @SafeVarargs
    public final CompletableFuture<List<Boolean>> smismember(K key, V... members) {
        return kvs(Cmd.SMISMEMBER, key, Reply.BOOL_LIST, members);
    }

    /**
     * Sends {@code SCARD key}.
     *
     * @param key the set key
     * @return the number of members, {@code 0} for a missing key
     * @throws RuvioServerException {@code WRONGTYPE} when the key holds a non-set
     */
    public CompletableFuture<Long> scard(K key) {
        return k(Cmd.SCARD, key, Reply.LONG);
    }

    /**
     * Sends {@code SMEMBERS key}: all members. Ruvio returns them sorted. Cost grows with the set size; for big sets page with {@link #sscan(Object, String)}.
     *
     * @param key the set key
     * @return the members, empty for a missing key
     * @throws RuvioServerException {@code WRONGTYPE} when the key holds a non-set
     */
    public CompletableFuture<List<V>> smembers(K key) {
        return k(Cmd.SMEMBERS, key, r(Reply.VALUE_LIST));
    }

    /**
     * Sends {@code SINTER key [key ...]}: members present in every set. In a cluster all keys must hash to one slot.
     *
     * @param keys one or more set keys
     * @return the common members, sorted; empty when there are none or any set is missing
     * @throws RuvioServerException {@code WRONGTYPE} for a non-set; {@code CROSSSLOT} in a cluster
     */
    @SafeVarargs
    public final CompletableFuture<List<V>> sinter(K... keys) {
        return keys(Cmd.SINTER, r(Reply.VALUE_LIST), keys);
    }

    /**
     * Sends {@code SUNION key [key ...]}: members present in any set. In a cluster all keys must hash to one slot.
     *
     * @param keys one or more set keys
     * @return the combined members, sorted; missing sets count as empty
     * @throws RuvioServerException {@code WRONGTYPE} for a non-set; {@code CROSSSLOT} in a cluster
     */
    @SafeVarargs
    public final CompletableFuture<List<V>> sunion(K... keys) {
        return keys(Cmd.SUNION, r(Reply.VALUE_LIST), keys);
    }

    /**
     * Sends {@code SDIFF key [key ...]}: members of the first set that are in none of the others. In a cluster all keys must hash to one slot.
     *
     * @param keys the first set followed by the sets to subtract
     * @return the remaining members, sorted
     * @throws RuvioServerException {@code WRONGTYPE} for a non-set; {@code CROSSSLOT} in a cluster
     */
    @SafeVarargs
    public final CompletableFuture<List<V>> sdiff(K... keys) {
        return keys(Cmd.SDIFF, r(Reply.VALUE_LIST), keys);
    }

    /**
     * Sends {@code SINTERSTORE destination key [key ...]}: like {@link #sinter(Object[])}, but stores the result in {@code destination} (overwriting it; an empty result removes it). All keys must hash to one slot in a cluster.
     *
     * @param destination the key that receives the result
     * @param keys one or more source set keys
     * @return the number of members in the stored result
     * @throws RuvioServerException {@code WRONGTYPE} for a non-set source; {@code CROSSSLOT} in a cluster
     */
    @SafeVarargs
    public final CompletableFuture<Long> sinterstore(K destination, K... keys) {
        return send(args().w(Cmd.SINTERSTORE).k(destination).keys(keys), Reply.LONG);
    }

    /**
     * Sends {@code SUNIONSTORE destination key [key ...]}: like {@link #sunion(Object[])}, but stores the result in {@code destination}. All keys must hash to one slot in a cluster.
     *
     * @param destination the key that receives the result
     * @param keys one or more source set keys
     * @return the number of members in the stored result
     * @throws RuvioServerException {@code WRONGTYPE} for a non-set source; {@code CROSSSLOT} in a cluster
     */
    @SafeVarargs
    public final CompletableFuture<Long> sunionstore(K destination, K... keys) {
        return send(args().w(Cmd.SUNIONSTORE).k(destination).keys(keys), Reply.LONG);
    }

    /**
     * Sends {@code SDIFFSTORE destination key [key ...]}: like {@link #sdiff(Object[])}, but stores the result in {@code destination}. All keys must hash to one slot in a cluster.
     *
     * @param destination the key that receives the result
     * @param keys the first set followed by the sets to subtract
     * @return the number of members in the stored result
     * @throws RuvioServerException {@code WRONGTYPE} for a non-set source; {@code CROSSSLOT} in a cluster
     */
    @SafeVarargs
    public final CompletableFuture<Long> sdiffstore(K destination, K... keys) {
        return send(args().w(Cmd.SDIFFSTORE).k(destination).keys(keys), Reply.LONG);
    }

    /**
     * Sends {@code SINTERCARD numkeys key [key ...]}: the size of the intersection without building it.
     *
     * @param keys one or more set keys (same slot in a cluster)
     * @return the number of common members
     * @throws RuvioServerException {@code WRONGTYPE} for a non-set; {@code CROSSSLOT} in a cluster
     */
    @SafeVarargs
    public final CompletableFuture<Long> sintercard(K... keys) {
        return send(args().w(Cmd.SINTERCARD).l(keys.length).keys(keys), Reply.LONG);
    }

    /**
     * Sends {@code SINTERCARD numkeys key [key ...] LIMIT limit}: like {@link #sintercard(Object[])} but stops counting at {@code limit}.
     *
     * @param limit the count at which to stop; {@code 0} means no limit
     * @param keys one or more set keys (same slot in a cluster)
     * @return the number of common members, at most {@code limit} when it is non-zero
     * @throws RuvioServerException {@code WRONGTYPE} for a non-set; {@code CROSSSLOT} in a cluster
     */
    @SafeVarargs
    public final CompletableFuture<Long> sintercard(long limit, K... keys) {
        return send(args().w(Cmd.SINTERCARD).l(keys.length).keys(keys).w(Cmd.LIMIT).l(limit), Reply.LONG);
    }

    /**
     * Sends {@code SMOVE source destination member}: moves a member from one set to another. In a cluster both keys must hash to one slot.
     *
     * @param source the set to take the member from
     * @param destination the set to add it to
     * @param member the member to move
     * @return {@code true} when moved, {@code false} when {@code member} is not in {@code source}
     * @throws RuvioServerException {@code WRONGTYPE} for a non-set; {@code CROSSSLOT} in a cluster
     */
    public CompletableFuture<Boolean> smove(K source, K destination, V member) {
        return send(args().w(Cmd.SMOVE).k(source).k(destination).v(member), Reply.BOOL);
    }

    /**
     * Sends {@code SPOP key}: removes and returns a random member.
     *
     * @param key the set key
     * @return the removed member, or {@code null} when the set is empty or missing
     * @throws RuvioServerException {@code WRONGTYPE} when the key holds a non-set
     */
    public CompletableFuture<V> spop(K key) {
        return k(Cmd.SPOP, key, r(Reply.VALUE));
    }

    /**
     * Sends {@code SPOP key count}: removes and returns up to {@code count} random members. Members come from neighbouring table positions, so they are random between calls but not independent draws.
     *
     * @param key the set key
     * @param count how many to remove; zero or greater
     * @return the removed members, empty when the set is empty or missing
     * @throws RuvioServerException {@code ERR} for a negative count; {@code WRONGTYPE} when the key holds a non-set
     */
    public CompletableFuture<List<V>> spop(K key, long count) {
        return kl(Cmd.SPOP, key, count, r(Reply.VALUE_LIST));
    }

    /**
     * Sends {@code SRANDMEMBER key}: returns a random member without removing it.
     *
     * @param key the set key
     * @return a member, or {@code null} when the set is empty or missing
     * @throws RuvioServerException {@code WRONGTYPE} when the key holds a non-set
     */
    public CompletableFuture<V> srandmember(K key) {
        return k(Cmd.SRANDMEMBER, key, r(Reply.VALUE));
    }

    /**
     * Sends {@code SRANDMEMBER key count}: returns random members without removing them.
     *
     * @param key the set key
     * @param count a positive count returns up to that many distinct members; a negative count returns {@code -count} members that may repeat (at most 1,048,576)
     * @return the members, empty when the set is empty or missing
     * @throws RuvioServerException {@code ERR count is out of range} beyond the limit; {@code WRONGTYPE} when the key holds a non-set
     */
    public CompletableFuture<List<V>> srandmember(K key, long count) {
        return kl(Cmd.SRANDMEMBER, key, count, r(Reply.VALUE_LIST));
    }

    /**
     * Sends {@code SSCAN key cursor}: walks a set a page at a time. Start with {@code "0"} and pass back {@link ScanResult#cursor()} until {@link ScanResult#isFinished()}. A member can appear twice if the set is rebuilt between pages, but none is skipped. See {@link io.github.salihcantekin.ruvio.RuvioClient#sscanIterable}.
     *
     * @param key the set key
     * @param cursor {@code "0"} to start, otherwise the previous page's cursor
     * @return the next cursor and the members of this page
     * @throws RuvioServerException {@code WRONGTYPE} when the key holds a non-set
     */
    public CompletableFuture<ScanResult<V>> sscan(K key, String cursor) {
        return send(args().w(Cmd.SSCAN).k(key).s(cursor), r(SCAN_VALUES));
    }

    /**
     * Sends {@code SSCAN key cursor [MATCH pattern] [COUNT n]}: like {@link #sscan(Object, String)} with a member filter and a page size hint.
     *
     * @param key the set key
     * @param cursor {@code "0"} to start, otherwise the previous page's cursor
     * @param options {@code MATCH} glob and {@code COUNT} hint
     * @return the next cursor and the matching members of this page
     * @throws RuvioServerException {@code WRONGTYPE} when the key holds a non-set
     */
    public CompletableFuture<ScanResult<V>> sscan(K key, String cursor, ScanArgs options) {
        Args a = args().w(Cmd.SSCAN).k(key).s(cursor);
        options.write(a, false);
        return send(a, r(SCAN_VALUES));
    }
}
