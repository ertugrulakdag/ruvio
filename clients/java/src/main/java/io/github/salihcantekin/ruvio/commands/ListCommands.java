// SPDX-License-Identifier: MIT
package io.github.salihcantekin.ruvio.commands;

import io.github.salihcantekin.ruvio.RuvioCodec;
import io.github.salihcantekin.ruvio.RuvioServerException;
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
 *
 * @param <K> key type (also hash fields and stream field names)
 * @param <V> value type (also members, list elements and stream field values)
 */
public abstract class ListCommands<K, V> extends HashCommands<K, V> {
    /**
     * Creates the command family with the given codecs.
     *
     * @param keys the key codec
     * @param values the value codec
     */
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

    /**
     * Sends {@code LPUSH key value [value ...]}: adds values to the head of a list, creating it when missing. Values are pushed one after another, so the last argument ends up first.
     *
     * <pre>{@code
     * client.lpush("queue", "a", "b"); // list is now [b, a]
     * }</pre>
     *
     * @param key the list key
     * @param values one or more values
     * @return the list length after the push
     * @throws IllegalArgumentException when no value is given
     * @throws RuvioServerException {@code WRONGTYPE} when the key holds a non-list
     */
    @SafeVarargs
    public final CompletableFuture<Long> lpush(K key, V... values) {
        return kvs(Cmd.LPUSH, key, Reply.LONG, values);
    }

    /**
     * Sends {@code RPUSH key value [value ...]}: adds values to the tail of a list, creating it when missing.
     *
     * @param key the list key
     * @param values one or more values
     * @return the list length after the push
     * @throws IllegalArgumentException when no value is given
     * @throws RuvioServerException {@code WRONGTYPE} when the key holds a non-list
     */
    @SafeVarargs
    public final CompletableFuture<Long> rpush(K key, V... values) {
        return kvs(Cmd.RPUSH, key, Reply.LONG, values);
    }

    /**
     * Sends {@code LPUSHX key value [value ...]}: like {@link #lpush(Object, Object[])} but only when the list already exists.
     *
     * @param key the list key
     * @param values one or more values
     * @return the list length after the push, {@code 0} when the list does not exist (nothing is created)
     * @throws RuvioServerException {@code WRONGTYPE} when the key holds a non-list
     */
    @SafeVarargs
    public final CompletableFuture<Long> lpushx(K key, V... values) {
        return kvs(Cmd.LPUSHX, key, Reply.LONG, values);
    }

    /**
     * Sends {@code RPUSHX key value [value ...]}: like {@link #rpush(Object, Object[])} but only when the list already exists.
     *
     * @param key the list key
     * @param values one or more values
     * @return the list length after the push, {@code 0} when the list does not exist (nothing is created)
     * @throws RuvioServerException {@code WRONGTYPE} when the key holds a non-list
     */
    @SafeVarargs
    public final CompletableFuture<Long> rpushx(K key, V... values) {
        return kvs(Cmd.RPUSHX, key, Reply.LONG, values);
    }

    /**
     * Sends the Ruvio command {@code LPUSHCAP key cap value [value ...]}: pushes to the head, then drops items from the tail until at most {@code cap} remain. It keeps a "latest N" list in one step (see {@link io.github.salihcantekin.ruvio.RuvioClient#pushRecent}).
     *
     * <pre>{@code
     * client.lpushcap("recent:user1", 10, "page-42"); // newest first, at most 10 kept
     * }</pre>
     *
     * @param key the list key
     * @param cap the most items the list may keep; must be greater than zero
     * @param values one or more values
     * @return the list length after the push and trim (never more than {@code cap})
     * @throws RuvioServerException {@code ERR} for an invalid cap; {@code WRONGTYPE} when the key holds a non-list
     */
    @SafeVarargs
    public final CompletableFuture<Long> lpushcap(K key, long cap, V... values) {
        return send(args().w(Cmd.LPUSHCAP).k(key).l(cap).values(values), Reply.LONG);
    }

    /**
     * Sends the Ruvio command {@code RPUSHCAP key cap value [value ...]}: pushes to the tail, then drops items from the head until at most {@code cap} remain.
     *
     * @param key the list key
     * @param cap the most items the list may keep; must be greater than zero
     * @param values one or more values
     * @return the list length after the push and trim (never more than {@code cap})
     * @throws RuvioServerException {@code ERR} for an invalid cap; {@code WRONGTYPE} when the key holds a non-list
     */
    @SafeVarargs
    public final CompletableFuture<Long> rpushcap(K key, long cap, V... values) {
        return send(args().w(Cmd.RPUSHCAP).k(key).l(cap).values(values), Reply.LONG);
    }

    /**
     * Sends {@code LPOP key}: removes and returns the first element.
     *
     * @param key the list key
     * @return the element, or {@code null} when the list is empty or missing
     * @throws RuvioServerException {@code WRONGTYPE} when the key holds a non-list
     */
    public CompletableFuture<V> lpop(K key) {
        return k(Cmd.LPOP, key, r(Reply.VALUE));
    }

    /**
     * Sends {@code LPOP key count}: removes and returns up to {@code count} elements from the head.
     *
     * @param key the list key
     * @param count how many to remove; must be positive
     * @return the removed elements, head first; empty when the list is empty or missing
     * @throws RuvioServerException {@code ERR} for a non-positive count; {@code WRONGTYPE} when the key holds a non-list
     */
    public CompletableFuture<List<V>> lpop(K key, long count) {
        return kl(Cmd.LPOP, key, count, r(Reply.VALUE_LIST));
    }

    /**
     * Sends {@code RPOP key}: removes and returns the last element.
     *
     * @param key the list key
     * @return the element, or {@code null} when the list is empty or missing
     * @throws RuvioServerException {@code WRONGTYPE} when the key holds a non-list
     */
    public CompletableFuture<V> rpop(K key) {
        return k(Cmd.RPOP, key, r(Reply.VALUE));
    }

    /**
     * Sends {@code RPOP key count}: removes and returns up to {@code count} elements from the tail.
     *
     * @param key the list key
     * @param count how many to remove; must be positive
     * @return the removed elements, tail first; empty when the list is empty or missing
     * @throws RuvioServerException {@code ERR} for a non-positive count; {@code WRONGTYPE} when the key holds a non-list
     */
    public CompletableFuture<List<V>> rpop(K key, long count) {
        return kl(Cmd.RPOP, key, count, r(Reply.VALUE_LIST));
    }

    /**
     * Sends {@code LLEN key}.
     *
     * @param key the list key
     * @return the number of elements, {@code 0} when the key does not exist
     * @throws RuvioServerException {@code WRONGTYPE} when the key holds a non-list
     */
    public CompletableFuture<Long> llen(K key) {
        return k(Cmd.LLEN, key, Reply.LONG);
    }

    /**
     * Sends {@code LINDEX key index}.
     *
     * @param key the list key
     * @param index zero-based position; negative counts from the tail ({@code -1} is the last element)
     * @return the element, or {@code null} when the index is out of range or the key is missing
     * @throws RuvioServerException {@code WRONGTYPE} when the key holds a non-list
     */
    public CompletableFuture<V> lindex(K key, long index) {
        return kl(Cmd.LINDEX, key, index, r(Reply.VALUE));
    }

    /**
     * Sends {@code LRANGE key start stop}: reads a slice of a list.
     *
     * <pre>{@code
     * List<String> all = client.lrange("queue", 0, -1);
     * }</pre>
     *
     * @param key the list key
     * @param start first position, inclusive; negative counts from the tail
     * @param stop last position, inclusive; negative counts from the tail ({@code -1} is the last element); clamped to the list
     * @return the elements in order; empty when the range is empty or the key is missing
     * @throws RuvioServerException {@code WRONGTYPE} when the key holds a non-list
     */
    public CompletableFuture<List<V>> lrange(K key, long start, long stop) {
        return kll(Cmd.LRANGE, key, start, stop, r(Reply.VALUE_LIST));
    }

    /**
     * Sends {@code LTRIM key start stop}: keeps only the elements in the range and drops the rest. An empty result removes the key.
     *
     * @param key the list key
     * @param start first position to keep, inclusive; negative counts from the tail
     * @param stop last position to keep, inclusive; negative counts from the tail
     * @throws RuvioServerException {@code WRONGTYPE} when the key holds a non-list
     * @return a future that completes with no value once the server has acknowledged the command
     */
    public CompletableFuture<Void> ltrim(K key, long start, long stop) {
        return kll(Cmd.LTRIM, key, start, stop, Reply.OK);
    }

    /**
     * Sends {@code BLPOP key [key ...] timeout}: removes the first element of the first non-empty list, waiting for one to appear. The wait happens on the server and holds the shared connection, so run it on {@link io.github.salihcantekin.ruvio.RuvioClient#dedicated()}. At most 1,024 clients can wait on one shard. The command is shard-local; in a cluster all keys must hash to one slot.
     *
     * <pre>{@code
     * try (RuvioClient<String, String> worker = client.dedicated()) {
     *     KeyValue<String, String> job = worker.blpop(5, "jobs");
     * }
     * }</pre>
     *
     * @param timeoutSeconds how long to wait in seconds (fractions allowed); {@code 0} waits until an element arrives or the server shuts down
     * @param keys one or more list keys, checked in order
     * @return the key the element came from and the element, or {@code null} when the timeout passed
     * @throws IllegalArgumentException when no key is given
     * @throws RuvioServerException {@code ERR max number of blocking clients reached}; {@code WRONGTYPE} when a key holds a non-list
     */
    @SafeVarargs
    public final CompletableFuture<KeyValue<K, V>> blpop(double timeoutSeconds, K... keys) {
        return send(args().w(Cmd.BLPOP).keys(keys).d(timeoutSeconds), r(KEY_VALUE));
    }

    /**
     * Sends {@code BRPOP key [key ...] timeout}: like {@link #blpop(double, Object[])} but takes from the tail.
     *
     * @param timeoutSeconds how long to wait in seconds; {@code 0} waits without limit
     * @param keys one or more list keys, checked in order
     * @return the key and the removed element, or {@code null} on timeout
     * @throws RuvioServerException {@code ERR max number of blocking clients reached}; {@code WRONGTYPE} when a key holds a non-list
     */
    @SafeVarargs
    public final CompletableFuture<KeyValue<K, V>> brpop(double timeoutSeconds, K... keys) {
        return send(args().w(Cmd.BRPOP).keys(keys).d(timeoutSeconds), r(KEY_VALUE));
    }

    /**
     * Sends {@code LINSERT key BEFORE|AFTER pivot value}: inserts next to the first element equal to {@code pivot}. Rewrites the list (O(N)).
     *
     * @param key the list key
     * @param before {@code true} to insert before the pivot, {@code false} to insert after it
     * @param pivot the existing element to look for
     * @param value the element to insert
     * @return the list length after the insert, {@code -1} when the pivot was not found, {@code 0} when the key does not exist
     * @throws RuvioServerException {@code WRONGTYPE} when the key holds a non-list
     */
    public CompletableFuture<Long> linsert(K key, boolean before, V pivot, V value) {
        return linsert(key, before ? InsertPosition.BEFORE : InsertPosition.AFTER, pivot, value);
    }

    /** LINSERT key BEFORE|AFTER pivot value with an enum position, for example {@code InsertPosition.AFTER}. */
    public CompletableFuture<Long> linsert(K key, InsertPosition position, V pivot, V value) {
        return send(args().w(Cmd.LINSERT).k(key).w(position.fragment()).v(pivot).v(value), Reply.LONG);
    }

    /**
     * Sends {@code LSET key index value}: replaces the element at a position (O(N)).
     *
     * @param key the list key
     * @param index zero-based position; negative counts from the tail
     * @param value the new element
     * @throws RuvioServerException {@code ERR no such key} when the list is missing; {@code ERR index out of range}; {@code WRONGTYPE} for a non-list
     * @return a future that completes with no value once the server has acknowledged the command
     */
    public CompletableFuture<Void> lset(K key, long index, V value) {
        return klv(Cmd.LSET, key, index, value, Reply.OK);
    }

    /**
     * Sends {@code LREM key count value}: removes elements equal to {@code value} (O(N)).
     *
     * @param key the list key
     * @param count positive: remove up to that many starting at the head; negative: starting at the tail; {@code 0}: remove all matches
     * @param value the element to remove
     * @return how many elements were removed, {@code 0} when the key is missing
     * @throws RuvioServerException {@code WRONGTYPE} when the key holds a non-list
     */
    public CompletableFuture<Long> lrem(K key, long count, V value) {
        return klv(Cmd.LREM, key, count, value, Reply.LONG);
    }

    /**
     * Sends {@code LPOS key element}: finds the position of the first match.
     *
     * @param key the list key
     * @param value the element to look for
     * @return the zero-based index, or {@code null} when not found or the key is missing
     * @throws RuvioServerException {@code WRONGTYPE} when the key holds a non-list
     */
    public CompletableFuture<Long> lpos(K key, V value) {
        return kv(Cmd.LPOS, key, value, Reply.LONG);
    }

    /**
     * Sends {@code LPOS key element [RANK r] [MAXLEN m]}.
     *
     * @param key the list key
     * @param value the element to look for
     * @param options {@code rank}: which match to return (1 is the first, 2 the second, negative counts from the tail); {@code maxLen}: stop after comparing this many elements ({@code 0} means no limit)
     * @return the zero-based index of the chosen match, or {@code null} when there is none
     * @throws RuvioServerException {@code ERR} for an invalid rank; {@code WRONGTYPE} when the key holds a non-list
     */
    public CompletableFuture<Long> lpos(K key, V value, LPosArgs options) {
        Args a = args().w(Cmd.LPOS).k(key).v(value);
        options.write(a);
        return send(a, Reply.LONG);
    }

    /**
     * Sends {@code LPOS key element COUNT n}: finds the positions of several matches.
     *
     * @param key the list key
     * @param value the element to look for
     * @param count the most matches to return; {@code 0} returns all
     * @return the zero-based indexes in list order, empty when there is no match
     * @throws RuvioServerException {@code WRONGTYPE} when the key holds a non-list
     */
    public CompletableFuture<List<Long>> lposAll(K key, V value, long count) {
        return send(args().w(Cmd.LPOS).k(key).v(value).w(Cmd.COUNT).l(count), Reply.LONG_LIST);
    }

    /**
     * Sends {@code LPOS key element [RANK r] [MAXLEN m] COUNT n}.
     *
     * @param key the list key
     * @param value the element to look for
     * @param count the most matches to return; {@code 0} returns all
     * @param options {@code rank} (where to start counting matches; negative scans from the tail) and {@code maxLen}
     * @return the zero-based indexes of the matches, empty when there is none
     * @throws RuvioServerException {@code ERR} for an invalid rank; {@code WRONGTYPE} when the key holds a non-list
     */
    public CompletableFuture<List<Long>> lposAll(K key, V value, long count, LPosArgs options) {
        Args a = args().w(Cmd.LPOS).k(key).v(value);
        options.write(a);
        return send(a.w(Cmd.COUNT).l(count), Reply.LONG_LIST);
    }

    /**
     * Sends {@code LMOVE source destination LEFT|RIGHT LEFT|RIGHT}: atomically pops an element from one list and pushes it to another (or the same one, to rotate). In a cluster both keys must hash to the same slot, otherwise the client throws {@code CROSSSLOT} before sending.
     *
     * @param source the list to pop from
     * @param destination the list to push to
     * @param from which end of {@code source} to pop
     * @param to which end of {@code destination} to push to
     * @return the moved element, or {@code null} when {@code source} is empty or missing
     * @throws RuvioServerException {@code WRONGTYPE} for a non-list; {@code CROSSSLOT} in a cluster
     */
    public CompletableFuture<V> lmove(K source, K destination, ListDirection from, ListDirection to) {
        return send(args().w(Cmd.LMOVE).k(source).k(destination).w(from.fragment()).w(to.fragment()), r(Reply.VALUE));
    }

    /**
     * Sends {@code RPOPLPUSH source destination}: pops the tail of one list and pushes it to the head of another. Same as {@code lmove(source, destination, RIGHT, LEFT)}.
     *
     * @param source the list to pop from
     * @param destination the list to push to
     * @return the moved element, or {@code null} when {@code source} is empty or missing
     * @throws RuvioServerException {@code WRONGTYPE} for a non-list; {@code CROSSSLOT} in a cluster
     */
    public CompletableFuture<V> rpoplpush(K source, K destination) {
        return kk(Cmd.RPOPLPUSH, source, destination, r(Reply.VALUE));
    }

    /**
     * Sends {@code BLMOVE source destination LEFT|RIGHT LEFT|RIGHT timeout}: like {@link #lmove} but waits for an element. The server polls every 5 ms, and the shared connection is held while waiting, so use {@link io.github.salihcantekin.ruvio.RuvioClient#dedicated()}. Not allowed inside a transaction.
     *
     * @param source the list to pop from
     * @param destination the list to push to
     * @param from which end of {@code source} to pop
     * @param to which end of {@code destination} to push to
     * @param timeoutSeconds seconds to wait; {@code 0} waits until an element arrives
     * @return the moved element, or {@code null} on timeout
     * @throws RuvioServerException {@code WRONGTYPE} for a non-list; {@code CROSSSLOT} in a cluster
     */
    public CompletableFuture<V> blmove(K source, K destination, ListDirection from, ListDirection to, double timeoutSeconds) {
        return send(args().w(Cmd.BLMOVE).k(source).k(destination).w(from.fragment()).w(to.fragment()).d(timeoutSeconds), r(Reply.VALUE));
    }

    /**
     * Sends {@code LMPOP numkeys key [key ...] LEFT|RIGHT [COUNT n]}: pops elements from the first non-empty list.
     *
     * @param direction which end to pop from
     * @param count the most elements to pop; zero or less sends no {@code COUNT} (pops one)
     * @param keys one or more list keys, checked in order
     * @return the key popped from and the elements, or {@code null} when every list is empty
     * @throws RuvioServerException {@code WRONGTYPE} when a key holds a non-list; {@code CROSSSLOT} in a cluster when keys span slots
     */
    @SafeVarargs
    public final CompletableFuture<KeyValue<K, List<V>>> lmpop(ListDirection direction, long count, K... keys) {
        Args a = args().w(Cmd.LMPOP).l(keys.length).keys(keys).w(direction.fragment());
        if (count > 0) {
            a.w(Cmd.COUNT).l(count);
        }
        return send(a, r(KEY_VALUES));
    }

    /**
     * Sends {@code BLMPOP timeout numkeys key [key ...] LEFT|RIGHT [COUNT n]}: like {@link #lmpop} but waits (polling every 5 ms on the server) until a list has an element. Holds the shared connection; use {@link io.github.salihcantekin.ruvio.RuvioClient#dedicated()}. Not allowed inside a transaction.
     *
     * @param timeoutSeconds seconds to wait; {@code 0} waits until an element arrives
     * @param direction which end to pop from
     * @param count the most elements to pop; zero or less pops one
     * @param keys one or more list keys
     * @return the key popped from and the elements, or {@code null} on timeout
     * @throws RuvioServerException {@code WRONGTYPE} when a key holds a non-list; {@code CROSSSLOT} in a cluster when keys span slots
     */
    @SafeVarargs
    public final CompletableFuture<KeyValue<K, List<V>>> blmpop(double timeoutSeconds, ListDirection direction, long count, K... keys) {
        Args a = args().w(Cmd.BLMPOP).d(timeoutSeconds).l(keys.length).keys(keys).w(direction.fragment());
        if (count > 0) {
            a.w(Cmd.COUNT).l(count);
        }
        return send(a, r(KEY_VALUES));
    }
}
