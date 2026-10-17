// SPDX-License-Identifier: MIT
package io.github.salihcantekin.ruvio.args;

import io.github.salihcantekin.ruvio.internal.RespOut;

/**
 * Value type filter of SCAN TYPE. Valid values: string, json, list, set, hash, zset, bloomfilter, stream, lease, limit, semaphore, once.
 *
 * <p>Example: {@code client.scan("0", ScanArgs.count(100).type(KeyType.HASH))}
 */
public enum KeyType {
    /** Plain string values. */
    STRING("string"),
    /** JSON documents. */
    JSON("json"),
    /** Lists. */
    LIST("list"),
    /** Sets. */
    SET("set"),
    /** Hashes. */
    HASH("hash"),
    /** Sorted sets. */
    ZSET("zset"),
    /** Bloom filters. */
    BLOOMFILTER("bloomfilter"),
    /** Streams. */
    STREAM("stream"),
    /** Leases created by {@code LEASE}. */
    LEASE("lease"),
    /** Rate-limit counters created by {@code LIMIT}. */
    LIMIT("limit"),
    /** Semaphores created by {@code SEMAPHORE}. */
    SEMAPHORE("semaphore"),
    /** Keys created by {@code ONCE}. */
    ONCE("once");

    private final String word;
    private final byte[] fragment;

    KeyType(String word) {
        this.word = word;
        this.fragment = RespOut.fragment(word);
    }

    /**
     * Returns the keyword as sent to the server.
     *
     * @return the keyword, for example {@code "string"}
     */
    public String word() {
        return word;
    }

    /**
     * Returns the keyword pre-encoded as a complete RESP bulk string ({@code $len\r\nWORD\r\n}).
     *
     * @return the shared encoded bytes; do not modify them
     */
    public byte[] fragment() {
        return fragment;
    }
}
