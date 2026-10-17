// SPDX-License-Identifier: MIT
package io.github.salihcantekin.ruvio.args;

import io.github.salihcantekin.ruvio.internal.RespOut;

/**
 * Value type filter of SCAN TYPE. Valid values: string, json, list, set, hash, zset, bloomfilter, stream, lease, limit, semaphore, once.
 *
 * <p>Example: {@code client.scan("0", ScanArgs.count(100).type(KeyType.HASH))}
 */
public enum KeyType {
    STRING("string"),
    JSON("json"),
    LIST("list"),
    SET("set"),
    HASH("hash"),
    ZSET("zset"),
    BLOOMFILTER("bloomfilter"),
    STREAM("stream"),
    LEASE("lease"),
    LIMIT("limit"),
    SEMAPHORE("semaphore"),
    ONCE("once");

    private final String word;
    private final byte[] fragment;

    KeyType(String word) {
        this.word = word;
        this.fragment = RespOut.fragment(word);
    }

    /** The wire keyword, for example {@code "STRING"}. */
    public String word() {
        return word;
    }

    /**
     * The keyword pre-encoded as a complete RESP bulk string ({@code $len\r\nWORD\r\n}). The array is
     * shared and must not be modified.
     */
    public byte[] fragment() {
        return fragment;
    }
}
