// SPDX-License-Identifier: MIT
package io.github.salihcantekin.ruvio.args;

import io.github.salihcantekin.ruvio.internal.RespOut;

/**
 * Connection type of {@code CLIENT LIST TYPE}. Ruvio checks the value but lists every connection.
 *
 * <p>Example: {@code client.clientList(ClientType.PUBSUB)}
 */
public enum ClientType {
    /** Ordinary client connections. */
    NORMAL("normal"),
    /** The connection to a primary. */
    MASTER("master"),
    /** Replica connections. */
    REPLICA("replica"),
    /** Pub/Sub subscriber connections. */
    PUBSUB("pubsub");

    private final String word;
    private final byte[] fragment;

    ClientType(String word) {
        this.word = word;
        this.fragment = RespOut.fragment(word);
    }

    /**
     * Returns the keyword as sent to the server.
     *
     * @return the keyword, for example {@code "normal"}
     */
    public String word() {
        return word;
    }

    /**
     * Returns the keyword pre-encoded as a complete RESP bulk string.
     *
     * @return the shared encoded bytes; do not modify them
     */
    public byte[] fragment() {
        return fragment;
    }
}
