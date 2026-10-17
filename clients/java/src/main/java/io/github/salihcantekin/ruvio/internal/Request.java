// SPDX-License-Identifier: MIT
package io.github.salihcantekin.ruvio.internal;

import io.github.salihcantekin.ruvio.RuvioCodec;

import java.util.concurrent.CompletableFuture;

/**
 * One pending reply. The future itself is the only per-command allocation of a typed call; the
 * decoder is a shared singleton that reads the reply straight from the connection's buffer.
 *
 * @param <T> the result type
 */
public class Request<T> extends CompletableFuture<T> {
    /** The decoder for this command's reply. */
    public final Reply<T> reply;
    /** The key codec used to decode keys in the reply. */
    public final RuvioCodec<?> keys;
    /** The value codec used to decode values in the reply. */
    public final RuvioCodec<?> values;

    /**
     * Creates a request.
     *
     * @param reply the reply decoder
     * @param keys the key codec
     * @param values the value codec
     */
    public Request(Reply<T> reply, RuvioCodec<?> keys, RuvioCodec<?> values) {
        this.reply = reply;
        this.keys = keys;
        this.values = values;
    }

    /**
     * Decodes this request's reply at the reader's position. May throw {@link RespReader#INCOMPLETE}.
     *
     * @param r the reader
     * @return the decoded value
     */
    public T decode(RespReader r) {
        return reply.decode(r, this);
    }

    /**
     * Completes with a decoded value; overridden by requests that route their result elsewhere.
     *
     * @param value the decoded value
     */
    @SuppressWarnings("unchecked")
    public void deliver(Object value) {
        complete((T) value);
    }

    /**
     * Completes exceptionally.
     *
     * @param error the failure
     */
    public void fail(Throwable error) {
        completeExceptionally(error);
    }
}
