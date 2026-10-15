// SPDX-License-Identifier: MIT
package io.github.salihcantekin.ruvio.internal;

import io.github.salihcantekin.ruvio.RuvioCodec;

import java.util.concurrent.CompletableFuture;

/**
 * One pending reply. The future itself is the only per-command allocation of a typed call; the
 * decoder is a shared singleton that reads the reply straight from the connection's buffer.
 */
public class Request<T> extends CompletableFuture<T> {
    public final Reply<T> reply;
    public final RuvioCodec<?> keys;
    public final RuvioCodec<?> values;

    public Request(Reply<T> reply, RuvioCodec<?> keys, RuvioCodec<?> values) {
        this.reply = reply;
        this.keys = keys;
        this.values = values;
    }

    /** Decodes this request's reply at the reader's position. May throw {@link RespReader#INCOMPLETE}. */
    public T decode(RespReader r) {
        return reply.decode(r, this);
    }

    /** Completes with a decoded value; overridden by requests that route their result elsewhere. */
    @SuppressWarnings("unchecked")
    public void deliver(Object value) {
        complete((T) value);
    }

    public void fail(Throwable error) {
        completeExceptionally(error);
    }
}
