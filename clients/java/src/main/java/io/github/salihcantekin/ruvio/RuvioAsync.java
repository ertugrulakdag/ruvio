// SPDX-License-Identifier: MIT
package io.github.salihcantekin.ruvio;

import io.github.salihcantekin.ruvio.commands.RuvioCommands;
import io.github.salihcantekin.ruvio.internal.ClusterRouter;
import io.github.salihcantekin.ruvio.internal.Link;
import io.github.salihcantekin.ruvio.internal.Request;
import io.github.salihcantekin.ruvio.internal.RespOut;
import io.github.salihcantekin.ruvio.internal.RoutedRequest;

import java.util.concurrent.CompletableFuture;

/**
 * Asynchronous commands on the shared multiplexed connection. Thread safe; every method returns
 * immediately with a future completed by the reader thread in reply order.
 *
 * <p>Futures complete on the connection's reader thread: callbacks attached with
 * {@code thenApply}/{@code thenAccept} run there and must not block (use the {@code *Async}
 * variants with an executor for slow work).
 */
public final class RuvioAsync<K, V> extends RuvioCommands<K, V> {
    final Link link;
    /** Non-null in cluster mode: every command is routed by its key; {@code link} is then the seed's. */
    final ClusterRouter router;

    RuvioAsync(Link link, RuvioCodec<K> keys, RuvioCodec<V> values) {
        this(link, null, keys, values);
    }

    RuvioAsync(Link link, ClusterRouter router, RuvioCodec<K> keys, RuvioCodec<V> values) {
        super(keys, values);
        this.link = link;
        this.router = router;
        this.routed = router != null;
    }

    @Override
    protected RespOut begin() {
        ClusterRouter r = router;
        return r == null ? link.get().begin() : r.begin();
    }

    @Override
    protected void abort(RespOut out) {
        if (router == null) {
            out.owner.abort();
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    protected <T> CompletableFuture<T> end(RespOut out, Request<T> request) {
        ClusterRouter r = router;
        if (r == null) {
            out.owner.end(request);
        } else {
            r.dispatch(out.frame, (RoutedRequest<T>) request, ClusterRouter.DIRECT);
        }
        return request;
    }

    /** Same connection, other codecs (for example String keys with byte[] values). */
    public <K2, V2> RuvioAsync<K2, V2> withCodecs(RuvioCodec<K2> keys, RuvioCodec<V2> values) {
        return new RuvioAsync<>(link, router, keys, values);
    }

    /** SELECT on the shared connection; also remembered for reconnects. */
    @Override
    public CompletableFuture<Void> select(int database) {
        CompletableFuture<Void> f = super.select(database);
        link.database = database;
        return f;
    }

    /** A new explicit pipeline: commands are buffered locally and sent in one write by {@code flush()}. */
    public Pipeline<K, V> pipeline() {
        return new Pipeline<>(link, router, kc, vc);
    }
}
