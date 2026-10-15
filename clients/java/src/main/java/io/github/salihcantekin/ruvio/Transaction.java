// SPDX-License-Identifier: MIT
package io.github.salihcantekin.ruvio;

import io.github.salihcantekin.ruvio.commands.RuvioCommands;
import io.github.salihcantekin.ruvio.internal.ClusterNode;
import io.github.salihcantekin.ruvio.internal.ClusterRouter;
import io.github.salihcantekin.ruvio.internal.Cmd;
import io.github.salihcantekin.ruvio.internal.Connection;
import io.github.salihcantekin.ruvio.internal.Frame;
import io.github.salihcantekin.ruvio.internal.Link;
import io.github.salihcantekin.ruvio.internal.Reply;
import io.github.salihcantekin.ruvio.internal.Request;
import io.github.salihcantekin.ruvio.internal.RespOut;
import io.github.salihcantekin.ruvio.internal.RespReader;
import io.github.salihcantekin.ruvio.internal.TxPool;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * MULTI/EXEC on a dedicated connection, so other callers of the shared connection never land
 * inside the transaction. Typical use:
 *
 * <pre>{@code
 * try (Transaction<String, String> tx = client.multi()) {
 *     tx.watch("balance");                 // optional, sent immediately
 *     CompletableFuture<Long> n = tx.incr("counter");
 *     tx.set("k", "v");
 *     if (tx.exec()) { long value = n.join(); }   // false: a watched key changed
 * }
 * }</pre>
 *
 * Typed calls return futures that complete when EXEC returns. MULTI is sent before the first
 * queued command. Not thread safe. Ruvio requires same-slot keys inside MULTI in multi-shard mode.
 */
public final class Transaction<K, V> extends RuvioCommands<K, V> implements AutoCloseable {
    private static final byte[] MULTI_FRAME = "*1\r\n$5\r\nMULTI\r\n".getBytes(java.nio.charset.StandardCharsets.US_ASCII);
    private static final byte[] EXEC_FRAME = "*1\r\n$4\r\nEXEC\r\n".getBytes(java.nio.charset.StandardCharsets.US_ASCII);
    private static final byte[] DISCARD_FRAME = "*1\r\n$7\r\nDISCARD\r\n".getBytes(java.nio.charset.StandardCharsets.US_ASCII);
    private static final byte[] UNWATCH_FRAME = "*1\r\n$7\r\nUNWATCH\r\n".getBytes(java.nio.charset.StandardCharsets.US_ASCII);

    private TxPool pool;
    private final long timeoutMillis;
    private Connection connection;
    // Cluster mode: the transaction binds to the node (and slot) of its first key.
    private final ClusterRouter router;
    private final Frame frame;
    private ClusterNode node;
    private int slot = -1;
    private boolean multiSent;
    private boolean watching;
    private boolean finished;
    private final List<Request<?>> queued = new ArrayList<>();

    Transaction(TxPool pool, long timeoutMillis, RuvioCodec<K> keys, RuvioCodec<V> values) {
        super(keys, values);
        this.pool = pool;
        this.timeoutMillis = timeoutMillis;
        this.connection = pool.borrow();
        this.router = null;
        this.frame = null;
    }

    /** Cluster mode: no connection until the first key (or the first keyless command) picks the node. */
    Transaction(ClusterRouter router, long timeoutMillis, RuvioCodec<K> keys, RuvioCodec<V> values) {
        super(keys, values);
        this.timeoutMillis = timeoutMillis;
        this.router = router;
        this.frame = new Frame();
    }

    private Connection conn() {
        if (finished) {
            throw new IllegalStateException("transaction already finished");
        }
        if (connection == null && router != null) {
            bind(router.anyNode());
        }
        return connection;
    }

    private void bind(ClusterNode n) {
        pool = n.txPool;
        connection = pool.borrow();
        node = n;
    }

    /**
     * Cluster mode: checks that the keys of the encoded command share one slot, and that it is the
     * slot (node) this transaction is already bound to; binds on the first key.
     */
    private void route() {
        int s = ClusterRouter.commonSlot(frame);
        if (s < 0) {
            return;
        }
        if (finished) {
            throw new IllegalStateException("transaction already finished");
        }
        if (slot < 0 && node == null) {
            slot = s;
            bind(router.owner(s));
        } else if (slot < 0) {
            if (router.owner(s) != node) {
                throw ClusterRouter.crossSlotError();
            }
            slot = s;
        } else if (s != slot) {
            throw ClusterRouter.crossSlotError();
        }
    }

    @Override
    protected RespOut begin() {
        if (router != null) {
            frame.reset();
            return frame.out;
        }
        Connection c = conn();
        RespOut o = c.begin();
        if (!multiSent) {
            o.raw(MULTI_FRAME);
            c.end(new Request<>(Reply.OK, kc, vc)); // releases the lock; FIFO order is kept
            multiSent = true;
            o = c.begin();
        }
        return o;
    }

    @Override
    protected void abort(RespOut out) {
        if (router == null) {
            out.owner.abort();
        }
    }

    @Override
    protected <T> CompletableFuture<T> end(RespOut out, Request<T> request) {
        if (router != null) {
            route();
            Connection c = conn();
            if (!multiSent) {
                RespOut m = c.begin();
                m.raw(MULTI_FRAME);
                c.end(new Request<>(Reply.OK, kc, vc));
                multiSent = true;
            }
            RespOut o = c.begin();
            try {
                o.raw(out.buf, 0, out.pos);
            } catch (RuntimeException e) {
                c.abort();
                throw e;
            }
            queued.add(request);
            c.end(new QueuedAck(request));
            return request;
        }
        queued.add(request);
        out.owner.end(new QueuedAck(request));
        return request;
    }

    private void sendFrame(byte[] frame, Request<?> r) {
        Connection c = conn();
        RespOut o = c.begin();
        o.raw(frame);
        c.end(r);
    }

    /** WATCH key...: sent immediately; must be called before the first queued command. */
    @SafeVarargs
    public final void watch(K... keys) {
        if (multiSent) {
            throw new IllegalStateException("WATCH is not allowed after MULTI");
        }
        if (router != null) {
            RespOut scratch = begin();
            scratch.header(1 + keys.length);
            scratch.raw(Cmd.WATCH);
            for (K k : keys) {
                kc.write(scratch, k);
            }
            route();
        }
        Connection c = conn();
        RespOut o = c.begin();
        try {
            o.header(1 + keys.length);
            o.raw(Cmd.WATCH);
            for (K k : keys) {
                kc.write(o, k);
            }
        } catch (RuntimeException e) {
            c.abort();
            throw e;
        }
        Request<Void> r = new Request<>(Reply.OK, kc, vc);
        c.end(r);
        watching = true;
        Link.await(r, timeoutMillis);
    }

    /** UNWATCH: forgets every watched key (before MULTI). */
    public void unwatch() {
        if (multiSent) {
            throw new IllegalStateException("UNWATCH is not allowed after MULTI");
        }
        Request<Void> r = new Request<>(Reply.OK, kc, vc);
        sendFrame(UNWATCH_FRAME, r);
        watching = false;
        Link.await(r, timeoutMillis);
    }

    /** EXEC asynchronously: true when executed, false when a watched key changed (results then fail). */
    public CompletableFuture<Boolean> execAsync() {
        if (!multiSent) {
            sendFrame(MULTI_FRAME, new Request<>(Reply.OK, kc, vc));
            multiSent = true;
        }
        // Commands the server refused at queue time never run; EXEC only returns the accepted ones.
        List<Request<?>> accepted = new ArrayList<>(queued.size());
        for (Request<?> q : queued) {
            if (!q.isDone()) {
                accepted.add(q);
            }
        }
        ExecRequest exec = new ExecRequest(accepted.toArray(new Request<?>[0]));
        sendFrame(EXEC_FRAME, exec);
        finished = true;
        exec.whenComplete((ok, err) -> release(err == null));
        return exec;
    }

    /** EXEC: true when executed, false when a watched key changed. Throws on EXECABORT. */
    public boolean exec() {
        return Link.await(execAsync(), timeoutMillis);
    }

    /** DISCARD: drops the queued commands (their futures fail) and clears watches. */
    public void discard() {
        if (finished) {
            return;
        }
        Request<Void> last = null;
        if (multiSent) {
            last = new Request<>(Reply.OK, kc, vc);
            sendFrame(DISCARD_FRAME, last);
        }
        if (watching) {
            last = new Request<>(Reply.OK, kc, vc);
            sendFrame(UNWATCH_FRAME, last);
        }
        finished = true;
        RuvioException discarded = new RuvioException("transaction discarded");
        for (Request<?> q : queued) {
            q.completeExceptionally(discarded);
        }
        boolean healthy = true;
        if (last != null) {
            try {
                Link.await(last, timeoutMillis);
            } catch (RuntimeException e) {
                healthy = false;
            }
        }
        release(healthy);
    }

    private void release(boolean healthy) {
        Connection c = connection;
        connection = null;
        if (c != null) {
            pool.giveBack(c, healthy);
        }
    }

    /** Discards the transaction if it was neither executed nor discarded. */
    @Override
    public void close() {
        if (!finished) {
            discard();
        }
    }

    /** Consumes +QUEUED; a queue-time error fails the wrapped command at once. */
    static final class QueuedAck extends Request<Void> {
        private final Request<?> inner;

        QueuedAck(Request<?> inner) {
            super(Reply.OK, inner.keys, inner.values);
            this.inner = inner;
        }

        @Override
        public void fail(Throwable error) {
            inner.completeExceptionally(error);
            super.fail(error);
        }
    }

    /** Decodes the EXEC array with each queued command's own decoder, then completes them all. */
    static final class ExecRequest extends Request<Boolean> {
        final Request<?>[] inner;
        final Object[] results;

        ExecRequest(Request<?>[] inner) {
            super(null, RuvioCodec.UTF8, RuvioCodec.UTF8);
            this.inner = inner;
            this.results = new Object[inner.length];
        }

        @Override
        public Boolean decode(RespReader r) {
            int n = r.arrayLength();
            if (n < 0) {
                return Boolean.FALSE;
            }
            for (int i = 0; i < n; i++) {
                int start = r.pos;
                if (i >= inner.length) {
                    r.skip();
                    continue;
                }
                if (r.peek() == '-') {
                    results[i] = new Failure(r.readError());
                    continue;
                }
                try {
                    results[i] = inner[i].decode(r);
                } catch (RespReader.Incomplete e) {
                    throw e;
                } catch (RuntimeException e) {
                    r.pos = start;
                    r.skip();
                    results[i] = new Failure(e);
                }
            }
            return Boolean.TRUE;
        }

        @Override
        public void deliver(Object value) {
            if (Boolean.TRUE.equals(value)) {
                for (int i = 0; i < inner.length; i++) {
                    Object v = results[i];
                    if (v instanceof Failure) {
                        inner[i].completeExceptionally(((Failure) v).error);
                    } else {
                        inner[i].deliver(v);
                    }
                }
            } else {
                RuvioException aborted = new RuvioException("transaction aborted: a watched key changed");
                for (Request<?> q : inner) {
                    q.completeExceptionally(aborted);
                }
            }
            complete((Boolean) value);
        }

        @Override
        public void fail(Throwable error) {
            for (Request<?> q : inner) {
                q.completeExceptionally(error);
            }
            super.fail(error);
        }
    }

    static final class Failure {
        final Throwable error;

        Failure(Throwable error) {
            this.error = error;
        }
    }
}
