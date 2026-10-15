// SPDX-License-Identifier: MIT
package io.github.salihcantekin.ruvio.internal;

import io.github.salihcantekin.ruvio.RuvioCodec;
import io.github.salihcantekin.ruvio.RuvioConnectionException;
import io.github.salihcantekin.ruvio.RuvioException;
import io.github.salihcantekin.ruvio.RuvioOptions;
import io.github.salihcantekin.ruvio.RuvioServerException;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Owns the shared connection of a client and reopens it before the next command after a drop.
 * Commands in flight when the socket drops fail with {@link RuvioConnectionException}; nothing is
 * replayed automatically.
 */
public final class Link implements AutoCloseable {
    private static final AtomicInteger IDS = new AtomicInteger();

    public final RuvioOptions options;
    private final String name;
    private volatile Connection connection;
    private volatile boolean closed;
    public volatile int database;

    public Link(RuvioOptions options) {
        this(options, false);
    }

    /** With {@code lazy} the socket is opened by the first command (cluster nodes). */
    public Link(RuvioOptions options, boolean lazy) {
        this.options = options;
        this.database = options.database;
        this.name = "ruvio-" + IDS.incrementAndGet();
        this.connection = lazy ? null : open();
    }

    /** True while the shared socket is open (no reconnect attempt). */
    public boolean isUp() {
        Connection c = connection;
        return c != null && c.isOpen();
    }

    public Connection get() {
        Connection c = connection;
        if (c != null && c.isOpen()) {
            return c;
        }
        return reopen();
    }

    private synchronized Connection reopen() {
        if (closed) {
            throw new RuvioConnectionException("client is closed");
        }
        Connection c = connection;
        if (c != null && c.isOpen()) {
            return c;
        }
        c = open();
        connection = c;
        return c;
    }

    private Connection open() {
        Connection c;
        try {
            c = new Connection(options.host, options.port, options.connectTimeoutMillis, options.maxInFlight, name);
        } catch (IOException e) {
            throw new RuvioConnectionException("cannot connect to " + options.host + ":" + options.port + ": " + e.getMessage(), e);
        }
        c.setPushHandler(options.pushListener);
        try {
            handshake(c, options, database);
        } catch (RuntimeException e) {
            c.close();
            throw e;
        }
        return c;
    }

    /** AUTH, SELECT and CLIENT SETNAME, pipelined in one write. */
    public static void handshake(Connection c, RuvioOptions o, int database) {
        if (o.password == null && database == 0 && o.clientName == null) {
            return;
        }
        Raw raw = new Raw(c);
        Request<?> last = null;
        if (o.password != null) {
            last = o.username != null ? raw.send(Reply.OK, "AUTH", o.username, o.password) : raw.send(Reply.OK, "AUTH", o.password);
            await(last, o.connectTimeoutMillis);
        }
        if (database != 0) {
            last = raw.send(Reply.OK, "SELECT", (long) database);
        }
        if (o.clientName != null) {
            last = raw.send(Reply.OK, "CLIENT", "SETNAME", o.clientName);
        }
        if (last != null) {
            await(last, o.connectTimeoutMillis);
        }
    }

    public static <T> T await(java.util.concurrent.CompletableFuture<T> f, long timeoutMillis) {
        try {
            return timeoutMillis > 0 ? f.get(timeoutMillis, TimeUnit.MILLISECONDS) : f.get();
        } catch (ExecutionException e) {
            throw rethrow(e.getCause());
        } catch (TimeoutException e) {
            throw new RuvioException("no reply within " + timeoutMillis + " ms");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuvioException("interrupted", e);
        }
    }

    /**
     * Re-creates a failure on the calling thread so its stack trace shows the caller, keeping the
     * reader-thread original as the cause.
     */
    public static RuntimeException rethrow(Throwable cause) {
        if (cause instanceof RuvioServerException) {
            RuvioServerException copy = new RuvioServerException(cause.getMessage());
            copy.initCause(cause);
            return copy;
        }
        if (cause instanceof RuvioConnectionException) {
            return new RuvioConnectionException(cause.getMessage(), cause);
        }
        if (cause instanceof RuntimeException) {
            return (RuntimeException) cause;
        }
        if (cause instanceof Error) {
            throw (Error) cause;
        }
        return new RuvioException(String.valueOf(cause), cause);
    }

    /** Opens a separate connection with the same settings (transactions, pub/sub, dedicated clients). */
    public Connection openExtra(String suffix) {
        Connection c;
        try {
            c = new Connection(options.host, options.port, options.connectTimeoutMillis, options.maxInFlight, name + "-" + suffix);
        } catch (IOException e) {
            throw new RuvioConnectionException("cannot connect to " + options.host + ":" + options.port + ": " + e.getMessage(), e);
        }
        try {
            handshake(c, options, database);
        } catch (RuntimeException e) {
            c.close();
            throw e;
        }
        return c;
    }

    @Override
    public synchronized void close() {
        closed = true;
        Connection c = connection;
        if (c != null) {
            c.close();
        }
    }

    /** Untyped command sender over one connection, used for handshakes and internal commands. */
    public static final class Raw {
        private final Connection c;

        Raw(Connection c) {
            this.c = c;
        }

        <T> Request<T> send(Reply<T> reply, Object... arguments) {
            RespOut o = c.begin();
            try {
                o.header(arguments.length);
                for (Object a : arguments) {
                    o.arg(a);
                }
            } catch (RuntimeException e) {
                c.abort();
                throw e;
            }
            Request<T> r = new Request<>(reply, RuvioCodec.UTF8, RuvioCodec.UTF8);
            c.end(r);
            return r;
        }
    }
}
