// SPDX-License-Identifier: MIT
package io.github.salihcantekin.ruvio.internal;

import io.github.salihcantekin.ruvio.RuvioConnectionException;

import java.util.ArrayDeque;

/** A few idle dedicated connections for transactions and dedicated clients, reused after use. */
public final class TxPool implements AutoCloseable {
    private static final int MAX_IDLE = 4;

    private final Link link;
    private final ArrayDeque<Connection> idle = new ArrayDeque<>();
    private boolean closed;

    public TxPool(Link link) {
        this.link = link;
    }

    public Connection borrow() {
        synchronized (this) {
            if (closed) {
                throw new RuvioConnectionException("client is closed");
            }
            while (!idle.isEmpty()) {
                Connection c = idle.pollFirst();
                if (c.isOpen()) {
                    return c;
                }
            }
        }
        return link.openExtra("tx");
    }

    public void giveBack(Connection c, boolean healthy) {
        if (healthy && c.isOpen() && c.inFlight() == 0) {
            synchronized (this) {
                if (!closed && idle.size() < MAX_IDLE) {
                    idle.addLast(c);
                    return;
                }
            }
        }
        c.close();
    }

    @Override
    public void close() {
        synchronized (this) {
            closed = true;
            for (Connection c : idle) {
                c.close();
            }
            idle.clear();
        }
    }
}
