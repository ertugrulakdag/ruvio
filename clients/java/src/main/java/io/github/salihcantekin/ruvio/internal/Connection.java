// SPDX-License-Identifier: MIT
package io.github.salihcantekin.ruvio.internal;

import io.github.salihcantekin.ruvio.RuvioConnectionException;
import io.github.salihcantekin.ruvio.RuvioException;
import io.github.salihcantekin.ruvio.RuvioServerException;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

/**
 * One multiplexed socket shared by every caller.
 *
 * <p>Write side: callers encode their frame straight into the shared output buffer under a short
 * lock and push their {@link Request} onto a FIFO ring in the same critical section, so byte order
 * and reply order always match. When nothing else is in flight the caller writes the buffer itself
 * (no thread hand-off for serial use); otherwise the writer thread drains everything queued in one
 * {@code write} (no timers, no batch-fill wait), swapping between two buffers.
 *
 * <p>Read side: the reader thread reads into one reusable buffer, decodes each reply in place with
 * the request's decoder and completes the futures in FIFO order.
 */
public class Connection implements AutoCloseable {
    private static final int READ_BUFFER = 64 * 1024;
    private static final int WRITE_BUFFER = 16 * 1024;
    private static final int SHRINK_ABOVE = 1024 * 1024;

    /** The server host. */
    public final String host;
    /** The server port. */
    public final int port;
    private final SocketChannel channel;
    private final ReentrantLock lock = new ReentrantLock();
    private final Thread writer;
    private final Thread reader;
    private volatile boolean writerSignal;

    // guarded by lock
    private RespOut out = new RespOut(WRITE_BUFFER);
    private RespOut spare = new RespOut(WRITE_BUFFER);
    private boolean writing;
    private int mark;

    // FIFO of pending replies: producers append under lock, the reader consumes.
    private final Request<?>[] ring;
    private final int mask;
    private volatile long tail;
    private volatile long head;

    private volatile Throwable closed;
    private volatile Consumer<Object> pushHandler;
    private ByteBuffer writeView;
    private byte[] writeViewArray;

    /**
     * Opens the socket and starts the reader and writer threads.
     *
     * @param host the server host
     * @param port the server port
     * @param connectTimeoutMs the connect timeout in milliseconds
     * @param maxInFlight the most replies outstanding at once (rounded up to a power of two)
     * @param name the name used for the threads
     * @throws IOException when the socket cannot be opened
     */
    public Connection(String host, int port, int connectTimeoutMs, int maxInFlight, String name) throws IOException {
        this.host = host;
        this.port = port;
        int cap = Integer.highestOneBit(Math.max(16, maxInFlight - 1)) << 1;
        this.ring = new Request<?>[cap];
        this.mask = cap - 1;
        SocketChannel ch = SocketChannel.open();
        try {
            ch.setOption(StandardSocketOptions.TCP_NODELAY, true);
            ch.setOption(StandardSocketOptions.SO_KEEPALIVE, true);
            ch.socket().connect(new InetSocketAddress(host, port), connectTimeoutMs);
        } catch (IOException e) {
            ch.close();
            throw e;
        }
        this.channel = ch;
        out.owner = this;
        spare.owner = this;
        writer = new Thread(this::writeLoop, name + "-writer");
        writer.setDaemon(true);
        reader = new Thread(this::readLoop, name + "-reader");
        reader.setDaemon(true);
        writer.start();
        reader.start();
    }

    /**
     * Sets the receiver of RESP3 push frames; it runs on the reader thread.
     *
     * @param handler the receiver, or null
     */
    public void setPushHandler(Consumer<Object> handler) {
        this.pushHandler = handler;
    }

    /**
     * Tells whether the connection is still usable.
     *
     * @return {@code true} until it fails or is closed
     */
    public boolean isOpen() {
        return closed == null;
    }

    /**
     * Returns how many requests wait for a reply.
     *
     * @return the count
     */
    public int inFlight() {
        return (int) (tail - head);
    }

    // ---- write side -----------------------------------------------------------------------

    /**
     * Locks the output buffer for one frame, waiting while the ring of outstanding requests is full. Must be followed by {@link #end} or {@link #abort}.
     *
     * @return the buffer to encode into
     * @throws RuvioConnectionException when the connection is closed
     */
    public RespOut begin() {
        lock.lock();
        for (;;) {
            if (closed != null) {
                lock.unlock();
                throw closedException();
            }
            if (tail - head < ring.length) {
                mark = out.pos;
                return out;
            }
            // Ring full: let the writer and reader make progress without holding the lock.
            lock.unlock();
            signalWriter();
            LockSupport.parkNanos(20_000);
            lock.lock();
        }
    }

    /** Drops the partly encoded frame and releases the lock. */
    public void abort() {
        out.pos = mark;
        lock.unlock();
    }

    /**
     * Enqueues {@code request} for the frame just encoded and releases the lock.
     *
     * @param request the request that will receive the reply
     */
    public void end(Request<?> request) {
        long t = tail;
        boolean serial = t == head;
        ring[(int) (t & mask)] = request;
        tail = t + 1;
        submitLocked(serial);
    }

    /**
     * Enqueues several requests whose frames were appended in order (pipelines).
     *
     * @param requests the requests, in frame order
     * @param count how many entries of {@code requests} to use
     * @throws RuvioException when {@code count} exceeds the ring size
     */
    public void endMany(Request<?>[] requests, int count) {
        if (tail - head + count > ring.length) {
            // Too many for the ring at once: fail fast rather than deadlock.
            abort();
            throw new RuvioException("pipeline of " + count + " exceeds maxInFlight " + ring.length);
        }
        long t = tail;
        boolean serial = t == head;
        for (int i = 0; i < count; i++) {
            ring[(int) ((t + i) & mask)] = requests[i];
        }
        tail = t + count;
        submitLocked(serial);
    }

    /**
     * Returns the size of the ring of outstanding requests.
     *
     * @return the most replies that can be outstanding
     */
    public int capacity() {
        return ring.length;
    }

    private void submitLocked(boolean serial) {
        if (writing) {
            lock.unlock(); // the active writer picks this frame up
            return;
        }
        writing = true;
        if (!serial) {
            lock.unlock();
            signalWriter();
            return;
        }
        RespOut b = out;
        out = spare;
        spare = null;
        lock.unlock();
        // Nothing else in flight: write on the caller's thread, no hand-off.
        if (writeBuffer(b)) {
            afterInlineWrite(b);
        }
    }

    private void afterInlineWrite(RespOut b) {
        recycle(b);
        lock.lock();
        spare = b;
        if (out.pos > 0) {
            lock.unlock();
            signalWriter(); // writing stays true; ownership moves to the writer thread
        } else {
            writing = false;
            lock.unlock();
        }
    }

    private static void recycle(RespOut b) {
        b.reset();
        if (b.buf.length > SHRINK_ABOVE) {
            b.buf = new byte[WRITE_BUFFER];
        }
    }

    private void signalWriter() {
        writerSignal = true;
        LockSupport.unpark(writer);
    }

    private void writeLoop() {
        while (closed == null) {
            while (!writerSignal) {
                LockSupport.park(this);
                if (closed != null) {
                    return;
                }
            }
            writerSignal = false;
            for (;;) {
                lock.lock();
                if (out.pos == 0) {
                    writing = false;
                    lock.unlock();
                    break;
                }
                writing = true;
                RespOut b = out;
                out = spare;
                spare = null;
                lock.unlock();
                if (!writeBuffer(b)) {
                    return;
                }
                recycle(b);
                lock.lock();
                spare = b;
                lock.unlock();
            }
        }
    }

    /** Writes the whole buffer. Returns false (and fails the connection) on an I/O error. */
    private boolean writeBuffer(RespOut b) {
        try {
            ByteBuffer bb = view(b.buf);
            bb.limit(b.pos).position(0);
            while (bb.hasRemaining()) {
                channel.write(bb);
            }
            return true;
        } catch (IOException | RuntimeException e) {
            fail(new RuvioConnectionException("write failed: " + e.getMessage(), e));
            return false;
        }
    }

    // Only one thread writes at a time (the "writing" flag), so the cached view is safe.
    private ByteBuffer view(byte[] array) {
        if (writeViewArray != array) {
            writeViewArray = array;
            writeView = ByteBuffer.wrap(array);
        }
        writeView.clear();
        return writeView;
    }

    // ---- read side ------------------------------------------------------------------------

    private void readLoop() {
        byte[] buf = new byte[READ_BUFFER];
        ByteBuffer bb = ByteBuffer.wrap(buf);
        RespReader r = new RespReader(buf);
        int lim = 0;
        try {
            for (;;) {
                bb.limit(buf.length).position(lim);
                int n = channel.read(bb);
                if (n < 0) {
                    throw new RuvioConnectionException("connection closed by server");
                }
                lim += n;
                r.wrap(buf, 0, lim);
                while (r.pos < lim) {
                    int start = r.pos;
                    try {
                        dispatch(r);
                    } catch (RespReader.Incomplete e) {
                        r.pos = start;
                        break;
                    }
                }
                int rest = lim - r.pos;
                if (rest > 0 && r.pos > 0) {
                    System.arraycopy(buf, r.pos, buf, 0, rest);
                }
                lim = rest;
                if (lim == buf.length) {
                    byte[] bigger = new byte[buf.length << 1];
                    System.arraycopy(buf, 0, bigger, 0, lim);
                    buf = bigger;
                    bb = ByteBuffer.wrap(buf);
                } else if (lim == 0 && buf.length > SHRINK_ABOVE) {
                    buf = new byte[READ_BUFFER];
                    bb = ByteBuffer.wrap(buf);
                }
            }
        } catch (Throwable e) {
            fail(e instanceof RuvioException ? e : new RuvioConnectionException("read failed: " + e.getMessage(), e));
        }
    }

    /**
     * Returns the pending request at the head of the FIFO. Reader thread only.
     *
     * @return the request, or null when none is pending
     */
    public final Request<?> peekRequest() {
        long h = head;
        if (h == tail) {
            return null;
        }
        return ring[(int) (h & mask)];
    }

    /** Removes the request at the head of the FIFO. Reader thread only. */
    public final void popRequest() {
        long h = head;
        ring[(int) (h & mask)] = null;
        head = h + 1;
    }

    /**
     * Decodes one complete reply at the reader position. Throws Incomplete to wait for bytes.
     *
     * @param r the reader
     */
    public void dispatch(RespReader r) {
        byte t = r.peek();
        if (t == '>') {
            Object push = r.readObject();
            Consumer<Object> h = pushHandler;
            if (h != null) {
                try {
                    h.accept(push);
                } catch (RuntimeException ignored) {
                    // listener failures must not kill the reader
                }
            }
            return;
        }
        Request<?> q = peekRequest();
        if (q == null) {
            throw new RuvioException("protocol error: reply without a pending request");
        }
        if (t == '-') {
            RuvioServerException err = r.readError();
            popRequest();
            q.fail(err);
            return;
        }
        int start = r.pos;
        Object value;
        try {
            value = q.decode(r);
        } catch (RespReader.Incomplete e) {
            throw e;
        } catch (RuntimeException e) {
            r.pos = start;
            r.skip(); // may throw Incomplete; the decode is retried then
            popRequest();
            q.fail(e);
            return;
        }
        popRequest();
        q.deliver(value);
    }

    // ---- shutdown -------------------------------------------------------------------------

    private RuvioConnectionException closedException() {
        Throwable c = closed;
        if (c instanceof RuvioConnectionException) {
            return new RuvioConnectionException(c.getMessage(), c);
        }
        return new RuvioConnectionException("connection closed", c);
    }

    /**
     * Closes the connection and fails every pending request with this cause.
     *
     * @param cause why the connection failed
     */
    public void fail(Throwable cause) {
        lock.lock();
        try {
            if (closed != null) {
                return;
            }
            closed = cause;
        } finally {
            lock.unlock();
        }
        try {
            channel.close();
        } catch (IOException ignored) {
            // closing anyway
        }
        LockSupport.unpark(writer);
        // Producers can no longer push (they check "closed" under the lock), so drain the ring.
        // Wait for the reader to stop touching head before draining from another thread.
        if (Thread.currentThread() != reader) {
            try {
                reader.join(TimeUnit.SECONDS.toMillis(5));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        RuvioConnectionException ex = closedException();
        long h = head;
        long t = tail;
        for (long i = h; i < t; i++) {
            Request<?> q = ring[(int) (i & mask)];
            ring[(int) (i & mask)] = null;
            if (q != null) {
                q.fail(ex);
            }
        }
        head = t;
        onClosed(cause);
    }

    /**
     * Hook for subclasses (pub/sub reconnect), called after the connection failed.
     *
     * @param cause why it failed
     */
    public void onClosed(Throwable cause) {
    }

    @Override
    public void close() {
        fail(new RuvioConnectionException("connection closed by client"));
    }
}
