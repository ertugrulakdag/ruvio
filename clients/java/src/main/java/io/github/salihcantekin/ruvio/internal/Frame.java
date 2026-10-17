// SPDX-License-Identifier: MIT
package io.github.salihcantekin.ruvio.internal;

/**
 * Scratch buffer for one encoded command in cluster mode. The typed methods encode into {@link #out};
 * {@link #parse()} then records where each argument sits so the router can read the command name and
 * hash the keys in place, without copying them.
 */
public final class Frame {
    private static final int ARENA = 8192;
    private static final int MIN_ROOM = 1024;

    /** The buffer the command is encoded into. */
    public final RespOut out;
    /**
     * Where the current frame starts in {@code out.buf}. A retaining frame ({@link #Frame(boolean)}) encodes
     * successive commands one after another into the same array and hands the array to the request, so a
     * redirect can re-send the bytes without a per-command copy; the array is replaced when it fills up.
     */
    public int base;
    private final boolean retain;
    /** Start offset of each argument's bytes in the buffer, filled by {@link #parse()}. */
    public int[] off = new int[16];
    /** Length of each argument, filled by {@link #parse()}. */
    public int[] len = new int[16];
    /** Argument indexes of the command's keys, filled by {@link ClusterKeys#keys}. */
    public int[] keyIdx = new int[16];
    /** Number of arguments of the parsed frame. */
    public int argc;

    /** Creates a frame that is reused for every command. */
    public Frame() {
        this(false);
    }

    /**
     * Creates a frame.
     *
     * @param retain {@code true} to keep each encoded command in the array so a request can re-send it after a redirect
     */
    public Frame(boolean retain) {
        this.retain = retain;
        this.out = new RespOut(retain ? ARENA : 512);
        out.frame = this;
    }

    /** Starts a new frame. */
    public void reset() {
        if (retain) {
            if (out.buf.length != ARENA || out.buf.length - base < MIN_ROOM) {
                out.buf = new byte[ARENA];
                base = 0;
            }
            out.pos = base;
            return;
        }
        out.pos = 0;
        if (out.buf.length > (1 << 20)) {
            out.buf = new byte[512];
        }
    }

    /**
     * Tells whether this frame keeps encoded commands.
     *
     * @return {@code true} for a retaining frame
     */
    public boolean retains() {
        return retain;
    }

    /** Marks the frame just routed as kept by its request: the next one starts after it. */
    public void keep() {
        base = out.pos;
    }

    /** Indexes the arguments of the frame in {@code out}: {@code *N\r\n} followed by N bulk strings. */
    public void parse() {
        byte[] b = out.buf;
        int p = base + 1;
        int n = 0;
        while (b[p] != '\r') {
            n = n * 10 + (b[p++] - '0');
        }
        p += 2;
        if (n > off.length) {
            off = new int[n + (n >> 1)];
            len = new int[off.length];
        }
        for (int i = 0; i < n; i++) {
            p++; // '$'
            int l = 0;
            while (b[p] != '\r') {
                l = l * 10 + (b[p++] - '0');
            }
            p += 2;
            off[i] = p;
            len[i] = l;
            p += l + 2;
        }
        argc = n;
    }

    /**
     * Returns a scratch array for key argument indexes, grown when needed.
     *
     * @param needed the number of entries required
     * @return the array
     */
    public int[] keyBuffer(int needed) {
        if (needed > keyIdx.length) {
            keyIdx = new int[needed + (needed >> 1)];
        }
        return keyIdx;
    }
}
