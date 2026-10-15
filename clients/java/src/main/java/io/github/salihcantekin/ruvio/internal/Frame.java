// SPDX-License-Identifier: MIT
package io.github.salihcantekin.ruvio.internal;

/**
 * Scratch buffer for one encoded command in cluster mode. The typed methods encode into {@link #out};
 * {@link #parse()} then records where each argument sits so the router can read the command name and
 * hash the keys in place, without copying them.
 */
public final class Frame {
    public final RespOut out = new RespOut(512);
    public int[] off = new int[16];
    public int[] len = new int[16];
    /** Argument indexes of the command's keys, filled by {@link ClusterKeys#keys}. */
    public int[] keyIdx = new int[16];
    public int argc;

    public Frame() {
        out.frame = this;
    }

    public void reset() {
        out.pos = 0;
        if (out.buf.length > (1 << 20)) {
            out.buf = new byte[512];
        }
    }

    /** Indexes the arguments of the frame in {@code out}: {@code *N\r\n} followed by N bulk strings. */
    public void parse() {
        byte[] b = out.buf;
        int p = 1;
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

    public int[] keyBuffer(int needed) {
        if (needed > keyIdx.length) {
            keyIdx = new int[needed + (needed >> 1)];
        }
        return keyIdx;
    }
}
