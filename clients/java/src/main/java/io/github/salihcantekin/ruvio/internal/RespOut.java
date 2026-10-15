// SPDX-License-Identifier: MIT
package io.github.salihcantekin.ruvio.internal;

import java.util.Arrays;

/**
 * Growable RESP request encoder over a plain {@code byte[]}. Integers are written as ASCII digits
 * straight into the buffer and ASCII strings are copied with one intrinsic, so encoding a typed
 * command allocates nothing.
 */
public final class RespOut {
    public static final byte[] CRLF = {'\r', '\n'};
    private static final byte[] MIN_LONG = "-9223372036854775808".getBytes(java.nio.charset.StandardCharsets.US_ASCII);

    public byte[] buf;
    public int pos;
    /** The connection this buffer belongs to (null for private buffers). */
    public Connection owner;
    /** The cluster scratch frame this buffer belongs to (null for connection and pipeline buffers). */
    public Frame frame;

    public RespOut(int capacity) {
        buf = new byte[capacity];
    }

    public void reset() {
        pos = 0;
    }

    public int size() {
        return pos;
    }

    public void ensure(int extra) {
        int need = pos + extra;
        if (need > buf.length) {
            grow(need);
        }
    }

    private void grow(int need) {
        int capacity = Math.max(buf.length << 1, need);
        buf = Arrays.copyOf(buf, capacity);
    }

    /** {@code *argc\r\n}. */
    public void header(int argc) {
        ensure(24);
        byte[] b = buf;
        int p = pos;
        b[p++] = '*';
        if (argc < 10) {
            b[p++] = (byte) ('0' + argc);
        } else {
            p = writeLong(b, p, argc);
        }
        b[p++] = '\r';
        b[p++] = '\n';
        pos = p;
    }

    /** Copies a pre-encoded fragment such as {@code $3\r\nGET\r\n}. */
    public void raw(byte[] fragment) {
        int n = fragment.length;
        ensure(n);
        System.arraycopy(fragment, 0, buf, pos, n);
        pos += n;
    }

    public void raw(byte[] src, int off, int len) {
        ensure(len);
        System.arraycopy(src, off, buf, pos, len);
        pos += len;
    }

    public void bulk(byte[] value) {
        bulk(value, 0, value.length);
    }

    public void bulk(byte[] value, int off, int len) {
        ensure(len + 24);
        byte[] b = buf;
        int p = lengthPrefix(b, pos, len);
        System.arraycopy(value, off, b, p, len);
        p += len;
        b[p++] = '\r';
        b[p++] = '\n';
        pos = p;
    }

    @SuppressWarnings("deprecation")
    public void bulk(String value) {
        int n = value.length();
        int utf8 = n;
        boolean ascii = true;
        for (int i = 0; i < n; i++) {
            if (value.charAt(i) >= 0x80) {
                utf8 = utf8Length(value, i);
                ascii = false;
                break;
            }
        }
        ensure(utf8 + 24);
        byte[] b = buf;
        int p = lengthPrefix(b, pos, utf8);
        if (ascii) {
            // Latin-1 compact strings copy with an intrinsic; every char here is < 0x80.
            value.getBytes(0, n, b, p);
            p += n;
        } else {
            p = encodeUtf8(value, b, p);
        }
        b[p++] = '\r';
        b[p++] = '\n';
        pos = p;
    }

    public void bulk(long value) {
        ensure(48);
        byte[] b = buf;
        int p = pos;
        int digits = digitCount(value);
        b[p++] = '$';
        if (digits < 10) {
            b[p++] = (byte) ('0' + digits);
        } else {
            b[p++] = (byte) ('0' + digits / 10);
            b[p++] = (byte) ('0' + digits % 10);
        }
        b[p++] = '\r';
        b[p++] = '\n';
        p = writeLong(b, p, value);
        b[p++] = '\r';
        b[p++] = '\n';
        pos = p;
    }

    public void bulk(double value) {
        if (value == (long) value && Math.abs(value) < 1e15 && !(value == 0 && 1 / value < 0)) {
            bulk((long) value);
        } else if (Double.isInfinite(value)) {
            bulk(value > 0 ? "+inf" : "-inf");
        } else {
            bulk(Double.toString(value));
        }
    }

    /** Writes any supported argument type: byte[], CharSequence, integral Number, Double/Float. */
    public void arg(Object value) {
        if (value instanceof byte[]) {
            bulk((byte[]) value);
        } else if (value instanceof String) {
            bulk((String) value);
        } else if (value instanceof Long || value instanceof Integer || value instanceof Short || value instanceof Byte) {
            bulk(((Number) value).longValue());
        } else if (value instanceof Double || value instanceof Float) {
            bulk(((Number) value).doubleValue());
        } else if (value == null) {
            throw new IllegalArgumentException("null argument");
        } else {
            bulk(value.toString());
        }
    }

    private static int lengthPrefix(byte[] b, int p, int len) {
        b[p++] = '$';
        if (len < 10) {
            b[p++] = (byte) ('0' + len);
        } else {
            p = writeLong(b, p, len);
        }
        b[p++] = '\r';
        b[p++] = '\n';
        return p;
    }

    public static int digitCount(long v) {
        if (v < 0) {
            return v == Long.MIN_VALUE ? 20 : 1 + digitCount(-v);
        }
        if (v < 10) return 1;
        if (v < 100) return 2;
        if (v < 1000) return 3;
        if (v < 10000) return 4;
        int n = 5;
        long x = v / 100000;
        while (x > 0) {
            n++;
            x /= 10;
        }
        return n;
    }

    /** Writes the ASCII digits of {@code v} at {@code p} and returns the new position. */
    public static int writeLong(byte[] b, int p, long v) {
        if (v < 0) {
            if (v == Long.MIN_VALUE) {
                System.arraycopy(MIN_LONG, 0, b, p, MIN_LONG.length);
                return p + MIN_LONG.length;
            }
            b[p++] = '-';
            v = -v;
        }
        int end = p + digitCount(v);
        int i = end;
        do {
            long q = v / 10;
            b[--i] = (byte) ('0' + (int) (v - q * 10));
            v = q;
        } while (v != 0);
        return end;
    }

    public static int utf8Length(CharSequence s, int from) {
        int n = s.length();
        int len = from;
        for (int i = from; i < n; i++) {
            char c = s.charAt(i);
            if (c < 0x80) {
                len++;
            } else if (c < 0x800) {
                len += 2;
            } else if (Character.isHighSurrogate(c) && i + 1 < n && Character.isLowSurrogate(s.charAt(i + 1))) {
                len += 4;
                i++;
            } else if (Character.isSurrogate(c)) {
                len += 1;
            } else {
                len += 3;
            }
        }
        return len;
    }

    private static int encodeUtf8(String s, byte[] b, int p) {
        int n = s.length();
        for (int i = 0; i < n; i++) {
            char c = s.charAt(i);
            if (c < 0x80) {
                b[p++] = (byte) c;
            } else if (c < 0x800) {
                b[p++] = (byte) (0xC0 | (c >> 6));
                b[p++] = (byte) (0x80 | (c & 0x3F));
            } else if (Character.isHighSurrogate(c) && i + 1 < n && Character.isLowSurrogate(s.charAt(i + 1))) {
                int cp = Character.toCodePoint(c, s.charAt(++i));
                b[p++] = (byte) (0xF0 | (cp >> 18));
                b[p++] = (byte) (0x80 | ((cp >> 12) & 0x3F));
                b[p++] = (byte) (0x80 | ((cp >> 6) & 0x3F));
                b[p++] = (byte) (0x80 | (cp & 0x3F));
            } else if (Character.isSurrogate(c)) {
                b[p++] = (byte) '?'; // lone surrogate, as String.getBytes(UTF_8) does
            } else {
                b[p++] = (byte) (0xE0 | (c >> 12));
                b[p++] = (byte) (0x80 | ((c >> 6) & 0x3F));
                b[p++] = (byte) (0x80 | (c & 0x3F));
            }
        }
        return p;
    }

    /** Pre-encodes a command word as a complete RESP bulk fragment, for example {@code $3\r\nGET\r\n}. */
    public static byte[] fragment(String word) {
        byte[] w = word.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        String head = "$" + w.length + "\r\n";
        byte[] out = new byte[head.length() + w.length + 2];
        System.arraycopy(head.getBytes(java.nio.charset.StandardCharsets.US_ASCII), 0, out, 0, head.length());
        System.arraycopy(w, 0, out, head.length(), w.length);
        out[out.length - 2] = '\r';
        out[out.length - 1] = '\n';
        return out;
    }
}
