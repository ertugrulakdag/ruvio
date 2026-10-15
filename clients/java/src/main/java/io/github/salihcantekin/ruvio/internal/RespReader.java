// SPDX-License-Identifier: MIT
package io.github.salihcantekin.ruvio.internal;

import io.github.salihcantekin.ruvio.RuvioCodec;
import io.github.salihcantekin.ruvio.RuvioException;
import io.github.salihcantekin.ruvio.RuvioServerException;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * In-place RESP2/RESP3 reply parser over a reusable {@code byte[]}. Every read either consumes a
 * complete element or throws {@link #INCOMPLETE}, in which case the caller rewinds {@link #pos} to
 * the start of the reply and reads more bytes. Scalars are decoded straight from the buffer, so an
 * integer, OK or nil reply allocates nothing beyond the returned value.
 */
public final class RespReader {
    /** Thrown when the buffer ends inside a reply. Preallocated, no stack trace. */
    public static final class Incomplete extends RuntimeException {
        private Incomplete() {
            super(null, null, false, false);
        }
    }

    public static final Incomplete INCOMPLETE = new Incomplete();

    /** RESP3 push frame ({@code >}); a list of elements. */
    public static final class Push extends ArrayList<Object> {
        Push(int n) {
            super(n);
        }
    }

    public byte[] buf;
    public int pos;
    public int lim;

    public RespReader(byte[] buf) {
        this.buf = buf;
    }

    public void wrap(byte[] b, int p, int l) {
        buf = b;
        pos = p;
        lim = l;
    }

    public byte peek() {
        if (pos >= lim) {
            throw INCOMPLETE;
        }
        return buf[pos];
    }

    /** Index of the '\r' that ends the line starting at {@code from}. */
    private int lineEnd(int from) {
        byte[] b = buf;
        int l = lim - 1;
        for (int i = from; i < l; i++) {
            if (b[i] == '\r') {
                return i;
            }
        }
        throw INCOMPLETE;
    }

    private long parseLong(int from, int end) {
        byte[] b = buf;
        int i = from;
        boolean neg = false;
        if (i < end && b[i] == '-') {
            neg = true;
            i++;
        } else if (i < end && b[i] == '+') {
            i++;
        }
        if (i == end) {
            throw new RuvioException("protocol error: empty integer");
        }
        long v = 0;
        for (; i < end; i++) {
            int d = b[i] - '0';
            if (d < 0 || d > 9) {
                throw new RuvioException("protocol error: bad integer " + new String(b, from, end - from, StandardCharsets.ISO_8859_1));
            }
            v = v * 10 - d;
        }
        return neg ? v : -v;
    }

    /** Reads {@code <type><long>\r\n} and returns the long (aggregate length, bulk length or integer). */
    private long headerLong() {
        int start = pos + 1;
        int end = lineEnd(start);
        long v = parseLong(start, end);
        pos = end + 2;
        return v;
    }

    /** Reads one line after the type byte as a String (simple string, error, big number). */
    public String readLine() {
        int start = pos + 1;
        int end = lineEnd(start);
        String s = new String(buf, start, end - start, StandardCharsets.UTF_8);
        pos = end + 2;
        return s;
    }

    /** Returns true when the current element is a null (RESP2 nil bulk/array or RESP3 {@code _}) and consumes it. */
    public boolean tryNull() {
        byte t = peek();
        if (t == '_') {
            lineEnd(pos + 1);
            pos += 3;
            return true;
        }
        if ((t == '$' || t == '*') && pos + 2 < lim && buf[pos + 1] == '-' && buf[pos + 2] == '1') {
            int end = lineEnd(pos + 1);
            pos = end + 2;
            return true;
        }
        return false;
    }

    /** Throws the server error at the current position, consuming it. */
    public RuvioServerException readError() {
        return new RuvioServerException(readLine());
    }

    /** RESP3 blob error ({@code !<len>\r\n<message>\r\n}); consumes it. */
    private RuvioServerException readBlobError() {
        long len = headerLong();
        if (len < 0 || pos + len + 2 > lim) {
            if (len < 0) {
                throw new RuvioException("protocol error: negative blob error length");
            }
            throw INCOMPLETE;
        }
        String message = new String(buf, pos, (int) len, StandardCharsets.UTF_8);
        pos += (int) len + 2;
        return new RuvioServerException(message);
    }

    /** Skips attribute frames ({@code |}) that precede a reply. */
    private void skipAttributes() {
        while (peek() == '|') {
            long n = headerLong();
            for (long i = 0; i < n * 2; i++) {
                skip();
            }
        }
    }

    /**
     * Locates the payload of a string-like element. Returns the payload offset and sets {@link #span}
     * to its length, or returns -1 for nil. Integers and doubles are returned as their text.
     */
    public int span;

    public int stringSpan() {
        skipAttributes();
        byte t = peek();
        switch (t) {
            case '$': {
                long len = headerLong();
                if (len < 0) {
                    return -1;
                }
                int off = pos;
                if (off + len + 2 > lim) {
                    pos = off; // caller rewinds anyway
                    throw INCOMPLETE;
                }
                pos = off + (int) len + 2;
                span = (int) len;
                return off;
            }
            case '+': case ':': case ',': case '(': {
                int start = pos + 1;
                int end = lineEnd(start);
                pos = end + 2;
                span = end - start;
                return start;
            }
            case '=': {
                long len = headerLong();
                int off = pos;
                if (off + len + 2 > lim) {
                    throw INCOMPLETE;
                }
                pos = off + (int) len + 2;
                int skipPrefix = len >= 4 ? 4 : 0; // "txt:"
                span = (int) len - skipPrefix;
                return off + skipPrefix;
            }
            case '_':
                lineEnd(pos + 1);
                pos += 3;
                return -1;
            case '*':
                if (tryNull()) {
                    return -1;
                }
                throw unexpected("string");
            case '-':
                throw readError();
            case '!':
                throw readBlobError();
            default:
                throw unexpected("string");
        }
    }

    public <T> T readBulk(RuvioCodec<T> codec) {
        int off = stringSpan();
        return off < 0 ? null : codec.decode(buf, off, span);
    }

    public String readString() {
        int off = stringSpan();
        return off < 0 ? null : new String(buf, off, span, StandardCharsets.UTF_8);
    }

    public byte[] readBytes() {
        int off = stringSpan();
        if (off < 0) {
            return null;
        }
        byte[] out = new byte[span];
        System.arraycopy(buf, off, out, 0, span);
        return out;
    }

    /** Integer reply (or a bulk holding decimal digits). Returns null for nil. */
    public Long readLongOrNull() {
        skipAttributes();
        byte t = peek();
        if (t == ':') {
            return headerLong();
        }
        if (t == '#') {
            int end = lineEnd(pos + 1);
            boolean v = buf[pos + 1] == 't';
            pos = end + 2;
            return v ? 1L : 0L;
        }
        int off = stringSpan();
        if (off < 0) {
            return null;
        }
        return parseLongText(off, span);
    }

    private long parseLongText(int off, int len) {
        return parseLong(off, off + len);
    }

    /** Double from a bulk, simple string, integer or RESP3 double. Returns null for nil. */
    public Double readDoubleOrNull() {
        int off = stringSpan();
        if (off < 0) {
            return null;
        }
        return parseDouble(buf, off, span);
    }

    public static double parseDouble(byte[] b, int off, int len) {
        // Fast path: plain integers.
        boolean simple = len > 0 && len < 18;
        long v = 0;
        int i = off;
        int end = off + len;
        boolean neg = false;
        if (simple && (b[i] == '-' || b[i] == '+')) {
            neg = b[i] == '-';
            i++;
            simple = i < end;
        }
        for (; simple && i < end; i++) {
            int d = b[i] - '0';
            if (d < 0 || d > 9) {
                simple = false;
                break;
            }
            v = v * 10 + d;
        }
        if (simple) {
            return neg ? -v : v;
        }
        String s = new String(b, off, len, StandardCharsets.ISO_8859_1);
        switch (s) {
            case "inf": case "+inf": case "Infinity": case "+Infinity":
                return Double.POSITIVE_INFINITY;
            case "-inf": case "-Infinity":
                return Double.NEGATIVE_INFINITY;
            case "nan": case "NaN":
                return Double.NaN;
            default:
                return Double.parseDouble(s);
        }
    }

    /**
     * Reads an aggregate header and returns the element count, or -1 for nil. RESP3 maps return
     * twice their entry count so callers can treat them as flat arrays.
     */
    public int arrayLength() {
        skipAttributes();
        byte t = peek();
        if (t == '*' || t == '~' || t == '>') {
            long n = headerLong();
            return (int) n;
        }
        if (t == '%') {
            return (int) (headerLong() * 2);
        }
        if (t == '_' || t == '$') {
            if (tryNull()) {
                return -1;
            }
        }
        if (t == '-') {
            throw readError();
        }
        throw unexpected("array");
    }

    public RuvioException unexpected(String wanted) {
        byte t = pos < lim ? buf[pos] : 0;
        return new RuvioException("unexpected reply type '" + (char) t + "', wanted " + wanted);
    }

    /** Skips one complete element of any type. */
    public void skip() {
        byte t = peek();
        switch (t) {
            case '+': case '-': case ':': case ',': case '(': case '#': case '_': {
                int end = lineEnd(pos + 1);
                pos = end + 2;
                return;
            }
            case '$': case '=': case '!': {
                long len = headerLong();
                if (len < 0) {
                    return;
                }
                if (pos + len + 2 > lim) {
                    throw INCOMPLETE;
                }
                pos += (int) len + 2;
                return;
            }
            case '*': case '~': case '>': case '%': case '|': {
                long n = headerLong();
                if (t == '%' || t == '|') {
                    n *= 2;
                }
                for (long i = 0; i < n; i++) {
                    skip();
                }
                if (t == '|') {
                    skip();
                }
                return;
            }
            default:
                throw new RuvioException("protocol error: unknown reply type '" + (char) t + "'");
        }
    }

    /**
     * Generic decode into a tree: bulk -> byte[], simple string -> String, integer -> Long,
     * nil -> null, array/set -> List, map -> LinkedHashMap (bulk keys become Strings),
     * error element -> {@link RuvioServerException} instance, double -> Double, boolean -> Boolean,
     * push -> {@link Push}.
     */
    public Object readObject() {
        byte t = peek();
        switch (t) {
            case '$': case '=': {
                return readBytes();
            }
            case '+':
                return readLine();
            case '(':
                return readLine();
            case ':':
                return headerLong();
            case '-':
                return readError();
            case '!':
                return readBlobError();
            case ',': {
                int off = stringSpan();
                return parseDouble(buf, off, span);
            }
            case '#': {
                int end = lineEnd(pos + 1);
                boolean v = buf[pos + 1] == 't';
                pos = end + 2;
                return v;
            }
            case '_': {
                lineEnd(pos + 1);
                pos += 3;
                return null;
            }
            case '*': case '~': case '>': {
                long n = headerLong();
                if (n < 0) {
                    return null;
                }
                List<Object> list = t == '>' ? new Push((int) n) : new ArrayList<>((int) Math.min(n, 1 << 16));
                for (long i = 0; i < n; i++) {
                    list.add(readObject());
                }
                return list;
            }
            case '%': {
                long n = headerLong();
                Map<Object, Object> map = new LinkedHashMap<>();
                for (long i = 0; i < n; i++) {
                    Object k = readObject();
                    if (k instanceof byte[]) {
                        k = new String((byte[]) k, StandardCharsets.UTF_8);
                    }
                    map.put(k, readObject());
                }
                return map;
            }
            case '|': {
                skipAttributes();
                return readObject();
            }
            default:
                throw new RuvioException("protocol error: unknown reply type '" + (char) t + "'");
        }
    }
}
