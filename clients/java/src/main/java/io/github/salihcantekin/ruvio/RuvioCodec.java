// SPDX-License-Identifier: MIT
package io.github.salihcantekin.ruvio;

import io.github.salihcantekin.ruvio.internal.RespOut;

import java.nio.charset.StandardCharsets;

/**
 * Converts keys or values between Java objects and the bytes Ruvio stores.
 *
 * <p>The two built-in codecs, {@link #UTF8} and {@link #BYTES}, are special-cased by the encoder:
 * strings are written straight into the output buffer and replies are decoded straight from the
 * read buffer, so neither allocates an intermediate {@code byte[]}. A custom codec pays one
 * {@code byte[]} per encoded argument.
 *
 * <pre>{@code
 * RuvioCodec<Integer> ints = new RuvioCodec<>() {
 *     public byte[] encode(Integer v) { return v.toString().getBytes(StandardCharsets.UTF_8); }
 *     public Integer decode(byte[] b, int off, int len) { return Integer.parseInt(new String(b, off, len, StandardCharsets.UTF_8)); }
 * };
 * }</pre>
 *
 * @param <T> the Java type
 */
public abstract class RuvioCodec<T> {
    /** Creates a codec; for subclasses. */
    protected RuvioCodec() {
    }

    /** UTF-8 strings. */
    public static final RuvioCodec<String> UTF8 = new Utf8();
    /** Raw bytes, binary safe. Arrays passed in must not change until the command is sent. */
    public static final RuvioCodec<byte[]> BYTES = new Bytes();

    /**
     * Converts a value to the bytes sent to the server.
     *
     * @param value the value
     * @return the encoded bytes
     */
    public abstract byte[] encode(T value);

    /**
     * Builds a value from bytes received from the server. The array is reused afterwards, so copy what you keep.
     *
     * @param buffer the read buffer
     * @param offset where the value starts
     * @param length how many bytes the value has
     * @return the decoded value
     */
    public abstract T decode(byte[] buffer, int offset, int length);

    /**
     * Writes a value as one bulk string. Built-in codecs override this to avoid a copy.
     *
     * @param out the output buffer
     * @param value the value
     */
    public void write(RespOut out, T value) {
        out.bulk(encode(value));
    }

    private static final class Utf8 extends RuvioCodec<String> {
        @Override
        public byte[] encode(String value) {
            return value.getBytes(StandardCharsets.UTF_8);
        }

        @Override
        public String decode(byte[] buffer, int offset, int length) {
            return new String(buffer, offset, length, StandardCharsets.UTF_8);
        }

        @Override
        public void write(RespOut out, String value) {
            out.bulk(value);
        }
    }

    private static final class Bytes extends RuvioCodec<byte[]> {
        @Override
        public byte[] encode(byte[] value) {
            return value;
        }

        @Override
        public byte[] decode(byte[] buffer, int offset, int length) {
            byte[] copy = new byte[length];
            System.arraycopy(buffer, offset, copy, 0, length);
            return copy;
        }

        @Override
        public void write(RespOut out, byte[] value) {
            out.bulk(value);
        }
    }
}
