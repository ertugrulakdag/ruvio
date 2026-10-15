// SPDX-License-Identifier: MIT
package io.github.salihcantekin.ruvio.internal;

import io.github.salihcantekin.ruvio.RuvioCodec;

/**
 * Argument builder for commands with optional parts. Arguments are encoded immediately into a
 * private buffer that is copied into the connection buffer in one {@code arraycopy}. Hot fixed-shape
 * commands do not use this class and encode straight into the connection buffer.
 */
public final class Args {
    public final RespOut out = new RespOut(128);
    public int count;
    private final RuvioCodec<Object> keys;
    private final RuvioCodec<Object> values;

    @SuppressWarnings("unchecked")
    public Args(RuvioCodec<?> keys, RuvioCodec<?> values) {
        this.keys = (RuvioCodec<Object>) keys;
        this.values = (RuvioCodec<Object>) values;
    }

    /** A pre-encoded word (command name, keyword). */
    public Args w(byte[] fragment) {
        out.raw(fragment);
        count++;
        return this;
    }

    public Args s(String text) {
        out.bulk(text);
        count++;
        return this;
    }

    public Args b(byte[] bytes) {
        out.bulk(bytes);
        count++;
        return this;
    }

    public Args l(long value) {
        out.bulk(value);
        count++;
        return this;
    }

    public Args d(double value) {
        out.bulk(value);
        count++;
        return this;
    }

    public Args k(Object key) {
        if (key == null) {
            throw new IllegalArgumentException("null key");
        }
        keys.write(out, key);
        count++;
        return this;
    }

    public Args v(Object value) {
        if (value == null) {
            throw new IllegalArgumentException("null value");
        }
        values.write(out, value);
        count++;
        return this;
    }

    public Args keys(Object[] keys) {
        for (Object key : keys) {
            k(key);
        }
        return this;
    }

    public Args values(Object[] values) {
        for (Object value : values) {
            v(value);
        }
        return this;
    }

    public Args strings(String[] items) {
        for (String item : items) {
            s(item);
        }
        return this;
    }

    public Args any(Object value) {
        out.arg(value);
        count++;
        return this;
    }
}
