// SPDX-License-Identifier: MIT
package io.github.salihcantekin.ruvio.args;

import io.github.salihcantekin.ruvio.internal.Args;

import java.util.ArrayList;
import java.util.List;

/**
 * BITFIELD operations. Types are {@code i1..i64} and {@code u1..u63}; offsets are numbers or
 * {@code #n}. BITFIELD_RO accepts only GET operations.
 */
public final class BitFieldArgs {
    public enum Overflow { WRAP, SAT, FAIL }

    private final List<Object> parts = new ArrayList<>();

    public static BitFieldArgs builder() {
        return new BitFieldArgs();
    }

    public BitFieldArgs get(String type, String offset) {
        parts.add("GET");
        parts.add(type);
        parts.add(offset);
        return this;
    }

    public BitFieldArgs get(String type, long offset) {
        return get(type, Long.toString(offset));
    }

    public BitFieldArgs set(String type, String offset, long value) {
        parts.add("SET");
        parts.add(type);
        parts.add(offset);
        parts.add(value);
        return this;
    }

    public BitFieldArgs set(String type, long offset, long value) {
        return set(type, Long.toString(offset), value);
    }

    public BitFieldArgs incrBy(String type, String offset, long increment) {
        parts.add("INCRBY");
        parts.add(type);
        parts.add(offset);
        parts.add(increment);
        return this;
    }

    public BitFieldArgs incrBy(String type, long offset, long increment) {
        return incrBy(type, Long.toString(offset), increment);
    }

    public BitFieldArgs overflow(Overflow mode) {
        parts.add("OVERFLOW");
        parts.add(mode.name());
        return this;
    }

    public void write(Args a) {
        for (Object p : parts) {
            a.any(p);
        }
    }
}
