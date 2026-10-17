// SPDX-License-Identifier: MIT
package io.github.salihcantekin.ruvio.args;

import io.github.salihcantekin.ruvio.internal.Args;
import io.github.salihcantekin.ruvio.internal.RespOut;

import java.util.ArrayList;
import java.util.List;

/**
 * BITFIELD operations. Types are {@code i1..i64} and {@code u1..u63}; offsets are numbers or
 * {@code #n}. BITFIELD_RO accepts only GET operations.
 */
public final class BitFieldArgs {
    /** OVERFLOW mode: WRAP, SAT or FAIL. */
    public enum Overflow {
        WRAP, SAT, FAIL;

        private final byte[] fragment;

        Overflow() {
            this.fragment = RespOut.fragment(name());
        }

        /**
         * The keyword pre-encoded as a complete RESP bulk string, identical on the wire to {@code name()}.
         * The array is shared and must not be modified.
         */
        public byte[] fragment() {
            return fragment;
        }
    }

    private static final byte[] OP_GET = RespOut.fragment("GET");
    private static final byte[] OP_SET = RespOut.fragment("SET");
    private static final byte[] OP_INCRBY = RespOut.fragment("INCRBY");
    private static final byte[] OP_OVERFLOW = RespOut.fragment("OVERFLOW");

    /**
     * A BITFIELD integer type: {@code i1..i64} (signed) or {@code u1..u63} (unsigned). The wire form is
     * pre-encoded once, so using a type adds no per-call encoding cost.
     *
     * <p>Example: {@code BitFieldArgs.builder().set(BitFieldArgs.Type.unsigned(8), 0, 200)}
     */
    public static final class Type {
        private static final Type[] SIGNED = new Type[65];
        private static final Type[] UNSIGNED = new Type[64];

        static {
            for (int i = 1; i < SIGNED.length; i++) {
                SIGNED[i] = new Type("i" + i);
            }
            for (int i = 1; i < UNSIGNED.length; i++) {
                UNSIGNED[i] = new Type("u" + i);
            }
        }

        private final String word;
        private final byte[] fragment;

        private Type(String word) {
            this.word = word;
            this.fragment = RespOut.fragment(word);
        }

        /** The signed type {@code i<bits>}; {@code bits} is 1 to 64. */
        public static Type signed(int bits) {
            if (bits < 1 || bits > 64) {
                throw new IllegalArgumentException("signed bit width must be 1..64: " + bits);
            }
            return SIGNED[bits];
        }

        /** The unsigned type {@code u<bits>}; {@code bits} is 1 to 63. */
        public static Type unsigned(int bits) {
            if (bits < 1 || bits > 63) {
                throw new IllegalArgumentException("unsigned bit width must be 1..63: " + bits);
            }
            return UNSIGNED[bits];
        }

        /** The wire text, for example {@code "u8"}. */
        public String word() {
            return word;
        }

        /** The type pre-encoded as a RESP bulk string. The array is shared and must not be modified. */
        public byte[] fragment() {
            return fragment;
        }

        @Override
        public String toString() {
            return word;
        }
    }

    private final List<Object> parts = new ArrayList<>();

    public static BitFieldArgs builder() {
        return new BitFieldArgs();
    }

    public BitFieldArgs get(String type, String offset) {
        parts.add(OP_GET);
        parts.add(type);
        parts.add(offset);
        return this;
    }

    public BitFieldArgs get(String type, long offset) {
        return get(type, Long.toString(offset));
    }

    public BitFieldArgs set(String type, String offset, long value) {
        parts.add(OP_SET);
        parts.add(type);
        parts.add(offset);
        parts.add(value);
        return this;
    }

    public BitFieldArgs set(String type, long offset, long value) {
        return set(type, Long.toString(offset), value);
    }

    public BitFieldArgs incrBy(String type, String offset, long increment) {
        parts.add(OP_INCRBY);
        parts.add(type);
        parts.add(offset);
        parts.add(increment);
        return this;
    }

    public BitFieldArgs incrBy(String type, long offset, long increment) {
        return incrBy(type, Long.toString(offset), increment);
    }

    /** GET with a typed integer width, for example {@code get(Type.unsigned(8), 0)}. */
    public BitFieldArgs get(Type type, String offset) {
        parts.add(OP_GET);
        parts.add(type.fragment());
        parts.add(offset);
        return this;
    }

    /** GET with a typed integer width and a numeric offset. */
    public BitFieldArgs get(Type type, long offset) {
        return get(type, Long.toString(offset));
    }

    /** SET with a typed integer width, for example {@code set(Type.signed(16), "#1", -5)}. */
    public BitFieldArgs set(Type type, String offset, long value) {
        parts.add(OP_SET);
        parts.add(type.fragment());
        parts.add(offset);
        parts.add(value);
        return this;
    }

    /** SET with a typed integer width and a numeric offset. */
    public BitFieldArgs set(Type type, long offset, long value) {
        return set(type, Long.toString(offset), value);
    }

    /** INCRBY with a typed integer width, for example {@code incrBy(Type.unsigned(4), 8, 1)}. */
    public BitFieldArgs incrBy(Type type, String offset, long increment) {
        parts.add(OP_INCRBY);
        parts.add(type.fragment());
        parts.add(offset);
        parts.add(increment);
        return this;
    }

    /** INCRBY with a typed integer width and a numeric offset. */
    public BitFieldArgs incrBy(Type type, long offset, long increment) {
        return incrBy(type, Long.toString(offset), increment);
    }

    public BitFieldArgs overflow(Overflow mode) {
        parts.add(OP_OVERFLOW);
        parts.add(mode.fragment());
        return this;
    }

    public void write(Args a) {
        for (Object p : parts) {
            if (p instanceof byte[]) {
                a.w((byte[]) p);
            } else {
                a.any(p);
            }
        }
    }
}
