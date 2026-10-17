// SPDX-License-Identifier: MIT
package io.github.salihcantekin.ruvio.args;

import io.github.salihcantekin.ruvio.internal.Args;
import io.github.salihcantekin.ruvio.internal.RespOut;

import java.util.ArrayList;
import java.util.List;

/**
 * Builds the operations of {@code BITFIELD} (and {@code BITFIELD_RO}, which accepts only {@code GET}): read, write and increment integer fields of any width inside a string. Types are {@code i1..i64} and {@code u1..u63}; offsets are bit numbers or {@code #n}. The reply holds one entry per operation: the value read, the old value for {@code SET}, the new value for {@code INCRBY}, or {@code null} when {@code OVERFLOW FAIL} refused it.
 *
 * <p>{@code BitFieldArgs.builder().set(BitFieldArgs.Type.unsigned(8), 0, 200).incrBy(BitFieldArgs.Type.unsigned(8), 0, 100)}
 */
public final class BitFieldArgs {
    /** Creates an empty option set; the static factories are the usual way in. */
    public BitFieldArgs() {
    }

    /** The {@code OVERFLOW} mode for the {@code SET} and {@code INCRBY} operations that follow it. */
    public enum Overflow {
        /** Wrap around on overflow (the default). */
        WRAP,
        /** Clamp to the smallest or largest value of the type. */
        SAT,
        /** Do not change the value; the operation replies {@code null}. */
        FAIL;

        private final byte[] fragment;

        Overflow() {
            this.fragment = RespOut.fragment(name());
        }

        /**
         * Returns the keyword pre-encoded as a complete RESP bulk string, identical on the wire to {@code name()}.
         *
         * @return the shared encoded bytes; do not modify them
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
     * A {@code BITFIELD} integer type: {@code i1..i64} (signed) or {@code u1..u63} (unsigned). The wire form is pre-encoded once, so using a type adds no per-call encoding cost.
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

        /**
         * Returns the signed type {@code i<bits>}.
         *
         * @param bits the width, from 1 to 64
         * @return the type
         * @throws IllegalArgumentException when the width is out of range
         */
        public static Type signed(int bits) {
            if (bits < 1 || bits > 64) {
                throw new IllegalArgumentException("signed bit width must be 1..64: " + bits);
            }
            return SIGNED[bits];
        }

        /**
         * Returns the unsigned type {@code u<bits>}.
         *
         * @param bits the width, from 1 to 63
         * @return the type
         * @throws IllegalArgumentException when the width is out of range
         */
        public static Type unsigned(int bits) {
            if (bits < 1 || bits > 63) {
                throw new IllegalArgumentException("unsigned bit width must be 1..63: " + bits);
            }
            return UNSIGNED[bits];
        }

        /**
         * Returns the wire text.
         *
         * @return the type text, for example {@code "u8"}
         */
        public String word() {
            return word;
        }

        /**
         * Returns the type pre-encoded as a RESP bulk string.
         *
         * @return the shared encoded bytes; do not modify them
         */
        public byte[] fragment() {
            return fragment;
        }

        /**
         *             return word;
         * Returns the wire text.
         *
         * @return the type text
         */
        @Override
        public String toString() {
            return word;
        }
    }

    private final List<Object> parts = new ArrayList<>();

    /**
     * Starts an empty operation list.
     *
     * @return a new operation list
     */
    public static BitFieldArgs builder() {
        return new BitFieldArgs();
    }

    /**
     * Adds {@code GET type offset}: reads a field.
     *
     * @param type the integer type as text, for example {@code "u8"}
     * @param offset the bit offset, or {@code #n} to count in units of the type's width
     * @return this builder
     */
    public BitFieldArgs get(String type, String offset) {
        parts.add(OP_GET);
        parts.add(type);
        parts.add(offset);
        return this;
    }

    /**
     * Adds {@code GET type offset}: reads a field.
     *
     * @param type the integer type as text, for example {@code "u8"}
     * @param offset the bit offset (0 or more)
     * @return this builder
     */
    public BitFieldArgs get(String type, long offset) {
        return get(type, Long.toString(offset));
    }

    /**
     * Adds {@code SET type offset value}: writes a field; the reply is its old value.
     *
     * @param type the integer type as text, for example {@code "u8"}
     * @param offset the bit offset, or {@code #n} to count in units of the type's width
     * @param value the new value
     * @return this builder
     */
    public BitFieldArgs set(String type, String offset, long value) {
        parts.add(OP_SET);
        parts.add(type);
        parts.add(offset);
        parts.add(value);
        return this;
    }

    /**
     * Adds {@code SET type offset value}: writes a field; the reply is its old value.
     *
     * @param type the integer type as text, for example {@code "u8"}
     * @param offset the bit offset (0 or more)
     * @param value the new value
     * @return this builder
     */
    public BitFieldArgs set(String type, long offset, long value) {
        return set(type, Long.toString(offset), value);
    }

    /**
     * Adds {@code INCRBY type offset increment}: adds to a field; the reply is the new value.
     *
     * @param type the integer type as text, for example {@code "u8"}
     * @param offset the bit offset, or {@code #n} to count in units of the type's width
     * @param increment the amount to add (may be negative)
     * @return this builder
     */
    public BitFieldArgs incrBy(String type, String offset, long increment) {
        parts.add(OP_INCRBY);
        parts.add(type);
        parts.add(offset);
        parts.add(increment);
        return this;
    }

    /**
     * Adds {@code INCRBY type offset increment}: adds to a field; the reply is the new value.
     *
     * @param type the integer type as text, for example {@code "u8"}
     * @param offset the bit offset (0 or more)
     * @param increment the amount to add (may be negative)
     * @return this builder
     */
    public BitFieldArgs incrBy(String type, long offset, long increment) {
        return incrBy(type, Long.toString(offset), increment);
    }

    /**
     * Adds {@code GET type offset} with a typed integer width, for example {@code get(Type.unsigned(8), "#2")}.
     *
     * @param type the integer width, for example {@code Type.unsigned(8)}
     * @param offset the bit offset, or {@code #n} to count in units of the type's width
     * @return this builder
     */
    public BitFieldArgs get(Type type, String offset) {
        parts.add(OP_GET);
        parts.add(type.fragment());
        parts.add(offset);
        return this;
    }

    /**
     * Adds {@code GET type offset} with a typed integer width and a numeric offset.
     *
     * @param type the integer width, for example {@code Type.unsigned(8)}
     * @param offset the bit offset (0 or more)
     * @return this builder
     */
    public BitFieldArgs get(Type type, long offset) {
        return get(type, Long.toString(offset));
    }

    /**
     * Adds {@code SET type offset value} with a typed integer width, for example {@code set(Type.signed(16), "#1", -5)}; the reply is the old value.
     *
     * @param type the integer width, for example {@code Type.unsigned(8)}
     * @param offset the bit offset, or {@code #n} to count in units of the type's width
     * @param value the new value
     * @return this builder
     */
    public BitFieldArgs set(Type type, String offset, long value) {
        parts.add(OP_SET);
        parts.add(type.fragment());
        parts.add(offset);
        parts.add(value);
        return this;
    }

    /**
     * Adds {@code SET type offset value} with a typed integer width and a numeric offset; the reply is the old value.
     *
     * @param type the integer width, for example {@code Type.unsigned(8)}
     * @param offset the bit offset (0 or more)
     * @param value the new value
     * @return this builder
     */
    public BitFieldArgs set(Type type, long offset, long value) {
        return set(type, Long.toString(offset), value);
    }

    /**
     * Adds {@code INCRBY type offset increment} with a typed integer width, for example {@code incrBy(Type.unsigned(4), "8", 1)}; the reply is the new value.
     *
     * @param type the integer width, for example {@code Type.unsigned(8)}
     * @param offset the bit offset, or {@code #n} to count in units of the type's width
     * @param increment the amount to add (may be negative)
     * @return this builder
     */
    public BitFieldArgs incrBy(Type type, String offset, long increment) {
        parts.add(OP_INCRBY);
        parts.add(type.fragment());
        parts.add(offset);
        parts.add(increment);
        return this;
    }

    /**
     * Adds {@code INCRBY type offset increment} with a typed integer width and a numeric offset; the reply is the new value.
     *
     * @param type the integer width, for example {@code Type.unsigned(8)}
     * @param offset the bit offset (0 or more)
     * @param increment the amount to add (may be negative)
     * @return this builder
     */
    public BitFieldArgs incrBy(Type type, long offset, long increment) {
        return incrBy(type, Long.toString(offset), increment);
    }

    /**
     * Adds {@code OVERFLOW mode}: sets how the {@code SET} and {@code INCRBY} operations added after it handle overflow.
     *
     * @param mode wrap, saturate or fail
     * @return this builder
     */
    public BitFieldArgs overflow(Overflow mode) {
        parts.add(OP_OVERFLOW);
        parts.add(mode.fragment());
        return this;
    }

    /**
     * Appends the operations to a command being encoded. Used by the client; you do not call it.
     *
     * @param a the argument writer
     */
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
