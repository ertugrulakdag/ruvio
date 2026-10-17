// SPDX-License-Identifier: MIT
package io.github.salihcantekin.ruvio.args;

import io.github.salihcantekin.ruvio.internal.Args;
import io.github.salihcantekin.ruvio.internal.RespOut;

/**
 * Options of {@code SET}: one expiry (EX, PX, EXAT, PXAT or KEEPTTL) and one condition (NX or XX).
 * For the Ruvio {@code IFEQ} condition use {@code setIfEq}; for {@code GET} use {@code setGet}.
 */
public final class SetOptions {
    /** Creates an empty option set; the static factories are the usual way in. */
    public SetOptions() {
    }

    private static final byte[] EX = RespOut.fragment("EX");
    private static final byte[] PX = RespOut.fragment("PX");
    private static final byte[] EXAT = RespOut.fragment("EXAT");
    private static final byte[] PXAT = RespOut.fragment("PXAT");
    private static final byte[] KEEPTTL = RespOut.fragment("KEEPTTL");
    private static final byte[] NX = RespOut.fragment("NX");
    private static final byte[] XX = RespOut.fragment("XX");

    private byte[] expiry;
    private long expiryValue;
    private byte[] condition;

    /**
     * Starts an empty option set (no expiry, no condition).
     *
     * @return a new option set
     */
    public static SetOptions builder() {
        return new SetOptions();
    }

    /**
     * Starts with {@code EX}.
     *
     * @param seconds time to live in seconds (above 0)
     * @return a new option set
     */
    public static SetOptions ex(long seconds) {
        return new SetOptions().expireSeconds(seconds);
    }

    /**
     * Starts with {@code PX}.
     *
     * @param millis time to live in milliseconds (above 0)
     * @return a new option set
     */
    public static SetOptions px(long millis) {
        return new SetOptions().expireMillis(millis);
    }

    /**
     * Starts with {@code NX}: write only when the key is missing.
     *
     * @return a new option set
     */
    public static SetOptions nx() {
        return new SetOptions().ifAbsent();
    }

    /**
     * Starts with {@code XX}: write only when the key exists.
     *
     * @return a new option set
     */
    public static SetOptions xx() {
        return new SetOptions().ifExists();
    }

    /**
     * Sets {@code EX}: expire after a number of seconds. Replaces any earlier expiry setting.
     *
     * @param seconds time to live in seconds (above 0)
     * @return this builder
     */
    public SetOptions expireSeconds(long seconds) {
        expiry = EX;
        expiryValue = seconds;
        return this;
    }

    /**
     * Sets {@code PX}: expire after a number of milliseconds. Replaces any earlier expiry setting.
     *
     * @param millis time to live in milliseconds (above 0)
     * @return this builder
     */
    public SetOptions expireMillis(long millis) {
        expiry = PX;
        expiryValue = millis;
        return this;
    }

    /**
     * Sets {@code EXAT}: expire at a Unix time. Replaces any earlier expiry setting.
     *
     * @param unixSeconds the absolute time in seconds since 1970
     * @return this builder
     */
    public SetOptions expireAtSeconds(long unixSeconds) {
        expiry = EXAT;
        expiryValue = unixSeconds;
        return this;
    }

    /**
     * Sets {@code PXAT}: expire at a Unix time. Replaces any earlier expiry setting.
     *
     * @param unixMillis the absolute time in milliseconds since 1970
     * @return this builder
     */
    public SetOptions expireAtMillis(long unixMillis) {
        expiry = PXAT;
        expiryValue = unixMillis;
        return this;
    }

    /**
     * Sets {@code KEEPTTL}: keep the key's current expiry. Replaces any earlier expiry setting.
     *
     * @return this builder
     */
    public SetOptions keepTtl() {
        expiry = KEEPTTL;
        return this;
    }

    /**
     * Sets {@code NX}: write only when the key is missing.
     *
     * @return this option set
     */
    public SetOptions ifAbsent() {
        condition = NX;
        return this;
    }

    /**
     * Sets {@code XX}: write only when the key exists.
     *
     * @return this option set
     */
    public SetOptions ifExists() {
        condition = XX;
        return this;
    }

    /**
     * Appends the expiry and condition to a command being encoded. Used by the client; you do not call it.
     *
     * @param a the argument writer
     */
    public void write(Args a) {
        if (expiry != null) {
            a.w(expiry);
            if (expiry != KEEPTTL) {
                a.l(expiryValue);
            }
        }
        if (condition != null) {
            a.w(condition);
        }
    }

    /**
     * Appends only the expiry (used by {@code IFEQ}, which does not combine with {@code NX}/{@code XX}). Used by the client; you do not call it.
     *
     * @param a the argument writer
     */
    public void writeExpiry(Args a) {
        if (expiry != null) {
            a.w(expiry);
            if (expiry != KEEPTTL) {
                a.l(expiryValue);
            }
        }
    }
}
