// SPDX-License-Identifier: MIT
package io.github.salihcantekin.ruvio.args;

import io.github.salihcantekin.ruvio.internal.Args;
import io.github.salihcantekin.ruvio.internal.RespOut;

/**
 * Options of {@code SET}: one expiry (EX, PX, EXAT, PXAT or KEEPTTL) and one condition (NX or XX).
 * For the Ruvio {@code IFEQ} condition use {@code setIfEq}; for {@code GET} use {@code setGet}.
 */
public final class SetOptions {
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

    public static SetOptions builder() {
        return new SetOptions();
    }

    public static SetOptions ex(long seconds) {
        return new SetOptions().expireSeconds(seconds);
    }

    public static SetOptions px(long millis) {
        return new SetOptions().expireMillis(millis);
    }

    public static SetOptions nx() {
        return new SetOptions().ifAbsent();
    }

    public static SetOptions xx() {
        return new SetOptions().ifExists();
    }

    public SetOptions expireSeconds(long seconds) {
        expiry = EX;
        expiryValue = seconds;
        return this;
    }

    public SetOptions expireMillis(long millis) {
        expiry = PX;
        expiryValue = millis;
        return this;
    }

    public SetOptions expireAtSeconds(long unixSeconds) {
        expiry = EXAT;
        expiryValue = unixSeconds;
        return this;
    }

    public SetOptions expireAtMillis(long unixMillis) {
        expiry = PXAT;
        expiryValue = unixMillis;
        return this;
    }

    public SetOptions keepTtl() {
        expiry = KEEPTTL;
        return this;
    }

    /** NX: write only when the key is missing. */
    public SetOptions ifAbsent() {
        condition = NX;
        return this;
    }

    /** XX: write only when the key exists. */
    public SetOptions ifExists() {
        condition = XX;
        return this;
    }

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

    /** Expiry only (IFEQ does not combine with NX/XX). */
    public void writeExpiry(Args a) {
        if (expiry != null) {
            a.w(expiry);
            if (expiry != KEEPTTL) {
                a.l(expiryValue);
            }
        }
    }
}
