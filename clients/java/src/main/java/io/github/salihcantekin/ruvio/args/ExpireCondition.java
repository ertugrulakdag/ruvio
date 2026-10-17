// SPDX-License-Identifier: MIT
package io.github.salihcantekin.ruvio.args;

/** Condition flag of {@code HEXPIRE} and the other field-expiry commands. {@link #ALWAYS} sends no flag. */
public enum ExpireCondition {
    /** Set the expiry unconditionally (no flag is sent). */
    ALWAYS,
    /** Set it only when the field has no expiry. */
    NX,
    /** Set it only when the field already has an expiry. */
    XX,
    /** Set it only when the new expiry is later than the current one. */
    GT,
    /** Set it only when the new expiry is earlier than the current one. */
    LT
}
