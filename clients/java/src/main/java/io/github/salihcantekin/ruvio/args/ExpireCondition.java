// SPDX-License-Identifier: MIT
package io.github.salihcantekin.ruvio.args;

/** Condition flag of HEXPIRE and friends. {@link #ALWAYS} sends no flag. */
public enum ExpireCondition {
    ALWAYS, NX, XX, GT, LT
}
