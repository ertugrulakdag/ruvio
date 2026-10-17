// SPDX-License-Identifier: MIT
package io.github.salihcantekin.ruvio.args;

import io.github.salihcantekin.ruvio.internal.Args;
import io.github.salihcantekin.ruvio.internal.Cmd;

/** MATCH / COUNT (and TYPE for SCAN) options of the SCAN family. */
public final class ScanArgs {
    private String match;
    private long count;
    private String type;
    private KeyType keyType;

    public static ScanArgs match(String pattern) {
        return new ScanArgs().matching(pattern);
    }

    public static ScanArgs count(long count) {
        return new ScanArgs().limit(count);
    }

    public ScanArgs matching(String pattern) {
        this.match = pattern;
        return this;
    }

    public ScanArgs limit(long count) {
        this.count = count;
        return this;
    }

    /** SCAN only: the value type (string, list, set, zset, hash, stream). */
    public ScanArgs type(String type) {
        this.type = type;
        this.keyType = null;
        return this;
    }

    /**
     * SCAN only: the value type as an enum, written from pre-encoded bytes. Example:
     * {@code ScanArgs.count(100).type(KeyType.HASH)}.
     */
    public ScanArgs type(KeyType type) {
        this.keyType = type;
        this.type = null;
        return this;
    }

    public void write(Args a, boolean allowType) {
        if (match != null) {
            a.w(Cmd.MATCH).s(match);
        }
        if (count > 0) {
            a.w(Cmd.COUNT).l(count);
        }
        if (allowType && keyType != null) {
            a.w(Cmd.TYPE).w(keyType.fragment());
        } else if (allowType && type != null) {
            a.w(Cmd.TYPE).s(type);
        }
    }
}
