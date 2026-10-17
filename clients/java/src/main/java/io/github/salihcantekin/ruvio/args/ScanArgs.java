// SPDX-License-Identifier: MIT
package io.github.salihcantekin.ruvio.args;

import io.github.salihcantekin.ruvio.internal.Args;
import io.github.salihcantekin.ruvio.internal.Cmd;

/**
 * Options of the {@code SCAN} family: {@code MATCH} pattern, {@code COUNT} hint and (for {@code SCAN} only) {@code TYPE}.
 *
 * <p>{@code ScanArgs.match("user:*").limit(500).type(KeyType.HASH)}
 */
public final class ScanArgs {
    /** Creates an empty option set; the static factories are the usual way in. */
    public ScanArgs() {
    }

    private String match;
    private long count;
    private String type;
    private KeyType keyType;

    /**
     * Starts with a {@code MATCH} pattern.
     *
     * @param pattern a glob pattern ({@code *}, {@code ?}, {@code [abc]}) applied to keys, fields or members
     * @return a new option set
     */
    public static ScanArgs match(String pattern) {
        return new ScanArgs().matching(pattern);
    }

    /**
     * Starts with a {@code COUNT} hint.
     *
     * @param count roughly how many elements to examine per call (above 0)
     * @return a new option set
     */
    public static ScanArgs count(long count) {
        return new ScanArgs().limit(count);
    }

    /**
     * Sets the {@code MATCH} pattern.
     *
     * @param pattern a glob pattern ({@code *}, {@code ?}, {@code [abc]})
     * @return this builder
     */
    public ScanArgs matching(String pattern) {
        this.match = pattern;
        return this;
    }

    /**
     * Sets the {@code COUNT} hint; the page may be larger or smaller.
     *
     * @param count roughly how many elements to examine per call (above 0)
     * @return this builder
     */
    public ScanArgs limit(long count) {
        this.count = count;
        return this;
    }

    /**
     * Sets {@code TYPE} (for {@code SCAN} only): return keys holding this type.
     *
     * @param type a type name such as {@code string}, {@code list}, {@code set}, {@code zset}, {@code hash} or {@code stream}; see {@link KeyType} for every value
     * @return this option set
     */
    public ScanArgs type(String type) {
        this.type = type;
        this.keyType = null;
        return this;
    }

    /**
     * Sets {@code TYPE} from {@link KeyType} (for {@code SCAN} only); sent from pre-encoded bytes. Example: {@code ScanArgs.count(100).type(KeyType.HASH)}.
     *
     * @param type the type to return
     * @return this option set
     */
    public ScanArgs type(KeyType type) {
        this.keyType = type;
        this.type = null;
        return this;
    }

    /**
     * Appends these options to a command being encoded. Used by the client; you do not call it.
     *
     * @param a the argument writer
     * @param allowType whether {@code TYPE} may be written ({@code SCAN} only)
     */
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
