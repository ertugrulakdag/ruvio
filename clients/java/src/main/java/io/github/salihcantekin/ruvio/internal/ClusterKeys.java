// SPDX-License-Identifier: MIT
package io.github.salihcantekin.ruvio.internal;

import java.nio.charset.StandardCharsets;

/**
 * Key positions of every command, looked up by the (case-insensitive) command word straight from the
 * encoded frame. Mirrors the Rust and C# clients' tables and adds the shapes of the Ruvio command set
 * (GEOSEARCHSTORE, GEORADIUS ... STORE, XGROUP/XINFO, ...). A command that is not listed has its single
 * key in the first argument, which is what the large majority of commands look like.
 */
public final class ClusterKeys {
    /** No key: any node can answer. */
    public static final int KEYLESS = 0;
    /** The key is the first argument. */
    public static final int FIRST = 1;
    /** Every argument is a key. */
    public static final int ALL = 2;
    /** The first two arguments are keys. */
    public static final int TWO = 3;
    /** Every other argument is a key (MSET). */
    public static final int EVERY2 = 4;
    /** Every argument but the last is a key (BLPOP ... timeout, JSON.MGET ... path). */
    public static final int DROP_LAST = 5;
    /** {@code numkeys} at argument 1, then the keys. */
    public static final int COUNTED1 = 6;
    /** {@code numkeys} at argument 2, then the keys. */
    public static final int COUNTED2 = 7;
    /** Destination at argument 1, {@code numkeys} at argument 2, then the keys. */
    public static final int DEST_COUNTED2 = 8;
    /** Keys after the STREAMS keyword. */
    public static final int STREAMS = 9;
    /** Everything from argument 2 on (BITOP op dest key...). */
    public static final int FROM2 = 10;
    /** The key is argument 2 (OBJECT/MEMORY/XGROUP/XINFO subcommand key). */
    public static final int ARG2 = 11;
    /** Argument 1 plus the key after STORE/STOREDIST. */
    public static final int GEO_STORE = 12;

    /** Cross-slot calls are split by slot and the replies re-assembled. */
    public static final int NO_SPLIT = 0;
    public static final int SPLIT_LIST = 1;
    public static final int SPLIT_SUM = 2;
    public static final int SPLIT_VOID = 3;

    public static final class Spec {
        final String name;
        final byte[] upper;
        final int kind;
        final int split;

        Spec(String name, int kind, int split) {
            this.name = name;
            this.upper = name.getBytes(StandardCharsets.US_ASCII);
            this.kind = kind;
            this.split = split;
        }
    }

    private static final Spec DEFAULT = new Spec("", FIRST, NO_SPLIT);
    private static final Spec[] TABLE = new Spec[1024];
    private static final int MASK = TABLE.length - 1;

    private static void add(int kind, int split, String... names) {
        for (String name : names) {
            Spec s = new Spec(name, kind, split);
            int h = hash(s.upper, 0, s.upper.length) & MASK;
            while (TABLE[h] != null) {
                h = (h + 1) & MASK;
            }
            TABLE[h] = s;
        }
    }

    static {
        add(KEYLESS, NO_SPLIT, "ACL", "AUTH", "BGSAVE", "CHANGES", "CLIENT", "CLUSTER", "COMMAND", "CONFIG", "DBSIZE",
                "DISCARD", "ECHO", "EXEC", "FLUSHALL", "FLUSHDB", "FUNCTION", "HELLO", "INFO",
                // JSON.INDEX and JSON.FIND name an index, not a key; the server fans them out itself.
                "JSON.FIND", "JSON.INDEX", "KEYS", "LASTSAVE", "LATENCY", "MULTI", "PING", "PSUBSCRIBE", "PUBLISH",
                "PUNSUBSCRIBE", "QUIT", "RANDOMKEY", "READONLY", "READWRITE", "RESET", "RUVIO", "SAVE", "SCAN",
                "SCRIPT", "SELECT", "SLOWLOG", "SUBSCRIBE", "SWAPDB", "TIME", "UNSUBSCRIBE", "UNWATCH", "WAIT",
                "PUBSUB", "DEBUG", "MONITOR", "SHUTDOWN", "REPLICAOF", "SLAVEOF", "FAILOVER", "LOLWUT", "MODULE");
        add(ALL, NO_SPLIT, "PFCOUNT", "PFMERGE", "SDIFF", "SDIFFSTORE", "SINTER", "SINTERSTORE", "SSUBSCRIBE",
                "SUNION", "SUNIONSTORE", "SUNSUBSCRIBE", "WATCH");
        add(ALL, SPLIT_LIST, "MGET");
        add(ALL, SPLIT_SUM, "DEL", "EXISTS", "TOUCH", "UNLINK");
        add(TWO, NO_SPLIT, "BLMOVE", "BRPOPLPUSH", "COPY", "LMOVE", "RENAME", "RENAMENX", "RPOPLPUSH", "SMOVE",
                "GEOSEARCHSTORE", "LCS", "ZRANGESTORE");
        add(EVERY2, SPLIT_VOID, "MSET");
        add(EVERY2, NO_SPLIT, "MSETNX");
        add(DROP_LAST, NO_SPLIT, "BLPOP", "BRPOP", "BZPOPMIN", "BZPOPMAX", "JSON.MGET");
        add(COUNTED1, NO_SPLIT, "ZUNION", "ZINTER", "ZDIFF", "LMPOP", "SINTERCARD", "ZMPOP", "ZINTERCARD");
        add(COUNTED2, NO_SPLIT, "EVAL", "EVALSHA", "EVAL_RO", "EVALSHA_RO", "FCALL", "FCALL_RO", "BLMPOP", "BZMPOP");
        add(DEST_COUNTED2, NO_SPLIT, "ZUNIONSTORE", "ZINTERSTORE", "ZDIFFSTORE");
        add(STREAMS, NO_SPLIT, "XREAD", "XREADGROUP");
        add(FROM2, NO_SPLIT, "BITOP");
        add(ARG2, NO_SPLIT, "OBJECT", "MEMORY", "XGROUP", "XINFO");
        add(GEO_STORE, NO_SPLIT, "GEORADIUS", "GEORADIUSBYMEMBER");
    }

    private ClusterKeys() {
    }

    private static int hash(byte[] b, int off, int len) {
        int h = 0;
        for (int i = 0; i < len; i++) {
            h = h * 31 + upper(b[off + i]);
        }
        return h ^ (h >>> 15);
    }

    private static int upper(byte c) {
        return c >= 'a' && c <= 'z' ? c - 32 : c;
    }

    /** The routing rule of the command word {@code name[off, off + len)}. */
    public static Spec spec(byte[] name, int off, int len) {
        int h = hash(name, off, len) & MASK;
        for (Spec s; (s = TABLE[h]) != null; h = (h + 1) & MASK) {
            byte[] u = s.upper;
            if (u.length == len) {
                int i = 0;
                while (i < len && u[i] == upper(name[off + i])) {
                    i++;
                }
                if (i == len) {
                    return s;
                }
            }
        }
        return DEFAULT;
    }

    public static Spec spec(String name) {
        byte[] b = name.getBytes(StandardCharsets.UTF_8);
        return spec(b, 0, b.length);
    }

    /**
     * Fills {@code f.keyIdx} with the argument indexes of the command's keys, in argument order, and
     * returns how many there are (0 for keyless commands). {@code f} must be parsed.
     */
    public static int keys(Spec spec, Frame f) {
        int argc = f.argc;
        if (argc < 2) {
            return 0;
        }
        int[] k;
        switch (spec.kind) {
            case KEYLESS:
                return 0;
            case FIRST:
                k = f.keyBuffer(1);
                k[0] = 1;
                return 1;
            case ALL:
                return range(f, 1, argc);
            case TWO:
                return range(f, 1, Math.min(argc, 3));
            case EVERY2: {
                int n = argc / 2;
                k = f.keyBuffer(n);
                for (int i = 0; i < n; i++) {
                    k[i] = 1 + 2 * i;
                }
                return n;
            }
            case DROP_LAST:
                return range(f, 1, argc - 1);
            case COUNTED1:
                return counted(f, 1, 0);
            case COUNTED2:
                return counted(f, 2, 0);
            case DEST_COUNTED2:
                return counted(f, 2, 1);
            case STREAMS: {
                for (int i = 1; i < argc; i++) {
                    if (equalsIgnoreCase(f, i, "STREAMS")) {
                        int n = (argc - i - 1) / 2;
                        return range(f, i + 1, i + 1 + n);
                    }
                }
                return 0;
            }
            case FROM2:
                return range(f, 2, argc);
            case ARG2:
                if (argc > 2) {
                    k = f.keyBuffer(1);
                    k[0] = 2;
                    return 1;
                }
                return 0;
            case GEO_STORE: {
                k = f.keyBuffer(2);
                k[0] = 1;
                for (int i = 2; i < argc - 1; i++) {
                    if (equalsIgnoreCase(f, i, "STORE") || equalsIgnoreCase(f, i, "STOREDIST")) {
                        k[1] = i + 1;
                        return 2;
                    }
                }
                return 1;
            }
            default:
                return 0;
        }
    }

    private static int range(Frame f, int from, int to) {
        int n = to - from;
        if (n <= 0) {
            return 0;
        }
        int[] k = f.keyBuffer(n);
        for (int i = 0; i < n; i++) {
            k[i] = from + i;
        }
        return n;
    }

    /** {@code numkeys} at {@code countIndex}; {@code prefix} leading keys (the destination of *STORE). */
    private static int counted(Frame f, int countIndex, int prefix) {
        int argc = f.argc;
        if (countIndex >= argc) {
            return prefix > 0 ? singleFirst(f) : 0;
        }
        byte[] b = f.out.buf;
        int p = f.off[countIndex];
        int l = f.len[countIndex];
        long count = 0;
        if (l == 0 || l > 9) {
            return prefix > 0 ? singleFirst(f) : 0;
        }
        for (int i = 0; i < l; i++) {
            int d = b[p + i] - '0';
            if (d < 0 || d > 9) {
                return prefix > 0 ? singleFirst(f) : 0;
            }
            count = count * 10 + d;
        }
        int available = argc - countIndex - 1;
        int n = (int) Math.min(count, available);
        int[] k = f.keyBuffer(n + prefix);
        if (prefix > 0) {
            k[0] = 1;
        }
        for (int i = 0; i < n; i++) {
            k[prefix + i] = countIndex + 1 + i;
        }
        return n + prefix;
    }

    private static int singleFirst(Frame f) {
        f.keyBuffer(1)[0] = 1;
        return 1;
    }

    public static boolean equalsIgnoreCase(Frame f, int arg, String word) {
        int l = f.len[arg];
        if (l != word.length()) {
            return false;
        }
        byte[] b = f.out.buf;
        int p = f.off[arg];
        for (int i = 0; i < l; i++) {
            if (upper(b[p + i]) != word.charAt(i)) {
                return false;
            }
        }
        return true;
    }
}
