// SPDX-License-Identifier: MIT
package io.github.salihcantekin.ruvio.args;

import io.github.salihcantekin.ruvio.internal.Args;
import io.github.salihcantekin.ruvio.internal.RespOut;

/**
 * Trim rule of {@code XADD} and {@code XTRIM}: a strategy ({@code MAXLEN} or {@code MINID}), a mode ({@code =} exact or {@code ~} approximate) and a threshold.
 * Ruvio trims exactly in both modes, and does not accept the {@code LIMIT} option.
 *
 * <p>Example: {@code client.xtrim("events", StreamTrim.maxLen(1000))} or {@code XAddArgs.builder().trim(StreamTrim.minId("1700000000000-0", StreamTrim.Mode.APPROXIMATE))}
 */
public final class StreamTrim {
    /** What the threshold counts. */
    public enum Strategy {
        /** Keep at most this many newest entries; the threshold is a count. */
        MAXLEN,
        /** Drop entries with an id lower than the threshold; the threshold is an id such as {@code 1700000000000-0}. */
        MINID;

        private final byte[] fragment;

        Strategy() {
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

    /** How strictly the threshold is applied. Ruvio trims exactly in both modes. */
    public enum Mode {
        /** Sends {@code =}: trim exactly. */
        EXACT("="),
        /** Sends {@code ~}: accepted for compatibility with Redis; the trim is still exact. */
        APPROXIMATE("~");

        private final String word;
        private final byte[] fragment;

        Mode(String word) {
            this.word = word;
            this.fragment = RespOut.fragment(word);
        }

        /**
         * Returns the token as sent to the server.
         *
         * @return {@code "="} or {@code "~"}
         */
        public String word() {
            return word;
        }

        /**
         * Returns the token pre-encoded as a complete RESP bulk string.
         *
         * @return the shared encoded bytes; do not modify them
         */
        public byte[] fragment() {
            return fragment;
        }
    }

    private final Strategy strategy;
    private final Mode mode;
    private final long maxLen;
    private final String minId;

    private StreamTrim(Strategy strategy, Mode mode, long maxLen, String minId) {
        this.strategy = strategy;
        this.mode = mode;
        this.maxLen = maxLen;
        this.minId = minId;
    }

    /**
     * Keeps at most {@code maxLen} newest entries, exact mode. Example: {@code StreamTrim.maxLen(1000)}.
     *
     * @param maxLen the most entries to keep; zero or greater
     * @return a new trim rule
     */
    public static StreamTrim maxLen(long maxLen) {
        return maxLen(maxLen, Mode.EXACT);
    }

    /**
     * Keeps at most {@code maxLen} newest entries. Example: {@code StreamTrim.maxLen(1000, StreamTrim.Mode.APPROXIMATE)}.
     *
     * @param maxLen the most entries to keep; zero or greater
     * @param mode {@code EXACT} ({@code =}) or {@code APPROXIMATE} ({@code ~})
     * @return a new trim rule
     * @throws IllegalArgumentException when {@code maxLen} is negative
     */
    public static StreamTrim maxLen(long maxLen, Mode mode) {
        if (maxLen < 0) throw new IllegalArgumentException("maxLen must not be negative");
        return new StreamTrim(Strategy.MAXLEN, mode, maxLen, null);
    }

    /**
     * Drops entries with an id lower than {@code minId}, exact mode. Example: {@code StreamTrim.minId("1700000000000-0")}.
     *
     * @param minId the lowest id to keep, as {@code ms} or {@code ms-seq}
     * @return a new trim rule
     */
    public static StreamTrim minId(String minId) {
        return minId(minId, Mode.EXACT);
    }

    /**
     * Drops entries with an id lower than {@code minId}. Example: {@code StreamTrim.minId("1700000000000-0", StreamTrim.Mode.APPROXIMATE)}.
     *
     * @param minId the lowest id to keep, as {@code ms} or {@code ms-seq}
     * @param mode {@code EXACT} ({@code =}) or {@code APPROXIMATE} ({@code ~})
     * @return a new trim rule
     */
    public static StreamTrim minId(String minId, Mode mode) {
        return new StreamTrim(Strategy.MINID, mode, -1, minId);
    }

    /**
     * Returns the strategy.
     *
     * @return {@code MAXLEN} or {@code MINID}
     */
    public Strategy strategy() {
        return strategy;
    }

    /**
     * Returns the mode.
     *
     * @return {@code EXACT} or {@code APPROXIMATE}
     */
    public Mode mode() {
        return mode;
    }

    /**
     * Appends {@code MAXLEN|MINID =|~ threshold} to a command being encoded, using pre-encoded keyword bytes. Used by the client; you do not call it.
     *
     * @param a the argument writer
     */
    public void write(Args a) {
        a.w(strategy.fragment()).w(mode.fragment());
        if (strategy == Strategy.MAXLEN) {
            a.l(maxLen);
        } else {
            a.s(minId);
        }
    }
}
