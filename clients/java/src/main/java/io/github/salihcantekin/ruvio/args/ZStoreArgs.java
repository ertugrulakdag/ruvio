// SPDX-License-Identifier: MIT
package io.github.salihcantekin.ruvio.args;

import io.github.salihcantekin.ruvio.internal.Args;
import io.github.salihcantekin.ruvio.internal.RespOut;

/**
 * Options of {@code ZUNION}, {@code ZINTER} and their {@code STORE} forms: per-set {@code WEIGHTS} and the {@code AGGREGATE} rule.
 *
 * <p>{@code ZStoreArgs.weights(2, 1).withAggregate(ZStoreArgs.Aggregate.MAX)}
 */
public final class ZStoreArgs {
    /** Creates an empty option set; the static factories are the usual way in. */
    public ZStoreArgs() {
    }

    /** How the scores of a member found in several input sets are combined. */
    public enum Aggregate {
        /** Add the (weighted) scores; the default. */
        SUM,
        /** Take the lowest. */
        MIN,
        /** Take the highest. */
        MAX;

        private final byte[] fragment;

        Aggregate() {
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

    private double[] weights;
    private Aggregate aggregate;

    /**
     * Starts with {@code WEIGHTS}.
     *
     * @param weights one multiplier per input set, in the same order as the keys; each score is multiplied before it is combined
     * @return a new option set
     */
    public static ZStoreArgs weights(double... weights) {
        ZStoreArgs a = new ZStoreArgs();
        a.weights = weights.clone();
        return a;
    }

    /**
     * Starts with {@code AGGREGATE}.
     *
     * @param aggregate how scores of a member found in several sets are combined
     * @return a new option set
     */
    public static ZStoreArgs aggregate(Aggregate aggregate) {
        return new ZStoreArgs().withAggregate(aggregate);
    }

    /**
     * Sets {@code WEIGHTS}.
     *
     * @param weights one multiplier per input set, in the same order as the keys
     * @return this builder
     */
    public ZStoreArgs withWeights(double... weights) {
        this.weights = weights.clone();
        return this;
    }

    /**
     * Sets {@code AGGREGATE}.
     *
     * @param aggregate how scores of a member found in several sets are combined (default {@code SUM})
     * @return this builder
     */
    public ZStoreArgs withAggregate(Aggregate aggregate) {
        this.aggregate = aggregate;
        return this;
    }

    /**
     * Appends these options to a command being encoded. Used by the client; you do not call it.
     *
     * @param a the argument writer
     */
    public void write(Args a) {
        if (weights != null) {
            a.s("WEIGHTS");
            for (double w : weights) {
                a.d(w);
            }
        }
        if (aggregate != null) {
            a.s("AGGREGATE").w(aggregate.fragment());
        }
    }
}
