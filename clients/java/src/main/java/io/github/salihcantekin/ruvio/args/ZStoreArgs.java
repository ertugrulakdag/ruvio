// SPDX-License-Identifier: MIT
package io.github.salihcantekin.ruvio.args;

import io.github.salihcantekin.ruvio.internal.Args;
import io.github.salihcantekin.ruvio.internal.RespOut;

/** WEIGHTS and AGGREGATE of ZUNION / ZINTER (and their STORE forms). */
public final class ZStoreArgs {
    /** AGGREGATE mode: SUM, MIN or MAX. */
    public enum Aggregate {
        SUM, MIN, MAX;

        private final byte[] fragment;

        Aggregate() {
            this.fragment = RespOut.fragment(name());
        }

        /**
         * The keyword pre-encoded as a complete RESP bulk string, identical on the wire to {@code name()}.
         * The array is shared and must not be modified.
         */
        public byte[] fragment() {
            return fragment;
        }
    }

    private double[] weights;
    private Aggregate aggregate;

    public static ZStoreArgs weights(double... weights) {
        ZStoreArgs a = new ZStoreArgs();
        a.weights = weights.clone();
        return a;
    }

    public static ZStoreArgs aggregate(Aggregate aggregate) {
        return new ZStoreArgs().withAggregate(aggregate);
    }

    public ZStoreArgs withWeights(double... weights) {
        this.weights = weights.clone();
        return this;
    }

    public ZStoreArgs withAggregate(Aggregate aggregate) {
        this.aggregate = aggregate;
        return this;
    }

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
