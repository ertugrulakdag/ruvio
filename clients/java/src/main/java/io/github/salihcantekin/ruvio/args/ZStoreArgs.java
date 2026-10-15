// SPDX-License-Identifier: MIT
package io.github.salihcantekin.ruvio.args;

import io.github.salihcantekin.ruvio.internal.Args;

/** WEIGHTS and AGGREGATE of ZUNION / ZINTER (and their STORE forms). */
public final class ZStoreArgs {
    public enum Aggregate { SUM, MIN, MAX }

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
            a.s("AGGREGATE").s(aggregate.name());
        }
    }
}
