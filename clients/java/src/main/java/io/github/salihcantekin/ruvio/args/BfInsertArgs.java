// SPDX-License-Identifier: MIT
package io.github.salihcantekin.ruvio.args;

import io.github.salihcantekin.ruvio.internal.Args;

/** CAPACITY / ERROR / EXPANSION / NOCREATE / NONSCALING of BF.INSERT (creation options apply to a new key). */
public final class BfInsertArgs {
    private long capacity = -1;
    private double error = -1;
    private long expansion = -1;
    private boolean noCreate;
    private boolean nonScaling;

    public static BfInsertArgs builder() {
        return new BfInsertArgs();
    }

    public BfInsertArgs capacity(long capacity) {
        this.capacity = capacity;
        return this;
    }

    public BfInsertArgs error(double rate) {
        this.error = rate;
        return this;
    }

    public BfInsertArgs expansion(long expansion) {
        this.expansion = expansion;
        return this;
    }

    public BfInsertArgs noCreate() {
        noCreate = true;
        return this;
    }

    public BfInsertArgs nonScaling() {
        nonScaling = true;
        return this;
    }

    public void write(Args a) {
        if (capacity >= 0) a.s("CAPACITY").l(capacity);
        if (error >= 0) a.s("ERROR").d(error);
        if (expansion >= 0) a.s("EXPANSION").l(expansion);
        if (noCreate) a.s("NOCREATE");
        if (nonScaling) a.s("NONSCALING");
    }
}
