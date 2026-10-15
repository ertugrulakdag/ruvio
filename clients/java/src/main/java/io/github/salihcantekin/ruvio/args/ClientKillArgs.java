// SPDX-License-Identifier: MIT
package io.github.salihcantekin.ruvio.args;

import io.github.salihcantekin.ruvio.internal.Args;

/** ID / ADDR / SKIPME filters of CLIENT KILL. */
public final class ClientKillArgs {
    private long id = -1;
    private String addr;
    private Boolean skipMe;

    public static ClientKillArgs id(long id) {
        ClientKillArgs a = new ClientKillArgs();
        a.id = id;
        return a;
    }

    public static ClientKillArgs addr(String hostPort) {
        ClientKillArgs a = new ClientKillArgs();
        a.addr = hostPort;
        return a;
    }

    public ClientKillArgs skipMe(boolean skip) {
        skipMe = skip;
        return this;
    }

    public void write(Args a) {
        if (id >= 0) a.s("ID").l(id);
        if (addr != null) a.s("ADDR").s(addr);
        if (skipMe != null) a.s("SKIPME").s(skipMe ? "yes" : "no");
    }
}
