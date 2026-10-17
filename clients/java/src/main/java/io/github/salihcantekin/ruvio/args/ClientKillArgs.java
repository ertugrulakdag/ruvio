// SPDX-License-Identifier: MIT
package io.github.salihcantekin.ruvio.args;

import io.github.salihcantekin.ruvio.internal.Args;

/**
 * Filters of {@code CLIENT KILL}: connection id or address, and whether to spare the caller's own connection.
 *
 * <p>{@code ClientKillArgs.id(42).skipMe(false)}
 */
public final class ClientKillArgs {
    /** Creates an empty option set; the static factories are the usual way in. */
    public ClientKillArgs() {
    }

    private long id = -1;
    private String addr;
    private Boolean skipMe;

    /**
     * Matches the connection with this id (see {@code CLIENT ID} / {@code CLIENT LIST}).
     *
     * @param id the connection id
     * @return a new filter
     */
    public static ClientKillArgs id(long id) {
        ClientKillArgs a = new ClientKillArgs();
        a.id = id;
        return a;
    }

    /**
     * Matches the connection with this remote address.
     *
     * @param hostPort the address as {@code ip:port}
     * @return a new filter
     */
    public static ClientKillArgs addr(String hostPort) {
        ClientKillArgs a = new ClientKillArgs();
        a.addr = hostPort;
        return a;
    }

    /**
     * Sets {@code SKIPME}: whether the caller's own connection is spared (the server default is to spare it).
     *
     * @param skip {@code true} to spare the caller, {@code false} to allow closing it
     * @return this builder
     */
    public ClientKillArgs skipMe(boolean skip) {
        skipMe = skip;
        return this;
    }

    /**
     * Appends these options to a command being encoded. Used by the client; you do not call it.
     *
     * @param a the argument writer
     */
    public void write(Args a) {
        if (id >= 0) a.s("ID").l(id);
        if (addr != null) a.s("ADDR").s(addr);
        if (skipMe != null) a.s("SKIPME").s(skipMe ? "yes" : "no");
    }
}
