// SPDX-License-Identifier: MIT
package io.github.salihcantekin.ruvio;

import io.github.salihcantekin.ruvio.internal.Connection;

import java.util.function.Consumer;

/** Connection settings. Immutable once built; use {@link #builder()}. */
public final class RuvioOptions {
    public final String host;
    public final int port;
    public final String username;
    public final String password;
    public final int database;
    public final String clientName;
    public final int connectTimeoutMillis;
    public final long commandTimeoutMillis;
    public final int maxInFlight;
    public final Consumer<Object> pushListener;
    public final boolean cluster;

    private RuvioOptions(Builder b) {
        this.host = b.host;
        this.port = b.port;
        this.username = b.username;
        this.password = b.password;
        this.database = b.database;
        this.clientName = b.clientName;
        this.connectTimeoutMillis = b.connectTimeoutMillis;
        this.commandTimeoutMillis = b.commandTimeoutMillis;
        this.maxInFlight = b.maxInFlight;
        this.pushListener = b.pushListener;
        this.cluster = b.cluster;
    }

    public static Builder builder() {
        return new Builder();
    }

    /** Parses {@code host:port}. */
    public static RuvioOptions of(String hostPort) {
        int colon = hostPort.lastIndexOf(':');
        if (colon < 0) {
            return builder().host(hostPort).build();
        }
        return builder().host(hostPort.substring(0, colon)).port(Integer.parseInt(hostPort.substring(colon + 1))).build();
    }

    public String host() {
        return host;
    }

    public int port() {
        return port;
    }

    public int database() {
        return database;
    }

    /** Whether cluster routing was requested (it still falls back to standalone on a standalone server). */
    public boolean cluster() {
        return cluster;
    }

    public Builder toBuilder() {
        Builder b = new Builder();
        b.host = host;
        b.port = port;
        b.username = username;
        b.password = password;
        b.database = database;
        b.clientName = clientName;
        b.connectTimeoutMillis = connectTimeoutMillis;
        b.commandTimeoutMillis = commandTimeoutMillis;
        b.maxInFlight = maxInFlight;
        b.pushListener = pushListener;
        b.cluster = cluster;
        return b;
    }

    public static final class Builder {
        private String host = "127.0.0.1";
        private int port = 6379;
        private String username;
        private String password;
        private int database;
        private String clientName;
        private int connectTimeoutMillis = 5_000;
        private long commandTimeoutMillis;
        private int maxInFlight = 16_384;
        private Consumer<Object> pushListener;
        private boolean cluster;

        public Builder host(String host) {
            this.host = host;
            return this;
        }

        public Builder port(int port) {
            this.port = port;
            return this;
        }

        /** AUTH password sent on every (re)connect. */
        public Builder password(String password) {
            this.password = password;
            return this;
        }

        /** AUTH username password sent on every (re)connect. */
        public Builder credentials(String username, String password) {
            this.username = username;
            this.password = password;
            return this;
        }

        /** Logical database selected on every (re)connect. */
        public Builder database(int database) {
            this.database = database;
            return this;
        }

        /** CLIENT SETNAME sent on every (re)connect. */
        public Builder clientName(String name) {
            this.clientName = name;
            return this;
        }

        public Builder connectTimeoutMillis(int millis) {
            this.connectTimeoutMillis = millis;
            return this;
        }

        /**
         * How long blocking (synchronous) calls wait for a reply; 0 (default) waits forever. The
         * command is not cancelled on the server when the wait times out.
         */
        public Builder commandTimeoutMillis(long millis) {
            this.commandTimeoutMillis = millis;
            return this;
        }

        /** Maximum replies outstanding on one connection (rounded up to a power of two). */
        public Builder maxInFlight(int count) {
            this.maxInFlight = count;
            return this;
        }

        /**
         * Receives RESP3 push frames (for example CLIENT TRACKING invalidations) on the reader
         * thread. The frame is a List whose first element is the kind as a byte[] or String.
         */
        public Builder pushListener(Consumer<Object> listener) {
            this.pushListener = listener;
            return this;
        }

        /**
         * Routes every command to the node that owns its key's slot. On connect the seed
         * ({@link #host}/{@link #port}) is asked for {@code CLUSTER SLOTS}; a server that answers with an
         * error (standalone, "cluster mode is disabled") makes the client behave exactly like a plain
         * standalone client, so this can be switched on unconditionally. The server only has database 0
         * in cluster mode, so {@link #database} is ignored while routing is active.
         */
        public Builder cluster(boolean enabled) {
            this.cluster = enabled;
            return this;
        }

        public RuvioOptions build() {
            return new RuvioOptions(this);
        }
    }
}
