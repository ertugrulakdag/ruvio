// SPDX-License-Identifier: MIT
package io.github.salihcantekin.ruvio;

import io.github.salihcantekin.ruvio.internal.Connection;

import java.util.function.Consumer;

/**
 * Connection settings. Immutable once built; create them with {@link #builder()} or {@link #of(String)}.
 *
 * <pre>{@code
 * RuvioOptions options = RuvioOptions.builder()
 *         .host("db.example.com").port(6379)
 *         .password("secret")
 *         .commandTimeoutMillis(2_000)
 *         .build();
 * }</pre>
 */
public final class RuvioOptions {
    /** Server host name or address (default {@code 127.0.0.1}). */
    public final String host;
    /** Server TCP port (default 6379). */
    public final int port;
    /** ACL user name sent with {@code AUTH} on every (re)connect, or {@code null} to authenticate as the default user. */
    public final String username;
    /** Password sent with {@code AUTH} on every (re)connect, or {@code null} for no authentication. */
    public final String password;
    /** Logical database selected on every (re)connect (default 0). */
    public final int database;
    /** Connection name set with {@code CLIENT SETNAME} on every (re)connect, or {@code null}. */
    public final String clientName;
    /** Timeout for opening the socket, in milliseconds (default 5000). */
    public final int connectTimeoutMillis;
    /** How long blocking calls wait for a reply, in milliseconds; 0 (default) waits forever. */
    public final long commandTimeoutMillis;
    /** Maximum replies outstanding on one connection (default 16384). */
    public final int maxInFlight;
    /** Receiver of RESP3 push frames, or {@code null}. */
    public final Consumer<Object> pushListener;
    /** Whether cluster routing was requested. */
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

    /**
     * Starts a builder with the defaults: host {@code 127.0.0.1}, port 6379, no authentication,
     * database 0, 5 s connect timeout, no command timeout, 16384 outstanding replies, standalone.
     *
     * @return a new builder
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Parses {@code host:port}; with no colon the whole text is the host and the port stays 6379.
     * All other settings are the defaults.
     *
     * @param hostPort for example {@code "localhost:6380"}
     * @return the options
     * @throws NumberFormatException when the port is not a number
     */
    public static RuvioOptions of(String hostPort) {
        int colon = hostPort.lastIndexOf(':');
        if (colon < 0) {
            return builder().host(hostPort).build();
        }
        return builder().host(hostPort.substring(0, colon)).port(Integer.parseInt(hostPort.substring(colon + 1))).build();
    }

    /**
     * Returns the host.
     *
     * @return the server host
     */
    public String host() {
        return host;
    }

    /**
     * Returns the port.
     *
     * @return the server port
     */
    public int port() {
        return port;
    }

    /**
     * Returns the database index.
     *
     * @return the logical database selected on connect
     */
    public int database() {
        return database;
    }

    /**
     * Tells whether cluster routing was requested. A standalone server still makes the client fall back
     * to plain standalone behaviour.
     *
     * @return {@code true} when cluster routing was requested
     */
    public boolean cluster() {
        return cluster;
    }

    /**
     * Copies these settings into a new builder, to change a few of them.
     *
     * @return a builder holding the same values
     */
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

    /** Builder for {@link RuvioOptions}. Not thread safe. */
    public static final class Builder {
        /** Creates a builder with the defaults; prefer {@link RuvioOptions#builder()}. */
        public Builder() {
        }

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

        /**
         * Sets the server host.
         *
         * @param host host name or address (default {@code 127.0.0.1})
         * @return this builder
         */
        public Builder host(String host) {
            this.host = host;
            return this;
        }

        /**
         * Sets the server port.
         *
         * @param port TCP port (default 6379)
         * @return this builder
         */
        public Builder port(int port) {
            this.port = port;
            return this;
        }

        /**
         * Sets the password sent with {@code AUTH password} on every (re)connect.
         *
         * @param password the password
         * @return this builder
         */
        public Builder password(String password) {
            this.password = password;
            return this;
        }

        /**
         * Sets the ACL user and password sent with {@code AUTH username password} on every (re)connect.
         *
         * @param username the ACL user name
         * @param password the password
         * @return this builder
         */
        public Builder credentials(String username, String password) {
            this.username = username;
            this.password = password;
            return this;
        }

        /**
         * Sets the logical database selected with {@code SELECT} on every (re)connect. Ignored in cluster mode.
         *
         * @param database the index, from 0 to {@code databases - 1} (default 0)
         * @return this builder
         */
        public Builder database(int database) {
            this.database = database;
            return this;
        }

        /**
         * Sets the connection name sent with {@code CLIENT SETNAME} on every (re)connect.
         *
         * @param name a name without spaces
         * @return this builder
         */
        public Builder clientName(String name) {
            this.clientName = name;
            return this;
        }

        /**
         * Sets the timeout for opening the socket.
         *
         * @param millis milliseconds (default 5000)
         * @return this builder
         */
        public Builder connectTimeoutMillis(int millis) {
            this.connectTimeoutMillis = millis;
            return this;
        }

        /**
         * How long blocking (synchronous) calls wait for a reply; 0 (default) waits forever. The
         * command is not cancelled on the server when the wait times out; the call throws a
         * {@link RuvioException}.
         *
         * @param millis the wait in milliseconds; 0 (default) waits forever
         * @return this builder
         */
        public Builder commandTimeoutMillis(long millis) {
            this.commandTimeoutMillis = millis;
            return this;
        }

        /**
         * Sets the maximum replies outstanding on one connection. More commands wait until replies arrive.
         *
         * @param count the limit, rounded up to a power of two (default 16384). A pipeline with more
         *        commands than this fails with a {@link RuvioException}
         * @return this builder
         */
        public Builder maxInFlight(int count) {
            this.maxInFlight = count;
            return this;
        }

        /**
         * Receives RESP3 push frames (for example CLIENT TRACKING invalidations) on the reader
         * thread. The frame is a List whose first element is the kind as a byte[] or String. It must not
         * block.
         *
         * @param listener the receiver
         * @return this builder
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
         *
         * @param enabled {@code true} to route by key
         * @return this builder
         */
        public Builder cluster(boolean enabled) {
            this.cluster = enabled;
            return this;
        }

        /**
         * Builds the immutable options.
         *
         * @return the options
         */
        public RuvioOptions build() {
            return new RuvioOptions(this);
        }
    }
}
