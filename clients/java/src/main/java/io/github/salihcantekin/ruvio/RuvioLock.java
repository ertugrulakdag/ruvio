// SPDX-License-Identifier: MIT
package io.github.salihcantekin.ruvio;

import io.github.salihcantekin.ruvio.args.SetOptions;
import io.github.salihcantekin.ruvio.internal.Link;

/**
 * A distributed lock acquired with {@code SET key token NX PX ttl}. {@link #renew(long)} extends it
 * with {@code SET key token IFEQ token PX ttl} and {@link #close()} releases it with
 * {@code DELEX key IFEQ token}, so a lock that expired and was taken by someone else is never
 * deleted. Use with try-with-resources. Get one from {@link RuvioClient#tryLock(Object, long)} or
 * {@link RuvioClient#lock(Object, long, long)}.
 *
 * <pre>{@code
 * try (RuvioLock<String> lock = client.lock("report", 30_000, 5_000)) {
 *     if (lock != null) {
 *         // exclusive work
 *     }
 * }
 * }</pre>
 *
 * @param <K> the key type
 */
public final class RuvioLock<K> implements AutoCloseable {
    private final RuvioAsync<K, String> commands;
    private final K key;
    private final String token;
    private final long timeout;
    private volatile boolean released;

    RuvioLock(RuvioAsync<K, String> commands, K key, String token, long timeout) {
        this.commands = commands;
        this.key = key;
        this.token = token;
        this.timeout = timeout;
    }

    /**
     * Returns the lock key.
     *
     * @return the key
     */
    public K key() {
        return key;
    }

    /**
     * Returns the random owner token stored as the lock value.
     *
     * @return the 32-character hexadecimal token
     */
    public String token() {
        return token;
    }

    /**
     * Extends the TTL with {@code SET key token IFEQ token PX ttl} if this lock is still held.
     *
     * @param ttlMillis the new time to live in milliseconds, counted from now (above 0)
     * @return {@code true} when extended, {@code false} when the lock expired or someone else holds it
     * @throws RuvioException when there is no reply within {@code commandTimeoutMillis}
     */
    public boolean renew(long ttlMillis) {
        return Link.await(commands.setIfEq(key, token, token, SetOptions.px(ttlMillis)), timeout);
    }

    /**
     * Checks with {@code GET} whether the lock key still holds this token.
     *
     * @return {@code true} while this lock is held
     */
    public boolean isHeld() {
        return token.equals(Link.await(commands.get(key), timeout));
    }

    /**
     * Releases the lock with {@code DELEX key IFEQ token}; only the owner's token deletes it. Calling it
     * again does nothing.
     *
     * @return {@code true} when this call deleted the lock, {@code false} when it had expired, was taken
     *         by someone else or was already released
     */
    public boolean release() {
        if (released) {
            return false;
        }
        released = true;
        return Link.await(commands.delex(key, token), timeout);
    }

    /** Releases the lock, same as {@link #release()}. */
    @Override
    public void close() {
        release();
    }
}
