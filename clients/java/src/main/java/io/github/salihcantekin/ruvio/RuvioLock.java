// SPDX-License-Identifier: MIT
package io.github.salihcantekin.ruvio;

import io.github.salihcantekin.ruvio.args.SetOptions;
import io.github.salihcantekin.ruvio.internal.Link;

/**
 * A distributed lock acquired with {@code SET key token NX PX ttl}. {@link #renew(long)} extends it
 * with {@code SET key token IFEQ token PX ttl} and {@link #close()} releases it with
 * {@code DELEX key IFEQ token}, so a lock that expired and was taken by someone else is never
 * deleted. Use with try-with-resources.
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

    public K key() {
        return key;
    }

    /** The random owner token stored as the lock value. */
    public String token() {
        return token;
    }

    /** Extends the TTL if this lock is still held. Returns false when it was lost. */
    public boolean renew(long ttlMillis) {
        return Link.await(commands.setIfEq(key, token, token, SetOptions.px(ttlMillis)), timeout);
    }

    /** True when the lock key still holds this token. */
    public boolean isHeld() {
        return token.equals(Link.await(commands.get(key), timeout));
    }

    /** Releases the lock if this token still holds it. Returns false when it had been lost. */
    public boolean release() {
        if (released) {
            return false;
        }
        released = true;
        return Link.await(commands.delex(key, token), timeout);
    }

    @Override
    public void close() {
        release();
    }
}
