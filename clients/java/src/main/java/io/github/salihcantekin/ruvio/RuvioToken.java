// SPDX-License-Identifier: MIT
package io.github.salihcantekin.ruvio;

import io.github.salihcantekin.ruvio.internal.Link;

/**
 * A held LEASE (fencing token) or SEMAPHORE slot, released with {@code RELEASE key token}. Pass
 * {@link #token()} to the protected resource as the fencing token. Use with try-with-resources.
 */
public final class RuvioToken<K> implements AutoCloseable {
    private final RuvioAsync<K, ?> commands;
    private final K key;
    private final long token;
    private final long timeout;
    private volatile boolean released;

    RuvioToken(RuvioAsync<K, ?> commands, K key, long token, long timeout) {
        this.commands = commands;
        this.key = key;
        this.token = token;
        this.timeout = timeout;
    }

    public K key() {
        return key;
    }

    public long token() {
        return token;
    }

    /** Releases the lease or slot. Returns false when the token no longer held it. */
    public boolean release() {
        if (released) {
            return false;
        }
        released = true;
        return Link.await(commands.release(key, token), timeout);
    }

    @Override
    public void close() {
        release();
    }
}
