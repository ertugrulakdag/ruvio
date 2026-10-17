// SPDX-License-Identifier: MIT
package io.github.salihcantekin.ruvio.model;

import java.util.Objects;

/**
 * A key with a value: the reply of blocking pops ({@code BLPOP}, {@code BRPOP}), {@code HSCAN} pages and similar.
 *
 * @param <K> the key type
 * @param <V> the value type
 */
public final class KeyValue<K, V> {
    private final K key;
    private final V value;

    /**
     * Creates a pair.
     *
     * @param key the key (or hash field)
     * @param value the value
     */
    public KeyValue(K key, V value) {
        this.key = key;
        this.value = value;
    }

    /**
     * Returns the key, or for a hash scan the field name.
     *
     * @return the key
     */
    public K key() {
        return key;
    }

    /**
     * Returns the value.
     *
     * @return the value
     */
    public V value() {
        return value;
    }

    /**
     * Compares key and value; byte arrays compare by content.
     *
     * @param o the other object
     * @return {@code true} when both are a pair with equal key and value
     */
    @Override
    public boolean equals(Object o) {
        if (!(o instanceof KeyValue)) {
            return false;
        }
        KeyValue<?, ?> other = (KeyValue<?, ?>) o;
        return Objects.deepEquals(key, other.key) && Objects.deepEquals(value, other.value);
    }

    /**
     * Returns a hash of key and value.
     *
     * @return the hash code
     */
    @Override
    public int hashCode() {
        return Objects.hash(key, value);
    }

    /**
     * Returns a short readable form, for logs.
     *
     * @return the text
     */
    @Override
    public String toString() {
        return key + "=" + value;
    }
}
