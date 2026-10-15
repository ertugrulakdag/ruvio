// SPDX-License-Identifier: MIT
package io.github.salihcantekin.ruvio.model;

import java.util.Objects;

/** A key with a value: the reply of blocking pops, LMPOP, HSCAN pages and similar. */
public final class KeyValue<K, V> {
    private final K key;
    private final V value;

    public KeyValue(K key, V value) {
        this.key = key;
        this.value = value;
    }

    public K key() {
        return key;
    }

    public V value() {
        return value;
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof KeyValue)) {
            return false;
        }
        KeyValue<?, ?> other = (KeyValue<?, ?>) o;
        return Objects.deepEquals(key, other.key) && Objects.deepEquals(value, other.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(key, value);
    }

    @Override
    public String toString() {
        return key + "=" + value;
    }
}
