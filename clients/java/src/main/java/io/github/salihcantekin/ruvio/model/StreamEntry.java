// SPDX-License-Identifier: MIT
package io.github.salihcantekin.ruvio.model;

import java.util.Map;

/** One stream entry: its id and its field/value pairs in stored order. */
public final class StreamEntry<K, V> {
    private final String id;
    private final Map<K, V> fields;

    public StreamEntry(String id, Map<K, V> fields) {
        this.id = id;
        this.fields = fields;
    }

    public String id() {
        return id;
    }

    public Map<K, V> fields() {
        return fields;
    }

    @Override
    public String toString() {
        return id + " " + fields;
    }
}
