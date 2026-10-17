// SPDX-License-Identifier: MIT
package io.github.salihcantekin.ruvio.model;

import java.util.Map;

/**
 * One stream entry: its id and its field/value pairs in stored order.
 *
 * @param <K> the field name type
 * @param <V> the field value type
 */
public final class StreamEntry<K, V> {
    private final String id;
    private final Map<K, V> fields;

    /**
     * Creates an entry.
     *
     * @param id the entry id
     * @param fields the field/value pairs
     */
    public StreamEntry(String id, Map<K, V> fields) {
        this.id = id;
        this.fields = fields;
    }

    /**
     * Returns the entry id.
     *
     * @return the id such as {@code 1700000000000-0}
     */
    public String id() {
        return id;
    }

    /**
     * Returns the field/value pairs in stored order.
     *
     * @return the fields
     */
    public Map<K, V> fields() {
        return fields;
    }

    /**
     * Returns a short readable form, for logs.
     *
     * @return the text
     */
    @Override
    public String toString() {
        return id + " " + fields;
    }
}
