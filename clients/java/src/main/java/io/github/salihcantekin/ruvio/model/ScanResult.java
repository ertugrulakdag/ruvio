// SPDX-License-Identifier: MIT
package io.github.salihcantekin.ruvio.model;

import java.util.List;

/**
 * One page of a SCAN-family walk ({@code SCAN}, {@code HSCAN}, {@code SSCAN}, {@code ZSCAN}). A cursor of
 * {@code "0"} means the walk is complete.
 *
 * @param <T> the element type
 */
public final class ScanResult<T> {
    private final String cursor;
    private final List<T> items;

    /**
     * Creates a page.
     *
     * @param cursor the cursor for the next call
     * @param items the elements of this page
     */
    public ScanResult(String cursor, List<T> items) {
        this.cursor = cursor;
        this.items = items;
    }

    /**
     * Returns the cursor to pass to the next call.
     *
     * @return the cursor; {@code "0"} when the walk is complete
     */
    public String cursor() {
        return cursor;
    }

    /**
     * Returns the elements of this page. A page may be empty or repeat elements seen earlier.
     *
     * @return the elements
     */
    public List<T> items() {
        return items;
    }

    /**
     * Tells whether the walk is complete.
     *
     * @return {@code true} when the cursor is {@code "0"}
     */
    public boolean isFinished() {
        return "0".equals(cursor);
    }

    /**
     * Returns a short readable form, for logs.
     *
     * @return the text
     */
    @Override
    public String toString() {
        return "ScanResult{cursor=" + cursor + ", items=" + items + "}";
    }
}
