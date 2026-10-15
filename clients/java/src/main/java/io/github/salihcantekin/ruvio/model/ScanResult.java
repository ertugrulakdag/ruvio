// SPDX-License-Identifier: MIT
package io.github.salihcantekin.ruvio.model;

import java.util.List;

/** One page of a SCAN-family cursor. A cursor of {@code "0"} means the iteration is complete. */
public final class ScanResult<T> {
    private final String cursor;
    private final List<T> items;

    public ScanResult(String cursor, List<T> items) {
        this.cursor = cursor;
        this.items = items;
    }

    public String cursor() {
        return cursor;
    }

    public List<T> items() {
        return items;
    }

    public boolean isFinished() {
        return "0".equals(cursor);
    }

    @Override
    public String toString() {
        return "ScanResult{cursor=" + cursor + ", items=" + items + "}";
    }
}
