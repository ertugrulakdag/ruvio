// SPDX-License-Identifier: MIT
package io.github.salihcantekin.ruvio.internal;

import io.github.salihcantekin.ruvio.model.ScanResult;

import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.Function;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/** Lazy, cursor-driven iteration over a SCAN-family command: one page is fetched at a time. */
public final class ScanIterator<T> implements Iterator<T> {
    private final Function<String, ScanResult<T>> page;
    private String cursor = "0";
    private boolean started;
    private List<T> items = List.of();
    private int index;

    public ScanIterator(Function<String, ScanResult<T>> page) {
        this.page = page;
    }

    @Override
    public boolean hasNext() {
        while (index >= items.size()) {
            if (started && "0".equals(cursor)) {
                return false;
            }
            ScanResult<T> r = page.apply(cursor);
            started = true;
            cursor = r.cursor();
            items = r.items();
            index = 0;
        }
        return true;
    }

    @Override
    public T next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        return items.get(index++);
    }

    public static <T> Iterable<T> iterable(Function<String, ScanResult<T>> page) {
        return () -> new ScanIterator<>(page);
    }

    public static <T> Stream<T> streamOf(Iterable<T> iterable) {
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(iterable.iterator(), Spliterator.ORDERED), false);
    }

    public static <T> Stream<T> stream(Function<String, ScanResult<T>> page) {
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(new ScanIterator<>(page), Spliterator.ORDERED), false);
    }
}
