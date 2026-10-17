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

/**
 * Lazy, cursor-driven iteration over a SCAN-family command: one page is fetched at a time.
 *
 * @param <T> the element type
 */
public final class ScanIterator<T> implements Iterator<T> {
    private final Function<String, ScanResult<T>> page;
    private String cursor = "0";
    private boolean started;
    private List<T> items = List.of();
    private int index;

    /**
     * Creates an iterator.
     *
     * @param page fetches the page for a cursor
     */
    public ScanIterator(Function<String, ScanResult<T>> page) {
        this.page = page;
    }

    /**
     * Tells whether another element exists, fetching the next page when needed.
     *
     * @return {@code true} when {@link #next()} will return an element
     */
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

    /**
     * Returns the next element.
     *
     * @return the element
     * @throws NoSuchElementException when the walk is complete
     */
    @Override
    public T next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        return items.get(index++);
    }

    /**
     * Wraps the page function as an iterable; each iterator starts a new walk.
     *
     * @param page fetches the page for a cursor
     * @param <T> the element type
     * @return the iterable
     */
    public static <T> Iterable<T> iterable(Function<String, ScanResult<T>> page) {
        return () -> new ScanIterator<>(page);
    }

    /**
     * Turns an iterable into a lazy stream.
     *
     * @param iterable the source
     * @param <T> the element type
     * @return the stream
     */
    public static <T> Stream<T> streamOf(Iterable<T> iterable) {
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(iterable.iterator(), Spliterator.ORDERED), false);
    }

    /**
     * Wraps the page function as a lazy stream.
     *
     * @param page fetches the page for a cursor
     * @param <T> the element type
     * @return the stream
     */
    public static <T> Stream<T> stream(Function<String, ScanResult<T>> page) {
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(new ScanIterator<>(page), Spliterator.ORDERED), false);
    }
}
