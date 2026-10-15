// SPDX-License-Identifier: MIT
package io.github.salihcantekin.ruvio.model;

/** One note of the CHANGES feed: its sequence (use it as the next cursor) and its text. */
public final class ChangeNote {
    private final long sequence;
    private final String text;

    public ChangeNote(long sequence, String text) {
        this.sequence = sequence;
        this.text = text;
    }

    public long sequence() {
        return sequence;
    }

    public String text() {
        return text;
    }

    @Override
    public String toString() {
        return sequence + ":" + text;
    }
}
