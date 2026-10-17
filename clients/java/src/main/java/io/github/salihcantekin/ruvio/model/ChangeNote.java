// SPDX-License-Identifier: MIT
package io.github.salihcantekin.ruvio.model;

/** One note of the {@code CHANGES} feed: its sequence (use it as the next cursor) and its text. */
public final class ChangeNote {
    private final long sequence;
    private final String text;

    /**
     * Creates a note.
     *
     * @param sequence the note's sequence number
     * @param text the note text
     */
    public ChangeNote(long sequence, String text) {
        this.sequence = sequence;
        this.text = text;
    }

    /**
     * Returns the sequence number; pass it as the next cursor to read only newer notes.
     *
     * @return the sequence
     */
    public long sequence() {
        return sequence;
    }

    /**
     * Returns the note text.
     *
     * @return the text
     */
    public String text() {
        return text;
    }

    /**
     * Returns a short readable form, for logs.
     *
     * @return the text
     */
    @Override
    public String toString() {
        return sequence + ":" + text;
    }
}
