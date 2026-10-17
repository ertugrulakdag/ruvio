// SPDX-License-Identifier: MIT
package io.github.salihcantekin.ruvio;

/**
 * An error reply from the server (a {@code -ERR ...} style line). {@link #code()} is the first word (ERR,
 * WRONGTYPE, ...). The message is the full text.
 */
public class RuvioServerException extends RuvioException {
    /** The first word of the error text. */
    private final String code;

    /**
     * Creates the exception from the server's error text; the code is its first word.
     *
     * @param message the full error text without the leading {@code -}, for example {@code WRONGTYPE Operation against ...}
     */
    public RuvioServerException(String message) {
        super(message);
        int space = message.indexOf(' ');
        this.code = space < 0 ? message : message.substring(0, space);
    }

    /**
     * Returns the error prefix.
     *
     * @return the first word, for example {@code WRONGTYPE}, {@code NOSCRIPT}, {@code BUSY} or {@code ERR}
     */
    public String code() {
        return code;
    }
}
