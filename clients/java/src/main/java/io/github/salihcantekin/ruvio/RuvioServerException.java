// SPDX-License-Identifier: MIT
package io.github.salihcantekin.ruvio;

/** A {@code -ERR ...} style reply from the server. {@link #code()} is the first word (ERR, WRONGTYPE, ...). */
public class RuvioServerException extends RuvioException {
    private final String code;

    public RuvioServerException(String message) {
        super(message);
        int space = message.indexOf(' ');
        this.code = space < 0 ? message : message.substring(0, space);
    }

    /** The error prefix, for example {@code WRONGTYPE}, {@code NOSCRIPT}, {@code BUSY} or {@code ERR}. */
    public String code() {
        return code;
    }
}
