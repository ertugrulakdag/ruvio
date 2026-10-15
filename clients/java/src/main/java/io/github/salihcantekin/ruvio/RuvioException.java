// SPDX-License-Identifier: MIT
package io.github.salihcantekin.ruvio;

/** Base class of every exception thrown by this client. Unchecked. */
public class RuvioException extends RuntimeException {
    public RuvioException(String message) {
        super(message);
    }

    public RuvioException(String message, Throwable cause) {
        super(message, cause);
    }
}
