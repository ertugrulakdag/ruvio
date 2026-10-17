// SPDX-License-Identifier: MIT
package io.github.salihcantekin.ruvio;

/**
 * The socket failed or was closed. A command that fails with this exception may or may not have
 * run on the server. The client opens a new socket for the next command.
 */
public class RuvioConnectionException extends RuvioException {
    /**
     * Creates the exception.
     *
     * @param message the description
     */
    public RuvioConnectionException(String message) {
        super(message);
    }

    /**
     * Creates the exception with a cause.
     *
     * @param message the description
     * @param cause the underlying I/O error
     */
    public RuvioConnectionException(String message, Throwable cause) {
        super(message, cause);
    }
}
