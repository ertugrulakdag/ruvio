// SPDX-License-Identifier: MIT
package io.github.salihcantekin.ruvio;

/**
 * The socket failed or was closed. A command that fails with this exception may or may not have
 * run on the server. The client opens a new socket for the next command.
 */
public class RuvioConnectionException extends RuvioException {
    public RuvioConnectionException(String message) {
        super(message);
    }

    public RuvioConnectionException(String message, Throwable cause) {
        super(message, cause);
    }
}
