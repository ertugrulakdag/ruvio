// SPDX-License-Identifier: MIT
package io.github.salihcantekin.ruvio.internal;

import io.github.salihcantekin.ruvio.RuvioCodec;

/**
 * A request sent by the cluster router. It keeps a copy of the encoded command so that a
 * {@code -MOVED} reply can be followed (at most {@link ClusterRouter#MAX_REDIRECTS} times); any other failure
 * (including a lost connection) completes it as is: nothing is ever replayed after a transport failure.
 *
 * @param <T> the result type
 */
public class RoutedRequest<T> extends Request<T> {
    /** The router that sent this request. */
    public ClusterRouter router;
    /**
     * The command as sent, kept for MOVED retries: {@code frame[frameOff, frameOff + frameLen)}. Null for parents of split commands.
     */
    public byte[] frame;
    /** Start of the command in {@link #frame}. */
    public int frameOff;
    /** Length of the command in {@link #frame}. */
    public int frameLen;
    /** How many MOVED redirects this command has followed so far. */
    public byte redirects;

    /**
     * Creates a request.
     *
     * @param reply the reply decoder
     * @param keys the key codec
     * @param values the value codec
     */
    public RoutedRequest(Reply<T> reply, RuvioCodec<?> keys, RuvioCodec<?> values) {
        super(reply, keys, values);
    }

    /**
     * Remembers the encoded command for redirects.
     *
     * @param buf the array holding the command
     * @param off where the command starts
     * @param len the command length
     */
    public void setFrame(byte[] buf, int off, int len) {
        frame = buf;
        frameOff = off;
        frameLen = len;
    }

    /**
     * Follows a {@code -MOVED} reply when the redirect budget allows, otherwise completes exceptionally.
     *
     * @param error the failure
     */
    @Override
    public void fail(Throwable error) {
        ClusterRouter r = router;
        if (r != null && frame != null && redirects < ClusterRouter.MAX_REDIRECTS && ClusterRouter.isMoved(error)) {
            redirects++;
            r.redirect(this, error);
            return;
        }
        failFinal(error);
    }

    /**
     * Completes exceptionally without looking at redirects.
     *
     * @param error the failure
     */
    public void failFinal(Throwable error) {
        frame = null;
        completeExceptionally(error);
    }
}
