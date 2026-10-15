// SPDX-License-Identifier: MIT
package io.github.salihcantekin.ruvio.internal;

import io.github.salihcantekin.ruvio.RuvioCodec;

/**
 * A request sent by the cluster router. It keeps a copy of the encoded command so that a
 * {@code -MOVED} reply can be followed once; any other failure (including a lost connection) completes
 * it as is: nothing is ever replayed after a transport failure.
 */
public class RoutedRequest<T> extends Request<T> {
    public ClusterRouter router;
    /** The command as sent, kept for the single MOVED retry. Null for parents of split commands. */
    public byte[] frame;
    public boolean retried;

    public RoutedRequest(Reply<T> reply, RuvioCodec<?> keys, RuvioCodec<?> values) {
        super(reply, keys, values);
    }

    @Override
    public void fail(Throwable error) {
        ClusterRouter r = router;
        if (r != null && frame != null && !retried && ClusterRouter.isMoved(error)) {
            retried = true;
            r.redirect(this, error);
            return;
        }
        failFinal(error);
    }

    /** Completes exceptionally without looking at redirects. */
    public void failFinal(Throwable error) {
        completeExceptionally(error);
    }
}
