// SPDX-License-Identifier: MIT
package io.github.salihcantekin.ruvio.internal;

/** One cluster node: its address, the lazily opened shared connection and its transaction pool. */
public final class ClusterNode {
    public final String host;
    public final int port;
    public final String address;
    public final Link link;
    public final TxPool txPool;

    public ClusterNode(String host, int port, Link link) {
        this.host = host;
        this.port = port;
        this.address = address(host, port);
        this.link = link;
        this.txPool = new TxPool(link);
    }

    public static String address(String host, int port) {
        return host + ":" + port;
    }

    public void close() {
        txPool.close();
        link.close();
    }

    @Override
    public String toString() {
        return address;
    }
}
