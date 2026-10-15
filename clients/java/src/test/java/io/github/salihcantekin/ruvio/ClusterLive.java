// SPDX-License-Identifier: MIT
package io.github.salihcantekin.ruvio;

import static org.junit.jupiter.api.Assumptions.assumeTrue;

/** Live cluster tests skip unless RUVIO_CLUSTER_ADDR=host:port (any shard of a 3-shard server) is set. */
final class ClusterLive {
    static final String ADDR = System.getenv("RUVIO_CLUSTER_ADDR");

    private ClusterLive() {
    }

    static RuvioOptions options() {
        assumeTrue(ADDR != null && !ADDR.isEmpty(), "RUVIO_CLUSTER_ADDR not set; cluster live test skipped");
        RuvioOptions base = RuvioOptions.of(ADDR);
        return RuvioOptions.builder().host(base.host()).port(base.port()).cluster(true)
                .commandTimeoutMillis(10_000).build();
    }

    /** A cluster client with every node flushed. */
    static RuvioClient<String, String> client() {
        RuvioClient<String, String> c = RuvioClient.connect(options());
        for (String node : c.nodeAddresses()) {
            c.onNode(node).flushdb();
        }
        return c;
    }
}
