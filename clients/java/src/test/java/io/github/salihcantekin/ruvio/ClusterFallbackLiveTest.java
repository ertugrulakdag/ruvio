// SPDX-License-Identifier: MIT
package io.github.salihcantekin.ruvio;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import org.junit.jupiter.api.Test;

/** cluster(true) against a plain (standalone) server: the client silently behaves like a standalone client. */
class ClusterFallbackLiveTest {
    @Test
    void clusterOptionFallsBackOnAStandaloneServer() {
        RuvioOptions plain = Live.options(Live.DB);
        RuvioOptions asked = plain.toBuilder().cluster(true).build();
        try (RuvioClient<String, String> c = RuvioClient.connect(asked);
                RuvioClient<String, String> reference = RuvioClient.connect(plain)) {
            assertFalse(c.isCluster(), "standalone server: no routing");
            assertTrue(c.nodeAddresses().isEmpty());
            assertEquals(c, c.onNode("127.0.0.1:1"), "onNode is a no-op without a cluster");
            c.flushdb();
            c.set("fb:a", "1");
            c.set("fb:b", "2");
            // Same logical database as the reference client: the caller's database was honoured.
            assertEquals("1", reference.get("fb:a"));
            assertEquals(Arrays.asList("1", "2", null), c.mget("fb:a", "fb:b", "fb:none"));
            // Cross-slot multi-key commands work as before (no client-side routing at all).
            c.rename("fb:a", "fb:z");
            assertEquals("1", c.get("fb:z"));
            assertNull(c.get("fb:a"));
            try (Transaction<String, String> tx = c.multi()) {
                tx.set("{fb}t1", "x");
                tx.set("{fb}t2", "y");
                assertTrue(tx.exec());
            }
            assertEquals("y", c.get("{fb}t2"));
            assertEquals(2, c.scanStream(null).filter(k -> k.startsWith("{fb}t")).count());
            c.flushdb();
        }
    }
}
