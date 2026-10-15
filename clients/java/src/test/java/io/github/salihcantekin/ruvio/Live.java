// SPDX-License-Identifier: MIT
package io.github.salihcantekin.ruvio;

import static org.junit.jupiter.api.Assumptions.assumeTrue;

/** Live-test helpers: tests skip unless RUVIO_TEST_ADDR=host:port is set. */
final class Live {
    static final String ADDR = System.getenv("RUVIO_TEST_ADDR");
    /** Logical database used by the live tests (flushed before each test class). */
    static final int DB = 9;

    private Live() {
    }

    static RuvioOptions options(int database) {
        assumeTrue(ADDR != null && !ADDR.isEmpty(), "RUVIO_TEST_ADDR not set; live test skipped");
        RuvioOptions base = RuvioOptions.of(ADDR);
        return RuvioOptions.builder().host(base.host()).port(base.port()).database(database)
                .commandTimeoutMillis(10_000).build();
    }

    static RuvioClient<String, String> client() {
        RuvioClient<String, String> c = RuvioClient.connect(options(DB));
        c.flushdb();
        return c;
    }
}
