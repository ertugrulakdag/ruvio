// SPDX-License-Identifier: MIT
package io.github.salihcantekin.ruvio;

import io.github.salihcantekin.ruvio.internal.Cmd;
import io.github.salihcantekin.ruvio.internal.Frame;
import io.github.salihcantekin.ruvio.internal.RoutedRequest;

import io.github.salihcantekin.ruvio.internal.ClusterRouter;
import io.github.salihcantekin.ruvio.internal.Reply;

import io.github.salihcantekin.ruvio.RuvioClient;
import io.github.salihcantekin.ruvio.Pipeline;
import io.github.salihcantekin.ruvio.RuvioCodec;


import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Cluster routing cost and cluster-vs-standalone GET/SET throughput, skipped by default. Run:
 * {@code RUVIO_CLUSTER_ADDR=127.0.0.1:17320 RUVIO_TEST_ADDR=127.0.0.1:17323 mvn -q test -Dtest=ClusterBenchTest -DexcludedGroups=none}
 * The standalone number is the same client without cluster(true) against a single-shard server.
 */
@Tag("bench")
class ClusterBenchTest {
    static volatile long sink;
    static final int KEYS = 4096;

    @Test
    void bench() throws Exception {
        byte[] value = new byte[256];
        java.util.Arrays.fill(value, (byte) 'v');
        String[] keys = new String[KEYS];
        for (int i = 0; i < KEYS; i++) {
            keys[i] = "bench:key:" + i;
        }
        if (Live.ADDR != null) {
            try (RuvioClient<String, byte[]> c = RuvioClient.connect(Live.options(Live.DB), RuvioCodec.UTF8, RuvioCodec.BYTES)) {
                c.flushdb();
                for (String k : keys) {
                    c.set(k, value);
                }
                run("warm-up", c, 16, "GET", keys, value, 1_000, false);
                run("warm-up", c, 16, "SET", keys, value, 1_000, false);
                for (int threads : new int[] {1, 16, 64}) {
                    for (String op : new String[] {"GET", "SET"}) {
                        run("standalone (1 shard)", c, threads, op, keys, value);
                    }
                }
                Pipeline<String, byte[]> p = c.pipeline();
                long end = System.nanoTime() + 1_500_000_000L;
                long done = 0;
                while (System.nanoTime() < end) {
                    for (int i = 0; i < 100; i++) {
                        p.get(keys[(int) ((done + i) % KEYS)]);
                    }
                    p.sync();
                    done += 100;
                }
                System.out.printf("[bench] standalone (1 shard) pipeline x100 GET: %,.0f ops/s%n", done / 1.5);
                c.flushdb();
            }
        } else {
            System.out.println("[bench] RUVIO_TEST_ADDR not set: standalone numbers skipped");
        }
        if (ClusterLive.ADDR != null) {
            try (RuvioClient<String, byte[]> c = RuvioClient.connect(ClusterLive.options(), RuvioCodec.UTF8, RuvioCodec.BYTES)) {
                routingCost(c, keys, value);
                for (String n : c.nodeAddresses()) {
                    c.onNode(n).flushdb();
                }
                for (String k : keys) {
                    c.set(k, value);
                }
                run("warm-up", c, 16, "GET", keys, value, 1_000, false);
                run("warm-up", c, 16, "SET", keys, value, 1_000, false);
                for (int threads : new int[] {1, 16, 64}) {
                    for (String op : new String[] {"GET", "SET"}) {
                        run("cluster (3 shards)", c, threads, op, keys, value);
                    }
                }
                clusterPipeline(c, keys);
                // Reference on the same server: one node only, no routing, keys that node owns.
                String node = c.nodeAddresses().get(0);
                List<String> own = new ArrayList<>();
                for (String k : keys) {
                    if (c.router().owner(RuvioClient.hashSlot(k)).address.equals(node)) {
                        own.add(k);
                    }
                }
                String[] mine = own.toArray(new String[0]);
                RuvioClient<String, byte[]> one = c.onNode(node);
                run("warm-up", one, 16, "GET", mine, value, 1_000, false);
                for (int threads : new int[] {16}) {
                    for (String op : new String[] {"GET", "SET"}) {
                        run("one node of the 3-shard server, no routing, own keys", one, threads, op, mine, value);
                    }
                }
                run("cluster routed, same single-node key set", c, 16, "GET", mine, value);
                run("cluster routed, same single-node key set", c, 16, "SET", mine, value);
            }
        } else {
            System.out.println("[bench] RUVIO_CLUSTER_ADDR not set: cluster numbers skipped");
        }
    }

    /** Local cost of routing one GET (encode -> parse -> hash -> table lookup -> frame copy), no socket. */
    static void routingCost(RuvioClient<String, byte[]> c, String[] keys, byte[] value) {
        ClusterRouter router = c.router();
        Frame f = new Frame();
        ClusterRouter.Sink none = (node, buf, off, len, r) -> sink += len;
        long[] iterations = {2_000_000};
        double ns = nsPerOp((int) iterations[0], () -> {
            f.reset();
            f.out.header(2);
            f.out.raw(Cmd.GET);
            f.out.bulk(keys[(int) (sink & (KEYS - 1))]);
            router.dispatch(f, new RoutedRequest<>(Reply.BYTES, RuvioCodec.UTF8, RuvioCodec.BYTES), none);
        });
        double encodeOnly = nsPerOp((int) iterations[0], () -> {
            f.reset();
            f.out.header(2);
            f.out.raw(Cmd.GET);
            f.out.bulk(keys[(int) (sink & (KEYS - 1))]);
            sink += f.out.pos;
        });
        com.sun.management.ThreadMXBean mx = (com.sun.management.ThreadMXBean) ManagementFactory.getThreadMXBean();
        long before = mx.getThreadAllocatedBytes(Thread.currentThread().getId());
        int n = 1_000_000;
        for (int i = 0; i < n; i++) {
            f.reset();
            f.out.header(2);
            f.out.raw(Cmd.GET);
            f.out.bulk(keys[i & (KEYS - 1)]);
            router.dispatch(f, new RoutedRequest<>(Reply.BYTES, RuvioCodec.UTF8, RuvioCodec.BYTES), none);
        }
        long bytes = (mx.getThreadAllocatedBytes(Thread.currentThread().getId()) - before) / n;
        System.out.printf("[bench] route GET locally: %.1f ns (encode alone %.1f ns), %d bytes allocated per command incl. the request object%n",
                ns, encodeOnly, bytes);
    }

    static void clusterPipeline(RuvioClient<String, byte[]> c, String[] keys) {
        Pipeline<String, byte[]> p = c.pipeline();
        long end = System.nanoTime() + 1_500_000_000L;
        long done = 0;
        while (System.nanoTime() < end) {
            for (int i = 0; i < 100; i++) {
                p.get(keys[(int) ((done + i) % KEYS)]);
            }
            p.sync();
            done += 100;
        }
        System.out.printf("[bench] cluster (3 shards) pipeline x100 GET (grouped per node): %,.0f ops/s%n", done / 1.5);
    }

    static void run(String label, RuvioClient<String, byte[]> c, int threads, String op, String[] keys, byte[] value) throws Exception {
        run(label, c, threads, op, keys, value, 1_500, true);
    }

    static void run(String label, RuvioClient<String, byte[]> c, int threads, String op, String[] keys, byte[] value,
            long millis, boolean print) throws Exception {
        AtomicLong total = new AtomicLong();
        final int mask = keys.length;
        long end = System.nanoTime() + millis * 1_000_000L;
        List<Thread> ts = new ArrayList<>();
        for (int t = 0; t < threads; t++) {
            int seed = t * 977;
            Thread th = new Thread(() -> {
                long n = 0;
                boolean get = op.equals("GET");
                while (System.nanoTime() < end) {
                    String k = keys[(int) ((seed + n * 31) % mask)];
                    if (get) {
                        c.get(k);
                    } else {
                        c.set(k, value);
                    }
                    n++;
                }
                total.addAndGet(n);
            });
            ts.add(th);
        }
        for (Thread th : ts) {
            th.start();
        }
        for (Thread th : ts) {
            th.join();
        }
        if (print) {
            System.out.printf("[bench] %s %s, %d thread(s), 256B value, %d keys: %,.0f ops/s%n", label, op, threads, keys.length,
                    total.get() * 1000.0 / millis);
        }
    }

    static double nsPerOp(int iterations, Runnable body) {
        for (int i = 0; i < iterations / 2; i++) {
            body.run();
        }
        long t0 = System.nanoTime();
        for (int i = 0; i < iterations; i++) {
            body.run();
        }
        return (System.nanoTime() - t0) / (double) iterations;
    }
}
