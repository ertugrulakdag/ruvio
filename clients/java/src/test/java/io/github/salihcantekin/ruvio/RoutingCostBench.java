// SPDX-License-Identifier: MIT
package io.github.salihcantekin.ruvio;

import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Plain vs cluster CPU per request. Not a test: run its main with the test classpath.
 * {@code java -cp target/classes:target/test-classes io.github.salihcantekin.ruvio.RoutingCostBench 127.0.0.1:17700}
 * Rows: plain, cluster (keys spread over the nodes) and one-node (cluster client, every key in one hash tag,
 * the routing code's own cost), at pipeline depths 16 and 64, plus plain depth 4 (the per-socket batch a
 * depth-16 pipeline leaves on each of four nodes). About a minute.
 * CPU is summed over the Java threads (workers plus the connections' reader and writer threads) with
 * ThreadMXBean user and total times; sys is total minus user. JIT and GC threads are not included.
 * Allocation is summed over the worker threads only.
 */
public final class RoutingCostBench {
    static final int W = 8;
    static final int KEYS = 4096;
    static final long WARM_MS = 500;
    static final long RUN_MS = 2_000;

    private RoutingCostBench() {
    }

    public static void main(String[] args) throws Exception {
        String addr = args[0];
        RuvioOptions base = RuvioOptions.of(addr);
        String[] keys = new String[KEYS];
        String[] tagged = new String[KEYS];
        for (int i = 0; i < KEYS; i++) {
            keys[i] = "key:" + i;
            tagged[i] = "{t}key:" + i;
        }
        byte[] value = new byte[32];
        java.util.Arrays.fill(value, (byte) 'x');
        List<RuvioClient<String, byte[]>> plain = open(base, false);
        List<RuvioClient<String, byte[]>> cluster = open(base, true);
        if (!cluster.get(0).isCluster() || plain.get(0).isCluster()) {
            throw new IllegalStateException("cluster/plain mode mismatch");
        }
        for (String op : new String[] {"GET", "SET"}) {
            row("plain", plain, op, 4, keys, value);
            for (int p : new int[] {16, 64}) {
                row("plain", plain, op, p, keys, value);
                row("cluster", cluster, op, p, keys, value);
                row("one-node", cluster, op, p, tagged, value);
            }
        }
        for (RuvioClient<String, byte[]> c : plain) {
            c.close();
        }
        for (RuvioClient<String, byte[]> c : cluster) {
            c.close();
        }
        System.exit(0);
    }

    static List<RuvioClient<String, byte[]>> open(RuvioOptions base, boolean cluster) {
        List<RuvioClient<String, byte[]>> out = new ArrayList<>();
        for (int i = 0; i < W; i++) {
            RuvioOptions o = RuvioOptions.builder().host(base.host()).port(base.port()).cluster(cluster)
                    .commandTimeoutMillis(10_000).build();
            out.add(RuvioClient.connect(o, RuvioCodec.UTF8, RuvioCodec.BYTES));
        }
        return out;
    }

    static void row(String label, List<RuvioClient<String, byte[]>> clients, String op, int p, String[] keys, byte[] value)
            throws Exception {
        if (op.equals("GET")) {
            // make the keys exist so GET returns a value like a real read
            runWorkers(clients, "SET", 1, keys, value, 300, null);
        }
        runWorkers(clients, op, p, keys, value, WARM_MS, null);
        long[] r = new long[4]; // ops, user ns, sys ns, alloc bytes
        long t0 = System.nanoTime();
        runWorkers(clients, op, p, keys, value, RUN_MS, r);
        double secs = (System.nanoTime() - t0) / 1e9;
        System.out.printf("%s %s P=%d ops/s=%.0f user_ns/op=%.0f sys_ns/op=%.0f bytes/op=%.0f%n", label, op, p, r[0] / secs,
                r[1] / (double) r[0], r[2] / (double) r[0], r[3] / (double) r[0]);
    }

    static void runWorkers(List<RuvioClient<String, byte[]>> clients, String op, int p, String[] keys, byte[] value,
            long millis, long[] result) throws Exception {
        com.sun.management.ThreadMXBean tm = (com.sun.management.ThreadMXBean) ManagementFactory.getThreadMXBean();
        AtomicLong ops = new AtomicLong();
        AtomicLong alloc = new AtomicLong();
        boolean get = op.equals("GET");
        AtomicLong user = new AtomicLong();
        AtomicLong cpu = new AtomicLong();
        long[] ids = tm.getAllThreadIds();
        long[] user0 = new long[ids.length];
        long[] cpu0 = new long[ids.length];
        for (int i = 0; i < ids.length; i++) {
            user0[i] = tm.getThreadUserTime(ids[i]);
            cpu0[i] = tm.getThreadCpuTime(ids[i]);
        }
        long end = System.nanoTime() + millis * 1_000_000L;
        List<Thread> ts = new ArrayList<>();
        for (int w = 0; w < W; w++) {
            RuvioClient<String, byte[]> c = clients.get(w);
            int start = w * (KEYS / W);
            Thread th = new Thread(() -> {
                long a0 = tm.getThreadAllocatedBytes(Thread.currentThread().getId());
                long u0 = tm.getCurrentThreadUserTime();
                long c0 = tm.getCurrentThreadCpuTime();
                long n = 0;
                int pos = start;
                Pipeline<String, byte[]> pl = p > 1 ? c.pipeline() : null;
                while (System.nanoTime() < end) {
                    if (p == 1) {
                        String k = keys[pos];
                        pos = (pos + 1) & (KEYS - 1);
                        if (get) {
                            c.get(k);
                        } else {
                            c.set(k, value);
                        }
                        n++;
                    } else {
                        for (int i = 0; i < p; i++) {
                            String k = keys[pos];
                            pos = (pos + 1) & (KEYS - 1);
                            if (get) {
                                pl.get(k);
                            } else {
                                pl.set(k, value);
                            }
                        }
                        pl.sync();
                        n += p;
                    }
                }
                ops.addAndGet(n);
                user.addAndGet(tm.getCurrentThreadUserTime() - u0);
                cpu.addAndGet(tm.getCurrentThreadCpuTime() - c0);
                alloc.addAndGet(tm.getThreadAllocatedBytes(Thread.currentThread().getId()) - a0);
            });
            ts.add(th);
        }
        for (Thread th : ts) {
            th.start();
        }
        for (Thread th : ts) {
            th.join();
        }
        for (int i = 0; i < ids.length; i++) {
            long u = tm.getThreadUserTime(ids[i]);
            long c = tm.getThreadCpuTime(ids[i]);
            if (u >= 0 && c >= 0 && user0[i] >= 0) {
                user.addAndGet(u - user0[i]);
                cpu.addAndGet(c - cpu0[i]);
            }
        }
        if (result != null) {
            result[0] = ops.get();
            result[1] = user.get();
            result[2] = cpu.get() - user.get();
            result[3] = alloc.get();
        }
    }
}
