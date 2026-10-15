// SPDX-License-Identifier: MIT
package io.github.salihcantekin.ruvio;

import io.github.salihcantekin.ruvio.internal.Cmd;
import io.github.salihcantekin.ruvio.internal.RespReader;

import io.github.salihcantekin.ruvio.internal.RespOut;
import io.github.salihcantekin.ruvio.internal.Reply;

import io.github.salihcantekin.ruvio.RuvioClient;
import io.github.salihcantekin.ruvio.Pipeline;
import io.github.salihcantekin.ruvio.RuvioCodec;


import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Micro-benchmark, skipped by default. Run:
 * {@code RUVIO_TEST_ADDR=127.0.0.1:17104 mvn -q test -Dtest=BenchTest -DexcludedGroups=none}
 */
@Tag("bench")
class BenchTest {
    static volatile long sink;

    @Test
    void bench() throws Exception {
        // encode: SET key value (256-byte value) and GET into a reused buffer
        RespOut o = new RespOut(1024);
        String key = "bench:key:000123";
        byte[] value = new byte[256];
        java.util.Arrays.fill(value, (byte) 'v');
        double enc = nsPerOp(3_000_000, () -> {
            o.reset();
            o.header(3);
            o.raw(Cmd.SET);
            o.bulk(key);
            o.bulk(value);
            sink += o.pos;
        });
        double encGet = nsPerOp(3_000_000, () -> {
            o.reset();
            o.header(2);
            o.raw(Cmd.GET);
            o.bulk(key);
            sink += o.pos;
        });
        double encInt = nsPerOp(3_000_000, () -> {
            o.reset();
            o.header(3);
            o.raw(Cmd.INCRBY);
            o.bulk(key);
            o.bulk(123456789L);
            sink += o.pos;
        });
        // parse: bulk 256 bytes decoded to String via the in-place reader, integer, 10-element array
        byte[] bulk = ("$256\r\n" + "v".repeat(256) + "\r\n").getBytes(StandardCharsets.ISO_8859_1);
        byte[] integer = ":123456\r\n".getBytes(StandardCharsets.ISO_8859_1);
        StringBuilder arr = new StringBuilder("*10\r\n");
        for (int i = 0; i < 10; i++) arr.append("$5\r\nhello\r\n");
        byte[] array = arr.toString().getBytes(StandardCharsets.ISO_8859_1);
        RespReader r = new RespReader(bulk);
        double pBulk = nsPerOp(3_000_000, () -> {
            r.wrap(bulk, 0, bulk.length);
            sink += r.readString().length();
        });
        double pInt = nsPerOp(3_000_000, () -> {
            r.wrap(integer, 0, integer.length);
            sink += r.readLongOrNull();
        });
        double pArr = nsPerOp(1_000_000, () -> {
            r.wrap(array, 0, array.length);
            sink += Reply.STRING_LIST.decode(r, null).size();
        });
        System.out.printf("[bench] encode SET(256B) %.1f ns, GET %.1f ns, INCRBY %.1f ns (0 allocations)%n", enc, encGet, encInt);
        System.out.printf("[bench] parse bulk(256B)->String %.1f ns, integer %.1f ns, array(10 x 5B)->List<String> %.1f ns%n", pBulk, pInt, pArr);

        if (Live.ADDR == null) {
            System.out.println("[bench] RUVIO_TEST_ADDR not set: live GET/SET numbers skipped");
            return;
        }
        try (RuvioClient<String, byte[]> c = RuvioClient.connect(Live.options(Live.DB), RuvioCodec.UTF8, RuvioCodec.BYTES)) {
            c.flushdb();
            c.set("bench:k", value);
            for (int threads : new int[] {1, 4, 16, 64}) {
                for (String op : new String[] {"GET", "SET"}) {
                    runLive(c, threads, op, value, 1_500);
                }
            }
            // explicit pipeline
            Pipeline<String, byte[]> p = c.pipeline();
            long end = System.nanoTime() + 1_500_000_000L;
            long done = 0;
            while (System.nanoTime() < end) {
                for (int i = 0; i < 100; i++) p.get("bench:k");
                p.sync();
                done += 100;
            }
            System.out.printf("[bench] live pipeline x100 GET: %,.0f ops/s%n", done / 1.5);
        }
    }

    static void runLive(RuvioClient<String, byte[]> c, int threads, String op, byte[] value, long millis) throws Exception {
        AtomicLong total = new AtomicLong();
        long end = System.nanoTime() + millis * 1_000_000L;
        List<Thread> ts = new ArrayList<>();
        for (int t = 0; t < threads; t++) {
            Thread th = new Thread(() -> {
                long n = 0;
                if (op.equals("GET")) {
                    while (System.nanoTime() < end) {
                        c.get("bench:k");
                        n++;
                    }
                } else {
                    while (System.nanoTime() < end) {
                        c.set("bench:k", value);
                        n++;
                    }
                }
                total.addAndGet(n);
            });
            ts.add(th);
        }
        // warm-up second is the first config's own ramp; keep runs short per the brief
        for (Thread th : ts) th.start();
        for (Thread th : ts) th.join();
        System.out.printf("[bench] live %s, %d thread(s), 256B value: %,.0f ops/s%n", op, threads, total.get() * 1000.0 / millis);
    }

    static double nsPerOp(int iterations, Runnable body) {
        for (int i = 0; i < iterations / 2; i++) body.run(); // warm-up
        long t0 = System.nanoTime();
        for (int i = 0; i < iterations; i++) body.run();
        return (System.nanoTime() - t0) / (double) iterations;
    }
}
