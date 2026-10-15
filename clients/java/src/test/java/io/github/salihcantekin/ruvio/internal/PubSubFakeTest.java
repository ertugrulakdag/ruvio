// SPDX-License-Identifier: MIT
package io.github.salihcantekin.ruvio.internal;

import static org.junit.jupiter.api.Assertions.*;

import io.github.salihcantekin.ruvio.RuvioClient;
import io.github.salihcantekin.ruvio.RuvioOptions;
import io.github.salihcantekin.ruvio.RuvioPubSub;
import io.github.salihcantekin.ruvio.model.PubSubMessage;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

/** Pub/sub confirmations, message kinds and subscription bookkeeping against a scripted server. */
class PubSubFakeTest {
    private static String bulk(String s) {
        return "$" + s.getBytes(StandardCharsets.UTF_8).length + "\r\n" + s + "\r\n";
    }

    private static String confirm(String kind, String name, int count) {
        return "*3\r\n" + bulk(kind) + bulk(name) + ":" + count + "\r\n";
    }

    /** Confirms every channel of a (P/S)(UN)SUBSCRIBE, then pushes one message for the subscribe forms. */
    private final java.util.Map<String, java.util.Set<String>> active = new java.util.concurrent.ConcurrentHashMap<>();

    private String handler(List<String> a) {
        String cmd = a.get(0).toLowerCase();
        StringBuilder out = new StringBuilder();
        switch (cmd) {
            case "subscribe": case "unsubscribe": case "psubscribe": case "punsubscribe": case "ssubscribe": case "sunsubscribe": {
                String family = cmd.replace("un", "");
                java.util.Set<String> members = active.computeIfAbsent(family, k -> java.util.concurrent.ConcurrentHashMap.newKeySet());
                boolean add = !cmd.contains("unsub");
                if (!add && a.size() == 1) {
                    // No arguments: the server confirms every active subscription (or once with a nil name).
                    List<String> all = new java.util.ArrayList<>(members);
                    members.clear();
                    if (all.isEmpty()) {
                        out.append("*3\r\n").append(bulk(cmd)).append("$-1\r\n:0\r\n");
                    }
                    for (int i = 0; i < all.size(); i++) {
                        out.append(confirm(cmd, all.get(i), all.size() - i - 1));
                    }
                }
                for (int i = 1; i < a.size(); i++) {
                    if (add) {
                        members.add(a.get(i));
                    } else {
                        members.remove(a.get(i));
                    }
                    out.append(confirm(cmd, a.get(i), members.size()));
                }
                if (cmd.equals("subscribe")) {
                    out.append("*3\r\n").append(bulk("message")).append(bulk(a.get(1))).append(bulk("hello")).append("");
                } else if (cmd.equals("psubscribe")) {
                    out.append("*4\r\n").append(bulk("pmessage")).append(bulk(a.get(1))).append(bulk("news.1")).append(bulk("pp"));
                } else if (cmd.equals("ssubscribe")) {
                    out.append(">3\r\n").append(bulk("smessage")).append(bulk(a.get(1))).append(bulk("shard"));
                }
                return out.toString();
            }
            case "ping":
                return "*2\r\n" + bulk("pong") + bulk("") ;
            default:
                return "+OK\r\n";
        }
    }

    @Test
    void confirmationsMessagesAndBookkeeping() throws Exception {
        try (FakeNode n = new FakeNode(this::handler);
                RuvioClient<String, String> c = RuvioClient.connect(RuvioOptions.builder().port(n.port).build())) {
            BlockingQueue<PubSubMessage> got = new LinkedBlockingQueue<>();
            try (RuvioPubSub sub = c.pubSub(got::add)) {
                sub.subscribe("alpha", "beta");
                assertEquals(Set.of("alpha", "beta"), sub.channels());
                PubSubMessage m = got.poll(5, TimeUnit.SECONDS);
                assertNotNull(m, "message pushed after the confirmation");
                assertEquals(PubSubMessage.Kind.MESSAGE, m.kind());
                assertEquals("alpha", m.channel());
                assertNull(m.pattern());
                assertArrayEquals("hello".getBytes(StandardCharsets.UTF_8), m.payload());

                sub.psubscribe("news.*");
                assertEquals(Set.of("news.*"), sub.patterns());
                PubSubMessage p = got.poll(5, TimeUnit.SECONDS);
                assertNotNull(p);
                assertEquals(PubSubMessage.Kind.PMESSAGE, p.kind());
                assertEquals("news.*", p.pattern());
                assertEquals("news.1", p.channel());

                sub.ssubscribe("shard-a");
                assertEquals(Set.of("shard-a"), sub.shardChannels());
                PubSubMessage s = got.poll(5, TimeUnit.SECONDS);
                assertNotNull(s);
                assertEquals(PubSubMessage.Kind.SMESSAGE, s.kind(), "RESP3 push frames are delivered too");

                sub.ping();

                sub.unsubscribe("alpha");
                assertEquals(Set.of("beta"), sub.channels());
                sub.unsubscribe();
                assertTrue(sub.channels().isEmpty(), "no arguments drops every exact channel");
                sub.punsubscribe();
                assertTrue(sub.patterns().isEmpty());
                sub.sunsubscribe();
                assertTrue(sub.shardChannels().isEmpty());

                assertEquals(List.of("SUBSCRIBE", "alpha", "beta"), n.commands("SUBSCRIBE").get(0));
                assertEquals(List.of("PSUBSCRIBE", "news.*"), n.commands("PSUBSCRIBE").get(0));
            }
        }
    }

    @Test
    void invalidUseIsRejected() throws Exception {
        try (FakeNode n = new FakeNode(this::handler);
                RuvioClient<String, String> c = RuvioClient.connect(RuvioOptions.builder().port(n.port).build())) {
            RuvioPubSub sub = c.pubSub(m -> { });
            assertThrows(IllegalArgumentException.class, sub::subscribe, "subscribing to nothing is a mistake");
            sub.close();
            sub.close(); // idempotent
            assertThrows(RuntimeException.class, () -> sub.subscribe("x"), "a closed subscriber refuses new work");
        }
    }

    @Test
    void aListenerThatThrowsDoesNotKillTheSubscriber() throws Exception {
        try (FakeNode n = new FakeNode(this::handler);
                RuvioClient<String, String> c = RuvioClient.connect(RuvioOptions.builder().port(n.port).build())) {
            BlockingQueue<PubSubMessage> got = new LinkedBlockingQueue<>();
            int[] calls = {0};
            try (RuvioPubSub sub = c.pubSub(m -> {
                if (calls[0]++ == 0) {
                    throw new IllegalStateException("listener bug");
                }
                got.add(m);
            })) {
                sub.subscribe("one");
                sub.psubscribe("p*");
                assertNotNull(got.poll(5, TimeUnit.SECONDS), "the second message still arrives");
            }
        }
    }
}
