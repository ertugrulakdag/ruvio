// SPDX-License-Identifier: MIT
package io.github.salihcantekin.ruvio.internal;

import static org.junit.jupiter.api.Assertions.*;

import io.github.salihcantekin.ruvio.RuvioClient;
import io.github.salihcantekin.ruvio.RuvioConnectionException;
import io.github.salihcantekin.ruvio.RuvioException;
import io.github.salihcantekin.ruvio.RuvioOptions;
import io.github.salihcantekin.ruvio.RuvioServerException;
import java.io.IOException;
import java.net.ServerSocket;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

/** Connection handshake, failure and reconnect behaviour against scripted servers (no live server needed). */
class LinkFakeTest {
    private static String ok(List<String> args) {
        switch (args.get(0).toUpperCase()) {
            case "PING":
                return "+PONG\r\n";
            case "GET":
                return "$5\r\nvalue\r\n";
            default:
                return "+OK\r\n";
        }
    }

    private static RuvioOptions.Builder to(FakeNode n) {
        return RuvioOptions.builder().host("127.0.0.1").port(n.port).connectTimeoutMillis(2000);
    }

    @Test
    void handshakeSendsOnlyWhatTheOptionsAsk() throws Exception {
        try (FakeNode plain = new FakeNode(LinkFakeTest::ok)) {
            RuvioClient.connect(to(plain).build()).close();
            assertTrue(plain.received.isEmpty(), "no options, no handshake commands");
        }
        try (FakeNode n = new FakeNode(LinkFakeTest::ok)) {
            RuvioClient.connect(to(n).password("pw").build()).close();
            assertEquals(List.of("AUTH", "pw"), n.received.get(0));
        }
        try (FakeNode n = new FakeNode(LinkFakeTest::ok)) {
            RuvioClient.connect(to(n).credentials("bob", "pw").build()).close();
            assertEquals(List.of("AUTH", "bob", "pw"), n.received.get(0));
        }
        try (FakeNode n = new FakeNode(LinkFakeTest::ok)) {
            RuvioClient.connect(to(n).database(3).clientName("svc").build()).close();
            assertEquals(List.of("SELECT", "3"), n.received.get(0));
            assertEquals(List.of("CLIENT", "SETNAME", "svc"), n.received.get(1));
        }
        try (FakeNode n = new FakeNode(LinkFakeTest::ok)) {
            RuvioClient.connect(to(n).password("pw").database(1).clientName("x").build()).close();
            assertEquals("AUTH", n.received.get(0).get(0));
            assertEquals("SELECT", n.received.get(1).get(0));
            assertEquals("CLIENT", n.received.get(2).get(0));
        }
    }

    @Test
    void aRefusedHandshakeFailsTheConnectWithTheServerMessage() throws Exception {
        try (FakeNode n = new FakeNode(a -> a.get(0).equals("AUTH") ? "-WRONGPASS invalid username-password pair\r\n" : "+OK\r\n")) {
            RuvioServerException e = assertThrows(RuvioServerException.class, () -> RuvioClient.connect(to(n).password("bad").build()));
            assertTrue(e.getMessage().contains("WRONGPASS"));
        }
        try (FakeNode n = new FakeNode(a -> a.get(0).equals("SELECT") ? "-ERR DB index is out of range\r\n" : "+OK\r\n")) {
            assertThrows(RuvioServerException.class, () -> RuvioClient.connect(to(n).database(99).build()));
        }
        try (FakeNode n = new FakeNode(a -> a.get(0).equals("CLIENT") ? "-ERR no\r\n" : "+OK\r\n")) {
            assertThrows(RuvioServerException.class, () -> RuvioClient.connect(to(n).clientName("x").build()));
        }
    }

    @Test
    void connectingToAClosedPortIsAConnectionException() throws IOException {
        int port;
        try (ServerSocket s = new ServerSocket(0)) {
            port = s.getLocalPort();
        }
        RuvioConnectionException e = assertThrows(RuvioConnectionException.class,
                () -> RuvioClient.connect(RuvioOptions.builder().port(port).connectTimeoutMillis(500).build()));
        assertTrue(e.getMessage().contains("cannot connect"));
    }

    @Test
    void aDroppedConnectionFailsTheCallAndTheNextCallReconnects() throws Exception {
        AtomicInteger gets = new AtomicInteger();
        try (FakeNode n = new FakeNode(a -> {
            if (a.get(0).equals("GET") && gets.getAndIncrement() == 0) {
                return FakeNode.CLOSE;
            }
            return ok(a);
        })) {
            try (RuvioClient<String, String> c = RuvioClient.connect(to(n).build())) {
                assertThrows(RuvioConnectionException.class, () -> c.get("k"));
                assertEquals("value", c.get("k"), "the next command reopens the socket");
                assertEquals("PONG", c.ping());
            }
        }
    }

    @Test
    void aSilentServerHitsTheCommandTimeout() throws Exception {
        try (FakeNode n = new FakeNode(a -> a.get(0).equals("GET") ? FakeNode.SILENT : ok(a))) {
            try (RuvioClient<String, String> c = RuvioClient.connect(to(n).commandTimeoutMillis(150).build())) {
                long start = System.nanoTime();
                RuvioException e = assertThrows(RuvioException.class, () -> c.get("k"));
                assertTrue(e.getMessage().contains("no reply within 150 ms"), e.getMessage());
                assertTrue((System.nanoTime() - start) / 1_000_000 < 2000, "the timeout must not hang");
            }
        }
    }

    @Test
    void usingAClosedClientFails() throws Exception {
        try (FakeNode n = new FakeNode(LinkFakeTest::ok)) {
            RuvioClient<String, String> c = RuvioClient.connect(to(n).build());
            c.close();
            c.close(); // idempotent
            RuntimeException e = assertThrows(RuntimeException.class, () -> c.get("k"));
            assertTrue(e instanceof RuvioConnectionException || e instanceof RuvioException, e.toString());
        }
    }

    @Test
    void serverErrorsKeepTheConnectionUsable() throws Exception {
        try (FakeNode n = new FakeNode(a -> a.get(0).equals("GET") ? "-WRONGTYPE bad kind\r\n" : ok(a))) {
            try (RuvioClient<String, String> c = RuvioClient.connect(to(n).build())) {
                RuvioServerException e = assertThrows(RuvioServerException.class, () -> c.get("k"));
                assertTrue(e.getMessage().startsWith("WRONGTYPE"));
                assertEquals("PONG", c.ping());
            }
        }
    }

    @Test
    void optionsBuilderRoundTripsAndParsesHostPort() {
        RuvioOptions o = RuvioOptions.builder().host("h").port(1234).credentials("u", "p").database(2).clientName("n")
                .connectTimeoutMillis(10).commandTimeoutMillis(20).maxInFlight(64).cluster(true).build();
        RuvioOptions copy = o.toBuilder().build();
        assertEquals("h", copy.host());
        assertEquals(1234, copy.port());
        assertEquals(2, copy.database());
        assertTrue(copy.cluster());
        assertEquals("u", copy.username);
        assertEquals("p", copy.password);
        assertEquals("n", copy.clientName);
        assertEquals(20, copy.commandTimeoutMillis);
        assertEquals(64, copy.maxInFlight);
        assertEquals(10, copy.connectTimeoutMillis);

        RuvioOptions parsed = RuvioOptions.of("cache.local:7000");
        assertEquals("cache.local", parsed.host());
        assertEquals(7000, parsed.port());
        assertEquals(6379, RuvioOptions.of("cache.local").port(), "a bare host uses the default port");
        assertThrows(RuntimeException.class, () -> RuvioOptions.of("host:notaport"));
    }
}
