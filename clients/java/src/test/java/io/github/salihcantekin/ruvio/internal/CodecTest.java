// SPDX-License-Identifier: MIT
package io.github.salihcantekin.ruvio.internal;

import io.github.salihcantekin.ruvio.RuvioCodec;
import io.github.salihcantekin.ruvio.JsonCodec;
import io.github.salihcantekin.ruvio.RuvioServerException;


import static org.junit.jupiter.api.Assertions.*;

import java.nio.charset.StandardCharsets;
import java.util.List;
import org.junit.jupiter.api.Test;

/** Encoder and parser unit tests; no server needed. */
class CodecTest {
    private static String enc(java.util.function.Consumer<RespOut> f) {
        RespOut o = new RespOut(8);
        f.accept(o);
        return new String(o.buf, 0, o.pos, StandardCharsets.ISO_8859_1);
    }

    @Test
    void encodesCommands() {
        assertEquals("*2\r\n$3\r\nGET\r\n$1\r\nk\r\n", enc(o -> {
            o.header(2);
            o.raw(Cmd.GET);
            o.bulk("k");
        }));
        assertEquals("$0\r\n\r\n", enc(o -> o.bulk("")));
        assertEquals("*12\r\n", enc(o -> o.header(12)));
        assertEquals("$1\r\n0\r\n", enc(o -> o.bulk(0L)));
        assertEquals("$3\r\n-42\r\n", enc(o -> o.bulk(-42L)));
        assertEquals("$19\r\n9223372036854775807\r\n", enc(o -> o.bulk(Long.MAX_VALUE)));
        assertEquals("$20\r\n-9223372036854775808\r\n", enc(o -> o.bulk(Long.MIN_VALUE)));
        assertEquals("$3\r\n1.5\r\n", enc(o -> o.bulk(1.5)));
        assertEquals("$1\r\n3\r\n", enc(o -> o.bulk(3.0)));
        assertEquals("$4\r\n+inf\r\n", enc(o -> o.bulk(Double.POSITIVE_INFINITY)));
        assertEquals("$3\r\nGET\r\n", new String(Cmd.GET, StandardCharsets.US_ASCII).substring(0, 0) + new String(Cmd.GET, StandardCharsets.US_ASCII).replace("$3\r\n", "$3\r\n"));
    }

    @Test
    void utf8MatchesJdk() {
        String[] samples = {"héllo", "日本語", "😀 emoji", "a\u0000b", "mix é 日 😀 z", "lone \ud800 surrogate"};
        for (String s : samples) {
            byte[] expect = s.getBytes(StandardCharsets.UTF_8);
            RespOut o = new RespOut(4);
            o.bulk(s);
            byte[] want = ("$" + expect.length + "\r\n").getBytes(StandardCharsets.US_ASCII);
            assertEquals(want.length + expect.length + 2, o.pos, s);
            for (int i = 0; i < expect.length; i++) {
                assertEquals(expect[i], o.buf[want.length + i], s + " @" + i);
            }
        }
    }

    @Test
    void growsBuffer() {
        RespOut o = new RespOut(4);
        char[] big = new char[100_000];
        java.util.Arrays.fill(big, 'x');
        o.bulk(new String(big));
        assertEquals(100_000 + 2 + "$100000\r\n".length(), o.pos);
    }

    private static RespReader reader(String s) {
        byte[] b = s.getBytes(StandardCharsets.ISO_8859_1);
        RespReader r = new RespReader(b);
        r.wrap(b, 0, b.length);
        return r;
    }

    @Test
    void parsesScalars() {
        assertEquals(42L, reader(":42\r\n").readLongOrNull());
        assertEquals(-1L, reader(":-1\r\n").readLongOrNull());
        assertEquals("OK", reader("+OK\r\n").readString());
        assertEquals("hello", reader("$5\r\nhello\r\n").readString());
        assertEquals("", reader("$0\r\n\r\n").readString());
        assertNull(reader("$-1\r\n").readString());
        assertNull(reader("_\r\n").readString());
        assertNull(reader("$-1\r\n").readLongOrNull());
        assertEquals(12L, reader("$2\r\n12\r\n").readLongOrNull());
        assertEquals(1.5, reader("$3\r\n1.5\r\n").readDoubleOrNull());
        assertEquals(Double.NEGATIVE_INFINITY, reader(",-inf\r\n").readDoubleOrNull());
        assertEquals(1L, reader("#t\r\n").readLongOrNull());
        assertArrayEquals(new byte[] {13, 10}, reader("$2\r\n\r\n\r\n").readBytes());
        assertThrows(RuvioServerException.class, () -> reader("-ERR boom\r\n").readString());
    }

    @Test
    void incompleteRepliesAreDetected() {
        String full = "*2\r\n$3\r\nabc\r\n:7\r\n";
        for (int cut = 0; cut < full.length(); cut++) {
            RespReader r = reader(full.substring(0, cut));
            assertThrows(RespReader.Incomplete.class, r::readObject, "cut at " + cut);
        }
        RespReader r = reader(full);
        List<?> l = (List<?>) r.readObject();
        assertArrayEquals("abc".getBytes(), (byte[]) l.get(0));
        assertEquals(7L, l.get(1));
        assertEquals(full.length(), r.pos);
    }

    @Test
    void parsesNested() {
        RespReader r = reader("*3\r\n*2\r\n:1\r\n$1\r\na\r\n$-1\r\n%1\r\n+k\r\n:2\r\n");
        List<?> l = (List<?>) r.readObject();
        assertEquals(1L, ((List<?>) l.get(0)).get(0));
        assertNull(l.get(1));
        assertEquals(java.util.Map.of("k", 2L), l.get(2));
    }

    @Test
    void skipsAndHashes() {
        RespReader r = reader("*2\r\n$1\r\na\r\n*1\r\n:1\r\n:9\r\n");
        r.skip();
        assertEquals(9L, r.readLongOrNull());
        assertEquals(12182, HashSlot.of("foo".getBytes()));
        assertEquals(HashSlot.of("a".getBytes()), HashSlot.of("{a}x".getBytes()));
        assertEquals(HashSlot.of("{}x".getBytes()), HashSlot.of("{}x".getBytes()));
    }

    @Test
    void codecs() {
        byte[] b = "héllo".getBytes(StandardCharsets.UTF_8);
        assertEquals("héllo", RuvioCodec.UTF8.decode(b, 0, b.length));
        assertArrayEquals(b, RuvioCodec.BYTES.decode(b, 0, b.length));
        assertNotSame(b, RuvioCodec.BYTES.decode(b, 0, b.length));
        assertEquals("{}", JsonCodec.PASSTHROUGH.toJson("{}"));
        assertThrows(IllegalArgumentException.class, () -> JsonCodec.PASSTHROUGH.toJson(5));
    }
}
