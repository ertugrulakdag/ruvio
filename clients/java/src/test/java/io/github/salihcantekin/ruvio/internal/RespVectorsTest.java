// SPDX-License-Identifier: MIT
package io.github.salihcantekin.ruvio.internal;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import io.github.salihcantekin.ruvio.RuvioServerException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * The RESP wire vectors in clients/testdata/resp.json are shared by every client, so the six
 * encoders and parsers cannot drift apart.
 */
class RespVectorsTest {
    private static final HexFormat HEX = HexFormat.of();

    @SuppressWarnings("unchecked")
    private static List<Map<String, Object>> vectors(String list) throws IOException {
        Path p = Paths.get(System.getProperty("user.dir")).resolve("../testdata/resp.json").normalize();
        Map<String, Object> root = (Map<String, Object>) new Json(Files.readString(p, StandardCharsets.UTF_8)).value();
        List<Map<String, Object>> out = (List<Map<String, Object>>) root.get(list);
        assertTrue(out != null && !out.isEmpty(), list);
        return out;
    }

    /** The wire bytes of a vector: {@code bytes} (UTF-8 text) or {@code bytes_hex}. */
    private static byte[] wire(Map<String, Object> v) {
        Object hex = v.get("bytes_hex");
        return hex != null ? HEX.parseHex((String) hex) : ((String) v.get("bytes")).getBytes(StandardCharsets.UTF_8);
    }

    private static byte[] encode(List<?> args, boolean viaArg) {
        RespOut out = new RespOut(16); // tiny buffer: exercises growth
        out.header(args.size());
        for (Object a : args) {
            if (viaArg) {
                out.arg(a);
            } else if (a instanceof String s) {
                out.bulk(s);
            } else {
                out.bulk((byte[]) a);
            }
        }
        return Arrays.copyOf(out.buf, out.pos);
    }

    @Test
    @SuppressWarnings("unchecked")
    void encoderWritesTheSharedVectors() throws IOException {
        for (Map<String, Object> v : vectors("encode")) {
            String name = (String) v.get("name");
            byte[] want = wire(v);
            List<byte[]> binary = new ArrayList<>();
            List<String> text = (List<String>) v.get("args");
            if (text != null) {
                assertArrayEquals(want, encode(text, false), name + ": bulk(String)");
                assertArrayEquals(want, encode(text, true), name + ": arg(String)");
                for (String a : text) {
                    binary.add(a.getBytes(StandardCharsets.UTF_8));
                }
            } else {
                for (String a : (List<String>) v.get("args_hex")) {
                    binary.add(HEX.parseHex(a));
                }
            }
            assertArrayEquals(want, encode(binary, false), name + ": bulk(byte[])");
            assertArrayEquals(want, encode(binary, true), name + ": arg(byte[])");
        }
    }

    @Test
    void readerReadsTheSharedVectors() throws IOException {
        for (Map<String, Object> v : vectors("decode")) {
            String name = (String) v.get("name");
            byte[] b = wire(v);
            RespReader r = new RespReader(b);
            r.wrap(b, 0, b.length);
            Object got;
            try {
                got = r.readObject();
            } catch (RuntimeException e) {
                throw new AssertionError(name + ": " + e, e);
            }
            assertValue(v.get("value"), got, name);
            assertEquals(b.length, r.pos, name + ": bytes left over");
        }
    }

    @Test
    void readerAsksForMoreDataOnAnIncompleteReply() throws IOException {
        for (Map<String, Object> v : vectors("incomplete")) {
            byte[] b = wire(v);
            RespReader r = new RespReader(b);
            r.wrap(b, 0, b.length);
            assertSame(RespReader.INCOMPLETE, assertThrows(RespReader.Incomplete.class, r::readObject), (String) v.get("name"));
        }
    }

    /** Maps the neutral value shape onto readObject's tree: String, RuvioServerException, Long, byte[], null, List. */
    @SuppressWarnings("unchecked")
    private static void assertValue(Object shape, Object got, String name) {
        Map.Entry<String, Object> e = ((Map<String, Object>) shape).entrySet().iterator().next();
        Object inner = e.getValue();
        switch (e.getKey()) {
            case "simple" -> assertEquals(inner, got, name);
            case "error" -> assertEquals(inner, assertInstanceOf(RuvioServerException.class, got, name).getMessage(), name);
            case "int" -> assertEquals(inner instanceof String s ? Long.valueOf(s) : inner, got, name);
            case "bulk" -> assertArrayEquals(((String) inner).getBytes(StandardCharsets.UTF_8), assertInstanceOf(byte[].class, got, name), name);
            case "bulk_hex" -> assertArrayEquals(HEX.parseHex((String) inner), assertInstanceOf(byte[].class, got, name), name);
            case "null" -> assertNull(got, name);
            case "array" -> {
                List<Object> want = (List<Object>) inner;
                List<Object> list = assertInstanceOf(List.class, got, name);
                assertEquals(want.size(), list.size(), name);
                for (int i = 0; i < want.size(); i++) {
                    assertValue(want.get(i), list.get(i), name);
                }
            }
            default -> fail(name + ": unknown value kind " + e.getKey());
        }
    }

    /** Just enough JSON for the vector file: objects, arrays, strings, integers, true/false/null. */
    private static final class Json {
        private final String s;
        private int i;

        Json(String s) {
            this.s = s;
        }

        Object value() {
            skipSpace();
            char c = s.charAt(i);
            switch (c) {
                case '{': {
                    Map<String, Object> m = new LinkedHashMap<>();
                    i++;
                    skipSpace();
                    if (s.charAt(i) == '}') {
                        i++;
                        return m;
                    }
                    while (true) {
                        skipSpace();
                        String k = string();
                        skipSpace();
                        expect(':');
                        m.put(k, value());
                        skipSpace();
                        if (s.charAt(i++) == '}') {
                            return m;
                        }
                    }
                }
                case '[': {
                    List<Object> l = new ArrayList<>();
                    i++;
                    skipSpace();
                    if (s.charAt(i) == ']') {
                        i++;
                        return l;
                    }
                    while (true) {
                        l.add(value());
                        skipSpace();
                        if (s.charAt(i++) == ']') {
                            return l;
                        }
                    }
                }
                case '"':
                    return string();
                case 't':
                    i += 4;
                    return Boolean.TRUE;
                case 'f':
                    i += 5;
                    return Boolean.FALSE;
                case 'n':
                    i += 4;
                    return null;
                default: {
                    int start = i;
                    while (i < s.length() && (s.charAt(i) == '-' || Character.isDigit(s.charAt(i)))) {
                        i++;
                    }
                    return Long.valueOf(s.substring(start, i));
                }
            }
        }

        private String string() {
            expect('"');
            StringBuilder b = new StringBuilder();
            while (true) {
                char c = s.charAt(i++);
                if (c == '"') {
                    return b.toString();
                }
                if (c != '\\') {
                    b.append(c);
                    continue;
                }
                char x = s.charAt(i++);
                switch (x) {
                    case 'n' -> b.append('\n');
                    case 'r' -> b.append('\r');
                    case 't' -> b.append('\t');
                    case 'b' -> b.append('\b');
                    case 'f' -> b.append('\f');
                    case 'u' -> {
                        b.append((char) Integer.parseInt(s.substring(i, i + 4), 16));
                        i += 4;
                    }
                    default -> b.append(x);
                }
            }
        }

        private void expect(char c) {
            if (s.charAt(i++) != c) {
                throw new IllegalStateException("expected '" + c + "' at " + (i - 1));
            }
        }

        private void skipSpace() {
            while (Character.isWhitespace(s.charAt(i))) {
                i++;
            }
        }
    }
}
