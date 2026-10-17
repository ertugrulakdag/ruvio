// SPDX-License-Identifier: MIT
package io.github.salihcantekin.ruvio;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.github.salihcantekin.ruvio.args.BitFieldArgs;
import io.github.salihcantekin.ruvio.args.BitOp;
import io.github.salihcantekin.ruvio.args.BitUnit;
import io.github.salihcantekin.ruvio.args.GeoOrder;
import io.github.salihcantekin.ruvio.args.GeoUnit;
import io.github.salihcantekin.ruvio.args.InsertPosition;
import io.github.salihcantekin.ruvio.args.KeyType;
import io.github.salihcantekin.ruvio.args.ListDirection;
import io.github.salihcantekin.ruvio.args.ScanArgs;
import io.github.salihcantekin.ruvio.args.ZStoreArgs;
import io.github.salihcantekin.ruvio.internal.Args;
import io.github.salihcantekin.ruvio.internal.RespOut;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import org.junit.jupiter.api.Test;

/** Typed keyword constants encode to the same bytes as the string path and carry the server's values. */
class KeywordConstantsTest {
    private static byte[] viaString(String word) {
        RespOut o = new RespOut(32);
        o.bulk(word);
        return Arrays.copyOf(o.buf, o.pos);
    }

    @Test
    void fragmentsEqualTheStringEncoding() {
        for (GeoUnit u : GeoUnit.values()) {
            assertArrayEquals(viaString(u.name()), u.fragment());
        }
        for (ListDirection d : ListDirection.values()) {
            assertArrayEquals(viaString(d.name()), d.fragment());
        }
        for (BitOp o : BitOp.values()) {
            assertArrayEquals(viaString(o.name()), o.fragment());
        }
        for (ZStoreArgs.Aggregate g : ZStoreArgs.Aggregate.values()) {
            assertArrayEquals(viaString(g.name()), g.fragment());
        }
        for (BitFieldArgs.Overflow f : BitFieldArgs.Overflow.values()) {
            assertArrayEquals(viaString(f.name()), f.fragment());
        }
        for (InsertPosition p : InsertPosition.values()) {
            assertArrayEquals(viaString(p.word()), p.fragment());
        }
        for (BitUnit u : BitUnit.values()) {
            assertArrayEquals(viaString(u.word()), u.fragment());
        }
        for (GeoOrder o : GeoOrder.values()) {
            assertArrayEquals(viaString(o.word()), o.fragment());
        }
        for (KeyType t : KeyType.values()) {
            assertArrayEquals(viaString(t.word()), t.fragment());
        }
    }

    @Test
    void valueTables() {
        assertEquals("BEFORE", InsertPosition.BEFORE.word());
        assertEquals("AFTER", InsertPosition.AFTER.word());
        assertEquals("BYTE", BitUnit.BYTE.word());
        assertEquals("BIT", BitUnit.BIT.word());
        assertEquals("ASC", GeoOrder.ASC.word());
        assertEquals("DESC", GeoOrder.DESC.word());
        String[] types = {"string", "json", "list", "set", "hash", "zset", "bloomfilter", "stream", "lease", "limit", "semaphore", "once"};
        KeyType[] all = KeyType.values();
        assertEquals(types.length, all.length);
        for (int i = 0; i < types.length; i++) {
            assertEquals(types[i], all[i].word());
        }
    }

    @Test
    void scanTypeEnumWritesTheSameBytesAsTheString() {
        assertArrayEquals(scan(ScanArgs.count(10).type("hash")), scan(ScanArgs.count(10).type(KeyType.HASH)));
    }

    private static byte[] scan(ScanArgs a) {
        Args args = new Args(null, null);
        a.write(args, true);
        return Arrays.copyOf(args.out.buf, args.out.pos);
    }

    @Test
    void bitFieldTypedHelpersMatchTheStrings() {
        assertEquals("u8", BitFieldArgs.Type.unsigned(8).word());
        assertEquals("i64", BitFieldArgs.Type.signed(64).word());
        assertEquals("u63", BitFieldArgs.Type.unsigned(63).word());
        assertThrows(IllegalArgumentException.class, () -> BitFieldArgs.Type.unsigned(64));
        assertThrows(IllegalArgumentException.class, () -> BitFieldArgs.Type.signed(0));
        BitFieldArgs typed = BitFieldArgs.builder().get(BitFieldArgs.Type.unsigned(8), 0)
                .set(BitFieldArgs.Type.signed(16), "#1", -5)
                .incrBy(BitFieldArgs.Type.unsigned(4), 8, 1)
                .overflow(BitFieldArgs.Overflow.SAT);
        BitFieldArgs plain = BitFieldArgs.builder().get("u8", 0).set("i16", "#1", -5).incrBy("u4", 8, 1)
                .overflow(BitFieldArgs.Overflow.SAT);
        assertArrayEquals(bits(plain), bits(typed));
        assertEquals("$3\r\nGET\r\n$2\r\nu8\r\n$1\r\n0\r\n", new String(bits(BitFieldArgs.builder().get(BitFieldArgs.Type.unsigned(8), 0)), StandardCharsets.US_ASCII));
    }

    private static byte[] bits(BitFieldArgs b) {
        Args args = new Args(null, null);
        b.write(args);
        return Arrays.copyOf(args.out.buf, args.out.pos);
    }
}
