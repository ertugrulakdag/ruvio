// SPDX-License-Identifier: MIT
package io.github.salihcantekin.ruvio.internal;

/** Redis Cluster key hash slot: CRC16 (XMODEM) of the key or of its first non-empty {hashtag}, mod 16384. */
public final class HashSlot {
    private static final int[] TABLE = new int[256];

    static {
        for (int i = 0; i < 256; i++) {
            int crc = i << 8;
            for (int j = 0; j < 8; j++) {
                crc = (crc & 0x8000) != 0 ? (crc << 1) ^ 0x1021 : crc << 1;
            }
            TABLE[i] = crc & 0xFFFF;
        }
    }

    private HashSlot() {
    }

    /**
     * Computes the CRC16 (XMODEM) of a byte range.
     *
     * @param b the bytes
     * @param from the first index
     * @param to the end index, exclusive
     * @return the checksum, 0 to 65535
     */
    public static int crc16(byte[] b, int from, int to) {
        int crc = 0;
        for (int i = from; i < to; i++) {
            crc = ((crc << 8) ^ TABLE[((crc >>> 8) ^ b[i]) & 0xFF]) & 0xFFFF;
        }
        return crc;
    }

    /**
     * Computes the hash slot of a key.
     *
     * @param key the key bytes
     * @return the slot, 0 to 16383
     */
    public static int of(byte[] key) {
        return of(key, 0, key.length);
    }

    /**
     * Computes the slot of {@code key[from, from + length)}, so a key inside an encoded frame needs no copy.
     *
     * @param key the array holding the key
     * @param from the first index of the key
     * @param length the key length
     * @return the slot, 0 to 16383
     */
    public static int of(byte[] key, int from, int length) {
        int end = from + length;
        int open = -1;
        for (int i = from; i < end; i++) {
            if (key[i] == '{') {
                open = i;
                break;
            }
        }
        if (open >= 0) {
            for (int j = open + 1; j < end; j++) {
                if (key[j] == '}') {
                    if (j > open + 1) {
                        return crc16(key, open + 1, j) & 16383;
                    }
                    break;
                }
            }
        }
        return crc16(key, from, end) & 16383;
    }
}
