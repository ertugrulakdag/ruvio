// SPDX-License-Identifier: MIT
// Typed builders for `BITFIELD` operation words. Each returns the words of one sub-operation, with the
// keywords already encoded, ready to spread into `bitfield`.

import { KW_BITFIELD_OP, KW_OVERFLOW, kw } from './keywords.js';
import type { BitfieldOverflow, BitfieldType, Value } from './types.js';

const GET = KW_BITFIELD_OP.get('GET')!;
const SET = KW_BITFIELD_OP.get('SET')!;
const INCRBY = KW_BITFIELD_OP.get('INCRBY')!;
const OVERFLOW = KW_BITFIELD_OP.get('OVERFLOW')!;

/**
 * Builders for `BITFIELD` sub-operations. Spread them into {@link Commands.bitfield}:
 *
 * ```ts
 * await c.bitfield('k',
 *   ...BitfieldOp.set('u8', 0, 200),
 *   ...BitfieldOp.overflow('SAT'),
 *   ...BitfieldOp.incrby('u8', 0, 100),
 *   ...BitfieldOp.get('u8', 0)); // [0, 255, 255]
 * ```
 *
 * Field types are `i1`..`i64` (signed) and `u1`..`u63` (unsigned); an offset is a bit number, or `'#n'`
 * for the n-th field of that width.
 */
export const BitfieldOp = {
  /** `GET type offset`. */
  get: (type: BitfieldType, offset: number | string): Value[] => [GET, type, offset],
  /** `SET type offset value`. */
  set: (type: BitfieldType, offset: number | string, value: number): Value[] => [SET, type, offset, value],
  /** `INCRBY type offset increment`. */
  incrby: (type: BitfieldType, offset: number | string, increment: number): Value[] => [INCRBY, type, offset, increment],
  /** `OVERFLOW WRAP|SAT|FAIL`: applies to the `incrby` / `set` operations that follow it. */
  overflow: (mode: BitfieldOverflow): Value[] => [OVERFLOW, kw(KW_OVERFLOW, mode)],
} as const;
