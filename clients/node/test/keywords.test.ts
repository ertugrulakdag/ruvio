// SPDX-License-Identifier: MIT
// Keyword constants: value tables and pre-encoded bytes, without a server.

import { test } from 'node:test';
import assert from 'node:assert/strict';
import {
  Aggregate, BitOp, BitUnit, BitfieldOp, BitfieldOverflow, Encoder, GeoOrder, GeoUnit, InsertPosition, ListSide,
  ScanType, cmd,
} from '../src/index.js';
import { KW_BITOP, KW_GEO_UNIT, KW_SCAN_TYPE, KW_SIDE, KW_WHERE, kw } from '../src/keywords.js';
import { Commands } from '../src/commands.js';

test('value tables match the server keywords', () => {
  assert.deepEqual(Object.values(GeoUnit), ['m', 'km', 'ft', 'mi']);
  assert.deepEqual(Object.values(GeoOrder), ['ASC', 'DESC']);
  assert.deepEqual(Object.values(ListSide), ['LEFT', 'RIGHT']);
  assert.deepEqual(Object.values(InsertPosition), ['BEFORE', 'AFTER']);
  assert.deepEqual(Object.values(BitOp), ['AND', 'OR', 'XOR', 'NOT']);
  assert.deepEqual(Object.values(BitUnit), ['BYTE', 'BIT']);
  assert.deepEqual(Object.values(Aggregate), ['SUM', 'MIN', 'MAX']);
  assert.deepEqual(Object.values(BitfieldOverflow), ['WRAP', 'SAT', 'FAIL']);
  assert.equal(Object.values(ScanType).length, 12);
});

test('every constant has a pre-encoded Buffer with the same bytes', () => {
  const pairs: [ReadonlyMap<string, Buffer>, readonly string[]][] = [
    [KW_GEO_UNIT, Object.values(GeoUnit)], [KW_SIDE, Object.values(ListSide)], [KW_WHERE, Object.values(InsertPosition)],
    [KW_BITOP, Object.values(BitOp)], [KW_SCAN_TYPE, Object.values(ScanType)],
  ];
  for (const [t, vals] of pairs) {
    for (const v of vals) {
      const b = kw(t, v);
      assert.ok(Buffer.isBuffer(b), v);
      assert.equal(b.toString('latin1'), v);
    }
  }
  assert.equal(kw(KW_GEO_UNIT, 'KM').toString(), 'KM', 'upper case keeps its spelling');
  assert.equal(kw(KW_GEO_UNIT, 'Km'), 'Km', 'unknown spelling passes through');
});

test('encoded frames equal the string path', () => {
  const c = cmd('GEODIST');
  const frame = (unit: string | Buffer): Buffer => {
    const enc = new Encoder();
    enc.begin(c, 4);
    for (const a of ['k', 'a', 'b', unit]) enc.arg(a);
    return Buffer.from(enc.take());
  };
  assert.ok(frame(GeoUnit.Km).equals(frame(kw(KW_GEO_UNIT, GeoUnit.Km))));
});

test('BitfieldOp builders produce the raw words', () => {
  assert.deepEqual(BitfieldOp.get('u8', 0).map(String), ['GET', 'u8', '0']);
  assert.deepEqual(BitfieldOp.set('i16', '#1', 5).map(String), ['SET', 'i16', '#1', '5']);
  assert.deepEqual(BitfieldOp.incrby('u4', 8, -1).map(String), ['INCRBY', 'u4', '8', '-1']);
  assert.deepEqual(BitfieldOp.overflow(BitfieldOverflow.Sat).map(String), ['OVERFLOW', 'SAT']);
  assert.ok(Buffer.isBuffer(BitfieldOp.get('u8', 0)[0]));
});

test('typed constants are accepted by the commands', () => {
  assert.equal(typeof Commands.prototype.lmove, 'function');
});
