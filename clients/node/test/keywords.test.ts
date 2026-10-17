// SPDX-License-Identifier: MIT
// Keyword constants: value tables and pre-encoded bytes, without a server.

import { test } from 'node:test';
import assert from 'node:assert/strict';
import {
  Aggregate, BitOp, BitUnit, BitfieldOp, BitfieldOverflow, Encoder, GeoOrder, GeoUnit, InsertPosition, ListSide,
  ScanType, ClientType, InfoSection, LexBound, ScoreBound, StreamId, StreamTrim, TrimMode, TrimStrategy, cmd,
} from '../src/index.js';
import {
  KW_BITOP, KW_CLIENT_TYPE, KW_GEO_UNIT, KW_INFO_SECTION, KW_SCAN_TYPE, KW_SIDE, KW_TRIM_MODE, KW_TRIM_STRATEGY, KW_WHERE, kw,
} from '../src/keywords.js';
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

const frameOf = (name: string, args: unknown[]): string => {
  const enc = new Encoder();
  enc.begin(cmd(name), args.length);
  for (const a of args) enc.arg(a as string | Buffer | number);
  return Buffer.from(enc.take()).toString('latin1');
};

test('stream trim, info and client type encode like the string path', () => {
  const typed: [StreamTrim, unknown[]][] = [
    [StreamTrim.maxLen(10), ['MAXLEN', 10]],
    [StreamTrim.maxLen(10, TrimMode.Approximate), ['MAXLEN', '~', 10]],
    [StreamTrim.minId('5-0', TrimMode.Exact), ['MINID', '=', '5-0']],
    [StreamTrim.of(TrimStrategy.MinId, '5-0'), ['MINID', '5-0']],
  ];
  for (const [t, words] of typed) {
    const a: unknown[] = ['s', kw(KW_TRIM_STRATEGY, t.strategy)];
    if (t.mode) a.push(kw(KW_TRIM_MODE, t.mode));
    a.push(t.threshold);
    assert.equal(frameOf('XTRIM', a), frameOf('XTRIM', ['s', ...words]));
  }
  for (const v of Object.values(InfoSection)) assert.equal((kw(KW_INFO_SECTION, v) as Buffer).toString('latin1'), v);
  assert.equal(kw(KW_INFO_SECTION, 'Memory'), 'Memory', 'unknown spelling passes through');
  for (const v of Object.values(ClientType)) assert.equal((kw(KW_CLIENT_TYPE, v) as Buffer).toString('latin1'), v);
  assert.equal(frameOf('INFO', [kw(KW_INFO_SECTION, InfoSection.MemoryBreakdown)]), frameOf('INFO', ['memorybreakdown']));
});

test('stream id and range bound helpers', () => {
  assert.deepEqual(Object.values(StreamId), ['*', '$', '>', '-', '+']);
  assert.equal(ScoreBound.NegativeInfinity, '-inf');
  assert.equal(ScoreBound.PositiveInfinity, '+inf');
  assert.equal(ScoreBound.inclusive(1.5), '1.5');
  assert.equal(ScoreBound.exclusive(100), '(100');
  assert.equal(LexBound.inclusive('a'), '[a');
  assert.equal(LexBound.exclusive('b'), '(b');
  assert.equal(LexBound.Min + LexBound.Max, '-+');
});
