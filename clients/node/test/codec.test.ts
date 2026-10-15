// SPDX-License-Identifier: MIT
// Unit tests for the RESP encoder and the incremental parser (no server needed).

import { test } from 'node:test';
import assert from 'node:assert/strict';
import { Encoder, cmd, Parser, NEED_MORE, PushMessage, RuvioError } from '../src/index.js';

function encode(name: string[], args: (string | Buffer | number | bigint)[]): Buffer {
  const e = new Encoder(null, 16); // tiny slab: exercises growth
  e.begin(cmd(...name), args.length);
  for (const a of args) e.arg(a);
  e.endFrame();
  return Buffer.from(e.take());
}

function reference(words: (string | Buffer)[]): Buffer {
  const parts: Buffer[] = [Buffer.from('*' + words.length + '\r\n')];
  for (const w of words) {
    const b = typeof w === 'string' ? Buffer.from(w, 'utf8') : w;
    parts.push(Buffer.from('$' + b.length + '\r\n'), b, Buffer.from('\r\n'));
  }
  return Buffer.concat(parts);
}

test('encoder: strings of every digit-boundary length, ASCII and multi-byte', () => {
  for (const len of [0, 1, 9, 10, 33, 34, 99, 100, 333, 334, 999, 1000, 4096, 4097, 70000]) {
    for (const ch of ['a', 'é', '€', '😀']) {
      const s = ch.repeat(Math.ceil(len / ch.length)).slice(0, len);
      assert.deepEqual(encode(['SET'], ['k', s]), reference(['SET', 'k', s]), `len ${len} ch ${ch}`);
    }
  }
});

test('encoder: integers, negatives, floats, infinities, bigint', () => {
  const nums = [0, 1, 9, 10, -1, -10, 123456789, 4294967295, 4294967296, -4294967297, Number.MAX_SAFE_INTEGER, -Number.MAX_SAFE_INTEGER, 1.5, -0.25, 1e21];
  for (const n of nums) assert.deepEqual(encode(['X'], [n]), reference(['X', String(n)]), String(n));
  assert.deepEqual(encode(['X'], [Infinity, -Infinity]), reference(['X', '+inf', '-inf']));
  assert.deepEqual(encode(['X'], [12345678901234567890n]), reference(['X', '12345678901234567890']));
  assert.throws(() => encode(['X'], [NaN]));
});

test('encoder: buffers, subcommands, many arguments', () => {
  const bin = Buffer.from([0, 255, 13, 10, 36]);
  assert.deepEqual(encode(['SET'], ['k', bin]), reference(['SET', 'k', bin]));
  assert.deepEqual(encode(['XGROUP', 'CREATE'], ['s', 'g', '$']), reference(['XGROUP', 'CREATE', 's', 'g', '$']));
  const many = Array.from({ length: 200 }, (_, i) => 'a' + i);
  assert.deepEqual(encode(['DEL'], many), reference(['DEL', ...many]));
});

test('encoder: large buffers go to the sink without a copy, in order', () => {
  const chunks: Buffer[] = [];
  const e = new Encoder((c) => chunks.push(Buffer.from(c)));
  const big = Buffer.alloc(100_000, 1);
  e.begin(cmd('SET'), 2);
  e.arg('k');
  e.arg(big);
  e.endFrame();
  e.begin(cmd('GET'), 1);
  e.arg('k');
  e.endFrame();
  e.flush();
  assert.deepEqual(Buffer.concat(chunks), Buffer.concat([reference(['SET', 'k', big]), reference(['GET', 'k'])]));
});

function parseAll(bytes: Buffer, split: number[] = [], binary = false): unknown[] {
  const p = new Parser();
  p.binary = binary;
  const out: unknown[] = [];
  let last = 0;
  for (const s of [...split, bytes.length]) {
    const chunk = Buffer.from(bytes.subarray(last, s)); // separate buffer each feed
    last = s;
    p.feed(chunk);
    for (;;) {
      const v = p.next();
      if (v === NEED_MORE) break;
      out.push(v);
    }
  }
  return out;
}

const SAMPLE = Buffer.from(
  '+OK\r\n+PONG\r\n:42\r\n:-7\r\n$5\r\nhello\r\n$-1\r\n$0\r\n\r\n*3\r\n:1\r\n$2\r\nhi\r\n*-1\r\n' +
    '*2\r\n*2\r\n$1\r\na\r\n$1\r\nb\r\n*0\r\n-ERR bad thing\r\n-WRONGTYPE nope\r\n:12345678901234567890\r\n' +
    '$4\r\néé\r\n',
  'utf8',
);
const SAMPLE_EXPECT: unknown[] = [
  'OK', 'PONG', 42, -7, 'hello', null, '', [1, 'hi', null], [['a', 'b'], []],
  'ERR bad thing', 'WRONGTYPE nope', 12345678901234567890n, 'éé',
];

function normalize(v: unknown[]): unknown[] {
  return v.map((x) => (x instanceof RuvioError ? x.message : x));
}

test('parser: every RESP2 type in one buffer', () => {
  assert.deepEqual(normalize(parseAll(SAMPLE)), SAMPLE_EXPECT);
});

test('parser: resumes correctly when split at every byte boundary', () => {
  for (let i = 1; i < SAMPLE.length; i++) {
    assert.deepEqual(normalize(parseAll(SAMPLE, [i])), SAMPLE_EXPECT, `split at ${i}`);
  }
  // Byte-by-byte feed.
  const all = Array.from({ length: SAMPLE.length - 1 }, (_, i) => i + 1);
  assert.deepEqual(normalize(parseAll(SAMPLE, all)), SAMPLE_EXPECT);
});

test('parser: error codes', () => {
  const [e] = parseAll(Buffer.from('-NOSCRIPT No matching script\r\n')) as RuvioError[];
  assert.ok(e instanceof RuvioError);
  assert.equal(e!.code, 'NOSCRIPT');
});

test('parser: large bulk and large array across many chunks', () => {
  const big = 'z'.repeat(1_000_000);
  const items = Array.from({ length: 50_000 }, (_, i) => 'item' + i);
  const bytes = Buffer.concat([
    Buffer.from('$' + big.length + '\r\n' + big + '\r\n'),
    Buffer.from('*' + items.length + '\r\n' + items.map((s) => '$' + s.length + '\r\n' + s + '\r\n').join('')),
  ]);
  const splits: number[] = [];
  for (let s = 1000; s < bytes.length; s += 65536) splits.push(s);
  const out = parseAll(bytes, splits);
  assert.equal(out.length, 2);
  assert.equal(out[0], big);
  assert.deepEqual(out[1], items);
});

test('parser: binary mode returns Buffer copies', () => {
  const out = parseAll(Buffer.from('$3\r\n\x00\xff\x01\r\n', 'latin1'), [], true);
  assert.ok(Buffer.isBuffer(out[0]));
  assert.deepEqual(out[0], Buffer.from([0, 255, 1]));
});

test('parser: RESP3 types', () => {
  const bytes = Buffer.from(
    '_\r\n#t\r\n#f\r\n,3.5\r\n,inf\r\n(123456789012345678901234\r\n%2\r\n$1\r\na\r\n:1\r\n$1\r\nb\r\n:2\r\n' +
      '~2\r\n:1\r\n:2\r\n=8\r\ntxt:abcd\r\n!5\r\nERR x\r\n>3\r\n$10\r\ninvalidate\r\n*1\r\n$1\r\nk\r\n$0\r\n\r\n' +
      '|1\r\n+key\r\n+val\r\n:5\r\n',
  );
  for (let i = 1; i < bytes.length; i += 3) {
    const out = parseAll(bytes, [i]);
    assert.equal(out[0], null);
    assert.equal(out[1], true);
    assert.equal(out[2], false);
    assert.equal(out[3], 3.5);
    assert.equal(out[4], Infinity);
    assert.equal(out[5], 123456789012345678901234n);
    assert.deepEqual(out[6], { a: 1, b: 2 });
    assert.deepEqual(out[7], [1, 2]);
    assert.equal(out[8], 'abcd');
    assert.ok(out[9] instanceof RuvioError);
    assert.ok(out[10] instanceof PushMessage);
    assert.deepEqual((out[10] as PushMessage).data, ['invalidate', ['k'], '']);
    assert.equal(out[11], 5, 'attribute is skipped');
    assert.equal(out.length, 12);
  }
});

test('parser: reused read buffer (caller overwrites chunk between feeds)', () => {
  const p = new Parser();
  const shared = Buffer.alloc(8);
  const bytes = Buffer.from('$11\r\nhello world\r\n:5\r\n');
  const out: unknown[] = [];
  for (let i = 0; i < bytes.length; i += 8) {
    const n = bytes.copy(shared, 0, i, Math.min(i + 8, bytes.length));
    p.feed(shared, n);
    for (let v = p.next(); v !== NEED_MORE; v = p.next()) out.push(v);
    shared.fill(0x41); // clobber, as a socket read buffer would be
  }
  assert.deepEqual(out, ['hello world', 5]);
});
