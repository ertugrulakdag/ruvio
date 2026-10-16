// SPDX-License-Identifier: MIT
// The RESP wire vectors in clients/testdata/resp.json are shared by every client, so the six
// encoders and parsers cannot drift apart.

import { test } from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { Encoder, cmd, Parser, NEED_MORE, RuvioError } from '../src/index.js';

interface Vector {
  name: string;
  args?: string[];
  args_hex?: string[];
  bytes?: string;
  bytes_hex?: string;
  value?: Shape;
}

type Shape =
  | { simple: string }
  | { error: string }
  | { int: number | string }
  | { bulk: string }
  | { bulk_hex: string }
  | { null: true }
  | { array: Shape[] };

const here = dirname(fileURLToPath(import.meta.url));
const file = join(here, '..', '..', '..', 'testdata', 'resp.json');
const vectors = JSON.parse(readFileSync(file, 'utf8')) as { encode: Vector[]; decode: Vector[]; incomplete: Vector[] };

function wire(v: Vector): Buffer {
  return v.bytes_hex !== undefined ? Buffer.from(v.bytes_hex, 'hex') : Buffer.from(v.bytes!, 'utf8');
}

function encode(args: (string | Buffer)[]): Buffer {
  const e = new Encoder(null, 16); // tiny slab: exercises growth
  e.begin(cmd(args[0] as string), args.length - 1);
  for (let i = 1; i < args.length; i++) e.arg(args[i]!);
  e.endFrame();
  return Buffer.from(e.take());
}

/**
 * The neutral shape as this parser returns it: strings in text mode, Buffers in binary mode.
 * `+OK` stays the string 'OK' in binary mode too, so binary commands still resolve to 'OK'.
 */
function expected(s: Shape, binary: boolean): unknown {
  if ('simple' in s) return binary && s.simple !== 'OK' ? Buffer.from(s.simple, 'utf8') : s.simple;
  if ('error' in s) return new RuvioError(s.error);
  if ('int' in s) {
    const n = BigInt(s.int);
    return Number.isSafeInteger(Number(n)) ? Number(n) : n;
  }
  if ('bulk' in s) return binary ? Buffer.from(s.bulk, 'utf8') : s.bulk;
  if ('bulk_hex' in s) return Buffer.from(s.bulk_hex, 'hex');
  if ('null' in s) return null;
  return s.array.map((item) => expected(item, binary));
}

function hasHex(s: Shape): boolean {
  return 'bulk_hex' in s || ('array' in s && s.array.some(hasHex));
}

/** Errors compare by class and message; deepEqual alone ignores the message of an Error. */
function same(got: unknown, want: unknown, name: string): void {
  if (want instanceof RuvioError) {
    assert.ok(got instanceof RuvioError, `${name}: expected an error, got ${String(got)}`);
    assert.equal(got.message, want.message, name);
  } else if (Array.isArray(want)) {
    assert.ok(Array.isArray(got), `${name}: expected an array`);
    assert.equal(got.length, want.length, name);
    want.forEach((w, i) => same(got[i], w, name));
  } else {
    assert.deepEqual(got, want, name);
  }
}

test('encoder writes the shared vectors', () => {
  assert.ok(vectors.encode.length > 0);
  for (const v of vectors.encode) {
    const want = wire(v);
    if (v.args !== undefined) {
      assert.deepEqual(encode(v.args), want, `${v.name}: string args`);
      const bin = v.args.map((a) => Buffer.from(a, 'utf8'));
      assert.deepEqual(encode([v.args[0]!, ...bin.slice(1)]), want, `${v.name}: Buffer args`);
    } else {
      const bin = v.args_hex!.map((a) => Buffer.from(a, 'hex'));
      assert.deepEqual(encode([bin[0]!.toString('latin1'), ...bin.slice(1)]), want, v.name);
    }
  }
});

test('parser reads the shared vectors', () => {
  assert.ok(vectors.decode.length > 0);
  for (const v of vectors.decode) {
    // Text mode cannot carry bytes that are not UTF-8, so those vectors run in binary mode only.
    for (const binary of hasHex(v.value!) ? [true] : [false, true]) {
      const p = new Parser();
      p.binary = binary;
      p.feed(wire(v));
      same(p.next(), expected(v.value!, binary), `${v.name} (binary ${binary})`);
      assert.equal(p.next(), NEED_MORE, `${v.name}: bytes left over`);
      assert.ok(p.idle, `${v.name}: parser not idle`);
    }
  }
});

test('parser asks for more data on an incomplete reply', () => {
  assert.ok(vectors.incomplete.length > 0);
  for (const v of vectors.incomplete) {
    const p = new Parser();
    p.feed(wire(v));
    assert.equal(p.next(), NEED_MORE, v.name);
  }
});
