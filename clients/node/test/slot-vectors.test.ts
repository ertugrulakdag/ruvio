// SPDX-License-Identifier: MIT
// The key to hash slot vectors in clients/testdata/slots.json are shared by the server and
// every client.

import { test } from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { hashSlot } from '../src/index.js';

test('hashSlot matches the shared vectors', () => {
  const here = dirname(fileURLToPath(import.meta.url));
  const file = join(here, '..', '..', '..', 'testdata', 'slots.json');
  const vectors = JSON.parse(readFileSync(file, 'utf8')) as { keys: { key: string; slot: number }[] };

  assert.ok(vectors.keys.length > 0);
  for (const { key, slot } of vectors.keys) {
    assert.equal(hashSlot(key), slot, key);
    assert.equal(hashSlot(new TextEncoder().encode(key)), slot, key);
  }
});
