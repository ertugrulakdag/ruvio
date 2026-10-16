// SPDX-License-Identifier: MIT
// The command to key vectors in clients/testdata/key_vectors.json come from the server parser
// and are shared by every client: cluster routing must find exactly the keys the server uses.

import { test } from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { keysOfFrame } from '../src/cluster.js';
import { Encoder, cmd } from '../src/encoder.js';

// The cluster client routes sharded Pub/Sub by channel through the same key lookup; the server
// lists no keys for them.
const CHANNEL_COMMANDS = new Set(['SPUBLISH', 'SSUBSCRIBE', 'SUNSUBSCRIBE']);

const byString = (a: string, b: string) => (a < b ? -1 : a > b ? 1 : 0);

test('cluster key extraction matches the shared key vectors', () => {
  const here = dirname(fileURLToPath(import.meta.url));
  const file = join(here, '..', '..', '..', 'testdata', 'key_vectors.json');
  const { vectors } = JSON.parse(readFileSync(file, 'utf8')) as { vectors: { args: string[]; keys: string[] }[] };

  assert.ok(vectors.length > 0);
  const e = new Encoder(null, 1024);
  let checked = 0;
  for (const { args, keys } of vectors) {
    if (CHANNEL_COMMANDS.has(args[0]!.toUpperCase())) continue;
    e.begin(cmd(args[0]!), args.length - 1);
    for (let i = 1; i < args.length; i++) e.arg(args[i]!);
    e.endFrame();
    assert.deepEqual(keysOfFrame(e.take()).sort(byString), keys, args.join(' '));
    checked++;
  }
  assert.ok(checked > 1000, 'checked ' + checked);
});
