// SPDX-License-Identifier: MIT
// Cluster client edge cases over scripted fake nodes: unserved slots, blocking commands on
// dedicated connections, redirects with a failing refresh, pipelines and transactions before the
// topology is known, standalone fallbacks, encoding failures, dispose/duplicate/isolated and
// seed parsing.

import { test } from 'node:test';
import assert from 'node:assert/strict';
import type * as net from 'node:net';
import { ClusterClient, ConnectionError, RuvioError, createClusterClient, hashSlot } from '../src/index.js';
import { _forceStaleTable, keysOfFrame } from '../src/cluster.js';
import { Encoder, cmd } from '../src/encoder.js';
import { arr, bulk, err, fakeServer, int, ok, slotsReply, type FakeServer } from './fake-server.js';
import { rejects, sleep } from './helpers.js';

function keyIn(lo: number, hi: number, prefix = 'k', skip = 0): string {
  for (let i = 0; ; i++) {
    const k = prefix + i;
    const s = hashSlot(k);
    if (s >= lo && s <= hi && skip-- <= 0) return k;
  }
}

const LO = (p = 'k', skip = 0) => keyIn(0, 8191, p, skip);
const HI = (p = 'k', skip = 0) => keyIn(8192, 16383, p, skip);

type NodeHandler = (node: number, args: string[], sock: net.Socket) => string | null | undefined;

interface Rig {
  nodes: FakeServer[];
  /** Ranges as `[from, to, nodeIndex]` that CLUSTER SLOTS reports; mutable. */
  table: [number, number, number][];
  seed(): { host: string; port: number };
  close(): Promise<void>;
}

/**
 * `count` fake nodes. Without a custom table node 0 owns 0-8191 and node 1 8192-16383 (a single
 * node owns everything). `handler` may answer first; CLUSTER SLOTS is answered from `table`.
 */
async function rig(count: number, handler: NodeHandler = () => undefined, table?: [number, number, number][]): Promise<Rig> {
  const nodes: FakeServer[] = [];
  const r: Rig = {
    nodes,
    table: table ?? (count === 1 ? [[0, 16383, 0]] : [[0, 8191, 0], [8192, 16383, 1]]),
    seed: () => ({ host: '127.0.0.1', port: nodes[0]!.port }),
    close: async () => {
      for (const n of nodes) await n.close();
    },
  };
  for (let i = 0; i < count; i++) {
    nodes.push(
      await fakeServer((a, sock) => {
        const x = handler(i, a, sock);
        if (x !== undefined) return x;
        if (a[0] === 'CLUSTER') return slotsReply(r.table.map(([f, t, n]) => [f, t, nodes[n]!.port]));
        if (a[0] === 'GET') return bulk('n' + i + ':' + a[1]);
        return ok;
      }),
    );
  }
  return r;
}

async function withCluster(
  r: Rig,
  fn: (c: ClusterClient) => Promise<void>,
  opts: Partial<ConstructorParameters<typeof ClusterClient>[0] & object> = {},
): Promise<void> {
  const c = new ClusterClient({ ...r.seed(), ...opts });
  try {
    await fn(c);
  } finally {
    c.disconnect();
    await r.close();
  }
}

const blpopReply = arr(bulk('k'), bulk('v'));

// ---- unserved slots -------------------------------------------------------------------------

test('slots that no node serves fail with CLUSTERDOWN in commands, pipelines, split commands and transactions', async () => {
  const table: [number, number, number][] = [[0, 8191, 0]];
  const r = await rig(
    1,
    (_i, a) => (a[0] === 'EXEC' ? arr(ok, bulk('v')) : a[0] === 'MULTI' ? ok : a[0] === 'MGET' ? arr(...a.slice(1).map((k) => bulk('m' + k))) : undefined),
    table,
  );
  await withCluster(r, async (c) => {
    const lo = LO();
    const hi = HI();
    const lo2 = LO('m');
    assert.match(String(await rejects(c.get(hi))), /CLUSTERDOWN Hash slot \d+ is not served/);
    assert.equal(await c.get(lo), 'n0:' + lo);
    // MGET across slots with one of them unserved
    assert.match(String(await rejects(c.mget(lo, hi))), /CLUSTERDOWN/);
    assert.deepEqual(await c.mget(lo, lo2), ['m' + lo, 'm' + lo2]);
    // pipeline: the unserved command fails in its own slot, the others run
    const res = await c.pipeline().get(lo).get(hi).get(lo2).exec();
    assert.equal(res[0], 'n0:' + lo);
    assert.ok(res[1] instanceof RuvioError && /CLUSTERDOWN/.test(res[1].message));
    assert.equal(res[2], 'n0:' + lo2);
    // cross-slot command inside a pipeline
    const res2 = await c.pipeline().rename(lo, lo2).exec();
    assert.ok(res2[0] instanceof RuvioError && /CROSSSLOT/.test((res2[0] as Error).message));
    // transactions
    assert.match(String(await rejects(c.multi().set(hi, '1').exec())), /CLUSTERDOWN/);
    assert.match(String(await rejects(c.multi().set(lo, '1').set(lo2, '1').exec())), /CROSSSLOT/);
    assert.match(String(await rejects(c.multi().set(lo, '1').set(hi, '1').exec())), /CROSSSLOT|CLUSTERDOWN/);
    assert.deepEqual(await c.multi().set(lo, '1').get(lo).exec(), ['OK', 'v']);
    assert.deepEqual(await c.multi().dbsize().set(lo, '1').exec(), ['OK', 'v'], 'fan-out commands do not pin a slot');
    assert.deepEqual(await c.multi().ping().ping().exec(), ['OK', 'v'], 'keyless transaction runs on the seed');
    // lookups
    assert.throws(() => c.nodeFor(hi), /CLUSTERDOWN/);
    assert.equal(c.nodeFor(lo).port, r.nodes[0]!.port);
    assert.equal(c.slotOf(lo), hashSlot(lo));
  });
});

test('nodeFor before the topology is known asks for connect(); a standalone seed is its own node', async () => {
  const r = await rig(1);
  await withCluster(r, async (c) => {
    assert.throws(() => c.nodeFor('k'), /await connect/);
    assert.equal(c.isCluster, false);
    await c.connect();
    assert.equal(c.isCluster, true);
    assert.equal(c.nodeFor('k').port, r.nodes[0]!.port);
    assert.equal(c.nodes.length, 1);
  });
  const s = await fakeServer((a) => (a[0] === 'CLUSTER' ? err('ERR This instance has cluster support disabled') : undefined));
  const c = new ClusterClient({ host: '127.0.0.1', port: s.port });
  try {
    await c.connect();
    assert.equal(c.isCluster, false);
    assert.equal(c.nodeFor('anything').port, s.port);
    await c.refreshSlots(); // nothing to refresh on a standalone server
    assert.equal(c.refreshCount, 0);
  } finally {
    c.disconnect();
    await s.close();
  }
});

// ---- blocking commands ----------------------------------------------------------------------

test('blocking commands use dedicated connections that are reused, discarded on a transport failure, kept after a server error', async () => {
  let mode = 'ok';
  const r = await rig(2, (i, a, sock) => {
    if (a[0] !== 'BLPOP') return undefined;
    if (mode === 'ok') return blpopReply;
    if (mode === 'drop') return (sock.destroy(), null);
    if (mode === 'error') return err('ERR nope');
    if (mode === 'slow') {
      setTimeout(() => sock.write(blpopReply), 40);
      return '';
    }
    return undefined;
  });
  await withCluster(r, async (c) => {
    const lo = LO();
    assert.deepEqual(await c.blpop(lo, 1), ['k', 'v']);
    const node = c.nodeFor(lo);
    assert.equal(node.idle.length, 1, 'the dedicated connection goes back to the pool');
    assert.deepEqual(await c.blpop([lo], 1), ['k', 'v']);
    assert.equal(node.idle.length, 1, 'and is reused');
    assert.equal(r.nodes[0]!.count('BLPOP'), 2);

    mode = 'error';
    const e = await rejects(c.blpop(lo, 1));
    assert.ok(e instanceof RuvioError && !(e instanceof ConnectionError) && /nope/.test(e.message));
    assert.equal(node.idle.length, 1, 'a server error leaves the connection usable');

    mode = 'drop';
    const e2 = await rejects(c.blpop(lo, 1));
    assert.ok(e2 instanceof ConnectionError);
    assert.equal(node.idle.length, 0, 'a dropped connection is discarded');

    mode = 'ok';
    assert.deepEqual(await c.blpop(lo, 1), ['k', 'v']);
    assert.equal(node.idle.length, 1);

    // more concurrent blockers than the pool keeps: the surplus is closed
    mode = 'slow';
    const many = await Promise.all(Array.from({ length: 10 }, () => c.blpop(lo, 1)));
    assert.equal(many.length, 10);
    assert.equal(node.idle.length, 8);
    // binary and BLOCK-option commands take the same route
    mode = 'ok';
    assert.deepEqual(await c.binary.blpop(lo, 1), [Buffer.from('k'), Buffer.from('v')]);
  });
});

test('a MOVED answer to a blocking command is followed once on the owner', async () => {
  const lo = LO();
  let ownerPort = 0;
  const r = await rig(2, (i, a) => {
    if (a[0] !== 'BLPOP') return undefined;
    return i === 0 ? err(`MOVED ${hashSlot(a[1]!)} 127.0.0.1:${ownerPort}`) : blpopReply;
  });
  ownerPort = r.nodes[1]!.port;
  await withCluster(r, async (c) => {
    assert.deepEqual(await c.blpop(lo, 1), ['k', 'v']);
    assert.equal(c.redirectCount, 1);
    assert.equal(r.nodes[1]!.count('BLPOP'), 1);
    assert.equal(c.nodeFor(lo).port, ownerPort, 'the table now points at the new owner');
  });
});

// ---- redirects and refreshes ----------------------------------------------------------------

test('MOVED to an unknown node still retries there when the table refresh fails', async () => {
  const c2 = await fakeServer((a) => (a[0] === 'GET' ? bulk('C') : ok));
  let phase = 0;
  const r = await rig(1, (_i, a) => {
    if (a[0] === 'CLUSTER') return phase === 0 ? undefined : err('ERR cannot answer now');
    if (a[0] === 'GET') return phase === 0 ? bulk('A') : err(`MOVED ${hashSlot(a[1]!)} 127.0.0.1:${c2.port}`);
    return undefined;
  });
  try {
    await withCluster(r, async (c) => {
      assert.equal(await c.get('x'), 'A');
      phase = 1;
      assert.equal(await c.get('x'), 'C');
      assert.equal(c.refreshCount, 0, 'the refresh failed');
      assert.equal(c.redirectCount, 1);
    });
  } finally {
    await c2.close();
  }
});

test('refreshSlots rejects when no node can answer and can be retried', async () => {
  const r = await rig(2);
  const c = new ClusterClient(r.seed());
  try {
    await c.connect();
    await r.close();
    await rejects(c.refreshSlots());
    await rejects(c.refreshSlots()); // the failed attempt is not cached
    assert.equal(c.refreshCount, 0);
  } finally {
    c.disconnect();
  }
});

test('refreshSlots tries the other known nodes when the seed is gone and fails on an empty answer', async () => {
  let empty = false;
  const r = await rig(2, (_i, a) => (a[0] === 'CLUSTER' && empty ? arr() : undefined));
  const c = new ClusterClient(r.seed());
  try {
    await c.connect();
    await c.refreshSlots();
    assert.equal(c.refreshCount, 1);
    empty = true;
    const e = await rejects(c.refreshSlots());
    assert.match(String(e), /CLUSTERDOWN CLUSTER SLOTS returned no ranges/);
    // the seed dies: the second node answers instead
    empty = false;
    await r.nodes[0]!.close();
    await c.refreshSlots();
    assert.equal(c.refreshCount, 2);
  } finally {
    c.disconnect();
    await r.close();
  }
});

test('a topology that moves the seed away picks another home node and closes the dropped ones', async () => {
  const r = await rig(2);
  const c = new ClusterClient(r.seed());
  try {
    assert.equal(await c.get(LO()), 'n0:' + LO());
    r.table.splice(0, r.table.length, [0, 16383, 1]);
    await c.refreshSlots();
    assert.deepEqual(c.nodes.map((n) => n.port), [r.nodes[1]!.port]);
    assert.equal(await c.get(LO()), 'n1:' + LO());
    assert.equal(await c.ping(), 'OK', 'keyless commands go to the new home node');
  } finally {
    c.disconnect();
    await r.close();
  }
});

test('a stale table pointing at one node is corrected slot by slot by the MOVED replies, without a refresh', async () => {
  const r = await rig(2, (i, a) => (i === 0 && a[0] === 'GET' && hashSlot(a[1]!) > 8191 ? err(`MOVED ${hashSlot(a[1]!)} 127.0.0.1:${r.nodes[1]!.port}`) : undefined));
  const c = new ClusterClient(r.seed());
  try {
    await c.connect();
    _forceStaleTable(c, c.nodes[0]!);
    assert.equal(await c.get(HI()), 'n1:' + HI());
    assert.equal(await c.get(HI('z')), 'n1:' + HI('z'));
    assert.equal(c.redirectCount, 2);
    assert.equal(c.refreshCount, 0, 'every MOVED named a known node');
    assert.equal(await c.get(HI()), 'n1:' + HI());
    assert.equal(c.redirectCount, 2, 'the patched slot goes straight to its owner');
  } finally {
    c.disconnect();
    await r.close();
  }
});

// ---- split commands inside a pipeline ------------------------------------------------------

test('a pipeline splits cross-slot MGET / MSET / DEL / EXISTS / UNLINK / TOUCH like the plain commands', async () => {
  const r = await rig(2, (i, a) => {
    if (a[0] === 'MGET') return arr(...a.slice(1).map((k) => bulk('n' + i + ':' + k)));
    if (a[0] === 'DEL' || a[0] === 'EXISTS' || a[0] === 'UNLINK' || a[0] === 'TOUCH') return int(a.length - 1);
    return undefined;
  });
  await withCluster(r, async (c) => {
    const [lo, lo2, hi, hi2] = [LO(), LO('m'), HI(), HI('m')];
    await c.connect();
    const sent = (cmd: string) => r.nodes[0]!.count(cmd) + r.nodes[1]!.count(cmd);
    const res = await c
      .pipeline()
      .get(lo)
      .mget(hi, lo, hi2, lo2)
      .mset({ [lo]: '1', [hi]: '2' })
      .del(lo, hi, lo2)
      .exists(hi, lo)
      .unlink(lo, hi)
      .touch(hi2, lo2)
      .get(hi)
      .rename(lo, hi)
      .exec();
    assert.deepEqual(res.slice(0, 8), ['n0:' + lo, ['n1:' + hi, 'n0:' + lo, 'n1:' + hi2, 'n0:' + lo2], 'OK', 3, 2, 2, 2, 'n1:' + hi]);
    assert.ok(res[8] instanceof RuvioError && res[8].code === 'CROSSSLOT', 'other cross-slot commands still fail at their index');
    // one sub-command per slot, as for the plain commands
    assert.equal(sent('MGET'), 4);
    assert.equal(sent('MSET'), 2);
    assert.equal(sent('DEL'), 3);
    assert.equal(sent('RENAME'), 0);
    // a transaction must still stay in one slot
    assert.equal((await rejects(c.multi().mget(lo, hi).exec())).code, 'CROSSSLOT');
  });
});

// ---- pipelines and transactions before the topology is known -------------------------------

test('a pipeline built before the topology is loaded waits for it; execStrict throws the first error', async () => {
  const r = await rig(2, (_i, a) => (a[0] === 'SET' && a[1] === 'bad' ? err('ERR bad key') : undefined));
  await withCluster(r, async (c) => {
    const lo = LO();
    const hi = HI();
    assert.equal(c.isCluster, false);
    const res = await c.pipeline().get(lo).get(hi).exec();
    assert.deepEqual(res, ['n0:' + lo, 'n1:' + hi]);
    assert.deepEqual(await c.pipeline().exec(), []);
    assert.deepEqual(await c.pipeline().get(lo).execStrict(), ['n0:' + lo]);
    const e = await rejects(c.pipeline().get(lo).set('bad', '1').execStrict());
    assert.ok(e instanceof RuvioError && /bad key/.test(e.message));
  });
  const r2 = await rig(1);
  await rejects(
    (async () => {
      const dead = new ClusterClient({ ...r2.seed(), port: 1 });
      try {
        await dead.pipeline().get('a').exec();
      } finally {
        dead.disconnect();
      }
    })(),
  );
  await r2.close();
});

test('a transaction built before the topology is loaded waits for it', async () => {
  const r = await rig(2, (_i, a) => (a[0] === 'EXEC' ? arr(ok) : a[0] === 'MULTI' ? ok : a[0] === 'SET' ? '+QUEUED\r\n' : undefined));
  await withCluster(r, async (c) => {
    const hi = HI();
    assert.deepEqual(await c.multi().set(hi, '1').exec(), ['OK']);
    assert.equal(r.nodes[1]!.count('EXEC'), 1);
    assert.equal(r.nodes[0]!.count('EXEC'), 0);
  });
});

test('against a standalone server pipelines, transactions and plain commands use the single connection', async () => {
  const s = await fakeServer((a) => {
    if (a[0] === 'CLUSTER') return err('ERR This instance has cluster support disabled');
    if (a[0] === 'EXEC') return arr(ok, bulk('v'));
    if (a[0] === 'MULTI') return ok;
    if (a[0] === 'SET' || a[0] === 'GET') return a[0] === 'GET' ? bulk('g') : '+QUEUED\r\n';
    return undefined;
  });
  // before the mode is known, the first pipeline and the first transaction discover it
  for (const first of ['pipeline', 'multi', 'command'] as const) {
    const c = new ClusterClient({ host: '127.0.0.1', port: s.port });
    try {
      if (first === 'pipeline') assert.deepEqual(await c.pipeline().get('a').get('b').exec(), ['g', 'g']);
      else if (first === 'multi') assert.deepEqual(await c.multi().set('a', '1').get('a').exec(), ['OK', 'v']);
      else assert.equal(await c.get('a'), 'g');
      assert.equal(c.isCluster, false);
      assert.deepEqual(await c.pipeline().get('a').exec(), ['g']);
      assert.deepEqual(await c.multi().set('a', '1').get('a').exec(), ['OK', 'v']);
      assert.equal(await c.get('b'), 'g');
      assert.equal(await c.mget('x', 'y').then(() => 'ok'), 'ok');
    } finally {
      c.disconnect();
    }
  }
  await s.close();
});

test('strict cluster mode refuses a standalone seed', async () => {
  const s = await fakeServer((a) => (a[0] === 'CLUSTER' ? err('ERR This instance has cluster support disabled') : undefined));
  const c = new ClusterClient({ host: '127.0.0.1', port: s.port, cluster: true });
  try {
    const e = await rejects(c.get('a'));
    assert.ok(e instanceof RuvioError && /cluster support disabled/.test(e.message));
    assert.equal(c.isCluster, false);
  } finally {
    c.disconnect();
    await s.close();
  }
});

// ---- encoding failures ----------------------------------------------------------------------

test('an argument that cannot be encoded rejects that call only, in cluster and standalone mode', async () => {
  const bad = Number.NaN as never;
  const calls: [string, (c: ClusterClient) => Promise<unknown>][] = [
    ['0-arg-adjacent: ping(msg)', (c) => c.ping(bad)],
    ['hget', (c) => c.hget('h', bad)],
    ['hincrby', (c) => c.hincrby('h', 'f', bad)],
    ['fillget', (c) => c.fillget('k', bad, 5)],
    ['sadd', (c) => c.sadd('s', 'a', bad)],
    ['get', (c) => c.get(bad)],
    ['mget', (c) => c.mget('a', bad)],
  ];
  const r = await rig(1);
  const s = await fakeServer((a) => (a[0] === 'CLUSTER' ? err('ERR This instance has cluster support disabled') : undefined));
  try {
    for (const target of [r.seed(), { host: '127.0.0.1', port: s.port }]) {
      const c = new ClusterClient(target);
      try {
        await c.connect();
        for (const [name, f] of calls) {
          const e = await rejects(f(c) as Promise<unknown>);
          assert.ok(e instanceof Error, name);
          assert.equal(await c.ping(), 'OK', `the connection survives ${name}`);
        }
      } finally {
        c.disconnect();
      }
    }
    // before the topology is known (the non-direct, not-ready path)
    const c = new ClusterClient(r.seed());
    try {
      await rejects(c.get(bad) as Promise<unknown>);
      assert.equal(await c.get('ok'), 'n0:ok');
    } finally {
      c.disconnect();
    }
  } finally {
    await r.close();
    await s.close();
  }
});

test('a value larger than one encoder chunk is sent intact', async () => {
  let got = 0;
  const r = await rig(2, (_i, a) => {
    if (a[0] === 'SET') {
      got = a[2]!.length;
      return ok;
    }
    return undefined;
  });
  await withCluster(r, async (c) => {
    await c.connect();
    const big = 'x'.repeat(3_000_000);
    assert.equal(await c.set(LO(), big), 'OK');
    assert.equal(got, big.length);
    assert.equal(await c.set(HI(), Buffer.alloc(2_000_000, 'y')), 'OK');
    assert.equal(got, 2_000_000);
  });
});

// ---- accessors, lifecycle and helpers -------------------------------------------------------

test('duplicate, binary view, inFlight, nodes, asyncDispose and quit', async () => {
  const r = await rig(2);
  const c = new ClusterClient({ ...r.seed(), name: 'orig' });
  try {
    const d = c.duplicate({ name: 'dup' });
    assert.equal(d.options.name, 'dup');
    assert.equal(d.options.port, r.seed().port);
    assert.notEqual(d.r, c.r);
    assert.equal(c.binary, c.binary, 'the binary view is cached');
    assert.deepEqual(await c.binary.get(LO()), Buffer.from('n0:' + LO()));
    assert.equal(c.inFlight, 0);
    await d[Symbol.asyncDispose](); // never connected: a no-op quit
    await c.get(HI());
    assert.equal(c.nodes.length, 2);
    await c[Symbol.asyncDispose]();
    await rejects(c.get(LO()));
  } finally {
    c.disconnect();
    await r.close();
  }
});

test('quit swallows errors from nodes that are already gone', async () => {
  const r = await rig(2);
  const c = new ClusterClient(r.seed());
  try {
    await c.get(LO());
    await c.get(HI());
    await r.close();
    await c.quit();
  } finally {
    c.disconnect();
  }
});

test('isolated runs on a dedicated connection to the owner and closes it', async () => {
  const r = await rig(2);
  await withCluster(r, async (c) => {
    const hi = HI();
    const seen = await c.isolated(hi, async (client) => {
      assert.equal(await client.get(hi), 'n1:' + hi);
      return client.options.port;
    });
    assert.equal(seen, r.nodes[1]!.port);
    await rejects(c.isolated(hi, async () => Promise.reject(new Error('inside'))));
    assert.equal(r.nodes[1]!.count('GET'), 1);
  });
});

test('subscriber() uses the seed node; subscriberFor() the owner of the channel', async () => {
  const r = await rig(2, (_i, a) => (a[0] === 'SSUBSCRIBE' ? '*3\r\n' + bulk('ssubscribe') + bulk(a[1]!) + int(1) : undefined));
  await withCluster(r, async (c) => {
    const s0 = c.subscriber({ reconnectDelayMs: 5 });
    try {
      await s0.connect();
      assert.equal(r.nodes[0]!.count('PING'), 1);
    } finally {
      s0.close();
    }
    const hi = HI();
    const s1 = await c.subscriberFor(hi);
    try {
      assert.equal(await s1.ssubscribe(hi), 1);
      assert.equal(r.nodes[1]!.count('SSUBSCRIBE'), 1);
      assert.equal(r.nodes[0]!.count('SSUBSCRIBE'), 0);
    } finally {
      s1.close();
    }
  });
});

test('scanIterator visits every node in turn', async () => {
  const r = await rig(2, (i, a) => (a[0] === 'SCAN' ? arr(bulk('0'), arr(bulk('key' + i))) : undefined));
  await withCluster(r, async (c) => {
    const seen: string[] = [];
    for await (const k of c.scanIterator()) seen.push(k);
    assert.deepEqual(seen, ['key0', 'key1']);
  });
});

test('fan-out commands merge replies from every node (DBSIZE, KEYS, FLUSHDB)', async () => {
  const r = await rig(2, (i, a) => (a[0] === 'DBSIZE' ? int(i + 1) : a[0] === 'KEYS' ? arr(bulk('a' + i)) : undefined));
  await withCluster(r, async (c) => {
    assert.equal(await c.dbsize(), 3);
    assert.deepEqual((await c.keys('*')).sort(), ['a0', 'a1']);
    assert.equal(await c.flushdb(), 'OK');
    assert.equal(r.nodes[1]!.count('FLUSHDB'), 1);
  });
});

// ---- seed parsing ---------------------------------------------------------------------------

test('a URL seed carries credentials, TLS scheme and port', async () => {
  const c1 = createClusterClient('redis://us%40er:p%3Ass@example.test:7000');
  assert.equal(c1.options.host, 'example.test');
  assert.equal(c1.options.port, 7000);
  assert.equal(c1.options.username, 'us@er');
  assert.equal(c1.options.password, 'p:ss');
  assert.equal(c1.options.tls, undefined);
  const c2 = createClusterClient('rediss://:secret@example.test');
  assert.equal(c2.options.port, 6379);
  assert.equal(c2.options.password, 'secret');
  assert.equal(c2.options.username, undefined);
  assert.equal(c2.options.tls, true);
  assert.equal(createClusterClient('ruvios://example.test').options.tls, true);
  assert.equal(createClusterClient().options.host, undefined);
  assert.throws(() => createClusterClient('not a url'));
});

// ---- key extraction -------------------------------------------------------------------------

function keysOf(...w: (string | number)[]): string[] {
  const e = new Encoder(null, 1024);
  e.begin(cmd(String(w[0])), w.length - 1);
  for (const x of w.slice(1)) e.arg(x);
  e.endFrame();
  return keysOfFrame(e.take());
}

test('key extraction handles shapes without keys', () => {
  assert.deepEqual(keysOf('XREAD', 'COUNT', 1), [], 'no STREAMS keyword');
  assert.deepEqual(keysOf('XREAD', 'STREAMS', 'a', 'b', '0', '0'), ['a', 'b']);
  assert.deepEqual(keysOf('XINFO', 'STREAM'), [], 'a sub-command without its key');
  assert.deepEqual(keysOf('XINFO', 'STREAM', 'k'), ['k']);
  assert.deepEqual(keysOf('OBJECT', 'FREQ', 'k'), ['k']);
});
