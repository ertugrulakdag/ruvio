// SPDX-License-Identifier: MIT
// Routing cost: plain single-connection client vs cluster client, CPU per request.
//
//   RUVIO_BENCH_ADDR=127.0.0.1:17800 npm run bench:cluster-cost
//
// The address is the seed of a per-shard server. "plain" connects there without discovery (the
// server forwards misrouted keys); "cluster" discovers the shards; "one-node" is the cluster client
// with every key in one hash tag (the routing code's own cost). W workers, each with its own client,
// loop over consecutive slices of 4096 keys. Pipeline depths 16 and 64, plus plain depth 4 (the
// per-socket batch a depth-16 pipeline leaves on each of four nodes); about a minute.

import { RuvioClient, ClusterClient } from '../src/index.js';

const addr = process.env.RUVIO_BENCH_ADDR;
if (!addr) {
  console.log('RUVIO_BENCH_ADDR not set: cluster-cost benchmark skipped');
  process.exit(0);
}
const host = addr.split(':')[0]!;
const port = Number(addr.split(':')[1]);
const W = Number(process.env.BENCH_WORKERS ?? 8);
const SECONDS = Number(process.env.BENCH_SECONDS ?? 2);
const WARM_MS = Number(process.env.BENCH_WARMUP_MS ?? 500);
const KEYS = Array.from({ length: 4096 }, (_, i) => 'key:' + i);
const TAGGED = KEYS.map((k) => '{t}' + k);
const VALUE = 'x'.repeat(32);

type Client = RuvioClient | ClusterClient;

async function worker(c: Client, w: number, op: 'GET' | 'SET', p: number, keys: string[], until: () => boolean, count: { n: number }): Promise<void> {
  const slice = keys.length / W;
  const base = w * slice;
  let i = 0;
  while (!until()) {
    if (p === 1) {
      const k = keys[base + (i++ % slice)]!;
      if (op === 'GET') await c.get(k);
      else await c.set(k, VALUE);
      count.n++;
    } else {
      const pl = c.pipeline();
      for (let j = 0; j < p; j++) {
        const k = keys[base + (i++ % slice)]!;
        if (op === 'GET') pl.get(k);
        else pl.set(k, VALUE);
      }
      await pl.exec();
      count.n += p;
    }
  }
}

async function row(kind: string, clients: Client[], op: 'GET' | 'SET', p: number, keys: string[]): Promise<void> {
  const count = { n: 0 };
  let stop = false;
  const warmEnd = Date.now() + WARM_MS;
  await Promise.all(clients.map((c, w) => worker(c, w, op, p, keys, () => Date.now() >= warmEnd, { n: 0 })));
  const runs = clients.map((c, w) => worker(c, w, op, p, keys, () => stop, count));
  const c0 = process.cpuUsage();
  const t0 = process.hrtime.bigint();
  const n0 = count.n;
  await new Promise((r) => setTimeout(r, SECONDS * 1000));
  const n1 = count.n;
  const c1 = process.cpuUsage(c0);
  const secs = Number(process.hrtime.bigint() - t0) / 1e9;
  stop = true;
  await Promise.all(runs);
  const ops = n1 - n0;
  console.log(
    `${kind.padEnd(8)} ${op} P=${p} ops/s=${Math.round(ops / secs)} user_ns/op=${Math.round((c1.user * 1000) / ops)} sys_ns/op=${Math.round((c1.system * 1000) / ops)}`,
  );
}

const plain: Client[] = [];
const cluster: Client[] = [];
for (let w = 0; w < W; w++) {
  const c = new RuvioClient({ host, port });
  await c.connect();
  plain.push(c);
  const d = new ClusterClient({ host, port });
  await d.connect();
  cluster.push(d);
}
// Populate so GET hits.
for (const keys of [KEYS, TAGGED]) {
  const pl = plain[0]!.pipeline();
  for (const k of keys) pl.set(k, VALUE);
  await pl.exec();
}
// Optional filter (e.g. for profiling): BENCH_OPS=GET
const ops = (process.env.BENCH_OPS ?? 'GET,SET').split(',') as ('GET' | 'SET')[];
for (const op of ops) {
  await row('plain', plain, op, 4, KEYS);
  for (const p of [16, 64]) {
    await row('plain', plain, op, p, KEYS);
    await row('cluster', cluster, op, p, KEYS);
    await row('one-node', cluster, op, p, TAGGED);
  }
}
for (const c of [...plain, ...cluster]) await c.quit();
