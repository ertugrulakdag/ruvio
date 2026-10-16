// SPDX-License-Identifier: MIT
// Routing cost: plain single-connection client vs cluster client, CPU per request.
//
//   RUVIO_BENCH_ADDR=127.0.0.1:17800 npm run bench:cluster-cost
//
// The address is the seed of a per-shard server. "plain" connects there without discovery (the
// server forwards misrouted keys); "cluster" discovers the shards. W workers, each with its own
// client, loop over consecutive slices of 4096 keys; P=1 uses get/set, P>1 the pipeline API.

import { RuvioClient, ClusterClient } from '../src/index.js';

const addr = process.env.RUVIO_BENCH_ADDR;
if (!addr) {
  console.log('RUVIO_BENCH_ADDR not set: cluster-cost benchmark skipped');
  process.exit(0);
}
const host = addr.split(':')[0]!;
const port = Number(addr.split(':')[1]);
const W = Number(process.env.BENCH_WORKERS ?? 8);
const SECONDS = Number(process.env.BENCH_SECONDS ?? 4);
const WARM_MS = Number(process.env.BENCH_WARMUP_MS ?? 1000);
// BENCH_TAG puts every key in one hash tag, so the cluster rows measure routing on one node.
const TAG = process.env.BENCH_TAG ? '{' + process.env.BENCH_TAG + '}' : '';
const KEYS = Array.from({ length: 4096 }, (_, i) => TAG + 'key:' + i);
const VALUE = 'x'.repeat(32);

type Client = RuvioClient | ClusterClient;

async function worker(c: Client, w: number, op: 'GET' | 'SET', p: number, until: () => boolean, count: { n: number }): Promise<void> {
  const slice = KEYS.length / W;
  const base = w * slice;
  let i = 0;
  while (!until()) {
    if (p === 1) {
      const k = KEYS[base + (i++ % slice)]!;
      if (op === 'GET') await c.get(k);
      else await c.set(k, VALUE);
      count.n++;
    } else {
      const pl = c.pipeline();
      for (let j = 0; j < p; j++) {
        const k = KEYS[base + (i++ % slice)]!;
        if (op === 'GET') pl.get(k);
        else pl.set(k, VALUE);
      }
      await pl.exec();
      count.n += p;
    }
  }
}

async function row(kind: 'plain' | 'cluster', clients: Client[], op: 'GET' | 'SET', p: number): Promise<void> {
  const count = { n: 0 };
  let stop = false;
  const warmEnd = Date.now() + WARM_MS;
  await Promise.all(clients.map((c, w) => worker(c, w, op, p, () => Date.now() >= warmEnd, { n: 0 })));
  const runs = clients.map((c, w) => worker(c, w, op, p, () => stop, count));
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
    `${kind} ${op} P=${p} ops/s=${Math.round(ops / secs)} user_ns/op=${Math.round((c1.user * 1000) / ops)} sys_ns/op=${Math.round((c1.system * 1000) / ops)}`,
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
{
  const pl = plain[0]!.pipeline();
  for (const k of KEYS) pl.set(k, VALUE);
  await pl.exec();
}
// Optional filters (e.g. for profiling): BENCH_OPS=GET BENCH_DEPTHS=1,16 BENCH_KINDS=cluster
const ops = (process.env.BENCH_OPS ?? 'GET,SET').split(',') as ('GET' | 'SET')[];
const depths = (process.env.BENCH_DEPTHS ?? '1,16,64').split(',').map(Number);
const kinds = process.env.BENCH_KINDS ?? 'plain,cluster';
for (const op of ops) {
  for (const p of depths) {
    if (kinds.includes('plain')) await row('plain', plain, op, p);
    if (kinds.includes('cluster')) await row('cluster', cluster, op, p);
  }
}
for (const c of [...plain, ...cluster]) await c.quit();
