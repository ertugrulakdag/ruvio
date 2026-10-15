// SPDX-License-Identifier: MIT
// Micro-benchmark: encode/parse ns and approximate bytes allocated per op, then a short live
// GET/SET run (serial and 128 concurrent) against RUVIO_TEST_ADDR, optionally next to ioredis.
//
//   npm run bench   (= node --expose-gc --min-semi-space-size=64 --max-semi-space-size=64 build/bench/bench.js)
//   RUVIO_TEST_ADDR=127.0.0.1:6379 BENCH_SECONDS=1.5 node --expose-gc build/bench/bench.js

import { Encoder, cmd, Parser, NEED_MORE, RuvioClient, ClusterClient } from '../src/index.js';

const gc = (globalThis as { gc?: () => void }).gc;
const SECONDS = Number(process.env.BENCH_SECONDS ?? 1.5);

function micro(name: string, n: number, fn: () => void): void {
  for (let i = 0; i < 300_000; i++) fn(); // warm-up / optimize
  const t0 = process.hrtime.bigint();
  for (let i = 0; i < n; i++) fn();
  const ns = Number(process.hrtime.bigint() - t0) / n;
  // Allocation: a short window after a full GC. Run with a large young generation
  // (--min-semi-space-size/--max-semi-space-size) so no scavenge lands inside it.
  let bytes = NaN;
  if (gc) {
    const w = 20_000;
    gc();
    const h0 = process.memoryUsage().heapUsed;
    for (let i = 0; i < w; i++) fn();
    bytes = Math.max(0, (process.memoryUsage().heapUsed - h0) / w);
  }
  console.log(
    `${name.padEnd(40)} ${ns.toFixed(1).padStart(8)} ns/op   ` +
      (gc ? `${bytes.toFixed(0).padStart(5)} B/op` : '(run with --expose-gc for B/op)'),
  );
}

function microBench(): void {
  console.log('--- codec (single thread, no I/O) ---');
  const GET = cmd('GET');
  const SET = cmd('SET');
  const value = 'v'.repeat(256);
  const valueBuf = Buffer.alloc(256, 118);
  let sinkBytes = 0;
  const enc = new Encoder((c) => (sinkBytes += c.length));
  // Allocation windows are kept small so no scavenge lands inside them.
  const N = 2_000_000;
  micro('encode GET key', N, () => {
    enc.begin(GET, 1);
    enc.arg('key:12345');
    enc.endFrame();
    if (enc.pending > 60_000) enc.flush();
  });
  micro('encode SET key <256B string>', N, () => {
    enc.begin(SET, 2);
    enc.arg('key:12345');
    enc.arg(value);
    enc.endFrame();
    if (enc.pending > 60_000) enc.flush();
  });
  micro('encode SET key <256B Buffer>', N, () => {
    enc.begin(SET, 2);
    enc.arg('key:12345');
    enc.arg(valueBuf);
    enc.endFrame();
    if (enc.pending > 60_000) enc.flush();
  });
  micro('encode INCRBY key 123456', N, () => {
    enc.begin(cmdIncr, 2);
    enc.arg('key:12345');
    enc.arg(123456);
    enc.endFrame();
    if (enc.pending > 60_000) enc.flush();
  });

  const p = new Parser();
  const bulk = Buffer.from('$256\r\n' + value + '\r\n');
  const okBuf = Buffer.from('+OK\r\n');
  const intBuf = Buffer.from(':123456\r\n');
  const nilBuf = Buffer.from('$-1\r\n');
  const arr = Buffer.from('*10\r\n' + Array.from({ length: 10 }, (_, i) => `$6\r\nvalue${i}\r\n`).join(''));
  const one = (b: Buffer) => () => {
    p.feed(b);
    const v = p.next();
    if (v === NEED_MORE) throw new Error('unexpected');
    p.next();
  };
  micro('parse +OK', N, one(okBuf));
  micro('parse :int', N, one(intBuf));
  micro('parse $-1 (nil)', N, one(nilBuf));
  micro('parse $256 bulk -> string', N, one(bulk));
  p.binary = true;
  micro('parse $256 bulk -> Buffer', N, one(bulk));
  p.binary = false;
  micro('parse *10 array of bulks', N, one(arr));
  // 64 replies per chunk, as a busy socket read would deliver them
  const many = Buffer.concat(Array.from({ length: 64 }, () => bulk));
  micro('parse 64 x $256 bulk in one chunk (per chunk)', N / 64, () => {
    p.feed(many);
    while (p.next() !== NEED_MORE);
  });
  void sinkBytes;
}
const cmdIncr = cmd('INCRBY');

interface Target {
  name: string;
  get(k: string): Promise<unknown>;
  set(k: string, v: string): Promise<unknown>;
  close(): Promise<unknown>;
}

async function run(t: Target, op: 'GET' | 'SET', conc: number, value: string): Promise<number> {
  const keys = Array.from({ length: 1024 }, (_, i) => 'bench:' + i);
  await Promise.all(keys.map((k) => t.set(k, value)));
  let ops = 0;
  let stop = false;
  const deadline = Date.now() + SECONDS * 1000;
  const worker = async (w: number) => {
    let i = w;
    while (!stop) {
      for (let j = 0; j < 64; j++) {
        const k = keys[i++ & 1023]!;
        if (op === 'GET') await t.get(k);
        else await t.set(k, value);
      }
      ops += 64;
      if (Date.now() >= deadline) stop = true;
    }
  };
  // warm-up
  const warm = Date.now() + 300;
  while (Date.now() < warm) await Promise.all(Array.from({ length: conc }, (_, w) => (op === 'GET' ? t.get(keys[w & 1023]!) : t.set(keys[w & 1023]!, value))));
  const t0 = process.hrtime.bigint();
  await Promise.all(Array.from({ length: conc }, (_, w) => worker(w)));
  const secs = Number(process.hrtime.bigint() - t0) / 1e9;
  return ops / secs;
}

async function live(addr: string): Promise<void> {
  const [host, portStr] = addr.split(':');
  const port = Number(portStr);
  const value = 'x'.repeat(256);
  const targets: (() => Promise<Target>)[] = [];
  for (const flush of ['microtask', 'immediate'] as const) {
    targets.push(async () => {
      const c = new RuvioClient({ host, port, db: 15, flush });
      await c.connect();
      return { name: `ruvio-client (flush=${flush})`, get: (k) => c.get(k), set: (k, v) => c.set(k, v), close: () => c.quit() };
    });
  }
  try {
    const { default: Redis } = (await import('ioredis' as string)) as { default: new (o: object) => any };
    targets.push(async () => {
      const r = new Redis({ host, port, db: 15, lazyConnect: true, enableAutoPipelining: false });
      await r.connect();
      return { name: 'ioredis 5', get: (k) => r.get(k), set: (k, v) => r.set(k, v), close: async () => r.disconnect() };
    });
    targets.push(async () => {
      const r = new Redis({ host, port, db: 15, lazyConnect: true, enableAutoPipelining: true });
      await r.connect();
      return { name: 'ioredis 5 (autoPipelining)', get: (k) => r.get(k), set: (k, v) => r.set(k, v), close: async () => r.disconnect() };
    });
  } catch {
    console.log('(ioredis not installed: comparison skipped)');
  }
  console.log(`--- live ${addr}, 256-byte values, ${SECONDS}s per case, one connection per client ---`);
  console.log('client'.padEnd(32) + 'GET c=1'.padStart(11) + 'SET c=1'.padStart(11) + 'GET c=128'.padStart(12) + 'SET c=128'.padStart(12));
  for (const make of targets) {
    const t = await make();
    const r: number[] = [];
    for (const conc of [1, 128]) for (const op of ['GET', 'SET'] as const) r.push(await run(t, op, conc, value));
    console.log(t.name.padEnd(32) + r.map((x, i) => Math.round(x).toLocaleString('en-US').padStart(i < 2 ? 11 : 12)).join(''));
    await t.close();
  }
  const c = new RuvioClient({ host, port, db: 15 });
  // explicit pipeline: 128 GETs per exec
  const keys = Array.from({ length: 128 }, (_, i) => 'bench:' + i);
  let ops = 0;
  const end = Date.now() + SECONDS * 1000;
  const t0 = process.hrtime.bigint();
  while (Date.now() < end) {
    const p = c.pipeline();
    for (const k of keys) p.get(k);
    await p.exec();
    ops += 128;
  }
  const secs = Number(process.hrtime.bigint() - t0) / 1e9;
  console.log('ruvio-client pipeline(128 GET)'.padEnd(32) + `${Math.round(ops / secs).toLocaleString('en-US')} ops/s`);
  await c.flushdb();
  await c.quit();
}

/** GET/SET through the cluster client (3-shard server) next to the plain client on a standalone server. */
async function liveCluster(clusterAddr: string, standaloneAddr: string | undefined): Promise<void> {
  const split = (a: string) => ({ host: a.split(':')[0]!, port: Number(a.split(':')[1]) });
  const value = 'x'.repeat(256);
  const targets: (() => Promise<Target>)[] = [];
  const wrap = (name: string, c: RuvioClient | ClusterClient): Target => ({
    name,
    get: (k) => c.get(k),
    set: (k, v) => c.set(k, v),
    close: () => c.quit(),
  });
  if (standaloneAddr) {
    targets.push(async () => {
      const c = new RuvioClient({ ...split(standaloneAddr), db: 15 });
      await c.connect();
      return wrap('RuvioClient, standalone server', c);
    });
    targets.push(async () => {
      const c = new ClusterClient({ ...split(standaloneAddr), db: 15 });
      await c.connect();
      return wrap('ClusterClient, standalone (fallback)', c);
    });
  }
  targets.push(async () => {
    const c = new ClusterClient(split(clusterAddr));
    await c.connect();
    return wrap('ClusterClient, 3-shard cluster', c);
  });
  console.log(`--- cluster ${clusterAddr}, 256-byte values, ${SECONDS}s per case ---`);
  console.log('client'.padEnd(38) + 'GET c=1'.padStart(11) + 'SET c=1'.padStart(11) + 'GET c=128'.padStart(12) + 'SET c=128'.padStart(12));
  for (const make of targets) {
    const t = await make();
    const r: number[] = [];
    for (const conc of [1, 128]) for (const op of ['GET', 'SET'] as const) r.push(await run(t, op, conc, value));
    console.log(t.name.padEnd(38) + r.map((x, i) => Math.round(x).toLocaleString('en-US').padStart(i < 2 ? 11 : 12)).join(''));
    await t.close();
  }
  const c = new ClusterClient(split(clusterAddr));
  const keys = Array.from({ length: 128 }, (_, i) => 'bench:' + i);
  let ops = 0;
  const end = Date.now() + SECONDS * 1000;
  const t0 = process.hrtime.bigint();
  while (Date.now() < end) {
    const p = c.pipeline();
    for (const k of keys) p.get(k);
    await p.exec();
    ops += 128;
  }
  console.log('ClusterClient pipeline(128 GET)'.padEnd(38) + `${Math.round(ops / (Number(process.hrtime.bigint() - t0) / 1e9)).toLocaleString('en-US')} ops/s`);
  await c.flushall();
  await c.quit();
}

microBench();
const addr = process.env.RUVIO_TEST_ADDR;
if (addr) await live(addr);
else console.log('RUVIO_TEST_ADDR not set: live benchmark skipped');
const caddr = process.env.RUVIO_CLUSTER_ADDR;
if (caddr) await liveCluster(caddr, addr);
