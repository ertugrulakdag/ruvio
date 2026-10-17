// SPDX-License-Identifier: MIT
// Subscriber edge cases against a scripted Pub/Sub server: confirmations, message kinds, binary
// payloads, the async iterator's buffer, reconnects (including a failing re-subscribe) and close.

import { test } from 'node:test';
import assert from 'node:assert/strict';
import type * as net from 'node:net';
import { Subscriber, type PubSubMessage } from '../src/index.js';
import { bulk, fakeServer, int, type FakeServer } from './fake-server.js';
import { sleep } from './helpers.js';

const frame = (kind: string, name: string | null, n: number): string =>
  '*3\r\n' + bulk(kind) + (name === null ? '$-1\r\n' : bulk(name)) + int(n);

interface PubSubServer extends FakeServer {
  /** Sockets currently connected (latest last). */
  socks: net.Socket[];
  push(raw: string): void;
}

/** A server that confirms (P/S)(UN)SUBSCRIBE like a real one, with per-connection state. */
async function pubsubServer(extra?: (a: string[], sock: net.Socket) => string | null | undefined): Promise<PubSubServer> {
  const socks: net.Socket[] = [];
  const subs = new Map<net.Socket, Set<string>>();
  const s = await fakeServer((a, sock) => {
    if (!socks.includes(sock)) socks.push(sock);
    const set = subs.get(sock) ?? subs.set(sock, new Set()).get(sock)!;
    const x = extra?.(a, sock);
    if (x !== undefined) return x;
    const name = a[0]!;
    const sub = /^(P|S)?SUBSCRIBE$/.exec(name);
    if (sub) {
      return a
        .slice(1)
        .map((ch) => {
          set.add((sub[1] ?? 'C') + ch);
          return frame(name.toLowerCase(), ch, set.size);
        })
        .join('');
    }
    const un = /^(P|S)?UNSUBSCRIBE$/.exec(name);
    if (un) {
      const kind = name.toLowerCase();
      const targets = a.length > 1 ? a.slice(1) : [...set].filter((k) => k[0] === (un[1] ?? 'C')).map((k) => k.slice(1));
      if (targets.length === 0) return frame(kind, null, set.size);
      return targets
        .map((ch) => {
          set.delete((un[1] ?? 'C') + ch);
          return frame(kind, ch, set.size);
        })
        .join('');
    }
    if (name === 'PING') return '*2\r\n' + bulk('pong') + bulk('');
    return undefined;
  });
  return Object.assign(s, {
    socks,
    push: (raw: string) => socks[socks.length - 1]!.write(raw),
  });
}

const msg = (ch: string, body: string) => '*3\r\n' + bulk('message') + bulk(ch) + bulk(body);
const pmsg = (pat: string, ch: string, body: string) => '*4\r\n' + bulk('pmessage') + bulk(pat) + bulk(ch) + bulk(body);
const smsg = (ch: string, body: string) => '*3\r\n' + bulk('smessage') + bulk(ch) + bulk(body);

async function withSub(
  fn: (sub: Subscriber, s: PubSubServer) => Promise<void>,
  opts: ConstructorParameters<typeof Subscriber>[0] = {},
  extra?: (a: string[], sock: net.Socket) => string | null | undefined,
): Promise<void> {
  const s = await pubsubServer(extra);
  const sub = new Subscriber({ host: '127.0.0.1', port: s.port, ...opts });
  try {
    await fn(sub, s);
  } finally {
    sub.close();
    await s.close();
  }
}

async function until(cond: () => boolean, ms = 10_000): Promise<void> {
  const end = Date.now() + ms;
  while (!cond()) {
    if (Date.now() > end) throw new Error('condition not reached');
    await sleep(5);
  }
}

test('subscribe/psubscribe/ssubscribe track their sets, counts and events; unsubscribe without arguments drops all', async () => {
  await withSub(async (sub) => {
    const events: string[] = [];
    sub.on('subscribe', (kind: string, name: string | null, n: number) => events.push(`${kind}:${name}:${n}`));
    sub.on('unsubscribe', (kind: string, name: string | null, n: number) => events.push(`${kind}:${name}:${n}`));
    await sub.connect();
    assert.equal(await sub.subscribe('a', 'b', 'c'), 3);
    assert.equal(await sub.psubscribe('p*', 'q?'), 5);
    assert.equal(await sub.ssubscribe('sh1'), 6);
    assert.deepEqual([...sub.channels], ['a', 'b', 'c']);
    assert.deepEqual([...sub.patterns], ['p*', 'q?']);
    assert.deepEqual([...sub.shardChannels], ['sh1']);
    assert.equal(await sub.unsubscribe('a'), 5);
    assert.equal(await sub.unsubscribe(), 3, 'no arguments: every exact channel, confirmed once each');
    assert.equal(sub.channels.size, 0);
    assert.equal(await sub.punsubscribe(), 1);
    assert.equal(await sub.sunsubscribe(), 0);
    assert.equal(await sub.unsubscribe(), 0, 'nothing subscribed: a single confirmation with a null name');
    assert.equal(await sub.punsubscribe('never'), 0);
    assert.equal(await sub.sunsubscribe('never'), 0);
    assert.ok(events.includes('subscribe:a:1') && events.includes('psubscribe:q?:5') && events.includes('ssubscribe:sh1:6'));
    assert.ok(events.includes('unsubscribe:b:4') && events.includes('unsubscribe:null:0'));
  });
});

test('message, pmessage and smessage arrive as text events and in iterators; an iterator opened later sees only later messages', async () => {
  await withSub(async (sub, s) => {
    const seen: PubSubMessage[] = [];
    sub.on('message', (m: PubSubMessage) => seen.push(m));
    await sub.subscribe('ch');
    await sub.psubscribe('c*');
    await sub.ssubscribe('sh');
    s.push(msg('ch', 'one') + pmsg('c*', 'ch', 'two') + smsg('sh', 'three'));
    await until(() => seen.length === 3);
    assert.deepEqual(seen, [
      { kind: 'message', channel: 'ch', message: 'one' },
      { kind: 'pmessage', pattern: 'c*', channel: 'ch', message: 'two' },
      { kind: 'smessage', channel: 'sh', message: 'three' },
    ]);
    const it = sub[Symbol.asyncIterator]();
    const first = it.next();
    s.push(msg('ch', 'four'));
    assert.deepEqual(await first, { done: false, value: { kind: 'message', channel: 'ch', message: 'four' } });
    await it.return!();
  });
});

test('binary mode delivers Buffers, channels stay strings', async () => {
  await withSub(
    async (sub, s) => {
      const seen: PubSubMessage<Buffer>[] = [];
      sub.on('message', (m: PubSubMessage<Buffer>) => seen.push(m));
      assert.equal(await sub.subscribe('bin'), 1);
      assert.deepEqual([...sub.channels], ['bin']);
      s.push('*3\r\n' + bulk('message') + bulk('bin') + '$3\r\n\xff\xfe\x00\r\n');
      await until(() => seen.length === 1);
      assert.ok(Buffer.isBuffer(seen[0]!.message));
      assert.equal(seen[0]!.channel, 'bin');
      assert.equal(seen[0]!.message.length >= 3, true);
      assert.deepEqual(await sub.ping(), ['pong', ''], 'ping replies are text even in binary mode');
    },
    { binary: true },
  );
});

test('ping answers with the Pub/Sub shape as text; other three-element arrays resolve the pending call untouched', async () => {
  await withSub(
    async (sub) => {
      await sub.connect(); // queues a PING of its own
      assert.deepEqual(await sub.ping(), ['pong', '']);
      // a reply that is not a message and not a (un)subscribe confirmation is handed back as is
      assert.deepEqual(await sub.ping(), ['x', 1, 2]);
      assert.deepEqual(await sub.ping(), 'plain');
    },
    {},
    (() => {
      let n = 0;
      return (a: string[]) => {
        if (a[0] !== 'PING') return undefined;
        n++;
        return n <= 2 ? undefined : n === 3 ? '*3\r\n' + bulk('x') + int(1) + int(2) : '+plain\r\n';
      };
    })(),
  );
});

test('a pmessage frame without a payload is not a message', async () => {
  await withSub(async (sub, s) => {
    const seen: unknown[] = [];
    sub.on('message', (m: unknown) => seen.push(m));
    const ping = sub.ping();
    await until(() => s.socks.length === 1 && s.count('PING') === 1);
    // a three-element "pmessage" is malformed: it falls through to the pending PING
    s.push('*3\r\n' + bulk('pmessage') + bulk('p') + bulk('c'));
    const r = (await ping) as unknown[];
    assert.deepEqual(r, ['pmessage', 'p', 'c']);
    assert.equal(seen.length, 0);
  }, {}, (a) => (a[0] === 'PING' ? null : undefined));
});

test('the iterator buffers up to iteratorBuffer messages and drops the oldest', async () => {
  await withSub(async (sub, s) => {
    await sub.subscribe('ch');
    const it = sub[Symbol.asyncIterator]();
    let delivered = 0;
    sub.on('message', () => delivered++);
    s.push(msg('ch', '1') + msg('ch', '2') + msg('ch', '3') + msg('ch', '4'));
    await until(() => delivered === 4);
    const got: string[] = [];
    for (let i = 0; i < 2; i++) got.push(((await it.next()).value as PubSubMessage).message);
    assert.deepEqual(got, ['3', '4']);
    await it.return!();
  }, { iteratorBuffer: 2 });
});

test('a large backlog is compacted while it is drained', async () => {
  await withSub(async (sub, s) => {
    await sub.subscribe('ch');
    const it = sub[Symbol.asyncIterator]();
    const total = 3000;
    let raw = '';
    for (let i = 0; i < total; i++) raw += msg('ch', String(i));
    s.push(raw);
    const got: number[] = [];
    while (got.length < total) got.push(Number(((await it.next()).value as PubSubMessage).message));
    assert.deepEqual(got.slice(0, 3), [0, 1, 2]);
    assert.equal(got[total - 1], total - 1);
    assert.equal(got.every((v, i) => v === i), true, 'order survives compaction');
    await it.return!();
  });
});

test('close() ends iterators (waiting and buffered), later iterators end at once, return() detaches', async () => {
  await withSub(async (sub, s) => {
    await sub.subscribe('ch');
    const waiting = sub[Symbol.asyncIterator]();
    const buffered = sub[Symbol.asyncIterator]();
    const detached = sub[Symbol.asyncIterator]();
    const w = waiting.next();
    s.push(msg('ch', 'x'));
    assert.equal(((await w).value as PubSubMessage).message, 'x');
    assert.equal((await detached.return!()).done, true);
    let arrived = false;
    sub.on('message', (m: PubSubMessage) => (arrived ||= m.message === 'y'));
    s.push(msg('ch', 'y'));
    await until(() => arrived);
    const w2 = waiting.next();
    sub.close();
    sub.close(); // idempotent
    assert.equal((await w2).done, false, 'the message that arrived before close is still delivered');
    assert.equal((await waiting.next()).done, true);
    // the buffered iterator still has x and y, then ends
    assert.equal(((await buffered.next()).value as PubSubMessage).message, 'x');
    assert.equal(((await buffered.next()).value as PubSubMessage).message, 'y');
    assert.equal((await buffered.next()).done, true);
    const late = sub[Symbol.asyncIterator]();
    assert.equal((await late.next()).done, true);
  });
});

test('a for-await loop ends on break and on close', async () => {
  await withSub(async (sub, s) => {
    await sub.subscribe('ch');
    s.push(msg('ch', 'a') + msg('ch', 'b'));
    const seen: string[] = [];
    for await (const m of sub) {
      seen.push(m.message);
      if (seen.length === 2) break;
    }
    assert.deepEqual(seen, ['a', 'b']);
    const loop = (async () => {
      const r: string[] = [];
      for await (const m of sub) r.push(m.message);
      return r;
    })();
    let arrived = false;
    sub.on('message', (m: PubSubMessage) => (arrived ||= m.message === 'c'));
    s.push(msg('ch', 'c'));
    // The loop's iterator was attached when the loop started; delivery reaches it and the
    // listener in the same step.
    await until(() => arrived);
    sub.close();
    assert.deepEqual(await loop, ['c']);
  });
});

test('Symbol.asyncDispose closes the subscriber', async () => {
  await withSub(async (sub) => {
    await sub.subscribe('ch');
    await sub[Symbol.asyncDispose]();
    const it = sub[Symbol.asyncIterator]();
    assert.equal((await it.next()).done, true);
  });
});

test('a dropped connection is re-established and every subscription is restored', async () => {
  await withSub(
    async (sub, s) => {
      const errors: Error[] = [];
      sub.on('error', (e: Error) => errors.push(e));
      let reconnects = 0;
      sub.on('reconnect', () => reconnects++);
      await sub.subscribe('a', 'b');
      await sub.psubscribe('p*');
      await sub.ssubscribe('s1');
      const got: string[] = [];
      sub.on('message', (m: PubSubMessage) => got.push(m.message));
      s.socks[s.socks.length - 1]!.destroy();
      await until(() => reconnects === 1);
      assert.ok(errors.length >= 1);
      assert.equal(s.socks.length, 2);
      assert.deepEqual(s.seen.filter((x) => x[0] === 'SUBSCRIBE').map((x) => x.slice(1)), [['a', 'b'], ['a', 'b']]);
      assert.equal(s.count('PSUBSCRIBE'), 2);
      assert.equal(s.count('SSUBSCRIBE'), 2);
      s.push(msg('a', 'again'));
      await until(() => got.length === 1);
      assert.deepEqual(got, ['again']);
    },
    { reconnectDelayMs: 10 },
  );
});

test('a drop without listeners or subscriptions is silent and does not reconnect', async () => {
  await withSub(
    async (sub, s) => {
      await sub.connect();
      await sub.ping();
      s.socks[0]!.destroy(); // no 'error' listener: must not throw; no subscriptions: no reconnect
      await sleep(80);
      assert.equal(s.socks.length, 1);
      await sub.subscribe('x'); // a new command opens a fresh connection on demand
      assert.equal(s.socks.length, 2);
    },
    { reconnectDelayMs: 10 },
  );
});

test('a failing re-subscribe is retried with a growing delay until it succeeds', async () => {
  let subscribes = 0;
  await withSub(
    async (sub, s) => {
      let reconnects = 0;
      sub.on('reconnect', () => reconnects++);
      sub.on('error', () => undefined);
      await sub.subscribe('a');
      s.socks[0]!.destroy();
      await until(() => reconnects === 1, 4000);
      assert.equal(subscribes, 3, 'initial, one failed restore, one good restore');
    },
    { reconnectDelayMs: 10 },
    (a, sock) => {
      if (a[0] === 'SUBSCRIBE') {
        subscribes++;
        if (subscribes === 2) {
          sock.destroy(); // the restore is cut off: its promise rejects and another attempt follows
          return null;
        }
      }
      return undefined;
    },
  );
});

test('commands after close() reject with a ConnectionError', async () => {
  await withSub(async (sub) => {
    await sub.subscribe('a');
    sub.close();
    await assert.rejects(sub.subscribe('b'), /closed/);
    await assert.rejects(sub.ping(), /closed/);
    await assert.rejects(sub.connect(), /closed/);
  });
});
