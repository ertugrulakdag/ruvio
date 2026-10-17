// SPDX-License-Identifier: MIT
// Pub/Sub on a dedicated connection: EventEmitter events and an async iterator.

import { EventEmitter } from 'node:events';
import { Connection, type ConnectionOptions } from './connection.js';
import { cmd, type Arg, type Cmd } from './encoder.js';
import { str } from './transforms.js';

/** One message delivered to a {@link Subscriber}. */
export interface PubSubMessage<M = string> {
  /** `message` (from `SUBSCRIBE`), `pmessage` (from `PSUBSCRIBE`) or `smessage` (from `SSUBSCRIBE`). */
  kind: 'message' | 'pmessage' | 'smessage';
  /** The channel the message was published to. */
  channel: string;
  /** The pattern that matched, for `pmessage` only. */
  pattern?: string;
  /** The payload: a string, or a Buffer when the subscriber was created with `binary: true`. */
  message: M;
}

/** Options for {@link Subscriber}: all {@link ConnectionOptions} (the `db` option is ignored) plus the fields below. */
export interface SubscriberOptions extends ConnectionOptions {
  /** Deliver payloads as Buffers (default false: UTF-8 strings). */
  binary?: boolean;
  /** Delay in milliseconds before reconnecting and re-subscribing after a drop (default 100, doubling up to 5000). Reset after a successful resubscribe. */
  reconnectDelayMs?: number;
  /** Messages buffered per async iterator that is not reading fast enough (default 10000; the oldest are dropped first). */
  iteratorBuffer?: number;
}

const C = {
  SUBSCRIBE: cmd('SUBSCRIBE'),
  UNSUBSCRIBE: cmd('UNSUBSCRIBE'),
  PSUBSCRIBE: cmd('PSUBSCRIBE'),
  PUNSUBSCRIBE: cmd('PUNSUBSCRIBE'),
  SSUBSCRIBE: cmd('SSUBSCRIBE'),
  SUNSUBSCRIBE: cmd('SUNSUBSCRIBE'),
  PING: cmd('PING'),
};

const pingTf = (v: unknown): unknown => (Array.isArray(v) ? v.map((x) => (Buffer.isBuffer(x) ? str(x) : x)) : Buffer.isBuffer(v) ? str(v) : v);
const countTf = (v: unknown[]): number => (Array.isArray(v) ? Number(v[2] ?? 0) : 0);

/**
 * A Pub/Sub connection, separate from the command connection (a connection in subscribe mode can
 * only run Pub/Sub commands and `PING`). Create one with `client.subscriber()`,
 * `cluster.subscriber()` or `cluster.subscriberFor(channel)`.
 *
 * Receive messages with the `message` event or with `for await (const m of subscriber)`.
 * Events:
 * - `message` ({@link PubSubMessage}): a message arrived.
 * - `subscribe` / `unsubscribe` (kind, channel, count): a confirmation from the server.
 * - `reconnect`: the connection dropped and all subscriptions were restored.
 * - `error` (error): a connection error; emitted only if you have an `error` listener.
 *
 * After a drop the subscriber reconnects by itself (with backoff) and re-subscribes to everything in
 * {@link channels}, {@link patterns} and {@link shardChannels}; messages published meanwhile are lost
 * (Pub/Sub is not stored). Classic channels are per shard on the server: `PUBLISH` reaches subscribers
 * on every shard, `SPUBLISH` only those on the shard that owns the channel.
 *
 * @example
 * ```ts
 * const sub = client.subscriber();
 * sub.on('message', (m) => console.log(m.channel, m.message));
 * await sub.subscribe('news');
 * await sub.psubscribe('alerts.*');
 * // later
 * sub.close();
 * ```
 */
export class Subscriber extends EventEmitter implements AsyncIterable<PubSubMessage<any>> {
  private readonly conn: Connection;
  private readonly binary: boolean;
  /** Remaining confirmations per pending (un)subscribe command, FIFO. */
  private expect: number[] = [];
  private expHead = 0;
  /** Exact channels currently subscribed (re-subscribed after a reconnect). */
  readonly channels = new Set<string>();
  /** Patterns currently subscribed with {@link psubscribe}. */
  readonly patterns = new Set<string>();
  /** Sharded channels currently subscribed with {@link ssubscribe}. */
  readonly shardChannels = new Set<string>();
  private closed = false;
  private delay: number;
  private readonly baseDelay: number;
  private readonly iterMax: number;
  private iters = new Set<MessageQueue>();

  /** @param opts {@link SubscriberOptions}; usually obtained via `client.subscriber()` instead. */
  constructor(opts: SubscriberOptions) {
    super();
    this.binary = opts.binary ?? false;
    this.baseDelay = this.delay = opts.reconnectDelayMs ?? 100;
    this.iterMax = opts.iteratorBuffer ?? 10000;
    // Pub/Sub has no SELECT dependency; keep auth/name, drop db.
    this.conn = new Connection({ ...opts, db: 0, flush: 'microtask' });
    this.conn.intercept = (v) => this.onReply(v);
    this.conn.forceBinary = this.binary;
    this.conn.on('disconnect', (err) => this.onDrop(err));
    this.conn.on('connectError', (err) => this.onDrop(err));
  }

  private onReply(v: unknown): boolean {
    if (!Array.isArray(v) || v.length < 3) return false;
    const kind = str(v[0]);
    switch (kind) {
      case 'message':
      case 'smessage': {
        const m: PubSubMessage<unknown> = { kind, channel: str(v[1]), message: this.payload(v[2]) };
        this.deliver(m);
        return true;
      }
      case 'pmessage': {
        if (v.length < 4) return false;
        const m: PubSubMessage<unknown> = { kind, pattern: str(v[1]), channel: str(v[2]), message: this.payload(v[3]) };
        this.deliver(m);
        return true;
      }
      case 'subscribe':
      case 'psubscribe':
      case 'ssubscribe':
      case 'unsubscribe':
      case 'punsubscribe':
      case 'sunsubscribe': {
        const name = v[1] === null ? null : str(v[1]);
        if (name !== null) {
          const set = kind[0] === 'p' ? this.patterns : kind === 'ssubscribe' || kind === 'sunsubscribe' ? this.shardChannels : this.channels;
          if (kind.endsWith('unsubscribe')) set.delete(name);
          else set.add(name);
        }
        this.emit(kind.endsWith('unsubscribe') ? 'unsubscribe' : 'subscribe', kind, name, Number(v[2]));
        if (this.expHead < this.expect.length) {
          const left = --this.expect[this.expHead]!;
          if (left > 0) return true;
          this.expHead++;
          if (this.expHead === this.expect.length) {
            this.expect = [];
            this.expHead = 0;
          }
        }
        return false; // resolves the pending (un)subscribe call
      }
      default:
        return false;
    }
  }

  private payload(v: unknown): unknown {
    if (this.binary) return v;
    return typeof v === 'string' ? v : str(v);
  }

  private deliver(m: PubSubMessage<unknown>): void {
    this.emit('message', m);
    for (const q of this.iters) q.push(m);
  }

  private onDrop(err: Error): void {
    this.expect = [];
    this.expHead = 0;
    if (this.closed) return;
    if (this.listenerCount('error') > 0) this.emit('error', err);
    const total = this.channels.size + this.patterns.size + this.shardChannels.size;
    if (total === 0) return;
    const d = this.delay;
    this.delay = Math.min(this.delay * 2, 5000);
    setTimeout(() => {
      if (this.closed) return;
      this.resubscribe().then(
        () => {
          this.delay = this.baseDelay;
          this.emit('reconnect');
        },
        () => {
          /* a further drop schedules another attempt */
        },
      );
    }, d).unref?.();
  }

  private resubscribe(): Promise<unknown> {
    const ps: Promise<unknown>[] = [];
    if (this.channels.size) ps.push(this.send(C.SUBSCRIBE, [...this.channels]));
    if (this.patterns.size) ps.push(this.send(C.PSUBSCRIBE, [...this.patterns]));
    if (this.shardChannels.size) ps.push(this.send(C.SSUBSCRIBE, [...this.shardChannels]));
    return Promise.all(ps);
  }

  private send(c: Cmd, names: Arg[], expected = names.length): Promise<number> {
    const e = this.conn.encoder();
    e.begin(c, names.length);
    for (const n of names) e.arg(n);
    e.endFrame();
    this.expect.push(Math.max(1, expected));
    return this.conn.track(countTf, this.binary ? 1 : 0);
  }

  /**
   * Opens the connection now. Optional: subscribing connects on demand.
   *
   * @throws {@link ConnectionError} when the connection fails.
   */
  connect(): Promise<void> {
    return this.conn.connect();
  }

  /**
   * Subscribes to exact channels (`SUBSCRIBE`). Matches the channel name exactly; no glob.
   *
   * @param channels Channel names.
   * @returns The connection's total subscription count (channels plus patterns) after the last channel.
   * @throws {@link RuvioError} when the server refuses (for example `NOPERM`).
   *
   * @example
   * ```ts
   * await sub.subscribe('news', 'sports');
   * ```
   */
  subscribe(...channels: string[]): Promise<number> {
    return this.send(C.SUBSCRIBE, channels);
  }
  /**
   * Unsubscribes from exact channels (`UNSUBSCRIBE`). Patterns are left in place.
   *
   * @param channels Channels to leave; none drops every exact channel of this connection.
   * @returns The remaining subscription count.
   */
  unsubscribe(...channels: string[]): Promise<number> {
    return this.send(C.UNSUBSCRIBE, channels, channels.length || this.channels.size);
  }
  /**
   * Subscribes to channel patterns (`PSUBSCRIBE`). Patterns use `*` and `?`; character classes are
   * not supported. Messages arrive with `kind: 'pmessage'` and the matching `pattern`.
   *
   * @param patterns Glob patterns such as `'alerts.*'`.
   * @returns The connection's total subscription count after the last pattern.
   *
   * @example
   * ```ts
   * await sub.psubscribe('user:*:events');
   * ```
   */
  psubscribe(...patterns: string[]): Promise<number> {
    return this.send(C.PSUBSCRIBE, patterns);
  }
  /**
   * Leaves patterns (`PUNSUBSCRIBE`). Exact channels are left in place.
   *
   * @param patterns Patterns to leave; none drops every pattern of this connection.
   * @returns The remaining subscription count.
   */
  punsubscribe(...patterns: string[]): Promise<number> {
    return this.send(C.PUNSUBSCRIBE, patterns, patterns.length || this.patterns.size);
  }
  /**
   * Subscribes to sharded channels (`SSUBSCRIBE`), also used for keyspace notices. A sharded
   * channel lives on the shard that owns its hash slot, so on a multi-shard server connect to
   * that shard (use `cluster.subscriberFor(channel)`); a wrong shard redirects the subscription.
   * There is no pattern form. Messages arrive with `kind: 'smessage'`.
   *
   * @param channels Sharded channel names.
   * @returns The connection's subscription count after the last channel.
   * @throws {@link RuvioError} `MOVED` when this connection is on the wrong shard.
   */
  ssubscribe(...channels: string[]): Promise<number> {
    return this.send(C.SSUBSCRIBE, channels);
  }
  /**
   * Leaves sharded channels (`SUNSUBSCRIBE`).
   *
   * @param channels Channels to leave; none leaves all sharded channels of this connection.
   * @returns The remaining subscription count.
   */
  sunsubscribe(...channels: string[]): Promise<number> {
    return this.send(C.SUNSUBSCRIBE, channels, channels.length || this.shardChannels.size);
  }
  /**
   * Sends `PING` in Pub/Sub mode, e.g. as a liveness check.
   *
   * @returns The raw reply: `['pong', '']` in subscribe mode.
   */
  ping(): Promise<unknown> {
    const e = this.conn.encoder();
    e.begin(C.PING, 0);
    e.endFrame();
    return this.conn.track(pingTf, 0);
  }

  /** Closes the connection for good, stops reconnecting and ends every `for await` loop. Safe to call twice. */
  close(): void {
    if (this.closed) return;
    this.closed = true;
    this.conn.disconnect();
    for (const q of this.iters) q.end();
    this.iters.clear();
  }

  async [Symbol.asyncDispose](): Promise<void> {
    this.close();
  }

  /**
   * Lets you write `for await (const m of subscriber)`. Each loop gets its own buffer of up to
   * `iteratorBuffer` messages (the oldest are dropped when it fills). The loop ends on {@link close} or `break`.
   */
  [Symbol.asyncIterator](): AsyncIterator<PubSubMessage<any>> {
    const q = new MessageQueue(this.iterMax, () => this.iters.delete(q));
    if (this.closed) q.end();
    else this.iters.add(q);
    return q;
  }
}

class MessageQueue implements AsyncIterator<PubSubMessage<any>> {
  private items: PubSubMessage<any>[] = [];
  private head = 0;
  private waiter: ((r: IteratorResult<PubSubMessage<any>>) => void) | null = null;
  private done = false;
  constructor(
    private readonly max: number,
    private readonly detach: () => void,
  ) {}
  push(m: PubSubMessage<any>): void {
    if (this.done) return;
    if (this.waiter !== null) {
      const w = this.waiter;
      this.waiter = null;
      w({ value: m, done: false });
      return;
    }
    if (this.items.length - this.head >= this.max) this.head++;
    this.items.push(m);
  }
  end(): void {
    this.done = true;
    if (this.waiter !== null) {
      const w = this.waiter;
      this.waiter = null;
      w({ value: undefined, done: true });
    }
  }
  next(): Promise<IteratorResult<PubSubMessage<any>>> {
    if (this.head < this.items.length) {
      const m = this.items[this.head++]!;
      if (this.head > 1024 && this.head * 2 > this.items.length) {
        this.items = this.items.slice(this.head);
        this.head = 0;
      }
      return Promise.resolve({ value: m, done: false });
    }
    if (this.done) return Promise.resolve({ value: undefined, done: true });
    return new Promise((r) => (this.waiter = r));
  }
  return(): Promise<IteratorResult<PubSubMessage<any>>> {
    this.end();
    this.detach();
    return Promise.resolve({ value: undefined, done: true });
  }
}
