// SPDX-License-Identifier: MIT
// The client: typed commands over one multiplexed connection, plus coordination helpers.

import './polyfill.js';
import { randomBytes } from 'node:crypto';
import { Commands } from './commands.js';
import { Connection, type ConnectionOptions } from './connection.js';
import type { Encoder } from './encoder.js';
import type { Tf } from './transforms.js';
import { str } from './transforms.js';
import { newPipeline, newTransaction, type Pipeline, type Transaction } from './pipeline.js';
import { Subscriber, type SubscriberOptions } from './subscriber.js';
import type { Key, Value, ScanOptions, SortedSetEntry } from './types.js';

export interface ClientOptions extends ConnectionOptions {}

/** Replaces `string` with `Buffer` throughout a reply type. */
export type Bin<T> = T extends string
  ? Buffer
  : T extends null | undefined | number | boolean | bigint
    ? T
    : T extends Buffer
      ? T
      : T extends readonly (infer U)[]
        ? Bin<U>[]
        : T extends object
          ? { [K in keyof T]: Bin<T[K]> }
          : T;

type AnyFn = (...a: any[]) => any;
/** Every command, resolving bulk strings as Buffers. */
export type BinaryCommands = {
  [K in keyof Commands as Commands[K] extends AnyFn ? (K extends `_${string}` ? never : K) : never]: Commands[K] extends (
    ...a: infer A
  ) => Promise<infer V>
    ? (...a: A) => Promise<Bin<V>>
    : never;
};

/** A distributed lock held by this process (SET NX PX / SET IFEQ / DELEX IFEQ). */
export class Lock {
  private released = false;
  constructor(
    private readonly client: Commands,
    readonly key: Key,
    readonly token: string,
    readonly ttlMs: number,
  ) {}

  /** Extends the lock if this token still owns it. False means it was lost. */
  async renew(ttlMs: number = this.ttlMs): Promise<boolean> {
    if (this.released) return false;
    return (await this.client.set(this.key, this.token, { ifEq: this.token, px: ttlMs })) !== null;
  }

  /** Deletes the key only if it still holds this token. True when this call released it. */
  async release(): Promise<boolean> {
    if (this.released) return false;
    this.released = true;
    return this.client.delex(this.key, this.token);
  }

  get isReleased(): boolean {
    return this.released;
  }

  async [Symbol.asyncDispose](): Promise<void> {
    await this.release();
  }
}

/** A fenced lease (LEASE/RELEASE). `token` increases on every grant: pass it to downstream writes. */
export class Lease {
  private released = false;
  constructor(
    private readonly client: Commands,
    readonly key: Key,
    readonly token: number,
  ) {}
  async release(): Promise<boolean> {
    if (this.released) return false;
    this.released = true;
    return this.client.release(this.key, this.token);
  }
  async [Symbol.asyncDispose](): Promise<void> {
    await this.release();
  }
}

/** One slot of a counting semaphore (SEMAPHORE/RELEASE). */
export class Permit {
  private released = false;
  constructor(
    private readonly client: Commands,
    readonly key: Key,
    readonly token: string,
  ) {}
  async release(): Promise<boolean> {
    if (this.released) return false;
    this.released = true;
    return this.client.release(this.key, this.token);
  }
  async [Symbol.asyncDispose](): Promise<void> {
    await this.release();
  }
}

/** The lock/lease/semaphore was not acquired within the wait. */
export class AcquireTimeoutError extends Error {
  override get name(): string {
    return 'AcquireTimeoutError';
  }
}

/** IDEM BEGIN answered `conflict`: the key is reserved for a different fingerprint. */
export class IdempotencyConflictError extends Error {
  override get name(): string {
    return 'IdempotencyConflictError';
  }
}

/** IDEM BEGIN kept answering `pending` until the wait ended. */
export class IdempotencyPendingError extends Error {
  override get name(): string {
    return 'IdempotencyPendingError';
  }
}

export interface AcquireOptions {
  /** How long to keep retrying (ms). 0 (default) tries once. */
  waitMs?: number;
  /** Delay between attempts (ms, default 50). */
  retryMs?: number;
}

export interface GetOrCreateOptions {
  /** How long the fill marker lives, i.e. the factory's time budget (ms, default 10 000). */
  fillMs?: number;
  /** Serve the old value this long after expiry while one caller refreshes (FILLSET SWR). */
  staleMs?: number;
  /** Give up waiting on another filler after this long (ms, default fillMs + 1000). */
  waitMs?: number;
}

const sleep = (ms: number) => new Promise<void>((r) => setTimeout(r, ms));
const newToken = () => randomBytes(16).toString('hex');

function parseUrl(url: string): ClientOptions {
  const u = new URL(url);
  const o: ClientOptions = { host: u.hostname || '127.0.0.1', port: u.port ? Number(u.port) : 6379 };
  if (u.protocol === 'rediss:' || u.protocol === 'ruvios:') o.tls = true;
  if (u.username) o.username = decodeURIComponent(u.username);
  if (u.password) o.password = decodeURIComponent(u.password);
  const db = u.pathname.replace(/^\//, '');
  if (db) o.db = Number(db);
  return o;
}

/**
 * Coordination helpers built only on single-key commands, so they behave the same on a
 * standalone client and on a cluster client.
 */
export abstract class Helpers extends Commands {
  // ---- helpers ---------------------------------------------------------------------------

  /**
   * Tries to take a distributed lock: `SET key token NX PX ttl`. Resolves a {@link Lock}
   * (use `await using`) or `null` when another owner holds it after `waitMs`.
   */
  async acquireLock(key: Key, ttlMs: number, opts: AcquireOptions & { token?: string } = {}): Promise<Lock | null> {
    const token = opts.token ?? newToken();
    const deadline = Date.now() + (opts.waitMs ?? 0);
    for (;;) {
      if ((await this.set(key, token, { nx: true, px: ttlMs })) !== null) return new Lock(this, key, token, ttlMs);
      if (Date.now() >= deadline) return null;
      await sleep(opts.retryMs ?? 50);
    }
  }

  /** Runs `fn` holding the lock; always releases (only its own token). Throws AcquireTimeoutError. */
  async withLock<T>(key: Key, ttlMs: number, fn: (lock: Lock) => Promise<T>, opts: AcquireOptions = {}): Promise<T> {
    const lock = await this.acquireLock(key, ttlMs, opts);
    if (lock === null) throw new AcquireTimeoutError('ruvio: lock ' + str(key) + ' is held');
    try {
      return await fn(lock);
    } finally {
      await lock.release();
    }
  }

  /** `LEASE key ttl`: a fenced lease, or `null` while another holder has it. */
  async acquireLease(key: Key, ttlMs: number, opts: AcquireOptions = {}): Promise<Lease | null> {
    const deadline = Date.now() + (opts.waitMs ?? 0);
    for (;;) {
      const t = await this.lease(key, ttlMs);
      if (t !== null) return new Lease(this, key, t);
      if (Date.now() >= deadline) return null;
      await sleep(opts.retryMs ?? 50);
    }
  }

  /** `SEMAPHORE key max ttl`: one of `max` slots, or `null` when full. */
  async acquireSemaphore(key: Key, max: number, ttlMs: number, opts: AcquireOptions = {}): Promise<Permit | null> {
    const deadline = Date.now() + (opts.waitMs ?? 0);
    for (;;) {
      const t = await this.semaphore(key, max, ttlMs);
      if (t !== null) return new Permit(this, key, t);
      if (Date.now() >= deadline) return null;
      await sleep(opts.retryMs ?? 50);
    }
  }

  /** Fixed-window rate limit on `LIMIT`: allowed, and calls remaining in the window. */
  async rateLimit(key: Key, max: number, windowMs: number): Promise<{ allowed: boolean; remaining: number }> {
    const r = await this.limit(key, max, windowMs);
    return r === null ? { allowed: false, remaining: 0 } : { allowed: true, remaining: r };
  }

  /**
   * Cache-aside with stampede protection on FILLGET/FILLSET: one caller runs `factory`, the
   * others poll plain FILLGET with a short backoff (never `WAIT`, which would park the shared
   * socket). With `staleMs`, an expired value is served while one caller refreshes it.
   */
  async getOrCreate(key: Key, ttlMs: number, factory: () => Promise<Value> | Value, opts: GetOrCreateOptions = {}): Promise<string> {
    const fillMs = opts.fillMs ?? 10_000;
    const deadline = Date.now() + (opts.waitMs ?? fillMs + 1000);
    let backoff = 2;
    for (;;) {
      const r = await this.fillget(key, fillMs);
      switch (r.state) {
        case 'hit':
          return r.value!;
        case 'fill': {
          const v = await factory();
          await this.fillset(key, v, ttlMs, r.token!, opts.staleMs);
          return str(v);
        }
        case 'stale':
          if (r.token !== null) {
            const token = r.token;
            // Refresh in the background; the stale value answers this caller now.
            Promise.resolve()
              .then(factory)
              .then((v) => this.fillset(key, v, ttlMs, token, opts.staleMs))
              .catch(() => undefined);
          }
          return r.value!;
        case 'busy':
          if (Date.now() >= deadline) throw new AcquireTimeoutError('ruvio: getOrCreate timed out waiting for ' + str(key));
          await sleep(backoff);
          backoff = Math.min(backoff * 2, 50);
      }
    }
  }

  /**
   * Runs `fn` at most once per (key, fingerprint) within `ttlMs` on IDEM BEGIN/COMPLETE. A
   * repeated call returns the stored result with `replayed: true`. While another owner is
   * running it, polls until `waitMs` (default 0: throw IdempotencyPendingError at once).
   */
  async idempotent(
    key: Key,
    fingerprint: Value,
    ttlMs: number,
    fn: () => Promise<Value> | Value,
    opts: { owner?: string; waitMs?: number; retryMs?: number } = {},
  ): Promise<{ result: string; replayed: boolean }> {
    const owner = opts.owner ?? newToken();
    const deadline = Date.now() + (opts.waitMs ?? 0);
    for (;;) {
      const r = await this.idemBegin(key, fingerprint, owner, ttlMs);
      switch (r.state) {
        case 'acquired': {
          const v = await fn();
          await this.idemComplete(key, fingerprint, owner, v);
          return { result: str(v), replayed: false };
        }
        case 'completed':
          return { result: r.result ?? '', replayed: true };
        case 'conflict':
          throw new IdempotencyConflictError('ruvio: ' + str(key) + ' is reserved for another fingerprint');
        case 'pending':
          if (Date.now() >= deadline) throw new IdempotencyPendingError('ruvio: ' + str(key) + ' is still running elsewhere');
          await sleep(opts.retryMs ?? 20);
      }
    }
  }

  /** "Recent items": LPUSHCAP, newest first, keeping at most `cap`. Returns the length. */
  pushRecent(key: Key, cap: number, ...values: Value[]): Promise<number> {
    return this.lpushcap(key, cap, ...values);
  }
  /** The newest `n` items pushed with {@link pushRecent}. */
  recent(key: Key, n: number): Promise<string[]> {
    return this.lrange(key, 0, n - 1);
  }

  /** Bloom "seen before": records items (BF.MADD) and reports, per item, whether it was probably seen already. */
  async seenBefore(key: Key, ...items: Value[]): Promise<boolean[]> {
    const added = await this.bfMadd(key, ...items);
    for (let i = 0; i < added.length; i++) added[i] = !added[i];
    return added;
  }
  /** Bloom membership without recording (BF.MEXISTS). */
  mightContain(key: Key, ...items: Value[]): Promise<boolean[]> {
    return this.bfMexists(key, ...items);
  }

  /** Lazily walks `SCAN`, yielding keys. */
  async *scanIterator(opts?: ScanOptions): AsyncGenerator<string> {
    let cursor = '0';
    do {
      const p = await this.scan(cursor, opts);
      cursor = p.cursor;
      for (const k of p.items) yield k;
    } while (cursor !== '0');
  }
  /** Lazily walks `HSCAN`, yielding `[field, value]`. */
  async *hscanIterator(key: Key, opts?: Omit<ScanOptions, 'type'>): AsyncGenerator<[string, string]> {
    let cursor = '0';
    do {
      const p = await this.hscan(key, cursor, opts);
      cursor = p.cursor;
      for (const e of p.items) yield e;
    } while (cursor !== '0');
  }
  /** Lazily walks `SSCAN`, yielding members. */
  async *sscanIterator(key: Key, opts?: Omit<ScanOptions, 'type'>): AsyncGenerator<string> {
    let cursor = '0';
    do {
      const p = await this.sscan(key, cursor, opts);
      cursor = p.cursor;
      for (const m of p.items) yield m;
    } while (cursor !== '0');
  }
  /** Lazily walks `ZSCAN`, yielding `{member, score}`. */
  async *zscanIterator(key: Key, opts?: Omit<ScanOptions, 'type'>): AsyncGenerator<SortedSetEntry> {
    let cursor = '0';
    do {
      const p = await this.zscan(key, cursor, opts);
      cursor = p.cursor;
      for (const e of p.items) yield e;
    } while (cursor !== '0');
  }
}

/**
 * The Ruvio client. All calls share one pipelined socket: concurrent callers' frames are
 * coalesced into one write, replies resolve in order. Blocking commands (BLPOP, XREAD BLOCK,
 * FILLGET WAIT, WAIT) park that socket for everyone: run them on `duplicate()`.
 */
export class RuvioClient extends Helpers {
  /** @internal */
  readonly conn: Connection;
  readonly options: ClientOptions;
  private _binary: BinaryCommands | null = null;

  constructor(options: ClientOptions | string = {}) {
    super();
    this.options = typeof options === 'string' ? parseUrl(options) : { ...options };
    this.conn = new Connection(this.options);
  }

  protected _enc(): Encoder {
    return this.conn.encoder();
  }
  protected _done(tf: Tf | null, fl: number): Promise<any> {
    return this.conn.track(tf, fl);
  }
  protected _fail(e: unknown, enc: Encoder): Promise<never> {
    if (!enc.abortFrame()) this.conn.destroySocket(e as Error);
    return Promise.reject(e);
  }

  /** Opens the socket and waits for the handshake (optional: commands connect lazily). */
  connect(): Promise<void> {
    return this.conn.connect();
  }
  /** Waits for in-flight replies, then closes. */
  quit(): Promise<void> {
    return this.conn.quit();
  }
  /** Closes now; in-flight calls reject with ConnectionError. */
  disconnect(): void {
    this.conn.disconnect();
  }
  async [Symbol.asyncDispose](): Promise<void> {
    await this.quit();
  }
  /** Commands awaiting a reply. */
  get inFlight(): number {
    return this.conn.inFlight;
  }
  /**
   * Connection events: `connect`, `disconnect` (err), `connectError` (err), `error` (err),
   * `push` (RESP3 push data, e.g. CLIENT TRACKING invalidations).
   */
  on(event: 'connect' | 'disconnect' | 'connectError' | 'error' | 'push' | 'unexpected', listener: (...a: any[]) => void): this {
    this.conn.on(event, listener);
    return this;
  }
  off(event: string, listener: (...a: any[]) => void): this {
    this.conn.off(event, listener);
    return this;
  }

  /** The same commands, resolving bulk strings as Buffers (binary-safe). */
  get binary(): BinaryCommands {
    if (this._binary === null) {
      const view = Object.create(this) as RuvioClient;
      view._fl = 1;
      this._binary = view as unknown as BinaryCommands;
    }
    return this._binary;
  }

  /** `SELECT db`, remembered so a reconnect selects it again. Affects every caller of this client. */
  override select(db: number): Promise<'OK'> {
    this.conn.db = db;
    return super.select(db);
  }

  /** A new client with the same options (its own socket). */
  duplicate(overrides: ClientOptions = {}): RuvioClient {
    return new RuvioClient({ ...this.options, db: this.conn.db, ...overrides });
  }

  /** Runs `fn` on a dedicated connection (WATCH, blocking reads), closed afterwards. */
  async isolated<T>(fn: (client: RuvioClient) => Promise<T>): Promise<T> {
    const c = this.duplicate();
    try {
      return await fn(c);
    } finally {
      await c.quit();
    }
  }

  /** A pipeline: queue commands, `exec()` sends them in one write. */
  pipeline(): Pipeline {
    return newPipeline(this.conn);
  }

  /** A MULTI/EXEC transaction sent as one contiguous write. */
  multi(): Transaction {
    return newTransaction(this.conn);
  }

  /** A Pub/Sub connection with this client's address and credentials. */
  subscriber(opts: Omit<SubscriberOptions, keyof ConnectionOptions> = {}): Subscriber {
    return new Subscriber({ ...this.options, ...opts });
  }
}

/** Creates a client (`createClient('redis://127.0.0.1:6379/0')` or options). */
export function createClient(options: ClientOptions | string = {}): RuvioClient {
  return new RuvioClient(options);
}
