// SPDX-License-Identifier: MIT
// Cluster routing: CLUSTER SLOTS table, one multiplexed connection per node, MOVED handling,
// split/merge of multi-key commands, grouped pipelines and single-slot transactions.
//
// How routing works without touching the typed commands: the cluster client encodes every
// command into a private scratch encoder (the same fast encoder the standalone client uses),
// then reads the key(s) straight out of the encoded RESP bytes. The frame is a zero-copy view
// of the encoder slab, so keeping it for a MOVED retry costs nothing extra.
// Against a standalone server the client detects `cluster mode is disabled` and from then on
// encodes straight into the one connection, exactly like RuvioClient.

import { Helpers, RuvioClient, type BinaryCommands, type ClientOptions } from './client.js';
import type { Connection, BatchSink } from './connection.js';
import { COMMAND_KEY_RULES, KeyRule, type CommandKeyRule } from './command-keys.js';
import { Encoder } from './encoder.js';
import { ConnectionError, RuvioError } from './errors.js';
import { Batch, PipelineSink, TransactionImpl, type Pipeline, type Transaction } from './pipeline.js';
import { Subscriber, type SubscriberOptions } from './subscriber.js';
import type { Tf } from './transforms.js';
import type { Key, ScanOptions } from './types.js';

export const SLOT_COUNT = 16384;

// ---- hash slots -----------------------------------------------------------------------------

const CRC = new Uint16Array(256);
for (let v = 0; v < 256; v++) {
  let crc = v << 8;
  for (let bit = 0; bit < 8; bit++) crc = crc & 0x8000 ? (crc << 1) ^ 0x1021 : crc << 1;
  CRC[v] = crc & 0xffff;
}

/** Slot of the key bytes `b[s..e)`: CRC16-XMODEM of the key or of its first non-empty `{tag}`. */
function slotRange(b: Uint8Array, s: number, e: number): number {
  for (let i = s; i < e; i++) {
    if (b[i] === 123) {
      for (let j = i + 1; j < e; j++) {
        if (b[j] === 125) {
          if (j > i + 1) {
            s = i + 1;
            e = j;
          }
          break;
        }
      }
      break;
    }
  }
  let crc = 0;
  for (let i = s; i < e; i++) crc = ((crc << 8) ^ CRC[((crc >> 8) ^ b[i]!) & 0xff]!) & 0xffff;
  return crc & (SLOT_COUNT - 1);
}

/** The hash slot (0..16383) of a key; `{hashtag}` aware, binary-safe for Buffers. */
export function hashSlot(key: string | Uint8Array): number {
  if (typeof key !== 'string') return slotRange(key, 0, key.length);
  const b = Buffer.from(key, 'utf8');
  return slotRange(b, 0, b.length);
}

// ---- reading keys out of an encoded frame ---------------------------------------------------

let A_H = new Int32Array(64); // offset of each argument's `$` header
let A_OFF = new Int32Array(64); // offset of each argument's data
let A_LEN = new Int32Array(64);
let A_N = 0; // arguments in the frame, command name included
let A_END = 0; // offset just past the frame
let KI = new Int32Array(64); // indexes of the key arguments
let LAST_SPEC: Spec = null as unknown as Spec;
let LAST_SLOT = -1;

/** Reads the RESP array at `p` (our own encoder output, always well-formed). */
function scanFrame(b: Uint8Array, p: number): void {
  let i = p + 1;
  let n = 0;
  while (b[i] !== 13) n = n * 10 + b[i++]! - 48;
  i += 2;
  if (n > A_H.length) {
    const sz = Math.max(n, A_H.length * 2);
    A_H = new Int32Array(sz);
    A_OFF = new Int32Array(sz);
    A_LEN = new Int32Array(sz);
    KI = new Int32Array(sz);
  }
  for (let k = 0; k < n; k++) {
    A_H[k] = i;
    i++; // '$'
    let len = 0;
    while (b[i] !== 13) len = len * 10 + b[i++]! - 48;
    i += 2;
    A_OFF[k] = i;
    A_LEN[k] = len;
    i += len + 2;
  }
  A_N = n;
  A_END = i;
}

const F_SPLIT = 1; // may be split per node when its keys span slots
const F_FANOUT = 2; // keyless, answered by every node and merged
const F_BLOCK = 4; // always blocks the connection
const F_BLOCKOPT = 8; // blocks when a BLOCK / WAIT word is present

/** A command's key rule from the generated table, with the client's own routing flags. */
interface Spec {
  readonly name: string;
  readonly up: Buffer;
  readonly kind: KeyRule;
  readonly a: number;
  readonly b: number;
  readonly c: number;
  readonly words: readonly string[];
  readonly flags: number;
}

// Key positions come from COMMAND_KEY_RULES; these lists only say how a command is sent.
const FLAG_LISTS: [number, string][] = [
  [F_FANOUT, 'DBSIZE FLUSHALL FLUSHDB KEYS'],
  [F_BLOCK, 'WAIT BLMOVE BRPOPLPUSH BLPOP BRPOP BZPOPMIN BZPOPMAX BLMPOP'],
  [F_SPLIT, 'DEL EXISTS MGET MSET TOUCH UNLINK'],
  [F_BLOCKOPT, 'XREAD XREADGROUP FILLGET'],
];

// Sharded Pub/Sub goes to the node that owns the channel's slot: the channel stands where a
// key would. The server lists no keys for these.
const CHANNEL_RULES: [string, CommandKeyRule][] = [
  ['SPUBLISH', [KeyRule.Range, 1, 1, 1, [], 0]],
  ['SSUBSCRIBE', [KeyRule.Range, 1, -1, 1, [], 0]],
  ['SUNSUBSCRIBE', [KeyRule.Range, 1, -1, 1, [], 0]],
];

function nameHash(b: Uint8Array, o: number, len: number): number {
  let h = 2166136261;
  for (let j = 0; j < len; j++) {
    let c = b[o + j]!;
    if (c >= 97 && c <= 122) c -= 32;
    h = Math.imul(h ^ c, 16777619);
  }
  return h;
}

/** Commands the table does not know route by their first argument. */
const DEFAULT_RULE: CommandKeyRule = [KeyRule.Range, 1, 1, 1, [], 0];

const SPECS = new Map<number, Spec>();
/** Command names with a split / fan-out / blocking flag, for tests. */
export const ROUTED_COMMANDS: ReadonlyMap<string, number> = (() => {
  const flags = new Map<string, number>();
  for (const [f, names] of FLAG_LISTS) for (const name of names.split(' ')) flags.set(name, (flags.get(name) ?? 0) | f);
  const rules = new Map<string, CommandKeyRule>(COMMAND_KEY_RULES);
  for (const [name, rule] of CHANNEL_RULES) rules.set(name, rule);
  for (const name of flags.keys()) if (!rules.has(name)) rules.set(name, DEFAULT_RULE);
  for (const [name, [kind, a, b, c, words]] of rules) {
    const up = Buffer.from(name, 'latin1');
    const h = nameHash(up, 0, up.length);
    if (SPECS.has(h)) throw new Error('ruvio: command name hash collision: ' + name);
    SPECS.set(h, { name, up, kind, a, b, c, words, flags: flags.get(name) ?? 0 });
  }
  return flags;
})();
const DEFAULT_SPEC: Spec = { name: '', up: Buffer.alloc(0), kind: KeyRule.Range, a: 1, b: 1, c: 1, words: [], flags: 0 };

/** The routing rule of the command in the frame just scanned. */
function specOfFrame(b: Uint8Array): Spec {
  const o = A_OFF[0]!;
  const len = A_LEN[0]!;
  const s = SPECS.get(nameHash(b, o, len));
  if (s === undefined || s.up.length !== len) return DEFAULT_SPEC;
  for (let j = 0; j < len; j++) {
    let c = b[o + j]!;
    if (c >= 97 && c <= 122) c -= 32;
    if (c !== s.up[j]) return DEFAULT_SPEC;
  }
  return s;
}

function uint(b: Uint8Array, idx: number): number {
  if (idx >= A_N) return -1;
  const o = A_OFF[idx]!;
  const len = A_LEN[idx]!;
  if (len === 0 || len > 9) return -1;
  let n = 0;
  for (let j = 0; j < len; j++) {
    const d = b[o + j]! - 48;
    if (d < 0 || d > 9) return -1;
    n = n * 10 + d;
  }
  return n;
}

/** Argument `idx` equals the upper-case word `w`, ignoring case (letters only). */
function wordIs(b: Uint8Array, idx: number, w: string): boolean {
  if (A_LEN[idx] !== w.length) return false;
  const o = A_OFF[idx]!;
  for (let j = 0; j < w.length; j++) if ((b[o + j]! & 0xdf) !== w.charCodeAt(j)) return false;
  return true;
}

/** Fills KI with the key argument indexes of the scanned frame; returns how many. */
function keyIndexes(s: Spec, b: Uint8Array): number {
  const n = A_N;
  let c = 0;
  switch (s.kind) {
    case KeyRule.None:
      return 0;
    case KeyRule.Range: {
      // a..=b every c; a negative b counts from the end.
      const last = s.b < 0 ? n + s.b : s.b;
      for (let i = s.a; i <= last && i < n; i += s.c) KI[c++] = i;
      return c;
    }
    case KeyRule.NumKeys: {
      // The count at a, that many keys after it, and one key at b ahead of the count.
      if (s.b > 0 && s.b < n) KI[c++] = s.b;
      const end = s.a + 1 + uint(b, s.a);
      for (let i = s.a + 1; i < end && i < n; i++) KI[c++] = i;
      return c;
    }
    case KeyRule.KeywordHalf: {
      // The first half of the arguments after the keyword.
      for (let i = 1; i < n; i++) {
        if (wordIs(b, i, s.words[0]!)) {
          const half = ((n - i - 1) / 2) | 0;
          for (let k = 0; k < half; k++) KI[c++] = i + 1 + k;
          return c;
        }
      }
      return 0;
    }
    case KeyRule.KeyAndStore: {
      // The key at a, and the argument after each keyword.
      if (s.a < n) KI[c++] = s.a;
      for (let i = s.a + 1; i + 1 < n; i++) {
        for (const w of s.words) {
          if (wordIs(b, i, w)) {
            KI[c++] = ++i;
            break;
          }
        }
      }
      return c;
    }
  }
}

/** The key arguments of an encoded command, for tests: the keys as strings, in argument order. */
export function keysOfFrame(frame: Uint8Array): string[] {
  scanFrame(frame, 0);
  const spec = specOfFrame(frame);
  const n = keyIndexes(spec, frame);
  const out: string[] = [];
  for (let i = 0; i < n; i++) {
    const a = KI[i]!;
    out.push(Buffer.from(frame.buffer, frame.byteOffset + A_OFF[a]!, A_LEN[a]!).toString('utf8'));
  }
  return out;
}

// ---- errors ---------------------------------------------------------------------------------

function crossSlot(): RuvioError {
  return new RuvioError('CROSSSLOT Keys in request do not hash to the same slot');
}
function noSlot(slot: number): RuvioError {
  return new RuvioError('CLUSTERDOWN Hash slot ' + slot + ' is not served by any known node');
}

// Routing outcomes that are not a single node.
const T_SPLIT = 1;
const T_CROSS = 2;
const T_FANOUT = 3;
const T_NOSLOT = 4;

/** MOVED redirects followed per command before the MOVED error is returned. */
const MAX_REDIRECTS = 3;

// ---- nodes ----------------------------------------------------------------------------------

/** One shard: its address and its own multiplexed client. */
export class ClusterNode {
  readonly addr: string;
  /** Idle dedicated connections used for blocking commands. */
  readonly idle: RuvioClient[] = [];
  readonly client: RuvioClient;
  constructor(
    readonly host: string,
    readonly port: number,
    options: ClientOptions,
  ) {
    this.addr = host + ':' + port;
    this.client = new RuvioClient(options);
  }
  /** @internal */
  get conn(): Connection {
    return this.client.conn;
  }
}

export interface ClusterOptions extends ClientOptions {
  /**
   * `'auto'` (default): ask the seed for CLUSTER SLOTS and fall back to plain standalone mode when
   * the server answers `cluster mode is disabled`. `true`: a standalone seed is an error.
   */
  cluster?: 'auto' | boolean;
}

/** MOVED / refresh bookkeeping and the routing logic, shared by a client and its `binary` view. */
class Router {
  ready = false;
  standalone = false;
  /** Encode straight into the home connection (standalone fallback only). */
  direct = false;
  slots: (ClusterNode | undefined)[] = new Array(SLOT_COUNT);
  nodes = new Map<string, ClusterNode>();
  home: ClusterNode;
  readonly enc: Encoder;
  private c0: Buffer | null = null;
  private more: Buffer[] = [];
  private nChunks = 0;
  private loading: Promise<void> | null = null;
  private refreshing: Promise<void> | null = null;
  /** Number of CLUSTER SLOTS refreshes done after the first load. */
  refreshes = 0;
  /** Number of MOVED redirects followed. */
  redirects = 0;
  readonly base: ClientOptions;
  readonly strict: boolean;

  constructor(options: ClusterOptions) {
    const { cluster, ...rest } = options;
    this.strict = cluster === true;
    this.base = rest;
    const host = rest.host ?? '127.0.0.1';
    const port = rest.port ?? 6379;
    this.home = new ClusterNode(host, port, rest);
    this.nodes.set(this.home.addr, this.home);
    this.enc = new Encoder((c) => {
      if (this.nChunks++ === 0) this.c0 = c;
      else this.more.push(c);
    });
  }

  // ---- encoder hand-off ----

  /** The frame just encoded into the scratch encoder. */
  take(): Buffer {
    this.enc.flush();
    if (this.nChunks === 1) {
      const f = this.c0!;
      this.c0 = null;
      this.nChunks = 0;
      return f;
    }
    const f = Buffer.concat([this.c0!, ...this.more]);
    this.reset();
    return f;
  }

  reset(): void {
    this.c0 = null;
    this.more.length = 0;
    this.nChunks = 0;
  }

  // ---- topology ----

  whenReady(): Promise<void> {
    if (this.ready) return Promise.resolve();
    return (this.loading ??= this.load(true).then(
      () => {
        this.loading = null;
      },
      (e) => {
        this.loading = null;
        throw e;
      },
    ));
  }

  /** Re-reads CLUSTER SLOTS (concurrent callers share one request). */
  refresh(): Promise<void> {
    if (this.standalone) return Promise.resolve();
    return (this.refreshing ??= this.load(false).then(
      () => {
        this.refreshing = null;
        this.refreshes++;
      },
      (e) => {
        this.refreshing = null;
        throw e;
      },
    ));
  }

  private async load(first: boolean): Promise<void> {
    const tried: ClusterNode[] = [this.home];
    for (const n of this.nodes.values()) if (n !== this.home) tried.push(n);
    let last: unknown = null;
    let reply: unknown[] | null = null;
    for (const n of tried) {
      try {
        reply = (await n.client.clusterSlots()) as unknown[];
        break;
      } catch (e) {
        if (e instanceof RuvioError && first && !this.strict) {
          // "cluster mode is disabled": plain standalone server.
          this.standalone = true;
          this.ready = true;
          return;
        }
        last = e;
        if (e instanceof RuvioError) break;
      }
    }
    if (reply === null) throw last;
    const seedHost = this.base.host ?? '127.0.0.1';
    const fresh = new Map<string, ClusterNode>();
    const slots: (ClusterNode | undefined)[] = new Array(SLOT_COUNT);
    for (const range of reply as unknown[][]) {
      const from = range[0] as number;
      const to = range[1] as number;
      const m = range[2] as unknown[];
      const host = typeof m[0] === 'string' && m[0] !== '' ? (m[0] as string) : seedHost;
      const port = Number(m[1]);
      const addr = host + ':' + port;
      let node = fresh.get(addr) ?? this.nodes.get(addr);
      if (node === undefined) node = this.makeNode(host, port);
      fresh.set(addr, node);
      for (let s = from; s <= to; s++) slots[s] = node;
    }
    if (fresh.size === 0) throw new RuvioError('CLUSTERDOWN CLUSTER SLOTS returned no ranges');
    const old = this.nodes;
    this.slots = slots;
    this.nodes = fresh;
    if (!fresh.has(this.home.addr)) this.home = fresh.values().next().value!;
    this.ready = true;
    for (const [addr, n] of old) if (!fresh.has(addr)) this.drop(n);
  }

  private makeNode(host: string, port: number): ClusterNode {
    const o: ClientOptions = { ...this.base, host, port };
    delete o.path;
    return new ClusterNode(host, port, o);
  }

  private drop(n: ClusterNode): void {
    n.client.quit().catch(() => undefined);
    for (const c of n.idle) c.quit().catch(() => undefined);
    n.idle.length = 0;
  }

  nodeAt(addr: string): ClusterNode {
    let n = this.nodes.get(addr);
    if (n === undefined) {
      const i = addr.lastIndexOf(':');
      n = this.makeNode(addr.slice(0, i), Number(addr.slice(i + 1)));
      this.nodes.set(addr, n);
    }
    return n;
  }

  distinctNodes(): ClusterNode[] {
    return [...this.nodes.values()];
  }

  // ---- routing ----

  /** Single owner node, or a T_* code. Leaves the scan in A_* and the rule in LAST_SPEC. */
  target(b: Uint8Array, p: number): ClusterNode | number {
    scanFrame(b, p);
    const spec = (LAST_SPEC = specOfFrame(b));
    LAST_SLOT = -1;
    if (spec.kind === KeyRule.None) return (spec.flags & F_FANOUT) !== 0 ? T_FANOUT : this.home;
    const n = keyIndexes(spec, b);
    if (n === 0) return this.home;
    let slot = -1;
    for (let i = 0; i < n; i++) {
      const a = KI[i]!;
      const s = slotRange(b, A_OFF[a]!, A_OFF[a]! + A_LEN[a]!);
      if (slot < 0) slot = s;
      else if (s !== slot) return (spec.flags & F_SPLIT) !== 0 ? T_SPLIT : T_CROSS;
    }
    LAST_SLOT = slot;
    const node = this.slots[slot];
    return node === undefined ? T_NOSLOT : node;
  }

  private blocking(b: Uint8Array): boolean {
    const f = LAST_SPEC.flags;
    if ((f & F_BLOCK) !== 0) return true;
    if ((f & F_BLOCKOPT) === 0) return false;
    for (let i = 1; i < A_N; i++) if (wordIs(b, i, 'BLOCK') || wordIs(b, i, 'WAIT')) return true;
    return false;
  }

  dispatch(frame: Buffer, tf: Tf | null, fl: number): Promise<any> {
    const r = this.target(frame, 0);
    if (typeof r !== 'number') return this.sendTo(r, frame, tf, fl, this.blocking(frame));
    switch (r) {
      case T_SPLIT:
        return this.split(frame, tf, fl);
      case T_FANOUT:
        return this.fanout(frame, tf, fl);
      case T_CROSS:
        return Promise.reject(crossSlot());
      default:
        return Promise.reject(noSlot(LAST_SLOT));
    }
  }

  /** Sends one frame to a node; `-MOVED` replies are followed up to `MAX_REDIRECTS` times. */
  sendTo(node: ClusterNode, frame: Buffer, tf: Tf | null, fl: number, blocking: boolean): Promise<any> {
    if (blocking) return this.sendBlocking(node, frame, tf, fl);
    const conn = node.conn;
    conn.writeFrames(frame);
    return new Promise((resolve, reject) => {
      conn.trackCb(
        resolve,
        (e) => {
          if (e instanceof RuvioError && e.code === 'MOVED') this.moved(e, frame, tf, fl).then(resolve, reject);
          else reject(e);
        },
        tf,
        fl,
      );
    });
  }

  private sendBlocking(node: ClusterNode, frame: Buffer, tf: Tf | null, fl: number): Promise<any> {
    const c = node.idle.pop() ?? node.client.duplicate();
    const conn = c.conn;
    conn.writeFrames(frame);
    return conn.track(tf, fl).then(
      (v) => {
        this.release(node, c);
        return v;
      },
      (e) => {
        if (e instanceof ConnectionError) c.disconnect();
        else this.release(node, c);
        return this.onError(e, frame, tf, fl);
      },
    );
  }

  private release(node: ClusterNode, c: RuvioClient): void {
    if (node.idle.length < 8 && !c.conn.isClosed) node.idle.push(c);
    else c.disconnect();
  }

  onError(e: unknown, frame: Buffer, tf: Tf | null, fl: number): Promise<any> {
    if (e instanceof RuvioError && e.code === 'MOVED') return this.moved(e, frame, tf, fl);
    return Promise.reject(e);
  }

  /**
   * `MOVED slot host:port`: point that slot at the named node and send the command there. The
   * whole table is reloaded only for an unknown node or a second MOVED for the same command; the
   * MOVED after `MAX_REDIRECTS` redirects is returned as the error.
   */
  async moved(e: RuvioError, frame: Buffer, tf: Tf | null, fl: number, hop = 1): Promise<any> {
    const parts = e.message.split(' ');
    const slot = Number(parts[1]);
    const addr = parts[2];
    if (hop > MAX_REDIRECTS || addr === undefined || !(slot >= 0 && slot < SLOT_COUNT)) throw e;
    this.redirects++;
    let node = this.nodes.get(addr);
    if (node === undefined || hop > 1) {
      try {
        await this.refresh();
      } catch {
        /* keep the redirect target below */
      }
      node = this.nodeAt(addr);
    }
    this.slots[slot] = node;
    const conn = node.conn;
    conn.writeFrames(frame);
    try {
      return await conn.track(tf, fl);
    } catch (e2) {
      if (e2 instanceof RuvioError && e2.code === 'MOVED') return this.moved(e2, frame, tf, fl, hop + 1);
      throw e2;
    }
  }

  /** MGET / MSET / DEL / UNLINK / EXISTS / TOUCH across slots: one sub-command per node, merged in caller order. */
  private split(b: Buffer, tf: Tf | null, fl: number): Promise<any> {
    const spec = LAST_SPEC;
    const step = spec.c; // MSET: key value pairs
    // The server demands one slot per command (CROSSSLOT even inside one shard): group by slot.
    const groups = new Map<number, number[]>();
    for (let i = 1; i < A_N; i += step) {
      const s = slotRange(b, A_OFF[i]!, A_OFF[i]! + A_LEN[i]!);
      if (this.slots[s] === undefined) return Promise.reject(noSlot(s));
      let g = groups.get(s);
      if (g === undefined) groups.set(s, (g = []));
      g.push(i);
    }
    const nameEnd = A_OFF[0]! + A_LEN[0]! + 2;
    const nameBulk = b.subarray(A_H[0], nameEnd);
    const jobs: [ClusterNode, Buffer][] = [];
    const order: number[][] = [];
    for (const [slot, idx] of groups) {
      const node = this.slots[slot]!;
      const parts: Buffer[] = [Buffer.from('*' + (1 + idx.length * step) + '\r\n', 'latin1'), nameBulk];
      for (const i of idx) {
        for (let k = 0; k < step; k++) parts.push(b.subarray(A_H[i + k], A_OFF[i + k]! + A_LEN[i + k]! + 2));
      }
      jobs.push([node, Buffer.concat(parts)]);
      order.push(idx);
    }
    const total = A_N - 1;
    const name = spec.name;
    return Promise.all(jobs.map(([n, f]) => this.sendTo(n, f, null, fl, false))).then((rs) => {
      let out: unknown;
      if (name === 'MGET') {
        const arr = new Array(total);
        for (let g = 0; g < rs.length; g++) {
          const part = rs[g] as unknown[];
          const idx = order[g]!;
          for (let k = 0; k < idx.length; k++) arr[idx[k]! - 1] = part[k];
        }
        out = arr;
      } else if (name === 'MSET') {
        out = rs[0];
      } else {
        let sum = 0;
        for (const r of rs) sum += r as number;
        out = sum;
      }
      return tf === null ? out : tf(out);
    });
  }

  /** FLUSHDB / FLUSHALL / DBSIZE / KEYS: ask every node, merge. */
  private fanout(b: Buffer, tf: Tf | null, fl: number): Promise<any> {
    const name = LAST_SPEC.name;
    const nodes = this.distinctNodes();
    return Promise.all(nodes.map((n) => this.sendTo(n, b, null, fl, false))).then((rs) => {
      let out: unknown;
      if (name === 'DBSIZE') {
        let sum = 0;
        for (const r of rs) sum += r as number;
        out = sum;
      } else if (name === 'KEYS') {
        out = ([] as unknown[]).concat(...(rs as unknown[][]));
      } else out = rs[0];
      return tf === null ? out : tf(out);
    });
  }

  // ---- batches ----

  /** Pipeline: group frames by node, one write per node, replies back into the caller's slots. */
  runBatch(bytes: Buffer, n: number, fls: number[], out: PipelineSink): void {
    if (this.standalone) {
      const conn = this.home.conn;
      conn.writeFrames(bytes);
      conn.trackBatch(out, fls, n);
      return;
    }
    // Frames are routed in call order: each goes into its node's write buffer (or its own
    // split / fan-out sub-commands) as it is met, so per-node ordering matches the caller's.
    const sinks = new Map<ClusterNode, GroupSink>();
    let p = 0;
    for (let i = 0; i < n; i++) {
      const start = p;
      const r = this.target(bytes, p);
      p = A_END;
      const frame = bytes.subarray(start, p);
      if (typeof r !== 'number') {
        let g = sinks.get(r);
        if (g === undefined) sinks.set(r, (g = new GroupSink(this, out)));
        const conn = r.conn;
        conn.writeFrames(frame);
        g.frames.push(frame);
        g.idx.push(i);
        g.fls.push(fls[i]!);
        conn.trackOne(g, g.idx.length - 1, fls[i]!);
      } else if (r === T_CROSS) out.set(i, crossSlot());
      else if (r === T_NOSLOT) out.set(i, noSlot(-1));
      else {
        this.dispatch(frame, null, fls[i]!).then(
          (v) => out.set(i, v),
          (e) => out.set(i, e),
        );
      }
    }
  }

  /** The unserved slot that made `batchSlot` return -3. */
  noSlotSlot = -1;

  /**
   * The slot shared by every keyed command in `body`, -1 when there is none, -2 when they differ,
   * -3 when the slot has no owner (see `noSlotSlot`).
   */
  batchSlot(body: Buffer, n: number): number {
    let slot = -1;
    let p = 0;
    for (let i = 0; i < n; i++) {
      const r = this.target(body, p);
      p = A_END;
      if (typeof r === 'number') {
        if (r === T_FANOUT) continue;
        if (r === T_NOSLOT) {
          this.noSlotSlot = LAST_SLOT;
          return -3;
        }
        return -2;
      }
      if (LAST_SLOT < 0) continue;
      if (slot < 0) slot = LAST_SLOT;
      else if (slot !== LAST_SLOT) return -2;
    }
    return slot;
  }
}

/** Receives one node's share of a pipeline and follows MOVED per command. */
class GroupSink implements BatchSink {
  readonly frames: Buffer[] = [];
  readonly idx: number[] = [];
  readonly fls: number[] = [];
  constructor(
    private readonly r: Router,
    private readonly out: PipelineSink,
  ) {}
  set(local: number, v: unknown): void {
    const g = this.idx[local]!;
    if (v instanceof RuvioError && v.code === 'MOVED') {
      this.r.moved(v, this.frames[local]!, null, this.fls[local]!).then(
        (x) => this.out.set(g, x),
        (e) => this.out.set(g, e),
      );
      return;
    }
    this.out.set(g, v);
  }
}

// ---- batches (client side) ------------------------------------------------------------------

class ClusterPipelineImpl extends Batch {
  constructor(private readonly r: Router) {
    super(null as unknown as Connection);
  }
  exec(): Promise<unknown[]> {
    const n = this.tfs.length;
    if (n === 0) return Promise.resolve([]);
    const bytes = this.benc.take();
    const tfs = this.tfs;
    const fls = this.fls;
    this.tfs = [];
    this.fls = [];
    const r = this.r;
    return new Promise<unknown[]>((resolve, reject) => {
      const out = new PipelineSink(n, tfs, resolve, reject);
      if (r.ready) r.runBatch(bytes, n, fls, out);
      else r.whenReady().then(() => r.runBatch(bytes, n, fls, out), reject);
    });
  }
  async execStrict(): Promise<unknown[]> {
    const out = await this.exec();
    for (const v of out) if (v instanceof Error) throw v;
    return out;
  }
}

class ClusterTransactionImpl extends TransactionImpl {
  constructor(private readonly r: Router) {
    super(null as unknown as Connection);
  }
  override exec(): Promise<unknown[] | null> {
    const r = this.r;
    if (!r.ready) return r.whenReady().then(() => this.exec());
    if (r.standalone) {
      this.conn = r.home.conn;
      return super.exec();
    }
    const n = this.tfs.length;
    const body = this.benc.buf.subarray(this.benc.start, this.benc.pos);
    const slot = r.batchSlot(body, n);
    if (slot === -2) {
      this.discard();
      return Promise.reject(crossSlot());
    }
    if (slot === -3) {
      this.discard();
      return Promise.reject(noSlot(r.noSlotSlot));
    }
    const node = slot < 0 ? r.home : r.slots[slot];
    if (node === undefined) {
      this.discard();
      return Promise.reject(noSlot(slot));
    }
    this.conn = node.conn;
    return super.exec();
  }
}

// ---- the client -----------------------------------------------------------------------------

/**
 * A cluster-aware client. Same typed commands, helpers, `pipeline()` and `multi()` as
 * {@link RuvioClient}; every command is routed to the node that owns its key. Against a
 * standalone server it falls back to a plain single connection automatically.
 */
export class ClusterClient extends Helpers {
  /** @internal */
  readonly r: Router;
  readonly options: ClusterOptions;
  private _binary: BinaryCommands | null = null;

  constructor(options: ClusterOptions | string = {}) {
    super();
    const o = typeof options === 'string' ? parseSeed(options) : { ...options };
    this.options = o;
    this.r = new Router(o);
  }

  protected _enc(): Encoder {
    const r = this.r;
    if (r.standalone) {
      r.direct = true;
      return r.home.conn.encoder();
    }
    r.direct = false;
    return r.enc;
  }
  protected _done(tf: Tf | null, fl: number): Promise<any> {
    const r = this.r;
    if (r.direct) return r.home.conn.track(tf, fl);
    const frame = r.take();
    if (!r.ready) return r.whenReady().then(() => this.routeLate(frame, tf, fl));
    return r.dispatch(frame, tf, fl);
  }
  private routeLate(frame: Buffer, tf: Tf | null, fl: number): Promise<any> {
    const r = this.r;
    if (r.standalone) {
      r.home.conn.writeFrames(frame);
      return r.home.conn.track(tf, fl);
    }
    return r.dispatch(frame, tf, fl);
  }
  protected _fail(e: unknown, enc: Encoder): Promise<never> {
    const r = this.r;
    if (r.direct) {
      if (!enc.abortFrame()) r.home.conn.destroySocket(e as Error);
    } else {
      enc.abortFrame();
      r.reset();
    }
    return Promise.reject(e);
  }

  /** Reads the topology from the seed (optional: the first command does it). */
  async connect(): Promise<void> {
    await this.r.whenReady();
    await this.r.home.client.connect();
  }
  /** Waits for in-flight replies on every node, then closes them all. */
  async quit(): Promise<void> {
    await Promise.all(this.allClients().map((c) => c.quit().catch(() => undefined)));
  }
  /** Closes every node connection now. */
  disconnect(): void {
    for (const c of this.allClients()) c.disconnect();
  }
  async [Symbol.asyncDispose](): Promise<void> {
    await this.quit();
  }
  private allClients(): RuvioClient[] {
    const out: RuvioClient[] = [];
    for (const n of this.r.nodes.values()) {
      out.push(n.client);
      for (const c of n.idle) out.push(c);
    }
    return out;
  }

  /** True when the seed is a cluster (known after the first command or `connect()`). */
  get isCluster(): boolean {
    return this.r.ready && !this.r.standalone;
  }
  /** The nodes of the current slot table. */
  get nodes(): readonly ClusterNode[] {
    return this.r.distinctNodes();
  }
  /** CLUSTER SLOTS refreshes done so far (after the first load). */
  get refreshCount(): number {
    return this.r.refreshes;
  }
  /** `-MOVED` redirects followed so far. */
  get redirectCount(): number {
    return this.r.redirects;
  }
  /** Commands awaiting a reply on all nodes. */
  get inFlight(): number {
    let n = 0;
    for (const c of this.allClients()) n += c.inFlight;
    return n;
  }
  /** Re-reads CLUSTER SLOTS now. */
  refreshSlots(): Promise<void> {
    return this.r.refresh();
  }
  /** The node that owns `key` (the topology must be loaded: `await connect()` first). */
  nodeFor(key: Key): ClusterNode {
    const r = this.r;
    if (!r.ready) throw new Error('ruvio: cluster topology not loaded yet, await connect() first');
    if (r.standalone) return r.home;
    const n = r.slots[hashSlot(key)];
    if (n === undefined) throw noSlot(hashSlot(key));
    return n;
  }
  /** The hash slot of a key. */
  slotOf(key: Key): number {
    return hashSlot(key);
  }

  /** The same commands, resolving bulk strings as Buffers. */
  get binary(): BinaryCommands {
    if (this._binary === null) {
      const view = Object.create(this) as ClusterClient;
      view._fl = 1;
      this._binary = view as unknown as BinaryCommands;
    }
    return this._binary;
  }

  /** A new cluster client with the same options (its own connections). */
  duplicate(overrides: ClusterOptions = {}): ClusterClient {
    return new ClusterClient({ ...this.options, ...overrides });
  }

  /**
   * Runs `fn` on a dedicated connection to the node that owns `key` (WATCH, or blocking reads
   * you want kept off the shared connection), closed afterwards.
   */
  async isolated<T>(key: Key, fn: (client: RuvioClient) => Promise<T>): Promise<T> {
    await this.r.whenReady();
    const c = this.nodeFor(key).client.duplicate();
    try {
      return await fn(c);
    } finally {
      await c.quit();
    }
  }

  /** A pipeline: commands are grouped by node, one write per node; replies come back in call order. */
  pipeline(): Pipeline {
    return new ClusterPipelineImpl(this.r) as unknown as Pipeline;
  }

  /** A MULTI/EXEC transaction; all keys must hash to one slot (otherwise `exec()` rejects with CROSSSLOT). */
  multi(): Transaction {
    return new ClusterTransactionImpl(this.r) as unknown as Transaction;
  }

  /** A Pub/Sub connection to the seed node (any node serves SUBSCRIBE/PSUBSCRIBE). */
  subscriber(opts: Omit<SubscriberOptions, 'host' | 'port' | 'path'> = {}): Subscriber {
    const h = this.r.home;
    return new Subscriber({ ...this.r.base, host: h.host, port: h.port, ...opts });
  }
  /** A Pub/Sub connection to the node that owns `channel`'s slot (for SSUBSCRIBE). */
  async subscriberFor(channel: string, opts: Omit<SubscriberOptions, 'host' | 'port' | 'path'> = {}): Promise<Subscriber> {
    await this.r.whenReady();
    const n = this.nodeFor(channel);
    return new Subscriber({ ...this.r.base, host: n.host, port: n.port, ...opts });
  }

  /**
   * Lazily walks `SCAN` on every node, one node after the other (the keys of the whole cluster).
   * A plain `scan(cursor)` call only reaches one node.
   */
  override async *scanIterator(opts?: ScanOptions): AsyncGenerator<string> {
    await this.r.whenReady();
    for (const n of this.r.distinctNodes()) yield* n.client.scanIterator(opts);
  }
}

function parseSeed(url: string): ClusterOptions {
  const u = new URL(url);
  const o: ClusterOptions = { host: u.hostname || '127.0.0.1', port: u.port ? Number(u.port) : 6379 };
  if (u.protocol === 'rediss:' || u.protocol === 'ruvios:') o.tls = true;
  if (u.username) o.username = decodeURIComponent(u.username);
  if (u.password) o.password = decodeURIComponent(u.password);
  return o;
}

/**
 * Creates a cluster-aware client from a seed address (`'redis://127.0.0.1:7000'` or options).
 * With the default `cluster: 'auto'` a standalone seed simply works as a plain client.
 */
export function createClusterClient(options: ClusterOptions | string = {}): ClusterClient {
  return new ClusterClient(options);
}

/** @internal test hook: replaces the slot table owner of every slot with `node`. */
export function _forceStaleTable(c: ClusterClient, node: ClusterNode): void {
  c.r.slots.fill(node);
}
