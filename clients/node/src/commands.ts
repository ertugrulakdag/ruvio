// SPDX-License-Identifier: MIT
// Typed methods for every command in docs/compatibility-matrix.json.
// Shared by the client (returns promises), pipelines and transactions (queue frames).

import { C } from './command-names.js';
import { cmd, Encoder, type Arg, type Cmd } from './encoder.js';
import { T, type Tf } from './transforms.js';
import type {
  Key,
  Value,
  SetOptions,
  ExpireCondition,
  ScanOptions,
  ScanPage,
  SortedSetEntry,
  PoppedEntry,
  ZAddOptions,
  ZRangeOptions,
  ZStoreOptions,
  ListPopResult,
  LPosOptions,
  GeoMember,
  GeoUnit,
  GeoSearchOptions,
  GeoRadiusOptions,
  GeoMatch,
  BfReserveOptions,
  BfInsertOptions,
  StreamEntry,
  StreamReadResult,
  XAddOptions,
  XTrimOptions,
  XReadOptions,
  XReadGroupOptions,
  XClaimOptions,
  AutoClaimResult,
  PendingSummary,
  PendingRow,
  FillResult,
  IdemBeginResult,
  ChangeNote,
  RestoreOptions,
  ClientKillOptions,
  BitUnit,
} from './types.js';


// Option words as Buffers: encoded with a single copy, no string scan.
const w = (s: string): Buffer => Buffer.from(s, 'latin1');
const W = {
  EX: w('EX'), PX: w('PX'), EXAT: w('EXAT'), PXAT: w('PXAT'), KEEPTTL: w('KEEPTTL'), NX: w('NX'), XX: w('XX'),
  GET: w('GET'), IFEQ: w('IFEQ'), MAX: w('MAX'), WAIT: w('WAIT'), SWR: w('SWR'), BEGIN: w('BEGIN'),
  COMPLETE: w('COMPLETE'), START: w('START'), MATCH: w('MATCH'), COUNT: w('COUNT'), TYPE: w('TYPE'),
  ASYNC: w('ASYNC'), REPLACE: w('REPLACE'), ABSTTL: w('ABSTTL'), IDLETIME: w('IDLETIME'), FREQ: w('FREQ'),
  SAMPLES: w('SAMPLES'), DB: w('DB'), RANK: w('RANK'), MAXLEN: w('MAXLEN'), LIMIT: w('LIMIT'),
  WITHSCORES: w('WITHSCORES'), WITHVALUES: w('WITHVALUES'), BYSCORE: w('BYSCORE'), BYLEX: w('BYLEX'), REV: w('REV'),
  GT: w('GT'), LT: w('LT'), CH: w('CH'), INCR: w('INCR'), WEIGHTS: w('WEIGHTS'), AGGREGATE: w('AGGREGATE'),
  FIELDS: w('FIELDS'), FROMMEMBER: w('FROMMEMBER'), FROMLONLAT: w('FROMLONLAT'), BYRADIUS: w('BYRADIUS'),
  BYBOX: w('BYBOX'), ANY: w('ANY'), WITHCOORD: w('WITHCOORD'), WITHDIST: w('WITHDIST'), WITHHASH: w('WITHHASH'),
  STORE: w('STORE'), STOREDIST: w('STOREDIST'), EXPANSION: w('EXPANSION'), NONSCALING: w('NONSCALING'),
  CAPACITY: w('CAPACITY'), ERROR: w('ERROR'), NOCREATE: w('NOCREATE'), ITEMS: w('ITEMS'), PREFIX: w('PREFIX'),
  PATH: w('PATH'), VALUE: w('VALUE'), AFTER: w('AFTER'), NOMKSTREAM: w('NOMKSTREAM'), MINID: w('MINID'),
  TILDE: w('~'), DELAY: w('DELAY'), BLOCK: w('BLOCK'), STREAMS: w('STREAMS'), MKSTREAM: w('MKSTREAM'),
  GROUP: w('GROUP'), NOACK: w('NOACK'), GTID: w('>'), IDLE: w('IDLE'), JUSTID: w('JUSTID'), TIME: w('TIME'),
  RETRYCOUNT: w('RETRYCOUNT'), FORCE: w('FORCE'), LASTID: w('LASTID'), ON: w('ON'), OFF: w('OFF'), ID: w('ID'),
  ADDR: w('ADDR'), SKIPME: w('SKIPME'), YES: w('yes'), NO: w('no'), SETNAME: w('SETNAME'), AUTH: w('AUTH'),
  OWNER: w('OWNER'), OVERFLOW: w('OVERFLOW'),
} as const;

type R<T> = Promise<T>;

function pushScan(a: Arg[], o?: ScanOptions): Arg[] {
  if (o !== undefined) {
    if (o.match !== undefined) a.push(W.MATCH, o.match);
    if (o.count !== undefined) a.push(W.COUNT, o.count);
    if (o.type !== undefined) a.push(W.TYPE, o.type);
  }
  return a;
}

function pushAll(a: Arg[], items: readonly Arg[]): Arg[] {
  for (let i = 0; i < items.length; i++) a.push(items[i]!);
  return a;
}

function flatPairs(a: Arg[], pairs: Record<string, Value> | ReadonlyArray<readonly [Key, Value]>): Arg[] {
  if (Array.isArray(pairs)) {
    for (const p of pairs as ReadonlyArray<readonly [Key, Value]>) a.push(p[0], p[1]);
  } else {
    for (const k in pairs) a.push(k, (pairs as Record<string, Value>)[k]!);
  }
  return a;
}

function geoTf(o: { withCoord?: boolean; withDist?: boolean; withHash?: boolean }): Tf | null {
  if (!o.withCoord && !o.withDist && !o.withHash) return null;
  return (v: unknown[]) =>
    v.map((row) => {
      const r = row as unknown[];
      const m: GeoMatch = { member: r[0] as string };
      let i = 1;
      if (o.withDist) m.distance = Number(r[i++]);
      if (o.withHash) m.hash = Number(r[i++]);
      if (o.withCoord) {
        const c = r[i++] as unknown[];
        m.coordinates = [Number(c[0]), Number(c[1])];
      }
      return m;
    });
}

function pushGeoShape(a: Arg[], o: GeoSearchOptions): void {
  if (o.fromMember !== undefined) a.push(W.FROMMEMBER, o.fromMember);
  if (o.fromLonLat !== undefined) a.push(W.FROMLONLAT, o.fromLonLat[0], o.fromLonLat[1]);
  if (o.byRadius !== undefined) a.push(W.BYRADIUS, o.byRadius.radius, o.byRadius.unit);
  if (o.byBox !== undefined) a.push(W.BYBOX, o.byBox.width, o.byBox.height, o.byBox.unit);
  if (o.order !== undefined) a.push(o.order);
  if (o.count !== undefined) {
    a.push(W.COUNT, o.count);
    if (o.any) a.push(W.ANY);
  }
}

function pushZStore(a: Arg[], keys: readonly Key[], o?: ZStoreOptions): Arg[] {
  a.push(keys.length);
  pushAll(a, keys);
  if (o !== undefined) {
    if (o.weights !== undefined) {
      a.push(W.WEIGHTS);
      pushAll(a, o.weights);
    }
    if (o.aggregate !== undefined) a.push(W.AGGREGATE, o.aggregate);
  }
  return a;
}

function pushStreamId(a: Arg[], o: XTrimOptions | XAddOptions): void {
  if (o.maxLen !== undefined) {
    a.push(W.MAXLEN);
    if (o.approximate) a.push(W.TILDE);
    a.push(o.maxLen);
  } else if (o.minId !== undefined) {
    a.push(W.MINID);
    if (o.approximate) a.push(W.TILDE);
    a.push(o.minId);
  }
}

function pushFields(a: Arg[], fields: Record<string, Value> | readonly Value[]): Arg[] {
  if (Array.isArray(fields)) return pushAll(a, fields as Value[]);
  for (const k in fields) a.push(k, (fields as Record<string, Value>)[k]!);
  return a;
}

/**
 * Every typed command. Subclasses decide what "send" means: the client returns a promise per
 * call, a pipeline queues the frame and returns itself.
 */
export abstract class Commands {
  /** @internal Reply flags for this view: bit 0 = bulk strings as Buffers. */
  _fl = 0;

  /** @internal The encoder the next frame goes into. */
  protected abstract _enc(): Encoder;
  /** @internal Called after a frame is complete. */
  protected abstract _done(tf: Tf | null, fl: number): any;
  /** @internal Called when a frame could not be encoded. */
  protected abstract _fail(e: unknown, enc: Encoder): any;

  /** @internal */
  _0(tf: Tf | null, c: Cmd): any {
    const e = this._enc();
    e.begin(c, 0);
    e.endFrame();
    return this._done(tf, this._fl);
  }

  /** @internal */
  _1(tf: Tf | null, c: Cmd, a: Arg): any {
    const e = this._enc();
    try {
      e.begin(c, 1);
      e.arg(a);
    } catch (x) {
      return this._fail(x, e);
    }
    e.endFrame();
    return this._done(tf, this._fl);
  }

  /** @internal */
  _2(tf: Tf | null, c: Cmd, a: Arg, b: Arg): any {
    const e = this._enc();
    try {
      e.begin(c, 2);
      e.arg(a);
      e.arg(b);
    } catch (x) {
      return this._fail(x, e);
    }
    e.endFrame();
    return this._done(tf, this._fl);
  }

  /** @internal */
  _3(tf: Tf | null, c: Cmd, a: Arg, b: Arg, d: Arg): any {
    const e = this._enc();
    try {
      e.begin(c, 3);
      e.arg(a);
      e.arg(b);
      e.arg(d);
    } catch (x) {
      return this._fail(x, e);
    }
    e.endFrame();
    return this._done(tf, this._fl);
  }

  /** @internal */
  _4(tf: Tf | null, c: Cmd, a: Arg, b: Arg, d: Arg, f: Arg): any {
    const e = this._enc();
    try {
      e.begin(c, 4);
      e.arg(a);
      e.arg(b);
      e.arg(d);
      e.arg(f);
    } catch (x) {
      return this._fail(x, e);
    }
    e.endFrame();
    return this._done(tf, this._fl);
  }

  /** @internal */
  _v(tf: Tf | null, c: Cmd, args: readonly Arg[]): any {
    const e = this._enc();
    try {
      const n = args.length;
      e.begin(c, n);
      for (let i = 0; i < n; i++) e.arg(args[i]!);
    } catch (x) {
      return this._fail(x, e);
    }
    e.endFrame();
    return this._done(tf, this._fl);
  }

  /** @internal Key followed by a variadic list. */
  _kv(tf: Tf | null, c: Cmd, key: Arg, rest: readonly Arg[]): any {
    const e = this._enc();
    try {
      const n = rest.length;
      e.begin(c, n + 1);
      e.arg(key);
      for (let i = 0; i < n; i++) e.arg(rest[i]!);
    } catch (x) {
      return this._fail(x, e);
    }
    e.endFrame();
    return this._done(tf, this._fl);
  }

  /** @internal Runs `f` with bulk strings returned as Buffers. */
  _bin<X>(f: () => X): X {
    const prev = this._fl;
    this._fl = prev | 1;
    try {
      return f();
    } finally {
      this._fl = prev;
    }
  }

  /**
   * Sends any command. `args[0]` is the command name (may contain no spaces; pass subcommands
   * as separate arguments). Replies come back raw (strings, numbers, arrays, null).
   */
  call(command: string, ...args: Arg[]): R<unknown> {
    return this._v(null, cmd(command), args);
  }

  /** Like {@link call}, but bulk strings come back as Buffers. */
  callBuffer(command: string, ...args: Arg[]): R<unknown> {
    return this._bin(() => this._v(null, cmd(command), args));
  }

  /** Sends a command with a pre-encoded name (see {@link Cmds}); the fastest generic path. */
  callCmd(c: Cmd, ...args: Arg[]): R<unknown> {
    return this._v(null, c, args);
  }

  // ---- connection ------------------------------------------------------------------------

  /** `RUVIO` (reply "PONG is taken") or `RUVIO OWNER` (names the shard rule). */
  ruvio(owner?: boolean): R<string> {
    return owner ? this._1(null, C.RUVIO, W.OWNER) : this._0(null, C.RUVIO);
  }
  ping(message?: Value): R<string> {
    return message === undefined ? this._0(null, C.PING) : this._1(null, C.PING, message);
  }
  /**
   * `HELLO [2|3] [AUTH user pass] [SETNAME name]`. Switching to 3 on a shared client changes reply
   * shapes for every caller; prefer the `protocol: 3` connection option.
   */
  hello(protocol?: 2 | 3, opts?: { auth?: { username: string; password: string }; setName?: string }): R<Record<string, unknown>> {
    const a: Arg[] = [];
    if (protocol !== undefined) {
      a.push(protocol);
      if (opts?.auth) a.push(W.AUTH, opts.auth.username, opts.auth.password);
      if (opts?.setName !== undefined) a.push(W.SETNAME, opts.setName);
    }
    return this._v(T.map, C.HELLO, a);
  }
  /** `AUTH password` or `AUTH username password`. */
  auth(passwordOrUser: string, password?: string): R<'OK'> {
    return password === undefined ? this._1(null, C.AUTH, passwordOrUser) : this._2(null, C.AUTH, passwordOrUser, password);
  }
  /** `SELECT db`. On the shared client this changes the database for every caller (and is kept across reconnects). */
  select(db: number): R<'OK'> {
    return this._1(null, C.SELECT, db);
  }
  echo(message: Value): R<string> {
    return this._1(null, C.ECHO, message);
  }
  /** `COMMAND`: the full command table. */
  command(): R<unknown[]> {
    return this._0(null, C.COMMAND);
  }
  commandCount(): R<number> {
    return this._0(null, C.COMMAND_COUNT);
  }
  commandInfo(...names: string[]): R<unknown[]> {
    return this._v(null, C.COMMAND_INFO, names);
  }
  commandDocs(...names: string[]): R<unknown> {
    return this._v(null, C.COMMAND_DOCS, names);
  }
  clientGetname(): R<string | null> {
    return this._0(null, C.CLIENT_GETNAME);
  }
  clientSetname(name: string): R<'OK'> {
    return this._1(null, C.CLIENT_SETNAME, name);
  }
  /** `CLIENT TRACKING ON|OFF` (ON needs RESP3; invalidations arrive as `push` events). */
  clientTracking(on: boolean): R<'OK'> {
    return this._1(null, C.CLIENT_TRACKING, on ? W.ON : W.OFF);
  }
  clientId(): R<number> {
    return this._0(null, C.CLIENT_ID);
  }
  /** `CLIENT LIST [TYPE type]`. */
  clientList(type?: string): R<string> {
    return type === undefined ? this._0(T.lines, C.CLIENT_LIST) : this._2(T.lines, C.CLIENT_LIST, W.TYPE, type);
  }
  clientInfo(): R<string> {
    return this._0(T.lines, C.CLIENT_INFO);
  }
  /** `CLIENT KILL addr` (legacy, replies OK) or `CLIENT KILL [ID id] [ADDR addr] [SKIPME yes|no]` (count). */
  clientKill(target: string | ClientKillOptions): R<number | 'OK'> {
    if (typeof target === 'string') return this._1(null, C.CLIENT_KILL, target);
    const a: Arg[] = [];
    if (target.id !== undefined) a.push(W.ID, target.id);
    if (target.addr !== undefined) a.push(W.ADDR, target.addr);
    if (target.skipMe !== undefined) a.push(W.SKIPME, target.skipMe ? W.YES : W.NO);
    return this._v(null, C.CLIENT_KILL, a);
  }
  slowlogGet(count?: number): R<unknown[]> {
    return count === undefined ? this._0(null, C.SLOWLOG_GET) : this._1(null, C.SLOWLOG_GET, count);
  }
  slowlogLen(): R<number> {
    return this._0(null, C.SLOWLOG_LEN);
  }
  slowlogReset(): R<'OK'> {
    return this._0(null, C.SLOWLOG_RESET);
  }
  /** `LATENCY LATEST` (always empty on Ruvio). */
  latencyLatest(): R<unknown[]> {
    return this._0(null, C.LATENCY_LATEST);
  }

  // ---- acl -------------------------------------------------------------------------------

  aclWhoami(): R<string> {
    return this._0(null, C.ACL_WHOAMI);
  }
  aclUsers(): R<string[]> {
    return this._0(null, C.ACL_USERS);
  }
  aclList(): R<string[]> {
    return this._0(null, C.ACL_LIST);
  }
  aclGetuser(user: string): R<Record<string, unknown> | null> {
    return this._1((v) => (v === null ? null : T.map(v)), C.ACL_GETUSER, user);
  }
  aclCat(category?: string): R<string[]> {
    return category === undefined ? this._0(null, C.ACL_CAT) : this._1(null, C.ACL_CAT, category);
  }
  /** `ACL SETUSER user rule...` e.g. `aclSetuser('app', 'on', '>secret', '+get', '~app:*')`. */
  aclSetuser(user: string, ...rules: string[]): R<'OK'> {
    return this._kv(null, C.ACL_SETUSER, user, rules);
  }
  aclLoad(): R<'OK'> {
    return this._0(null, C.ACL_LOAD);
  }
  aclSave(): R<'OK'> {
    return this._0(null, C.ACL_SAVE);
  }

  // ---- strings ---------------------------------------------------------------------------

  get(key: Key): R<string | null> {
    return this._1(null, C.GET, key);
  }
  /** `GET` returning raw bytes. */
  getBuffer(key: Key): R<Buffer | null> {
    return this._bin(() => this._1(null, C.GET, key));
  }
  /**
   * `SET key value [EX|PX|EXAT|PXAT|KEEPTTL] [NX|XX] [GET]` or Ruvio's `SET key value IFEQ expected [...]`.
   * Resolves `'OK'`, `null` when NX/XX/IFEQ did not write, or the previous value with `get`.
   */
  set(key: Key, value: Value, opts?: SetOptions): R<string | null> {
    if (opts === undefined) return this._2(null, C.SET, key, value);
    const a: Arg[] = [key, value];
    if (opts.ifEq !== undefined) a.push(W.IFEQ, opts.ifEq);
    if (opts.ex !== undefined) a.push(W.EX, opts.ex);
    else if (opts.px !== undefined) a.push(W.PX, opts.px);
    else if (opts.exat !== undefined) a.push(W.EXAT, opts.exat);
    else if (opts.pxat !== undefined) a.push(W.PXAT, opts.pxat);
    else if (opts.keepTtl) a.push(W.KEEPTTL);
    if (opts.nx) a.push(W.NX);
    else if (opts.xx) a.push(W.XX);
    if (opts.get) a.push(W.GET);
    return this._v(null, C.SET, a);
  }
  /** `SETV key value [version]`: versioned write; returns the new version. */
  setv(key: Key, value: Value, version?: number): R<number> {
    return version === undefined ? this._2(null, C.SETV, key, value) : this._3(null, C.SETV, key, value, version);
  }
  /** `TAKE key count`: the remainder, or `null` when the key holds less than `count` (nothing changes). */
  take(key: Key, count: number): R<number | null> {
    return this._2(null, C.TAKE, key, count);
  }
  /** `DELEX key IFEQ value`: deletes only when the string equals `value`. */
  delex(key: Key, ifEq: Value): R<boolean> {
    return this._3(T.bool, C.DELEX, key, W.IFEQ, ifEq);
  }
  /** `GETEX key ttlMs` (Ruvio form): returns the value and sets its TTL in milliseconds. */
  getex(key: Key, ttlMs: number): R<string | null> {
    return this._2(null, C.GETEX, key, ttlMs);
  }
  mget(...keys: Key[]): R<(string | null)[]> {
    return this._v(null, C.MGET, keys);
  }
  /** `MGET` returning raw bytes. */
  mgetBuffer(...keys: Key[]): R<(Buffer | null)[]> {
    return this._bin(() => this._v(null, C.MGET, keys));
  }
  mset(pairs: Record<string, Value> | ReadonlyArray<readonly [Key, Value]>): R<'OK'> {
    return this._v(null, C.MSET, flatPairs([], pairs));
  }
  msetnx(pairs: Record<string, Value> | ReadonlyArray<readonly [Key, Value]>): R<boolean> {
    return this._v(T.bool, C.MSETNX, flatPairs([], pairs));
  }
  getset(key: Key, value: Value): R<string | null> {
    return this._2(null, C.GETSET, key, value);
  }
  append(key: Key, value: Value): R<number> {
    return this._2(null, C.APPEND, key, value);
  }
  strlen(key: Key): R<number> {
    return this._1(null, C.STRLEN, key);
  }
  incr(key: Key): R<number> {
    return this._1(null, C.INCR, key);
  }
  decr(key: Key): R<number> {
    return this._1(null, C.DECR, key);
  }
  incrby(key: Key, delta: number): R<number> {
    return this._2(null, C.INCRBY, key, delta);
  }
  /** `INCRBY key delta MAX limit`: `null` (and no write) when the result would pass `limit`. */
  incrbyMax(key: Key, delta: number, limit: number): R<number | null> {
    return this._4(null, C.INCRBY, key, delta, W.MAX, limit);
  }
  decrby(key: Key, delta: number): R<number> {
    return this._2(null, C.DECRBY, key, delta);
  }
  setex(key: Key, seconds: number, value: Value): R<'OK'> {
    return this._3(null, C.SETEX, key, seconds, value);
  }
  psetex(key: Key, ms: number, value: Value): R<'OK'> {
    return this._3(null, C.PSETEX, key, ms, value);
  }
  setnx(key: Key, value: Value): R<boolean> {
    return this._2(T.bool, C.SETNX, key, value);
  }
  getdel(key: Key): R<string | null> {
    return this._1(null, C.GETDEL, key);
  }
  incrbyfloat(key: Key, delta: number): R<number> {
    return this._2(T.float, C.INCRBYFLOAT, key, delta);
  }
  getrange(key: Key, start: number, end: number): R<string> {
    return this._3(null, C.GETRANGE, key, start, end);
  }
  substr(key: Key, start: number, end: number): R<string> {
    return this._3(null, C.SUBSTR, key, start, end);
  }
  setrange(key: Key, offset: number, value: Value): R<number> {
    return this._3(null, C.SETRANGE, key, offset, value);
  }
  /**
   * `FILLGET key fillMs [WAIT waitMs]`. `WAIT` parks the whole shared connection: leave it unset on
   * a shared client (see {@link RuvioClient.getOrCreate}).
   */
  fillget(key: Key, fillMs: number, waitMs?: number): R<FillResult> {
    return waitMs === undefined
      ? this._2(T.fill, C.FILLGET, key, fillMs)
      : this._4(T.fill, C.FILLGET, key, fillMs, W.WAIT, waitMs);
  }
  /** `FILLSET key value ttlMs token [SWR staleMs]`: true when the token was live and the value was written. */
  fillset(key: Key, value: Value, ttlMs: number, token: Value, staleMs?: number): R<boolean> {
    if (staleMs === undefined) return this._4(T.bool, C.FILLSET, key, value, ttlMs, token);
    return this._v(T.bool, C.FILLSET, [key, value, ttlMs, token, W.SWR, staleMs]);
  }

  // ---- Ruvio coordination primitives -----------------------------------------------------

  /** `LIMIT key max windowMs`: calls left in the window after this one, or `null` when full. */
  limit(key: Key, max: number, windowMs: number): R<number | null> {
    return this._3(null, C.LIMIT, key, max, windowMs);
  }
  /** `IDEM key BEGIN fingerprint owner ttlMs`. */
  idemBegin(key: Key, fingerprint: Value, owner: Value, ttlMs: number): R<IdemBeginResult> {
    return this._v(T.idem, C.IDEM, [key, W.BEGIN, fingerprint, owner, ttlMs]);
  }
  /** `IDEM key COMPLETE fingerprint owner result`: 1 when published (or identical), 0 otherwise. */
  idemComplete(key: Key, fingerprint: Value, owner: Value, result: Value): R<number> {
    return this._v(null, C.IDEM, [key, W.COMPLETE, fingerprint, owner, result]);
  }
  /** `ONCE key ttlMs`: true for the first call within the TTL. */
  once(key: Key, ttlMs: number): R<boolean> {
    return this._2(T.bool, C.ONCE, key, ttlMs);
  }
  /** `LEASE key ttlMs`: a new fencing token, or `null` while held. */
  lease(key: Key, ttlMs: number): R<number | null> {
    return this._2(T.intOrNull, C.LEASE, key, ttlMs);
  }
  /** `SEMAPHORE key max ttlMs`: a holder token (as a decimal string; it can exceed 2^53), or `null` when full. */
  semaphore(key: Key, max: number, ttlMs: number): R<string | null> {
    return this._3(T.token, C.SEMAPHORE, key, max, ttlMs);
  }
  /** `RELEASE key token` for a lease or a semaphore holder. */
  release(key: Key, token: Value | bigint): R<boolean> {
    return this._2(T.bool, C.RELEASE, key, token);
  }
  /** `CHANGES START`: arms the change feed and returns the cursor. */
  changesStart(): R<number> {
    return this._1(null, C.CHANGES, W.START);
  }
  /** `CHANGES cursor`: notes after `cursor` (at most 1024). */
  changes(cursor: number): R<ChangeNote[]> {
    return this._1(T.changes, C.CHANGES, cursor);
  }

  // ---- keys ------------------------------------------------------------------------------

  del(...keys: Key[]): R<number> {
    return keys.length === 1 ? this._1(null, C.DEL, keys[0]!) : this._v(null, C.DEL, keys);
  }
  unlink(...keys: Key[]): R<number> {
    return this._v(null, C.UNLINK, keys);
  }
  exists(...keys: Key[]): R<number> {
    return keys.length === 1 ? this._1(null, C.EXISTS, keys[0]!) : this._v(null, C.EXISTS, keys);
  }
  type(key: Key): R<string> {
    return this._1(null, C.TYPE, key);
  }
  rename(key: Key, newKey: Key): R<'OK'> {
    return this._2(null, C.RENAME, key, newKey);
  }
  renamenx(key: Key, newKey: Key): R<boolean> {
    return this._2(T.bool, C.RENAMENX, key, newKey);
  }
  /** One `SCAN` page. See {@link RuvioClient.scanIterator} for the lazy walk. */
  scan(cursor: string | number, opts?: ScanOptions): R<ScanPage<string>> {
    return this._v(T.scan, C.SCAN, pushScan([cursor], opts));
  }
  dbsize(): R<number> {
    return this._0(null, C.DBSIZE);
  }
  flushdb(async?: boolean): R<'OK'> {
    return async ? this._1(null, C.FLUSHDB, W.ASYNC) : this._0(null, C.FLUSHDB);
  }
  flushall(async?: boolean): R<'OK'> {
    return async ? this._1(null, C.FLUSHALL, W.ASYNC) : this._0(null, C.FLUSHALL);
  }
  swapdb(a: number, b: number): R<'OK'> {
    return this._2(null, C.SWAPDB, a, b);
  }
  move(key: Key, db: number): R<boolean> {
    return this._2(T.bool, C.MOVE, key, db);
  }
  /** `EXPIRE key seconds` (Ruvio has no NX/XX/GT/LT here; zero or less is an error). */
  expire(key: Key, seconds: number): R<boolean> {
    return this._2(T.bool, C.EXPIRE, key, seconds);
  }
  pexpire(key: Key, ms: number): R<boolean> {
    return this._2(T.bool, C.PEXPIRE, key, ms);
  }
  /** A past deadline deletes the key and still answers true. */
  expireat(key: Key, unixSeconds: number): R<boolean> {
    return this._2(T.bool, C.EXPIREAT, key, unixSeconds);
  }
  pexpireat(key: Key, unixMs: number): R<boolean> {
    return this._2(T.bool, C.PEXPIREAT, key, unixMs);
  }
  ttl(key: Key): R<number> {
    return this._1(null, C.TTL, key);
  }
  pttl(key: Key): R<number> {
    return this._1(null, C.PTTL, key);
  }
  persist(key: Key): R<boolean> {
    return this._1(T.bool, C.PERSIST, key);
  }
  keys(pattern: string): R<string[]> {
    return this._1(null, C.KEYS, pattern);
  }
  randomkey(): R<string | null> {
    return this._0(null, C.RANDOMKEY);
  }
  touch(...keys: Key[]): R<number> {
    return this._v(null, C.TOUCH, keys);
  }
  expiretime(key: Key): R<number> {
    return this._1(null, C.EXPIRETIME, key);
  }
  pexpiretime(key: Key): R<number> {
    return this._1(null, C.PEXPIRETIME, key);
  }
  copy(source: Key, destination: Key, opts?: { replace?: boolean }): R<boolean> {
    return opts?.replace
      ? this._3(T.bool, C.COPY, source, destination, W.REPLACE)
      : this._2(T.bool, C.COPY, source, destination);
  }
  objectEncoding(key: Key): R<string | null> {
    return this._1(null, C.OBJECT_ENCODING, key);
  }
  objectIdletime(key: Key): R<number | null> {
    return this._1(null, C.OBJECT_IDLETIME, key);
  }
  objectFreq(key: Key): R<number | null> {
    return this._1(null, C.OBJECT_FREQ, key);
  }
  memoryUsage(key: Key, samples?: number): R<number | null> {
    return samples === undefined
      ? this._1(null, C.MEMORY_USAGE, key)
      : this._3(null, C.MEMORY_USAGE, key, W.SAMPLES, samples);
  }
  /** `DUMP key`: Ruvio's own payload (always a Buffer). */
  dump(key: Key): R<Buffer | null> {
    return this._bin(() => this._1(null, C.DUMP, key));
  }
  restore(key: Key, ttlMs: number, payload: Buffer, opts?: RestoreOptions): R<'OK'> {
    const a: Arg[] = [key, ttlMs, payload];
    if (opts !== undefined) {
      if (opts.replace) a.push(W.REPLACE);
      if (opts.absTtl) a.push(W.ABSTTL);
      if (opts.idleTime !== undefined) a.push(W.IDLETIME, opts.idleTime);
      if (opts.freq !== undefined) a.push(W.FREQ, opts.freq);
    }
    return this._v(null, C.RESTORE, a);
  }

  // ---- lists -----------------------------------------------------------------------------

  lpush(key: Key, ...values: Value[]): R<number> {
    return values.length === 1 ? this._2(null, C.LPUSH, key, values[0]!) : this._kv(null, C.LPUSH, key, values);
  }
  rpush(key: Key, ...values: Value[]): R<number> {
    return values.length === 1 ? this._2(null, C.RPUSH, key, values[0]!) : this._kv(null, C.RPUSH, key, values);
  }
  lpushx(key: Key, ...values: Value[]): R<number> {
    return this._kv(null, C.LPUSHX, key, values);
  }
  rpushx(key: Key, ...values: Value[]): R<number> {
    return this._kv(null, C.RPUSHX, key, values);
  }
  /** `LPUSHCAP key cap value...`: push left, trim the tail to `cap`; returns the new length. */
  lpushcap(key: Key, cap: number, ...values: Value[]): R<number> {
    return this._v(null, C.LPUSHCAP, pushAll([key, cap], values));
  }
  /** `RPUSHCAP key cap value...`: push right, trim the head to `cap`; returns the new length. */
  rpushcap(key: Key, cap: number, ...values: Value[]): R<number> {
    return this._v(null, C.RPUSHCAP, pushAll([key, cap], values));
  }
  lpop(key: Key): R<string | null>;
  lpop(key: Key, count: number): R<string[] | null>;
  lpop(key: Key, count?: number): R<string | string[] | null>;
  lpop(key: Key, count?: number): R<any> {
    return count === undefined ? this._1(null, C.LPOP, key) : this._2(null, C.LPOP, key, count);
  }
  rpop(key: Key): R<string | null>;
  rpop(key: Key, count: number): R<string[] | null>;
  rpop(key: Key, count?: number): R<string | string[] | null>;
  rpop(key: Key, count?: number): R<any> {
    return count === undefined ? this._1(null, C.RPOP, key) : this._2(null, C.RPOP, key, count);
  }
  llen(key: Key): R<number> {
    return this._1(null, C.LLEN, key);
  }
  lindex(key: Key, index: number): R<string | null> {
    return this._2(null, C.LINDEX, key, index);
  }
  lrange(key: Key, start: number, stop: number): R<string[]> {
    return this._3(null, C.LRANGE, key, start, stop);
  }
  ltrim(key: Key, start: number, stop: number): R<'OK'> {
    return this._3(null, C.LTRIM, key, start, stop);
  }
  /** `BLPOP key... timeoutSeconds`. Blocks the connection: use a dedicated client (`duplicate()`). */
  blpop(keys: Key | Key[], timeoutSeconds: number): R<[string, string] | null> {
    const a: Arg[] = Array.isArray(keys) ? [...keys] : [keys];
    a.push(timeoutSeconds);
    return this._v(T.kv, C.BLPOP, a);
  }
  /** `BRPOP key... timeoutSeconds`. Blocks the connection: use a dedicated client (`duplicate()`). */
  brpop(keys: Key | Key[], timeoutSeconds: number): R<[string, string] | null> {
    const a: Arg[] = Array.isArray(keys) ? [...keys] : [keys];
    a.push(timeoutSeconds);
    return this._v(T.kv, C.BRPOP, a);
  }
  linsert(key: Key, where: 'BEFORE' | 'AFTER', pivot: Value, value: Value): R<number> {
    return this._4(null, C.LINSERT, key, where, pivot, value);
  }
  lset(key: Key, index: number, value: Value): R<'OK'> {
    return this._3(null, C.LSET, key, index, value);
  }
  lrem(key: Key, count: number, value: Value): R<number> {
    return this._3(null, C.LREM, key, count, value);
  }
  lpos(key: Key, value: Value): R<number | null>;
  lpos(key: Key, value: Value, opts: LPosOptions & { count: number }): R<number[]>;
  lpos(key: Key, value: Value, opts?: LPosOptions): R<number | null | number[]>;
  lpos(key: Key, value: Value, opts: LPosOptions): R<number | null | number[]>;
  lpos(key: Key, value: Value, opts?: LPosOptions): R<any> {
    if (opts === undefined) return this._2(null, C.LPOS, key, value);
    const a: Arg[] = [key, value];
    if (opts.rank !== undefined) a.push(W.RANK, opts.rank);
    if (opts.count !== undefined) a.push(W.COUNT, opts.count);
    if (opts.maxlen !== undefined) a.push(W.MAXLEN, opts.maxlen);
    return this._v(null, C.LPOS, a);
  }
  lmove(source: Key, destination: Key, from: 'LEFT' | 'RIGHT', to: 'LEFT' | 'RIGHT'): R<string | null> {
    return this._4(null, C.LMOVE, source, destination, from, to);
  }
  rpoplpush(source: Key, destination: Key): R<string | null> {
    return this._2(null, C.RPOPLPUSH, source, destination);
  }
  /** Blocks the connection: use a dedicated client (`duplicate()`). */
  blmove(source: Key, destination: Key, from: 'LEFT' | 'RIGHT', to: 'LEFT' | 'RIGHT', timeoutSeconds: number): R<string | null> {
    return this._v(null, C.BLMOVE, [source, destination, from, to, timeoutSeconds]);
  }
  lmpop(keys: Key[], where: 'LEFT' | 'RIGHT', count?: number): R<ListPopResult | null> {
    const a: Arg[] = [keys.length, ...keys, where];
    if (count !== undefined) a.push(W.COUNT, count);
    return this._v(T.lmpop, C.LMPOP, a);
  }
  /** Blocks the connection: use a dedicated client (`duplicate()`). */
  blmpop(timeoutSeconds: number, keys: Key[], where: 'LEFT' | 'RIGHT', count?: number): R<ListPopResult | null> {
    const a: Arg[] = [timeoutSeconds, keys.length, ...keys, where];
    if (count !== undefined) a.push(W.COUNT, count);
    return this._v(T.lmpop, C.BLMPOP, a);
  }

  // ---- sets ------------------------------------------------------------------------------

  sadd(key: Key, ...members: Value[]): R<number> {
    return members.length === 1 ? this._2(null, C.SADD, key, members[0]!) : this._kv(null, C.SADD, key, members);
  }
  srem(key: Key, ...members: Value[]): R<number> {
    return this._kv(null, C.SREM, key, members);
  }
  sismember(key: Key, member: Value): R<boolean> {
    return this._2(T.bool, C.SISMEMBER, key, member);
  }
  smismember(key: Key, ...members: Value[]): R<boolean[]> {
    return this._kv(T.boolArr, C.SMISMEMBER, key, members);
  }
  scard(key: Key): R<number> {
    return this._1(null, C.SCARD, key);
  }
  smembers(key: Key): R<string[]> {
    return this._1(null, C.SMEMBERS, key);
  }
  sinter(...keys: Key[]): R<string[]> {
    return this._v(null, C.SINTER, keys);
  }
  sunion(...keys: Key[]): R<string[]> {
    return this._v(null, C.SUNION, keys);
  }
  sdiff(...keys: Key[]): R<string[]> {
    return this._v(null, C.SDIFF, keys);
  }
  sinterstore(destination: Key, ...keys: Key[]): R<number> {
    return this._kv(null, C.SINTERSTORE, destination, keys);
  }
  sunionstore(destination: Key, ...keys: Key[]): R<number> {
    return this._kv(null, C.SUNIONSTORE, destination, keys);
  }
  sdiffstore(destination: Key, ...keys: Key[]): R<number> {
    return this._kv(null, C.SDIFFSTORE, destination, keys);
  }
  sintercard(keys: Key[], limit?: number): R<number> {
    const a: Arg[] = [keys.length, ...keys];
    if (limit !== undefined) a.push(W.LIMIT, limit);
    return this._v(null, C.SINTERCARD, a);
  }
  smove(source: Key, destination: Key, member: Value): R<boolean> {
    return this._3(T.bool, C.SMOVE, source, destination, member);
  }
  spop(key: Key): R<string | null>;
  spop(key: Key, count: number): R<string[]>;
  spop(key: Key, count?: number): R<string | string[] | null>;
  spop(key: Key, count?: number): R<any> {
    return count === undefined ? this._1(null, C.SPOP, key) : this._2(T.strs, C.SPOP, key, count);
  }
  srandmember(key: Key): R<string | null>;
  srandmember(key: Key, count: number): R<string[]>;
  srandmember(key: Key, count?: number): R<string | string[] | null>;
  srandmember(key: Key, count?: number): R<any> {
    return count === undefined ? this._1(null, C.SRANDMEMBER, key) : this._2(T.strs, C.SRANDMEMBER, key, count);
  }
  sscan(key: Key, cursor: string | number, opts?: ScanOptions): R<ScanPage<string>> {
    return this._v(T.scan, C.SSCAN, pushScan([key, cursor], opts));
  }

  // ---- hashes ----------------------------------------------------------------------------

  /** `HSET key field value` or `HSET key {field: value, ...}`; returns new-field count. */
  hset(key: Key, field: Key, value: Value): R<number>;
  hset(key: Key, fields: Record<string, Value> | ReadonlyArray<readonly [Key, Value]>): R<number>;
  hset(key: Key, fieldOrFields: Key | Record<string, Value> | ReadonlyArray<readonly [Key, Value]>, value?: Value): R<number>;
  hset(key: Key, f: any, value?: Value): R<number> {
    if (value !== undefined) return this._3(null, C.HSET, key, f, value);
    return this._v(null, C.HSET, flatPairs([key], f));
  }
  hget(key: Key, field: Key): R<string | null> {
    return this._2(null, C.HGET, key, field);
  }
  /** `HGET` returning raw bytes. */
  hgetBuffer(key: Key, field: Key): R<Buffer | null> {
    return this._bin(() => this._2(null, C.HGET, key, field));
  }
  hdel(key: Key, ...fields: Key[]): R<number> {
    return fields.length === 1 ? this._2(null, C.HDEL, key, fields[0]!) : this._kv(null, C.HDEL, key, fields);
  }
  hlen(key: Key): R<number> {
    return this._1(null, C.HLEN, key);
  }
  hgetall(key: Key): R<Record<string, string>> {
    return this._1(T.map, C.HGETALL, key);
  }
  hmget(key: Key, ...fields: Key[]): R<(string | null)[]> {
    return this._kv(null, C.HMGET, key, fields);
  }
  hexists(key: Key, field: Key): R<boolean> {
    return this._2(T.bool, C.HEXISTS, key, field);
  }
  hkeys(key: Key): R<string[]> {
    return this._1(null, C.HKEYS, key);
  }
  hvals(key: Key): R<string[]> {
    return this._1(null, C.HVALS, key);
  }
  hincrby(key: Key, field: Key, delta: number): R<number> {
    return this._3(null, C.HINCRBY, key, field, delta);
  }
  hincrbyfloat(key: Key, field: Key, delta: number): R<number> {
    return this._3(T.float, C.HINCRBYFLOAT, key, field, delta);
  }
  hsetnx(key: Key, field: Key, value: Value): R<boolean> {
    return this._3(T.bool, C.HSETNX, key, field, value);
  }
  hstrlen(key: Key, field: Key): R<number> {
    return this._2(null, C.HSTRLEN, key, field);
  }
  hscan(key: Key, cursor: string | number, opts?: ScanOptions): R<ScanPage<[string, string]>> {
    return this._v(T.hscan, C.HSCAN, pushScan([key, cursor], opts));
  }
  private _hfe(c: Cmd, key: Key, time: number | null, fields: readonly Key[], cond?: ExpireCondition): R<number[]> {
    const a: Arg[] = time === null ? [key] : [key, time];
    if (cond !== undefined) a.push(cond);
    a.push(W.FIELDS, fields.length);
    pushAll(a, fields);
    return this._v(null, c, a);
  }
  /** Per field: -2 missing, 0 condition not met, 1 set, 2 deleted. */
  hexpire(key: Key, seconds: number, fields: Key[], cond?: ExpireCondition): R<number[]> {
    return this._hfe(C.HEXPIRE, key, seconds, fields, cond);
  }
  hpexpire(key: Key, ms: number, fields: Key[], cond?: ExpireCondition): R<number[]> {
    return this._hfe(C.HPEXPIRE, key, ms, fields, cond);
  }
  hexpireat(key: Key, unixSeconds: number, fields: Key[], cond?: ExpireCondition): R<number[]> {
    return this._hfe(C.HEXPIREAT, key, unixSeconds, fields, cond);
  }
  hpexpireat(key: Key, unixMs: number, fields: Key[], cond?: ExpireCondition): R<number[]> {
    return this._hfe(C.HPEXPIREAT, key, unixMs, fields, cond);
  }
  httl(key: Key, ...fields: Key[]): R<number[]> {
    return this._hfe(C.HTTL, key, null, fields);
  }
  hpttl(key: Key, ...fields: Key[]): R<number[]> {
    return this._hfe(C.HPTTL, key, null, fields);
  }
  hexpiretime(key: Key, ...fields: Key[]): R<number[]> {
    return this._hfe(C.HEXPIRETIME, key, null, fields);
  }
  hpexpiretime(key: Key, ...fields: Key[]): R<number[]> {
    return this._hfe(C.HPEXPIRETIME, key, null, fields);
  }
  hpersist(key: Key, ...fields: Key[]): R<number[]> {
    return this._hfe(C.HPERSIST, key, null, fields);
  }
  hrandfield(key: Key): R<string | null>;
  hrandfield(key: Key, count: number): R<string[]>;
  hrandfield(key: Key, count: number, withValues: true): R<[string, string][]>;
  hrandfield(key: Key, count?: number, withValues?: boolean): R<string | null | string[] | [string, string][]>;
  hrandfield(key: Key, count?: number, withValues?: boolean): R<any> {
    if (count === undefined) return this._1(null, C.HRANDFIELD, key);
    if (withValues) return this._3(T.pairs, C.HRANDFIELD, key, count, W.WITHVALUES);
    return this._2(null, C.HRANDFIELD, key, count);
  }

  // ---- sorted sets -----------------------------------------------------------------------

  /** `ZADD key [NX|XX] [GT|LT] [CH] score member [score member ...]`. */
  zadd(key: Key, score: number, member: Value, opts?: ZAddOptions): R<number>;
  zadd(key: Key, members: ReadonlyArray<SortedSetEntry | readonly [number, Value]>, opts?: ZAddOptions): R<number>;
  zadd(key: Key, scoreOrMembers: number | ReadonlyArray<SortedSetEntry | readonly [number, Value]>, memberOrOpts?: Value | ZAddOptions, opts?: ZAddOptions): R<number>;
  zadd(key: Key, a: any, b?: any, c?: any): R<number> {
    if (typeof a === 'number' && c === undefined) return this._3(null, C.ZADD, key, a, b);
    const opts: ZAddOptions | undefined = typeof a === 'number' ? c : b;
    const args: Arg[] = [key];
    pushZAddFlags(args, opts);
    if (typeof a === 'number') args.push(a, b);
    else for (const e of a) Array.isArray(e) ? args.push(e[0], e[1]) : args.push(e.score, e.member);
    return this._v(null, C.ZADD, args);
  }
  /** `ZADD key ... INCR score member`: the new score, or `null` when a condition blocked it. */
  zaddIncr(key: Key, increment: number, member: Value, opts?: ZAddOptions): R<number | null> {
    const args: Arg[] = [key];
    pushZAddFlags(args, opts);
    args.push(W.INCR, increment, member);
    return this._v(T.float, C.ZADD, args);
  }
  /** `ZRANGE key start stop [BYSCORE|BYLEX] [REV] [LIMIT offset count]`. */
  zrange(key: Key, start: Value, stop: Value, opts?: ZRangeOptions): R<string[]> {
    return opts === undefined ? this._3(null, C.ZRANGE, key, start, stop) : this._v(null, C.ZRANGE, zrangeArgs(key, start, stop, opts, false));
  }
  /** `ZRANGE ... WITHSCORES`. */
  zrangeWithScores(key: Key, start: Value, stop: Value, opts?: ZRangeOptions): R<SortedSetEntry[]> {
    return this._v(T.zentries, C.ZRANGE, zrangeArgs(key, start, stop, opts, true));
  }
  zrevrange(key: Key, start: number, stop: number): R<string[]> {
    return this._3(null, C.ZREVRANGE, key, start, stop);
  }
  zrevrangeWithScores(key: Key, start: number, stop: number): R<SortedSetEntry[]> {
    return this._4(T.zentries, C.ZREVRANGE, key, start, stop, W.WITHSCORES);
  }
  zrangebyscore(key: Key, min: Value, max: Value, limit?: { offset: number; count: number }): R<string[]> {
    const a: Arg[] = [key, min, max];
    if (limit !== undefined) a.push(W.LIMIT, limit.offset, limit.count);
    return this._v(null, C.ZRANGEBYSCORE, a);
  }
  zrangebyscoreWithScores(key: Key, min: Value, max: Value, limit?: { offset: number; count: number }): R<SortedSetEntry[]> {
    const a: Arg[] = [key, min, max, W.WITHSCORES];
    if (limit !== undefined) a.push(W.LIMIT, limit.offset, limit.count);
    return this._v(T.zentries, C.ZRANGEBYSCORE, a);
  }
  zrangebylex(key: Key, min: Value, max: Value, limit?: { offset: number; count: number }): R<string[]> {
    const a: Arg[] = [key, min, max];
    if (limit !== undefined) a.push(W.LIMIT, limit.offset, limit.count);
    return this._v(null, C.ZRANGEBYLEX, a);
  }
  zrevrangebylex(key: Key, max: Value, min: Value, limit?: { offset: number; count: number }): R<string[]> {
    const a: Arg[] = [key, max, min];
    if (limit !== undefined) a.push(W.LIMIT, limit.offset, limit.count);
    return this._v(null, C.ZREVRANGEBYLEX, a);
  }
  zrem(key: Key, ...members: Value[]): R<number> {
    return this._kv(null, C.ZREM, key, members);
  }
  zincrby(key: Key, increment: number, member: Value): R<number> {
    return this._3(T.float, C.ZINCRBY, key, increment, member);
  }
  zcard(key: Key): R<number> {
    return this._1(null, C.ZCARD, key);
  }
  zscore(key: Key, member: Value): R<number | null> {
    return this._2(T.float, C.ZSCORE, key, member);
  }
  zmscore(key: Key, ...members: Value[]): R<(number | null)[]> {
    return this._kv(T.floatArr, C.ZMSCORE, key, members);
  }
  zrank(key: Key, member: Value): R<number | null> {
    return this._2(null, C.ZRANK, key, member);
  }
  zrevrank(key: Key, member: Value): R<number | null> {
    return this._2(null, C.ZREVRANK, key, member);
  }
  zpopmin(key: Key, count?: number): R<SortedSetEntry[]> {
    return count === undefined ? this._1(T.zentries, C.ZPOPMIN, key) : this._2(T.zentries, C.ZPOPMIN, key, count);
  }
  zpopmax(key: Key, count?: number): R<SortedSetEntry[]> {
    return count === undefined ? this._1(T.zentries, C.ZPOPMAX, key) : this._2(T.zentries, C.ZPOPMAX, key, count);
  }
  /** Blocks the connection: use a dedicated client (`duplicate()`). */
  bzpopmin(keys: Key | Key[], timeoutSeconds: number): R<PoppedEntry | null> {
    const a: Arg[] = Array.isArray(keys) ? [...keys] : [keys];
    a.push(timeoutSeconds);
    return this._v(T.bzpop, C.BZPOPMIN, a);
  }
  /** Blocks the connection: use a dedicated client (`duplicate()`). */
  bzpopmax(keys: Key | Key[], timeoutSeconds: number): R<PoppedEntry | null> {
    const a: Arg[] = Array.isArray(keys) ? [...keys] : [keys];
    a.push(timeoutSeconds);
    return this._v(T.bzpop, C.BZPOPMAX, a);
  }
  zremrangebyrank(key: Key, start: number, stop: number): R<number> {
    return this._3(null, C.ZREMRANGEBYRANK, key, start, stop);
  }
  zremrangebyscore(key: Key, min: Value, max: Value): R<number> {
    return this._3(null, C.ZREMRANGEBYSCORE, key, min, max);
  }
  zremrangebylex(key: Key, min: Value, max: Value): R<number> {
    return this._3(null, C.ZREMRANGEBYLEX, key, min, max);
  }
  zcount(key: Key, min: Value, max: Value): R<number> {
    return this._3(null, C.ZCOUNT, key, min, max);
  }
  zlexcount(key: Key, min: Value, max: Value): R<number> {
    return this._3(null, C.ZLEXCOUNT, key, min, max);
  }
  zunionstore(destination: Key, keys: Key[], opts?: ZStoreOptions): R<number> {
    return this._v(null, C.ZUNIONSTORE, pushZStore([destination], keys, opts));
  }
  zinterstore(destination: Key, keys: Key[], opts?: ZStoreOptions): R<number> {
    return this._v(null, C.ZINTERSTORE, pushZStore([destination], keys, opts));
  }
  zdiffstore(destination: Key, keys: Key[]): R<number> {
    return this._v(null, C.ZDIFFSTORE, pushZStore([destination], keys));
  }
  zunion(keys: Key[], opts?: ZStoreOptions): R<string[]> {
    return this._v(null, C.ZUNION, pushZStore([], keys, opts));
  }
  zunionWithScores(keys: Key[], opts?: ZStoreOptions): R<SortedSetEntry[]> {
    const a = pushZStore([], keys, opts);
    a.push(W.WITHSCORES);
    return this._v(T.zentries, C.ZUNION, a);
  }
  zinter(keys: Key[], opts?: ZStoreOptions): R<string[]> {
    return this._v(null, C.ZINTER, pushZStore([], keys, opts));
  }
  zinterWithScores(keys: Key[], opts?: ZStoreOptions): R<SortedSetEntry[]> {
    const a = pushZStore([], keys, opts);
    a.push(W.WITHSCORES);
    return this._v(T.zentries, C.ZINTER, a);
  }
  zdiff(keys: Key[]): R<string[]> {
    return this._v(null, C.ZDIFF, pushZStore([], keys));
  }
  zdiffWithScores(keys: Key[]): R<SortedSetEntry[]> {
    const a = pushZStore([], keys);
    a.push(W.WITHSCORES);
    return this._v(T.zentries, C.ZDIFF, a);
  }
  zrandmember(key: Key): R<string | null>;
  zrandmember(key: Key, count: number): R<string[]>;
  zrandmember(key: Key, count: number, withScores: true): R<SortedSetEntry[]>;
  zrandmember(key: Key, count?: number, withScores?: boolean): R<string | null | string[] | SortedSetEntry[]>;
  zrandmember(key: Key, count?: number, withScores?: boolean): R<any> {
    if (count === undefined) return this._1(null, C.ZRANDMEMBER, key);
    if (withScores) return this._3(T.zentries, C.ZRANDMEMBER, key, count, W.WITHSCORES);
    return this._2(null, C.ZRANDMEMBER, key, count);
  }
  zscan(key: Key, cursor: string | number, opts?: ScanOptions): R<ScanPage<SortedSetEntry>> {
    return this._v(T.zscan, C.ZSCAN, pushScan([key, cursor], opts));
  }

  // ---- geo -------------------------------------------------------------------------------

  geoadd(key: Key, members: GeoMember[], opts?: { nx?: boolean; xx?: boolean; ch?: boolean }): R<number> {
    const a: Arg[] = [key];
    if (opts?.nx) a.push(W.NX);
    else if (opts?.xx) a.push(W.XX);
    if (opts?.ch) a.push(W.CH);
    for (const m of members) a.push(m.longitude, m.latitude, m.member);
    return this._v(null, C.GEOADD, a);
  }
  geodist(key: Key, member1: Value, member2: Value, unit?: GeoUnit): R<number | null> {
    return unit === undefined
      ? this._3(T.float, C.GEODIST, key, member1, member2)
      : this._4(T.float, C.GEODIST, key, member1, member2, unit);
  }
  geohash(key: Key, ...members: Value[]): R<(string | null)[]> {
    return this._kv(null, C.GEOHASH, key, members);
  }
  geopos(key: Key, ...members: Value[]): R<([number, number] | null)[]> {
    return this._kv(T.geopos, C.GEOPOS, key, members);
  }
  /** `GEOSEARCH`; with any `with*` option each match is a {@link GeoMatch}, else a member name. */
  geosearch(key: Key, opts: GeoSearchOptions): R<(string | GeoMatch)[]> {
    const a: Arg[] = [key];
    pushGeoShape(a, opts);
    if (opts.withCoord) a.push(W.WITHCOORD);
    if (opts.withDist) a.push(W.WITHDIST);
    if (opts.withHash) a.push(W.WITHHASH);
    return this._v(geoTf(opts), C.GEOSEARCH, a);
  }
  geosearchstore(destination: Key, source: Key, opts: GeoSearchOptions & { storeDist?: boolean }): R<number> {
    const a: Arg[] = [destination, source];
    pushGeoShape(a, opts);
    if (opts.storeDist) a.push(W.STOREDIST);
    return this._v(null, C.GEOSEARCHSTORE, a);
  }
  georadius(key: Key, longitude: number, latitude: number, radius: number, unit: GeoUnit, opts?: GeoRadiusOptions): R<(string | GeoMatch)[] | number> {
    const a: Arg[] = [key, longitude, latitude, radius, unit];
    pushRadius(a, opts);
    return this._v(opts ? geoTf(opts) : null, C.GEORADIUS, a);
  }
  georadiusbymember(key: Key, member: Value, radius: number, unit: GeoUnit, opts?: GeoRadiusOptions): R<(string | GeoMatch)[] | number> {
    const a: Arg[] = [key, member, radius, unit];
    pushRadius(a, opts);
    return this._v(opts ? geoTf(opts) : null, C.GEORADIUSBYMEMBER, a);
  }

  // ---- bloom -----------------------------------------------------------------------------

  bfReserve(key: Key, errorRate: number, capacity: number, opts?: BfReserveOptions): R<'OK'> {
    const a: Arg[] = [key, errorRate, capacity];
    if (opts?.expansion !== undefined) a.push(W.EXPANSION, opts.expansion);
    if (opts?.nonScaling) a.push(W.NONSCALING);
    return this._v(null, C.BF_RESERVE, a);
  }
  /** True when the item was new. */
  bfAdd(key: Key, item: Value): R<boolean> {
    return this._2(T.bool, C.BF_ADD, key, item);
  }
  /** Per item: true when added (new), false when probably present. */
  bfMadd(key: Key, ...items: Value[]): R<boolean[]> {
    return this._kv(T.boolArr, C.BF_MADD, key, items);
  }
  bfInsert(key: Key, items: Value[], opts?: BfInsertOptions): R<boolean[]> {
    const a: Arg[] = [key];
    if (opts !== undefined) {
      if (opts.capacity !== undefined) a.push(W.CAPACITY, opts.capacity);
      if (opts.error !== undefined) a.push(W.ERROR, opts.error);
      if (opts.expansion !== undefined) a.push(W.EXPANSION, opts.expansion);
      if (opts.noCreate) a.push(W.NOCREATE);
      if (opts.nonScaling) a.push(W.NONSCALING);
    }
    a.push(W.ITEMS);
    pushAll(a, items);
    return this._v(T.boolArr, C.BF_INSERT, a);
  }
  /** `BF.INFO key` as an object, or one field's value. */
  bfInfo(key: Key): R<Record<string, number | null>>;
  bfInfo(key: Key, field: string): R<unknown>;
  bfInfo(key: Key, field?: string): R<unknown>;
  bfInfo(key: Key, field?: string): R<any> {
    return field === undefined ? this._1(T.map, C.BF_INFO, key) : this._2(null, C.BF_INFO, key, field);
  }
  bfCard(key: Key): R<number> {
    return this._1(null, C.BF_CARD, key);
  }
  bfExists(key: Key, item: Value): R<boolean> {
    return this._2(T.bool, C.BF_EXISTS, key, item);
  }
  bfMexists(key: Key, ...items: Value[]): R<boolean[]> {
    return this._kv(T.boolArr, C.BF_MEXISTS, key, items);
  }

  // ---- json ------------------------------------------------------------------------------

  /** `JSON.GET key [path]`: the JSON text, or `null`. See {@link jsonGetAs} for parsed values. */
  jsonGet(key: Key, path?: string): R<string | null> {
    return path === undefined ? this._1(null, C.JSON_GET, key) : this._2(null, C.JSON_GET, key, path);
  }
  /** `JSON.GET` parsed with `JSON.parse`. */
  jsonGetAs<T = unknown>(key: Key, path?: string): R<T | null> {
    const tf = (v: string | null) => (v === null ? null : (JSON.parse(v) as T));
    return path === undefined ? this._1(tf, C.JSON_GET, key) : this._2(tf, C.JSON_GET, key, path);
  }
  /** `JSON.SET key path json [NX|XX]`: false when NX/XX blocked the write. */
  jsonSet(key: Key, path: string, json: Value, cond?: 'NX' | 'XX'): R<boolean> {
    return cond === undefined
      ? this._3(T.okBool, C.JSON_SET, key, path, json)
      : this._4(T.okBool, C.JSON_SET, key, path, json, cond === 'NX' ? W.NX : W.XX);
  }
  /** `JSON.SET` with `JSON.stringify(value)`. */
  jsonSetAs<T>(key: Key, path: string, value: T, cond?: 'NX' | 'XX'): R<boolean> {
    return this.jsonSet(key, path, JSON.stringify(value), cond);
  }
  jsonDel(key: Key, path?: string): R<number> {
    return path === undefined ? this._1(null, C.JSON_DEL, key) : this._2(null, C.JSON_DEL, key, path);
  }
  jsonForget(key: Key, path?: string): R<number> {
    return path === undefined ? this._1(null, C.JSON_FORGET, key) : this._2(null, C.JSON_FORGET, key, path);
  }
  jsonMget(keys: Key[], path: string): R<(string | null)[]> {
    const a: Arg[] = [...keys, path];
    return this._v(null, C.JSON_MGET, a);
  }
  /** `JSON.MERGE key path patch` (RFC 7396). */
  jsonMerge(key: Key, path: string, patch: Value): R<'OK'> {
    return this._3(null, C.JSON_MERGE, key, path, patch);
  }
  /** `JMERGE` alias of JSON.MERGE. */
  jmerge(key: Key, path: string, patch: Value): R<'OK'> {
    return this._3(null, C.JMERGE, key, path, patch);
  }
  jsonType(key: Key, path?: string): R<string | null> {
    return path === undefined ? this._1(null, C.JSON_TYPE, key) : this._2(null, C.JSON_TYPE, key, path);
  }
  jsonStrlen(key: Key, path?: string): R<number | null> {
    return path === undefined ? this._1(null, C.JSON_STRLEN, key) : this._2(null, C.JSON_STRLEN, key, path);
  }
  jsonArrlen(key: Key, path?: string): R<number | null> {
    return path === undefined ? this._1(null, C.JSON_ARRLEN, key) : this._2(null, C.JSON_ARRLEN, key, path);
  }
  jsonObjkeys(key: Key, path?: string): R<string[] | null> {
    return path === undefined ? this._1(null, C.JSON_OBJKEYS, key) : this._2(null, C.JSON_OBJKEYS, key, path);
  }
  jsonObjlen(key: Key, path?: string): R<number | null> {
    return path === undefined ? this._1(null, C.JSON_OBJLEN, key) : this._2(null, C.JSON_OBJLEN, key, path);
  }
  jsonNumincrby(key: Key, path: string, delta: number): R<number | null> {
    return this._3(T.jsonNum, C.JSON_NUMINCRBY, key, path, delta);
  }
  jsonNummultby(key: Key, path: string, factor: number): R<number | null> {
    return this._3(T.jsonNum, C.JSON_NUMMULTBY, key, path, factor);
  }
  jsonToggle(key: Key, path: string): R<boolean | null> {
    return this._2(T.jsonBool, C.JSON_TOGGLE, key, path);
  }
  /** `JSON.ARRAPPEND key path json...` (each value is JSON text). */
  jsonArrappend(key: Key, path: string, ...json: Value[]): R<number | null> {
    return this._v(null, C.JSON_ARRAPPEND, pushAll([key, path], json));
  }
  jsonArrinsert(key: Key, path: string, index: number, ...json: Value[]): R<number | null> {
    return this._v(null, C.JSON_ARRINSERT, pushAll([key, path, index], json));
  }
  jsonArrpop(key: Key, path?: string, index?: number): R<string | null> {
    if (path === undefined) return this._1(null, C.JSON_ARRPOP, key);
    return index === undefined ? this._2(null, C.JSON_ARRPOP, key, path) : this._3(null, C.JSON_ARRPOP, key, path, index);
  }
  jsonArrtrim(key: Key, path: string, start: number, stop: number): R<number | null> {
    return this._4(null, C.JSON_ARRTRIM, key, path, start, stop);
  }
  /** `JSON.ARRINDEX key path scalarJson [start [stop]]`; -1 when absent. */
  jsonArrindex(key: Key, path: string, scalarJson: Value, start?: number, stop?: number): R<number | null> {
    const a: Arg[] = [key, path, scalarJson];
    if (start !== undefined) {
      a.push(start);
      if (stop !== undefined) a.push(stop);
    }
    return this._v(null, C.JSON_ARRINDEX, a);
  }
  jsonClear(key: Key, path?: string): R<number> {
    return path === undefined ? this._1(null, C.JSON_CLEAR, key) : this._2(null, C.JSON_CLEAR, key, path);
  }
  /** `JSON.STRAPPEND key path jsonString` (the value is JSON text, e.g. `'"abc"'`). */
  jsonStrappend(key: Key, path: string, jsonString: Value): R<number | null> {
    return this._3(null, C.JSON_STRAPPEND, key, path, jsonString);
  }
  /** `JSON.INDEX CREATE name PREFIX prefix PATH path [TYPE string|number|bool]` (database 0 only). */
  jsonIndexCreate(name: string, prefix: string, path: string, type?: 'string' | 'number' | 'bool'): R<'OK'> {
    const a: Arg[] = [name, W.PREFIX, prefix, W.PATH, path];
    if (type !== undefined) a.push(W.TYPE, type);
    return this._v(null, C.JSON_INDEX_CREATE, a);
  }
  jsonIndexDrop(name: string): R<boolean> {
    return this._1(T.bool, C.JSON_INDEX_DROP, name);
  }
  jsonIndexList(): R<string[]> {
    return this._0(null, C.JSON_INDEX_LIST);
  }
  /** `JSON.FIND name VALUE v [COUNT n] [AFTER key]`: matching keys, ascending. */
  jsonFind(name: string, value: Value, opts?: { count?: number; after?: Key }): R<string[]> {
    const a: Arg[] = [name, W.VALUE, value];
    if (opts?.count !== undefined) a.push(W.COUNT, opts.count);
    if (opts?.after !== undefined) a.push(W.AFTER, opts.after);
    return this._v(null, C.JSON_FIND, a);
  }

  // ---- streams ---------------------------------------------------------------------------

  /** `XADD key [NOMKSTREAM] [MAXLEN|MINID [~] n] [DELAY ms] id field value ...`; `null` with NOMKSTREAM on a missing key. */
  xadd(key: Key, id: string, fields: Record<string, Value> | Value[], opts?: XAddOptions): R<string | null> {
    const a: Arg[] = [key];
    if (opts !== undefined) {
      if (opts.noMkStream) a.push(W.NOMKSTREAM);
      pushStreamId(a, opts);
      if (opts.delayMs !== undefined) a.push(W.DELAY, opts.delayMs);
    }
    a.push(id);
    return this._v(null, C.XADD, pushFields(a, fields));
  }
  xlen(key: Key): R<number> {
    return this._1(null, C.XLEN, key);
  }
  xrange(key: Key, start = '-', end = '+', count?: number): R<StreamEntry[]> {
    return count === undefined
      ? this._3(T.stream, C.XRANGE, key, start, end)
      : this._v(T.stream, C.XRANGE, [key, start, end, W.COUNT, count]);
  }
  xrevrange(key: Key, end = '+', start = '-', count?: number): R<StreamEntry[]> {
    return count === undefined
      ? this._3(T.stream, C.XREVRANGE, key, end, start)
      : this._v(T.stream, C.XREVRANGE, [key, end, start, W.COUNT, count]);
  }
  /** `XREAD [COUNT n] [BLOCK ms] STREAMS key... id...`. `block` parks the connection: use `duplicate()`. */
  xread(streams: Record<string, string> | ReadonlyArray<readonly [Key, string]>, opts?: XReadOptions): R<StreamReadResult[] | null> {
    const a: Arg[] = [];
    if (opts?.count !== undefined) a.push(W.COUNT, opts.count);
    if (opts?.block !== undefined) a.push(W.BLOCK, opts.block);
    a.push(W.STREAMS);
    const pairs: (readonly [Key, string])[] = Array.isArray(streams) ? (streams as (readonly [Key, string])[]) : Object.entries(streams);
    for (const p of pairs) a.push(p[0]);
    for (const p of pairs) a.push(p[1]);
    return this._v(T.xread, C.XREAD, a);
  }
  xdel(key: Key, ...ids: string[]): R<number> {
    return this._kv(null, C.XDEL, key, ids);
  }
  xsetid(key: Key, id: string): R<'OK'> {
    return this._2(null, C.XSETID, key, id);
  }
  xtrim(key: Key, opts: XTrimOptions): R<number> {
    const a: Arg[] = [key];
    pushStreamId(a, opts);
    return this._v(null, C.XTRIM, a);
  }
  xgroupCreate(key: Key, group: string, id: string, opts?: { mkStream?: boolean }): R<'OK'> {
    return opts?.mkStream
      ? this._4(null, C.XGROUP_CREATE, key, group, id, W.MKSTREAM)
      : this._3(null, C.XGROUP_CREATE, key, group, id);
  }
  xgroupCreateconsumer(key: Key, group: string, consumer: string): R<boolean> {
    return this._3(T.bool, C.XGROUP_CREATECONSUMER, key, group, consumer);
  }
  xgroupDestroy(key: Key, group: string): R<boolean> {
    return this._2(T.bool, C.XGROUP_DESTROY, key, group);
  }
  xgroupSetid(key: Key, group: string, id: string): R<'OK'> {
    return this._3(null, C.XGROUP_SETID, key, group, id);
  }
  xgroupDelconsumer(key: Key, group: string, consumer: string): R<number> {
    return this._3(null, C.XGROUP_DELCONSUMER, key, group, consumer);
  }
  /** `XREADGROUP GROUP g c [COUNT n] [BLOCK ms] [NOACK] STREAMS key >` (Ruvio: one stream, `>` only). */
  xreadgroup(group: string, consumer: string, key: Key, opts?: XReadGroupOptions): R<StreamReadResult[] | null> {
    const a: Arg[] = [W.GROUP, group, consumer];
    if (opts?.count !== undefined) a.push(W.COUNT, opts.count);
    if (opts?.block !== undefined) a.push(W.BLOCK, opts.block);
    if (opts?.noAck) a.push(W.NOACK);
    a.push(W.STREAMS, key, W.GTID);
    return this._v(T.xread, C.XREADGROUP, a);
  }
  xack(key: Key, group: string, ...ids: string[]): R<number> {
    return this._v(null, C.XACK, pushAll([key, group], ids));
  }
  /** `XPENDING key group` summary. */
  xpending(key: Key, group: string): R<PendingSummary> {
    return this._2(T.pendingSummary, C.XPENDING, key, group);
  }
  /** `XPENDING key group [IDLE ms] start end count [consumer]`. */
  xpendingRange(key: Key, group: string, start: string, end: string, count: number, opts?: { idle?: number; consumer?: string }): R<PendingRow[]> {
    const a: Arg[] = [key, group];
    if (opts?.idle !== undefined) a.push(W.IDLE, opts.idle);
    a.push(start, end, count);
    if (opts?.consumer !== undefined) a.push(opts.consumer);
    return this._v(T.pendingRows, C.XPENDING, a);
  }
  xautoclaim(key: Key, group: string, consumer: string, minIdleMs: number, start: string, opts?: { count?: number; justId?: boolean }): R<AutoClaimResult> {
    const a: Arg[] = [key, group, consumer, minIdleMs, start];
    if (opts?.count !== undefined) a.push(W.COUNT, opts.count);
    if (opts?.justId) a.push(W.JUSTID);
    return this._v(T.autoclaim, C.XAUTOCLAIM, a);
  }
  /** `XCLAIM`; entries, or ids with `justId`. */
  xclaim(key: Key, group: string, consumer: string, minIdleMs: number, ids: string[], opts?: XClaimOptions): R<StreamEntry[] | string[]> {
    const a: Arg[] = [key, group, consumer, minIdleMs];
    pushAll(a, ids);
    if (opts !== undefined) {
      if (opts.idle !== undefined) a.push(W.IDLE, opts.idle);
      if (opts.time !== undefined) a.push(W.TIME, opts.time);
      if (opts.retryCount !== undefined) a.push(W.RETRYCOUNT, opts.retryCount);
      if (opts.force) a.push(W.FORCE);
      if (opts.justId) a.push(W.JUSTID);
      if (opts.lastId !== undefined) a.push(W.LASTID, opts.lastId);
    }
    return this._v(T.streamOrIds, C.XCLAIM, a);
  }
  xinfoStream(key: Key): R<Record<string, unknown>> {
    return this._1(T.map, C.XINFO_STREAM, key);
  }
  xinfoGroups(key: Key): R<Record<string, unknown>[]> {
    return this._1(T.mapArr, C.XINFO_GROUPS, key);
  }
  xinfoConsumers(key: Key, group: string): R<Record<string, unknown>[]> {
    return this._2(T.mapArr, C.XINFO_CONSUMERS, key, group);
  }

  // ---- server / introspection ------------------------------------------------------------

  info(section?: string): R<string> {
    return section === undefined ? this._0(T.lines, C.INFO) : this._1(T.lines, C.INFO, section);
  }
  /** `CONFIG GET pattern` as an object (requirepass is redacted). */
  configGet(pattern: string): R<Record<string, string>> {
    return this._1(T.map, C.CONFIG_GET, pattern);
  }
  save(): R<'OK'> {
    return this._0(null, C.SAVE);
  }
  bgsave(): R<string> {
    return this._0(null, C.BGSAVE);
  }
  /** `WAIT numReplicas timeoutMs`. Blocks the connection while it waits. */
  wait(numReplicas: number, timeoutMs: number): R<number> {
    return this._2(null, C.WAIT, numReplicas, timeoutMs);
  }

  // ---- transactions (raw; prefer client.multi() and client.isolated()) -------------------

  /**
   * `WATCH key...`. Only meaningful on a connection you own: use
   * `client.isolated(c => ...)` or a `duplicate()`d client, not the shared one.
   */
  watch(...keys: Key[]): R<'OK'> {
    return this._v(null, C.WATCH, keys);
  }
  unwatch(): R<'OK'> {
    return this._0(null, C.UNWATCH);
  }

  // ---- scripts ---------------------------------------------------------------------------

  eval(script: string, keys: Key[] = [], args: Value[] = []): R<unknown> {
    return this._v(null, C.EVAL, pushAll(pushAll([script, keys.length], keys), args));
  }
  evalsha(sha1: string, keys: Key[] = [], args: Value[] = []): R<unknown> {
    return this._v(null, C.EVALSHA, pushAll(pushAll([sha1, keys.length], keys), args));
  }
  scriptLoad(script: string): R<string> {
    return this._1(null, C.SCRIPT_LOAD, script);
  }
  scriptExists(...sha1s: string[]): R<boolean[]> {
    return this._v(T.boolArr, C.SCRIPT_EXISTS, sha1s);
  }
  scriptFlush(): R<'OK'> {
    return this._0(null, C.SCRIPT_FLUSH);
  }
  scriptKill(): R<'OK'> {
    return this._0(null, C.SCRIPT_KILL);
  }
  fcall(name: string, keys: Key[] = [], args: Value[] = []): R<unknown> {
    return this._v(null, C.FCALL, pushAll(pushAll([name, keys.length], keys), args));
  }
  /** `FUNCTION LOAD name body` (Ruvio form: one named Lua body, memory only). */
  functionLoad(name: string, body: string): R<unknown> {
    return this._2(null, C.FUNCTION_LOAD, name, body);
  }
  functionList(): R<unknown[]> {
    return this._0(null, C.FUNCTION_LIST);
  }
  functionDelete(name: string): R<boolean> {
    return this._1(T.bool, C.FUNCTION_DELETE, name);
  }

  // ---- cluster ---------------------------------------------------------------------------

  clusterSlots(): R<unknown[]> {
    return this._0(null, C.CLUSTER_SLOTS);
  }
  clusterNodes(): R<string> {
    return this._0(T.lines, C.CLUSTER_NODES);
  }
  clusterShards(): R<unknown[]> {
    return this._0(null, C.CLUSTER_SHARDS);
  }
  clusterInfo(): R<string> {
    return this._0(T.lines, C.CLUSTER_INFO);
  }
  clusterMyid(): R<string> {
    return this._0(null, C.CLUSTER_MYID);
  }
  clusterKeyslot(key: Key): R<number> {
    return this._1(null, C.CLUSTER_KEYSLOT, key);
  }
  readonly(): R<'OK'> {
    return this._0(null, C.READONLY);
  }
  readwrite(): R<'OK'> {
    return this._0(null, C.READWRITE);
  }

  // ---- pub/sub (publishing; subscribing lives on Subscriber) ----------------------------

  publish(channel: Key, message: Value): R<number> {
    return this._2(null, C.PUBLISH, channel, message);
  }
  spublish(channel: Key, message: Value): R<number> {
    return this._2(null, C.SPUBLISH, channel, message);
  }

  // ---- bitmaps ---------------------------------------------------------------------------

  setbit(key: Key, offset: number, bit: 0 | 1): R<number> {
    return this._3(null, C.SETBIT, key, offset, bit);
  }
  getbit(key: Key, offset: number): R<number> {
    return this._2(null, C.GETBIT, key, offset);
  }
  bitcount(key: Key, start?: number, end?: number, unit?: BitUnit): R<number> {
    if (start === undefined || end === undefined) return this._1(null, C.BITCOUNT, key);
    return unit === undefined ? this._3(null, C.BITCOUNT, key, start, end) : this._4(null, C.BITCOUNT, key, start, end, unit);
  }
  bitpos(key: Key, bit: 0 | 1, start?: number, end?: number, unit?: BitUnit): R<number> {
    const a: Arg[] = [key, bit];
    if (start !== undefined) {
      a.push(start);
      if (end !== undefined) {
        a.push(end);
        if (unit !== undefined) a.push(unit);
      }
    }
    return this._v(null, C.BITPOS, a);
  }
  bitop(op: 'AND' | 'OR' | 'XOR' | 'NOT', destination: Key, ...keys: Key[]): R<number> {
    return this._v(null, C.BITOP, pushAll([op, destination], keys));
  }
  /**
   * `BITFIELD key op...` with raw operation words, e.g.
   * `bitfield(k, 'SET', 'u8', 0, 200, 'GET', 'u8', 0, 'OVERFLOW', 'SAT', 'INCRBY', 'u8', 0, 100)`.
   */
  bitfield(key: Key, ...ops: Value[]): R<(number | null)[]> {
    return this._kv(null, C.BITFIELD, key, ops);
  }
  /** `BITFIELD_RO key GET type offset ...`. */
  bitfieldRo(key: Key, ...gets: [type: string, offset: number | string][]): R<number[]> {
    const a: Arg[] = [key];
    for (const g of gets) a.push(W.GET, g[0], g[1]);
    return this._v(null, C.BITFIELD_RO, a);
  }

  // ---- hyperloglog -----------------------------------------------------------------------

  pfadd(key: Key, ...elements: Value[]): R<boolean> {
    return this._kv(T.bool, C.PFADD, key, elements);
  }
  pfcount(...keys: Key[]): R<number> {
    return this._v(null, C.PFCOUNT, keys);
  }
  pfmerge(destination: Key, ...sources: Key[]): R<'OK'> {
    return this._kv(null, C.PFMERGE, destination, sources);
  }
}

function pushZAddFlags(a: Arg[], o?: ZAddOptions): void {
  if (o === undefined) return;
  if (o.nx) a.push(W.NX);
  else if (o.xx) a.push(W.XX);
  if (o.gt) a.push(W.GT);
  else if (o.lt) a.push(W.LT);
  if (o.ch) a.push(W.CH);
}

function zrangeArgs(key: Key, start: Value, stop: Value, o: ZRangeOptions | undefined, scores: boolean): Arg[] {
  const a: Arg[] = [key, start, stop];
  if (o !== undefined) {
    if (o.by === 'SCORE') a.push(W.BYSCORE);
    else if (o.by === 'LEX') a.push(W.BYLEX);
    if (o.rev) a.push(W.REV);
    if (o.limit !== undefined) a.push(W.LIMIT, o.limit.offset, o.limit.count);
  }
  if (scores) a.push(W.WITHSCORES);
  return a;
}

function pushRadius(a: Arg[], o?: GeoRadiusOptions): void {
  if (o === undefined) return;
  if (o.withCoord) a.push(W.WITHCOORD);
  if (o.withDist) a.push(W.WITHDIST);
  if (o.withHash) a.push(W.WITHHASH);
  if (o.count !== undefined) {
    a.push(W.COUNT, o.count);
    if (o.any) a.push(W.ANY);
  }
  if (o.order !== undefined) a.push(o.order);
  if (o.store !== undefined) a.push(W.STORE, o.store);
  if (o.storeDist !== undefined) a.push(W.STOREDIST, o.storeDist);
}
