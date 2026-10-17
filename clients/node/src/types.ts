// SPDX-License-Identifier: MIT
// Public option and reply types.

/** A key: UTF-8 string or raw bytes. */
export type Key = string | Buffer;
/** A value argument: UTF-8 string, raw bytes, or a number sent as ASCII. */
export type Value = string | Buffer | number;

export interface SetOptions {
  /** Seconds to live. */
  ex?: number;
  /** Milliseconds to live. */
  px?: number;
  /** Unix seconds deadline. */
  exat?: number;
  /** Unix milliseconds deadline. */
  pxat?: number;
  keepTtl?: boolean;
  /** Only when the key is missing. */
  nx?: boolean;
  /** Only when the key exists. */
  xx?: boolean;
  /** Return the previous value. */
  get?: boolean;
  /** Ruvio: write only when the current string equals this (not combinable with nx/xx/get). */
  ifEq?: Value;
}

/** Condition for `HEXPIRE` and friends. */
export type ExpireCondition = 'NX' | 'XX' | 'GT' | 'LT';
/**
 * Named {@link ExpireCondition} values for autocomplete, e.g. `c.hexpire('k', 60, ['f'], ExpireCondition.Nx)`.
 * Plain string literals (`'NX'`) work the same.
 */
export const ExpireCondition = { Nx: 'NX', Xx: 'XX', Gt: 'GT', Lt: 'LT' } as const;

/** Key type filter for `SCAN`: `string`, `json`, `list`, `set`, `hash`, `zset`, `bloomfilter`, `stream`, `lease`, `limit`, `semaphore` or `once`. */
export type ScanType =
  | 'string' | 'json' | 'list' | 'set' | 'hash' | 'zset' | 'bloomfilter' | 'stream' | 'lease' | 'limit' | 'semaphore' | 'once';
/**
 * Named {@link ScanType} values, e.g. `c.scan(0, { type: ScanType.Hash })`.
 */
export const ScanType = {
  String: 'string', Json: 'json', List: 'list', Set: 'set', Hash: 'hash', Zset: 'zset', Bloomfilter: 'bloomfilter',
  Stream: 'stream', Lease: 'lease', Limit: 'limit', Semaphore: 'semaphore', Once: 'once',
} as const;

export interface ScanOptions {
  match?: string;
  count?: number;
  /** Only keys of this type (`SCAN` only). Use {@link ScanType} for the known names. */
  type?: ScanType | (string & {});
}

export interface ScanPage<T> {
  /** Next cursor; `'0'` when the walk is done. */
  cursor: string;
  items: T[];
}

export interface SortedSetEntry {
  member: string;
  score: number;
}

export interface PoppedEntry extends SortedSetEntry {
  key: string;
}

export interface ZAddOptions {
  nx?: boolean;
  xx?: boolean;
  gt?: boolean;
  lt?: boolean;
  ch?: boolean;
}

export interface ZRangeOptions {
  by?: 'SCORE' | 'LEX';
  rev?: boolean;
  limit?: { offset: number; count: number };
}

export interface ZStoreOptions {
  weights?: number[];
  aggregate?: Aggregate;
}

/** How `ZUNION` / `ZINTER` combine scores of the same member: `SUM`, `MIN` or `MAX`. */
export type Aggregate = 'SUM' | 'MIN' | 'MAX';
/** Named {@link Aggregate} values, e.g. `{ aggregate: Aggregate.Max }`. */
export const Aggregate = { Sum: 'SUM', Min: 'MIN', Max: 'MAX' } as const;

/** Which end of a list: `LEFT` or `RIGHT` (`LMOVE`, `BLMOVE`, `LMPOP`, `BLMPOP`). */
export type ListSide = 'LEFT' | 'RIGHT';
/** Named {@link ListSide} values, e.g. `c.lmove('a', 'b', ListSide.Left, ListSide.Right)`. */
export const ListSide = { Left: 'LEFT', Right: 'RIGHT' } as const;

/** Where `LINSERT` places the new element relative to the pivot: `BEFORE` or `AFTER`. */
export type InsertPosition = 'BEFORE' | 'AFTER';
/** Named {@link InsertPosition} values, e.g. `c.linsert('l', InsertPosition.Before, 'pivot', 'x')`. */
export const InsertPosition = { Before: 'BEFORE', After: 'AFTER' } as const;

/** Result order of geo queries: `ASC` (nearest first) or `DESC`. */
export type GeoOrder = 'ASC' | 'DESC';
/** Named {@link GeoOrder} values, e.g. `{ order: GeoOrder.Asc }`. */
export const GeoOrder = { Asc: 'ASC', Desc: 'DESC' } as const;

export interface ListPopResult {
  key: string;
  elements: string[];
}

export interface LPosOptions {
  rank?: number;
  count?: number;
  maxlen?: number;
}

export interface GeoMember {
  longitude: number;
  latitude: number;
  member: Value;
}

/** Geo distance unit: metres, kilometres, feet or miles (`m`, `km`, `ft`, `mi`; upper case also accepted). */
export type GeoUnit = 'M' | 'KM' | 'FT' | 'MI' | 'm' | 'km' | 'ft' | 'mi';
/** Named {@link GeoUnit} values, e.g. `c.geodist('g', 'a', 'b', GeoUnit.Km)`. */
export const GeoUnit = { M: 'm', Km: 'km', Ft: 'ft', Mi: 'mi' } as const;

export interface GeoSearchOptions {
  fromMember?: Value;
  fromLonLat?: [number, number];
  byRadius?: { radius: number; unit: GeoUnit };
  byBox?: { width: number; height: number; unit: GeoUnit };
  order?: GeoOrder;
  count?: number;
  any?: boolean;
  withCoord?: boolean;
  withDist?: boolean;
  withHash?: boolean;
}

export interface GeoRadiusOptions {
  withCoord?: boolean;
  withDist?: boolean;
  withHash?: boolean;
  count?: number;
  any?: boolean;
  order?: GeoOrder;
  store?: Key;
  storeDist?: Key;
}

export interface GeoMatch {
  member: string;
  distance?: number;
  hash?: number;
  coordinates?: [number, number];
}

export interface BfReserveOptions {
  expansion?: number;
  nonScaling?: boolean;
}

export interface BfInsertOptions {
  capacity?: number;
  error?: number;
  expansion?: number;
  noCreate?: boolean;
  nonScaling?: boolean;
}

export interface StreamEntry {
  id: string;
  fields: Record<string, string>;
}

export interface StreamReadResult {
  key: string;
  entries: StreamEntry[];
}

export interface XAddOptions {
  noMkStream?: boolean;
  maxLen?: number;
  minId?: string;
  /** `~` (accepted; Ruvio trims exactly anyway). */
  approximate?: boolean;
  /** Ruvio: hold the entry from XREADGROUP for this many milliseconds. */
  delayMs?: number;
}

export interface XTrimOptions {
  maxLen?: number;
  minId?: string;
  approximate?: boolean;
}

export interface XReadOptions {
  count?: number;
  /** Milliseconds; 0 waits forever. Blocks the whole connection: use a dedicated client. */
  block?: number;
}

export interface XReadGroupOptions extends XReadOptions {
  noAck?: boolean;
}

export interface XClaimOptions {
  idle?: number;
  time?: number;
  retryCount?: number;
  force?: boolean;
  justId?: boolean;
  lastId?: string;
}

export interface AutoClaimResult {
  next: string;
  entries: StreamEntry[] | string[];
  deleted: string[];
}

export interface PendingSummary {
  count: number;
  smallest: string | null;
  greatest: string | null;
  consumers: { name: string; pending: number }[];
}

export interface PendingRow {
  id: string;
  consumer: string;
  idleMs: number;
  deliveries: number;
}

export interface FillResult {
  /** `hit`: cached; `fill`: you must build and FILLSET; `busy`: someone else fills; `stale`: old value. */
  state: 'hit' | 'fill' | 'busy' | 'stale';
  value: string | null;
  /** The fill token for `fill`, and for `stale` when this caller should refresh. */
  token: string | null;
}

export interface IdemBeginResult {
  state: 'acquired' | 'pending' | 'conflict' | 'completed';
  /** The stored result when `completed`. */
  result: string | null;
}

export interface ChangeNote {
  seq: number;
  note: string;
}

export interface RestoreOptions {
  replace?: boolean;
  absTtl?: boolean;
  idleTime?: number;
  freq?: number;
}

export interface ClientKillOptions {
  id?: number;
  addr?: string;
  skipMe?: boolean;
}

/** Unit of the `start` / `end` range of `BITCOUNT` and `BITPOS`: `BYTE` or `BIT`. */
export type BitUnit = 'BYTE' | 'BIT';
/** Named {@link BitUnit} values, e.g. `c.bitcount('k', 0, 7, BitUnit.Bit)`. */
export const BitUnit = { Byte: 'BYTE', Bit: 'BIT' } as const;

/** Operation of `BITOP`: `AND`, `OR`, `XOR` or `NOT`. */
export type BitOp = 'AND' | 'OR' | 'XOR' | 'NOT';
/** Named {@link BitOp} values, e.g. `c.bitop(BitOp.And, 'dst', 'a', 'b')`. */
export const BitOp = { And: 'AND', Or: 'OR', Xor: 'XOR', Not: 'NOT' } as const;

/** `BITFIELD` field type: signed `i1`..`i64` or unsigned `u1`..`u63`, e.g. `'u8'`, `'i16'`. */
export type BitfieldType = `i${number}` | `u${number}`;
/** `BITFIELD OVERFLOW` mode: `WRAP`, `SAT` or `FAIL`. */
export type BitfieldOverflow = 'WRAP' | 'SAT' | 'FAIL';
/** Named {@link BitfieldOverflow} values, e.g. `BitfieldOp.overflow(BitfieldOverflow.Sat)`. */
export const BitfieldOverflow = { Wrap: 'WRAP', Sat: 'SAT', Fail: 'FAIL' } as const;
