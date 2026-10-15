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

export type ExpireCondition = 'NX' | 'XX' | 'GT' | 'LT';

export interface ScanOptions {
  match?: string;
  count?: number;
  type?: string;
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
  aggregate?: 'SUM' | 'MIN' | 'MAX';
}

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

export type GeoUnit = 'M' | 'KM' | 'FT' | 'MI' | 'm' | 'km' | 'ft' | 'mi';

export interface GeoSearchOptions {
  fromMember?: Value;
  fromLonLat?: [number, number];
  byRadius?: { radius: number; unit: GeoUnit };
  byBox?: { width: number; height: number; unit: GeoUnit };
  order?: 'ASC' | 'DESC';
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
  order?: 'ASC' | 'DESC';
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

export type BitUnit = 'BYTE' | 'BIT';

export type BitfieldType = string; // i1..i64, u1..u63
