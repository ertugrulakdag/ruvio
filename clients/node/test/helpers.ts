// SPDX-License-Identifier: MIT
import { RuvioClient, RuvioError } from '../src/index.js';

/** `host:port` of a live Ruvio server, or undefined (live tests then skip). */
export const ADDR = process.env.RUVIO_TEST_ADDR;
export const SKIP = ADDR ? false : 'RUVIO_TEST_ADDR not set';

export function addr(): { host: string; port: number } {
  const [host, port] = ADDR!.split(':');
  return { host: host!, port: Number(port) };
}

/** A client on its own logical database, flushed. */
export async function live(db: number): Promise<RuvioClient> {
  const c = new RuvioClient({ ...addr(), db });
  await c.flushdb();
  return c;
}

/** Resolves the error a promise rejects with (fails if it resolves). */
export async function rejects(p: Promise<unknown>): Promise<RuvioError> {
  try {
    await p;
  } catch (e) {
    return e as RuvioError;
  }
  throw new Error('expected a rejection');
}

/** Resolves either the value or the server error (for commands whose success depends on config). */
export async function settle<T>(p: Promise<T>): Promise<T | RuvioError> {
  try {
    return await p;
  } catch (e) {
    if (e instanceof RuvioError) return e;
    throw e;
  }
}

export const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

/** `host:port` of the first shard of a live 3-shard (per-shard port) server, or undefined. */
export const CLUSTER_ADDR = process.env.RUVIO_CLUSTER_ADDR;
export const CSKIP = CLUSTER_ADDR ? false : 'RUVIO_CLUSTER_ADDR not set';

export function clusterAddr(): { host: string; port: number } {
  const [host, port] = CLUSTER_ADDR!.split(':');
  return { host: host!, port: Number(port) };
}
