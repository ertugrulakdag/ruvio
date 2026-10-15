// SPDX-License-Identifier: MIT
// A scripted RESP server for routing tests: parses command arrays, lets a handler answer.

import * as net from 'node:net';

export type Handler = (args: string[], sock: net.Socket) => string | null | undefined;

export interface FakeServer {
  readonly port: number;
  /** Every command received, upper-cased name first. */
  readonly seen: string[][];
  /** Count of received commands whose name matches. */
  count(name: string): number;
  close(): Promise<void>;
}

export const bulk = (s: string): string => '$' + Buffer.byteLength(s) + '\r\n' + s + '\r\n';
export const arr = (...items: string[]): string => '*' + items.length + '\r\n' + items.join('');
export const int = (n: number): string => ':' + n + '\r\n';
export const err = (s: string): string => '-' + s + '\r\n';
export const ok = '+OK\r\n';

/** CLUSTER SLOTS reply for `[from, to, port]` ranges. */
export function slotsReply(ranges: [number, number, number][]): string {
  return arr(...ranges.map(([a, b, port]) => arr(int(a), int(b), arr(bulk('127.0.0.1'), int(port), bulk('node' + port)))));
}

export async function fakeServer(handler: Handler): Promise<FakeServer> {
  const seen: string[][] = [];
  const socks = new Set<net.Socket>();
  const server = net.createServer((sock) => {
    socks.add(sock);
    sock.on('close', () => socks.delete(sock));
    sock.on('error', () => undefined);
    let buf = Buffer.alloc(0);
    sock.on('data', (d) => {
      buf = Buffer.concat([buf, d]);
      for (;;) {
        const r = readCommand(buf);
        if (r === null) return;
        buf = buf.subarray(r.end);
        const args = r.args;
        args[0] = args[0]!.toUpperCase();
        seen.push(args);
        const reply = handler(args, sock);
        if (reply === null) return; // handler took over (e.g. destroyed the socket)
        sock.write(reply ?? ok);
      }
    });
  });
  await new Promise<void>((res) => server.listen(0, '127.0.0.1', res));
  const port = (server.address() as net.AddressInfo).port;
  return {
    port,
    seen,
    count: (name) => seen.filter((a) => a[0] === name).length,
    close: () =>
      new Promise<void>((res) => {
        for (const s of socks) s.destroy();
        server.close(() => res());
      }),
  };
}

function readCommand(b: Buffer): { args: string[]; end: number } | null {
  if (b.length === 0 || b[0] !== 42) return null;
  let i = b.indexOf('\r\n');
  if (i < 0) return null;
  const n = Number(b.toString('latin1', 1, i));
  i += 2;
  const args: string[] = [];
  for (let k = 0; k < n; k++) {
    const e = b.indexOf('\r\n', i);
    if (e < 0) return null;
    const len = Number(b.toString('latin1', i + 1, e));
    if (b.length < e + 2 + len + 2) return null;
    args.push(b.toString('utf8', e + 2, e + 2 + len));
    i = e + 2 + len + 2;
  }
  return { args, end: i };
}
