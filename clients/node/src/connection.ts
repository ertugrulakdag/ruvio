// SPDX-License-Identifier: MIT
// One multiplexed socket: frames from concurrent callers share it, replies resolve FIFO.

import * as net from 'node:net';
import * as tls from 'node:tls';
import { EventEmitter } from 'node:events';
import { Encoder, cmd, type Arg, type Cmd } from './encoder.js';
import { Parser, NEED_MORE, PushMessage } from './parser.js';
import { RuvioError, ConnectionError } from './errors.js';
import type { Tf } from './transforms.js';

/**
 * Connection settings shared by {@link RuvioClient}, {@link ClusterClient} and {@link Subscriber}.
 * All fields are optional.
 *
 * @example
 * ```ts
 * const client = createClient({
 *   host: 'ruvio.internal',
 *   port: 6379,
 *   tls: true,
 *   username: 'app',
 *   password: process.env.RUVIO_PASSWORD,
 *   db: 1,
 *   name: 'api-worker',
 *   connectTimeoutMs: 2000,
 * });
 * ```
 */
export interface ConnectionOptions {
  /** Server host (default `'127.0.0.1'`). */
  host?: string;
  /** Server port (default `6379`). */
  port?: number;
  /** Unix socket path, used instead of host/port. Not combined with `tls`. */
  path?: string;
  /** Use TLS: `true` for defaults, or Node's `tls.ConnectionOptions` (CA, client certificate, `servername`, ...). Default: plain TCP. */
  tls?: boolean | tls.ConnectionOptions;
  /** ACL user name sent with the password on connect (default user when omitted). Needs `password`. */
  username?: string;
  /** Password sent on every (re)connect (`AUTH`, or inside `HELLO 3`). A wrong password makes the connect fail. */
  password?: string;
  /** Logical database selected on every (re)connect (default 0). A cluster-mode server allows only 0. */
  db?: number;
  /** Connection name set with `CLIENT SETNAME` on every (re)connect. */
  name?: string;
  /** RESP protocol version: 2 (default) or 3 (sends `HELLO 3` on connect; enables push messages such as client-side-cache invalidation). */
  protocol?: 2 | 3;
  /** Abort a connection attempt after this many milliseconds with a {@link ConnectionError}. Default: no timeout. There is no per-command timeout. */
  connectTimeoutMs?: number;
  /** Disable Nagle's algorithm (`TCP_NODELAY`). Default true. */
  noDelay?: boolean;
  /** Enable TCP keep-alive probes after 30 s idle. Default true. */
  keepAlive?: boolean;
  /**
   * How frames queued by concurrent callers are coalesced into one socket write:
   * `'microtask'` (default) flushes after the current synchronous run of code;
   * `'immediate'` waits for `setImmediate` (also collects callers resumed by the same I/O event).
   * Neither adds a timer delay.
   */
  flush?: 'microtask' | 'immediate';
  /**
   * Reopen the socket lazily on the next command after a drop (default true). With `false` the
   * first drop closes the client for good and later commands reject with {@link ConnectionError}.
   */
  reconnect?: boolean;
  /** Size in bytes of the reused socket read buffer (default 65536). */
  readBufferSize?: number;
}

/** Receives a `-MOVED` error for a kept frame `buf[off..off+len)` together with its pending reply slot. */
export type MovedHandler = (
  e: RuvioError,
  buf: Buffer,
  off: number,
  len: number,
  tf: Tf | null,
  fl: number,
  res: (v: unknown) => void,
  rej: (e: unknown) => void,
) => void;

/** Batch receiver: one slot per frame of a pipeline or transaction. */
export interface BatchSink {
  set(index: number, value: unknown): void;
}

const F_BINARY = 1;

// Shared executor state: `new Promise(EXEC)` with no per-call closure.
let xConn: Connection | null = null;
let xTf: Tf | null = null;
let xFl = 0;
function EXEC(resolve: (v: unknown) => void, reject: (e: unknown) => void): void {
  xConn!.enqueue(resolve, reject, xTf, xFl);
}

const C_AUTH = cmd('AUTH');
const C_SELECT = cmd('SELECT');
const C_SETNAME = cmd('CLIENT', 'SETNAME');
const C_HELLO = cmd('HELLO');

/** @internal The multiplexed transport behind a client. */
export class Connection extends EventEmitter {
  readonly opts: ConnectionOptions;
  readonly enc: Encoder;
  private socket: net.Socket | null = null;
  private readonly parser = new Parser();
  private readonly readBuf: Buffer;
  // Pending-reply ring: callback (resolve fn or BatchSink), context (reject fn or slot index), transform, flags.
  private rRes: unknown[] = new Array(1024);
  private rRej: unknown[] = new Array(1024);
  private rTf: (Tf | null)[] = new Array(1024);
  private rFl: Uint8Array = new Uint8Array(1024);
  private mask = 1023;
  private head = 0;
  private count = 0;
  private scheduled = false;
  private closed = false;
  private connected = false;
  private readonly flushCb: () => void;
  private readonly immediate: boolean;
  /** Intercepts a reply before FIFO dispatch (Pub/Sub). Return true when consumed. */
  intercept: ((v: unknown) => boolean) | null = null;
  /** Parse every bulk string as a Buffer (binary Pub/Sub). */
  forceBinary = false;
  /** Database to restore on reconnect (tracks SELECT through the typed client). */
  db: number;
  // Cluster routing keeps, per pending single command, where its frame sits in the write slab
  // (not overwritten while the command waits for its reply), so a `-MOVED` reply can re-send it;
  // the router copies the frame before any wait, since a reconnect rewinds the slab.
  private onMoved: MovedHandler | null = null;
  private rFB: (Buffer | null)[] | null = null;
  private rFO: Int32Array | null = null;
  private rFN: Int32Array | null = null;
  private lastBuf: Buffer | null = null;
  private lastOff = 0;
  private lastLen = 0;

  constructor(opts: ConnectionOptions) {
    super();
    this.opts = opts;
    this.db = opts.db ?? 0;
    this.enc = new Encoder((chunk) => this.write(chunk));
    this.readBuf = Buffer.allocUnsafe(opts.readBufferSize ?? 64 * 1024);
    this.immediate = opts.flush === 'immediate';
    this.flushCb = () => {
      this.scheduled = false;
      this.enc.flush();
    };
  }

  /** Pending replies. */
  get inFlight(): number {
    return this.count;
  }

  get isOpen(): boolean {
    return this.socket !== null;
  }

  get isConnected(): boolean {
    return this.connected;
  }

  get isClosed(): boolean {
    return this.closed;
  }

  private write(chunk: Buffer): void {
    const s = this.socket;
    if (s !== null) s.write(chunk);
  }

  /** Encoder for the next frame, opening the socket first when needed. */
  encoder(): Encoder {
    if (this.socket === null) this.open();
    return this.enc;
  }

  /** Registers the reply slot for the frame just encoded and returns its promise. */
  track(tf: Tf | null, fl: number): Promise<any> {
    if (this.closed) {
      this.enc.pos = this.enc.frameStart = this.enc.start;
      return Promise.reject(new ConnectionError('ruvio: client is closed'));
    }
    xConn = this;
    xTf = tf;
    xFl = fl;
    const p = new Promise(EXEC);
    xConn = null;
    xTf = null;
    this.schedule();
    return p;
  }

  /** Like {@link track}, but with the caller's own resolve / reject functions (cluster routing). */
  trackCb(res: (v: any) => void, rej: (e: unknown) => void, tf: Tf | null, fl: number): void {
    if (this.closed) {
      this.enc.pos = this.enc.frameStart = this.enc.start;
      rej(new ConnectionError('ruvio: client is closed'));
      return;
    }
    this.enqueue(res, rej, tf, fl);
    this.schedule();
  }

  /** Turns on frame keeping for {@link trackKept}; `h` receives every `-MOVED` reply of such a command. */
  keepFrames(h: MovedHandler): void {
    this.onMoved = h;
    this.rFB = new Array(this.mask + 1).fill(null);
    this.rFO = new Int32Array(this.mask + 1);
    this.rFN = new Int32Array(this.mask + 1);
  }

  /** Like {@link track}, for the frame just copied in by {@link writeRaw}: remembers where it sits. */
  trackKept(tf: Tf | null, fl: number): Promise<any> {
    const p = this.track(tf, fl);
    if (!this.closed && this.rFB !== null) {
      const i = (this.head + this.count - 1) & this.mask;
      this.rFB[i] = this.lastBuf;
      this.rFO![i] = this.lastOff;
      this.rFN![i] = this.lastLen;
    }
    return p;
  }

  /** Registers `from..to` reply slots of a batch (their frames already written); slot `k` is `sink.set(k, ...)`. */
  trackRange(sink: BatchSink, from: number, to: number, fls: Uint8Array | number[]): void {
    if (this.closed) {
      this.enc.pos = this.enc.frameStart = this.enc.start;
      const err = new ConnectionError('ruvio: client is closed');
      for (let i = from; i < to; i++) sink.set(i, err);
      return;
    }
    for (let i = from; i < to; i++) this.enqueue(sink, i, null, fls[i]!);
    this.schedule();
  }

  /** Registers `n` reply slots for a batch (frames already encoded). */
  trackBatch(sink: BatchSink, fls: Uint8Array | number[], n: number): void {
    if (this.closed) {
      this.enc.pos = this.enc.frameStart = this.enc.start;
      const err = new ConnectionError('ruvio: client is closed');
      for (let i = 0; i < n; i++) sink.set(i, err);
      return;
    }
    for (let i = 0; i < n; i++) this.enqueue(sink, i, null, fls[i]!);
    this.schedule();
  }

  /** Registers one reply slot (`sink.set(index, reply)`) for a frame already written. */
  trackOne(sink: BatchSink, index: number, fl: number): void {
    if (this.closed) {
      this.enc.pos = this.enc.frameStart = this.enc.start;
      sink.set(index, new ConnectionError('ruvio: client is closed'));
      return;
    }
    this.enqueue(sink, index, null, fl);
    this.schedule();
  }

  /** Copies pre-encoded frames into the outgoing buffer. */
  writeFrames(bytes: Buffer): void {
    const e = this.encoder();
    e.ensure(bytes.length);
    bytes.copy(e.buf, e.pos);
    e.pos += bytes.length;
    e.endFrame();
  }

  /** Copies `src[s..e)` (one or more whole frames) into the outgoing buffer. */
  writeRaw(src: Uint8Array, s: number, e: number): void {
    const enc = this.encoder();
    const n = e - s;
    enc.ensure(n);
    const p = enc.pos;
    if (n <= 64) for (let i = 0; i < n; i++) enc.buf[p + i] = src[s + i]!;
    else enc.buf.set(src.subarray(s, e), p);
    enc.pos = p + n;
    enc.frameStart = enc.pos;
    enc.partial = false;
    this.lastBuf = enc.buf;
    this.lastOff = p;
    this.lastLen = n;
  }

  enqueue(res: unknown, rej: unknown, tf: Tf | null, fl: number): void {
    if (this.count > this.mask) this.grow();
    const i = (this.head + this.count) & this.mask;
    this.rRes[i] = res;
    this.rRej[i] = rej;
    this.rTf[i] = tf;
    this.rFl[i] = fl;
    this.count++;
  }

  private grow(): void {
    const n = (this.mask + 1) * 2;
    const res = new Array(n), rej = new Array(n), tf = new Array(n), fl = new Uint8Array(n);
    for (let k = 0; k < this.count; k++) {
      const i = (this.head + k) & this.mask;
      res[k] = this.rRes[i];
      rej[k] = this.rRej[i];
      tf[k] = this.rTf[i];
      fl[k] = this.rFl[i]!;
    }
    if (this.rFB !== null) {
      const fb = new Array(n).fill(null), fo = new Int32Array(n), fn = new Int32Array(n);
      for (let k = 0; k < this.count; k++) {
        const i = (this.head + k) & this.mask;
        fb[k] = this.rFB[i];
        fo[k] = this.rFO![i]!;
        fn[k] = this.rFN![i]!;
      }
      this.rFB = fb;
      this.rFO = fo;
      this.rFN = fn;
    }
    this.rRes = res;
    this.rRej = rej;
    this.rTf = tf;
    this.rFl = fl;
    this.head = 0;
    this.mask = n - 1;
  }

  private schedule(): void {
    if (!this.scheduled) {
      this.scheduled = true;
      if (this.immediate) setImmediate(this.flushCb);
      else queueMicrotask(this.flushCb);
    }
  }

  /** Writes queued frames now. */
  flushNow(): void {
    this.enc.flush();
  }

  // ---- socket lifecycle ----------------------------------------------------------------

  /** Opens the socket (if needed) and resolves once the handshake replies have arrived. */
  connect(): Promise<void> {
    if (this.closed) return Promise.reject(new ConnectionError('ruvio: client is closed'));
    if (this.socket === null) this.open();
    if (this.connected && this.count === 0) return Promise.resolve();
    // A PING queued behind the handshake resolves once everything before it has answered.
    const e = this.enc;
    e.begin(PING, 0);
    e.endFrame();
    return this.track(null, 0).then(() => undefined);
  }

  private open(): void {
    if (this.closed) return;
    const o = this.opts;
    const onread = {
      buffer: this.readBuf,
      callback: (n: number, buf: Buffer): boolean => {
        this.onData(buf, n);
        return true;
      },
    };
    let socket: net.Socket;
    if (o.tls) {
      const topts: tls.ConnectionOptions = o.tls === true ? {} : { ...o.tls };
      socket = tls.connect({ host: o.host ?? '127.0.0.1', port: o.port ?? 6379, servername: o.host, ...topts });
      socket.on('data', (d: Buffer) => this.onData(d, d.length));
    } else if (o.path !== undefined) {
      socket = net.connect({ path: o.path, onread } as net.NetConnectOpts);
    } else {
      socket = net.connect({ host: o.host ?? '127.0.0.1', port: o.port ?? 6379, onread } as net.NetConnectOpts);
    }
    this.socket = socket;
    this.connected = false;
    this.parser.reset();
    socket.setNoDelay(o.noDelay ?? true);
    if (o.keepAlive ?? true) socket.setKeepAlive(true, 30000);
    let timer: NodeJS.Timeout | null = null;
    if (o.connectTimeoutMs !== undefined && o.connectTimeoutMs > 0) {
      timer = setTimeout(() => socket.destroy(new ConnectionError('ruvio: connect timeout')), o.connectTimeoutMs);
    }
    socket.once(o.tls ? 'secureConnect' : 'connect', () => {
      if (timer !== null) clearTimeout(timer);
      if (this.socket !== socket) return;
      this.connected = true;
      this.emit('connect');
    });
    socket.on('error', (err) => {
      if (this.socket === socket) this.lastError = err;
    });
    socket.on('close', () => {
      if (timer !== null) clearTimeout(timer);
      if (this.socket === socket) this.onClose();
    });
    this.handshake();
  }

  private lastError: Error | null = null;

  private handshake(): void {
    const o = this.opts;
    const e = this.enc;
    // A failed handshake step kills this socket at once, before any later reply is dispatched.
    const fail = (err: unknown) => this.abort(err as Error);
    const send = (c: Cmd, args: Arg[]) => {
      e.begin(c, args.length);
      for (const a of args) e.arg(a);
      e.endFrame();
      this.enqueue(noop, fail, null, 0);
    };
    if (o.protocol === 3) {
      const a: Arg[] = [3];
      if (o.password !== undefined) a.push('AUTH', o.username ?? 'default', o.password);
      send(C_HELLO, a);
    } else if (o.password !== undefined) {
      send(C_AUTH, o.username !== undefined ? [o.username, o.password] : [o.password]);
    }
    if (this.db !== 0) send(C_SELECT, [this.db]);
    if (o.name !== undefined) send(C_SETNAME, [o.name]);
    this.schedule();
  }

  /** Fails the current socket synchronously: every pending call rejects now. */
  private abort(err: Error): void {
    const s = this.socket;
    if (s === null) return;
    this.lastError = err;
    this.onClose();
    s.destroy();
  }

  /** Bumped on every socket close; stops a read loop that outlived its socket. */
  private gen = 0;

  private onClose(): void {
    this.gen++;
    const err = new ConnectionError(
      'ruvio: connection closed' + (this.lastError ? ': ' + this.lastError.message : ''),
    );
    if (this.lastError) (err as Error & { cause?: unknown }).cause = this.lastError;
    this.lastError = null;
    this.socket = null;
    const wasConnected = this.connected;
    this.connected = false;
    // Drop unsent bytes; their callers are rejected with everyone else.
    this.enc.pos = this.enc.start = this.enc.frameStart = 0;
    this.parser.reset();
    this.rejectAll(err);
    if (wasConnected) this.emit('disconnect', err);
    else this.emit('connectError', err);
    if (this.listenerCount('error') > 0) this.emit('error', err);
    if (this.opts.reconnect === false) this.closed = true;
  }

  private rejectAll(err: Error): void {
    while (this.count > 0) {
      const i = this.head;
      const res = this.rRes[i];
      const rej = this.rRej[i];
      this.rRes[i] = this.rRej[i] = this.rTf[i] = null;
      if (this.rFB !== null) this.rFB[i] = null;
      this.head = (i + 1) & this.mask;
      this.count--;
      if (typeof res === 'function') (rej as (e: unknown) => void)(err);
      else (res as BatchSink).set(rej as number, err);
    }
  }

  /** Closes the socket after pending replies arrive (QUIT-free graceful close). */
  async quit(): Promise<void> {
    if (this.closed) return;
    if (this.socket !== null && this.count > 0) {
      try {
        await this.connect();
      } catch {
        /* closing anyway */
      }
    }
    this.disconnect();
  }

  /** Closes now; pending calls reject. */
  disconnect(): void {
    this.closed = true;
    const s = this.socket;
    if (s !== null) {
      this.enc.flush();
      s.destroy();
    }
  }

  /** Destroys the socket (pending calls reject); a later command reconnects. */
  destroySocket(err?: Error): void {
    this.socket?.destroy(err);
  }

  // ---- replies -------------------------------------------------------------------------

  private onData(chunk: Buffer, n: number): void {
    const p = this.parser;
    const gen = this.gen;
    p.feed(chunk, n);
    for (;;) {
      if (this.gen !== gen) return;
      p.binary = this.forceBinary || (this.count > 0 && (this.rFl[this.head]! & F_BINARY) !== 0);
      let v: unknown;
      try {
        v = p.next();
      } catch (err) {
        this.lastError = err as Error;
        this.socket?.destroy(err as Error);
        return;
      }
      if (v === NEED_MORE) return;
      if (v instanceof PushMessage) {
        this.emit('push', v.data);
        continue;
      }
      if (this.intercept !== null && this.intercept(v)) continue;
      this.dispatch(v);
    }
  }

  private dispatch(v: unknown): void {
    if (this.count === 0) {
      this.emit('unexpected', v);
      return;
    }
    const i = this.head;
    const res = this.rRes[i];
    const rej = this.rRej[i];
    const tf = this.rTf[i]!;
    this.rRes[i] = null;
    this.rRej[i] = null;
    this.rTf[i] = null;
    this.head = (i + 1) & this.mask;
    this.count--;
    let fb: Buffer | null = null;
    if (this.rFB !== null) {
      fb = this.rFB[i]!;
      this.rFB[i] = null;
    }
    if (typeof res === 'function') {
      if (v instanceof RuvioError) {
        if (fb !== null && v.code === 'MOVED') {
          this.onMoved!(v, fb, this.rFO![i]!, this.rFN![i]!, tf, this.rFl[i]!, res as (x: unknown) => void, rej as (e: unknown) => void);
          return;
        }
        (rej as (e: unknown) => void)(v);
      } else if (tf === null) (res as (x: unknown) => void)(v);
      else {
        let out: unknown;
        try {
          out = tf(v);
        } catch (err) {
          (rej as (e: unknown) => void)(err);
          return;
        }
        (res as (x: unknown) => void)(out);
      }
    } else {
      (res as BatchSink).set(rej as number, v);
    }
  }
}

const PING = cmd('PING');
function noop(): void {}
