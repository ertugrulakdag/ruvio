// SPDX-License-Identifier: MIT

/**
 * The server answered a command with an error (`-ERR ...`, `-WRONGTYPE ...`, `-NOPERM ...`,
 * `-CROSSSLOT ...`). The command did not take effect. `message` is the full error text and `code`
 * is its first word, so branch on `err.code`.
 *
 * @example
 * ```ts
 * try {
 *   await client.lpush('a-string-key', 'x');
 * } catch (err) {
 *   if (err instanceof RuvioError && err.code === 'WRONGTYPE') console.log('not a list');
 * }
 * ```
 */
export class RuvioError extends Error {
  /** The error prefix sent by the server, e.g. `ERR`, `WRONGTYPE`, `NOSCRIPT`. */
  readonly code: string;
  /** The command name that failed. Reserved: the client does not fill it in today. */
  command?: string;

  constructor(message: string) {
    super(message);
    const sp = message.indexOf(' ');
    this.code = sp < 0 ? message : message.slice(0, sp);
  }

  override get name(): string {
    return 'RuvioError';
  }
}

/**
 * The connection failed or closed before a reply arrived (also: connect timeout, refused connection,
 * client already closed). The command may or may not have run, and the client never re-sends it on
 * its own. The low-level cause, if any, is in `cause`.
 */
export class ConnectionError extends Error {
  override get name(): string {
    return 'ConnectionError';
  }
}

/** A reply had a shape the typed method does not understand (for example an unknown `FILLGET` state). It points to a client/server version mismatch. */
export class ReplyError extends Error {
  override get name(): string {
    return 'ReplyError';
  }
}
