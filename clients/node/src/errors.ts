// SPDX-License-Identifier: MIT

/** A server error reply (`-ERR ...`, `-WRONGTYPE ...`). `code` is the first word. */
export class RuvioError extends Error {
  /** The error prefix sent by the server, e.g. `ERR`, `WRONGTYPE`, `NOSCRIPT`. */
  readonly code: string;
  /** The command that failed, when known. */
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

/** The connection failed or closed before a reply arrived. The command may or may not have run. */
export class ConnectionError extends Error {
  override get name(): string {
    return 'ConnectionError';
  }
}

/** A reply had a shape the typed method does not understand. */
export class ReplyError extends Error {
  override get name(): string {
    return 'ReplyError';
  }
}
