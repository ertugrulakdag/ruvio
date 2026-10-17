// Pure helpers for the Console tab: command-line parsing, safety checks, error hints, and
// a size-capped text rendering of a reply. No DOM, so everything here is unit-tested.

const ESCAPES = { n: "\n", r: "\r", t: "\t", b: "\b", a: "\x07", "\\": "\\", '"': '"' };

/** Split a command line into arguments. Double quotes allow \n \t \xHH \" \\ escapes; single quotes allow \'. */
export function tokenize(line) {
  const args = [];
  let i = 0;
  const n = line.length;
  while (i < n) {
    while (i < n && /\s/.test(line[i])) i += 1;
    if (i >= n) break;
    let current = "";
    const quote = line[i] === '"' || line[i] === "'" ? line[i] : null;
    if (quote) {
      i += 1;
      let closed = false;
      while (i < n) {
        const ch = line[i];
        if (ch === "\\" && i + 1 < n) {
          const next = line[i + 1];
          if (quote === '"' && next === "x" && /^[0-9a-fA-F]{2}$/.test(line.slice(i + 2, i + 4))) {
            current += String.fromCharCode(parseInt(line.slice(i + 2, i + 4), 16));
            i += 4;
            continue;
          }
          if (quote === '"' && ESCAPES[next] !== undefined) {
            current += ESCAPES[next];
            i += 2;
            continue;
          }
          if (quote === "'" && next === "'") {
            current += "'";
            i += 2;
            continue;
          }
        }
        if (ch === quote) {
          closed = true;
          i += 1;
          break;
        }
        current += ch;
        i += 1;
      }
      if (!closed) return { error: `Missing closing ${quote} quote.` };
      if (i < n && !/\s/.test(line[i])) return { error: "A closing quote must be followed by a space." };
    } else {
      while (i < n && !/\s/.test(line[i])) {
        current += line[i];
        i += 1;
      }
    }
    args.push(current);
  }
  return { args };
}

const BLOCKED = {
  SUBSCRIBE: "Subscriptions stream messages. Use the Pub/Sub tab.",
  PSUBSCRIBE: "Subscriptions stream messages. Use the Pub/Sub tab.",
  SSUBSCRIBE: "Subscriptions stream messages. Use the Pub/Sub tab.",
  UNSUBSCRIBE: "Subscriptions are managed in the Pub/Sub tab.",
  PUNSUBSCRIBE: "Subscriptions are managed in the Pub/Sub tab.",
  MONITOR: "MONITOR streams every command and cannot run in a request/response console.",
  SYNC: "Replication handshakes are not supported here.",
  PSYNC: "Replication handshakes are not supported here.",
  MULTI: "Transactions need one connection across several commands, which this console does not keep.",
  EXEC: "Transactions need one connection across several commands, which this console does not keep.",
  DISCARD: "Transactions need one connection across several commands, which this console does not keep.",
  WATCH: "Transactions need one connection across several commands, which this console does not keep.",
  AUTH: "You are already signed in. Sign out and sign in again to change user.",
  HELLO: "The console manages the protocol itself.",
  QUIT: "Use Disconnect to leave the console.",
  RESET: "The console manages the connection itself.",
};

const CONFIRM = {
  FLUSHALL: "FLUSHALL deletes every key on the server.",
  FLUSHDB: "FLUSHDB deletes every key in this database.",
  SHUTDOWN: "SHUTDOWN stops the server.",
  DEBUG: "DEBUG commands can crash or stall the server.",
  KEYS: "KEYS walks the whole keyspace in one call and can be slow on large data. SCAN is safer.",
};

/** Decide whether a parsed command may run: { kind: "ok" | "blocked" | "confirm", reason? }. */
export function classify(args) {
  if (!args.length) return { kind: "ok" };
  const name = args[0].toUpperCase();
  const sub = (args[1] || "").toUpperCase();
  if (BLOCKED[name]) return { kind: "blocked", reason: BLOCKED[name] };
  if (name.length > 1 && name[0] === "B" && ["BLPOP", "BRPOP", "BLMOVE", "BLMPOP", "BRPOPLPUSH", "BZPOPMIN", "BZPOPMAX", "BZMPOP"].includes(name)) {
    return { kind: "blocked", reason: "Blocking commands would hold the connection until data arrives. Use the non-blocking form (LPOP, RPOP, ...)." };
  }
  if (["XREAD", "XREADGROUP"].includes(name) && args.some((arg) => arg.toUpperCase() === "BLOCK")) {
    return { kind: "blocked", reason: "XREAD with BLOCK would hold the connection. Drop BLOCK to read what is there now." };
  }
  if (name === "WAIT" || name === "WAITAOF") return { kind: "blocked", reason: "WAIT blocks until replicas acknowledge, which the console does not do." };
  if (name === "CLIENT" && ["PAUSE", "KILL", "REPLY"].includes(sub)) return { kind: "confirm", reason: `CLIENT ${sub} affects other connections.` };
  if (name === "CONFIG" && sub === "SET") return { kind: "confirm", reason: "CONFIG SET changes the live server configuration." };
  if (name === "ACL" && ["DELUSER", "SETUSER"].includes(sub)) return { kind: "confirm", reason: `ACL ${sub} changes who can access the server.` };
  if (CONFIRM[name]) return { kind: "confirm", reason: CONFIRM[name] };
  return { kind: "ok" };
}

const HINTS = [
  [/^NOAUTH/i, "The server needs authentication. Sign out and sign in with a user that has access."],
  [/^(NOPERM|ERR .*no permissions)/i, "Your ACL user is not allowed to run this command or touch this key. Ask an admin to extend the user, or sign in with another one."],
  [/^WRONGTYPE/i, "The key holds a different type than this command expects. Check it with TYPE <key>."],
  [/^ERR unknown command/i, "The server does not know this command. Check the spelling, or see the compatibility list in the docs."],
  [/wrong number of arguments/i, "The argument count is off. Check the command's syntax."],
  [/^ERR syntax error/i, "An option or argument is malformed. Check the command's syntax."],
  [/not an integer|out of range/i, "A numeric argument is not a valid integer, or is out of range."],
  [/^(MOVED|ASK)\b/i, "The key lives on another shard. Pick All shards in the header so commands route to the owner."],
  [/^READONLY/i, "This node is read-only. Send writes to the primary."],
  [/^OOM/i, "The server is out of memory for writes. Free memory or raise the limit."],
  [/^LOADING/i, "The server is still loading data. Retry in a moment."],
  [/^BUSY/i, "The server is busy with a long-running operation. Retry shortly."],
  [/timed? ?out/i, "The server did not answer in time. Retry, or narrow the request."],
  [/not connected|session/i, "The panel lost its server session. Sign out and sign in again."],
  [/failed to fetch|network|ECONN|refused/i, "The operator could not reach the server. Check that Ruvio is running and reachable."],
];

/** Turn an error message into { text, hint }. The raw server text is always kept. */
export function explainError(message) {
  const text = String(message ?? "Unknown error");
  const hit = HINTS.find(([pattern]) => pattern.test(text));
  return { text, hint: hit ? hit[1] : "" };
}

export const LIMITS = { lines: 200, maxLines: 5000, step: 500, string: 1000 };

function quoteText(text, cap) {
  const shown = text.length > cap ? text.slice(0, cap) : text;
  const body = JSON.stringify(shown);
  return text.length > cap ? `${body} ... (${(text.length - cap).toLocaleString()} more characters hidden)` : body;
}

function leaf(value, cap) {
  switch (value?.kind) {
    case "null": return { text: "(nil)", type: "nil" };
    case "integer": return { text: `(integer) ${value.value}`, type: "number" };
    case "simple": return { text: value.text, type: "ok" };
    case "error": return { text: `(error) ${value.text}`, type: "error" };
    case "bulk":
      if (value.base64 != null) {
        const bytes = Math.floor((value.base64.length * 3) / 4);
        return { text: `(binary, about ${bytes.toLocaleString()} bytes, base64) ${quoteText(value.base64, 80)}`, type: "binary" };
      }
      return { text: quoteText(value.text ?? "", cap), type: "string" };
    default: return { text: "(empty)", type: "nil" };
  }
}

export const JSON_FORMAT_MAX = 1_000_000;

function textOf(value) {
  return value?.kind === "bulk" || value?.kind === "simple" ? value.text : null;
}

/** Pretty-printed JSON for a reply that is one JSON object/array string, else null. */
export function jsonText(value) {
  const text = value?.kind === "bulk" ? value.text : null;
  if (text == null || text.length > JSON_FORMAT_MAX) return null;
  const head = text.trimStart()[0];
  if (head !== "{" && head !== "[") return null;
  try {
    return JSON.stringify(JSON.parse(text), null, 2);
  } catch {
    return null;
  }
}

/** Views offered for a reply: the labels the console shows next to it. */
export function viewsFor(value) {
  const views = [{ id: "auto", label: "Auto" }];
  if (textOf(value) != null) views.push({ id: "text", label: "Text" });
  if (jsonText(value) != null) views.push({ id: "json", label: "JSON" });
  views.push({ id: "quoted", label: "Quoted" });
  return views;
}

function capLine(text, cap) {
  return text.length > cap ? `${text.slice(0, cap)} ... (${(text.length - cap).toLocaleString()} more characters hidden)` : text;
}

/**
 * Render a reply as lines, stopping at `limit` lines.
 *   view: "auto" (JSON or plain text for one string reply, redis-cli style for the rest),
 *         "text" (a string reply as is), "json" (pretty JSON), "quoted" (redis-cli style).
 *   trim: trim each line and drop blank ones.
 * Returns { lines: [{ text, type }], more, total } where `more` says lines were left out and
 * `total` is the top-level item count (1 for a scalar).
 */
export function formatValue(value, { limit = LIMITS.lines, string = LIMITS.string, view = "auto", trim = false } = {}) {
  const scalarText = textOf(value);
  let mode = view;
  let block = null;
  if (scalarText != null && (view === "auto" || view === "text" || view === "json")) {
    const pretty = view === "text" ? null : jsonText(value);
    if (pretty != null) {
      block = { source: pretty, type: "json" };
      mode = "json";
    } else if (view !== "json" && (view === "text" || /[\r\n]/.test(scalarText))) {
      block = { source: scalarText, type: value.kind === "simple" ? "ok" : "string" };
      mode = "text";
    }
  }
  if (block) {
    let all = block.source.split(/\r?\n/);
    if (trim) all = all.map((line) => line.trim()).filter(Boolean);
    const shown = all.slice(0, limit).map((line) => ({ text: capLine(line, Math.max(string, 2000)), type: block.type }));
    return { lines: shown, more: all.length > limit, total: 1, mode };
  }
  const lines = [];
  let more = false;
  const walk = (node, depth, label) => {
    if (lines.length >= limit) {
      more = true;
      return;
    }
    const pad = "   ".repeat(depth);
    if (node?.kind === "array") {
      if (!node.items.length) {
        lines.push({ text: `${pad}${label}(empty array)`, type: "nil" });
        return;
      }
      if (label) lines.push({ text: `${pad}${label}array of ${node.items.length.toLocaleString()}`, type: "meta" });
      const childDepth = label ? depth + 1 : depth;
      for (let index = 0; index < node.items.length; index += 1) {
        if (lines.length >= limit) {
          more = true;
          return;
        }
        walk(node.items[index], childDepth, `${index + 1}) `);
      }
      return;
    }
    const item = leaf(node, string);
    lines.push({ text: `${pad}${label}${item.text}`, type: item.type });
  };
  walk(value, 0, "");
  return { lines, more, total: value?.kind === "array" ? value.items.length : 1, mode: "quoted" };
}
