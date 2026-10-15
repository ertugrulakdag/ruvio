// Helpers for Ruvio JSON documents. Paths use the dialect the server accepts:
// `$`, `.field` and `[N]` only, so a key with other characters has no path.

const simpleKey = /^[A-Za-z_][A-Za-z0-9_-]*$/;

export function kindOf(value) {
  if (value === null) return "null";
  if (Array.isArray(value)) return "array";
  return typeof value;
}

export function childPath(parent, key) {
  if (parent === null) return null;
  if (typeof key === "number") return `${parent}[${key}]`;
  return simpleKey.test(key) ? `${parent}.${key}` : null;
}

// Value at one of the paths this UI creates (`$`, `.field`, `[N]`), or undefined.
export function resolvePath(root, path) {
  if (path === "$") return root;
  let current = root;
  const steps = path.slice(1).match(/\.[A-Za-z_][A-Za-z0-9_-]*|\[\d+\]/g);
  if (!steps || steps.join("") !== path.slice(1)) return undefined;
  for (const step of steps) {
    if (current === null || typeof current !== "object") return undefined;
    current = step[0] === "[" ? current[Number(step.slice(1, -1))] : current[step.slice(1)];
    if (current === undefined) return undefined;
  }
  return current;
}

export function parseJson(text) {
  try {
    return { ok: true, value: JSON.parse(text) };
  } catch (error) {
    return { ok: false, error: error.message };
  }
}

export function pretty(value) {
  return JSON.stringify(value, null, 2);
}

export function prettyText(text) {
  const parsed = parseJson(text);
  return parsed.ok ? pretty(parsed.value) : text;
}

export function describe(value) {
  const kind = kindOf(value);
  if (kind === "array") return `array · ${value.length} ${value.length === 1 ? "item" : "items"}`;
  if (kind === "object") {
    const count = Object.keys(value).length;
    return `object · ${count} ${count === 1 ? "key" : "keys"}`;
  }
  return kind;
}

export function preview(value, limit = 60) {
  const kind = kindOf(value);
  if (kind === "array") return `[${value.length}]`;
  if (kind === "object") return `{${Object.keys(value).length}}`;
  const text = JSON.stringify(value);
  return text.length > limit ? `${text.slice(0, limit)}…` : text;
}

export function entries(value) {
  if (Array.isArray(value)) return value.map((item, index) => [index, item]);
  return Object.entries(value);
}

// The server answers JSON.GET with one bulk string. Anything else (nil, errors,
// an array of replies) is not a document.
export function documentOf(command, value) {
  if (!/^JSON\.(GET|MERGE)$/i.test(command) || value?.kind !== "bulk" || typeof value.text !== "string") return null;
  const parsed = parseJson(value.text);
  return parsed.ok && parsed.value !== null && typeof parsed.value === "object" ? { text: value.text, value: parsed.value } : null;
}

export function bytesLabel(count) {
  if (count < 1024) return `${count} B`;
  if (count < 1024 * 1024) return `${(count / 1024).toFixed(1)} KiB`;
  return `${(count / 1024 / 1024).toFixed(1)} MiB`;
}
