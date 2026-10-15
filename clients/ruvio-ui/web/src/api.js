const uiSessionId = typeof sessionStorage === "undefined"
  ? crypto.randomUUID()
  : sessionStorage.getItem("ruvio.session") || crypto.randomUUID();
if (typeof sessionStorage !== "undefined") sessionStorage.setItem("ruvio.session", uiSessionId);

export async function api(path, body) {
  const readOnly = ["/api/session", "/api/defaults", "/api/shards"].includes(path)
    || (path === "/api/command" && isReadOnly(body?.args || []));
  for (let attempt = 0; ; attempt += 1) {
    try {
      return await request(path, body, readOnly && attempt < 2);
    } catch (error) {
      if (!readOnly || attempt >= 2 || !(error instanceof TypeError || error.transient)) throw error;
      await new Promise((resolve) => setTimeout(resolve, 100 * (attempt + 1)));
    }
  }
}

function isReadOnly(args) {
  const command = String(args[0]).toUpperCase();
  return ["PING", "INFO", "TYPE", "TTL", "PTTL", "GET", "MGET", "EXISTS", "SCAN",
    "LLEN", "LRANGE", "HLEN", "HSCAN", "HGET", "HGETALL", "SCARD", "SMEMBERS",
    "SISMEMBER", "ZCARD", "ZRANGE", "ZSCORE", "XLEN", "XRANGE", "XINFO", "DBSIZE"].includes(command)
    || (command === "ACL" && ["USERS", "GETUSER", "LIST"].includes(String(args[1]).toUpperCase()));
}

async function request(path, body, canRetry) {
  const response = await fetch(path, {
    method: body ? "POST" : "GET",
    headers: {
      "x-ruvio-session": uiSessionId,
      ...(body ? { "content-type": "application/json" } : {}),
    },
    body: body ? JSON.stringify(body) : undefined,
  });
  if (canRetry && [502, 503, 504].includes(response.status)) {
    const error = new Error(`HTTP ${response.status}: ${response.statusText}`);
    error.transient = true;
    throw error;
  }
  const result = await response.json();
  if (!response.ok || result.ok === false) {
    throw new Error(result.error || `HTTP ${response.status}: ${response.statusText}`);
  }
  return result;
}

export function textOf(value) {
  if (!value) return "";
  if (value.kind === "bulk" || value.kind === "simple" || value.kind === "error") {
    return value.text ?? value.base64 ?? "";
  }
  if (value.kind === "integer") return String(value.value);
  if (value.kind === "null") return "(nil)";
  if (value.kind === "array") {
    return value.items.map((item, index) => `${index + 1}) ${textOf(item)}`).join("\n");
  }
  return "";
}
