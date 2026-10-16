// Build-time access to the repository's own measurement records and command list.
// Charts never carry numbers of their own: everything is read from these files.
import fs from "node:fs";
import path from "node:path";

const repo = path.resolve(process.cwd(), "..");
const readJson = (rel: string) => JSON.parse(fs.readFileSync(path.join(repo, rel), "utf8"));

export type Row = { label: string; ruvio: number; redis: number; ratio: number };
export type MemRow = { label: string; ruvioKib: number; redisKib: number; saved: number };
export type Platform = {
  id: "mac" | "linux";
  name: string;
  short: string;
  file: string;
  date: string;
  machine: string;
  clients: number;
  pipeline: number;
  redisVersion: string;
  rows: Row[];
  memory: MemRow[];
  geomean: number;
  atPar: number;
  faster: number;
  memSaved: number;
  memLower: number;
};

const cleanLabel = (s: string) => s.replace(/\s*\(baseline, for scale\)/, "");

function load(id: Platform["id"], file: string, name: string, short: string): Platform {
  const d = readJson(file);
  const rows: Row[] = d.rows.map((r: any) => ({ label: cleanLabel(r.label), ruvio: r.ruvio, redis: r.redis, ratio: r.ruvio / r.redis }));
  const memory: MemRow[] = d.memory.map((m: any) => ({
    label: m.label,
    ruvioKib: m.ruvio.delta_kib,
    redisKib: m.redis.delta_kib,
    saved: 1 - m.ruvio.delta_kib / m.redis.delta_kib,
  }));
  const geomean = Math.exp(rows.reduce((a, r) => a + Math.log(r.ratio), 0) / rows.length);
  const memR = memory.reduce((a, m) => a + m.ruvioKib, 0);
  const memX = memory.reduce((a, m) => a + m.redisKib, 0);
  const redisVersion = (String(d.redis_version).match(/v=([\d.]+)/) || [])[1] ?? "";
  return {
    id, name, short, file, date: d.date,
    machine: d.environment?.machine ?? "",
    clients: d.config.clients, pipeline: d.config.pipeline, redisVersion,
    rows, memory, geomean,
    atPar: rows.filter((r) => r.ratio >= 0.97).length,
    faster: rows.filter((r) => r.ratio >= 1).length,
    memSaved: 1 - memR / memX,
    memLower: memory.filter((m) => m.ruvioKib < m.redisKib).length,
  };
}

export const platforms: Platform[] = [
  load("mac", "site/data/mac-2026-10-14.json", "Apple M4 Pro", "Apple M4 Pro"),
  load("linux", "site/data/linux-hostinger-2026-10-14.json", "AMD EPYC, 2 vCPU", "AMD EPYC (x86)"),
];
export const primary = platforms[0];

// Command list checked by CI.
const matrix = readJson("docs/compatibility-matrix.json");
export type Command = { name: string; family: string; limitations: string };
export const commands: Command[] = matrix.commands.map((c: any) => ({ name: c.name, family: c.family, limitations: c.limitations ?? "" }));

const opsFamilies = new Set(["connection", "scripts", "acl", "cluster", "pubsub", "transactions", "introspection", "replication"]);
const keyFamilies = new Set(["keys", "keyspace"]);
export type FamilyKind = "data" | "keys" | "ops";
export const families: { family: string; count: number; kind: FamilyKind }[] = Object.entries(
  commands.reduce<Record<string, number>>((acc, c) => ((acc[c.family] = (acc[c.family] ?? 0) + 1), acc), {}),
)
  .map(([family, count]) => ({ family, count, kind: (keyFamilies.has(family) ? "keys" : opsFamilies.has(family) ? "ops" : "data") as FamilyKind }))
  .sort((a, b) => b.count - a.count);


// Formatting helpers shared by charts and tables.
export const fmtOps = (v: number) => (v >= 1e6 ? `${(v / 1e6).toFixed(2)}M` : `${Math.round(v / 1e3)}K`);
export const fmtX = (v: number) => `${v.toFixed(2)}×`;
export const fmtMiB = (kib: number) => (kib / 1024).toFixed(1);
export const pct = (v: number) => `${Math.round(v * 100)}%`;
