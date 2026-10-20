# Threat model

Ruvio is a memory-first key-value store that speaks the Redis protocol (RESP2),
written in Rust. The full model is in `docs/THREAT_MODEL.md`; read it first.
This file summarizes what matters for a scan.

## What this project does and where untrusted input enters
- The RESP listener (default `127.0.0.1:6379`, optional TLS) takes commands from
  any TCP client. The parser in `src/protocol` is the first untrusted-data path.
- Persistence files (WAL, snapshots, manifests) are read at startup and by
  `ruvio-ctl check`, `inspect`, and `restore`. A well-formed hostile backup is
  an operator-trust issue; a crash or memory error while loading a malformed
  one is a bug.
- The replication port and the metrics HTTP listener are separate, optional
  listeners.
- `EVAL` runs Lua 5.1 in a sandbox; escaping it or evading its quotas is in
  scope.

## Components that matter most / least
- Most: RESP parser, command dispatch, ACL and `AUTH`, persistence codecs,
  scripting sandbox, memory accounting, TLS and replication handshakes.
- Less: `clients/`, `integrations/`, `site-next/`, `benchmark/`, `deploy/`.
  Report only issues that reach the server.
- Out of scope: cluster bus, gossip, and failover do not exist.

## How to exercise it
- Binaries: `target/debug/ruvio` (server) and `target/debug/ruvio-ctl`.
- Start `target/debug/ruvio` with the defaults and talk RESP on port 6379
  (`printf 'PING\r\n' | nc 127.0.0.1 6379`).
- Library tests: `cargo test --locked --lib`. Fuzz targets are in `fuzz/`.
- The library crate denies `unsafe` by default; the few modules that allow it
  mark each use with a `// SAFETY:` comment, and those are worth a close look.

## How you rate severity
- Memory-safety errors, authentication or ACL bypass, and sandbox escape reachable
  from the network are high or critical.
- Remote crash or unbounded memory growth by a client inside the configured
  limits is medium to high.
- Anything that needs write access to the data directory or the config file is
  low, unless it bypasses the checksum checks.

## Anything to leave alone
- No encryption at rest, plain-text replication with `replication.tls = false`,
  and `nopass` default ACL with protected mode on are documented behavior.
- Large legal values inside the limits are documented residual risk.
