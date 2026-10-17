# Changelog

Notable changes to the Ruvio server (`ruvio`, `ruvio-ctl`, the Docker image and the release binaries). New entries go under "Unreleased"; `scripts/release_version.py` moves them under the version number when the package is released.

## Unreleased

## 0.3.3 - 2026-10-17

- **Behaviour change:** `port_mode = "per-shard"` is now the default. A multi-shard server also listens on port+1 .. port+N-1, one port per shard; `single` is deprecated. A key sent to the wrong shard's port is forwarded to its owner instead of answered with `MOVED` (`SSUBSCRIBE` and `SPUBLISH` still answer `MOVED`).
- **Behaviour change:** in per-shard mode `FLUSHDB` empties only the shard it is sent to, like Redis Cluster; a plain client on one port sees only that shard for `KEYS`, `DBSIZE`, `SCAN` and `FLUSHALL`.
- `PUBLISH` reaches subscribers on every shard without waiting for them. Each shard's fan-out is bounded (16384 messages or 64 MiB); messages over the bound are dropped and counted in `INFO` as `pubsub_cluster_dropped`. The reply counts the local subscribers. `PUBLISH` inside `MULTI` is refused on a multi-shard server.
- `WAIT` also waits for the replicas of the shards a connection's writes were forwarded to.
- More writes share one WAL group write (`INCRBYFLOAT`, `SETRANGE`, `BITFIELD`, `COPY`, `LSET`, `LMOVE`), and an `SADD` that adds nothing writes no WAL record. `SLOWLOG` times commands with the CPU cycle counter.
- `RUVIO_WAL_PREFILL=auto|on|off` controls zero-filling of new WAL segments.
- Fixed: ACL rules naming a subcommand allocated a new copy of the name each time.
- Fixed: on Windows, backup, restore and replication failed to flush files opened read-only.
- Fixed: a replication port past 65535 (listen port plus shard number) wrapped around to an unrelated port instead of being refused.
- Fixed: on macOS a blocked client that hung up behind unread input was not noticed until its timeout.

## 0.3.2

- First release from the public repository: prebuilt `ruvio` and `ruvio-ctl` binaries for Linux x64 and arm64, macOS arm64 and Windows x64 on GitHub Releases, and the `salihcantekin/ruvio` Docker image for amd64 and arm64.
