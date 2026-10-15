# Ruvio Product Roadmap

Ruvio's target is a production-grade, Redis-compatible, shared-nothing data
store: memory-first for low latency, with configurable on-disk durability and a
clear recovery story. Redis protocol compatibility is a means to ecosystem
compatibility; identical internals are not a goal.

This document is the canonical product backlog. Items are ordered by dependency
and risk, not by implementation convenience.

Every change follows [Performance and memory](PERFORMANCE.md). A change that
slows a command or raises RSS for the same data does not land.

## Opt-in document and search features

- [x] **Native JSON foundation.** `JSON.GET`, `JSON.SET`, `JSON.DEL`, and
  RFC 7396 `JSON.MERGE` support root/simple paths as a distinct value type.
  Simple path reads scan without materializing unrelated fields, and existing
  path writes splice a new compact value without building the whole JSON tree;
  each write still records one atomic full-document WAL mutation. Missing-field
  writes and root merges may rebuild the tree. No index work runs for non-JSON
  keys. Snapshots, TTL, eviction, routing, and ACL use existing key behavior. See
  [the compatibility contract](COMPATIBILITY.md#native-json-ruvio-extension).
- [ ] **Hybrid large-document JSON representation, conditional on measured gains.**
  Keep the compact bytes for small documents and non-JSON keys unchanged. An
  isolated October 2026 prototype compared the existing compact path code with
  a parsed `serde_json::Value` tree (three in-process timing runs; RSS from
  separate processes). On a 1 MiB document with one large string, existing-path
  updates ran at ~14k/s on compact bytes, ~20M/s on the tree *without* a WAL
  record, but only ~3.9k/s when the tree was serialized in full per update.
  For a 144 KiB document with 4,000 fields, 465 copies consumed ~67 MiB as
  compact bytes versus ~292 MiB as trees. These are component measurements,
  not server throughput or a production memory guarantee. A size-only switch
  with the existing full-document `JsonPut` is therefore not acceptable.
  The current implementation keeps small JSON compact. Above 32 KiB (lowered from 256 KiB after the 2026-10-09 Mac run, see J2),
  documents with at most 128 indexed paths and a large string retain one
  compact body plus small scalar field patches (up to 64 patches of at most
  8 KiB each before rebuilding). Unchanged root reads borrow
  the original body; patched root reads stream its spans. Scalar path reads and
  writes use internal byte-range indexes (not user-facing search indexes).
  Structural edits rebuild the body;
  documents not eligible for compact indexing use the bounded tree fallback.
  Both variants record validated path-result WAL mutations. The original tree
  design measured 1 MiB field SET at ~1.11M/s versus ~13.6k/s on the compact
  baseline and ~373k/s on RedisJSON, but its root GET was ~2,303/s versus
  ~6,250/s compact (RedisJSON ~2,347/s) and RSS increased ~84,640 KiB for
  64 documents versus ~77,104 KiB compact. Wide 140 KiB documents remain
  compact; 64 KiB documents remain slower than RedisJSON.
  Everysec/always restart smoke tests pass on the final release binary. A
  missing replica path target is rejected before WAL append and requests a
  primary checkpoint; catch-up replays pinned WAL from that checkpoint to
  an explicit watermark with expiration deferred and replica requests temporarily
  returning `LOADING`. Mixed transactions, expiry followed by PERSIST, restart,
  and catch-up while the primary keeps writing have targeted tests; WAL history
  is read in bounded batches. Interrupted catch-up now leaves a durable marker:
  restart retries a fresh snapshot or fails closed, and incomplete manual
  promotion needs explicit one-shot `--force-promote-incomplete`. Single- and
  multi-shard recovery and forced writer startup have targeted tests. Direct
  tree-to-RESP serialization alone only reached ~2,485/s root GET and
  ~84,608 KiB RSS versus ~6,098/s and ~77,088 KiB on the compact baseline.
  Indexed compact storage instead reached ~6,098/s root GET versus ~6,173/s
  compact and ~76,496 KiB RSS versus ~77,088 KiB (64 × 1 MiB, 3 rounds,
  25 clients, pipeline 8). After 50 field patches, root GET measured ~5,102/s
  in one 500-request sample. In separate 100,000-request runs, field SET
  reached ~1.10M/s versus ~13.3k/s compact; field GET ~1.27M/s versus
  ~16.3k/s. Five 100,000-request plain GET/SET rounds showed no clear
  non-JSON regression. These same-machine results are not a production
  guarantee. A two-process
  `everysec` deployment smoke test (`cargo build --release` then
  `python3 scripts/json_replica_backlog_smoke.py`) compacted the replica's old
  WAL cursor, recovered from snapshot sequence 2,001 through >38 MiB of
  retained WAL, and converged at sequence 47,001 while the primary accepted
  another 10,000 JSON path writes (~16 s on the test machine). The script
  verifies final document content and clearance of catch-up markers. Leave
  acceptance open for representative indexed and tree-fallback mixes, repeated
  structural edits, longer durability/replication trials, and sustained
  slow-replica/disk-retention limits: a lagging replica can prolong primary
  disk retention without a fixed cap. The [2026-10-07 RedisJSON comparison](BENCHMARK_RESULTS.md#native-json-vs-redisjson-2026-10-07)
  on build `7aebdaf` confirms that 1 MiB scalar field GET/SET and root GET
  outpace RedisJSON for the tested shape, but 64 KiB field operations trail it
  by roughly 2–3× and 1 MiB structural `JSON.MERGE` is roughly 72× slower
  because it rebuilds the document. Prioritize representative fallback and
  structural-edit workloads, transient peak RSS, and longer replication and
  slow-replica retention trials before closing this acceptance item.
  Remaining work, ordered by the measured gaps (the benchmark above is the
  baseline, not a universal RedisJSON compatibility or performance target):
  - [ ] **P0 — Profile and improve mid-size field operations.** For the tested
    64 KiB shape, GET/SET/MERGE reach ~196k/~135k/~139k requests/s versus
    RedisJSON ~435k/~417k/~455k. Profile compact path scanning and copying;
    evaluate a measured, shape-aware index or patch threshold rather than
    enabling indexes for every JSON value or lowering 256 KiB unconditionally.
    Re-run paired throughput and p99 on 1 KiB, 64 KiB, threshold-adjacent,
    1 MiB, and wide/array-heavy documents. Preserve small-document wins,
    correct path semantics, and no JSON overhead on ordinary keys.
  - [ ] **P0 — Make structural edits cheaper without regressing scalar paths.**
    The 1 MiB nested `JSON.MERGE` runs ~5.6k/s versus RedisJSON ~400k/s
    because it rebuilds the body; optimize affected subtrees/spans where
    semantics permit instead of assuming every merge can be a scalar patch.
    Include root and nested merge, deletion via RFC 7396 null, missing paths,
    arrays, repeated edits beyond the 64-patch cap, and fallback documents.
    Compare throughput, p99, root reads, and peak RSS against this baseline.
    Keep the existing atomic, replayable path-result WAL and replica behavior.
  - [ ] **P0 — Validate the hybrid boundary and mixed workloads.** Exercise
    eligible indexed documents alongside tree fallback, small JSON, and
    non-JSON GET/SET and other hot commands, both with and without JSON data;
    measure throughput, tail latency, steady and transient RSS, and the cost
    of patch compaction and repeated structural edits. Include many keys,
    multiple shards, large payloads, TTL/eviction, transactions, and restart.
    Do not treat the 20,000-key RSS sample or a single hot document as a
    general memory/performance guarantee.
  - [ ] **P1 — Extend durability and replica acceptance.** Run longer
    `everysec`/`always` WAL and snapshot/restart trials with mixed JSON writes,
    structural edits, and a writer that continues during catch-up; compare
    primary and replica content, sequence, TTL, and recovery after interruption.
    Preserve fail-closed loading and the explicit incomplete-promotion guard.
    Measure prolonged slow-replica WAL retention and disk use, then define and
    test a bounded retention/backpressure/resync policy before promising a
    disk bound; never silently promote an incomplete replica.
- [ ] **Selected JSON field indexes.** Each explicitly named index selects a
  binary key prefix and one simple JSON path. The first version supports exact
  equality of scalar JSON values only (no ranges, wildcard paths, or implicit
  indexing). Queries return bounded pages with a continuation cursor, not
  unlimited result sets. They must aggregate results across shards; a
  shard-local result alone must not be presented as a complete result. Persist
  index definitions, rebuild derived postings after WAL/snapshot restoration, and
  update them for changes, expiry, eviction, overwrite, delete, and rename.
  Unindexed keys must have no per-key index/metadata overhead.
- [ ] **Full-text search.** Build on the opt-in indexing lifecycle rather
  than scanning all documents or automatically indexing every string.
  Specify tokenization and consistency before adding commands.

## Immediate — first-party client APIs

- [x] **P1 — Single-key overloads for multi-key commands.**
  Redis allows `DEL key` and `BLPOP key timeout` without wrapping the key in a
  collection. First-party clients must do the same. Multi-key forms stay for
  `DEL a b` and multi-list blocking pops.
  - C# `DeleteAsync(string)` (list form remains), `PopListLeftBlockingAsync` /
    `PopListRightBlockingAsync` one-key overloads. `UnlinkAsync` / `ExistsAsync`
    / `WatchAsync` / `GetManyAsync` already take `params`.
  - Rust `del_key`, `unlink_key`, `exists_key`, `watch_key`, `blpop_key`,
    `brpop_key` next to the existing `&[&str]` methods.
  Shipped in `Ruvio.Client` / `Ruvio.Client.AspNetCore` 0.2.4 and `ruvio-client`
  0.2.2.
- [x] **P1 — Client-owned reconnect.** A dropped command or shard socket fails
  the in-flight command and is opened again before the next one. Pub/Sub
  channels are subscribed again. Retry policy and lost/restored events are
  on both SDKs. ASP.NET Core binds the policy and can attach the events.
  Shipped in `Ruvio.Client` / `Ruvio.Client.AspNetCore` 0.2.6 and `ruvio-client`
  0.2.4.

## Status Legend

- `[x]` implemented and covered by the current test surface
- `[ ]` TODO
- `P0` required before a production preview
- `P1` required before a stable standalone release
- `P2` required for a production cluster
- `P3` later work that does not gate standalone or cluster

## Current Baseline

- [x] Single-owner, lock-free `KvShard`
- [x] Evented Tokio runtime on Linux, macOS and Windows with one owner executor per shard; the portable-hop and Glommio adapters were removed
- [x] Borrowed RESP2 parsing for `PING`, `GET`, `SET`, `INCR`, and `DECR`
- [x] Redis-compatible hash slots, hash tags, `MOVED`, and `CROSSSLOT`
- [x] Native bytes, list, set, sorted-set, and Bloom-filter values
- [x] Bounded RESP argument, bulk-value, and connection-buffer sizes
- [x] Cross-platform CI and Linux cluster smoke tests
- [x] Development benchmarks against Redis

## Product Decisions to Lock First

- [x] **P0 — Define supported deployment modes.**
  - Standalone, single process with one or more local shards.
  - Clustered, multiple processes with explicit membership.
  - Document which mode is supported at every release stage.
- [x] **P0 — Define durability modes and guarantees.**
  - `memory`: no recovery guarantee.
  - `snapshot`: recover the latest completed periodic checkpoint.
  - `everysec`: acknowledge after append; synchronize dirty logs at least once
    per second during normal operation.
  - `always`: acknowledge only after the relevant record is durable.
  - Define exactly what can be lost after process, OS, or power failure.
- [x] **P0 — Write down the format and versioning rules.**
  - Persistence format and compatibility policy.
  - Snapshot algorithm and consistency boundary.
  - Per-shard log ordering and global checkpoint semantics.
  - Clock and expiration semantics across restart.
- [x] **P0 — Establish a compatibility contract.**
  - Publish the supported Redis protocol version and command subset.
  - Separate intentional incompatibilities from bugs.
  - Do not claim Redis Cluster compatibility until its required semantics exist.

Contract:

- [Redis Compatibility Contract](COMPATIBILITY.md)

## Milestone 0 — Performance-Safe Foundations

This milestone prevents persistence and command growth from hardening prototype
ownership and allocation patterns. macOS/Docker measurements are acceptable for
functional tests and same-machine regression comparisons. Bare-metal Linux
profiling remains a release gate and is not replaced by Docker numbers.

- [x] **P0 — Add a canonical mutation boundary with
  validate-record-apply ordering.**
  - Sink failure must leave shard state unchanged.
  - WAL and replication consume validated mutations, not RESP frames.
  - Increment records the resulting integer state, not arithmetic to rerun.
- [x] **P0 — Move every command to a borrow-first command representation.**
  - Linux fallback parsing borrows payloads and allocates only argument
    descriptors.
  - Portable commands use the same representation with owned payloads because
    they cross the shard channel.
  - Copy only data that becomes shard-owned.
- [x] **P0 — Encode reads directly from shard storage into reusable response
  buffers.**
  - Remove owned `GET` and collection-response copies from fallback execution.
  - Preserve batched pipeline writes.
- [x] **P0 — Replace runtime-only expiration metadata with a persistable time
  model and injectable clock.**
- [x] **P0 — Add per-mutation memory deltas and shard-local memory accounting.**
- [x] **P0 — Keep the storage layout replaceable without dynamic dispatch in
  the hot path.**
- [x] **P0 — Add repeatable Mac/Docker regression workloads with stored
  environment metadata and no cross-machine performance claims.**

## Milestone 1 — Safe Standalone Core

### Protocol and Command Safety

- [x] **P0 — Add configurable maximum key, value, command, pipeline, and
  connection-buffer sizes.**
- [x] **P0 — Enforce limits consistently in the hot and fallback parsers.**
- [x] **P0 — Add per-connection idle, read, and write timeouts.**
- [x] **P0 — Add global and per-IP connection limits.**
- [x] **P0 — Add request admission control and explicit overload responses.**
- [x] **P0 — Add output-buffer limits for slow clients.**
- [x] **P0 — Add graceful shutdown: stop accepting, drain within a deadline,
  flush durable state, and close listeners.**
- [x] **P0 — Make malformed-frame errors deterministic without panics or
  unbounded allocations.**
- [x] **P1 — Add RESP3 negotiation and responses where they materially improve
  client compatibility.** `HELLO 2`/`HELLO 3` negotiate per connection. RESP3
  uses null, bool, double, map, and set replies. `HELLO AUTH`/`SETNAME` and
  `CLIENT GETNAME`/`SETNAME`/`TRACKING ON|OFF` are in. Tracking is shard-local
  `GET`/`MGET` and one `invalidate` push. Attributes stay unsupported. RESP2
  bytes are unchanged.

### Configuration and Operations

- [x] **P0 — Replace environment-only configuration with a typed configuration
  layer supporting file, environment, and CLI overrides.**
- [x] **P0 — Validate incompatible settings before opening sockets or files.**
- [x] **P0 — Add structured logging with levels and connection/shard context.**
- [x] **P0 — Expose health, readiness, build version, backend, and durability
  state.**
- [x] **P0 — Expose shard-local metrics on the evented runtime.**
- [x] **P1 — Export metrics in a standard scrape format without adding
  cross-core contention to the hot path.**
  `INFO prometheus` and optional per-shard `/metrics` use shard-local
  counters. Scrapes are not aggregated in-process.
- [x] **P1 — Add configuration introspection with secret redaction.**
  `CONFIG GET` returns the effective settings, supports `*`/`?` globs, and
  never emits `requirepass` in the clear.

### Testing Foundation

- [x] **P0 — Fuzz RESP hot and fallback parsers, including arbitrary frame
  truncation and oversized declarations.**
- [x] **P0 — Add property tests for every data type and wrong-type transition.**
- [x] **P0 — Add differential command tests against Redis for the supported
  command subset.**
- [x] **P0 — Run protocol and storage conformance tests on the evented
  runtime.** The earlier portable/Glommio comparison is historical; both
  adapters were removed.
- [x] **P0 — Add loom or equivalent concurrency-model tests around queues,
  shutdown, and durability handoff boundaries.**
- [x] **P1 — Add long-running soak tests with churn, pipelines, reconnects, and
  slow clients.**
  `scripts/soak.py --seconds N` repeats that mix against a running server.
  The 24-hour hardware gate stays open.
- [x] **P1 — Add sanitizer and Miri jobs where supported.**
  Linux nightly `asan` and `miri` CI jobs. Miri covers the unsafe-backed tables and the protocol units
  (`--no-default-features` drops mimalloc); persistence, sockets, and Lua FFI
  stay out of scope. Local: `scripts/run_sanitizers.sh`.

## Milestone 2 — Persistence and Recovery

Persistence must preserve the shared-nothing model. Each shard owns its mutation
order and durability stream; background workers may perform I/O, but they must
not mutate shard state or reorder acknowledged writes.

### Durable Log

- [x] **P0 — Define a versioned binary write-ahead log (WAL) record format.**
  - Magic/version, shard ID, monotonic sequence number, operation, payload
    lengths, and checksum.
  - Binary-safe encoding for every value type and mutation.
  - Forward-compatible record framing and explicit unsupported-version errors.
- [x] **P0 — Implement per-shard WAL append in mutation order.**
- [x] **P0 — Implement `memory`, `everysec`, and `always` durability policies.**
- [x] **P0 — Ensure replies obey the selected acknowledgement boundary.**
- [x] **P0 — Detect and safely truncate a torn or partial final WAL record.**
- [x] **P0 — Reject checksum corruption before applying the affected record.**
- [x] **P0 — Recover all supported data types and metadata by replaying WAL.**
- [x] **P0 — Restore expiration using absolute wall-clock deadlines while
  guarding against clock anomalies.**
- [x] **P0 — Make WAL directory, segment size, sync policy, and disk-full
  behavior configurable.**
- [x] **P0 — Enter a safe, visible state on write, sync, or disk-space failure;
  never acknowledge a write that violates the selected durability contract.**
- [x] **P1 — Rotate WAL segments and delete only segments covered by a durable
  checkpoint.**
- [x] **P1 — Add a read-only recovery/inspection CLI for WAL files.**
  `ruvio-ctl check` and `inspect` replay snapshot plus WAL without truncating
  torn tails.
- [ ] **P1 — Prototype bounded, sequence-based group commit without blocking
  the shard executor on disk synchronization.**
  - Motivation: standalone native LIMIT/IDEM now share the hot-write durability
    barrier, but `always` still synchronizes on the shard runtime thread. The
    remaining concurrent GET/SET gap is not claimed fixed. Establish the
    disk-wait contribution before committing to this architectural change.
  - Preserve per-shard mutation ordering and the central execution-path/session
    grouping eligibility rule. Do not infer eligibility from a write flag;
    review blocking, script rollback, transaction and administrative boundaries.
  - Prototype behind an opt-in configuration with the current implementation
    remaining the default. Initial sync-only prototype:
    `sync_worker` (see CONFIGURATION.md). It offloads grouped sync
    waits only; append, fallback and rotation remain synchronous. This backlog
    item stays unchecked until the broader safety and measurement gates pass.
    Short repeated local/Linux evidence is recorded in
    [the scenario results](SCENARIO_LAB_RESULTS.md#experimental-sync-worker-isolated-repeated-linux-probe):
    Linux concurrent throughput/tails improved, while macOS p99 regressed.
    Defaults remain unchanged; full client/API and cross-platform gates are open.
  - Use a long-lived persistence worker with explicit file/buffer ownership,
    reusable buffers and bounded handoff. Do not spawn a thread, task, timer or
    allocate a completion object per command. Determine worker count from
    measurements rather than automatically adding one worker per connection.
  - Publish a monotonic durable sequence only after successful synchronization.
    Pending response batches carry their required sequence; release them only
    when it is durable. Reads observing unsynchronized mutations must obey the
    same observed-write durability contract, not only mutation replies.
  - At low load, submit promptly without an artificial batching delay. Under
    load, coalesce ready writes naturally while I/O is in progress. Bound bytes,
    batches and outstanding acknowledgements; apply explicit backpressure when
    disk cannot keep up, including slow-client/output-buffer limits.
  - Integrate append-before-apply safety, segment rotation, checkpoints,
    replication durable offsets, expiry/eviction and database isolation. Never
    hold a shard borrow or persistence mutex across a worker wait. Specify what
    remains shard-owned versus worker-owned; offloading fsync alone must not
    leave append blocked by a worker-held lock.
  - Surface write/sync/worker failures and fail pending acknowledgements without
    success-shaped replies or ambiguous write replay. Define disconnect,
    cancellation and graceful shutdown/drain behavior with bounded deadlines.
  - Add concurrency-model tests for publication/wakeup races and fault-injection
    tests for partial writes, sync failure, disk full, rotation, worker failure,
    shutdown and crash/restart. Cover write-then-read visibility, scripts,
    transactions and native commands across both runtime backends.
  - Measure against the unchanged default on the same Linux host: one and many
    clients, pipelines, GET/SET/INCR/DEL, native IDEM/LIMIT and reviewed fallback
    commands; test `memory`, `everysec` and `always` separately. Record p50/p95/p99,
    achieved throughput, fsync/batch sizes, queue bytes, allocations, CPU, RSS
    and GC where clients apply. Include a fresh Redis AOF-always reference.
  - Acceptance: preserve durability/error behavior, bounded memory and the
    memory-mode hot path; demonstrate repeatable concurrent durable gains with
    no material single-client regression. If gains do not justify complexity,
    keep the default and do not promote the prototype. Set numeric regression
    budgets from baseline measurements before implementation.

### Snapshots and Checkpoints

- [x] **P0 — Define a versioned snapshot format for all value types, TTL
  metadata, shard identity, and last included WAL sequence.**
- [x] **P0 — Implement startup from the latest valid snapshot plus subsequent
  WAL replay.**
- [x] **P0 — Create point-in-time-consistent snapshots without a long shard
  pause.**
- [x] **P0 — Bound copy-on-write or snapshot buffering memory under sustained
  writes.**
- [x] **P0 — Write snapshots atomically: temporary file, checksum, sync, rename,
  and parent-directory sync where required.**
- [x] **P0 — Keep the previous valid checkpoint until the replacement is fully
  durable.**
- [x] **P1 — Add manual `SAVE`-style and asynchronous checkpoint commands.**
- [x] **P1 — Add scheduled checkpoints based on time and mutation volume.**
- [x] **P1 — Compact WAL after a successful checkpoint without blocking the hot
  path.**
- [x] **P1 — Add snapshot/WAL compatibility fixtures for every released format
  version.**
  Frozen hex files in `tests/fixtures/persistence/v1/` cover the WAL header,
  every mutation type, `CURRENT`/`CLOCK`/`MANIFEST`, and empty plus
  all-types snapshots. Encode must match; decode must apply. Major `2` is
  rejected.

### Crash and Data-Integrity Validation

- [x] **P0 — Build a crash harness that kills the process at randomized points
  during append, sync, rotation, snapshot, and rename.**
  `killed_process_recovers_at_crash_sites` aborts at a randomized hit of each
  site and reopens the directory. A truncated header on the final WAL segment
  is discarded. This is process death, not power loss.
- [x] **P0 — Verify acknowledged-write recovery for each durability mode.**
- [x] **P0 — Test full disk, permission changes, short writes, corrupted
  records, missing segments, and truncated snapshots.**
  Corrupted records, missing WAL segments, truncated CURRENT, missing
  manifests, and oversize snapshots are tested. A thread-local fault writes
  half a record and then returns `ENOSPC`; the sink stays failed and recovery
  keeps the acked prefix. A read-only WAL directory blocks segment rotation.
  This is an injected full disk, not a filled volume.
- [x] **P0 — Test restart with expired and near-expiry keys.**
- [x] **P1 — Add backup, restore, and integrity-check tooling.**
  `ruvio-ctl backup` and `restore` copy `CURRENT`/`CLOCK`/checkpoints/WAL,
  refuse a live lock, and re-check the copy.
- [x] **P1 — Document recovery-point and recovery-time expectations.**
  See `docs/RECOVERY.md`. Numerical RTO remains a hardware soak gate.

## Milestone 3 — Memory Lifecycle and Cache Semantics

### Expiration

- [x] **P0 — Implement `EXPIRE`, `PEXPIRE`, `EXPIREAT`, `PEXPIREAT`, `TTL`,
  `PTTL`, and `PERSIST`.**
- [x] **P0 — Treat expired keys as absent in every command path.**
- [x] **P0 — Add passive expiration on lookup.**
- [x] **P0 — Add bounded active expiration per shard without latency spikes.**
- [x] **P0 — Persist and recover expiration deadlines.**
- [x] **P1 — Add `SET` expiry options (`EX`, `PX`, `EXAT`, `PXAT`, `KEEPTTL`).**
- [x] **P1 — Add expiration metrics and deterministic tests with an injectable
  clock.**

### Memory Limits and Eviction

- [x] **P0 — Track owned, pooled, index, collection, connection, and persistence
  buffer memory per shard.**
- [x] **P0 — Add process and per-shard memory limits with reserved headroom.**
- [x] **P0 — Implement `noeviction` with deterministic out-of-memory errors.**
- [x] **P1 — Implement and benchmark `allkeys-lru`, `allkeys-lfu`,
  `volatile-lru`, and `volatile-ttl` policies.**
  Shard-local sampled eviction (16 candidates, 16 deletes per command).
  Recency/frequency are not persisted; recovery treats all keys as cold.
  Policy selection is unit-tested. Hardware eviction throughput is not a
  software gate.
- [x] **P1 — Ensure eviction is shard-local, bounded per event loop turn, and
  reflected in WAL/snapshots.**
  Evictions persist as `Delete` records. Snapshots omit evicted keys.
- [x] **P1 — Add fragmentation, allocation-rate, pool-retention, and eviction
  metrics.**
  `ruvio_evicted_keys_total`, `ruvio_allocation_events_total`,
  `ruvio_allocation_bytes_total`. Pool retention remains
  `ruvio_pooled_value_bytes`.
- [x] **P1 — Define behavior for a single value larger than the shard budget.**
  A write whose admission charge exceeds the shard budget is rejected with
  `OOM` and does not evict.
- [ ] **P0 — Bring steady-state string RSS into the same band as Redis for the
  same keyspace.**
  The Cherry soak of 2026-09-28 has not been repeated. The local Docker probe
  since then is inside the band; the soak is not. Both sets of numbers and the
  analysis are in `docs/MEMORY_DENSITY.md`. The long-running cache claim stays
  blocked until the soak key mix is measured again.
- [ ] **P1 — Next cycle is throughput, then only the memory still outside the
  band.**
  Do not start another RSS rewrite before a `SET`/`GET`/`INCR` pass on this
  layout. A first native, single-shard, memory-only pass on 2026-10-07
  (`python3 scripts/redis_compare.py --tests set,get,incr --rounds 3
  --memory-keys 100000`) measured Ruvio medians of 2.54M SET, 2.72M GET,
  3.19M INCR/s versus local Redis 2.07M/2.28M/2.26M; 100k distinct
  64-byte strings added 8.7 MiB versus 13.4 MiB RSS. Samples varied
  substantially, and the benchmark uses a small fixed keyspace for commands;
  this is a **baseline**, not evidence of a new optimization or a reason to
  close the separate long-running soak gap. A follow-up paired LRU read probe
  with 100k distinct keys and concurrent random GET/SET PX churn found a
  small improvement after eliminating a redundant live-key lookup; its
  measurements and limits are in `docs/BENCHMARK_RESULTS.md`.
  Broader key/value distributions, durability modes and longer runs still
  need profiling. A packed record log stays a later option if the repeated
  soak is still above about 1.5× Redis. Docker Desktop numbers are not that soak.

## Milestone 4 — Redis Compatibility for Real Workloads

Implement commands by workload need and semantic test coverage, not raw command
count.

- [x] **P1 — Complete string semantics:** `MGET`, `MSET`,
  `GETSET`, `APPEND`, `STRLEN`, `INCRBY`, and `DECRBY`.
  `SET NX`/`XX`, `GET`, and expiry options are implemented. `SET GET` returns the previous string.
- [x] **P1 — Complete keyspace semantics:** `EXISTS`, `TYPE`, `UNLINK`,
  `RENAME`, `SCAN`, and database-size introspection.
  `UNLINK` is synchronous. `SCAN`/`DBSIZE` inspect the receiving shard.
- [x] **P1 — Complete lists:** `RPUSH`, `RPOP`, lengths, indexing, trimming,
  and blocking operations with bounded waiter management.
  `BLPOP`/`BRPOP` waiters are shard-local and capped at 1,024.
- [x] **P1 — Complete sets:** cardinality, enumeration, and same-slot set
  operations.
  `SCARD`, `SMEMBERS`, `SINTER`/`SUNION`/`SDIFF` plus `STORE` variants,
  `SMOVE`, and `SPOP`. Members are returned in sorted order. Algebra is same-slot.
  `SPOP` records the removed members so recovery does not draw again.
- [x] **P1 — Add native hashes with compact small-value representation.**
  `HSET`/`HGET`/`HDEL`/`HLEN`/`HGETALL`/`HMGET`/`HEXISTS`/`HKEYS`/`HVALS`/`HINCRBY`.
  Small hashes stay compact; larger field counts or values promote to a table.
  Enumeration order is unspecified, as in Redis.
- [x] **P1 — Complete sorted sets:** multiple pairs, `WITHSCORES`, rank, score
  ranges, removal, increments, and cardinality.
  `ZADD` multi-pair, `ZRANGE`/`ZREVRANGE` `WITHSCORES`, `ZRANGEBYSCORE`,
  `ZREM`, `ZINCRBY`, `ZCARD`, `ZSCORE`, `ZRANK`, and `ZREVRANK`.
  `ZINCRBY` records the resulting score. `ZADD` accepts `NX`, `XX`, `GT`, `LT`, `CH`, and `INCR`.
  `ZRANGE` accepts `BYSCORE`, `BYLEX`, `REV`, and `LIMIT`.
- [x] **P1 — Define and test Redis transaction semantics (`MULTI`, `EXEC`,
  `DISCARD`, and `WATCH`) within one shard.**
  Same-slot only. `EXEC` is one atomic WAL `Transaction` record (type 23).
  Dirty `WATCH` returns a null array. Persist failure rolls back in-memory
  keys. Nested `MULTI`, `WATCH` inside `MULTI`, and blocking/control commands
  inside `MULTI` are errors. Scripts and cross-shard 2PC are not.
- [x] **P1 — Implement script/function support only after resource limits,
  determinism, and durability semantics are designed.**
  `EVAL`/`EVALSHA`/`SCRIPT LOAD`/`EXISTS`/`FLUSH`/`KILL`, plus memory-only
  `FUNCTION LOAD`/`LIST`/`DELETE` and `FCALL`. Lua 5.1 sandbox with
  call and instruction quotas. Writes persist as one `Transaction` WAL batch;
  recovery never runs Lua. Function bodies are not durable. Same-slot declared keys only. Not queueable inside
  `MULTI`. Redis library shebangs are not.
- [x] **P1 — Maintain a machine-readable compatibility matrix and run
  first-party Rust and C# client tests.**
  `docs/compatibility-matrix.json` is checked against the parser,
  `COMPATIBILITY.md`, and coverage surfaces. `ruvio-client` and
  `Ruvio.Client` tests run in CI.
- [ ] **P3 — Evaluate Pub/Sub and Streams as separate product milestones.**
  Sharded Pub/Sub is in: `SPUBLISH`, `SSUBSCRIBE`, and `SUNSUBSCRIBE` stay on
  the channel's slot owner and are not durable. Exact keyspace notices for
  `SET`, `DEL`, expire, and eviction ride that queue when
  `notify-keyspace-events` is on and a client is subscribed. Classic
  `PUBLISH` / `SUBSCRIBE` and `PSUBSCRIBE` stay on the shard that accepted
  the connection. They are not broadcast and not durable. A sync delivery
  wait is not implemented. A narrow stream slice is in:
  `XADD`, `XLEN`, `XRANGE`, `XDEL`, and `XTRIM`, with the assigned id stored
  in the WAL. `MAXLEN ~` and `MINID ~` are exact. The approximate trim option `LIMIT` is not accepted.
  `XREAD BLOCK` waits on the shard.
  `XGROUP CREATE`, `XGROUP CREATECONSUMER`, `XGROUP DESTROY`, `XGROUP SETID`,
  `XGROUP DELCONSUMER`,
  `XREADGROUP` with `>`, `NOACK`, `XACK`, `XPENDING`, and `XCLAIM`
  keep the group cursor in the WAL and in snapshots. `XPENDING` can list
  the pending rows. `XCLAIM` accepts `IDLE`, `TIME`, `RETRYCOUNT`, `FORCE`,
  `JUSTID`, and `LASTID`.
  `XREADGROUP BLOCK` waits for a later `XADD` and delivers that id.
  `XINFO STREAM`, `XINFO GROUPS`, and `XINFO CONSUMERS` read that stored
  stream. They do not add a counter on `XADD`. `XSETID` moves the last
  generated id forward without storing an entry. `XADD DELAY` keeps the
  entry out of `XREADGROUP` until the delay passes, and stores that hold
  in the WAL and in snapshots. `LEASE` / `RELEASE` and `SETV` keep their
  token or version only on keys that use them. `CHANGES START` arms an
  in-memory note ring on that shard; nothing is recorded before it, and
  the ring is empty after restart.

## Socket I/O stays on the shard thread

Moving `recv`/`send` of client sockets to separate threads (Redis-style `io-threads`) measured slower everywhere: on Linux
(4 cores, EPYC 7763) 0.57-0.85x at 50/200 clients and pipeline 16, and +8-16 us per request for a single client; on macOS the
same. The two cross-thread hand-offs per request (queue wake-up, buffer allocation and copy) cost about what the saved system
calls cost, so there is no `io_threads` setting. A new attempt would have to beat 1.3x on a `perf`-profiled shard thread.

## Milestone 5 — Storage Engine Performance

Correctness, persistence, and reproducible profiles must precede structural
optimization.

- [ ] **P0 — Establish repeatable bare-metal Linux baselines with isolated
  server and load-generator CPUs.**
  Blocked: no dedicated bare-metal host in CI. Methodology lives in
  `docs/BENCHMARKING.md`; container results are not release evidence.
- [ ] **P0 — Record throughput, p50/p95/p99/p99.9, CPU, RSS, allocations, cache
  misses, context switches, and saturation behavior.**
  Blocked on the same bare-metal host. The regression harness records
  throughput and selected percentiles on whatever machine it runs.
- [x] **P1 — Evented shared-nothing runtime (Redis shard ownership +
  Garnet-style no-hop I/O).**
  The process is Tokio `current_thread` on the shard owner. Portable and
  `io_uring` were removed.
  The Docker Desktop Linux-VM sample (2026-09-22) is in
  `docs/BENCHMARK_RESULTS.md`. That is not Redis parity and not a bare-metal baseline.
- [ ] **P1 — Profile realistic key/value distributions, churn, TTL, and mixed
  commands, not only fixed-size GET/SET.**
- [ ] **P1 — Evaluate shard-local open-addressing indexes against `AHashMap`.**
  Do this after the string entry is small. Packing today's `Entry` tighter
  does not remove the `Value` enum or the extra heap blocks. See
  `docs/MEMORY_DENSITY.md`.
- [ ] **P1 — Evaluate arenas/compact handles while preserving deletion and
  snapshot correctness.**
- [ ] **P1 — Extend bounded pooling to collection elements only when profiles
  justify the complexity.**
- [ ] **P1 — Remove avoidable Bloom-filter and sorted-set hot-path allocations.**
- [ ] **P1 — Integrate SIMD only into measured production bottlenecks with
  scalar and architecture-specific conformance tests.**
- [ ] **P1 — Evaluate registered/fixed buffers, batching, and queue depths on
  supported Linux kernels.**
- [ ] **P1 — Define performance regression budgets and enforce stable
  microbenchmarks in CI without relying on noisy absolute throughput.**
- [ ] **P3 — Evaluate a custom binary protocol separately from Redis
  compatibility; do not complicate RESP paths without a concrete use case.**

### Windows

Windows, macOS, and Linux run this same process. Windows I/O is IOCP through Tokio. There is no second adapter to design.

## Milestone 6 — Security and Multi-Tenancy Basics

- [x] **P0 — Add secure default binding and explicit warnings for public
  unauthenticated listeners.**
- [x] **P0 — Add authentication with constant-time secret verification and
  redacted logs/configuration.**
- [x] **P1 — Add TLS with certificate reload and documented client behavior.**
- [x] **P1 — Add Redis-compatible ACL concepts with command and key-pattern
  restrictions.**
- [x] **P1 — Add per-user/client quotas and rate limits.**
- [x] **P1 — Threat-model parser, persistence files, administrative commands,
  cluster messages, and denial-of-service paths.**
  See `docs/THREAT_MODEL.md`.
- [x] **P1 — Add dependency auditing, supply-chain policy, SBOM generation, and
  a private vulnerability-reporting process.**
  `deny.toml` plus `cargo deny check` in CI, CycloneDX from
  `scripts/generate_sbom.py`, and `SECURITY.md` private GitHub advisories.

### Exposure hardening

Garnet separates client auth (`--auth` Password, ACL, or Aad) from node-to-node
credentials (`--cluster-password`) and can require TLS client certificates.
Ruvio keeps the same split without taking an identity-provider dependency yet.

- [x] **P1 — Refuse a public listener with protected mode off and no
  authentication.** `RUVIO_ALLOW_UNAUTHENTICATED=true` is the explicit
  exception for a private network. Container images need `RUVIO_REQUIREPASS`.
- [x] **P1 — Require a bearer token on a public metrics listener.**
  `RUVIO_METRICS_TOKEN`. Loopback scrape stays open when the token is empty.
- [x] **P1 — Use a replication handshake password distinct from `requirepass`.**
  `RUVIO_REPLICATION_PASSWORD`. Empty falls back to `requirepass`. A public
  replication listen needs one of them.
- [x] **P1 — Load and save ACL users from a file.** `security.acl_file` /
  `RUVIO_ACL_FILE`. `ACL SAVE` writes the live directory. `ACL LOAD` replaces
  it from that file. `GET` and `SET` do not open the file. `ACL LIST` still
  redacts passwords. An open connection keeps the rules from its last `AUTH`.
- [x] **P1 — Optional TLS client certificates.** `tls_client_ca` /
  `RUVIO_TLS_CLIENT_CA` / `--tls-client-ca` is the issuer. `tls_client_auth` /
  `RUVIO_TLS_CLIENT_AUTH` / `--tls-client-auth` requires a certificate at
  handshake. Plaintext `GET` and `SET` do not read it.
- [ ] **P3 — Identity-provider authentication.** Garnet `Aad` and `AclWithAad`.
  After stable standalone. Do not block the RESP password and ACL path on it.
- [x] **P1 — Sign backup directories.** `ruvio-ctl backup` writes `SIGNATURE`
  with the signer and a blake3 digest of every copied file. `--sign-key` or
  `RUVIO_BACKUP_KEY` adds a keyed digest so a rewritten checksum cannot replace
  the tree. `check` and `restore` verify it. The server never reads the file.
- [ ] **P3 — Encrypt the data directory at rest.** This is a data-directory format change.
  Host permissions remain the control until then.
- [ ] **P2 — Authenticate a future cluster bus before it exists.** Garnet
  `--cluster-username` and `--cluster-password`. Design the identity with the
  bus, not after it.

## Milestone 7 — Production Cluster

Do not build automatic failover before standalone durability is proven.

### Membership and Routing

- [ ] **P2 — Define authenticated internal cluster protocol and stable node
  identities.**
- [ ] **P2 — Add cluster membership, epochs, topology persistence, and
  convergence rules.**
- [ ] **P2 — Implement complete `CLUSTER SLOTS`/`NODES` semantics and a real
  cluster bus endpoint.**
- [ ] **P2 — Add online slot migration with importing/migrating states and
  bounded transfer queues.**
- [ ] **P2 — Add hot-key and skew observability before automated rebalancing.**
- [ ] **P2 — Define cross-slot command behavior; do not hide distributed
  coordination behind local-looking semantics.**

### Replication and Failover

- [x] **P2 — Stream ordered per-shard WAL to replicas with offsets and
  checksums.** Asynchronous primary/replica roles; no automatic failover.
- [x] **P2 — Recover a covered WAL gap from a checkpoint plus WAL catch-up.**
  This is bounded snapshot resynchronization, not cluster membership or
  automatic replacement of a failed primary. A gap without a usable checkpoint
  remains a gap.
- [ ] **P2 — Establish full cluster synchronization and node replacement.**
  Covered-gap checkpoint transfer is not an unconditional full-sync guarantee
  for any replica state or a way to join/migrate cluster members online.
- [ ] **P2 — Add complete replication lag, backlog, reconnect, and disk-pressure
  controls.** Per-shard send lag, a disk WAL backlog and reconnect exist; the
  remaining operational controls and failure evidence are not complete.
- [ ] **P2 — Define consistency and acknowledgement policies for asynchronous
  and quorum-backed writes.**
- [ ] **P2 — Add failure detection and leader/primary election with a documented
  split-brain model.**
- [ ] **P2 — Prove failover with deterministic fault injection, partitions,
  delayed packets, process crashes, and disk loss.**
- [ ] **P2 — Add rolling-upgrade and mixed-version compatibility tests.**

## Milestone 8 — Packaging and Release Readiness

- [x] **P0 — Run containers as non-root with read-only root filesystem support,
  explicit data directories, and signal-correct shutdown.**
- [x] **P0 — Publish reproducible binaries and container images with checksums
  and provenance.**
- [x] **P0 — Document supported OS, architecture, kernel, and filesystem
  combinations.**
- [x] **P0 — Add startup checks for writable storage, free disk space, file
  descriptor limits, and CPU placement.**
- [x] **P1 — Add systemd/Kubernetes examples, health probes, resource sizing,
  backup, restore, and upgrade runbooks.**
  `deploy/ruvio.service`, `deploy/ruvio.toml`, and `deploy/kubernetes.yaml`.
  Backup, restore, and upgrade steps are in `docs/RECOVERY.md`. The RESP port
  opens after recovery, so a TCP probe is readiness. `GET /metrics` is scrape
  only.
- [x] **P1 — Define semantic versioning and persisted-format compatibility
  guarantees.**
  Package rules are in `docs/COMPATIBILITY.md`. Format major/minor rules are
  in `docs/COMPATIBILITY.md` (Package Versions).
- [ ] **P1 — Publish benchmark methodology and repeated bare-metal results.**
- [ ] **P1 — Define beta/stable release gates, incident policy, and support
  boundaries.**
- [ ] **P1 — Publish a GitHub Pages site for Ruvio.**
  A public site that explains the store in detail: features, configuration
  parameters, durability and replication, and worked examples. The site
  follows `docs/CONFIGURATION.md`, `docs/COMPATIBILITY.md`, and the command
  tables. It is not a second source of truth.

## Milestone 9 — AI Prefix Cache

This waits until the stable standalone gates are met. It is not a vector
database and it does not add a query language. Exact prompt, tool, and
embedding caches stay `SET` / `GET` with TTL.

Inference repeats the same system prompt, tool schema, and document prefix.
Those blocks are opaque bytes addressed by their content hash. A block that
an in-flight request still holds must survive eviction. This is the remote
tier consulted at the start of a request, not the GPU-resident cache.

- [ ] **P3 — Store a prefix block under its content hash.** The value is
  opaque bytes on the shard that owns the hash. Memory limits and TTL apply.
  `memory` is the hot tier. `everysec` keeps hot prefixes across a restart.
- [ ] **P3 — Pin a block for the life of a request.** Eviction skips a block
  whose pin count is above zero. Unpin when the request finishes.
- [ ] **P3 — Split a block that exceeds the configured value limit.** The
  parts stay one logical prefix. A partial read is not a hit.

## Milestone 10 — Ruvio UI

This waits until the stable standalone gates are met. RedisInsight is an
Electron shell around a private client. Ruvio UI is one static binary that
speaks RESP with `ruvio-client` and serves a browser UI. The same binary is
the container entrypoint and the downloadable executable. The browser, or the
operating-system web view, draws the window. The process does not ship a
second Chromium.

The C# client stays for application code. It is not the UI process. A
self-contained .NET publish is larger than a static Rust binary and would
split the UI from the server repository.

The working tree has this in `clients/ruvio-ui`. The browser talks to that
process over HTTP. The process uses `ruvio-client`, including `execute`, so
the screen can send every command the caller's ACL allows. Sign-in is that
ACL user, not a separate portal account. All shards walks `SCAN` across the
slot map; one selected shard pins every screen to that node.

- [x] **P3 — Ship `ruvio-ui` as one binary.** It links `ruvio-client`, binds a
  local HTTP port, and serves the static UI. The local Docker image and the
  host executable are that binary. No Electron, no second runtime.
- [x] **P3 — Build the screens in Svelte.** Key browser, command forms for the
  supported types, shard selection, Users (`ACL`), and Pub/Sub. The compiled
  assets are embedded in the binary.
- [x] **P3 — Publish the UI image.** `salihcantekin/ruvio-ui` tags `linux`,
  `latest`, and `0.1.0` are the same Linux image for amd64 and arm64.
- [ ] **P3 — Publish host executables.** Build the same binary locally for
  Linux, macOS, and Windows. No shell and no Redis CLI. Do not publish it
  from GitHub Actions while the repository is private.

## Release Gates

### Production Preview

- [ ] All `P0` standalone, persistence, integrity, expiration, memory, security,
  and packaging items are complete.
  Remaining hardware-only P0s: full-disk and permission fault injection, and a
  24-hour soak. The string-RSS gap (`docs/MEMORY_DENSITY.md`) is
  an open product P0 in Milestone 3. The Cherry EPYC `kill -9` SET harness and
  the single-host performance record are in `docs/BARE_METAL.md`. See
  `docs/RELEASE_EVIDENCE.md`.
- [ ] No known acknowledged-write loss in crash testing under the documented
  durability contract.
- [ ] Twenty-four-hour mixed-workload soak passes with bounded memory.
  Blocked on a 24-hour dedicated soak host.
- [x] Supported Redis commands pass differential and client-library tests.
- [x] Recovery, backup, restore, and upgrade procedures have automated tests.
  `backup_restore_round_trip_is_the_upgrade_directory` writes through the
  server, copies the directory, and opens the copy as a new process.
- [ ] Performance is measured on bare-metal Linux and published without
  cherry-picked runs.

### Stable Standalone

- [ ] All `P1` items selected for the release are complete.
- [x] Persistence format compatibility is tested against all supported prior
  releases.
  Format major 1 is the only supported generation. `frozen_v1_directory_opens_as_a_prior_release`
  loads the frozen `CURRENT`, `CLOCK`, manifest, and snapshot as a data directory.
- [ ] Security review and operational failure drills are complete.
  Unknown names, wrong arity, `CONFIG SET`, `SHUTDOWN`, `DEBUG`, and
  `CLUSTER MEET` reply with an error and keep the socket on the evented
  runtime. Process-kill, full-disk, and live permission drills stay
  blocked on a dedicated host.
- [ ] Tail-latency and recovery objectives are documented and met.

### Stable Cluster

- [ ] All `P2` membership, migration, replication, and failover items are
  complete.
- [ ] Partition and split-brain behavior matches the published consistency
  model.
- [ ] Rolling upgrades, node replacement, resharding, and disaster recovery
  pass automated fault-injection suites.

## Recommended Execution Order

1. Lock durability and compatibility decisions.
2. Complete the performance-safe execution, time, and memory foundations.
3. Build safety limits, shutdown, configuration, and the testing foundation.
4. Implement per-shard WAL and crash-safe replay.
5. Implement snapshots, checkpoint/WAL compaction, and recovery tooling.
6. Complete TTL and memory limits so the process has a bounded lifecycle.
7. Expand Redis command compatibility based on target workloads.
8. Optimize the storage layout from bare-metal profiles.
9. Add authentication, TLS, packaging, and production runbooks.
10. Harden standalone durability and the existing asynchronous WAL replication;
    do not claim automated failover before election and partition behavior exist.
11. Validate the single evented runtime on supported operating systems and
    production-like hardware; Windows uses Tokio's IOCP path.
12. Add the AI prefix-block cache only after stable standalone. Do not build a
    vector database.
13. Build Ruvio UI after stable standalone: one `ruvio-client` binary, Svelte
    screens, container and executable from the same bits. Do not embed
    Chromium.

Every implementation change should update this file, add its acceptance tests,
and state whether it changes wire, persistence, or operational compatibility.
