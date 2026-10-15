# Ruvio Architecture

Ruvio is structured as a shared-nothing in-memory key-value store. The design goal is to keep hot data local to a single execution context and avoid the cache-line bouncing, mutex contention, and scheduler churn that appear in traditional multi-threaded designs.

## Design Philosophy

The store is meant to run as a set of isolated shards, each owned by one executor. Instead of protecting a single global map with locks, each shard owns its own memory and request queue. That keeps writes local, reduces synchronization points, and makes latency more predictable under load.

This is the same core idea used by many high-performance databases and networking systems: if one core owns a shard, it can make most decisions without cross-core coordination. It can improve tail latency and hardware utilization when routing and load distribution are well designed, but it does not remove coordination for cross-shard operations.

## Shared-Nothing Execution

The storage, protocol, and routing layers remain independent of asynchronous runtime and operating-system APIs. The process runs one Tokio `current_thread` evented runtime per shard, with epoll on Linux, kqueue on macOS, and IOCP on Windows. On Linux, owner threads can be pinned to cores. This model can provide three mechanical advantages:

1. The working set stays warm in the L1/L2 caches of that core.
2. Requests do not bounce between worker threads.
3. The system can scale horizontally by adding more shards instead of adding more locking.

The tradeoff is that ownership matters. Data must be routed to the right shard up front, but the gain is that once the request arrives, the shard can operate without locks in its hot path.

Redis-compatible key hashing selects a slot and its static shard owner. Online rebalancing and cross-slot transactions are not implemented. A slow shard can delay other requests on that shard; a fast shard is not guaranteed to retain its unloaded throughput under skew. See `INFRASTRUCTURE_IMPROVEMENTS.md`.

Linux, macOS, and Windows run the same evented runtime. The command engine does not depend on epoll, kqueue, or IOCP. See `PORTABILITY.md`.

Multi-shard mode divides the Redis-compatible 16,384 hash slots across shard owners. Each executor listens on a consecutive port and owns its shard exclusively. Requests arriving at the wrong shard receive `MOVED`, while multi-key requests spanning slots receive `CROSSSLOT`. `CLUSTER SLOTS` / `SHARDS` / `NODES` / `INFO` / `MYID` / `KEYSLOT` describe that static map. Official clients take one seed address and route by slot. There is no cluster bus, slot migration, or failover.

## Network I/O

The process uses Tokio on the shard owner. Linux uses epoll, macOS uses kqueue, and Windows uses IOCP. Parse, apply, and write stay on that owner. The storage core does not take a socket type.

## Cache Alignment and SIMD

The store should keep selected hot metadata cache-aligned when measurement justifies it. Alignment can prevent a field from straddling cache lines and can isolate independently written data, but it does not inherently reduce cache misses. Over-alignment can waste memory and reduce cache density.

SIMD is useful for scanning memory quickly when the store needs to:

- search a block for a key prefix,
- compare a run of bytes against a query byte,
- accelerate probing in dense in-memory structures.

The `examples/02_simd_search.rs` file demonstrates the core pattern: compare multiple bytes at once with SSE2 or NEON and fall back to scalar logic for the tail. Production code should benchmark against established primitives such as `memchr`, which already provide architecture-specific runtime dispatch and tuned implementations.

## Zero-Copy Networking

Zero-copy in this project means zero-copy parsing: the wire format is designed so incoming bytes can be viewed directly as structured data rather than decoded into temporary heap allocations. It does not claim that the kernel-to-userspace network transfer is copy-free.

That is a mechanical advantage because it removes:

- heap churn,
- extra memcpy work,
- unnecessary cache pollution from transient objects.

The wire header uses network byte order and alignment-1 integer wrappers so it can be read from arbitrary packet boundaries. Lengths must be validated before slicing. In production, pair that format with bounded length-prefixed frames, registered or pooled buffers, and careful ownership boundaries so a buffer cannot be reused while a parsed view still borrows it.

## Current Storage Core

`KvShard` is a correctness-oriented, single-owner reference shard. It deliberately has no locks. Keys and values are copied once into shard-owned boxed slices; reads borrow those stored bytes without allocating.

The shard uses randomized AHash indexes. This is not a claim of peak performance; open-addressing layouts, deletion behavior, and memory fragmentation still require realistic workload measurements.

The current shard uses AHash indexes and a bounded size-class pool for byte-value buffers up to 64 KiB. Deletes return eligible buffers to the owning shard, and later writes reuse them without cross-thread synchronization. The pool is intentionally bounded per size class; unlike an unbounded bump arena, it does not retain every deleted value indefinitely.

Expiration metadata is stored as absolute Unix milliseconds rather than
process-local `Instant` values, making it suitable for snapshots and restart.
`KvShard` is generic over a clock without dynamic dispatch; production uses the
system wall clock and tests inject deterministic time. Restart loads `CLOCK`
and the last checkpoint timestamp and never reports a time below that floor.
The clock is consulted
only for entries that actually have an expiration deadline.

Each shard also maintains O(1) memory-usage snapshots. Key allocations, value
payloads, collection capacity, primary-index capacity, and retained pool
buffers are charged separately. Signed deltas between snapshots describe a
mutation without scanning the keyspace. The charge model is deterministic for
limits and eviction decisions; it is deliberately not presented as process
RSS. See `MEMORY_ACCOUNTING.md`.

The primary key index is a compile-time `StorageIndex` parameter. Production
uses `AHashIndex`; an ordered `BTreeIndex` provides a conformance baseline.
The index trait is deliberately incompatible with trait objects, so alternate
layouts do not add virtual dispatch to shard operations. Storage semantics,
expiration, memory accounting, and persistence remain outside the index. See
`STORAGE_LAYOUT.md`.

The server binary uses mimalloc by default. The `mimalloc-allocator` feature can be disabled to compare against the system allocator. Allocator choices must be retained only when repeated workload-specific measurements justify them.

## Current Command Server

The `ruvio` binary runs the evented runtime only. Each shard owner parses requests from its sockets, applies commands and encodes responses on its Tokio `current_thread` executor; borrowed reads stay on the owner until encoded. Hot and fallback RESP parsers borrow from bounded connection input buffers. The server validates protocol and connection limits before opening listeners, enforces connection timeouts, and bounds global and per-source-IP admission. Shard owners share the admission tracker on accept/disconnect, not for each command.

WAL-backed `primary` and `replica` roles provide asynchronous per-shard WAL tail replication. A replica can install a usable checkpoint after a compacted WAL gap and then catch up from the tail. The writer does not wait for replicas unless a client explicitly uses `WAIT`; `WAIT` does not roll back a write on timeout. No election, automatic promotion, online slot migration or cluster bus is provided. See `RECOVERY.md` and `COMPATIBILITY.md`.

## Production Gaps

This is not a certified production cluster. Before making stronger availability or performance claims it needs:

- measured overload isolation and tail-latency budgets under saturation,
- production-like fault, disk and recovery drills,
- defined membership, migration, election and split-brain behavior,
- hardware and long-duration durability evidence.

Report at least p50, p95, p99, and p99.9 latency together with throughput, CPU utilization, allocation rate, key/value distribution, connection count, compiler version, kernel version, and CPU model. Compare against a scalar or conventional threaded baseline.

## Practical Boundary

This scaffold is intentionally small. The examples show the core techniques in isolation so the eventual storage engine can be built around a few stable invariants:

- one shard owns one memory region,
- one executor owns one shard,
- wire data stays layout-compatible with its Rust representation,
- hot scans use cache-friendly data layout and SIMD where it helps.