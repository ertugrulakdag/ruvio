# Bare-metal measurements, 2026-09-28

One Cherry Servers host, billed hourly until it is terminated. These numbers
are from that machine only. Frequency boost was disabled in `lscpu`. Docker
and laptop numbers in `BENCHMARK_RESULTS.md` are not this run.

## Machine

| Item | Value |
| --- | --- |
| Provider / name | Cherry Servers (one hourly-billed host) |
| CPU | AMD EPYC 7313P, 1 socket, 16 cores. SMT off after tuning (threads 16–31 offline) |
| Frequency | Boost disabled. Max 3730 MHz. Governor `performance` on online CPUs |
| Memory | 62 GiB |
| Disk | 2× Micron 7450 NVMe, mdraid, about 406 GiB free |
| OS | Ubuntu 24.04.5, kernel 6.8.0-142-generic |
| `io_uring` | `kernel.io_uring_disabled=0` |
| `perf` | `kernel.perf_event_paranoid=-1` |
| Rust | rustc 1.98.1, `RUSTFLAGS="-C target-cpu=native"`, `--locked --release` |
| Ruvio | 0.2.3. Evented binary `target-evented/release/ruvio`. That run also built an io_uring binary; that adapter is no longer compiled |
| Redis server | 8.10.2 at `/usr/local/bin/redis-server-8`, jemalloc. No `io-threads`. Not Redis Cluster |
| Client | Ubuntu `redis-benchmark` / `redis-cli` 7.0.15, for every server |

Pins, disjoint:

| Shape | Server CPUs | Client CPUs |
| --- | --- | --- |
| 1 core | 2 | 4–7 |
| 4 cores | 2–5 | 8–11 |

Quad Redis is four `redis-server-8` processes on ports 6379–6382. Quad Ruvio
is one process with four shards on those ports. Durability for the matrix is
`memory`. Loopback only.

`p99.9` below is redis-benchmark's 99.902% histogram bucket, not a computed
p99.9. Quad rows have no p50: `scripts/benchmark_shards.py` keeps aggregate
rps and max p99.

## Scripts

| Script | What it runs |
| --- | --- |
| `scripts/baremetal_compare.py` | 5-sample matrix (evented, io_uring, Redis; 1 core and 4 cores), pipeline / client / payload sweeps, durability, two 15-minute soaks. Host paths are absolute (`/root/ruvio`, `/usr/local/bin/redis-server-8`). |
| `scripts/benchmark_shards.py` | One `redis-benchmark` per shard, hash-tag keys, aggregate rps from wall time. |
| `scripts/durability_rebench.py` | Single-core SET after the WAL change. 3 samples, n=1,000,000, 50 clients, pipeline 16, 64-byte values. |
| `scripts/kill9_harness.py` | Live `kill -9` during pipelined SET, then restart and prefix check. |

Raw matrix output from the host copy lives under `profile/results/` locally.
That directory is gitignored.

## Matrix medians (before the WAL change)

5 samples. 1 core: n=2,000,000, 50 clients, pipeline 16, 64 bytes.
4 cores: 500,000 requests per shard, 25 clients per shard.

### One core

| Server | SET rps | SET p50 / p99 | GET rps | INCR rps |
| --- | ---: | --- | ---: | ---: |
| evented | 816,994 | 0.495 / 0.655 ms | 824,742 | 839,983 |
| io_uring | 814,332 | 0.591 / 0.751 ms | 820,008 | 829,187 |
| Redis 8.10.2 | 798,722 | 0.831 / 1.279 ms | 837,521 | 847,099 |

SET 99.902% bucket: evented 0.727 ms, io_uring 0.823 ms, Redis 1.407 ms.

### Four cores (aggregate)

| Server | SET rps (p99) | GET rps (p99) | INCR rps (p99) |
| --- | --- | --- | --- |
| evented | 3,045,982 (0.575 ms) | 3,207,004 (0.407 ms) | 2,890,182 (0.631 ms) |
| io_uring | 2,891,506 (0.647 ms) | 3,163,467 (0.559 ms) | 2,740,706 (0.687 ms) |
| Redis ×4 | 2,487,261 (1.031 ms) | 3,259,788 (0.471 ms) | 2,531,307 (0.951 ms) |

On this CPU, evented was slightly ahead of io_uring. One `perf stat` window
(not a median): evented about 7.45% cache misses and 14,013 context-switches;
io_uring about 9.11% cache misses and 268 context-switches; 0 migrations either way.

### Sweeps (1 sample, n=1,000,000, evented vs Redis, SET)

| Knob | evented rps (p99) | Redis rps (p99) |
| --- | --- | --- |
| pipeline 1 | 54,051 (0.639 ms) | 53,987 (0.631 ms) |
| pipeline 16 | 811,030 (0.623 ms) | 796,813 (1.279 ms) |
| pipeline 64 | 1,805,054 (1.815 ms) | 1,333,333 (2.831 ms) |
| 1 client | 374,813 (0.047 ms) | 336,814 (0.055 ms) |
| 50 clients | 814,332 (0.615 ms) | 799,361 (1.263 ms) |
| 200 clients | 811,030 (2.175 ms) | 809,062 (4.951 ms) |
| 64 bytes | 799,361 (0.671 ms) | 794,913 (1.295 ms) |
| 1024 bytes | 643,501 (0.767 ms) | 693,481 (0.863 ms) |
| 4096 bytes | 347,464 (1.319 ms) | 255,037 (1.095 ms) |

Pipeline 1 is the round-trip floor: both land near 54k rps with p50 0.471 ms.
At 1024 bytes Redis was faster. At 4096 bytes evented had higher throughput
and Redis had a lower p50 (0.823 ms vs 1.183 ms).

### Fifteen-minute soak, four cores, `memory`

About 900 seconds, keyspace `-r 1000000`. The first evented attempt failed because a 1-shard checkpoint was
still in `/var/tmp/ruvio-run` (`checkpoint shard count does not match`). The
rerun used an empty data directory.

| Server | SET median rps | GET median | INCR median |
| --- | ---: | ---: | ---: |
| evented | 3.09M | 3.11M | 2.75M |
| Redis ×4 | 2.73M | 3.23M | 3.12M |

The soak keyspace is warm after the first pass, so Redis INCR here (3.12M)
is higher than the cold matrix INCR (2.53M).

The RSS plateau of this soak, the key mix behind it, and the open P0 are in
`docs/MEMORY_DENSITY.md`.

### Hot path after the redundant GET lookup (3 samples, n=1,000,000)

Default `noeviction` no longer hashes a GET key twice, and a write with no
open snapshot and no eviction no longer allocates a one-key `Vec`. Same pins,
`memory`, pipeline 16, 64 bytes. Medians: SET 812k, GET 813k, INCR 831k.
Morning medians at n=2,000,000 were SET 817k, GET 825k, INCR 840k. The ceiling
did not move.

## Durability

Workload unless noted: 1 core, SET, 50 clients, pipeline 16, 64-byte values.
`everysec` acknowledges after the kernel accepts the bytes. `always`
acknowledges only after `fdatasync`. Several records may share
one sync.

### Before the WAL change (1 sample, n=1,000,000)

The client thread called `sync_data` on the request path: every second for
`everysec`, and once per record for `always`.

| Profile | rps | p50 | p99 | 99.902% bucket |
| --- | ---: | ---: | ---: | ---: |
| evented `memory` | 800,000 | 0.503 ms | 0.647 ms | 0.863 ms |
| evented `everysec` | 293,083 | 2.607 ms | 2.647 ms | 53.631 ms |
| evented `always` | 9,066 | 88.511 ms | 92.223 ms | 92.863 ms |
| Redis AOF `everysec` | 506,073 | 1.359 ms | 2.663 ms | 7.535 ms |

### What changed

`everysec` no longer syncs on the client thread. A batch is one `write`.
A background thread calls `fdatasync` on the interval (default 1s). If that
sync is still running, the next one waits, so the loss window can exceed one
second, which the durability contract allows.

`always` stages a client batch into one `write`, yields so other ready
clients on the same shard can append, then one `fdatasync` covers that wave.
Replies are not written to the socket until that sync returns. A single
`record()` outside a batch still syncs immediately, so tests and replication
keep the old contract.

### After the WAL change (3 samples, n=1,000,000, medians)

| Profile | rps | p50 | p99 | 99.902% bucket |
| --- | ---: | ---: | ---: | ---: |
| evented `memory` | 807,103 | 0.503 ms | 0.631 ms | 0.823 ms |
| evented `everysec` | 605,327 | 1.295 ms | 1.479 ms | 4.671 ms |
| Redis AOF `everysec` | 510,986 | 1.367 ms | 3.287 ms | 9.767 ms |
| evented `always` | 462,535 | 1.735 ms | 2.031 ms | 2.143 ms |
| Redis AOF `always` | 429,738 | 1.751 ms | 2.311 ms | 6.391 ms |

`memory` stayed near 800k, so the batching path did not tax the cache
profile. `everysec` went from 293k with a 54 ms tail to 605k with a 4.7 ms
bucket, ahead of Redis AOF `everysec` on this SET workload. `always` went
from 9k to a 463k median. The first `always` sample was 260k rps (p99 3.94 ms);
the next two were 463k. Redis `always` median was 430k.

Quad durability was not re-measured after the change.

## `kill -9` harness

`scripts/kill9_harness.py`, seed 20260928, 12 trials per profile. One shard,
evented, pipelined SET of `k:1…`, `kill -9` after a random 50–350 ms, restart
on the same data directory. Recovery must start, and the keyspace must be a
prefix (a missing key cannot be followed by a later key). `always` must still
contain every key whose `+OK` the client had read.

| Profile | Trials | Result |
| --- | ---: | --- |
| `everysec` | 12 | All recovered. Acknowledged keys were present. In-flight pipeline (16 keys) was not required to survive. |
| `always` | 12 | All recovered. Every acknowledged key was present. Some trials also had the in-flight batch, which had been synced before the client read those replies. |

Largest `everysec` trial: 111,136 acknowledged keys, all present after restart.
This is a process crash. It does not stand in for power loss. The in-process
abort test `persistence::crash::tests::killed_process_recovers_at_crash_sites`
still covers append, sync, rotate, snapshot, and rename.

Full-disk injection and a 24-hour soak were not run.
