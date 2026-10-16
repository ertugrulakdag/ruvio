# Eight shards on one machine against Redis: one core and an eight-node cluster

Throughput and memory for the 60 commands of `scripts/redis_compare_features.py`, Ruvio with eight shards in one process (`port_mode = "per-shard"`) against one Redis process on one core and against a Redis Cluster of eight masters on the same eight cores. Raw data, every sample and the one-shard control run: [site/data/x86-azure-2026-10-16-8-shards.json](../../site/data/x86-azure-2026-10-16-8-shards.json).

## Conditions (read before quoting any number)

- Host: Azure F16as_v6, AMD EPYC 9V74 (virtualized), 16 vCPU = 16 cores without SMT, 64 GiB, Ubuntu 24.04, kernel 6.17.0-azure. Servers on cpus 8-15, load generators on cpus 0-7, loopback.
- Ruvio: the per-shard forwarding branch (`claude/project-thread-dd1fzw`), plain `release`, `memory` durability, eight shards pinned to cpus 8-15. Redis 8.10.2 built from source (jemalloc 5.3.0), no AOF, no RDB. Redis Cluster: eight `redis-server --cluster-enabled yes` on cpus 8-15, `redis-cli --cluster create`, no replicas.
- Method (`scripts/shard_compare.py`): one `redis-benchmark` 8.10.2 per shard, pinned to its own client core, sends the row's command to that shard's port with the row's keys inside a hash tag the shard owns, so no command crosses shards and no client routes. 50 clients, pipeline 16, 300,000 requests per shard per row, three rounds with alternating order, median. The one-Redis column is one process on cpu 8 with one benchmark on cpu 0; "per core" divides Ruvio's total by eight. Memory: the twelve structures of the earlier record, loaded by slot into every system; the eight-shard systems hold eight copies, so a shard holds what the single process holds.
- Everything shares one VM and one memory bus; eight benchmark processes run on cpus 0-7 (cpus 0 and 1 also carry the OS). Compare the systems with each other only.

## Result

Over the 59 rows without `PUBLISH`: Ruvio against the eight-node Redis Cluster geometric mean 1.10×, median 1.03×, 52 rows at or above 0.97×, none below 0.90×. Ruvio per core against one Redis on one core: geometric mean 1.06×. Memory per shard against one Redis: geometric mean 0.94× (0.66× to 1.16×); against the cluster 0.93×.

A control run on the same host, one Ruvio shard on cpu 8 against one Redis on cpu 8 (client on cpus 2 and 3): geometric mean 1.07×, median 1.00×, no row below 0.90×. Going from one shard to eight costs Ruvio 4% per shard (geometric mean 0.96) and Redis Cluster 7% per node (0.93).

What the day's two runs changed: the first eight-shard run of the same binary measured 0.99× against the cluster (geometric mean) with 21 rows below 0.90×. The cause was in the server: in cluster mode plain commands outside the hot path skipped the lean dispatch and went through the generic gate. Letting a plain command on one slot take the lean path in per-shard mode gave +11% geometric mean over the 59 rows (median +14%, no row slower), which is the run recorded here.

`PUBLISH` is the one regression: 0.29M/s total against 2.21M/s for the cluster and 1.15M/s for one Redis. In per-shard mode every `PUBLISH` is forwarded to the other seven shards through the fabric and the reply waits for theirs; Redis Cluster broadcasts over its bus without waiting. Open: a fire-and-forget fan-out.

## Table

| Command | Ruvio 8 shards | Redis 1 core | Redis Cluster 8 | Ruvio / Redis 1 core (per core) | Ruvio / Redis Cluster | Note |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| GET (baseline, for scale) | 8.63M/s | 1.13M/s | 9.00M/s | 0.95× | 0.96× | not a new command |
| SET (baseline, for scale) | 8.74M/s | 1.07M/s | 8.05M/s | 1.02× | 1.09× | not a new command |
| LPOS (100 elements, hit in the middle) | 6.53M/s | 670k/s | 5.16M/s | 1.22× | 1.26× |  |
| LSET (100 elements) | 7.14M/s | 993k/s | 7.52M/s | 0.90× | 0.95× |  |
| LINSERT (pivot missing, scans 100) | 6.87M/s | 601k/s | 4.67M/s | 1.43× | 1.47× |  |
| LREM (value missing, scans 100) | 6.82M/s | 617k/s | 4.76M/s | 1.38× | 1.43× |  |
| LMOVE (rotate a 100-element list) | 7.44M/s | 875k/s | 6.50M/s | 1.06× | 1.14× |  |
| BITFIELD INCRBY u8 (always writes) | 7.32M/s | 965k/s | 7.29M/s | 0.95× | 1.00× | Redis refuses zero-padded offsets, so the offset is fixed |
| BITFIELD GET u8 | 8.25M/s | 1.06M/s | 8.02M/s | 0.98× | 1.03× |  |
| SETBIT (1 KiB bitmap, same bit) | 9.24M/s | 1.12M/s | 8.74M/s | 1.04× | 1.06× | no-op after the first write |
| GETBIT (1 KiB bitmap) | 9.14M/s | 1.16M/s | 9.13M/s | 0.99× | 1.00× |  |
| BITCOUNT (1 KiB bitmap) | 8.55M/s | 1.13M/s | 8.87M/s | 0.95× | 0.96× |  |
| PFADD | 9.07M/s | 1.13M/s | 8.92M/s | 1.01× | 1.02× |  |
| PFCOUNT | 9.26M/s | 1.19M/s | 9.21M/s | 0.97× | 1.01× |  |
| COPY ... REPLACE (64 B string) | 8.23M/s | 838k/s | 6.34M/s | 1.23× | 1.30× |  |
| DUMP (64 B string) | 6.69M/s | 384k/s | 2.87M/s | 2.18× | 2.33× | formats differ: Ruvio's is not RDB |
| EXPIRETIME | 9.30M/s | 1.18M/s | 9.34M/s | 0.99× | 1.00× |  |
| TOUCH | 9.33M/s | 1.17M/s | 9.40M/s | 1.00× | 0.99× |  |
| OBJECT ENCODING | 9.15M/s | 1.17M/s | 8.88M/s | 0.98× | 1.03× |  |
| MEMORY USAGE | 9.22M/s | 1.20M/s | 8.97M/s | 0.96× | 1.03× |  |
| GETRANGE | 9.08M/s | 1.15M/s | 8.62M/s | 0.99× | 1.05× |  |
| SETRANGE | 9.21M/s | 1.15M/s | 8.60M/s | 1.01× | 1.07× |  |
| SETEX | 8.91M/s | 831k/s | 6.34M/s | 1.34× | 1.41× |  |
| MSET (3 keys) | 7.67M/s | 787k/s | 5.96M/s | 1.22× | 1.29× |  |
| MGET (3 keys) | 7.79M/s | 949k/s | 6.97M/s | 1.03× | 1.12× |  |
| DEL (missing key) | 8.86M/s | 1.18M/s | 9.31M/s | 0.94× | 0.95× |  |
| EXISTS | 9.33M/s | 1.18M/s | 9.36M/s | 0.99× | 1.00× |  |
| TYPE | 9.10M/s | 1.16M/s | 9.23M/s | 0.98× | 0.99× |  |
| TTL | 9.27M/s | 1.13M/s | 9.27M/s | 1.03× | 1.00× |  |
| PTTL | 9.21M/s | 1.12M/s | 9.27M/s | 1.03× | 0.99× |  |
| PERSIST (key without TTL) | 9.27M/s | 1.19M/s | 9.19M/s | 0.97× | 1.01× |  |
| EXPIRE (key with TTL) | 8.78M/s | 917k/s | 7.61M/s | 1.20× | 1.15× |  |
| APPEND (1 byte) | 8.36M/s | 1.15M/s | 8.67M/s | 0.91× | 0.96× | value grows |
| INCRBYFLOAT | 5.42M/s | 591k/s | 4.73M/s | 1.15× | 1.15× | Ruvio prints 15 digits |
| HSET (existing field) | 8.81M/s | 1.00M/s | 8.05M/s | 1.10× | 1.10× |  |
| HGET | 9.14M/s | 1.09M/s | 8.75M/s | 1.05× | 1.04× |  |
| HMGET (2 fields) | 8.38M/s | 993k/s | 7.99M/s | 1.05× | 1.05× |  |
| HSTRLEN | 9.25M/s | 1.10M/s | 8.82M/s | 1.05× | 1.05× |  |
| LPUSH | 7.91M/s | 1.07M/s | 8.05M/s | 0.93× | 0.98× | list grows |
| LLEN | 9.29M/s | 1.18M/s | 9.30M/s | 0.99× | 1.00× |  |
| LRANGE 0 2 | 7.98M/s | 1.03M/s | 8.01M/s | 0.97× | 1.00× |  |
| SADD (existing member) | 8.73M/s | 1.15M/s | 8.96M/s | 0.95× | 0.97× |  |
| SISMEMBER | 9.22M/s | 1.15M/s | 8.98M/s | 1.00× | 1.03× |  |
| SRANDMEMBER | 8.98M/s | 1.14M/s | 8.89M/s | 0.98× | 1.01× |  |
| ZADD (existing member, same score) | 8.76M/s | 993k/s | 7.90M/s | 1.10× | 1.11× |  |
| ZINCRBY | 5.87M/s | 761k/s | 6.06M/s | 0.96× | 0.97× |  |
| ZSCORE | 9.17M/s | 1.07M/s | 8.59M/s | 1.07× | 1.07× |  |
| RENAME (same key) | 9.20M/s | 1.20M/s | 8.65M/s | 0.96× | 1.06× |  |
| PUBLISH (no subscribers) | 295k/s | 1.15M/s | 2.21M/s | 0.03× | 0.13× |  |
| HGET (10-field hash) | 4.63M/s | 531k/s | 3.30M/s | 1.09× | 1.40× |  |
| HSET (10-field hash, existing field) | 7.28M/s | 488k/s | 3.21M/s | 1.87× | 2.27× |  |
| HDEL (10-field hash, missing field) | 8.54M/s | 1.08M/s | 7.70M/s | 0.99× | 1.11× |  |
| HGETALL (10-field hash) | 4.58M/s | 593k/s | 4.71M/s | 0.97× | 0.97× |  |
| LINDEX (20-item list) | 9.14M/s | 1.14M/s | 8.56M/s | 1.01× | 1.07× |  |
| LRANGE 0 9 (20-item list) | 5.86M/s | 750k/s | 5.99M/s | 0.98× | 0.98× |  |
| LSET (20-item list) | 8.25M/s | 1.09M/s | 8.16M/s | 0.95× | 1.01× |  |
| GEOADD | 7.81M/s | 772k/s | 5.97M/s | 1.26× | 1.31× |  |
| GEOSEARCH (radius 50 km, COUNT 10) | 149k/s | 13k/s | 105k/s | 1.42× | 1.42× | all points share one coordinate: worst case |
| GETEX (set a 100 s deadline) | 8.47M/s | 952k/s | 7.18M/s | 1.11× | 1.18× | Ruvio's GETEX takes milliseconds only; each side uses its own syntax |
| FILLGET (value present) | 8.48M/s | 1.15M/s | 9.28M/s | 0.92× | 0.91× | Redis column: plain GET |

| Structure (per shard; the 8-shard systems hold 8 copies) | Ruvio 8 shards, total | Redis 1 core | Redis Cluster 8, total | Ruvio per shard / Redis 1 | Ruvio / Cluster |
| --- | ---: | ---: | ---: | ---: | ---: |
| 20,000 lists of 20 elements | 46.6 MiB | 5.4 MiB | 42.4 MiB | 1.09× | 1.10× |
| 5,000 HyperLogLogs of 100 elements | 15.5 MiB | 2.0 MiB | 14.3 MiB | 0.98× | 1.09× |
| 20,000 bitmaps of 128 B (SETBIT) | 31.6 MiB | 3.4 MiB | 27.3 MiB | 1.15× | 1.16× |
| 100,000 BITFIELD counters (u16) | 27.6 MiB | 5.1 MiB | 41.8 MiB | 0.68× | 0.66× |
| 100,000 geo points in one key | 41.6 MiB | 5.6 MiB | 47.6 MiB | 0.92× | 0.87× |
| 100,000 strings of 16 B | 45.0 MiB | 6.7 MiB | 54.1 MiB | 0.84× | 0.83× |
| 100,000 strings of 128 B | 139.2 MiB | 19.6 MiB | 157.1 MiB | 0.89× | 0.89× |
| 100,000 strings of 16 B with TTL | 79.4 MiB | 9.9 MiB | 77.6 MiB | 1.00× | 1.02× |
| 100,000 one-element lists | 42.3 MiB | 6.7 MiB | 54.5 MiB | 0.79× | 0.78× |
| 50,000 small hashes (3 fields) | 27.0 MiB | 3.4 MiB | 28.1 MiB | 1.01× | 0.96× |
| 50,000 small sets (4 members) | 24.7 MiB | 3.1 MiB | 25.5 MiB | 1.00× | 0.97× |
| 50,000 small sorted sets (3 members) | 23.7 MiB | 2.8 MiB | 24.5 MiB | 1.07× | 0.97× |

## Follow-up runs the same evening

Data: [x86-azure-2026-10-16-8-shards-everysec.json](../../site/data/x86-azure-2026-10-16-8-shards-everysec.json) and [x86-azure-2026-10-16-8-shards-p64.json](../../site/data/x86-azure-2026-10-16-8-shards-p64.json); same host, same method, Ruvio at `f4920ac` (non-blocking `PUBLISH` fan-out added).

### `PUBLISH` after the non-blocking fan-out

`PUBLISH` is now handed to the other seven shards as a message their loop delivers without any reply: 2.85M/s against 2.19M/s for the Redis Cluster (1.30×) and 0.29M/s before. Per core it stays at 0.56× of one Redis without any fan-out: every publish still costs seven cross-shard messages, which a single process does not pay.

### Pipeline 64: the client was the limit of the hot path

With pipeline 16 one `redis-benchmark` per shard was at its own ceiling for the cheapest commands (one Ruvio shard under `GET` showed 19,000 context switches a second against Redis's 200: it drained its sockets and slept). With pipeline 64 the picture splits in two (59 rows without `PUBLISH`, geometric mean 1.15× against the cluster, median 1.09×, 45 rows at or above 0.97×, 6 below 0.90×):

| Group | Examples | Ruvio / Redis Cluster |
| --- | --- | ---: |
| Hot path (`GET`, `SET`, `DEL`, `INCR`, `SETEX`) | `GET` 29.5M against 16.8M, `SET` 30.6M against 13.6M, `DEL` 31.1M against 18.1M | 1.7× to 2.3× |
| Generic dispatch, cheap commands | `APPEND` 0.88×, `SADD` existing 0.87×, `BITCOUNT` 0.90×, `HDEL` missing 0.87×, `FILLGET` 0.85× | 0.85× to 0.96× |

So Ruvio's hot path is about twice as fast as Redis per core and its generic path about 10% slower. A `perf record` (software clock; the VM exposes no hardware counters) of one shard under `SADD s a` at pipeline 64 shows where the generic path spends its time: 7.7% in the vDSO clock (`Instant::now` for `SLOWLOG` timing, taken twice per command in `dispatch_plain`), 4.8% in `parse_frame_spans`, 2.9% in the command table lookup, 2.7% in `charge_of` (memory accounting), 2.7% in `lookup_ttl`, 9% in the kernel waking the client socket. The `GET` profile has none of the clock cost. Open: time the slow log from a per-turn sample or a cheaper clock, then look at the parse and table steps.

### `everysec` against Redis `appendfsync everysec` (data disk, 300 MB/s)

Geometric mean 1.04× against the cluster (median 1.02×, 48 rows at or above 0.97×, 8 below 0.90×). Writes that log a small record win (`SET` 1.64×, `SETEX` 1.82×, `HSET` 1.58×, `MSET` 1.41×, `GEOADD` 1.31×); `GET` is 0.99×. The rows below 0.90× are of two kinds, both in the WAL layer, not in the shard engine:

| Row | Ruvio / Cluster | Cause (code) |
| --- | ---: | --- |
| `LSET` (100 elements) 0.53×, `LSET` (20) 0.52×, `LMOVE` 0.57×, `SETRANGE` 0.48×, `BITFIELD INCRBY` 0.55×, `COPY ... REPLACE` 0.63×, `INCRBYFLOAT` 0.69× | | not in the WAL group list (`wal_group.rs`, `eligible_outside_multi`): each one ends the pipeline's group and pays its own `write()`, under `everysec` before its reply. `LSET`/`LINSERT`/`LREM` also log the whole list as one transaction record |
| `SADD` (existing member) 0.72× | | the record is written before the member is checked (`dispatch.rs`, `Command::SetAdd`), so a no-op `SADD` logs a record on every call; Redis logs nothing when nothing changed |

Open: extend the WAL group list to the synchronous in-place commands above (each needs the "no await before the reply" review that list demands), and skip the record for a `SADD`/`ZADD` that changes nothing.

## After the three fixes (same evening, commit `7376885`)

Data: [x86-azure-2026-10-16-8-shards-everysec-after.json](../../site/data/x86-azure-2026-10-16-8-shards-everysec-after.json) and [x86-azure-2026-10-16-8-shards-p64-after.json](../../site/data/x86-azure-2026-10-16-8-shards-p64-after.json), same host and method as the two grids above. The fixes: `SLOWLOG` timed with the cycle counter instead of two vDSO clock calls per command; `INCRBYFLOAT`, `SETRANGE`, `BITFIELD`, `COPY`, `LSET` and plain `LMOVE` join the WAL group; a `SADD` whose members are all present logs nothing.

| Grid (59 rows, Ruvio / Redis Cluster) | Before, geometric mean | After | Rows below 0.90× | Rows at or above 0.97× |
| --- | ---: | ---: | ---: | ---: |
| `everysec` against AOF `everysec` | 1.03× | **1.14×** | 8 → 0 | 47 → 54 |
| memory, pipeline 64 | 1.15× | **1.23×** | 6 → 1 | 45 → 55 |

The rows the fixes aimed at, `everysec`: `LSET` 0.53× → 1.09×, `SETRANGE` 0.48× → 1.47×, `BITFIELD INCRBY` 0.55× → 1.37×, `COPY REPLACE` 0.63× → 1.68×, `LMOVE` 0.57× → 1.23×, `INCRBYFLOAT` 0.69× → 1.30×, `SADD` existing 0.72× → 1.04×. Memory mode, pipeline 64: `SADD` existing 0.87× → 1.12×, `APPEND` 0.88× → 0.95×, `BITCOUNT` 0.90× → 0.93×, `LSET` 0.93× → 1.06×; the generic path gained 5.8% (geometric mean over all rows, min 0.94×, max 1.23×). Hot-path rows did not move (`GET` 1.75×, `SET` 2.21×). The one row still below 0.90× is `HDEL` of a missing field on a 10-field hash (0.89×); `FILLGET` against plain `GET` is 0.91×. Both are open.

## Final grid with the fixed binary (pipeline 16, memory table included)

[x86-azure-2026-10-16-8-shards-final.json](../../site/data/x86-azure-2026-10-16-8-shards-final.json), the run the site shows: same method as the first table, Ruvio at `7376885`. Against the eight-node Redis Cluster the geometric mean over the 59 rows without `PUBLISH` is 1.13× (median 1.05×, 55 rows at or above 0.97×, none below 0.90×); per core against one Redis 1.09×; memory per shard against one Redis, geometric mean over the twelve shapes, 0.96×. `PUBLISH` 2.98M/s against 2.19M/s (1.36×). `GET` stays 0.96× at this pipeline depth, where one client core per shard is the limit (see the pipeline-64 grid above).
