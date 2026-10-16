# Command and memory comparison against Redis

Per-command throughput and memory growth for 60 commands and 12 data structures, Ruvio against Redis 8.10.2. The raw data behind the table is [site/data/x86-azure-2026-10-11.json](../../site/data/x86-azure-2026-10-11.json); the [performance page](https://salihcantekin.github.io/ruvio/testing-performance.html) shows the same run with all samples.

## Conditions (read before quoting any number)

- Host: Azure VM, AMD EPYC 7763 (virtualized), 8 vCPU = 4 cores x 2 threads, 31 GiB, Ubuntu 24.04, kernel 6.17.0-azure; data disk Premium SSD v2 32 GiB (16,000 IOPS, 400 MB/s, host caching none) with ext4; OS disk Standard SSD. Native processes only (no Docker).
- Ruvio commit `5520164`; Redis 8.10.2 built from source (jemalloc 5.3.0); redis-benchmark is the 8.10.2 binary from the same build.
- 60 commands from `scripts/redis_compare_features.py` run through `scripts/pinned_run.py`: `redis-benchmark` 8.10.2, 50 clients, pipeline 16, 64-byte values, five rounds with alternating order, median requests/s. Ruvio and Redis each run alone on cpu 0 (core 0, its hyper-thread idle), the load generator on cpus 2 and 4 (two other cores), one shard, `memory` durability.
- one VM, one CPU model (virtualized AMD EPYC 7763), native processes only, loopback, 64-byte values, short runs, five rounds. The data disk is network-attached (an `fdatasync` costs about 0.8 ms); a local NVMe disk would change every `always` row for both servers. Server and load generator share the VM, so absolute numbers describe this host; compare the two servers with each other only.

## Reproduce

```bash
cargo build --profile release-fast --bin ruvio
python3 scripts/redis_compare_features.py --ruvio target/release-fast/ruvio
```

## Throughput

| Command | Ruvio | Redis | Ruvio / Redis | Note |
| --- | --- | --- | --- | --- |
| GET (baseline, for scale) | 1.20M/s | 1.12M/s | 1.08× | not a new command |
| SET (baseline, for scale) | 1.21M/s | 917k/s | 1.32× | not a new command |
| LPOS (100 elements, hit in the middle) | 817k/s | 548k/s | 1.49× |  |
| LSET (100 elements) | 920k/s | 826k/s | 1.11× |  |
| LINSERT (pivot missing, scans 100) | 904k/s | 480k/s | 1.88× |  |
| LREM (value missing, scans 100) | 896k/s | 500k/s | 1.79× |  |
| LMOVE (rotate a 100-element list) | 1.03M/s | 730k/s | 1.41× |  |
| BITFIELD INCRBY u8 (always writes) | 952k/s | 809k/s | 1.18× | Redis refuses zero-padded offsets, so the offset is fixed |
| BITFIELD GET u8 | 1.11M/s | 929k/s | 1.19× |  |
| SETBIT (1 KiB bitmap, same bit) | 1.24M/s | 1.01M/s | 1.24× | no-op after the first write |
| GETBIT (1 KiB bitmap) | 1.25M/s | 1.08M/s | 1.16× |  |
| BITCOUNT (1 KiB bitmap) | 1.18M/s | 1.08M/s | 1.09× |  |
| PFADD | 1.22M/s | 1.05M/s | 1.17× |  |
| PFCOUNT | 1.27M/s | 1.14M/s | 1.11× |  |
| COPY ... REPLACE (64 B string) | 1.15M/s | 708k/s | 1.63× |  |
| DUMP (64 B string) | 898k/s | 418k/s | 2.15× | formats differ: Ruvio's is not RDB |
| EXPIRETIME | 1.26M/s | 1.15M/s | 1.10× |  |
| TOUCH | 1.27M/s | 1.15M/s | 1.10× |  |
| OBJECT ENCODING | 1.23M/s | 1.05M/s | 1.18× |  |
| MEMORY USAGE | 1.25M/s | 1.08M/s | 1.15× |  |
| GETRANGE | 1.23M/s | 1000k/s | 1.23× |  |
| SETRANGE | 1.27M/s | 987k/s | 1.28× |  |
| SETEX | 1.19M/s | 688k/s | 1.73× |  |
| MSET (3 keys) | 1.06M/s | 610k/s | 1.74× |  |
| MGET (3 keys) | 1.03M/s | 813k/s | 1.27× |  |
| DEL (missing key) | 1.23M/s | 1.19M/s | 1.04× |  |
| EXISTS | 1.28M/s | 1.14M/s | 1.13× |  |
| TYPE | 1.25M/s | 1.11M/s | 1.13× |  |
| TTL | 1.22M/s | 1.13M/s | 1.08× |  |
| PTTL | 1.21M/s | 1.13M/s | 1.08× |  |
| PERSIST (key without TTL) | 1.29M/s | 1.09M/s | 1.18× |  |
| EXPIRE (key with TTL) | 1.06M/s | 872k/s | 1.21× |  |
| APPEND (1 byte) | 1.01M/s | 997k/s | 1.02× | value grows |
| INCRBYFLOAT | 694k/s | 505k/s | 1.37× | Ruvio prints 15 digits |
| HSET (existing field) | 1.08M/s | 890k/s | 1.21× |  |
| HGET | 1.26M/s | 1.02M/s | 1.23× |  |
| HMGET (2 fields) | 1.09M/s | 901k/s | 1.22× |  |
| HSTRLEN | 1.28M/s | 1.05M/s | 1.22× |  |
| LPUSH | 943k/s | 901k/s | 1.05× | list grows |
| LLEN | 1.27M/s | 1.12M/s | 1.14× |  |
| LRANGE 0 2 | 1.05M/s | 898k/s | 1.17× |  |
| SADD (existing member) | 1.08M/s | 1.02M/s | 1.06× |  |
| SISMEMBER | 1.27M/s | 1.01M/s | 1.26× |  |
| SRANDMEMBER | 1.02M/s | 997k/s | 1.03× |  |
| ZADD (existing member, same score) | 1.12M/s | 917k/s | 1.22× |  |
| ZINCRBY | 829k/s | 662k/s | 1.25× |  |
| ZSCORE | 1.20M/s | 1.01M/s | 1.19× |  |
| RENAME (same key) | 1.26M/s | 1.01M/s | 1.25× |  |
| PUBLISH (no subscribers) | 1.28M/s | 1.28M/s | 1.00× |  |
| HGET (10-field hash) | 1.20M/s | 968k/s | 1.24× |  |
| HSET (10-field hash, existing field) | 1.02M/s | 852k/s | 1.19× |  |
| HDEL (10-field hash, missing field) | 1.04M/s | 990k/s | 1.05× |  |
| HGETALL (10-field hash) | 524k/s | 540k/s | 0.97× |  |
| LINDEX (20-item list) | 1.19M/s | 968k/s | 1.23× |  |
| LRANGE 0 9 (20-item list) | 721k/s | 743k/s | 0.97× |  |
| LSET (20-item list) | 1.07M/s | 904k/s | 1.19× |  |
| GEOADD | 909k/s | 656k/s | 1.39× |  |
| GEOSEARCH (radius 50 km, COUNT 10) | 14k/s | 10k/s | 1.31× | all points share one coordinate: worst case |
| GETEX (set a 100 s deadline) | 971k/s | 804k/s | 1.21× | Ruvio's GETEX takes milliseconds only; each side uses its own syntax |
| FILLGET (value present) | 1.11M/s | 1.16M/s | 0.96× | Redis column: plain GET |

Geometric mean of the 60 rows: 1.22×; 59 rows at or above 0.97×; lowest 0.96×.

## Memory

| Data | Ruvio growth | Redis growth | Ruvio total RSS | Redis total RSS | Total ratio |
| --- | --- | --- | --- | --- | --- |
| 20,000 lists of 20 elements | 5.9 MiB | 5.3 MiB | 15.7 MiB | 18.5 MiB | 0.85× |
| 5,000 HyperLogLogs of 100 elements | 2.2 MiB | 2.0 MiB | 12.3 MiB | 15.0 MiB | 0.82× |
| 20,000 bitmaps of 128 B (SETBIT) | 3.6 MiB | 3.3 MiB | 13.4 MiB | 16.4 MiB | 0.82× |
| 100,000 BITFIELD counters (u16) | 3.5 MiB | 5.1 MiB | 13.6 MiB | 18.3 MiB | 0.75× |
| 100,000 geo points in one key | 6.3 MiB | 5.7 MiB | 16.2 MiB | 18.9 MiB | 0.86× |
| 100,000 strings of 16 B | 5.2 MiB | 6.5 MiB | 15.1 MiB | 19.6 MiB | 0.77× |
| 100,000 strings of 128 B | 17.4 MiB | 19.2 MiB | 27.4 MiB | 32.6 MiB | 0.84× |
| 100,000 strings of 16 B with TTL | 9.9 MiB | 9.9 MiB | 19.8 MiB | 23.1 MiB | 0.86× |
| 100,000 one-element lists | 4.7 MiB | 6.5 MiB | 14.7 MiB | 19.9 MiB | 0.74× |
| 50,000 small hashes (3 fields) | 3.0 MiB | 3.4 MiB | 12.8 MiB | 16.6 MiB | 0.77× |
| 50,000 small sets (4 members) | 2.6 MiB | 3.1 MiB | 12.7 MiB | 16.3 MiB | 0.78× |
| 50,000 small sorted sets (3 members) | 2.7 MiB | 2.8 MiB | 12.8 MiB | 17.9 MiB | 0.71× |

## Reading the memory table

Growth is Ruvio's RSS increase from loading against Redis's. Redis's idle RSS on this machine was 13.2 MiB (8.4 MiB on the previous one), which pre-faults pages and makes its growth look smaller, so the total RSS after loading is the steadier number: Ruvio is lower in all 12 rows (0.71–0.86×). By growth Ruvio is lower in 7 rows, equal in 1 and higher in 4 (lists of 20 elements, HyperLogLogs, 128-byte bitmaps, geo points). Ruvio's own growth values are unchanged from the previous run (within 0.2 MiB), so the difference comes from the Redis side.

## Compatibility notes

- `GETEX key EX seconds` (Redis syntax) is refused by Ruvio, whose `GETEX` takes milliseconds only; the compatibility matrix lists it.
- `BITFIELD` and `SETBIT` in Redis refuse zero-padded numbers such as `000000000123`; Ruvio accepts them.
- `DUMP` is not comparable in content: Ruvio's payload is its own checksummed WAL record, not RDB.
