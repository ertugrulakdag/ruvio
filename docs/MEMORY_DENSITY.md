# String memory density

Status: open P0. Recorded from the Cherry EPYC soak on 2026-09-28.
See `docs/BARE_METAL.md` for the run and `docs/ROADMAP.md`.

## What was compared

Fifteen minutes, four cores, durability `memory`, the same `redis-benchmark`
client, pipeline 16, 64-byte values, key range `-r 1000000`. Ruvio was one
evented process with four shards. Redis was four 8.10.2 processes on the same
four cores and ports. RSS is `VmRSS`, summed across the engine processes.

| | RSS plateau | Logical data |
| --- | ---: | --- |
| evented | 2.78 GiB | same key range |
| Redis ×4 | 648 MiB | same key range |

The ratio is about 4.4×. Both curves flattened once the range was full, so
Ruvio was not accumulating extra keys. SET and INCR use different key names,
so the steady set is about 1 million 64-byte strings plus 1 million small
counters per shard, about 8 million keys in total. The raw SET payloads are
about 256 MiB. Redis holds that whole set, names included, in 648 MiB.
Ruvio holds the same set in 2.78 GiB.

This run did not print `DBSIZE`. The next measurement has to record key counts
on every shard and every Redis process before the ratio is quoted again.

A long-running cache with this layout will use about four times the RAM of
Redis for the same string working set. That grows with key count, then stops.
It does not grow without bound, and it does not shrink when the key count
stays flat. mimalloc keeping empty arenas is a second-order effect; the
plateau is the filled table.

## Where a string key pays

`AHashIndex` stores `AHashMap<Box<[u8]>, Entry>`.

- The key bytes are their own heap block. The map slot only holds the box.
- `Entry` always carries `expires_at`, `recency`, and `frequency`. The soak
  used `noeviction` and no TTL, so those fields were unused.
- `value` is a `Value` enum. The largest arm is `SortedSet` (`AHashMap` plus
  `BTreeMap`). A string still occupies that arm's size. `Stream` is already
  boxed; lists, sets, hashes, sorted sets, and bloom filters are not.
- `BytesValue` is a `Vec<u8>` plus `Option<i64>`. The bytes are a second heap
  block. Capacity rounds up to a power of two. An `INCR` key keeps both the
  digit buffer and the cached integer.
- The Swiss table leaves empty slots, about one eighth of the buckets.

Redis stores a short string in one allocation with the dict entry. That is why
648 MiB covers roughly 8 million keys and 2.78 GiB does not.

WAL and snapshot bytes are not part of this gap. The soak profile was
`memory`. The on-disk format does not have to change to shrink RAM.

## Local Docker probe

`scripts/rss_docker.py` loads 1,000,000 `SET` keys (`k:0` … `k:999999`, 64-byte values) into a fresh container named `ruvio-rss` on `127.0.0.1:6399`. One shard, durability `memory`. It does not use the existing `ruvio` or `redis` containers. RSS is `VmRSS` inside the container after `DBSIZE` reads 1000000. Each improvement rebuilds the image and runs this from an empty volume.

| Step | Image | VmRSS | Docker reported | Kept |
| --- | --- | ---: | ---: | --- |
| Baseline, `main` at `4397478` | `ruvio-rss:baseline` | 517 MiB | 516 MiB | |
| Large collections boxed | `ruvio-rss:boxed` | 327 MiB | 326 MiB | yes |
| 64-byte value stored in the slot | `ruvio-rss:inline` | 455 MiB | 458 MiB | no |
| Short key stored in the slot, value still a `Vec` | `ruvio-rss:inline-key` | 367 MiB | 368 MiB | no |
| TTL and eviction fields moved off `Entry` | `ruvio-rss:metadata` | 254 MiB | 253 MiB | yes |
| String bytes in one shard log, values ≤ 64 bytes | `ruvio-rss:log` | 230 MiB | 233 MiB | yes |
| Key bytes in the index log, table holds the span | `ruvio-rss:keylog` | 223 MiB | 227 MiB | yes |
| Cached `i64` kept only for `INCR` keys | `ruvio-rss:intcache` | 175 MiB | 182 MiB | yes |
| Redis 8. alpine, `--save "" --appendonly no`, container `redis-rss` | `redis:8-alpine` | 148 MiB | 191 MiB | |

The boxed layout is 190 MiB less, about 63% of the baseline, for the same 1 million strings. Raw payloads are 64 MiB. Moving the integer cache off every string dropped the key-log image from 223 MiB to 175 MiB on two repeats (178720 kB and 178704 kB). The same script against Redis 8 was 151428 kB. That is 1.18×, inside the 1.5× band for this probe. Load time stayed on the 1.2–1.5 second band. A `SET`-only keyspace leaves the integer map empty, so the hot path only checks a counter.

One million distinct `INCR` keys are a different mix. With the cache keyed by a copy of the key, Ruvio was 190052 kB and Redis 8 was 72952 kB. Keying that cache by the value-log span measured 170212 kB. Storing the `i64` in the eight bytes before the decimal text, and deleting the second hash table, measured 123996 kB. The 64-byte `SET` probe stayed at 178592 kB. Hot-key `SET`/`GET`/`INCR` stayed in the same Docker band as Redis.

A replacement table at the same 63/64 mark made empty slots resident and raised `SET` RSS to 203868–212072 kB, so it was not kept. The load factor change lives inside hashbrown instead: one empty slot in 64, data pages still unwritten until a record lands. One million keys stay in 2^20 slots. `INCR` RSS fell from 123996 kB to 87064 kB. The 64-byte `SET` probe stayed at 179284 kB, next to 180636 kB before the change. Hot-key throughput stayed in the same Docker band as Redis. The patched SwissTable lives in `src/hashbrown`. Tiny counters are about 1.2× Redis on this probe.

Putting the 64-byte value inside the hash slot raised RSS to 455 MiB. The slot grew by more than the removed allocation, and hashbrown still leaves empty buckets. Inlining only the short key (`k:N`, 23 bytes in the slot) landed at 367 MiB, still above 327 MiB, because the inline key is wider than a `Box` pointer. Neither change stayed. Cache-line alignment was not used: padding every entry to 64 bytes would make this worse. Key compare already goes through `memcmp`, which the standard library vectorizes; that does not shrink RSS.

Hash-slot inlining stays rejected. A packed record log is the next structural experiment only if a later soak is still outside 1.5×. This 1 million `SET` probe is not.

## Order of work

Do these in order. Each one keeps RESP, WAL records, and snapshot payloads
the same. Measure RSS and key count after each one, on the same soak, before
starting the next. Stop when the plateau is in the same band as Redis for
this key mix. "Same band" means within about 1.5× on that soak, not a
byte-identical allocator trace.

1. **Shrink `Value` for strings.** Box `List`, `Set`, `Hash`, `SortedSet`,
   and `BloomFilter` the way `Stream` is already boxed. A string `Entry`
   should be a small header plus the byte buffer, not a sorted-set object.
   Add a `size_of` test so a new large arm cannot land inline again.
   `StorageIndex` already treats `Entry` as opaque, so the map code stays.

2. **Do not inline the 64-byte value or the short key into the hash slot.**
   Both were measured and rejected; see the table and the paragraph after it.
   A packed log plus a small index address is the shape that can still win.

3. **Keep TTL and eviction metadata off `Entry`.** Measured at 254 MiB.
   `expires_at` lives in a per-database map and is skipped entirely when no
   key has a TTL. `recency` and `frequency` live in a second map that stays
   empty under `noeviction` and is cleared when that policy is selected.
   `Entry` is only the `Value`. LRU, LFU, and TTL behavior stay.
4. **Put string bytes of at most 64 bytes in one shard `Vec` log.** The entry
   stores an offset and a length. Longer values stay on the byte pool. Measured
   at 230 MiB. Undo copies log bytes out so a later overwrite does not change
   the saved entry. Key bytes then moved into the index's own log; the hash
   table stores the span, not a `Box`. Three repeats sat at 223 MiB versus
   230 MiB for values only. The load time stayed on the same 1.2–1.3 second
   band.

5. **Keep the cached `i64` off ordinary strings.** Measured at 175 MiB.
   `SET` clears it. `INCR` writes it into a per-database map that stays empty
   when no key has been incremented. `SWAPDB` moves that map with the TTL and
   eviction maps, so a swapped key keeps its deadline.

6. **Pack the index only if a soak is still outside 1.5×.** This Docker probe
   is 1.18× Redis. The Cherry 4.4× soak has not been repeated, so the P0 stays
   open until that key mix is measured again. Do not densify the table before
   that number exists.

Do not start with mimalloc settings. Allocator retention is not the P0 while
the probe is already inside Redis's band.

## What stays put

- Shard ownership, `AHashIndex` as the default until step 5, and the
  validate-then-record-then-apply order.
- Durability profiles. `sync_interval_ms` is the `everysec` window and is
  unrelated to this layout.
- The byte pool. It recycles freed buffers; it does not make a live 64-byte
  value smaller.
