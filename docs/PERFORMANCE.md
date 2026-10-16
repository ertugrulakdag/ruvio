# Performance and memory

Every change is kept only when it does not make a command slower and does not
raise RSS for the same data. Correctness is required. A slower or larger
result is not an improvement.

This applies to the server and to both first-party clients. The C# and Rust
clients are the path to Ruvio. A cost added on every command is paid even
when the server itself is fast.

## What to protect

- The command path for `SET`, `GET`, and `INCR` does not allocate a second
  copy of the key or the value, and does not take a lock that another shard
  needs.
- A collection, a geographic member, or a reply stays in one representation.
  A second index or a second copy of the same bytes raises RSS.
- Work that is not that command stays off that command. File IO, ACL reload,
  backup signing, and snapshot publication run on the operation that asked
  for them.
- A client command frame is written from pooled or stack memory. A generated
  command name is a UTF-8 span. Do not add a `string[]` on the standalone
  `GET`, `INCR`, or `GEOADD` path.

## How to decide

Measure the command you touched, from an empty process, and record `DBSIZE`
and `VmRSS` plus a throughput sample. Keep the change only when RSS is not
higher and the command is not slower. Docker Desktop numbers are relative to
the Redis container on the same machine. They are not a bare-metal result and
must not be published as one.

One noisy sample is not a regression and not a win. Repeat it before
publishing a number. A new measurement goes in a new dated file in
`docs/evidence/` (see [its README](evidence/README.md)); older records stay as
they were. The site's [benchmarks page](https://ruviodb.com/benchmarks/) shows the current numbers, and
`README.md` links to them without copying the table.

## Changes that were measured and rejected

- Putting string values back into the key log.
- Putting collections back into one doubling log.
- Keeping a second copy of geographic coordinates next to the geohash score.
- A second network runtime beside the evented shard owner.
- Returning log memory as single 4 KiB (or 256 KiB) blocks. mimalloc keeps
  freed blocks up to 512 KiB, so the log takes and returns 1 MiB extents.

Do not retry those without a new measurement that beats the current layout.

## How to prove it

When adding a command or feature, the "an unused feature costs nothing" rule is
proven by these three checks; a task is not accepted until all three pass.

1. **Allocation counter** - `cargo test --test zero_alloc`. On a pre-warmed
   shard, GET, SET (same length), INCR and DEL+SET make 0 allocations over
   10,000 repetitions. A new hot command is added to `tests/zero_alloc.rs`. The
   `everysec` case stays `#[ignore]`; run it by hand with
   `-- --ignored`.
2. **Size ceiling** - `cargo test --test layout`. Upper bounds on `size_of` for
   `Value`, `Entry`, `ByteBody` and the index slot. If it turns red, the new
   state must live in an `Option<Box<_>>`, a side table or a boxed `Value`
   arm; the ceiling is not raised.
3. **Instruction count (Linux)** - `cargo bench --bench hot_path` (needs
   valgrind and `iai-callgrind-runner 0.16.1`), then
   `python3 scripts/check_instruction_baseline.py <output.log>`. Baselines are
   in `benchmark/baselines/instructions.json`; the tolerance is +2% and at
   least 32 instructions. The baseline is measured on a GitHub runner, because
   counts from another machine differ by several percent: start the CI workflow
   with `jobs=instruction-baseline` and commit the JSON it prints. Refresh it
   for an intentional change and state the reason in the PR. CI: the
   `instruction-count` job in `pr-gate.yml`. macOS has no valgrind, so it is skipped locally.

## Client sockets stay on shard threads

Threads that own client sockets (`io_threads`) measured 0.57-0.85x on Linux 4 cores and slower on macOS, +8-16 us per
request for one client, so the server has none.

## Linux WAL: write zeros ahead, do not only `fallocate`

`always` durability on Linux/ext4: syncing sequential appends costs ~2.0 ms per `fdatasync` on a network SSD when the file grows or is merely `fallocate`d
(unwritten extents), and ~0.8 ms when the blocks were really written with zeros first. The WAL therefore fills each 4 MiB chunk with zeros on a helper thread before the cursor
reaches it. Measured +90% SET throughput at 50 clients. Keep this in mind if the WAL write path changes;
`O_DSYNC`/`O_DIRECT` and keeping several syncs in flight were measured and gave nothing.
