[Ruvio](../../README.md) / [Scripts](../README.md) / Attic

# One-off experiments

Probes and A/B runs that produced a recorded result once. They are kept so a past measurement can be traced back to the exact script, not maintained.

- Several assume a specific host: absolute `/root/...` or `/mnt/data` paths, fixed CPU pins, Linux-only tools (`taskset`, `redis-benchmark`).
- `bench_get_p16.py`, `bench_rss.py` and `bench_soak.py` load a helper that is not in the repository and do not run as they are.
- `bench_always_shards.sh` and `bench_forward_group.sh` stop every `target/release/ruvio` process on the machine.

To repeat one of these measurements, copy the script out, fix its paths, and build the result on the shared helpers in `scripts/` instead.

| Script | What it measured |
| --- | --- |
| `baremetal_compare.py`, `durability_rebench.py` | The 2026-09-28 bare-metal comparison in [docs/evidence/2026-09-28-bare-metal.md](../../docs/evidence/2026-09-28-bare-metal.md) |
| `extra_runs.py` | Concurrency and durability scenarios against Redis |
| `bench_prealloc_ab.py`, `bench_shards_ab.py`, `bench_worker_repl.py`, `bench_always_shards.sh`, `bench_forward_group.sh` | Sync worker and shard-count A/B runs under `always` durability |
| `bench_get_p16.py`, `bench_rss.py`, `bench_soak.py` | GET pipeline, RSS and soak runs from one benchmark session |
| `fsync_extent_probe.py`, `fsync_modes_probe.py`, `fsync_parallel_probe.py` | Raw `fsync` cost on a file system, outside Ruvio |
