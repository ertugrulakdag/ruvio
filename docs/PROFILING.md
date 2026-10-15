# Bare-Metal Linux Profiling

Use a dedicated Linux host for actionable CPU and cache data. Docker Desktop results include a virtual machine and are useful only as development baselines.

## Prerequisites

- `perf`, commonly provided by `linux-tools` or `perf`
- `redis-cli` and `redis-benchmark`
- `taskset`, commonly provided by `util-linux`
- Optional `numactl` for NUMA topology capture

The server and load generator must use disjoint CPU sets. Disable unrelated workloads and record CPU frequency policy, SMT state, kernel version, and NUMA topology.

## Counter Profile

```bash
SERVER_CORES=2 CLIENT_CORES=4-7 MODE=stat ./scripts/profile_linux.sh
```

This records cycles, instructions, cache references/misses, branch misses, context switches, migrations, page faults, benchmark output, and `INFO` metrics.

## Call Graph

```bash
SERVER_CORES=2 CLIENT_CORES=4-7 MODE=record ./scripts/profile_linux.sh
```

Inspect the generated report under `profile/results/`. Run at least five samples for each configuration and compare medians rather than selecting the best run.

Profile four shards with one pinned server CPU per shard and separate load-generator CPUs:

```bash
SHARDS=4 SERVER_CORES=2-5 CLIENT_CORES=8-11 MODE=both \
	REQUESTS=500000 CLIENTS=25 ./scripts/profile_linux.sh
```

The profile captures the whole process and writes one `INFO` snapshot per shard.

If `perf` reports a permission error, adjust `kernel.perf_event_paranoid` according to the host security policy. Do not grant elevated container privileges merely to collect benchmark numbers.