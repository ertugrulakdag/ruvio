# Benchmarks

`hot_path.rs` counts instructions for one command on a warm shard with iai-callgrind (Linux, needs `valgrind`).

```bash
cargo install iai-callgrind-runner --version 0.16.1
cargo bench --bench hot_path
```

`scripts/check_instruction_baseline.py` compares the totals with `benchmark/baselines/instructions.json`. Throughput and memory comparisons against Redis are in [docs/BENCHMARKING.md](../docs/BENCHMARKING.md).
