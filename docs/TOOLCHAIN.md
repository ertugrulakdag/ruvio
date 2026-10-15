# Toolchain Notes

This project favors a small, explicit toolchain so the performance work stays visible:

- `cargo fmt` for formatting.
- `cargo clippy --all-targets --all-features` for static checks.
- `cargo test` for library validation.
- `cargo run --example ...` for targeted architectural demos.
- GitHub Actions for Linux, macOS, and Windows checks, Docker image validation, and pinned Redis client-library suites.
- Linux nightly AddressSanitizer (`asan` job) and Miri (`miri` job).
  Persistence, TCP, Lua, and mimalloc stay out of
  Miri. The parser property test is skipped under Miri (covered by
  `cargo fuzz`); Miri still checks consume bounds on a few garbage frames.
  Reproduce with `scripts/run_sanitizers.sh`.

When you start benchmarking, capture the exact compiler, kernel, and CPU model so throughput numbers remain comparable.