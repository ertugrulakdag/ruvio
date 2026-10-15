# Scripts

| Group | Scripts |
| --- | --- |
| Run the checks | `test.sh` / `test.ps1` (format, tests, optional `--clippy`, `--native-resilience`, `--docker-resilience`), `test_area.sh`, `run_client_suites.py`, `run_with_servers.sh` |
| Compatibility | `check_compatibility_matrix.py`, `resp_conformance.py`, `redis_differential.py` |
| Release | `release_version.py`, `release_artifacts.sh`, `check_package_metadata.py`, `generate_sbom.py`, `collect_third_party_notices.py` |
| Benchmarks | `bench_*.py`, `redis_compare*.py`, `perf_*.py`, `pinned_run.py`, `docker_compare.py`, `render_*_pages.py` |
| Durability and faults | `kill9_*.py`, `disk_faults.py`, `enospc_check.py`, `native_resilience.py`, `docker_resilience.py`, `soak.py` |
| Profiling | `profile_linux.sh`, `hot_symbols.py`, `command_cost.py`, `pgo.sh` |

Most scripts take the path to a built `ruvio` binary and start their own server on a free port. Read the header of a script before running it; benchmark scripts expect a quiet machine. [docs/TESTING.md](../docs/TESTING.md) says which checks CI runs.
