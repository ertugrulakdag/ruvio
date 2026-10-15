#!/usr/bin/env bash
# Run the tests for one area. With no area, run `cargo test --lib`.
#
#   scripts/test_area.sh [store|protocol|engine|persistence|server|client-rust|client-csharp|bench] [cargo test args...]
#
# Library areas use a module-path filter. libtest filters are substrings, so
# modules whose path contains the area name (`hot_store::`, `linux_server::`)
# are skipped explicitly.
#
# Client areas talk to a live server at RUVIO_HOST:RUVIO_PORT
# (default 127.0.0.1:6379). The bench area runs the redis-benchmark scripts
# and stays out of `cargo test`; tune it with the variables in
# docs/BENCHMARKING.md.
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$root"

areas="store protocol engine persistence server client-rust client-csharp bench"

usage() {
	echo "usage: $0 [area] [cargo test args...]" >&2
	echo "areas: $areas" >&2
	echo "no area: cargo test --lib" >&2
}

# A hung test must fail the run, not stall it: cargo is stopped after
# RUVIO_TEST_TIMEOUT seconds (default 600). libtest names any test still
# running after 60 seconds, so the log shows which one hung.
limit="${RUVIO_TEST_TIMEOUT:-600}"
timeout_cmd=""
if command -v timeout >/dev/null; then
	timeout_cmd="timeout"
elif command -v gtimeout >/dev/null; then
	timeout_cmd="gtimeout"
fi

run() {
	echo "+ $*" >&2
	if [[ -n "$timeout_cmd" && "$1" == cargo ]]; then
		"$timeout_cmd" "$limit" "$@"
	else
		"$@"
	fi
}

lib_area() {
	local filter="$1"
	shift
	local skips=()
	while [[ $# -gt 0 && "$1" == --skip=* ]]; do
		skips+=(--skip "${1#--skip=}")
		shift
	done
	run cargo test "$@" --lib -- "$filter" ${skips[@]+"${skips[@]}"}
}

clients() {
	run python3 scripts/run_client_suites.py \
		--host "${RUVIO_HOST:-127.0.0.1}" \
		--port "${RUVIO_PORT:-6379}" \
		--languages "$1" \
		--require-all
}

if [[ $# -eq 0 ]]; then
	run cargo test --lib
	exit 0
fi

area="$1"
shift

case "$area" in
store)
	lib_area store:: --skip=hot_store:: "$@"
	run cargo test "$@" --test store_workflow --test storage_properties
	;;
protocol)
	lib_area protocol:: "$@"
	;;
engine)
	lib_area engine:: "$@"
	;;
persistence)
	lib_area persistence:: "$@"
	;;
server)
	lib_area server:: --skip=linux_server:: "$@"
	run cargo test "$@" --test concurrency_model
	;;
client-rust)
	clients ruvio-client
	;;
client-csharp)
	clients ruvio-csharp
	;;
bench)
	run python3 scripts/test_benchmark_regression.py
	run ./scripts/benchmark.sh
	;;
-h | --help)
	usage
	;;
*)
	echo "unknown area: $area" >&2
	usage
	exit 2
	;;
esac
