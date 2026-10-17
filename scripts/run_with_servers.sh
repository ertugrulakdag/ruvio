#!/usr/bin/env bash
# Run a client test command against throwaway Ruvio servers.
#
#   scripts/run_with_servers.sh <ruvio-binary> <base-port> -- <command...>
#
# Starts a standalone server on <base-port> and a 3-shard server (per-shard
# ports <base-port>+10 .. +12), exports RUVIO_TEST_ADDR / RUVIO_ADDR /
# RUVIO_CLUSTER_ADDR for the command, and stops both servers afterwards.
# The repository ruvio.example.toml is copied, never edited.
#
# Each port must answer PING with PONG from the server this script started
# (checked through its pid and CONFIG GET dir) before the command runs. When
# the command fails, the server logs are printed and kept.
set -euo pipefail

binary="$1"
base="$2"
shift 2
[ "$1" = "--" ] && shift

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
work="$(mktemp -d)"
pids=()

print_logs() {
  for log in "$work"/*.log; do
    [ -f "$log" ] || continue
    echo "----- $log -----" >&2
    cat "$log" >&2
  done
}

# Stops one server: SIGTERM, up to 10 s to exit, then SIGKILL.
stop() {
  local pid="$1"

  kill "$pid" 2>/dev/null || return 0
  for _ in $(seq 1 100); do
    kill -0 "$pid" 2>/dev/null || break
    sleep 0.1
  done
  if kill -0 "$pid" 2>/dev/null; then
    echo "server $pid did not stop within 10 s; killing it" >&2
    kill -9 "$pid" 2>/dev/null || true
  fi
  wait "$pid" 2>/dev/null || true
}

cleanup() {
  local status=$?

  for pid in "${pids[@]:-}"; do
    [ -n "$pid" ] && stop "$pid"
  done
  if [ "$status" -ne 0 ]; then
    print_logs
    echo "server logs kept in $work" >&2
  else
    rm -rf "$work"
  fi
  exit "$status"
}
trap cleanup EXIT

# A short shutdown grace: a client that leaves connections open must not hold up the stop.
sed 's/^shutdown_grace_ms = .*/shutdown_grace_ms = 2000/' "$root/ruvio.example.toml" >"$work/standalone.toml"
sed 's/^shards = 1/shards = 3\
port_mode = "per-shard"/' "$work/standalone.toml" >"$work/cluster.toml"

"$binary" --config "$work/standalone.toml" --address "127.0.0.1:$base" --data-dir "$work/standalone" >"$work/standalone.log" 2>&1 &
pids+=("$!")
"$binary" --config "$work/cluster.toml" --address "127.0.0.1:$((base + 10))" --data-dir "$work/cluster" >"$work/cluster.log" 2>&1 &
pids+=("$!")

ready() {
  python3 "$root/scripts/_ruvio.py" wait-ready --timeout 30 --port "$@"
}
ready "$base" --pid "${pids[0]}" --data-dir "$work/standalone"
for port in $((base + 10)) $((base + 11)) $((base + 12)); do
  ready "$port" --pid "${pids[1]}" --data-dir "$work/cluster"
done
ready "$((base + 10))" --pid "${pids[1]}" --cluster

export RUVIO_ADDR="127.0.0.1:$base"
export RUVIO_TEST_ADDR="127.0.0.1:$base"
export RUVIO_CLUSTER_ADDR="127.0.0.1:$((base + 10))"
"$@"
