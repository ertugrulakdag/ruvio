#!/usr/bin/env bash
# Run a client test command against throwaway Ruvio servers.
#
#   scripts/run_with_servers.sh <ruvio-binary> <base-port> -- <command...>
#
# Starts a standalone server on <base-port> and a 3-shard server (per-shard
# ports <base-port>+10 .. +12), exports RUVIO_TEST_ADDR / RUVIO_ADDR /
# RUVIO_CLUSTER_ADDR for the command, and stops both servers afterwards.
# The repository ruvio.example.toml is copied, never edited.
set -euo pipefail

binary="$1"
base="$2"
shift 2
[ "$1" = "--" ] && shift

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
work="$(mktemp -d)"
pids=()

cleanup() {
  for pid in "${pids[@]:-}"; do
    [ -n "$pid" ] && kill "$pid" 2>/dev/null || true
  done
  wait 2>/dev/null || true
  rm -rf "$work"
}
trap cleanup EXIT

cp "$root/ruvio.example.toml" "$work/standalone.toml"
sed 's/^shards = 1/shards = 3\
port_mode = "per-shard"/' "$root/ruvio.example.toml" >"$work/cluster.toml"

"$binary" --config "$work/standalone.toml" --address "127.0.0.1:$base" --data-dir "$work/standalone" >"$work/standalone.log" 2>&1 &
pids+=("$!")
"$binary" --config "$work/cluster.toml" --address "127.0.0.1:$((base + 10))" --data-dir "$work/cluster" >"$work/cluster.log" 2>&1 &
pids+=("$!")

wait_for() {
  for _ in $(seq 1 100); do
    if (exec 3<>"/dev/tcp/127.0.0.1/$1") 2>/dev/null; then
      return 0
    fi
    sleep 0.1
  done
  echo "server on port $1 did not start" >&2
  cat "$work"/*.log >&2
  return 1
}
wait_for "$base"
wait_for "$((base + 10))"
wait_for "$((base + 12))"

export RUVIO_ADDR="127.0.0.1:$base"
export RUVIO_TEST_ADDR="127.0.0.1:$base"
export RUVIO_CLUSTER_ADDR="127.0.0.1:$((base + 10))"
"$@"
