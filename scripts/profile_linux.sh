#!/usr/bin/env bash
set -euo pipefail

MODE="${MODE:-stat}"
SHARDS="${SHARDS:-1}"
SERVER_CORES="${SERVER_CORES:-0}"
CLIENT_CORES="${CLIENT_CORES:-1}"
PORT="${PORT:-6379}"
REQUESTS="${REQUESTS:-1000000}"
CLIENTS="${CLIENTS:-50}"
PIPELINE="${PIPELINE:-16}"
DATA_SIZE="${DATA_SIZE:-64}"
TESTS="${TESTS:-set,get,incr}"
RESULT_DIR="profile/results/$(date -u +%Y%m%dT%H%M%SZ)"

if [[ "$(uname -s)" != "Linux" ]]; then
    echo "profile_linux.sh must run on a Linux host, not inside macOS." >&2
    exit 1
fi

for command in cargo perf redis-cli redis-benchmark taskset; do
    command -v "$command" >/dev/null || { echo "$command is required" >&2; exit 1; }
done

if [[ "$MODE" != "stat" && "$MODE" != "record" && "$MODE" != "both" ]]; then
    echo "MODE must be stat, record, or both" >&2
    exit 1
fi

mkdir -p "$RESULT_DIR"
SERVER_PID=""
PERF_PID=""

cleanup() {
    if [[ -n "$PERF_PID" ]]; then
        kill -INT "$PERF_PID" >/dev/null 2>&1 || true
        wait "$PERF_PID" >/dev/null 2>&1 || true
    fi
    if [[ -n "$SERVER_PID" ]]; then
        kill "$SERVER_PID" >/dev/null 2>&1 || true
        wait "$SERVER_PID" >/dev/null 2>&1 || true
    fi
}
trap cleanup EXIT

RUSTFLAGS="-C target-cpu=native" cargo build --locked --release --bin ruvio

taskset -c "$SERVER_CORES" env \
    RUVIO_SHARDS="$SHARDS" \
    RUVIO_CPUSET="$SERVER_CORES" \
    RUVIO_ADVERTISE_HOST=127.0.0.1 \
    RUVIO_ADDR="127.0.0.1:$PORT" \
    target/release/ruvio >"$RESULT_DIR/server.log" 2>&1 &
SERVER_PID=$!

for shard in $(seq 0 $((SHARDS - 1))); do
    shard_port=$((PORT + shard))
    for _ in $(seq 1 100); do
        if redis-cli -p "$shard_port" PING >/dev/null 2>&1; then
            break
        fi
        sleep 0.05
    done
    redis-cli -p "$shard_port" PING >/dev/null
done

cat >"$RESULT_DIR/environment.txt" <<EOF
timestamp_utc=$(date -u +%Y-%m-%dT%H:%M:%SZ)
kernel=$(uname -a)
rustc=$(rustc --version)
perf=$(perf --version)
shards=$SHARDS
server_cores=$SERVER_CORES
client_cores=$CLIENT_CORES
requests=$REQUESTS
clients=$CLIENTS
pipeline=$PIPELINE
data_size=$DATA_SIZE
tests=$TESTS
perf_event_paranoid=$(cat /proc/sys/kernel/perf_event_paranoid)
EOF
lscpu >"$RESULT_DIR/lscpu.txt"
if command -v numactl >/dev/null; then
    numactl --hardware >"$RESULT_DIR/numa.txt"
fi

run_load() {
    if (( SHARDS > 1 )); then
        taskset -c "$CLIENT_CORES" python3 scripts/benchmark_shards.py \
            --base-port "$PORT" --shards "$SHARDS" \
            --requests-per-shard "$REQUESTS" --clients-per-shard "$CLIENTS" \
            --pipeline "$PIPELINE" --data-size "$DATA_SIZE" \
            | tee "$RESULT_DIR/benchmark.csv"
    else
        taskset -c "$CLIENT_CORES" redis-benchmark -h 127.0.0.1 -p "$PORT" --csv \
            -t "$TESTS" -n "$REQUESTS" -c "$CLIENTS" -P "$PIPELINE" \
            -d "$DATA_SIZE" | tee "$RESULT_DIR/benchmark.csv"
    fi
}

if (( SHARDS > 1 )); then
    taskset -c "$CLIENT_CORES" python3 scripts/benchmark_shards.py \
        --base-port "$PORT" --shards "$SHARDS" \
        --requests-per-shard 10000 --clients-per-shard "$CLIENTS" \
        --pipeline "$PIPELINE" --data-size "$DATA_SIZE" >/dev/null
else
    redis-benchmark -h 127.0.0.1 -p "$PORT" -q -t "$TESTS" \
        -n 50000 -c "$CLIENTS" -P "$PIPELINE" -d "$DATA_SIZE" >/dev/null
fi

if [[ "$MODE" == "stat" || "$MODE" == "both" ]]; then
    perf stat -x, -e cycles,instructions,cache-references,cache-misses,branches,branch-misses,context-switches,cpu-migrations,page-faults \
        -p "$SERVER_PID" -o "$RESULT_DIR/perf-stat.csv" &
    PERF_PID=$!
    run_load
    kill -INT "$PERF_PID" >/dev/null 2>&1 || true
    wait "$PERF_PID" >/dev/null 2>&1 || true
    PERF_PID=""
fi

if [[ "$MODE" == "record" || "$MODE" == "both" ]]; then
    perf record -F 999 -g --call-graph dwarf -p "$SERVER_PID" \
        -o "$RESULT_DIR/perf.data" &
    PERF_PID=$!
    run_load
    kill -INT "$PERF_PID" >/dev/null 2>&1 || true
    wait "$PERF_PID" >/dev/null 2>&1 || true
    PERF_PID=""
    perf report --stdio -i "$RESULT_DIR/perf.data" >"$RESULT_DIR/perf-report.txt"
fi

for shard in $(seq 0 $((SHARDS - 1))); do
    redis-cli -p "$((PORT + shard))" INFO >"$RESULT_DIR/shard-$shard-info.txt"
done
echo "Profile written to $RESULT_DIR"