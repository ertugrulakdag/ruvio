#!/usr/bin/env bash
set -euo pipefail

REQUESTS="${REQUESTS:-100000}"
CLIENTS="${CLIENTS:-50}"
PIPELINE="${PIPELINE:-16}"
DATA_SIZE="${DATA_SIZE:-64}"
TESTS="${TESTS:-set,get,incr}"
CPU_LIMIT="${CPU_LIMIT:-2}"
MEMORY_LIMIT="${MEMORY_LIMIT:-512m}"
RUVIO_SHARDS="${RUVIO_SHARDS:-1}"
RUVIO_ALLOCATOR="${RUVIO_ALLOCATOR:-mimalloc}"
REDIS_PORT="${REDIS_PORT:-16379}"
RUVIO_PORT="${RUVIO_PORT:-16380}"
SAMPLES="${SAMPLES:-1}"
BASELINE="${BASELINE:-}"
WRITE_BASELINE="${WRITE_BASELINE:-}"
MAX_RPS_REGRESSION_PERCENT="${MAX_RPS_REGRESSION_PERCENT:-}"
MAX_P99_REGRESSION_PERCENT="${MAX_P99_REGRESSION_PERCENT:-}"

REDIS_CONTAINER="ruvio-bench-redis"
RUVIO_CONTAINER="ruvio-bench-server"
RESULT_DIR="${RESULT_DIR:-benchmark/results/$(date -u +%Y%m%dT%H%M%SZ)}"

cleanup() {
    docker rm --force "$REDIS_CONTAINER" "$RUVIO_CONTAINER" >/dev/null 2>&1 || true
}
trap cleanup EXIT

command -v docker >/dev/null || { echo "docker is required" >&2; exit 1; }
command -v redis-benchmark >/dev/null || { echo "redis-benchmark is required" >&2; exit 1; }
command -v python3 >/dev/null || { echo "python3 is required" >&2; exit 1; }
docker info >/dev/null
[[ "$SAMPLES" =~ ^[1-9][0-9]*$ ]] || { echo "SAMPLES must be a positive integer" >&2; exit 1; }

mkdir -p "$RESULT_DIR"
cleanup

RUVIO_IMAGE="ruvio:benchmark"
case "$RUVIO_ALLOCATOR" in
    mimalloc) CARGO_ARGS="" ;;
    system) CARGO_ARGS="--no-default-features" ;;
    *) echo "RUVIO_ALLOCATOR must be mimalloc or system" >&2; exit 1 ;;
esac
docker build --file Dockerfile.linux \
    --build-arg CARGO_ARGS="$CARGO_ARGS" \
    --tag "$RUVIO_IMAGE" .
docker pull redis:8-alpine

docker run --detach --name "$REDIS_CONTAINER" \
    --cpus "$CPU_LIMIT" --memory "$MEMORY_LIMIT" \
    --publish "$REDIS_PORT:6379" \
    redis:8-alpine redis-server --save '' --appendonly no --protected-mode no >/dev/null

if (( RUVIO_SHARDS > 1 )); then
    RUVIO_END_PORT=$((RUVIO_PORT + RUVIO_SHARDS - 1))
    docker run --detach --name "$RUVIO_CONTAINER" \
        --env RUVIO_ALLOW_UNAUTHENTICATED=true \
        --cpus "$CPU_LIMIT" --memory "$MEMORY_LIMIT" \
        --env RUVIO_ADDR="0.0.0.0:$RUVIO_PORT" \
        --env RUVIO_SHARDS="$RUVIO_SHARDS" \
        --env RUVIO_ADVERTISE_HOST=127.0.0.1 \
        --publish "$RUVIO_PORT-$RUVIO_END_PORT:$RUVIO_PORT-$RUVIO_END_PORT" \
        "$RUVIO_IMAGE" >/dev/null
else
    docker run --detach --name "$RUVIO_CONTAINER" \
        --env RUVIO_ALLOW_UNAUTHENTICATED=true \
        --cpus "$CPU_LIMIT" --memory "$MEMORY_LIMIT" \
        --publish "$RUVIO_PORT:6379" \
        "$RUVIO_IMAGE" >/dev/null
fi

READY_PORTS=("$REDIS_PORT" "$RUVIO_PORT")
if (( RUVIO_SHARDS > 1 )); then
    for offset in $(seq 1 $((RUVIO_SHARDS - 1))); do
        READY_PORTS+=("$((RUVIO_PORT + offset))")
    done
fi

for port in "${READY_PORTS[@]}"; do
    for _ in $(seq 1 100); do
        if redis-cli -p "$port" PING >/dev/null 2>&1; then
            break
        fi
        sleep 0.1
    done
    redis-cli -p "$port" PING >/dev/null
done

reported_backend="$(redis-cli -p "$RUVIO_PORT" INFO | sed -n 's/^ruvio_backend://p' | tr -d '\r')"
if [[ "$reported_backend" != "evented" ]]; then
    echo "compiled adapter is '$reported_backend'" >&2
    exit 1
fi

benchmark() {
    local name="$1"
    local port="$2"
    local sample="$3"

    redis-benchmark -h 127.0.0.1 -p "$port" -q -t "$TESTS" \
        -n 10000 -c "$CLIENTS" -P "$PIPELINE" -d "$DATA_SIZE" >/dev/null

    redis-benchmark -h 127.0.0.1 -p "$port" --csv -t "$TESTS" \
        -n "$REQUESTS" -c "$CLIENTS" -P "$PIPELINE" -d "$DATA_SIZE" \
        | tee "$RESULT_DIR/$name-$sample.csv"
}

benchmark_cluster() {
    local name="$1"
    local port="$2"
    local sample="$3"

    redis-benchmark --cluster -h 127.0.0.1 -p "$port" -q -t "$TESTS" \
        -n 10000 -c "$CLIENTS" -P "$PIPELINE" -d "$DATA_SIZE" >/dev/null

    redis-benchmark --cluster -h 127.0.0.1 -p "$port" --csv -t "$TESTS" \
        -n "$REQUESTS" -c "$CLIENTS" -P "$PIPELINE" -d "$DATA_SIZE" \
        | tee "$RESULT_DIR/$name-$sample.csv"
}

HOST_OS="$(sw_vers -productVersion 2>/dev/null || uname -sr)"
HOST_ARCH="$(uname -m)"
HOST_CPU="$(sysctl -n machdep.cpu.brand_string 2>/dev/null \
    || lscpu 2>/dev/null | awk -F: '/Model name/ {sub(/^[ \t]+/, "", $2); print $2; exit}' \
    || echo unknown)"
GIT_COMMIT="$(git rev-parse HEAD 2>/dev/null || echo unknown)"
if [[ -n "$(git status --porcelain 2>/dev/null || true)" ]]; then
    GIT_DIRTY=true
else
    GIT_DIRTY=false
fi

cat > "$RESULT_DIR/environment.txt" <<EOF
timestamp_utc=$(date -u +%Y-%m-%dT%H:%M:%SZ)
host=$(uname -a)
host_os=$HOST_OS
host_arch=$HOST_ARCH
host_cpu=$HOST_CPU
docker_server=$(docker version --format '{{.Server.Version}}')
git_commit=$GIT_COMMIT
git_dirty=$GIT_DIRTY
requests=$REQUESTS
clients=$CLIENTS
pipeline=$PIPELINE
data_size=$DATA_SIZE
tests=$TESTS
cpu_limit=$CPU_LIMIT
memory_limit=$MEMORY_LIMIT
ruvio_backend=evented
ruvio_shards=$RUVIO_SHARDS
ruvio_allocator=$RUVIO_ALLOCATOR
samples=$SAMPLES
redis_image=$(docker image inspect redis:8-alpine --format '{{.Id}}')
ruvio_image=$(docker image inspect "$RUVIO_IMAGE" --format '{{.Id}}')
EOF

run_redis_sample() {
    local sample="$1"
    echo "Sample $sample/$SAMPLES: Redis benchmark"
    benchmark redis "$REDIS_PORT" "$sample"
}

run_ruvio_sample() {
    local sample="$1"
    echo "Sample $sample/$SAMPLES: Ruvio benchmark"
    if (( RUVIO_SHARDS > 1 )); then
        benchmark_cluster ruvio "$RUVIO_PORT" "$sample"
    else
        benchmark ruvio "$RUVIO_PORT" "$sample"
    fi
}

for sample_number in $(seq 1 "$SAMPLES"); do
    sample="$(printf '%02d' "$sample_number")"
    if (( sample_number % 2 == 1 )); then
        run_redis_sample "$sample"
        run_ruvio_sample "$sample"
    else
        run_ruvio_sample "$sample"
        run_redis_sample "$sample"
    fi
done

# Preserve the original single-file names for ad-hoc tooling.
cp "$RESULT_DIR/redis-01.csv" "$RESULT_DIR/redis.csv"
cp "$RESULT_DIR/ruvio-01.csv" "$RESULT_DIR/ruvio.csv"

for shard in $(seq 0 $((RUVIO_SHARDS - 1))); do
    redis-cli -p "$((RUVIO_PORT + shard))" INFO \
        >"$RESULT_DIR/ruvio-shard-$shard-info.txt"
done

SUMMARY_ARGS=("$RESULT_DIR")
[[ -z "$BASELINE" ]] || SUMMARY_ARGS+=(--baseline "$BASELINE")
[[ -z "$WRITE_BASELINE" ]] || SUMMARY_ARGS+=(--write-baseline "$WRITE_BASELINE")
[[ -z "$MAX_RPS_REGRESSION_PERCENT" ]] \
    || SUMMARY_ARGS+=(--max-rps-regression-percent "$MAX_RPS_REGRESSION_PERCENT")
[[ -z "$MAX_P99_REGRESSION_PERCENT" ]] \
    || SUMMARY_ARGS+=(--max-p99-regression-percent "$MAX_P99_REGRESSION_PERCENT")
python3 scripts/benchmark_regression.py "${SUMMARY_ARGS[@]}"

echo "Results written to $RESULT_DIR"