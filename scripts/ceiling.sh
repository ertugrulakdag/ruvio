#!/bin/bash
# Host single-core ceiling. Does not start or stop the Docker redis/ruvio/ruvio-ui containers.
set -euo pipefail

root="$(cd "$(dirname "$0")/.." && pwd)"
# shellcheck disable=SC1091
source "$root/scripts/ceiling_logic.sh"

PORT=6411
REQUESTS=1000000
WARMUP=100000
DATA_SIZE=64
GRID="pipeline=16,32,64,128;clients=50,100,200,400;requests=1000000;data=64"
SERVER_PID=""
RESULT_DIR=""

case "$(uname -s)" in
    MINGW*|MSYS*|CYGWIN*) windows=1 ;;
    *) windows=0 ;;
esac

die() {
    echo "$*" >&2
    exit 1
}

cpu_seconds() {
    local pid="$1"
    if [[ "$windows" -eq 1 ]]; then
        powershell.exe -NoProfile -Command "(Get-Process -Id $pid -ErrorAction Stop).TotalProcessorTime.TotalSeconds.ToString([Globalization.CultureInfo]::InvariantCulture)"
        return
    fi
    ps -p "$pid" -o time= | awk -F: '{
        if (NF == 3) seconds = $1 * 3600 + $2 * 60 + $3
        else seconds = $1 * 60 + $2
        printf "%.6f\n", seconds
    }'
}

core_percent() {
    awk -v before="$1" -v after="$2" -v rps="$3" -v n="$REQUESTS" 'BEGIN {
        active = n / rps
        if (active <= 0) { print "0"; exit }
        printf "%.4f\n", (after - before) / active * 100
    }'
}

port_busy() {
    redis-cli -h 127.0.0.1 -p "$PORT" PING 2>/dev/null | tr -d '\r' | grep -q PONG
}

stop_server() {
    if [[ -z "$SERVER_PID" ]]; then
        return
    fi
    if [[ "$windows" -eq 1 ]]; then
        powershell.exe -NoProfile -Command "Stop-Process -Id $SERVER_PID -Force -ErrorAction SilentlyContinue" >/dev/null 2>&1 || true
    else
        kill "$SERVER_PID" >/dev/null 2>&1 || true
    fi
    SERVER_PID=""
}

trap stop_server EXIT

command -v redis-benchmark >/dev/null || die "redis-benchmark is required on PATH"
command -v redis-cli >/dev/null || die "redis-cli is required on PATH"
port_busy && die "port $PORT is in use; refusing to stop the existing listener"

stamp="$(date -u +%Y%m%dT%H%M%SZ)"
os_name="$(uname -s | tr '[:upper:]' '[:lower:]' | tr -cd 'a-z0-9')"
RESULT_DIR="$root/benchmark/results/ceiling-${os_name}-${stamp}"
mkdir -p "$RESULT_DIR/data"
printf 'command,phase,clients,pipeline,rps,p50_ms,p99_ms,server_core_percent,client_core_percent\n' >"$RESULT_DIR/grid.csv"

if [[ -z "${RUVIO_BIN:-}" ]]; then
    (cd "$root" && RUSTFLAGS="-C target-cpu=native" cargo build --locked --release --bin ruvio)
    if [[ "$windows" -eq 1 ]]; then
        RUVIO_BIN="$root/target/release/ruvio.exe"
    else
        RUVIO_BIN="$root/target/release/ruvio"
    fi
fi
[[ -x "$RUVIO_BIN" || -f "$RUVIO_BIN" ]] || die "ruvio binary not found at $RUVIO_BIN"

start_server() {
    if [[ "$windows" -eq 1 ]]; then
        local bin data err
        bin="$(cygpath -w "$RUVIO_BIN")"
        data="$(cygpath -w "$RESULT_DIR/data")"
        err="$(cygpath -w "$RESULT_DIR/server.err")"
        SERVER_PID="$(powershell.exe -NoProfile -Command "\$env:RUVIO_ADDR='127.0.0.1:$PORT'; \$env:RUVIO_SHARDS='1'; \$env:RUVIO_DURABILITY='memory'; \$env:RUVIO_DATA_DIR='$data'; \$p = Start-Process -FilePath '$bin' -WorkingDirectory '$data' -PassThru -WindowStyle Hidden -RedirectStandardError '$err'; \$p.Id" | tr -d '\r' | awk 'NF { line=$0 } END { print line }')"
    else
        local saved="$PWD"
        cd "$RESULT_DIR/data"
        RUVIO_ADDR="127.0.0.1:$PORT" \
            RUVIO_SHARDS=1 \
            RUVIO_DURABILITY=memory \
            RUVIO_DATA_DIR="$RESULT_DIR/data" \
            "$RUVIO_BIN" >"$RESULT_DIR/server.err" 2>&1 &
        SERVER_PID=$!
        cd "$saved"
    fi
    [[ "$SERVER_PID" =~ ^[0-9]+$ ]] || die "failed to start ruvio"
}

start_server
for _ in $(seq 1 50); do
    if port_busy; then
        break
    fi
    sleep 0.1
done
port_busy || die "ruvio did not answer PING on $PORT"
backend="$(redis-cli -h 127.0.0.1 -p "$PORT" INFO | tr -d '\r' | sed -n 's/^ruvio_backend://p')"
[[ "$backend" == "evented" ]] || die "ruvio_backend is '$backend'"

# Sets RPS P50 P99 SERVER_PCT CLIENT_PCT from one redis-benchmark invocation.
measure() {
    local command="$1" phase="$2" clients="$3" pipeline="$4"
    local csv="$RESULT_DIR/last.csv"
    local server_before server_after client_cpu line
    server_before="$(cpu_seconds "$SERVER_PID" | tr -d '\r' | awk 'NF { line=$0 } END { print line }')"
    if [[ "$windows" -eq 1 ]]; then
        local csv_win bench
        csv_win="$(cygpath -w "$csv")"
        bench="$(command -v redis-benchmark)"
        bench="$(cygpath -w "$bench")"
        client_cpu="$(powershell.exe -NoProfile -Command "\$p = Start-Process -FilePath '$bench' -ArgumentList @('-h','127.0.0.1','-p','$PORT','-n','$REQUESTS','-c','$clients','-P','$pipeline','-d','$DATA_SIZE','-t','$command','--csv') -PassThru -Wait -NoNewWindow -RedirectStandardOutput '$csv_win'; \$p.TotalProcessorTime.TotalSeconds.ToString([Globalization.CultureInfo]::InvariantCulture)" | tr -d '\r' | awk 'NF { line=$0 } END { print line }')"
    else
        /usr/bin/time -p redis-benchmark -h 127.0.0.1 -p "$PORT" -n "$REQUESTS" -c "$clients" -P "$pipeline" -d "$DATA_SIZE" -t "$command" --csv >"$csv" 2>"$RESULT_DIR/time.txt"
        client_cpu="$(awk '/^user/ { u=$2 } /^sys/ { s=$2 } END { printf "%.6f\n", u + s }' "$RESULT_DIR/time.txt")"
    fi
    server_after="$(cpu_seconds "$SERVER_PID" | tr -d '\r' | awk 'NF { line=$0 } END { print line }')"
    line="$(tr -d '\r' <"$csv" | grep "\"$(printf '%s' "$command" | tr '[:lower:]' '[:upper:]')\"" | tail -n 1)"
    [[ -n "$line" ]] || die "redis-benchmark produced no CSV for $command"
    line="${line//\"/}"
    IFS=',' read -r _test RPS _avg _min P50 _p95 P99 _max <<<"$line"
    SERVER_PCT="$(core_percent "$server_before" "$server_after" "$RPS")"
    CLIENT_PCT="$(core_percent 0 "$client_cpu" "$RPS")"
    printf '%s,%s,%s,%s,%s,%s,%s,%s,%s\n' \
        "$(printf '%s' "$command" | tr '[:lower:]' '[:upper:]')" \
        "$phase" "$clients" "$pipeline" "$RPS" "$P50" "$P99" "$SERVER_PCT" "$CLIENT_PCT" >>"$RESULT_DIR/grid.csv"
}

warmup() {
    local command="$1"
    redis-benchmark -h 127.0.0.1 -p "$PORT" -n "$WARMUP" -c 50 -P 16 -d "$DATA_SIZE" -t "$command" -q >/dev/null
}

search_command() {
    local command="$1"
    local decision prev_rps="" prev_pipeline="" kept_pipeline="" stop_all=0
    local candidate_clients="" candidate_pipeline="" base_rps=""
    warmup "$command"
    local pipeline
    for pipeline in 16 32 64 128; do
        measure "$command" pipeline 50 "$pipeline"
        if awk -v cpu="$SERVER_PCT" 'BEGIN { exit !(cpu + 0 >= 90) }'; then
            candidate_clients=50
            candidate_pipeline=$pipeline
            stop_all=1
            break
        fi
        if [[ -z "$prev_rps" ]]; then
            prev_rps=$RPS
            prev_pipeline=$pipeline
            kept_pipeline=$pipeline
            continue
        fi
        decision="$(ceiling_decide "$prev_rps" "$RPS" "$SERVER_PCT")"
        case "$decision" in
            saturated)
                candidate_clients=50
                candidate_pipeline=$pipeline
                stop_all=1
                break
                ;;
            plateau)
                kept_pipeline=$prev_pipeline
                break
                ;;
            continue)
                prev_rps=$RPS
                prev_pipeline=$pipeline
                kept_pipeline=$pipeline
                ;;
        esac
    done
    if [[ "$stop_all" -eq 0 ]]; then
        base_rps=$prev_rps
        candidate_clients=50
        candidate_pipeline=$kept_pipeline
        local clients
        for clients in 100 200 400; do
            measure "$command" clients "$clients" "$kept_pipeline"
            if awk -v cpu="$SERVER_PCT" 'BEGIN { exit !(cpu + 0 >= 90) }'; then
                candidate_clients=$clients
                candidate_pipeline=$kept_pipeline
                break
            fi
            decision="$(ceiling_decide "$base_rps" "$RPS" "$SERVER_PCT")"
            case "$decision" in
                saturated)
                    candidate_clients=$clients
                    candidate_pipeline=$kept_pipeline
                    break
                    ;;
                plateau)
                    break
                    ;;
                continue)
                    base_rps=$RPS
                    candidate_clients=$clients
                    ;;
            esac
        done
    fi
    CANDIDATE_CLIENTS=$candidate_clients
    CANDIDATE_PIPELINE=$candidate_pipeline
}

median3() {
    printf '%s\n' "$1" "$2" "$3" | sort -n | awk 'NR==2 { printf "%.6f\n", $1 }'
}

confirm_command() {
    local command="$1" clients="$2" pipeline="$3"
    local upper sample_file i
    upper="$(printf '%s' "$command" | tr '[:lower:]' '[:upper:]')"
    sample_file="$RESULT_DIR/${upper}.samples"
    : >"$sample_file"
    for i in 1 2 3; do
        measure "$command" confirm "$clients" "$pipeline"
        printf '%s %s %s %s %s\n' "$RPS" "$P50" "$P99" "$SERVER_PCT" "$CLIENT_PCT" >>"$sample_file"
    done
    local s1_rps s1_p50 s1_p99 s1_srv s1_cli
    local s2_rps s2_p50 s2_p99 s2_srv s2_cli
    local s3_rps s3_p50 s3_p99 s3_srv s3_cli
    {
        read -r s1_rps s1_p50 s1_p99 s1_srv s1_cli
        read -r s2_rps s2_p50 s2_p99 s2_srv s2_cli
        read -r s3_rps s3_p50 s3_p99 s3_srv s3_cli
    } <"$sample_file"
    MED_RPS="$(median3 "$s1_rps" "$s2_rps" "$s3_rps")"
    MED_P50="$(median3 "$s1_p50" "$s2_p50" "$s3_p50")"
    MED_P99="$(median3 "$s1_p99" "$s2_p99" "$s3_p99")"
    MED_SERVER="$(median3 "$s1_srv" "$s2_srv" "$s3_srv")"
    MED_CLIENT="$(median3 "$s1_cli" "$s2_cli" "$s3_cli")"
    if awk -v cpu="$MED_SERVER" 'BEGIN { exit !(cpu + 0 >= 90) }'; then
        CMD_STATUS=saturated
    else
        CMD_STATUS=client-bound
    fi
}

json_samples() {
    local file="$1"
    awk 'BEGIN { printf "[" }
        {
            if (NR > 1) printf ","
            printf "{\"rps\":%s,\"p50_ms\":%s,\"p99_ms\":%s,\"server_core_percent\":%s,\"client_core_percent\":%s}", $1, $2, $3, $4, $5
        }
        END { printf "]" }' "$file"
}

host_cpu() {
    if [[ "$windows" -eq 1 ]]; then
        powershell.exe -NoProfile -Command "(Get-CimInstance Win32_Processor | Select-Object -First 1 -ExpandProperty Name)" | tr -d '\r' | awk '{$1=$1; print}'
    elif sysctl -n machdep.cpu.brand_string >/dev/null 2>&1; then
        sysctl -n machdep.cpu.brand_string
    elif command -v lscpu >/dev/null; then
        lscpu | awk -F: '/Model name/ { sub(/^[ \t]+/, "", $2); print $2; exit }'
    else
        printf '%s\n' unknown
    fi
}

cd "$root"
git_commit="$(git rev-parse HEAD 2>/dev/null || echo unknown)"
if [[ -n "$(git status --porcelain 2>/dev/null || true)" ]]; then
    git_dirty=true
else
    git_dirty=false
fi
host_os="$(uname -sr)"
host_arch="$(uname -m)"
cpu_name="$(host_cpu)"
bench_version="$(redis-benchmark --version 2>&1 | tr -d '\r' | head -n 1)"

for command in set get incr; do
    search_command "$command"
    confirm_command "$command" "$CANDIDATE_CLIENTS" "$CANDIDATE_PIPELINE"
    upper="$(printf '%s' "$command" | tr '[:lower:]' '[:upper:]')"
    eval "STATUS_$upper=\$CMD_STATUS"
    eval "CLIENTS_$upper=\$CANDIDATE_CLIENTS"
    eval "PIPELINE_$upper=\$CANDIDATE_PIPELINE"
    eval "MED_RPS_$upper=\$MED_RPS"
    eval "MED_P50_$upper=\$MED_P50"
    eval "MED_P99_$upper=\$MED_P99"
    eval "MED_SERVER_$upper=\$MED_SERVER"
    eval "MED_CLIENT_$upper=\$MED_CLIENT"
done

file_status="$(ceiling_file_status "$STATUS_SET" "$STATUS_GET" "$STATUS_INCR")"

{
    printf 'host_os=%s\n' "$host_os"
    printf 'host_arch=%s\n' "$host_arch"
    printf 'host_cpu=%s\n' "$cpu_name"
    printf 'git_commit=%s\n' "$git_commit"
    printf 'git_dirty=%s\n' "$git_dirty"
    printf 'redis_benchmark_version=%s\n' "$bench_version"
    printf 'ruvio_backend=evented\n'
    printf 'grid=%s\n' "$GRID"
} >"$RESULT_DIR/environment.txt"

command_json() {
    local name="$1"
    local status clients pipeline rps p50 p99 server client
    eval "status=\$STATUS_$name"
    eval "clients=\$CLIENTS_$name"
    eval "pipeline=\$PIPELINE_$name"
    eval "rps=\$MED_RPS_$name"
    eval "p50=\$MED_P50_$name"
    eval "p99=\$MED_P99_$name"
    eval "server=\$MED_SERVER_$name"
    eval "client=\$MED_CLIENT_$name"
    printf '{"status":"%s","clients":%s,"pipeline":%s,"median_rps":%s,"median_p50_ms":%s,"median_p99_ms":%s,"median_server_core_percent":%s,"median_client_core_percent":%s,"samples":%s}' \
        "$status" "$clients" "$pipeline" "$rps" "$p50" "$p99" "$server" "$client" "$(json_samples "$RESULT_DIR/${name}.samples")"
}

{
    printf '{\n'
    printf '  "schema_version": 1,\n'
    printf '  "role": "single-core-ceiling",\n'
    printf '  "status": "%s",\n' "$file_status"
    printf '  "grid": "%s",\n' "$GRID"
    printf '  "git_commit": "%s",\n' "$git_commit"
    printf '  "git_dirty": %s,\n' "$git_dirty"
    printf '  "host_os": "%s",\n' "$host_os"
    printf '  "host_arch": "%s",\n' "$host_arch"
    printf '  "host_cpu": "%s",\n' "$cpu_name"
    printf '  "redis_benchmark_version": "%s",\n' "$bench_version"
    printf '  "ruvio_backend": "evented",\n'
    printf '  "commands": {\n'
    printf '    "SET": %s,\n' "$(command_json SET)"
    printf '    "GET": %s,\n' "$(command_json GET)"
    printf '    "INCR": %s\n' "$(command_json INCR)"
    printf '  }\n'
    printf '}\n'
} >"$RESULT_DIR/summary.json"

echo "$RESULT_DIR"
