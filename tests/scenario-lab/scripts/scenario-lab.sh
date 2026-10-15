#!/usr/bin/env bash
set -euo pipefail

lab_dir="$(cd "$(dirname "$0")/.." && pwd)"
cd "$lab_dir"

command_name="${1:-}"
shift || true

if [[ ! -f .env ]]; then
  echo "Missing tests/scenario-lab/.env; copy .env.example and configure both hosts." >&2
  exit 2
fi

require_compose() {
  docker compose version >/dev/null
}

compose_sut() {
  docker compose -f compose.sut.yml "$@"
}

compose_observer() {
  docker compose -f compose.observer.yml "$@"
}

prepare_observer_config() {
  python3 scripts/render-prometheus-config.py
}

run_k6() {
  local scenario="$1"
  local run_id
  run_id="$(date -u +%Y%m%dT%H%M%SZ)-$$"
  mkdir -p load/results
  if [[ "$scenario" == stress ]]; then
    run_stress_with_watchdog "$run_id"
    return
  fi
  if [[ "$scenario" == sustained && "${ENABLE_RUVIO_PARTITION:-false}" == true ]]; then
    local fault_control_url="${FAULT_CONTROL_URL:-$(sed -n 's/^FAULT_CONTROL_URL=//p' .env | tail -n 1)}"
    if [[ -z "$fault_control_url" ]]; then
      echo "Set FAULT_CONTROL_URL in tests/scenario-lab/.env before enabling fault injection." >&2
      return 2
    fi

    restore_proxy() {
      curl --fail --silent --show-error --max-time 5 \
        -H 'Content-Type: application/json' \
        --data '{"enabled":true}' \
        "${fault_control_url%/}/proxies/ruvio" >/dev/null ||
        echo "WARNING: automatic Toxiproxy recovery failed; restore the ruvio proxy manually." >&2
    }

    curl --fail --silent --show-error --max-time 5 \
      -H 'Content-Type: application/json' \
      --data '{"enabled":true}' \
      "${fault_control_url%/}/proxies/ruvio" >/dev/null
    trap restore_proxy EXIT
    trap 'restore_proxy; exit 130' INT
    trap 'restore_proxy; exit 143' TERM
    compose_observer --profile load run --rm \
      -e "TEST_RUN=${run_id}" \
      k6 run \
      --out experimental-prometheus-rw \
      --tag "test_run=${run_id}" \
      --summary-export="/results/${run_id}-${scenario}.json" \
      "/scripts/${scenario}.js"
    restore_proxy
    trap - EXIT INT TERM
    return
  fi

  compose_observer --profile load run --rm \
    -e "TEST_RUN=${run_id}" \
    k6 run \
    --out experimental-prometheus-rw \
    --tag "test_run=${run_id}" \
    --summary-export="/results/${run_id}-${scenario}.json" \
    "/scripts/${scenario}.js"
}

run_backend_comparison() {
  local run_id
  local log_path
  run_id="$(date -u +%Y%m%dT%H%M%SZ)-$$"
  mkdir -p load/results
  log_path="load/results/${run_id}-compare.log"
  compose_observer --profile load run --rm \
    -e "TEST_RUN=${run_id}" \
    k6 run \
    --out experimental-prometheus-rw \
    --tag "test_run=${run_id}" \
    --summary-export="/results/${run_id}-compare.json" \
    /scripts/compare.js 2>&1 | tee "$log_path"
}

run_stress_with_watchdog() {
  local run_id="$1"
  local container_name="ruvio-stress-${run_id//[^a-zA-Z0-9_.-]/-}"
  local log_path="load/results/${run_id}-stress.log"
  local stop_path="load/results/${run_id}-stress-stop-reason.txt"
  local target_url="${TARGET_URL:-$(sed -n 's/^TARGET_URL=//p' .env | tail -n 1)}"
  local min_memory="${STRESS_MIN_MEMORY_AVAILABLE_PERCENT:-}"
  local failed_health_checks=0
  local observer_cpu_saturation=0
  local stop_reason=""

  if [[ -z "$min_memory" ]]; then
    min_memory="$(sed -n 's/^STRESS_MIN_MEMORY_AVAILABLE_PERCENT=//p' .env | tail -n 1)"
  fi
  min_memory="${min_memory:-10}"
  if ! [[ "$min_memory" =~ ^[0-9]+([.][0-9]+)?$ ]] ||
    ! awk -v minimum="$min_memory" 'BEGIN { exit !(minimum > 0 && minimum < 50) }'; then
    echo "STRESS_MIN_MEMORY_AVAILABLE_PERCENT must be greater than 0 and less than 50." >&2
    return 2
  fi

  compose_observer --profile load run --rm --name "$container_name" k6 run \
    --out experimental-prometheus-rw \
    --tag "test_run=${run_id}" \
    --summary-export="/results/${run_id}-stress.json" \
    /scripts/stress.js >"$log_path" 2>&1 &
  local k6_pid=$!

  stop_test() {
    stop_reason="$1"
    printf '%s\n' "$stop_reason" >"$stop_path"
    docker stop --time 15 "$container_name" >/dev/null 2>&1 || true
  }
  cleanup_stress() {
    if [[ "$(docker inspect --format '{{.State.Running}}' "$container_name" 2>/dev/null || true)" == true ]]; then
      if [[ -z "$stop_reason" ]]; then
        stop_reason="Safety stop: watchdog exited unexpectedly."
        printf '%s\n' "$stop_reason" >"$stop_path"
      fi
      docker stop --time 15 "$container_name" >/dev/null 2>&1 || true
    fi
  }
  trap cleanup_stress EXIT
  trap 'stop_test "Interrupted by operator."; wait "$k6_pid" || true; exit 130' INT
  trap 'stop_test "Terminated by operator."; wait "$k6_pid" || true; exit 143' TERM

  while kill -0 "$k6_pid" 2>/dev/null; do
    if curl --fail --silent --show-error --max-time 4 \
      "${target_url%/}/health/ready" >/dev/null 2>&1; then
      failed_health_checks=0
    else
      failed_health_checks=$((failed_health_checks + 1))
      if (( failed_health_checks >= 3 )); then
        stop_test "Safety stop: API/Ruvio readiness failed three consecutive checks."
        break
      fi
    fi

    local available_percent
    available_percent="$(
      curl --fail --silent --max-time 4 --get \
        --data-urlencode 'query=node_memory_MemAvailable_bytes{job="linux-node"} / node_memory_MemTotal_bytes{job="linux-node"} * 100' \
        http://127.0.0.1:9090/api/v1/query 2>/dev/null |
        python3 -c 'import json,sys; d=json.load(sys.stdin); print(d["data"]["result"][0]["value"][1]) if d.get("data",{}).get("result") else None' 2>/dev/null || true
    )"
    if [[ "$available_percent" =~ ^[0-9]+([.][0-9]+)?$ ]] &&
      awk -v available="$available_percent" -v minimum="$min_memory" 'BEGIN { exit !(available < minimum) }'; then
      stop_test "Safety stop: SUT host available memory fell below ${min_memory}%."
      break
    fi

    if [[ -r /proc/meminfo && -r /proc/loadavg ]]; then
      local observer_available_percent observer_load observer_cores
      observer_available_percent="$(
        python3 -c 'import re; d=open("/proc/meminfo").read(); t=int(re.search(r"^MemTotal:\s+(\d+)",d,re.M).group(1)); a=int(re.search(r"^MemAvailable:\s+(\d+)",d,re.M).group(1)); print(a*100/t)'
      )"
      if awk -v available="$observer_available_percent" -v minimum="$min_memory" 'BEGIN { exit !(available < minimum) }'; then
        stop_test "Safety stop: observer host available memory fell below ${min_memory}%."
        break
      fi

      observer_load="$(awk '{ print $1 }' /proc/loadavg)"
      observer_cores="$(getconf _NPROCESSORS_ONLN)"
      if awk -v load_avg="$observer_load" -v cores="$observer_cores" 'BEGIN { exit !(load_avg > cores * 2) }'; then
        observer_cpu_saturation=$((observer_cpu_saturation + 1))
        if (( observer_cpu_saturation >= 3 )); then
          stop_test "Safety stop: observer one-minute load average exceeded twice its CPU count for three checks."
          break
        fi
      else
        observer_cpu_saturation=0
      fi
    fi
    sleep 5
  done

  local exit_code=0
  wait "$k6_pid" || exit_code=$?
  trap - EXIT INT TERM
  cat "$log_path"

  if [[ -n "$stop_reason" ]]; then
    echo "$stop_reason" >&2
    return 1
  fi
  return "$exit_code"
}

wait_for_api() {
  local target_url
  target_url="$(sed -n 's/^TARGET_URL=//p' .env | tail -n 1)"
  if [[ -z "$target_url" ]]; then
    echo "Set TARGET_URL in tests/scenario-lab/.env." >&2
    return 2
  fi
  local attempts=0
  until curl --fail --silent --show-error "${target_url%/}/health/ready" >/dev/null 2>&1; do
    attempts=$((attempts + 1))
    if (( attempts >= 120 )); then
      echo "API did not become ready at ${target_url%/}/health/ready within 4 minutes." >&2
      return 1
    fi
    sleep 2
  done
}

require_compose

case "$command_name" in
  sut-up)
    if [[ "$(uname -s)" == Linux ]]; then
      compose_sut --profile host-metrics up --build -d
    else
      echo "Skipping Node Exporter: host filesystem metrics are Linux-only in this lab."
      compose_sut up --build -d
    fi
    ;;
  observer-up)
    prepare_observer_config
    compose_observer up -d prometheus otel-collector tempo grafana
    ;;
  smoke)
    run_k6 smoke
    ;;
  sustained)
    run_k6 sustained
    ;;
  stress)
    wait_for_api
    run_k6 stress
    ;;
  compare)
    wait_for_api
    run_backend_comparison
    ;;
  suite)
    prepare_observer_config
    compose_observer up -d prometheus otel-collector tempo grafana
    wait_for_api
    run_k6 smoke
    ENABLE_RUVIO_PARTITION=true run_k6 sustained
    ;;
  sut-down)
    compose_sut down
    ;;
  observer-down)
    compose_observer down
    ;;
  *)
    cat >&2 <<'USAGE'
Usage: scripts/scenario-lab.sh <command>

Commands:
  sut-up       Build and start Ruvio + the scenario API on the SUT host.
  observer-up  Start telemetry and Grafana on the observer host.
  smoke        Run the bounded k6 functional scenario.
  sustained    Run the configured long-running mixed workload.
  stress       Run the bounded ramp/peak/step-down/recovery stress profile.
  compare      Run the same five-minute traced workload against Ruvio, then Redis.
  suite        Wait for the SUT, run smoke, then run the sustained workload.
  sut-down     Stop the SUT stack without deleting its named data volume.
  observer-down Stop the observer stack without deleting telemetry volumes.
USAGE
    exit 2
    ;;
esac
