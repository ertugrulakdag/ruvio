#!/usr/bin/env bash
# Start, stop, and check a local Ruvio, Redis, and Ruvio UI stack in Docker.
#
#   deploy/local.sh up       ask once for the OS and CPU type, then start the stack
#   deploy/local.sh status   show containers and run PING, SET, GET on Ruvio and Redis
#   deploy/local.sh down     stop and remove the containers (keeps the Ruvio volume)
#
# Ruvio: 127.0.0.1:6379, no username or password (localhost only).
# Redis: 127.0.0.1:6380, no password (localhost only).
# UI:    http://127.0.0.1:8080
set -eu

root=$(CDPATH= cd -- "$(dirname "$0")/.." && pwd)
cd "$root"

compose_file=deploy/compose.stack.yml
settings_file=deploy/.local.env
compose() { docker compose -f "$compose_file" "$@"; }

# The first `up` asks which machine this is and which container CPU to use.
# The answer goes to deploy/.local.env (git-ignored) and is reused next time.
# Set LOCAL_OS (mac|windows) and LOCAL_ARCH (arm64|amd64) to skip the questions.
ask_settings() {
  local os arch detected
  if [ -f "$settings_file" ]; then
    # shellcheck disable=SC1090
    . "$settings_file"
  fi
  if [ -z "${LOCAL_OS:-}" ]; then
    if [ -t 0 ]; then
      echo "Which machine is this?"
      echo "  1) Mac"
      echo "  2) Windows"
      read -r -p "Choose 1 or 2: " os
      case "$os" in 2) LOCAL_OS=windows ;; *) LOCAL_OS=mac ;; esac
    else
      LOCAL_OS=mac
    fi
  fi
  if [ -z "${LOCAL_ARCH:-}" ]; then
    detected=$(uname -m 2>/dev/null || echo unknown)
    case "$detected" in arm64|aarch64) detected=arm64 ;; *) detected=amd64 ;; esac
    if [ -t 0 ]; then
      echo "Which container CPU type? (this machine looks like $detected)"
      echo "  1) ARM (arm64): Apple Silicon Mac, Windows on ARM"
      echo "  2) AMD (amd64): Intel or AMD processor"
      read -r -p "Choose 1 or 2: " arch
      case "$arch" in 1) LOCAL_ARCH=arm64 ;; 2) LOCAL_ARCH=amd64 ;; *) LOCAL_ARCH=$detected ;; esac
    else
      LOCAL_ARCH=$detected
    fi
  fi
  umask 077
  printf 'LOCAL_OS=%s\nLOCAL_ARCH=%s\n' "$LOCAL_OS" "$LOCAL_ARCH" > "$settings_file"
  export LOCAL_ARCH
  echo "Machine: $LOCAL_OS, container CPU: $LOCAL_ARCH (saved in $settings_file)"
}

check() {
  local port=$1 label=$2
  if ! command -v redis-cli >/dev/null 2>&1; then
    echo "$label: redis-cli is not installed here, so PING and GET are skipped"
    return 0
  fi
  if ! redis-cli -h 127.0.0.1 -p "$port" PING >/dev/null 2>&1; then
    echo "$label: nothing answers on 127.0.0.1:$port. Start it with deploy/local.sh up"
    return 0
  fi
  printf '%s PING: ' "$label"; redis-cli -h 127.0.0.1 -p "$port" PING
  redis-cli -h 127.0.0.1 -p "$port" SET ruvio:local:check ok >/dev/null
  printf '%s GET:  ' "$label"; redis-cli -h 127.0.0.1 -p "$port" GET ruvio:local:check
}

case "${1:-status}" in
  up)
    ask_settings
    compose up -d --pull missing
    compose ps
    ;;
  status)
    if [ -f "$settings_file" ]; then . "$settings_file"; export LOCAL_ARCH; fi
    compose ps
    check 6380 redis
    check 6379 ruvio
    ;;
  down)
    compose down
    ;;
  *)
    echo "usage: deploy/local.sh up|status|down" >&2
    exit 2
    ;;
esac
