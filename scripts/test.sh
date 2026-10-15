#!/usr/bin/env bash
set -euo pipefail

root="$(cd "$(dirname "$0")/.." && pwd)"
cd "$root"

native_resilience=false
docker_resilience=false
clippy=false
for arg in "$@"; do
  case "$arg" in
    --clippy) clippy=true ;;
    --native-resilience) native_resilience=true ;;
    --docker-resilience) docker_resilience=true ;;
    *)
      echo "Unknown option: $arg" >&2
      echo "Usage: $0 [--clippy] [--native-resilience] [--docker-resilience]" >&2
      exit 2
      ;;
  esac
done

command -v cargo >/dev/null || { echo "cargo is required" >&2; exit 1; }
python_cmd="$(command -v python3 || command -v python || true)"
if [[ -z "$python_cmd" && ( "$native_resilience" == true || "$docker_resilience" == true ) ]]; then
  echo "Python 3 is required for resilience scenarios" >&2
  exit 1
fi

echo "==> Rust formatting"
cargo fmt --check
if [[ "$clippy" == true ]]; then
  echo "==> Rust Clippy"
  cargo clippy --all-targets -- -D warnings
fi
echo "==> Rust tests"
cargo test --all-targets
echo "==> Store smoke example"
cargo run --example 04_store_smoke

if [[ "$native_resilience" == true ]]; then
  echo "==> Native crash/recovery tests"
  "$python_cmd" scripts/native_resilience.py --build
fi

if [[ "$docker_resilience" == true ]]; then
  command -v docker >/dev/null || { echo "docker is required" >&2; exit 1; }
  echo "==> Build resilience test image"
  docker build --tag ruvio:resilience-test .
  echo "==> Docker resilience tests"
  "$python_cmd" scripts/docker_resilience.py
fi
