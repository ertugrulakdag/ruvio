#!/usr/bin/env bash
set -euo pipefail

root="$(cd "$(dirname "$0")/.." && pwd)"
dest="${1:-"$root/dist"}"
binary="${2:-"$root/target/release/ruvio"}"

if [[ ! -x "$binary" ]]; then
  echo "missing release binary: $binary" >&2
  echo "build with: cargo build --locked --release --bin ruvio --bin ruvio-ctl" >&2
  exit 1
fi

mkdir -p "$dest"
cp "$binary" "$dest/ruvio"
ctl="$(dirname "$binary")/ruvio-ctl"
if [[ -x "$ctl" ]]; then
  cp "$ctl" "$dest/ruvio-ctl"
fi
(
  cd "$dest"
  files=(ruvio)
  if [[ -x ruvio-ctl ]]; then
    files+=(ruvio-ctl)
  fi
  if command -v sha256sum >/dev/null; then
    sha256sum "${files[@]}" > SHA256SUMS
  else
    shasum -a 256 "${files[@]}" > SHA256SUMS
  fi
)

python3 "$root/scripts/generate_sbom.py" --root "$root" --output "$dest/sbom.cdx.json"

if command -v cyclonedx-cargo >/dev/null; then
  (cd "$root" && cyclonedx-cargo --format json --output-file "$dest/sbom.cyclonedx-cargo.json")
fi

echo "wrote artifacts to $dest"
