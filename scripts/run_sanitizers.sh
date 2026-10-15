#!/usr/bin/env bash
# AddressSanitizer and Miri entry point. Requires nightly on Linux.
# Miri covers shard/protocol units only: no sockets, files, Lua FFI, or io_uring.
set -euo pipefail

mode="${1:-all}"
if [[ "${mode}" != all && "${mode}" != miri && "${mode}" != asan ]]; then
  echo "usage: $0 [all|miri|asan]" >&2
  exit 2
fi

export CARGO_INCREMENTAL="${CARGO_INCREMENTAL:-0}"
export RUSTUP_TOOLCHAIN="${RUSTUP_TOOLCHAIN:-nightly}"

miri_filters=(
  time::
  protocol::
  routing::
  memory::
  metrics::
  config::
  output::
  admission::
  security::
  store::arena_table::
  store::intset::
  store::smallbuf::
  transaction::
  list_waiters::
)

run_miri() {
  export MIRIFLAGS="${MIRIFLAGS:--Zmiri-strict-provenance -Zmiri-disable-isolation}"
  # Filters match substrings ("time::" also matches "runtime::"), so skip the
  # runtime and persistence tests, which need fsync and sockets.
  cargo miri test --no-default-features --lib -- "${miri_filters[@]}" --skip runtime:: --skip persistence::
}

run_asan() {
  local host
  host="$(rustc --print host-tuple 2>/dev/null || rustc -vV | awk '/^host:/{print $2}')"
  export RUSTFLAGS="${RUSTFLAGS:--Zsanitizer=address}"
  export ASAN_OPTIONS="${ASAN_OPTIONS:-detect_leaks=1:detect_stack_use_after_return=1}"
  cargo test --no-default-features --lib --target "${host}"
}

case "${mode}" in
  miri) run_miri ;;
  asan) run_asan ;;
  all)
    run_miri
    run_asan
    ;;
esac
