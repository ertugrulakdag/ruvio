#!/bin/bash
set -euo pipefail
root="$(cd "$(dirname "$0")/.." && pwd)"
# shellcheck disable=SC1091
source "$root/scripts/ceiling_logic.sh"

fail() { echo "FAIL: $*" >&2; exit 1; }

[[ "$(ceiling_decide 100 110 95)" == "saturated" ]] || fail "90 percent ends the search on this point"
[[ "$(ceiling_decide 100 104 50)" == "plateau" ]] || fail "under 105 percent ends the phase on the previous point"
[[ "$(ceiling_decide 100 105 89.9)" == "continue" ]] || fail "105 percent keeps walking"
[[ "$(ceiling_decide 100 200 90)" == "saturated" ]] || fail "saturation wins over a large gain"
[[ "$(ceiling_file_status saturated saturated saturated)" == "saturated" ]] || fail "file status"
[[ "$(ceiling_file_status saturated client-bound saturated)" == "client-bound" ]] || fail "one client-bound command"
echo OK
