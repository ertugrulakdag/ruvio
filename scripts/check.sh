#!/usr/bin/env bash
# Local gate before a push: the tests that tie hand-kept lists to their one source
# (command table, format versions, settings template, value kinds, hot parser, INFO fields), the
# compatibility matrix check, and a build of every target. About two minutes on a warm
# debug build; no release build, no full test suite.
#
#   scripts/check.sh                 run the checks
#   scripts/check.sh --install-hook  run them before every `git push` from this clone
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$root"

if [[ "${1:-}" == "--install-hook" ]]; then
	hook="$(git rev-parse --git-path hooks)/pre-push"
	printf '#!/usr/bin/env bash\nexec "%s/scripts/check.sh"\n' "$root" >"$hook"
	chmod +x "$hook"
	echo "pre-push hook installed: $hook"
	exit 0
fi

limit="${RUVIO_TEST_TIMEOUT:-300}"
run() {
	echo "+ $*" >&2
	timeout "$limit" "$@"
}

run python3 scripts/check_compatibility_matrix.py
run cargo check -q --all-targets
run cargo clippy -q --lib -- -D warnings

# Each filter names the test that holds one standard; a renamed test must be renamed here.
standards=(
	hand_kept_command_lists_agree_with_the_table
	table_arity_and_key_positions_match_what_the_parser_accepts
	every_keyless_command_is_decided_for_scripts
	every_keyless_command_is_decided_for_fan_out
	hot_parser_agrees_with_the_general_parser
	every_record_type_the_decoder_knows_has_a_format_minor
	every_value_kind_the_decoder_knows_is_counted
	every_template_setting_answers_config_get
	scan_type_finds_every_kind_by_the_name_type_reports
	info_fields_and_the_observability_table_agree
	every_value_kind_survives_log_replay_checkpoint_and_dump
	version_one_wal
)
run cargo test -q --lib -- "${standards[@]}"
run cargo test -q --test zero_alloc --test layout
echo "check.sh: ok"
