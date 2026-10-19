# Changelog

Notable changes to `ruvio-ui`, the web console (Docker image and host binaries). New entries go under "Unreleased"; `scripts/release_version.py` moves them under the version number when the package is released.

## Unreleased

- New workbench form for `SET … IFGT` and `IFLT` (keep the highest or lowest number). The forms for TRANSFER, HTRANSFER, RENEW, ZPOPBYSCORE, ZMOVEDUE, LIMIT SLIDING and TOKENS, and the lifetime on INCR were already there.

## 0.1.9 - 2026-10-17

- New Dusk Rust theme, matching the project site. Every color and font is in `web/src/tokens.css`.
- New **Console** screen: run server commands with formatted, size-capped replies, copy and download of the full result, confirmation before destructive commands, history, and full screen.
- Command arguments are always sent as text. Setting an expiry no longer fails with "invalid type: integer".
- Shard health is collapsible and starts collapsed; shard notices moved to a header button.
- When the server names its shards `127.0.0.1` and you signed in with another host, shards are reached through that host. See `docs/UI_CONNECTIONS.md`.

## 0.1.8 - 2026-10-17

No recorded changes.

## 0.1.7

Earlier versions were not recorded in a changelog.
