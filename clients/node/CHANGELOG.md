# Changelog

Notable changes to the Node.js client `ruvio-client` on npm. New entries go under "Unreleased"; `scripts/release_version.py` moves them under the version number when the package is released.

## Unreleased

- The cluster client now takes each command's key positions from the server's command table instead of its own hand-kept lists. This fixes routing of `GEORADIUS` and `GEORADIUSBYMEMBER` with `STORE` / `STOREDIST`, which used to ignore the destination key.

## 0.1.1

- Same code as 0.1.0.

## 0.1.0

- First release.
