# Changelog

Notable changes to the Java client `io.github.salihcantekin:ruvio-client`. New entries go under "Unreleased"; `scripts/release_version.py` moves them under the version number when the package is released.

## Unreleased

- Behaviour change: a cluster command now follows up to 3 `MOVED` redirects instead of 1 (the 4th is returned as the error). A `MOVED` points only that slot at the named node; the whole slot table is re-read with `CLUSTER SLOTS` only when that node is unknown or on the second `MOVED` for the same command (before, any second `MOVED` from any command triggered a reload).
- The cluster client now takes each command's key positions from the server's command table (`CommandKeyRules`, generated from `clients/testdata/commands.json`) instead of a hand-kept list, and a test checks them against the server's key vectors. Every vector already matched, so no command was routed wrong before; words the server does not know (such as `BRPOPLPUSH`, `LCS`, `ZRANGESTORE`, `ZMPOP`) now take the unknown-command default, the first argument.

## 0.1.0

- First release.
