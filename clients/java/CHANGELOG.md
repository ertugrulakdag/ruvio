# Changelog

Notable changes to the Java client `io.github.salihcantekin:ruvio-client`. New entries go under "Unreleased"; `scripts/release_version.py` moves them under the version number when the package is released.

## Unreleased

- The cluster client now takes each command's key positions from the server's command table (`CommandKeyRules`, generated from `clients/testdata/commands.json`) instead of a hand-kept list, and a test checks them against the server's key vectors. Every vector already matched, so no command was routed wrong before; words the server does not know (such as `BRPOPLPUSH`, `LCS`, `ZRANGESTORE`, `ZMPOP`) now take the unknown-command default, the first argument.

## 0.1.0

- First release.
