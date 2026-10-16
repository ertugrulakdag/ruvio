# Changelog

Notable changes to the Python client `ruvio-client` on PyPI. New entries go under "Unreleased"; `scripts/release_version.py` moves them under the version number when the package is released.

## Unreleased

- The cluster client now takes each command's key positions from the server's command table instead of its own hand-written lists, and is tested against the server's key vectors. No server command was routed by the wrong keys before; commands the server does not have (`BRPOPLPUSH`, `BZMPOP`, `EVAL_RO`, `EVALSHA_RO`, `FCALL_RO`, `LCS`, `ZINTERCARD`, `ZMPOP`, `ZRANGESTORE`, `LASTSAVE`, `QUIT`, `RESET`, `TIME`) now go by argument 1 like any other unknown command.

## 0.1.0

- First release.
