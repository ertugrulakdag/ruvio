# Changelog

Notable changes to the Python client `ruvio-client` on PyPI. New entries go under "Unreleased"; `scripts/release_version.py` moves them under the version number when the package is released.

## Unreleased

- Fixed: with the optional `hiredis` parser installed, commands that answer `OK` (`set`, `select`, `auth`, `client_setname` and others) returned `False` instead of `True` when replies were not decoded (`decode_responses=False` or the `binary` view), because hiredis returns the reply as `b"OK"`.
- **Behavior change:** the cluster client now follows up to 3 `-MOVED` redirects per command (was 1) before raising `MovedError`. A command's first `MOVED` to a known node updates only that slot; the whole slot map is re-read only when the named node is unknown or the same command gets a second `MOVED` (before, any second `MOVED` since the last reload re-read it). Transactions follow the same limit.
- **Behavior change:** a cluster pipeline (not a transaction) containing `MGET`, `MSET`, `DEL`, `EXISTS`, `UNLINK` or `TOUCH` with keys in several slots is no longer refused with `CrossSlotError`: the command is split per slot like the plain command, and its place in the reply list holds the merged reply. A transaction must still use one slot.
- The cluster client now takes each command's key positions from the server's command table instead of its own hand-written lists, and is tested against the server's key vectors. No server command was routed by the wrong keys before; commands the server does not have (`BRPOPLPUSH`, `BZMPOP`, `EVAL_RO`, `EVALSHA_RO`, `FCALL_RO`, `LCS`, `ZINTERCARD`, `ZMPOP`, `ZRANGESTORE`, `LASTSAVE`, `QUIT`, `RESET`, `TIME`) now go by argument 1 like any other unknown command.

## 0.1.0

- First release.
