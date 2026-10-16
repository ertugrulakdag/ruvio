# Changelog

Notable changes to the Go client `github.com/salihcantekin/ruvio/clients/go`. A Go version is the git tag `clients/go/vX.Y.Z`; move the "Unreleased" notes under the new version by hand when tagging. New entries go under "Unreleased"; `scripts/release_version.py` moves them under the version number when the package is released.

## Unreleased

- **Behaviour change:** a cluster command now follows up to 3 `MOVED` redirects (was 1) before the `MOVED` error is returned. Each `MOVED` patches that slot; the whole slot table is reloaded only when the named node is unknown or from the second `MOVED` of the same command on (before, any second moved slot since the last reload triggered a reload). Pipelined commands follow the same limit.
- **Behaviour change:** a pipeline holding `MGET`, `MSET`, `DEL`, `UNLINK`, `EXISTS` or `TOUCH` whose keys span slots is no longer refused as a whole with `ErrCrossSlot`; the command is split per slot like the plain command and its merged reply sits at its index. Other cross-slot commands, and any cross-slot key inside a `MULTI` pipeline, still fail with `ErrCrossSlot` before anything is sent.
- Fixed: the `ErrCrossSlot` error for a pipeline could be built from an already recycled buffer (a panic or a wrong command name in the message).
- The cluster client now takes each command's key positions from the server's command table (`command_keys_gen.go`, generated from `clients/testdata/commands.json`) instead of hand-kept lists, and is checked against the shared `clients/testdata/key_vectors.json`. No routing bug was found: the old lists already gave the server's keys for all 1,062 vectors.

## v0.1.0

- First release.
