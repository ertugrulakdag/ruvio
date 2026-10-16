# Changelog

Notable changes to the Go client `github.com/salihcantekin/ruvio/clients/go`. A Go version is the git tag `clients/go/vX.Y.Z`; move the "Unreleased" notes under the new version by hand when tagging. New entries go under "Unreleased"; `scripts/release_version.py` moves them under the version number when the package is released.

## Unreleased

- The cluster client now takes each command's key positions from the server's command table (`command_keys_gen.go`, generated from `clients/testdata/commands.json`) instead of hand-kept lists, and is checked against the shared `clients/testdata/key_vectors.json`. No routing bug was found: the old lists already gave the server's keys for all 1,062 vectors.

## v0.1.0

- First release.
