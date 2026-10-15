# Releasing

No release runs on its own. Every workflow in `.github/workflows/` is started by hand from the Actions tab (`workflow_dispatch`). Only `pr-gate` also runs on pull requests and on pushes to `main`; it publishes nothing.

| Workflow | What it does |
| --- | --- |
| `pr-gate` | Format, tests, client suites, package metadata check |
| `ci` | Wider checks: three operating systems, clippy, supply chain, fuzz, container and fault tests |
| `release` | Publishes the packages below; choose one target or `all` |
| `integration-packages` | Builds and optionally publishes the .NET integration packages |
| `pages` | Publishes the `site/` folder to GitHub Pages |

## What `release` publishes

| Target | Result | Needs |
| --- | --- | --- |
| `docker` | `salihcantekin/ruvio` image (linux/amd64) | `DOCKERHUB_TOKEN` |
| `docker-ui` | `salihcantekin/ruvio-ui` image (amd64, arm64) | `DOCKERHUB_TOKEN` |
| `rust-client` | crate `ruvio-client` | `CARGO_REGISTRY_TOKEN` |
| `nuget` | `Ruvio.Client`, `Ruvio.Client.AspNetCore` | NuGet trusted publishing |
| `server-binaries` | `ruvio` and `ruvio-ctl` archives for Linux x64 and arm64, macOS arm64, Windows x64 | none |
| `ui-host` | `ruvio-ui` host archives for Linux x64, macOS arm64, Windows x64 | none |
| `npm`, `pypi`, `maven` | the Node.js, Python and Java clients | trusted publishing (npm, PyPI) and Central Portal secrets (Maven), each behind its own environment |

The Go client has no registry; Go modules are versions of a git tag. The server itself is not published as a crate (`publish = false` in `Cargo.toml`).

Binary archives are workflow artifacts. Turn on `create_github_release` to attach them, with a `SHA256SUMS` file, to a GitHub Release. The binaries use the release profile (fat LTO, no profile-guided optimization). Performance numbers come from the builds described on each results page, so check those before quoting one for a downloaded binary.

Leave a version input empty to bump that package's patch number, or fill it to publish that exact version. The site shows the current published versions by asking the registries when a page loads, so no page needs an edit after a release.
