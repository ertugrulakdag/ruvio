# Security Policy

## Supported versions

Ruvio is a standalone preview (`0.3.x`). Security fixes land on `main`. There
is no long-term-support branch yet.

## Reporting a vulnerability

Do not open a public GitHub issue, pull request, or discussion for a security
defect.

Report privately through GitHub Security Advisories:

https://github.com/salihcantekin/ruvio/security/advisories/new

Include:

- Ruvio version or commit SHA
- operating system
- reproduction steps or a proof of concept
- expected impact (confidentiality, integrity, or availability)

We acknowledge private reports within seven days and aim to ship a fix or a
documented workaround before any coordinated public disclosure.

Parser, persistence, administrative, cluster, and denial-of-service assumptions
are in [docs/THREAT_MODEL.md](docs/THREAT_MODEL.md). Dependency and SBOM policy
is in [docs/SUPPLY_CHAIN.md](docs/SUPPLY_CHAIN.md).
