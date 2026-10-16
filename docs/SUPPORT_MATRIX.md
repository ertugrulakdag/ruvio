# Support Matrix

Ruvio is tested in CI on the combinations below. Anything not listed is
unsupported until it appears here.

| Host OS | Architecture | Runtime | Notes |
| --- | --- | --- | --- |
| macOS | aarch64 / x86_64 | evented (kqueue) | The manual `ci` workflow runs the same tests as Linux on Apple silicon (`macos-latest`). x86_64 builds but is not tested in CI. |
| Windows | x86_64 | evented (IOCP) | The manual `ci` workflow runs the same tests as Linux. |
| Linux | x86_64 | evented (epoll) | Production images use this process. |

## Kernel and Filesystem

- Persistence uses ordinary files and `fsync`/`fdatasync`.
- WAL and snapshot directories should live on a local POSIX filesystem with
  working directory `fsync` (ext4, XFS, APFS). Network filesystems and
  `tmpfs` without durability are not a supported durability target.
- Container images run as UID 10001, write only to `RUVIO_DATA_DIR` (default
  `/data`), and stop on `SIGTERM`. Read-only root filesystems are supported
  when `/data` is a writable volume.
- TLS (rustls) is supported. Behavior is in [COMPATIBILITY.md](COMPATIBILITY.md#protocol).

## Explicitly Unsupported

- Redis RDB/AOF import.
- Redis Cluster failover, migration, and replica sync.
- A second network runtime. Windows, macOS, and Linux share this process.
- AddressSanitizer and Miri on macOS/Windows, or under mimalloc / Lua /
  filesystem persistence. Those jobs run on Linux nightly with
  `--no-default-features`, which drops mimalloc and keeps the same runtime.
- 32-bit hosts.
- Binding a public address without `requirepass` while `protected_mode` is
  enabled; non-loopback clients are rejected.
