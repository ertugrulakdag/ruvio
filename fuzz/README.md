[Ruvio](../README.md) / Fuzzing

# Fuzz targets

[cargo-fuzz](https://github.com/rust-fuzz/cargo-fuzz) targets for the parsers that read untrusted bytes.

| Target | Input |
| --- | --- |
| `resp_parsers` | RESP frames and command arguments |
| `json_ops` | JSON documents and paths for the native `JSON.*` commands |

```bash
cargo install cargo-fuzz
cargo +nightly fuzz run resp_parsers -- -max_total_time=15
```

CI runs `resp_parsers` for a short time on manual dispatch. See [docs/TOOLCHAIN.md](../docs/TOOLCHAIN.md) for the toolchain notes.
