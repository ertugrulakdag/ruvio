//! Links the server's hot functions first and contiguously.
//!
//! A generic command runs a few hundred instruction-cache lines spread over the whole
//! binary; lines that land in the same cache set evict each other on every request.
//! `benchmark/hot-symbols.txt` (written by `scripts/hot_symbols.py`) lists those functions
//! hottest first, and lld's `--symbol-ordering-file` places them at the start of `.text`.
//! Names the file no longer matches are skipped, so a stale list only costs the benefit.
//!
//! Only x86_64 Linux with the toolchain's default linker (rust-lld): other linkers do not
//! take this flag. `RUVIO_NO_SYMBOL_ORDER=1` turns it off.

use std::{env, path::PathBuf};

fn main() {
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-changed=benchmark/hot-symbols.txt");
    println!("cargo:rerun-if-env-changed=RUVIO_NO_SYMBOL_ORDER");
    println!("cargo:rerun-if-env-changed=CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_LINKER");

    let target = |key: &str| env::var(key).unwrap_or_default();
    let default_linker = env::var_os("CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_LINKER").is_none()
        && !target("CARGO_ENCODED_RUSTFLAGS").contains("linker");

    if target("CARGO_CFG_TARGET_ARCH") != "x86_64"
        || target("CARGO_CFG_TARGET_OS") != "linux"
        || target("CARGO_CFG_TARGET_ENV") != "gnu"
        || !default_linker
        || env::var_os("RUVIO_NO_SYMBOL_ORDER").is_some()
    {
        return;
    }

    let list = PathBuf::from(env::var("CARGO_MANIFEST_DIR").unwrap())
        .join("benchmark")
        .join("hot-symbols.txt");

    if list.exists() {
        println!(
            "cargo:rustc-link-arg-bins=-Wl,--symbol-ordering-file={}",
            list.display()
        );
        println!("cargo:rustc-link-arg-bins=-Wl,--no-warn-symbol-ordering");
    }
}
