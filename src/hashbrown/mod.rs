//! Only `HashTable` is used by the server; the map and set wrappers, the raw-entry API and the
//! serde/rayon glue of the upstream crate were removed.
//!
//! This crate is a Rust port of Google's high-performance [SwissTable] hash
//! map, adapted to make it a drop-in replacement for Rust's standard `HashMap`
//! and `HashSet` types.
//!
//! The original C++ version of [SwissTable] can be found [here], and this
//! [CppCon talk] gives an overview of how the algorithm works.
//!
//! [SwissTable]: https://abseil.io/blog/20180927-swisstables
//! [here]: https://github.com/abseil/abseil-cpp/blob/master/absl/container/internal/raw_hash_set.h
//! [CppCon talk]: https://www.youtube.com/watch?v=ncHmEUmJZf4

#![allow(unexpected_cfgs, unused_imports)]
// Vendored upstream code: its unsafe blocks follow upstream's own invariants and keep
// upstream's text, so the crate-wide SAFETY-comment lint stops at this module.
#![allow(clippy::undocumented_unsafe_blocks)]

pub(crate) extern crate alloc as stdalloc;

#[macro_use]
mod macros;

mod alloc;
mod control;
mod raw;
mod util;

mod scopeguard;
mod table;

pub use crate::hashbrown::table::HashTable;
pub(crate) use raw::scan_cursor_next;

/// Ruvio: hints the CPU to load the cache line holding `value`.
#[inline]
pub(crate) fn prefetch_read<T>(value: &T) {
    #[cfg(target_arch = "x86_64")]
    // SAFETY: SSE is part of the x86_64 baseline, and a prefetch is only a hint.
    unsafe {
        core::arch::x86_64::_mm_prefetch::<{ core::arch::x86_64::_MM_HINT_T0 }>(
            core::ptr::from_ref(value).cast(),
        );
    }
    #[cfg(not(target_arch = "x86_64"))]
    let _ = value;
}

/// Ruvio: [`prefetch_read`] for a raw pointer that may not be valid to reference.
#[inline]
pub(crate) fn prefetch_ptr(pointer: *const u8) {
    #[cfg(target_arch = "x86_64")]
    // SAFETY: a prefetch is only a hint and never faults, whatever the address.
    unsafe {
        core::arch::x86_64::_mm_prefetch::<{ core::arch::x86_64::_MM_HINT_T0 }>(pointer.cast());
    }
    #[cfg(not(target_arch = "x86_64"))]
    let _ = pointer;
}

/// The error type for `try_reserve` methods.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum TryReserveError {
    /// Error due to the computed capacity exceeding the collection's maximum
    /// (usually `isize::MAX` bytes).
    CapacityOverflow,

    /// The memory allocator returned an error
    AllocError {
        /// The layout of the allocation request that failed.
        layout: stdalloc::alloc::Layout,
    },
}

// matches stdalloc::collections::TryReserveError
impl core::fmt::Display for TryReserveError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str("memory allocation failed")?;
        let reason = match self {
            TryReserveError::CapacityOverflow => {
                " because the computed capacity exceeded the collection's maximum"
            }
            TryReserveError::AllocError { .. } => " because the memory allocator returned an error",
        };
        f.write_str(reason)
    }
}

impl core::error::Error for TryReserveError {}
