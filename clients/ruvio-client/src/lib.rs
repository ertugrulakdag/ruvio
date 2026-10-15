//! RESP2 client for Ruvio.
//!
//! Connecting does not send `CLIENT SETINFO` or `HELLO`.

mod args;
mod client;
mod cluster;
mod codec;
mod commands;
mod error;
mod extra;
mod helpers;
#[cfg(feature = "serde")]
mod json_value;
mod models;
mod scan;
mod value;

pub use client::{Client, ClientOptions, ConnectionNotice, SetMode};

pub use cluster::{hash_slot, hash_slot_bytes};

pub use error::Error;
pub use helpers::{CacheOptions, IdemOutcome, LeaseGuard, LockGuard, SemaphorePermit, new_token};
pub use models::{
    Aggregate, BitOperation, BitUnit, BloomInfo, BloomInsertOptions, BloomReserveOptions,
    FieldExpireCondition, FillOutcome, GeoFrom, GeoMatch, GeoSearch, GeoShape, HashScanPage,
    IdemState, JsonIndexType, JsonSetCondition, ListPositionOptions, PubSubMessage, RangeBy,
    RangeOptions, RateLimit, RestoreOptions, ScanPage, SetScanPage, SortedSetAddOptions,
    SortedSetCombine, SortedSetEntry, SortedSetScanPage, StreamClaimOptions, StreamEntry,
    StreamPendingEntry, StreamPendingFilter, StreamReadOptions, StreamReadResult,
};
pub use scan::ScanIter;

pub use value::RespValue;

/// Codec entry points for `examples/alloc_bench.rs`. Not a stable API.
#[doc(hidden)]
pub mod __bench {
    pub use crate::codec::{append_strings as encode, read_value as parse};
}
