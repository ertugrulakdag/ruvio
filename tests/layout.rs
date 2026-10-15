//! Size ceilings for the per-key types. Growing one of these taxes every key,
//! so a failure here means the new state belongs behind `Option<Box<_>>`, in a
//! side table, or in a boxed `Value` arm instead.

use std::mem::size_of;

use ruvio::{AHashIndex, BYTE_BODY_SIZE, Entry, INDEX_SLOT_SIZE, KvShard, Value};

#[test]
fn value_is_at_most_24_bytes() {
    assert!(
        size_of::<Value>() <= 24,
        "Value is {} bytes",
        size_of::<Value>()
    );
}

#[test]
fn entry_is_at_most_24_bytes() {
    assert!(
        size_of::<Entry>() <= 24,
        "Entry is {} bytes",
        size_of::<Entry>()
    );
}

#[test]
#[allow(clippy::assertions_on_constants)]
fn byte_body_is_at_most_16_bytes() {
    assert!(BYTE_BODY_SIZE <= 16, "ByteBody is {BYTE_BODY_SIZE} bytes");
}

#[test]
fn index_slot_is_four_bytes() {
    assert_eq!(INDEX_SLOT_SIZE, 4);
}

#[test]
fn shard_handle_stays_small() {
    // The shard is one per thread, but it is moved into the runtime by value.
    // Per-connection state must not grow it without a deliberate decision.
    let size = size_of::<KvShard<ruvio::SystemClock, AHashIndex>>();

    assert!(size <= SHARD_MAX, "KvShard is {size} bytes");
}

const SHARD_MAX: usize = 2048; // 1864 bytes when the ceiling was set
