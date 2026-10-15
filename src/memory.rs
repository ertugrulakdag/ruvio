/// Deterministic shard-local memory charges.
///
/// These values describe memory owned or retained by the storage model, including
/// key-record headers, table slots and allocator size classes, so a limit tracks real
/// memory. They are not a replacement for process RSS: free space inside the logs, the
/// allocator's own fragmentation and the process baseline are not charged.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct MemoryUsage {
    pub entries: usize,
    pub key_bytes: usize,
    pub value_bytes: usize,
    pub collection_bytes: usize,
    pub index_bytes: usize,
    pub pooled_bytes: usize,
    pub connection_bytes: usize,
    pub output_bytes: usize,
    /// WAL bytes written but not yet synced, sampled on the maintenance tick. Reported,
    /// not charged: they are in the file already, so eviction cannot free them, and a
    /// sample taken during a burst would otherwise evict the keyspace (Redis likewise
    /// leaves AOF and replication buffers out of `maxmemory`).
    pub persistence_bytes: usize,
    pub snapshot_journal_bytes: usize,
}

impl MemoryUsage {
    pub const fn charged_bytes(self) -> usize {
        self.key_bytes
            .saturating_add(self.value_bytes)
            .saturating_add(self.collection_bytes)
            .saturating_add(self.index_bytes)
            .saturating_add(self.pooled_bytes)
            .saturating_add(self.connection_bytes)
            .saturating_add(self.output_bytes)
            .saturating_add(self.snapshot_journal_bytes)
    }

    pub fn delta_from(self, previous: Self) -> MemoryDelta {
        MemoryDelta {
            entries: signed_difference(self.entries, previous.entries),
            key_bytes: signed_difference(self.key_bytes, previous.key_bytes),
            value_bytes: signed_difference(self.value_bytes, previous.value_bytes),
            collection_bytes: signed_difference(self.collection_bytes, previous.collection_bytes),
            index_bytes: signed_difference(self.index_bytes, previous.index_bytes),
            pooled_bytes: signed_difference(self.pooled_bytes, previous.pooled_bytes),
            connection_bytes: signed_difference(self.connection_bytes, previous.connection_bytes),
            output_bytes: signed_difference(self.output_bytes, previous.output_bytes),
            persistence_bytes: signed_difference(
                self.persistence_bytes,
                previous.persistence_bytes,
            ),
            snapshot_journal_bytes: signed_difference(
                self.snapshot_journal_bytes,
                previous.snapshot_journal_bytes,
            ),
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct MemoryDelta {
    pub entries: i128,
    pub key_bytes: i128,
    pub value_bytes: i128,
    pub collection_bytes: i128,
    pub index_bytes: i128,
    pub pooled_bytes: i128,
    pub connection_bytes: i128,
    pub output_bytes: i128,
    pub persistence_bytes: i128,
    pub snapshot_journal_bytes: i128,
}

impl MemoryDelta {
    pub const fn charged_bytes(self) -> i128 {
        self.key_bytes
            + self.value_bytes
            + self.collection_bytes
            + self.index_bytes
            + self.pooled_bytes
            + self.connection_bytes
            + self.output_bytes
            + self.snapshot_journal_bytes
    }
}

/// Bytes the allocator hands out for a request of `len`: mimalloc's size classes, which
/// are 8-byte steps up to 64 bytes and four classes per power of two up to 512 KiB, and
/// whole 64 KiB arena slices past the largest class. Charges use it so a memory limit tracks real
/// memory. The slab classes of small collections follow the same steps.
#[inline]
pub(crate) const fn alloc_size(len: usize) -> usize {
    const LARGEST_CLASS: usize = 512 * 1024;
    const SLICE: usize = 64 * 1024;

    if len > LARGEST_CLASS {
        return (len + SLICE - 1) & !(SLICE - 1);
    }

    // A quarter of the power of two below `len`, at least 8: or-ing in 63 gives the
    // 8-byte steps of the classes up to 64 bytes without a second branch.
    let top = usize::BITS - 1 - (len.saturating_sub(1) | 63).leading_zeros();
    let step = 1usize << (top - 2);

    (len + step - 1) & !(step - 1)
}

fn signed_difference(current: usize, previous: usize) -> i128 {
    let current = i128::try_from(current).unwrap_or(i128::MAX);
    let previous = i128::try_from(previous).unwrap_or(i128::MAX);

    current - previous
}

/// Asks the allocator to hand freed pages back to the OS. Called after large frees such
/// as `FLUSHALL`, because an idle thread would otherwise keep them until its next allocation.
#[allow(unsafe_code)]
pub(crate) fn release_free_pages() {
    #[cfg(feature = "mimalloc-allocator")]
    {
        use mimalloc as _;

        unsafe extern "C" {
            fn mi_collect(force: bool);
        }

        // SAFETY: mi_collect only walks the calling thread's heap and has no preconditions.
        unsafe { mi_collect(true) };
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn computes_signed_category_deltas() {
        let before = MemoryUsage {
            entries: 2,
            key_bytes: 10,
            value_bytes: 20,
            pooled_bytes: 8,
            ..MemoryUsage::default()
        };

        let after = MemoryUsage {
            entries: 1,
            key_bytes: 4,
            value_bytes: 30,
            pooled_bytes: 0,
            ..MemoryUsage::default()
        };

        let delta = after.delta_from(before);

        assert_eq!(delta.entries, -1);
        assert_eq!(delta.key_bytes, -6);
        assert_eq!(delta.value_bytes, 10);
        assert_eq!(delta.pooled_bytes, -8);
        assert_eq!(delta.charged_bytes(), -4);
    }

    #[test]
    fn alloc_size_follows_the_allocator_classes() {
        let cases = [
            (0, 0),
            (1, 8),
            (8, 8),
            (9, 16),
            (64, 64),
            (65, 80),
            (100, 112),
            (128, 128),
            (129, 160),
            (200, 224),
            (256, 256),
            (1000, 1024),
            (1024, 1024),
            (3000, 3072),
            (128 * 1024 + 1, 160 * 1024),
            (512 * 1024, 512 * 1024),
            (512 * 1024 + 1, 576 * 1024),
        ];

        for (len, size) in cases {
            assert_eq!(alloc_size(len), size, "len {len}");
        }
    }
}
