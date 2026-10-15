use super::*;

#[derive(Debug)]
pub(super) struct BytePool {
    // Boxed so the shard handle stays small (tests/layout.rs).
    pub(super) bins: Box<[Vec<Vec<u8>>; VALUE_POOL_CLASSES]>,
    pub(super) cached_bytes: usize,
}

impl Default for BytePool {
    fn default() -> Self {
        Self {
            bins: Box::new(std::array::from_fn(|_| Vec::new())),
            cached_bytes: 0,
        }
    }
}

/// Size classes of pooled buffers: steps of 16 bytes up to 256, then four steps per doubling up
/// to 64 KiB (mimalloc's classes from 256 bytes on), so a 279-byte value gets 320 bytes, not
/// 512. A heap string is charged its capacity, which is then its allocator block.
/// Returns the class index and its capacity for a request of `requested` bytes (at least 1).
fn class_of(requested: usize) -> (usize, usize) {
    if requested <= 256 {
        let index = requested.div_ceil(16) - 1;

        return (index, (index + 1) * 16);
    }

    let shift = (requested - 1).ilog2() as usize;
    let base = 1usize << shift;
    let step = base / 4;
    let k = (requested - base).div_ceil(step);

    (16 + (shift - 8) * 4 + (k - 1), base + k * step)
}

impl BytePool {
    pub(super) fn acquire(&mut self, value: &[u8], minimum_capacity: usize) -> Vec<u8> {
        let requested = value.len().max(minimum_capacity).max(1);
        let mut buffer = if requested <= MAX_POOLED_VALUE_BYTES {
            let (index, capacity) = class_of(requested);

            self.bins[index].pop().map_or_else(
                || Vec::with_capacity(capacity),
                |buffer| {
                    self.cached_bytes = self.cached_bytes.saturating_sub(buffer.capacity());
                    buffer
                },
            )
        } else {
            // The allocator's block: the capacity is what the string is charged.
            Vec::with_capacity(crate::memory::alloc_size(requested))
        };

        buffer.extend_from_slice(value);
        buffer
    }

    pub(super) fn clear(&mut self) {
        for bin in self.bins.iter_mut() {
            bin.clear();
        }
        self.cached_bytes = 0;
    }

    pub(super) fn release(&mut self, mut buffer: Vec<u8>) {
        let capacity = buffer.capacity();

        if capacity == 0 || capacity > MAX_POOLED_VALUE_BYTES {
            return;
        }

        // The largest class this buffer can serve in full.
        let (mut index, class_capacity) = class_of(capacity);

        if class_capacity > capacity {
            let Some(lower) = index.checked_sub(1) else {
                return;
            };

            index = lower;
        }

        if index >= VALUE_POOL_CLASSES || self.bins[index].len() >= MAX_BUFFERS_PER_CLASS {
            return;
        }

        buffer.clear();
        self.cached_bytes = self.cached_bytes.saturating_add(capacity);
        self.bins[index].push(buffer);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classes_cover_every_size_with_bounded_waste() {
        let mut previous = (0, 0);

        for requested in 1..=MAX_POOLED_VALUE_BYTES {
            let (index, capacity) = class_of(requested);

            assert!(capacity >= requested, "{requested}");
            assert!(index < VALUE_POOL_CLASSES, "{requested}");
            assert!(index >= previous.0 && capacity >= previous.1, "{requested}");
            assert_eq!(
                class_of(capacity),
                (index, capacity),
                "class capacity maps to itself"
            );
            assert!(
                capacity - requested <= capacity / 4 + 16,
                "{requested} -> {capacity}"
            );
            previous = (index, capacity);
        }

        assert_eq!(class_of(279).1, 320);
        assert_eq!(class_of(MAX_POOLED_VALUE_BYTES).0, VALUE_POOL_CLASSES - 1);
    }
}
