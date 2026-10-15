//! Byte buffer that keeps up to `INLINE` bytes inside its owner and spills to a `Vec` above that.
//! Small packed lists and hashes are then one allocation (their box) instead of two.

use std::ops::{Deref, DerefMut, Range};

/// Sized so `SmallBuf` stays within 32 B (asserted below), which keeps `HashValue` at 48 B.
pub(super) const INLINE: usize = 30;

#[derive(Clone)]
pub(crate) enum SmallBuf {
    Inline { len: u8, data: [u8; INLINE] },
    Heap(Vec<u8>),
}

const _: () = assert!(size_of::<SmallBuf>() <= 32);

/// What the packed encoders need from a buffer; implemented for `Vec<u8>` (list nodes) and
/// `SmallBuf` (small lists and hashes).
pub(super) trait PackBuf: DerefMut<Target = [u8]> {
    fn push(&mut self, byte: u8);
    fn extend_from_slice(&mut self, bytes: &[u8]);
    fn capacity(&self) -> usize;
    fn reserve_exact(&mut self, extra: usize);
    fn resize(&mut self, len: usize, value: u8);
    fn truncate(&mut self, len: usize);
    /// Replaces `range` with `replacement`.
    fn replace_range(&mut self, range: Range<usize>, replacement: &[u8]);
}

impl PackBuf for Vec<u8> {
    fn push(&mut self, byte: u8) {
        Vec::push(self, byte);
    }

    fn extend_from_slice(&mut self, bytes: &[u8]) {
        Vec::extend_from_slice(self, bytes);
    }

    fn capacity(&self) -> usize {
        Vec::capacity(self)
    }

    fn reserve_exact(&mut self, extra: usize) {
        Vec::reserve_exact(self, extra);
    }

    fn resize(&mut self, len: usize, value: u8) {
        Vec::resize(self, len, value);
    }

    fn truncate(&mut self, len: usize) {
        Vec::truncate(self, len);
    }

    fn replace_range(&mut self, range: Range<usize>, replacement: &[u8]) {
        self.splice(range, replacement.iter().copied());
    }
}

impl Default for SmallBuf {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for SmallBuf {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_list().entries(self.iter()).finish()
    }
}

impl PartialEq for SmallBuf {
    fn eq(&self, other: &Self) -> bool {
        **self == **other
    }
}

impl Eq for SmallBuf {}

impl Deref for SmallBuf {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        match self {
            Self::Inline { len, data } => &data[..usize::from(*len)],
            Self::Heap(vec) => vec,
        }
    }
}

impl DerefMut for SmallBuf {
    fn deref_mut(&mut self) -> &mut [u8] {
        match self {
            Self::Inline { len, data } => &mut data[..usize::from(*len)],
            Self::Heap(vec) => vec,
        }
    }
}

impl SmallBuf {
    pub(crate) fn new() -> Self {
        Self::Inline {
            len: 0,
            data: [0; INLINE],
        }
    }

    pub(crate) fn from_slice(bytes: &[u8]) -> Self {
        if bytes.len() <= INLINE {
            let mut data = [0; INLINE];

            data[..bytes.len()].copy_from_slice(bytes);
            Self::Inline {
                len: bytes.len() as u8,
                data,
            }
        } else {
            Self::Heap(bytes.to_vec())
        }
    }

    pub(crate) fn with_capacity(capacity: usize) -> Self {
        if capacity <= INLINE {
            Self::new()
        } else {
            Self::Heap(Vec::with_capacity(capacity))
        }
    }

    /// Bytes owned on the heap (zero while inline).
    pub(crate) fn heap_capacity(&self) -> usize {
        match self {
            Self::Inline { .. } => 0,
            Self::Heap(vec) => vec.capacity(),
        }
    }

    /// Moves the bytes to a `Vec` with room for `capacity` and returns it.
    fn spill(&mut self, capacity: usize) -> &mut Vec<u8> {
        if let Self::Inline { len, data } = self {
            let mut vec = Vec::with_capacity(capacity.max(usize::from(*len)));

            vec.extend_from_slice(&data[..usize::from(*len)]);
            *self = Self::Heap(vec);
        }

        match self {
            Self::Heap(vec) => vec,
            Self::Inline { .. } => unreachable!("spilled"),
        }
    }

    /// Back inline once the contents fit again.
    fn settle(&mut self) {
        if let Self::Heap(vec) = self
            && vec.len() <= INLINE
        {
            let mut data = [0; INLINE];

            data[..vec.len()].copy_from_slice(vec);
            *self = Self::Inline {
                len: vec.len() as u8,
                data,
            };
        }
    }

    pub(crate) fn truncate(&mut self, len: usize) {
        match self {
            Self::Inline { len: current, .. } => *current = (*current).min(len as u8),
            Self::Heap(vec) => vec.truncate(len),
        }
    }

    pub(crate) fn drain_range(&mut self, range: Range<usize>) {
        match self {
            Self::Inline { len, data } => {
                let end = usize::from(*len);

                data.copy_within(range.end..end, range.start);
                *len -= (range.end - range.start) as u8;
            }
            Self::Heap(vec) => {
                vec.drain(range);
            }
        }
    }

    pub(crate) fn resize(&mut self, len: usize, value: u8) {
        match self {
            Self::Inline { len: current, data } if len <= INLINE => {
                if len > usize::from(*current) {
                    data[usize::from(*current)..len].fill(value);
                }

                *current = len as u8;
            }
            _ => self.spill(len).resize(len, value),
        }
    }

    pub(crate) fn shrink_to(&mut self, target: usize) {
        if let Self::Heap(vec) = self {
            vec.shrink_to(target);
        }

        self.settle();
    }

    #[cfg(test)]
    pub(crate) fn shrink_to_fit(&mut self) {
        if let Self::Heap(vec) = self {
            vec.shrink_to_fit();
        }

        self.settle();
    }
}

impl PackBuf for SmallBuf {
    fn push(&mut self, byte: u8) {
        self.extend_from_slice(&[byte]);
    }

    fn extend_from_slice(&mut self, bytes: &[u8]) {
        match self {
            Self::Inline { len, data } if usize::from(*len) + bytes.len() <= INLINE => {
                let at = usize::from(*len);

                data[at..at + bytes.len()].copy_from_slice(bytes);
                *len += bytes.len() as u8;
            }
            _ => {
                let wanted = self.len() + bytes.len();

                self.spill(wanted).extend_from_slice(bytes);
            }
        }
    }

    fn resize(&mut self, len: usize, value: u8) {
        SmallBuf::resize(self, len, value);
    }

    fn truncate(&mut self, len: usize) {
        SmallBuf::truncate(self, len);
    }

    fn capacity(&self) -> usize {
        match self {
            Self::Inline { .. } => INLINE,
            Self::Heap(vec) => vec.capacity(),
        }
    }

    fn reserve_exact(&mut self, extra: usize) {
        let wanted = self.len() + extra;

        if wanted > INLINE || matches!(self, Self::Heap(_)) {
            self.spill(wanted).reserve_exact(extra);
        }
    }

    fn replace_range(&mut self, range: Range<usize>, replacement: &[u8]) {
        let new_len = self.len() - (range.end - range.start) + replacement.len();

        match self {
            Self::Inline { len, data } if new_len <= INLINE => {
                let old = usize::from(*len);

                data.copy_within(range.end..old, range.start + replacement.len());
                data[range.start..range.start + replacement.len()].copy_from_slice(replacement);
                *len = new_len as u8;
            }
            _ => {
                self.spill(new_len)
                    .splice(range, replacement.iter().copied());
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Random edits against a plain `Vec`, across the inline/heap boundary in both directions.
    #[test]
    fn follows_a_vec_across_the_inline_boundary() {
        let mut state = 0x9E37_79B9_7F4A_7C15u64;
        let mut next = move |bound: usize| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            (state % bound as u64) as usize
        };
        let mut buf = SmallBuf::new();
        let mut model: Vec<u8> = Vec::new();

        for step in 0..20_000 {
            match next(7) {
                0 => {
                    let byte = next(256) as u8;

                    PackBuf::push(&mut buf, byte);
                    model.push(byte);
                }
                1 => {
                    let extra: Vec<u8> = (0..next(12)).map(|_| next(256) as u8).collect();

                    PackBuf::extend_from_slice(&mut buf, &extra);
                    model.extend_from_slice(&extra);
                }
                2 if !model.is_empty() => {
                    let from = next(model.len());
                    let to = from + next(model.len() - from + 1);

                    buf.drain_range(from..to);
                    model.drain(from..to);
                }
                3 if !model.is_empty() => {
                    let from = next(model.len());
                    let to = from + next(model.len() - from + 1);
                    let replacement: Vec<u8> = (0..next(10)).map(|_| next(256) as u8).collect();

                    PackBuf::replace_range(&mut buf, from..to, &replacement);
                    model.splice(from..to, replacement.iter().copied());
                }
                4 => {
                    let len = next(model.len() + 1);

                    buf.truncate(len);
                    model.truncate(len);
                }
                5 => {
                    let len = next(45);

                    buf.resize(len, 7);
                    model.resize(len, 7);
                }
                _ => {
                    buf.shrink_to_fit();
                    PackBuf::reserve_exact(&mut buf, next(40));
                }
            }

            assert_eq!(&*buf, &model[..], "diverged at step {step}");

            if model.len() <= INLINE && matches!(buf, SmallBuf::Heap(_)) {
                buf.shrink_to_fit();
                assert!(
                    matches!(buf, SmallBuf::Inline { .. }),
                    "small buffers settle inline"
                );
            }
        }
    }
}
