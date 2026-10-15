use std::{
    io::{self, Write},
    ops::Deref,
};

pub(crate) struct BoundedOutput {
    bytes: Vec<u8>,
    maximum: usize,
    exceeded: bool,
}

impl BoundedOutput {
    pub(crate) fn with_capacity(capacity: usize, maximum: usize) -> Self {
        Self {
            bytes: Vec::with_capacity(capacity.min(maximum)),
            maximum,
            exceeded: false,
        }
    }

    pub(crate) fn clear(&mut self) {
        self.bytes.clear();
        self.exceeded = false;
    }

    /// Drops the first `count` bytes, which were sent; the limit then counts only the rest.
    pub(crate) fn consume_front(&mut self, count: usize) {
        self.bytes.drain(..count);
    }

    pub(crate) fn truncate_to(&mut self, length: usize) {
        self.bytes.truncate(length);
    }

    pub(crate) fn release_idle(&mut self) {
        if self.bytes.is_empty() {
            self.bytes.shrink_to(0);
        }
    }

    /// Gives back a large buffer once it is empty, as Redis's `clientsCron` does.
    pub(crate) fn release_oversized(&mut self, above: usize, keep: usize) {
        if self.bytes.is_empty() && self.bytes.capacity() > above {
            self.bytes.shrink_to(keep);
        }
    }

    pub(crate) fn maximum(&self) -> usize {
        self.maximum
    }

    pub(crate) fn exceeded(&self) -> bool {
        self.exceeded
    }
}

impl Deref for BoundedOutput {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        &self.bytes
    }
}

impl Write for BoundedOutput {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        if self.exceeded {
            return Ok(buffer.len());
        }

        let Some(new_length) = self.bytes.len().checked_add(buffer.len()) else {
            self.exceeded = true;

            return Ok(buffer.len());
        };

        if new_length > self.maximum {
            self.exceeded = true;

            return Ok(buffer.len());
        }

        self.bytes.extend_from_slice(buffer);
        Ok(buffer.len())
    }

    #[inline]
    fn write_all(&mut self, buffer: &[u8]) -> io::Result<()> {
        self.write(buffer).map(|_| ())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_writes_before_exceeding_the_limit() {
        let mut output = BoundedOutput::with_capacity(4, 5);

        output.write_all(b"1234").unwrap();
        output.write_all(b"67").unwrap();
        assert!(output.exceeded());
        assert_eq!(&*output, b"1234");
    }

    #[test]
    fn sent_bytes_no_longer_count_against_the_limit() {
        let mut output = BoundedOutput::with_capacity(0, 6);

        output.write_all(b"12345").unwrap();
        output.consume_front(4);
        output.write_all(b"6789").unwrap();
        assert!(!output.exceeded());
        assert_eq!(&*output, b"56789");
    }
}
