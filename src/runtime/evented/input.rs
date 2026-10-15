use std::{io, ops::Deref};

use tokio::net::TcpStream;

use super::READ_CHUNK_BYTES;

/// Idle buffers above this capacity shrink back to `IDLE_KEEP_BYTES`.
pub(super) const IDLE_SHRINK_ABOVE_BYTES: usize = 32 * 1024;
pub(super) const IDLE_KEEP_BYTES: usize = 16 * 1024;
/// Unparsed bytes one `read_from` stops at, unless a frame needs more. A large pipeline is
/// then parsed and answered in slices, as Redis reads 16 KiB at a time, instead of growing
/// the buffer to the whole batch; the allocator would keep that peak after it shrinks.
const READ_BATCH_BYTES: usize = 64 * 1024;

/// A connection's unparsed input. Sockets read straight into it and consumed bytes
/// advance `start`, so a pipeline batch never moves the unparsed tail.
#[derive(Default)]
pub(super) struct InputBuffer {
    bytes: Vec<u8>,
    start: usize,
    /// Total unparsed bytes the first incomplete frame needs; zero when unknown.
    need: usize,
}

impl Deref for InputBuffer {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        &self.bytes[self.start..]
    }
}

impl InputBuffer {
    pub(super) fn consume(&mut self, count: usize) {
        self.start += count;
        self.need = self.need.saturating_sub(count);

        if self.start >= self.bytes.len() {
            self.bytes.clear();
            self.start = 0;
        } else if self.start > self.bytes.len() / 2 {
            self.bytes.drain(..self.start);
            self.start = 0;
        }
    }

    pub(super) fn extend_from_slice(&mut self, data: &[u8]) {
        self.bytes.extend_from_slice(data);
    }

    pub(super) fn set_need(&mut self, need: usize) {
        self.need = need;
    }

    /// True while a known-incomplete frame has not fully arrived.
    pub(super) fn waiting_for_frame(&self) -> bool {
        self.len() < self.need
    }

    pub(super) fn release_idle(&mut self) {
        if self.is_empty() && self.bytes.capacity() > IDLE_SHRINK_ABOVE_BYTES {
            self.bytes.shrink_to(IDLE_KEEP_BYTES);
        }
    }

    pub(super) fn release_all(&mut self) {
        if self.is_empty() {
            self.bytes.shrink_to(0);
        }
    }

    /// Reads whatever the socket has ready, up to `limit` unparsed bytes. Returns
    /// true at end of stream.
    pub(super) fn read_from(&mut self, stream: &TcpStream, limit: usize) -> io::Result<bool> {
        let before = self.bytes.len();
        let ended = self.read_socket(stream, limit);

        super::net::note_input(self.bytes.len().saturating_sub(before));
        ended
    }

    fn read_socket(&mut self, stream: &TcpStream, limit: usize) -> io::Result<bool> {
        loop {
            if self.len() > limit {
                return Ok(false);
            }

            let missing = self.need.saturating_sub(self.len());
            let want = READ_CHUNK_BYTES.max(missing.min(limit.saturating_add(1)));

            if self.bytes.capacity() - self.bytes.len() < want {
                self.bytes.reserve(want);
            }

            match stream.try_read_buf(&mut self.bytes) {
                Ok(0) => return Ok(true),
                // A read that left room in the buffer drained the socket. Clearing the
                // readiness here saves the read that would only return EAGAIN (one syscall
                // per batch; Redis does one read per event). The runtime is single-threaded,
                // so no readiness event can be processed between that read and this clear;
                // data arriving later raises a new edge.
                Ok(_) if self.bytes.len() < self.bytes.capacity() => {
                    let _ = stream.try_io(tokio::io::Interest::READABLE, || {
                        Err::<(), _>(io::ErrorKind::WouldBlock.into())
                    });

                    return Ok(false);
                }
                // The socket may hold more; its readiness stays set, so the connection reads
                // again right after parsing what is here.
                Ok(_) if self.len() >= READ_BATCH_BYTES.max(self.need) => return Ok(false),
                Ok(_) => {}
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => return Ok(false),
                Err(error) => return Err(error),
            }
        }
    }
}

/// Discards a frame refused for an oversized argument as its bytes arrive, so the
/// connection survives without buffering the frame. The frame still counts against
/// `max_command_bytes`; past that, the client is not speaking RESP and is cut off.
pub(super) struct FrameSkip {
    /// Bytes of the current argument (payload and CRLF) still to drop.
    left: usize,
    args_left: usize,
    /// Frame bytes still allowed under `max_command_bytes`.
    budget: usize,
    pub(super) message: &'static str,
}

impl FrameSkip {
    pub(super) fn new(
        message: &'static str,
        parsed: usize,
        skip: usize,
        args_left: usize,
        max_command_bytes: usize,
    ) -> Self {
        Self {
            left: skip,
            args_left,
            budget: max_command_bytes.saturating_sub(parsed.saturating_add(skip)),
            message,
        }
    }

    /// Drops what `input` holds of the frame. True once the whole frame is gone.
    pub(super) fn advance(
        &mut self,
        input: &mut InputBuffer,
    ) -> Result<bool, crate::protocol::ProtocolError> {
        loop {
            let dropped = self.left.min(input.len());

            input.consume(dropped);
            self.left -= dropped;

            if self.left > 0 {
                return Ok(false);
            }

            if self.args_left == 0 {
                return Ok(true);
            }

            let Some((header, length)) = crate::protocol::bulk_header(input)? else {
                return Ok(false);
            };
            let size = header.saturating_add(length).saturating_add(2);

            if size > self.budget {
                return Err(crate::protocol::ProtocolError::LimitExceeded(
                    "command is too large",
                ));
            }

            self.budget -= size;
            input.consume(header);
            self.left = length + 2;
            self.args_left -= 1;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn consume_advances_without_moving_the_tail_until_half_is_dead() {
        let mut input = InputBuffer::default();

        input.extend_from_slice(b"0123456789");
        input.consume(2);
        assert_eq!(&*input, b"23456789");
        assert_eq!(input.start, 2);

        input.consume(4);
        assert_eq!(&*input, b"6789");
        assert_eq!(input.start, 0, "more than half dead compacts");

        input.consume(4);
        assert!(input.is_empty());
        assert_eq!(input.start, 0);
    }

    #[test]
    fn release_idle_shrinks_only_large_empty_buffers() {
        let mut input = InputBuffer::default();

        input.extend_from_slice(&vec![0; 64 * 1024]);
        input.release_idle();
        assert!(
            input.bytes.capacity() >= 64 * 1024,
            "non-empty keeps its buffer"
        );

        input.consume(64 * 1024);
        input.release_idle();
        assert!(input.bytes.capacity() <= IDLE_KEEP_BYTES * 2);
    }

    #[test]
    fn known_frame_size_holds_back_parsing() {
        let mut input = InputBuffer::default();

        input.extend_from_slice(b"abc");
        input.set_need(10);
        assert!(input.waiting_for_frame());

        input.extend_from_slice(b"defghij");
        assert!(!input.waiting_for_frame());
    }
}
