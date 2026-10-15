//! A byte arena addressed by `u32` offsets, for collections that keep many small byte strings.
//!
//! A collection that boxes every member pays one allocation header and one pointer per
//! member. Here the members live back to back in 64 KiB chunks and the lookup table holds
//! 4-byte offsets instead of 16-byte `Box<[u8]>` values. The compact set, the compact hash
//! and the sorted-set B+ tree share this helper.
//!
//! Chunks are never reallocated, so growth copies nothing and leaves no freed giant
//! buffers behind for the allocator to hold on to. A block is `varint(body_len) body`; a
//! block larger than `JUMBO_MIN` bytes gets an allocation of its own (the body only). The
//! body is opaque: a set stores the member, a hash stores `varint(field_len) field value`.
//! Freeing a chunk block counts its bytes as dead in the arena and in its chunk. A chunk
//! whose every block is dead is released at once, unless it still takes new blocks or a
//! sliced compaction is emptying it; a set popped from one end so gives its memory back
//! chunk by chunk and never needs a rewrite. [`Arena::should_compact`] says when the
//! holes left in partly live chunks outweigh the live data and [`Arena::compact`] then
//! rewrites the live blocks and the offsets that point at them.
//!
//! An offset is `chunk << 16 | position`, or `JUMBO_FLAG | slot` for a jumbo block.
//! [`Arena::can_alloc`] tells the caller before the 2 GiB chunk space runs out so the
//! collection can move to a different encoding.

const CHUNK_BITS: u32 = 16;
const CHUNK_SIZE: usize = 1 << CHUNK_BITS;
const MAX_CHUNKS: usize = 1 << 15;

#[cfg(test)]
thread_local! {
    /// A smaller chunk limit for tests that need a full arena without 2 GiB of data.
    static TEST_MAX_CHUNKS: std::cell::Cell<usize> = const { std::cell::Cell::new(MAX_CHUNKS) };
}

#[cfg(not(test))]
#[inline(always)]
fn max_chunks() -> usize {
    MAX_CHUNKS
}

#[cfg(test)]
fn max_chunks() -> usize {
    TEST_MAX_CHUNKS.with(std::cell::Cell::get)
}

/// Caps every arena created or grown on this thread at `chunks` chunks until the guard
/// drops, so tests can fill one.
#[cfg(test)]
pub fn limit_chunks_for_test(chunks: usize) -> impl Drop {
    struct Restore(usize);

    impl Drop for Restore {
        fn drop(&mut self) {
            TEST_MAX_CHUNKS.with(|limit| limit.set(self.0));
        }
    }

    Restore(TEST_MAX_CHUNKS.with(|limit| limit.replace(chunks)))
}
const JUMBO_FLAG: u32 = 1 << 31;
/// Blocks larger than this skip the chunks, so a chunk never wastes much of its tail.
const JUMBO_MIN: usize = CHUNK_SIZE / 4;

/// Chunks that can be needed to hold `live` bytes of chunk blocks: a block never
/// straddles chunks, so a chunk may leave up to one block's size of its tail unused.
fn chunks_for(live: usize) -> usize {
    live / (CHUNK_SIZE - JUMBO_MIN) + 2
}

/// Dead bytes below this never trigger a compaction, so a small arena is not rewritten
/// for a few removals.
const COMPACT_MIN_DEAD: usize = 4096;

#[derive(Debug, Clone, Default)]
pub struct Arena {
    chunks: Vec<Chunk>,
    /// The chunk taking new blocks. `None` before the first block and while an evacuation
    /// waits for a chunk outside the evacuated set.
    tail: Option<u32>,
    /// The chunk taking relocated blocks during an evacuation. New writes never share
    /// it, so the chunks reserved for relocation cannot be eaten by writes appending to
    /// a chunk a relocation opened.
    move_tail: Option<u32>,
    /// Chunk numbers a finished evacuation emptied, reused before `chunks` grows.
    free_chunks: Vec<u32>,
    jumbo: Vec<Option<Box<[u8]>>>,
    jumbo_free: Vec<u32>,
    /// Bytes written to chunks outside an evacuation, dead blocks included.
    used: usize,
    dead: usize,
    /// Capacity of every chunk and jumbo body, kept current for accounting.
    capacity: usize,
    /// A compaction in slices: see [`Arena::begin_evacuation`].
    evacuation: Option<Box<Evacuation>>,
}

/// One chunk's bytes and how many of them are dead blocks. An empty `bytes` (no
/// capacity) is a released chunk number waiting in `free_chunks`.
#[derive(Debug, Clone, Default)]
struct Chunk {
    bytes: Vec<u8>,
    /// Bytes of blocks freed or relocated out since the chunk was opened, so a chunk is
    /// released only when nothing in it is live.
    dead: u32,
}

/// The chunks a sliced compaction is emptying. Their live blocks move to fresh chunks
/// one by one ([`Arena::relocate`]); the chunks are released together at the end.
#[derive(Debug, Clone)]
struct Evacuation {
    /// One flag per chunk number that existed when the evacuation began.
    marked: Vec<bool>,
    /// Bytes written to the marked chunks, dead blocks included.
    used: usize,
    /// Bytes of the marked chunks that are dead or already moved out.
    gone: usize,
}

fn varint_size(mut value: usize) -> usize {
    let mut size = 1;

    while value >= 0x80 {
        value >>= 7;
        size += 1;
    }

    size
}

fn put_varint(buf: &mut Vec<u8>, mut value: usize) {
    while value >= 0x80 {
        buf.push((value as u8 & 0x7f) | 0x80);
        value >>= 7;
    }

    buf.push(value as u8);
}

/// Reads a varint at `at`; returns the value and the offset after it.
pub fn read_varint(buf: &[u8], mut at: usize) -> (usize, usize) {
    let mut value = 0usize;
    let mut shift = 0;

    loop {
        let byte = buf[at];

        at += 1;
        value |= usize::from(byte & 0x7f) << shift;

        if byte & 0x80 == 0 {
            return (value, at);
        }

        shift += 7;
    }
}

/// `varint(value)` in a stack buffer, for callers that build block bodies; the length is
/// returned with it.
pub fn encode_varint(mut value: usize) -> ([u8; 10], usize) {
    let mut out = [0u8; 10];
    let mut len = 0;

    while value >= 0x80 {
        out[len] = (value as u8 & 0x7f) | 0x80;
        value >>= 7;
        len += 1;
    }

    out[len] = value as u8;
    (out, len + 1)
}

/// Bytes a block with a body of `body_len` occupies, header included.
pub fn block_size(body_len: usize) -> usize {
    varint_size(body_len) + body_len
}

impl Arena {
    pub fn new() -> Self {
        Self::default()
    }

    /// True when a block with a body of `body_len` still gets an offset.
    pub fn can_alloc(&self, body_len: usize) -> bool {
        self.can_alloc_reserving(body_len, self.reserved_chunks())
    }

    /// `can_alloc` that leaves `reserve` chunk numbers unused.
    fn can_alloc_reserving(&self, body_len: usize, reserve: usize) -> bool {
        let size = block_size(body_len);

        if size > JUMBO_MIN {
            return self.jumbo.len() < (JUMBO_FLAG - 1) as usize || !self.jumbo_free.is_empty();
        }

        self.spare_chunks() > reserve
            || self
                .tail_chunk()
                .is_some_and(|chunk| chunk.len() + size <= CHUNK_SIZE)
    }

    /// Chunk numbers not holding a chunk.
    fn spare_chunks(&self) -> usize {
        max_chunks()
            .saturating_sub(self.chunks.len())
            .saturating_add(self.free_chunks.len())
    }

    /// Chunk numbers kept for the blocks a running evacuation still has to move, so
    /// new blocks can never leave a live block without a place to go.
    fn reserved_chunks(&self) -> usize {
        self.evacuation.as_ref().map_or(0, |evacuation| {
            chunks_for(evacuation.used.saturating_sub(evacuation.gone))
        })
    }

    fn tail_chunk(&self) -> Option<&Vec<u8>> {
        self.tail.map(|tail| &self.chunks[tail as usize].bytes)
    }

    /// True when blocks with bodies of `body_lens`, allocated in that order, would all get
    /// an offset. Mirrors the chunk packing of [`Arena::alloc`]; `O(len)`.
    pub fn fits(&self, body_lens: impl Iterator<Item = usize>) -> bool {
        let mut free_chunks = self.spare_chunks().saturating_sub(self.reserved_chunks());
        let mut tail = self
            .tail_chunk()
            .map_or(0, |chunk| CHUNK_SIZE - chunk.len());
        let mut jumbo_room = ((JUMBO_FLAG - 1) as usize)
            .saturating_sub(self.jumbo.len())
            .saturating_add(self.jumbo_free.len());

        for body_len in body_lens {
            let size = block_size(body_len);

            if size > JUMBO_MIN {
                if jumbo_room == 0 {
                    return false;
                }

                jumbo_room -= 1;
            } else if size <= tail {
                tail -= size;
            } else if free_chunks > 0 {
                free_chunks -= 1;
                tail = CHUNK_SIZE - size;
            } else {
                return false;
            }
        }

        true
    }

    /// Bytes of dead chunk blocks a compaction would give back.
    pub fn reclaimable(&self) -> usize {
        self.dead
            + self
                .evacuation
                .as_ref()
                .map_or(0, |evacuation| evacuation.gone)
    }

    /// Stores the concatenation of `parts` as one block and returns its offset.
    /// `None` when no offset is left.
    pub fn alloc(&mut self, parts: &[&[u8]]) -> Option<u32> {
        self.alloc_reserving(parts, self.reserved_chunks())
    }

    fn alloc_reserving(&mut self, parts: &[&[u8]], reserve: usize) -> Option<u32> {
        let body_len: usize = parts.iter().map(|part| part.len()).sum();

        if !self.can_alloc_reserving(body_len, reserve) {
            return None;
        }

        let size = block_size(body_len);

        if size > JUMBO_MIN {
            let mut body = Vec::with_capacity(body_len);

            for part in parts {
                body.extend_from_slice(part);
            }

            self.capacity += body.capacity();

            let body = Some(body.into_boxed_slice());
            let slot = match self.jumbo_free.pop() {
                Some(slot) => {
                    self.jumbo[slot as usize] = body;
                    slot
                }
                None => {
                    self.jumbo.push(body);
                    (self.jumbo.len() - 1) as u32
                }
            };

            return Some(JUMBO_FLAG | slot);
        }

        if self
            .tail_chunk()
            .is_none_or(|chunk| chunk.len() + size > CHUNK_SIZE)
        {
            if let Some(tail) = self.tail.take() {
                let full = &mut self.chunks[tail as usize].bytes;

                if full.capacity() > full.len() + 1024 {
                    let before = full.capacity();

                    full.shrink_to_fit();
                    self.capacity -= before - full.capacity();
                }

                // Its blocks may all have died while it was still taking new ones.
                self.release_if_dead(tail as usize);
            }

            let next = match self.free_chunks.pop() {
                Some(index) => index,
                None => {
                    self.chunks.push(Chunk::default());
                    (self.chunks.len() - 1) as u32
                }
            };

            self.tail = Some(next);
        }

        let index = self.tail.expect("tail chunk") as usize;
        let chunk = &mut self.chunks[index].bytes;

        if chunk.capacity() - chunk.len() < size {
            // Doubles inside the chunk, so a small arena does not pay for 64 KiB at once.
            let before = chunk.capacity();
            let grow = size
                .max(chunk.len())
                .min(CHUNK_SIZE - chunk.len())
                .max(size);

            chunk.reserve_exact(grow);
            self.capacity += chunk.capacity() - before;
        }

        let position = chunk.len();

        put_varint(chunk, body_len);

        for part in parts {
            chunk.extend_from_slice(part);
        }

        self.used += size;
        Some(((index as u32) << CHUNK_BITS) | position as u32)
    }

    /// The body of the block at `offset`.
    pub fn get(&self, offset: u32) -> &[u8] {
        if offset & JUMBO_FLAG != 0 {
            return self.jumbo[(offset & !JUMBO_FLAG) as usize]
                .as_deref()
                .expect("live jumbo block");
        }

        let chunk = &self.chunks[(offset >> CHUNK_BITS) as usize].bytes;
        let (len, start) = read_varint(chunk, (offset as usize) & (CHUNK_SIZE - 1));

        &chunk[start..start + len]
    }

    /// The body of the block at `offset`, writable in place. The length cannot change.
    pub fn get_mut(&mut self, offset: u32) -> &mut [u8] {
        if offset & JUMBO_FLAG != 0 {
            return self.jumbo[(offset & !JUMBO_FLAG) as usize]
                .as_deref_mut()
                .expect("live jumbo block");
        }

        let chunk = &mut self.chunks[(offset >> CHUNK_BITS) as usize].bytes;
        let (len, start) = read_varint(chunk, (offset as usize) & (CHUNK_SIZE - 1));

        &mut chunk[start..start + len]
    }

    /// Releases the block at `offset`. A chunk block counts as dead until its chunk is
    /// wholly dead or the next compaction; a jumbo block is dropped at once.
    pub fn free(&mut self, offset: u32) {
        if offset & JUMBO_FLAG != 0 {
            let slot = offset & !JUMBO_FLAG;

            if let Some(body) = self.jumbo[slot as usize].take() {
                self.capacity -= body.len();
                self.jumbo_free.push(slot);
            }

            return;
        }

        let size = block_size(self.get(offset).len());

        let index = (offset >> CHUNK_BITS) as usize;

        self.chunks[index].dead += size as u32;

        match &mut self.evacuation {
            Some(evacuation) if evacuation.holds(offset) => evacuation.gone += size,
            _ => {
                self.dead += size;
                self.release_if_dead(index);
            }
        }
    }

    /// Releases chunk `index` when every block in it is dead, it is not taking new or
    /// relocated blocks, and no evacuation holds it. Its bytes leave `used` and `dead`
    /// together, so the compaction trigger only sees the holes in chunks still in use.
    fn release_if_dead(&mut self, index: usize) {
        let chunk = &self.chunks[index];

        if chunk.dead as usize != chunk.bytes.len()
            || chunk.bytes.is_empty()
            || self.tail == Some(index as u32)
            || self.move_tail == Some(index as u32)
            || self
                .evacuation
                .as_ref()
                .is_some_and(|evacuation| evacuation.marks(index))
        {
            return;
        }

        let chunk = std::mem::take(&mut self.chunks[index]);

        self.capacity -= chunk.bytes.capacity();
        self.used -= chunk.bytes.len();
        self.dead -= chunk.bytes.len();

        if index + 1 == self.chunks.len() {
            self.chunks.pop();
        } else {
            self.free_chunks.push(index as u32);
        }
    }

    /// Bytes in chunks in use, dead blocks included, plus jumbo bodies.
    #[cfg(test)]
    pub fn len(&self) -> usize {
        self.used
            + self
                .jumbo
                .iter()
                .flatten()
                .map(|body| body.len())
                .sum::<usize>()
    }

    #[cfg(test)]
    pub fn dead_bytes(&self) -> usize {
        self.dead
    }

    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Recounts the running totals from the chunks and jumbo slots and checks them.
    #[cfg(test)]
    pub fn assert_consistent(&self) {
        let marked = |index: usize| {
            self.evacuation
                .as_ref()
                .is_some_and(|evacuation| evacuation.marks(index))
        };
        let capacity = self
            .chunks
            .iter()
            .map(|chunk| chunk.bytes.capacity())
            .sum::<usize>()
            + self
                .jumbo
                .iter()
                .flatten()
                .map(|body| body.len())
                .sum::<usize>();
        let (mut used, mut dead) = (0, 0);

        for (index, chunk) in self.chunks.iter().enumerate() {
            assert!(chunk.dead as usize <= chunk.bytes.len(), "chunk {index}");

            if !marked(index) {
                used += chunk.bytes.len();
                dead += chunk.dead as usize;
            }
        }

        for index in &self.free_chunks {
            assert_eq!(self.chunks[*index as usize].bytes.capacity(), 0);
        }

        assert_eq!(self.capacity, capacity, "capacity");
        assert_eq!(self.used, used, "used");
        assert_eq!(self.dead, dead, "dead");
    }

    /// Chunks holding bytes, released ones not counted.
    #[cfg(test)]
    pub fn chunks_in_use(&self) -> usize {
        self.chunks
            .iter()
            .filter(|chunk| chunk.bytes.capacity() > 0)
            .count()
    }

    /// True once at least half of the chunk bytes are dead blocks, and no sliced
    /// compaction is running.
    pub fn should_compact(&self) -> bool {
        self.evacuation.is_none() && self.dead >= COMPACT_MIN_DEAD && self.dead * 2 >= self.used
    }

    /// True while a sliced compaction is running.
    pub fn is_evacuating(&self) -> bool {
        self.evacuation.is_some()
    }

    /// Whether a sliced compaction is worth its bookkeeping and has room: below
    /// `min_chunks` chunks, or with too few chunk numbers left to hold the moved
    /// copies beside the originals, [`Arena::compact`] runs at once instead.
    pub fn can_evacuate(&self, min_chunks: usize) -> bool {
        let live = self.used.saturating_sub(self.dead);

        self.evacuation.is_none()
            && self.chunks.len() >= min_chunks
            && self.spare_chunks() >= chunks_for(live)
    }

    /// Starts a sliced compaction: every chunk in use is marked, new blocks go to fresh
    /// chunks, and the owner moves each live block with [`Arena::relocate`] before
    /// calling [`Arena::finish_evacuation`]. Reads stay valid throughout: a block is
    /// readable at its old offset until the marked chunks are released.
    pub fn begin_evacuation(&mut self) {
        debug_assert!(self.evacuation.is_none());

        let marked = self
            .chunks
            .iter()
            .map(|chunk| chunk.bytes.capacity() > 0)
            .collect();

        self.evacuation = Some(Box::new(Evacuation {
            marked,
            used: self.used,
            gone: self.dead,
        }));
        self.tail = None;
        self.move_tail = None;
        self.used = 0;
        self.dead = 0;
    }

    /// Gives up a running evacuation: the marked chunks become ordinary chunks again,
    /// holding the blocks not yet moved, and the moved-out copies count as dead there.
    /// For a relocation that found no room, which the reserve should rule out.
    pub fn abort_evacuation(&mut self) {
        let Some(evacuation) = self.evacuation.take() else {
            return;
        };

        self.used += evacuation.used;
        self.dead += evacuation.gone;
        self.release_move_tail();

        // A marked chunk whose blocks all moved out or died is empty now. Highest first,
        // so releasing the last chunk never shifts one still to be looked at.
        for (index, marked) in evacuation.marked.iter().enumerate().rev() {
            if *marked && index < self.chunks.len() {
                self.release_if_dead(index);
            }
        }
    }

    /// Trims the spare capacity of the relocation chunk once relocations end.
    fn release_move_tail(&mut self) {
        if let Some(index) = self.move_tail.take() {
            let chunk = &mut self.chunks[index as usize].bytes;
            let before = chunk.capacity();

            chunk.shrink_to_fit();
            self.capacity -= before - chunk.capacity();
            self.release_if_dead(index as usize);
        }
    }

    /// True when `offset` sits in a chunk the running evacuation empties.
    #[inline]
    pub fn needs_relocation(&self, offset: u32) -> bool {
        self.evacuation
            .as_ref()
            .is_some_and(|evacuation| evacuation.holds(offset))
    }

    /// Copies a block out of an evacuated chunk and returns its new offset. Relocated
    /// blocks fill chunks of their own, which the reserve keeps room for; `None` only
    /// when no offset is left anyway (the caller then aborts the evacuation).
    pub fn relocate(&mut self, offset: u32) -> Option<u32> {
        debug_assert!(self.needs_relocation(offset));

        // The source chunk is marked, so new blocks never land in it: lend it out while
        // the copy is written, without a temporary buffer.
        let index = (offset >> CHUNK_BITS) as usize;
        let source = std::mem::take(&mut self.chunks[index].bytes);
        let (len, start) = read_varint(&source, (offset as usize) & (CHUNK_SIZE - 1));
        std::mem::swap(&mut self.tail, &mut self.move_tail);
        let moved = self.alloc_reserving(&[&source[start..start + len]], 0);
        std::mem::swap(&mut self.tail, &mut self.move_tail);

        self.chunks[index].bytes = source;
        let moved = moved?;

        self.chunks[index].dead += block_size(len) as u32;

        if let Some(evacuation) = &mut self.evacuation {
            evacuation.gone += block_size(len);
        }

        Some(moved)
    }

    /// Releases the evacuated chunks. Every live block in them must have been relocated.
    pub fn finish_evacuation(&mut self) {
        let Some(evacuation) = self.evacuation.take() else {
            return;
        };

        debug_assert_eq!(evacuation.gone, evacuation.used, "live blocks left behind");
        self.release_move_tail();

        for (index, marked) in evacuation.marked.iter().enumerate() {
            if *marked {
                let chunk = std::mem::take(&mut self.chunks[index]);

                self.capacity -= chunk.bytes.capacity();
                self.free_chunks.push(index as u32);
            }
        }

        // Trailing free chunk numbers are dropped, so the list stays short.
        while self
            .chunks
            .last()
            .is_some_and(|chunk| chunk.bytes.capacity() == 0)
            && self.tail != Some((self.chunks.len() - 1) as u32)
        {
            let last = (self.chunks.len() - 1) as u32;

            self.chunks.pop();
            self.free_chunks.retain(|index| *index != last);
        }

        // Lowest numbers last, so `pop` reuses them first.
        self.free_chunks.sort_unstable_by(|a, b| b.cmp(a));
        self.chunks.shrink_to_fit();
    }

    /// Rewrites the live blocks into fresh chunks. `offsets` must yield the offset of every
    /// live block exactly once; each is updated to the block's new place.
    pub fn compact<'a>(&mut self, offsets: impl Iterator<Item = &'a mut u32>) {
        debug_assert!(self.evacuation.is_none());
        let mut fresh = Self::new();

        for offset in offsets {
            *offset = fresh
                .alloc(&[self.get(*offset)])
                .expect("live blocks fit a fresh arena");
        }

        if let Some(last) = fresh.chunks.last_mut().map(|chunk| &mut chunk.bytes) {
            let before = last.capacity();

            last.shrink_to_fit();
            fresh.capacity -= before - last.capacity();
        }

        *self = fresh;
    }
}

impl Evacuation {
    fn marks(&self, index: usize) -> bool {
        self.marked.get(index).is_some_and(|marked| *marked)
    }

    #[inline]
    fn holds(&self, offset: u32) -> bool {
        offset & JUMBO_FLAG == 0
            && self
                .marked
                .get((offset >> CHUNK_BITS) as usize)
                .is_some_and(|marked| *marked)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn blocks_round_trip_and_compact() {
        let mut arena = Arena::new();
        let mut offsets = Vec::new();
        let long = vec![7u8; 300];

        for index in 0..2_000u32 {
            let body = if index % 50 == 0 {
                long.clone()
            } else {
                index.to_le_bytes().to_vec()
            };

            offsets.push((arena.alloc(&[&body[..2], &body[2..]]).unwrap(), body));
        }

        for (offset, body) in &offsets {
            assert_eq!(arena.get(*offset), &body[..]);
        }

        let mut kept = Vec::new();

        for (index, (offset, body)) in offsets.into_iter().enumerate() {
            if index % 3 == 0 {
                kept.push((offset, body));
            } else {
                arena.free(offset);
            }
        }

        assert!(arena.should_compact());
        arena.compact(kept.iter_mut().map(|(offset, _)| offset));
        assert_eq!(arena.dead_bytes(), 0);
        assert_eq!(
            arena.len(),
            kept.iter().map(|(_, b)| block_size(b.len())).sum::<usize>()
        );

        for (offset, body) in &kept {
            assert_eq!(arena.get(*offset), &body[..]);
        }
    }

    #[test]
    fn evacuation_moves_blocks_in_slices_and_keeps_room_for_them() {
        let _limit = limit_chunks_for_test(24);
        let mut arena = Arena::new();
        let mut live = Vec::new();

        // About twelve chunks of blocks, two thirds of them freed.
        for index in 0..870u32 {
            let offset = arena.alloc(&[&[index as u8; 900]]).unwrap();

            if index % 3 == 0 {
                live.push((offset, index as u8));
            } else {
                arena.free(offset);
            }
        }

        assert!(arena.should_compact());
        assert!(arena.can_evacuate(2));
        arena.begin_evacuation();

        // New blocks fill what is left but never the room the moved copies need.
        let mut fresh = Vec::new();
        while let Some(offset) = arena.alloc(&[b"new block"]) {
            fresh.push(offset);
        }

        for (offset, byte) in &mut live {
            assert!(arena.needs_relocation(*offset));
            *offset = arena.relocate(*offset).expect("reserved room");
            assert!(!arena.needs_relocation(*offset));
            assert_eq!(arena.get(*offset), &[*byte; 900][..]);
        }

        let before = arena.capacity();
        arena.finish_evacuation();
        assert!(arena.capacity() < before);
        assert!(!arena.is_evacuating());
        assert!(
            live.iter()
                .all(|(offset, byte)| arena.get(*offset) == &[*byte; 900][..])
        );
        assert!(
            fresh
                .iter()
                .all(|offset| arena.get(*offset) == b"new block")
        );

        // Released chunk numbers are reused.
        assert!(arena.alloc(&[&[1u8; 900]]).is_some());
    }

    #[test]
    fn writes_filling_the_tail_cannot_starve_an_evacuation() {
        let _limit = limit_chunks_for_test(64);
        let mut arena = Arena::new();
        let mut live = Vec::new();

        for index in 0..36_000u32 {
            let offset = arena.alloc(&[&[index as u8; 20]]).unwrap();

            if index % 3 == 0 {
                live.push(offset);
            } else {
                arena.free(offset);
            }
        }

        assert!(arena.can_evacuate(2));
        arena.begin_evacuation();

        let mut pending = live.iter_mut();
        let mut done = false;

        while !done {
            // A write sized to nearly fill whatever chunk takes new blocks.
            let room = arena
                .tail_chunk()
                .map_or(CHUNK_SIZE, |chunk| CHUNK_SIZE - chunk.len());
            let body = room.saturating_sub(4).clamp(1, JUMBO_MIN - 4);
            let _ = arena.alloc(&[&vec![7u8; body]]);

            for _ in 0..32 {
                let Some(offset) = pending.next() else {
                    done = true;
                    break;
                };
                *offset = arena.relocate(*offset).expect("relocation room");
            }
        }

        arena.finish_evacuation();
        assert!(live.iter().all(|offset| arena.get(*offset).len() == 20));
    }

    #[test]
    fn an_aborted_evacuation_keeps_every_block_and_can_compact_later() {
        let mut arena = Arena::new();
        let mut live = Vec::new();

        for index in 0..9_000u32 {
            let offset = arena.alloc(&[&index.to_le_bytes(), &[1u8; 30]]).unwrap();

            if index % 3 == 0 {
                live.push((offset, index));
            } else {
                arena.free(offset);
            }
        }

        arena.begin_evacuation();
        for (offset, _) in live.iter_mut().take(500) {
            *offset = arena.relocate(*offset).unwrap();
        }
        arena.abort_evacuation();

        assert!(!arena.is_evacuating());
        assert!(arena.should_compact());
        assert!(
            live.iter()
                .all(|(offset, index)| arena.get(*offset)[..4] == index.to_le_bytes())
        );
        arena.compact(live.iter_mut().map(|(offset, _)| offset));
        assert_eq!(arena.dead_bytes(), 0);
        assert!(
            live.iter()
                .all(|(offset, index)| arena.get(*offset)[..4] == index.to_le_bytes())
        );
    }

    #[test]
    fn jumbo_blocks_get_their_own_slot_and_are_reused() {
        let mut arena = Arena::new();
        let big = vec![3u8; 40_000];
        let small = arena.alloc(&[b"small"]).unwrap();
        let first = arena.alloc(&[&big]).unwrap();

        assert_ne!(first & JUMBO_FLAG, 0);
        assert_eq!(arena.get(first), &big[..]);
        arena.free(first);

        let second = arena.alloc(&[&big[..20_000], &big[20_000..]]).unwrap();

        assert_eq!(second, first);
        assert_eq!(arena.get(second), &big[..]);
        assert_eq!(arena.get(small), b"small");
    }

    /// A `(offset, byte, len)` block, so a test can check what it reads back.
    fn put(arena: &mut Arena, byte: u8, len: usize) -> (u32, u8, usize) {
        (arena.alloc(&[&vec![byte; len]]).unwrap(), byte, len)
    }

    fn intact(arena: &Arena, blocks: &[(u32, u8, usize)]) -> bool {
        blocks
            .iter()
            .all(|(offset, byte, len)| arena.get(*offset) == &vec![*byte; *len][..])
    }

    #[test]
    fn popping_from_the_front_releases_each_emptied_chunk_without_a_compaction() {
        let mut arena = Arena::new();
        let mut blocks: std::collections::VecDeque<_> = (0..12_000u32)
            .map(|index| put(&mut arena, index as u8, 60))
            .collect();
        let chunks = arena.chunks_in_use();

        assert!(chunks >= 10, "{chunks} chunks");

        let mut released = 0;

        while let Some((offset, _, _)) = blocks.pop_front() {
            let before = arena.chunks_in_use();

            arena.free(offset);
            released += before - arena.chunks_in_use();

            // The holes never outweigh the live data: at most one chunk is part dead.
            assert!(arena.dead_bytes() < CHUNK_SIZE);
            assert!(
                arena.capacity() <= (blocks.len() * 61).max(1) + 2 * CHUNK_SIZE,
                "capacity {} for {} blocks",
                arena.capacity(),
                blocks.len()
            );

            if blocks.len().is_multiple_of(997) {
                arena.assert_consistent();
                assert!(intact(&arena, blocks.make_contiguous()));
            }
        }

        // Every chunk but the one still taking blocks went back.
        assert_eq!(released, chunks - 1);
        assert_eq!(arena.chunks_in_use(), 1);
        arena.assert_consistent();

        // The tail goes too once it stops taking blocks and its last block dies.
        let filler: Vec<_> = (0..1_200).map(|_| put(&mut arena, 9, 60)).collect();

        for (offset, _, _) in filler {
            arena.free(offset);
        }

        assert_eq!(arena.chunks_in_use(), 1);
        arena.assert_consistent();
    }

    #[test]
    fn a_queue_of_adds_and_pops_keeps_its_memory_flat() {
        let mut arena = Arena::new();
        let mut queue: std::collections::VecDeque<_> = (0..5_000u32)
            .map(|index| put(&mut arena, index as u8, 40))
            .collect();
        let mut peak_after_warmup = 0;

        for round in 0..400u32 {
            for step in 0..500u32 {
                queue.push_back(put(
                    &mut arena,
                    (round ^ step) as u8,
                    30 + step as usize % 20,
                ));
                arena.free(queue.pop_front().unwrap().0);
            }

            if round == 20 {
                peak_after_warmup = arena.capacity();
            } else if round > 20 {
                assert!(
                    arena.capacity() <= peak_after_warmup + CHUNK_SIZE,
                    "round {round}: {} > {peak_after_warmup}",
                    arena.capacity()
                );
            }

            assert!(!arena.should_compact(), "round {round} needed a compaction");
        }

        arena.assert_consistent();
        assert!(intact(&arena, queue.make_contiguous()));
    }

    #[test]
    fn a_chunk_with_one_live_block_stays_and_freed_numbers_are_reused() {
        let mut arena = Arena::new();
        let blocks: Vec<_> = (0..3_000u32)
            .map(|index| put(&mut arena, index as u8, 60))
            .collect();
        let first_chunk = |offset: u32| offset >> CHUNK_BITS == 0;
        let keep = blocks[100];

        for block in blocks.iter().filter(|block| first_chunk(block.0)) {
            if block.0 != keep.0 {
                arena.free(block.0);
            }
        }

        let before = arena.chunks_in_use();

        assert!(intact(&arena, &[keep]));
        arena.free(keep.0);
        assert_eq!(arena.chunks_in_use(), before - 1);
        arena.assert_consistent();

        // New chunks take the released number first.
        let fill: Vec<_> = (0..2_000).map(|_| put(&mut arena, 5, 60)).collect();

        assert!(fill.iter().any(|block| first_chunk(block.0)));
        assert!(intact(&arena, &fill));
        assert!(intact(
            &arena,
            &blocks
                .iter()
                .copied()
                .filter(|b| !first_chunk(b.0))
                .collect::<Vec<_>>()
        ));
        arena.assert_consistent();
    }

    #[test]
    fn evacuated_chunks_are_left_to_the_evacuation_and_an_abort_keeps_counts_exact() {
        let mut arena = Arena::new();
        let mut live = Vec::new();

        for index in 0..9_000u32 {
            let block = put(&mut arena, index as u8, 30);

            if index % 3 == 0 {
                live.push(block);
            } else {
                arena.free(block.0);
            }
        }

        let chunks = arena.chunks_in_use();

        arena.begin_evacuation();
        arena.assert_consistent();

        // Freeing everything in a marked chunk does not release it under the evacuation.
        let (first, rest): (Vec<_>, Vec<_>) = live
            .into_iter()
            .partition(|block| block.0 >> CHUNK_BITS == 0);

        for block in &first {
            arena.free(block.0);
        }

        assert_eq!(arena.chunks_in_use(), chunks);

        // Half the rest moves, then the evacuation is given up.
        let (mut moved, stayed): (Vec<_>, Vec<_>) = rest
            .into_iter()
            .enumerate()
            .partition(|(index, _)| index % 2 == 0);

        for (_, block) in &mut moved {
            block.0 = arena.relocate(block.0).unwrap();
        }

        arena.abort_evacuation();
        arena.assert_consistent();

        let mut live: Vec<_> = moved.into_iter().chain(stayed).map(|(_, b)| b).collect();

        assert!(intact(&arena, &live));

        // Once ordinary again, the dead chunks go as their last live blocks die.
        live.sort_by_key(|block| block.0);

        while let Some(block) = live.pop() {
            arena.free(block.0);
        }

        arena.assert_consistent();
        assert!(
            arena.chunks_in_use() <= 2,
            "{} chunks",
            arena.chunks_in_use()
        );
    }

    #[test]
    fn a_relocation_chunk_whose_blocks_all_died_keeps_taking_relocations() {
        let mut arena = Arena::new();
        let blocks: Vec<_> = (0..3_000u32)
            .map(|index| put(&mut arena, index as u8, 60))
            .collect();

        arena.begin_evacuation();

        // The first moved copy opens the relocation chunk and is its only block; freeing
        // it leaves that chunk wholly dead while it is still the relocation target.
        let moved = arena.relocate(blocks[0].0).unwrap();

        arena.free(moved);
        arena.assert_consistent();

        let again = arena.relocate(blocks[1].0).unwrap();

        assert_eq!(arena.get(again), &[blocks[1].1; 60][..]);
        assert_eq!(again >> CHUNK_BITS, moved >> CHUNK_BITS);
        arena.assert_consistent();
    }

    #[test]
    fn an_abort_releases_marked_chunks_that_were_wholly_relocated() {
        let mut arena = Arena::new();
        let mut blocks: Vec<_> = (0..3_000u32)
            .map(|index| put(&mut arena, index as u8, 60))
            .collect();

        assert_eq!(arena.chunks_in_use(), 3);
        arena.begin_evacuation();

        for block in blocks.iter_mut().filter(|block| block.0 >> CHUNK_BITS == 0) {
            block.0 = arena.relocate(block.0).unwrap();
        }

        // Chunk 0 is empty, chunks 1 and 2 still hold their blocks, one chunk took the
        // moved copies.
        assert_eq!(arena.chunks_in_use(), 4);
        arena.abort_evacuation();
        arena.assert_consistent();
        assert_eq!(arena.chunks_in_use(), 3);
        assert_eq!(arena.chunks[0].bytes.capacity(), 0);
        assert_eq!(arena.chunks[0].dead, 0);
        assert_eq!(arena.dead_bytes(), 0);
        assert!(intact(&arena, &blocks));
    }

    #[test]
    fn get_mut_overwrites_in_place() {
        let mut arena = Arena::new();
        let offset = arena.alloc(&[b"abcd"]).unwrap();

        arena.get_mut(offset).copy_from_slice(b"wxyz");
        assert_eq!(arena.get(offset), b"wxyz");
    }
}
