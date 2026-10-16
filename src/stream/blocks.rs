//! A stream's entries packed into blocks of up to `BLOCK_RECORDS` records.
//!
//! A block keeps its records back to back in one buffer, so appending allocates nothing
//! per entry and an entry costs its bytes plus a few for the id. A record is
//!
//! - a varint of `(millis - block base) << 2 | flags`,
//! - the sequence as a varint,
//! - the length of the body as a varint,
//! - the body: either the field-value pairs in the layout of [`StreamFields`], or, when
//!   the entry names the same fields in the same order as the block's first entry (the
//!   usual case), only the values.
//!
//! Deleting marks the record and leaves its bytes; a block is rewritten once half of it
//! is dead. Trimming moves the block's start forward the same way, and drops whole
//! blocks in one step.

use std::cmp::Ordering;
use std::collections::VecDeque;

use crate::stream::{StreamEntry, StreamFields};

const BLOCK_RECORDS: u32 = 128;
const BLOCK_BYTES: usize = 4096;
/// A record whose millis is this far past the block's base starts a block of its own, so
/// the delta and the flags fit one `u64`.
const MAX_DELTA: u64 = 1 << 56;
const DELETED: u8 = 1;
const SHAPED: u8 = 2;

type Id = (u64, u64);

/// Bytes one block costs besides its records: the block itself with the slack of the ring
/// that holds it, and the allocator's header for its two buffers.
pub(crate) const BLOCK_COST: usize = size_of::<Block>() + size_of::<Block>() / 4 + 32;

#[derive(Clone, Debug)]
struct Block {
    base_millis: u64,
    /// Id of the first live record.
    first_millis: u64,
    first_sequence: u64,
    /// Live records; a block with none is dropped.
    live: u32,
    /// Records in the buffer, dead ones included.
    records: u32,
    /// Offset of the first live record.
    head: usize,
    /// Offset of the last live record.
    last: usize,
    /// Bytes of records that are no longer live, those before `head` included.
    dead: usize,
    /// The field names of the block's first entry, each as a length and its bytes.
    names: Box<[u8]>,
    data: Vec<u8>,
}

struct Record<'a> {
    millis: u64,
    sequence: u64,
    flags: u8,
    body: &'a [u8],
    start: usize,
    end: usize,
}

impl Record<'_> {
    fn id(&self) -> Id {
        (self.millis, self.sequence)
    }

    fn deleted(&self) -> bool {
        self.flags & DELETED != 0
    }
}

impl Block {
    fn record_at(&self, at: usize) -> Record<'_> {
        let (head, used_head) = get_varint(&self.data[at..]);
        let (sequence, used_sequence) = get_varint(&self.data[at + used_head..]);
        let (length, used_length) = get_varint(&self.data[at + used_head + used_sequence..]);
        let body_start = at + used_head + used_sequence + used_length;
        let end = body_start + length as usize;

        Record {
            millis: self.base_millis + (head >> 2),
            sequence,
            flags: (head & 3) as u8,
            body: &self.data[body_start..end],
            start: at,
            end,
        }
    }

    /// The live records from offset `at`.
    fn live_from(&self, at: usize) -> Live<'_> {
        Live { block: self, at }
    }

    fn id_at(&self, at: usize) -> Id {
        self.record_at(at).id()
    }

    fn first_id(&self) -> Id {
        (self.first_millis, self.first_sequence)
    }

    fn entry(&self, record: &Record<'_>) -> StreamEntry {
        let fields = if record.flags & SHAPED == 0 {
            StreamFields::from_packed(record.body)
        } else {
            StreamFields::interleave(&self.names, record.body)
        };

        StreamEntry {
            millis: record.millis,
            sequence: record.sequence,
            fields,
        }
    }

    /// Whether a record with these fields can leave its names out.
    fn names_match(&self, fields: &[(impl AsRef<[u8]>, impl AsRef<[u8]>)]) -> bool {
        let mut names = &self.names[..];

        for (field, _) in fields {
            let field = field.as_ref();
            if names.is_empty() {
                return false;
            }

            let (length, used) = get_varint(names);

            if length as usize != field.len() || names[used..used + field.len()] != *field {
                return false;
            }

            names = &names[used + field.len()..];
        }

        names.is_empty()
    }

    /// Marks the record at `start..end` dead and keeps `first`, `head` and `last` on live
    /// records. The block must still have a live record afterwards.
    fn kill(&mut self, start: usize, end: usize) {
        self.data[start] |= DELETED;
        self.live -= 1;
        self.dead += end - start;
    }

    /// Moves `head` and `first` to the first live record.
    fn settle_head(&mut self) {
        let (head, id) = {
            let record = self
                .live_from(self.head)
                .next()
                .expect("a block with live records has a first one");

            (record.start, record.id())
        };

        self.head = head;
        self.first_millis = id.0;
        self.first_sequence = id.1;
    }

    fn settle_last(&mut self) {
        let last = self
            .live_from(self.head)
            .last()
            .expect("a block with live records has a last one")
            .start;

        self.last = last;
    }

    /// Drops the live records at the front while `drop` says to, never the last one of
    /// the block. Returns how many went.
    fn drop_head(&mut self, mut drop: impl FnMut(Id) -> bool) -> usize {
        let mut dropped = 0;
        let mut at = self.head;
        let first;

        loop {
            let (end, id, deleted) = {
                let record = self.record_at(at);

                (record.end, record.id(), record.deleted())
            };

            if deleted {
                at = end;
                continue;
            }

            if self.live == 1 || !drop(id) {
                first = id;
                break;
            }

            self.live -= 1;
            self.dead += end - at;
            dropped += 1;
            at = end;
        }

        self.head = at;
        self.first_millis = first.0;
        self.first_sequence = first.1;
        dropped
    }

    /// Rewrites the buffer without the dead records once they are the larger part.
    /// Returns how many bytes the buffer lost.
    fn compact(&mut self) -> usize {
        if self.dead * 2 <= self.data.len() {
            return 0;
        }

        let before = self.data.len();
        let mut packed = Vec::with_capacity(before - self.dead);
        let mut last = 0;

        for record in self.live_from(self.head) {
            last = packed.len();
            packed.extend_from_slice(&self.data[record.start..record.end]);
        }

        self.data = packed;
        self.head = 0;
        self.last = last;
        self.dead = 0;
        self.records = self.live;
        before - self.data.len()
    }
}

struct Live<'a> {
    block: &'a Block,
    at: usize,
}

impl<'a> Iterator for Live<'a> {
    type Item = Record<'a>;

    fn next(&mut self) -> Option<Record<'a>> {
        while self.at < self.block.data.len() {
            let record = self.block.record_at(self.at);

            self.at = record.end;

            if !record.deleted() {
                return Some(record);
            }
        }

        None
    }
}

/// The blocks of one stream, in id order.
#[derive(Clone, Debug, Default)]
pub(crate) struct Blocks {
    blocks: VecDeque<Block>,
    /// Live entries.
    len: usize,
    /// Bytes in the blocks' buffers.
    data_bytes: usize,
    /// Bytes in the blocks' field-name lists.
    name_bytes: usize,
}

impl Blocks {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) fn len(&self) -> usize {
        self.len
    }

    /// Heap bytes the blocks take, counting the open block at its capacity.
    pub(crate) fn bytes(&self) -> usize {
        let slack = self
            .blocks
            .back()
            .map_or(0, |block| block.data.capacity() - block.data.len());

        self.blocks.len() * BLOCK_COST + self.data_bytes + self.name_bytes + slack
    }

    /// Adds an entry whose id is above every id held.
    pub(crate) fn push(
        &mut self,
        millis: u64,
        sequence: u64,
        fields: &[(impl AsRef<[u8]>, impl AsRef<[u8]>)],
    ) {
        // A block stays within `BLOCK_BYTES`, an allocation size the allocator serves
        // without rounding up.
        let reach: usize = fields
            .iter()
            .map(|(field, value)| field.as_ref().len() + value.as_ref().len() + 4)
            .sum::<usize>()
            + 8;
        let open = self.blocks.back().is_some_and(|block| {
            block.records < BLOCK_RECORDS
                && block.data.len() + reach <= BLOCK_BYTES
                && millis
                    .checked_sub(block.base_millis)
                    .is_some_and(|delta| delta < MAX_DELTA)
        });

        if !open {
            // A stream that fills blocks keeps filling them alike: the next one starts
            // with the room the last one needed, so it grows without copying.
            let mut room = 0;

            if let Some(sealed) = self.blocks.back_mut() {
                sealed.data.shrink_to_fit();
                room = sealed.data.len() - sealed.dead;
            }

            let names = encode_names(fields);

            self.name_bytes += names.len();
            self.blocks.push_back(Block {
                base_millis: millis,
                first_millis: millis,
                first_sequence: sequence,
                live: 0,
                records: 0,
                head: 0,
                last: 0,
                dead: 0,
                names,
                data: Vec::with_capacity(room),
            });
        }

        let block = self.blocks.back_mut().expect("a block was just ensured");
        let shaped = block.names_match(fields);
        let body: usize = fields
            .iter()
            .map(|(field, value)| {
                let (field, value) = (field.as_ref(), value.as_ref());
                let value_part = varint_len(value.len() as u64) + value.len();

                if shaped {
                    value_part
                } else {
                    varint_len(field.len() as u64) + field.len() + value_part
                }
            })
            .sum();
        let head = ((millis - block.base_millis) << 2) | if shaped { u64::from(SHAPED) } else { 0 };
        let total = varint_len(head) + varint_len(sequence) + varint_len(body as u64) + body;

        if block.data.capacity() - block.data.len() < total {
            // Grow by a fraction, not a doubling: a sealed block gives back what is unused.
            block
                .data
                .reserve_exact(total.max(block.data.len() / 4).max(16));
        }

        block.last = block.data.len();

        if block.live == 0 {
            block.first_millis = millis;
            block.first_sequence = sequence;
            block.head = block.last;
        }

        put_varint(&mut block.data, head);
        put_varint(&mut block.data, sequence);
        put_varint(&mut block.data, body as u64);

        for (field, value) in fields {
            if !shaped {
                put_bytes(&mut block.data, field.as_ref());
            }

            put_bytes(&mut block.data, value.as_ref());
        }

        block.records += 1;
        block.live += 1;
        self.len += 1;
        self.data_bytes += total;
    }

    /// The index of the last block that starts at or before `id`.
    fn block_for(&self, id: Id) -> Option<usize> {
        self.blocks
            .partition_point(|block| block.first_id() <= id)
            .checked_sub(1)
    }

    /// Where the first live record at or above `id` (strictly above when `after`) sits:
    /// a block index and an offset. A block index past the end means there is none.
    fn position(&self, id: Id, after: bool) -> (usize, usize) {
        let Some(index) = self.block_for(id) else {
            return (0, self.blocks.front().map_or(0, |block| block.head));
        };
        let block = &self.blocks[index];

        for record in block.live_from(block.head) {
            let order = record.id().cmp(&id);

            if order == Ordering::Greater || (order == Ordering::Equal && !after) {
                return (index, record.start);
            }
        }

        (
            index + 1,
            self.blocks.get(index + 1).map_or(0, |next| next.head),
        )
    }

    fn find(&self, id: Id) -> Option<(usize, usize, usize)> {
        let index = self.block_for(id)?;
        let block = &self.blocks[index];

        for record in block.live_from(block.head) {
            match record.id().cmp(&id) {
                Ordering::Equal => return Some((index, record.start, record.end)),
                Ordering::Greater => return None,
                Ordering::Less => {}
            }
        }

        None
    }

    pub(crate) fn get(&self, millis: u64, sequence: u64) -> Option<StreamEntry> {
        let (index, start, _) = self.find((millis, sequence))?;
        let block = &self.blocks[index];

        Some(block.entry(&block.record_at(start)))
    }

    pub(crate) fn contains(&self, millis: u64, sequence: u64) -> bool {
        // The newest entry is the one most often asked for.
        if let Some(block) = self.blocks.back() {
            match (millis, sequence).cmp(&block.id_at(block.last)) {
                Ordering::Equal => return true,
                Ordering::Greater => return false,
                Ordering::Less => {}
            }
        }

        self.find((millis, sequence)).is_some()
    }

    pub(crate) fn first(&self) -> Option<StreamEntry> {
        let block = self.blocks.front()?;

        Some(block.entry(&block.record_at(block.head)))
    }

    pub(crate) fn last(&self) -> Option<StreamEntry> {
        let block = self.blocks.back()?;

        Some(block.entry(&block.record_at(block.last)))
    }

    /// Every entry, oldest first.
    pub(crate) fn to_vec(&self) -> Vec<StreamEntry> {
        let mut entries = Vec::with_capacity(self.len);

        for block in &self.blocks {
            entries.extend(
                block
                    .live_from(block.head)
                    .map(|record| block.entry(&record)),
            );
        }

        entries
    }

    /// Removes one entry. Returns whether it was there.
    pub(crate) fn remove(&mut self, millis: u64, sequence: u64) -> bool {
        let id = (millis, sequence);
        let Some((index, start, end)) = self.find(id) else {
            return false;
        };

        self.len -= 1;

        let block = &mut self.blocks[index];

        if block.live == 1 {
            self.drop_block(index);
            return true;
        }

        block.kill(start, end);

        if start == block.head {
            block.settle_head();
        }

        if start == block.last {
            block.settle_last();
        }

        let freed = block.compact();

        self.data_bytes -= freed;
        true
    }

    fn drop_block(&mut self, index: usize) {
        let block = self.blocks.remove(index).expect("index was found");

        self.data_bytes -= block.data.len();
        self.name_bytes -= block.names.len();
    }

    /// Removes the `count` oldest entries.
    pub(crate) fn drop_front(&mut self, mut count: usize) {
        count = count.min(self.len);

        while count > 0 {
            let live = self.blocks[0].live as usize;

            if live <= count {
                count -= live;
                self.len -= live;
                self.drop_block(0);
                continue;
            }

            let block = &mut self.blocks[0];
            let mut left = count;
            let dropped = block.drop_head(|_| {
                let go = left > 0;

                left = left.saturating_sub(1);
                go
            });

            self.len -= dropped;
            self.data_bytes -= block.compact();
            break;
        }
    }

    /// Removes the entries with an id below `id`. Returns how many went.
    pub(crate) fn drop_before(&mut self, millis: u64, sequence: u64) -> usize {
        let id = (millis, sequence);
        let before = self.len;

        while let Some(block) = self.blocks.front_mut() {
            if block.id_at(block.last) < id {
                self.len -= block.live as usize;
                self.drop_block(0);
                continue;
            }

            let dropped = block.drop_head(|record| record < id);

            self.len -= dropped;
            self.data_bytes -= block.compact();
            break;
        }

        before - self.len
    }

    /// Up to `limit` entries above `id`, oldest first.
    pub(crate) fn after(&self, millis: u64, sequence: u64, limit: usize) -> Vec<StreamEntry> {
        let (index, at) = self.position((millis, sequence), true);

        self.forward(index, at, None, limit)
    }

    fn forward(
        &self,
        mut index: usize,
        mut at: usize,
        end: Option<Id>,
        limit: usize,
    ) -> Vec<StreamEntry> {
        let mut entries = Vec::with_capacity(limit.min(64));

        while let Some(block) = self.blocks.get(index) {
            for record in block.live_from(at) {
                if entries.len() == limit || end.is_some_and(|end| record.id() > end) {
                    return entries;
                }

                entries.push(block.entry(&record));
            }

            index += 1;
            at = self.blocks.get(index).map_or(0, |next| next.head);
        }

        entries
    }

    /// Up to `count` entries from `start` up to and including `end` (no bound for
    /// `None`), oldest first, or newest first when `reverse`.
    pub(crate) fn range(
        &self,
        start: Option<Id>,
        end: Option<Id>,
        count: usize,
        reverse: bool,
    ) -> Vec<StreamEntry> {
        if count == 0 || self.len == 0 {
            return Vec::new();
        }

        if !reverse {
            let (index, at) = match start {
                Some(start) => self.position(start, false),
                None => (0, self.blocks[0].head),
            };

            return self.forward(index, at, end, count);
        }

        let mut entries = Vec::with_capacity(count.min(64));
        let mut index = match end {
            Some(end) => match self.block_for(end) {
                Some(index) => index,
                None => return entries,
            },
            None => self.blocks.len() - 1,
        };
        let mut offsets: Vec<usize> = Vec::new();

        loop {
            let block = &self.blocks[index];

            if end.is_none() && count == 1 && index == self.blocks.len() - 1 {
                offsets.push(block.last);
            } else {
                offsets.extend(
                    block
                        .live_from(block.head)
                        .take_while(|record| end.is_none_or(|end| record.id() <= end))
                        .map(|record| record.start),
                );
            }

            for &at in offsets.iter().rev() {
                let record = block.record_at(at);

                if start.is_some_and(|start| record.id() < start) {
                    return entries;
                }

                entries.push(block.entry(&record));

                if entries.len() == count {
                    return entries;
                }
            }

            offsets.clear();

            if index == 0 {
                return entries;
            }

            index -= 1;
        }
    }

    /// Entries with an id above `id`.
    pub(crate) fn count_after(&self, millis: u64, sequence: u64) -> usize {
        let id = (millis, sequence);
        let Some(index) = self.block_for(id) else {
            return self.len;
        };
        let block = &self.blocks[index];
        let inside = block
            .live_from(block.head)
            .filter(|record| record.id() > id)
            .count();

        inside
            + self
                .blocks
                .range(index + 1..)
                .map(|block| block.live as usize)
                .sum::<usize>()
    }
}

fn encode_names(fields: &[(impl AsRef<[u8]>, impl AsRef<[u8]>)]) -> Box<[u8]> {
    let mut names = Vec::new();

    for (field, _) in fields {
        put_bytes(&mut names, field.as_ref());
    }

    names.into_boxed_slice()
}

fn put_bytes(buffer: &mut Vec<u8>, bytes: &[u8]) {
    put_varint(buffer, bytes.len() as u64);
    buffer.extend_from_slice(bytes);
}

fn varint_len(mut value: u64) -> usize {
    let mut len = 1;

    while value >= 0x80 {
        value >>= 7;
        len += 1;
    }

    len
}

fn put_varint(buffer: &mut Vec<u8>, mut value: u64) {
    while value >= 0x80 {
        buffer.push((value as u8) | 0x80);
        value >>= 7;
    }

    buffer.push(value as u8);
}

fn get_varint(buffer: &[u8]) -> (u64, usize) {
    let first = buffer[0];

    if first < 0x80 {
        return (u64::from(first), 1);
    }

    let mut value = u64::from(first & 0x7f);

    for (index, byte) in buffer.iter().enumerate().skip(1) {
        value |= u64::from(byte & 0x7f) << (7 * index);

        if byte & 0x80 == 0 {
            return (value, index + 1);
        }
    }

    unreachable!("stream blocks are written by `Blocks::push`")
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    type Fields = Vec<(Vec<u8>, Vec<u8>)>;

    fn entry_of(millis: u64, sequence: u64, fields: &Fields) -> StreamEntry {
        StreamEntry {
            millis,
            sequence,
            fields: StreamFields::from_pairs(
                fields
                    .iter()
                    .map(|(field, value)| (field.as_slice(), value.as_slice())),
            ),
        }
    }

    #[derive(Clone, Debug)]
    enum Op {
        Push {
            millis_step: u64,
            shape: u8,
            size: u8,
        },
        Remove(usize),
        DropFront(usize),
        DropBefore(usize),
        Probe(usize, usize, usize),
    }

    fn op() -> impl Strategy<Value = Op> {
        prop_oneof![
            6 => (
                prop_oneof![Just(0u64), 0u64..4, Just(1u64 << 56), Just(u64::MAX / 8)],
                0u8..4,
                0u8..40,
            )
                .prop_map(|(millis_step, shape, size)| Op::Push { millis_step, shape, size }),
            2 => (0usize..400).prop_map(Op::Remove),
            1 => (0usize..300).prop_map(Op::DropFront),
            1 => (0usize..400).prop_map(Op::DropBefore),
            3 => (0usize..400, 0usize..400, 0usize..6).prop_map(|(a, b, c)| Op::Probe(a, b, c)),
        ]
    }

    fn fields_for(shape: u8, size: u8, tag: usize) -> Fields {
        let value = vec![b'a' + (tag % 26) as u8; usize::from(size)];

        match shape {
            0 => vec![(b"f".to_vec(), value)],
            1 => vec![
                (b"name".to_vec(), value.clone()),
                (b"other".to_vec(), value),
            ],
            2 => vec![(b"f".to_vec(), value.clone()), (b"g".to_vec(), value)],
            _ => vec![(vec![b'x'; usize::from(size) + 1], value)],
        }
    }

    fn check(blocks: &Blocks, model: &[(u64, u64, Fields)]) {
        let want: Vec<StreamEntry> = model
            .iter()
            .map(|(millis, sequence, fields)| entry_of(*millis, *sequence, fields))
            .collect();

        assert_eq!(blocks.len(), model.len());
        assert_eq!(blocks.to_vec(), want);
        assert_eq!(blocks.first(), want.first().cloned());
        assert_eq!(blocks.last(), want.last().cloned());

        if model.is_empty() {
            assert_eq!(blocks.bytes(), 0);
        }
    }

    fn run(ops: Vec<Op>) {
        let mut blocks = Blocks::new();
        let mut model: Vec<(u64, u64, Fields)> = Vec::new();
        let (mut millis, mut sequence) = (1u64, 0u64);

        for (tag, op) in ops.into_iter().enumerate() {
            match op {
                Op::Push {
                    millis_step,
                    shape,
                    size,
                } => {
                    if millis_step == 0 {
                        sequence += 1;
                    } else if let Some(next) = millis.checked_add(millis_step) {
                        millis = next;
                        sequence = 0;
                    } else {
                        sequence += 1;
                    }

                    let fields = fields_for(shape, size, tag);

                    blocks.push(millis, sequence, &fields);
                    model.push((millis, sequence, fields));
                }
                Op::Remove(pick) => {
                    let (id, present) = match model.get(pick % (model.len() + 1)) {
                        Some(entry) => ((entry.0, entry.1), true),
                        None => ((millis, sequence + 1), false),
                    };

                    assert_eq!(blocks.remove(id.0, id.1), present);
                    model.retain(|entry| (entry.0, entry.1) != id);
                }
                Op::DropFront(count) => {
                    blocks.drop_front(count);
                    model.drain(..count.min(model.len()));
                }
                Op::DropBefore(pick) => {
                    let id = match model.get(pick % (model.len() + 1)) {
                        Some(entry) => (entry.0, entry.1),
                        None => (millis, sequence + 1),
                    };
                    let before = model.len();

                    model.retain(|entry| (entry.0, entry.1) >= id);
                    assert_eq!(blocks.drop_before(id.0, id.1), before - model.len());
                }
                Op::Probe(a, b, limit) => {
                    let pick = |index: usize| match model.get(index % (model.len() + 1)) {
                        Some(entry) => (entry.0, entry.1),
                        None => (millis, sequence + 1),
                    };
                    let (low, high) = (pick(a), pick(b));

                    for id in [low, high, (0, 0), (millis, u64::MAX)] {
                        let want = model.iter().find(|entry| (entry.0, entry.1) == id);

                        assert_eq!(
                            blocks.get(id.0, id.1),
                            want.map(|entry| entry_of(entry.0, entry.1, &entry.2))
                        );
                        assert_eq!(blocks.contains(id.0, id.1), want.is_some());
                        assert_eq!(
                            blocks.count_after(id.0, id.1),
                            model.iter().filter(|entry| (entry.0, entry.1) > id).count()
                        );

                        let after: Vec<StreamEntry> = model
                            .iter()
                            .filter(|entry| (entry.0, entry.1) > id)
                            .take(limit + 1)
                            .map(|entry| entry_of(entry.0, entry.1, &entry.2))
                            .collect();

                        assert_eq!(blocks.after(id.0, id.1, limit + 1), after);
                    }

                    for (start, end) in [
                        (Some(low), Some(high)),
                        (None, Some(high)),
                        (Some(low), None),
                        (None, None),
                    ] {
                        for reverse in [false, true] {
                            let mut want: Vec<StreamEntry> = model
                                .iter()
                                .filter(|entry| {
                                    let id = (entry.0, entry.1);

                                    start.is_none_or(|start| id >= start)
                                        && end.is_none_or(|end| id <= end)
                                })
                                .map(|entry| entry_of(entry.0, entry.1, &entry.2))
                                .collect();

                            if reverse {
                                want.reverse();
                            }

                            want.truncate(limit);
                            assert_eq!(blocks.range(start, end, limit, reverse), want);
                        }
                    }
                }
            }

            check(&blocks, &model);
        }
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(
            std::env::var("PROPTEST_CASES").ok().and_then(|v| v.parse().ok()).unwrap_or(256)
        ))]

        #[test]
        fn blocks_match_a_linear_model(ops in proptest::collection::vec(op(), 1..500)) {
            run(ops);
        }
    }

    #[test]
    fn small_entries_take_a_few_bytes_each() {
        let mut blocks = Blocks::new();

        for sequence in 0..10_000u64 {
            blocks.push(
                1_700_000_000_000 + sequence / 100,
                sequence % 100,
                &[(b"f", b"v")],
            );
        }

        let per_entry = blocks.bytes() / 10_000;

        assert!(per_entry <= 12, "{per_entry} bytes per entry");
    }

    #[test]
    fn repeated_field_names_are_kept_once_per_block() {
        let mut blocks = Blocks::new();
        let fields = [
            (&b"temperature"[..], &b"21.5"[..]),
            (&b"humidity"[..], &b"40"[..]),
        ];

        for sequence in 0..1_000u64 {
            blocks.push(1, sequence, &fields);
        }

        let per_entry = blocks.bytes() / 1_000;

        assert!(per_entry <= 14, "{per_entry} bytes per entry");
    }

    #[test]
    fn trimming_one_entry_at_a_time_keeps_memory_bounded() {
        let mut blocks = Blocks::new();

        for sequence in 0..20_000u64 {
            blocks.push(1, sequence, &[(b"f", b"0123456789abcdef")]);
            blocks.drop_front(blocks.len().saturating_sub(100));
        }

        assert_eq!(blocks.len(), 100);
        assert!(blocks.bytes() < 100 * 40 + 4 * BLOCK_COST + 8192);
    }
}
