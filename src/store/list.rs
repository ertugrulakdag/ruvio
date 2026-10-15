use super::smallbuf::{PackBuf, SmallBuf};
use super::*;

// Lists up to `CollectionLimits::list_max_entries` items, each at most `list_max_item_bytes`,
// stay in one packed buffer (defaults 128 and 64).

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ListValue {
    pub(super) body: ListBody,
    pub(super) payload_bytes: usize,
}

/// A quicklist node holds at most this many bytes of packed items. One item larger than
/// this gets a node to itself.
pub(super) const LIST_NODE_MAX_BYTES: usize = 8 * 1024;
const LIST_FRONT_NODE_BYTES: usize = 1024;

/// One quicklist node: items in the same `len item backlen` format as a packed list, plus
/// the item count so `LINDEX` skips whole nodes without decoding them.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(crate) struct PackedNode {
    pub(super) bytes: Vec<u8>,
    pub(super) count: u32,
    /// Bytes at the front already popped; compacted once they outweigh the live half.
    head: u32,
}

/// `Packed` keeps small lists in one buffer. Each item is `len item backlen`: `len` as in a
/// compact hash, `backlen` its mirror (one byte, or a `u32` then `255`) so the tail can be
/// read without walking the list. `Quick` chains packed nodes of at most
/// `LIST_NODE_MAX_BYTES`; `len` is the total item count and `node_capacity` the summed
/// buffer capacity, both kept current so neither is a walk.
#[derive(Debug)]
pub(super) enum ListBody {
    Packed {
        bytes: SmallBuf,
        count: u32,
    },
    // The queues are boxed on purpose: it keeps every `ListValue` at 48 bytes.
    #[allow(clippy::box_collection)]
    Quick {
        nodes: Box<VecDeque<PackedNode>>,
        /// The item count of each node, densely packed so `LINDEX` sums them quickly.
        counts: Box<VecDeque<u32>>,
        len: usize,
        node_capacity: usize,
    },
}

impl Default for ListBody {
    fn default() -> Self {
        Self::Packed {
            bytes: SmallBuf::new(),
            count: 0,
        }
    }
}

pub(super) fn list_item_size(len: usize) -> usize {
    packed_len_size(len) * 2 + len
}

pub(super) fn encode_list_item(item: &[u8]) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(list_item_size(item.len()));

    put_list_item(&mut bytes, item);
    bytes
}

pub(super) fn put_list_item(bytes: &mut impl PackBuf, item: &[u8]) {
    put_packed(bytes, item);

    if item.len() < PACKED_LONG_LEN as usize {
        bytes.push(item.len() as u8);
    } else {
        bytes.extend_from_slice(&(item.len() as u32).to_le_bytes());
        bytes.push(PACKED_LONG_LEN);
    }
}

/// Start of the item that ends at `end`.
pub(super) fn list_item_start(bytes: &[u8], end: usize) -> usize {
    let last = bytes[end - 1];

    if last == PACKED_LONG_LEN {
        let mut raw = [0u8; 4];

        raw.copy_from_slice(&bytes[end - 5..end - 1]);
        end - list_item_size(u32::from_le_bytes(raw) as usize)
    } else {
        end - list_item_size(last as usize)
    }
}

/// `bytes.splice(from..to, replacement)` as one tail move and one copy.
fn replace_range(bytes: &mut impl PackBuf, from: usize, to: usize, replacement: &[u8]) {
    let length = bytes.len();
    let end = from + replacement.len();

    if end > to {
        bytes.resize(length + (end - to), 0);
        bytes.copy_within(to..length, end);
    } else if end < to {
        bytes.copy_within(to..length, end);
        bytes.truncate(length - (to - end));
    }

    bytes[from..end].copy_from_slice(replacement);
}

/// Byte offset of item `index`, or the buffer length when `index` is the count.
pub(super) fn list_item_offset(bytes: &[u8], index: usize) -> usize {
    let mut at = 0;

    for _ in 0..index {
        at = next_list_item(bytes, at).1;
    }

    at
}

/// Like `list_item_offset` for a buffer of `count` items, but walks from whichever end is
/// nearer: the trailing `backlen` of each item lets the tail be read backwards.
pub(super) fn list_item_offset_near(bytes: &[u8], index: usize, count: usize) -> usize {
    if index * 2 <= count {
        return list_item_offset(bytes, index);
    }

    let mut end = bytes.len();

    for _ in 0..count - index {
        end = list_item_start(bytes, end);
    }

    end
}

pub(super) fn next_list_item(bytes: &[u8], at: usize) -> (&[u8], usize) {
    let (item, end) = read_packed(bytes, at);

    (item, end + packed_len_size(item.len()))
}

pub(crate) enum ListIter<'a> {
    Packed {
        bytes: &'a [u8],
        at: usize,
    },
    Quick {
        nodes: std::collections::vec_deque::Iter<'a, PackedNode>,
        bytes: &'a [u8],
        at: usize,
    },
}

impl<'a> Iterator for ListIter<'a> {
    type Item = &'a [u8];

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Packed { bytes, at } => {
                if *at >= bytes.len() {
                    return None;
                }

                let (item, next) = next_list_item(bytes, *at);

                *at = next;
                Some(item)
            }
            Self::Quick { nodes, bytes, at } => loop {
                if *at < bytes.len() {
                    let (item, next) = next_list_item(bytes, *at);

                    *at = next;
                    return Some(item);
                }

                let node = nodes.next()?;

                *bytes = node.live();
                *at = 0;
            },
        }
    }
}

impl PackedNode {
    fn with_item(value: &[u8]) -> Self {
        let mut bytes = Vec::with_capacity(list_item_size(value.len()));

        put_list_item(&mut bytes, value);
        Self {
            bytes,
            count: 1,
            head: 0,
        }
    }

    /// Releases the slack of a node that will no longer grow.
    fn seal(&mut self) {
        self.compact();

        if self.bytes.capacity() > self.bytes.len() {
            self.bytes.shrink_to_fit();
        }
    }

    fn fits(&self, size: usize) -> bool {
        self.bytes.len() - self.head as usize + size <= LIST_NODE_MAX_BYTES
    }

    /// Front pushes shift the node, so they stop growing a node at this size and start the
    /// next one: the shift stays short and the sealed nodes are still dense.
    fn fits_front(&self, size: usize) -> bool {
        self.bytes.len() - self.head as usize + size <= LIST_FRONT_NODE_BYTES
    }

    fn live(&self) -> &[u8] {
        &self.bytes[self.head as usize..]
    }

    fn compact(&mut self) {
        if self.head > 0 {
            self.bytes.drain(..self.head as usize);
            self.head = 0;
        }
    }
}

/// Inserts `item` before everything in `bytes`.
fn insert_list_item_front(bytes: &mut impl PackBuf, item: &[u8]) {
    let size = list_item_size(item.len());
    let old = bytes.len();
    let head = packed_len_size(item.len());
    let tail = head + item.len();

    bytes.resize(old + size, 0);
    bytes.copy_within(0..old, size);

    if item.len() < PACKED_LONG_LEN as usize {
        bytes[0] = item.len() as u8;
        bytes[tail] = item.len() as u8;
    } else {
        bytes[0] = PACKED_LONG_LEN;
        bytes[1..5].copy_from_slice(&(item.len() as u32).to_le_bytes());
        bytes[tail..tail + 4].copy_from_slice(&(item.len() as u32).to_le_bytes());
        bytes[tail + 4] = PACKED_LONG_LEN;
    }

    bytes[head..tail].copy_from_slice(item);
}

/// Gives back the capacity of a node that has emptied out.
fn reclaim_node(node: &mut PackedNode, node_capacity: &mut usize) {
    if let Some(target) = reclaim_capacity(node.bytes.len(), node.bytes.capacity()) {
        let before = node.bytes.capacity();

        node.bytes.shrink_to(target);
        *node_capacity -= before - node.bytes.capacity();
    }
}

/// Like `reserve_packed`, but never plans a node past its byte limit.
fn reserve_node(bytes: &mut Vec<u8>, extra: usize) {
    if bytes.capacity() - bytes.len() < extra {
        let room = LIST_NODE_MAX_BYTES.saturating_sub(bytes.len());

        bytes.reserve_exact(extra.max((bytes.len() / 4).min(room)));
    }
}

fn locate_in_nodes(counts: &VecDeque<u32>, index: usize, len: usize) -> (usize, usize) {
    let (front, back) = counts.as_slices();

    if index < len / 2 {
        let mut rest = index;
        let mut at = 0;

        for slice in [front, back] {
            for &count in slice {
                if rest < count as usize {
                    return (at, rest);
                }

                rest -= count as usize;
                at += 1;
            }
        }
    } else {
        let mut rest = len - 1 - index;
        let mut at = counts.len();

        for slice in [back, front] {
            for &count in slice.iter().rev() {
                at -= 1;

                if rest < count as usize {
                    return (at, count as usize - 1 - rest);
                }

                rest -= count as usize;
            }
        }
    }

    unreachable!("list index within length")
}

impl Clone for ListBody {
    fn clone(&self) -> Self {
        match self {
            Self::Packed { bytes, count } => Self::Packed {
                bytes: bytes.clone(),
                count: *count,
            },
            Self::Quick {
                nodes, counts, len, ..
            } => {
                let nodes: Box<VecDeque<PackedNode>> = nodes.clone();
                let node_capacity = nodes.iter().map(|node| node.bytes.capacity()).sum();

                Self::Quick {
                    nodes,
                    counts: counts.clone(),
                    len: *len,
                    node_capacity,
                }
            }
        }
    }
}

impl ListBody {
    fn iter(&self) -> ListIter<'_> {
        match self {
            Self::Packed { bytes, .. } => ListIter::Packed { bytes, at: 0 },
            Self::Quick { nodes, .. } => ListIter::Quick {
                nodes: nodes.iter(),
                bytes: &[],
                at: 0,
            },
        }
    }
}

/// Equal when the items are equal, however they are split into nodes.
impl PartialEq for ListBody {
    fn eq(&self, other: &Self) -> bool {
        self.iter().eq(other.iter())
    }
}

impl Eq for ListBody {}

impl ListValue {
    pub(super) fn fits_packed(&self, value: &[u8], limits: CollectionLimits) -> bool {
        value.len() <= limits.list_max_item_bytes && self.len() < limits.list_max_entries
    }

    pub(super) fn push_front(&mut self, value: &[u8], limits: CollectionLimits) {
        self.payload_bytes = self.payload_bytes.saturating_add(value.len());

        if !self.fits_packed(value, limits) {
            self.promote();
        }

        match &mut self.body {
            ListBody::Packed { bytes, count } => {
                reserve_packed(bytes, list_item_size(value.len()));
                insert_list_item_front(bytes, value);
                *count += 1;
            }
            ListBody::Quick {
                nodes,
                counts,
                len,
                node_capacity,
            } => {
                let size = list_item_size(value.len());

                match nodes.front_mut() {
                    Some(node) if node.fits_front(size) => {
                        node.compact();
                        let before = node.bytes.capacity();

                        reserve_node(&mut node.bytes, size);
                        insert_list_item_front(&mut node.bytes, value);
                        node.count += 1;
                        counts[0] += 1;
                        *node_capacity = *node_capacity - before + node.bytes.capacity();
                    }
                    _ => {
                        if let Some(node) = nodes.front_mut() {
                            let before = node.bytes.capacity();

                            node.seal();
                            *node_capacity -= before - node.bytes.capacity();
                        }

                        let node = PackedNode::with_item(value);

                        *node_capacity += node.bytes.capacity();
                        nodes.push_front(node);
                        counts.push_front(1);
                    }
                }

                *len += 1;
            }
        }
    }

    pub(super) fn push_back(&mut self, value: &[u8], limits: CollectionLimits) {
        self.payload_bytes = self.payload_bytes.saturating_add(value.len());

        if !self.fits_packed(value, limits) {
            self.promote();
        }

        match &mut self.body {
            ListBody::Packed { bytes, count } => {
                reserve_packed(bytes, list_item_size(value.len()));
                put_list_item(bytes, value);
                *count += 1;
            }
            ListBody::Quick {
                nodes,
                counts,
                len,
                node_capacity,
            } => {
                let size = list_item_size(value.len());

                match nodes.back_mut() {
                    Some(node) if node.fits(size) => {
                        if node.head > 0 && node.bytes.capacity() - node.bytes.len() < size {
                            node.compact();
                        }

                        let before = node.bytes.capacity();

                        reserve_node(&mut node.bytes, size);
                        put_list_item(&mut node.bytes, value);
                        node.count += 1;
                        *counts.back_mut().expect("a node has a count") += 1;
                        *node_capacity = *node_capacity - before + node.bytes.capacity();
                    }
                    _ => {
                        if let Some(node) = nodes.back_mut() {
                            let before = node.bytes.capacity();

                            node.seal();
                            *node_capacity -= before - node.bytes.capacity();
                        }

                        let node = PackedNode::with_item(value);

                        *node_capacity += node.bytes.capacity();
                        nodes.push_back(node);
                        counts.push_back(1);
                    }
                }

                *len += 1;
            }
        }
    }

    pub(super) fn pop_front(&mut self, limits: CollectionLimits) -> Option<Box<[u8]>> {
        let value: Box<[u8]> = match &mut self.body {
            ListBody::Packed { bytes, count } => {
                if *count == 0 {
                    return None;
                }

                let (item, next) = next_list_item(bytes, 0);
                let item = Box::from(item);

                bytes.drain_range(0..next);
                *count -= 1;
                item
            }
            ListBody::Quick {
                nodes,
                counts,
                len,
                node_capacity,
            } => {
                let node = nodes.front_mut()?;
                let (item, next) = next_list_item(node.live(), 0);
                let item: Box<[u8]> = Box::from(item);

                node.count -= 1;
                *len -= 1;

                if node.count == 0 {
                    *node_capacity -= node.bytes.capacity();
                    nodes.pop_front();
                    counts.pop_front();
                } else {
                    counts[0] -= 1;
                    node.head += next as u32;

                    if node.head as usize * 2 > node.bytes.len() {
                        node.compact();
                    }

                    reclaim_node(node, node_capacity);
                }

                item
            }
        };

        self.payload_bytes = self.payload_bytes.saturating_sub(value.len());
        self.shrink_if_sparse(limits);
        Some(value)
    }

    pub(super) fn pop_back(&mut self, limits: CollectionLimits) -> Option<Box<[u8]>> {
        let value: Box<[u8]> = match &mut self.body {
            ListBody::Packed { bytes, count } => {
                if *count == 0 {
                    return None;
                }

                let start = list_item_start(bytes, bytes.len());
                let (item, _) = read_packed(bytes, start);
                let item = Box::from(item);

                bytes.truncate(start);
                *count -= 1;
                item
            }
            ListBody::Quick {
                nodes,
                counts,
                len,
                node_capacity,
            } => {
                let node = nodes.back_mut()?;
                let start = list_item_start(&node.bytes, node.bytes.len());
                let (item, _) = read_packed(&node.bytes, start);
                let item: Box<[u8]> = Box::from(item);

                node.count -= 1;
                *len -= 1;

                if node.count == 0 {
                    *node_capacity -= node.bytes.capacity();
                    nodes.pop_back();
                    counts.pop_back();
                } else {
                    *counts.back_mut().expect("a node has a count") -= 1;
                    node.bytes.truncate(start);
                    reclaim_node(node, node_capacity);
                }

                item
            }
        };

        self.payload_bytes = self.payload_bytes.saturating_sub(value.len());
        self.shrink_if_sparse(limits);
        Some(value)
    }

    pub(super) fn len(&self) -> usize {
        match &self.body {
            ListBody::Packed { count, .. } => *count as usize,
            ListBody::Quick { len, .. } => *len,
        }
    }

    pub(super) fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Bytes beyond the payload. A packed list stores the payload inside its buffer.
    pub(super) fn capacity_bytes(&self) -> usize {
        match &self.body {
            ListBody::Packed { bytes, .. } => {
                bytes.heap_capacity().saturating_sub(self.payload_bytes)
            }
            ListBody::Quick {
                nodes,
                node_capacity,
                ..
            } => node_capacity
                .saturating_sub(self.payload_bytes)
                .saturating_add(nodes.capacity().saturating_mul(size_of::<PackedNode>())),
        }
    }

    pub(super) fn get(&self, index: usize) -> Option<&[u8]> {
        match &self.body {
            ListBody::Packed { bytes, count } => {
                if index >= *count as usize {
                    return None;
                }

                Some(next_list_item(bytes, list_item_offset(bytes, index)).0)
            }
            ListBody::Quick {
                nodes, counts, len, ..
            } => {
                if index >= *len {
                    return None;
                }

                let (at, within) = locate_in_nodes(counts, index, *len);
                let node = &nodes[at];
                let bytes = node.live();
                let count = node.count as usize;

                Some(next_list_item(bytes, list_item_offset_near(bytes, within, count)).0)
            }
        }
    }

    pub(super) fn iter(&self) -> ListIter<'_> {
        self.body.iter()
    }

    /// Iterates from item `index` on, skipping whole nodes instead of decoding them.
    pub(super) fn iter_from(&self, index: usize) -> ListIter<'_> {
        match &self.body {
            ListBody::Packed { bytes, count } => ListIter::Packed {
                bytes,
                at: if index >= *count as usize {
                    bytes.len()
                } else {
                    list_item_offset(bytes, index)
                },
            },
            ListBody::Quick {
                nodes, counts, len, ..
            } => {
                if index >= *len {
                    return ListIter::Quick {
                        nodes: nodes.range(nodes.len()..),
                        bytes: &[],
                        at: 0,
                    };
                }

                let (at, within) = locate_in_nodes(counts, index, *len);
                let node = &nodes[at];
                let bytes = node.live();

                ListIter::Quick {
                    nodes: nodes.range(at + 1..),
                    bytes,
                    at: list_item_offset_near(bytes, within, node.count as usize),
                }
            }
        }
    }

    pub(super) fn retain_range(&mut self, start: usize, stop: usize, limits: CollectionLimits) {
        // Walk whichever side is shorter: the kept range, or the dropped head and tail.
        let length = self.len();
        let kept_count = stop + 1 - start;
        let kept: usize = if kept_count <= length - kept_count {
            self.iter_from(start)
                .take(kept_count)
                .map(<[u8]>::len)
                .sum()
        } else {
            let head: usize = self.iter().take(start).map(<[u8]>::len).sum();
            let tail: usize = self.iter_from(stop + 1).map(<[u8]>::len).sum();

            self.payload_bytes - head - tail
        };

        match &mut self.body {
            ListBody::Packed { bytes, count } => {
                let tail = list_item_offset(bytes, stop + 1);

                bytes.truncate(tail);
                let head = list_item_offset(bytes, start);

                bytes.drain_range(0..head);
                *count = (stop + 1 - start) as u32;
            }
            ListBody::Quick {
                nodes,
                counts,
                len,
                node_capacity,
            } => {
                let mut drop_back = *len - 1 - stop;

                while drop_back > 0 {
                    let node = nodes.back_mut().expect("items remain to drop");

                    if node.count as usize <= drop_back {
                        drop_back -= node.count as usize;
                        *node_capacity -= node.bytes.capacity();
                        nodes.pop_back();
                        counts.pop_back();
                    } else {
                        let keep = node.count as usize - drop_back;
                        let before = node.bytes.capacity();

                        node.compact();
                        node.bytes.truncate(list_item_offset(&node.bytes, keep));
                        node.count = keep as u32;
                        *counts.back_mut().expect("a node has a count") = keep as u32;
                        node.seal();
                        *node_capacity -= before - node.bytes.capacity();
                        drop_back = 0;
                    }
                }

                let mut drop_front = start;

                while drop_front > 0 {
                    let node = nodes.front_mut().expect("items remain to drop");

                    if node.count as usize <= drop_front {
                        drop_front -= node.count as usize;
                        *node_capacity -= node.bytes.capacity();
                        nodes.pop_front();
                        counts.pop_front();
                    } else {
                        let before = node.bytes.capacity();
                        let head = list_item_offset(node.live(), drop_front);

                        node.compact();
                        node.bytes.drain(..head);
                        node.count -= drop_front as u32;
                        counts[0] -= drop_front as u32;
                        node.seal();
                        *node_capacity -= before - node.bytes.capacity();
                        drop_front = 0;
                    }
                }

                *len = stop + 1 - start;
            }
        }

        self.payload_bytes = kept;
        self.shrink_if_sparse(limits);
    }

    pub(super) fn promote(&mut self) {
        let ListBody::Packed { .. } = &self.body else {
            return;
        };

        let mut nodes: VecDeque<PackedNode> = VecDeque::new();
        let mut counts: VecDeque<u32> = VecDeque::new();
        let mut node_capacity = 0;
        let mut len = 0;
        let mut node = PackedNode::default();

        for item in self.iter() {
            if node.count > 0 && !node.fits(list_item_size(item.len())) {
                node.seal();
                node_capacity += node.bytes.capacity();
                counts.push_back(node.count);
                nodes.push_back(std::mem::take(&mut node));
            }

            put_list_item(&mut node.bytes, item);
            node.count += 1;
            len += 1;
        }

        if node.count > 0 {
            node_capacity += node.bytes.capacity();
            counts.push_back(node.count);
            nodes.push_back(node);
        }

        self.body = ListBody::Quick {
            nodes: Box::new(nodes),
            counts: Box::new(counts),
            len,
            node_capacity,
        };
    }

    pub(super) fn shrink_if_sparse(&mut self, limits: CollectionLimits) {
        match &mut self.body {
            ListBody::Packed { bytes, .. } => {
                if let Some(target) = reclaim_capacity(bytes.len(), bytes.capacity()) {
                    bytes.shrink_to(target);
                }
            }
            ListBody::Quick { len, .. } => {
                // Half the packed limit, so a list at the boundary does not flip each push.
                if *len <= limits.list_max_entries / 2
                    && self
                        .body
                        .iter()
                        .all(|item| item.len() <= limits.list_max_item_bytes)
                {
                    let mut bytes = SmallBuf::with_capacity(self.payload_bytes + self.len() * 2);
                    let mut count = 0;

                    for item in self.body.iter() {
                        put_list_item(&mut bytes, item);
                        count += 1;
                    }

                    self.body = ListBody::Packed { bytes, count };

                    return;
                }

                if let ListBody::Quick { nodes, .. } = &mut self.body
                    && let Some(target) = reclaim_capacity(nodes.len(), nodes.capacity())
                {
                    nodes.shrink_to(target);
                }
            }
        }
    }

    /// Replaces the bytes of items `[from, to)` of one buffer (the packed list, or node
    /// `node`) with `replacement` (already encoded items), adjusting `count` by `delta_items`.
    /// Splits a node that grows past twice the node limit so later edits stay short.
    fn splice_items(
        &mut self,
        node: usize,
        from: usize,
        to: usize,
        replacement: &[u8],
        delta_items: i64,
    ) {
        match &mut self.body {
            ListBody::Packed { bytes, count } => {
                replace_range(bytes, from, to, replacement);
                *count = (i64::from(*count) + delta_items) as u32;
            }
            ListBody::Quick {
                nodes,
                counts,
                len,
                node_capacity,
            } => {
                let target = &mut nodes[node];
                let before = target.bytes.capacity();
                let head = target.head as usize;

                replace_range(&mut target.bytes, head + from, head + to, replacement);
                target.count = (i64::from(target.count) + delta_items) as u32;
                counts[node] = target.count;
                *len = (*len as i64 + delta_items) as usize;
                *node_capacity = *node_capacity - before + target.bytes.capacity();

                if target.count == 0 {
                    *node_capacity -= target.bytes.capacity();
                    nodes.remove(node);
                    counts.remove(node);
                } else if target.bytes.len() - head > LIST_NODE_MAX_BYTES * 2 && target.count > 1 {
                    target.compact();
                    let half = target.count as usize / 2;
                    let at = list_item_offset(&target.bytes, half);
                    let tail = target.bytes.split_off(at);
                    let moved = target.count - half as u32;
                    let before = target.bytes.capacity();

                    target.count = half as u32;
                    target.bytes.shrink_to_fit();
                    counts[node] = target.count;
                    *node_capacity = *node_capacity - before + target.bytes.capacity();

                    let split = PackedNode {
                        bytes: tail,
                        count: moved,
                        head: 0,
                    };

                    *node_capacity += split.bytes.capacity();
                    nodes.insert(node + 1, split);
                    counts.insert(node + 1, moved);
                }
            }
        }
    }

    /// (buffer, byte offset within its live bytes) of item `index`; `index == len` is the
    /// end of the last buffer.
    fn locate(&self, index: usize) -> (usize, usize) {
        match &self.body {
            ListBody::Packed { bytes, count } => {
                (0, list_item_offset_near(bytes, index, *count as usize))
            }
            ListBody::Quick {
                nodes, counts, len, ..
            } => {
                if index >= *len {
                    let last = nodes.len() - 1;

                    return (last, nodes[last].live().len());
                }

                let (at, within) = locate_in_nodes(counts, index, *len);
                let node = &nodes[at];

                (
                    at,
                    list_item_offset_near(node.live(), within, node.count as usize),
                )
            }
        }
    }

    fn buffer(&self, node: usize) -> &[u8] {
        match &self.body {
            ListBody::Packed { bytes, .. } => bytes,
            ListBody::Quick { nodes, .. } => nodes[node].live(),
        }
    }

    /// `LSET` in place: only the buffer holding item `index` is rewritten.
    pub(super) fn set_at(&mut self, index: usize, value: &[u8], limits: CollectionLimits) {
        if value.len() > limits.list_max_item_bytes {
            self.promote();
        }

        let (node, at) = self.locate(index);
        let (old, end) = next_list_item(self.buffer(node), at);
        let old_len = old.len();

        self.payload_bytes = self.payload_bytes - old_len + value.len();
        self.splice_items(node, at, end, &encode_list_item(value), 0);
    }

    /// `LINSERT` in place: `value` becomes item `index` (`index == len` appends).
    pub(super) fn insert_at(&mut self, index: usize, value: &[u8], limits: CollectionLimits) {
        if !self.fits_packed(value, limits) {
            self.promote();
        }

        let (node, at) = self.locate(index);

        self.payload_bytes += value.len();
        self.splice_items(node, at, at, &encode_list_item(value), 1);
    }

    /// Removes the item at `index`, returning its length.
    fn remove_at(&mut self, index: usize) {
        let (node, at) = self.locate(index);
        let (old, end) = next_list_item(self.buffer(node), at);

        self.payload_bytes -= old.len();
        self.splice_items(node, at, end, &[], -1);
    }

    /// `LREM`: removes up to `count` items equal to `value` (from the tail when negative,
    /// all when 0). Returns how many went.
    pub(super) fn remove_matching(
        &mut self,
        count: i64,
        value: &[u8],
        limits: CollectionLimits,
    ) -> usize {
        let budget = if count == 0 {
            usize::MAX
        } else {
            count.unsigned_abs() as usize
        };
        let mut hits: Vec<usize> = Vec::new();

        if count < 0 {
            let matches: Vec<usize> = self
                .iter()
                .enumerate()
                .filter_map(|(index, item)| (item == value).then_some(index))
                .collect();

            hits.extend(matches.into_iter().rev().take(budget));
        } else {
            hits.extend(
                self.iter()
                    .enumerate()
                    .filter_map(|(index, item)| (item == value).then_some(index))
                    .take(budget),
            );
            hits.reverse();
        }

        // Highest index first, so earlier indexes stay valid.
        for &index in &hits {
            self.remove_at(index);
        }

        if !hits.is_empty() {
            self.shrink_if_sparse(limits);
        }

        hits.len()
    }

    /// The slab record of a small list: one packed buffer that fits a slot.
    pub(super) fn slab_record(&self) -> Option<crate::index::SlabBuf> {
        let ListBody::Packed { bytes, count } = &self.body else {
            return None;
        };

        (!bytes.is_empty() && bytes.len() <= crate::index::COLL_DATA)
            .then(|| crate::index::SlabBuf::coll(crate::index::COLL_LIST, *count as usize, bytes))
    }

    /// Slab record length (header included) when this list fits one.
    pub(super) fn slab_len(&self) -> Option<usize> {
        match &self.body {
            ListBody::Packed { bytes, .. }
                if !bytes.is_empty() && bytes.len() <= crate::index::COLL_DATA =>
            {
                Some(3 + bytes.len())
            }
            _ => None,
        }
    }

    pub(super) fn from_slab(count: usize, data: &[u8]) -> Self {
        let mut payload_bytes = 0;
        let mut at = 0;

        while at < data.len() {
            let (item, next) = next_list_item(data, at);

            payload_bytes += item.len();
            at = next;
        }

        Self {
            body: ListBody::Packed {
                bytes: SmallBuf::from_slice(data),
                count: count as u32,
            },
            payload_bytes,
        }
    }

    pub(super) fn from_values(
        values: impl IntoIterator<Item = Vec<u8>>,
        limits: CollectionLimits,
    ) -> Self {
        let mut list = Self::default();

        for value in values {
            list.push_back(&value, limits);
        }

        list
    }
}

impl<C: Clock, I: StorageIndex> KvShard<C, I> {
    /// Pushes with a single index probe; the charge is read from the entry itself.
    pub(super) fn list_push<T: AsRef<[u8]>>(
        &mut self,
        key: &[u8],
        values: &[T],
        front: bool,
    ) -> Result<usize, WrongType> {
        self.remove_expired(key);
        let limits = self.collection_limits;
        let (done, count) = self.list_push_in_slab(key, values, front, limits);
        let values = &values[done..];

        if values.is_empty() {
            return Ok(count);
        }

        let mut created = false;
        let Some(entry) = self.entries.get_or_insert_with(key, || {
            created = true;
            Entry::fresh(Value::List(Box::default()))
        }) else {
            return Err(WrongType);
        };

        let before = if created {
            EntryCharge::default()
        } else {
            EntryCharge::for_entry(key, entry)
        };

        let Value::List(list) = &mut entry.value else {
            return Err(WrongType);
        };

        for value in values {
            if front {
                list.push_front(value.as_ref(), limits);
            } else {
                list.push_back(value.as_ref(), limits);
            }
        }

        let length = list.len();
        let after = EntryCharge::for_entry(key, entry);

        self.apply_entry_charge(before, after, key);
        self.entries.demote_collection(key);
        Ok(length)
    }

    /// Pushes leading values inside the key's slab record; returns how many fit.
    fn list_push_in_slab<T: AsRef<[u8]>>(
        &mut self,
        key: &[u8],
        values: &[T],
        front: bool,
        limits: CollectionLimits,
    ) -> (usize, usize) {
        let mut done = 0;
        let mut count = 0;
        let mut first_len = 0usize;
        let mut last_len = 0usize;

        for value in values {
            let mut used_before = 0usize;
            let mut used_after = 0usize;
            let mut items = 0usize;
            let mut touch = SlabTouch::Absent;

            // A record that outgrows its slot moves up a size class and is retried once.
            for attempt in 0..2 {
                touch = self
                    .entries
                    .touch_slab(key, crate::index::TAG_COLL_SLAB, |slot| {
                        used_before = usize::from(slot[2]);

                        let result = super::slab_ops::list_push(
                            slot,
                            value.as_ref(),
                            front,
                            limits.list_max_entries,
                            limits.list_max_item_bytes,
                        )
                        .map(|()| true);

                        used_after = usize::from(slot[2]);
                        items = usize::from(slot[1]);
                        result
                    });

                let needed = 3 + used_before + list_item_size(value.as_ref().len());

                if attempt == 0
                    && matches!(touch, SlabTouch::Done(None))
                    && self.entries.enlarge_slab(key, needed)
                {
                    continue;
                }

                break;
            }

            if !matches!(touch, SlabTouch::Done(Some(_))) {
                break;
            }

            if done == 0 {
                first_len = 3 + used_before;
            }

            last_len = 3 + used_after;
            count = items;
            done += 1;
        }

        if done > 0 {
            let charge = |len: usize| EntryCharge::slab(key, len);

            self.apply_entry_charge(charge(first_len), charge(last_len), key);
        }

        (done, count)
    }

    /// Pops one item inside the key's slab record, when the list stays non-empty.
    fn list_pop_in_slab(&mut self, key: &[u8], front: bool) -> Option<Box<[u8]>> {
        let mut popped = None;
        let mut used_before = 0usize;
        let mut used_after = 0usize;
        let touch = self
            .entries
            .touch_slab(key, crate::index::TAG_COLL_SLAB, |slot| {
                used_before = usize::from(slot[2]);
                popped = super::slab_ops::list_pop(slot, front);
                used_after = usize::from(slot[2]);
                popped.as_ref().map(|_| true)
            });

        if !matches!(touch, SlabTouch::Done(Some(_))) {
            return None;
        }

        let charge = |len: usize| EntryCharge::slab(key, len);

        self.apply_entry_charge(charge(3 + used_before), charge(3 + used_after), key);
        popped
    }

    pub fn list_push_front<T: AsRef<[u8]>>(
        &mut self,
        key: &[u8],
        values: &[T],
    ) -> Result<usize, WrongType> {
        self.list_push(key, values, true)
    }

    pub fn list_pop_front(&mut self, key: &[u8]) -> Result<Option<Box<[u8]>>, WrongType> {
        if self.remove_expired(key) {
            return Ok(None);
        }

        if let Some(item) = self.list_pop_in_slab(key, true) {
            return Ok(Some(item));
        }

        let before = self.entry_charge(key);
        let limits = self.collection_limits;
        let Some(entry) = self.entries.object_mut(key) else {
            return Ok(None);
        };

        let Value::List(list) = &mut entry.value else {
            return Err(WrongType);
        };

        let value = list.pop_front(limits);
        let remove_entry = list.is_empty();

        if remove_entry {
            self.remove_emptied(key);
        }

        self.reconcile_entry_charge(before, key);
        self.entries.demote_collection(key);
        Ok(value)
    }

    pub fn list_push_back<T: AsRef<[u8]>>(
        &mut self,
        key: &[u8],
        values: &[T],
    ) -> Result<usize, WrongType> {
        self.list_push(key, values, false)
    }

    pub fn list_pop_back(&mut self, key: &[u8]) -> Result<Option<Box<[u8]>>, WrongType> {
        if self.remove_expired(key) {
            return Ok(None);
        }

        if let Some(item) = self.list_pop_in_slab(key, false) {
            return Ok(Some(item));
        }

        let before = self.entry_charge(key);
        let limits = self.collection_limits;
        let Some(entry) = self.entries.object_mut(key) else {
            return Ok(None);
        };

        let Value::List(list) = &mut entry.value else {
            return Err(WrongType);
        };

        let value = list.pop_back(limits);
        let remove_entry = list.is_empty();

        if remove_entry {
            self.remove_emptied(key);
        }

        self.reconcile_entry_charge(before, key);
        self.entries.demote_collection(key);
        Ok(value)
    }

    /// The list at `key` for reading: an object, or a small list read in place from its slab.
    pub(crate) fn live_list(&self, key: &[u8]) -> Result<Option<ListRead<'_>>, WrongType> {
        self.live_view(
            key,
            |value| match value {
                Value::List(list) => Some(ListRead::Value(list)),
                _ => None,
            },
            |record| match record {
                IndexRef::CollSlab(slot) if slot[0] == crate::index::COLL_LIST => {
                    let (_, count, data) = crate::index::coll_parts(slot);

                    Some(ListRead::Packed { data, count })
                }
                _ => None,
            },
        )
    }

    pub fn list_len(&self, key: &[u8]) -> Result<usize, WrongType> {
        Ok(self.live_list(key)?.map_or(0, |list| list.len()))
    }

    pub fn list_index(&self, key: &[u8], index: isize) -> Result<Option<&[u8]>, WrongType> {
        let Some(list) = self.live_list(key)? else {
            return Ok(None);
        };

        Ok(normalize_index(list.len(), index).and_then(|index| list.get(index)))
    }

    pub fn list_trim(&mut self, key: &[u8], start: isize, stop: isize) -> Result<(), WrongType> {
        if self.remove_expired(key) {
            return Ok(());
        }

        let before = self.entry_charge(key);
        let limits = self.collection_limits;
        let Some(entry) = self.entries.object_mut(key) else {
            return Ok(());
        };

        let Value::List(list) = &mut entry.value else {
            return Err(WrongType);
        };

        match normalize_range(list.len(), start, stop) {
            None => {
                self.remove_emptied(key);
            }

            Some((start, stop)) => {
                list.retain_range(start, stop, limits);

                if list.is_empty() {
                    self.remove_emptied(key);
                }
            }
        }

        self.reconcile_entry_charge(before, key);
        self.entries.demote_collection(key);
        Ok(())
    }

    /// The items of the list under `key`, in order, without copying them.
    pub(crate) fn list_iter(&self, key: &[u8]) -> Result<Option<ListIter<'_>>, WrongType> {
        Ok(self.live_list(key)?.map(ListRead::iter))
    }

    /// `LSET`: `Ok(Err(message))` for a missing key or an index out of range.
    pub fn list_set(
        &mut self,
        key: &[u8],
        index: i64,
        value: &[u8],
    ) -> Result<Result<(), &'static [u8]>, WrongType> {
        let limits = self.collection_limits;
        let edited = self.edit_object(key, |entry| {
            let Value::List(list) = &mut entry.value else {
                return (Err(WrongType), false);
            };

            let length = list.len() as i64;
            let position = if index < 0 { length + index } else { index };

            if position < 0 || position >= length {
                return (Ok(Err(&b"ERR index out of range"[..])), false);
            }

            list.set_at(position as usize, value, limits);
            (Ok(Ok(())), true)
        });

        edited.unwrap_or(Ok(Err(b"ERR no such key")))
    }

    /// `LMOVE key key …`: moves the item at one end of the list to an end of the same list,
    /// with one key lookup and no change in length. `None` for a missing key.
    pub fn list_rotate(
        &mut self,
        key: &[u8],
        from_left: bool,
        to_left: bool,
    ) -> Result<Option<Box<[u8]>>, WrongType> {
        let limits = self.collection_limits;
        let edited = self.edit_object(key, |entry| {
            let Value::List(list) = &mut entry.value else {
                return (Err(WrongType), false);
            };

            let moved = if from_left {
                list.pop_front(limits)
            } else {
                list.pop_back(limits)
            };
            let Some(value) = moved else {
                return (Ok(None), false);
            };

            if to_left {
                list.push_front(&value, limits);
            } else {
                list.push_back(&value, limits);
            }

            (Ok(Some(value)), true)
        });

        edited.unwrap_or(Ok(None))
    }

    /// `LINSERT`: the new length, -1 when `pivot` is absent, 0 for a missing key.
    pub fn list_insert(
        &mut self,
        key: &[u8],
        before_pivot: bool,
        pivot: &[u8],
        value: &[u8],
    ) -> Result<i64, WrongType> {
        if self.remove_expired(key) {
            return Ok(0);
        }

        let before = self.entry_charge(key);
        let limits = self.collection_limits;
        let Some(entry) = self.entries.object_mut(key) else {
            return Ok(0);
        };

        let Value::List(list) = &mut entry.value else {
            return Err(WrongType);
        };

        let Some(position) = list.iter().position(|item| item == pivot) else {
            return Ok(-1);
        };

        list.insert_at(position + usize::from(!before_pivot), value, limits);
        let length = list.len() as i64;

        self.reconcile_entry_charge(before, key);
        self.entries.demote_collection(key);
        Ok(length)
    }

    /// `LREM`: how many items were removed; the key goes when the list empties.
    pub fn list_remove(
        &mut self,
        key: &[u8],
        count: i64,
        value: &[u8],
    ) -> Result<usize, WrongType> {
        if self.remove_expired(key) {
            return Ok(0);
        }

        let before = self.entry_charge(key);
        let limits = self.collection_limits;
        let Some(entry) = self.entries.object_mut(key) else {
            return Ok(0);
        };

        let Value::List(list) = &mut entry.value else {
            return Err(WrongType);
        };

        let removed = list.remove_matching(count, value, limits);

        if list.is_empty() {
            self.remove_emptied(key);
        }

        self.reconcile_entry_charge(before, key);
        self.entries.demote_collection(key);
        Ok(removed)
    }

    /// A borrowed view of `LRANGE key start stop` for streaming a reply.
    pub fn list_range_stream(
        &self,
        key: &[u8],
        start: isize,
        stop: isize,
    ) -> Result<ListStream<'_>, WrongType> {
        let Some(list) = self.live_list(key)? else {
            return Ok(ListStream::empty());
        };

        Ok(match normalize_range(list.len(), start, stop) {
            Some((start, stop)) => ListStream {
                list: Some(list),
                skip: start,
                take: stop - start + 1,
            },
            None => ListStream::empty(),
        })
    }

    pub fn list_range(
        &self,
        key: &[u8],
        start: isize,
        stop: isize,
    ) -> Result<Vec<&[u8]>, WrongType> {
        let Some(list) = self.live_list(key)? else {
            return Ok(Vec::new());
        };

        let Some((start, stop)) = normalize_range(list.len(), start, stop) else {
            return Ok(Vec::new());
        };

        Ok(list.iter_from(start).take(stop - start + 1).collect())
    }
}

/// A list being read: an object, or a small list's packed bytes inside an index slab.
#[derive(Clone, Copy)]
pub(crate) enum ListRead<'a> {
    Value(&'a ListValue),
    Packed { data: &'a [u8], count: usize },
}

impl<'a> ListRead<'a> {
    pub(crate) fn len(self) -> usize {
        match self {
            Self::Value(list) => list.len(),
            Self::Packed { count, .. } => count,
        }
    }

    pub(crate) fn get(self, index: usize) -> Option<&'a [u8]> {
        match self {
            Self::Value(list) => list.get(index),
            Self::Packed { data, count } => {
                (index < count).then(|| next_list_item(data, list_item_offset(data, index)).0)
            }
        }
    }

    pub(crate) fn iter(self) -> ListIter<'a> {
        self.iter_from(0)
    }

    pub(crate) fn iter_from(self, index: usize) -> ListIter<'a> {
        match self {
            Self::Value(list) => list.iter_from(index),
            Self::Packed { data, count } => ListIter::Packed {
                bytes: data,
                at: if index >= count {
                    data.len()
                } else {
                    list_item_offset(data, index)
                },
            },
        }
    }
}

/// Items of one list slice, borrowed from the store.
#[derive(Clone, Copy)]
pub struct ListStream<'a> {
    list: Option<ListRead<'a>>,
    skip: usize,
    take: usize,
}

impl<'a> ListStream<'a> {
    fn empty() -> Self {
        Self {
            list: None,
            skip: 0,
            take: 0,
        }
    }

    pub fn len(&self) -> usize {
        self.take
    }

    pub fn is_empty(&self) -> bool {
        self.take == 0
    }

    pub fn visit<E>(&self, mut visit: impl FnMut(&[u8]) -> Result<(), E>) -> Result<(), E> {
        if let Some(list) = self.list {
            for item in list.iter_from(self.skip).take(self.take) {
                visit(item)?;
            }
        }

        Ok(())
    }
}

impl std::fmt::Debug for ListStream<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ListStream({} items)", self.take)
    }
}

impl PartialEq for ListStream<'_> {
    fn eq(&self, other: &Self) -> bool {
        let collect = |stream: &Self| {
            let mut all: Vec<Vec<u8>> = Vec::new();
            let _ = stream.visit::<()>(|item| {
                all.push(item.to_vec());
                Ok(())
            });
            all
        };

        collect(self) == collect(other)
    }
}
