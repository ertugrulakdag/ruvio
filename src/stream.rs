use crate::id_map::IdMap;
use crate::stream_blocks::Blocks;
use std::cmp::Ordering;
use std::ops::Bound;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StreamEntry {
    pub millis: u64,
    pub sequence: u64,
    pub fields: StreamFields,
}

/// Bytes an entry's fields keep inside the entry before they move to the heap.
const INLINE_FIELDS: usize = 22;

/// An entry's field-value pairs packed into one buffer: each field and each value as a
/// LEB128 length followed by its bytes. Small entries (`f v`, a short reading) fit in
/// the entry itself; larger ones take one allocation instead of one per pair, field and
/// value.
#[derive(Clone, PartialEq, Eq)]
pub struct StreamFields(Packed);

#[derive(Clone, PartialEq, Eq)]
enum Packed {
    Inline(u8, [u8; INLINE_FIELDS]),
    Heap(Box<[u8]>),
}

impl StreamFields {
    pub fn from_pairs<'p>(pairs: impl Iterator<Item = (&'p [u8], &'p [u8])> + Clone) -> Self {
        let size = pairs.clone().fold(0usize, |size, (field, value)| {
            size.saturating_add(varint_len(field.len()) + field.len())
                .saturating_add(varint_len(value.len()) + value.len())
        });
        let write = |buffer: &mut [u8]| {
            let mut at = 0;

            for (field, value) in pairs {
                for bytes in [field, value] {
                    at += put_varint(&mut buffer[at..], bytes.len());
                    buffer[at..at + bytes.len()].copy_from_slice(bytes);
                    at += bytes.len();
                }
            }
        };

        if size <= INLINE_FIELDS {
            let mut inline = [0; INLINE_FIELDS];

            write(&mut inline[..size]);
            Self(Packed::Inline(size as u8, inline))
        } else {
            let mut heap = vec![0; size].into_boxed_slice();

            write(&mut heap);
            Self(Packed::Heap(heap))
        }
    }

    /// Fields already in the packed layout, as a block keeps them.
    pub(crate) fn from_packed(bytes: &[u8]) -> Self {
        Self::build(bytes.len(), |buffer| buffer.copy_from_slice(bytes))
    }

    /// Field names and values, each a run of lengths and bytes in the packed layout,
    /// merged into pairs.
    pub(crate) fn interleave(names: &[u8], values: &[u8]) -> Self {
        Self::build(names.len() + values.len(), |buffer| {
            let (mut names, mut values, mut at) = (names, values, 0);

            while !names.is_empty() {
                for run in [&mut names, &mut values] {
                    let (length, used) = read_varint(run);
                    let (part, rest) = run.split_at(used + length);

                    buffer[at..at + part.len()].copy_from_slice(part);
                    at += part.len();
                    *run = rest;
                }
            }
        })
    }

    fn build(size: usize, write: impl FnOnce(&mut [u8])) -> Self {
        if size <= INLINE_FIELDS {
            let mut inline = [0; INLINE_FIELDS];

            write(&mut inline[..size]);
            Self(Packed::Inline(size as u8, inline))
        } else {
            let mut heap = vec![0; size].into_boxed_slice();

            write(&mut heap);
            Self(Packed::Heap(heap))
        }
    }

    fn bytes(&self) -> &[u8] {
        match &self.0 {
            Packed::Inline(len, bytes) => &bytes[..usize::from(*len)],
            Packed::Heap(bytes) => bytes,
        }
    }

    pub fn iter(&self) -> FieldPairs<'_> {
        FieldPairs(self.bytes())
    }

    /// Number of field-value pairs.
    pub fn len(&self) -> usize {
        self.iter().count()
    }

    pub fn is_empty(&self) -> bool {
        self.bytes().is_empty()
    }

    /// Field and value bytes, without the length prefixes.
    pub fn payload(&self) -> usize {
        self.iter()
            .fold(0, |total, (field, value)| total + field.len() + value.len())
    }

    /// Heap bytes the entry owns beyond its own size.
    pub fn heap_bytes(&self) -> usize {
        match &self.0 {
            Packed::Inline(..) => 0,
            Packed::Heap(bytes) => bytes.len(),
        }
    }
}

impl std::fmt::Debug for StreamFields {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_list().entries(self.iter()).finish()
    }
}

impl<'a> IntoIterator for &'a StreamFields {
    type Item = (&'a [u8], &'a [u8]);
    type IntoIter = FieldPairs<'a>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

/// The pairs of a [`StreamFields`], in insertion order.
#[derive(Clone)]
pub struct FieldPairs<'a>(&'a [u8]);

impl<'a> FieldPairs<'a> {
    fn take(&mut self) -> &'a [u8] {
        let (len, used) = read_varint(self.0);
        let (bytes, rest) = self.0[used..].split_at(len);

        self.0 = rest;
        bytes
    }
}

impl<'a> Iterator for FieldPairs<'a> {
    type Item = (&'a [u8], &'a [u8]);

    fn next(&mut self) -> Option<Self::Item> {
        if self.0.is_empty() {
            return None;
        }

        let field = self.take();
        let value = self.take();

        Some((field, value))
    }
}

fn varint_len(mut value: usize) -> usize {
    let mut len = 1;

    while value >= 0x80 {
        value >>= 7;
        len += 1;
    }

    len
}

fn put_varint(buffer: &mut [u8], mut value: usize) -> usize {
    let mut at = 0;

    while value >= 0x80 {
        buffer[at] = (value as u8) | 0x80;
        value >>= 7;
        at += 1;
    }

    buffer[at] = value as u8;
    at + 1
}

fn read_varint(buffer: &[u8]) -> (usize, usize) {
    let mut value = 0usize;

    for (index, byte) in buffer.iter().enumerate() {
        value |= usize::from(byte & 0x7f) << (7 * index);

        if byte & 0x80 == 0 {
            return (value, index + 1);
        }
    }

    unreachable!("stream fields are written by `StreamFields::from_pairs`")
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PendingDelivery {
    pub millis: u64,
    pub sequence: u64,
    pub consumer: Vec<u8>,
    pub delivered_at: u64,
    pub deliveries: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ConsumerGroup {
    pub name: Vec<u8>,
    pub last_millis: u64,
    pub last_sequence: u64,
    pub pending: Vec<PendingDelivery>,
    /// Names registered before they have a pending entry.
    pub idle: Vec<Vec<u8>>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct ReclaimUpdate {
    pub(crate) millis: u64,
    pub(crate) sequence: u64,
    pub(crate) delivered_at: u64,
    pub(crate) deliveries: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum GroupError {
    Busy,
    NoGroup,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct PendingSummary {
    pub(crate) count: usize,
    pub(crate) smallest: Option<(u64, u64)>,
    pub(crate) greatest: Option<(u64, u64)>,
    pub(crate) consumers: Vec<(Vec<u8>, i64)>,
}

pub(crate) struct PendingFilter<'a> {
    pub(crate) start: StreamBound,
    pub(crate) end: StreamBound,
    pub(crate) min_idle: Option<u64>,
    pub(crate) consumer: Option<&'a [u8]>,
    pub(crate) count: usize,
    pub(crate) now: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct PendingRow {
    pub(crate) millis: u64,
    pub(crate) sequence: u64,
    pub(crate) consumer: Vec<u8>,
    pub(crate) idle: u64,
    pub(crate) deliveries: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct StreamOverview {
    pub(crate) length: usize,
    pub(crate) last_millis: Option<u64>,
    pub(crate) last_sequence: u64,
    pub(crate) groups: usize,
    pub(crate) first: Option<StreamEntry>,
    pub(crate) last: Option<StreamEntry>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct GroupOverview {
    pub(crate) name: Vec<u8>,
    pub(crate) consumers: usize,
    pub(crate) pending: usize,
    pub(crate) last_millis: u64,
    pub(crate) last_sequence: u64,
    pub(crate) lag: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum AutoclaimStep {
    Claim {
        millis: u64,
        sequence: u64,
        deliveries: u64,
    },
    /// The pending id is no longer in the stream.
    Drop { millis: u64, sequence: u64 },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct AutoclaimPlan {
    pub(crate) steps: Vec<AutoclaimStep>,
    /// `0-0` when the pending list was finished. Otherwise the last id visited.
    pub(crate) next: (u64, u64),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ConsumerOverview {
    pub(crate) name: Vec<u8>,
    pub(crate) pending: usize,
    pub(crate) seen_at: u64,
    pub(crate) idle: u64,
}

/// A stream: entries in id order, consumer groups and delayed holds.
///
/// Entries sit in a ring buffer, so trimming from the front (`MAXLEN`, `MINID`) moves
/// nothing. Each group keeps its pending entries list in an id map (see `IdMap`), and every
/// consumer the ids it owns, so acknowledging, claiming and paging the list are logarithmic
/// in its size.
#[derive(Clone, Debug)]
pub struct StreamValue {
    started: bool,
    last_millis: u64,
    last_sequence: u64,
    entries: Blocks,
    /// Sorted by name.
    groups: Vec<Group>,
    /// `(millis, sequence, ready_at)`. Empty until an `XADD DELAY` writes one. `Vec::new` does not allocate.
    delayed: Vec<(u64, u64, u64)>,
    /// Group names and consumer names (each once). Updated on each mutation so memory
    /// admission does not walk the stream.
    payload_bytes: usize,
    /// Pending deliveries across all groups, charged at `PENDING_ROW_BYTES` each.
    pending_rows: usize,
}

impl PartialEq for StreamValue {
    fn eq(&self, other: &Self) -> bool {
        self.started == other.started
            && self.last_millis == other.last_millis
            && self.last_sequence == other.last_sequence
            && self.groups == other.groups
            && self.delayed == other.delayed
            && self.entries.len() == other.entries.len()
            && self.entries.to_vec() == other.entries.to_vec()
    }
}

impl Eq for StreamValue {}

/// Heap bytes one pending delivery takes: its row in the group's id map (id and delivery)
/// plus its id in the consumer's map, with run slack: 60 to 80 bytes per row whether ids
/// were delivered in order or scattered and however many were acknowledged
/// (`a_pending_row_costs_no_more_than_it_is_charged`), charged with some margin.
pub(crate) const PENDING_ROW_BYTES: usize = 88;

type Id = crate::id_map::Id;

#[derive(Clone, Debug, PartialEq, Eq)]
struct Group {
    name: Vec<u8>,
    last_millis: u64,
    last_sequence: u64,
    pending: IdMap<Delivery>,
    /// Slots addressed by the consumer number stored in each delivery; a deleted consumer
    /// leaves `None` until a new one reuses the slot.
    consumers: Vec<Option<Consumer>>,
    /// Slot numbers of the live consumers, sorted by consumer name.
    by_name: Vec<u32>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Delivery {
    consumer: u32,
    delivered_at: u64,
    deliveries: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct Consumer {
    name: Vec<u8>,
    /// Latest delivery or claim time this consumer got, kept after its entries are
    /// acknowledged. `None` for a consumer only ever created (its WAL record and the
    /// snapshot carry no time).
    seen_at: Option<u64>,
    /// Ids of this consumer's pending deliveries.
    owned: IdMap<()>,
}

impl Group {
    fn new(name: &[u8], last_millis: u64, last_sequence: u64) -> Self {
        Self {
            name: name.to_vec(),
            last_millis,
            last_sequence,
            pending: IdMap::new(),
            consumers: Vec::new(),
            by_name: Vec::new(),
        }
    }

    fn consumer(&self, slot: u32) -> &Consumer {
        self.consumers[slot as usize]
            .as_ref()
            .expect("a delivery names a live consumer")
    }

    fn consumer_mut(&mut self, slot: u32) -> &mut Consumer {
        self.consumers[slot as usize]
            .as_mut()
            .expect("a delivery names a live consumer")
    }

    fn name_position(&self, name: &[u8]) -> Result<usize, usize> {
        self.by_name
            .binary_search_by(|slot| self.consumer(*slot).name.as_slice().cmp(name))
    }

    fn find_consumer(&self, name: &[u8]) -> Option<u32> {
        self.name_position(name)
            .ok()
            .map(|position| self.by_name[position])
    }

    /// The consumer's slot, created when missing; the bool says whether it is new.
    fn consumer_slot(&mut self, name: &[u8]) -> (u32, bool) {
        match self.name_position(name) {
            Ok(position) => (self.by_name[position], false),
            Err(position) => {
                let consumer = Consumer {
                    name: name.to_vec(),
                    seen_at: None,
                    owned: IdMap::new(),
                };
                let slot = match self.consumers.iter().position(Option::is_none) {
                    Some(free) => {
                        self.consumers[free] = Some(consumer);
                        free
                    }
                    None => {
                        self.consumers.push(Some(consumer));
                        self.consumers.len() - 1
                    }
                };
                let slot = u32::try_from(slot).expect("fewer than 2^32 consumers");

                self.by_name.insert(position, slot);
                (slot, true)
            }
        }
    }

    /// Inserts or replaces the delivery of `id`, moving it to `consumer`, who counts as seen
    /// at `delivered_at`. Returns the bytes of a consumer name this added and whether the
    /// row is new.
    fn put(
        &mut self,
        id: Id,
        consumer: &[u8],
        delivered_at: u64,
        deliveries: u64,
    ) -> (usize, bool) {
        let (slot, created) = self.consumer_slot(consumer);
        let delivery = Delivery {
            consumer: slot,
            delivered_at,
            deliveries,
        };

        let previous = self.pending.insert(id, delivery);

        if let Some(previous) = previous
            && previous.consumer != slot
        {
            self.consumer_mut(previous.consumer).owned.remove(&id);
        }

        let owner = self.consumer_mut(slot);

        owner.owned.insert(id, ());
        owner.seen_at = Some(
            owner
                .seen_at
                .map_or(delivered_at, |seen| seen.max(delivered_at)),
        );

        (if created { consumer.len() } else { 0 }, previous.is_none())
    }

    fn row(&self, id: Id, delivery: &Delivery) -> PendingDelivery {
        PendingDelivery {
            millis: id.0,
            sequence: id.1,
            consumer: self.consumer(delivery.consumer).name.clone(),
            delivered_at: delivery.delivered_at,
            deliveries: delivery.deliveries,
        }
    }

    fn live_consumers(&self) -> impl Iterator<Item = &Consumer> {
        self.by_name.iter().map(|slot| self.consumer(*slot))
    }

    /// The interchange form snapshots and DUMP write: rows in id order, and the consumers
    /// without a pending entry by name.
    fn export(&self) -> ConsumerGroup {
        ConsumerGroup {
            name: self.name.clone(),
            last_millis: self.last_millis,
            last_sequence: self.last_sequence,
            pending: self
                .pending
                .iter()
                .map(|(id, delivery)| self.row(*id, delivery))
                .collect(),
            idle: self
                .live_consumers()
                .filter(|consumer| consumer.owned.is_empty())
                .map(|consumer| consumer.name.clone())
                .collect(),
        }
    }

    fn import(group: ConsumerGroup) -> Self {
        let mut imported = Self::new(&group.name, group.last_millis, group.last_sequence);

        for name in &group.idle {
            imported.consumer_slot(name);
        }
        for delivery in group.pending {
            imported.put(
                (delivery.millis, delivery.sequence),
                &delivery.consumer,
                delivery.delivered_at,
                delivery.deliveries,
            );
        }

        imported
    }

    fn payload(&self) -> usize {
        self.live_consumers()
            .fold(self.name.len(), |total, consumer| {
                total.saturating_add(consumer.name.len())
            })
    }
}

impl StreamValue {
    pub(crate) fn new() -> Self {
        Self {
            started: false,
            last_millis: 0,
            last_sequence: 0,
            entries: Blocks::new(),
            groups: Vec::new(),
            delayed: Vec::new(),
            payload_bytes: 0,
            pending_rows: 0,
        }
    }

    /// Bytes the stream holds beyond its payload: the packed entries and pending deliveries.
    pub(crate) fn collection_bytes(&self) -> usize {
        self.entries
            .bytes()
            .saturating_add(self.pending_rows.saturating_mul(PENDING_ROW_BYTES))
    }

    /// Books what `Group::put` reports: a new consumer name and a new row.
    fn account(&mut self, (name_bytes, new_row): (usize, bool)) {
        self.payload_bytes = self.payload_bytes.saturating_add(name_bytes);
        self.pending_rows += usize::from(new_row);
    }

    pub(crate) fn len(&self) -> usize {
        self.entries.len()
    }

    pub(crate) fn entries(&self) -> Entries<'_> {
        Entries(&self.entries)
    }

    pub(crate) fn payload_bytes(&self) -> usize {
        self.payload_bytes
    }

    pub(crate) fn append(
        &mut self,
        millis: u64,
        sequence: u64,
        fields: &[(impl AsRef<[u8]>, impl AsRef<[u8]>)],
    ) -> Result<(), &'static [u8]> {
        if fields.is_empty() {
            return Err(b"ERR stream entry needs at least one field");
        }

        if self.started
            && compare_id(millis, sequence, self.last_millis, self.last_sequence)
                != Ordering::Greater
        {
            return Err(
                b"ERR The ID specified in XADD is equal or smaller than the target stream top item",
            );
        }

        self.started = true;
        self.last_millis = millis;
        self.last_sequence = sequence;
        self.entries.push(millis, sequence, fields);

        Ok(())
    }

    pub(crate) fn accepts(&self, millis: u64, sequence: u64) -> bool {
        !self.started
            || compare_id(millis, sequence, self.last_millis, self.last_sequence)
                == Ordering::Greater
    }

    pub(crate) fn last_id_is_too_small(&self, millis: u64, sequence: u64) -> bool {
        self.started
            && compare_id(millis, sequence, self.last_millis, self.last_sequence) == Ordering::Less
    }

    /// Moves the last generated id forward. An equal id is kept. A smaller id is refused.
    pub(crate) fn set_last_id(&mut self, millis: u64, sequence: u64) -> Result<(), &'static [u8]> {
        if self.last_id_is_too_small(millis, sequence) {
            return Err(
                b"ERR The ID specified in XSETID is smaller than the target stream top item",
            );
        }

        self.started = true;
        self.last_millis = millis;
        self.last_sequence = sequence;

        Ok(())
    }

    pub(crate) fn entry(&self, millis: u64, sequence: u64) -> Option<StreamEntry> {
        self.entries.get(millis, sequence)
    }

    pub(crate) fn contains(&self, millis: u64, sequence: u64) -> bool {
        self.entries.contains(millis, sequence)
    }

    /// Sequence for `XADD <millis>-*`. A millis behind the top id is refused.
    pub(crate) fn next_seq_at(&self, millis: u64) -> Result<u64, ()> {
        if !self.started || millis > self.last_millis {
            return Ok(0);
        }

        if millis < self.last_millis || self.last_sequence == u64::MAX {
            return Err(());
        }

        Ok(self.last_sequence + 1)
    }

    pub(crate) fn next_auto_id(&self, now_millis: u64) -> (u64, u64) {
        if !self.started {
            return (now_millis, 0);
        }

        if now_millis > self.last_millis {
            return (now_millis, 0);
        }

        (self.last_millis, self.last_sequence.saturating_add(1))
    }

    pub(crate) fn delete(&mut self, millis: u64, sequence: u64) -> bool {
        if !self.entries.remove(millis, sequence) {
            return false;
        }

        self.drop_holds_for_missing_entries();
        true
    }

    /// Drops entries whose id is strictly below `millis-sequence`.
    pub(crate) fn trim_before(&mut self, millis: u64, sequence: u64) -> usize {
        let removed = self.entries.drop_before(millis, sequence);

        self.drop_holds_for_missing_entries();
        removed
    }

    pub(crate) fn trim(&mut self, max_len: usize) {
        if self.entries.len() > max_len {
            self.drop_front(self.entries.len() - max_len);
        }
    }

    /// Removes the `count` oldest entries.
    fn drop_front(&mut self, count: usize) {
        if count == 0 {
            return;
        }

        self.entries.drop_front(count);
        self.drop_holds_for_missing_entries();
    }

    fn drop_holds_for_missing_entries(&mut self) {
        if self.delayed.is_empty() {
            return;
        }

        let kept: Vec<_> = self
            .delayed
            .iter()
            .copied()
            .filter(|(millis, sequence, _)| self.entries.contains(*millis, *sequence))
            .collect();

        self.delayed = kept;
    }

    pub(crate) fn read_after(&self, cursor: StreamCursor, limit: usize) -> Vec<StreamEntry> {
        match cursor {
            StreamCursor::End => Vec::new(),
            StreamCursor::After(millis, sequence) => self.entries.after(millis, sequence, limit),
        }
    }

    /// At most `count` entries between the bounds, from the start (or from the end when
    /// `reverse`); only the entries returned are visited.
    pub(crate) fn range_page(
        &self,
        start: StreamBound,
        end: StreamBound,
        count: usize,
        reverse: bool,
    ) -> Vec<StreamEntry> {
        let start = match start {
            StreamBound::Min => None,
            StreamBound::Max => return Vec::new(),
            StreamBound::Id(millis, sequence) => Some((millis, sequence)),
        };
        let end = match end {
            StreamBound::Max => None,
            StreamBound::Min => return Vec::new(),
            StreamBound::Id(millis, sequence) => Some((millis, sequence)),
        };

        self.entries.range(start, end, count, reverse)
    }

    fn group(&self, name: &[u8]) -> Result<&Group, GroupError> {
        self.groups
            .binary_search_by(|group| group.name.as_slice().cmp(name))
            .map(|index| &self.groups[index])
            .map_err(|_| GroupError::NoGroup)
    }

    fn group_mut(&mut self, name: &[u8]) -> Result<&mut Group, GroupError> {
        match self
            .groups
            .binary_search_by(|group| group.name.as_slice().cmp(name))
        {
            Ok(index) => Ok(&mut self.groups[index]),
            Err(_) => Err(GroupError::NoGroup),
        }
    }

    pub(crate) fn has_group(&self, name: &[u8]) -> bool {
        self.group(name).is_ok()
    }

    /// Every group in the interchange form, for snapshots and DUMP.
    pub(crate) fn groups(&self) -> Vec<ConsumerGroup> {
        self.groups.iter().map(Group::export).collect()
    }

    pub(crate) fn overview(&self) -> StreamOverview {
        StreamOverview {
            length: self.entries.len(),
            last_millis: self.started.then_some(self.last_millis),
            last_sequence: self.last_sequence,
            groups: self.groups.len(),
            first: self.entries.first(),
            last: self.entries.last(),
        }
    }

    pub(crate) fn group_overviews(&self) -> Vec<GroupOverview> {
        self.groups
            .iter()
            .map(|group| GroupOverview {
                name: group.name.clone(),
                consumers: group.by_name.len(),
                pending: group.pending.len(),
                last_millis: group.last_millis,
                last_sequence: group.last_sequence,
                lag: self.entries_after(group.last_millis, group.last_sequence),
            })
            .collect()
    }

    pub(crate) fn consumer_overviews(
        &self,
        group: &[u8],
        now: u64,
    ) -> Result<Vec<ConsumerOverview>, GroupError> {
        let group = self.group(group)?;

        Ok(group
            .live_consumers()
            .map(|consumer| {
                // A consumer never handed an entry counts as seen now.
                let seen_at = consumer.seen_at.unwrap_or(now);

                ConsumerOverview {
                    name: consumer.name.clone(),
                    pending: consumer.owned.len(),
                    seen_at,
                    idle: now.saturating_sub(seen_at),
                }
            })
            .collect())
    }

    pub(crate) fn autoclaim_plan(
        &self,
        group: &[u8],
        start: (u64, u64),
        min_idle: u64,
        count: usize,
        now: u64,
    ) -> Result<AutoclaimPlan, GroupError> {
        let group = self.group(group)?;
        let mut pending = group
            .pending
            .range((Bound::Excluded(start), Bound::Unbounded))
            .peekable();
        let mut steps = Vec::new();
        let mut claimed = 0usize;

        // Like Redis, look at no more than ten pending entries per one asked for.
        let budget = count.saturating_mul(10);
        let mut scanned = 0usize;

        while let Some((&(millis, sequence), delivery)) = pending.next() {
            scanned += 1;

            if !self.contains(millis, sequence) {
                steps.push(AutoclaimStep::Drop { millis, sequence });
            } else if now.saturating_sub(delivery.delivered_at) >= min_idle {
                steps.push(AutoclaimStep::Claim {
                    millis,
                    sequence,
                    deliveries: delivery.deliveries.saturating_add(1),
                });
                claimed += 1;
            }

            if claimed == count || scanned >= budget {
                let next = if pending.peek().is_none() {
                    (0, 0)
                } else {
                    (millis, sequence)
                };

                return Ok(AutoclaimPlan { steps, next });
            }
        }

        Ok(AutoclaimPlan {
            steps,
            next: (0, 0),
        })
    }

    fn entries_after(&self, millis: u64, sequence: u64) -> usize {
        self.entries.count_after(millis, sequence)
    }

    pub(crate) fn group_cursor(&self, name: &[u8]) -> Result<(u64, u64), GroupError> {
        let group = self.group(name)?;

        Ok((group.last_millis, group.last_sequence))
    }

    pub(crate) fn create_group(
        &mut self,
        name: &[u8],
        last_millis: u64,
        last_sequence: u64,
    ) -> Result<(), GroupError> {
        let Err(index) = self
            .groups
            .binary_search_by(|group| group.name.as_slice().cmp(name))
        else {
            return Err(GroupError::Busy);
        };

        self.groups
            .insert(index, Group::new(name, last_millis, last_sequence));
        self.payload_bytes = self.payload_bytes.saturating_add(name.len());
        Ok(())
    }

    pub(crate) fn destroy_group(&mut self, name: &[u8]) -> bool {
        let Ok(index) = self
            .groups
            .binary_search_by(|group| group.name.as_slice().cmp(name))
        else {
            return false;
        };

        let removed = self.groups.remove(index);

        self.payload_bytes = self.payload_bytes.saturating_sub(removed.payload());
        self.pending_rows -= removed.pending.len();
        true
    }

    pub(crate) fn set_group_id(
        &mut self,
        name: &[u8],
        millis: u64,
        sequence: u64,
    ) -> Result<(), GroupError> {
        let group = self.group_mut(name)?;

        group.last_millis = millis;
        group.last_sequence = sequence;

        Ok(())
    }

    /// Registers a consumer with no pending entry. Returns whether the name is new.
    pub(crate) fn create_consumer(
        &mut self,
        name: &[u8],
        consumer: &[u8],
    ) -> Result<bool, GroupError> {
        let (_, created) = self.group_mut(name)?.consumer_slot(consumer);

        if created {
            self.payload_bytes = self.payload_bytes.saturating_add(consumer.len());
        }
        Ok(created)
    }

    /// Removes a consumer and its pending deliveries; returns how many deliveries went.
    pub(crate) fn delete_consumer(
        &mut self,
        name: &[u8],
        consumer: &[u8],
    ) -> Result<usize, GroupError> {
        let group = self.group_mut(name)?;
        let Ok(position) = group.name_position(consumer) else {
            return Ok(0);
        };
        let slot = group.by_name.remove(position);
        let removed = group.consumers[slot as usize]
            .take()
            .expect("a listed consumer is live");

        for (id, ()) in &removed.owned {
            group.pending.remove(id);
        }

        self.payload_bytes = self.payload_bytes.saturating_sub(removed.name.len());
        self.pending_rows -= removed.owned.len();
        Ok(removed.owned.len())
    }

    pub(crate) fn advance_group(
        &mut self,
        name: &[u8],
        millis: u64,
        sequence: u64,
    ) -> Result<(), GroupError> {
        let group = self.group_mut(name)?;

        if compare_id(millis, sequence, group.last_millis, group.last_sequence) == Ordering::Greater
        {
            group.last_millis = millis;
            group.last_sequence = sequence;
        }

        Ok(())
    }

    pub(crate) fn hold_until(&mut self, millis: u64, sequence: u64, ready_at: u64) {
        if let Some((held_ms, held_seq, _)) = self.delayed.last() {
            match compare_id(millis, sequence, *held_ms, *held_seq) {
                Ordering::Greater => {
                    self.delayed.push((millis, sequence, ready_at));

                    return;
                }

                Ordering::Equal => return,
                Ordering::Less => {}
            }
        }

        let index = self
            .delayed
            .binary_search_by(|(held_ms, held_seq, _)| {
                compare_id(*held_ms, *held_seq, millis, sequence)
            })
            .unwrap_or_else(|index| index);

        if self
            .delayed
            .get(index)
            .is_some_and(|(held_ms, held_seq, _)| *held_ms == millis && *held_seq == sequence)
        {
            return;
        }

        self.delayed.insert(index, (millis, sequence, ready_at));
    }

    /// Entries held back from groups by a delayed add.
    pub(crate) fn held_count(&self) -> usize {
        self.delayed.len()
    }

    pub(crate) fn new_for_group(
        &mut self,
        name: &[u8],
        limit: usize,
        now: u64,
    ) -> Result<Vec<StreamEntry>, GroupError> {
        let group = self.group(name)?;
        let start = (group.last_millis, group.last_sequence);
        let cursor = StreamCursor::After(start.0, start.1);

        if self.delayed.is_empty() {
            return Ok(self.read_after(cursor, limit));
        }

        self.delayed.retain(|(_, _, ready_at)| *ready_at > now);

        // Held ids are few; read ahead by the limit plus the holds, so at most that many
        // entries are decoded.
        let mut ready = Vec::new();
        let mut after = start;

        while ready.len() < limit {
            let want = (limit - ready.len()).saturating_add(self.delayed.len());
            let chunk = self.entries.after(after.0, after.1, want);
            let exhausted = chunk.len() < want;

            if let Some(last) = chunk.last() {
                after = (last.millis, last.sequence);
            }

            ready.extend(chunk.into_iter().filter(|entry| {
                !self.delayed.iter().any(|(millis, sequence, _)| {
                    *millis == entry.millis && *sequence == entry.sequence
                })
            }));

            if exhausted {
                break;
            }
        }

        ready.truncate(limit);
        Ok(ready)
    }

    pub(crate) fn claim(
        &mut self,
        name: &[u8],
        consumer: &[u8],
        millis: u64,
        sequence: u64,
        delivered_at: u64,
    ) -> Result<(), GroupError> {
        let group = self.group_mut(name)?;

        if group.pending.contains_key(&(millis, sequence)) {
            return Ok(());
        }

        if compare_id(millis, sequence, group.last_millis, group.last_sequence) != Ordering::Greater
        {
            return Ok(());
        }

        group.last_millis = millis;
        group.last_sequence = sequence;
        let added = group.put((millis, sequence), consumer, delivered_at, 1);

        self.account(added);
        Ok(())
    }

    pub(crate) fn reclaim(
        &mut self,
        name: &[u8],
        consumer: &[u8],
        update: ReclaimUpdate,
    ) -> Result<bool, GroupError> {
        let group = self.group_mut(name)?;
        let id = (update.millis, update.sequence);

        if !group.pending.contains_key(&id) {
            return Ok(false);
        }

        let added = group.put(id, consumer, update.delivered_at, update.deliveries);

        self.account(added);
        Ok(true)
    }

    /// Inserts a pending row without moving the group cursor. An existing row is replaced.
    pub(crate) fn upsert_pending(
        &mut self,
        name: &[u8],
        consumer: &[u8],
        update: ReclaimUpdate,
    ) -> Result<(), GroupError> {
        let added = self.group_mut(name)?.put(
            (update.millis, update.sequence),
            consumer,
            update.delivered_at,
            update.deliveries,
        );

        self.account(added);
        Ok(())
    }

    /// Removes a pending delivery. Its consumer stays in the group.
    pub(crate) fn ack(
        &mut self,
        name: &[u8],
        millis: u64,
        sequence: u64,
    ) -> Result<bool, GroupError> {
        let group = self.group_mut(name)?;
        let Some(delivery) = group.pending.remove(&(millis, sequence)) else {
            return Ok(false);
        };

        group
            .consumer_mut(delivery.consumer)
            .owned
            .remove(&(millis, sequence));
        self.pending_rows -= 1;
        Ok(true)
    }

    /// The pending delivery of one id, if the group has it.
    pub(crate) fn pending_delivery(
        &self,
        name: &[u8],
        millis: u64,
        sequence: u64,
    ) -> Result<Option<PendingDelivery>, GroupError> {
        let group = self.group(name)?;

        Ok(group
            .pending
            .get(&(millis, sequence))
            .map(|delivery| group.row((millis, sequence), delivery)))
    }

    pub(crate) fn is_pending(
        &self,
        name: &[u8],
        millis: u64,
        sequence: u64,
    ) -> Result<bool, GroupError> {
        Ok(self.group(name)?.pending.contains_key(&(millis, sequence)))
    }

    pub(crate) fn pending_summary(&self, name: &[u8]) -> Result<PendingSummary, GroupError> {
        let group = self.group(name)?;

        Ok(PendingSummary {
            count: group.pending.len(),
            smallest: group.pending.first_key(),
            greatest: group.pending.last_key(),
            consumers: group
                .live_consumers()
                .filter(|consumer| !consumer.owned.is_empty())
                .map(|consumer| (consumer.name.clone(), consumer.owned.len() as i64))
                .collect(),
        })
    }

    pub(crate) fn pending_rows(
        &self,
        name: &[u8],
        filter: PendingFilter<'_>,
    ) -> Result<Vec<PendingRow>, GroupError> {
        let group = self.group(name)?;

        if filter.count == 0 {
            return Ok(Vec::new());
        }

        let start = match filter.start {
            StreamBound::Min => Bound::Unbounded,
            StreamBound::Max => return Ok(Vec::new()),
            StreamBound::Id(millis, sequence) => Bound::Included((millis, sequence)),
        };
        let end = match filter.end {
            StreamBound::Max => Bound::Unbounded,
            StreamBound::Min => return Ok(Vec::new()),
            StreamBound::Id(millis, sequence) => Bound::Included((millis, sequence)),
        };

        if let (Bound::Included(low), Bound::Included(high)) = (start, end)
            && low > high
        {
            return Ok(Vec::new());
        }

        let row = |id: Id, delivery: &Delivery| {
            let idle = filter.now.saturating_sub(delivery.delivered_at);

            (!filter.min_idle.is_some_and(|minimum| idle < minimum)).then(|| PendingRow {
                millis: id.0,
                sequence: id.1,
                consumer: group.consumer(delivery.consumer).name.clone(),
                idle,
                deliveries: delivery.deliveries,
            })
        };

        let rows = match filter.consumer {
            Some(consumer) => {
                let Some(slot) = group.find_consumer(consumer) else {
                    return Ok(Vec::new());
                };

                group
                    .consumer(slot)
                    .owned
                    .range((start, end))
                    .filter_map(|(id, ())| row(*id, &group.pending[id]))
                    .take(filter.count)
                    .collect()
            }
            None => group
                .pending
                .range((start, end))
                .filter_map(|(id, delivery)| row(*id, delivery))
                .take(filter.count)
                .collect(),
        };

        Ok(rows)
    }

    pub(crate) fn high_water(&self) -> (bool, u64, u64) {
        (self.started, self.last_millis, self.last_sequence)
    }

    pub(crate) fn holds(&self) -> &[(u64, u64, u64)] {
        &self.delayed
    }

    pub(crate) fn restore(
        started: bool,
        last_millis: u64,
        last_sequence: u64,
        entries: Vec<StreamEntry>,
        groups: Vec<ConsumerGroup>,
        delayed: Vec<(u64, u64, u64)>,
    ) -> Result<Self, &'static str> {
        if !started && !entries.is_empty() {
            return Err("stream snapshot has entries before it started");
        }

        let mut previous = None;

        for entry in &entries {
            if let Some((millis, sequence)) = previous
                && compare_id(entry.millis, entry.sequence, millis, sequence) != Ordering::Greater
            {
                return Err("stream snapshot entries are not ordered");
            }

            if started
                && compare_id(entry.millis, entry.sequence, last_millis, last_sequence)
                    == Ordering::Greater
            {
                return Err("stream snapshot entry exceeds the last id");
            }

            previous = Some((entry.millis, entry.sequence));
        }

        let mut blocks = Blocks::new();

        for entry in entries {
            let pairs: Vec<_> = entry.fields.iter().collect();

            blocks.push(entry.millis, entry.sequence, &pairs);
        }

        let mut imported: Vec<Group> = groups.into_iter().map(Group::import).collect();

        imported.sort_by(|left, right| left.name.cmp(&right.name));
        if imported.windows(2).any(|pair| pair[0].name == pair[1].name) {
            return Err("duplicate stream group");
        }

        let payload_bytes = imported
            .iter()
            .fold(0usize, |total, group| total.saturating_add(group.payload()));

        let pending_rows = imported.iter().map(|group| group.pending.len()).sum();

        Ok(Self {
            started,
            last_millis,
            last_sequence,
            entries: blocks,
            groups: imported,
            delayed,
            payload_bytes,
            pending_rows,
        })
    }
}

/// Read access to a stream's entries in id order.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Entries<'a>(&'a Blocks);

impl<'a> Entries<'a> {
    pub(crate) fn to_vec(self) -> Vec<StreamEntry> {
        self.0.to_vec()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StreamBound {
    Min,
    Max,
    Id(u64, u64),
}

/// Last id stored by `XGROUP CREATE`. `End` is `$` and is resolved to the current top.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum GroupStart {
    Id(u64, u64),
    End,
}

/// Exclusive cursor for non-blocking `XREAD`. `End` is `$` and yields nothing.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StreamCursor {
    After(u64, u64),
    End,
}

pub(crate) fn parse_id(bytes: &[u8]) -> Option<(u64, u64)> {
    let text = std::str::from_utf8(bytes).ok()?;
    let (millis, sequence) = text.split_once('-')?;

    Some((millis.parse().ok()?, sequence.parse().ok()?))
}

/// `millis-sequence`, written digit by digit: XADD replies with one per call and `format!`
/// cost more than the append itself.
pub(crate) fn format_id(millis: u64, sequence: u64) -> Vec<u8> {
    fn digits(mut value: u64, buffer: &mut [u8; 20]) -> &[u8] {
        let mut at = buffer.len();

        loop {
            at -= 1;
            buffer[at] = b'0' + (value % 10) as u8;
            value /= 10;

            if value == 0 {
                return &buffer[at..];
            }
        }
    }

    let (mut left, mut right) = ([0; 20], [0; 20]);
    let millis = digits(millis, &mut left);
    let sequence = digits(sequence, &mut right);
    let mut id = Vec::with_capacity(millis.len() + 1 + sequence.len());

    id.extend_from_slice(millis);
    id.push(b'-');
    id.extend_from_slice(sequence);
    id
}

fn compare_id(left_ms: u64, left_seq: u64, right_ms: u64, right_seq: u64) -> Ordering {
    left_ms.cmp(&right_ms).then(left_seq.cmp(&right_seq))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ids_are_written_as_decimal_millis_and_sequence() {
        for (millis, sequence) in [
            (0, 0),
            (1, 9),
            (10, 100),
            (1_760_000_000_000, 7),
            (u64::MAX, u64::MAX),
        ] {
            assert_eq!(
                format_id(millis, sequence),
                format!("{millis}-{sequence}").into_bytes()
            );
        }
    }

    #[test]
    fn packed_fields_keep_every_pair_inline_or_on_the_heap() {
        let long = vec![b'x'; 300];
        let cases: [&[(&[u8], &[u8])]; 4] = [
            &[(b"f", b"v")],
            &[(b"", b""), (b"temp", b"23.5")],
            &[(b"payload", &long), (b"k", b"")],
            &[
                (b"a", b"1"),
                (b"b", b"2"),
                (b"c", b"3"),
                (b"d", b"4"),
                (b"e", b"5"),
            ],
        ];

        for pairs in cases {
            let fields = StreamFields::from_pairs(pairs.iter().copied());

            assert_eq!(fields.iter().collect::<Vec<_>>(), pairs);
            assert_eq!(fields.len(), pairs.len());
            assert_eq!(
                fields.payload(),
                pairs.iter().map(|(f, v)| f.len() + v.len()).sum::<usize>()
            );
        }

        assert_eq!(
            StreamFields::from_pairs([(&b"f"[..], &b"v"[..])].into_iter()).heap_bytes(),
            0
        );
        assert_eq!(size_of::<StreamEntry>(), 40);
    }

    #[test]
    fn auto_ids_increase_and_trim_keeps_the_tail() {
        let mut stream = StreamValue::new();
        let (millis, sequence) = stream.next_auto_id(10);

        stream
            .append(millis, sequence, &[(b"f".as_slice(), b"a".as_slice())])
            .unwrap();
        let (millis, sequence) = stream.next_auto_id(10);

        stream
            .append(millis, sequence, &[(b"f".as_slice(), b"b".as_slice())])
            .unwrap();
        stream.trim(1);

        assert_eq!(stream.len(), 1);
        assert_eq!(stream.entries().to_vec()[0].sequence, 1);
        assert!(
            stream
                .append(10, 1, &[(b"f".as_slice(), b"c".as_slice())])
                .is_err()
        );
    }

    #[test]
    fn payload_and_id_lookup_stay_cheap_as_the_stream_grows() {
        let mut stream = StreamValue::new();
        let field = b"f".as_slice();
        let value = b"0123456789abcdef".as_slice();
        let started = std::time::Instant::now();

        for sequence in 0..8_000 {
            stream.append(1, sequence, &[(field, value)]).unwrap();
            assert!(stream.contains(1, sequence));
        }

        assert!(stream.contains(1, 7_999));
        assert!(stream.contains(1, 0));
        assert!(!stream.contains(1, 8_000));
        assert_eq!(
            stream
                .range_page(
                    StreamBound::Id(1, 10),
                    StreamBound::Id(1, 19),
                    usize::MAX,
                    false
                )
                .len(),
            10
        );
        assert!(started.elapsed() < std::time::Duration::from_millis(400));
    }

    #[test]
    fn mutations_keep_the_payload_counter_exact() {
        let mut stream = StreamValue::new();

        stream
            .append(1, 0, &[(b"field".as_slice(), b"value".as_slice())])
            .unwrap();
        stream
            .append(1, 1, &[(b"field".as_slice(), b"other".as_slice())])
            .unwrap();
        stream.trim(1);
        assert!(!stream.delete(1, 0));
        assert!(stream.delete(1, 1));
        stream
            .append(2, 0, &[(b"n".as_slice(), b"x".as_slice())])
            .unwrap();
        stream.create_group(b"workers", 0, 0).unwrap();
        stream.claim(b"workers", b"ada", 2, 0, 5).unwrap();
        stream
            .reclaim(
                b"workers",
                b"bea",
                ReclaimUpdate {
                    millis: 2,
                    sequence: 0,
                    delivered_at: 6,
                    deliveries: 2,
                },
            )
            .unwrap();
        stream.ack(b"workers", 2, 0).unwrap();
        stream.create_group(b"other", 0, 0).unwrap();
        stream.destroy_group(b"other");
        // Like Redis, a consumer outlives its acknowledged and reclaimed entries.
        assert_eq!(
            stream.payload_bytes(),
            b"workers".len() + b"ada".len() + b"bea".len()
        );
        assert_eq!(stream.payload_bytes(), walked_payload(&stream));
        assert_eq!(stream.read_after(StreamCursor::After(2, 0), 10).len(), 0);
        assert_eq!(
            stream
                .range_page(StreamBound::Min, StreamBound::Max, usize::MAX, false)
                .len(),
            1
        );
    }

    #[test]
    fn a_consumer_stays_seen_at_its_last_delivery_after_acknowledging_it() {
        let mut stream = StreamValue::new();

        stream
            .append(1, 0, &[(b"f".as_slice(), b"v".as_slice())])
            .unwrap();
        stream.create_group(b"g", 0, 0).unwrap();
        stream.create_consumer(b"g", b"new").unwrap();
        stream.claim(b"g", b"ada", 1, 0, 5).unwrap();
        stream.ack(b"g", 1, 0).unwrap();

        let rows = stream.consumer_overviews(b"g", 12).unwrap();

        assert_eq!(
            rows.iter()
                .map(|row| (row.name.as_slice(), row.pending, row.idle))
                .collect::<Vec<_>>(),
            [(b"ada".as_slice(), 0, 7), (b"new".as_slice(), 0, 0)]
        );
    }

    fn walked_payload(stream: &StreamValue) -> usize {
        stream.groups().iter().fold(0usize, |total, group| {
            let mut names: Vec<&[u8]> = group
                .pending
                .iter()
                .map(|delivery| delivery.consumer.as_slice())
                .chain(group.idle.iter().map(Vec::as_slice))
                .collect();

            names.sort_unstable();
            names.dedup();
            total.saturating_add(group.name.len()).saturating_add(
                names
                    .iter()
                    .fold(0usize, |sum, name| sum.saturating_add(name.len())),
            )
        })
    }

    #[test]
    fn a_pending_row_costs_no_more_than_it_is_charged() {
        // The group's row and the owner's id, as `Group::put` stores them.
        let per_row = |order: &[u64], acked: &[u64]| {
            let mut rows = IdMap::new();
            let mut owned = IdMap::new();

            for &sequence in order {
                rows.insert(
                    (1, sequence),
                    Delivery {
                        consumer: 0,
                        delivered_at: 0,
                        deliveries: 1,
                    },
                );
                owned.insert((1, sequence), ());
            }
            for &sequence in acked {
                rows.remove(&(1, sequence));
                owned.remove(&(1, sequence));
            }

            (rows.heap_bytes() + owned.heap_bytes()) / rows.len()
        };
        let in_order: Vec<u64> = (0..20_000).collect();
        // A fixed odd stride visits every id once in a scattered order.
        let scattered: Vec<u64> = (0..20_000u64).map(|i| i * 7_919 % 20_000).collect();
        let every_other: Vec<u64> = scattered.iter().copied().filter(|i| i % 2 == 0).collect();
        // Just under half of each full run: too many left to merge with a neighbour.
        let most_of_each_run: Vec<u64> = (0..20_000u64).filter(|i| i % 32 < 15).collect();
        let a_third_of_each_run: Vec<u64> = (0..20_000u64).filter(|i| i % 32 < 10).collect();
        // All but one in 64, in a scattered order: runs disappear by merging.
        let all_but_few: Vec<u64> = scattered.iter().copied().filter(|i| i % 64 != 0).collect();

        for bytes in [
            per_row(&in_order, &[]),
            per_row(&scattered, &[]),
            per_row(&in_order, &every_other),
            per_row(&scattered, &every_other),
            per_row(&in_order, &most_of_each_run),
            per_row(&scattered, &most_of_each_run),
            per_row(&in_order, &a_third_of_each_run),
            per_row(&in_order, &all_but_few),
            per_row(&scattered, &all_but_few),
        ] {
            assert!(bytes <= PENDING_ROW_BYTES, "{bytes} bytes per row");
        }
    }

    mod model {
        use super::*;
        use proptest::prelude::*;
        use std::collections::BTreeMap as Map;

        type Id = (u64, u64);

        /// The plain form the stream must agree with: entries and pending rows in vectors,
        /// scanned linearly.
        #[derive(Default)]
        struct Model {
            entries: Vec<(Id, Vec<u8>)>,
            groups: Map<Vec<u8>, ModelGroup>,
        }

        #[derive(Default)]
        struct ModelGroup {
            last: Id,
            /// `(id, consumer, delivered_at, deliveries)`, sorted by id.
            pending: Vec<(Id, Vec<u8>, u64, u64)>,
            /// Each consumer with the latest time it was handed an entry.
            consumers: Map<Vec<u8>, Option<u64>>,
        }

        impl ModelGroup {
            fn put(&mut self, id: Id, consumer: &[u8], at: u64, deliveries: u64) {
                let seen = self.consumers.entry(consumer.to_vec()).or_default();

                *seen = Some(seen.map_or(at, |seen| seen.max(at)));
                match self.pending.binary_search_by(|row| row.0.cmp(&id)) {
                    Ok(index) => self.pending[index] = (id, consumer.to_vec(), at, deliveries),
                    Err(index) => self
                        .pending
                        .insert(index, (id, consumer.to_vec(), at, deliveries)),
                }
            }
        }

        #[derive(Clone, Debug)]
        enum Op {
            Add {
                step: u64,
                value: u8,
            },
            Trim(usize),
            TrimBefore(usize),
            Delete(usize),
            CreateGroup(u8),
            DestroyGroup(u8),
            Read {
                group: u8,
                consumer: u8,
                count: usize,
            },
            Ack {
                group: u8,
                pick: usize,
            },
            Claim {
                group: u8,
                consumer: u8,
                pick: usize,
            },
            Autoclaim {
                group: u8,
                consumer: u8,
                pick: usize,
                min_idle: u64,
                count: usize,
            },
            CreateConsumer {
                group: u8,
                consumer: u8,
            },
            DeleteConsumer {
                group: u8,
                consumer: u8,
            },
            Tick(u64),
        }

        fn op() -> impl Strategy<Value = Op> {
            prop_oneof![
                6 => (0u64..3, any::<u8>()).prop_map(|(step, value)| Op::Add { step, value }),
                1 => (0usize..40).prop_map(Op::Trim),
                1 => (0usize..60).prop_map(Op::TrimBefore),
                1 => (0usize..60).prop_map(Op::Delete),
                1 => (0u8..3).prop_map(Op::CreateGroup),
                1 => (0u8..3).prop_map(Op::DestroyGroup),
                4 => (0u8..3, 0u8..4, 1usize..6)
                    .prop_map(|(group, consumer, count)| Op::Read { group, consumer, count }),
                3 => (0u8..3, 0usize..60).prop_map(|(group, pick)| Op::Ack { group, pick }),
                2 => (0u8..3, 0u8..4, 0usize..60)
                    .prop_map(|(group, consumer, pick)| Op::Claim { group, consumer, pick }),
                2 => (0u8..3, 0u8..4, 0usize..60, 0u64..4, 1usize..5).prop_map(
                    |(group, consumer, pick, min_idle, count)| Op::Autoclaim {
                        group,
                        consumer,
                        pick,
                        min_idle,
                        count
                    }
                ),
                1 => (0u8..3, 0u8..4)
                    .prop_map(|(group, consumer)| Op::CreateConsumer { group, consumer }),
                1 => (0u8..3, 0u8..4)
                    .prop_map(|(group, consumer)| Op::DeleteConsumer { group, consumer }),
                2 => (1u64..3).prop_map(Op::Tick),
            ]
        }

        fn group_name(group: u8) -> Vec<u8> {
            format!("g{group}").into_bytes()
        }

        fn consumer_name(consumer: u8) -> Vec<u8> {
            // Names of different lengths keep the payload counter honest.
            "c".repeat(consumer as usize + 1).into_bytes()
        }

        /// An id near the stream: existing entries, a removed one, or one past the end.
        fn pick_id(stream: &StreamValue, model: &Model, pick: usize) -> Id {
            if let Some(group) = model
                .groups
                .values()
                .find(|group| !group.pending.is_empty())
                && pick.is_multiple_of(3)
            {
                return group.pending[pick % group.pending.len()].0;
            }

            let (_, millis, sequence) = stream.high_water();

            match model.entries.get(pick % (model.entries.len() + 1)) {
                Some((id, _)) => *id,
                None => (millis, sequence + (pick as u64 % 2)),
            }
        }

        fn check(stream: &StreamValue, model: &Model, now: u64) {
            let entries: Vec<(Id, Vec<u8>)> = stream
                .entries()
                .to_vec()
                .iter()
                .map(|entry| {
                    (
                        (entry.millis, entry.sequence),
                        entry.fields.iter().next().unwrap().1.to_vec(),
                    )
                })
                .collect();
            assert_eq!(entries, model.entries);
            assert_eq!(stream.len(), model.entries.len());
            assert_eq!(
                stream
                    .range_page(StreamBound::Min, StreamBound::Max, usize::MAX, false)
                    .len(),
                model.entries.len()
            );

            // XRANGE / XREVRANGE pages from inside the stream.
            let middle = model
                .entries
                .get(model.entries.len() / 2)
                .map(|entry| entry.0);
            for start in [
                StreamBound::Min,
                middle.map_or(StreamBound::Max, |id| StreamBound::Id(id.0, id.1)),
            ] {
                for (count, reverse) in [(0, false), (3, false), (3, true), (usize::MAX, true)] {
                    let got: Vec<Id> = stream
                        .range_page(start, StreamBound::Max, count, reverse)
                        .iter()
                        .map(|entry| (entry.millis, entry.sequence))
                        .collect();
                    let mut want: Vec<Id> = model
                        .entries
                        .iter()
                        .map(|entry| entry.0)
                        .filter(|id| bound_ok(start, *id, true))
                        .collect();
                    if reverse {
                        want.reverse();
                    }
                    want.truncate(count);
                    assert_eq!(got, want);
                }
            }

            let groups = stream.groups();
            assert_eq!(groups.len(), model.groups.len());

            for (exported, (name, group)) in groups.iter().zip(&model.groups) {
                assert_eq!(&exported.name, name);
                assert_eq!((exported.last_millis, exported.last_sequence), group.last);
                let rows: Vec<_> = exported
                    .pending
                    .iter()
                    .map(|row| {
                        (
                            (row.millis, row.sequence),
                            row.consumer.clone(),
                            row.delivered_at,
                            row.deliveries,
                        )
                    })
                    .collect();
                assert_eq!(rows, group.pending);

                let summary = stream.pending_summary(name).unwrap();
                assert_eq!(summary.count, group.pending.len());
                assert_eq!(summary.smallest, group.pending.first().map(|row| row.0));
                assert_eq!(summary.greatest, group.pending.last().map(|row| row.0));

                let overviews = stream.consumer_overviews(name, now).unwrap();
                let names: Vec<_> = overviews.iter().map(|row| row.name.clone()).collect();
                assert_eq!(names, group.consumers.keys().cloned().collect::<Vec<_>>());
                for row in &overviews {
                    let owned = group.pending.iter().filter(|p| p.1 == row.name).count();
                    assert_eq!(row.pending, owned);
                    // Seen when last handed an entry, even if it was acknowledged since.
                    let seen = group.consumers[&row.name].unwrap_or(now);
                    assert_eq!(row.idle, now.saturating_sub(seen));
                }

                // XPENDING ranges, with and without a consumer and an idle floor.
                let ids: Vec<Id> = group.pending.iter().map(|row| row.0).collect();
                let bounds = [
                    (StreamBound::Min, StreamBound::Max),
                    (
                        ids.first()
                            .map_or(StreamBound::Min, |id| StreamBound::Id(id.0, id.1 + 1)),
                        ids.last()
                            .map_or(StreamBound::Max, |id| StreamBound::Id(id.0, id.1)),
                    ),
                ];
                for (start, end) in bounds {
                    for consumer in [None, Some(consumer_name(0)), Some(consumer_name(2))] {
                        for (min_idle, count) in [(None, 100), (Some(2), 2)] {
                            let got = stream
                                .pending_rows(
                                    name,
                                    PendingFilter {
                                        start,
                                        end,
                                        min_idle,
                                        consumer: consumer.as_deref(),
                                        count,
                                        now,
                                    },
                                )
                                .unwrap();
                            let want: Vec<_> = group
                                .pending
                                .iter()
                                .filter(|row| {
                                    bound_ok(start, row.0, true) && bound_ok(end, row.0, false)
                                })
                                .filter(|row| consumer.as_ref().is_none_or(|c| &row.1 == c))
                                .filter(|row| {
                                    min_idle.is_none_or(|m| now.saturating_sub(row.2) >= m)
                                })
                                .take(count)
                                .map(|row| PendingRow {
                                    millis: row.0.0,
                                    sequence: row.0.1,
                                    consumer: row.1.clone(),
                                    idle: now.saturating_sub(row.2),
                                    deliveries: row.3,
                                })
                                .collect();
                            assert_eq!(got, want);
                        }
                    }
                }
            }

            let expected_payload = model
                .groups
                .iter()
                .map(|(name, group)| {
                    name.len() + group.consumers.keys().map(Vec::len).sum::<usize>()
                })
                .sum::<usize>();
            assert_eq!(stream.payload_bytes(), expected_payload);
            let rows: usize = model.groups.values().map(|group| group.pending.len()).sum();
            let packed = stream.collection_bytes() - rows * PENDING_ROW_BYTES;
            let held: usize = model.entries.iter().map(|entry| 1 + entry.1.len()).sum();

            if model.entries.is_empty() {
                assert_eq!(packed, 0);
            } else {
                assert!(packed >= held, "{packed} bytes for {held} of payload");
            }

            let (started, millis, sequence) = stream.high_water();
            let restored = StreamValue::restore(
                started,
                millis,
                sequence,
                stream.entries().to_vec(),
                stream.groups(),
                stream.holds().to_vec(),
            )
            .unwrap();
            // Consumer slots may be numbered differently; everything observable matches.
            assert_eq!(restored.groups(), stream.groups());
            assert_eq!(restored.entries().to_vec(), stream.entries().to_vec());
            assert_eq!(restored.payload_bytes(), stream.payload_bytes());
            assert_eq!(restored.len(), stream.len());
        }

        fn bound_ok(bound: StreamBound, id: Id, start: bool) -> bool {
            match (bound, start) {
                (StreamBound::Min, true) | (StreamBound::Max, false) => true,
                (StreamBound::Min, false) | (StreamBound::Max, true) => false,
                (StreamBound::Id(millis, sequence), true) => id >= (millis, sequence),
                (StreamBound::Id(millis, sequence), false) => id <= (millis, sequence),
            }
        }

        fn apply(stream: &mut StreamValue, model: &mut Model, op: &Op, now: &mut u64) {
            match *op {
                Op::Add { step, value } => {
                    *now += step;
                    let (millis, sequence) = stream.next_auto_id(*now);
                    stream
                        .append(millis, sequence, &[(b"f".as_slice(), [value].as_slice())])
                        .unwrap();
                    model.entries.push(((millis, sequence), vec![value]));
                }
                Op::Trim(max_len) => {
                    stream.trim(max_len);
                    let excess = model.entries.len().saturating_sub(max_len);
                    model.entries.drain(..excess);
                }
                Op::TrimBefore(pick) => {
                    let id = pick_id(stream, model, pick);
                    let removed = stream.trim_before(id.0, id.1);
                    let before = model.entries.len();
                    model.entries.retain(|entry| entry.0 >= id);
                    assert_eq!(removed, before - model.entries.len());
                }
                Op::Delete(pick) => {
                    let id = pick_id(stream, model, pick);
                    let present = model.entries.iter().any(|entry| entry.0 == id);
                    assert_eq!(stream.delete(id.0, id.1), present);
                    model.entries.retain(|entry| entry.0 != id);
                }
                Op::CreateGroup(group) => {
                    let name = group_name(group);
                    let created = stream.create_group(&name, 0, 0).is_ok();
                    assert_eq!(created, !model.groups.contains_key(&name));
                    model.groups.entry(name).or_default();
                }
                Op::DestroyGroup(group) => {
                    let name = group_name(group);
                    assert_eq!(
                        stream.destroy_group(&name),
                        model.groups.remove(&name).is_some()
                    );
                }
                Op::Read {
                    group,
                    consumer,
                    count,
                } => {
                    let name = group_name(group);
                    let consumer = consumer_name(consumer);
                    let Some(state) = model.groups.get_mut(&name) else {
                        assert!(stream.new_for_group(&name, count, *now).is_err());
                        return;
                    };
                    let ids: Vec<Id> = stream
                        .new_for_group(&name, count, *now)
                        .unwrap()
                        .iter()
                        .map(|entry| (entry.millis, entry.sequence))
                        .collect();
                    let want: Vec<Id> = model
                        .entries
                        .iter()
                        .map(|entry| entry.0)
                        .filter(|id| *id > state.last)
                        .take(count)
                        .collect();
                    assert_eq!(ids, want);
                    for id in ids {
                        stream.claim(&name, &consumer, id.0, id.1, *now).unwrap();
                        state.last = id;
                        state.put(id, &consumer, *now, 1);
                    }
                }
                Op::Ack { group, pick } => {
                    let name = group_name(group);
                    let id = pick_id(stream, model, pick);
                    let Some(state) = model.groups.get_mut(&name) else {
                        assert!(stream.ack(&name, id.0, id.1).is_err());
                        return;
                    };
                    let before = state.pending.len();
                    state.pending.retain(|row| row.0 != id);
                    assert_eq!(
                        stream.ack(&name, id.0, id.1).unwrap(),
                        before != state.pending.len()
                    );
                }
                Op::Claim {
                    group,
                    consumer,
                    pick,
                } => {
                    let name = group_name(group);
                    let consumer = consumer_name(consumer);
                    let id = pick_id(stream, model, pick);
                    let Some(state) = model.groups.get_mut(&name) else {
                        return;
                    };
                    let row = state.pending.iter().find(|row| row.0 == id).cloned();
                    assert_eq!(stream.is_pending(&name, id.0, id.1).unwrap(), row.is_some());
                    assert_eq!(
                        stream
                            .pending_delivery(&name, id.0, id.1)
                            .unwrap()
                            .map(|row| (row.consumer, row.delivered_at, row.deliveries)),
                        row.as_ref().map(|row| (row.1.clone(), row.2, row.3))
                    );
                    let update = ReclaimUpdate {
                        millis: id.0,
                        sequence: id.1,
                        delivered_at: *now,
                        deliveries: row.as_ref().map_or(1, |row| row.3 + 1),
                    };
                    assert_eq!(
                        stream.reclaim(&name, &consumer, update).unwrap(),
                        row.is_some()
                    );
                    if row.is_some() {
                        state.put(id, &consumer, *now, update.deliveries);
                    }
                }
                Op::Autoclaim {
                    group,
                    consumer,
                    pick,
                    min_idle,
                    count,
                } => {
                    let name = group_name(group);
                    let consumer = consumer_name(consumer);
                    let start = if pick.is_multiple_of(4) {
                        (0, 0)
                    } else {
                        pick_id(stream, model, pick)
                    };
                    let Some(state) = model.groups.get_mut(&name) else {
                        return;
                    };
                    let plan = stream
                        .autoclaim_plan(&name, start, min_idle, count, *now)
                        .unwrap();

                    let after: Vec<_> = state.pending.iter().filter(|row| row.0 > start).collect();
                    let mut steps = Vec::new();
                    let mut next = (0, 0);
                    let mut claimed = 0;
                    for (index, row) in after.iter().enumerate() {
                        if !model.entries.iter().any(|entry| entry.0 == row.0) {
                            steps.push(AutoclaimStep::Drop {
                                millis: row.0.0,
                                sequence: row.0.1,
                            });
                        } else if now.saturating_sub(row.2) >= min_idle {
                            steps.push(AutoclaimStep::Claim {
                                millis: row.0.0,
                                sequence: row.0.1,
                                deliveries: row.3 + 1,
                            });
                            claimed += 1;
                        }
                        // At most ten pending entries looked at per one asked for.
                        if claimed == count || index + 1 >= count * 10 {
                            if index + 1 != after.len() {
                                next = row.0;
                            }
                            break;
                        }
                    }
                    assert_eq!(plan, AutoclaimPlan { steps, next });

                    // Carry the plan out the way the engine does.
                    for step in plan.steps {
                        match step {
                            AutoclaimStep::Claim {
                                millis,
                                sequence,
                                deliveries,
                            } => {
                                stream
                                    .reclaim(
                                        &name,
                                        &consumer,
                                        ReclaimUpdate {
                                            millis,
                                            sequence,
                                            delivered_at: *now,
                                            deliveries,
                                        },
                                    )
                                    .unwrap();
                                state.put((millis, sequence), &consumer, *now, deliveries);
                            }
                            AutoclaimStep::Drop { millis, sequence } => {
                                stream.ack(&name, millis, sequence).unwrap();
                                state.pending.retain(|row| row.0 != (millis, sequence));
                            }
                        }
                    }
                }
                Op::CreateConsumer { group, consumer } => {
                    let name = group_name(group);
                    let consumer = consumer_name(consumer);
                    match model.groups.get_mut(&name) {
                        Some(state) => {
                            let created = !state.consumers.contains_key(&consumer);

                            state.consumers.entry(consumer.clone()).or_insert(None);
                            assert_eq!(stream.create_consumer(&name, &consumer).unwrap(), created);
                        }
                        None => assert!(stream.create_consumer(&name, &consumer).is_err()),
                    }
                }
                Op::DeleteConsumer { group, consumer } => {
                    let name = group_name(group);
                    let consumer = consumer_name(consumer);
                    let Some(state) = model.groups.get_mut(&name) else {
                        return;
                    };
                    let before = state.pending.len();
                    state.pending.retain(|row| row.1 != consumer);
                    state.consumers.remove(&consumer);
                    assert_eq!(
                        stream.delete_consumer(&name, &consumer).unwrap(),
                        before - state.pending.len()
                    );
                }
                Op::Tick(step) => *now += step,
            }
        }

        proptest! {
            #![proptest_config(ProptestConfig::with_cases(
                std::env::var("PROPTEST_CASES").ok().and_then(|v| v.parse().ok()).unwrap_or(256)
            ))]

            #[test]
            fn stream_matches_a_linear_model(ops in proptest::collection::vec(op(), 1..120)) {
                let mut stream = StreamValue::new();
                let mut model = Model::default();
                let mut now = 1u64;

                for op in &ops {
                    apply(&mut stream, &mut model, op, &mut now);
                    check(&stream, &model, now);
                }
            }
        }
    }
}
