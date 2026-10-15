use super::*;
use crate::arena::{Arena, encode_varint, read_varint};
use crate::slots::{MIGRATE_PER_ELEMENT, Slots};

/// A hash table over members (`PAIRED = false`) or field-value pairs (`PAIRED = true`) whose
/// bytes live in one [`Arena`]. The table holds 4-byte offsets, so a member costs its bytes,
/// a one-byte length and about five bytes of table, with no allocation of its own.
///
/// A set block is the member. A hash block is `varint(field_len) field value`, so the field
/// and its value share one block.
///
/// Large tables resize and compact in slices: the table is a [`Slots`], which grows and
/// shrinks by moving a bounded number of offsets per write, and a sparse arena is
/// emptied chunk set by chunk set ([`Arena::begin_evacuation`]) while writes move a
/// bounded number of live blocks each. [`ArenaTable::background_step`] finishes both
/// for a collection that stopped receiving writes.
#[derive(Debug, Clone)]
pub(super) struct ArenaTable<const PAIRED: bool> {
    arena: Arena,
    table: Slots<u32>,
    hasher: ahash::RandomState,
    /// Next table position the running evacuation inspects, and the table epoch it
    /// was read under; a resize starts the pass over.
    evacuation_cursor: usize,
    evacuation_epoch: u32,
}

/// Arenas with fewer chunks compact in one go: copying under 1 MiB is cheaper than
/// the bookkeeping.
const EVACUATE_MIN_CHUNKS: usize = if cfg!(test) { 2 } else { 16 };
/// Live blocks moved, and table slots inspected, per write while an evacuation runs.
const RELOCATE_PER_WRITE: usize = 32;
const INSPECT_PER_WRITE: usize = 512;

thread_local! {
    /// Set when a collection write starts work that later writes or maintenance must
    /// finish; the shard then remembers the key the write was for.
    static WORK_STARTED: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

/// True when a collection on this thread started a resize or compaction since the last
/// `clear_background_work_started`.
#[inline]
pub(super) fn background_work_started() -> bool {
    WORK_STARTED.get()
}

pub(super) fn clear_background_work_started() {
    WORK_STARTED.set(false);
}

#[cold]
fn note_work_started() {
    WORK_STARTED.set(true);
}

/// True when `value` is a large set or hash with a resize or compaction under way.
#[inline]
pub(super) fn has_background_work(value: &Value) -> bool {
    match value {
        Value::Set(set) => {
            matches!(&set.body, SetBody::Arena(table) if table.has_background_work())
        }
        Value::Hash(hash) => {
            matches!(&hash.inner, HashInner::Arena(table) if table.has_background_work())
        }
        _ => false,
    }
}

/// Moves about `budget` elements of `value`'s unfinished work; true when none is left.
pub(super) fn background_step(value: &mut Value, budget: usize) -> bool {
    match value {
        Value::Set(set) => set.background_step(budget),
        Value::Hash(hash) => hash.inner.background_step(budget),
        _ => true,
    }
}

impl SetValue {
    /// See [`ArenaTable::background_step`]; other encodings have none.
    pub(super) fn background_step(&mut self, budget: usize) -> bool {
        match &mut self.body {
            SetBody::Arena(table) => table.background_step(budget),
            _ => true,
        }
    }
}

impl HashInner {
    /// See [`ArenaTable::background_step`]; other encodings have none.
    pub(super) fn background_step(&mut self, budget: usize) -> bool {
        match self {
            HashInner::Arena(table) => table.background_step(budget),
            _ => true,
        }
    }
}

/// What `insert_new` did when the arena had no room left for another block.
#[derive(Debug, PartialEq, Eq)]
pub(super) struct ArenaFull;

impl<const PAIRED: bool> ArenaTable<PAIRED> {
    #[cfg(test)]
    pub(super) fn new() -> Self {
        Self::with_capacity(0)
    }

    pub(super) fn with_capacity(entries: usize) -> Self {
        let mut table = Slots::with_capacity(entries);

        // A scan cursor names this table's epoch; a random start keeps a cursor from an
        // earlier table under the same key (demoted and promoted again) from matching.
        table.seed_epoch(sample_seed() as u32);

        Self {
            arena: Arena::new(),
            table,
            hasher: ahash::RandomState::new(),
            evacuation_cursor: 0,
            evacuation_epoch: 0,
        }
    }

    fn key_of(body: &[u8]) -> &[u8] {
        if PAIRED {
            let (len, at) = read_varint(body, 0);

            &body[at..at + len]
        } else {
            body
        }
    }

    fn value_of(body: &[u8]) -> &[u8] {
        if PAIRED {
            let (len, at) = read_varint(body, 0);

            &body[at + len..]
        } else {
            &[]
        }
    }

    fn hash(&self, key: &[u8]) -> u64 {
        self.hasher.hash_one(key)
    }

    pub(super) fn len(&self) -> usize {
        self.table.len()
    }

    pub(super) fn hasher(&self) -> &ahash::RandomState {
        &self.hasher
    }

    /// The key stored under `hash` that `accept` takes. `hash` must come from [`Self::hasher`].
    pub(super) fn find_key(
        &self,
        hash: u64,
        mut accept: impl FnMut(&[u8]) -> bool,
    ) -> Option<&[u8]> {
        self.table
            .find(hash, |offset| accept(Self::key_of(self.arena.get(*offset))))
            .map(|offset| Self::key_of(self.arena.get(*offset)))
    }

    fn find(&self, key: &[u8]) -> Option<u32> {
        let hash = self.hash(key);

        self.table
            .find(hash, |offset| Self::key_of(self.arena.get(*offset)) == key)
            .copied()
    }

    pub(super) fn contains(&self, key: &[u8]) -> bool {
        self.find(key).is_some()
    }

    /// The value stored under `key`; empty for a set.
    pub(super) fn get(&self, key: &[u8]) -> Option<&[u8]> {
        self.find(key)
            .map(|offset| Self::value_of(self.arena.get(offset)))
    }

    /// Every `(key, value)`; the value is empty for a set. Order is unspecified.
    pub(super) fn iter(&self) -> impl Iterator<Item = (&[u8], &[u8])> + '_ {
        self.table.iter().map(|offset| {
            let body = self.arena.get(*offset);

            (Self::key_of(body), Self::value_of(body))
        })
    }

    /// Visits entries by home bucket, about `count`, starting where `cursor` left off;
    /// 0 starts a walk and is returned when it is over. The cursor is a reverse-binary
    /// position over hash homes, as in Redis, so it survives grows, shrinks, in-place
    /// rehashes and compaction: an entry present for the whole walk is returned at least
    /// once (a resize can repeat some), and a walk ends after one pass however the table
    /// changes under it.
    pub(super) fn scan_buckets<'a>(
        &'a self,
        cursor: u64,
        count: usize,
        mut visit: impl FnMut(&'a [u8], &'a [u8]),
    ) -> u64 {
        super::home_cursor_out(self.table.scan(
            super::home_cursor_in(cursor),
            count,
            super::scan_budget(count),
            &|offset| self.hash(Self::key_of(self.arena.get(*offset))),
            &|_| {},
            &mut |offset| {
                let body = self.arena.get(*offset);

                visit(Self::key_of(body), Self::value_of(body));
            },
        ))
    }

    /// Visits `take` entries drawn uniformly at random: distinct ones when `distinct`
    /// (all of them when `take` reaches the size), otherwise independent draws that may
    /// repeat. A draw picks a random bucket and retries an empty one, so every entry is
    /// equally likely; the retries are bounded and a run of misses falls back to the
    /// next occupied bucket. Cost follows `take`, plus one pass over the buckets when
    /// `take` is at least half the table.
    pub(super) fn sample(&self, take: usize, distinct: bool, mut visit: impl FnMut(&[u8], &[u8])) {
        const TRIES: usize = 64;

        let buckets = self.table.num_buckets();
        let len = self.table.len();

        if buckets == 0 || len == 0 || take == 0 {
            return;
        }

        let mut emit = |bucket: usize| {
            let offset = self.table.get_bucket(bucket).expect("occupied bucket");
            let body = self.arena.get(*offset);

            visit(Self::key_of(body), Self::value_of(body));
        };

        if distinct && take.saturating_mul(2) >= len {
            let occupied: Vec<usize> = (0..buckets)
                .filter(|&bucket| self.table.get_bucket(bucket).is_some())
                .collect();

            for at in distinct_positions(occupied.len(), take) {
                emit(occupied[at]);
            }

            return;
        }

        // Fewer than half of the entries are wanted when `distinct`, so a fresh pick
        // is never rarer than one occupied bucket in two.
        let mut picked = ahash::AHashSet::new();

        for _ in 0..take {
            let fresh = |bucket: usize, picked: &mut ahash::AHashSet<usize>| {
                self.table.get_bucket(bucket).is_some() && (!distinct || picked.insert(bucket))
            };
            let mut chosen = (0..TRIES)
                .map(|_| random_below(buckets))
                .find(|&bucket| fresh(bucket, &mut picked));

            if chosen.is_none() {
                let start = random_below(buckets);

                chosen = (0..buckets)
                    .map(|step| (start + step) % buckets)
                    .find(|&bucket| fresh(bucket, &mut picked));
            }

            match chosen {
                Some(bucket) => emit(bucket),
                None => return,
            }
        }
    }

    fn block_parts<'a>(key: &'a [u8], value: &'a [u8], header: &'a [u8]) -> [&'a [u8]; 3] {
        if PAIRED {
            [header, key, value]
        } else {
            [key, &[], &[]]
        }
    }

    /// Adds `key` with `value`, which the caller has checked is absent.
    pub(super) fn insert_new(&mut self, key: &[u8], value: &[u8]) -> Result<(), ArenaFull> {
        let (header, header_len) = encode_varint(key.len());
        let parts = Self::block_parts(key, value, &header[..header_len]);
        let offset = self.arena.alloc(&parts).ok_or(ArenaFull)?;
        let hash = self.hash(key);
        let (arena, hasher) = (&self.arena, &self.hasher);
        let was_migrating = self.table.is_migrating();

        self.table.insert_unique_paced(
            hash,
            offset,
            |stored| hasher.hash_one(Self::key_of(arena.get(*stored))),
            MIGRATE_PER_ELEMENT,
        );

        if self.table.is_migrating() && !was_migrating {
            note_work_started();
        }

        Ok(())
    }

    /// True when a new entry of these sizes still gets an offset.
    pub(super) fn can_insert(&self, key_len: usize, value_len: usize) -> bool {
        let body = if PAIRED {
            crate::arena::block_size(key_len) + value_len
        } else {
            key_len
        };

        self.arena.can_alloc(body)
    }

    /// Replaces the value of an existing paired entry; returns the old value length. `None`
    /// when the key is absent or the arena is full: check `can_insert` first.
    pub(super) fn replace_value(&mut self, key: &[u8], value: &[u8]) -> Option<usize> {
        debug_assert!(PAIRED);
        let hash = self.hash(key);
        let offset = self
            .table
            .find(hash, |offset| Self::key_of(self.arena.get(*offset)) == key)
            .copied()?;
        let old = Self::value_of(self.arena.get(offset)).len();

        if old == value.len() {
            let body = self.arena.get_mut(offset);
            let start = body.len() - old;

            body[start..].copy_from_slice(value);
            return Some(old);
        }

        let (header, header_len) = encode_varint(key.len());
        let parts = Self::block_parts(key, value, &header[..header_len]);
        let fresh = self.arena.alloc(&parts)?;

        self.arena.free(offset);

        if let Some(slot) = self.table.find_mut(hash, |stored| *stored == offset) {
            *slot = fresh;
        }

        self.compact_if_sparse();
        Some(old)
    }

    /// Removes `key`; returns the length of the removed value (0 for a set).
    pub(super) fn remove(&mut self, key: &[u8]) -> Option<usize> {
        let hash = self.hash(key);
        let offset = self
            .table
            .remove(hash, |offset| Self::key_of(self.arena.get(*offset)) == key)?;
        let value_len = Self::value_of(self.arena.get(offset)).len();

        self.arena.free(offset);
        self.compact_if_sparse();

        let (arena, hasher) = (&self.arena, &self.hasher);
        let hash_of = |stored: &u32| hasher.hash_one(Self::key_of(arena.get(*stored)));

        if self.table.is_migrating() {
            // Removals drive a resize under way too, so a mass delete finishes it.
            self.table.migrate(MIGRATE_PER_ELEMENT, &hash_of);
        } else if let Some(target) = reclaim_capacity(self.table.len(), self.table.capacity()) {
            self.table.begin_shrink(target, hash_of);

            if self.table.is_migrating() {
                note_work_started();
            }
        }

        Some(value_len)
    }

    /// Starts compacting a sparse arena, or moves the next slice of a compaction under
    /// way. A large arena is evacuated over many calls; a small one is rewritten now.
    fn compact_if_sparse(&mut self) {
        if self.arena.is_evacuating() {
            self.evacuate(RELOCATE_PER_WRITE, INSPECT_PER_WRITE);
        } else if self.arena.should_compact() {
            if self.arena.can_evacuate(EVACUATE_MIN_CHUNKS) {
                self.arena.begin_evacuation();
                self.evacuation_cursor = 0;
                self.evacuation_epoch = self.table.epoch();
                self.evacuate(RELOCATE_PER_WRITE, INSPECT_PER_WRITE);

                if self.arena.is_evacuating() {
                    note_work_started();
                }
            } else {
                self.arena.compact(self.table.iter_mut());
            }
        }
    }

    /// Moves up to `moves` live blocks out of the evacuated chunks, inspecting at most
    /// `slots` table positions, and releases the chunks once a whole pass found every
    /// block moved. Positions run over the draining table first, and a migration only
    /// moves offsets forward, so one pass meets every offset; a rebuild changes the
    /// epoch and starts the pass over.
    fn evacuate(&mut self, moves: usize, slots: usize) {
        if self.table.epoch() != self.evacuation_epoch {
            self.evacuation_epoch = self.table.epoch();
            self.evacuation_cursor = 0;
        }

        let buckets = if self.table.is_empty() {
            0
        } else {
            self.table.num_buckets()
        };
        let end = self.evacuation_cursor.saturating_add(slots).min(buckets);
        let mut moved = 0;

        while self.evacuation_cursor < end && moved < moves {
            if let Some(offset) = self.table.get_bucket_mut(self.evacuation_cursor)
                && self.arena.needs_relocation(*offset)
            {
                let Some(moved_to) = self.arena.relocate(*offset) else {
                    // No room after all: keep every block where it is and stop.
                    self.arena.abort_evacuation();
                    return;
                };

                *offset = moved_to;
                moved += 1;
            }

            self.evacuation_cursor += 1;
        }

        if self.evacuation_cursor >= buckets {
            self.arena.finish_evacuation();
        }
    }

    /// True while a resize or a compaction is unfinished.
    pub(super) fn has_background_work(&self) -> bool {
        self.table.is_migrating() || self.arena.is_evacuating()
    }

    /// Moves about `budget` offsets or blocks of unfinished resize and compaction work,
    /// for a collection that stopped receiving the writes that would drive it. Returns
    /// true when none is left.
    pub(super) fn background_step(&mut self, budget: usize) -> bool {
        if self.table.is_migrating() {
            let (arena, hasher) = (&self.arena, &self.hasher);

            self.table.migrate(budget, &|stored| {
                hasher.hash_one(Self::key_of(arena.get(*stored)))
            });
        } else if self.arena.is_evacuating() {
            self.evacuate(budget, budget.saturating_mul(16));
        }

        !self.has_background_work()
    }

    /// Table and arena capacity in bytes, payload included.
    pub(super) fn capacity_bytes(&self) -> usize {
        self.arena
            .capacity()
            .saturating_add(self.table.capacity().saturating_mul(size_of::<u32>() + 1))
    }
}

impl<const PAIRED: bool> PartialEq for ArenaTable<PAIRED> {
    fn eq(&self, other: &Self) -> bool {
        self.len() == other.len()
            && self
                .iter()
                .all(|(key, value)| other.get(key) == Some(value))
    }
}

impl<const PAIRED: bool> Eq for ArenaTable<PAIRED> {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn set_table_inserts_removes_and_compacts() {
        let mut table = ArenaTable::<false>::new();
        // Miri interprets every instruction; a few hundred members still cross several growths.
        let count: u32 = if cfg!(miri) { 200 } else { 5_000 };

        for index in 0..count {
            table
                .insert_new(format!("member-{index}").as_bytes(), &[])
                .unwrap();
        }

        for index in (0..count).filter(|index| index % 4 != 0) {
            assert_eq!(table.remove(format!("member-{index}").as_bytes()), Some(0));
        }

        assert_eq!(table.len(), count as usize / 4);

        for index in 0..count {
            assert_eq!(
                table.contains(format!("member-{index}").as_bytes()),
                index % 4 == 0
            );
        }
    }

    fn member(index: u32) -> Vec<u8> {
        format!("member-{index:08}-padding").into_bytes()
    }

    fn has_all(table: &ArenaTable<false>, live: &std::collections::BTreeSet<u32>) -> bool {
        live.iter().all(|index| table.contains(&member(*index)))
    }

    #[test]
    fn growth_is_incremental_and_every_member_stays_reachable() {
        let mut table = ArenaTable::<false>::new();
        let mut live = std::collections::BTreeSet::new();
        let mut migrating = 0;

        for index in 0..20_000u32 {
            table.insert_new(&member(index), &[]).unwrap();
            live.insert(index);

            if table.table.is_migrating() {
                migrating += 1;
                assert!(table.contains(&member(index / 2)));
            }
        }

        assert!(migrating > 0, "growth was never incremental");
        assert!(has_all(&table, &live));
        assert!(background_work_started());
        clear_background_work_started();
    }

    #[test]
    fn mass_removal_shrinks_and_compacts_in_bounded_slices() {
        let mut table = ArenaTable::<false>::new();
        let count = 40_000u32;

        for index in 0..count {
            table.insert_new(&member(index), &[]).unwrap();
        }
        while !table.background_step(usize::MAX) {}

        let mut live: std::collections::BTreeSet<u32> = (0..count).collect();
        let (mut saw_shrink, mut saw_evacuation) = (false, false);
        let mut next = count;

        for index in 0..count - 1_000 {
            let chunks_before = table.arena.capacity();
            let evacuating = table.arena.is_evacuating();
            // Spread over the arena, so chunks stay partly live and only a compaction
            // gives their holes back (wholly dead chunks are released without one).
            let victim = index * 7_919 % count;

            assert_eq!(table.remove(&member(victim)), Some(0));
            live.remove(&victim);

            // A slice copies at most RELOCATE_PER_WRITE blocks into fresh chunks.
            if evacuating && table.arena.is_evacuating() {
                let grown = table.arena.capacity().saturating_sub(chunks_before);
                assert!(grown <= 64 * 1024 + RELOCATE_PER_WRITE * 64, "grew {grown}");
            }

            saw_shrink |= table.table.is_migrating();
            saw_evacuation |= table.arena.is_evacuating();

            // Inserts interleave with the background work.
            if index % 7 == 0 {
                table.insert_new(&member(next), &[]).unwrap();
                live.insert(next);
                next += 1;
            }
            if index % 997 == 0 {
                assert!(has_all(&table, &live));
            }
        }

        assert!(saw_shrink, "the table shrank in one step");
        assert!(saw_evacuation, "the arena compacted in one step");
        assert!(has_all(&table, &live));
        assert_eq!(table.len(), live.len());

        let mut steps = 0;
        while !table.background_step(64) {
            steps += 1;
            assert!(has_all(&table, &live) || steps % 50 != 0);
        }
        assert!(has_all(&table, &live));
        // Memory follows the live members once the work is done.
        assert!(table.capacity_bytes() < 64 * live.len() + 256 * 1024);
        clear_background_work_started();
    }

    #[test]
    fn a_scan_returns_every_stable_member_while_work_runs() {
        let mut table = ArenaTable::<false>::new();

        for index in 0..30_000u32 {
            table.insert_new(&member(index), &[]).unwrap();
        }
        // Stop removing while a resize or a compaction is under way.
        let mut stop = 2_000u32;
        while stop < 22_000 || !table.has_background_work() {
            table.remove(&member(stop)).unwrap();
            stop += 1;
        }

        let stable: std::collections::BTreeSet<u32> = (0..2_000).chain(stop..30_000).collect();
        let mut seen = std::collections::BTreeSet::new();
        let mut cursor = 0;
        let mut next = 100_000u32;
        let mut busy = false;

        loop {
            busy |= table.has_background_work();
            cursor = table.scan_buckets(cursor, 20, |key, _| {
                seen.insert(key.to_vec());
            });
            if cursor == 0 {
                break;
            }
            table.insert_new(&member(next), &[]).unwrap();
            table.remove(&member(next)).unwrap();
            next += 1;
            table.background_step(16);
        }

        assert!(busy);
        assert!(stable.iter().all(|index| seen.contains(&member(*index))));
        clear_background_work_started();
    }

    #[derive(Debug, Clone)]
    enum Op {
        Insert(u16),
        Remove(u16),
        Replace(u16, u8),
        Step(u8),
    }

    use proptest::prelude::*;

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(48))]
        #[test]
        fn paired_table_matches_a_map_through_resizes_and_compaction(
            ops in prop::collection::vec(
                prop_oneof![
                    5 => (0u16..6_000).prop_map(Op::Insert),
                    5 => (0u16..6_000).prop_map(Op::Remove),
                    2 => (0u16..6_000, 0u8..60).prop_map(|(k, n)| Op::Replace(k, n)),
                    1 => (1u8..100).prop_map(Op::Step),
                ],
                1..6_000,
            )
        ) {
            let mut table = ArenaTable::<true>::new();
            let mut model = std::collections::BTreeMap::<Vec<u8>, Vec<u8>>::new();

            for op in ops {
                match op {
                    Op::Insert(key) => {
                        let field = format!("field-{key}").into_bytes();
                        if let std::collections::btree_map::Entry::Vacant(slot) = model.entry(field) {
                            table.insert_new(slot.key(), b"v").unwrap();
                            slot.insert(b"v".to_vec());
                        }
                    }
                    Op::Remove(key) => {
                        let field = format!("field-{key}").into_bytes();
                        prop_assert_eq!(table.remove(&field).is_some(), model.remove(&field).is_some());
                    }
                    Op::Replace(key, len) => {
                        let field = format!("field-{key}").into_bytes();
                        let value = vec![b'x'; usize::from(len)];
                        if let Some(stored) = model.get_mut(&field) {
                            prop_assert!(table.replace_value(&field, &value).is_some());
                            *stored = value;
                        }
                    }
                    Op::Step(budget) => {
                        table.background_step(usize::from(budget));
                    }
                }
                prop_assert_eq!(table.len(), model.len());
            }

            for (field, value) in &model {
                prop_assert_eq!(table.get(field), Some(&value[..]));
            }
            while !table.background_step(32) {}
            for (field, value) in &model {
                prop_assert_eq!(table.get(field), Some(&value[..]));
            }
            prop_assert_eq!(table.iter().count(), model.len());
            clear_background_work_started();
        }
    }

    #[test]
    fn paired_table_replaces_values_in_place_and_by_move() {
        let mut table = ArenaTable::<true>::new();
        let count: u32 = if cfg!(miri) { 150 } else { 3_000 };

        for index in 0..count {
            table
                .insert_new(format!("f{index}").as_bytes(), b"value")
                .unwrap();
        }

        for index in 0..count {
            let field = format!("f{index}");

            if index % 2 == 0 {
                assert_eq!(table.replace_value(field.as_bytes(), b"VALUE"), Some(5));
            } else {
                assert_eq!(table.replace_value(field.as_bytes(), &[9; 40]), Some(5));
            }
        }

        for index in 0..count {
            let expected: &[u8] = if index % 2 == 0 { b"VALUE" } else { &[9; 40] };

            assert_eq!(table.get(format!("f{index}").as_bytes()), Some(expected));
        }
    }
}
