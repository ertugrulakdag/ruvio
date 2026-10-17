//! Slot table that grows and shrinks without a stop-the-world rehash.
//!
//! A hashbrown table that runs out of room allocates twice the buckets and moves every
//! element before the insert returns; at 10M keys that is more than a second on the shard
//! thread, and shrinking after a mass delete costs the same. `Slots` instead allocates the
//! new table, keeps the full one as `old`, and moves a bounded number of elements per
//! insert and per maintenance step. Lookups try the new table first and fall back to the
//! old one. Elements are small copyable handles (key-log addresses, record ids, arena
//! offsets), so a move is a rehash of the key plus one slot write.

use crate::hashbrown::{HashTable, scan_cursor_next};
use crate::index::Addr;

/// Slots moved per insert while a migration is running.
const MIGRATE_PER_INSERT: usize = 64;
/// Slots moved per removal, and per insert into a collection, while a migration runs.
/// Commands remove (or add) up to thousands of elements at once, so this stays small;
/// maintenance moves the rest.
pub(crate) const MIGRATE_PER_ELEMENT: usize = 8;
/// Tables smaller than this resize in place: the rehash is cheaper than the bookkeeping.
const INCREMENTAL_MIN_BUCKETS: usize = if cfg!(test) { 32 } else { 1 << 14 };
/// Empty or tombstoned old slots a migration step may pass per element it moves, so a
/// step over a sparse old table stays bounded too.
const SKIP_PER_MOVE: usize = 16;

#[derive(Debug, Clone)]
struct Draining<T> {
    table: HashTable<T>,
    /// Next bucket of `table` to move.
    cursor: usize,
}

#[derive(Debug, Clone)]
pub(crate) struct Slots<T = Addr> {
    table: HashTable<T>,
    old: Option<Box<Draining<T>>>,
    /// Counts events that move elements between slots; see `epoch`.
    events: u32,
}

impl<T: Copy> Slots<T> {
    pub(crate) fn with_capacity(capacity: usize) -> Self {
        Self {
            table: HashTable::with_capacity(capacity),
            old: None,
            events: 0,
        }
    }

    /// Hints the CPU to load where a lookup of `hash` starts in the current table.
    #[inline]
    pub(crate) fn prefetch(&self, hash: u64) {
        self.table.prefetch(hash);
    }

    #[inline]
    pub(crate) fn find(&self, hash: u64, mut eq: impl FnMut(&T) -> bool) -> Option<&T> {
        if let Some(found) = self.table.find(hash, &mut eq) {
            return Some(found);
        }

        match &self.old {
            None => None,
            Some(old) => old.table.find(hash, eq),
        }
    }

    /// Replaces the stored address of an existing key. Returns whether it was found.
    pub(crate) fn replace(
        &mut self,
        hash: u64,
        mut eq: impl FnMut(&T) -> bool,
        address: T,
    ) -> bool {
        if let Some(slot) = self.table.find_mut(hash, &mut eq) {
            *slot = address;
            return true;
        }

        if let Some(old) = &mut self.old
            && let Some(slot) = old.table.find_mut(hash, eq)
        {
            *slot = address;
            return true;
        }

        false
    }

    pub(crate) fn remove(&mut self, hash: u64, mut eq: impl FnMut(&T) -> bool) -> Option<T> {
        if let Ok(occupied) = self.table.find_entry(hash, &mut eq) {
            return Some(occupied.remove().0);
        }

        let old = self.old.as_mut()?;
        let removed = old.table.find_entry(hash, eq).ok()?.remove().0;

        if old.table.is_empty() {
            self.finish_drain();
        }

        Some(removed)
    }

    /// Inserts an address whose key is not in the table.
    pub(crate) fn insert_unique(&mut self, hash: u64, address: T, hasher: impl Fn(&T) -> u64) {
        self.insert_unique_paced(hash, address, hasher, MIGRATE_PER_INSERT);
    }

    /// `insert_unique` that moves up to `budget` slots of a running migration.
    pub(crate) fn insert_unique_paced(
        &mut self,
        hash: u64,
        address: T,
        hasher: impl Fn(&T) -> u64,
        budget: usize,
    ) {
        if self.old.is_some() {
            self.migrate(budget, &hasher);
        }

        // A full table with real contents must grow. A full table that is mostly
        // tombstones is left to hashbrown, which rehashes in place or reuses one.
        if self.table.growth_left() == 0 && self.table.len() > self.table.full_capacity() / 2 {
            if self.old.is_some() {
                // The new table filled before the migration ended: finish it, then grow.
                self.migrate(usize::MAX, &hasher);
            }

            if self.table.allocated_buckets() >= INCREMENTAL_MIN_BUCKETS {
                self.begin_growth();
            }
        }

        self.table.insert_unique(hash, address, hasher);
    }

    fn begin_growth(&mut self) {
        let wanted = self.table.full_capacity().saturating_add(1);
        let grown = HashTable::with_capacity(wanted);
        let full = std::mem::replace(&mut self.table, grown);

        self.events = self.events.wrapping_add(full.epoch()).wrapping_add(1);
        self.old = Some(Box::new(Draining {
            table: full,
            cursor: 0,
        }));
    }

    /// Moves up to `budget` elements from the old table. Returns true when no
    /// migration is left.
    pub(crate) fn migrate(&mut self, budget: usize, hasher: &impl Fn(&T) -> u64) -> bool {
        let Some(old) = self.old.as_mut() else {
            return true;
        };

        let buckets = old.table.num_buckets();
        let end = old
            .cursor
            .saturating_add(budget.saturating_mul(SKIP_PER_MOVE))
            .min(buckets);
        let mut moved = 0usize;

        while old.cursor < end && moved < budget && !old.table.is_empty() {
            if let Ok(occupied) = old.table.get_bucket_entry(old.cursor) {
                let address = occupied.remove().0;
                let hash = hasher(&address);

                self.table.insert_unique(hash, address, hasher);
                moved += 1;
            }

            old.cursor += 1;
        }

        // The epoch stays put: a migration only moves elements from the old table, which
        // positions first, into the new one, which positions after it. A walk in position
        // order therefore still meets every element it had not yet passed.
        if old.cursor >= buckets || old.table.is_empty() {
            self.finish_drain();
            return true;
        }

        false
    }

    fn finish_drain(&mut self) {
        if let Some(old) = self.old.take() {
            debug_assert!(old.table.is_empty());
            self.events = self.events.wrapping_add(old.table.epoch()).wrapping_add(1);
        }
    }

    pub(crate) fn is_migrating(&self) -> bool {
        self.old.is_some()
    }

    pub(crate) fn len(&self) -> usize {
        self.table.len() + self.old.as_ref().map_or(0, |old| old.table.len())
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Slot positions across both tables: the draining table first, then the one taking
    /// inserts.
    pub(crate) fn num_buckets(&self) -> usize {
        self.table.num_buckets() + self.old.as_ref().map_or(0, |old| old.table.num_buckets())
    }

    pub(crate) fn get_bucket(&self, slot: usize) -> Option<&T> {
        let old = self.old.as_ref().map_or(0, |old| old.table.num_buckets());

        if slot < old {
            self.old.as_ref()?.table.get_bucket(slot)
        } else {
            self.table.get_bucket(slot - old)
        }
    }

    /// Writable element at a `get_bucket` position. Rewriting an address in place keeps
    /// the slot, so the epoch does not change.
    pub(crate) fn get_bucket_mut(&mut self, slot: usize) -> Option<&mut T> {
        let old = self.old.as_ref().map_or(0, |old| old.table.num_buckets());

        if slot < old {
            self.old.as_mut()?.table.get_bucket_mut(slot)
        } else {
            self.table.get_bucket_mut(slot - old)
        }
    }

    /// One page of a keyspace scan, Redis `dictScan` style: the cursor walks blocks of
    /// home buckets in reverse-binary order (see `RawTable::scan_block`). While a
    /// migration runs, a block of the smaller table is read together with every block
    /// of the larger table it splits into, so an element is met wherever it sits at
    /// that moment. A key present for the whole walk is visited at least once through
    /// growth, migration, shrinking and tombstone reclaim (a resize may repeat some),
    /// and the walk ends after one pass. Stops after a block once `keys` elements were
    /// visited or `slots` slots inspected; returns the next cursor, 0 when done.
    pub(crate) fn scan(
        &self,
        cursor: u64,
        keys: usize,
        slots: usize,
        hash_of: &dyn Fn(&T) -> u64,
        prefetch: &dyn Fn(&T),
        visit: &mut dyn FnMut(&T),
    ) -> u64 {
        if self.is_empty() {
            return 0;
        }

        let mut cursor = cursor;
        let found = std::cell::Cell::new(0usize);
        let mut inspected = 0usize;
        let mut count = |address: &T| {
            found.set(found.get() + 1);
            visit(address);
        };

        loop {
            match &self.old {
                None => {
                    inspected += self.table.scan_block(cursor, hash_of, prefetch, &mut count);
                    cursor = scan_cursor_next(cursor, self.table.scan_select_mask());
                }
                Some(old) => {
                    let (small, large) = if old.table.num_buckets() <= self.table.num_buckets() {
                        (&old.table, &self.table)
                    } else {
                        (&self.table, &old.table)
                    };
                    let narrow = small.scan_select_mask();
                    let wide = large.scan_select_mask();

                    inspected += small.scan_block(cursor, hash_of, prefetch, &mut count);

                    // The larger table's blocks that share the smaller one's selected bits.
                    loop {
                        inspected += large.scan_block(cursor, hash_of, prefetch, &mut count);
                        cursor = scan_cursor_next(cursor, wide);

                        if cursor & (narrow ^ wide) == 0 {
                            break;
                        }
                    }
                }
            }

            if cursor == 0 || found.get() >= keys || inspected >= slots {
                return cursor;
            }
        }
    }

    /// Changes whenever elements may have changed slots, so a saved slot position is
    /// only valid within one epoch.
    pub(crate) fn epoch(&self) -> u32 {
        self.events.wrapping_add(self.table.epoch())
    }

    pub(crate) fn iter(&self) -> impl Iterator<Item = &T> {
        self.table
            .iter()
            .chain(self.old.iter().flat_map(|old| old.table.iter()))
    }

    pub(crate) fn into_iter(self) -> impl Iterator<Item = T> {
        self.table
            .into_iter()
            .chain(self.old.into_iter().flat_map(|old| old.table.into_iter()))
    }

    /// Bytes-relevant bucket count of the table that takes new inserts.
    #[cfg(test)]
    pub(crate) fn allocated_buckets(&self) -> usize {
        self.table.allocated_buckets()
    }

    pub(crate) fn full_capacity(&self) -> usize {
        self.table.full_capacity()
    }

    pub(crate) fn growth_left(&self) -> usize {
        self.table.growth_left()
    }

    /// Active-table length, for the tombstone heuristics.
    pub(crate) fn active_len(&self) -> usize {
        self.table.len()
    }

    pub(crate) fn reclaim_tombstones(&mut self, hasher: impl Fn(&T) -> u64) {
        self.table.reclaim_tombstones(hasher);
        self.events = self.events.wrapping_add(1);
    }

    pub(crate) fn shrink_to(&mut self, min_capacity: usize, hasher: impl Fn(&T) -> u64) {
        self.table.shrink_to(min_capacity, hasher);
        self.events = self.events.wrapping_add(1);
    }

    /// Shrinks to hold at least `min_capacity` elements. A small table is rebuilt at
    /// once; a large one becomes the draining `old` table behind a smaller one, and
    /// `migrate` (driven by inserts, removals and maintenance) moves its elements in
    /// bounded steps, so a mass delete never rehashes the whole table in one call.
    /// Call only when no migration is running.
    pub(crate) fn begin_shrink(&mut self, min_capacity: usize, hasher: impl Fn(&T) -> u64) {
        debug_assert!(self.old.is_none());

        if self.old.is_some()
            || self.table.is_empty()
            || self.table.allocated_buckets() < INCREMENTAL_MIN_BUCKETS
        {
            self.shrink_to(min_capacity, hasher);
            return;
        }

        let smaller = HashTable::with_capacity(min_capacity.max(self.table.len()));

        if smaller.allocated_buckets() >= self.table.allocated_buckets() {
            return;
        }

        let full = std::mem::replace(&mut self.table, smaller);

        self.events = self.events.wrapping_add(full.epoch()).wrapping_add(1);
        self.old = Some(Box::new(Draining {
            table: full,
            cursor: 0,
        }));
    }

    /// Starts the epoch at `seed`, so a cursor issued by another table (a collection
    /// rebuilt under the same key) does not match this one.
    pub(crate) fn seed_epoch(&mut self, seed: u32) {
        self.events = seed.wrapping_sub(self.table.epoch());
    }

    #[inline]
    pub(crate) fn find_mut(&mut self, hash: u64, mut eq: impl FnMut(&T) -> bool) -> Option<&mut T> {
        if self.old.is_none() {
            return self.table.find_mut(hash, eq);
        }

        if let Some(found) = self.table.find_mut(hash, &mut eq) {
            return Some(found);
        }

        self.old.as_mut()?.table.find_mut(hash, eq)
    }

    pub(crate) fn iter_mut(&mut self) -> impl Iterator<Item = &mut T> {
        self.table
            .iter_mut()
            .chain(self.old.iter_mut().flat_map(|old| old.table.iter_mut()))
    }

    /// Buckets of every allocated table, the draining one included: what memory
    /// charges count, so a resize under way is charged for both tables.
    pub(crate) fn all_allocated_buckets(&self) -> usize {
        self.table.allocated_buckets()
            + self
                .old
                .as_ref()
                .map_or(0, |old| old.table.allocated_buckets())
    }

    /// Element capacity of every allocated table, the draining one included.
    pub(crate) fn capacity(&self) -> usize {
        self.table.capacity() + self.old.as_ref().map_or(0, |old| old.table.capacity())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    use std::collections::BTreeSet;

    /// Miri interprets every instruction, so it runs these on tables a fortieth the size. They
    /// still cross the incremental-resize threshold, which is 32 buckets in tests.
    const fn scaled(count: u32) -> u32 {
        if cfg!(miri) { count / 40 } else { count }
    }

    fn hash(address: &Addr) -> u64 {
        // Any bijective mix will do; the table only needs spread.
        let mut x = u64::from(address.0).wrapping_add(0x9e37_79b9_7f4a_7c15);

        x = (x ^ (x >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        x = (x ^ (x >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        x ^ (x >> 31)
    }

    fn find(slots: &Slots, id: u32) -> bool {
        slots.find(hash(&Addr(id)), |a| a.0 == id).is_some()
    }

    #[derive(Debug, Clone)]
    enum Op {
        Insert(u32),
        Remove(u32),
        Migrate(u8),
        Shrink,
    }

    fn ops() -> impl Strategy<Value = Vec<Op>> {
        prop::collection::vec(
            prop_oneof![
                6 => (0u32..scaled(3_000)).prop_map(Op::Insert),
                3 => (0u32..scaled(3_000)).prop_map(Op::Remove),
                1 => (1u8..40).prop_map(Op::Migrate),
                1 => Just(Op::Shrink),
            ],
            1..scaled(1_500) as usize,
        )
    }

    proptest! {
        #[test]
        fn behaves_like_a_set_across_incremental_growth_and_shrink(ops in ops()) {
            let mut slots = Slots::with_capacity(0);
            let mut model = BTreeSet::new();
            let mut saw_migration = false;

            for op in ops {
                match op {
                    Op::Insert(id) => {
                        if model.insert(id) {
                            slots.insert_unique(hash(&Addr(id)), Addr(id), hash);
                        }
                    }
                    Op::Remove(id) => {
                        let removed = slots.remove(hash(&Addr(id)), |a| a.0 == id);

                        prop_assert_eq!(removed.is_some(), model.remove(&id));
                    }
                    Op::Migrate(budget) => {
                        slots.migrate(usize::from(budget), &hash);
                    }
                    Op::Shrink if !slots.is_migrating() => {
                        slots.begin_shrink(slots.len() * 2, hash);
                    }
                    Op::Shrink => {}
                }

                saw_migration |= slots.is_migrating();
                prop_assert_eq!(slots.len(), model.len());
            }

            let mut seen: Vec<u32> = slots.iter().map(|a| a.0).collect();

            seen.sort_unstable();
            prop_assert_eq!(seen, model.iter().copied().collect::<Vec<_>>());

            for id in 0..scaled(3_000) {
                prop_assert_eq!(find(&slots, id), model.contains(&id), "id {}", id);
            }

            while !slots.migrate(64, &hash) {}
            prop_assert!(!slots.is_migrating());
            prop_assert_eq!(slots.len(), model.len());
            let _ = saw_migration;
        }
    }

    #[test]
    fn growth_is_incremental_and_every_key_stays_reachable() {
        let mut slots = Slots::with_capacity(0);
        let mut migrating_steps = 0usize;
        let count = scaled(50_000);

        for id in 0..count {
            slots.insert_unique(hash(&Addr(id)), Addr(id), hash);

            if slots.is_migrating() {
                migrating_steps += 1;
                // A key from before and after the growth point is found mid-migration.
                assert!(find(&slots, id));
                assert!(find(&slots, id / 2));
            }
        }

        assert!(migrating_steps > 0, "growth never became incremental");
        while !slots.migrate(1_000, &hash) {}
        assert_eq!(slots.len(), count as usize);
        assert!((0..count).all(|id| find(&slots, id)));
    }

    #[test]
    fn a_walk_in_slot_order_meets_every_element_while_migration_runs() {
        let mut slots = Slots::with_capacity(0);

        for id in 0..40u32 {
            slots.insert_unique(hash(&Addr(id)), Addr(id), hash);
        }

        // Grow until a migration is in flight, then walk while it keeps moving elements.
        let mut next = 40u32;
        while !slots.is_migrating() {
            slots.insert_unique(hash(&Addr(next)), Addr(next), hash);
            next += 1;
        }

        let original = next;
        let epoch = slots.epoch();
        let mut seen = BTreeSet::new();
        let mut slot = 0;

        while slot < slots.num_buckets() {
            if let Some(addr) = slots.get_bucket(slot) {
                seen.insert(addr.0);
            }

            slot += 1;

            if slots.is_migrating() && slots.epoch() == epoch {
                slots.insert_unique(hash(&Addr(next)), Addr(next), hash);
                next += 1;
            }
        }

        assert!(
            (0..original).all(|id| seen.contains(&id)),
            "walk missed an element that was present throughout"
        );
    }

    #[test]
    #[cfg_attr(
        miri,
        ignore = "needs tables too large for Miri; the other slot tests run these paths"
    )]
    fn a_scan_ends_and_meets_every_stable_element_while_the_tables_change() {
        for seed in 1..=24u64 {
            let mut rng = seed.wrapping_mul(0x9e37_79b9_7f4a_7c15) | 1;
            let mut random = move || {
                rng ^= rng << 13;
                rng ^= rng >> 7;
                rng ^= rng << 17;
                rng
            };
            let mut slots = Slots::with_capacity(0);
            let stable = 2_000u32;
            let mut churn: Vec<u32> = Vec::new();
            let mut next = 1_000_000u32;

            for id in 0..stable {
                slots.insert_unique(hash(&Addr(id)), Addr(id), hash);
            }

            let mut seen = BTreeSet::new();
            let (mut cursor, mut pages, mut changes) = (0u64, 0usize, BTreeSet::new());

            loop {
                cursor = slots.scan(cursor, 40, 400, &hash, &|_| {}, &mut |addr| {
                    seen.insert(addr.0);
                });
                pages += 1;
                assert!(pages < 5_000, "seed {seed}: the scan did not end");

                if cursor == 0 {
                    break;
                }

                // Grow (starting migrations), shrink, drain, rehash in place.
                match random() % 8 {
                    0..=2 => {
                        for _ in 0..random() % 1_500 {
                            slots.insert_unique(hash(&Addr(next)), Addr(next), hash);
                            churn.push(next);
                            next += 1;
                        }
                        changes.insert("grow");
                    }
                    3 | 4 => {
                        let keep = churn.len() / 4;
                        for id in churn.drain(keep..) {
                            slots.remove(hash(&Addr(id)), |a| a.0 == id).unwrap();
                        }
                        changes.insert("remove");
                    }
                    5 => {
                        slots.migrate(1 + random() as usize % 200, &hash);
                        changes.insert("migrate");
                    }
                    6 if !slots.is_migrating() => {
                        if random() % 2 == 0 {
                            slots.shrink_to(0, hash);
                        } else {
                            slots.begin_shrink(slots.len() * 2, hash);
                        }
                        changes.insert("shrink");
                    }
                    _ => {
                        slots.reclaim_tombstones(hash);
                        changes.insert("reclaim");
                    }
                }
                if slots.is_migrating() {
                    changes.insert("migrating");
                }
            }

            let missed: Vec<u32> = (0..stable).filter(|id| !seen.contains(id)).collect();
            assert!(
                missed.is_empty(),
                "seed {seed}: missed {} elements",
                missed.len()
            );
            assert!(
                changes.len() >= 5,
                "seed {seed}: too little churn: {changes:?}"
            );
        }
    }

    #[test]
    #[cfg_attr(
        miri,
        ignore = "needs tables too large for Miri; the other slot tests run these paths"
    )]
    fn shrink_is_incremental_and_every_key_stays_reachable() {
        let mut slots = Slots::with_capacity(0);
        let (count, removed) = (40_000, 36_000);

        for id in 0..count {
            slots.insert_unique(hash(&Addr(id)), Addr(id), hash);
        }
        while !slots.migrate(usize::MAX, &hash) {}
        for id in 0..removed {
            slots.remove(hash(&Addr(id)), |a| a.0 == id).unwrap();
        }

        let before = slots.table.allocated_buckets();
        slots.begin_shrink(slots.len() * 2, hash);
        assert!(slots.is_migrating(), "a large table shrank in one step");
        assert!(slots.table.allocated_buckets() < before);

        let mut live: BTreeSet<u32> = (removed..count).collect();
        let mut next = 100_000u32;
        let mut steps = 0;

        while slots.is_migrating() {
            let old_len = slots.old.as_ref().map_or(0, |old| old.table.len());
            let old_cursor = slots.old.as_ref().map_or(0, |old| old.cursor);

            slots.migrate(8, &hash);

            // Each step moves at most its budget and passes a bounded run of slots.
            if let Some(old) = &slots.old {
                assert!(old_len - old.table.len() <= 8);
                assert!(old.cursor - old_cursor <= 8 * SKIP_PER_MOVE);
            }

            // Inserts and deletes interleave with the migration.
            if steps % 3 == 0 {
                slots.insert_unique(hash(&Addr(next)), Addr(next), hash);
                live.insert(next);
                next += 1;
            }
            if steps % 5 == 0 {
                let gone = *live.iter().next().unwrap();
                slots.remove(hash(&Addr(gone)), |a| a.0 == gone).unwrap();
                live.remove(&gone);
            }

            assert!(live.iter().all(|id| find(&slots, *id)));
            assert_eq!(slots.len(), live.len());
            steps += 1;
        }

        assert!(steps > 10, "the shrink finished in {steps} steps");
        assert!(slots.table.allocated_buckets() * 4 <= before);
        assert!(live.iter().all(|id| find(&slots, *id)));
    }

    #[test]
    fn a_scan_meets_every_element_while_a_shrink_runs() {
        let mut slots = Slots::with_capacity(0);
        let (count, kept) = (scaled(20_000), scaled(2_000));

        for id in 0..count {
            slots.insert_unique(hash(&Addr(id)), Addr(id), hash);
        }
        while !slots.migrate(usize::MAX, &hash) {}
        for id in kept..count {
            slots.remove(hash(&Addr(id)), |a| a.0 == id).unwrap();
        }
        slots.begin_shrink(slots.len() * 2, hash);
        assert!(slots.is_migrating());

        let mut seen = Vec::new();
        let mut cursor = 0;

        loop {
            cursor = slots.scan(cursor, 10, 100, &hash, &|_| {}, &mut |addr| {
                seen.push(addr.0);
            });
            slots.migrate(5, &hash);

            if cursor == 0 {
                break;
            }
        }

        let unique: BTreeSet<u32> = seen.iter().copied().collect();
        assert_eq!(unique, (0..kept).collect::<BTreeSet<_>>());
    }

    #[test]
    fn epoch_changes_when_the_tables_are_rebuilt() {
        let mut slots = Slots::with_capacity(0);
        let mut epochs = BTreeSet::new();

        for id in 0..scaled(4_000) {
            slots.insert_unique(hash(&Addr(id)), Addr(id), hash);
            epochs.insert(slots.epoch());
        }

        assert!(epochs.len() > 2);
    }
}
