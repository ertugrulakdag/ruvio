//! An ordered map keyed by stream id, packed for memory.
//!
//! Rows sit in sorted runs of at most `RUN` rows; a deque of runs keeps them in id order.
//! Finding a row is a binary search over the runs' first ids and then inside one run, and
//! inserting or removing moves at most one run's rows. Ids delivered in order (the
//! common case) fill each run completely, so a row costs little more than its own bytes,
//! where a B-tree node keeps a third of its slots empty and adds a node header per few
//! rows. Runs that shrink are merged with a neighbour, so acknowledging out of order
//! cannot leave many nearly empty runs behind.

use std::collections::VecDeque;
use std::ops::Bound;

pub(crate) type Id = (u64, u64);

/// Most rows one run holds.
const RUN: usize = 32;

/// Gives back slack once a run holds less than three quarters of its capacity, keeping a
/// quarter of its length spare so removals in a row do not reallocate each time.
fn shrink<T>(rows: &mut Vec<T>) {
    let len = rows.len();

    if 4 * len < 3 * rows.capacity() {
        rows.shrink_to(len + len / 4);
    }
}

/// Makes room for one more row with at most half the run's length as slack (and never past
/// `RUN`), where `Vec` would double and leave split runs half empty.
fn grow<T>(rows: &mut Vec<T>) {
    if rows.len() == rows.capacity() {
        let len = rows.len();

        rows.reserve_exact((len / 2).max(4).min(RUN - len));
    }
}

#[derive(Clone, Debug)]
pub(crate) struct IdMap<V> {
    /// Non-empty sorted runs; every id of a run is below every id of the next one.
    runs: VecDeque<Run<V>>,
    len: usize,
}

/// A run with a copy of its first id beside it, so the search over runs reads one
/// contiguous array instead of following a pointer into every run it probes.
#[derive(Clone, Debug)]
struct Run<V> {
    first: Id,
    rows: Vec<(Id, V)>,
}

impl<V> Run<V> {
    fn new(rows: Vec<(Id, V)>) -> Self {
        Self {
            first: rows[0].0,
            rows,
        }
    }
}

impl<V> Default for IdMap<V> {
    fn default() -> Self {
        Self {
            runs: VecDeque::new(),
            len: 0,
        }
    }
}

impl<V: PartialEq> PartialEq for IdMap<V> {
    /// Same rows in the same order, however they are split into runs.
    fn eq(&self, other: &Self) -> bool {
        self.len == other.len && self.iter().eq(other.iter())
    }
}

impl<V: Eq> Eq for IdMap<V> {}

impl<V> IdMap<V> {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) fn len(&self) -> usize {
        self.len
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// The run that holds `id` or would take it: the last run starting at or below `id`,
    /// or the first run when `id` is below them all.
    fn run_of(&self, id: &Id) -> usize {
        self.runs
            .partition_point(|run| run.first <= *id)
            .saturating_sub(1)
    }

    fn find(&self, id: &Id) -> Option<(usize, usize)> {
        let run = self.run_of(id);
        let rows = &self.runs.get(run)?.rows;

        rows.binary_search_by(|row| row.0.cmp(id))
            .ok()
            .map(|index| (run, index))
    }

    pub(crate) fn get(&self, id: &Id) -> Option<&V> {
        self.find(id)
            .map(|(run, index)| &self.runs[run].rows[index].1)
    }

    pub(crate) fn contains_key(&self, id: &Id) -> bool {
        self.find(id).is_some()
    }

    /// Inserts or replaces the row of `id`; returns the replaced value.
    pub(crate) fn insert(&mut self, id: Id, value: V) -> Option<V> {
        if self.runs.is_empty() {
            self.runs.push_back(Run::new(vec![(id, value)]));
            self.len = 1;
            return None;
        }

        let run = self.run_of(&id);
        let last = run + 1 == self.runs.len();
        let Run { first, rows } = &mut self.runs[run];
        let index = match rows.binary_search_by(|row| row.0.cmp(&id)) {
            Ok(index) => return Some(std::mem::replace(&mut rows[index].1, value)),
            Err(index) => index,
        };

        self.len += 1;

        if rows.len() < RUN {
            grow(rows);
            rows.insert(index, (id, value));
            *first = rows[0].0;
        } else if index == RUN && last {
            // Past the end of the last full run: ids arriving in order start a new run,
            // so earlier runs stay full.
            self.runs.push_back(Run::new(vec![(id, value)]));
        } else {
            let mut upper = Vec::with_capacity(RUN / 2 + RUN / 4);

            upper.extend(rows.drain(RUN / 2..));
            if index <= RUN / 2 {
                rows.insert(index, (id, value));
                *first = rows[0].0;
            } else {
                upper.insert(index - RUN / 2, (id, value));
            }
            rows.shrink_to(RUN / 2 + RUN / 4);
            self.runs.insert(run + 1, Run::new(upper));
        }

        None
    }

    pub(crate) fn remove(&mut self, id: &Id) -> Option<V> {
        let (run, index) = self.find(id)?;
        let (_, value) = self.runs[run].rows.remove(index);

        self.len -= 1;
        self.settle(run);
        Some(value)
    }

    /// Keeps runs dense after `run` lost a row: an empty run goes, a sparse one joins a
    /// neighbour that has room, and one far below its capacity gives memory back.
    fn settle(&mut self, run: usize) {
        let len = self.runs[run].rows.len();

        if len == 0 {
            self.runs.remove(run);
            self.shrink_runs();
            return;
        }

        self.runs[run].first = self.runs[run].rows[0].0;

        if len > RUN / 2 {
            shrink(&mut self.runs[run].rows);
            return;
        }

        if let Some(next) = self.runs.get(run + 1)
            && len + next.rows.len() <= RUN
        {
            let next = self.runs.remove(run + 1).expect("checked above");
            let rows = &mut self.runs[run].rows;

            rows.reserve_exact(next.rows.len());
            rows.extend(next.rows);
            self.shrink_runs();
        } else if run > 0 && self.runs[run - 1].rows.len() + len <= RUN {
            let taken = self.runs.remove(run).expect("run exists");
            let rows = &mut self.runs[run - 1].rows;

            rows.reserve_exact(taken.rows.len());
            rows.extend(taken.rows);
            self.shrink_runs();
        } else {
            shrink(&mut self.runs[run].rows);
        }
    }

    /// Gives back the run deque's slack once runs went away by emptying or merging.
    fn shrink_runs(&mut self) {
        if self.runs.capacity() >= 4 * self.runs.len().max(4) {
            self.runs.shrink_to(2 * self.runs.len());
        }
    }

    pub(crate) fn first_key(&self) -> Option<Id> {
        self.runs.front().map(|run| run.first)
    }

    pub(crate) fn last_key(&self) -> Option<Id> {
        self.runs
            .back()
            .map(|run| run.rows.last().expect("runs are never empty").0)
    }

    pub(crate) fn iter(&self) -> Range<'_, V> {
        Range {
            runs: &self.runs,
            run: 0,
            index: 0,
            end: Bound::Unbounded,
        }
    }

    /// Rows with ids inside `(start, end)`, in id order.
    pub(crate) fn range(&self, (start, end): (Bound<Id>, Bound<Id>)) -> Range<'_, V> {
        let (run, index) = match start {
            Bound::Unbounded => (0, 0),
            Bound::Included(id) | Bound::Excluded(id) => {
                let run = self.run_of(&id);
                let index = self.runs.get(run).map_or(0, |run| match start {
                    Bound::Excluded(_) => run.rows.partition_point(|row| row.0 <= id),
                    _ => run.rows.partition_point(|row| row.0 < id),
                });

                (run, index)
            }
        };

        Range {
            runs: &self.runs,
            run,
            index,
            end,
        }
    }
}

#[cfg(test)]
impl<V> IdMap<V> {
    /// Heap bytes the map holds, slack capacity included.
    pub(crate) fn heap_bytes(&self) -> usize {
        self.runs.capacity() * size_of::<Run<V>>()
            + self
                .runs
                .iter()
                .map(|run| run.rows.capacity() * size_of::<(Id, V)>())
                .sum::<usize>()
    }
}

impl<V> std::ops::Index<&Id> for IdMap<V> {
    type Output = V;

    fn index(&self, id: &Id) -> &V {
        self.get(id).expect("id is in the map")
    }
}

impl<'a, V> IntoIterator for &'a IdMap<V> {
    type Item = (&'a Id, &'a V);
    type IntoIter = Range<'a, V>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

/// Rows in id order from a starting position up to an end bound.
pub(crate) struct Range<'a, V> {
    runs: &'a VecDeque<Run<V>>,
    run: usize,
    index: usize,
    end: Bound<Id>,
}

impl<'a, V> Iterator for Range<'a, V> {
    type Item = (&'a Id, &'a V);

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let run = self.runs.get(self.run)?;

            if let Some((id, value)) = run.rows.get(self.index) {
                let inside = match self.end {
                    Bound::Unbounded => true,
                    Bound::Included(end) => *id <= end,
                    Bound::Excluded(end) => *id < end,
                };

                if !inside {
                    self.run = self.runs.len();
                    return None;
                }

                self.index += 1;
                return Some((id, value));
            }

            self.run += 1;
            self.index = 0;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    use std::collections::BTreeMap;

    fn check<V: PartialEq + std::fmt::Debug>(map: &IdMap<V>, model: &BTreeMap<Id, V>) {
        assert_eq!(map.len(), model.len());
        assert!(map.iter().eq(model.iter()));
        assert_eq!(map.first_key(), model.keys().next().copied());
        assert_eq!(map.last_key(), model.keys().next_back().copied());

        for (index, run) in map.runs.iter().enumerate() {
            assert!(!run.rows.is_empty() && run.rows.len() <= RUN);
            assert_eq!(run.first, run.rows[0].0);
            assert!(run.rows.windows(2).all(|pair| pair[0].0 < pair[1].0));

            if let Some(next) = map.runs.get(index + 1) {
                assert!(run.rows.last().unwrap().0 < next.first);
            }
        }
    }

    #[test]
    fn ids_inserted_in_order_fill_every_run() {
        let mut map = IdMap::new();

        for sequence in 0..10 * RUN as u64 {
            assert_eq!(map.insert((1, sequence), sequence), None);
        }

        assert_eq!(map.runs.len(), 10);
        assert!(
            map.runs
                .iter()
                .all(|run| run.rows.len() == RUN && run.rows.capacity() == RUN)
        );
        assert_eq!(map.insert((1, 5), 50), Some(5));
        assert_eq!(map[&(1, 5)], 50);
    }

    #[test]
    fn acknowledging_out_of_order_merges_sparse_runs() {
        let mut map = IdMap::new();

        for sequence in 0..20 * RUN as u64 {
            map.insert((0, sequence), ());
        }
        // Keep every eighth row: each run would hold 8 rows of 64 without merging.
        for sequence in 0..20 * RUN as u64 {
            if sequence % 8 != 0 {
                map.remove(&(0, sequence));
            }
        }

        assert_eq!(map.len(), 20 * RUN / 8);
        assert!(map.runs.len() <= 20 / 4 + 1, "{} runs", map.runs.len());

        for sequence in (0..20 * RUN as u64).step_by(8) {
            map.remove(&(0, sequence));
        }

        assert!(map.is_empty());
        assert!(
            map.heap_bytes() <= 16 * size_of::<Run<()>>(),
            "{} bytes left",
            map.heap_bytes()
        );
    }

    #[test]
    fn ranges_honour_both_bounds() {
        let mut map = IdMap::new();

        for sequence in (0..400u64).step_by(2) {
            map.insert((7, sequence), sequence);
        }

        let ids =
            |start, end| -> Vec<u64> { map.range((start, end)).map(|(id, _)| id.1).collect() };

        assert_eq!(
            ids(Bound::Excluded((7, 10)), Bound::Included((7, 16))),
            [12, 14, 16]
        );
        assert_eq!(
            ids(Bound::Included((7, 10)), Bound::Excluded((7, 16))),
            [10, 12, 14]
        );
        assert_eq!(
            ids(Bound::Included((7, 11)), Bound::Included((7, 11))),
            Vec::<u64>::new()
        );
        assert_eq!(ids(Bound::Excluded((7, 396)), Bound::Unbounded), [398]);
        assert_eq!(
            ids(Bound::Included((0, 0)), Bound::Excluded((7, 4))),
            [0, 2]
        );
        assert_eq!(
            ids(Bound::Excluded((8, 0)), Bound::Unbounded),
            Vec::<u64>::new()
        );
    }

    proptest! {
        #[test]
        fn agrees_with_a_btree_map(
            operations in prop::collection::vec((0u8..3, 0u64..3, 0u64..300, any::<u16>()), 1..1500)
        ) {
            let mut map = IdMap::new();
            let mut model = BTreeMap::new();

            for (kind, millis, sequence, value) in operations {
                let id = (millis, sequence);

                match kind {
                    0 | 1 => prop_assert_eq!(map.insert(id, value), model.insert(id, value)),
                    _ => prop_assert_eq!(map.remove(&id), model.remove(&id)),
                }

                prop_assert_eq!(map.get(&id), model.get(&id));
            }

            check(&map, &model);

            for start in [(0, 0), (1, 150), (2, 200)] {
                let ours: Vec<_> = map
                    .range((Bound::Excluded(start), Bound::Included((2, 250))))
                    .collect();
                let theirs: Vec<_> = model
                    .range((Bound::Excluded(start), Bound::Included((2, 250))))
                    .collect();

                prop_assert_eq!(ours, theirs);
            }
        }
    }
}
