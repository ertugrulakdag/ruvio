//! Counted B+ tree over `(score, member)` for the indexed sorted set.
//!
//! Entries are `(score, offset)` pairs; the offset names an [`Arena`] block laid out as
//! `score f64 LE ‖ member`. Order is `(score.total_cmp, member bytes)`, so the arena is
//! read only when two scores tie. Inner nodes keep, for every child, the number of
//! entries below it and the smallest key below it, so rank, select and seek are
//! `O(log_CAP N)`. Nodes live in two `Vec` pools addressed by `u32`; leaves are chained
//! both ways for range scans. No `unsafe`.
//!
//! Invariants (checked by `check` in tests): every non-root node holds between `MIN` and
//! `CAP` slots, `counts[i]` is the size of child `i`, `keys[i]` is the smallest key in
//! child `i`, leaves are in key order along the chain.

use std::cmp::Ordering;

use crate::arena::Arena;

pub(crate) const NONE: u32 = u32::MAX;

/// Bytes of score at the front of every block; the member follows.
pub(crate) const SCORE_LEN: usize = 8;

pub(crate) fn block_score(block: &[u8]) -> f64 {
    let mut raw = [0u8; SCORE_LEN];

    raw.copy_from_slice(&block[..SCORE_LEN]);
    f64::from_le_bytes(raw)
}

pub(crate) fn block_member(block: &[u8]) -> &[u8] {
    &block[SCORE_LEN..]
}

/// Order of the stored entry `(score, off)` against the target `(t_score, t_member)`.
fn cmp_entry(arena: &Arena, score: f64, off: u32, t_score: f64, t_member: &[u8]) -> Ordering {
    score
        .total_cmp(&t_score)
        .then_with(|| block_member(arena.get(off)).cmp(t_member))
}

#[derive(Clone)]
struct Leaf<const CAP: usize> {
    len: u32,
    prev: u32,
    next: u32,
    scores: [f64; CAP],
    offs: [u32; CAP],
}

impl<const CAP: usize> Leaf<CAP> {
    fn empty() -> Self {
        Self {
            len: 0,
            prev: NONE,
            next: NONE,
            scores: [0.0; CAP],
            offs: [0; CAP],
        }
    }

    fn insert(&mut self, at: usize, score: f64, off: u32) {
        let len = self.len as usize;

        self.scores.copy_within(at..len, at + 1);
        self.offs.copy_within(at..len, at + 1);
        self.scores[at] = score;
        self.offs[at] = off;
        self.len += 1;
    }

    fn remove(&mut self, at: usize) -> (f64, u32) {
        let len = self.len as usize;
        let taken = (self.scores[at], self.offs[at]);

        self.scores.copy_within(at + 1..len, at);
        self.offs.copy_within(at + 1..len, at);
        self.len -= 1;
        taken
    }
}

#[derive(Clone)]
struct Inner<const CAP: usize> {
    len: u32,
    counts: [u32; CAP],
    kids: [u32; CAP],
    scores: [f64; CAP],
    offs: [u32; CAP],
}

/// One child slot of an inner node: smallest key, child, entries below.
type Slot = (f64, u32, u32, u32);

impl<const CAP: usize> Inner<CAP> {
    fn empty() -> Self {
        Self {
            len: 0,
            counts: [0; CAP],
            kids: [0; CAP],
            scores: [0.0; CAP],
            offs: [0; CAP],
        }
    }

    fn slot(&self, at: usize) -> Slot {
        (
            self.scores[at],
            self.offs[at],
            self.kids[at],
            self.counts[at],
        )
    }

    fn insert(&mut self, at: usize, (score, off, kid, count): Slot) {
        let len = self.len as usize;

        self.scores.copy_within(at..len, at + 1);
        self.offs.copy_within(at..len, at + 1);
        self.kids.copy_within(at..len, at + 1);
        self.counts.copy_within(at..len, at + 1);
        self.scores[at] = score;
        self.offs[at] = off;
        self.kids[at] = kid;
        self.counts[at] = count;
        self.len += 1;
    }

    fn remove(&mut self, at: usize) -> Slot {
        let len = self.len as usize;
        let taken = self.slot(at);

        self.scores.copy_within(at + 1..len, at);
        self.offs.copy_within(at + 1..len, at);
        self.kids.copy_within(at + 1..len, at);
        self.counts.copy_within(at + 1..len, at);
        self.len -= 1;
        taken
    }

    fn total(&self) -> u32 {
        self.counts[..self.len as usize].iter().sum()
    }

    /// The child to descend into: the last `i` with `i == 0` or `before(key[i])`.
    fn child(&self, mut before: impl FnMut(f64, u32) -> bool) -> usize {
        let (mut lo, mut hi) = (1usize, self.len as usize);

        while lo < hi {
            let mid = (lo + hi) / 2;

            if before(self.scores[mid], self.offs[mid]) {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }

        lo - 1
    }
}

/// The first slot of a leaf for which `before` is false.
fn leaf_bound<const CAP: usize>(
    leaf: &Leaf<CAP>,
    mut before: impl FnMut(f64, u32) -> bool,
) -> usize {
    let (mut lo, mut hi) = (0usize, leaf.len as usize);

    while lo < hi {
        let mid = (lo + hi) / 2;

        if before(leaf.scores[mid], leaf.offs[mid]) {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }

    lo
}

fn pair<T>(items: &mut [T], a: usize, b: usize) -> (&mut T, &mut T) {
    debug_assert_ne!(a, b);

    if a < b {
        let (left, right) = items.split_at_mut(b);

        (&mut left[a], &mut right[0])
    } else {
        let (left, right) = items.split_at_mut(a);

        (&mut right[0], &mut left[b])
    }
}

/// A place in the leaf chain; `leaf == NONE` is past either end.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Pos {
    leaf: u32,
    slot: u32,
}

impl Pos {
    pub(crate) const END: Self = Self {
        leaf: NONE,
        slot: 0,
    };

    pub(crate) fn is_end(self) -> bool {
        self.leaf == NONE
    }
}

/// std `BTreeMap` cannot keep subtree counts, and a skiplist costs memory and cache misses per
/// rank step. Node capacity 64 keeps 1M members at height 4.
#[derive(Clone)]
pub(crate) struct CountedTree<const CAP: usize = 64> {
    leaves: Vec<Leaf<CAP>>,
    inners: Vec<Inner<CAP>>,
    free_leaves: Vec<u32>,
    free_inners: Vec<u32>,
    root: u32,
    /// Inner levels above the leaves; 0 when the root is a leaf.
    height: u32,
    len: u32,
    first: u32,
    last: u32,
}

impl<const CAP: usize> std::fmt::Debug for CountedTree<CAP> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CountedTree")
            .field("len", &self.len)
            .field("height", &self.height)
            .field("leaves", &self.leaves.len())
            .field("inners", &self.inners.len())
            .finish()
    }
}

impl<const CAP: usize> Default for CountedTree<CAP> {
    fn default() -> Self {
        Self::new()
    }
}

impl<const CAP: usize> CountedTree<CAP> {
    /// Fewest slots a non-root node keeps; two at least, so every inner node has a sibling
    /// to borrow from or merge with, and `2 * MIN - 1 <= CAP` lets two nodes merge.
    const MIN: usize = if CAP / 4 > 2 { CAP / 4 } else { 2 };

    pub(crate) fn new() -> Self {
        Self {
            leaves: Vec::new(),
            inners: Vec::new(),
            free_leaves: Vec::new(),
            free_inners: Vec::new(),
            root: NONE,
            height: 0,
            len: 0,
            first: NONE,
            last: NONE,
        }
    }

    pub(crate) fn len(&self) -> usize {
        self.len as usize
    }

    /// Bytes held by the node pools.
    pub(crate) fn capacity_bytes(&self) -> usize {
        self.leaves
            .capacity()
            .saturating_mul(size_of::<Leaf<CAP>>())
            .saturating_add(
                self.inners
                    .capacity()
                    .saturating_mul(size_of::<Inner<CAP>>()),
            )
            .saturating_add(
                (self.free_leaves.capacity() + self.free_inners.capacity())
                    .saturating_mul(size_of::<u32>()),
            )
    }

    fn alloc_leaf(&mut self) -> u32 {
        match self.free_leaves.pop() {
            Some(index) => {
                self.leaves[index as usize] = Leaf::empty();
                index
            }
            None => {
                self.leaves.push(Leaf::empty());
                (self.leaves.len() - 1) as u32
            }
        }
    }

    fn alloc_inner(&mut self) -> u32 {
        match self.free_inners.pop() {
            Some(index) => {
                self.inners[index as usize] = Inner::empty();
                index
            }
            None => {
                self.inners.push(Inner::empty());
                (self.inners.len() - 1) as u32
            }
        }
    }

    fn free_leaf(&mut self, index: u32) {
        self.leaves[index as usize].len = 0;
        self.free_leaves.push(index);
    }

    fn free_inner(&mut self, index: u32) {
        self.inners[index as usize].len = 0;
        self.free_inners.push(index);
    }

    /// Slots a full node keeps when the new slot lands at `at`. An append leaves the right
    /// node at `MIN`, so ascending loads fill leaves to about three quarters, not half.
    fn split_point(at: usize) -> usize {
        if at == CAP {
            CAP + 1 - Self::MIN
        } else {
            CAP / 2
        }
    }

    fn is_leaf_level(&self, level: u32) -> bool {
        level == self.height
    }

    fn node_len(&self, node: u32, level: u32) -> usize {
        if self.is_leaf_level(level) {
            self.leaves[node as usize].len as usize
        } else {
            self.inners[node as usize].len as usize
        }
    }

    fn node_min(&self, node: u32, level: u32) -> (f64, u32) {
        if self.is_leaf_level(level) {
            let leaf = &self.leaves[node as usize];

            (leaf.scores[0], leaf.offs[0])
        } else {
            let inner = &self.inners[node as usize];

            (inner.scores[0], inner.offs[0])
        }
    }

    /// Adds the entry whose block is at `off`. The member must be absent.
    pub(crate) fn insert(&mut self, arena: &Arena, score: f64, off: u32) {
        let member = block_member(arena.get(off));

        self.len += 1;

        if self.root == NONE {
            let leaf = self.alloc_leaf();

            self.leaves[leaf as usize].insert(0, score, off);
            self.root = leaf;
            self.height = 0;
            self.first = leaf;
            self.last = leaf;
            return;
        }

        if let Some(split) = self.insert_at(arena, self.root, 0, score, off, member) {
            let old = self.root;
            let (min_score, min_off) = self.node_min(old, 0);
            let root = self.alloc_inner();
            let inner = &mut self.inners[root as usize];

            inner.insert(0, (min_score, min_off, old, self.len - split.3));
            inner.insert(1, split);
            self.root = root;
            self.height += 1;
        }
    }

    fn insert_at(
        &mut self,
        arena: &Arena,
        node: u32,
        level: u32,
        score: f64,
        off: u32,
        member: &[u8],
    ) -> Option<Slot> {
        let before = |s: f64, o: u32| cmp_entry(arena, s, o, score, member).is_lt();

        if self.is_leaf_level(level) {
            let leaf = &mut self.leaves[node as usize];
            let at = leaf_bound(leaf, before);

            if (leaf.len as usize) < CAP {
                leaf.insert(at, score, off);
                return None;
            }

            let mid = Self::split_point(at);
            let right = self.alloc_leaf();
            let (left_leaf, right_leaf) = pair(&mut self.leaves, node as usize, right as usize);
            let moved = CAP - mid;

            right_leaf.scores[..moved].copy_from_slice(&left_leaf.scores[mid..]);
            right_leaf.offs[..moved].copy_from_slice(&left_leaf.offs[mid..]);
            right_leaf.len = moved as u32;
            left_leaf.len = mid as u32;
            right_leaf.prev = node;
            right_leaf.next = left_leaf.next;
            left_leaf.next = right;

            if at < mid {
                left_leaf.insert(at, score, off);
            } else {
                right_leaf.insert(at - mid, score, off);
            }

            let after = right_leaf.next;
            let slot = (
                right_leaf.scores[0],
                right_leaf.offs[0],
                right,
                right_leaf.len,
            );

            if after == NONE {
                self.last = right;
            } else {
                self.leaves[after as usize].prev = right;
            }

            return Some(slot);
        }

        let inner = &mut self.inners[node as usize];
        let child = inner.child(before);

        inner.counts[child] += 1;

        if child == 0 && cmp_entry(arena, inner.scores[0], inner.offs[0], score, member).is_gt() {
            inner.scores[0] = score;
            inner.offs[0] = off;
        }

        let kid = inner.kids[child];
        let split = self.insert_at(arena, kid, level + 1, score, off, member)?;
        let inner = &mut self.inners[node as usize];
        let at = child + 1;

        inner.counts[child] -= split.3;

        if (inner.len as usize) < CAP {
            inner.insert(at, split);
            return None;
        }

        let mid = Self::split_point(at);
        let right = self.alloc_inner();
        let (left_inner, right_inner) = pair(&mut self.inners, node as usize, right as usize);
        let moved = CAP - mid;

        right_inner.scores[..moved].copy_from_slice(&left_inner.scores[mid..]);
        right_inner.offs[..moved].copy_from_slice(&left_inner.offs[mid..]);
        right_inner.kids[..moved].copy_from_slice(&left_inner.kids[mid..]);
        right_inner.counts[..moved].copy_from_slice(&left_inner.counts[mid..]);
        right_inner.len = moved as u32;
        left_inner.len = mid as u32;

        if at < mid {
            left_inner.insert(at, split);
        } else {
            right_inner.insert(at - mid, split);
        }

        Some((
            right_inner.scores[0],
            right_inner.offs[0],
            right,
            right_inner.total(),
        ))
    }

    /// Removes `(score, member)`; returns its block offset, which the caller frees.
    pub(crate) fn remove(&mut self, arena: &Arena, score: f64, member: &[u8]) -> Option<u32> {
        if self.root == NONE {
            return None;
        }

        let off = self.remove_at(arena, self.root, 0, score, member)?;

        self.len -= 1;

        while self.height > 0 && self.inners[self.root as usize].len == 1 {
            let old = self.root;

            self.root = self.inners[old as usize].kids[0];
            self.free_inner(old);
            self.height -= 1;
        }

        if self.len == 0 {
            *self = Self::new();
        }

        Some(off)
    }

    fn remove_at(
        &mut self,
        arena: &Arena,
        node: u32,
        level: u32,
        score: f64,
        member: &[u8],
    ) -> Option<u32> {
        if self.is_leaf_level(level) {
            let leaf = &mut self.leaves[node as usize];
            let at = leaf_bound(leaf, |s, o| cmp_entry(arena, s, o, score, member).is_lt());

            if at < leaf.len as usize
                && cmp_entry(arena, leaf.scores[at], leaf.offs[at], score, member).is_eq()
            {
                return Some(leaf.remove(at).1);
            }

            return None;
        }

        let inner = &self.inners[node as usize];
        let child = inner.child(|s, o| cmp_entry(arena, s, o, score, member).is_le());
        let kid = inner.kids[child];
        let off = self.remove_at(arena, kid, level + 1, score, member)?;
        let kid_len = self.node_len(kid, level + 1);
        let kid_min = (kid_len > 0).then(|| self.node_min(kid, level + 1));
        let inner = &mut self.inners[node as usize];

        inner.counts[child] -= 1;

        if let Some((min_score, min_off)) = kid_min {
            inner.scores[child] = min_score;
            inner.offs[child] = min_off;
        }

        if kid_len < Self::MIN {
            self.rebalance(node, child, level + 1);
        }

        Some(off)
    }

    /// Child `child` of `parent` is below `MIN`: borrow one slot from a sibling or merge.
    fn rebalance(&mut self, parent: u32, child: usize, kid_level: u32) {
        let (left_at, right_at) = if child > 0 {
            (child - 1, child)
        } else {
            (0, 1)
        };
        let p = &self.inners[parent as usize];
        let (left, right) = (p.kids[left_at], p.kids[right_at]);
        let sibling = if child == left_at { right } else { left };
        let sibling_len = self.node_len(sibling, kid_level);

        if self.is_leaf_level(kid_level) {
            let (l, r) = pair(&mut self.leaves, left as usize, right as usize);

            if sibling_len > Self::MIN {
                let moved = if child == right_at {
                    let (score, off) = l.remove(l.len as usize - 1);

                    r.insert(0, score, off);
                    -1i64
                } else {
                    let (score, off) = r.remove(0);

                    l.insert(l.len as usize, score, off);
                    1
                };
                let right_min = (r.scores[0], r.offs[0]);
                let p = &mut self.inners[parent as usize];

                p.counts[left_at] = (i64::from(p.counts[left_at]) + moved) as u32;
                p.counts[right_at] = (i64::from(p.counts[right_at]) - moved) as u32;
                (p.scores[right_at], p.offs[right_at]) = right_min;
                return;
            }

            let (l_len, r_len) = (l.len as usize, r.len as usize);

            l.scores[l_len..l_len + r_len].copy_from_slice(&r.scores[..r_len]);
            l.offs[l_len..l_len + r_len].copy_from_slice(&r.offs[..r_len]);
            l.len += r.len;
            l.next = r.next;

            let after = r.next;

            if after == NONE {
                self.last = left;
            } else {
                self.leaves[after as usize].prev = left;
            }

            self.free_leaf(right);
        } else {
            let (l, r) = pair(&mut self.inners, left as usize, right as usize);

            if sibling_len > Self::MIN {
                let moved = if child == right_at {
                    let slot = l.remove(l.len as usize - 1);

                    r.insert(0, slot);
                    -i64::from(slot.3)
                } else {
                    let slot = r.remove(0);

                    l.insert(l.len as usize, slot);
                    i64::from(slot.3)
                };
                let right_min = (r.scores[0], r.offs[0]);
                let p = &mut self.inners[parent as usize];

                p.counts[left_at] = (i64::from(p.counts[left_at]) + moved) as u32;
                p.counts[right_at] = (i64::from(p.counts[right_at]) - moved) as u32;
                (p.scores[right_at], p.offs[right_at]) = right_min;
                return;
            }

            let (l_len, r_len) = (l.len as usize, r.len as usize);

            l.scores[l_len..l_len + r_len].copy_from_slice(&r.scores[..r_len]);
            l.offs[l_len..l_len + r_len].copy_from_slice(&r.offs[..r_len]);
            l.kids[l_len..l_len + r_len].copy_from_slice(&r.kids[..r_len]);
            l.counts[l_len..l_len + r_len].copy_from_slice(&r.counts[..r_len]);
            l.len += r.len;
            self.free_inner(right);
        }

        let p = &mut self.inners[parent as usize];
        let (_, _, _, count) = p.remove(right_at);

        p.counts[left_at] += count;
    }

    /// Zero-based rank of `(score, member)`, if present.
    pub(crate) fn rank(&self, arena: &Arena, score: f64, member: &[u8]) -> Option<usize> {
        if self.root == NONE {
            return None;
        }

        let mut node = self.root;
        let mut rank = 0usize;

        for _ in 0..self.height {
            let inner = &self.inners[node as usize];
            let child = inner.child(|s, o| cmp_entry(arena, s, o, score, member).is_le());

            rank += inner.counts[..child]
                .iter()
                .map(|&c| c as usize)
                .sum::<usize>();
            node = inner.kids[child];
        }

        let leaf = &self.leaves[node as usize];
        let at = leaf_bound(leaf, |s, o| cmp_entry(arena, s, o, score, member).is_lt());

        (at < leaf.len as usize
            && cmp_entry(arena, leaf.scores[at], leaf.offs[at], score, member).is_eq())
        .then_some(rank + at)
    }

    /// The entry at zero-based `rank`.
    pub(crate) fn select(&self, rank: usize) -> Pos {
        if rank >= self.len() {
            return Pos::END;
        }

        let mut node = self.root;
        let mut rank = rank as u32;

        for _ in 0..self.height {
            let inner = &self.inners[node as usize];
            let mut child = 0;

            while rank >= inner.counts[child] {
                rank -= inner.counts[child];
                child += 1;
            }

            node = inner.kids[child];
        }

        Pos {
            leaf: node,
            slot: rank,
        }
    }

    /// The first entry whose score fails `before`; `before` must hold for a prefix of the
    /// order only (as "below the range start" does).
    pub(crate) fn seek(&self, mut before: impl FnMut(f64) -> bool) -> Pos {
        if self.root == NONE {
            return Pos::END;
        }

        let mut node = self.root;

        for _ in 0..self.height {
            let inner = &self.inners[node as usize];

            node = inner.kids[inner.child(|s, _| before(s))];
        }

        let leaf = &self.leaves[node as usize];
        let at = leaf_bound(leaf, |s, _| before(s));

        if at < leaf.len as usize {
            Pos {
                leaf: node,
                slot: at as u32,
            }
        } else if leaf.next == NONE {
            Pos::END
        } else {
            Pos {
                leaf: leaf.next,
                slot: 0,
            }
        }
    }

    /// `(score, offset)` at `pos`, which must not be the end.
    pub(crate) fn get(&self, pos: Pos) -> (f64, u32) {
        let leaf = &self.leaves[pos.leaf as usize];

        (leaf.scores[pos.slot as usize], leaf.offs[pos.slot as usize])
    }

    pub(crate) fn next(&self, pos: Pos) -> Pos {
        let leaf = &self.leaves[pos.leaf as usize];

        if pos.slot + 1 < leaf.len {
            Pos {
                leaf: pos.leaf,
                slot: pos.slot + 1,
            }
        } else if leaf.next == NONE {
            Pos::END
        } else {
            Pos {
                leaf: leaf.next,
                slot: 0,
            }
        }
    }

    pub(crate) fn prev(&self, pos: Pos) -> Pos {
        if pos.slot > 0 {
            return Pos {
                leaf: pos.leaf,
                slot: pos.slot - 1,
            };
        }

        let prev = self.leaves[pos.leaf as usize].prev;

        if prev == NONE {
            Pos::END
        } else {
            Pos {
                leaf: prev,
                slot: self.leaves[prev as usize].len - 1,
            }
        }
    }

    /// Every `(score, offset)` in order.
    pub(crate) fn iter(&self) -> impl Iterator<Item = (f64, u32)> + '_ {
        let mut leaf = self.first;

        std::iter::from_fn(move || {
            if leaf == NONE {
                return None;
            }

            let node = &self.leaves[leaf as usize];

            leaf = node.next;
            Some(node)
        })
        .flat_map(|node| {
            let len = node.len as usize;

            node.scores[..len]
                .iter()
                .copied()
                .zip(node.offs[..len].iter().copied())
        })
    }

    /// Compacts `arena`, whose live blocks are exactly this tree's entries, writing the
    /// blocks back in key order and updating every offset.
    pub(crate) fn compact(&mut self, arena: &mut Arena) {
        let mut offs: Vec<u32> = self.iter().map(|(_, off)| off).collect();

        arena.compact(offs.iter_mut());

        let mut fresh = offs.into_iter();
        let mut leaf = self.first;

        while leaf != NONE {
            let node = &mut self.leaves[leaf as usize];

            for slot in &mut node.offs[..node.len as usize] {
                *slot = fresh.next().expect("one offset per entry");
            }

            leaf = node.next;
        }

        if self.root != NONE {
            self.refresh_keys(self.root, 0);
        }
    }

    fn refresh_keys(&mut self, node: u32, level: u32) -> (f64, u32) {
        if self.is_leaf_level(level) {
            return self.node_min(node, level);
        }

        for at in 0..self.inners[node as usize].len as usize {
            let kid = self.inners[node as usize].kids[at];
            let (score, off) = self.refresh_keys(kid, level + 1);
            let inner = &mut self.inners[node as usize];

            inner.scores[at] = score;
            inner.offs[at] = off;
        }

        self.node_min(node, level)
    }

    /// Panics unless every structural invariant holds.
    #[cfg(test)]
    pub(crate) fn check(&self, arena: &Arena) {
        if self.root == NONE {
            assert_eq!(self.len, 0);
            assert_eq!(self.first, NONE);
            return;
        }

        let mut leaves = Vec::new();
        let total = self.check_node(arena, self.root, 0, &mut leaves);

        assert_eq!(total, self.len as usize);
        assert_eq!(self.first, leaves[0]);
        assert_eq!(self.last, *leaves.last().unwrap());

        for (index, &leaf) in leaves.iter().enumerate() {
            let node = &self.leaves[leaf as usize];
            let prev = if index == 0 { NONE } else { leaves[index - 1] };
            let next = leaves.get(index + 1).copied().unwrap_or(NONE);

            assert_eq!(node.prev, prev);
            assert_eq!(node.next, next);
        }

        let entries: Vec<_> = self.iter().collect();

        assert_eq!(entries.len(), total);

        for pair in entries.windows(2) {
            let (score, off) = pair[1];
            let member = block_member(arena.get(off));

            assert!(cmp_entry(arena, pair[0].0, pair[0].1, score, member).is_lt());
        }

        for &(score, off) in &entries {
            assert_eq!(block_score(arena.get(off)).to_bits(), score.to_bits());
        }
    }

    #[cfg(test)]
    fn check_node(&self, arena: &Arena, node: u32, level: u32, leaves: &mut Vec<u32>) -> usize {
        let len = self.node_len(node, level);

        assert!(len <= CAP);
        assert!(len >= if node == self.root { 1 } else { Self::MIN });

        if self.is_leaf_level(level) {
            leaves.push(node);
            return len;
        }

        if node == self.root {
            assert!(len >= 2);
        }

        let inner = &self.inners[node as usize];
        let mut total = 0;

        for at in 0..len {
            let kid = inner.kids[at];
            let count = self.check_node(arena, kid, level + 1, leaves);
            let (score, off) = self.node_min(kid, level + 1);

            assert_eq!(inner.counts[at] as usize, count);
            assert_eq!(inner.scores[at].to_bits(), score.to_bits());
            assert_eq!(inner.offs[at], off);
            total += count;
        }

        let _ = arena;
        total
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    const SCORES: [f64; 8] = [
        f64::NEG_INFINITY,
        -1.5,
        -0.0,
        0.0,
        1.0,
        2.0,
        3.0,
        f64::INFINITY,
    ];

    /// A tree, its arena and a sorted reference model `(score, member, offset)`.
    struct Harness<const CAP: usize> {
        arena: Arena,
        tree: CountedTree<CAP>,
        model: Vec<(f64, Vec<u8>, u32)>,
    }

    impl<const CAP: usize> Harness<CAP> {
        fn new() -> Self {
            Self {
                arena: Arena::new(),
                tree: CountedTree::new(),
                model: Vec::new(),
            }
        }

        fn model_at(&self, member: &[u8]) -> Option<usize> {
            self.model.iter().position(|(_, m, _)| m == member)
        }

        fn model_cmp(a: &(f64, Vec<u8>, u32), score: f64, member: &[u8]) -> Ordering {
            a.0.total_cmp(&score)
                .then_with(|| a.1.as_slice().cmp(member))
        }

        fn insert(&mut self, score: f64, member: &[u8]) {
            if let Some(at) = self.model_at(member) {
                let (old, _, off) = self.model.remove(at);

                assert_eq!(self.tree.remove(&self.arena, old, member), Some(off));
                self.arena.free(off);
            }

            let off = self.arena.alloc(&[&score.to_le_bytes(), member]).unwrap();

            self.tree.insert(&self.arena, score, off);

            let at = self
                .model
                .partition_point(|entry| Self::model_cmp(entry, score, member).is_lt());

            self.model.insert(at, (score, member.to_vec(), off));
        }

        fn remove(&mut self, member: &[u8]) {
            let expected = self.model_at(member);
            let score = expected.map_or(1.0, |at| self.model[at].0);
            let got = self.tree.remove(&self.arena, score, member);

            match expected {
                Some(at) => {
                    let (_, _, off) = self.model.remove(at);

                    assert_eq!(got, Some(off));
                    self.arena.free(off);
                }
                None => assert_eq!(got, None),
            }
        }

        fn verify(&self) {
            self.tree.check(&self.arena);

            let got: Vec<_> = self.tree.iter().map(|(_, off)| off).collect();
            let want: Vec<_> = self.model.iter().map(|(_, _, off)| *off).collect();

            assert_eq!(got, want);
        }

        fn verify_queries(&self, member: &[u8], rank: usize, score: f64) {
            let want = self.model_at(member);
            let got = self
                .model
                .get(want.unwrap_or(0))
                .and_then(|(s, _, _)| self.tree.rank(&self.arena, *s, member));

            assert_eq!(got, want);

            match self.model.get(rank) {
                Some((_, _, off)) => {
                    let pos = self.tree.select(rank);

                    assert_eq!(self.tree.get(pos).1, *off);
                    let next = self.tree.next(pos);

                    match self.model.get(rank + 1) {
                        Some(entry) => assert_eq!(self.tree.get(next).1, entry.2),
                        None => assert!(next.is_end()),
                    }

                    let prev = self.tree.prev(pos);

                    if rank == 0 {
                        assert!(prev.is_end());
                    } else {
                        assert_eq!(self.tree.get(prev).1, self.model[rank - 1].2);
                    }
                }
                None => assert!(self.tree.select(rank).is_end()),
            }

            let at = self.model.partition_point(|entry| entry.0 < score);
            let pos = self.tree.seek(|s| s < score);

            match self.model.get(at) {
                Some(entry) => assert_eq!(self.tree.get(pos).1, entry.2),
                None => assert!(pos.is_end()),
            }
        }
    }

    #[derive(Debug, Clone)]
    enum Op {
        Insert(usize, u16),
        Remove(u16),
        Compact,
    }

    fn ops(members: u16) -> impl Strategy<Value = Vec<Op>> {
        prop::collection::vec(
            prop_oneof![
                6 => (0usize..SCORES.len(), 0..members).prop_map(|(s, m)| Op::Insert(s, m)),
                3 => (0..members).prop_map(Op::Remove),
                1 => Just(Op::Compact),
            ],
            1..300,
        )
    }

    fn member(id: u16) -> Vec<u8> {
        format!("m{id}").into_bytes()
    }

    fn run<const CAP: usize>(ops: &[Op], probe: (u16, usize, usize)) {
        let mut harness = Harness::<CAP>::new();

        for op in ops {
            match op {
                Op::Insert(score, id) => harness.insert(SCORES[*score], &member(*id)),
                Op::Remove(id) => harness.remove(&member(*id)),
                Op::Compact => {
                    harness.tree.compact(&mut harness.arena);

                    for (entry, (_, off)) in harness.model.iter_mut().zip(harness.tree.iter()) {
                        entry.2 = off;
                    }
                }
            }

            harness.verify();
            harness.verify_queries(&member(probe.0), probe.1, SCORES[probe.2]);
        }
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(
            std::env::var("PROPTEST_CASES").ok().and_then(|v| v.parse().ok()).unwrap_or(256)
        ))]

        #[test]
        fn tree_matches_sorted_model_cap4(
            ops in ops(200),
            probe in (0u16..200, 0usize..220, 0usize..SCORES.len()),
        ) {
            run::<4>(&ops, probe);
        }

        #[test]
        fn tree_matches_sorted_model_cap5(
            ops in ops(120),
            probe in (0u16..120, 0usize..140, 0usize..SCORES.len()),
        ) {
            run::<5>(&ops, probe);
        }
    }

    #[test]
    fn large_tree_with_default_capacity() {
        let mut harness = Harness::<64>::new();
        let mut state = 0x9e37_79b9_7f4a_7c15u64;
        let mut step = || {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state
        };

        let members: u32 = if cfg!(miri) { 300 } else { 5_000 };

        for id in 0..members {
            let score = (step() % 50) as f64;

            harness.insert(score, format!("m{id}").as_bytes());
        }

        harness.verify();
        assert!(harness.tree.height >= 2);

        for _ in 0..members * 3 / 4 {
            let id = (step() % u64::from(members)) as u32;

            harness.remove(format!("m{id}").as_bytes());
        }

        harness.verify();

        for rank in (0..harness.model.len()).step_by(97) {
            let (score, member, off) = &harness.model[rank];

            assert_eq!(
                harness.tree.rank(&harness.arena, *score, member),
                Some(rank)
            );
            assert_eq!(harness.tree.get(harness.tree.select(rank)).1, *off);
        }

        harness.tree.compact(&mut harness.arena);

        for (entry, (_, off)) in harness.model.iter_mut().zip(harness.tree.iter()) {
            entry.2 = off;
        }

        harness.verify();

        let members: Vec<_> = harness.model.iter().map(|(_, m, _)| m.clone()).collect();

        for member in members {
            harness.remove(&member);
        }

        harness.verify();
        assert_eq!(harness.tree.len(), 0);
    }

    #[test]
    fn ascending_load_packs_leaves() {
        let mut harness = Harness::<64>::new();

        for id in 0..64 * 100u32 {
            harness.insert(f64::from(id), format!("m{id:06}").as_bytes());
        }

        harness.verify();
        assert!(harness.tree.leaves.len() <= 64 * 100 / (64 - 16) + 1);
    }
}
