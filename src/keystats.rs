//! Sampled hot-key counting (Space-Saving, 128 entries) and a ring of recently written
//! big keys. Both are fixed-size per shard and only fed from sampled accesses.

use std::collections::VecDeque;

pub const HOT_KEY_ENTRIES: usize = 128;
pub const BIG_KEY_ENTRIES: usize = 64;
/// Values written at or above this many bytes are remembered by `RUVIO BIGKEYS`.
pub const BIG_KEY_BYTES: usize = 1024 * 1024;
/// One access in this many is counted for `RUVIO HOTKEYS`.
pub const HOT_KEY_SAMPLE: u32 = 1024;
const MAX_KEY_BYTES: usize = 256;

#[derive(Default)]
pub struct KeyStats {
    hot: Vec<(Box<[u8]>, u64, u64)>,
    big: VecDeque<(Box<[u8]>, usize)>,
}

impl KeyStats {
    /// Count one sampled access. Space-Saving: a full table evicts its smallest entry and
    /// the newcomer inherits that count as its error bound.
    pub fn hit(&mut self, key: &[u8]) {
        let key = &key[..key.len().min(MAX_KEY_BYTES)];

        if let Some(entry) = self.hot.iter_mut().find(|entry| &*entry.0 == key) {
            entry.1 += 1;
            return;
        }

        if self.hot.len() < HOT_KEY_ENTRIES {
            self.hot.push((key.into(), 1, 0));
            return;
        }

        if let Some(smallest) = self.hot.iter_mut().min_by_key(|entry| entry.1) {
            *smallest = (key.into(), smallest.1 + 1, smallest.1);
        }
    }

    pub fn big(&mut self, key: &[u8], bytes: usize) {
        let key = &key[..key.len().min(MAX_KEY_BYTES)];

        if let Some(position) = self.big.iter().position(|entry| &*entry.0 == key) {
            self.big.remove(position);
        } else if self.big.len() == BIG_KEY_ENTRIES {
            self.big.pop_front();
        }

        self.big.push_back((key.into(), bytes));
    }

    /// `(key, estimated samples, error bound)`, hottest first.
    pub fn hot_keys(&self) -> Vec<(&[u8], u64, u64)> {
        let mut keys: Vec<_> = self
            .hot
            .iter()
            .map(|(key, count, error)| (&**key, *count, *error))
            .collect();
        keys.sort_by_key(|entry| std::cmp::Reverse(entry.1));
        keys
    }

    /// `(key, bytes)`, most recent first.
    pub fn big_keys(&self) -> Vec<(&[u8], usize)> {
        self.big
            .iter()
            .rev()
            .map(|(key, bytes)| (&**key, *bytes))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn space_saving_keeps_a_dominant_key_on_top_when_full() {
        let mut stats = KeyStats::default();

        for round in 0..2000u32 {
            stats.hit(b"hot");
            stats.hit(format!("cold{round}").as_bytes());
        }

        let top = stats.hot_keys();
        assert_eq!(top.len(), HOT_KEY_ENTRIES);
        assert_eq!(top[0].0, b"hot");
        assert!(top[0].1 >= 2000);
    }

    #[test]
    fn big_key_ring_keeps_latest_and_dedups() {
        let mut stats = KeyStats::default();

        for index in 0..(BIG_KEY_ENTRIES + 5) {
            stats.big(format!("k{index}").as_bytes(), index);
        }
        stats.big(b"k10", 99);

        let keys = stats.big_keys();
        assert_eq!(keys.len(), BIG_KEY_ENTRIES);
        assert_eq!(keys[0], (&b"k10"[..], 99));
        assert!(keys.iter().all(|(key, _)| *key != b"k0"));
    }
}
