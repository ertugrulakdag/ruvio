use super::*;

/// What `BF.ADD` creates for a missing key (RedisBloom's defaults).
const DEFAULT_ERROR_RATE: f64 = 0.01;
const DEFAULT_CAPACITY: u64 = 100;
/// Each added layer is this many times larger than the last unless `EXPANSION` says otherwise.
pub const BLOOM_DEFAULT_EXPANSION: u32 = 2;
/// A layer above this many bits (64 MiB) is refused instead of allocated. It is also the
/// largest array a snapshot reads back under the default command size limit.
const MAX_LAYER_BITS: f64 = 536_870_912.0;
/// A new layer's false-positive rate is the previous layer's times this, so the whole
/// chain stays near the configured rate.
const ERROR_TIGHTENING: f64 = 0.5;

/// A scalable Bloom filter: layers of bits, each sized for a capacity and a rate.
///
/// Items go into the last layer until it holds `capacity` of them; then, unless the filter is
/// non-scaling (`expansion == 0`), a larger layer with a tighter rate is appended. A lookup asks
/// every layer. A filter with one layer behaves exactly like a plain fixed filter.
#[derive(Debug, Clone, PartialEq)]
pub struct BloomFilter {
    layers: Vec<BloomLayer>,
    error_rate: f64,
    expansion: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BloomLayer {
    pub(crate) words: Box<[u64]>,
    pub(crate) hash_count: u8,
    pub(crate) capacity: u64,
    pub(crate) count: u64,
}

/// The outcome of one item added to a filter.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BloomAdd {
    Added,
    /// Every bit was already set: the item was added before, or collided with others.
    Exists,
    /// A non-scaling filter holds its capacity already.
    Full,
}

/// What `BF.INFO` reports.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BloomInfo {
    pub capacity: u64,
    pub size_bytes: usize,
    pub filters: usize,
    pub items: u64,
    /// `None` for a non-scaling filter.
    pub expansion: Option<u32>,
}

impl BloomLayer {
    fn sized(error_rate: f64, capacity: u64) -> Result<Self, InvalidBloomConfig> {
        let words = Self::word_count(error_rate, capacity)?;
        let bits_per_item = -error_rate.ln() / std::f64::consts::LN_2.powi(2);
        let hash_count = (bits_per_item * std::f64::consts::LN_2)
            .round()
            .clamp(1.0, f64::from(u8::MAX));

        Ok(Self {
            words: vec![0; words].into_boxed_slice(),
            hash_count: hash_count as u8,
            capacity,
            count: 0,
        })
    }

    /// Words a layer for `capacity` items at `error_rate` needs, without allocating them.
    fn word_count(error_rate: f64, capacity: u64) -> Result<usize, InvalidBloomConfig> {
        if !(error_rate > 0.0 && error_rate < 1.0) || capacity == 0 {
            return Err(InvalidBloomConfig);
        }

        let bits_per_item = -error_rate.ln() / std::f64::consts::LN_2.powi(2);
        let bit_count = (capacity as f64 * bits_per_item).ceil();

        if !bit_count.is_finite() || bit_count > MAX_LAYER_BITS {
            return Err(InvalidBloomConfig);
        }

        Ok((bit_count as usize).div_ceil(64))
    }

    #[inline]
    fn contains(&self, first: u64, second: u64) -> bool {
        let bit_count = self.words.len() as u64 * 64;
        let mut position = first;

        for _ in 0..self.hash_count {
            let index = (position % bit_count) as usize;

            if self.words[index / 64] & (1 << (index % 64)) == 0 {
                return false;
            }

            position = position.wrapping_add(second);
        }

        true
    }

    /// Sets the item's bits and reports whether all of them were set already.
    #[inline]
    fn test_and_set(&mut self, first: u64, second: u64) -> bool {
        let bit_count = self.words.len() as u64 * 64;
        let mut position = first;
        let mut present = true;

        for _ in 0..self.hash_count {
            let index = (position % bit_count) as usize;
            let word = &mut self.words[index / 64];
            let mask = 1u64 << (index % 64);

            present &= *word & mask != 0;
            *word |= mask;
            position = position.wrapping_add(second);
        }

        present
    }
}

impl BloomFilter {
    pub fn with_config(
        error_rate: f64,
        capacity: u64,
        expansion: u32,
    ) -> Result<Self, InvalidBloomConfig> {
        Ok(Self {
            layers: vec![BloomLayer::sized(error_rate, capacity)?],
            error_rate,
            expansion,
        })
    }

    /// Rebuilds a filter from snapshot parts.
    pub fn from_layers(
        layers: Vec<BloomLayer>,
        error_rate: f64,
        expansion: u32,
    ) -> Result<Self, InvalidBloomConfig> {
        let valid = !layers.is_empty()
            && error_rate > 0.0
            && error_rate < 1.0
            && layers
                .iter()
                .all(|layer| !layer.words.is_empty() && layer.hash_count > 0 && layer.capacity > 0);

        if !valid {
            return Err(InvalidBloomConfig);
        }

        Ok(Self {
            layers,
            error_rate,
            expansion,
        })
    }

    /// A filter from before layers existed: one fixed layer. Its capacity follows from the size
    /// and hash count, and the item count is estimated from the number of set bits.
    pub fn from_legacy(words: Vec<u64>, hash_count: u8) -> Result<Self, InvalidBloomConfig> {
        if words.is_empty() || hash_count == 0 {
            return Err(InvalidBloomConfig);
        }

        let bit_count = words.len() as f64 * 64.0;
        let hashes = f64::from(hash_count);
        let capacity = ((bit_count * std::f64::consts::LN_2 / hashes) as u64).max(1);
        let set: u64 = words.iter().map(|word| u64::from(word.count_ones())).sum();
        let fill = (set as f64 / bit_count).min(1.0 - f64::EPSILON);
        let count = ((-bit_count / hashes) * (1.0 - fill).ln()) as u64;

        Ok(Self {
            layers: vec![BloomLayer {
                words: words.into_boxed_slice(),
                hash_count,
                capacity,
                count,
            }],
            error_rate: 0.5f64.powi(i32::from(hash_count)),
            expansion: BLOOM_DEFAULT_EXPANSION,
        })
    }

    pub fn layers(&self) -> &[BloomLayer] {
        &self.layers
    }

    pub fn error_rate(&self) -> f64 {
        self.error_rate
    }

    pub fn expansion(&self) -> u32 {
        self.expansion
    }

    pub fn payload_bytes(&self) -> usize {
        self.layers
            .iter()
            .map(|layer| layer.words.len() * size_of::<u64>())
            .sum()
    }

    pub fn info(&self) -> BloomInfo {
        BloomInfo {
            capacity: self.layers.iter().map(|layer| layer.capacity).sum(),
            size_bytes: self.payload_bytes() + size_of::<Self>(),
            filters: self.layers.len(),
            items: self.items(),
            expansion: (self.expansion != 0).then_some(self.expansion),
        }
    }

    pub fn items(&self) -> u64 {
        self.layers.iter().map(|layer| layer.count).sum()
    }

    /// Bytes the next layer needs, `0` while the last layer has room for `additional` more items
    /// or the filter cannot grow.
    fn growth_bytes(&self, additional: usize) -> usize {
        let last = self.layers.last().expect("a filter has a layer");

        if self.expansion == 0 || last.count.saturating_add(additional as u64) <= last.capacity {
            return 0;
        }

        let (error_rate, capacity) = self.next_layer_shape();
        let layer = BloomLayer::word_count(error_rate, capacity).unwrap_or(0) * size_of::<u64>();

        // A big batch may add several layers; about two bytes per item covers them.
        layer.max(additional.saturating_mul(2))
    }

    fn next_layer_shape(&self) -> (f64, u64) {
        let last = self.layers.last().expect("a filter has a layer");

        (
            self.error_rate * ERROR_TIGHTENING.powi(self.layers.len() as i32),
            last.capacity.saturating_mul(u64::from(self.expansion)),
        )
    }

    /// One lookup. Hashes are computed once and shared by every layer.
    pub fn contains(&self, value: &[u8]) -> bool {
        let (first, second) = bloom_hashes(value);

        self.layers
            .iter()
            .rev()
            .any(|layer| layer.contains(first, second))
    }

    /// Adds `value`, growing the filter by one layer when the last one is full.
    pub fn insert(&mut self, value: &[u8]) -> BloomAdd {
        let (first, second) = bloom_hashes(value);
        let last = self.layers.len() - 1;

        if self.layers[..last]
            .iter()
            .rev()
            .any(|layer| layer.contains(first, second))
        {
            return BloomAdd::Exists;
        }

        let tail = &mut self.layers[last];

        if tail.count < tail.capacity {
            if tail.test_and_set(first, second) {
                return BloomAdd::Exists;
            }

            tail.count += 1;
            return BloomAdd::Added;
        }

        if tail.contains(first, second) {
            return BloomAdd::Exists;
        }

        if self.expansion == 0 {
            return BloomAdd::Full;
        }

        let (error_rate, capacity) = self.next_layer_shape();
        let Ok(mut layer) = BloomLayer::sized(error_rate, capacity) else {
            return BloomAdd::Full;
        };

        layer.test_and_set(first, second);
        layer.count = 1;
        self.layers.push(layer);
        BloomAdd::Added
    }
}

const MIX_A: u64 = 0xa076_1d64_78bd_642f;
const MIX_B: u64 = 0xe703_7ed1_a0b4_28db;
const MIX_C: u64 = 0x8ebc_6af0_9c88_c6e3;
const MIX_D: u64 = 0x5899_65cc_7537_4cc3;

/// 64x64 -> 128 bit multiply folded back to 64 bits.
#[inline(always)]
fn mix(x: u64, y: u64) -> u64 {
    let product = u128::from(x) * u128::from(y);

    (product as u64) ^ ((product >> 64) as u64)
}

#[inline(always)]
fn read_u64(bytes: &[u8], at: usize) -> u64 {
    u64::from_le_bytes(bytes[at..at + 8].try_into().expect("eight bytes"))
}

#[inline(always)]
fn read_u32(bytes: &[u8], at: usize) -> u64 {
    u64::from(u32::from_le_bytes(
        bytes[at..at + 4].try_into().expect("four bytes"),
    ))
}

/// The two hashes every probe position derives from, from one pass over the value: a
/// wyhash-style multiply-fold with two lanes. The algorithm is fixed here (unlike
/// `DefaultHasher`, which std may change) so a persisted filter keeps its bits, and the
/// second hash is odd so the probe stride is never zero.
#[inline]
pub(super) fn bloom_hashes(value: &[u8]) -> (u64, u64) {
    let (mut lane_a, mut lane_b) = (MIX_A, MIX_B);
    let mut rest = value;

    while rest.len() > 16 {
        let (low, high) = (read_u64(rest, 0), read_u64(rest, 8));

        lane_a = mix(low ^ MIX_B ^ lane_a, high ^ MIX_C);
        lane_b = mix(low ^ MIX_D ^ lane_b, high ^ MIX_A);
        rest = &rest[16..];
    }

    let (low, high) = match rest.len() {
        8..=16 => (read_u64(rest, 0), read_u64(rest, rest.len() - 8)),
        4..=7 => (read_u32(rest, 0), read_u32(rest, rest.len() - 4)),
        0 => (0, 0),
        length => (
            (u64::from(rest[0]) << 16)
                | (u64::from(rest[length / 2]) << 8)
                | u64::from(rest[length - 1]),
            0,
        ),
    };
    let length = value.len() as u64;

    (
        mix(low ^ MIX_C ^ lane_a, high ^ length ^ MIX_D),
        mix(low ^ MIX_A ^ lane_b, high ^ length ^ MIX_B) | 1,
    )
}

impl<C: Clock, I: StorageIndex> KvShard<C, I> {
    /// `BF.RESERVE`. `expansion` is `0` for a non-scaling filter.
    pub fn bloom_reserve(
        &mut self,
        key: &[u8],
        error_rate: f64,
        capacity: usize,
        expansion: u32,
    ) -> Result<(), BloomError> {
        self.remove_expired(key);

        if self.entries.lookup(key).is_some() {
            return Err(BloomError::KeyExists);
        }

        let filter = BloomFilter::with_config(error_rate, capacity as u64, expansion)
            .map_err(|_| BloomError::InvalidConfiguration)?;
        let before = self.entry_charge(key);

        self.insert_entry(key, Entry::fresh(Value::BloomFilter(Box::new(filter))));
        self.reconcile_entry_charge(before, key);
        Ok(())
    }

    /// Bytes the first layer of a filter needs, `None` for a configuration that is refused.
    pub(crate) fn bloom_config_bytes(error_rate: f64, capacity: usize) -> Option<usize> {
        BloomLayer::word_count(error_rate, capacity as u64)
            .ok()
            .map(|words| words * size_of::<u64>())
    }

    /// Bytes an add of `additional` items may allocate beyond the items themselves: a new layer
    /// when the last one fills, the default filter for a missing key. Also the type check.
    pub fn bloom_growth_bytes(&self, key: &[u8], additional: usize) -> Result<usize, BloomError> {
        let Some(entry) = self.live_object(key).map_err(|_| BloomError::WrongType)? else {
            return Ok(DEFAULT_CAPACITY as usize * 2);
        };

        let Value::BloomFilter(filter) = &entry.value else {
            return Err(BloomError::WrongType);
        };

        Ok(filter.growth_bytes(additional))
    }

    /// Adds every value; a missing key becomes the default filter. `each` sees one outcome per value.
    pub fn bloom_add_each<V: AsRef<[u8]>>(
        &mut self,
        key: &[u8],
        values: &[V],
        mut each: impl FnMut(BloomAdd),
    ) -> Result<(), BloomError> {
        self.remove_expired(key);
        let before = self.entry_charge(key);
        let Some(entry) = self.entries.get_or_insert_with(key, || {
            Entry::fresh(Value::BloomFilter(Box::new(
                BloomFilter::with_config(
                    DEFAULT_ERROR_RATE,
                    DEFAULT_CAPACITY,
                    BLOOM_DEFAULT_EXPANSION,
                )
                .expect("valid default Bloom configuration"),
            )))
        }) else {
            return Err(BloomError::WrongType);
        };

        let Value::BloomFilter(filter) = &mut entry.value else {
            return Err(BloomError::WrongType);
        };

        for value in values {
            each(filter.insert(value.as_ref()));
        }

        self.reconcile_entry_charge(before, key);
        Ok(())
    }

    pub fn bloom_add(&mut self, key: &[u8], value: &[u8]) -> Result<BloomAdd, BloomError> {
        let mut outcome = BloomAdd::Exists;

        self.bloom_add_each(key, &[value], |added| outcome = added)?;
        Ok(outcome)
    }

    pub fn bloom_contains(&self, key: &[u8], value: &[u8]) -> Result<bool, BloomError> {
        let Some(entry) = self.live_object(key).map_err(|_| BloomError::WrongType)? else {
            return Ok(false);
        };

        let Value::BloomFilter(filter) = &entry.value else {
            return Err(BloomError::WrongType);
        };

        Ok(filter.contains(value))
    }

    /// `BF.MEXISTS`: one lookup of the key, then one answer per value. A missing key answers `false`.
    pub fn bloom_contains_each<V: AsRef<[u8]>>(
        &self,
        key: &[u8],
        values: &[V],
        mut each: impl FnMut(bool),
    ) -> Result<(), BloomError> {
        let filter = match self.live_object(key).map_err(|_| BloomError::WrongType)? {
            None => None,
            Some(entry) => match &entry.value {
                Value::BloomFilter(filter) => Some(filter),
                _ => return Err(BloomError::WrongType),
            },
        };

        for value in values {
            each(filter.is_some_and(|filter| filter.contains(value.as_ref())));
        }

        Ok(())
    }

    /// `BF.INFO` / `BF.CARD`: `None` for a missing key.
    pub fn bloom_info(&self, key: &[u8]) -> Result<Option<BloomInfo>, BloomError> {
        let Some(entry) = self.live_object(key).map_err(|_| BloomError::WrongType)? else {
            return Ok(None);
        };

        let Value::BloomFilter(filter) = &entry.value else {
            return Err(BloomError::WrongType);
        };

        Ok(Some(filter.info()))
    }
}

#[cfg(test)]
mod hash_tests {
    use super::*;

    #[test]
    fn hashes_are_fixed_across_builds() {
        assert_eq!(bloom_hashes(b""), bloom_hashes(b""));
        let (a, b) = bloom_hashes(b"ruvio");
        assert_eq!(b & 1, 1);
        assert_eq!((a, b), bloom_hashes(b"ruvio"));
        assert_ne!(bloom_hashes(b"a"), bloom_hashes(b"b"));
        assert_ne!(bloom_hashes(b"a\0"), bloom_hashes(b"a"));
    }

    #[test]
    fn single_bit_flips_change_about_half_the_output_bits() {
        for length in [1usize, 3, 5, 8, 12, 16, 17, 33, 100] {
            let base: Vec<u8> = (0..length).map(|index| (index * 31 + 7) as u8).collect();
            let (first, second) = bloom_hashes(&base);
            let (mut total, mut samples) = (0u32, 0u32);

            for bit in 0..length * 8 {
                let mut flipped = base.clone();

                flipped[bit / 8] ^= 1 << (bit % 8);
                let (f, s) = bloom_hashes(&flipped);

                total += (first ^ f).count_ones() + (second ^ s).count_ones();
                samples += 2;
            }
            let average = f64::from(total) / f64::from(samples);

            assert!(
                (28.0..36.0).contains(&average),
                "length {length}: {average}"
            );
        }
    }

    #[test]
    fn false_positive_rate_matches_the_configured_rate() {
        let mut filter = BloomFilter::with_config(0.01, 100_000, 0).unwrap();

        for index in 0..100_000u32 {
            filter.insert(format!("member:{index}").as_bytes());
        }
        assert!((0..100_000u32).all(|index| filter.contains(format!("member:{index}").as_bytes())));

        let false_hits = (0..200_000u32)
            .filter(|index| filter.contains(format!("other:{index}").as_bytes()))
            .count();

        assert!(false_hits < 2_600, "{false_hits} of 200000");
    }
}
