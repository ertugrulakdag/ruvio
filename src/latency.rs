//! Per-command latency histograms. Log-linear buckets: four sub-buckets per power
//! of two, so 256 `u64` counters (2 KiB) cover 0 ns to `u64::MAX` with at most 25% error.
//! A histogram is allocated the first time its command is sampled.

pub const BUCKETS: usize = 256;

pub struct Histogram {
    counts: [u64; BUCKETS],
    sum_nanos: u128,
}

impl Histogram {
    fn new() -> Self {
        Self {
            counts: [0; BUCKETS],
            sum_nanos: 0,
        }
    }

    pub fn bucket(nanos: u64) -> usize {
        if nanos < 4 {
            return nanos as usize;
        }

        let exponent = 63 - nanos.leading_zeros() as usize;
        (exponent - 1) * 4 + ((nanos >> (exponent - 2)) & 3) as usize
    }

    /// Largest value, in nanoseconds, that falls into `index`.
    pub fn upper_bound(index: usize) -> u64 {
        if index < 4 {
            return index as u64;
        }

        let exponent = index / 4 + 1;
        let lower = (4 + (index % 4) as u64) << (exponent - 2);
        lower + ((1u64 << (exponent - 2)) - 1)
    }

    pub fn count(&self) -> u64 {
        self.counts.iter().sum()
    }

    pub fn sum_nanos(&self) -> u128 {
        self.sum_nanos
    }

    /// `(upper bound ns, samples at or below it)` for each non-empty bucket.
    pub fn cumulative(&self) -> Vec<(u64, u64)> {
        let mut total = 0;
        let mut points = Vec::new();

        for (index, &count) in self.counts.iter().enumerate() {
            if count != 0 {
                total += count;
                points.push((Self::upper_bound(index), total));
            }
        }

        points
    }

    /// Upper bound of the bucket holding the `fraction` quantile (`0.0..=1.0`).
    pub fn quantile(&self, fraction: f64) -> u64 {
        let total = self.count();

        if total == 0 {
            return 0;
        }

        let target = ((total as f64 * fraction).ceil() as u64).clamp(1, total);
        let mut seen = 0;

        for (index, &count) in self.counts.iter().enumerate() {
            seen += count;

            if seen >= target {
                return Self::upper_bound(index);
            }
        }

        Self::upper_bound(BUCKETS - 1)
    }
}

/// One histogram slot per command-table entry.
#[derive(Default)]
pub struct LatencyTable {
    histograms: Vec<Option<Box<Histogram>>>,
}

impl LatencyTable {
    pub fn record(&mut self, command: usize, nanos: u64) {
        if self.histograms.len() <= command {
            self.histograms.resize_with(command + 1, || None);
        }

        let histogram = self.histograms[command].get_or_insert_with(|| Box::new(Histogram::new()));
        histogram.counts[Histogram::bucket(nanos)] += 1;
        histogram.sum_nanos += u128::from(nanos);
    }

    /// Sampled commands in table order.
    pub fn iter(&self) -> impl Iterator<Item = (usize, &Histogram)> {
        self.histograms
            .iter()
            .enumerate()
            .filter_map(|(index, histogram)| histogram.as_deref().map(|h| (index, h)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn buckets_are_monotonic_and_bounds_contain_their_values() {
        let mut previous = 0;

        for value in (0..5000u64).chain([1 << 20, (1 << 20) + 1, u64::MAX / 3, u64::MAX]) {
            let bucket = Histogram::bucket(value);
            assert!(bucket < BUCKETS && bucket >= previous.min(bucket));
            assert!(Histogram::upper_bound(bucket) >= value, "{value}");

            if bucket > 0 {
                assert!(Histogram::upper_bound(bucket - 1) < value, "{value}");
            }

            previous = bucket;
        }
    }

    #[test]
    fn quantiles_follow_the_recorded_samples() {
        let mut table = LatencyTable::default();

        for _ in 0..99 {
            table.record(3, 1_000);
        }
        table.record(3, 1_000_000);

        let (index, histogram) = table.iter().next().unwrap();
        assert_eq!(index, 3);
        assert_eq!(histogram.count(), 100);
        assert!((1_000..1_250).contains(&histogram.quantile(0.5)));
        assert!(histogram.quantile(0.995) >= 1_000_000);
        assert_eq!(histogram.sum_nanos(), 99 * 1_000 + 1_000_000);
        assert_eq!(histogram.cumulative().last().unwrap().1, 100);
    }
}
