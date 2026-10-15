//! In-process eviction pressure benchmark for the production `AHashIndex` shard.
//!
//! Run with `cargo run --release --example 05_eviction_pressure [keys] [writes] [case-filter]`.
//! Use `expired-pressure` to measure admission after a mass expiration with active sweeping disabled.
//! It drives the engine `execute` path without networking, WAL or replication,
//! so results isolate admission and victim selection on one shard.

use std::borrow::Cow;
use std::time::{Duration, Instant};

use ruvio::engine::{Response, execute};
use ruvio::protocol::Command;
use ruvio::{EvictionPolicy, KvShard, MemoryLimits, UnixMillis};

const VALUE: [u8; 64] = [b'v'; 64];
const READS_PER_WRITE: usize = 9;
const HOT_PERCENT: usize = 10;

#[derive(Clone, Copy)]
enum Ttl {
    None,
    /// Every key, written ones included, has a TTL; hot keys expire last.
    All,
    /// One preloaded key in this many has a TTL; written keys never do.
    Sparse(usize),
}

struct Case {
    name: &'static str,
    policy: EvictionPolicy,
    samples: usize,
    ttl: Ttl,
}

struct Rng(u64);

impl Rng {
    fn next(&mut self, bound: usize) -> usize {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        (self.0 % bound as u64) as usize
    }
}

fn key(prefix: char, index: usize) -> Vec<u8> {
    format!("{prefix}{index:08}").into_bytes()
}

fn percentile(sorted: &[Duration], fraction: f64) -> Duration {
    let index = ((sorted.len() as f64 * fraction).ceil() as usize).clamp(1, sorted.len()) - 1;
    sorted[index]
}

fn micros(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1e6
}

fn run(case: &Case, keys: usize, writes: usize) {
    let hot = keys * HOT_PERCENT / 100;
    let mut shard = KvShard::with_capacity(keys);
    let now = shard.now_unix_millis().get();

    for index in 0..keys {
        let name = key('k', index);
        shard.set_bytes(&name, &VALUE);
        let deadline = match case.ttl {
            Ttl::None => None,
            Ttl::All if index < hot => Some(now + 7_200_000),
            Ttl::All => Some(now + 3_600_000 + index as u64),
            Ttl::Sparse(every) => (index % every == 0).then_some(now + 3_600_000),
        };
        if let Some(deadline) = deadline {
            shard.expire_at(&name, UnixMillis::new(deadline));
        }
    }

    // Admit exactly the preloaded set; every new key requires at least one victim.
    shard.apply_memory_limits(MemoryLimits {
        max_shard_bytes: shard.memory_usage().charged_bytes(),
        reserved_headroom_bytes: 0,
        eviction_policy: case.policy,
        maxmemory_samples: case.samples,
        ..MemoryLimits::default()
    });

    let mut rng = Rng(0x9e37_79b9_7f4a_7c15);
    for _ in 0..4 {
        for index in 0..hot {
            execute(&mut shard, Command::Get(Cow::Owned(key('k', index))));
        }
    }

    let before = shard.memory_stats();
    let mut latencies = Vec::with_capacity(writes);
    let mut rejected = 0usize;
    let mut hot_names: Vec<Vec<u8>> = (0..hot).map(|index| key('k', index)).collect();
    let started = Instant::now();

    for write in 0..writes {
        for _ in 0..READS_PER_WRITE {
            let name = &hot_names[rng.next(hot)];
            execute(&mut shard, Command::Get(Cow::Borrowed(name)));
        }

        let name = key('n', write);
        let begin = Instant::now();
        let failed = matches!(
            execute(
                &mut shard,
                Command::set(Cow::Borrowed(&name), Cow::Borrowed(&VALUE)),
            ),
            Response::Error(_)
        );
        if matches!(case.ttl, Ttl::All) && !failed {
            let deadline = now + 3_600_000 + (keys + write) as u64;
            shard.expire_at(&name, UnixMillis::new(deadline));
        }
        latencies.push(begin.elapsed());
        if failed {
            rejected += 1;
        }
    }

    let elapsed = started.elapsed();
    let after = shard.memory_stats();
    hot_names.retain(|name| shard.get(name).is_some());
    let scans = after.eviction_scans - before.eviction_scans;
    let slots = after.eviction_slots_inspected - before.eviction_slots_inspected;
    let write_time: Duration = latencies.iter().sum();
    latencies.sort_unstable();

    println!(
        "{:<22} {:>3} {:>10.0} {:>8.2} {:>8.2} {:>9.2} {:>10.2} {:>7.2}% {:>9} {:>10.1} {:>7.2}% {:>8.0}",
        case.name,
        case.samples,
        writes as f64 / write_time.as_secs_f64(),
        micros(percentile(&latencies, 0.5)),
        micros(percentile(&latencies, 0.99)),
        micros(percentile(&latencies, 0.999)),
        micros(latencies[latencies.len() - 1]),
        rejected as f64 * 100.0 / writes as f64,
        after.evicted_keys - before.evicted_keys,
        if scans == 0 {
            0.0
        } else {
            slots as f64 / scans as f64
        },
        hot_names.len() as f64 * 100.0 / hot as f64,
        (writes * (READS_PER_WRITE + 1)) as f64 / elapsed.as_secs_f64(),
    );
}

fn run_expired_pressure(keys: usize, writes: usize) {
    let mut shard = KvShard::with_capacity(keys);
    let deadline = shard.now_unix_millis().get() + 1_000;
    for index in 0..keys {
        let name = key('k', index);
        shard.set_bytes(&name, &VALUE);
        assert!(shard.expire_at(&name, UnixMillis::new(deadline)));
    }
    shard.apply_memory_limits(MemoryLimits {
        max_shard_bytes: shard.memory_usage().charged_bytes(),
        reserved_headroom_bytes: 0,
        eviction_policy: EvictionPolicy::AllKeysLru,
        maxmemory_samples: 5,
        active_expire_keys: 0,
        ..MemoryLimits::default()
    });
    std::thread::sleep(Duration::from_millis(
        deadline.saturating_sub(shard.now_unix_millis().get()) + 1,
    ));

    let before = shard.memory_stats();
    let mut latencies = Vec::with_capacity(writes);
    let mut rejected = 0usize;
    for index in 0..writes {
        let name = key('n', index);
        let begin = Instant::now();
        let response = execute(
            &mut shard,
            Command::set(Cow::Borrowed(&name), Cow::Borrowed(&VALUE)),
        );
        match response {
            Response::Simple(b"OK") => {}
            Response::Error(_) => rejected += 1,
            other => panic!("unexpected response at {index}: {other:?}"),
        }
        latencies.push(begin.elapsed());
    }
    let after = shard.memory_stats();
    let scans = after.eviction_scans - before.eviction_scans;
    let slots = after.eviction_slots_inspected - before.eviction_slots_inspected;
    let write_time: Duration = latencies.iter().sum();
    latencies.sort_unstable();
    println!(
        "{:<22} {:>3} {:>10.0} {:>8.2} {:>8.2} {:>9.2} {:>10.2} {:>7.2}% {:>9} {:>10.1} {:>8} {:>8.0}",
        "expired-pressure",
        5,
        writes as f64 / write_time.as_secs_f64(),
        micros(percentile(&latencies, 0.5)),
        micros(percentile(&latencies, 0.99)),
        micros(percentile(&latencies, 0.999)),
        micros(latencies[latencies.len() - 1]),
        rejected as f64 * 100.0 / writes as f64,
        after.evicted_keys - before.evicted_keys,
        if scans == 0 {
            0.0
        } else {
            slots as f64 / scans as f64
        },
        "n/a",
        writes as f64 / write_time.as_secs_f64(),
    );
}

fn main() {
    let mut args = std::env::args().skip(1);
    let keys = args
        .next()
        .map_or(200_000, |value| value.parse().expect("keys"));
    let writes = args
        .next()
        .map_or(100_000, |value| value.parse().expect("writes"));
    let filter = args.next().unwrap_or_default();

    let mut cases = Vec::new();
    for samples in [1, 5, 16] {
        cases.push(Case {
            name: "allkeys-lru",
            policy: EvictionPolicy::AllKeysLru,
            samples,
            ttl: Ttl::None,
        });
        cases.push(Case {
            name: "allkeys-lfu",
            policy: EvictionPolicy::AllKeysLfu,
            samples,
            ttl: Ttl::None,
        });
    }
    cases.extend([
        Case {
            name: "volatile-lru all-ttl",
            policy: EvictionPolicy::VolatileLru,
            samples: 5,
            ttl: Ttl::All,
        },
        Case {
            name: "volatile-ttl all-ttl",
            policy: EvictionPolicy::VolatileTtl,
            samples: 5,
            ttl: Ttl::All,
        },
        Case {
            name: "volatile-lru 1/1000",
            policy: EvictionPolicy::VolatileLru,
            samples: 5,
            ttl: Ttl::Sparse(1_000),
        },
        Case {
            name: "volatile-lru none",
            policy: EvictionPolicy::VolatileLru,
            samples: 5,
            ttl: Ttl::None,
        },
    ]);

    if filter == "expired-pressure" {
        println!(
            "keys={keys} writes={writes} expired keys, active sweep=off, reads/write=0 value=64B"
        );
    } else {
        println!(
            "keys={keys} writes={writes} hot={HOT_PERCENT}% reads/write={READS_PER_WRITE} value=64B"
        );
    }
    println!(
        "{:<22} {:>3} {:>10} {:>8} {:>8} {:>9} {:>10} {:>8} {:>9} {:>10} {:>8} {:>8}",
        "case",
        "smp",
        "set/s",
        "p50us",
        "p99us",
        "p99.9us",
        "maxus",
        "oom",
        "evicted",
        "slots/scan",
        "hot-kept",
        "ops/s"
    );
    if filter == "expired-pressure" {
        run_expired_pressure(keys, writes);
    } else {
        for case in cases
            .iter()
            .filter(|case| case.name.contains(filter.as_str()))
        {
            run(case, keys, writes);
        }
    }
}
