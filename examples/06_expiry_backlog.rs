//! In-process expiry backlog probe; no networking, WAL, or competing clients.
//!
//! `cargo run --release --example 06_expiry_backlog -- [due-keys] [future-keys] [budget]`
//! Models one maintenance turn per 100 ms, including a worst-case insertion order
//! with unexpired deadlines ahead of expired ones.

use std::{
    process::Command,
    time::{Duration, Instant},
};

use ruvio::{KvShard, UnixMillis};

fn key(prefix: u8, index: usize) -> Vec<u8> {
    format!("{}-{index:08}", prefix as char).into_bytes()
}

fn rss_bytes() -> Option<usize> {
    let output = Command::new("ps")
        .args(["-o", "rss=", "-p"])
        .arg(std::process::id().to_string())
        .output()
        .ok()?;
    String::from_utf8(output.stdout)
        .ok()?
        .trim()
        .parse::<usize>()
        .ok()
        .map(|kib| kib * 1024)
}

fn main() {
    let mut args = std::env::args().skip(1);
    let due: usize = args.next().map_or(20_000, |n| n.parse().unwrap());
    let future: usize = args.next().map_or(0, |n| n.parse().unwrap());
    let budget: usize = args.next().map_or(16, |n| n.parse().unwrap());
    let updates: usize = args.next().map_or(0, |n| n.parse().unwrap());
    assert!(due > 0 && budget > 0);

    let mut shard = KvShard::with_capacity(due + future);
    let now = shard.now_unix_millis().get();
    let load = Instant::now();
    for index in 0..future {
        let name = key(b'f', index);
        shard.set_bytes(&name, &[b'f'; 64]);
        assert!(shard.expire_at(&name, UnixMillis::new(now + 3_600_000)));
    }
    for index in 0..due {
        let name = key(b'd', index);
        shard.set_bytes(&name, &[b'd'; 64]);
        assert!(shard.expire_at(&name, UnixMillis::new(now + 500)));
    }
    let loaded = load.elapsed();
    let updated = Instant::now();
    for index in 0..updates {
        let name = key(b'f', index % future.max(1));
        if future == 0 {
            break;
        }
        let deadline = now
            + if (index / future) & 1 == 0 {
                1_800_000
            } else {
                7_200_000
            };
        assert!(shard.expire_at(&name, UnixMillis::new(deadline)));
    }
    let updated = updated.elapsed();
    let rss_before = rss_bytes();
    let before = shard.memory_usage().charged_bytes();
    std::thread::sleep(Duration::from_millis(
        (now + 500).saturating_sub(shard.now_unix_millis().get()) + 1,
    ));

    let mut ticks = 0usize;
    let mut first_due = None;
    let mut first_ten = 0usize;
    let mut latencies = Vec::new();
    let started = Instant::now();
    while shard.expiration_stats().expired_keys_active < due as u64 {
        let begin = Instant::now();
        let removed = shard.expire_cycle(budget);
        latencies.push(begin.elapsed());
        ticks += 1;
        if removed > 0 && first_due.is_none() {
            first_due = Some(ticks);
        }
        if ticks == 10 {
            first_ten = shard.expiration_stats().expired_keys_active as usize;
        }
        assert!(
            ticks <= (due + future).div_ceil(budget) * 2 + 10,
            "expiry stalled"
        );
    }
    let elapsed = started.elapsed();
    latencies.sort_unstable();
    let pct = |fraction: f64| {
        let index =
            ((latencies.len() as f64 * fraction).ceil() as usize).clamp(1, latencies.len()) - 1;
        latencies[index].as_secs_f64() * 1e6
    };
    println!(
        "due={due} future={future} budget={budget} updates={updates} loaded_ms={:.2} updated_ms={:.2} ticks={ticks} first_due_tick={} first_10_reaped={first_ten} drain_ms={:.2} charged_before={before} charged_after={} rss_before={rss_before:?} rss_after={:?} tick_p50_us={:.2} tick_p99_us={:.2} tick_max_us={:.2}",
        loaded.as_secs_f64() * 1e3,
        updated.as_secs_f64() * 1e3,
        first_due.unwrap(),
        elapsed.as_secs_f64() * 1e3,
        shard.memory_usage().charged_bytes(),
        rss_bytes(),
        pct(0.5),
        pct(0.99),
        pct(1.0)
    );
}
