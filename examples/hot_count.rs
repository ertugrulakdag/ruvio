//! Local proxy for benches/hot_path on macOS: instructions retired per measured
//! call (command + shard drop, like iai-callgrind's wrapper), via proc_pid_rusage.
//!
//! Build with `RUSTFLAGS="--cfg count_allocs"` to also count allocations.
#![allow(unexpected_cfgs)]
use std::hint::black_box;

use ruvio::{KvShard, engine::execute_hot_into, protocol::HotCommand};

const KEY: &[u8] = b"bench:key";
const VALUE: &[u8] = b"0123456789abcdef";

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};

static FREES: AtomicU64 = AtomicU64::new(0);
static ALLOCS: AtomicU64 = AtomicU64::new(0);
static BIG_FREES: AtomicU64 = AtomicU64::new(0);

static LOG: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);
static SIZES: [AtomicU64; 64] = [const { AtomicU64::new(0) }; 64];
// Only installed as the global allocator under `--cfg count_allocs`.
#[allow(dead_code)]
struct Count;
// SAFETY: forwards to System.
unsafe impl GlobalAlloc for Count {
    unsafe fn alloc(&self, l: Layout) -> *mut u8 {
        ALLOCS.fetch_add(1, Relaxed);
        // SAFETY: forwards the caller's GlobalAlloc contract to System unchanged.
        unsafe { System.alloc(l) }
    }
    unsafe fn alloc_zeroed(&self, l: Layout) -> *mut u8 {
        ALLOCS.fetch_add(1, Relaxed);
        // SAFETY: forwards the caller's GlobalAlloc contract to System unchanged.
        unsafe { System.alloc_zeroed(l) }
    }
    unsafe fn dealloc(&self, p: *mut u8, l: Layout) {
        let i = FREES.fetch_add(1, Relaxed) as usize;
        if LOG.load(Relaxed) {
            SIZES[i % 64].store(l.size() as u64, Relaxed);
        }
        if l.size() > 64 * 1024 {
            BIG_FREES.fetch_add(1, Relaxed);
        }
        // SAFETY: forwards the caller's GlobalAlloc contract to System unchanged.
        unsafe { System.dealloc(p, l) }
    }
    unsafe fn realloc(&self, p: *mut u8, l: Layout, n: usize) -> *mut u8 {
        ALLOCS.fetch_add(1, Relaxed);
        // SAFETY: forwards the caller's GlobalAlloc contract to System unchanged.
        unsafe { System.realloc(p, l, n) }
    }
}

#[cfg(count_allocs)]
#[global_allocator]
static GLOBAL: Count = Count;

#[cfg(target_os = "macos")]
unsafe extern "C" {
    fn proc_pid_rusage(pid: i32, flavor: i32, buffer: *mut u64) -> i32;
    fn getpid() -> i32;
}

#[cfg(target_os = "macos")]
#[inline(never)]
fn instructions() -> u64 {
    let mut buf = [0u64; 64];
    // SAFETY: buffer is larger than rusage_info_v4.
    unsafe { proc_pid_rusage(getpid(), 4, buf.as_mut_ptr()) };
    buf[31]
}

// proc_pid_rusage is macOS only; elsewhere the example still builds (so `cargo test
// --all-targets` links) and main exits before measuring.
#[cfg(not(target_os = "macos"))]
fn instructions() -> u64 {
    0
}

fn warm() -> (KvShard, Vec<u8>) {
    let mut shard = KvShard::new();
    let mut output = Vec::with_capacity(4096);

    shard.set_bytes(KEY, VALUE);
    shard.increment(b"bench:counter", 1).unwrap();
    shard
        .hash_set(b"bench:hash", &[(b"f".as_slice(), VALUE)])
        .unwrap();
    shard.sorted_set_add(b"bench:zset", 1.0, b"m").unwrap();
    shard.list_push_front(b"bench:list", &[VALUE]).unwrap();
    execute_hot_into(&mut shard, HotCommand::Get(KEY), &mut output).unwrap();
    output.clear();

    (shard, output)
}

#[inline(never)]
fn run(name: &str, mut shard: KvShard, mut output: Vec<u8>, drop_shard: bool) -> Vec<u8> {
    match name {
        "get" => {
            execute_hot_into(&mut shard, black_box(HotCommand::Get(KEY)), &mut output).unwrap();
        }
        "set" => {
            execute_hot_into(
                &mut shard,
                black_box(HotCommand::Set(KEY, VALUE, None)),
                &mut output,
            )
            .unwrap();
        }
        "incr" => {
            execute_hot_into(
                &mut shard,
                black_box(HotCommand::Increment(b"bench:counter", 1)),
                &mut output,
            )
            .unwrap();
        }
        "hset" => {
            black_box(
                shard
                    .hash_set(b"bench:hash", &[(black_box(b"f".as_slice()), VALUE)])
                    .unwrap(),
            );
        }
        "zadd" => {
            black_box(
                shard
                    .sorted_set_add(b"bench:zset", black_box(2.0), b"m")
                    .is_ok(),
            );
        }
        "lpush" => {
            black_box(
                shard
                    .list_push_front(b"bench:list", &[black_box(VALUE)])
                    .unwrap(),
            );
        }
        _ => {}
    }
    if drop_shard {
        drop(shard);
    } else {
        std::mem::forget(shard);
    }
    output
}

fn measure(name: &str, drop_shard: bool, n: usize) -> u64 {
    let mut samples = Vec::with_capacity(n);
    for _ in 0..n {
        let (shard, output) = warm();
        let name = black_box(name);
        let (f0, a0, b0) = (
            FREES.load(Relaxed),
            ALLOCS.load(Relaxed),
            BIG_FREES.load(Relaxed),
        );
        LOG.store(samples.is_empty(), Relaxed);
        let t0 = instructions();
        let out = run(name, shard, output, drop_shard);
        LOG.store(false, Relaxed);
        let t1 = instructions();
        if cfg!(count_allocs) && samples.is_empty() {
            println!(
                "  {name} drop={drop_shard} frees {} allocs {} big-frees {}",
                FREES.load(Relaxed) - f0,
                ALLOCS.load(Relaxed) - a0,
                BIG_FREES.load(Relaxed) - b0
            );
            let sizes: Vec<u64> = (f0..FREES.load(Relaxed))
                .map(|i| SIZES[i as usize % 64].load(Relaxed))
                .collect();
            println!("    sizes {sizes:?}");
        }
        black_box(out);
        samples.push(t1 - t0);
    }
    samples.sort_unstable();
    samples[n / 2]
}

#[inline(never)]
fn one(name: &str, shard: &mut KvShard, output: &mut Vec<u8>) {
    output.clear();
    match name {
        "get" => execute_hot_into(shard, black_box(HotCommand::Get(KEY)), output).unwrap(),
        "set" => {
            execute_hot_into(shard, black_box(HotCommand::Set(KEY, VALUE, None)), output).unwrap()
        }
        "incr" => execute_hot_into(
            shard,
            black_box(HotCommand::Increment(b"bench:counter", 1)),
            output,
        )
        .unwrap(),
        "hset" => {
            black_box(
                shard
                    .hash_set(b"bench:hash", &[(black_box(b"f".as_slice()), VALUE)])
                    .unwrap(),
            );
        }
        "zadd" => {
            black_box(
                shard
                    .sorted_set_add(b"bench:zset", black_box(2.0), b"m")
                    .is_ok(),
            );
        }
        _ => {}
    }
}

fn main() {
    if !cfg!(target_os = "macos") {
        eprintln!(
            "hot_count reads instruction counts through proc_pid_rusage and runs on macOS only"
        );
        return;
    }
    if std::env::args().nth(1).as_deref() == Some("loop") {
        const N: u64 = 200_000;
        for name in ["none", "get", "set", "incr", "hset", "zadd"] {
            let (mut shard, mut output) = warm();
            for _ in 0..1000 {
                one(name, &mut shard, &mut output);
            }
            let t0 = instructions();
            for _ in 0..N {
                one(black_box(name), &mut shard, &mut output);
            }
            let t1 = instructions();
            println!("loop {name:5} {:.1}", (t1 - t0) as f64 / N as f64);
        }
        return;
    }
    let n: usize = std::env::args().nth(1).map_or(400, |v| v.parse().unwrap());
    let base = measure("none", false, n);
    let drop_only = measure("none", true, n);
    println!("calib {base}  drop-only {}", drop_only - base);
    for name in ["get", "set", "incr", "hset", "zadd", "lpush"] {
        let with = measure(name, true, n) - base;
        let without = measure(name, false, n) - base;
        println!("{name:6} with-drop {with:6}  cmd-only {without:6}");
    }
}
