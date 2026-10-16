//! Client CPU and allocations per request: plain (one socket, no discovery) against cluster routing.
//!
//! `cargo run --release --example cluster_cost -- --port 7000`
//!
//! Point it at a per-shard server. Eight threads, one client each, GET and SET at pipeline depths
//! 16 and 64, two seconds per row, about a minute in all. CPU is the process's user and system time (`getrusage`).
//! Rows: `plain` (no discovery), `cluster` (keys spread over the nodes) and `one-node` (cluster
//! client, every key in one hash tag): `one-node` against `plain` is the routing code's own cost,
//! `cluster` adds the writes and reads of the extra nodes. Plain depth 4 is the per-socket batch a
//! depth-16 pipeline leaves on each of four nodes.

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::{Arc, Barrier};
use std::time::{Duration, Instant};

use ruvio_client::{Client, ClientOptions};

struct Counting;

static ALLOCATIONS: AtomicU64 = AtomicU64::new(0);
static BYTES: AtomicU64 = AtomicU64::new(0);

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        ALLOCATIONS.fetch_add(1, Relaxed);
        BYTES.fetch_add(layout.size() as u64, Relaxed);

        unsafe { System.alloc(layout) }
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        ALLOCATIONS.fetch_add(1, Relaxed);
        BYTES.fetch_add(layout.size() as u64, Relaxed);

        unsafe { System.alloc_zeroed(layout) }
    }

    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        unsafe { System.dealloc(pointer, layout) }
    }

    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        ALLOCATIONS.fetch_add(1, Relaxed);
        BYTES.fetch_add(new_size as u64, Relaxed);

        unsafe { System.realloc(pointer, layout, new_size) }
    }
}

#[global_allocator]
static ALLOCATOR: Counting = Counting;

#[repr(C)]
#[derive(Clone, Copy)]
struct TimeValue {
    seconds: i64,
    #[cfg(target_os = "macos")]
    micros: i32,
    #[cfg(target_os = "macos")]
    _pad: i32,
    #[cfg(not(target_os = "macos"))]
    micros: i64,
}

#[repr(C)]
struct Usage {
    user: TimeValue,
    system: TimeValue,
    _rest: [i64; 14],
}

unsafe extern "C" {
    fn getrusage(who: i32, usage: *mut Usage) -> i32;
}

/// Process (user, system) CPU seconds.
fn cpu() -> (f64, f64) {
    let mut usage = Usage {
        user: TimeValue {
            seconds: 0,
            micros: 0,
            #[cfg(target_os = "macos")]
            _pad: 0,
        },
        system: TimeValue {
            seconds: 0,
            micros: 0,
            #[cfg(target_os = "macos")]
            _pad: 0,
        },
        _rest: [0; 14],
    };

    // SAFETY: `usage` is a valid, writable rusage-shaped buffer; 0 is RUSAGE_SELF.
    unsafe { getrusage(0, &mut usage) };

    let seconds = |time: TimeValue| time.seconds as f64 + time.micros as f64 / 1e6;

    (seconds(usage.user), seconds(usage.system))
}

fn arg(name: &str, default: &str) -> String {
    let arguments: Vec<String> = std::env::args().collect();

    arguments
        .iter()
        .position(|argument| argument == name)
        .and_then(|index| arguments.get(index + 1).cloned())
        .unwrap_or_else(|| default.to_owned())
}

fn run(port: u16, mode: &str, op: &str, pipeline: usize, seconds: u64, threads: usize) {
    let cluster = mode != "plain";
    let tag = if mode == "one-node" { "{t}" } else { "" };
    let keys: Arc<Vec<String>> =
        Arc::new((0..4096).map(|index| format!("{tag}key:{index}")).collect());
    let barrier = Arc::new(Barrier::new(threads + 1));
    let workers: Vec<_> = (0..threads)
        .map(|thread| {
            let keys = keys.clone();
            let barrier = barrier.clone();
            let op = op.to_owned();

            std::thread::spawn(move || {
                let mut client = Client::connect_with(ClientOptions {
                    host: "127.0.0.1".to_owned(),
                    port,
                    discover_cluster: cluster,
                    ..ClientOptions::default()
                })
                .expect("connect");
                let value = "x".repeat(32);
                let commands: Vec<Vec<&str>> = keys
                    .iter()
                    .map(|key| match op.as_str() {
                        "SET" => vec!["SET", key.as_str(), value.as_str()],
                        _ => vec!["GET", key.as_str()],
                    })
                    .collect();
                let slices: Vec<&[&str]> = commands.iter().map(Vec::as_slice).collect();
                let span = slices.len() - pipeline;
                let mut index = thread * 997 % span;
                let step = |client: &mut Client, index: &mut usize| {
                    if pipeline == 1 {
                        match op.as_str() {
                            "SET" => drop(client.set(&keys[*index], &value).expect("set")),
                            _ => drop(client.get(&keys[*index]).expect("get")),
                        }
                    } else {
                        let replies = client
                            .execute_many(&slices[*index..*index + pipeline])
                            .expect("pipeline");

                        assert_eq!(replies.len(), pipeline);
                    }

                    *index = (*index + pipeline) % span;
                };
                let warm = Instant::now() + Duration::from_millis(500);

                while Instant::now() < warm {
                    step(&mut client, &mut index);
                }

                barrier.wait();
                barrier.wait();

                let deadline = Instant::now() + Duration::from_secs(seconds);
                let mut ops = 0u64;

                while Instant::now() < deadline {
                    step(&mut client, &mut index);
                    ops += pipeline as u64;
                }

                ops
            })
        })
        .collect();

    barrier.wait();

    let (user_before, system_before) = cpu();
    let allocations_before = ALLOCATIONS.load(Relaxed);
    let bytes_before = BYTES.load(Relaxed);
    let started = Instant::now();

    barrier.wait();

    let total: u64 = workers
        .into_iter()
        .map(|worker| worker.join().expect("worker"))
        .sum();
    let elapsed = started.elapsed().as_secs_f64();
    let (user_after, system_after) = cpu();
    let ops = total as f64;

    println!(
        "{mode:<8} {op} P={pipeline} ops/s={:.0} user_ns/op={:.0} sys_ns/op={:.0} allocs/op={:.2} bytes/op={:.0}",
        ops / elapsed,
        (user_after - user_before) * 1e9 / ops,
        (system_after - system_before) * 1e9 / ops,
        (ALLOCATIONS.load(Relaxed) - allocations_before) as f64 / ops,
        (BYTES.load(Relaxed) - bytes_before) as f64 / ops,
    );
}

fn main() {
    let port: u16 = arg("--port", "7000").parse().expect("port");
    let seconds: u64 = arg("--seconds", "2").parse().expect("seconds");
    let threads: usize = arg("--threads", "8").parse().expect("threads");

    for op in ["GET", "SET"] {
        for (mode, pipeline) in [
            ("plain", 4),
            ("plain", 16),
            ("cluster", 16),
            ("one-node", 16),
            ("plain", 64),
            ("cluster", 64),
            ("one-node", 64),
        ] {
            run(port, mode, op, pipeline, seconds, threads);
        }
    }
}
