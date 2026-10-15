//! Live heap bytes per key in a bare `KvShard` (requested and as rounded by the allocator).
//! Mac/Linux: `cargo run --profile release-fast --example mem_per_key [filter]`
use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicI64, Ordering::Relaxed};

use ruvio::KvShard;
use ruvio::engine::execute_in_database;
use ruvio::mutation::NoopMutationSink;
use ruvio::protocol::parse_command;

static REQ: AtomicI64 = AtomicI64::new(0);
static USABLE: AtomicI64 = AtomicI64::new(0);

#[cfg(target_os = "macos")]
unsafe extern "C" {
    fn malloc_size(p: *const u8) -> usize;
}
#[cfg(not(target_os = "macos"))]
unsafe extern "C" {
    fn malloc_usable_size(p: *mut u8) -> usize;
}

fn usable(p: *mut u8) -> i64 {
    // SAFETY: p came from the system allocator.
    #[cfg(target_os = "macos")]
    unsafe {
        malloc_size(p) as i64
    }
    #[cfg(not(target_os = "macos"))]
    // SAFETY: `p` was returned by the system allocator and is still live.
    unsafe {
        malloc_usable_size(p) as i64
    }
}

struct Count;
// SAFETY: forwards to System; only counters are added.
unsafe impl GlobalAlloc for Count {
    unsafe fn alloc(&self, l: Layout) -> *mut u8 {
        // SAFETY: forwards the caller's GlobalAlloc contract to System unchanged.
        let p = unsafe { System.alloc(l) };
        REQ.fetch_add(l.size() as i64, Relaxed);
        USABLE.fetch_add(usable(p), Relaxed);
        p
    }
    unsafe fn dealloc(&self, p: *mut u8, l: Layout) {
        REQ.fetch_sub(l.size() as i64, Relaxed);
        USABLE.fetch_sub(usable(p), Relaxed);
        // SAFETY: forwards the caller's GlobalAlloc contract to System unchanged.
        unsafe { System.dealloc(p, l) }
    }
    unsafe fn realloc(&self, p: *mut u8, l: Layout, n: usize) -> *mut u8 {
        let old = usable(p);
        // SAFETY: forwards the caller's GlobalAlloc contract to System unchanged.
        let q = unsafe { System.realloc(p, l, n) };
        REQ.fetch_add(n as i64 - l.size() as i64, Relaxed);
        USABLE.fetch_add(usable(q) - old, Relaxed);
        q
    }
}
#[global_allocator]
static A: Count = Count;

fn frame(parts: &[String]) -> Vec<u8> {
    let mut out = format!("*{}\r\n", parts.len()).into_bytes();
    for p in parts {
        out.extend_from_slice(format!("${}\r\n{}\r\n", p.len(), p).as_bytes());
    }
    out
}

type Case = (&'static str, Box<dyn Fn(usize) -> Vec<String>>);

fn main() {
    let filter = std::env::args().nth(1).unwrap_or_default();
    let n = 50_000;
    let cases: Vec<Case> = vec![
        (
            "str16",
            Box::new(|i| vec!["SET".into(), format!("k:{i:08}"), "v".repeat(16)]),
        ),
        (
            "str16+ttl",
            Box::new(|i| {
                vec![
                    "SET".into(),
                    format!("k:{i:08}"),
                    "v".repeat(16),
                    "EX".into(),
                    "100000".into(),
                ]
            }),
        ),
        (
            "list1",
            Box::new(|i| vec!["RPUSH".into(), format!("k:{i:08}"), "element-0".into()]),
        ),
        (
            "list20",
            Box::new(|i| {
                let mut v = vec!["RPUSH".into(), format!("k:{i:08}")];
                v.extend((0..20).map(|j| format!("element-{j}")));
                v
            }),
        ),
        (
            "hash3",
            Box::new(|i| {
                vec![
                    "HSET".into(),
                    format!("k:{i:08}"),
                    "f1".into(),
                    "v1".into(),
                    "f2".into(),
                    "v2".into(),
                    "f3".into(),
                    "v3".into(),
                ]
            }),
        ),
        (
            "hll100",
            Box::new(|i| {
                let mut v = vec!["PFADD".to_string(), format!("k:{i:08}")];
                v.extend((0..100).map(|j| format!("m{j}")));
                v
            }),
        ),
        (
            "zset3",
            Box::new(|i| {
                vec![
                    "ZADD".to_string(),
                    format!("k:{i:08}"),
                    "1".into(),
                    "a".into(),
                    "2".into(),
                    "b".into(),
                    "3".into(),
                    "c".into(),
                ]
            }),
        ),
        (
            "set4",
            Box::new(|i| {
                vec![
                    "SADD".into(),
                    format!("k:{i:08}"),
                    "a".into(),
                    "b".into(),
                    "c".into(),
                    "d".into(),
                ]
            }),
        ),
    ];
    for (name, make) in &cases {
        if !filter.is_empty() && !name.contains(&filter) {
            continue;
        }
        let frames: Vec<Vec<u8>> = (0..n).map(|i| frame(&make(i))).collect();
        let mut shard = KvShard::new();
        let (r0, u0) = (REQ.load(Relaxed), USABLE.load(Relaxed));
        for f in &frames {
            let (command, _) = parse_command(f).unwrap().unwrap();
            execute_in_database(&mut shard, command, &mut NoopMutationSink, &mut (), 0).unwrap();
        }
        let (r1, u1) = (REQ.load(Relaxed), USABLE.load(Relaxed));
        println!(
            "{name:10} requested {:6.1} B/key  allocator {:6.1} B/key",
            (r1 - r0) as f64 / n as f64,
            (u1 - u0) as f64 / n as f64
        );
    }
}
