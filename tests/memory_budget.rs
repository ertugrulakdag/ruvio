//! Heap bytes per key for the small structures. A change that spends more than the budget
//! (about 15% above today's measurement) fails here instead of being found in a benchmark.
//!
//! One test function runs every case in turn: the counting allocator is process-wide.
//! The HyperLogLog case is the slow one in debug, so it runs over fewer keys; the key table
//! is then less amortized, and its budget is about 15% above the measurement at that count.

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicI64, Ordering::Relaxed};

use ruvio::KvShard;
use ruvio::engine::execute_in_database;
use ruvio::mutation::NoopMutationSink;
use ruvio::protocol::parse_command;

static LIVE: AtomicI64 = AtomicI64::new(0);

struct Count;

// SAFETY: forwards to System; only a counter is added.
unsafe impl GlobalAlloc for Count {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        LIVE.fetch_add(layout.size() as i64, Relaxed);
        // SAFETY: forwards the caller's GlobalAlloc contract to System unchanged.
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        LIVE.fetch_sub(layout.size() as i64, Relaxed);
        // SAFETY: forwards the caller's GlobalAlloc contract to System unchanged.
        unsafe { System.dealloc(pointer, layout) }
    }

    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        LIVE.fetch_add(new_size as i64 - layout.size() as i64, Relaxed);
        // SAFETY: forwards the caller's GlobalAlloc contract to System unchanged.
        unsafe { System.realloc(pointer, layout, new_size) }
    }
}

#[global_allocator]
static ALLOCATOR: Count = Count;

fn frame(parts: &[String]) -> Vec<u8> {
    let mut out = format!("*{}\r\n", parts.len()).into_bytes();

    for part in parts {
        out.extend_from_slice(format!("${}\r\n{}\r\n", part.len(), part).as_bytes());
    }

    out
}

type Case = (&'static str, f64, usize, Box<dyn Fn(usize) -> Vec<String>>);

#[test]
fn bytes_per_key_stay_within_budget() {
    let keys = 50_000;
    let key = |i: usize| format!("k:{i:08}");
    let cases: Vec<Case> = vec![
        (
            "string 16 B",
            58.0,
            keys,
            Box::new(move |i| vec!["SET".into(), key(i), "v".repeat(16)]),
        ),
        (
            "string 16 B with TTL",
            105.0,
            keys,
            Box::new(move |i| {
                vec![
                    "SET".into(),
                    key(i),
                    "v".repeat(16),
                    "EX".into(),
                    "100000".into(),
                ]
            }),
        ),
        (
            "list of 1",
            56.0,
            keys,
            Box::new(move |i| vec!["RPUSH".into(), key(i), "element-0".into()]),
        ),
        (
            "list of 20",
            330.0,
            keys,
            Box::new(move |i| {
                let mut v = vec!["RPUSH".to_string(), key(i)];
                v.extend((0..20).map(|j| format!("element-{j}")));
                v
            }),
        ),
        (
            "hash of 3",
            64.0,
            keys,
            Box::new(move |i| {
                let mut v = vec!["HSET".to_string(), key(i)];
                v.extend(["f1", "v1", "f2", "v2", "f3", "v3"].map(String::from));
                v
            }),
        ),
        (
            "set of 4",
            56.0,
            keys,
            Box::new(move |i| {
                vec![
                    "SADD".into(),
                    key(i),
                    "a".into(),
                    "b".into(),
                    "c".into(),
                    "d".into(),
                ]
            }),
        ),
        (
            "sorted set of 3",
            56.0,
            keys,
            Box::new(move |i| {
                vec![
                    "ZADD".into(),
                    key(i),
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
            "HyperLogLog of 100",
            470.0,
            20_000,
            Box::new(move |i| {
                let mut v = vec!["PFADD".to_string(), key(i)];
                v.extend((0..100).map(|j| format!("m{j}")));
                v
            }),
        ),
    ];

    for (name, budget, keys, make) in &cases {
        let frames: Vec<Vec<u8>> = (0..*keys).map(|i| frame(&make(i))).collect();
        let mut shard = KvShard::new();
        let before = LIVE.load(Relaxed);

        for bytes in &frames {
            let (command, _) = parse_command(bytes).unwrap().unwrap();

            execute_in_database(&mut shard, command, &mut NoopMutationSink, &mut (), 0).unwrap();
        }

        let per_key = (LIVE.load(Relaxed) - before) as f64 / *keys as f64;

        assert!(
            per_key <= *budget,
            "{name}: {per_key:.1} B per key, budget {budget}"
        );
        drop(shard);
    }
}
