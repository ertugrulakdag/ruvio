//! In-process cost of one command (parse + execute + reply), no sockets: ns and allocations
//! per call on a warm shard. Mac-friendly stand-in for callgrind when a change is too small
//! for a socket benchmark to see.
//!
//!     cargo run --profile release-fast --example cmd_cost [filter]
use std::alloc::{GlobalAlloc, Layout, System};
use std::hint::black_box;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::time::Instant;

use ruvio::KvShard;
use ruvio::engine::{execute_in_database, write_response};
use ruvio::mutation::NoopMutationSink;
use ruvio::protocol::parse_command;

static ALLOCS: AtomicU64 = AtomicU64::new(0);
/// Set by `TRACE_ALLOC=1`: prints the backtrace of the first allocation of the timed loop.
static TRACE: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);
struct Count;
// SAFETY: forwards to System; only a counter is added.
unsafe impl GlobalAlloc for Count {
    unsafe fn alloc(&self, l: Layout) -> *mut u8 {
        if TRACE.swap(false, Relaxed) {
            eprintln!("{}", std::backtrace::Backtrace::force_capture());
        }
        ALLOCS.fetch_add(1, Relaxed);
        // SAFETY: forwards the caller's GlobalAlloc contract to System unchanged.
        unsafe { System.alloc(l) }
    }
    unsafe fn dealloc(&self, p: *mut u8, l: Layout) {
        // SAFETY: forwards the caller's GlobalAlloc contract to System unchanged.
        unsafe { System.dealloc(p, l) }
    }
    unsafe fn realloc(&self, p: *mut u8, l: Layout, n: usize) -> *mut u8 {
        ALLOCS.fetch_add(1, Relaxed);
        // SAFETY: forwards the caller's GlobalAlloc contract to System unchanged.
        unsafe { System.realloc(p, l, n) }
    }
}
#[global_allocator]
static A: Count = Count;

fn frame(parts: &[&str]) -> Vec<u8> {
    let mut out = format!("*{}\r\n", parts.len()).into_bytes();
    for p in parts {
        out.extend_from_slice(format!("${}\r\n{}\r\n", p.len(), p).as_bytes());
    }
    out
}

fn run(shard: &mut KvShard, bytes: &[u8], out: &mut Vec<u8>) {
    let (command, _) = parse_command(bytes).unwrap().unwrap();
    out.clear();
    let response = execute_in_database(shard, command, &mut NoopMutationSink, &mut (), 0).unwrap();
    write_response(out, response).unwrap();
}

fn main() {
    let filter = std::env::args().nth(1).unwrap_or_default().to_lowercase();
    let value = "x".repeat(64);
    let setup: Vec<Vec<u8>> = vec![
        frame(&["SET", "k", &value]),
        frame(&["SET", "bm", "0"]),
        frame(&["SETBIT", "bm", "7999", "1"]),
        frame(&["SET", "bf", "0"]),
        frame(&["BITFIELD", "bf", "SET", "u8", "1000", "1"]),
        frame(&["BITFIELD", "bs", "SET", "u8", "8", "1"]),
        frame(&["SET", "kt", &value]),
        frame(&["EXPIRE", "kt", "1000000"]),
        frame(&["PFADD", "hc", "a", "b", "c"]),
    ];
    let rows: Vec<(&str, Vec<&str>)> = vec![
        ("GET", vec!["GET", "k"]),
        ("SET", vec!["SET", "sk", "x"]),
        (
            "BITFIELD INCRBY",
            vec!["BITFIELD", "bf", "INCRBY", "u8", "800", "1"],
        ),
        (
            "BITFIELD INCRBY small",
            vec!["BITFIELD", "bs", "INCRBY", "u8", "8", "1"],
        ),
        ("BITFIELD GET", vec!["BITFIELD", "bf", "GET", "u8", "800"]),
        (
            "BITFIELD 2 ops",
            vec![
                "BITFIELD", "bf", "GET", "u8", "800", "INCRBY", "u8", "800", "1",
            ],
        ),
        ("SETBIT", vec!["SETBIT", "bm", "5000", "1"]),
        ("GETBIT", vec!["GETBIT", "bm", "5000"]),
        ("COPY REPLACE", vec!["COPY", "k", "k2", "REPLACE"]),
        ("COPY REPLACE ttl", vec!["COPY", "kt", "k3", "REPLACE"]),
        ("EXPIRETIME", vec!["EXPIRETIME", "k"]),
        ("EXPIRETIME ttl", vec!["EXPIRETIME", "kt"]),
        ("TTL", vec!["TTL", "kt"]),
        ("PTTL", vec!["PTTL", "k"]),
        ("TOUCH", vec!["TOUCH", "k"]),
        ("EXISTS", vec!["EXISTS", "k"]),
        ("OBJECT ENCODING", vec!["OBJECT", "ENCODING", "k"]),
        ("GETRANGE", vec!["GETRANGE", "k", "0", "31"]),
        ("SETRANGE", vec!["SETRANGE", "k", "4", "xx"]),
        ("EXPIRE ttl", vec!["EXPIRE", "kt", "1000000"]),
        ("PERSIST none", vec!["PERSIST", "k"]),
        ("TYPE", vec!["TYPE", "k"]),
        ("STRLEN", vec!["STRLEN", "k"]),
        ("APPEND", vec!["APPEND", "ap", "x"]),
        ("INCR", vec!["INCR", "ctr"]),
        ("DEL missing", vec!["DEL", "nokey"]),
        ("PFADD", vec!["PFADD", "hll", "elem"]),
        ("PFCOUNT", vec!["PFCOUNT", "hc"]),
        ("DEL hit+miss", vec!["DEL", "k", "nokey"]),
        ("MSET 1", vec!["MSET", "m1", "v"]),
        ("MGET 1", vec!["MGET", "k"]),
        ("LPUSH+LPOP", vec!["LPUSH", "ll", "v"]),
        ("RPUSH", vec!["RPUSH", "lr", "v"]),
        ("LRANGE 0 9", vec!["LRANGE", "lr", "0", "9"]),
        ("HSET", vec!["HSET", "hh", "f", "v"]),
        ("HGET", vec!["HGET", "hh", "f"]),
        ("HGETALL", vec!["HGETALL", "hh"]),
        ("HMGET", vec!["HMGET", "hh", "f", "g"]),
        ("SADD", vec!["SADD", "ss", "m"]),
        ("SISMEMBER", vec!["SISMEMBER", "ss", "m"]),
        ("ZADD", vec!["ZADD", "zz", "1", "m"]),
        ("ZSCORE", vec!["ZSCORE", "zz", "m"]),
        ("ZRANGE 0 9", vec!["ZRANGE", "zz", "0", "9"]),
        ("ZINCRBY", vec!["ZINCRBY", "zz", "1", "m"]),
        ("SETEX", vec!["SETEX", "ex", "100", "v"]),
        ("GETSET-ish GETDEL miss", vec!["GETDEL", "nokey"]),
        ("INCRBY", vec!["INCRBY", "ctr", "5"]),
        ("MGET 2", vec!["MGET", "k", "bf"]),
        ("EXISTS 2", vec!["EXISTS", "k", "bf"]),
        ("SETNX", vec!["SETNX", "k", "v"]),
        ("GETSET", vec!["GETSET", "k", "v"]),
    ];
    println!("{:22} {:>9} {:>8}", "row", "ns/call", "allocs");
    for (label, parts) in rows {
        if !filter.is_empty() && !label.to_lowercase().contains(&filter) {
            continue;
        }
        let mut shard = KvShard::new();
        let mut out = Vec::with_capacity(4096);
        // Only the rows that read a deadline get the key that has one: a shard with
        // deadlines reads the clock on every command (the server caches it per batch).
        let ttl_row = parts.contains(&"kt");
        for (i, s) in setup.iter().enumerate() {
            if ttl_row || i < 6 {
                run(&mut shard, s, &mut out);
            }
        }
        // The server reads the clock once per batch of commands; do the same here.
        shard.sample_time();
        let bytes = frame(&parts);
        for _ in 0..2000 {
            run(&mut shard, &bytes, &mut out);
        }
        let n: u64 = std::env::var("CMD_N")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(1_000_000);
        let mut best = f64::MAX;
        let mut allocs = 0;
        for _ in 0..5 {
            TRACE.store(std::env::var_os("TRACE_ALLOC").is_some(), Relaxed);
            let a0 = ALLOCS.load(Relaxed);
            let t = Instant::now();
            for _ in 0..n {
                run(&mut shard, black_box(&bytes), &mut out);
            }
            best = best.min(t.elapsed().as_nanos() as f64 / n as f64);
            allocs = ALLOCS.load(Relaxed) - a0;
        }
        println!("{label:22} {best:9.1} {:8.2}", allocs as f64 / n as f64);
    }
}
