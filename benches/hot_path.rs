//! Instruction counts for one command on a warm shard (Linux, valgrind).
//!
//! The shard is boxed so the harness moves a pointer, not the whole struct, and each benchmark returns
//! it so that dropping it is not counted: neither is a command's cost. The list is warmed with a push and a pop so the size class the measured push lands in
//! already has a page; the first page of a class costs one zeroed 4 KiB allocation, shared by many
//! keys in a real shard.
//!
//! Run: `cargo bench --bench hot_path` (needs `valgrind` and
//! `cargo install iai-callgrind-runner --version 0.16.1`). CI compares the
//! totals with `benchmark/baselines/instructions.json` through
//! `scripts/check_instruction_baseline.py`.

use std::hint::black_box;

use iai_callgrind::{library_benchmark, library_benchmark_group, main};
use ruvio::{KvShard, engine::execute_hot_into, protocol::HotCommand};

const KEY: &[u8] = b"bench:key";
const VALUE: &[u8] = b"0123456789abcdef";

fn warm() -> (Box<KvShard>, Vec<u8>) {
    let mut shard = Box::new(KvShard::new());
    let mut output = Vec::with_capacity(4096);

    shard.set_bytes(KEY, VALUE);
    shard.increment(b"bench:counter", 1).unwrap();
    shard
        .hash_set(b"bench:hash", &[(b"f".as_slice(), VALUE)])
        .unwrap();
    shard.sorted_set_add(b"bench:zset", 1.0, b"m").unwrap();
    shard.list_push_front(b"bench:list", &[VALUE]).unwrap();
    shard.list_push_front(b"bench:list", &[VALUE]).unwrap();
    shard.list_pop_front(b"bench:list").unwrap();
    // Touch the paths once so the measured call sees no first-use setup.
    execute_hot_into(&mut shard, HotCommand::Get(KEY), &mut output).unwrap();
    output.clear();

    (shard, output)
}

#[library_benchmark]
#[bench::get(warm())]
fn get((mut shard, mut output): (Box<KvShard>, Vec<u8>)) -> (Box<KvShard>, Vec<u8>) {
    execute_hot_into(&mut shard, black_box(HotCommand::Get(KEY)), &mut output).unwrap();
    (shard, output)
}

#[library_benchmark]
#[bench::set(warm())]
fn set((mut shard, mut output): (Box<KvShard>, Vec<u8>)) -> (Box<KvShard>, Vec<u8>) {
    execute_hot_into(
        &mut shard,
        black_box(HotCommand::Set(KEY, VALUE, None)),
        &mut output,
    )
    .unwrap();
    (shard, output)
}

#[library_benchmark]
#[bench::incr(warm())]
fn incr((mut shard, mut output): (Box<KvShard>, Vec<u8>)) -> (Box<KvShard>, Vec<u8>) {
    execute_hot_into(
        &mut shard,
        black_box(HotCommand::Increment(b"bench:counter", 1)),
        &mut output,
    )
    .unwrap();
    (shard, output)
}

#[library_benchmark]
#[bench::hset(warm())]
fn hset((mut shard, _): (Box<KvShard>, Vec<u8>)) -> (Box<KvShard>, usize) {
    let added = shard
        .hash_set(b"bench:hash", &[(black_box(b"f".as_slice()), VALUE)])
        .unwrap();
    (shard, added)
}

#[library_benchmark]
#[bench::zadd(warm())]
fn zadd((mut shard, _): (Box<KvShard>, Vec<u8>)) -> (Box<KvShard>, bool) {
    let added = shard
        .sorted_set_add(b"bench:zset", black_box(2.0), b"m")
        .is_ok();
    (shard, added)
}

#[library_benchmark]
#[bench::lpush(warm())]
fn lpush((mut shard, _): (Box<KvShard>, Vec<u8>)) -> (Box<KvShard>, usize) {
    let length = shard
        .list_push_front(b"bench:list", &[black_box(VALUE)])
        .unwrap();
    (shard, length)
}

library_benchmark_group!(name = hot_path; benchmarks = get, set, incr, hset, zadd, lpush);
main!(library_benchmark_groups = hot_path);
