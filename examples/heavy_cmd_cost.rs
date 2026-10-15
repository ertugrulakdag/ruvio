//! Shard-thread time of O(N) multi-key commands (Mac/Linux): `cargo run --profile release-fast --example heavy_cmd_cost`
use std::time::Instant;

use ruvio::KvShard;
use ruvio::engine::execute_in_database;
use ruvio::mutation::NoopMutationSink;
use ruvio::protocol::parse_command;

fn frame(parts: &[Vec<u8>]) -> Vec<u8> {
    let mut out = format!("*{}\r\n", parts.len()).into_bytes();
    for p in parts {
        out.extend_from_slice(format!("${}\r\n", p.len()).as_bytes());
        out.extend_from_slice(p);
        out.extend_from_slice(b"\r\n");
    }
    out
}

fn b(s: &str) -> Vec<u8> {
    s.as_bytes().to_vec()
}

fn run(shard: &mut KvShard, parts: &[Vec<u8>]) -> f64 {
    let f = frame(parts);
    let t = Instant::now();
    let (command, _) = parse_command(&f).unwrap().unwrap();
    let r = execute_in_database(shard, command, &mut NoopMutationSink, &mut (), 0);
    let us = t.elapsed().as_nanos() as f64 / 1e3;
    assert!(r.is_ok());
    us
}

fn best(shard: &mut KvShard, parts: &[Vec<u8>]) -> f64 {
    (0..5).map(|_| run(shard, parts)).fold(f64::MAX, f64::min)
}

fn main() {
    let mut shard = KvShard::new();
    // Two sets of 200k members with 50% overlap.
    for set in ["s1", "s2"] {
        let base = if set == "s1" { 0 } else { 100_000 };
        for chunk in 0..(200_000 / 500) {
            let mut p = vec![b("SADD"), b(set)];
            for j in 0..500 {
                p.push(format!("member-{:08}", base + chunk * 500 + j).into_bytes());
            }
            run(&mut shard, &p);
        }
    }
    println!(
        "SINTER   200k x 200k   {:10.1} us",
        best(&mut shard, &[b("SINTER"), b("s1"), b("s2")])
    );
    println!(
        "SUNION   200k x 200k   {:10.1} us",
        best(&mut shard, &[b("SUNION"), b("s1"), b("s2")])
    );
    println!(
        "SDIFF    200k x 200k   {:10.1} us",
        best(&mut shard, &[b("SDIFF"), b("s1"), b("s2")])
    );
    println!(
        "SINTERCARD            {:10.1} us",
        best(&mut shard, &[b("SINTERCARD"), b("2"), b("s1"), b("s2")])
    );
    // Bitmaps 8 MiB.
    for key in ["b1", "b2"] {
        run(&mut shard, &[b("SETBIT"), b(key), b("67108863"), b("1")]);
    }
    println!(
        "BITOP AND 8 MiB       {:10.1} us",
        best(
            &mut shard,
            &[b("BITOP"), b("AND"), b("bd"), b("b1"), b("b2")]
        )
    );
    println!(
        "BITCOUNT 8 MiB        {:10.1} us",
        best(&mut shard, &[b("BITCOUNT"), b("b1")])
    );
    // HLLs.
    for key in ["h1", "h2", "h3"] {
        for chunk in 0..20 {
            let mut p = vec![b("PFADD"), b(key)];
            for j in 0..1000 {
                p.push(format!("{key}-{}", chunk * 1000 + j).into_bytes());
            }
            run(&mut shard, &p);
        }
    }
    println!(
        "PFCOUNT 3 HLL         {:10.1} us",
        best(&mut shard, &[b("PFCOUNT"), b("h1"), b("h2"), b("h3")])
    );
    println!(
        "PFMERGE 3 HLL         {:10.1} us",
        best(
            &mut shard,
            &[b("PFMERGE"), b("hd"), b("h1"), b("h2"), b("h3")]
        )
    );
}
