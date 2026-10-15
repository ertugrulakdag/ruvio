//! Shard-thread cost of big write payloads (Mac/Linux): `cargo run --profile release-fast --example big_payload_cost`
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

fn timed(shard: &mut KvShard, parts: Vec<Vec<u8>>) -> (f64, ()) {
    let f = frame(&parts);
    let t = Instant::now();
    let (command, _) = parse_command(&f).unwrap().unwrap();
    let r = execute_in_database(shard, command, &mut NoopMutationSink, &mut (), 0);
    let us = t.elapsed().as_nanos() as f64 / 1e3;
    assert!(r.is_ok());
    (us, ())
}

fn b(s: &str) -> Vec<u8> {
    s.as_bytes().to_vec()
}

fn main() {
    for kib in [16usize, 64, 256, 1024, 4096] {
        let target = kib * 1024;
        let mut doc = String::from("{\"items\":[");
        let mut i = 0;
        while doc.len() < target {
            if i > 0 {
                doc.push(',');
            }
            doc.push_str(&format!("{{\"id\":{i},\"name\":\"item-{i}\",\"tags\":[\"a\",\"b\",\"c\"],\"price\":{}.5,\"ok\":true}}", i % 977));
            i += 1;
        }
        doc.push_str("]}");
        let mut shard = KvShard::new();
        let mut best = f64::MAX;
        for _ in 0..7 {
            let (us, _) = timed(
                &mut shard,
                vec![b("JSON.SET"), b("doc"), b("$"), doc.clone().into_bytes()],
            );
            best = best.min(us);
        }
        println!("JSON.SET  {kib:5} KiB  parse+store {best:9.1} us");
        let value = vec![b'x'; target];
        let mut best_set = f64::MAX;
        for _ in 0..7 {
            let (us, _) = timed(&mut shard, vec![b("SET"), b("blob"), value.clone()]);
            best_set = best_set.min(us);
        }
        println!("SET plain {kib:5} KiB  store       {best_set:9.1} us");
    }
}
