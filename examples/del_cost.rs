//! Cost of dropping a collection in place, by charged size (Mac/Linux): `cargo run --profile release-fast --example del_cost`
use std::time::Instant;

use ruvio::KvShard;
use ruvio::engine::execute_in_database;
use ruvio::mutation::NoopMutationSink;
use ruvio::protocol::parse_command;

fn frame(parts: &[String]) -> Vec<u8> {
    let mut out = format!("*{}\r\n", parts.len()).into_bytes();
    for p in parts {
        out.extend_from_slice(format!("${}\r\n{}\r\n", p.len(), p).as_bytes());
    }
    out
}

fn run(shard: &mut KvShard, parts: Vec<String>) {
    let f = frame(&parts);
    let (command, _) = parse_command(&f).unwrap().unwrap();
    execute_in_database(shard, command, &mut NoopMutationSink, &mut (), 0).unwrap();
}

fn main() {
    for (kind, add) in [
        ("set", "SADD"),
        ("hash", "HSET"),
        ("zset", "ZADD"),
        ("list", "RPUSH"),
    ] {
        for members in [64usize, 256, 1024, 4096, 16384] {
            let mut samples = Vec::new();
            for round in 0..15 {
                let mut shard = KvShard::new();
                let key = format!("k{round}");
                for chunk in 0..members.div_ceil(64) {
                    let mut p = vec![add.to_string(), key.clone()];
                    for j in 0..64 {
                        let m = chunk * 64 + j;
                        match kind {
                            "hash" => {
                                p.push(format!("field-{m:08}"));
                                p.push(format!("value-{m:08}"));
                            }
                            "zset" => {
                                p.push(format!("{m}"));
                                p.push(format!("member-{m:08}"));
                            }
                            _ => p.push(format!("member-{m:08}-pad")),
                        }
                    }
                    run(&mut shard, p);
                }
                let t = Instant::now();
                run(&mut shard, vec!["DEL".into(), key]);
                samples.push(t.elapsed().as_nanos() as u64);
            }
            samples.sort();
            println!(
                "{kind:5} {members:6} members  DEL median {:8.1} us  max {:8.1} us",
                samples[7] as f64 / 1e3,
                samples[14] as f64 / 1e3
            );
        }
    }
}
