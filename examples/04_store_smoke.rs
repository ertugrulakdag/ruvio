use ruvio::KvShard;

fn main() {
    let mut shard = KvShard::with_capacity(1_024);

    shard.insert(b"service", b"ruvio");
    shard.insert(b"status", b"ready");

    assert_eq!(shard.get(b"service"), Some(&b"ruvio"[..]));
    assert_eq!(shard.get(b"status"), Some(&b"ready"[..]));
    assert_eq!(shard.len(), 2);

    println!(
        "portable shard smoke test passed with {} entries",
        shard.len()
    );
}
