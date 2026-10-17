//! The memory limit on a real server: with an `allkeys` policy a write that fits the limit is
//! never refused while older keys can be evicted, whatever the value sizes, the shard count
//! (the limit is split across shards) or the durability profile (unsynced log bytes are not
//! keyspace memory), and the keyspace stays near the limit.

mod common;

use common::{Client, Server};

struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
}

const LIMIT: u64 = 24 * 1024 * 1024;

fn info_field(client: &mut Client, field: &str) -> Option<u64> {
    client
        .call(&["INFO", "memory"])
        .lines()
        .find_map(|line| line.strip_prefix(&format!("{field}:")))
        .and_then(|value| value.trim().parse().ok())
}

fn run(shards: &str, durability: &str, policy: &str) {
    let limit = LIMIT.to_string();
    let server = Server::start_with(&[
        ("RUVIO_MAXMEMORY", &limit),
        ("RUVIO_MEMORY_HEADROOM_BYTES", "1048576"),
        ("RUVIO_MAXMEMORY_POLICY", policy),
        ("RUVIO_SHARDS", shards),
        ("RUVIO_PORT_MODE", "single"),
        ("RUVIO_DURABILITY", durability),
    ]);
    let mut client = Client::connect(&server.address);
    let mut rng = Rng(0x2545_f491_4f6c_dd1d);
    let mut written = 0u64;
    let what = format!("{shards} shard(s), {durability}, {policy}");

    // About four times the limit, in values from 100 bytes to 1 MiB.
    let mut index = 0u64;

    while written < LIMIT * 4 {
        let size = match rng.next() % 10 {
            0 => 1 + rng.next() % (1024 * 1024),
            1..=3 => 1 + rng.next() % (64 * 1024),
            _ => 100 + rng.next() % 2_000,
        } as usize;
        let value = "v".repeat(size);
        let key = format!("key:{index}");
        let reply = client.call(&["SET", &key, &value]);

        if reply != "OK" {
            eprintln!(
                "{what}: #{index} SET {size}: {reply}; dbsize {} {}",
                client.call(&["DBSIZE"]),
                client.call(&["INFO", "memory"]).replace("\r\n", " ")
            );
        }
        assert_eq!(
            reply, "OK",
            "{what}: SET of {size} bytes after {written} bytes written was refused"
        );
        written += size as u64;
        index += 1;
    }

    // The latest write is still there: eviction took older keys, not the new one.
    assert_ne!(
        client.call(&["EXISTS", &format!("key:{}", index - 1)]),
        "0",
        "{what}: the last write was evicted at once"
    );
    let keys: u64 = client.call(&["DBSIZE"]).parse().unwrap();
    assert!(
        keys > 0 && keys < index,
        "{what}: {keys} keys of {index}: nothing was evicted"
    );

    if let Some(used) = info_field(&mut client, "used_memory") {
        assert!(
            used < LIMIT * 2,
            "{what}: used_memory {used} is far past the {LIMIT} byte limit"
        );
    }
}

// One test per configuration, so they run side by side.
#[test]
fn allkeys_lru_eviction_never_refuses_a_write_that_fits() {
    run("1", "memory", "allkeys-lru");
}

#[test]
fn allkeys_lru_eviction_never_refuses_a_write_that_fits_on_two_shards() {
    run("2", "memory", "allkeys-lru");
}

#[test]
fn allkeys_lfu_eviction_never_refuses_a_write_that_fits_with_everysec() {
    run("1", "everysec", "allkeys-lfu");
}

#[test]
fn allkeys_lru_eviction_never_refuses_a_write_that_fits_on_two_shards_with_always() {
    run("2", "always", "allkeys-lru");
}
