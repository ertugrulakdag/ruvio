//! Crash recovery on a real server: every data type written, then the process is killed without a
//! clean shutdown, and the restarted server must answer exactly as before. Covers WAL-only recovery,
//! checkpoint + WAL tail recovery, and a second crash after recovery.

mod common;

use common::{Client, Server};

const ENV: &[(&str, &str)] = &[
    ("RUVIO_DURABILITY", "always"),
    ("RUVIO_SNAPSHOT_INTERVAL_MS", "0"),
];

/// Reads back everything the writers produced; the text of each reply is the comparison value.
fn observe(client: &mut Client) -> Vec<(String, String)> {
    let probes: &[&[&str]] = &[
        &["GET", "str"],
        &["GET", "counter"],
        &["GETRANGE", "ranged", "0", "-1"],
        &["LRANGE", "list", "0", "-1"],
        &["SMEMBERS", "set"],
        &["HGETALL", "hash"],
        &["ZRANGE", "zset", "0", "-1", "WITHSCORES"],
        &["XRANGE", "stream", "-", "+"],
        &["XLEN", "stream"],
        &["JSON.GET", "doc", "$"],
        &["BF.EXISTS", "bloom", "alpha"],
        &["BF.EXISTS", "bloom", "never-added"],
        &["PFCOUNT", "hll"],
        &["GETBIT", "bits", "7"],
        &["BITCOUNT", "bits"],
        &["EXISTS", "gone"],
        &["EXISTS", "renamed-from"],
        &["GET", "renamed-to"],
        &["TTL", "ttl-key"],
        &["PTTL", "no-ttl"],
        &["DBSIZE"],
        &["GEOPOS", "geo", "a"],
    ];

    probes
        .iter()
        .map(|args| (args.join(" "), client.call(args)))
        .collect()
}

fn write_everything(client: &mut Client) {
    client.call(&["SET", "str", "hello"]);
    client.call(&["SET", "counter", "1"]);
    client.call(&["INCRBY", "counter", "41"]);
    client.call(&["SET", "ranged", "abcdef"]);
    client.call(&["SETRANGE", "ranged", "2", "ZZ"]);
    client.call(&["RPUSH", "list", "a", "b", "c", "d"]);
    client.call(&["LPUSH", "list", "z"]);
    client.call(&["LTRIM", "list", "0", "3"]);
    client.call(&["LSET", "list", "1", "B"]);
    client.call(&["SADD", "set", "x", "y", "z"]);
    client.call(&["SREM", "set", "y"]);
    client.call(&["HSET", "hash", "f1", "v1", "f2", "v2"]);
    client.call(&["HDEL", "hash", "f1"]);
    client.call(&["ZADD", "zset", "1", "a", "2", "b", "3", "c"]);
    client.call(&["ZINCRBY", "zset", "10", "a"]);
    client.call(&["XADD", "stream", "1-1", "k", "v"]);
    client.call(&["XADD", "stream", "2-1", "k", "w"]);
    client.call(&["XADD", "stream", "3-1", "k", "x"]);
    client.call(&["XDEL", "stream", "2-1"]);
    client.call(&["JSON.SET", "doc", "$", "{\"a\":1,\"b\":[1,2]}"]);
    client.call(&["JSON.SET", "doc", "$.a", "5"]);
    client.call(&["BF.ADD", "bloom", "alpha"]);
    client.call(&["PFADD", "hll", "a", "b", "c", "a"]);
    client.call(&["SETBIT", "bits", "7", "1"]);
    client.call(&["SETBIT", "bits", "9", "1"]);
    client.call(&["SET", "gone", "x"]);
    client.call(&["DEL", "gone"]);
    client.call(&["SET", "renamed-from", "moved"]);
    client.call(&["RENAME", "renamed-from", "renamed-to"]);
    client.call(&["SET", "ttl-key", "t", "EX", "3600"]);
    client.call(&["SET", "no-ttl", "t"]);
    client.call(&["GEOADD", "geo", "13.361389", "38.115556", "a"]);
}

#[test]
fn a_killed_server_recovers_every_type_from_the_wal() {
    let mut server = Server::start_with(ENV);
    let mut client = Client::connect(&server.address);

    write_everything(&mut client);

    let before = observe(&mut client);

    assert_eq!(before[0].1, "hello", "sanity: the writes landed");
    assert_eq!(before[1].1, "42");

    drop(client);
    server.restart();

    let after = observe(&mut Client::connect(&server.address));

    for (b, a) in before.iter().zip(&after) {
        if b.0.starts_with("TTL") {
            let (b, a): (i64, i64) = (b.1.parse().unwrap(), a.1.parse().unwrap());

            assert!(a > 0 && a <= b, "TTL must survive a restart: {b} -> {a}");
        } else {
            assert_eq!(b, a, "{} changed across the crash", b.0);
        }
    }
}

#[test]
fn checkpoint_plus_wal_tail_and_a_second_crash_stay_consistent() {
    let mut server = Server::start_with(ENV);
    let mut client = Client::connect(&server.address);

    write_everything(&mut client);
    assert!(client.call(&["SAVE"]).contains("OK"));

    // Writes after the checkpoint live only in the WAL tail.
    client.call(&["SET", "after-save", "tail"]);
    client.call(&["RPUSH", "list", "tail-item"]);
    client.call(&["DEL", "str"]);

    let before = observe(&mut client);

    drop(client);
    server.restart();

    let mut client = Client::connect(&server.address);

    assert_eq!(client.call(&["GET", "after-save"]), "tail");
    assert_eq!(client.call(&["GET", "str"]), "(nil)");

    let after_first = observe(&mut client);

    for (b, a) in before.iter().zip(&after_first) {
        if !b.0.starts_with("TTL") {
            assert_eq!(b, a, "{} changed across the first crash", b.0);
        }
    }

    // Write more, crash again with no checkpoint in between.
    client.call(&["SET", "second", "round"]);
    drop(client);
    server.restart();

    let mut client = Client::connect(&server.address);

    assert_eq!(client.call(&["GET", "second"]), "round");
    assert_eq!(client.call(&["GET", "after-save"]), "tail");
    assert_eq!(client.call(&["LRANGE", "list", "-1", "-1"]), "[tail-item]");
}

fn observe_sorted_set(client: &mut Client) -> Vec<String> {
    let mut seen = vec![
        client.call(&["ZCARD", "big"]),
        client.call(&["ZRANGE", "big", "0", "-1", "WITHSCORES"]),
        client.call(&["EXISTS", "drained"]),
    ];

    for member in ["m-012000", "m-019000", "m-020100", "m-003000"] {
        seen.push(client.call(&["ZSCORE", "big", member]));
        seen.push(client.call(&["ZRANK", "big", member]));
    }

    seen
}

fn zadd_range(client: &mut Client, key: &str, from: u32, to: u32) {
    for start in (from..to).step_by(500) {
        let entries: Vec<(String, String)> = (start..(start + 500).min(to))
            .map(|index| (index.to_string(), format!("m-{index:06}")))
            .collect();
        let mut args = vec!["ZADD", key];

        for (score, member) in &entries {
            args.extend([score.as_str(), member.as_str()]);
        }

        assert!(client.call(&args).parse::<u32>().is_ok());
    }
}

/// A sorted set spanning many arena chunks, popped from both ends before and after a
/// checkpoint, reads back the same after a crash: the checkpoint holds the popped set and
/// the WAL tail replays the later pops and adds on top of it.
#[test]
fn a_popped_sorted_set_recovers_from_the_checkpoint_and_the_wal_tail() {
    let mut server = Server::start_with(ENV);
    let mut client = Client::connect(&server.address);

    zadd_range(&mut client, "big", 0, 20_000);
    zadd_range(&mut client, "drained", 0, 3_000);

    for _ in 0..20 {
        client.call(&["ZPOPMIN", "big", "250"]);
    }

    client.call(&["ZPOPMAX", "big", "1000"]);
    assert!(client.call(&["SAVE"]).contains("OK"));

    // After the checkpoint: only in the WAL tail.
    for _ in 0..12 {
        client.call(&["ZPOPMIN", "big", "250"]);
    }

    zadd_range(&mut client, "big", 20_000, 20_500);
    client.call(&["ZPOPMAX", "big", "200"]);

    for _ in 0..30 {
        client.call(&["ZPOPMIN", "drained", "100"]);
    }

    let before = observe_sorted_set(&mut client);

    assert_eq!(before[0], "11300", "sanity: the pops landed");
    assert_eq!(before[2], "0", "the drained set is gone");

    drop(client);
    server.restart();

    let mut client = Client::connect(&server.address);

    assert_eq!(observe_sorted_set(&mut client), before);

    // Pop more, crash again with no checkpoint in between.
    client.call(&["ZPOPMIN", "big", "5000"]);

    let before = observe_sorted_set(&mut client);

    drop(client);
    server.restart();
    assert_eq!(
        observe_sorted_set(&mut Client::connect(&server.address)),
        before
    );
}
