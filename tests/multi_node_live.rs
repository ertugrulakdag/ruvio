//! Two nodes of one multi-node cluster, each with two shards: every node serves the slots of
//! its own group, answers `MOVED` with the owning shard's address for the others, and
//! describes the whole map in `CLUSTER SLOTS`.

mod common;

use common::{Client, Server};
use ruvio::routing::hash_slot;

fn key_in(range: std::ops::Range<u16>, label: &str) -> String {
    (0..)
        .map(|n| format!("{label}:{n}"))
        .find(|key| range.contains(&hash_slot(key.as_bytes())))
        .unwrap()
}

fn write_topology(path: &std::path::Path, a_port: u16, b_port: u16) {
    std::fs::write(
        path,
        format!(
            "version = 7\n\
             [[groups]]\nname = \"a\"\naddress = \"127.0.0.1:{a_port}\"\nshards = 2\nslots = [\"0-8191\"]\n\
             [[groups]]\nname = \"b\"\naddress = \"127.0.0.1:{b_port}\"\nshards = 2\nslots = [\"8192-16383\"]\n"
        ),
    )
    .unwrap();
}

#[test]
fn a_node_holding_keys_the_map_gives_away_refuses_to_start() {
    let mut a = Server::start_with(&[("RUVIO_SHARDS", "2"), ("RUVIO_DURABILITY", "always")]);
    let dir = tempfile::tempdir().unwrap();
    let topology = dir.path().join("topology.toml");
    let elsewhere = key_in(8192..16384, "elsewhere");

    // Written while the node owned every slot.
    Client::connect(&a.ports().address_at(1).to_string()).call(&["SET", &elsewhere, "1"]);
    write_topology(&topology, a.ports().port_at(0), a.ports().port_at(2));
    a.set_env("RUVIO_CLUSTER_TOPOLOGY", topology.to_str().unwrap());
    a.set_env("RUVIO_CLUSTER_GROUP", "a");

    let log = a.restart_refused();

    assert!(
        log.contains("does not serve under cluster_topology"),
        "{log}"
    );
}

#[test]
fn each_node_serves_its_slots_and_redirects_the_rest() {
    let mut a = Server::start_with(&[("RUVIO_SHARDS", "2")]);
    let mut b = Server::start_with(&[("RUVIO_SHARDS", "2")]);
    let dir = tempfile::tempdir().unwrap();
    let topology = dir.path().join("topology.toml");
    let (a_port, b_port) = (a.ports().port_at(0), b.ports().port_at(0));

    write_topology(&topology, a_port, b_port);

    for (server, group) in [(&mut a, "a"), (&mut b, "b")] {
        server.set_env("RUVIO_CLUSTER_TOPOLOGY", topology.to_str().unwrap());
        server.set_env("RUVIO_CLUSTER_GROUP", group);
        server.restart();
    }

    let local = key_in(0..8192, "here");
    // Slot 12288 and above is shard 1 of group b.
    let remote = key_in(12288..16384, "there");
    let remote_slot = hash_slot(remote.as_bytes());
    let moved = format!("ERR:MOVED {remote_slot} 127.0.0.1:{}", b_port + 1);
    let mut on_a = Client::connect(&a.address);
    let mut on_b1 = Client::connect(&b.ports().address_at(1).to_string());

    // Local keys run on either shard of node a; remote ones are redirected, on the lean
    // path (GET, SET) and the generic one (APPEND) alike.
    assert_eq!(on_a.call(&["SET", &local, "1"]), "OK");
    assert_eq!(on_a.call(&["GET", &local]), "1");
    assert_eq!(on_a.call(&["SET", &remote, "1"]), moved);
    assert_eq!(on_a.call(&["GET", &remote]), moved);
    assert_eq!(on_a.call(&["APPEND", &remote, "x"]), moved);

    // The owner takes them.
    assert_eq!(on_b1.call(&["SET", &remote, "2"]), "OK");
    assert_eq!(on_b1.call(&["GET", &remote]), "2");
    let b_local = format!("ERR:MOVED {} 127.0.0.1:", hash_slot(local.as_bytes()));
    assert!(on_b1.call(&["GET", &local]).starts_with(&b_local));

    // A transaction touching another node's key is aborted.
    assert_eq!(on_a.call(&["MULTI"]), "OK");
    assert_eq!(on_a.call(&["SET", &remote, "3"]), moved);
    assert!(on_a.call(&["EXEC"]).starts_with("ERR:EXECABORT"));
    assert_eq!(on_b1.call(&["GET", &remote]), "2");

    // Every node describes the whole map: its two shards, then the other group's.
    let slots = on_a.call(&["CLUSTER", "SLOTS"]);
    for (first, last, port) in [
        (0, 4095, a_port),
        (4096, 8191, a_port + 1),
        (8192, 12287, b_port),
        (12288, 16383, b_port + 1),
    ] {
        let range = format!("[{first},{last},[127.0.0.1,{port},");

        assert!(slots.contains(&range), "{range} missing in {slots}");
    }
    assert_eq!(slots, on_b1.call(&["CLUSTER", "SLOTS"]));

    let info = on_a.call(&["CLUSTER", "INFO"]);
    assert!(info.contains("cluster_current_epoch:7"), "{info}");
    assert!(info.contains("cluster_size:4"), "{info}");
    assert_eq!(
        on_a.call(&["CONFIG", "GET", "cluster-group"]),
        "[cluster-group,a]"
    );

    // A newer map may move another group's primary, not this group's slots.
    let map = |version: u64, a_slots: &str, b_port: u16| {
        format!(
            "version = {version}\n[[groups]]\nname = \"a\"\naddress = \"127.0.0.1:{a_port}\"\nshards = 2\nslots = [\"{a_slots}\"]\n\
             [[groups]]\nname = \"b\"\naddress = \"127.0.0.1:{b_port}\"\nshards = 2\nslots = [\"8192-16383\"]\n"
        )
        .replace("\"8192-16383\"", if a_slots == "0-8191" { "\"8192-16383\"" } else { "\"8191-16383\"" })
    };

    assert_eq!(on_a.call(&["RUVIO", "TOPOLOGY", "VERSION"]), "7");
    assert!(
        on_a.call(&["RUVIO", "TOPOLOGY", "SET", &map(6, "0-8191", 9000)])
            .contains("older")
    );
    assert!(
        on_a.call(&["RUVIO", "TOPOLOGY", "SET", &map(8, "0-8190", 9000)])
            .contains("this group's slots")
    );
    assert_eq!(
        on_a.call(&["RUVIO", "TOPOLOGY", "SET", &map(8, "0-8191", 9000)]),
        "OK"
    );
    assert_eq!(
        on_a.call(&["GET", &remote]),
        format!("ERR:MOVED {remote_slot} 127.0.0.1:9001")
    );
    assert_eq!(on_a.call(&["RUVIO", "TOPOLOGY", "VERSION"]), "8");

    // The newer map survives a restart (kept in the data directory).
    a.restart();
    let mut on_a = Client::connect(&a.address);

    assert_eq!(on_a.call(&["RUVIO", "TOPOLOGY", "VERSION"]), "8");
}
