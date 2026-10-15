//! Runs against a sharded server when `RUVIO_CLUSTER_ADDR` is set, for example
//! `RUVIO_CLUSTER_ADDR=127.0.0.1:16400 cargo test -p ruvio-client --test cluster_live`.

use std::net::SocketAddr;
use std::sync::{Mutex, MutexGuard};

use ruvio_client::{Client, hash_slot};

// Both tests flush and count keys in the one database a cluster has, so they take turns.
static TURN: Mutex<()> = Mutex::new(());

fn turn() -> MutexGuard<'static, ()> {
    TURN.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn connect() -> Option<Client> {
    let address: SocketAddr = std::env::var("RUVIO_CLUSTER_ADDR").ok()?.parse().ok()?;
    let mut client = Client::connect(address.ip().to_string(), address.port()).unwrap();

    client
        .set_read_timeout(Some(std::time::Duration::from_secs(10)))
        .unwrap();

    Some(client)
}

#[test]
fn routes_keys_and_splits_mget() {
    let _turn = turn();
    let Some(mut db) = connect() else { return };

    assert!(db.is_cluster());
    assert!(db.shard_count() >= 2);
    assert_eq!(hash_slot("somekey"), 11_058);
    db.flushdb().unwrap();
    db.set("{user1000}.following", "left").unwrap();
    db.set("somekey", "right").unwrap();
    assert_eq!(
        db.mget(&["{user1000}.following", "somekey"]).unwrap(),
        vec![Some("left".to_owned()), Some("right".to_owned())]
    );
    assert_eq!(db.cluster_keyslot("somekey").unwrap(), 11_058);
    assert!(db.cluster_info().unwrap().contains("cluster_state:ok"));
    assert_eq!(db.dbsize().unwrap(), 2);
    db.readonly().unwrap();
    db.readwrite().unwrap();
}

#[test]
fn renames_a_key_onto_another_slot() {
    let _turn = turn();
    let Some(mut db) = connect() else {
        return;
    };

    let source = "portal-rename-src";
    let destination = "key1-rename-dst";

    assert_ne!(hash_slot(source), hash_slot(destination));
    db.del(&[source, destination]).unwrap();
    db.set(source, "from-screen").unwrap();
    db.execute(&["RENAME", source, destination]).unwrap();
    assert_eq!(
        db.get(destination).unwrap().as_deref(),
        Some(b"from-screen".as_slice())
    );
    assert_eq!(db.get(source).unwrap(), None);
    db.del(&[destination]).unwrap();
}
