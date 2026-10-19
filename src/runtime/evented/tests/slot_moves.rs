use super::*;
use crate::slot_moves::SlotMoves;

fn serve_durable_pair(
    data_dir: &std::path::Path,
    configure: impl FnOnce(&mut AppConfig),
) -> (
    SocketAddr,
    SocketAddr,
    ShutdownToken,
    thread::JoinHandle<io::Result<()>>,
) {
    let ports = crate::test_ports::reserve();
    let (left, right) = (ports.address_at(0), ports.address_at(1));
    let mut config = AppConfig {
        address: left.to_string(),
        shards: 2,
        advertised_host: "127.0.0.1".into(),
        shutdown_grace: Duration::from_secs(1),
        ..AppConfig::default()
    };

    config.persistence.profile = crate::DurabilityProfile::Always;
    config.persistence.data_dir = data_dir.to_path_buf();
    configure(&mut config);
    let shutdown = ShutdownToken::default();
    let stop = shutdown.clone();
    let server = thread::spawn(move || {
        let _ports = ports;

        run(&config, shutdown)
    });

    (left, right, stop, server)
}

fn stop(shutdown: ShutdownToken, server: thread::JoinHandle<io::Result<()>>) {
    shutdown.request();
    join_within(server, Duration::from_secs(10))
        .unwrap()
        .unwrap();
}

#[test]
fn a_moved_range_is_served_by_its_new_shard_after_a_restart() {
    let dir = tempfile::tempdir().unwrap();
    let moved = key_on_shard(0, "moved");
    let stays = key_on_shard(0, "stays");
    let slot = crate::routing::hash_slot(moved.as_bytes());

    assert_ne!(slot, crate::routing::hash_slot(stays.as_bytes()));

    let (left, _, shutdown, server) = serve_durable_pair(dir.path(), |_| {});

    wait_until_listening(left, &server);
    for key in [&moved, &stays] {
        let set = resp(&[&[b"SET", key.as_bytes(), b"before"]]);

        assert_eq!(request_connected(left, &set), b"+OK\r\n");
    }
    stop(shutdown, server);

    // As if the slot had moved to shard 1 before shard 0 wrote anything: shard 0's
    // records for it are not replayed, and shard 1 holds none.
    let mut moves = SlotMoves::new(2);

    moves.begin(slot, slot, 1, 1).unwrap();
    moves.complete(1).unwrap();
    moves.save(dir.path()).unwrap();

    let (left, right, shutdown, server) = serve_durable_pair(dir.path(), |_| {});

    wait_until_listening(left, &server);
    wait_until_listening(right, &server);
    for address in [left, right] {
        let get = resp(&[&[b"GET", moved.as_bytes()]]);

        assert_eq!(request_connected(address, &get), b"$-1\r\n");
    }
    let get = resp(&[&[b"GET", stays.as_bytes()]]);

    assert_eq!(request_connected(left, &get), b"$6\r\n");

    // Written through shard 0, the key goes to shard 1 and survives another restart there.
    let set = resp(&[&[b"SET", moved.as_bytes(), b"after"]]);

    assert_eq!(request_connected(left, &set), b"+OK\r\n");
    stop(shutdown, server);

    let (left, right, shutdown, server) = serve_durable_pair(dir.path(), |_| {});

    wait_until_listening(right, &server);
    let get = resp(&[&[b"GET", moved.as_bytes()]]);

    assert_eq!(request_connected(right, &get), b"$5\r\n");
    let slots = request(left, &resp(&[&[b"CLUSTER", b"SHARDS"]]));
    let text = String::from_utf8_lossy(&slots);

    assert!(text.contains(&format!(":{slot}\r\n")), "{text}");
    stop(shutdown, server);
}

#[test]
fn a_moved_table_refuses_replication() {
    let dir = tempfile::tempdir().unwrap();
    let mut moves = SlotMoves::new(2);

    moves.begin(0, 10, 1, 1).unwrap();
    moves.complete(1).unwrap();
    moves.save(dir.path()).unwrap();

    let (_, _, _, server) = serve_durable_pair(dir.path(), |config| {
        config.replication.role = crate::config::ReplicationRole::Primary;
    });
    let error = join_within(server, Duration::from_secs(10))
        .unwrap()
        .unwrap_err();

    assert!(
        error.to_string().contains("replication is not supported"),
        "{error}"
    );
}

#[test]
fn a_damaged_table_refuses_to_start() {
    let dir = tempfile::tempdir().unwrap();

    std::fs::write(
        dir.path().join(crate::slot_moves::FILE_NAME),
        b"not a table",
    )
    .unwrap();

    let (_, _, _, server) = serve_durable_pair(dir.path(), |_| {});
    let error = join_within(server, Duration::from_secs(10))
        .unwrap()
        .unwrap_err();

    assert!(error.to_string().contains("damaged"), "{error}");
}
