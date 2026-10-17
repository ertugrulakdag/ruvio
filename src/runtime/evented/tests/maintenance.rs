use super::*;

#[test]
fn maintenance_batch_is_independent_of_command_sweep() {
    let clock = crate::time::TestClock::new(1_000);
    let mut store = KvShard::with_clock(clock.clone());
    let limits = crate::MemoryLimits {
        active_expire_keys: 1,
        active_expire_maintenance_keys: 32,
        ..crate::MemoryLimits::default()
    };
    store.apply_memory_limits(limits);
    for index in 0..64 {
        let key = format!("due-{index}");
        store.set_bytes(key.as_bytes(), b"v");
        assert!(store.expire_at(key.as_bytes(), UnixMillis::new(2_000)));
    }
    clock.set(2_000);
    assert_eq!(store.expire_due(), 1);
    assert_eq!(
        expire_maintenance(
            &mut store,
            limits.active_expire_maintenance_keys,
            Duration::from_secs(1),
            32,
        ),
        32
    );
    assert_eq!(store.expiration_stats().keys_with_ttl, 31);
    assert_eq!(store.expire_due(), 1);
}

#[test]
fn maintenance_catches_up_to_due_backlog_without_exceeding_key_cap() {
    let clock = crate::time::TestClock::new(1_000);
    let mut store = KvShard::with_clock(clock.clone());
    for index in 0..128 {
        let key = format!("future-{index}");
        store.set_bytes(key.as_bytes(), b"v");
        assert!(store.expire_at(key.as_bytes(), UnixMillis::new(10_000)));
    }
    for index in 0..128 {
        let key = format!("due-{index}");
        store.set_bytes(key.as_bytes(), b"v");
        assert!(store.expire_at(key.as_bytes(), UnixMillis::new(2_000)));
    }
    clock.set(2_000);
    assert_eq!(
        expire_maintenance(&mut store, 16, Duration::from_secs(1), 64),
        64
    );
    assert_eq!(store.expiration_stats().keys_with_ttl, 192);
    assert_eq!(
        expire_maintenance(&mut store, 16, Duration::from_secs(1), 64),
        64
    );
    assert_eq!(store.expiration_stats().keys_with_ttl, 128);
    assert_eq!(store.get(b"future-0"), Some(&b"v"[..]));
}

#[test]
fn background_checkpoint_publishes_while_writes_continue_and_recovers() {
    let directory = tempfile::tempdir().unwrap();
    let persistence = PersistenceConfig {
        profile: crate::DurabilityProfile::Snapshot,
        data_dir: directory.path().to_path_buf(),
        snapshot_interval: Duration::from_millis(200),
        snapshot_min_mutations: 1,
        ..PersistenceConfig::default()
    };
    let (address, stop, server) = serve_with(|config| config.persistence = persistence.clone());
    let write_all = |value: &str| {
        for start in (0..20_000).step_by(500) {
            let commands: Vec<Vec<Vec<u8>>> = (start..start + 500)
                .map(|index| {
                    vec![
                        b"SET".to_vec(),
                        format!("key:{index}").into_bytes(),
                        value.as_bytes().to_vec(),
                    ]
                })
                .collect();
            let borrowed: Vec<Vec<&[u8]>> = commands
                .iter()
                .map(|command| command.iter().map(Vec::as_slice).collect())
                .collect();
            let refs: Vec<&[&[u8]]> = borrowed.iter().map(Vec::as_slice).collect();

            assert_eq!(request(address, &resp(&refs)).len(), 5 * 500);
        }
    };

    write_all("a");

    let deadline = Instant::now() + Duration::from_secs(20);

    while !directory.path().join("CURRENT").exists() {
        assert!(
            Instant::now() < deadline,
            "no background checkpoint published"
        );
        write_all("b");
    }

    write_all("b");
    stop.request();
    join_within(server, PATIENCE).unwrap();

    let (address, stop, server) = serve_with(|config| config.persistence = persistence.clone());

    for index in (0..20_000).step_by(997) {
        let key = format!("key:{index}");

        assert_eq!(
            request(address, &resp(&[&[b"GET", key.as_bytes()]])),
            b"$1\r\nb\r\n",
            "{key}"
        );
    }

    stop.request();
    join_within(server, PATIENCE).unwrap();
}

/// Without a WAL the checkpoint is the only copy: `SAVE` while a background checkpoint is
/// running must save what was written after that checkpoint began.
#[test]
fn save_during_a_background_checkpoint_keeps_writes_made_after_it_began() {
    let directory = tempfile::tempdir().unwrap();
    let persistence = PersistenceConfig {
        profile: crate::DurabilityProfile::Snapshot,
        data_dir: directory.path().to_path_buf(),
        snapshot_interval: Duration::from_secs(3600),
        ..PersistenceConfig::default()
    };
    let (address, stop, server) = serve_with(|config| config.persistence = persistence.clone());

    // One batch: the background checkpoint has no slice before `SAVE` runs.
    assert_eq!(
        request(
            address,
            &resp(&[
                &[b"SET", b"k", b"old"],
                &[b"BGSAVE"],
                &[b"SET", b"k", b"new"],
                &[b"SAVE"],
            ])
        ),
        b"+OK\r\n+Background saving started\r\n+OK\r\n+OK\r\n"
    );
    // Replacing the background checkpoint is not an abort.
    assert_eq!(info_field(address, "snapshot_aborts_total"), Some(0));

    // What `SAVE` wrote, before shutdown writes a checkpoint of its own.
    let saved = tempfile::tempdir().unwrap();

    copy_dir(directory.path(), saved.path());
    stop.request();
    join_within(server, PATIENCE).unwrap();

    let (address, stop, server) = serve_with(|config| {
        config.persistence = PersistenceConfig {
            data_dir: saved.path().to_path_buf(),
            ..persistence.clone()
        };
    });

    assert_eq!(
        request(address, &resp(&[&[b"GET", b"k"]])),
        b"$3\r\nnew\r\n"
    );
    stop.request();
    join_within(server, PATIENCE).unwrap();
}

/// Under `always` a write whose sync fails is refused, yet stays applied in memory. No
/// checkpoint taken afterwards (`SAVE`, a scheduled one, or the final one at shutdown) may
/// hold it, or a restart would revive a write its client was told failed.
#[test]
fn a_write_refused_by_a_failed_always_sync_never_reaches_a_checkpoint() {
    let directory = tempfile::tempdir().unwrap();
    let persistence = PersistenceConfig {
        profile: crate::DurabilityProfile::Always,
        data_dir: directory.path().to_path_buf(),
        snapshot_interval: Duration::from_millis(20),
        ..PersistenceConfig::default()
    };
    let (address, stop, server) = serve_with(|config| config.persistence = persistence.clone());

    assert_eq!(
        request(address, &resp(&[&[b"SET", b"k", b"old"]])),
        b"+OK\r\n"
    );
    crate::persistence::FAILING_SYNC_DIRS
        .lock()
        .unwrap()
        .push(directory.path().to_path_buf());
    let refused = request(address, &resp(&[&[b"SET", b"k", b"new"]]));
    crate::persistence::FAILING_SYNC_DIRS
        .lock()
        .unwrap()
        .retain(|dir| dir != directory.path());
    // The client whose sync failed loses its connection; others would get MISCONF.
    assert!(!refused.starts_with(b"+"), "{refused:?}");

    let saved = request(address, &resp(&[&[b"SAVE"]]));

    assert!(saved.starts_with(b"-"), "{saved:?}");
    // Room for scheduled checkpoints (the interval is short), then a kill: the files as
    // they are now.
    thread::sleep(Duration::from_millis(100));
    let killed = tempfile::tempdir().unwrap();

    copy_dir(directory.path(), killed.path());
    stop.request();
    // Nothing acknowledged is lost, but the shard ends with an error: its WAL failed.
    assert!(
        join_within(server, PATIENCE).is_err(),
        "a shard whose WAL failed exited cleanly"
    );

    for data_dir in [killed.path(), directory.path()] {
        let (address, stop, server) = serve_with(|config| {
            config.persistence = PersistenceConfig {
                data_dir: data_dir.to_path_buf(),
                ..persistence.clone()
            };
        });

        assert_eq!(
            request(address, &resp(&[&[b"GET", b"k"]])),
            b"$3\r\nold\r\n",
            "{}",
            data_dir.display()
        );
        stop.request();
        join_within(server, PATIENCE).unwrap();
    }
}

/// `SAVE` under `everysec` while a background checkpoint runs: when the log cannot be
/// synced, the writes made since that checkpoint began are not replayable, so `SAVE` writes
/// a new one instead of finishing the old.
#[test]
fn save_with_a_failing_wal_sync_writes_a_new_checkpoint() {
    let directory = tempfile::tempdir().unwrap();
    let persistence = PersistenceConfig {
        profile: crate::DurabilityProfile::Everysec,
        data_dir: directory.path().to_path_buf(),
        snapshot_interval: Duration::from_secs(3600),
        sync_interval: Duration::from_secs(3600),
        ..PersistenceConfig::default()
    };
    let (address, stop, server) = serve_with(|config| config.persistence = persistence.clone());

    assert_eq!(
        request(
            address,
            &resp(&[
                &[b"SET", b"k", b"old"],
                &[b"BGSAVE"],
                &[b"SET", b"k", b"new"],
            ])
        ),
        b"+OK\r\n+Background saving started\r\n+OK\r\n"
    );
    crate::persistence::FAILING_SYNC_DIRS
        .lock()
        .unwrap()
        .push(directory.path().to_path_buf());
    let saved_reply = request(address, &resp(&[&[b"SAVE"]]));
    crate::persistence::FAILING_SYNC_DIRS
        .lock()
        .unwrap()
        .retain(|dir| dir != directory.path());
    assert_eq!(saved_reply, b"+OK\r\n");

    // The checkpoint alone, as after a power loss that took the unsynced log with it.
    let saved = tempfile::tempdir().unwrap();

    copy_dir(directory.path(), saved.path());
    std::fs::remove_dir_all(saved.path().join("wal")).unwrap();
    stop.request();
    join_within(server, PATIENCE).unwrap();

    let (address, stop, server) = serve_with(|config| {
        config.persistence = PersistenceConfig {
            data_dir: saved.path().to_path_buf(),
            ..persistence.clone()
        };
    });

    assert_eq!(
        request(address, &resp(&[&[b"GET", b"k"]])),
        b"$3\r\nnew\r\n"
    );
    stop.request();
    join_within(server, PATIENCE).unwrap();
}

/// Without a WAL a failed shutdown checkpoint loses the writes since the last one: the
/// server says so by ending with an error, not as a clean save.
#[test]
fn a_failed_final_checkpoint_without_a_wal_ends_the_server_with_an_error() {
    let directory = tempfile::tempdir().unwrap();
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let config = AppConfig {
        address: address.to_string(),
        shutdown_grace: Duration::from_secs(1),
        persistence: PersistenceConfig {
            profile: crate::DurabilityProfile::Snapshot,
            data_dir: directory.path().to_path_buf(),
            snapshot_interval: Duration::from_secs(3600),
            // Too small for any file with the value below.
            snapshot_max_bytes: 4096,
            ..PersistenceConfig::default()
        },
        ..AppConfig::default()
    };
    let shutdown = ShutdownToken::default();
    let stop = shutdown.clone();
    let server = thread::spawn(move || serve(listener, config, shutdown));
    let value = vec![b'v'; 64 * 1024];

    assert_eq!(
        request(address, &resp(&[&[b"SET", b"k", &value]])),
        b"+OK\r\n"
    );
    stop.request();

    let ended = join_within(server, PATIENCE).unwrap();

    assert!(
        ended
            .as_ref()
            .is_err_and(|error| error.to_string().contains("final checkpoint failed")),
        "{ended:?}"
    );
}

#[test]
fn expired_keys_are_logged_as_one_group_with_one_sync() {
    let directory = tempfile::tempdir().unwrap();
    let config = crate::PersistenceConfig {
        profile: crate::DurabilityProfile::Always,
        data_dir: directory.path().to_path_buf(),
        ..crate::PersistenceConfig::default()
    };
    let (mut sink, _, _) =
        crate::persistence::WalSink::open(config, 0, crate::protocol::ProtocolLimits::default())
            .unwrap();
    let clock = crate::time::TestClock::new(1_000);
    let mut store = KvShard::with_clock(clock.clone());

    store.log_removals(true);
    for index in 0..500 {
        let key = format!("due-{index}");
        store.set_bytes(key.as_bytes(), b"v");
        assert!(store.expire_at(key.as_bytes(), UnixMillis::new(2_000)));
    }
    clock.set(2_000);
    store.expire_cycle(usize::MAX);

    let before = sink.sync_totals().0;
    let last = sink.last_sequence();
    log_expired_removals(&mut store, &mut sink);

    assert_eq!(sink.last_sequence() - last, 500);
    assert!(!store.has_unlogged_removals());
    assert_eq!(
        sink.sync_totals().0 - before,
        1,
        "one sync for the whole batch"
    );
    assert_eq!(
        sink.durable().load(std::sync::atomic::Ordering::Acquire),
        sink.last_sequence()
    );
}

#[test]
fn back_to_back_large_writes_are_admitted_and_maintenance_brings_memory_back() {
    const LIMIT: usize = 8 * 1024 * 1024;
    let (address, stop, server) = serve_with(|config| {
        config.shards = 1;
        config.memory.max_shard_bytes = LIMIT;
        config.memory.reserved_headroom_bytes = 0;
        config.memory.eviction_policy = crate::EvictionPolicy::AllKeysLru;
    });
    let value = [b'v'; 100];

    for start in (0..80_000).step_by(1_000) {
        let keys: Vec<String> = (start..start + 1_000)
            .map(|index| format!("key:{index}"))
            .collect();
        let commands: Vec<[&[u8]; 3]> = keys
            .iter()
            .map(|key| [&b"SET"[..], key.as_bytes(), &value[..]])
            .collect();
        let refs: Vec<&[&[u8]]> = commands.iter().map(|command| &command[..]).collect();

        assert_eq!(request(address, &resp(&refs)), b"+OK\r\n".repeat(1_000));
    }

    // Three large writes in one pipeline, with no pause for maintenance between them.
    let big = vec![b'z'; 512 * 1024];
    let payload = resp(&[
        &[b"SET", b"big:1", &big],
        &[b"SET", b"big:2", &big],
        &[b"SET", b"big:3", &big],
    ]);

    assert_eq!(request(address, &payload), b"+OK\r\n".repeat(3));
    assert!(
        wait_for(Duration::from_secs(20), || {
            info_field(address, "charged_memory_bytes").is_some_and(|bytes| bytes <= LIMIT as u64)
        }),
        "memory did not come back under the limit"
    );
    assert_eq!(
        request(address, &resp(&[&[b"STRLEN", b"big:3"]])),
        b":524288\r\n"
    );

    stop.request();
    join_within(server, PATIENCE).unwrap();
}

#[test]
fn eviction_debt_slice_is_logged_as_one_group_with_one_sync() {
    let directory = tempfile::tempdir().unwrap();
    let config = crate::PersistenceConfig {
        profile: crate::DurabilityProfile::Always,
        data_dir: directory.path().to_path_buf(),
        ..crate::PersistenceConfig::default()
    };
    let (mut sink, _, _) =
        crate::persistence::WalSink::open(config, 0, crate::protocol::ProtocolLimits::default())
            .unwrap();
    let mut store = KvShard::new();

    store.set_eviction_policy(crate::EvictionPolicy::AllKeysLru);
    for index in 0..2_000 {
        store.set_bytes(format!("key:{index}").as_bytes(), &[b'x'; 100]);
    }
    let limit = store.memory_usage().charged_bytes();
    let per_key = store.key_memory_usage(b"key:1999").unwrap();

    store.set_memory_limit(Some(limit));
    // A large value admitted on a debt (see the engine's admission), here set directly.
    store.set_bytes(b"big", &[b'y'; 40_000]);
    store.owe_eviction(b"big");

    let before = sink.sync_totals().0;
    let last = sink.last_sequence();
    let owed = evict_debt_logged(&mut store, &mut sink, Duration::from_secs(5)).unwrap();
    let evicted = 2_001 - store.len();

    assert!(!owed);
    assert!(store.memory_usage().charged_bytes() <= limit);
    // Most of the large value's charge is paid by evicting small keys.
    assert!(evicted * per_key > 36_000, "evicted {evicted}");
    assert_eq!(sink.last_sequence() - last, evicted as u64);
    assert_eq!(sink.sync_totals().0 - before, 1, "one sync for the slice");
    assert_eq!(store.get(b"big"), Some(&[b'y'; 40_000][..]));
}
