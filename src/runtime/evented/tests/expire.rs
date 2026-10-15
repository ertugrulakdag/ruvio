use super::*;

#[test]
fn expiration_progresses_during_continuous_connection_accepts() {
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    let (address, stop, server) = serve_on_ephemeral();
    assert_eq!(
        request(address, &resp(&[&[b"SET", b"due", b"v", b"PX", b"50"]])),
        b"+OK\r\n"
    );

    let running = Arc::new(AtomicBool::new(true));
    let accepted = Arc::new(AtomicUsize::new(0));
    let flood_running = Arc::clone(&running);
    let flood_accepted = Arc::clone(&accepted);
    let flood = thread::spawn(move || {
        while flood_running.load(Ordering::Relaxed) {
            let stream = StdTcpStream::connect(address).unwrap();
            drop(stream);
            flood_accepted.fetch_add(1, Ordering::Relaxed);
            thread::sleep(Duration::from_millis(1));
        }
    });

    thread::sleep(Duration::from_millis(350));
    let info = String::from_utf8(request(address, &resp(&[&[b"INFO"]]))).unwrap();
    running.store(false, Ordering::Relaxed);
    flood.join().unwrap();
    stop.request();
    server.join().unwrap();

    assert!(
        accepted.load(Ordering::Relaxed) >= 20,
        "connection flood was too slow"
    );
    assert!(info.contains("keys:0\r\nexpired_keys:1"), "{info}");
}

#[test]
fn expiration_progresses_with_a_long_lived_pipelined_client() {
    let (address, stop, server) = serve_on_ephemeral();
    let mut client = StdTcpStream::connect(address).unwrap();
    client
        .set_read_timeout(Some(Duration::from_secs(2)))
        .unwrap();
    assert_eq!(
        request(address, &resp(&[&[b"SET", b"due", b"v", b"PX", b"50"]])),
        b"+OK\r\n"
    );

    let ping: &[&[u8]] = &[b"PING"];
    let ping_batch = resp(&[ping; 8]);
    let mut replies = [0u8; 8 * 7];
    let started = Instant::now();
    let mut batches = 0;
    while started.elapsed() < Duration::from_millis(350) {
        client.write_all(&ping_batch).unwrap();
        client.read_exact(&mut replies).unwrap();
        assert_eq!(replies.as_slice(), b"+PONG\r\n".repeat(8));
        batches += 1;
        thread::sleep(Duration::from_millis(2));
    }

    let info = String::from_utf8(request(address, &resp(&[&[b"INFO"]]))).unwrap();
    assert!(batches >= 5, "pipelined client was too slow");
    assert!(info.contains("keys:0\r\nexpired_keys:1"), "{info}");
    assert_eq!(request(address, &resp(&[&[b"GET", b"due"]])), b"$-1\r\n");
    drop(client);
    stop.request();
    server.join().unwrap();
}

#[test]
fn active_expiry_publishes_one_database_scoped_notice_and_counter() {
    let (address, stop, server) = serve_with(|config| {
        config.memory.notify_mask = crate::notify::parse_mask("Ex").unwrap();
    });
    let channel = b"__keyevent@1__:expired";
    let mut subscriber = StdTcpStream::connect(address).unwrap();
    subscriber
        .set_read_timeout(Some(Duration::from_secs(2)))
        .unwrap();
    subscriber
        .write_all(&resp(&[&[b"SSUBSCRIBE", channel]]))
        .unwrap();
    let subscription = crate::pubsub::encode_ssubscribe(channel, 1);
    let mut confirmation = vec![0; subscription.len()];
    subscriber.read_exact(&mut confirmation).unwrap();
    assert_eq!(confirmation, subscription);

    assert_eq!(
        request(
            address,
            &resp(&[&[b"SELECT", b"1"], &[b"SET", b"due", b"v", b"PX", b"50"],])
        ),
        b"+OK\r\n+OK\r\n"
    );
    let notice = crate::pubsub::encode_smessage(channel, b"due");
    let mut received = vec![0; notice.len()];
    subscriber.read_exact(&mut received).unwrap();
    assert_eq!(received, notice);
    let metrics = String::from_utf8(request(address, &resp(&[&[b"INFO", b"prometheus"]]))).unwrap();
    assert!(
        metrics
            .lines()
            .any(|line| line.starts_with("ruvio_expired_keys_total{")
                && line.contains("reason=\"active\"")
                && line.ends_with(" 1")),
        "{metrics}"
    );
    stop.request();
    server.join().unwrap();
}

#[test]
fn equal_deadline_storm_drains_without_blocking_unrelated_requests() {
    let (address, stop, server) = serve_with(|config| {
        config.memory.active_expire_keys = 16;
    });
    let mut payload = Vec::new();
    for index in 0..128 {
        let key = format!("storm-{index}");
        payload.extend(resp(&[&[b"SET", key.as_bytes(), b"v", b"PX", b"500"]]));
    }
    let replies = request(address, &payload);
    assert_eq!(replies, b"+OK\r\n".repeat(128));
    let remaining = request(address, &resp(&[&[b"PTTL", b"storm-127"]]));
    let remaining: u64 = std::str::from_utf8(&remaining[1..remaining.len() - 2])
        .unwrap()
        .parse()
        .unwrap();
    assert!(remaining > 0);

    let start = Instant::now();
    let mut ping_latencies = Vec::new();
    let drained_at = loop {
        let began = Instant::now();
        assert_eq!(request(address, &resp(&[&[b"PING"]])), b"+PONG\r\n");
        ping_latencies.push(began.elapsed());
        let info = String::from_utf8(request(address, &resp(&[&[b"INFO"]]))).unwrap();
        if info.contains("keys:0\r\nexpired_keys:128") {
            break start.elapsed();
        }
        assert!(start.elapsed() < Duration::from_secs(3), "{info}");
        thread::sleep(Duration::from_millis(10));
    };
    stop.request();
    server.join().unwrap();

    ping_latencies.sort_unstable();
    let p95 = ping_latencies[ping_latencies.len() * 95 / 100];
    eprintln!(
        "128 simultaneous TTLs: drain={drained_at:?}, cleanup lag~{:?}, PING p95={p95:?}, max={:?}",
        drained_at.saturating_sub(Duration::from_millis(remaining)),
        ping_latencies.last().unwrap()
    );
}

#[test]
fn mixed_expiration_load_drains_while_ping_latency_is_measured() {
    use std::sync::atomic::{AtomicBool, Ordering};

    let (address, stop, server) = serve_with(|config| {
        config.memory.active_expire_keys = 8;
    });
    let running = Arc::new(AtomicBool::new(true));
    let ping_running = Arc::clone(&running);
    let pinger = thread::spawn(move || {
        let mut latencies = Vec::new();
        while ping_running.load(Ordering::Relaxed) {
            let began = Instant::now();
            assert_eq!(request(address, &resp(&[&[b"PING"]])), b"+PONG\r\n");
            latencies.push(began.elapsed());
            thread::sleep(Duration::from_millis(2));
        }
        latencies
    });

    let started = Instant::now();
    let mut rounds = 0;
    while started.elapsed() < Duration::from_secs(2) {
        let database = if rounds % 2 == 0 { b"0" } else { b"1" };
        let key = format!("mixed-{rounds}");
        let hash = format!("hash-{rounds}");
        let reply = request(
            address,
            &resp(&[
                &[b"SELECT", database],
                &[b"SET", key.as_bytes(), b"v", b"PX", b"120"],
                &[b"HSET", hash.as_bytes(), b"field", b"v"],
                &[b"PEXPIRE", hash.as_bytes(), b"120"],
                &[b"GET", key.as_bytes()],
            ]),
        );
        assert_eq!(reply, b"+OK\r\n+OK\r\n:1\r\n:1\r\n$1\r\nv\r\n");
        rounds += 1;
        thread::sleep(Duration::from_millis(10));
    }
    running.store(false, Ordering::Relaxed);
    let mut latencies = pinger.join().unwrap();
    let drain_started = Instant::now();
    loop {
        let info = String::from_utf8(request(address, &resp(&[&[b"INFO"]]))).unwrap();
        if info.contains(&format!("keys:0\r\nexpired_keys:{}", rounds * 2)) {
            break;
        }
        assert!(drain_started.elapsed() < Duration::from_secs(3), "{info}");
        thread::sleep(Duration::from_millis(10));
    }
    let drain_lag = drain_started.elapsed();
    stop.request();
    server.join().unwrap();

    assert!(rounds >= 20, "mixed workload was too slow: {rounds}");
    assert!(latencies.len() >= 20, "PING sample was too small");
    latencies.sort_unstable();
    eprintln!(
        "2s mixed TTL load: rounds={rounds}, expired={}, drain after writes={drain_lag:?}, PING p99={:?}, max={:?}",
        rounds * 2,
        latencies[latencies.len() * 99 / 100],
        latencies.last().unwrap()
    );
}

#[test]
fn expired_deadlines_remain_expired_after_durable_restart() {
    let directory = tempfile::tempdir().unwrap();
    let persistence = PersistenceConfig {
        profile: crate::DurabilityProfile::Always,
        data_dir: directory.path().to_path_buf(),
        ..PersistenceConfig::default()
    };
    let (address, stop, server) = serve_with(|config| {
        config.persistence = persistence.clone();
    });
    assert_eq!(
        request(
            address,
            &resp(&[
                &[b"SELECT", b"1"],
                &[b"SET", b"due", b"old", b"PX", b"150"],
                &[b"SET", b"kept", b"v", b"PX", b"150"],
                &[b"PERSIST", b"kept"],
            ])
        ),
        b"+OK\r\n+OK\r\n+OK\r\n:1\r\n"
    );
    stop.request();
    server.join().unwrap();
    thread::sleep(Duration::from_millis(200));

    let (address, stop, server) = serve_with(|config| {
        config.persistence = persistence;
    });
    assert_eq!(
        request(
            address,
            &resp(&[
                &[b"SELECT", b"1"],
                &[b"GET", b"due"],
                &[b"PTTL", b"due"],
                &[b"GET", b"kept"],
                &[b"PTTL", b"kept"],
                &[b"SET", b"due", b"new", b"NX"],
                &[b"GET", b"due"],
            ])
        ),
        b"+OK\r\n$-1\r\n:-2\r\n$1\r\nv\r\n:-1\r\n+OK\r\n$3\r\nnew\r\n"
    );
    stop.request();
    server.join().unwrap();
}
