//! Blocked clients on every transport: a client that hangs up while parked, plainly or by
//! a TLS close, gives up its waiter, so data pushed later is not popped for nobody.

use super::*;

/// Parks `BLPOP q 0` over TLS, then closes: with a `close_notify` record (the clean TLS
/// close, which leaves unread bytes in front of the FIN) or by dropping the socket.
fn park_over_tls_and_leave(address: SocketAddr, cert: &std::path::Path, clean_close: bool) {
    let mut client = rustls_client(address, cert);

    client.write_all(&resp(&[&[b"BLPOP", b"q", b"0"]])).unwrap();
    client.flush().unwrap();
    thread::sleep(Duration::from_millis(150));

    if clean_close {
        client.conn.send_close_notify();
        let _ = client.conn.complete_io(&mut client.sock);
        let _ = client.sock.shutdown(std::net::Shutdown::Write);
        // Leave the socket half open: only the close record and the FIN say it is gone.
        thread::sleep(Duration::from_millis(900));
        drop(client);
    } else {
        drop(client);
    }
}

// A hang-up behind unread bytes is seen only where the OS reports the FIN (Linux, macOS).
#[cfg(any(target_os = "linux", target_vendor = "apple"))]
#[test]
fn a_tls_client_that_hangs_up_while_blocked_gives_up_its_waiter() {
    let directory = tempfile::tempdir().unwrap();
    let (cert, key) = crate::tls::write_self_signed_pair(directory.path(), "hangup");
    let cert_for_client = cert.clone();
    let (address, stop, server) = serve_with(|config| {
        config.security = SecurityConfig {
            tls_cert: cert,
            tls_key: key,
            ..SecurityConfig::default()
        };
    });

    for clean_close in [true, false] {
        park_over_tls_and_leave(address, &cert_for_client, clean_close);
        thread::sleep(Duration::from_millis(300));

        let mut client = rustls_client(address, &cert_for_client);

        client
            .write_all(&resp(&[
                &[b"RPUSH", b"q", b"x"],
                &[b"LLEN", b"q"],
                &[b"DEL", b"q"],
            ]))
            .unwrap();
        let mut reply = [0u8; 12];

        client.read_exact(&mut reply).unwrap();
        assert_eq!(
            &reply, b":1\r\n:1\r\n:1\r\n",
            "a TLS client that left (clean close: {clean_close}) still took the pushed value"
        );
    }

    stop.request();
    server.join().unwrap();
}

/// Redis sends the replies of a pipeline as they are produced; a command that parks must not
/// hold back the replies queued in front of it.
#[test]
fn replies_ahead_of_a_blocking_command_in_one_pipeline_are_sent_before_it_parks() {
    let (address, stop, server) = serve_on_ephemeral();

    for waiting in [
        &[b"BLPOP".as_slice(), b"never", b"0"][..],
        &[b"BLMOVE", b"never", b"dst", b"LEFT", b"LEFT", b"0"],
        &[b"BLMPOP", b"0", b"1", b"never", b"LEFT"],
        &[b"BZPOPMIN", b"never", b"0"],
        &[b"XREAD", b"BLOCK", b"0", b"STREAMS", b"never-stream", b"$"],
    ] {
        let mut client = StdTcpStream::connect(address).unwrap();

        client
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        // A write and a read ahead of the parked command, in one segment.
        client
            .write_all(&resp(&[
                &[b"SET", b"ahead", b"1"],
                &[b"GET", b"ahead"],
                waiting,
            ]))
            .unwrap();
        let mut replies = [0u8; 12];

        client.read_exact(&mut replies).unwrap_or_else(|error| {
            panic!(
                "replies were held back behind a parked {}: {error}",
                String::from_utf8_lossy(waiting[0])
            )
        });
        assert_eq!(&replies, b"+OK\r\n$1\r\n1\r\n");
        drop(client);
    }

    stop.request();
    server.join().unwrap();
}

/// A blocking command that finds its data does not wait, so it adds no flush and no WAL
/// sync of its own: `LPUSH` then `BLPOP` in one pipeline syncs exactly as `LPUSH` then
/// `LPOP` does.
#[test]
fn a_blocking_pop_that_does_not_wait_shares_the_pipeline_sync() {
    let directory = tempfile::tempdir().unwrap();
    let persistence = PersistenceConfig {
        profile: crate::DurabilityProfile::Always,
        data_dir: directory.path().to_path_buf(),
        ..PersistenceConfig::default()
    };
    let (address, stop, server) = serve_with(|config| {
        config.persistence = persistence.clone();
    });
    let syncs = || {
        let metrics = request(address, &resp(&[&[b"INFO", b"prometheus"]]));

        String::from_utf8(metrics)
            .unwrap()
            .lines()
            .filter(|line| line.starts_with("ruvio_wal_syncs_total{"))
            .map(|line| line.rsplit(' ').next().unwrap().parse::<u64>().unwrap())
            .sum::<u64>()
    };
    let mut deltas = Vec::new();

    for (pop, reply) in [
        (&[b"LPOP".as_slice(), b"k"][..], &b":1\r\n$1\r\nv\r\n"[..]),
        (
            &[b"BLPOP", b"k", b"0"],
            b":1\r\n*2\r\n$1\r\nk\r\n$1\r\nv\r\n",
        ),
    ] {
        let before = syncs();

        assert_eq!(
            request(address, &resp(&[&[b"LPUSH", b"k", b"v"], pop])),
            reply
        );
        deltas.push(syncs() - before);
    }

    assert_eq!(deltas[1], deltas[0], "syncs for LPOP vs BLPOP: {deltas:?}");
    stop.request();
    server.join().unwrap();
}

/// Pipelines three large GETs and a `BLPOP q 0` behind them from a client that does not
/// read yet, so the replies ahead of the pop are still being flushed.
fn pipeline_big_replies_then_blpop(address: SocketAddr, value_len: usize) -> StdTcpStream {
    let value = vec![b'v'; value_len];

    assert_eq!(
        request(address, &resp(&[&[b"SET", b"big", &value]])),
        b"+OK\r\n"
    );

    let mut client = connect_bounded(address);

    client
        .write_all(&resp(&[
            &[b"GET", b"big"],
            &[b"GET", b"big"],
            &[b"GET", b"big"],
            &[b"BLPOP", b"q", b"0"],
        ]))
        .unwrap();
    client
}

/// A client whose replies cannot be flushed (it never reads) is dropped at the write
/// timeout; its `BLPOP` was never parked, so a later push stays in the list.
#[test]
fn a_blocking_pop_behind_an_unflushable_pipeline_takes_nothing_when_the_client_is_dropped() {
    let (address, stop, server) = serve_with(|config| {
        config.timeouts = ConnectionTimeouts {
            write: Duration::from_millis(300),
            ..config.timeouts
        };
    });
    let stalled = pipeline_big_replies_then_blpop(address, 8 << 20);

    thread::sleep(Duration::from_millis(1500));
    assert_eq!(
        request(address, &resp(&[&[b"RPUSH", b"q", b"x"], &[b"LLEN", b"q"]])),
        b":1\r\n:1\r\n"
    );
    drop(stalled);
    stop.request();
    server.join().unwrap();
}

/// A push that lands while the replies ahead of a `BLPOP` are still being flushed is not
/// missed: the pop takes it once those replies are out.
#[test]
fn a_push_during_the_flush_ahead_of_a_blocking_pop_is_delivered_to_it() {
    let (address, stop, server) = serve_on_ephemeral();
    let value_len = 8 << 20;
    let mut client = pipeline_big_replies_then_blpop(address, value_len);

    thread::sleep(Duration::from_millis(300));
    request(address, &resp(&[&[b"RPUSH", b"q", b"x"]]));

    let bulk = format!("${value_len}\r\n").len() + value_len + 2;
    let pop = b"*2\r\n$1\r\nq\r\n$1\r\nx\r\n";
    let mut replies = vec![0u8; 3 * bulk + pop.len()];

    client
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    client.read_exact(&mut replies).unwrap();
    assert_eq!(&replies[3 * bulk..], pop);
    assert_eq!(request(address, &resp(&[&[b"LLEN", b"q"]])), b":0\r\n");
    stop.request();
    server.join().unwrap();
}

/// `XREAD BLOCK ... $` behind replies still being flushed reads from the end it saw when it
/// first ran, so an entry added during the flush is delivered.
#[test]
fn an_entry_added_during_the_flush_ahead_of_xread_dollar_is_delivered() {
    let (address, stop, server) = serve_on_ephemeral();
    let value_len = 8 << 20;
    let value = vec![b'v'; value_len];

    assert_eq!(
        request(address, &resp(&[&[b"SET", b"big", &value]])),
        b"+OK\r\n"
    );

    let mut client = connect_bounded(address);

    client
        .write_all(&resp(&[
            &[b"GET", b"big"],
            &[b"GET", b"big"],
            &[b"GET", b"big"],
            &[b"XREAD", b"BLOCK", b"0", b"STREAMS", b"s", b"$"],
        ]))
        .unwrap();
    thread::sleep(Duration::from_millis(300));
    assert_eq!(
        request(address, &resp(&[&[b"XADD", b"s", b"5-0", b"f", b"x"]])),
        b"$3\r\n5-0\r\n"
    );

    let bulk = format!("${value_len}\r\n").len() + value_len + 2;
    let read = b"*1\r\n*2\r\n$1\r\ns\r\n*1\r\n*2\r\n$3\r\n5-0\r\n*2\r\n$1\r\nf\r\n$1\r\nx\r\n";
    let mut replies = vec![0u8; 3 * bulk + read.len()];

    client
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    client.read_exact(&mut replies).unwrap();
    assert_eq!(
        String::from_utf8_lossy(&replies[3 * bulk..]),
        String::from_utf8_lossy(read)
    );
    stop.request();
    server.join().unwrap();
}
