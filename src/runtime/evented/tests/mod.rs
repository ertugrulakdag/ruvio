use super::{ClusterRoute, serve_unlocked, *};

use crate::{
    ConnectionLimits, ConnectionTimeouts, PersistenceConfig, SecurityConfig, config::AppConfig,
    mutation::MutationSink, protocol::ProtocolLimits,
};

use std::{
    io::{Read, Write},
    net::{Shutdown, SocketAddr, TcpStream as StdTcpStream},
    sync::Arc,
    thread,
    time::{Duration, Instant},
};

/// A client socket whose reads and writes give up after 10 s, so a test cannot hang.
fn connect_bounded(address: SocketAddr) -> StdTcpStream {
    let client = StdTcpStream::connect(address).unwrap();

    client
        .set_read_timeout(Some(Duration::from_secs(10)))
        .unwrap();
    client
        .set_write_timeout(Some(Duration::from_secs(10)))
        .unwrap();
    client
}

/// Accepts one connection within 10 s (the listener is left non-blocking); the accepted
/// socket is blocking with 10 s read and write timeouts.
fn accept_bounded(listener: &std::net::TcpListener) -> StdTcpStream {
    listener.set_nonblocking(true).unwrap();
    let deadline = Instant::now() + Duration::from_secs(10);

    loop {
        match listener.accept() {
            Ok((stream, _)) => {
                stream.set_nonblocking(false).unwrap();
                stream
                    .set_read_timeout(Some(Duration::from_secs(10)))
                    .unwrap();
                stream
                    .set_write_timeout(Some(Duration::from_secs(10)))
                    .unwrap();
                return stream;
            }
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                assert!(
                    Instant::now() < deadline,
                    "no connection reached the test listener within 10 s"
                );
                thread::sleep(Duration::from_millis(5));
            }
            Err(error) => panic!("test listener accept failed: {error}"),
        }
    }
}

fn request(address: SocketAddr, payload: &[u8]) -> Vec<u8> {
    let mut client = StdTcpStream::connect(address).unwrap();

    client
        .set_read_timeout(Some(Duration::from_secs(2)))
        .unwrap();
    client.write_all(payload).unwrap();
    client.shutdown(Shutdown::Write).unwrap();
    let mut response = Vec::new();

    client.read_to_end(&mut response).unwrap();
    response
}

/// One single-line reply, read with the write side left open: a client still connected,
/// unlike [`request`]'s, whose half-close tells the server it has gone.
fn request_connected(address: SocketAddr, payload: &[u8]) -> Vec<u8> {
    let mut client = StdTcpStream::connect(address).unwrap();

    client
        .set_read_timeout(Some(Duration::from_secs(2)))
        .unwrap();
    client.write_all(payload).unwrap();
    let mut response = Vec::new();
    let mut byte = [0u8; 1];

    while !response.ends_with(b"\r\n") {
        client.read_exact(&mut byte).unwrap();
        response.push(byte[0]);
    }
    response
}

/// Fails when `timeout` elapses. A shared lock held by a blocked peer shows up here.
fn request_within(address: SocketAddr, payload: &[u8], timeout: Duration) -> Vec<u8> {
    let mut client = StdTcpStream::connect(address).unwrap();

    client.set_read_timeout(Some(timeout)).unwrap();
    client.set_write_timeout(Some(timeout)).unwrap();
    client.write_all(payload).unwrap();
    client.shutdown(Shutdown::Write).unwrap();
    let mut response = Vec::new();

    client
        .read_to_end(&mut response)
        .expect("shard did not answer while its peer was blocked");
    response
}

fn key_on_shard(shard: usize, label: &str) -> String {
    for n in 0..4096 {
        let key = format!("{label}-{n}");

        if crate::routing::shard_for_slot(crate::routing::hash_slot(key.as_bytes()), 2) == shard {
            return key;
        }
    }

    panic!("no key landed on shard {shard}");
}

fn serve_with(
    configure: impl FnOnce(&mut AppConfig),
) -> (SocketAddr, ShutdownToken, thread::JoinHandle<()>) {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let mut config = AppConfig {
        address: address.to_string(),
        shutdown_grace: Duration::from_secs(1),
        ..AppConfig::default()
    };

    configure(&mut config);
    let shutdown = ShutdownToken::default();
    let stop = shutdown.clone();
    let server = thread::spawn(move || serve(listener, config, shutdown).unwrap());

    (address, stop, server)
}

/// Copies a data directory as it is now, to start a server on what was saved so far. A
/// shard may still be writing a later checkpoint meanwhile; a copy that saw one of its
/// files vanish, or `CURRENT` change, is taken again.
fn copy_dir(from: &std::path::Path, to: &std::path::Path) {
    fn copy(from: &std::path::Path, to: &std::path::Path) -> std::io::Result<()> {
        std::fs::create_dir_all(to)?;
        for entry in std::fs::read_dir(from)? {
            let entry = entry?;
            let target = to.join(entry.file_name());

            if entry.file_type()?.is_dir() {
                copy(&entry.path(), &target)?;
            } else {
                std::fs::copy(entry.path(), target)?;
            }
        }

        Ok(())
    }

    for _ in 0..50 {
        let before = std::fs::read(from.join("CURRENT")).ok();

        if copy(from, to).is_ok() && std::fs::read(from.join("CURRENT")).ok() == before {
            return;
        }

        let _ = std::fs::remove_dir_all(to);
        thread::sleep(Duration::from_millis(5));
    }

    panic!("{} kept changing while it was copied", from.display());
}

fn serve_on_ephemeral() -> (SocketAddr, ShutdownToken, thread::JoinHandle<()>) {
    serve_with(|_| {})
}

fn bind_consecutive_pair() -> (std::net::TcpListener, std::net::TcpListener, u16) {
    for _ in 0..32 {
        let first = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = first.local_addr().unwrap();
        let Some(next) = address.port().checked_add(1) else {
            continue;
        };

        if let Ok(second) = std::net::TcpListener::bind((address.ip(), next)) {
            first.set_nonblocking(true).unwrap();
            second.set_nonblocking(true).unwrap();

            return (first, second, address.port());
        }
    }

    panic!("could not bind consecutive ports");
}

fn serve_two_shards() -> (
    SocketAddr,
    SocketAddr,
    ShutdownToken,
    thread::JoinHandle<()>,
    thread::JoinHandle<()>,
) {
    serve_two_shards_with(|_| {})
}

fn serve_two_shards_with(
    configure: impl FnOnce(&mut AppConfig),
) -> (
    SocketAddr,
    SocketAddr,
    ShutdownToken,
    thread::JoinHandle<()>,
    thread::JoinHandle<()>,
) {
    let (first, second, base_port) = bind_consecutive_pair();
    let advertised = "127.0.0.1".to_string();
    let mut config = AppConfig {
        address: format!("127.0.0.1:{base_port}"),
        shards: 2,
        advertised_host: advertised.clone(),
        shutdown_grace: Duration::from_secs(1),
        ..AppConfig::default()
    };

    configure(&mut config);
    let hot_store = crate::hot_store::shared_memory_store(&config.hot_keys);
    let cluster = if config.persistence.profile == crate::DurabilityProfile::Memory {
        None
    } else {
        let manager = CheckpointManager::new(config.persistence.clone(), config.protocol);
        let generation = manager
            .load()
            .unwrap()
            .map_or(0, |checkpoint| checkpoint.generation);
        Some(Arc::new(ClusterCheckpointCoordinator::new(
            manager,
            config.checkpoint_partition_count(),
            generation,
            hot_store.clone(),
        )))
    };
    let shutdown = ShutdownToken::default();
    let advertised_port = config.advertised_port.unwrap_or(base_port);
    let route0 = ClusterRoute {
        shard_id: 0,
        shard_count: 2,
        advertised_host: advertised.clone(),
        advertised_port,
    };

    let route1 = ClusterRoute {
        shard_id: 1,
        shard_count: 2,
        advertised_host: advertised,
        advertised_port,
    };

    let left = first.local_addr().unwrap();
    let right = second.local_addr().unwrap();
    let cfg0 = config.clone();
    let cfg1 = config;
    let stop0 = shutdown.clone();
    let stop1 = shutdown.clone();
    let cluster0 = cluster.clone();
    let cluster1 = cluster;
    let hot0 = hot_store.clone();
    let hot1 = hot_store;
    let t0 = thread::spawn(move || {
        serve_unlocked(
            Some(first),
            cfg0,
            stop0,
            Some(route0),
            None,
            cluster0,
            hot0,
            false,
        )
        .unwrap()
    });

    let t1 = thread::spawn(move || {
        serve_unlocked(
            Some(second),
            cfg1,
            stop1,
            Some(route1),
            None,
            cluster1,
            hot1,
            false,
        )
        .unwrap()
    });

    for address in [left, right] {
        for _ in 0..50 {
            if StdTcpStream::connect(address).is_ok() {
                break;
            }

            thread::sleep(Duration::from_millis(2));
        }
    }

    (left, right, shutdown, t0, t1)
}

fn rustls_client(
    address: SocketAddr,
    cert_path: &std::path::Path,
) -> rustls::StreamOwned<rustls::ClientConnection, StdTcpStream> {
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();

    use rustls::pki_types::pem::PemObject;
    let cert_bytes = std::fs::read(cert_path).unwrap();
    let certs = rustls::pki_types::CertificateDer::pem_slice_iter(&cert_bytes)
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    let mut roots = rustls::RootCertStore::empty();

    for cert in certs {
        roots.add(cert).unwrap();
    }

    let config = rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    let name = rustls::pki_types::ServerName::try_from("localhost")
        .unwrap()
        .to_owned();
    let connection = rustls::ClientConnection::new(Arc::new(config), name).unwrap();
    let tcp = StdTcpStream::connect(address).unwrap();

    tcp.set_read_timeout(Some(Duration::from_secs(2))).unwrap();
    tcp.set_write_timeout(Some(Duration::from_secs(2))).unwrap();
    rustls::StreamOwned::new(connection, tcp)
}

fn ping_over_tls(address: SocketAddr, cert_path: &std::path::Path) {
    let mut client = rustls_client(address, cert_path);

    client.write_all(b"*1\r\n$4\r\nPING\r\n").unwrap();
    let mut reply = [0u8; 7];

    client.read_exact(&mut reply).unwrap();
    assert_eq!(&reply, b"+PONG\r\n");
}

fn info_field(address: SocketAddr, field: &str) -> Option<u64> {
    let info = String::from_utf8(request(address, &resp(&[&[b"INFO"]]))).unwrap();

    info.lines()
        .find_map(|line| line.strip_prefix(&format!("{field}:")))
        .and_then(|value| value.trim().parse().ok())
}

fn wait_for(limit: Duration, mut done: impl FnMut() -> bool) -> bool {
    let started = Instant::now();

    while started.elapsed() < limit {
        if done() {
            return true;
        }
        thread::sleep(Duration::from_millis(20));
    }
    false
}

fn resp(commands: &[&[&[u8]]]) -> Vec<u8> {
    let mut out = Vec::new();

    for parts in commands {
        out.extend_from_slice(format!("*{}\r\n", parts.len()).as_bytes());

        for part in *parts {
            out.extend_from_slice(format!("${}\r\n", part.len()).as_bytes());
            out.extend_from_slice(part);
            out.extend_from_slice(b"\r\n");
        }
    }

    out
}

fn tag_on_shard(shard: usize) -> String {
    for n in 0..4096 {
        let tag = format!("tag{n}");

        if crate::routing::shard_for_slot(crate::routing::hash_slot(tag.as_bytes()), 2) == shard {
            return tag;
        }
    }

    panic!("no hash tag landed on shard {shard}");
}

mod cluster;
mod connection;
mod expire;
mod hangups;
mod maintenance;
mod misc;
mod replica;
mod single_port;
mod wal;
