//! Process-level behaviour of a real server: signals, descriptor exhaustion, and recovery
//! after a kill at an arbitrary point in a stream of acknowledged writes.

mod common;

use std::{
    io::{BufRead, BufReader, Write},
    net::TcpStream,
    path::Path,
    process::{Child, Command, Stdio},
    time::{Duration, Instant},
};

use common::{Client, answers_as, test_ports};

struct Process {
    child: Child,
    address: String,
    /// Held until the process is gone, so no other test takes its ports meanwhile.
    _ports: test_ports::PortBlock,
}

impl Drop for Process {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// Starts `ruvio` on a reserved port with a copy of the repository config; `shell_prefix`
/// runs before `exec` (for example a descriptor limit). The block is reserved against every
/// test, but a program outside the tests may still take the port; then this server exits or
/// a stranger answers, and another block is tried.
fn start(dir: &Path, env: &[(&str, &str)], shell_prefix: &str) -> Process {
    let config = dir.join("ruvio.toml");

    if !config.exists() {
        std::fs::copy(
            concat!(env!("CARGO_MANIFEST_DIR"), "/ruvio.example.toml"),
            &config,
        )
        .unwrap();
    }

    for _ in 0..3 {
        let ports = test_ports::reserve();
        let address = ports.address_at(0).to_string();
        let mut command = Command::new("sh");

        command
            .arg("-c")
            .arg(format!("{shell_prefix} exec \"$0\" \"$@\""))
            .arg(env!("CARGO_BIN_EXE_ruvio"))
            .arg("--config")
            .arg(&config)
            .args(["--address", &address, "--data-dir"])
            .arg(dir.join("data"))
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        for (key, value) in env {
            command.env(key, value);
        }

        // Not while this process probes ports: see `test_ports::spawn_lock`.
        let spawning = test_ports::spawn_lock();
        let child = command.spawn().unwrap();

        drop(spawning);
        let mut process = Process {
            child,
            address,
            _ports: ports,
        };

        if process.wait_ready(&dir.join("data")) {
            return process;
        }
    }

    panic!("server did not start on any of three port blocks");
}

impl Process {
    /// True once this server answers on its address; false when it exited first or a
    /// stranger answers there.
    fn wait_ready(&mut self, data_dir: &Path) -> bool {
        let deadline = Instant::now() + Duration::from_secs(20);

        loop {
            if self.child.try_wait().unwrap().is_some() {
                return false;
            }
            // A broken probe or an error reply means whoever answered is not ready or went
            // away mid-probe; look again.
            if let Ok(ours) = answers_as(&self.address, data_dir, None) {
                return ours;
            }
            assert!(Instant::now() < deadline, "server did not start");
            std::thread::sleep(Duration::from_millis(10));
        }
    }
}

fn ping(address: &str) -> bool {
    let Ok(stream) = TcpStream::connect(address) else {
        return false;
    };

    stream
        .set_read_timeout(Some(Duration::from_secs(2)))
        .unwrap();
    let mut writer = stream.try_clone().unwrap();
    let mut reader = BufReader::new(stream);
    let mut line = String::new();

    writer.write_all(b"PING\r\n").is_ok()
        && reader.read_line(&mut line).is_ok()
        && line == "+PONG\r\n"
}

#[cfg(unix)]
#[test]
fn sighup_neither_stops_nor_disturbs_the_server() {
    let dir = tempfile::tempdir().unwrap();
    let mut server = start(dir.path(), &[], "");
    let address = server.address.clone();
    let mut client = Client::connect(&address);

    client.call(&["SET", "k", "v"]);
    for _ in 0..3 {
        // SAFETY: plain kill(2) on a child process this test owns.
        let sent = unsafe { libc::kill(server.child.id() as libc::pid_t, libc::SIGHUP) };

        assert_eq!(sent, 0);
        std::thread::sleep(Duration::from_millis(100));
    }

    assert!(
        server.child.try_wait().unwrap().is_none(),
        "SIGHUP stopped the server"
    );
    assert_eq!(
        client.call(&["GET", "k"]),
        "v",
        "an open client was dropped"
    );
    assert!(ping(&address));
}

#[cfg(target_os = "linux")]
#[test]
fn descriptor_exhaustion_keeps_existing_clients_and_recovers_for_new_ones() {
    let dir = tempfile::tempdir().unwrap();
    let mut server = start(dir.path(), &[], "ulimit -n 64 &&");
    let address = server.address.clone();
    let mut veteran = Client::connect(&address);

    veteran.call(&["SET", "k", "v"]);

    // Far more clients than descriptors: the server runs out while accepting them.
    let flood: Vec<TcpStream> = (0..200)
        .filter_map(|_| TcpStream::connect(&address).ok())
        .collect();

    std::thread::sleep(Duration::from_millis(300));
    assert!(
        server.child.try_wait().unwrap().is_none(),
        "the server exited when descriptors ran out"
    );
    assert_eq!(
        veteran.call(&["GET", "k"]),
        "v",
        "an established client stopped being served"
    );

    drop(flood);

    // Once descriptors are free again, new clients are accepted.
    let deadline = Instant::now() + Duration::from_secs(10);

    while !ping(&address) {
        assert!(
            Instant::now() < deadline,
            "no new client was served after descriptors were released"
        );
        std::thread::sleep(Duration::from_millis(50));
    }
    assert_eq!(veteran.call(&["GET", "k"]), "v");
}

/// Writes `w{n}` in order, acknowledging each reply, until the connection breaks. Keys with a
/// one-millisecond expiry are mixed in, so restart replays deletes of expired keys too.
fn write_until_killed(address: &str) -> u64 {
    let stream = TcpStream::connect(address).unwrap();

    stream
        .set_read_timeout(Some(Duration::from_secs(10)))
        .unwrap();
    let mut writer = stream.try_clone().unwrap();
    let mut reader = BufReader::new(stream);
    let mut acknowledged = 0u64;
    let mut line = String::new();

    for n in 0.. {
        let key = format!("w{n}");
        let value = n.to_string();
        let ephemeral = format!("e{n}");
        let frame = format!(
            "*3\r\n$3\r\nSET\r\n${}\r\n{key}\r\n${}\r\n{value}\r\n*5\r\n$3\r\nSET\r\n${}\r\n{ephemeral}\r\n$1\r\nx\r\n$2\r\nPX\r\n$1\r\n1\r\n",
            key.len(),
            value.len(),
            ephemeral.len()
        );

        if writer.write_all(frame.as_bytes()).is_err() {
            break;
        }

        let mut ok = true;

        for _ in 0..2 {
            line.clear();
            if !matches!(reader.read_line(&mut line), Ok(n) if n > 0) || line != "+OK\r\n" {
                ok = false;
                break;
            }
        }
        if !ok {
            break;
        }
        acknowledged = n + 1;
    }

    acknowledged
}

#[cfg(unix)]
#[test]
fn a_kill_at_any_point_recovers_every_acknowledged_write_as_a_prefix() {
    let dir = tempfile::tempdir().unwrap();
    let env = [
        ("RUVIO_DURABILITY", "always"),
        ("RUVIO_SNAPSHOT_INTERVAL_MS", "200"),
    ];
    let mut total_acknowledged = 0;

    for (trial, kill_after_ms) in [150u64, 420, 700].into_iter().enumerate() {
        let mut server = start(dir.path(), &env, "");
        let writer = {
            let address = server.address.clone();

            std::thread::spawn(move || write_until_killed(&address))
        };

        std::thread::sleep(Duration::from_millis(kill_after_ms));
        server.child.kill().unwrap();
        server.child.wait().unwrap();
        let acknowledged = writer.join().unwrap();
        drop(server);

        let _server = start(dir.path(), &env, "");
        let mut client = Client::connect(&_server.address);
        // Each trial starts writing from w0 again, so the keys present now come from the
        // trial with the most writes; all of this trial's acknowledged ones must be there.
        let mut present = 0u64;

        while client.call(&["GET", &format!("w{present}")]) == present.to_string() {
            present += 1;
        }
        assert!(
            present >= acknowledged,
            "trial {trial}: {acknowledged} writes acknowledged, only the first {present} recovered"
        );
        // Nothing beyond the prefix: no hole followed by later writes.
        for n in present + 1..present + 50 {
            assert_eq!(
                client.call(&["EXISTS", &format!("w{n}")]),
                "0",
                "trial {trial}: w{n} recovered after a hole at w{present}"
            );
        }
        // Expired keys stay expired after replay.
        let ephemeral = client.call(&["EXISTS", "e0", "e1", "e2"]);
        assert_eq!(ephemeral, "0", "trial {trial}: expired keys came back");
        total_acknowledged += acknowledged;
        drop(_server);
    }

    assert!(
        total_acknowledged > 30,
        "too few writes were acknowledged to mean anything: {total_acknowledged}"
    );
}
