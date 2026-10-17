//! Helpers shared by the integration tests.

// Each test binary compiles this module on its own and uses only some of the helpers.
#![allow(dead_code)]

use std::collections::HashMap;
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::Mutex;
use std::thread;
use std::time::{Duration, Instant};

/// How long a fake server waits for a client before the test fails.
pub const ACCEPT_DEADLINE: Duration = Duration::from_secs(10);

/// Accepts one connection, polling a non-blocking listener until [`ACCEPT_DEADLINE`].
///
/// Panics with a clear message when no client shows up, so a broken test fails instead of
/// hanging. The accepted stream is switched back to blocking mode.
pub fn accept_within_deadline(listener: &TcpListener) -> (TcpStream, SocketAddr) {
    listener.set_nonblocking(true).unwrap();

    let deadline = Instant::now() + ACCEPT_DEADLINE;

    loop {
        match listener.accept() {
            Ok((stream, addr)) => {
                stream.set_nonblocking(false).unwrap();

                return (stream, addr);
            }
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                assert!(
                    Instant::now() < deadline,
                    "fake server: no client connected within {ACCEPT_DEADLINE:?}"
                );
                thread::sleep(Duration::from_millis(2));
            }
            Err(error) => panic!("fake server: accept failed: {error}"),
        }
    }
}

/// Records that the calling test owns `database` on the shared live server.
///
/// The live tests of one binary run in parallel against one server and flush their database
/// when they start, so two tests on the same database would wipe each other's keys. Panics
/// when a second test claims a database another test already holds.
pub fn claim_database(database: u32) {
    static OWNERS: Mutex<Option<HashMap<u32, String>>> = Mutex::new(None);

    let test = thread::current().name().unwrap_or("main").to_owned();
    let mut owners = OWNERS
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let owner = owners
        .get_or_insert_with(HashMap::new)
        .entry(database)
        .or_insert_with(|| test.clone());

    assert_eq!(
        *owner, test,
        "database {database} is used by both {owner} and {test}; give each test its own"
    );
}
