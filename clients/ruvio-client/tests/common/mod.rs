//! Helpers shared by the integration tests that run a fake server thread.

use std::net::{SocketAddr, TcpListener, TcpStream};
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
