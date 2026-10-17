//! Helpers for the unit tests that run a scripted server thread.

use std::net::{SocketAddr, TcpListener, TcpStream};
use std::thread;
use std::time::{Duration, Instant};

/// How long a scripted server waits for a client before the test fails.
const ACCEPT_DEADLINE: Duration = Duration::from_secs(10);

/// Accepts one connection, polling a non-blocking listener until [`ACCEPT_DEADLINE`].
///
/// Panics when no client shows up, so a broken test fails instead of hanging. The accepted
/// stream is switched back to blocking mode.
pub(crate) fn accept(listener: &TcpListener) -> (TcpStream, SocketAddr) {
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
                    "scripted server: no client connected within {ACCEPT_DEADLINE:?}"
                );
                thread::sleep(Duration::from_millis(2));
            }
            Err(error) => panic!("scripted server: accept failed: {error}"),
        }
    }
}
