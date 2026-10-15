//! The process runs the evented adapter. Parse, apply, and write stay on the shard owner.

pub(crate) mod affinity;
pub(crate) mod cluster;
pub mod evented;
pub(crate) mod replica_guard;
pub(crate) mod session;

/// Blocks until `listener` has a connection to accept or `timeout` passes, so an idle
/// acceptor sleeps in the kernel and still accepts the moment a client connects.
pub(crate) fn wait_for_connection(listener: &std::net::TcpListener, timeout: std::time::Duration) {
    #[cfg(unix)]
    {
        use std::os::fd::AsRawFd;

        let mut poll = libc::pollfd {
            fd: listener.as_raw_fd(),
            events: libc::POLLIN,
            revents: 0,
        };
        let millis = libc::c_int::try_from(timeout.as_millis()).unwrap_or(libc::c_int::MAX);

        // SAFETY: `poll` points at one initialised pollfd that lives across the call, and
        // the count passed is 1. The result is ignored: the caller's accept says what
        // happened, and an error or timeout only means it looks again.
        #[allow(unsafe_code)]
        unsafe {
            libc::poll(&mut poll, 1, millis);
        }
    }

    #[cfg(not(unix))]
    {
        let _ = listener;
        std::thread::sleep(timeout.min(std::time::Duration::from_millis(1)));
    }
}

#[cfg(all(test, unix))]
mod tests {
    use std::time::{Duration, Instant};

    use super::wait_for_connection;

    #[test]
    fn an_idle_acceptor_sleeps_until_a_connection_arrives() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();

        let started = Instant::now();
        wait_for_connection(&listener, Duration::from_millis(200));
        assert!(started.elapsed() >= Duration::from_millis(150));

        let _client = std::net::TcpStream::connect(listener.local_addr().unwrap()).unwrap();
        let started = Instant::now();
        wait_for_connection(&listener, Duration::from_secs(5));
        assert!(started.elapsed() < Duration::from_secs(1));
        assert!(listener.accept().is_ok());
    }
}
