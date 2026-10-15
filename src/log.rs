use std::io::{self, Write};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, SyncSender, TrySendError};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use tracing_subscriber::EnvFilter;

/// Queued log lines. A full queue drops the new line instead of blocking the caller.
const LOG_QUEUE_CAPACITY: usize = 256;

/// One formatted event. Larger writes are dropped so a single line cannot fill the queue.
pub(crate) const LOG_LINE_LIMIT: usize = 16 * 1024;

const PUMP_POLL: Duration = Duration::from_millis(50);

static DROPPED: std::sync::OnceLock<Arc<AtomicU64>> = std::sync::OnceLock::new();

/// Process-wide lines the bounded writer did not keep. Zero until the pump starts.
pub fn dropped_log_lines() -> u64 {
    DROPPED
        .get()
        .map(|counter| counter.load(Ordering::Relaxed))
        .unwrap_or(0)
}

/// Install the process log pump and the fmt subscriber. Drop the pump after the
/// server returns so the worker can drain lines already queued.
pub fn init_process_logging() -> LogPump {
    let dropped = Arc::new(AtomicU64::new(0));
    let _ = DROPPED.set(Arc::clone(&dropped));
    let (sender, pump) = spawn_log_pump_with(LOG_QUEUE_CAPACITY, io::stderr(), dropped);
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into());

    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(move || sender.clone())
        .with_target(false)
        .init();
    pump
}

pub struct LogPump {
    stop: Arc<AtomicBool>,
    join: Option<JoinHandle<()>>,
}

impl Drop for LogPump {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);

        if let Some(handle) = self.join.take() {
            let _ = handle.join();
        }
    }
}

#[derive(Clone)]
pub(crate) struct LogSender {
    tx: SyncSender<Vec<u8>>,
    dropped: Arc<AtomicU64>,
}

impl LogSender {
    #[cfg(test)]
    pub(crate) fn bounded_for_test(capacity: usize) -> (Self, Arc<AtomicU64>, Receiver<Vec<u8>>) {
        let (tx, rx) = mpsc::sync_channel(capacity.max(1));
        let dropped = Arc::new(AtomicU64::new(0));
        let sender = Self {
            tx,
            dropped: Arc::clone(&dropped),
        };

        (sender, dropped, rx)
    }

    fn write_line(&self, buf: &[u8]) {
        if buf.is_empty() {
            return;
        }

        if buf.len() > LOG_LINE_LIMIT {
            self.dropped.fetch_add(1, Ordering::Relaxed);

            return;
        }

        if let Err(TrySendError::Full(_) | TrySendError::Disconnected(_)) =
            self.tx.try_send(buf.to_vec())
        {
            self.dropped.fetch_add(1, Ordering::Relaxed);
        }
    }
}

impl Write for LogSender {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.write_line(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[cfg(test)]
pub(crate) fn spawn_log_pump(
    capacity: usize,
    destination: impl Write + Send + 'static,
) -> (LogSender, LogPump) {
    spawn_log_pump_with(capacity, destination, Arc::new(AtomicU64::new(0)))
}

fn spawn_log_pump_with(
    capacity: usize,
    mut destination: impl Write + Send + 'static,
    dropped: Arc<AtomicU64>,
) -> (LogSender, LogPump) {
    let (tx, rx) = mpsc::sync_channel(capacity.max(1));
    let stop = Arc::new(AtomicBool::new(false));
    let worker_stop = Arc::clone(&stop);
    let worker_dropped = Arc::clone(&dropped);
    let join = thread::Builder::new()
        .name("ruvio-log".into())
        .spawn(move || pump(&mut destination, rx, &worker_stop, &worker_dropped))
        .expect("log pump thread");
    let sender = LogSender { tx, dropped };

    (
        sender,
        LogPump {
            stop,
            join: Some(join),
        },
    )
}

fn pump(
    destination: &mut impl Write,
    rx: Receiver<Vec<u8>>,
    stop: &AtomicBool,
    dropped: &AtomicU64,
) {
    loop {
        if stop.load(Ordering::Acquire) {
            while let Ok(line) = rx.try_recv() {
                forward(destination, dropped, &line);
            }

            return;
        }

        match rx.recv_timeout(PUMP_POLL) {
            Ok(line) => forward(destination, dropped, &line),
            Err(RecvTimeoutError::Timeout) => {}
            Err(RecvTimeoutError::Disconnected) => {
                while let Ok(line) = rx.try_recv() {
                    forward(destination, dropped, &line);
                }

                return;
            }
        }
    }
}

fn forward(destination: &mut impl Write, dropped: &AtomicU64, line: &[u8]) {
    if destination.write_all(line).is_err() || destination.flush().is_err() {
        dropped.fetch_add(1, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use std::io::Write;
    use std::sync::{Arc, Condvar, Mutex};
    use std::time::Duration;

    use super::{LOG_LINE_LIMIT, LogSender};

    #[test]
    fn a_full_queue_drops_the_line_and_keeps_the_caller() {
        let (mut sender, dropped, rx) = LogSender::bounded_for_test(1);

        sender.write_all(b"first\n").unwrap();
        sender.write_all(b"second\n").unwrap();

        assert_eq!(dropped.load(std::sync::atomic::Ordering::Relaxed), 1);
        assert_eq!(rx.try_recv().unwrap(), b"first\n");
        assert!(rx.try_recv().is_err());
    }

    #[test]
    fn an_oversized_line_is_dropped_instead_of_queued() {
        let (mut sender, dropped, rx) = LogSender::bounded_for_test(1);
        let line = vec![b'x'; LOG_LINE_LIMIT + 1];

        sender.write_all(&line).unwrap();

        assert_eq!(dropped.load(std::sync::atomic::Ordering::Relaxed), 1);
        assert!(rx.try_recv().is_err());
    }

    #[test]
    fn the_pump_writes_a_queued_line_without_the_caller_waiting() {
        let sink = SharedSink::default();
        let (mut sender, pump) = super::spawn_log_pump(4, sink.clone());

        sender.write_all(b"hello\n").unwrap();
        drop(sender);
        drop(pump);

        assert_eq!(sink.bytes(), b"hello\n");
    }

    #[derive(Clone, Default)]
    struct SharedSink {
        bytes: Arc<(Mutex<Vec<u8>>, Condvar)>,
    }

    impl Write for SharedSink {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            let (lock, cv) = &*self.bytes;

            lock.lock().expect("sink").extend_from_slice(buf);
            cv.notify_one();
            Ok(buf.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    impl SharedSink {
        fn bytes(&self) -> Vec<u8> {
            let (lock, cv) = &*self.bytes;
            let mut bytes = lock.lock().expect("sink");
            let started = std::time::Instant::now();

            while bytes.is_empty() && started.elapsed() < Duration::from_secs(2) {
                let (next, wait) = cv
                    .wait_timeout(bytes, Duration::from_millis(50))
                    .expect("sink");

                bytes = next;

                if wait.timed_out() && !bytes.is_empty() {
                    break;
                }
            }

            bytes.clone()
        }
    }
}
