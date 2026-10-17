//! Background release of large values.
//!
//! A shard runs on one thread, so freeing a million-member set inside `UNLINK` would
//! stall every client on that shard. Large owned values are handed to one process-wide
//! drop thread instead. The thread starts on the first deferral, so a process that never
//! deletes a large value never creates it.
//!
//! The queue is bounded by the bytes it holds and by its length. When either is reached
//! the caller drops the value in place, which is the back pressure: deferral never holds
//! freed memory without limit and never blocks.

use std::sync::{
    OnceLock,
    atomic::{AtomicUsize, Ordering},
    mpsc::{Sender, channel},
};

/// Values charged below this many bytes are dropped in place: the hand-off costs more
/// than freeing them.
pub(crate) const MIN_DEFERRED_BYTES: usize = 64 * 1024;

/// Most bytes waiting for the drop thread. Past this a value is dropped in place.
const MAX_PENDING_BYTES: usize = 1 << 30;

/// Most jobs waiting for the drop thread, so many medium values cannot queue without end.
const MAX_PENDING_JOBS: usize = 16 * 1024;

struct Job {
    value: Box<dyn Send>,
    bytes: usize,
    /// Set once the value is dropped, for the test that handed it over.
    #[cfg(test)]
    freed: std::sync::Arc<std::sync::atomic::AtomicBool>,
}

#[cfg(test)]
thread_local! {
    static LAST_DEFERRED: std::cell::RefCell<Option<std::sync::Arc<std::sync::atomic::AtomicBool>>> =
        const { std::cell::RefCell::new(None) };
}

/// The flag that turns true once the last value this thread handed to the drop thread is
/// freed, so a test waits for its own value instead of for a queue every test shares.
#[cfg(test)]
pub(crate) fn take_last_deferred() -> Option<std::sync::Arc<std::sync::atomic::AtomicBool>> {
    LAST_DEFERRED.with(|last| last.borrow_mut().take())
}

static QUEUE: OnceLock<Option<Sender<Job>>> = OnceLock::new();
static PENDING_BYTES: AtomicUsize = AtomicUsize::new(0);
static PENDING_JOBS: AtomicUsize = AtomicUsize::new(0);

fn queue() -> Option<&'static Sender<Job>> {
    QUEUE
        .get_or_init(|| {
            let (sender, receiver) = channel::<Job>();

            std::thread::Builder::new()
                .name("ruvio-lazy-free".into())
                .spawn(move || {
                    while let Ok(job) = receiver.recv() {
                        drop(job.value);
                        PENDING_BYTES.fetch_sub(job.bytes, Ordering::Relaxed);
                        PENDING_JOBS.fetch_sub(1, Ordering::Relaxed);
                        #[cfg(test)]
                        job.freed.store(true, Ordering::Release);
                    }
                })
                .ok()
                .map(|_| sender)
        })
        .as_ref()
}

/// Bytes handed to the drop thread and not yet freed.
pub(crate) fn pending_bytes() -> usize {
    PENDING_BYTES.load(Ordering::Relaxed)
}

/// Drops `value` on the drop thread when it is charged at least `MIN_DEFERRED_BYTES`
/// and the queue has room; otherwise drops it here.
pub(crate) fn defer_drop<T: Send + 'static>(value: T, bytes: usize) {
    if bytes < MIN_DEFERRED_BYTES {
        return;
    }

    // Shard threads check and add separately, so the bounds can overshoot by a job per
    // shard. An empty queue takes any one value, however large.
    let pending = PENDING_BYTES.load(Ordering::Relaxed);

    if PENDING_JOBS.load(Ordering::Relaxed) >= MAX_PENDING_JOBS
        || (pending != 0 && pending.saturating_add(bytes) > MAX_PENDING_BYTES)
    {
        return;
    }

    enqueue(value, bytes);
}

/// Drops the contents of a flushed database on the drop thread whatever is already
/// queued. A flush must stay constant time on every shard: when `FLUSHALL` fans out,
/// all shards hand over their data at once, and the byte bound would make all but the
/// first free theirs inline. Flushes are one job per shard each, so this cannot queue
/// without end the way many deleted values could.
pub(crate) fn defer_flush<T: Send + 'static>(value: T, bytes: usize) {
    enqueue(value, bytes);
}

fn enqueue<T: Send + 'static>(value: T, bytes: usize) {
    let Some(queue) = queue() else {
        return;
    };

    PENDING_BYTES.fetch_add(bytes, Ordering::Relaxed);
    PENDING_JOBS.fetch_add(1, Ordering::Relaxed);
    #[cfg(test)]
    let freed = std::sync::Arc::<std::sync::atomic::AtomicBool>::default();
    #[cfg(test)]
    LAST_DEFERRED.with(|last| *last.borrow_mut() = Some(std::sync::Arc::clone(&freed)));

    if let Err(error) = queue.send(Job {
        value: Box::new(value),
        bytes,
        #[cfg(test)]
        freed,
    }) {
        PENDING_BYTES.fetch_sub(bytes, Ordering::Relaxed);
        PENDING_JOBS.fetch_sub(1, Ordering::Relaxed);
        drop(error.0.value);
        #[cfg(test)]
        error.0.freed.store(true, Ordering::Release);
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use std::{
        sync::{Arc, atomic::AtomicBool},
        time::{Duration, Instant},
    };

    /// The queue bounds are process-wide; tests that fill them must not overlap, and a test
    /// that needs its value deferred (not dropped in place for a full queue) holds this too.
    pub(crate) static QUEUE_BOUNDS: std::sync::Mutex<()> = std::sync::Mutex::new(());

    struct Flag(Arc<AtomicBool>);

    impl Drop for Flag {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }

    fn wait_for(done: &AtomicBool) {
        let start = Instant::now();

        while !done.load(Ordering::SeqCst) {
            assert!(start.elapsed() < Duration::from_secs(5), "drop never ran");
            std::thread::yield_now();
        }
    }

    #[test]
    fn small_values_drop_in_place() {
        let dropped = Arc::new(AtomicBool::new(false));

        defer_drop(Flag(dropped.clone()), MIN_DEFERRED_BYTES - 1);
        assert!(dropped.load(Ordering::SeqCst));
    }

    #[test]
    fn large_values_drop_on_the_background_thread() {
        let _serial = QUEUE_BOUNDS
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let dropped = Arc::new(AtomicBool::new(false));
        let caller = std::thread::current().id();
        let seen = Arc::new(std::sync::Mutex::new(None));

        struct Where(
            Arc<std::sync::Mutex<Option<std::thread::ThreadId>>>,
            #[allow(dead_code)] Flag,
        );

        impl Drop for Where {
            fn drop(&mut self) {
                *self.0.lock().unwrap() = Some(std::thread::current().id());
            }
        }

        defer_drop(
            Where(seen.clone(), Flag(dropped.clone())),
            MIN_DEFERRED_BYTES,
        );
        wait_for(&dropped);

        assert_ne!(seen.lock().unwrap().unwrap(), caller);
    }

    #[test]
    fn a_burst_longer_than_the_old_queue_still_leaves_the_caller_thread() {
        let _serial = QUEUE_BOUNDS
            .lock()
            .unwrap_or_else(|error| error.into_inner());

        struct Gate(std::sync::mpsc::Receiver<()>);

        impl Drop for Gate {
            fn drop(&mut self) {
                let _ = self.0.recv_timeout(Duration::from_secs(5));
            }
        }

        struct Seen(Arc<std::sync::Mutex<Vec<std::thread::ThreadId>>>);

        impl Drop for Seen {
            fn drop(&mut self) {
                self.0.lock().unwrap().push(std::thread::current().id());
            }
        }

        let (open, gate) = std::sync::mpsc::channel();
        let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
        let caller = std::thread::current().id();

        // The drop thread waits on the gate while the burst queues behind it.
        defer_drop(Gate(gate), MIN_DEFERRED_BYTES);

        for _ in 0..500 {
            defer_drop(Seen(seen.clone()), MIN_DEFERRED_BYTES);
        }

        assert!(
            seen.lock().unwrap().iter().all(|thread| *thread != caller),
            "no value of the burst was dropped in place"
        );
        open.send(()).unwrap();

        let start = Instant::now();

        while seen.lock().unwrap().len() < 500 {
            assert!(start.elapsed() < Duration::from_secs(5), "drops never ran");
            std::thread::yield_now();
        }
    }

    #[test]
    fn flushes_of_every_shard_leave_the_caller_thread_however_large() {
        let _serial = QUEUE_BOUNDS
            .lock()
            .unwrap_or_else(|error| error.into_inner());

        struct Gate(std::sync::mpsc::Receiver<()>);

        impl Drop for Gate {
            fn drop(&mut self) {
                let _ = self.0.recv_timeout(Duration::from_secs(5));
            }
        }

        struct Seen(Arc<std::sync::Mutex<Vec<std::thread::ThreadId>>>);

        impl Drop for Seen {
            fn drop(&mut self) {
                self.0.lock().unwrap().push(std::thread::current().id());
            }
        }

        let (open, gate) = std::sync::mpsc::channel();
        let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
        let caller = std::thread::current().id();

        // The byte bound is already reached when eight shards flush 4 GiB each.
        defer_drop(Gate(gate), MAX_PENDING_BYTES);

        for _ in 0..8 {
            defer_flush(Seen(seen.clone()), 4 << 30);
        }

        assert!(
            seen.lock().unwrap().iter().all(|thread| *thread != caller),
            "a flush was freed inline"
        );
        open.send(()).unwrap();

        let start = Instant::now();

        while seen.lock().unwrap().len() < 8 {
            assert!(start.elapsed() < Duration::from_secs(5), "drops never ran");
            std::thread::yield_now();
        }
    }
}
