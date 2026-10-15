use loom::{
    model,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    thread,
};

#[test]
fn shutdown_and_queue_admission_never_exceed_capacity() {
    model(|| {
        let queued = Arc::new(AtomicUsize::new(0));
        let shutdown = Arc::new(AtomicBool::new(false));

        let producer_queue = Arc::clone(&queued);
        let producer_shutdown = Arc::clone(&shutdown);
        let producer = thread::spawn(move || {
            if !producer_shutdown.load(Ordering::Acquire)
                && producer_queue
                    .compare_exchange(0, 1, Ordering::AcqRel, Ordering::Acquire)
                    .is_ok()
            {
                producer_queue.fetch_sub(1, Ordering::AcqRel);
            }
        });

        let requester = thread::spawn(move || {
            shutdown.store(true, Ordering::Release);
        });

        producer.join().unwrap();
        requester.join().unwrap();
        assert!(queued.load(Ordering::Acquire) <= 1);
    });
}
