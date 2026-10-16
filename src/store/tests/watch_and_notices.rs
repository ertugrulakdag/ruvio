use super::*;

#[test]
fn watch_index_tracks_shared_keys_databases_and_unwatch() {
    let mut shard = KvShard::new();
    assert!(shard.watchers_by_key.is_none());
    shard.watch(1, 0, &[b"shared", b"shared"]);
    shard.watch(2, 0, &[b"shared"]);
    shard.watch(2, 1, &[b"shared"]);
    shard.watch(3, 1, &[b"other"]);
    assert_eq!(shard.watches[&1].len(), 1);
    assert!(Arc::ptr_eq(
        &shard.watches[&1][0].1,
        &shard.watches[&2][0].1
    ));
    assert_eq!(
        shard.watchers_by_key.as_ref().unwrap()[&0][b"shared".as_slice()].len(),
        2
    );

    shard.set_bytes(b"unrelated", b"v");
    assert!(!shard.watches_dirty(1));
    shard.enter(1);
    shard.set_bytes(b"shared", b"v");
    assert!(!shard.watches_dirty(1));
    assert!(shard.watches_dirty(2));
    assert!(!shard.watches_dirty(3));

    shard.unwatch(2);
    assert!(!shard.watches_dirty(2));
    shard.enter(0);
    shard.set_bytes(b"shared", b"v");
    assert!(shard.watches_dirty(1));
    assert!(!shard.watches_dirty(2));
    shard.unwatch(1);
    assert!(!shard.watchers_by_key.as_ref().unwrap().contains_key(&0));
    shard.enter(1);
    shard.set_bytes(b"other", b"v");
    assert!(shard.watches_dirty(3));
    shard.unwatch(3);
    assert!(shard.watchers_by_key.is_none());

    shard.watch(2, 0, &[b"shared"]);
    shard.enter(0);
    shard.set_bytes(b"shared", b"new");
    assert!(shard.watches_dirty(2));
    shard.unwatch(2);
    assert!(shard.watchers_by_key.is_none());
}

#[test]
fn key_notices_stay_empty_until_the_gate_is_open() {
    let mut shard = KvShard::new();

    shard.set_notify_mask(crate::notify::parse_mask("Eg$e").unwrap());
    shard.set_bytes(b"live", b"1");
    assert!(shard.delete(b"live"));
    assert!(shard.take_notices().is_empty());

    shard.set_notify_listeners(1);
    shard.set_bytes(b"live", b"1");
    assert_eq!(shard.get(b"live"), Some(&b"1"[..]));
    let pending = shard.take_notices();

    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].2, crate::notify::KeyEvent::Set);

    assert!(shard.delete(b"live"));
    let deleted = shard.take_notices();

    assert_eq!(deleted.len(), 1);
    assert_eq!(deleted[0].2, crate::notify::KeyEvent::Del);

    shard.set_bytes(b"gone", b"1");
    shard.take_notices();
    shard.apply_eviction(0, b"gone");
    let evicted = shard.take_notices();

    assert_eq!(evicted.len(), 1);
    assert_eq!(evicted[0].2, crate::notify::KeyEvent::Evicted);
    let frames = crate::notify::notices(
        shard.notify_mask(),
        evicted[0].0,
        &evicted[0].1,
        evicted[0].2,
    );

    assert_eq!(frames.len(), 1);
    assert_eq!(frames[0].channel, b"__keyevent@0__:evicted");
    assert_eq!(frames[0].payload, b"gone");
}

#[test]
fn deleting_an_expired_key_records_expired_not_del() {
    let clock = crate::time::TestClock::new(1_000);
    let mut shard = KvShard::with_clock(clock.clone());

    shard.set_notify_mask(crate::notify::parse_mask("Egx").unwrap());
    shard.set_notify_listeners(1);
    shard.set_bytes(b"temp", b"1");
    assert!(shard.take_notices().is_empty());
    assert!(shard.expire_at(b"temp", UnixMillis::new(1_500)));
    clock.set(1_500);
    assert!(!shard.delete(b"temp"));
    let pending = shard.take_notices();

    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].1.as_ref(), b"temp");
    assert_eq!(pending[0].2, crate::notify::KeyEvent::Expired);
    let frames = crate::notify::notices(
        shard.notify_mask(),
        pending[0].0,
        &pending[0].1,
        pending[0].2,
    );

    assert_eq!(frames[0].channel, b"__keyevent@0__:expired");
    assert_eq!(frames[0].payload, b"temp");
}

struct CountingWaker(std::sync::atomic::AtomicUsize);

impl std::task::Wake for CountingWaker {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }
}

#[test]
fn a_change_to_a_watched_key_wakes_the_task_waiting_on_it() {
    let mut shard = KvShard::new();
    let counter = Arc::new(CountingWaker(Default::default()));
    let waker = std::task::Waker::from(Arc::clone(&counter));
    let woken = || counter.0.load(std::sync::atomic::Ordering::SeqCst);
    let id = 1 << 63;

    shard.watch(id, 0, &[b"list"]);
    shard.wake_on_watch(id, &waker);
    shard.set_bytes(b"other", b"v");
    assert_eq!(woken(), 0);
    assert!(!shard.take_watch_change(id));

    shard
        .list_push_back(b"list", &[std::borrow::Cow::Borrowed(b"x".as_slice())])
        .unwrap();
    assert_eq!(woken(), 1);
    assert!(shard.take_watch_change(id));
    assert!(!shard.take_watch_change(id), "the change was taken");

    // A change made before the task waits again wakes it at once.
    shard
        .list_push_back(b"list", &[std::borrow::Cow::Borrowed(b"y".as_slice())])
        .unwrap();
    shard.wake_on_watch(id, &waker);
    assert_eq!(woken(), 2);

    shard.take_watch_change(id);
    shard.wake_on_watch(id, &waker);
    shard.unwatch(id);
    shard
        .list_push_back(b"list", &[std::borrow::Cow::Borrowed(b"z".as_slice())])
        .unwrap();
    assert_eq!(woken(), 2, "an unwatched task is not woken");
}

#[test]
fn deadline_changes_dirty_watches() {
    let mut shard = KvShard::new();
    shard.set_bytes(b"k", b"v");
    shard.watch(1, 0, &[b"k"]);
    let later = UnixMillis::new(shard.now_unix_millis().get() + 60_000);
    assert!(shard.expire_at(b"k", later));
    assert!(shard.take_watch_change(1), "EXPIRE must dirty WATCH");
    assert!(shard.persist(b"k"));
    assert!(shard.take_watch_change(1), "PERSIST must dirty WATCH");
    shard.keep_for(b"k", later.get());
    assert!(shard.take_watch_change(1), "GETEX must dirty WATCH");
}
