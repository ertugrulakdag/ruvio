//! Process-global string store for `hot_keys` prefixes.
//!
//! v1 is a striped concurrent map. A later hybrid-log impl must satisfy
//! `HotStringStore` without changing RESP or `hot_keys` matching.

use std::sync::Mutex;

use std::borrow::Cow;

use crate::engine::Response;
use crate::mutation::{Mutation, MutationSink, NoopMutationSink};

use crate::persistence::ShardCheckpoint;
use crate::protocol::{Command, SetCondition, SetExpiry, SetOptions, glob_match};

use crate::routing::command_keys;
use crate::store::{SnapshotEntry, SnapshotValue};

use crate::time::UnixMillis;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum HotDispatch {
    PassThrough,
    Handle,
    RejectUnsupported,
}

pub(crate) fn matches_hot_key(patterns: &[Vec<u8>], key: &[u8]) -> bool {
    !patterns.is_empty()
        && patterns
            .iter()
            .any(|pattern| !pattern.is_empty() && glob_match(pattern, key))
}

pub(crate) fn dispatch(patterns: &[Vec<u8>], command: &Command<'_>) -> HotDispatch {
    if patterns.is_empty() {
        return HotDispatch::PassThrough;
    }

    let keys = command_keys(command);

    if keys.is_empty() {
        return HotDispatch::PassThrough;
    }

    let matched = keys
        .iter()
        .copied()
        .filter(|key| matches_hot_key(patterns, key))
        .count();

    if matched == 0 {
        return HotDispatch::PassThrough;
    }

    if matched != keys.len() {
        return HotDispatch::RejectUnsupported;
    }

    match command {
        Command::Get(_) | Command::Set(_, _, _) | Command::Del(_) => HotDispatch::Handle,
        _ => HotDispatch::RejectUnsupported,
    }
}

pub(crate) fn unsupported(command: &Command<'_>) -> Response<'static> {
    Response::OwnedError(
        format!("ERR hot key does not support {}", command_verb(command)).into_bytes(),
    )
}

fn command_verb(command: &Command<'_>) -> &'static str {
    match command {
        Command::ListPush(_, _) => "lpush",
        Command::Watch(_) => "watch",
        Command::Increment(_, _) => "incr",
        Command::Multi => "multi",
        _ => "command",
    }
}

#[cfg_attr(not(test), allow(dead_code))]
pub(crate) fn execute<S: HotStringStore>(
    store: &S,
    now: UnixMillis,
    command: &Command<'_>,
) -> Result<Response<'static>, S::Error> {
    execute_with_write(store, now, command).map(|(response, _wrote)| response)
}

pub(crate) fn execute_with_write<S: HotStringStore>(
    store: &S,
    now: UnixMillis,
    command: &Command<'_>,
) -> Result<(Response<'static>, bool), S::Error> {
    match command {
        Command::Get(key) => Ok((
            Response::Bulk(store.get(now, key.as_ref())?.map(Cow::Owned)),
            false,
        )),
        Command::Set(key, value, options) => {
            let (applied, previous) = store.set(now, key.as_ref(), value.as_ref(), *options)?;
            let response = if options.get {
                Response::Bulk(previous.map(Cow::Owned))
            } else if applied {
                Response::Simple(b"OK")
            } else {
                Response::Bulk(None)
            };

            Ok((response, applied))
        }

        Command::Del(keys) => {
            let mut deleted = 0i64;

            for key in keys.iter() {
                if store.del(now, key.as_ref())? {
                    deleted += 1;
                }
            }

            Ok((Response::Integer(deleted), deleted > 0))
        }

        _ => Ok((unsupported(command), false)),
    }
}

/// Answers a hot-key command whose [`dispatch`] the caller already found; `None` when it
/// is not one (or there is no hot store).
pub(crate) fn reply_dispatched<S: HotStringStore>(
    store: Option<&S>,
    handled: HotDispatch,
    now: UnixMillis,
    command: &Command<'_>,
) -> Option<Result<(Response<'static>, bool), S::Error>> {
    let store = store?;

    match handled {
        HotDispatch::PassThrough => None,
        HotDispatch::RejectUnsupported => Some(Ok((unsupported(command), false))),
        HotDispatch::Handle => Some(execute_with_write(store, now, command)),
    }
}

/// The deadline a SET gives its key; `kept` is the key's current one, for KEEPTTL. A
/// deadline already past stores the key expired, as the main store does.
fn set_deadline(
    now: UnixMillis,
    expiry: SetExpiry,
    kept: impl FnOnce() -> Option<UnixMillis>,
) -> Option<UnixMillis> {
    match expiry {
        SetExpiry::None => None,
        SetExpiry::RelativeMs(millis) => u64::try_from(millis)
            .ok()
            .and_then(|millis| now.checked_add(millis)),
        SetExpiry::AbsoluteMs(at) => Some(UnixMillis::new(u64::try_from(at).unwrap_or(0))),
        SetExpiry::KeepTtl => kept(),
    }
}

pub(crate) trait HotStringStore: Send + Sync {
    type Error;

    fn get(&self, now: UnixMillis, key: &[u8]) -> Result<Option<Vec<u8>>, Self::Error>;
    fn set(
        &self,
        now: UnixMillis,
        key: &[u8],
        value: &[u8],
        options: SetOptions,
    ) -> Result<(bool, Option<Vec<u8>>), Self::Error>;
    fn del(&self, now: UnixMillis, key: &[u8]) -> Result<bool, Self::Error>;

    fn checkpoint(&self, now: UnixMillis, shard_id: u32, last_sequence: u64) -> ShardCheckpoint;
}

struct HotEntry {
    value: Vec<u8>,
    expire_at: Option<UnixMillis>,
}

pub(crate) struct ConcurrentMapStore<S> {
    entries: Mutex<ahash::AHashMap<Vec<u8>, HotEntry>>,
    sink: Mutex<S>,
    /// A replica: a key past its deadline reads as missing but stays until the
    /// primary's delete, as in the main store.
    keep_expired: std::sync::atomic::AtomicBool,
}

impl ConcurrentMapStore<NoopMutationSink> {
    pub(crate) fn memory() -> Self {
        Self::new(NoopMutationSink)
    }
}

pub(crate) type SharedMemoryStore = std::sync::Arc<ConcurrentMapStore<NoopMutationSink>>;

pub(crate) fn shared_memory_store(patterns: &[Vec<u8>]) -> Option<SharedMemoryStore> {
    (!patterns.is_empty()).then(|| std::sync::Arc::new(ConcurrentMapStore::memory()))
}

/// Records a hot-key write the shared store already applied. A SET is recorded with the
/// deadline it gave the key, so EX, PX, EXAT, PXAT and KEEPTTL all replay as they ran;
/// `kept` reads the deadline a KEEPTTL write left in place.
pub(crate) fn record_applied_hot_command(
    wal: &mut crate::persistence::WalSink,
    now: UnixMillis,
    kept: impl Fn(&[u8]) -> Option<UnixMillis>,
    command: &Command<'_>,
) -> Result<(), crate::persistence::PersistenceError> {
    wal.select_database(0);

    match command {
        Command::Set(key, value, options) => {
            match set_deadline(now, options.expiry, || kept(key.as_ref())) {
                Some(deadline) => wal.record(Mutation::SetBytesExpiring {
                    key: key.as_ref(),
                    value: value.as_ref(),
                    expires_at: deadline.get(),
                }),
                None => wal.record(Mutation::SetBytes {
                    key: key.as_ref(),
                    value: value.as_ref(),
                }),
            }
        }

        Command::Del(keys) => {
            let owned: Vec<Cow<[u8]>> = keys
                .iter()
                .map(|key| Cow::Owned(key.as_ref().to_vec()))
                .collect();

            wal.record(Mutation::Delete { keys: &owned })
        }

        // A read leaves nothing to record. Any other command is refused before it runs
        // (`dispatch`), so a new hot write must be given its record above.
        other => {
            debug_assert!(
                matches!(other, Command::Get(_)),
                "a hot write without a WAL record"
            );
            Ok(())
        }
    }
}

/// Removes every hot key, as FLUSHDB on database 0 and FLUSHALL do, and records one `Delete`.
/// True when there was a key to remove (and so a record was made, or refused).
pub(crate) fn flush(
    store: &SharedMemoryStore,
    wal: &mut crate::persistence::WalSink,
    now: UnixMillis,
) -> Result<bool, crate::persistence::PersistenceError> {
    let keys: Vec<Cow<[u8]>> = store.keys().into_iter().map(Cow::Owned).collect();

    if keys.is_empty() {
        return Ok(false);
    }

    let command = Command::Del(keys.into());
    let Ok(_) = execute_with_write(store.as_ref(), now, &command);

    record_applied_hot_command(wal, now, |_| None, &command).map(|()| true)
}

pub(crate) fn apply_owned(
    store: &SharedMemoryStore,
    patterns: &[Vec<u8>],
    mutation: &crate::OwnedMutation,
) -> bool {
    let keys = mutation.write_keys();

    if keys.is_empty() || !keys.iter().all(|key| matches_hot_key(patterns, key)) {
        return false;
    }

    let mut entries = store
        .entries
        .lock()
        .unwrap_or_else(|error| error.into_inner());

    match mutation {
        crate::OwnedMutation::SetBytes { key, value } => {
            entries.insert(
                key.clone(),
                HotEntry {
                    value: value.clone(),
                    expire_at: None,
                },
            );
            true
        }

        crate::OwnedMutation::SetBytesExpiring {
            key,
            value,
            expires_at,
        } => {
            entries.insert(
                key.clone(),
                HotEntry {
                    value: value.clone(),
                    expire_at: Some(UnixMillis::new(*expires_at)),
                },
            );
            true
        }

        crate::OwnedMutation::Delete { keys } => {
            for key in keys {
                entries.remove(key);
            }

            true
        }

        crate::OwnedMutation::ExpireAt { key, unix_millis } => {
            if let Some(entry) = entries.get_mut(key) {
                entry.expire_at = Some(UnixMillis::new(*unix_millis));
            }

            true
        }

        _ => false,
    }
}

impl<S: MutationSink> ConcurrentMapStore<S> {
    pub(crate) fn new(sink: S) -> Self {
        Self {
            entries: Mutex::new(ahash::AHashMap::new()),
            sink: Mutex::new(sink),
            keep_expired: std::sync::atomic::AtomicBool::new(false),
        }
    }

    pub(crate) fn keep_expired_keys(&self, on: bool) {
        self.keep_expired
            .store(on, std::sync::atomic::Ordering::Relaxed);
    }

    fn keeps_expired(&self) -> bool {
        self.keep_expired.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// The deadline `key` holds, expired or not.
    pub(crate) fn deadline(&self, key: &[u8]) -> Option<UnixMillis> {
        self.entries
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .get(key)
            .and_then(|entry| entry.expire_at)
    }

    pub(crate) fn keys(&self) -> Vec<Vec<u8>> {
        self.entries
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .keys()
            .cloned()
            .collect()
    }

    pub(crate) fn restore(&self, entries: Vec<SnapshotEntry>) {
        let mut map = self.entries.lock().unwrap_or_else(|e| e.into_inner());

        map.clear();

        for entry in entries {
            let SnapshotValue::Bytes(value) = entry.value else {
                continue;
            };

            map.insert(
                entry.key,
                HotEntry {
                    value,
                    expire_at: entry.expires_at,
                },
            );
        }
    }
}

fn take_hot(entries: &mut ahash::AHashMap<Vec<u8>, HotEntry>, key: &[u8]) -> Option<HotEntry> {
    let removed = entries.remove(key);

    if removed.is_some()
        && let Some(target) = crate::index::reclaim_capacity(entries.len(), entries.capacity())
    {
        entries.shrink_to(target);
    }

    removed
}

impl<S> HotStringStore for ConcurrentMapStore<S>
where
    S: MutationSink + Send,
    S::Error: Send,
{
    type Error = S::Error;

    fn get(&self, now: UnixMillis, key: &[u8]) -> Result<Option<Vec<u8>>, Self::Error> {
        let mut entries = self.entries.lock().unwrap_or_else(|e| e.into_inner());
        let expired = entries
            .get(key)
            .is_some_and(|entry| entry.expire_at.is_some_and(|deadline| deadline <= now));

        if expired && self.keeps_expired() {
            return Ok(None);
        }

        if expired {
            take_hot(&mut entries, key);
            let owned = std::borrow::Cow::Owned(key.to_vec());

            self.sink
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .record(Mutation::Delete { keys: &[owned] })?;

            return Ok(None);
        }

        Ok(entries.get(key).map(|entry| entry.value.clone()))
    }

    fn set(
        &self,
        now: UnixMillis,
        key: &[u8],
        value: &[u8],
        options: SetOptions,
    ) -> Result<(bool, Option<Vec<u8>>), Self::Error> {
        let mut entries = self.entries.lock().unwrap_or_else(|e| e.into_inner());

        if let Some(entry) = entries.get(key)
            && entry.expire_at.is_some_and(|deadline| deadline <= now)
        {
            take_hot(&mut entries, key);
        }

        let expire_at = set_deadline(now, options.expiry, || {
            entries.get(key).and_then(|entry| entry.expire_at)
        });

        let previous = options
            .get
            .then(|| entries.get(key).map(|entry| entry.value.clone()))
            .flatten();
        let exists = entries.contains_key(key);

        match options.condition {
            SetCondition::NotExists if exists => return Ok((false, previous)),
            SetCondition::Exists if !exists => return Ok((false, previous)),
            _ => {}
        }

        let mut sink = self.sink.lock().unwrap_or_else(|e| e.into_inner());

        match expire_at {
            Some(deadline) => sink.record(Mutation::SetBytesExpiring {
                key,
                value,
                expires_at: deadline.get(),
            })?,
            None => sink.record(Mutation::SetBytes { key, value })?,
        }

        drop(sink);
        entries.insert(
            key.to_vec(),
            HotEntry {
                value: value.to_vec(),
                expire_at,
            },
        );
        Ok((true, previous))
    }

    fn del(&self, now: UnixMillis, key: &[u8]) -> Result<bool, Self::Error> {
        let mut entries = self.entries.lock().unwrap_or_else(|e| e.into_inner());

        if let Some(entry) = entries.get(key)
            && entry.expire_at.is_some_and(|deadline| deadline <= now)
        {
            take_hot(&mut entries, key);

            return Ok(false);
        }

        if !entries.contains_key(key) {
            return Ok(false);
        }

        let owned = std::borrow::Cow::Owned(key.to_vec());

        self.sink
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .record(Mutation::Delete { keys: &[owned] })?;

        take_hot(&mut entries, key);
        Ok(true)
    }

    fn checkpoint(&self, now: UnixMillis, shard_id: u32, last_sequence: u64) -> ShardCheckpoint {
        let entries = self.entries.lock().unwrap_or_else(|e| e.into_inner());
        let keep = self.keeps_expired();

        ShardCheckpoint {
            shard_id,
            last_sequence,
            entries: entries
                .iter()
                .filter(|(_, entry)| {
                    keep || !entry.expire_at.is_some_and(|deadline| deadline <= now)
                })
                .map(|(key, entry)| SnapshotEntry {
                    database: 0,
                    key: key.clone(),
                    expires_at: entry.expire_at,
                    value: SnapshotValue::Bytes(entry.value.clone()),
                })
                .collect(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Barrier};

    use std::{convert::Infallible, thread};

    use crate::engine::Response;
    use crate::protocol::{Bytes, Command, SetCondition, SetExpiry};

    fn patterns(glob: &str) -> Vec<Vec<u8>> {
        vec![glob.as_bytes().to_vec()]
    }

    fn get(key: &[u8]) -> Command<'static> {
        Command::Get(Bytes::from(key.to_vec()))
    }

    fn set(key: &[u8], value: &[u8], options: SetOptions) -> Command<'static> {
        Command::Set(
            Bytes::from(key.to_vec()),
            Bytes::from(value.to_vec()),
            options,
        )
    }

    fn del(key: &[u8]) -> Command<'static> {
        Command::Del(vec![Bytes::from(key.to_vec())].into())
    }

    #[test]
    fn empty_patterns_never_match_and_always_pass_through() {
        assert!(!matches_hot_key(&[], b"lock:foo"));
        assert_eq!(
            dispatch(&[], &set(b"lock:foo", b"1", SetOptions::default())),
            HotDispatch::PassThrough
        );
    }

    #[test]
    fn glob_star_and_question_match_only_listed_prefixes() {
        let lock = patterns("lock:*");

        assert!(matches_hot_key(&lock, b"lock:foo"));
        assert!(!matches_hot_key(&lock, b"user:foo"));
        assert!(matches_hot_key(&patterns("lock:?"), b"lock:x"));
        assert!(!matches_hot_key(&patterns("lock:?"), b"lock:xy"));
    }

    #[test]
    fn dispatch_handles_get_set_del_and_rejects_other_matching_commands() {
        let lock = patterns("lock:*");

        assert_eq!(dispatch(&lock, &get(b"lock:foo")), HotDispatch::Handle);
        assert_eq!(
            dispatch(&lock, &set(b"lock:foo", b"1", SetOptions::default())),
            HotDispatch::Handle
        );
        assert_eq!(dispatch(&lock, &del(b"lock:foo")), HotDispatch::Handle);
        assert_eq!(dispatch(&lock, &get(b"user:1")), HotDispatch::PassThrough);
        assert_eq!(
            dispatch(
                &lock,
                &Command::ListPush(Bytes::from(&b"lock:foo"[..]), vec![Bytes::from(&b"x"[..])])
            ),
            HotDispatch::RejectUnsupported
        );
        assert_eq!(dispatch(&lock, &Command::Multi), HotDispatch::PassThrough);
        assert_eq!(
            dispatch(&lock, &Command::Watch(vec![Bytes::from(&b"lock:foo"[..])])),
            HotDispatch::RejectUnsupported
        );
    }

    #[test]
    fn set_then_get_round_trips_and_del_removes() {
        let store = ConcurrentMapStore::memory();
        let now = UnixMillis::new(1_000);

        assert_eq!(store.get(now, b"lock:a").unwrap(), None);
        assert!(
            store
                .set(now, b"lock:a", b"held", SetOptions::default())
                .unwrap()
                .0
        );
        assert_eq!(
            store.get(now, b"lock:a").unwrap().as_deref(),
            Some(&b"held"[..])
        );
        assert!(store.del(now, b"lock:a").unwrap());
        assert_eq!(store.get(now, b"lock:a").unwrap(), None);
        assert!(!store.del(now, b"lock:a").unwrap());
    }

    #[test]
    fn set_nx_xx_follow_redis_existence_rules() {
        let store = ConcurrentMapStore::memory();
        let now = UnixMillis::new(1);
        let nx = SetOptions {
            condition: SetCondition::NotExists,
            ..SetOptions::default()
        };

        let xx = SetOptions {
            condition: SetCondition::Exists,
            ..SetOptions::default()
        };

        assert!(store.set(now, b"lock:a", b"first", nx).unwrap().0);
        assert!(!store.set(now, b"lock:a", b"second", nx).unwrap().0);
        assert_eq!(
            store.get(now, b"lock:a").unwrap().as_deref(),
            Some(&b"first"[..])
        );
        assert!(!store.set(now, b"missing", b"x", xx).unwrap().0);
        assert!(store.set(now, b"lock:a", b"replaced", xx).unwrap().0);
        assert_eq!(
            store.get(now, b"lock:a").unwrap().as_deref(),
            Some(&b"replaced"[..])
        );
    }

    #[test]
    fn set_px_expires_and_lazy_get_returns_nil() {
        let store = ConcurrentMapStore::memory();
        let start = UnixMillis::new(10_000);
        let options = SetOptions {
            expiry: SetExpiry::RelativeMs(50),
            ..SetOptions::default()
        };

        assert!(store.set(start, b"lock:ttl", b"1", options).unwrap().0);
        assert_eq!(
            store.get(start, b"lock:ttl").unwrap().as_deref(),
            Some(&b"1"[..])
        );
        assert_eq!(
            store.get(UnixMillis::new(10_050), b"lock:ttl").unwrap(),
            None
        );
    }

    #[test]
    fn a_replica_hides_an_expired_hot_key_until_the_primary_deletes_it() {
        let store = Arc::new(ConcurrentMapStore::memory());
        let patterns = vec![b"lock:*".to_vec()];
        let apply = |mutation: crate::OwnedMutation| {
            assert!(apply_owned(&store, &patterns, &mutation));
        };

        store.keep_expired_keys(true);
        apply(crate::OwnedMutation::SetBytes {
            key: b"lock:a".to_vec(),
            value: b"v".to_vec(),
        });
        apply(crate::OwnedMutation::ExpireAt {
            key: b"lock:a".to_vec(),
            unix_millis: 2_000,
        });

        // Past the deadline by this node's clock: missing to readers, still stored.
        let late = UnixMillis::new(3_000);

        assert_eq!(store.get(late, b"lock:a").unwrap(), None);
        assert_eq!(store.checkpoint(late, 0, 0).entries.len(), 1);

        // The primary extended it before its own clock reached the deadline.
        apply(crate::OwnedMutation::ExpireAt {
            key: b"lock:a".to_vec(),
            unix_millis: 60_000,
        });
        assert_eq!(
            store.get(late, b"lock:a").unwrap().as_deref(),
            Some(&b"v"[..])
        );

        apply(crate::OwnedMutation::Delete {
            keys: vec![b"lock:a".to_vec()],
        });
        assert_eq!(store.get(late, b"lock:a").unwrap(), None);
        assert!(store.keys().is_empty());
    }

    struct FailingSink;

    impl MutationSink for FailingSink {
        type Error = &'static str;

        fn record(&mut self, _mutation: Mutation<'_>) -> Result<(), Self::Error> {
            Err("persistence unavailable")
        }
    }

    #[test]
    fn sink_failure_does_not_apply_set() {
        let store = ConcurrentMapStore::new(FailingSink);
        let now = UnixMillis::new(1);

        assert_eq!(
            store.set(now, b"lock:a", b"x", SetOptions::default()),
            Err("persistence unavailable")
        );
        assert_eq!(store.get(now, b"lock:a").unwrap(), None);
    }

    #[derive(Default)]
    struct RecordingSink {
        mutations: Vec<String>,
    }

    impl MutationSink for RecordingSink {
        type Error = Infallible;

        fn record(&mut self, mutation: Mutation<'_>) -> Result<(), Self::Error> {
            self.mutations.push(format!("{mutation:?}"));
            Ok(())
        }
    }

    #[test]
    fn records_set_before_visible_get() {
        let store = ConcurrentMapStore::new(RecordingSink::default());
        let now = UnixMillis::new(1);

        store
            .set(now, b"lock:a", b"v", SetOptions::default())
            .unwrap();
        assert_eq!(
            store.get(now, b"lock:a").unwrap().as_deref(),
            Some(&b"v"[..])
        );
        let sink = store.sink.lock().unwrap_or_else(|e| e.into_inner());

        assert!(
            sink.mutations.iter().any(|line| line.contains("SetBytes")),
            "{:?}",
            sink.mutations
        );
    }

    #[test]
    fn concurrent_set_nx_has_exactly_one_winner() {
        let store = Arc::new(ConcurrentMapStore::memory());
        let barrier = Arc::new(Barrier::new(8));
        let nx = SetOptions {
            condition: SetCondition::NotExists,
            ..SetOptions::default()
        };

        let now = UnixMillis::new(1);
        let winners: Vec<_> = (0..8)
            .map(|i| {
                let store = Arc::clone(&store);
                let barrier = Arc::clone(&barrier);
                thread::spawn(move || {
                    barrier.wait();
                    store
                        .set(now, b"lock:global", format!("t{i}").as_bytes(), nx)
                        .unwrap()
                        .0
                })
            })
            .collect();
        let wins = winners
            .into_iter()
            .map(|handle| handle.join().unwrap())
            .filter(|won| *won)
            .count();

        assert_eq!(wins, 1);
        assert!(store.get(now, b"lock:global").unwrap().is_some());
    }

    #[test]
    fn execute_get_set_del_and_nx_match_redis_replies() {
        let store = ConcurrentMapStore::memory();
        let now = UnixMillis::new(1);

        assert_eq!(
            execute(&store, now, &get(b"lock:a")).unwrap(),
            Response::Bulk(None)
        );
        assert_eq!(
            execute(&store, now, &set(b"lock:a", b"held", SetOptions::default())).unwrap(),
            Response::Simple(b"OK")
        );
        assert_eq!(
            execute(&store, now, &get(b"lock:a")).unwrap(),
            Response::Bulk(Some(Cow::Owned(b"held".to_vec())))
        );
        let nx = SetOptions {
            condition: SetCondition::NotExists,
            ..SetOptions::default()
        };

        assert_eq!(
            execute(&store, now, &set(b"lock:a", b"other", nx)).unwrap(),
            Response::Bulk(None)
        );
        assert_eq!(
            execute(&store, now, &del(b"lock:a")).unwrap(),
            Response::Integer(1)
        );
        assert_eq!(
            execute(&store, now, &del(b"lock:a")).unwrap(),
            Response::Integer(0)
        );
    }

    #[test]
    fn set_get_on_a_hot_key_returns_the_previous_string() {
        let store = ConcurrentMapStore::memory();
        let now = UnixMillis::new(1);

        execute(&store, now, &set(b"lock:a", b"old", SetOptions::default())).unwrap();
        let get_old = SetOptions {
            get: true,
            ..SetOptions::default()
        };

        assert_eq!(
            execute(&store, now, &set(b"lock:a", b"new", get_old)).unwrap(),
            Response::Bulk(Some(Cow::Owned(b"old".to_vec())))
        );
        assert_eq!(
            execute(&store, now, &get(b"lock:a")).unwrap(),
            Response::Bulk(Some(Cow::Owned(b"new".to_vec())))
        );
    }

    #[test]
    fn execute_rejects_unsupported_with_named_error() {
        let store = ConcurrentMapStore::memory();
        let now = UnixMillis::new(1);

        assert_eq!(
            execute(
                &store,
                now,
                &Command::ListPush(Bytes::from(&b"lock:foo"[..]), vec![Bytes::from(&b"x"[..])])
            )
            .unwrap(),
            unsupported(&Command::ListPush(
                Bytes::from(&b"lock:foo"[..]),
                vec![Bytes::from(&b"x"[..])]
            ))
        );
        assert_eq!(
            unsupported(&Command::ListPush(
                Bytes::from(&b"lock:foo"[..]),
                vec![Bytes::from(&b"x"[..])]
            )),
            Response::OwnedError(b"ERR hot key does not support lpush".to_vec())
        );
    }

    #[test]
    fn checkpoint_exports_live_strings_as_hot_partition() {
        let store = ConcurrentMapStore::memory();
        let start = UnixMillis::new(10_000);

        assert!(
            store
                .set(start, b"lock:a", b"held", SetOptions::default())
                .unwrap()
                .0
        );
        let ttl = SetOptions {
            expiry: SetExpiry::RelativeMs(50),
            ..SetOptions::default()
        };

        assert!(store.set(start, b"lock:ttl", b"1", ttl).unwrap().0);
        let later = UnixMillis::new(10_050);
        let checkpoint = store.checkpoint(later, 2, 7);

        assert_eq!(checkpoint.shard_id, 2);
        assert_eq!(checkpoint.last_sequence, 7);
        assert_eq!(checkpoint.entries.len(), 1);
        assert_eq!(checkpoint.entries[0].key, b"lock:a");
        assert_eq!(
            checkpoint.entries[0].value,
            SnapshotValue::Bytes(b"held".to_vec())
        );
    }

    #[test]
    fn deleting_most_hot_keys_shrinks_the_map() {
        let store = ConcurrentMapStore::memory();
        let now = UnixMillis::new(1);
        // Same reclaim check as the shard tests. Miri keeps the table just
        // above the shrink threshold instead of walking four thousand keys.
        let (inserted, removed) = if cfg!(miri) {
            (128, 120)
        } else {
            (4_000, 3_900)
        };

        for index in 0..inserted {
            let key = format!("lock:{index:04}");

            assert!(
                store
                    .set(now, key.as_bytes(), b"v", SetOptions::default())
                    .unwrap()
                    .0
            );
        }

        let grown = store.entries.lock().expect("lock").capacity();

        for index in 0..removed {
            let key = format!("lock:{index:04}");

            assert!(store.del(now, key.as_bytes()).unwrap());
        }

        let shrunk = store.entries.lock().expect("lock").capacity();

        assert!(
            shrunk.saturating_mul(4) < grown,
            "hot map stayed large after deletes: {shrunk} vs {grown}"
        );
    }

    #[test]
    fn apply_owned_writes_a_matching_set_only_to_the_hot_map() {
        let patterns = vec![b"lock:*".to_vec()];
        let store = shared_memory_store(&patterns).expect("patterns");
        let copied = crate::OwnedMutation::SetBytes {
            key: b"lock:a".to_vec(),
            value: b"held".to_vec(),
        };

        assert!(apply_owned(&store, &patterns, &copied));
        assert_eq!(
            execute(&*store, UnixMillis::new(1), &get(b"lock:a")).unwrap(),
            Response::Bulk(Some(Cow::Owned(b"held".to_vec())))
        );

        let other = crate::OwnedMutation::SetBytes {
            key: b"other".to_vec(),
            value: b"no".to_vec(),
        };

        assert!(!apply_owned(&store, &patterns, &other));
    }
}
