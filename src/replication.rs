use std::{
    collections::{HashMap, VecDeque},
    io::{self, Read, Write},
    net::{Shutdown, SocketAddr, TcpListener, TcpStream, ToSocketAddrs},
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
        mpsc::{Receiver, SyncSender, TrySendError},
    },
    thread::{self, JoinHandle},
    time::Duration,
};

use tracing::warn;

#[cfg(test)]
use crate::persistence::read_bounded_history;

use crate::{
    KvShard, OwnedMutation,
    persistence::{
        CheckpointManager, FrameCursor, FrameRead, PersistenceConfig, PersistenceError, WalSink,
        apply_mutation, decode_record, read_frames, read_frames_resuming,
    },
    protocol::ProtocolLimits,
};

pub const HANDSHAKE_MAGIC: &[u8; 8] = b"RUVREP01";
/// Handshake versions this build speaks; the version also names the request: resume from the
/// replica's log, resync from a snapshot at a sequence, or resync an incomplete image.
pub const VERSION_RESUME: u16 = 6;
pub const VERSION_RESYNC: u16 = 7;
pub const VERSION_RESYNC_INCOMPLETE: u16 = 8;
const SUPPORTED_VERSIONS: std::ops::RangeInclusive<u16> =
    VERSION_RESUME..=VERSION_RESYNC_INCOMPLETE;
/// Versions of builds whose handshake ended after the password and whose reply carried no
/// history id. They are refused in their own reply format so they log the refusal.
const LEGACY_VERSIONS: std::ops::RangeInclusive<u16> = 1..=5;
const LEGACY_REPLY_LEN: usize = 8 + 2 + 8 + CHECKSUM_LEN;
const VERSION_MISMATCH: &str =
    "primary and replicas must run the same Ruvio version (replication handshake version mismatch)";
pub const READONLY_LINE: &[u8] = b"READONLY You can't write against a read only replica.";
pub const UNCOPIED_LINE: &[u8] = b"ERR replication does not copy this command";
pub const RETRY_BACKOFF_MAX: Duration = Duration::from_secs(30);
pub const QUEUE_FRAMES: usize = 1024;
pub const QUEUE_BYTES: usize = 16 * 1024 * 1024;
const SHIPPER_READ_BYTES: usize = 16 * 1024 * 1024;
pub const RETRY_INTERVAL: Duration = Duration::from_secs(1);

/// First reconnect pause. Unit tests start replicas before primaries and must not wait a second.
#[cfg(not(test))]
const RETRY_START: Duration = RETRY_INTERVAL;
#[cfg(test)]
const RETRY_START: Duration = Duration::from_millis(20);
pub const SHIPPER_POLL: Duration = Duration::from_millis(10);
pub const MAX_REPLICAS: usize = 16;
/// Pause after an accept error (such as running out of file descriptors) before retrying.
const ACCEPT_ERROR_PAUSE: Duration = Duration::from_millis(100);

const HANDSHAKE_PREFIX: usize = 24;
const PASSWORD_LEN_OFFSET: usize = 22;
/// Identity of one data directory's write history.
pub type HistoryId = [u8; HISTORY_LEN];

pub const HISTORY_LEN: usize = 16;
/// History id (16) and shard count (4) follow the password; the checksum closes the frame.
const HANDSHAKE_TAIL: usize = HISTORY_LEN + 4;
const HANDSHAKE_MIN: usize = HANDSHAKE_PREFIX + HANDSHAKE_TAIL + CHECKSUM_LEN;
const REPLY_LEN: usize = 8 + 2 + 8 + HISTORY_LEN + CHECKSUM_LEN;
const CHECKSUM_LEN: usize = 4;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u16)]
pub enum HandshakeStatus {
    Ok = 1,
    Gap = 2,
    Auth = 3,
    Shard = 4,
    Version = 5,
    Ahead = 6,
    Local = 7,
    Snapshot = 8,
    /// The replica follows another write history than this primary's.
    History = 9,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Handshake {
    pub version: u16,
    pub shard_id: u32,
    pub after_sequence: u64,
    pub password: Vec<u8>,
    /// History of the primary this replica last followed; all zero when it has none.
    pub history: HistoryId,
    pub shard_count: u32,
}

pub fn encode_handshake(handshake: &Handshake) -> Result<Vec<u8>, ()> {
    let Ok(password_len) = u16::try_from(handshake.password.len()) else {
        return Err(());
    };

    let mut bytes = Vec::with_capacity(HANDSHAKE_MIN + handshake.password.len());

    bytes.extend_from_slice(HANDSHAKE_MAGIC);
    put_u16(&mut bytes, handshake.version);
    put_u32(&mut bytes, handshake.shard_id);
    put_u64(&mut bytes, handshake.after_sequence);
    put_u16(&mut bytes, password_len);
    bytes.extend_from_slice(&handshake.password);
    bytes.extend_from_slice(&handshake.history);
    put_u32(&mut bytes, handshake.shard_count);
    append_checksum(&mut bytes);
    Ok(bytes)
}

pub fn decode_handshake(bytes: &[u8]) -> Result<Handshake, ()> {
    if bytes.len() < HANDSHAKE_MIN {
        return Err(());
    }

    if &bytes[..HANDSHAKE_MAGIC.len()] != HANDSHAKE_MAGIC {
        return Err(());
    }

    let password_len = usize::from(read_u16(bytes, PASSWORD_LEN_OFFSET));

    if bytes.len() != HANDSHAKE_MIN + password_len {
        return Err(());
    }

    if !checksum_matches(bytes) {
        return Err(());
    }

    let version = read_u16(bytes, 8);
    let shard_id = read_u32(bytes, 10);
    let after_sequence = read_u64(bytes, 14);
    let tail = bytes.len() - CHECKSUM_LEN - HANDSHAKE_TAIL;
    let password = bytes[HANDSHAKE_PREFIX..tail].to_vec();
    let mut history = [0; HISTORY_LEN];

    history.copy_from_slice(&bytes[tail..tail + HISTORY_LEN]);
    let shard_count = read_u32(bytes, tail + HISTORY_LEN);

    Ok(Handshake {
        version,
        shard_id,
        after_sequence,
        password,
        history,
        shard_count,
    })
}

pub fn encode_reply(status: HandshakeStatus, next_sequence: u64) -> Vec<u8> {
    encode_reply_with_history(status, next_sequence, &[0; HISTORY_LEN])
}

pub fn encode_reply_with_history(
    status: HandshakeStatus,
    next_sequence: u64,
    history: &HistoryId,
) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(REPLY_LEN);

    bytes.extend_from_slice(HANDSHAKE_MAGIC);
    put_u16(&mut bytes, status as u16);
    put_u64(&mut bytes, next_sequence);
    bytes.extend_from_slice(history);
    append_checksum(&mut bytes);
    bytes
}

/// A refusal in the reply format of builds before the history id: magic, status, sequence,
/// checksum.
fn encode_legacy_reply(status: HandshakeStatus) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(LEGACY_REPLY_LEN);

    bytes.extend_from_slice(HANDSHAKE_MAGIC);
    put_u16(&mut bytes, status as u16);
    put_u64(&mut bytes, 0);
    append_checksum(&mut bytes);
    bytes
}

#[cfg_attr(not(test), allow(dead_code))]
pub fn decode_reply(bytes: &[u8]) -> Result<(HandshakeStatus, u64), ()> {
    decode_reply_with_history(bytes).map(|(status, next, _)| (status, next))
}

pub fn decode_reply_with_history(bytes: &[u8]) -> Result<(HandshakeStatus, u64, HistoryId), ()> {
    if bytes.len() != REPLY_LEN {
        return Err(());
    }

    if &bytes[..HANDSHAKE_MAGIC.len()] != HANDSHAKE_MAGIC {
        return Err(());
    }

    if !checksum_matches(bytes) {
        return Err(());
    }

    let status = status_from_u16(read_u16(bytes, 8))?;
    let next_sequence = read_u64(bytes, 10);
    let mut history = [0; HISTORY_LEN];

    history.copy_from_slice(&bytes[18..18 + HISTORY_LEN]);

    Ok((status, next_sequence, history))
}

/// Write-history identity of `data_dir`: random, created with the directory, so a wiped or
/// restored directory is a different history even if its sequence numbers line up.
pub fn load_or_create_history(data_dir: &Path) -> io::Result<HistoryId> {
    let path = data_dir.join("HISTORY");

    if let Some(id) = read_history_file(&path) {
        return Ok(id);
    }

    std::fs::create_dir_all(data_dir)?;
    let id = random_history_id();
    let staged = data_dir.join(format!(
        "HISTORY.{}.tmp",
        id.iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>()
    ));

    std::fs::write(&staged, id)?;
    std::fs::File::open(&staged)?.sync_all()?;
    // Linking never replaces: when another shard created the file first, its id wins.
    let linked = std::fs::hard_link(&staged, &path);

    let _ = std::fs::remove_file(&staged);
    #[cfg(unix)]
    std::fs::File::open(data_dir)?.sync_all()?;

    match linked {
        Ok(()) => Ok(id),
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
            read_history_file(&path).ok_or(error)
        }
        Err(error) => Err(error),
    }
}

/// Name of the file holding the history shard `shard_id`'s log continues. Shards resync on
/// their own, so each keeps its own record.
pub fn upstream_file_name(shard_id: u32) -> String {
    format!("UPSTREAM_{shard_id:04}")
}

/// History of the primary shard `shard_id` of this replica last synchronised from; zeros when
/// unknown, which makes the primary refuse a resume and send a full copy.
///
/// A directory copied from a primary has no upstream record but carries the primary's own
/// `HISTORY`, which is what it continues. A directory with the older shared `UPSTREAM` file
/// cannot tell which shard that record was written for, so it claims nothing: each shard
/// takes one full copy and records its own id.
pub fn upstream_history(data_dir: &Path, shard_id: u32) -> HistoryId {
    read_history_file(&data_dir.join(upstream_file_name(shard_id)))
        .or_else(|| {
            if data_dir.join("UPSTREAM").exists() {
                None
            } else {
                read_history_file(&data_dir.join("HISTORY"))
            }
        })
        .unwrap_or([0; HISTORY_LEN])
}

/// Records that shard `shard_id`'s log now continues history `id`. Called only once the log
/// really does: on a resume the primary accepted, or after a snapshot from it is installed.
pub fn record_upstream_history(data_dir: &Path, shard_id: u32, id: &HistoryId) -> io::Result<()> {
    if upstream_history(data_dir, shard_id) == *id {
        return Ok(());
    }

    write_history_file(&data_dir.join(upstream_file_name(shard_id)), id)
}

fn read_history_file(path: &Path) -> Option<HistoryId> {
    let bytes = std::fs::read(path).ok()?;

    <HistoryId>::try_from(bytes.as_slice()).ok()
}

fn write_history_file(path: &Path, id: &HistoryId) -> io::Result<()> {
    let temporary = path.with_extension("tmp");

    std::fs::write(&temporary, id)?;
    std::fs::File::open(&temporary)?.sync_all()?;
    std::fs::rename(&temporary, path)?;

    if let Some(directory) = path.parent() {
        #[cfg(unix)]
        std::fs::File::open(directory)?.sync_all()?;
        #[cfg(not(unix))]
        let _ = directory;
    }

    Ok(())
}

fn random_history_id() -> HistoryId {
    use std::hash::{BuildHasher, Hasher};

    let mut id = [0; HISTORY_LEN];

    for chunk in id.chunks_mut(8) {
        // `RandomState` is seeded from the operating system for each instance.
        let mut hasher = std::collections::hash_map::RandomState::new().build_hasher();

        hasher.write_u128(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_or(0, |elapsed| elapsed.as_nanos()),
        );
        hasher.write_u32(std::process::id());
        chunk.copy_from_slice(&hasher.finish().to_le_bytes());
    }

    if id == [0; HISTORY_LEN] {
        id[0] = 1;
    }

    id
}

#[derive(Debug, PartialEq, Eq)]
pub enum ApplyFailure {
    BadFrame,
    NeedsSnapshot,
    /// The frame's sequence is already in this replica's log: an earlier link queued it and
    /// the next link, which resumes from the applied cursor, sent it again.
    Applied,
    Sequence,
    Shard,
    Persist(String),
}

pub fn accept_frame(
    wal: &mut WalSink,
    frame: &[u8],
    limits: ProtocolLimits,
) -> Result<(u8, crate::OwnedMutation), ApplyFailure> {
    let record = decode_record(frame, wal.shard_id(), limits).map_err(decode_failure)?;

    if record.sequence != wal.last_sequence() + 1 {
        return Err(ApplyFailure::Sequence);
    }

    wal.append_frame(frame).map_err(persist_failure)?;
    Ok((record.database, record.mutation))
}

/// Refuse a delta whose prerequisite JSON document is no longer available locally.
/// Only a newer primary snapshot can safely recover the document: an expired
/// deadline does not prove that a later PERSIST left the document absent.
pub(crate) fn accept_replica_frame(
    shard: &mut KvShard,
    wal: &mut WalSink,
    _checkpoints: &CheckpointManager,
    _config: &PersistenceConfig,
    frame: &[u8],
    limits: ProtocolLimits,
) -> Result<(u8, OwnedMutation, bool), ApplyFailure> {
    let record = decode_record(frame, wal.shard_id(), limits).map_err(decode_failure)?;
    // The link resumes from the last applied sequence, while frames an earlier link already
    // queued may still be waiting in the shard's queue: the new link resends those. Under one
    // history a sequence names one record, so the copy is already in the log.
    if record.sequence <= wal.last_sequence() {
        return Err(ApplyFailure::Applied);
    }
    if record.sequence != wal.last_sequence() + 1 {
        return Err(ApplyFailure::Sequence);
    }

    let missing = match &record.mutation {
        OwnedMutation::Transaction { mutations } => {
            transaction_needs_snapshot(shard, record.database, mutations)
        }
        mutation => {
            let previous = shard.active_database();
            shard.enter(record.database);
            let missing = needs_snapshot(shard, mutation);
            shard.enter(previous);
            missing
        }
    };
    if missing {
        return Err(ApplyFailure::NeedsSnapshot);
    }

    wal.append_frame(frame).map_err(persist_failure)?;
    Ok((record.database, record.mutation, false))
}

/// Track only key kinds, never clone documents or apply edits during the WAL preflight.
/// The map is local to transactions with JSON/TTL prerequisites; ordinary frames
/// retain their single decode-and-append path.
struct TransactionKinds {
    changed: HashMap<(u8, Vec<u8>), Option<crate::store::ValueKind>>,
    original: [Option<u8>; 256],
}

impl TransactionKinds {
    fn new() -> Self {
        Self {
            changed: HashMap::new(),
            original: std::array::from_fn(|db| Some(db as u8)),
        }
    }

    fn kind(&self, shard: &mut KvShard, db: u8, key: &[u8]) -> Option<crate::store::ValueKind> {
        if let Some(kind) = self.changed.get(&(db, key.to_vec())) {
            return *kind;
        }
        let original = self.original[db as usize]?;
        shard.enter(original);
        shard.value_kind(key)
    }

    fn set(&mut self, db: u8, key: &[u8], kind: Option<crate::store::ValueKind>) {
        self.changed.insert((db, key.to_vec()), kind);
    }

    fn clear_database(&mut self, db: u8) {
        self.changed.retain(|(database, _), _| *database != db);
        self.original[db as usize] = None;
    }

    fn swap(&mut self, first: u8, second: u8) {
        self.original.swap(first as usize, second as usize);
        let mut changed = HashMap::with_capacity(self.changed.len());
        for ((db, key), kind) in self.changed.drain() {
            let db = if db == first {
                second
            } else if db == second {
                first
            } else {
                db
            };
            changed.insert((db, key), kind);
        }
        self.changed = changed;
    }
}

fn transaction_needs_snapshot(shard: &mut KvShard, db: u8, mutations: &[OwnedMutation]) -> bool {
    use crate::store::ValueKind;

    if !mutations.iter().any(|mutation| {
        matches!(
            mutation,
            OwnedMutation::JsonSetPath { .. }
                | OwnedMutation::JsonMergePath { .. }
                | OwnedMutation::JsonDelPath { .. }
                | OwnedMutation::Persist { .. }
                | OwnedMutation::ExpireAt { .. }
        )
    }) {
        return false;
    }

    let previous = shard.active_database();
    let mut kinds = TransactionKinds::new();
    let mut missing = false;
    for mutation in mutations {
        match mutation {
            OwnedMutation::JsonSetPath { key, .. }
            | OwnedMutation::JsonMergePath { key, .. }
            | OwnedMutation::JsonDelPath { key, .. } => {
                missing = kinds.kind(shard, db, key) != Some(ValueKind::Json);
            }
            OwnedMutation::Persist { key } => {
                missing = kinds.kind(shard, db, key).is_none();
            }
            // A replica keeps keys past its own clock until the primary deletes them, so
            // a deadline for a key it does not hold is a divergence.
            OwnedMutation::ExpireAt { key, .. } => {
                missing = kinds.kind(shard, db, key).is_none();
            }
            // A replica applies frames on a paused clock and keeps a key past its deadline
            // until the primary deletes it, so a value with a deadline always leaves a key.
            OwnedMutation::SetBytesExpiring { key, .. }
            | OwnedMutation::SetBytes { key, .. }
            | OwnedMutation::SetRange { key, .. }
            | OwnedMutation::IdempotencyPut { key, .. }
            | OwnedMutation::SetInteger { key, .. } => kinds.set(db, key, Some(ValueKind::Bytes)),
            OwnedMutation::JsonPut { key, .. } => kinds.set(db, key, Some(ValueKind::Json)),
            OwnedMutation::SetVersioned { key, .. } => {
                kinds.set(db, key, Some(ValueKind::Versioned))
            }
            OwnedMutation::LeasePut { key, .. } => kinds.set(db, key, Some(ValueKind::Lease)),
            OwnedMutation::LimitPut { key, .. } => kinds.set(db, key, Some(ValueKind::Limit)),
            OwnedMutation::OncePut { key, .. } => kinds.set(db, key, Some(ValueKind::Once)),
            OwnedMutation::ListPushFront { key, values }
            | OwnedMutation::ListPushBack { key, values }
                if !values.is_empty() =>
            {
                kinds.set(db, key, Some(ValueKind::List));
            }
            OwnedMutation::SetAdd { key, members } if !members.is_empty() => {
                kinds.set(db, key, Some(ValueKind::Set));
            }
            OwnedMutation::SetStore { key, members } => {
                kinds.set(db, key, (!members.is_empty()).then_some(ValueKind::Set));
            }
            OwnedMutation::HashSet { key, pairs } if !pairs.is_empty() => {
                kinds.set(db, key, Some(ValueKind::Hash));
            }
            OwnedMutation::SortedSetAdd { key, pairs } if !pairs.is_empty() => {
                kinds.set(db, key, Some(ValueKind::SortedSet));
            }
            OwnedMutation::BloomReserve { key, .. }
            | OwnedMutation::BloomAdd { key, .. }
            | OwnedMutation::BloomAddMany { key, .. } => {
                kinds.set(db, key, Some(ValueKind::BloomFilter));
            }
            OwnedMutation::StreamAppend { key, .. } => kinds.set(db, key, Some(ValueKind::Stream)),
            OwnedMutation::StreamGroupCreate {
                key,
                mkstream: true,
                ..
            } => {
                kinds.set(db, key, Some(ValueKind::Stream));
            }
            OwnedMutation::SemaphoreAcquire { key, .. } => {
                kinds.set(db, key, Some(ValueKind::Semaphore));
            }
            OwnedMutation::Delete { keys } => {
                for key in keys {
                    kinds.set(db, key, None);
                }
            }
            OwnedMutation::Rename {
                source,
                destination,
            } => {
                let kind = kinds.kind(shard, db, source);
                missing = kind.is_none();
                kinds.set(db, source, None);
                kinds.set(db, destination, kind);
            }
            OwnedMutation::MoveKey { key, destination } => {
                let kind = kinds.kind(shard, db, key);
                missing = kind.is_none() || kinds.kind(shard, *destination, key).is_some();
                kinds.set(db, key, None);
                kinds.set(*destination, key, kind);
            }
            OwnedMutation::FlushDatabase => kinds.clear_database(db),
            OwnedMutation::FlushAll => {
                kinds.changed.clear();
                kinds.original.fill(None);
            }
            OwnedMutation::SwapDatabases { first, second } => kinds.swap(*first, *second),
            // These operations can remove the final member. Treat their result as
            // absent rather than risk accepting a later path/TTL dependency.
            OwnedMutation::ListPopFront { key }
            | OwnedMutation::ListPopBack { key }
            | OwnedMutation::ListTrim { key, .. }
            | OwnedMutation::ListRemove { key, .. }
            | OwnedMutation::SetRemove { key, .. }
            | OwnedMutation::HashDelete { key, .. }
            | OwnedMutation::SortedSetRemove { key, .. }
            | OwnedMutation::StreamTrim { key, .. }
            | OwnedMutation::StreamTrimMinId { key, .. }
            | OwnedMutation::SemaphoreRelease { key, .. } => kinds.set(db, key, None),
            OwnedMutation::SetMove {
                source,
                destination,
                ..
            } => {
                kinds.set(db, source, None);
                kinds.set(db, destination, Some(ValueKind::Set));
            }
            // An empty push or add creates nothing, and a group without MKSTREAM needs a
            // stream that already exists.
            OwnedMutation::ListPushFront { .. }
            | OwnedMutation::ListPushBack { .. }
            | OwnedMutation::SetAdd { .. }
            | OwnedMutation::HashSet { .. }
            | OwnedMutation::SortedSetAdd { .. }
            | OwnedMutation::StreamGroupCreate { .. } => {}
            // These change a value in place: they never create or remove a key. A replica
            // keeps fields past their deadline until the primary deletes them.
            OwnedMutation::ListSet { .. }
            | OwnedMutation::ListInsert { .. }
            | OwnedMutation::HashFieldExpire { .. }
            | OwnedMutation::HashFieldPersist { .. }
            | OwnedMutation::StreamDelete { .. }
            | OwnedMutation::StreamSetId { .. }
            | OwnedMutation::StreamClaim { .. }
            | OwnedMutation::StreamReclaim { .. }
            | OwnedMutation::StreamForceClaim { .. }
            | OwnedMutation::StreamAck { .. }
            | OwnedMutation::StreamGroupDestroy { .. }
            | OwnedMutation::StreamGroupSetId { .. }
            | OwnedMutation::StreamGroupDelConsumer { .. }
            | OwnedMutation::StreamGroupCreateConsumer { .. }
            | OwnedMutation::StreamHold { .. }
            | OwnedMutation::StreamGroupAdvance { .. } => {}
            // A transaction record never nests another one.
            OwnedMutation::Transaction { .. } => {}
        }
        if missing {
            break;
        }
    }
    shard.enter(previous);
    missing
}

fn needs_snapshot(shard: &mut KvShard, mutation: &OwnedMutation) -> bool {
    match mutation {
        // The primary records a rename or move only for a key it holds: a replica without
        // it has diverged and would fail the apply.
        OwnedMutation::Rename { source, .. } => shard.value_kind(source).is_none(),
        OwnedMutation::MoveKey { key, destination } => {
            if shard.value_kind(key).is_none() {
                return true;
            }
            let current = shard.active_database();
            shard.enter(*destination);
            let taken = shard.value_kind(key).is_some();
            shard.enter(current);
            taken
        }
        OwnedMutation::JsonSetPath { key, .. }
        | OwnedMutation::JsonMergePath { key, .. }
        | OwnedMutation::JsonDelPath { key, .. } => {
            shard.value_kind(key) != Some(crate::store::ValueKind::Json)
        }
        OwnedMutation::Persist { key } | OwnedMutation::ExpireAt { key, .. } => {
            shard.value_kind(key).is_none()
        }
        // Every other record applies whether or not the replica holds the key.
        OwnedMutation::IdempotencyPut { .. }
        | OwnedMutation::SetBytes { .. }
        | OwnedMutation::SetBytesExpiring { .. }
        | OwnedMutation::JsonPut { .. }
        | OwnedMutation::SetInteger { .. }
        | OwnedMutation::Delete { .. }
        | OwnedMutation::ListPushFront { .. }
        | OwnedMutation::ListPopFront { .. }
        | OwnedMutation::ListPushBack { .. }
        | OwnedMutation::ListPopBack { .. }
        | OwnedMutation::ListTrim { .. }
        | OwnedMutation::SetRange { .. }
        | OwnedMutation::ListSet { .. }
        | OwnedMutation::ListInsert { .. }
        | OwnedMutation::ListRemove { .. }
        | OwnedMutation::SetAdd { .. }
        | OwnedMutation::SetRemove { .. }
        | OwnedMutation::SetStore { .. }
        | OwnedMutation::SetMove { .. }
        | OwnedMutation::HashSet { .. }
        | OwnedMutation::HashDelete { .. }
        | OwnedMutation::HashFieldExpire { .. }
        | OwnedMutation::HashFieldPersist { .. }
        | OwnedMutation::SemaphoreAcquire { .. }
        | OwnedMutation::SemaphoreRelease { .. }
        | OwnedMutation::SortedSetAdd { .. }
        | OwnedMutation::SortedSetRemove { .. }
        | OwnedMutation::BloomReserve { .. }
        | OwnedMutation::BloomAdd { .. }
        | OwnedMutation::BloomAddMany { .. }
        | OwnedMutation::StreamAppend { .. }
        | OwnedMutation::StreamDelete { .. }
        | OwnedMutation::StreamTrim { .. }
        | OwnedMutation::StreamTrimMinId { .. }
        | OwnedMutation::StreamSetId { .. }
        | OwnedMutation::StreamGroupCreate { .. }
        | OwnedMutation::StreamClaim { .. }
        | OwnedMutation::StreamReclaim { .. }
        | OwnedMutation::StreamForceClaim { .. }
        | OwnedMutation::StreamAck { .. }
        | OwnedMutation::StreamGroupDestroy { .. }
        | OwnedMutation::StreamGroupSetId { .. }
        | OwnedMutation::StreamGroupDelConsumer { .. }
        | OwnedMutation::StreamGroupCreateConsumer { .. }
        | OwnedMutation::StreamHold { .. }
        | OwnedMutation::StreamGroupAdvance { .. }
        | OwnedMutation::FlushDatabase
        | OwnedMutation::FlushAll
        | OwnedMutation::SwapDatabases { .. }
        | OwnedMutation::LeasePut { .. }
        | OwnedMutation::SetVersioned { .. }
        | OwnedMutation::LimitPut { .. }
        | OwnedMutation::OncePut { .. }
        | OwnedMutation::Transaction { .. } => false,
    }
}

#[cfg_attr(not(test), allow(dead_code))]
pub fn apply_frame(
    shard: &mut KvShard,
    wal: &mut WalSink,
    frame: &[u8],
    limits: ProtocolLimits,
) -> Result<(), ApplyFailure> {
    let (database, mutation) = accept_frame(wal, frame, limits)?;

    apply_mutation(shard, database, &mutation).map_err(persist_failure)
}

pub const PUBLISH_MAGIC: &[u8; 8] = b"RUVPUB01";
pub const ACK_MAGIC: &[u8; 8] = b"RUVACK01";
const ACK_LEN: usize = 16;

pub fn encode_ack(sequence: u64) -> [u8; ACK_LEN] {
    let mut bytes = [0; ACK_LEN];

    bytes[..8].copy_from_slice(ACK_MAGIC);
    bytes[8..].copy_from_slice(&sequence.to_le_bytes());
    bytes
}

pub fn decode_ack(bytes: &[u8]) -> Option<u64> {
    if bytes.len() != ACK_LEN || &bytes[..8] != ACK_MAGIC {
        return None;
    }

    Some(u64::from_le_bytes(bytes[8..].try_into().ok()?))
}

/// Durable sequences reported by connected replicas. `SET` does not read this.
pub struct DurableAcks {
    slots: Mutex<Vec<Option<u64>>>,
    notify: tokio::sync::Notify,
}

impl DurableAcks {
    pub fn new() -> Self {
        Self {
            slots: Mutex::new(Vec::new()),
            notify: tokio::sync::Notify::new(),
        }
    }

    pub fn reserve(&self, index: usize) {
        let mut slots = self.slots.lock().unwrap_or_else(|error| error.into_inner());

        if slots.len() <= index {
            slots.resize(index + 1, None);
        }

        slots[index] = Some(0);
    }

    pub fn observe(&self, index: usize, sequence: u64) {
        let mut slots = self.slots.lock().unwrap_or_else(|error| error.into_inner());

        if slots.len() <= index {
            slots.resize(index + 1, None);
        }

        let current = slots[index].unwrap_or(0);

        if sequence > current {
            slots[index] = Some(sequence);
            self.notify.notify_waiters();
        }
    }

    pub fn clear(&self, index: usize) {
        let mut slots = self.slots.lock().unwrap_or_else(|error| error.into_inner());

        if let Some(slot) = slots.get_mut(index) {
            *slot = None;
            self.notify.notify_waiters();
        }
    }

    /// How many connected replicas have synced `target` to disk.
    pub fn reached(&self, target: u64) -> u64 {
        let slots = self.slots.lock().unwrap_or_else(|error| error.into_inner());

        slots
            .iter()
            .filter(|slot| slot.is_some_and(|sequence| sequence >= target))
            .count() as u64
    }

    pub async fn notified(&self) {
        self.notify.notified().await;
    }
}

pub fn encode_copied_publish(sharded: bool, channel: &[u8], message: &[u8]) -> Vec<u8> {
    let mut body = Vec::new();

    body.extend_from_slice(PUBLISH_MAGIC);
    body.push(u8::from(sharded));
    body.extend_from_slice(&(u32::try_from(channel.len()).unwrap_or(u32::MAX)).to_le_bytes());
    body.extend_from_slice(channel);
    body.extend_from_slice(&(u32::try_from(message.len()).unwrap_or(u32::MAX)).to_le_bytes());
    body.extend_from_slice(message);

    while 4 + body.len() + 4 < 32 {
        body.push(0);
    }

    let mut frame = Vec::with_capacity(4 + body.len() + 4);
    let total = u32::try_from(4 + body.len() + 4).unwrap_or(u32::MAX);

    frame.extend_from_slice(&total.to_le_bytes());
    frame.extend_from_slice(&body);
    let checksum = crc32c::crc32c(&frame);

    frame.extend_from_slice(&checksum.to_le_bytes());
    frame
}

pub fn decode_copied_publish(frame: &[u8]) -> Option<(bool, Vec<u8>, Vec<u8>)> {
    if frame.len() < 32 || u32::from_le_bytes(frame[..4].try_into().ok()?) as usize != frame.len() {
        return None;
    }

    if &frame[4..12] != PUBLISH_MAGIC {
        return None;
    }

    let checksum_at = frame.len() - 4;

    if crc32c::crc32c(&frame[..checksum_at])
        != u32::from_le_bytes(frame[checksum_at..].try_into().ok()?)
    {
        return None;
    }

    let sharded = frame[12] == 1;
    let channel_len = u32::from_le_bytes(frame[13..17].try_into().ok()?) as usize;
    let channel_at: usize = 17;
    let channel_end = channel_at.checked_add(channel_len)?;

    if channel_end + 4 > checksum_at {
        return None;
    }

    let message_len =
        u32::from_le_bytes(frame[channel_end..channel_end + 4].try_into().ok()?) as usize;
    let message_at = channel_end + 4;
    let message_end = message_at.checked_add(message_len)?;

    if message_end > checksum_at {
        return None;
    }

    Some((
        sharded,
        frame[channel_at..channel_end].to_vec(),
        frame[message_at..message_end].to_vec(),
    ))
}

pub struct PublishFanout {
    senders: Mutex<Vec<SyncSender<Vec<u8>>>>,
}

impl PublishFanout {
    pub fn new() -> Self {
        Self {
            senders: Mutex::new(Vec::new()),
        }
    }

    pub fn register(&self) -> std::sync::mpsc::Receiver<Vec<u8>> {
        let (sender, receiver) = std::sync::mpsc::sync_channel(64);

        self.senders
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .push(sender);
        receiver
    }

    pub fn send(&self, frame: Vec<u8>) {
        let mut senders = self
            .senders
            .lock()
            .unwrap_or_else(|error| error.into_inner());

        senders.retain(|sender| match sender.try_send(frame.clone()) {
            Ok(()) => true,
            Err(TrySendError::Full(_)) => true,
            Err(TrySendError::Disconnected(_)) => false,
        });
    }
}

fn decode_failure(error: PersistenceError) -> ApplyFailure {
    match error {
        PersistenceError::Corruption(message) if message == "WAL record shard mismatch" => {
            ApplyFailure::Shard
        }

        _ => ApplyFailure::BadFrame,
    }
}

fn persist_failure(error: PersistenceError) -> ApplyFailure {
    ApplyFailure::Persist(error.to_string())
}

fn status_from_u16(value: u16) -> Result<HandshakeStatus, ()> {
    match value {
        1 => Ok(HandshakeStatus::Ok),
        2 => Ok(HandshakeStatus::Gap),
        3 => Ok(HandshakeStatus::Auth),
        4 => Ok(HandshakeStatus::Shard),
        5 => Ok(HandshakeStatus::Version),
        6 => Ok(HandshakeStatus::Ahead),
        7 => Ok(HandshakeStatus::Local),
        8 => Ok(HandshakeStatus::Snapshot),
        9 => Ok(HandshakeStatus::History),
        _ => Err(()),
    }
}

fn checksum_matches(bytes: &[u8]) -> bool {
    let checksum_at = bytes.len() - CHECKSUM_LEN;
    let expected = read_u32(bytes, checksum_at);

    crc32c::crc32c(&bytes[..checksum_at]) == expected
}

fn append_checksum(bytes: &mut Vec<u8>) {
    let checksum = crc32c::crc32c(bytes);

    put_u32(bytes, checksum);
}

fn read_u16(bytes: &[u8], offset: usize) -> u16 {
    u16::from_le_bytes(bytes[offset..offset + 2].try_into().unwrap())
}

fn read_u32(bytes: &[u8], offset: usize) -> u32 {
    u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap())
}

fn read_u64(bytes: &[u8], offset: usize) -> u64 {
    u64::from_le_bytes(bytes[offset..offset + 8].try_into().unwrap())
}

fn put_u16(output: &mut Vec<u8>, value: u16) {
    output.extend_from_slice(&value.to_le_bytes());
}

fn put_u32(output: &mut Vec<u8>, value: u32) {
    output.extend_from_slice(&value.to_le_bytes());
}

fn put_u64(output: &mut Vec<u8>, value: u64) {
    output.extend_from_slice(&value.to_le_bytes());
}

pub struct Shipment {
    pub listen: SocketAddr,
    pub shard_id: u32,
    pub published: Arc<AtomicU64>,
    pub data_dir: PathBuf,
    pub requirepass: Vec<u8>,
    pub limits: ProtocolLimits,
    pub max_replicas: usize,
    pub backlog_floor: Arc<AtomicU64>,
    pub replica_count: Arc<AtomicU64>,
    pub acks: Arc<DurableAcks>,
    pub fanout: Arc<PublishFanout>,
    pub snapshot_max_bytes: u64,
    pub shard_count: u32,
    pub history: HistoryId,
    /// Request a coordinated checkpoint after a rejected delta; never used by ordinary streams.
    pub snapshot_requests: Arc<AtomicU64>,
}

struct ReplicaProgress {
    floor: Arc<AtomicU64>,
    count: Arc<AtomicU64>,
    acks: Arc<DurableAcks>,
    cursors: Mutex<Vec<Option<u64>>>,
}

impl ReplicaProgress {
    fn new(floor: Arc<AtomicU64>, count: Arc<AtomicU64>, acks: Arc<DurableAcks>) -> Self {
        Self {
            floor,
            count,
            acks,
            cursors: Mutex::new(Vec::new()),
        }
    }

    fn hold(self: &Arc<Self>, cursor: u64) -> CursorHold {
        let mut cursors = self
            .cursors
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let index = cursors
            .iter()
            .position(|slot| slot.is_none())
            .unwrap_or_else(|| {
                cursors.push(None);
                cursors.len() - 1
            });

        cursors[index] = Some(cursor);
        self.acks.reserve(index);
        publish_floor(&self.floor, &self.count, &cursors);
        CursorHold {
            progress: Arc::clone(self),
            index,
        }
    }
}

struct CursorHold {
    progress: Arc<ReplicaProgress>,
    index: usize,
}

impl CursorHold {
    fn advance(&self, cursor: u64) {
        let mut cursors = self
            .progress
            .cursors
            .lock()
            .unwrap_or_else(|error| error.into_inner());

        if let Some(slot) = cursors.get_mut(self.index) {
            *slot = Some(cursor);
        }

        publish_floor(&self.progress.floor, &self.progress.count, &cursors);
    }

    fn ack(&self, sequence: u64) {
        self.progress.acks.observe(self.index, sequence);
    }
}

impl Drop for CursorHold {
    fn drop(&mut self) {
        let mut cursors = self
            .progress
            .cursors
            .lock()
            .unwrap_or_else(|error| error.into_inner());

        if let Some(slot) = cursors.get_mut(self.index) {
            *slot = None;
        }

        self.progress.acks.clear(self.index);
        publish_floor(&self.progress.floor, &self.progress.count, &cursors);
    }
}

fn publish_floor(floor: &AtomicU64, count: &AtomicU64, cursors: &[Option<u64>]) {
    let connected = cursors.iter().flatten().copied();
    let mut retained = u64::MAX;
    let mut replicas = 0u64;

    for cursor in connected {
        retained = retained.min(cursor);
        replicas = replicas.saturating_add(1);
    }

    floor.store(retained, Ordering::Release);
    count.store(replicas, Ordering::Release);
}

pub struct ReplicationThread {
    stop: Arc<AtomicBool>,
    wake: Arc<Mutex<Option<TcpStream>>>,
    join: Option<JoinHandle<()>>,
}

impl ReplicationThread {
    /// The thread that reads from the primary, so the shard can wake it once it has made
    /// room in the inbound queue.
    pub fn reader(&self) -> Option<thread::Thread> {
        self.join.as_ref().map(|join| join.thread().clone())
    }
}

impl Drop for ReplicationThread {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);

        if let Ok(mut slot) = self.wake.lock()
            && let Some(stream) = slot.take()
        {
            let _ = stream.shutdown(Shutdown::Both);
        }

        if let Some(handle) = self.join.take() {
            let _ = handle.join();
        }
    }
}

pub fn spawn_primary(shipment: Shipment) -> io::Result<ReplicationThread> {
    let listener = TcpListener::bind(shipment.listen)?;

    listener.set_nonblocking(true)?;
    let stop = Arc::new(AtomicBool::new(false));
    let shipment = Arc::new(shipment);
    let thread_stop = Arc::clone(&stop);
    let join = thread::Builder::new()
        .name(format!("ruvio-ship-{}", shipment.shard_id))
        .spawn(move || accept_replicas(listener, shipment, thread_stop))?;

    Ok(ReplicationThread {
        stop,
        wake: Arc::new(Mutex::new(None)),
        join: Some(join),
    })
}

pub enum ReplicaDelivery {
    Frame(Vec<u8>),
    Snapshot {
        shard: Vec<u8>,
        hot: Option<Vec<u8>>,
        catchup_through: u64,
        /// The primary's history: the replica records it once the snapshot is installed.
        history: HistoryId,
    },
    CaughtUp(u64),
}

/// What the replica reader tells the rest of the process about its connection.
#[derive(Debug, Default)]
pub struct LinkStatus {
    up: AtomicBool,
    last_io_millis: AtomicU64,
}

impl LinkStatus {
    fn mark_up(&self) {
        self.up.store(true, Ordering::Release);
        self.touch();
    }

    fn mark_down(&self) {
        self.up.store(false, Ordering::Release);
    }

    fn touch(&self) {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |elapsed| elapsed.as_millis() as u64);

        self.last_io_millis.store(now, Ordering::Release);
    }

    pub fn is_up(&self) -> bool {
        self.up.load(Ordering::Acquire)
    }

    /// Seconds since the last frame arrived, or `None` when nothing ever did.
    pub fn seconds_since_io(&self) -> Option<u64> {
        let last = self.last_io_millis.load(Ordering::Acquire);

        (last != 0).then(|| {
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_or(0, |elapsed| elapsed.as_millis() as u64);

            now.saturating_sub(last) / 1000
        })
    }
}

pub struct ReplicaLink {
    pub primary: SocketAddr,
    pub primary_name: Option<(String, u16)>,
    pub shard_id: u32,
    pub wal: Arc<Mutex<WalSink>>,
    pub inbound: SyncSender<ReplicaDelivery>,
    pub password: Vec<u8>,
    pub limits: ProtocolLimits,
    pub snapshot_max_bytes: u64,
    /// Sequence of the first rejected frame, or zero when ordinary streaming is safe.
    pub resync_sequence: Arc<AtomicU64>,
    pub data_dir: PathBuf,
    pub shard_count: u32,
    pub status: Arc<LinkStatus>,
    /// Woken after every delivery so the shard applies frames as they arrive instead of
    /// on its next timer tick.
    pub wake_shard: Arc<tokio::sync::Notify>,
}

pub fn spawn_replica_reader(link: ReplicaLink) -> ReplicationThread {
    let stop = Arc::new(AtomicBool::new(false));
    let wake = Arc::new(Mutex::new(None));
    let thread_stop = Arc::clone(&stop);
    let thread_wake = Arc::clone(&wake);
    let join = thread::Builder::new()
        .name(format!("ruvio-replica-{}", link.shard_id))
        .spawn(move || {
            let mut outstanding = VecDeque::new();
            let mut delay = RETRY_START;

            while !thread_stop.load(Ordering::Acquire) {
                let (retry, steady) =
                    follow_primary(&link, &mut outstanding, &thread_stop, &thread_wake);

                if !retry {
                    break;
                }

                sleep_unless_stopped(&thread_stop, delay);

                if steady {
                    delay = RETRY_START;
                } else {
                    delay = delay.saturating_mul(2).min(RETRY_BACKOFF_MAX);
                }
            }

            warn!(
                shard_id = link.shard_id,
                "replication replica reader stopping"
            );
        })
        .expect("replica reader thread");

    ReplicationThread {
        stop,
        wake,
        join: Some(join),
    }
}

fn sleep_unless_stopped(stop: &AtomicBool, total: Duration) {
    let slice = Duration::from_millis(50);
    let mut slept = Duration::ZERO;

    while slept < total {
        if stop.load(Ordering::Acquire) {
            return;
        }

        let pause = slice.min(total - slept);

        thread::sleep(pause);
        slept += pause;
    }
}

fn accept_replicas(listener: TcpListener, shipment: Arc<Shipment>, stop: Arc<AtomicBool>) {
    // An idle listener waits in the kernel for a replica instead of waking every poll.
    let accept = || match listener.accept() {
        Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
            crate::runtime::wait_for_connection(&listener, REPLICA_ACCEPT_WAIT);
            listener.accept()
        }
        accepted => accepted,
    };

    accept_replicas_from(accept, shipment, stop);
}

/// How long an idle replica listener waits for a connection between looks at stop.
const REPLICA_ACCEPT_WAIT: Duration = Duration::from_millis(100);

fn accept_replicas_from(
    mut accept: impl FnMut() -> io::Result<(TcpStream, SocketAddr)>,
    shipment: Arc<Shipment>,
    stop: Arc<AtomicBool>,
) {
    let progress = Arc::new(ReplicaProgress::new(
        Arc::clone(&shipment.backlog_floor),
        Arc::clone(&shipment.replica_count),
        Arc::clone(&shipment.acks),
    ));
    let mut connections = Vec::new();
    let mut accept_warnings = crate::admission::WarnLimiter::new(Duration::from_secs(1));

    loop {
        if stop.load(Ordering::Acquire) {
            break;
        }

        match accept() {
            Ok((stream, _)) => {
                connections.retain(|handle: &JoinHandle<()>| !handle.is_finished());

                if connections.len() >= shipment.max_replicas {
                    warn!(
                        shard_id = shipment.shard_id,
                        limit = shipment.max_replicas,
                        "replication replica limit reached"
                    );
                    let _ = stream.shutdown(Shutdown::Both);

                    continue;
                }

                let shipment = Arc::clone(&shipment);
                let progress = Arc::clone(&progress);
                let connection_stop = Arc::clone(&stop);
                let handle =
                    thread::Builder::new()
                        .name("ruvio-ship-conn".into())
                        .spawn(move || {
                            let _ = stream.set_nonblocking(false);
                            let _ = serve_replica(stream, &shipment, &progress, &connection_stop);
                        });

                if let Ok(handle) = handle {
                    connections.push(handle);
                }
            }

            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                thread::sleep(SHIPPER_POLL);
            }

            Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
            // Out of descriptors or memory, or an aborted connection: the listener is still
            // fine, so wait and accept again instead of shutting replication down for good.
            Err(error) => {
                if accept_warnings.allow(std::time::Instant::now()) {
                    warn!(
                        shard_id = shipment.shard_id,
                        %error,
                        suppressed = accept_warnings.take_suppressed(),
                        "replication accept failed; retrying"
                    );
                }

                thread::sleep(ACCEPT_ERROR_PAUSE);
            }
        }

        connections.retain(|handle: &JoinHandle<()>| !handle.is_finished());
    }

    for handle in connections {
        let _ = handle.join();
    }
}

fn serve_replica(
    mut stream: TcpStream,
    shipment: &Shipment,
    progress: &Arc<ReplicaProgress>,
    stop: &AtomicBool,
) -> io::Result<()> {
    if stop.load(Ordering::Acquire) {
        return Ok(());
    }

    let _ = stream.set_nodelay(true);
    let _ = stream.set_read_timeout(Some(Duration::from_secs(5)));
    let _ = stream.set_write_timeout(Some(SHIPPER_POLL));
    let handshake = match read_handshake(&mut stream) {
        Ok(Incoming::Handshake(handshake)) => handshake,
        Ok(Incoming::Unsupported(version)) => {
            warn!(
                shard_id = shipment.shard_id,
                replica_version = version,
                primary_versions = ?SUPPORTED_VERSIONS,
                "replica refused: {VERSION_MISMATCH}"
            );
            let reply = if LEGACY_VERSIONS.contains(&version) {
                encode_legacy_reply(HandshakeStatus::Version)
            } else {
                encode_reply(HandshakeStatus::Version, 0)
            };

            stream.write_all(&reply)?;

            return stream.flush();
        }

        Err(error) => {
            if error.kind() == io::ErrorKind::InvalidData {
                warn!(
                    shard_id = shipment.shard_id,
                    %error,
                    "malformed replication handshake; {VERSION_MISMATCH}"
                );
            }

            return Ok(());
        }
    };

    if handshake.shard_id != shipment.shard_id {
        refuse_handshake(&mut stream, HandshakeStatus::Shard)?;

        return Ok(());
    }

    if !crate::security::constant_time_eq(&handshake.password, &shipment.requirepass) {
        refuse_handshake(&mut stream, HandshakeStatus::Auth)?;

        return Ok(());
    }

    if handshake.shard_count != shipment.shard_count {
        refuse_handshake(&mut stream, HandshakeStatus::Shard)?;

        return Ok(());
    }

    let forced_snapshot = handshake.version != VERSION_RESUME;
    let incomplete = handshake.version == VERSION_RESYNC_INCOMPLETE;

    // A replica that resumes from its own log must have followed this very history; a
    // wiped, restored or unrelated primary can reuse the same sequence numbers.
    if !forced_snapshot && handshake.after_sequence > 0 && handshake.history != shipment.history {
        refuse_handshake(&mut stream, HandshakeStatus::History)?;

        return Ok(());
    }

    let through = shipment.published.load(Ordering::Acquire);
    let hold = progress.hold(handshake.after_sequence);
    if forced_snapshot
        && (handshake.after_sequence > through
            || (!incomplete && handshake.after_sequence == through))
    {
        refuse_handshake(&mut stream, HandshakeStatus::Gap)?;
        return Ok(());
    }
    // Where the shipper stopped in the log, so each chunk continues there.
    let mut reader = FrameCursor::default();
    let read = if forced_snapshot {
        Ok(FrameRead::Gap)
    } else {
        read_frames_resuming(
            &mut reader,
            &shipment.data_dir,
            shipment.shard_id,
            handshake.after_sequence,
            through,
            shipment.limits,
            SHIPPER_READ_BYTES,
        )
    };

    let frames = match read {
        Ok(FrameRead::Frames(frames)) => {
            if frames.is_empty()
                && handshake.after_sequence == through
                && !caught_up_cursor_is_retained(shipment, handshake.after_sequence, through)
            {
                refuse_handshake(&mut stream, HandshakeStatus::Gap)?;

                return Ok(());
            }

            frames
        }

        Ok(FrameRead::Gap) => {
            let Some(resync) = crate::persistence::load_resync_bytes(
                &shipment.data_dir,
                shipment.limits,
                shipment.snapshot_max_bytes,
                shipment.shard_id,
                shipment.shard_count,
                handshake.after_sequence + u64::from(forced_snapshot && !incomplete),
                through,
            )
            .unwrap_or(None) else {
                if forced_snapshot {
                    shipment.snapshot_requests.fetch_add(1, Ordering::AcqRel);
                }
                refuse_handshake(&mut stream, HandshakeStatus::Gap)?;

                return Ok(());
            };
            let last_sequence = resync.last_sequence;
            hold.advance(last_sequence);
            let copied_publishes = shipment.fanout.register();

            if !write_live(
                &mut stream,
                &encode_reply_with_history(HandshakeStatus::Snapshot, through, &shipment.history),
                stop,
            )? {
                return Ok(());
            }

            if !write_snapshot_blob(&mut stream, &resync.shard, stop)? {
                return Ok(());
            }

            if shipment.shard_id == 0 {
                let hot = resync.hot.as_deref().unwrap_or(&[]);

                if !write_snapshot_blob(&mut stream, hot, stop)? {
                    return Ok(());
                }
            }

            drop(resync);

            let Some(last_sent) = stream_frames_through(
                &mut stream,
                shipment,
                &hold,
                stop,
                &mut reader,
                last_sequence,
                through,
            )?
            else {
                return Ok(());
            };

            return stream_new_frames(
                &mut stream,
                shipment,
                &hold,
                &copied_publishes,
                stop,
                &mut reader,
                last_sent,
            );
        }

        Ok(FrameRead::Ahead) => {
            refuse_handshake(&mut stream, HandshakeStatus::Ahead)?;

            return Ok(());
        }

        Ok(FrameRead::Local) | Err(_) => {
            refuse_handshake(&mut stream, HandshakeStatus::Local)?;

            return Ok(());
        }
    };

    let next_sequence = handshake.after_sequence.saturating_add(1);
    let copied_publishes = shipment.fanout.register();

    if !write_live(
        &mut stream,
        &encode_reply_with_history(HandshakeStatus::Ok, next_sequence, &shipment.history),
        stop,
    )? {
        return Ok(());
    }

    let mut last_sent = handshake.after_sequence;

    for frame in &frames {
        if !write_live(&mut stream, frame, stop)? {
            return Ok(());
        }

        last_sent = frame_sequence(frame);
        hold.advance(last_sent);
    }

    let _ = stream.flush();

    stream_new_frames(
        &mut stream,
        shipment,
        &hold,
        &copied_publishes,
        stop,
        &mut reader,
        last_sent,
    )
}

fn caught_up_cursor_is_retained(shipment: &Shipment, after_sequence: u64, through: u64) -> bool {
    if after_sequence == 0 {
        return true;
    }

    if let Ok(FrameRead::Frames(_)) = read_frames(
        &shipment.data_dir,
        shipment.shard_id,
        after_sequence - 1,
        after_sequence,
        shipment.limits,
    ) {
        return true;
    }

    matches!(
        crate::persistence::load_resync_bytes(
            &shipment.data_dir,
            shipment.limits,
            shipment.snapshot_max_bytes,
            shipment.shard_id,
            shipment.shard_count,
            after_sequence,
            through,
        ),
        Ok(Some(resync)) if resync.last_sequence == after_sequence
    )
}

/// Reads the replica's acknowledgements. False once the replica has hung up or the
/// connection failed: an idle primary writes nothing that would find out, so without this
/// the connection would hold one of the replica slots for good.
fn poll_acks(stream: &mut TcpStream, hold: &CursorHold, pending: &mut Vec<u8>) -> bool {
    let _ = stream.set_read_timeout(Some(Duration::from_millis(1)));
    let mut scratch = [0u8; 64];

    loop {
        match stream.read(&mut scratch) {
            Ok(0) => return false,
            Ok(read) => pending.extend_from_slice(&scratch[..read]),
            Err(error)
                if error.kind() == io::ErrorKind::WouldBlock
                    || error.kind() == io::ErrorKind::TimedOut =>
            {
                break;
            }
            Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
            Err(_) => return false,
        }
    }

    while pending.len() >= ACK_LEN {
        let Some(sequence) = decode_ack(&pending[..ACK_LEN]) else {
            pending.clear();

            return true;
        };

        pending.drain(..ACK_LEN);
        hold.ack(sequence);
    }

    true
}

struct AckSend {
    last: u64,
    offset: usize,
    bytes: [u8; ACK_LEN],
    queued: bool,
}

impl AckSend {
    fn poll(&mut self, stream: &mut TcpStream, wal: &Mutex<WalSink>) {
        if !self.queued {
            let durable = wal
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .durable()
                .load(Ordering::Acquire);

            if durable == self.last {
                return;
            }

            self.bytes = encode_ack(durable);
            self.offset = 0;
            self.queued = true;
        }

        match stream.write(&self.bytes[self.offset..]) {
            Ok(0) | Err(_) => {}

            Ok(written) => {
                self.offset += written;

                if self.offset == self.bytes.len() {
                    self.last = decode_ack(&self.bytes).unwrap_or(self.last);
                    self.queued = false;
                }
            }
        }
    }
}

fn stream_frames_through(
    stream: &mut TcpStream,
    shipment: &Shipment,
    hold: &CursorHold,
    stop: &AtomicBool,
    reader: &mut FrameCursor,
    mut cursor: u64,
    through: u64,
) -> io::Result<Option<u64>> {
    while cursor < through {
        let frames = match read_frames_resuming(
            reader,
            &shipment.data_dir,
            shipment.shard_id,
            cursor,
            through,
            shipment.limits,
            SHIPPER_READ_BYTES,
        ) {
            Ok(FrameRead::Frames(frames)) if !frames.is_empty() => frames,
            _ => return Ok(None),
        };
        for frame in frames {
            if !write_live(stream, &frame, stop)? {
                return Ok(None);
            }
            cursor = frame_sequence(&frame);
            hold.advance(cursor);
        }
    }
    Ok(Some(cursor))
}

fn stream_new_frames(
    stream: &mut TcpStream,
    shipment: &Shipment,
    hold: &CursorHold,
    copied_publishes: &Receiver<Vec<u8>>,
    stop: &AtomicBool,
    reader: &mut FrameCursor,
    mut last_sent: u64,
) -> io::Result<()> {
    let mut ack_bytes = Vec::new();

    loop {
        if stop.load(Ordering::Acquire) {
            return Ok(());
        }

        if !poll_acks(stream, hold, &mut ack_bytes) {
            return Ok(());
        }

        while let Ok(frame) = copied_publishes.try_recv() {
            if !write_live(stream, &frame, stop)? {
                return Ok(());
            }
        }

        let through = shipment.published.load(Ordering::Acquire);

        if through <= last_sent {
            thread::sleep(SHIPPER_POLL);

            continue;
        }

        let Some(sent) =
            stream_frames_through(stream, shipment, hold, stop, reader, last_sent, through)?
        else {
            return Ok(());
        };
        last_sent = sent;
        let _ = stream.flush();
    }
}

pub(crate) fn follow_primary(
    link: &ReplicaLink,
    outstanding: &mut VecDeque<(u64, usize)>,
    stop: &AtomicBool,
    wake: &Mutex<Option<TcpStream>>,
) -> (bool, bool) {
    if stop.load(Ordering::Acquire) {
        return (false, false);
    }

    let address = match dial_address(link) {
        Ok(address) => address,
        Err(error) => {
            warn!(%error, "replication dial failed");

            return (!stop.load(Ordering::Acquire), false);
        }
    };

    let mut stream = match TcpStream::connect_timeout(&address, Duration::from_millis(200)) {
        Ok(stream) => stream,
        Err(error) => {
            warn!(%error, primary = %address, "replication dial failed");

            return (!stop.load(Ordering::Acquire), false);
        }
    };

    let _ = stream.set_nodelay(true);
    let _ = stream.set_write_timeout(Some(Duration::from_secs(5)));
    let _ = stream.set_read_timeout(Some(Duration::from_secs(5)));

    if let Ok(cloned) = stream.try_clone() {
        *wake.lock().unwrap_or_else(|error| error.into_inner()) = Some(cloned);
    }

    let (retry, steady) = read_primary(&mut stream, link, outstanding, stop);

    link.status.mark_down();
    *wake.lock().unwrap_or_else(|error| error.into_inner()) = None;

    if stop.load(Ordering::Acquire) {
        return (false, steady);
    }

    (retry, steady)
}

fn dial_address(link: &ReplicaLink) -> io::Result<SocketAddr> {
    let Some((host, port)) = &link.primary_name else {
        return Ok(link.primary);
    };

    let port = port.wrapping_add(link.shard_id as u16);
    let mut addresses = (host.as_str(), port).to_socket_addrs()?;

    addresses.next().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "replication primary did not resolve",
        )
    })
}

struct CatchupReconnect<'a> {
    link: &'a ReplicaLink,
    through: Option<u64>,
}

impl Drop for CatchupReconnect<'_> {
    fn drop(&mut self) {
        if self.through.is_some() && self.link.resync_sequence.load(Ordering::Acquire) == 0 {
            let next = self
                .link
                .wal
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .last_sequence()
                .saturating_add(1);
            self.link.resync_sequence.store(next, Ordering::Release);
        }
    }
}

fn read_primary(
    stream: &mut TcpStream,
    link: &ReplicaLink,
    outstanding: &mut VecDeque<(u64, usize)>,
    stop: &AtomicBool,
) -> (bool, bool) {
    let requested = link.resync_sequence.load(Ordering::Acquire);
    let after_sequence = if requested == u64::MAX {
        0 // A persisted incomplete image has no trustworthy resume cursor.
    } else {
        let wal = link.wal.lock().unwrap_or_else(|error| error.into_inner());
        wal.published().load(Ordering::Acquire)
    };
    if requested != 0 {
        outstanding.clear();
    }

    let handshake = Handshake {
        version: if requested == 0 {
            VERSION_RESUME
        } else if requested == u64::MAX {
            VERSION_RESYNC_INCOMPLETE
        } else {
            VERSION_RESYNC
        },
        shard_id: link.shard_id,
        after_sequence,
        password: link.password.clone(),
        history: upstream_history(&link.data_dir, link.shard_id),
        shard_count: link.shard_count,
    };

    let Ok(encoded) = encode_handshake(&handshake) else {
        warn!("replication handshake password exceeds 65535 bytes");

        return (true, false);
    };

    if stream.write_all(&encoded).is_err() {
        return (true, false);
    }

    let (status, snapshot_through, primary_history) = match read_reply(stream) {
        Ok(reply) => reply,
        Err(error) => {
            // A primary of another build cannot parse this handshake and closes the link (or
            // answers in a format this build cannot read); say so instead of retrying silently.
            warn!(
                shard_id = link.shard_id,
                %error,
                "no valid reply to the replication handshake; {VERSION_MISMATCH}, and the \
                 replication password and address must match"
            );

            return (true, false);
        }
    };

    if status == HandshakeStatus::Version {
        warn!(
            shard_id = link.shard_id,
            replica_version = handshake.version,
            "primary refused this replica: {VERSION_MISMATCH}"
        );

        return (true, false);
    }

    if status == HandshakeStatus::History {
        // This replica followed another history: its log cannot be continued, so take a
        // full copy of the primary instead.
        warn!("primary has a different write history; requesting a full resync");
        link.resync_sequence.store(u64::MAX, Ordering::Release);

        return (true, false);
    }

    // A resume continues the log the primary just vouched for. A snapshot's history is
    // recorded only after it is installed: a crash while it downloads must not leave the old
    // log claiming the new history.
    if status == HandshakeStatus::Ok
        && let Err(error) = record_upstream_history(&link.data_dir, link.shard_id, &primary_history)
    {
        warn!(%error, "could not record the primary's write history");

        return (true, false);
    }
    let mut catchup = CatchupReconnect {
        link,
        through: None,
    };

    if requested != 0 && status != HandshakeStatus::Snapshot {
        return (true, false);
    }
    if status == HandshakeStatus::Snapshot {
        let shard = match read_snapshot_blob(stream, link.snapshot_max_bytes) {
            Ok(bytes) => bytes,
            Err(_) => return (true, false),
        };

        let hot = if link.shard_id == 0 {
            let bytes = match read_snapshot_blob(stream, link.snapshot_max_bytes) {
                Ok(bytes) => bytes,
                Err(_) => return (true, false),
            };

            (!bytes.is_empty()).then_some(bytes)
        } else {
            None
        };

        let snapshot_sequence = match crate::persistence::decode_snapshot_blob(
            &shard,
            link.limits,
            link.snapshot_max_bytes,
        ) {
            Ok(checkpoint) if checkpoint.last_sequence <= snapshot_through => {
                checkpoint.last_sequence
            }
            _ => return (true, false),
        };
        if link
            .inbound
            .send(ReplicaDelivery::Snapshot {
                shard,
                hot,
                catchup_through: snapshot_through,
                history: primary_history,
            })
            .is_err()
        {
            return (true, false);
        }
        link.wake_shard.notify_one();
        if snapshot_sequence == snapshot_through {
            if link
                .inbound
                .send(ReplicaDelivery::CaughtUp(snapshot_through))
                .is_err()
            {
                return (true, false);
            }
            link.wake_shard.notify_one();
        } else {
            catchup.through = Some(snapshot_through);
        }
        if requested != 0 {
            let deadline = std::time::Instant::now() + Duration::from_secs(30);
            while link.resync_sequence.load(Ordering::Acquire) != 0 {
                if stop.load(Ordering::Acquire) {
                    return (false, false);
                }
                if std::time::Instant::now() >= deadline {
                    return (true, false);
                }
                thread::sleep(SHIPPER_POLL);
            }
        }
    } else if status != HandshakeStatus::Ok {
        warn!(?status, "replication handshake rejected");

        return (true, false);
    }

    // Blocking with a short timeout: a frame is read the moment it arrives, and the
    // timeout still lets the loop send acks and notice stop or a resync request.
    let _ = stream.set_nonblocking(false);
    let _ = stream.set_read_timeout(Some(SHIPPER_POLL));
    let _ = stream.set_write_timeout(Some(SHIPPER_POLL));
    let mut acks = AckSend {
        last: 0,
        offset: 0,
        bytes: [0; ACK_LEN],
        queued: false,
    };

    acks.poll(stream, &link.wal);
    link.status.mark_up();
    // Frames carry records written under the primary's limits, not this replica's; but
    // none larger than this replica's own log segment can be stored, so a declared length
    // past it is refused before it is buffered.
    let segment_bytes = link
        .wal
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .segment_bytes();
    let mut reader = FrameReader {
        pending: Vec::new(),
        max_frame_bytes: usize::try_from(segment_bytes)
            .unwrap_or(usize::MAX)
            .min(crate::persistence::WAL_RECORD_MAX_BYTES),
    };

    loop {
        if !wait_for_queue_room(link, outstanding, stop) {
            return (!stop.load(Ordering::Acquire), true);
        }

        let frame =
            match reader.next_frame(stream, stop, &link.resync_sequence, &mut acks, &link.wal) {
                Ok(Some(frame)) => frame,
                Ok(None) => return (false, true),
                Err(error) => {
                    if error.kind() == io::ErrorKind::InvalidData {
                        warn!(%error, "replication frame rejected");
                    }

                    return (!stop.load(Ordering::Acquire), true);
                }
            };

        link.status.touch();

        let sequence = if decode_copied_publish(&frame).is_some() {
            None
        } else {
            Some(frame_sequence(&frame))
        };
        if !enqueue_frame(link, outstanding, frame, stop) {
            return (!stop.load(Ordering::Acquire), true);
        }
        if let (Some(sequence), Some(through)) = (sequence, catchup.through)
            && sequence >= through
        {
            if link
                .inbound
                .send(ReplicaDelivery::CaughtUp(sequence))
                .is_err()
            {
                return (true, false);
            }
            link.wake_shard.notify_one();
            catchup.through = None;
        }

        acks.poll(stream, &link.wal);
    }
}

struct FrameReader {
    pending: Vec<u8>,
    max_frame_bytes: usize,
}

impl FrameReader {
    fn next_frame(
        &mut self,
        stream: &mut TcpStream,
        stop: &AtomicBool,
        resync_sequence: &AtomicU64,
        acks: &mut AckSend,
        wal: &Mutex<WalSink>,
    ) -> io::Result<Option<Vec<u8>>> {
        let mut scratch = [0; 8192];

        loop {
            if resync_sequence.load(Ordering::Acquire) != 0 {
                return Err(io::Error::new(
                    io::ErrorKind::Interrupted,
                    "snapshot resync requested",
                ));
            }
            if let Some(frame) = self.take_frame()? {
                return Ok(Some(frame));
            }

            if stop.load(Ordering::Acquire) {
                return Ok(None);
            }

            match stream.read(&mut scratch) {
                Ok(0) => {
                    return Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "replication closed",
                    ));
                }

                Ok(read) => self.pending.extend_from_slice(&scratch[..read]),
                Err(error)
                    if error.kind() == io::ErrorKind::WouldBlock
                        || error.kind() == io::ErrorKind::TimedOut =>
                {
                    acks.poll(stream, wal);
                }

                Err(error) => return Err(error),
            }
        }
    }

    fn take_frame(&mut self) -> io::Result<Option<Vec<u8>>> {
        if self.pending.len() < 4 {
            return Ok(None);
        }

        let length = u32::from_le_bytes(self.pending[..4].try_into().unwrap()) as usize;

        if !(32..=self.max_frame_bytes).contains(&length) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "wal frame length",
            ));
        }

        if self.pending.len() < length {
            return Ok(None);
        }

        Ok(Some(self.pending.drain(..length).collect()))
    }
}

fn wait_for_queue_room(
    link: &ReplicaLink,
    outstanding: &mut VecDeque<(u64, usize)>,
    stop: &AtomicBool,
) -> bool {
    loop {
        if stop.load(Ordering::Acquire) || link.resync_sequence.load(Ordering::Acquire) != 0 {
            return false;
        }

        let queued_bytes = reclaim_applied(link, outstanding);

        if outstanding.len() < QUEUE_FRAMES
            && (outstanding.is_empty() || queued_bytes < QUEUE_BYTES)
        {
            return true;
        }

        // The shard unparks this thread after it drains; the timeout covers stop.
        link.wake_shard.notify_one();
        thread::park_timeout(SHIPPER_POLL);
    }
}

fn reclaim_applied(link: &ReplicaLink, outstanding: &mut VecDeque<(u64, usize)>) -> usize {
    let applied = {
        let wal = link.wal.lock().unwrap_or_else(|error| error.into_inner());

        wal.last_sequence()
    };

    while outstanding
        .front()
        .is_some_and(|(sequence, _)| *sequence <= applied)
    {
        outstanding.pop_front();
    }

    outstanding.iter().map(|(_, len)| *len).sum()
}

fn enqueue_frame(
    link: &ReplicaLink,
    outstanding: &mut VecDeque<(u64, usize)>,
    mut frame: Vec<u8>,
    stop: &AtomicBool,
) -> bool {
    let sequence = frame_sequence(&frame);
    let len = frame.len();

    loop {
        if stop.load(Ordering::Acquire) || link.resync_sequence.load(Ordering::Acquire) != 0 {
            return false;
        }

        match link.inbound.try_send(ReplicaDelivery::Frame(frame)) {
            Ok(()) => {
                outstanding.push_back((sequence, len));
                link.wake_shard.notify_one();

                return true;
            }

            Err(TrySendError::Full(ReplicaDelivery::Frame(returned))) => {
                frame = returned;
                // The shard unparks this thread after it drains; the timeout covers stop.
                link.wake_shard.notify_one();
                thread::park_timeout(SHIPPER_POLL);
            }

            Err(
                TrySendError::Full(ReplicaDelivery::Snapshot { .. } | ReplicaDelivery::CaughtUp(_))
                | TrySendError::Disconnected(_),
            ) => return false,
        }
    }
}

fn frame_sequence(frame: &[u8]) -> u64 {
    if frame.len() < 24 {
        return u64::MAX;
    }

    u64::from_le_bytes(frame[16..24].try_into().unwrap())
}

enum Incoming {
    Handshake(Handshake),
    /// A handshake of another build, by its version.
    Unsupported(u16),
}

fn read_handshake(stream: &mut TcpStream) -> io::Result<Incoming> {
    let mut prefix = [0; HANDSHAKE_PREFIX];

    stream.read_exact(&mut prefix)?;

    if &prefix[..HANDSHAKE_MAGIC.len()] != HANDSHAKE_MAGIC {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "bad handshake magic",
        ));
    }

    let password_len = usize::from(u16::from_le_bytes([prefix[22], prefix[23]]));
    let version = read_u16(&prefix, 8);

    // Decided before the rest is read: other builds send a different tail, and waiting for
    // bytes that never come would stall both sides until the read timeout.
    if !SUPPORTED_VERSIONS.contains(&version) {
        if LEGACY_VERSIONS.contains(&version) {
            // Their handshake ends with the password and checksum; consume it so the peer
            // reads the refusal instead of a reset.
            let mut rest = vec![0; password_len + CHECKSUM_LEN];

            let _ = stream.read_exact(&mut rest);
        }

        return Ok(Incoming::Unsupported(version));
    }
    let mut rest = vec![0; password_len + HANDSHAKE_TAIL + CHECKSUM_LEN];

    stream.read_exact(&mut rest)?;
    let mut bytes = prefix.to_vec();

    bytes.extend_from_slice(&rest);
    decode_handshake(&bytes)
        .map(Incoming::Handshake)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "bad handshake"))
}

/// A fake primary's side of the handshake, for tests outside this module.
#[cfg(test)]
pub(crate) fn read_current_handshake(stream: &mut TcpStream) -> Handshake {
    match read_handshake(stream) {
        Ok(Incoming::Handshake(handshake)) => handshake,
        _ => panic!("replica sent no current handshake"),
    }
}

pub fn read_snapshot_blob(stream: &mut TcpStream, maximum: u64) -> io::Result<Vec<u8>> {
    let mut length_bytes = [0; 8];

    stream.read_exact(&mut length_bytes)?;
    let length = u64::from_le_bytes(length_bytes);

    if length > maximum {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "snapshot exceeds configured size limit",
        ));
    }

    let length = usize::try_from(length)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "snapshot length"))?;

    let mut bytes = vec![0; length];

    stream.read_exact(&mut bytes)?;
    Ok(bytes)
}

fn read_reply(stream: &mut TcpStream) -> io::Result<(HandshakeStatus, u64, HistoryId)> {
    let mut bytes = [0; REPLY_LEN];

    stream.read_exact(&mut bytes)?;
    decode_reply_with_history(&bytes)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "bad reply"))
}

fn refuse_handshake(stream: &mut TcpStream, status: HandshakeStatus) -> io::Result<()> {
    warn!(?status, "replication handshake rejected");
    write_status(stream, status, 0)
}

fn write_status(
    stream: &mut TcpStream,
    status: HandshakeStatus,
    next_sequence: u64,
) -> io::Result<()> {
    stream.write_all(&encode_reply(status, next_sequence))?;
    stream.flush()
}

fn write_snapshot_blob(
    stream: &mut TcpStream,
    bytes: &[u8],
    stop: &AtomicBool,
) -> io::Result<bool> {
    let mut payload = (bytes.len() as u64).to_le_bytes().to_vec();

    payload.extend_from_slice(bytes);
    write_live(stream, &payload, stop)
}

fn write_live(stream: &mut TcpStream, bytes: &[u8], stop: &AtomicBool) -> io::Result<bool> {
    let mut offset = 0;
    let mut last_progress = std::time::Instant::now();

    while offset < bytes.len() {
        if stop.load(Ordering::Acquire) {
            return Ok(false);
        }

        match stream.write(&bytes[offset..]) {
            Ok(0) => {
                return Err(io::Error::new(
                    io::ErrorKind::WriteZero,
                    "replication write closed",
                ));
            }

            Ok(written) => {
                offset += written;
                last_progress = std::time::Instant::now();
            }
            Err(error)
                if error.kind() == io::ErrorKind::TimedOut
                    || error.kind() == io::ErrorKind::WouldBlock =>
            {
                if last_progress.elapsed() >= Duration::from_secs(30) {
                    return Err(io::Error::new(
                        io::ErrorKind::TimedOut,
                        "replica stopped accepting data",
                    ));
                }
            }

            Err(error) => return Err(error),
        }
    }

    Ok(true)
}

#[test]
fn a_deadline_or_persist_for_a_key_the_replica_lacks_is_a_divergence() {
    let mut shard = KvShard::new();
    let now = shard.now_unix_millis().get();
    let past = OwnedMutation::ExpireAt {
        key: b"gone".to_vec(),
        unix_millis: now.saturating_sub(1),
    };
    let future = OwnedMutation::ExpireAt {
        key: b"gone".to_vec(),
        unix_millis: now + 60_000,
    };

    assert!(needs_snapshot(&mut shard, &past));
    assert!(needs_snapshot(&mut shard, &future));
    assert!(needs_snapshot(
        &mut shard,
        &OwnedMutation::Persist {
            key: b"gone".to_vec()
        }
    ));
}

#[test]
fn a_rename_or_move_of_a_key_the_replica_lacks_is_a_divergence() {
    let mut shard = KvShard::new();
    let rename = OwnedMutation::Rename {
        source: b"held".to_vec(),
        destination: b"renamed".to_vec(),
    };
    let moved = OwnedMutation::MoveKey {
        key: b"held".to_vec(),
        destination: 1,
    };

    assert!(needs_snapshot(&mut shard, &rename));
    assert!(needs_snapshot(&mut shard, &moved));

    shard.set_bytes(b"held", b"v");
    assert!(!needs_snapshot(&mut shard, &rename));
    assert!(!needs_snapshot(&mut shard, &moved));

    shard.enter(1);
    shard.set_bytes(b"held", b"other");
    shard.enter(0);
    assert!(needs_snapshot(&mut shard, &moved));
    assert_eq!(shard.active_database(), 0);
}

#[test]
fn a_transaction_setting_an_already_expired_value_then_its_deadline_is_not_a_divergence() {
    let mut shard = KvShard::new();
    let now = shard.now_unix_millis().get();
    // The primary's clock is behind this replica's: its deadline is already past here. The
    // replica keeps the key until the primary deletes it, so the next deadline has a key.
    let mutations = [
        OwnedMutation::SetBytesExpiring {
            key: b"k".to_vec(),
            value: b"v".to_vec(),
            expires_at: now.saturating_sub(5),
        },
        OwnedMutation::ExpireAt {
            key: b"k".to_vec(),
            unix_millis: now + 60_000,
        },
    ];

    assert!(!transaction_needs_snapshot(&mut shard, 0, &mutations));
}

#[test]
fn durable_acks_count_only_replicas_that_synced_the_target() {
    let acks = DurableAcks::new();

    acks.reserve(0);
    acks.reserve(1);
    assert_eq!(acks.reached(0), 2);
    assert_eq!(acks.reached(1), 0);
    acks.observe(0, 4);
    assert_eq!(acks.reached(4), 1);
    assert_eq!(acks.reached(5), 0);
    acks.observe(1, 4);
    assert_eq!(acks.reached(4), 2);
    acks.clear(0);
    assert_eq!(acks.reached(4), 1);
}

#[test]
fn connected_replica_cursors_publish_the_oldest_and_the_count() {
    let floor = Arc::new(AtomicU64::new(u64::MAX));
    let count = Arc::new(AtomicU64::new(0));
    let progress = Arc::new(ReplicaProgress::new(
        Arc::clone(&floor),
        Arc::clone(&count),
        Arc::new(DurableAcks::new()),
    ));
    let first = progress.hold(4);

    assert_eq!(floor.load(Ordering::Acquire), 4);
    assert_eq!(count.load(Ordering::Acquire), 1);

    let second = progress.hold(9);

    assert_eq!(floor.load(Ordering::Acquire), 4);
    assert_eq!(count.load(Ordering::Acquire), 2);
    first.advance(12);
    assert_eq!(floor.load(Ordering::Acquire), 9);
    drop(second);
    assert_eq!(floor.load(Ordering::Acquire), 12);
    assert_eq!(count.load(Ordering::Acquire), 1);
    drop(first);
    assert_eq!(floor.load(Ordering::Acquire), u64::MAX);
    assert_eq!(count.load(Ordering::Acquire), 0);
}

#[test]
fn encode_handshake_returns_an_error_when_the_password_does_not_fit() {
    let handshake = Handshake {
        version: VERSION_RESUME,
        shard_id: 0,
        after_sequence: 0,
        history: [0; HISTORY_LEN],
        shard_count: 1,
        password: vec![b'a'; 65_536],
    };

    assert!(encode_handshake(&handshake).is_err());
}

#[test]
fn handshake_round_trips_and_rejects_a_bad_checksum() {
    let handshake = Handshake {
        version: VERSION_RESUME,
        shard_id: 2,
        after_sequence: 40,
        history: [0; HISTORY_LEN],
        shard_count: 1,
        password: b"secret".to_vec(),
    };

    let bytes = encode_handshake(&handshake).unwrap();

    assert_eq!(&bytes[..8], b"RUVREP01");
    assert_eq!(decode_handshake(&bytes), Ok(handshake));
    let mut broken = bytes.clone();

    *broken.last_mut().unwrap() ^= 0xff;
    assert!(decode_handshake(&broken).is_err());
    let reply = encode_reply(HandshakeStatus::Gap, 0);

    assert_eq!(decode_reply(&reply), Ok((HandshakeStatus::Gap, 0)));
    assert_eq!(reply.len(), 38);
    let snapshot = encode_reply(HandshakeStatus::Snapshot, 41);

    assert_eq!(decode_reply(&snapshot), Ok((HandshakeStatus::Snapshot, 41)));
    assert_eq!(snapshot.len(), 38);
}

#[test]
fn decode_handshake_rejects_a_short_buffer_bad_magic_and_length_mismatch() {
    assert!(decode_handshake(&[]).is_err());
    assert!(decode_handshake(&[0; 47]).is_err());

    let empty = Handshake {
        version: VERSION_RESUME,
        shard_id: 0,
        after_sequence: 0,
        history: [0; HISTORY_LEN],
        shard_count: 1,
        password: Vec::new(),
    };

    let empty_bytes = encode_handshake(&empty).unwrap();

    assert_eq!(empty_bytes.len(), 48);
    assert_eq!(decode_handshake(&empty_bytes), Ok(empty));

    let handshake = Handshake {
        version: VERSION_RESUME,
        shard_id: 2,
        after_sequence: 40,
        history: [0; HISTORY_LEN],
        shard_count: 1,
        password: b"secret".to_vec(),
    };

    let mut bad_magic = encode_handshake(&handshake).unwrap();

    bad_magic[0] ^= 0xff;
    let checksum_at = bad_magic.len() - 4;
    let checksum = crc32c::crc32c(&bad_magic[..checksum_at]);

    bad_magic[checksum_at..].copy_from_slice(&checksum.to_le_bytes());
    assert!(decode_handshake(&bad_magic).is_err());

    let mut bad_length = encode_handshake(&handshake).unwrap();

    bad_length[22] = 5;
    let checksum_at = bad_length.len() - 4;
    let checksum = crc32c::crc32c(&bad_length[..checksum_at]);

    bad_length[checksum_at..].copy_from_slice(&checksum.to_le_bytes());
    assert!(decode_handshake(&bad_length).is_err());

    let mut long = encode_handshake(&handshake).unwrap();

    long.push(0);
    assert!(decode_handshake(&long).is_err());
}

#[test]
fn decode_reply_rejects_a_short_buffer_bad_magic_bad_checksum_and_unknown_status() {
    assert!(decode_reply(&[]).is_err());
    assert!(decode_reply(&[0; 21]).is_err());

    let mut bad_magic = encode_reply(HandshakeStatus::Ok, 9);

    bad_magic[0] ^= 0xff;
    let checksum = crc32c::crc32c(&bad_magic[..18]);

    bad_magic[18..22].copy_from_slice(&checksum.to_le_bytes());
    assert!(decode_reply(&bad_magic).is_err());

    let mut bad_checksum = encode_reply(HandshakeStatus::Ok, 9);

    *bad_checksum.last_mut().unwrap() ^= 0xff;
    assert!(decode_reply(&bad_checksum).is_err());

    let mut unknown = encode_reply(HandshakeStatus::Local, 3);

    unknown[8] = 9;
    unknown[9] = 0;
    let checksum = crc32c::crc32c(&unknown[..18]);

    unknown[18..22].copy_from_slice(&checksum.to_le_bytes());
    assert!(decode_reply(&unknown).is_err());

    let mut long = encode_reply(HandshakeStatus::Ok, 9);

    long.push(0);
    assert!(decode_reply(&long).is_err());
    assert_eq!(
        decode_reply(&encode_reply(HandshakeStatus::Ahead, 41)),
        Ok((HandshakeStatus::Ahead, 41))
    );
}

#[test]
fn handshake_status_codes_and_limits_match_the_contract() {
    assert_eq!(HANDSHAKE_MAGIC, b"RUVREP01");
    assert_eq!(
        READONLY_LINE,
        b"READONLY You can't write against a read only replica."
    );
    assert_eq!(QUEUE_FRAMES, 1024);
    assert_eq!(QUEUE_BYTES, 16 * 1024 * 1024);
    assert_eq!(RETRY_INTERVAL, Duration::from_secs(1));
    assert_eq!(SHIPPER_POLL, Duration::from_millis(10));
    assert_eq!(
        (VERSION_RESUME, VERSION_RESYNC, VERSION_RESYNC_INCOMPLETE),
        (6, 7, 8)
    );

    let handshake = Handshake {
        version: VERSION_RESUME,
        shard_id: 2,
        after_sequence: 40,
        history: [0; HISTORY_LEN],
        shard_count: 1,
        password: b"secret".to_vec(),
    };

    let bytes = encode_handshake(&handshake).unwrap();

    assert_eq!(&bytes[8..10], &6u16.to_le_bytes());
    assert_eq!(&bytes[10..14], &2u32.to_le_bytes());
    assert_eq!(&bytes[14..22], &40u64.to_le_bytes());
    assert_eq!(&bytes[22..24], &6u16.to_le_bytes());
    assert_eq!(&bytes[24..30], b"secret");
    assert_eq!(&bytes[30..46], &[0; HISTORY_LEN]);
    assert_eq!(&bytes[46..50], &1u32.to_le_bytes());
    assert_eq!(
        u32::from_le_bytes(bytes[50..54].try_into().unwrap()),
        crc32c::crc32c(&bytes[..50])
    );

    let reply = encode_reply(HandshakeStatus::Gap, 0);

    assert_eq!(&reply[8..10], &2u16.to_le_bytes());
    assert_eq!(&reply[10..18], &0u64.to_le_bytes());
    assert_eq!(
        u32::from_le_bytes(reply[34..38].try_into().unwrap()),
        crc32c::crc32c(&reply[..34])
    );

    let statuses = [
        (HandshakeStatus::Ok, 1),
        (HandshakeStatus::Gap, 2),
        (HandshakeStatus::Auth, 3),
        (HandshakeStatus::Shard, 4),
        (HandshakeStatus::Version, 5),
        (HandshakeStatus::Ahead, 6),
        (HandshakeStatus::Local, 7),
    ];

    for (status, code) in statuses {
        let reply = encode_reply(status, 11);

        assert_eq!(u16::from_le_bytes(reply[8..10].try_into().unwrap()), code);
        assert_eq!(decode_reply(&reply), Ok((status, 11)));
    }
}

#[test]
fn mixed_expired_delta_and_compacted_history_never_enter_the_replica_wal() {
    use crate::{
        mutation::{Mutation, MutationSink},
        persistence::{DurabilityProfile, ShardCheckpoint},
    };

    let limits = ProtocolLimits::default();
    let source_dir = tempfile::tempdir().unwrap();
    let replica_dir = tempfile::tempdir().unwrap();
    let config = |path: &std::path::Path| PersistenceConfig {
        profile: DurabilityProfile::Everysec,
        data_dir: path.to_path_buf(),
        ..PersistenceConfig::default()
    };
    let (mut primary, _, _) = WalSink::open(config(source_dir.path()), 0, limits).unwrap();
    primary
        .record(Mutation::Transaction {
            mutations: &[
                OwnedMutation::JsonSetPath {
                    key: b"doc".to_vec(),
                    path: b"$.count".to_vec(),
                    value: b"2".to_vec(),
                },
                OwnedMutation::SetBytes {
                    key: b"other".to_vec(),
                    value: b"written".to_vec(),
                },
            ],
        })
        .unwrap();
    primary
        .record(Mutation::JsonSetPath {
            key: b"doc",
            path: b"$.count",
            value: b"3",
        })
        .unwrap();
    let FrameRead::Frames(frames) = read_frames(source_dir.path(), 0, 0, 2, limits).unwrap() else {
        panic!("primary WAL frames unavailable");
    };
    assert!(matches!(
        read_bounded_history(source_dir.path(), 0, 0, 2, limits, 1, u64::MAX),
        Ok(FrameRead::Local),
    ));
    assert!(matches!(
        read_bounded_history(source_dir.path(), 0, 0, 2, limits, 2, 1),
        Ok(FrameRead::Local),
    ));

    let mut original = KvShard::new();
    original
        .put_json(b"doc", br#"{"count":1}"#.as_slice())
        .unwrap();
    let mut entries = original.snapshot_entries();
    entries[0].expires_at = Some(crate::UnixMillis::new(1));
    let replica_config = config(replica_dir.path());
    let manager = CheckpointManager::new(replica_config.clone(), limits);
    manager
        .publish(
            1,
            &[ShardCheckpoint {
                shard_id: 0,
                last_sequence: 0,
                entries: entries.clone(),
            }],
        )
        .unwrap();
    assert!(manager.load_bounded_history(1).is_err());
    let mut replica = KvShard::new();
    let paused = replica.pause_expiration_for_replay();
    for entry in entries {
        replica.restore_snapshot_entry(entry).unwrap();
    }
    replica.resume_expiration_after_replay(paused);
    assert_eq!(replica.expire_cycle(usize::MAX), 1);

    let (mut wal, _, _) = WalSink::open_after(replica_config.clone(), 0, limits, 0).unwrap();
    assert_eq!(
        accept_replica_frame(
            &mut replica,
            &mut wal,
            &manager,
            &replica_config,
            &frames[0],
            limits,
        ),
        Err(ApplyFailure::NeedsSnapshot),
    );
    assert_eq!(wal.last_sequence(), 0);
    assert_eq!(replica.value_kind(b"other"), None);

    manager
        .publish(
            2,
            &[ShardCheckpoint {
                shard_id: 0,
                last_sequence: 1,
                entries: vec![],
            }],
        )
        .unwrap();
    wal.install_base(1).unwrap();
    assert_eq!(
        accept_replica_frame(
            &mut replica,
            &mut wal,
            &manager,
            &replica_config,
            &frames[1],
            limits,
        ),
        Err(ApplyFailure::NeedsSnapshot),
    );
    assert_eq!(wal.last_sequence(), 1);
    drop(wal);
    let (_, mutations, _) = WalSink::open_after(replica_config, 0, limits, 1).unwrap();
    assert!(mutations.is_empty());
}

#[test]
fn transaction_preflight_uses_sequential_key_kinds_before_append() {
    use crate::{
        mutation::{Mutation, MutationSink},
        persistence::DurabilityProfile,
    };

    let run = |mutations: Vec<OwnedMutation>, valid: bool| {
        let source_dir = tempfile::tempdir().unwrap();
        let replica_dir = tempfile::tempdir().unwrap();
        let config = |path: &std::path::Path| PersistenceConfig {
            profile: DurabilityProfile::Everysec,
            data_dir: path.to_path_buf(),
            ..PersistenceConfig::default()
        };
        let limits = ProtocolLimits::default();
        let (mut primary, _, _) = WalSink::open(config(source_dir.path()), 0, limits).unwrap();
        primary
            .record(Mutation::Transaction {
                mutations: &mutations,
            })
            .unwrap();
        let FrameRead::Frames(frames) = read_frames(source_dir.path(), 0, 0, 1, limits).unwrap()
        else {
            panic!("transaction frame unavailable");
        };
        let replica_config = config(replica_dir.path());
        let manager = CheckpointManager::new(replica_config.clone(), limits);
        let (mut wal, _, _) = WalSink::open(replica_config.clone(), 0, limits).unwrap();
        let mut shard = KvShard::new();
        let result = accept_replica_frame(
            &mut shard,
            &mut wal,
            &manager,
            &replica_config,
            &frames[0],
            limits,
        );
        if valid {
            let (db, mutation, _) = result.unwrap();
            crate::persistence::apply_mutation(&mut shard, db, &mutation).unwrap();
            assert_eq!(wal.last_sequence(), 1);
        } else {
            assert_eq!(result, Err(ApplyFailure::NeedsSnapshot));
            assert_eq!(wal.last_sequence(), 0);
            drop(wal);
            let (_, replayed, _) = WalSink::open(replica_config, 0, limits).unwrap();
            assert!(replayed.is_empty());
        }
    };

    let large = format!(
        r#"{{"field":0,"padding":"{}"}}"#,
        "x".repeat(crate::json::TREE_THRESHOLD)
    );
    let put = || OwnedMutation::JsonPut {
        key: b"doc".to_vec(),
        value: large.as_bytes().to_vec(),
    };
    let path = |key: &[u8]| OwnedMutation::JsonSetPath {
        key: key.to_vec(),
        path: b"$.field".to_vec(),
        value: b"1".to_vec(),
    };
    let bytes = |key: &[u8]| OwnedMutation::SetBytes {
        key: key.to_vec(),
        value: b"value".to_vec(),
    };
    run(
        vec![
            put(),
            path(b"doc"),
            OwnedMutation::Persist {
                key: b"doc".to_vec(),
            },
        ],
        true,
    );
    run(vec![put(), put(), path(b"doc")], true);
    run(
        vec![
            bytes(b"other"),
            OwnedMutation::Persist {
                key: b"other".to_vec(),
            },
            OwnedMutation::ExpireAt {
                key: b"other".to_vec(),
                unix_millis: u64::MAX / 2,
            },
        ],
        true,
    );
    run(
        vec![
            put(),
            OwnedMutation::Rename {
                source: b"doc".to_vec(),
                destination: b"renamed".to_vec(),
            },
            path(b"renamed"),
        ],
        true,
    );
    run(
        vec![
            put(),
            OwnedMutation::Delete {
                keys: vec![b"doc".to_vec()],
            },
            path(b"doc"),
        ],
        false,
    );
    run(vec![put(), bytes(b"doc"), path(b"doc")], false);
    run(
        vec![put(), OwnedMutation::FlushDatabase, path(b"doc")],
        false,
    );
    run(
        vec![
            put(),
            OwnedMutation::SwapDatabases {
                first: 0,
                second: 1,
            },
            path(b"doc"),
        ],
        false,
    );
    run(
        vec![
            put(),
            OwnedMutation::SwapDatabases {
                first: 0,
                second: 1,
            },
            OwnedMutation::SwapDatabases {
                first: 0,
                second: 1,
            },
            path(b"doc"),
        ],
        true,
    );
    run(
        vec![
            OwnedMutation::SetStore {
                key: b"other".to_vec(),
                members: vec![],
            },
            OwnedMutation::Persist {
                key: b"other".to_vec(),
            },
        ],
        false,
    );
    run(
        vec![
            OwnedMutation::SetStore {
                key: b"other".to_vec(),
                members: vec![b"member".to_vec()],
            },
            OwnedMutation::Persist {
                key: b"other".to_vec(),
            },
        ],
        true,
    );
    run(
        vec![
            bytes(b"other"),
            OwnedMutation::Delete {
                keys: vec![b"other".to_vec()],
            },
            OwnedMutation::Persist {
                key: b"other".to_vec(),
            },
        ],
        false,
    );
    run(
        vec![
            OwnedMutation::LimitPut {
                key: b"other".to_vec(),
                used: 1,
                expires_at: None,
            },
            OwnedMutation::Persist {
                key: b"other".to_vec(),
            },
        ],
        true,
    );
    run(
        vec![
            OwnedMutation::SemaphoreAcquire {
                key: b"other".to_vec(),
                token: 1,
                expires_at: u64::MAX / 2,
            },
            OwnedMutation::SemaphoreRelease {
                key: b"other".to_vec(),
                token: 1,
            },
            OwnedMutation::Persist {
                key: b"other".to_vec(),
            },
        ],
        false,
    );
}

#[test]
fn expired_replica_snapshot_json_delta_requests_snapshot_without_appending() {
    use crate::{
        mutation::{Mutation, MutationSink},
        persistence::{DurabilityProfile, ShardCheckpoint},
    };

    let limits = ProtocolLimits::default();
    let source_dir = tempfile::tempdir().unwrap();
    let replica_dir = tempfile::tempdir().unwrap();
    let config = |path: &std::path::Path| PersistenceConfig {
        profile: DurabilityProfile::Everysec,
        data_dir: path.to_path_buf(),
        ..PersistenceConfig::default()
    };
    let (mut primary, _, _) = WalSink::open(config(source_dir.path()), 0, limits).unwrap();
    primary
        .record(Mutation::JsonSetPath {
            key: b"doc",
            path: b"$.count",
            value: b"2",
        })
        .unwrap();
    primary.record(Mutation::Persist { key: b"doc" }).unwrap();
    let FrameRead::Frames(frames) = read_frames(source_dir.path(), 0, 0, 2, limits).unwrap() else {
        panic!("primary WAL frames unavailable");
    };

    let mut original = KvShard::new();
    original.put_json(b"doc", br#"{"count":1}"#).unwrap();
    let mut entries = original.snapshot_entries();
    entries[0].expires_at = Some(crate::UnixMillis::new(1));
    let replica_config = config(replica_dir.path());
    let manager = CheckpointManager::new(replica_config.clone(), limits);
    manager
        .publish(
            1,
            &[ShardCheckpoint {
                shard_id: 0,
                last_sequence: 0,
                entries: entries.clone(),
            }],
        )
        .unwrap();
    let mut replica = KvShard::new();
    let paused = replica.pause_expiration_for_replay();
    for entry in entries {
        replica.restore_snapshot_entry(entry).unwrap();
    }
    replica.resume_expiration_after_replay(paused);
    assert_eq!(replica.expire_cycle(usize::MAX), 1);

    let (mut wal, _, _) = WalSink::open_after(replica_config.clone(), 0, limits, 0).unwrap();
    assert_eq!(
        accept_replica_frame(
            &mut replica,
            &mut wal,
            &manager,
            &replica_config,
            &frames[0],
            limits
        ),
        Err(ApplyFailure::NeedsSnapshot)
    );
    assert_eq!(wal.last_sequence(), 0);
    manager
        .publish(
            2,
            &[ShardCheckpoint {
                shard_id: 0,
                last_sequence: 1,
                entries: vec![],
            }],
        )
        .unwrap();
    wal.install_base(1).unwrap();
    assert_eq!(
        accept_replica_frame(
            &mut replica,
            &mut wal,
            &manager,
            &replica_config,
            &frames[1],
            limits
        ),
        Err(ApplyFailure::NeedsSnapshot)
    );
    assert_eq!(wal.last_sequence(), 1);
    drop(wal);
    let (_, mutations, _) = WalSink::open_after(replica_config, 0, limits, 1).unwrap();
    assert!(mutations.is_empty());
}

#[test]
fn apply_frame_appends_then_applies_and_stops_on_a_bad_frame() {
    use std::sync::atomic::Ordering;

    use crate::{
        KvShard,
        mutation::{Mutation, MutationSink},
        persistence::{DurabilityProfile, FrameRead, PersistenceConfig, WalSink, read_frames},
        protocol::ProtocolLimits,
    };

    let limits = ProtocolLimits::default();
    let primary_dir = tempfile::tempdir().unwrap();
    let primary_config = PersistenceConfig {
        profile: DurabilityProfile::Everysec,
        data_dir: primary_dir.path().to_path_buf(),
        wal_segment_bytes: 4096,
        sync_interval: Duration::from_millis(1),
        ..PersistenceConfig::default()
    };

    let (mut primary, _, _) = WalSink::open(primary_config, 0, limits).unwrap();

    primary
        .record(Mutation::SetBytes {
            key: b"k",
            value: b"v",
        })
        .unwrap();
    primary
        .record(Mutation::SetBytes {
            key: b"k",
            value: b"w",
        })
        .unwrap();

    let FrameRead::Frames(frames) = read_frames(primary_dir.path(), 0, 0, 1, limits).unwrap()
    else {
        panic!("primary did not return the first frame");
    };

    let frame = frames[0].clone();
    let FrameRead::Frames(next_frames) = read_frames(primary_dir.path(), 0, 1, 2, limits).unwrap()
    else {
        panic!("primary did not return the next frame");
    };

    let next = next_frames[0].clone();
    let replica_dir = tempfile::tempdir().unwrap();
    let replica_config = PersistenceConfig {
        profile: DurabilityProfile::Everysec,
        data_dir: replica_dir.path().to_path_buf(),
        wal_segment_bytes: 4096,
        sync_interval: Duration::from_millis(1),
        ..PersistenceConfig::default()
    };

    let (mut replica, _, _) = WalSink::open(replica_config, 0, limits).unwrap();
    let mut shard = KvShard::new();

    assert_eq!(
        apply_frame(&mut shard, &mut replica, &frame, limits),
        Ok(())
    );
    assert_eq!(shard.get(b"k"), Some(&b"v"[..]));
    assert_eq!(replica.published().load(Ordering::Acquire), 1);
    assert_eq!(
        apply_frame(&mut shard, &mut replica, &frame, limits),
        Err(ApplyFailure::Sequence)
    );
    assert_eq!(shard.get(b"k"), Some(&b"v"[..]));

    let mut broken = frame.clone();

    *broken.last_mut().unwrap() ^= 0xff;
    assert_eq!(
        apply_frame(&mut shard, &mut replica, &broken, limits),
        Err(ApplyFailure::BadFrame)
    );
    assert_eq!(replica.published().load(Ordering::Acquire), 1);
    crate::persistence::arm_io_fault(crate::persistence::CrashSite::Append);

    let persisted = apply_frame(&mut shard, &mut replica, &next, limits);

    assert!(
        matches!(persisted, Err(ApplyFailure::Persist(ref message)) if message.contains("no space")),
        "{persisted:?}"
    );
    assert_eq!(shard.get(b"k"), Some(&b"v"[..]));
}

#[test]
fn frame_reader_accepts_a_length_above_the_default_within_configured_max() {
    let configured = crate::protocol::DEFAULT_MAX_COMMAND_BYTES + 64;
    let length = crate::protocol::DEFAULT_MAX_COMMAND_BYTES + 1;
    let mut reader = FrameReader {
        pending: (length as u32).to_le_bytes().to_vec(),
        max_frame_bytes: configured,
    };

    let taken = reader
        .take_frame()
        .expect("length within configured max_command_bytes is a legal frame");

    assert!(taken.is_none());
    reader.pending = 31u32.to_le_bytes().to_vec();
    assert!(reader.take_frame().is_err());
}

#[test]
fn gap_handshake_leaves_the_live_generation() {
    use std::sync::atomic::Ordering;

    use crate::{
        mutation::{Mutation, MutationSink},
        persistence::{DurabilityProfile, PersistenceConfig, WalSink},
        protocol::ProtocolLimits,
    };

    let limits = ProtocolLimits::default();
    let directory = tempfile::tempdir().unwrap();
    let config = PersistenceConfig {
        profile: DurabilityProfile::Always,
        data_dir: directory.path().to_path_buf(),
        wal_segment_bytes: 4096,
        sync_interval: Duration::from_millis(1),
        ..PersistenceConfig::default()
    };

    let (mut sink, _, _) = WalSink::open(config, 0, limits).unwrap();
    let value = vec![b'v'; 2500];

    sink.record(Mutation::SetBytes {
        key: b"k",
        value: &value,
    })
    .unwrap();
    sink.record(Mutation::SetBytes {
        key: b"k",
        value: &value,
    })
    .unwrap();
    sink.flush().unwrap();

    let published = sink.published();

    assert_eq!(published.load(Ordering::Acquire), 2);
    drop(sink);

    let shard_dir = directory.path().join("wal").join("shard-0000");
    let mut segments = std::fs::read_dir(&shard_dir)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .filter(|path| path.extension().is_some_and(|extension| extension == "wal"))
        .collect::<Vec<_>>();

    segments.sort();
    assert_eq!(segments.len(), 2, "{segments:?}");

    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();

    drop(listener);

    let primary = spawn_primary(Shipment {
        listen: address,
        shard_id: 0,
        published,
        data_dir: directory.path().to_path_buf(),
        requirepass: Vec::new(),
        limits,
        max_replicas: MAX_REPLICAS,
        backlog_floor: Arc::new(AtomicU64::new(u64::MAX)),
        replica_count: Arc::new(AtomicU64::new(0)),
        acks: Arc::new(DurableAcks::new()),
        fanout: Arc::new(PublishFanout::new()),
        snapshot_max_bytes: 1024 * 1024 * 1024,
        shard_count: 1,
        history: [0; HISTORY_LEN],
        snapshot_requests: Arc::new(AtomicU64::new(0)),
    })
    .unwrap();

    let mut live = connect_replication(address);
    let live_handshake = encode_handshake(&Handshake {
        version: VERSION_RESUME,
        shard_id: 0,
        after_sequence: 1,
        history: [0; HISTORY_LEN],
        shard_count: 1,
        password: Vec::new(),
    })
    .unwrap();

    live.write_all(&live_handshake).unwrap();
    let reply = read_socket(&mut live, REPLY_LEN);

    assert_eq!(decode_reply(&reply).unwrap().0, HandshakeStatus::Ok);
    let _frame = read_replication_frame(&mut live);

    std::fs::remove_file(&segments[0]).unwrap();

    let mut gap = connect_replication(address);
    let gap_handshake = encode_handshake(&Handshake {
        version: VERSION_RESUME,
        shard_id: 0,
        after_sequence: 0,
        history: [0; HISTORY_LEN],
        shard_count: 1,
        password: Vec::new(),
    })
    .unwrap();

    gap.write_all(&gap_handshake).unwrap();
    let gap_reply = read_socket(&mut gap, REPLY_LEN);

    assert_eq!(decode_reply(&gap_reply).unwrap().0, HandshakeStatus::Gap);
    drop(gap);
    thread::sleep(Duration::from_millis(200));
    live.set_read_timeout(Some(Duration::from_millis(200)))
        .unwrap();

    let mut byte = [0; 1];
    let still_live = live.read(&mut byte);

    assert!(
        matches!(
            still_live,
            Err(ref error)
                if error.kind() == io::ErrorKind::TimedOut
                    || error.kind() == io::ErrorKind::WouldBlock
        ),
        "gap handshake closed the live replica stream: {still_live:?}"
    );
    drop(primary);
}

#[test]
fn two_replicas_receive_the_same_appended_frame() {
    use crate::{
        mutation::{Mutation, MutationSink},
        persistence::{DurabilityProfile, PersistenceConfig, WalSink},
        protocol::ProtocolLimits,
    };

    let limits = ProtocolLimits::default();
    let directory = tempfile::tempdir().unwrap();
    let config = PersistenceConfig {
        profile: DurabilityProfile::Always,
        data_dir: directory.path().to_path_buf(),
        wal_segment_bytes: 4096,
        sync_interval: Duration::from_millis(1),
        ..PersistenceConfig::default()
    };

    let (mut sink, _, _) = WalSink::open(config, 0, limits).unwrap();
    let published = sink.published();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();

    drop(listener);
    let primary = spawn_primary(Shipment {
        listen: address,
        shard_id: 0,
        published,
        data_dir: directory.path().to_path_buf(),
        requirepass: Vec::new(),
        limits,
        max_replicas: MAX_REPLICAS,
        backlog_floor: Arc::new(AtomicU64::new(u64::MAX)),
        replica_count: Arc::new(AtomicU64::new(0)),
        acks: Arc::new(DurableAcks::new()),
        fanout: Arc::new(PublishFanout::new()),
        snapshot_max_bytes: 1024 * 1024 * 1024,
        shard_count: 1,
        history: [0; HISTORY_LEN],
        snapshot_requests: Arc::new(AtomicU64::new(0)),
    })
    .unwrap();
    let mut first = open_ok_replica(address, 0);
    let mut second = open_ok_replica(address, 0);

    sink.record(Mutation::SetBytes {
        key: b"k",
        value: b"v",
    })
    .unwrap();
    sink.flush().unwrap();
    let first_frame = read_replication_frame(&mut first);
    let second_frame = read_replication_frame(&mut second);

    assert_eq!(first_frame, second_frame);
    drop(primary);
}

#[test]
fn closing_one_replica_leaves_the_other_streaming() {
    use crate::{
        mutation::{Mutation, MutationSink},
        persistence::{DurabilityProfile, PersistenceConfig, WalSink},
        protocol::ProtocolLimits,
    };

    let limits = ProtocolLimits::default();
    let directory = tempfile::tempdir().unwrap();
    let config = PersistenceConfig {
        profile: DurabilityProfile::Always,
        data_dir: directory.path().to_path_buf(),
        wal_segment_bytes: 4096,
        sync_interval: Duration::from_millis(1),
        ..PersistenceConfig::default()
    };

    let (mut sink, _, _) = WalSink::open(config, 0, limits).unwrap();
    let published = sink.published();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();

    drop(listener);
    let primary = spawn_primary(Shipment {
        listen: address,
        shard_id: 0,
        published,
        data_dir: directory.path().to_path_buf(),
        requirepass: Vec::new(),
        limits,
        max_replicas: MAX_REPLICAS,
        backlog_floor: Arc::new(AtomicU64::new(u64::MAX)),
        replica_count: Arc::new(AtomicU64::new(0)),
        acks: Arc::new(DurableAcks::new()),
        fanout: Arc::new(PublishFanout::new()),
        snapshot_max_bytes: 1024 * 1024 * 1024,
        shard_count: 1,
        history: [0; HISTORY_LEN],
        snapshot_requests: Arc::new(AtomicU64::new(0)),
    })
    .unwrap();
    let gone = open_ok_replica(address, 0);
    let mut staying = open_ok_replica(address, 0);

    drop(gone);
    sink.record(Mutation::SetBytes {
        key: b"k",
        value: b"v",
    })
    .unwrap();
    sink.flush().unwrap();
    let frame = read_replication_frame(&mut staying);

    assert!(frame.len() > 4);
    drop(primary);
}

#[test]
fn replica_limit_closes_the_extra_connection() {
    use crate::{
        persistence::{DurabilityProfile, PersistenceConfig, WalSink},
        protocol::ProtocolLimits,
    };

    let limits = ProtocolLimits::default();
    let directory = tempfile::tempdir().unwrap();
    let config = PersistenceConfig {
        profile: DurabilityProfile::Always,
        data_dir: directory.path().to_path_buf(),
        wal_segment_bytes: 4096,
        sync_interval: Duration::from_millis(1),
        ..PersistenceConfig::default()
    };

    let (sink, _, _) = WalSink::open(config, 0, limits).unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();

    drop(listener);
    let primary = spawn_primary(Shipment {
        listen: address,
        shard_id: 0,
        published: sink.published(),
        data_dir: directory.path().to_path_buf(),
        requirepass: Vec::new(),
        limits,
        max_replicas: 1,
        backlog_floor: Arc::new(AtomicU64::new(u64::MAX)),
        replica_count: Arc::new(AtomicU64::new(0)),
        acks: Arc::new(DurableAcks::new()),
        fanout: Arc::new(PublishFanout::new()),
        snapshot_max_bytes: 1024 * 1024 * 1024,
        shard_count: 1,
        history: [0; HISTORY_LEN],
        snapshot_requests: Arc::new(AtomicU64::new(0)),
    })
    .unwrap();
    let _live = open_ok_replica(address, 0);
    let mut extra = connect_replication(address);
    let handshake = encode_handshake(&Handshake {
        version: VERSION_RESUME,
        shard_id: 0,
        after_sequence: 0,
        history: [0; HISTORY_LEN],
        shard_count: 1,
        password: Vec::new(),
    })
    .unwrap();
    let wrote = extra.write_all(&handshake);

    if wrote.is_ok() {
        let mut byte = [0; 1];
        let read = extra.read(&mut byte);

        assert!(
            matches!(read, Ok(0) | Err(_)),
            "extra replica stayed open: {read:?}"
        );
    }

    drop(primary);
}

/// A replica that hangs up gives its slot back even while the primary has nothing to ship:
/// with a limit of one, the replica that dials again is served.
#[test]
fn a_replica_that_hangs_up_on_an_idle_primary_frees_its_slot() {
    use crate::{
        persistence::{DurabilityProfile, PersistenceConfig, WalSink},
        protocol::ProtocolLimits,
    };

    let limits = ProtocolLimits::default();
    let directory = tempfile::tempdir().unwrap();
    let config = PersistenceConfig {
        profile: DurabilityProfile::Always,
        data_dir: directory.path().to_path_buf(),
        wal_segment_bytes: 4096,
        sync_interval: Duration::from_millis(1),
        ..PersistenceConfig::default()
    };
    let (sink, _, _) = WalSink::open(config, 0, limits).unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();

    drop(listener);
    let primary = spawn_primary(Shipment {
        listen: address,
        shard_id: 0,
        published: sink.published(),
        data_dir: directory.path().to_path_buf(),
        requirepass: Vec::new(),
        limits,
        max_replicas: 1,
        backlog_floor: Arc::new(AtomicU64::new(u64::MAX)),
        replica_count: Arc::new(AtomicU64::new(0)),
        acks: Arc::new(DurableAcks::new()),
        fanout: Arc::new(PublishFanout::new()),
        snapshot_max_bytes: 1024 * 1024 * 1024,
        shard_count: 1,
        history: [0; HISTORY_LEN],
        snapshot_requests: Arc::new(AtomicU64::new(0)),
    })
    .unwrap();
    let handshake = encode_handshake(&Handshake {
        version: VERSION_RESUME,
        shard_id: 0,
        after_sequence: 0,
        history: [0; HISTORY_LEN],
        shard_count: 1,
        password: Vec::new(),
    })
    .unwrap();

    // Each replica is served, then hangs up; the next one waits at most for that
    // connection to notice.
    for _ in 0..4 {
        let deadline = std::time::Instant::now() + Duration::from_secs(3);
        let served = loop {
            let mut stream = connect_replication(address);
            let mut reply = [0u8; REPLY_LEN];
            let answered = stream
                .write_all(&handshake)
                .and_then(|()| stream.read_exact(&mut reply));

            if answered.is_ok() {
                break decode_reply(&reply).ok().map(|reply| reply.0);
            }
            if std::time::Instant::now() >= deadline {
                break None;
            }
            thread::sleep(Duration::from_millis(20));
        };

        assert_eq!(served, Some(HandshakeStatus::Ok));
    }

    drop(primary);
}

#[test]
fn primary_refuses_a_replica_from_another_history_or_shard_layout() {
    use crate::{
        persistence::{DurabilityProfile, PersistenceConfig, WalSink},
        protocol::ProtocolLimits,
    };

    let limits = ProtocolLimits::default();
    let directory = tempfile::tempdir().unwrap();
    let config = PersistenceConfig {
        profile: DurabilityProfile::Always,
        data_dir: directory.path().to_path_buf(),
        wal_segment_bytes: 4096,
        sync_interval: Duration::from_millis(1),
        ..PersistenceConfig::default()
    };
    let (sink, _, _) = WalSink::open(config, 0, limits).unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();

    drop(listener);
    let primary = spawn_primary(Shipment {
        listen: address,
        shard_id: 0,
        published: sink.published(),
        data_dir: directory.path().to_path_buf(),
        requirepass: Vec::new(),
        limits,
        max_replicas: 4,
        backlog_floor: Arc::new(AtomicU64::new(u64::MAX)),
        replica_count: Arc::new(AtomicU64::new(0)),
        acks: Arc::new(DurableAcks::new()),
        fanout: Arc::new(PublishFanout::new()),
        snapshot_max_bytes: 1024 * 1024 * 1024,
        shard_count: 2,
        history: [7; HISTORY_LEN],
        snapshot_requests: Arc::new(AtomicU64::new(0)),
    })
    .unwrap();
    let ask = |version: u16, after_sequence: u64, history: [u8; HISTORY_LEN], shard_count: u32| {
        let mut stream = connect_replication(address);
        let handshake = encode_handshake(&Handshake {
            version,
            shard_id: 0,
            after_sequence,
            history,
            shard_count,
            password: Vec::new(),
        })
        .unwrap();

        stream.write_all(&handshake).unwrap();
        decode_reply(&read_socket(&mut stream, REPLY_LEN))
            .unwrap()
            .0
    };

    // Resuming from its own log under a different history is refused; a forced snapshot
    // and a replica with no log yet are not.
    assert_eq!(
        ask(VERSION_RESUME, 5, [9; HISTORY_LEN], 2),
        HandshakeStatus::History
    );
    // Same history: the ordinary checks follow (this log is shorter than the replica's).
    assert_eq!(
        ask(VERSION_RESUME, 5, [7; HISTORY_LEN], 2),
        HandshakeStatus::Ahead
    );
    assert_eq!(
        ask(VERSION_RESUME, 0, [9; HISTORY_LEN], 2),
        HandshakeStatus::Ok
    );
    assert_eq!(
        ask(VERSION_RESUME, 0, [7; HISTORY_LEN], 3),
        HandshakeStatus::Shard
    );

    drop(primary);
}

#[cfg(test)]
fn open_ok_replica(address: SocketAddr, after_sequence: u64) -> TcpStream {
    let mut stream = connect_replication(address);
    let handshake = encode_handshake(&Handshake {
        version: VERSION_RESUME,
        shard_id: 0,
        after_sequence,
        history: [0; HISTORY_LEN],
        shard_count: 1,
        password: Vec::new(),
    })
    .unwrap();

    stream.write_all(&handshake).unwrap();
    let reply = read_socket(&mut stream, REPLY_LEN);

    assert_eq!(decode_reply(&reply).unwrap().0, HandshakeStatus::Ok);
    stream
}

#[cfg(test)]
fn connect_replication(address: SocketAddr) -> TcpStream {
    let deadline = std::time::Instant::now() + Duration::from_secs(2);

    loop {
        if let Ok(stream) = TcpStream::connect(address) {
            stream.set_nodelay(true).unwrap();
            stream
                .set_read_timeout(Some(Duration::from_secs(2)))
                .unwrap();
            stream
                .set_write_timeout(Some(Duration::from_secs(2)))
                .unwrap();

            return stream;
        }

        if std::time::Instant::now() >= deadline {
            panic!("timed out connecting to {address}");
        }

        thread::sleep(Duration::from_millis(10));
    }
}

#[cfg(test)]
fn read_socket(stream: &mut TcpStream, length: usize) -> Vec<u8> {
    let mut buffer = vec![0; length];

    stream.read_exact(&mut buffer).unwrap();
    buffer
}

#[cfg(test)]
fn read_replication_frame(stream: &mut TcpStream) -> Vec<u8> {
    let mut length_bytes = [0; 4];

    stream.read_exact(&mut length_bytes).unwrap();
    let length = u32::from_le_bytes(length_bytes) as usize;
    let mut frame = vec![0; length];

    frame[..4].copy_from_slice(&length_bytes);
    stream.read_exact(&mut frame[4..]).unwrap();
    frame
}

#[test]
fn copied_publish_round_trips_and_rejects_a_wal_frame() {
    let mut frame = encode_copied_publish(false, b"news", b"hello");

    assert!(frame.len() >= 32);
    assert_eq!(
        decode_copied_publish(&frame),
        Some((false, b"news".to_vec(), b"hello".to_vec()))
    );
    frame[4] = b'X';
    assert_eq!(decode_copied_publish(&frame), None);
}

#[cfg(test)]
fn idle_shipment(data_dir: &Path, history: HistoryId) -> Shipment {
    Shipment {
        listen: "127.0.0.1:0".parse().unwrap(),
        shard_id: 0,
        published: Arc::new(AtomicU64::new(0)),
        data_dir: data_dir.to_path_buf(),
        requirepass: Vec::new(),
        limits: ProtocolLimits::default(),
        max_replicas: 4,
        backlog_floor: Arc::new(AtomicU64::new(u64::MAX)),
        replica_count: Arc::new(AtomicU64::new(0)),
        acks: Arc::new(DurableAcks::new()),
        fanout: Arc::new(PublishFanout::new()),
        snapshot_max_bytes: 1024 * 1024,
        shard_count: 1,
        history,
        snapshot_requests: Arc::new(AtomicU64::new(0)),
    }
}

#[test]
fn upstream_history_is_kept_per_shard_and_ignores_the_shared_legacy_file() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path();

    // A copy of a primary's directory continues that primary's history.
    std::fs::write(path.join("HISTORY"), [3; HISTORY_LEN]).unwrap();
    assert_eq!(upstream_history(path, 0), [3; HISTORY_LEN]);

    record_upstream_history(path, 0, &[1; HISTORY_LEN]).unwrap();
    record_upstream_history(path, 1, &[2; HISTORY_LEN]).unwrap();
    assert_eq!(upstream_history(path, 0), [1; HISTORY_LEN]);
    assert_eq!(upstream_history(path, 1), [2; HISTORY_LEN]);
    assert_eq!(upstream_history(path, 2), [3; HISTORY_LEN]);
    assert!(!path.join("UPSTREAM_0000.tmp").exists());
    assert!(!path.join("UPSTREAM_0001.tmp").exists());

    // The shared file of older builds may have been written by another shard: a shard
    // without its own record claims no history, so the primary sends it a full copy.
    std::fs::write(path.join("UPSTREAM"), [4; HISTORY_LEN]).unwrap();
    assert_eq!(upstream_history(path, 2), [0; HISTORY_LEN]);
    assert_eq!(upstream_history(path, 1), [2; HISTORY_LEN]);
}

#[cfg(test)]
fn link_to(
    primary: SocketAddr,
    data_dir: &Path,
) -> (ReplicaLink, std::sync::mpsc::Receiver<ReplicaDelivery>) {
    let config = PersistenceConfig {
        profile: crate::persistence::DurabilityProfile::Everysec,
        data_dir: data_dir.to_path_buf(),
        ..PersistenceConfig::default()
    };
    let (wal, _, _) = WalSink::open(config, 0, ProtocolLimits::default()).unwrap();
    let (inbound, outbound) = std::sync::mpsc::sync_channel(QUEUE_FRAMES);

    (
        ReplicaLink {
            primary,
            primary_name: None,
            shard_id: 0,
            wal: Arc::new(Mutex::new(wal)),
            inbound,
            password: Vec::new(),
            limits: ProtocolLimits::default(),
            snapshot_max_bytes: 1024 * 1024,
            resync_sequence: Arc::new(AtomicU64::new(0)),
            data_dir: data_dir.to_path_buf(),
            shard_count: 1,
            status: Arc::new(LinkStatus::default()),
            wake_shard: Arc::new(tokio::sync::Notify::new()),
        },
        outbound,
    )
}

/// Accepts one connection within 10 s and gives the socket 10 s read and write timeouts.
#[cfg(test)]
fn accept_within_deadline(listener: &TcpListener) -> TcpStream {
    listener.set_nonblocking(true).unwrap();
    let deadline = std::time::Instant::now() + Duration::from_secs(10);

    loop {
        match listener.accept() {
            Ok((stream, _)) => {
                stream.set_nonblocking(false).unwrap();
                stream
                    .set_read_timeout(Some(Duration::from_secs(10)))
                    .unwrap();
                stream
                    .set_write_timeout(Some(Duration::from_secs(10)))
                    .unwrap();
                return stream;
            }
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                assert!(
                    std::time::Instant::now() < deadline,
                    "no connection reached the fake primary within 10 s"
                );
                thread::sleep(Duration::from_millis(5));
            }
            Err(error) => panic!("fake primary accept failed: {error}"),
        }
    }
}

/// One fake primary connection: reads the handshake, answers `reply`, then hangs up.
#[cfg(test)]
fn answer_once(reply: Vec<u8>) -> (SocketAddr, JoinHandle<Handshake>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let server = thread::spawn(move || {
        let mut stream = accept_within_deadline(&listener);
        let Ok(Incoming::Handshake(handshake)) = read_handshake(&mut stream) else {
            panic!("replica sent no current handshake");
        };

        stream.write_all(&reply).unwrap();
        handshake
    });

    (address, server)
}

#[test]
fn snapshot_reply_leaves_the_upstream_record_until_the_snapshot_is_installed() {
    let directory = tempfile::tempdir().unwrap();

    record_upstream_history(directory.path(), 0, &[1; HISTORY_LEN]).unwrap();

    // The primary answers with a snapshot of another history, then the link breaks before
    // the snapshot arrives (as a crash during the download would).
    let (address, server) = answer_once(encode_reply_with_history(
        HandshakeStatus::Snapshot,
        10,
        &[9; HISTORY_LEN],
    ));
    let (link, _deliveries) = link_to(address, directory.path());
    let mut stream = connect_replication(address);
    let (retry, _) = read_primary(
        &mut stream,
        &link,
        &mut VecDeque::new(),
        &AtomicBool::new(false),
    );

    assert!(retry);
    assert_eq!(server.join().unwrap().version, VERSION_RESUME);
    assert_eq!(upstream_history(directory.path(), 0), [1; HISTORY_LEN]);

    // A resume the primary accepts continues its history and is recorded at once.
    let (address, server) = answer_once(encode_reply_with_history(
        HandshakeStatus::Ok,
        1,
        &[9; HISTORY_LEN],
    ));
    let (link, _deliveries) = link_to(address, directory.path());
    let mut stream = connect_replication(address);

    // The fake primary hangs up after the reply, which ends the stream.
    read_primary(
        &mut stream,
        &link,
        &mut VecDeque::new(),
        &AtomicBool::new(false),
    );
    server.join().unwrap();
    assert_eq!(upstream_history(directory.path(), 0), [9; HISTORY_LEN]);
}

#[test]
fn primary_refuses_older_and_newer_handshake_versions_at_once() {
    let directory = tempfile::tempdir().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();

    listener.set_nonblocking(true).unwrap();
    let stop = Arc::new(AtomicBool::new(false));
    let shipment = Arc::new(idle_shipment(directory.path(), [7; HISTORY_LEN]));
    let accept_stop = Arc::clone(&stop);
    let primary = thread::spawn(move || accept_replicas(listener, shipment, accept_stop));

    // A build before the history id: its handshake ends after the password, and it reads a
    // reply without the history id.
    for legacy in [1u16, 3, 5] {
        let mut bytes = HANDSHAKE_MAGIC.to_vec();

        put_u16(&mut bytes, legacy);
        put_u32(&mut bytes, 0);
        put_u64(&mut bytes, 7);
        put_u16(&mut bytes, 0);
        append_checksum(&mut bytes);
        let mut stream = connect_replication(address);
        let started = std::time::Instant::now();

        stream.write_all(&bytes).unwrap();
        let reply = read_socket(&mut stream, LEGACY_REPLY_LEN);

        assert!(
            started.elapsed() < Duration::from_secs(1),
            "refusal waited for a timeout"
        );
        assert_eq!(&reply[..8], HANDSHAKE_MAGIC);
        assert_eq!(read_u16(&reply, 8), HandshakeStatus::Version as u16);
        assert!(checksum_matches(&reply));
    }

    // A newer build gets a refusal in this build's reply format.
    let mut stream = connect_replication(address);

    stream
        .write_all(
            &encode_handshake(&Handshake {
                version: VERSION_RESYNC_INCOMPLETE + 1,
                shard_id: 0,
                after_sequence: 0,
                password: Vec::new(),
                history: [0; HISTORY_LEN],
                shard_count: 1,
            })
            .unwrap(),
        )
        .unwrap();
    assert_eq!(
        decode_reply(&read_socket(&mut stream, REPLY_LEN))
            .unwrap()
            .0,
        HandshakeStatus::Version
    );

    stop.store(true, Ordering::Release);
    primary.join().unwrap();
}

#[test]
fn replica_accept_loop_survives_accept_errors() {
    let directory = tempfile::tempdir().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();

    listener.set_nonblocking(true).unwrap();
    let stop = Arc::new(AtomicBool::new(false));
    let shipment = Arc::new(idle_shipment(directory.path(), [7; HISTORY_LEN]));
    let accept_stop = Arc::clone(&stop);
    let primary = thread::spawn(move || {
        let mut failures = 3;

        accept_replicas_from(
            move || {
                if failures > 0 {
                    failures -= 1;

                    // What `accept` returns once the process is out of file descriptors.
                    return Err(io::Error::from_raw_os_error(24));
                }

                listener.accept()
            },
            shipment,
            accept_stop,
        )
    });
    let mut stream = connect_replication(address);

    stream
        .write_all(
            &encode_handshake(&Handshake {
                version: VERSION_RESUME,
                shard_id: 0,
                after_sequence: 0,
                password: Vec::new(),
                history: [0; HISTORY_LEN],
                shard_count: 1,
            })
            .unwrap(),
        )
        .unwrap();
    assert_eq!(
        decode_reply(&read_socket(&mut stream, REPLY_LEN))
            .unwrap()
            .0,
        HandshakeStatus::Ok
    );
    assert!(!primary.is_finished());

    stop.store(true, Ordering::Release);
    drop(stream);
    primary.join().unwrap();
}

#[test]
fn a_replica_clock_ahead_of_the_primary_neither_loses_an_extended_key_nor_logs_its_own_deletes() {
    use crate::{
        engine::{Response, execute_with_sink},
        persistence::{DurabilityProfile, apply_mutation},
        protocol::parse_command,
        time::{Clock, SystemClock, TestClock, UnixMillis},
    };

    fn command(text: &str) -> crate::protocol::Command<'static> {
        let parts: Vec<&str> = text.split(' ').collect();
        let mut frame = format!("*{}\r\n", parts.len()).into_bytes();

        for part in parts {
            frame.extend(format!("${}\r\n{part}\r\n", part.len()).bytes());
        }

        parse_command(&frame)
            .unwrap()
            .expect("complete")
            .0
            .into_owned()
    }

    let limits = ProtocolLimits::default();
    let primary_dir = tempfile::tempdir().unwrap();
    let replica_dir = tempfile::tempdir().unwrap();
    let config = |path: &std::path::Path| PersistenceConfig {
        profile: DurabilityProfile::Everysec,
        data_dir: path.to_path_buf(),
        ..PersistenceConfig::default()
    };
    let start = SystemClock::default().now_unix_millis().get();
    let primary_clock = TestClock::new(start);
    let mut primary = KvShard::with_clock(primary_clock.clone());
    let (mut primary_wal, _, _) = WalSink::open(config(primary_dir.path()), 0, limits).unwrap();

    primary.log_removals(true);
    // The replica's clock runs 5 s ahead of the primary's.
    let mut replica = KvShard::with_clock(SystemClock::with_floor(UnixMillis::new(start + 5_000)));
    let replica_config = config(replica_dir.path());
    let manager = CheckpointManager::new(replica_config.clone(), limits);
    let (mut replica_wal, _, _) = WalSink::open(replica_config.clone(), 0, limits).unwrap();

    replica.follow_primary(true);
    let primary_last = std::cell::Cell::new(0);
    let mut run = |text: &str| -> Response<'static> {
        let reply = execute_with_sink(&mut primary, command(text), &mut primary_wal)
            .unwrap()
            .into_owned();

        primary_last.set(primary_wal.last_sequence());
        reply
    };
    let mut replicate = |replica: &mut KvShard| {
        let after = replica_wal.last_sequence();
        let FrameRead::Frames(frames) =
            read_frames(primary_dir.path(), 0, after, primary_last.get(), limits).unwrap()
        else {
            panic!("primary frames unavailable");
        };

        for frame in frames {
            let paused = replica.pause_expiration_for_replay();
            let (database, mutation, _) = accept_replica_frame(
                replica,
                &mut replica_wal,
                &manager,
                &replica_config,
                &frame,
                limits,
            )
            .expect("no divergence, no resync");

            apply_mutation(replica, database, &mutation).unwrap();
            replica.resume_expiration_after_replay(paused);
        }
        replica_wal.last_sequence()
    };
    let read = |replica: &mut KvShard, text: &str| {
        crate::engine::execute(replica, command(text)).into_owned()
    };

    run("SET k v PX 2000");
    run("SET g v PX 2000");
    replicate(&mut replica);

    // Past the deadline by the replica's clock only: hidden, not removed.
    assert_eq!(read(&mut replica, "GET k"), Response::Bulk(None));
    assert_eq!(read(&mut replica, "TTL k"), Response::Integer(-2));
    assert_eq!(read(&mut replica, "DBSIZE"), Response::Integer(2));

    // The primary extends k just before its own deadline.
    primary_clock.set(start + 1_900);
    assert_eq!(run("PEXPIRE k 60000"), Response::Integer(1));
    replicate(&mut replica);
    assert_eq!(
        read(&mut replica, "GET k"),
        Response::Bulk(Some(std::borrow::Cow::Borrowed(b"v")))
    );

    // g expires on the primary; its delete reaches the replica with the next write.
    primary_clock.set(start + 2_500);
    assert_eq!(run("GET g"), Response::Bulk(None));
    run("SET other x");
    let replicated = replicate(&mut replica);

    assert_eq!(read(&mut replica, "EXISTS g"), Response::Integer(0));
    assert_eq!(read(&mut replica, "DBSIZE"), Response::Integer(2));
    // The replica's log is exactly the primary's: it recorded no delete of its own.
    assert_eq!(replicated, primary_last.get());
    assert_eq!(replica.expiration_stats().expired_keys, 0);
}

#[test]
fn a_replica_checkpoint_holds_its_snapshot_point_while_frames_keep_arriving() {
    use crate::{
        engine::execute_with_sink,
        persistence::{DurabilityProfile, apply_mutation},
        protocol::parse_command,
        store::{SnapshotEntry, SnapshotStep},
    };

    fn command(text: &str) -> crate::protocol::Command<'static> {
        let parts: Vec<&str> = text.split(' ').collect();
        let mut frame = format!("*{}\r\n", parts.len()).into_bytes();

        for part in parts {
            frame.extend(format!("${}\r\n{part}\r\n", part.len()).bytes());
        }

        parse_command(&frame)
            .unwrap()
            .expect("complete")
            .0
            .into_owned()
    }

    fn sorted(mut entries: Vec<SnapshotEntry>) -> Vec<SnapshotEntry> {
        entries.sort_by(|left, right| {
            left.database
                .cmp(&right.database)
                .then_with(|| left.key.cmp(&right.key))
        });
        entries
    }

    let limits = ProtocolLimits::default();
    let primary_dir = tempfile::tempdir().unwrap();
    let replica_dir = tempfile::tempdir().unwrap();
    let config = |path: &std::path::Path| PersistenceConfig {
        profile: DurabilityProfile::Everysec,
        data_dir: path.to_path_buf(),
        ..PersistenceConfig::default()
    };
    let mut primary = KvShard::new();
    let (mut primary_wal, _, _) = WalSink::open(config(primary_dir.path()), 0, limits).unwrap();
    let mut replica = KvShard::new();
    let replica_config = config(replica_dir.path());
    let manager = CheckpointManager::new(replica_config.clone(), limits);
    let (mut replica_wal, _, _) = WalSink::open(replica_config.clone(), 0, limits).unwrap();

    replica.follow_primary(true);
    let mut run = |text: &str| {
        execute_with_sink(&mut primary, command(text), &mut primary_wal).unwrap();
        primary_wal.last_sequence()
    };
    // As the replica's shard loop does: log each frame, then apply it.
    let mut replicate = |replica: &mut KvShard, through: u64| {
        let after = replica_wal.last_sequence();
        let FrameRead::Frames(frames) =
            read_frames(primary_dir.path(), 0, after, through, limits).unwrap()
        else {
            panic!("primary frames unavailable");
        };

        for frame in frames {
            let paused = replica.pause_expiration_for_replay();
            let (database, mutation, _) = accept_replica_frame(
                replica,
                &mut replica_wal,
                &manager,
                &replica_config,
                &frame,
                limits,
            )
            .expect("no divergence, no resync");

            apply_mutation(replica, database, &mutation).unwrap();
            replica.resume_expiration_after_replay(paused);
        }
        replica_wal.last_sequence()
    };

    let keys = 128;
    for index in 0..keys {
        run(&format!("RPUSH list:{index} a b"));
        run(&format!("XADD stream:{index} {}-1 f v", index + 1));
    }
    run("SET gone v");
    run("SET renamed v");
    run("SET moved v");
    let covered = replicate(&mut replica, run("HSET hash f v"));

    // A background checkpoint begins and streams part of the store into its file, as its
    // slices do.
    let generation = 1;

    manager.begin_round(generation).unwrap();
    let mut writer = manager.shard_writer(generation, 0, 1, covered).unwrap();
    replica.begin_snapshot(64 * 1024 * 1024).unwrap();
    let at_snapshot_point = sorted(replica.snapshot_entries());
    let mut buffer = Vec::new();
    let walked = std::cell::Cell::new(0u64);
    let mut emit = |entry: crate::store::WalkedEntry<'_>| {
        entry.encode_into(&mut buffer).unwrap();
        walked.set(walked.get() + 1);
    };

    while walked.get() == 0 {
        assert_eq!(replica.snapshot_step(1, &mut emit), SnapshotStep::More);
    }

    // Frames for keys the walk did and did not pass arrive before it ends.
    for index in 0..keys {
        run(&format!("RPUSH list:{index} c"));
        run(&format!("XADD stream:{index} {}-2 f w", index + 1));
    }
    run("DEL gone");
    run("RENAME renamed fresh");
    run("MOVE moved 3");
    let last = replicate(&mut replica, run("HSET hash g w"));

    while replica.snapshot_step(keys, &mut emit) != SnapshotStep::Done {}
    for entry in replica.finish_snapshot_journal() {
        crate::persistence::encode_entry(&mut buffer, &entry).unwrap();
        walked.set(walked.get() + 1);
    }
    writer.write_chunk(&buffer, walked.get()).unwrap();
    manager
        .commit_round(generation, &[writer.finish().unwrap()])
        .unwrap();

    let published = manager.load().unwrap().expect("published checkpoint");
    let [shard] = <[_; 1]>::try_from(published.shards)
        .ok()
        .expect("one shard");

    assert_eq!(shard.last_sequence, covered);
    let written = sorted(shard.entries);

    let differing: Vec<String> = written
        .iter()
        .filter(|entry| !at_snapshot_point.contains(entry))
        .chain(
            at_snapshot_point
                .iter()
                .filter(|entry| !written.contains(entry)),
        )
        .map(|entry| String::from_utf8_lossy(&entry.key).into_owned())
        .collect();

    assert!(
        differing.is_empty(),
        "the checkpoint does not hold these keys as they were at its snapshot point: {differing:?}"
    );

    // A restart loads the file and replays the log after the point it covers.
    let mut restarted = KvShard::new();

    restarted.follow_primary(true);
    for entry in written {
        restarted.restore_snapshot_entry(entry).unwrap();
    }
    let FrameRead::Frames(frames) =
        read_frames(replica_dir.path(), 0, covered, last, limits).unwrap()
    else {
        panic!("replica frames unavailable");
    };
    for frame in frames {
        let record = decode_record(&frame, 0, limits).unwrap();

        apply_mutation(&mut restarted, record.database, &record.mutation).unwrap();
    }
    assert_eq!(
        sorted(restarted.snapshot_entries()),
        sorted(replica.snapshot_entries())
    );
}
