use std::{
    fs::{self, File, OpenOptions},
    io::{self, Read, Seek, SeekFrom},
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Instant,
};

use crate::{
    OwnedMutation,
    mutation::{Mutation, MutationSink},
    protocol::ProtocolLimits,
};

use super::{
    DurabilityProfile, PersistenceConfig, PersistenceError,
    codec::{
        FORMAT_MINOR, RECORD_MIN_LEN, WAL_HEADER_LEN, decode_record, decode_segment_header,
        encode_borrowed_record_into, encode_segment_header, record_header_plausible,
    },
};

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RecoveryReport {
    pub records_replayed: u64,
    pub last_sequence: u64,
    pub torn_tail_bytes: u64,
}

/// Replayed WAL records with the database each one belongs to.
pub type RecoveredMutations = Vec<(u8, OwnedMutation)>;

/// Bytes staged for one client batch before a single `write`.
const WAL_GROUP_BYTES: usize = 64 * 1024;
/// How far ahead of the write cursor a segment is extended with `fallocate`.
const WAL_PREALLOC_CHUNK: u64 = 4 * 1024 * 1024;
/// A new chunk is requested when less than this much written-zero space is left ahead of the cursor.
const WAL_PREALLOC_AHEAD: u64 = 3 * 1024 * 1024;

/// Data directories whose WAL syncs fail (fault injection for tests that run a server).
#[cfg(test)]
pub(crate) static FAILING_SYNC_DIRS: Mutex<Vec<PathBuf>> = Mutex::new(Vec::new());

/// Like `FAILING_SYNC_DIRS`, for one shard's WAL of a data directory.
#[cfg(test)]
pub(crate) static FAILING_SYNC_SHARDS: Mutex<Vec<(PathBuf, u32)>> = Mutex::new(Vec::new());

/// Like `FAILING_SYNC_SHARDS`, but only the next sync of that shard's WAL fails.
#[cfg(test)]
pub(crate) static FAILING_NEXT_SYNC_SHARDS: Mutex<Vec<(PathBuf, u32)>> = Mutex::new(Vec::new());

/// One shard's WAL of a data directory whose next writes fail, as many as the count says
/// (fault injection for tests that run a server; the log resumes by itself afterwards).
#[cfg(test)]
pub(crate) static FAILING_WRITE_SHARDS: Mutex<Vec<(PathBuf, u32, u32)>> = Mutex::new(Vec::new());

/// Data directories whose WAL file writes are counted (for tests that run a server).
#[cfg(test)]
pub(crate) static COUNTED_WRITE_DIRS: Mutex<Vec<(PathBuf, u64)>> = Mutex::new(Vec::new());

#[cfg(test)]
fn count_write(data_dir: &Path) {
    let mut counted = COUNTED_WRITE_DIRS
        .lock()
        .unwrap_or_else(|error| error.into_inner());

    if let Some((_, writes)) = counted.iter_mut().find(|(dir, _)| dir == data_dir) {
        *writes += 1;
    }
}

#[cfg(test)]
fn injected_write_fault(data_dir: &Path, shard_id: u32) -> Option<io::Error> {
    let mut failing = FAILING_WRITE_SHARDS
        .lock()
        .unwrap_or_else(|error| error.into_inner());
    let (_, _, left) = failing
        .iter_mut()
        .find(|(dir, shard, left)| dir == data_dir && *shard == shard_id && *left > 0)?;

    *left -= 1;
    Some(io::Error::new(
        io::ErrorKind::StorageFull,
        "injected write failure",
    ))
}

#[cfg(test)]
fn injected_sync_fault(data_dir: &Path, shard_id: u32) -> Option<io::Error> {
    super::crash::take_io_fault(super::crash::CrashSite::Sync)
        .or_else(|| {
            FAILING_SYNC_DIRS
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .iter()
                .any(|dir| dir == data_dir)
                .then(|| io::Error::other("injected sync failure"))
        })
        .or_else(|| {
            FAILING_SYNC_SHARDS
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .iter()
                .any(|(dir, shard)| dir == data_dir && *shard == shard_id)
                .then(|| io::Error::other("injected sync failure"))
        })
        .or_else(|| {
            let mut once = FAILING_NEXT_SYNC_SHARDS
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            let index = once
                .iter()
                .position(|(dir, shard)| dir == data_dir && *shard == shard_id)?;

            once.remove(index);
            Some(io::Error::other("injected sync failure"))
        })
}

/// How many times a connected replica's WAL hold was dropped for exceeding the budget.
pub(crate) static REPLICA_HOLDS_DROPPED: AtomicU64 = AtomicU64::new(0);

pub struct WalSink {
    profile: DurabilityProfile,
    config: PersistenceConfig,
    limits: ProtocolLimits,
    shard_id: u32,
    next_sequence: u64,
    segment_id: u64,
    segment_first_sequence: u64,
    file: Option<File>,
    file_length: u64,
    /// Bytes physically written; `file` is not in append mode so a preallocated tail does not move the cursor.
    write_pos: u64,
    /// End of what the last successful shard-thread `fsync` of the open segment covered.
    /// Not advanced by the sync worker, so only used where the worker is off.
    synced_pos: u64,
    /// Size the segment file has been extended to by `fallocate` (equals `write_pos` when not preallocating).
    prealloc_end: u64,
    prealloc: bool,
    prefill: Prefill,
    dirty_bytes: u64,
    /// The last sync barrier handed to the worker: its sequence and `dirty_bytes` then.
    /// Once that sequence is durable those bytes are synced, so steady writes, where a
    /// new record always follows before the check, do not grow `dirty_bytes` forever.
    barrier_mark: (u64, u64),
    last_sync: Instant,
    failed: Option<String>,
    /// An `fsync` failed. The kernel may have dropped the dirty pages it covered and report a
    /// later sync as clean, so nothing written before it can be trusted as durable again:
    /// writes stay refused until a restart, whose recovery reads what actually reached disk.
    sync_failed: bool,
    /// The sequence of the last record of a hot-key write, which the shard applied to the
    /// shared hot store before the record synced (see `sync_hot_records`).
    hot_through: u64,
    /// A hot-key write was applied to the shared hot store and then refused by this log:
    /// the store may hold it until restart (see `sync_hot_records`).
    hot_refused: bool,
    last_recovery: Option<Instant>,
    published: Arc<AtomicU64>,
    durable: Arc<AtomicU64>,
    backlog_floor: Arc<AtomicU64>,
    replica_count: Arc<AtomicU64>,
    database: u8,
    staged: Vec<u8>,
    /// Sequence of the first record in `staged`; meaningful only while it is non-empty.
    staged_first_sequence: u64,
    record: Vec<u8>,
    group: u32,
    group_bytes: u64,
    /// One level of `group` is held for the shard's current scheduler turn (`hold_for_tick`).
    tick_hold: bool,
    sync_running: Arc<AtomicBool>,
    sync_error: Arc<Mutex<Option<String>>>,
    /// Bytes covered by the running background `everysec` sync; still counted in `dirty_bytes`.
    sync_in_flight: u64,
    sync_worker: Option<super::sync_worker::SyncWorker>,
    sync_worker_file: Option<Arc<File>>,
    /// `fsync`s issued on the shard thread (rotation, shutdown, fallback, no worker) and their time.
    inline_syncs: u64,
    inline_sync_nanos: u64,
}

struct ClearSync(Arc<AtomicBool>);

impl Drop for ClearSync {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
    }
}

impl WalSink {
    pub fn open(
        config: PersistenceConfig,
        shard_id: u32,
        limits: ProtocolLimits,
    ) -> Result<(Self, RecoveredMutations, RecoveryReport), PersistenceError> {
        Self::open_after(config, shard_id, limits, 0)
    }

    pub fn open_after(
        config: PersistenceConfig,
        shard_id: u32,
        limits: ProtocolLimits,
        after_sequence: u64,
    ) -> Result<(Self, RecoveredMutations, RecoveryReport), PersistenceError> {
        config.validate().map_err(|message| {
            PersistenceError::Failed(format!("invalid persistence configuration: {message}"))
        })?;

        if !config.profile.uses_wal() {
            let published = Arc::new(AtomicU64::new(0));
            let durable = Arc::new(AtomicU64::new(0));
            let backlog_floor = Arc::new(AtomicU64::new(u64::MAX));
            let replica_count = Arc::new(AtomicU64::new(0));

            return Ok((
                Self {
                    profile: config.profile,
                    config,
                    limits,
                    shard_id,
                    next_sequence: 1,
                    segment_id: 0,
                    segment_first_sequence: 1,
                    file: None,
                    file_length: 0,
                    write_pos: 0,
                    synced_pos: 0,
                    prealloc_end: 0,
                    prealloc: false,
                    prefill: Prefill::default(),
                    dirty_bytes: 0,
                    barrier_mark: (0, 0),
                    last_sync: Instant::now(),
                    failed: None,
                    sync_failed: false,
                    hot_through: 0,
                    hot_refused: false,
                    last_recovery: None,
                    published,
                    durable,
                    backlog_floor,
                    replica_count,
                    database: 0,
                    staged: Vec::new(),
                    staged_first_sequence: 0,
                    record: Vec::new(),
                    group: 0,
                    group_bytes: 0,
                    tick_hold: false,
                    sync_running: Arc::new(AtomicBool::new(false)),
                    sync_error: Arc::new(Mutex::new(None)),
                    sync_in_flight: 0,
                    sync_worker: None,
                    sync_worker_file: None,
                    inline_syncs: 0,
                    inline_sync_nanos: 0,
                },
                Vec::new(),
                RecoveryReport::default(),
            ));
        }

        let directory = shard_wal_directory(&config.data_dir, shard_id);

        fs::create_dir_all(&directory)?;
        let segments = discover_segments(&directory)?;
        let (mutations, report, last_segment) = recover_segments(
            &segments,
            shard_id,
            limits,
            config.wal_segment_bytes,
            after_sequence,
            false,
        )?;
        // A checkpoint can cover records that never reached the log (it was published while
        // the log was failing): continue after the checkpoint, never inside what it covers.
        let mut report = report;

        report.last_sequence = report.last_sequence.max(after_sequence);
        let next_sequence = report
            .last_sequence
            .checked_add(1)
            .ok_or_else(|| PersistenceError::Corruption("WAL sequence overflow".into()))?;

        // A segment written under an older minor is not appended to: its header would let an
        // older server open records it cannot read. The next record starts a new segment.
        let (file, segment_id, segment_first_sequence, file_length) = if let Some(segment) =
            last_segment.filter(|segment| {
                segment.next_sequence == next_sequence && segment.minor == FORMAT_MINOR
            }) {
            let mut file = OpenOptions::new()
                .read(true)
                .write(true)
                .open(&segment.path)?;
            let file_length = file.seek(SeekFrom::End(0))?;
            (
                file,
                segment.segment_id,
                segment.first_sequence,
                file_length,
            )
        } else {
            create_segment(&directory, shard_id, next_sequence)?
        };

        let published = Arc::new(AtomicU64::new(report.last_sequence));
        let durable = Arc::new(AtomicU64::new(report.last_sequence));
        let backlog_floor = Arc::new(AtomicU64::new(u64::MAX));
        let replica_count = Arc::new(AtomicU64::new(0));
        let sync_error = Arc::new(Mutex::new(None));
        let sync_worker = if config.sync_worker_active {
            Some(super::sync_worker::SyncWorker::new(
                Arc::clone(&durable),
                shard_id,
            )?)
        } else {
            None
        };

        Ok((
            Self {
                profile: config.profile,
                config,
                limits,
                shard_id,
                next_sequence,
                segment_id,
                segment_first_sequence,
                file: Some(file),
                file_length,
                write_pos: file_length,
                synced_pos: file_length,
                prealloc_end: file_length,
                prealloc: cfg!(target_os = "linux"),
                prefill: Prefill::reporting_to(Arc::clone(&sync_error)),
                dirty_bytes: 0,
                barrier_mark: (0, 0),
                last_sync: Instant::now(),
                failed: None,
                sync_failed: false,
                hot_through: 0,
                hot_refused: false,
                last_recovery: None,
                published,
                durable,
                backlog_floor,
                replica_count,
                database: 0,
                staged: Vec::with_capacity(WAL_GROUP_BYTES),
                staged_first_sequence: 0,
                record: Vec::new(),
                group: 0,
                group_bytes: 0,
                tick_hold: false,
                sync_running: Arc::new(AtomicBool::new(false)),
                sync_error,
                sync_in_flight: 0,
                sync_worker,
                sync_worker_file: None,
                inline_syncs: 0,
                inline_sync_nanos: 0,
            },
            mutations,
            report,
        ))
    }

    /// The configured segment size: no record larger than this fits in the log.
    pub(crate) fn segment_bytes(&self) -> u64 {
        self.config.wal_segment_bytes
    }

    pub fn published(&self) -> Arc<AtomicU64> {
        Arc::clone(&self.published)
    }

    /// Last sequence synced to disk. Replication ships through this cursor.
    pub fn durable(&self) -> Arc<AtomicU64> {
        Arc::clone(&self.durable)
    }

    /// Lowest sequence a connected replica still needs. `u64::MAX` holds nothing.
    pub fn backlog_floor(&self) -> Arc<AtomicU64> {
        Arc::clone(&self.backlog_floor)
    }

    /// Connected replicas whose socket this primary is writing.
    pub fn replica_count(&self) -> Arc<AtomicU64> {
        Arc::clone(&self.replica_count)
    }

    pub fn profile(&self) -> DurabilityProfile {
        self.profile
    }

    /// Last sequence synced to disk (see `durable`).
    pub fn durable_sequence(&self) -> u64 {
        self.durable.load(Ordering::Acquire)
    }

    pub fn last_sequence(&self) -> u64 {
        self.next_sequence.saturating_sub(1)
    }

    /// Whether the log on disk still starts at the first sequence: nothing was compacted
    /// away and no base was installed, so replaying it from sequence 0 rebuilds the shard.
    pub fn holds_every_record(&self) -> Result<bool, PersistenceError> {
        let directory = shard_wal_directory(&self.config.data_dir, self.shard_id);
        let first = discover_segments(&directory)?
            .first()
            .and_then(|path| path.file_stem()?.to_str()?.parse::<u64>().ok());

        Ok(first.is_none_or(|first| first <= 1))
    }

    pub fn install_base(&mut self, sequence: u64) -> Result<(), PersistenceError> {
        if !self.profile.uses_wal() {
            return Ok(());
        }

        let next = sequence
            .checked_add(1)
            .ok_or_else(|| PersistenceError::Failed("WAL sequence overflow".into()))?;
        let directory = shard_wal_directory(&self.config.data_dir, self.shard_id);

        if let Some(worker) = &self.sync_worker {
            worker.drain()?;
            worker.reset(sequence)?;
            self.sync_worker_file = None;
        }
        self.file = None;
        fs::create_dir_all(&directory)?;

        for entry in fs::read_dir(&directory)? {
            let path = entry?.path();

            if path.extension().and_then(|ext| ext.to_str()) == Some("wal") {
                fs::remove_file(path)?;
            }
        }

        let (file, segment_id, first_sequence, file_length) =
            create_segment(&directory, self.shard_id, next)?;

        self.file = Some(file);
        self.segment_id = segment_id;
        self.segment_first_sequence = first_sequence;
        self.file_length = file_length;
        self.write_pos = file_length;
        self.synced_pos = file_length;
        self.prealloc_end = file_length;
        self.prefill.reset();
        self.next_sequence = next;
        self.dirty_bytes = 0;
        self.staged.clear();
        self.wait_background_sync();
        self.sync_in_flight = 0;
        self.published.store(sequence, Ordering::Release);
        self.durable.store(sequence, Ordering::Release);
        Ok(())
    }

    pub fn shard_id(&self) -> u32 {
        self.shard_id
    }

    pub fn snapshot_journal_bytes(&self) -> usize {
        self.config.snapshot_journal_bytes
    }

    pub fn snapshot_interval(&self) -> std::time::Duration {
        self.config.snapshot_interval
    }

    pub fn snapshot_min_mutations(&self) -> u64 {
        self.config.snapshot_min_mutations
    }

    pub fn dirty_bytes(&self) -> u64 {
        self.dirty_bytes
    }

    pub fn log_fields(&self) -> (u64, u64) {
        (self.last_sequence(), self.dirty_bytes())
    }

    pub fn sync_interval(&self) -> std::time::Duration {
        self.config.sync_interval
    }

    /// A sync failed: the log refuses writes until restart (see `sync_failed`).
    pub fn sync_failed(&self) -> bool {
        self.sync_failed
    }

    /// Before a hot-key write is applied to the shared hot store: a failed log tries to
    /// resume, as its next record would, and refuses the write while it cannot, so the
    /// write is refused before any other shard can read it.
    pub fn ready_for_record(&mut self) -> Result<(), PersistenceError> {
        if self.failed.is_none() {
            return Ok(());
        }

        self.recover_if_failed()
    }

    /// The records just made hold a hot-key write already in the shared hot store.
    pub fn note_hot_record(&mut self) {
        self.hot_through = self.last_sequence();
    }

    /// A hot-key write already in the shared hot store was refused (its record failed).
    pub fn note_hot_refused(&mut self) {
        self.hot_refused = true;
    }

    /// For a cluster round's hot partition, taken from the shared hot store: under
    /// `always`, syncs this log's hot-key records, so every hot write the partition holds
    /// is acknowledged or will be. Fails while that is not so; the flag says whether a
    /// later attempt may succeed: not after this log refused a hot write that the store
    /// may still hold (a failed record, or a failed sync past which `hot_through` lies),
    /// which lasts until restart. A log that failed for other records alone does not keep
    /// the partition out.
    pub fn sync_hot_records(&mut self) -> Result<(), (PersistenceError, bool)> {
        if self.profile != DurabilityProfile::Always {
            return Ok(());
        }

        let shard_id = self.shard_id;
        let refused = move |message: &str| {
            PersistenceError::Failed(format!(
                "hot partition not taken: shard {shard_id}'s log refused a hot-key write the \
                 hot store may still hold ({message})"
            ))
        };

        if self.hot_refused {
            return Err((refused("record failed"), false));
        }

        // A base installed since (a replica's resync) may have moved the sequence back.
        let hot_through = self.hot_through.min(self.last_sequence());

        if hot_through <= self.durable.load(Ordering::Acquire) {
            return Ok(());
        }

        // Records of a write failure are written again once the log resumes.
        if self.failed.is_some() && !self.sync_failed {
            let _ = self.recover_if_failed();
        }

        self.sync_for_checkpoint(hot_through).map_err(|error| {
            if self.sync_failed {
                (refused(&error.to_string()), false)
            } else {
                (error, true)
            }
        })
    }

    pub fn failure(&self) -> Option<&str> {
        self.failed.as_deref().or_else(|| {
            self.sync_worker
                .as_ref()
                .filter(|worker| worker.unavailable())
                .map(|_| "WAL sync worker unavailable")
        })
    }

    /// Whether a checkpoint of memory as it stood at `sequence` may be published. With
    /// `always`, records are applied in memory before their batch syncs; when that sync
    /// fails their clients are refused and the records are cut from the log, but memory
    /// keeps them. A checkpoint holding them would revive refused writes after a restart, so
    /// a failed log publishes none past its last good sync. Acknowledged writes are all at
    /// or before that sync and stay in the log.
    pub fn checkpoint_may_cover(&self, sequence: u64) -> Result<(), PersistenceError> {
        if self.profile == DurabilityProfile::Always
            && sequence > self.durable.load(Ordering::Acquire)
            && let Some(error) = self.failure()
        {
            return Err(PersistenceError::Failed(error.to_owned()));
        }

        Ok(())
    }

    /// Before a checkpoint of memory as it stood at `sequence` is published: under `always`,
    /// records up to there that are not synced yet (a batch between its group's end and its
    /// sync, or one the sync worker holds) are synced now. If that sync fails their clients
    /// are refused, so the checkpoint is too (see `checkpoint_may_cover`).
    pub fn sync_for_checkpoint(&mut self, sequence: u64) -> Result<(), PersistenceError> {
        if self.profile != DurabilityProfile::Always
            || sequence <= self.durable.load(Ordering::Acquire)
        {
            return Ok(());
        }

        self.checkpoint_may_cover(sequence)?;
        if self.sync_worker.is_some() {
            self.flush()?;
        } else {
            self.sync_pending()?;
        }

        if sequence > self.durable.load(Ordering::Acquire) {
            return Err(PersistenceError::Failed(
                "checkpoint covers WAL records that are not synced".into(),
            ));
        }

        Ok(())
    }

    pub fn flush(&mut self) -> Result<(), PersistenceError> {
        if let Some(error) = &self.failed {
            return Err(PersistenceError::Failed(error.clone()));
        }

        // Wait before looking for errors: a background sync may fail after the check.
        self.wait_background_sync();
        self.observe_sync_error()?;
        self.settle_background_sync();
        if let Some(worker) = &self.sync_worker {
            worker.drain()?;
            if self.durable.load(Ordering::Acquire) >= self.last_sequence() {
                self.dirty_bytes = 0;
            }
        }
        self.publish_staged()?;

        if self.dirty_bytes != 0 {
            self.fsync_always_or_cut()?;
            self.durable.store(self.last_sequence(), Ordering::Release);
        }
        self.mark_published();

        Ok(())
    }

    /// Shutdown: try to leave the failed state at once (no retry spacing), then flush.
    pub fn flush_for_shutdown(&mut self) -> Result<(), PersistenceError> {
        if self.failed.is_some() && self.profile.uses_wal() {
            self.last_recovery = None;
            self.recover_if_failed()?;
        }

        self.flush()
    }

    pub fn sync_if_due(&mut self) -> Result<(), PersistenceError> {
        if self.profile != DurabilityProfile::Everysec {
            return Ok(());
        }

        self.poke_background_sync()
    }

    /// Stage later records so one client batch shares a write and, for `always`, one sync.
    pub fn begin_group(&mut self) {
        if self.profile.uses_wal() {
            if self.group == 0 {
                self.group_bytes = 0;
            }
            self.group = self.group.saturating_add(1);
        }
    }

    /// Bytes appended since the outermost open group began; zero outside a group.
    pub fn group_bytes(&self) -> u64 {
        if self.group == 0 { 0 } else { self.group_bytes }
    }

    /// Write staged records. `always` sync happens in `sync_pending` so several clients can share it.
    pub fn end_group(&mut self) -> Result<(), PersistenceError> {
        if self.group == 0 {
            return Ok(());
        }

        self.group -= 1;

        if self.group > 0 {
            return Ok(());
        }

        // A batch that appended nothing while the log is failed (reads, refused writes) has
        // nothing to publish; records kept from the failed batch wait for recovery.
        if self.failed.is_some() && self.group_bytes == 0 {
            return Ok(());
        }

        self.observe_sync_error()?;
        self.publish_staged()?;
        // An `always` batch reaches replicas from `sync_pending`, once it is on disk: a
        // batch whose sync fails is cut, and a restarted primary reuses its sequences.
        if self.profile != DurabilityProfile::Always || self.sync_worker.is_some() {
            self.mark_published();
        }

        if self.profile == DurabilityProfile::Everysec {
            self.poke_background_sync()?;
        }

        Ok(())
    }

    /// Writes the records staged behind another task's open group before a reply that may
    /// acknowledge one of them leaves. The group counter is shared by the shard's tasks, so a
    /// write made outside a group, or in a group nested in another task's, is only staged;
    /// an `everysec` reply promises its record reached the kernel. `always` replies wait for
    /// `sync_pending` or a worker barrier, which publish it themselves. Records staged
    /// before a failure wait for recovery, as in `end_group`: a write staged behind another
    /// task's group when that group's write failed is answered while it is still in memory,
    /// and a process kill before recovery loses it. Refusing the reply instead would cut off
    /// every client of the shard, readers too, until recovery. A reply that waited for the
    /// turn's shared write (`publish_through`) is the exception: when that write fails,
    /// every waiter of the turn, readers included, gets the error instead of a reply.
    pub fn publish_before_reply(&mut self) -> Result<(), PersistenceError> {
        debug_assert!(self.profile != DurabilityProfile::Always);
        if self.staged.is_empty() || self.failed.is_some() {
            return Ok(());
        }

        self.publish_staged()?;
        self.mark_published();

        Ok(())
    }

    /// An `everysec` reply that waits for records staged in memory (its own or another
    /// task's) may wait for the end of the scheduler turn instead of writing them now, so
    /// every connection served in the turn shares one `write`. True when there is such a
    /// record and the log can still write it.
    pub fn reply_waits_for_publish(&self) -> bool {
        self.profile == DurabilityProfile::Everysec
            && !self.staged.is_empty()
            && self.failed.is_none()
    }

    /// Keeps the log staging until the turn's first waiter calls `publish_through`: one
    /// level of the shared group counter belongs to the turn, so closing a connection's
    /// group does not write. Only called when a reply waits for the staged records, so
    /// a waiter always releases it.
    pub fn hold_for_tick(&mut self) {
        if !self.tick_hold && self.profile == DurabilityProfile::Everysec {
            self.begin_group();
            self.tick_hold = true;
        }
    }

    /// Ends the turn's hold, writing what is staged (unless another task's group is still
    /// open). Also called from the shard's maintenance tick, so a hold whose waiter went
    /// away does not keep records in memory.
    pub fn release_tick_hold(&mut self) -> Result<(), PersistenceError> {
        if !std::mem::take(&mut self.tick_hold) {
            return Ok(());
        }

        self.end_group()
    }

    /// Writes the records up to `through` (the last sequence when a reply began waiting)
    /// before that reply leaves. A waiter whose records another waiter's `write` already
    /// published makes no system call; one whose records are still staged after a failed
    /// write gets the error and no reply, as it would have from its own `write`.
    pub fn publish_through(&mut self, through: u64) -> Result<(), PersistenceError> {
        self.release_tick_hold()?;
        if self.staged.is_empty() || self.staged_first_sequence > through {
            return Ok(());
        }
        if let Some(error) = &self.failed {
            return Err(PersistenceError::Failed(error.clone()));
        }

        self.publish_staged()?;
        self.mark_published();

        Ok(())
    }

    /// Sync dirty `always` bytes before their replies are sent.
    pub fn sync_pending(&mut self) -> Result<(), PersistenceError> {
        if self.profile != DurabilityProfile::Always {
            return Ok(());
        }
        // A failed log may have cut this batch with another one, or lost its pages in a
        // sync another client saw fail: no batch is acknowledged until recovery. A caller
        // whose batch wrote nothing does not call this while the log is failed.
        if let Some(error) = &self.failed {
            return Err(PersistenceError::Failed(error.clone()));
        }
        if self.dirty_bytes == 0 {
            self.mark_published();
            return Ok(());
        }

        self.observe_sync_error()?;
        self.publish_staged()?;
        self.fsync_always_or_cut()?;
        self.durable.store(self.last_sequence(), Ordering::Release);
        self.mark_published();
        Ok(())
    }

    /// Sync the open segment. Under `always` the unsynced records are not acknowledged yet,
    /// and once a sync fails the log refuses every batch until restart, so every client of
    /// those records gets the error, whichever caller's sync failed (its own batch, a `SAVE`
    /// or a rotation in between): cut them so a restart does not revive them. With the
    /// worker the shard thread does not know what it synced.
    fn fsync_always_or_cut(&mut self) -> Result<(), PersistenceError> {
        let synced = self.fsync_file();

        if synced.is_err() {
            self.cut_unsynced_always_records();
        }
        synced
    }

    /// See `fsync_always_or_cut`: the log failed, so no unsynced `always` record will be
    /// acknowledged.
    fn cut_unsynced_always_records(&mut self) {
        if self.profile == DurabilityProfile::Always
            && self.sync_worker.is_none()
            && self.synced_pos < self.write_pos
        {
            self.forget_unsynced_since(self.synced_pos);
        }
    }

    /// `sync_pending` for a batch that began when the last sequence was `start`. A batch
    /// that wrote nothing has nothing to acknowledge, so a failed log does not fail it.
    pub fn sync_batch(&mut self, start: u64) -> Result<(), PersistenceError> {
        if self.failed.is_some() && self.last_sequence() == start {
            return Ok(());
        }

        self.sync_pending()
    }

    /// Closes a batch whose client went away before its commit (`start` as in `sync_batch`).
    /// Its records are still synced and published now, so they do not wait for another
    /// client's sync on this shard.
    pub fn abandon_group(&mut self, start: u64) -> Result<(), PersistenceError> {
        self.end_group()?;
        if self.profile != DurabilityProfile::Always {
            return Ok(());
        }
        if self.sync_worker.is_some() {
            // The worker syncs it; nobody waits for the result.
            return self.prepare_sync_barrier().map(drop);
        }

        self.sync_batch(start)
    }

    /// Capture the observed-write sequence before releasing the persistence lock.
    pub(crate) fn prepare_sync_barrier(
        &mut self,
    ) -> Result<Option<super::sync_worker::SyncBarrier>, PersistenceError> {
        if self.sync_worker.is_none() {
            return Ok(None);
        }
        if let Some(error) = &self.failed {
            return Err(PersistenceError::Failed(error.clone()));
        }
        self.observe_sync_error()?;
        self.publish_staged()?;
        self.mark_published();
        #[cfg(test)]
        if FAILING_SYNC_DIRS
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .contains(&self.config.data_dir)
            && let Some(worker) = &self.sync_worker
        {
            // The worker's syncs fail from here on, like the shard thread's would.
            worker.fail_sync_for_test();
        }
        let sequence = self.last_sequence();
        let durable = self.durable.load(Ordering::Acquire);
        if durable >= sequence {
            self.dirty_bytes = 0;
        } else if durable >= self.barrier_mark.0 {
            self.dirty_bytes = self.dirty_bytes.saturating_sub(self.barrier_mark.1);
        }
        self.barrier_mark = (sequence, self.dirty_bytes);
        if self.sync_worker_file.is_none() {
            let file = self.file.as_ref().ok_or_else(|| {
                PersistenceError::Failed("WAL-backed profile has no open segment".into())
            })?;
            let cloned = match file.try_clone() {
                Ok(cloned) => cloned,
                Err(error) => return self.fail(error),
            };
            self.sync_worker_file = Some(Arc::new(cloned));
        }
        self.sync_worker
            .as_ref()
            .expect("enabled sync worker")
            .submit(
                Arc::clone(self.sync_worker_file.as_ref().expect("sync file")),
                sequence,
            )
            .map(Some)
    }

    /// Every sync so far, the worker's and the shard thread's: `(count, nanoseconds)`.
    pub(crate) fn sync_totals(&self) -> (u64, u64) {
        let (count, nanos) = self.sync_worker_stats().unwrap_or((0, 0));

        (
            count.saturating_add(self.inline_syncs),
            nanos.saturating_add(self.inline_sync_nanos),
        )
    }

    pub(crate) fn sync_worker_stats(&self) -> Option<(u64, u64)> {
        self.sync_worker.as_ref().map(|worker| worker.stats())
    }

    pub fn compact(&mut self, covered_sequence: u64) -> Result<(), PersistenceError> {
        if let Some(error) = &self.failed {
            return Err(PersistenceError::Failed(error.clone()));
        }

        let floor = self.backlog_floor.load(Ordering::Acquire);
        let wanted = covered_sequence;
        let mut covered_sequence = covered_sequence.min(floor);

        if !self.profile.uses_wal() || wanted == 0 || self.file.is_none() {
            return Ok(());
        }

        if floor < wanted && self.replica_hold_exceeds_budget(floor, wanted)? {
            REPLICA_HOLDS_DROPPED.fetch_add(1, Ordering::Relaxed);
            covered_sequence = wanted;
        }

        if covered_sequence == 0 {
            return Ok(());
        }

        if self.last_sequence() <= covered_sequence
            && self.segment_first_sequence <= covered_sequence
        {
            self.rotate()?;
        }

        let directory = shard_wal_directory(&self.config.data_dir, self.shard_id);
        let current = segment_path(&directory, self.segment_first_sequence);
        let segments = discover_segments(&directory)?;

        let retained_from = self.cdc_retained_from(&segments)?;

        for (index, path) in segments.iter().enumerate() {
            if path == &current || index >= retained_from {
                continue;
            }

            let next_first = match segments.get(index + 1) {
                Some(next) => segment_first_sequence(next)?,
                None => self.segment_first_sequence,
            };

            let last_sequence = next_first.saturating_sub(1);

            if last_sequence <= covered_sequence {
                fs::remove_file(path)?;
            }
        }

        Ok(())
    }

    /// True when the segments kept only because a connected replica has not received them
    /// (sequences above `floor` up to `wanted`) are larger than `replica_retain_max_bytes`.
    fn replica_hold_exceeds_budget(
        &self,
        floor: u64,
        wanted: u64,
    ) -> Result<bool, PersistenceError> {
        let budget = self.config.replica_retain_max_bytes;

        if budget == 0 {
            return Ok(false);
        }

        let directory = shard_wal_directory(&self.config.data_dir, self.shard_id);
        let segments = discover_segments(&directory)?;
        let mut held = 0u64;

        for (index, path) in segments.iter().enumerate() {
            let next_first = match segments.get(index + 1) {
                Some(next) => segment_first_sequence(next)?,
                None => self.segment_first_sequence,
            };
            let last_sequence = next_first.saturating_sub(1);

            if last_sequence > floor && last_sequence <= wanted {
                held = held.saturating_add(fs::metadata(path)?.len());
            }
        }

        Ok(held > budget)
    }

    /// Index of the oldest segment the change-stream window keeps; `usize::MAX` (nothing
    /// held back) when `cdc_retain_bytes` is `0`.
    fn cdc_retained_from(&self, segments: &[PathBuf]) -> Result<usize, PersistenceError> {
        let budget = self.config.cdc_retain_bytes;

        if budget == 0 {
            return Ok(usize::MAX);
        }

        let mut kept = 0u64;
        let mut from = segments.len();

        for (index, path) in segments.iter().enumerate().rev() {
            kept = kept.saturating_add(fs::metadata(path)?.len());

            if kept > budget {
                break;
            }

            from = index;
        }

        Ok(from)
    }

    fn append(&mut self, mutation: Mutation<'_>) -> Result<(), PersistenceError> {
        self.append_as::<false>(mutation)
    }

    /// `THROUGH`: inside an `always` group the record is written now (after what is staged)
    /// instead of staged, so a failed write reaches the caller while it can still roll back.
    /// Its sync still waits for the group's commit. Other profiles stage it as before.
    fn append_as<const THROUGH: bool>(
        &mut self,
        mutation: Mutation<'_>,
    ) -> Result<(), PersistenceError> {
        if !self.profile.uses_wal() {
            return Ok(());
        }

        self.recover_if_failed()?;
        self.observe_sync_error()?;

        encode_borrowed_record_into(
            &mut self.record,
            self.shard_id,
            self.next_sequence,
            self.database,
            &mutation,
        )?;
        let record = std::mem::take(&mut self.record);

        if record.len() > self.limits.max_command_bytes {
            self.record = record;

            return Err(PersistenceError::Failed(
                "encoded WAL record exceeds configured command limit".into(),
            ));
        }

        let record_length = record.len() as u64;

        if WAL_HEADER_LEN as u64 + record_length > self.config.wal_segment_bytes {
            self.record = record;

            // The runtime matches this prefix to answer the client with a clear error.
            return Err(PersistenceError::Failed(format!(
                "encoded WAL record exceeds configured segment size ({record_length} bytes, \
                 wal_segment_bytes = {})",
                self.config.wal_segment_bytes
            )));
        }

        self.record = record;

        if self.file_length > WAL_HEADER_LEN as u64
            && self.file_length.saturating_add(record_length) > self.config.wal_segment_bytes
        {
            self.rotate()?;
        }

        let record = std::mem::take(&mut self.record);
        let written = if THROUGH && self.group > 0 && self.profile == DurabilityProfile::Always {
            self.publish_staged_io()
                .and_then(|()| self.write_file(&record))
        } else {
            self.write_record(&record)
        };

        self.record = record;
        self.record.clear();

        if let Err(error) = written {
            return self.fail(error);
        }

        self.file_length += record_length;
        self.dirty_bytes += record_length;
        self.group_bytes += record_length;

        let synced = match self.sync_open_segment() {
            Ok(synced) => synced,
            Err(error) => {
                self.forget_refused(record_length);
                return Err(error);
            }
        };

        self.note_appended(synced)?;

        if self.group == 0 && self.profile == DurabilityProfile::Everysec {
            self.poke_background_sync()?;
        }

        Ok(())
    }

    pub fn append_frame(&mut self, frame: &[u8]) -> Result<(), PersistenceError> {
        if !self.profile.uses_wal() {
            return Err(PersistenceError::Failed(
                "WAL frame append requires a WAL-backed profile".into(),
            ));
        }

        self.recover_if_failed()?;
        self.observe_sync_error()?;

        let record = decode_record(frame, self.shard_id, self.limits)?;

        if record.sequence != self.next_sequence {
            return Err(PersistenceError::Failed(
                "WAL frame sequence is not the next sequence".into(),
            ));
        }

        let record_length = frame.len() as u64;

        if WAL_HEADER_LEN as u64 + record_length > self.config.wal_segment_bytes {
            return Err(PersistenceError::Failed(
                "encoded WAL record exceeds configured segment size".into(),
            ));
        }

        if self.file_length > WAL_HEADER_LEN as u64
            && self.file_length.saturating_add(record_length) > self.config.wal_segment_bytes
        {
            self.rotate()?;
        }

        if let Err(error) = self.write_record(frame) {
            return self.fail(error);
        }

        self.file_length += record_length;
        self.dirty_bytes += record_length;
        self.group_bytes += record_length;

        let synced = match self.sync_open_segment() {
            Ok(synced) => synced,
            Err(error) => {
                self.forget_refused(record_length);
                return Err(error);
            }
        };

        self.note_appended(synced)?;

        if self.group == 0 && self.profile == DurabilityProfile::Everysec {
            self.poke_background_sync()?;
        }

        Ok(())
    }

    fn write_record(&mut self, bytes: &[u8]) -> io::Result<()> {
        if self.group > 0 {
            if !self.staged.is_empty() && self.staged.len() + bytes.len() > WAL_GROUP_BYTES {
                self.publish_staged_io()?;
            }

            if self.staged.is_empty() {
                // Every caller writes the record that will get `next_sequence`.
                self.staged_first_sequence = self.next_sequence;
            }
            self.staged.extend_from_slice(bytes);

            return Ok(());
        }

        self.write_file(bytes)
    }

    fn write_file(&mut self, bytes: &[u8]) -> io::Result<()> {
        let Some(file) = &mut self.file else {
            return Err(io::Error::other("WAL-backed profile has no open segment"));
        };
        let end = self.write_pos + bytes.len() as u64;

        // Preallocated *and zero-filled* space: a sync of blocks that already hold data needs no extent
        // conversion or size change, which made `fdatasync` ~2.4x faster (2.0 -> 0.8 ms per sync) on ext4.
        if self.prealloc {
            let cap = self.config.wal_segment_bytes.max(end);

            self.prefill.reserve(
                file,
                &mut self.prealloc_end,
                &mut self.prealloc,
                end,
                cap,
                self.write_pos,
            );
        }

        #[cfg(test)]
        if let Some(error) = injected_write_fault(&self.config.data_dir, self.shard_id) {
            self.prealloc_end = self.write_pos;
            return Err(error);
        }
        #[cfg(test)]
        count_write(&self.config.data_dir);
        let written = super::crash::hit_write(super::crash::CrashSite::Append, file, bytes);

        if written.is_ok() {
            self.write_pos = end;
        } else {
            // Keep whatever a failed write left behind (recovery classifies it); only a clean tail is trimmed.
            self.prealloc_end = self.write_pos;
        }

        written
    }

    fn publish_staged(&mut self) -> Result<(), PersistenceError> {
        if let Err(error) = self.publish_staged_io() {
            return self.fail(error);
        }

        Ok(())
    }

    fn publish_staged_io(&mut self) -> io::Result<()> {
        if self.staged.is_empty() {
            return Ok(());
        }

        // After a failed write the file cursor may sit past torn bytes; only recovery writes again.
        if let Some(message) = &self.failed {
            return Err(io::Error::other(message.clone()));
        }

        let staged = std::mem::take(&mut self.staged);
        let written = self.write_file(&staged);

        self.staged = staged;
        // The staged records already own their sequences (and are applied in memory): a failed
        // write keeps them so recovery writes them before anything newer.
        if written.is_ok() {
            self.staged.clear();
        }
        written
    }

    /// A single `always` record's sync failed. Every unsynced record before it belongs to a
    /// batch that has not synced yet and now gets an error too, so all are cut. With the
    /// worker the shard thread does not know what it synced: only this record goes.
    fn forget_refused(&mut self, record_length: u64) {
        let position = if self.sync_worker.is_none() {
            self.synced_pos
        } else {
            self.write_pos.saturating_sub(record_length)
        };

        self.forget_unsynced_since(position.min(self.write_pos));
    }

    /// The records from `position` on reached the file but their `always` sync failed, so
    /// their clients get an error: move the end back to `position` and cut them, so a
    /// restart does not revive writes that were refused. Never called with a `position`
    /// before the end of a successful sync.
    fn forget_unsynced_since(&mut self, position: u64) {
        let length = self.write_pos.saturating_sub(position);

        self.write_pos = position;
        self.file_length = self.file_length.saturating_sub(length);
        self.dirty_bytes = self.dirty_bytes.saturating_sub(length);
        self.group_bytes = self.group_bytes.saturating_sub(length);
        // If the file cannot be shortened, overwrite the records with zeros instead: replay
        // then sees an unwritten preallocated tail.
        self.prefill.finish(&mut self.prealloc_end);
        if let Some(file) = &self.file {
            if file.set_len(self.write_pos).is_ok() {
                self.prealloc_end = self.write_pos;
            } else {
                let zeros = vec![0u8; usize::try_from(length).unwrap_or(0)];
                let _ = write_all_at(file, &zeros, self.write_pos);
            }
        }
    }

    fn sync_open_segment(&mut self) -> Result<bool, PersistenceError> {
        if self.profile != DurabilityProfile::Always || self.group > 0 {
            return Ok(false);
        }

        self.fsync_file()?;
        Ok(true)
    }

    fn fsync_file(&mut self) -> Result<(), PersistenceError> {
        if let Some(worker) = &self.sync_worker {
            worker.drain()?;
        }
        super::crash::hit(super::crash::CrashSite::Sync);
        // The kernel reports a failed sync once: a later one may come back clean for pages
        // it dropped.
        if self.sync_failed {
            return Err(PersistenceError::Failed(
                self.failed
                    .clone()
                    .unwrap_or_else(|| "WAL fsync failed earlier".into()),
            ));
        }

        let started = Instant::now();
        let synced = {
            let Some(file) = &mut self.file else {
                return Err(PersistenceError::Failed(
                    "WAL-backed profile has no open segment".into(),
                ));
            };

            #[cfg(test)]
            if let Some(error) = injected_sync_fault(&self.config.data_dir, self.shard_id) {
                return self.fail_sync(error.to_string());
            }

            // Data-only sync: on the preallocated segment the size rarely changes, so no
            // metadata commit is forced per sync.
            file.sync_data()
        };

        self.inline_syncs = self.inline_syncs.saturating_add(1);
        self.inline_sync_nanos = self
            .inline_sync_nanos
            .saturating_add(started.elapsed().as_nanos() as u64);

        if let Err(error) = synced {
            return self.fail_sync(error.to_string());
        }

        // A sync on another descriptor of this file (a background or prefill sync) may have
        // taken the error for pages this one now reports as clean.
        self.observe_sync_error()?;
        self.synced_pos = self.write_pos;
        self.dirty_bytes = 0;
        self.last_sync = Instant::now();
        Ok(())
    }

    fn note_appended(&mut self, synced: bool) -> Result<(), PersistenceError> {
        let sequence = self.next_sequence;

        if self.group == 0 {
            self.published.store(sequence, Ordering::Release);
        }

        if synced {
            self.durable.store(sequence, Ordering::Release);
        }

        self.next_sequence = sequence
            .checked_add(1)
            .ok_or_else(|| PersistenceError::Failed("WAL sequence exhausted".into()))?;
        Ok(())
    }

    fn mark_published(&mut self) {
        let sequence = self.last_sequence();

        if sequence > 0 {
            self.published.store(sequence, Ordering::Release);
        }
    }

    fn observe_sync_error(&mut self) -> Result<(), PersistenceError> {
        if let Some(worker) = &self.sync_worker
            && let Err(error) = worker.check()
        {
            let message = match error {
                PersistenceError::Failed(message) => message,
                other => other.to_string(),
            };
            return self.fail_sync(message);
        }
        let message = self
            .sync_error
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .take();

        if let Some(message) = message {
            return self.fail_sync(message);
        }

        Ok(())
    }

    fn wait_background_sync(&self) {
        while self.sync_running.load(Ordering::Acquire) {
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
    }

    /// Retire the bytes a finished background sync covered. A failed one leaves them dirty and
    /// its error for `observe_sync_error`.
    fn settle_background_sync(&mut self) {
        if self.sync_in_flight == 0 || self.sync_running.load(Ordering::Acquire) {
            return;
        }

        if self
            .sync_error
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .is_some()
        {
            return;
        }

        self.dirty_bytes = self.dirty_bytes.saturating_sub(self.sync_in_flight);
        self.sync_in_flight = 0;
    }

    fn poke_background_sync(&mut self) -> Result<(), PersistenceError> {
        self.observe_sync_error()?;
        // After a failed sync a clean one proves nothing: `durable` stays where it was.
        if self.sync_failed {
            return Ok(());
        }
        self.settle_background_sync();

        if self.profile != DurabilityProfile::Everysec {
            return Ok(());
        }

        self.publish_staged()?;
        self.mark_published();

        if self.dirty_bytes == 0 || self.last_sync.elapsed() < self.config.sync_interval {
            return Ok(());
        }

        if self.sync_running.swap(true, Ordering::AcqRel) {
            return Ok(());
        }

        let Some(file) = &self.file else {
            self.sync_running.store(false, Ordering::Release);

            return Ok(());
        };

        let cloned = match file.try_clone() {
            Ok(file) => file,
            Err(error) => {
                self.sync_running.store(false, Ordering::Release);

                return self.fail(error);
            }
        };

        #[cfg(test)]
        let injected = super::crash::take_io_fault(super::crash::CrashSite::Sync);
        let sequence = self.last_sequence();
        let running = Arc::clone(&self.sync_running);
        let durable = Arc::clone(&self.durable);
        let sync_error = Arc::clone(&self.sync_error);

        self.sync_in_flight = self.dirty_bytes;
        self.last_sync = Instant::now();

        if let Err(error) = std::thread::Builder::new()
            .name("ruvio-wal-sync".into())
            .spawn(move || {
                let _clear = ClearSync(running);
                #[cfg(test)]
                let synced = match injected {
                    Some(error) => {
                        // Fail after the caller moved on, like a slow device.
                        std::thread::sleep(std::time::Duration::from_millis(30));
                        Err(error)
                    }
                    None => cloned.sync_data(),
                };
                #[cfg(not(test))]
                let synced = cloned.sync_data();

                if let Err(error) = synced {
                    *sync_error
                        .lock()
                        .unwrap_or_else(|poison| poison.into_inner()) = Some(error.to_string());

                    return;
                }

                durable.fetch_max(sequence, Ordering::Release);
            })
        {
            self.sync_running.store(false, Ordering::Release);
            self.sync_in_flight = 0;

            return self.fail(io::Error::other(error));
        }

        Ok(())
    }

    fn rotate(&mut self) -> Result<(), PersistenceError> {
        self.flush()?;
        self.shrink_to_written();
        self.sync_worker_file = None;
        let directory = shard_wal_directory(&self.config.data_dir, self.shard_id);
        let created = create_segment(&directory, self.shard_id, self.next_sequence);
        let (file, segment_id, first_sequence, file_length) = match created {
            Ok(created) => created,
            Err(error) => {
                // Enter the failed state so appends are refused until recovery opens the segment.
                self.failed = Some(error.to_string());
                return Err(error);
            }
        };

        self.file = Some(file);
        self.segment_id = segment_id;
        self.segment_first_sequence = first_sequence;
        self.file_length = file_length;
        self.write_pos = file_length;
        self.synced_pos = file_length;
        self.prealloc_end = file_length;
        self.prefill.reset();
        Ok(())
    }

    /// Drop the preallocated, never-written tail so closed segments have their exact size.
    fn shrink_to_written(&mut self) {
        // A fill still running would write past the new end.
        self.prefill.finish(&mut self.prealloc_end);

        if self.prealloc_end > self.write_pos {
            if let Some(file) = &self.file {
                let _ = file.set_len(self.write_pos);
            }
            self.prealloc_end = self.write_pos;
        }
    }

    /// After a write or sync error the log refuses appends. Once the disk accepts a clean
    /// start again (space freed, transient error gone), drop the torn tail and continue in a
    /// fresh segment, so writes resume without a restart. Tried at most twice a second.
    fn recover_if_failed(&mut self) -> Result<(), PersistenceError> {
        let Some(message) = self.failed.clone() else {
            return Ok(());
        };

        // A sync still running, or one that failed after the write error, may have taken
        // the error for pages recovery's own sync would then report as clean: settle them
        // first and treat any error they left as a failed sync.
        self.wait_background_sync();
        self.prefill.finish(&mut self.prealloc_end);
        if !self.sync_failed
            && let Err(error) = self.observe_sync_error()
        {
            return Err(error);
        }

        // Recovery would sync and continue after bytes a failed sync may have lost.
        if self.sync_failed {
            return Err(PersistenceError::Failed(message));
        }

        if self
            .last_recovery
            .is_some_and(|at| at.elapsed() < std::time::Duration::from_millis(500))
        {
            return Err(PersistenceError::Failed(message));
        }

        self.last_recovery = Some(Instant::now());

        let mut sync_lost = None;
        let recovered = (|| -> Result<(), PersistenceError> {
            self.sync_worker_file = None;

            if let Some(file) = &mut self.file {
                // Drop torn bytes and put the cursor back: a failed write may have moved it.
                // Only bytes past `write_pos` go; they never reached a successful write.
                file.set_len(self.write_pos)?;
                file.seek(SeekFrom::Start(self.write_pos))?;
                #[cfg(test)]
                let synced = match super::crash::take_io_fault(super::crash::CrashSite::Sync) {
                    Some(error) => Err(error),
                    None => file.sync_data(),
                };
                #[cfg(not(test))]
                let synced = file.sync_data();
                if let Err(error) = synced {
                    sync_lost = Some(error.to_string());
                    return Err(error.into());
                }
            }

            self.prealloc_end = self.write_pos;
            self.prefill.reset();

            // Records staged before the failure keep their sequences and are written first.
            let resume_at = if self.staged.is_empty() {
                self.next_sequence
            } else {
                self.staged_first_sequence
            };

            if self.write_pos > WAL_HEADER_LEN as u64 {
                let directory = shard_wal_directory(&self.config.data_dir, self.shard_id);
                let (file, segment_id, first_sequence, file_length) =
                    create_segment(&directory, self.shard_id, resume_at)?;

                self.file = Some(file);
                self.segment_id = segment_id;
                self.segment_first_sequence = first_sequence;
                self.write_pos = file_length;
                self.synced_pos = file_length;
                self.prealloc_end = file_length;
            } else if self.segment_first_sequence != resume_at {
                return Err(PersistenceError::Failed(
                    "WAL recovery found an empty segment at the wrong sequence".into(),
                ));
            }

            // Everything written before the failure was synced above.
            self.file_length = self.write_pos;
            self.dirty_bytes = 0;

            if !self.staged.is_empty() {
                let staged = std::mem::take(&mut self.staged);
                let written = self.write_file(&staged);

                self.staged = staged;
                written?;
                self.file_length += self.staged.len() as u64;
                self.dirty_bytes = self.staged.len() as u64;
                self.staged.clear();
            }

            Ok(())
        })();

        match recovered {
            Ok(()) => {
                self.observe_sync_error()?;
                self.failed = None;
                self.synced_pos = self.file_length - self.dirty_bytes;
                self.sync_in_flight = 0;
                self.group_bytes = 0;
                // Rewritten `always` records reach replicas once a sync covers them.
                if self.dirty_bytes == 0
                    || self.profile != DurabilityProfile::Always
                    || self.sync_worker.is_some()
                {
                    self.mark_published();
                }
                tracing::warn!(previous = %message, "WAL writes resumed after an earlier failure");

                Ok(())
            }

            Err(_) => match sync_lost {
                Some(error) => self.fail_sync(error),
                None => Err(PersistenceError::Failed(message)),
            },
        }
    }

    /// Enter the sticky failed state after an `fsync` error; see `sync_failed`.
    fn fail_sync<T>(&mut self, error: String) -> Result<T, PersistenceError> {
        if !self.sync_failed {
            tracing::error!(
                shard = self.shard_id,
                %error,
                "WAL fsync failed; writes are refused until restart"
            );
        }
        self.sync_failed = true;
        self.failed = Some(error.clone());
        // Also when another descriptor's sync (the preallocation fill) failed and this is
        // only where the error is picked up: the unsynced records' clients are refused.
        self.cut_unsynced_always_records();
        Err(PersistenceError::Failed(error))
    }

    fn fail<T>(&mut self, error: io::Error) -> Result<T, PersistenceError> {
        let message = error.to_string();

        self.failed = Some(message.clone());
        Err(PersistenceError::Failed(message))
    }
}

impl Drop for WalSink {
    fn drop(&mut self) {
        self.wait_background_sync();
        self.shrink_to_written();
    }
}

impl MutationSink for WalSink {
    type Error = PersistenceError;

    fn record(&mut self, mutation: Mutation<'_>) -> Result<(), Self::Error> {
        self.append(mutation)
    }

    fn record_now(&mut self, mutation: Mutation<'_>) -> Result<(), Self::Error> {
        self.append_as::<true>(mutation)
    }

    fn select_database(&mut self, database: u8) {
        self.database = database;
    }

    fn persists(&self) -> bool {
        self.profile.uses_wal()
    }

    fn written_sequence(&self) -> Option<u64> {
        self.profile.uses_wal().then(|| self.last_sequence())
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum FrameRead {
    Frames(Vec<Vec<u8>>),
    Gap,
    Ahead,
    Local,
}

#[cfg(test)]
pub(crate) fn read_bounded_history(
    data_dir: &Path,
    shard_id: u32,
    after_sequence: u64,
    through_sequence: u64,
    limits: ProtocolLimits,
    max_records: u64,
    max_bytes: u64,
) -> Result<FrameRead, PersistenceError> {
    if through_sequence.saturating_sub(after_sequence) > max_records {
        return Ok(FrameRead::Local);
    }
    if after_sequence == through_sequence {
        return Ok(FrameRead::Frames(Vec::new()));
    }
    let directory = shard_wal_directory(data_dir, shard_id);
    if !directory.is_dir() {
        return Ok(FrameRead::Gap);
    }
    let segments = discover_segments(&directory)?;
    let start = first_usable_segment_index(&segments, after_sequence)?;
    let mut bytes = 0u64;
    for path in segments.iter().skip(start) {
        if segment_first_sequence(path)? > through_sequence {
            break;
        }
        bytes = bytes.saturating_add(fs::metadata(path)?.len());
        if bytes > max_bytes {
            return Ok(FrameRead::Local);
        }
    }
    read_frames(data_dir, shard_id, after_sequence, through_sequence, limits)
}

pub fn read_frames(
    data_dir: &Path,
    shard_id: u32,
    after_sequence: u64,
    through_sequence: u64,
    limits: ProtocolLimits,
) -> Result<FrameRead, PersistenceError> {
    read_frames_chunk(
        data_dir,
        shard_id,
        after_sequence,
        through_sequence,
        limits,
        usize::MAX,
    )
}

/// Return a contiguous prefix up to the byte budget (or one oversized frame).
/// The caller advances by the last returned sequence before requesting another chunk.
pub(crate) fn read_frames_chunk(
    data_dir: &Path,
    shard_id: u32,
    after_sequence: u64,
    through_sequence: u64,
    limits: ProtocolLimits,
    max_bytes: usize,
) -> Result<FrameRead, PersistenceError> {
    read_frames_resuming(
        &mut FrameCursor::default(),
        data_dir,
        shard_id,
        after_sequence,
        through_sequence,
        limits,
        max_bytes,
    )
}

/// Where a reader that follows the log stopped: the segment, the byte offset just past
/// the last record it returned, and the sequence that record's successor carries. A
/// chunk that starts there seeks straight to it instead of decoding the segment again
/// from its header, so following a long segment costs its length once, not per chunk.
#[derive(Debug, Default)]
pub(crate) struct FrameCursor {
    position: Option<(PathBuf, u64)>,
    next_sequence: u64,
}

/// [`read_frames_chunk`] for a reader that keeps a [`FrameCursor`] between chunks. Any
/// request that does not continue where the cursor stopped, or whose segment is gone,
/// falls back to scanning from the first usable segment.
pub(crate) fn read_frames_resuming(
    cursor: &mut FrameCursor,
    data_dir: &Path,
    shard_id: u32,
    after_sequence: u64,
    through_sequence: u64,
    limits: ProtocolLimits,
    max_bytes: usize,
) -> Result<FrameRead, PersistenceError> {
    if after_sequence > through_sequence {
        return Ok(FrameRead::Ahead);
    }

    if after_sequence == through_sequence {
        return Ok(FrameRead::Frames(Vec::new()));
    }

    let directory = shard_wal_directory(data_dir, shard_id);

    if !directory.is_dir() {
        return Ok(FrameRead::Gap);
    }

    let segments = discover_segments(&directory)?;

    if segments.is_empty() {
        return Ok(FrameRead::Gap);
    }

    let mut frames = Vec::new();
    let mut collected_bytes = 0usize;
    let mut expected_sequence = 0u64;
    let mut saw_segment = false;
    let mut resume = cursor
        .position
        .take()
        .filter(|_| cursor.next_sequence == after_sequence.saturating_add(1))
        .and_then(|(path, offset)| {
            let index = segments.iter().position(|segment| *segment == path)?;
            Some((index, offset))
        });
    let start = match resume {
        Some((index, _)) => index,
        None => first_usable_segment_index(&segments, after_sequence)?,
    };
    // Just past the last record consumed, so the cursor can be left there on return.
    let mut position: Option<(usize, u64)> = None;

    'segments: for (index, path) in segments.iter().enumerate().skip(start) {
        let is_last = index + 1 == segments.len();
        let file = File::open(path)?;
        let segment_length = file.metadata()?.len();
        let mut file = io::BufReader::with_capacity(FRAME_READ_BUFFER, file);

        if segment_length < WAL_HEADER_LEN as u64 {
            if !is_last {
                return Err(PersistenceError::Corruption(
                    "truncated WAL segment header".into(),
                ));
            }

            if !saw_segment {
                return Ok(FrameRead::Gap);
            }

            return Ok(FrameRead::Local);
        }

        let mut header_bytes = [0; WAL_HEADER_LEN];

        file.read_exact(&mut header_bytes).map_err(|error| {
            if error.kind() == io::ErrorKind::UnexpectedEof {
                PersistenceError::Corruption("truncated WAL segment header".into())
            } else {
                error.into()
            }
        })?;

        let header = decode_segment_header(&header_bytes)?;

        if header.shard_id != shard_id {
            return Err(PersistenceError::Corruption(
                "WAL segment shard mismatch".into(),
            ));
        }

        let file_first_sequence = segment_first_sequence(path)?;

        if header.first_sequence != file_first_sequence {
            return Err(PersistenceError::Corruption(
                "WAL segment sequence discontinuity".into(),
            ));
        }

        let mut offset = WAL_HEADER_LEN as u64;

        match resume.take() {
            Some((_, resume_offset))
                if (WAL_HEADER_LEN as u64..=segment_length).contains(&resume_offset) =>
            {
                file.seek(SeekFrom::Start(resume_offset))?;
                offset = resume_offset;
                expected_sequence = after_sequence.saturating_add(1);
                saw_segment = true;
            }

            // The segment shrank under the cursor: rescan from the start.
            Some(_) => {
                return read_frames_resuming(
                    cursor,
                    data_dir,
                    shard_id,
                    after_sequence,
                    through_sequence,
                    limits,
                    max_bytes,
                );
            }

            None => {
                if !saw_segment {
                    if header.first_sequence > after_sequence.saturating_add(1) {
                        return Ok(FrameRead::Gap);
                    }

                    expected_sequence = header.first_sequence;
                    saw_segment = true;
                }

                if header.first_sequence != expected_sequence {
                    return Ok(FrameRead::Local);
                }
            }
        }

        position = Some((index, offset));

        loop {
            if expected_sequence > through_sequence {
                break 'segments;
            }

            let mut length_bytes = [0; 4];

            match read_partial(&mut file, &mut length_bytes)? {
                0 => break,
                4 => {}
                _ if is_last => return Ok(FrameRead::Local),

                _ => {
                    return Err(PersistenceError::Corruption(
                        "torn record length in non-final WAL segment".into(),
                    ));
                }
            }

            let record_length = u32::from_le_bytes(length_bytes) as usize;

            if record_length == 0 {
                // Preallocated tail of the live segment: end of its data.
                break;
            }

            // Not the configured command limit: records written under a larger one (or by a
            // primary with one) must still replay. A record never outgrows its segment file.
            if record_length < RECORD_MIN_LEN {
                return Err(PersistenceError::Corruption(
                    "invalid WAL record length".into(),
                ));
            }

            let mut frame = Vec::new();
            // Longer than its segment: torn or corrupt, and never worth the allocation.
            let body_read = if record_length as u64 <= segment_length {
                frame.resize(record_length, 0);
                frame[..4].copy_from_slice(&length_bytes);
                read_partial(&mut file, &mut frame[4..])?
            } else {
                0
            };

            if body_read != record_length - 4 {
                if !is_last {
                    return Err(PersistenceError::Corruption(
                        "torn record in non-final WAL segment".into(),
                    ));
                }

                return Ok(FrameRead::Local);
            }

            match decode_record(&frame, shard_id, limits) {
                Ok(record) => {
                    if record.sequence != expected_sequence {
                        return Ok(FrameRead::Local);
                    }

                    if record.sequence > after_sequence {
                        collected_bytes = collected_bytes.saturating_add(frame.len());
                        frames.push(frame);
                    }

                    expected_sequence = expected_sequence.checked_add(1).ok_or_else(|| {
                        PersistenceError::Corruption("WAL sequence overflow".into())
                    })?;
                    offset += record_length as u64;
                    position = Some((index, offset));
                    if collected_bytes >= max_bytes && !frames.is_empty() {
                        leave_cursor(cursor, &segments, position, expected_sequence);
                        return Ok(FrameRead::Frames(frames));
                    }
                }

                Err(PersistenceError::Corruption(message))
                    if message == "WAL record checksum mismatch" =>
                {
                    return Ok(FrameRead::Local);
                }

                Err(error) => return Err(error),
            }
        }
    }

    if !saw_segment || expected_sequence <= through_sequence {
        if !saw_segment {
            return Ok(FrameRead::Gap);
        }

        return Ok(FrameRead::Local);
    }

    leave_cursor(cursor, &segments, position, expected_sequence);
    Ok(FrameRead::Frames(frames))
}

const FRAME_READ_BUFFER: usize = 64 * 1024;

fn leave_cursor(
    cursor: &mut FrameCursor,
    segments: &[PathBuf],
    position: Option<(usize, u64)>,
    next_sequence: u64,
) {
    cursor.position = position.map(|(index, offset)| (segments[index].clone(), offset));
    cursor.next_sequence = next_sequence;
}

struct Segment {
    path: PathBuf,
    segment_id: u64,
    first_sequence: u64,
    /// The sequence the next record appended to this segment must carry.
    next_sequence: u64,
    /// The format minor its header declares.
    minor: u16,
}

fn shard_wal_directory(data_dir: &Path, shard_id: u32) -> PathBuf {
    data_dir.join("wal").join(format!("shard-{shard_id:04}"))
}

fn segment_path(directory: &Path, first_sequence: u64) -> PathBuf {
    directory.join(format!("{first_sequence:020}.wal"))
}

fn create_segment(
    directory: &Path,
    shard_id: u32,
    first_sequence: u64,
) -> Result<(File, u64, u64, u64), PersistenceError> {
    fs::create_dir_all(directory)?;
    let segment_id = first_sequence;
    let path = segment_path(directory, first_sequence);
    let mut file = match OpenOptions::new()
        .create_new(true)
        .read(true)
        .write(true)
        .open(&path)
    {
        Ok(file) => file,
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
            // A leftover of an earlier attempt that failed before any record was written (its
            // cleanup failed too) holds no data: reuse it. Anything longer is a real segment.
            let file = OpenOptions::new().read(true).write(true).open(&path)?;

            if file.metadata()?.len() > WAL_HEADER_LEN as u64 {
                return Err(error.into());
            }
            file.set_len(0)?;
            file
        }
        Err(error) => return Err(error.into()),
    };
    let header = encode_segment_header(shard_id, segment_id, first_sequence);
    let initialized = (|| -> Result<(), PersistenceError> {
        super::crash::hit_write(super::crash::CrashSite::Rotate, &mut file, &header)?;
        file.sync_data()?;
        // The new directory entry must be durable too, or a power cut can lose the whole segment.
        super::snapshot::sync_directory(directory)
    })();

    if let Err(error) = initialized {
        // Do not leave a partial segment behind to block the next attempt with EEXIST.
        drop(file);
        let _ = fs::remove_file(&path);
        return Err(error);
    }
    Ok((file, segment_id, first_sequence, WAL_HEADER_LEN as u64))
}

fn discover_segments(directory: &Path) -> Result<Vec<PathBuf>, PersistenceError> {
    let mut segments = Vec::new();

    for entry in fs::read_dir(directory)? {
        let entry = entry?;
        let path = entry.path();

        if path.extension().and_then(|value| value.to_str()) != Some("wal") {
            continue;
        }

        let Some(stem) = path.file_stem().and_then(|value| value.to_str()) else {
            return Err(PersistenceError::Corruption(
                "non-UTF-8 WAL segment name".into(),
            ));
        };

        stem.parse::<u64>().map_err(|_| {
            PersistenceError::Corruption(format!("invalid WAL segment name: {stem}"))
        })?;

        segments.push(path);
    }

    segments.sort();
    Ok(segments)
}

fn segment_first_sequence(path: &Path) -> Result<u64, PersistenceError> {
    path.file_stem()
        .and_then(|value| value.to_str())
        .and_then(|value| value.parse().ok())
        .ok_or_else(|| PersistenceError::Corruption("invalid WAL filename".into()))
}

fn first_usable_segment_index(
    paths: &[PathBuf],
    after_sequence: u64,
) -> Result<usize, PersistenceError> {
    if paths.is_empty() {
        return Ok(0);
    }

    let target = after_sequence.saturating_add(1).max(1);

    for (index, path) in paths.iter().enumerate().rev() {
        if segment_first_sequence(path)? <= target {
            return Ok(index);
        }
    }

    Ok(0)
}

pub(crate) fn inspect_shard_wal(
    data_dir: &Path,
    shard_id: u32,
    limits: ProtocolLimits,
    maximum_segment_bytes: u64,
    after_sequence: u64,
) -> Result<(RecoveredMutations, RecoveryReport), PersistenceError> {
    let directory = shard_wal_directory(data_dir, shard_id);

    if !directory.exists() {
        return Ok((
            Vec::new(),
            RecoveryReport {
                last_sequence: after_sequence,
                ..RecoveryReport::default()
            },
        ));
    }

    let segments = discover_segments(&directory)?;
    let (mutations, report, _) = recover_segments(
        &segments,
        shard_id,
        limits,
        maximum_segment_bytes,
        after_sequence,
        true,
    )?;

    Ok((mutations, report))
}

pub(crate) fn discover_wal_shards(data_dir: &Path) -> Result<Vec<u32>, PersistenceError> {
    let directory = data_dir.join("wal");
    let entries = match fs::read_dir(&directory) {
        Ok(entries) => entries,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => return Err(error.into()),
    };

    let mut shards = Vec::new();

    for entry in entries {
        let entry = entry?;

        if !entry.file_type()?.is_dir() {
            continue;
        }

        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            continue;
        };

        let Some(id) = name.strip_prefix("shard-") else {
            continue;
        };

        let Ok(shard_id) = id.parse::<u32>() else {
            return Err(PersistenceError::Corruption(format!(
                "invalid WAL shard directory: {name}"
            )));
        };

        shards.push(shard_id);
    }

    shards.sort_unstable();
    shards.dedup();
    Ok(shards)
}

fn recover_segments(
    paths: &[PathBuf],
    shard_id: u32,
    limits: ProtocolLimits,
    maximum_segment_bytes: u64,
    after_sequence: u64,
    read_only: bool,
) -> Result<(RecoveredMutations, RecoveryReport, Option<Segment>), PersistenceError> {
    let start = first_usable_segment_index(paths, after_sequence)?;
    let paths = paths.get(start..).unwrap_or(&[]);
    let mut mutations = Vec::new();
    let mut report = RecoveryReport {
        last_sequence: after_sequence,
        ..RecoveryReport::default()
    };

    let mut expected_sequence = 0u64;
    let mut saw_segment = false;
    let mut last_segment = None;

    for (index, path) in paths.iter().enumerate() {
        let is_last = index + 1 == paths.len();
        let mut file = if read_only {
            File::open(path)?
        } else {
            OpenOptions::new().read(true).write(is_last).open(path)?
        };

        let physical_length = file.metadata()?.len();

        if physical_length < WAL_HEADER_LEN as u64 {
            drop(file);

            if !is_last {
                return Err(PersistenceError::Corruption(
                    "truncated WAL segment header".into(),
                ));
            }

            report.torn_tail_bytes += physical_length;

            if !read_only {
                fs::remove_file(path)?;
            }

            break;
        }

        if physical_length > maximum_segment_bytes {
            return Err(PersistenceError::Corruption(format!(
                "WAL segment {} exceeds configured size",
                path.display()
            )));
        }

        let mut header_bytes = [0; WAL_HEADER_LEN];

        file.read_exact(&mut header_bytes).map_err(|error| {
            if error.kind() == io::ErrorKind::UnexpectedEof {
                PersistenceError::Corruption("truncated WAL segment header".into())
            } else {
                error.into()
            }
        })?;

        // A power cut after `create_segment` wrote the header but before its sync completed can
        // leave the file at its length with zeros for content. Nothing in it was acknowledged
        // (records are appended only after the header is synced), so like a short header on the
        // final segment it is a torn tail. A non-zero byte after it, or a later segment, means
        // the header had been synced, so either stays corruption.
        if is_last
            && header_bytes.iter().all(|byte| *byte == 0)
            && rest_is_zero(&mut file, WAL_HEADER_LEN as u64)?
        {
            drop(file);
            report.torn_tail_bytes += physical_length;

            if !read_only {
                fs::remove_file(path)?;
            }

            break;
        }

        let header = decode_segment_header(&header_bytes)?;

        if header.shard_id != shard_id {
            return Err(PersistenceError::Corruption(
                "WAL segment shard mismatch".into(),
            ));
        }

        let file_first_sequence = segment_first_sequence(path)?;

        if header.first_sequence != file_first_sequence {
            return Err(PersistenceError::Corruption(
                "WAL segment sequence discontinuity".into(),
            ));
        }

        if !saw_segment {
            if after_sequence == 0 && header.first_sequence != 1 {
                return Err(PersistenceError::Corruption(
                    "WAL segment sequence discontinuity".into(),
                ));
            }

            if after_sequence > 0 && header.first_sequence > after_sequence.saturating_add(1) {
                return Err(PersistenceError::Corruption(
                    "WAL segment sequence discontinuity".into(),
                ));
            }

            expected_sequence = header.first_sequence;
            saw_segment = true;
        }

        if header.first_sequence != expected_sequence {
            return Err(PersistenceError::Corruption(
                "WAL segment sequence discontinuity".into(),
            ));
        }

        let mut valid_length = WAL_HEADER_LEN as u64;

        loop {
            let record_start = valid_length;
            let mut length_bytes = [0; 4];

            match read_partial(&mut file, &mut length_bytes)? {
                0 => break,
                4 => {}
                _count if is_last => {
                    report.torn_tail_bytes += physical_length - record_start;

                    if !read_only {
                        file.set_len(record_start)?;
                        file.seek(SeekFrom::End(0))?;
                    }

                    break;
                }

                _ => {
                    return Err(PersistenceError::Corruption(
                        "torn record length in non-final WAL segment".into(),
                    ));
                }
            }

            let record_length = u32::from_le_bytes(length_bytes) as usize;

            if record_length == 0 && rest_is_zero(&mut file, record_start)? {
                // Unwritten preallocated tail, not a torn record.
                if is_last && !read_only {
                    file.set_len(record_start)?;
                    file.seek(SeekFrom::End(0))?;
                }

                break;
            }

            // A zeroed length with data after it in the last segment: pages lost behind a
            // failed sync or a torn write leave such a tail. It is cut only when no intact
            // record continuing the log follows; otherwise records that were synced would be
            // dropped silently, so startup stops and says where.
            if record_length == 0 && is_last {
                if let Some((offset, sequence)) = intact_record_after(
                    &mut file,
                    record_start,
                    physical_length,
                    shard_id,
                    limits,
                    expected_sequence,
                )? {
                    return Err(PersistenceError::Corruption(format!(
                        "WAL segment {} has a zeroed record length at offset {record_start}, \
                         but an intact record (sequence {sequence}) follows at offset \
                         {offset}; refusing to drop it. To start without the records from \
                         offset {record_start} on, move the segment aside or truncate it \
                         there (truncate -s {record_start} <segment>)",
                        path.display()
                    )));
                }
                tracing::warn!(
                    segment = %path.display(),
                    offset = record_start,
                    bytes = physical_length - record_start,
                    "WAL ends at a zeroed record length; dropping the bytes after it"
                );
                report.torn_tail_bytes += physical_length - record_start;

                if !read_only {
                    file.set_len(record_start)?;
                    file.seek(SeekFrom::End(0))?;
                }

                break;
            }

            // Not the configured command limit: records written under a larger one (or by a
            // primary with one) must still replay. A record never outgrows its segment file.
            if record_length < RECORD_MIN_LEN {
                return Err(PersistenceError::Corruption(
                    "invalid WAL record length".into(),
                ));
            }

            let mut frame = Vec::new();
            // Longer than its segment: torn or corrupt, and never worth the allocation.
            let body_read = if record_length as u64 <= physical_length {
                frame.resize(record_length, 0);
                frame[..4].copy_from_slice(&length_bytes);
                read_partial(&mut file, &mut frame[4..])?
            } else {
                0
            };

            if body_read != record_length - 4 {
                if !is_last {
                    return Err(PersistenceError::Corruption(
                        "torn record in non-final WAL segment".into(),
                    ));
                }

                report.torn_tail_bytes += physical_length - record_start;

                if !read_only {
                    file.set_len(record_start)?;
                    file.seek(SeekFrom::End(0))?;
                }

                break;
            }

            match decode_record(&frame, shard_id, limits) {
                Ok(record) => {
                    if record.sequence != expected_sequence {
                        return Err(PersistenceError::Corruption(
                            "WAL record sequence discontinuity".into(),
                        ));
                    }

                    expected_sequence = expected_sequence.checked_add(1).ok_or_else(|| {
                        PersistenceError::Corruption("WAL sequence overflow".into())
                    })?;

                    report.last_sequence = record.sequence;

                    if record.sequence > after_sequence {
                        report.records_replayed += 1;
                        mutations.push((record.database, record.mutation));
                    }

                    valid_length += record_length as u64;
                }

                // Preallocated zeros after the record count as end of file, so a bad last
                // record is still the final physical record: a torn tail, not mid-log
                // corruption.
                Err(PersistenceError::Corruption(_))
                    if is_last
                        && (record_start + record_length as u64 == physical_length
                            || rest_is_zero(&mut file, record_start + record_length as u64)
                                .unwrap_or(false)) =>
                {
                    report.torn_tail_bytes += physical_length - record_start;

                    if !read_only {
                        file.set_len(record_start)?;
                        file.seek(SeekFrom::End(0))?;
                    }

                    break;
                }

                Err(error) => return Err(error),
            }
        }

        last_segment = Some(Segment {
            path: path.clone(),
            segment_id: header.segment_id,
            first_sequence: header.first_sequence,
            next_sequence: expected_sequence,
            minor: header.minor,
        });
    }

    Ok((mutations, report, last_segment))
}

struct PrefillJob {
    file: File,
    start: u64,
    length: u64,
    sync_error: Option<Arc<Mutex<Option<String>>>>,
    #[cfg(test)]
    fail_sync: bool,
}

#[derive(Default)]
struct PrefillState {
    job: Option<PrefillJob>,
    /// Outcome of the last job; `None` while it runs.
    result: Option<bool>,
    stop: bool,
}

struct PrefillShared {
    state: Mutex<PrefillState>,
    changed: std::sync::Condvar,
}

/// Extends a segment ahead of the write cursor and fills the new space with zeros on a helper thread,
/// so the shard never waits for it in steady state. One long-lived thread and one shared slot:
/// asking for a chunk allocates nothing on the shard's write path.
#[derive(Default)]
struct Prefill {
    shared: Option<Arc<PrefillShared>>,
    /// Target size of the chunk being filled, if one is.
    pending: Option<u64>,
    /// Where a failed sync of a fill is reported: it is a sync of the live segment, so it
    /// also covers record pages (see `WalSink::sync_failed`).
    sync_error: Option<Arc<Mutex<Option<String>>>>,
}

impl Drop for Prefill {
    fn drop(&mut self) {
        if let Some(shared) = &self.shared {
            shared.state.lock().unwrap_or_else(|e| e.into_inner()).stop = true;
            shared.changed.notify_all();
        }
    }
}

impl Prefill {
    fn reporting_to(sync_error: Arc<Mutex<Option<String>>>) -> Self {
        Self {
            shared: None,
            pending: None,
            sync_error: Some(sync_error),
        }
    }

    /// Makes `[.., end)` ready (blocking only if the helper lags or this is the first chunk) and
    /// asks for the next chunk once less than `WAL_PREALLOC_AHEAD` is left.
    fn reserve(
        &mut self,
        file: &File,
        prealloc_end: &mut u64,
        enabled: &mut bool,
        end: u64,
        cap: u64,
        write_pos: u64,
    ) {
        self.poll(prealloc_end, enabled, false);

        if end > *prealloc_end && *enabled {
            self.poll(prealloc_end, enabled, true);
        }

        if end > *prealloc_end && *enabled {
            self.request(file, *prealloc_end, end, cap, enabled);
            self.poll(prealloc_end, enabled, true);
        }

        if *enabled
            && self.pending.is_none()
            && *prealloc_end < cap
            && prealloc_end.saturating_sub(write_pos) < WAL_PREALLOC_AHEAD
        {
            self.request(file, *prealloc_end, *prealloc_end + 1, cap, enabled);
        }
    }

    fn request(&mut self, file: &File, start: u64, min_end: u64, cap: u64, enabled: &mut bool) {
        let target = start
            .max(min_end)
            .saturating_add(WAL_PREALLOC_CHUNK)
            .min(cap)
            .max(min_end);
        // A descriptor of its own (not a `dup`, which shares the error cursor): an error the
        // fill's sync reports is then also reported to the shard's next sync of the file.
        let Ok(clone) = reopen(file) else {
            *enabled = false;
            return;
        };

        if self.shared.is_none() {
            let shared = Arc::new(PrefillShared {
                state: Mutex::new(PrefillState::default()),
                changed: std::sync::Condvar::new(),
            });
            let worker = Arc::clone(&shared);

            if std::thread::Builder::new()
                .name("ruvio-wal-prefill".into())
                .spawn(move || prefill_thread(&worker))
                .is_err()
            {
                *enabled = false;
                return;
            }

            self.shared = Some(shared);
        }

        let Some(shared) = &self.shared else {
            return;
        };
        let mut state = shared.state.lock().unwrap_or_else(|e| e.into_inner());

        state.job = Some(PrefillJob {
            file: clone,
            start,
            length: target - start,
            sync_error: self.sync_error.clone(),
            #[cfg(test)]
            fail_sync: super::crash::take_io_fault(super::crash::CrashSite::PrefillSync).is_some(),
        });
        state.result = None;
        shared.changed.notify_all();
        self.pending = Some(target);
    }

    fn poll(&mut self, prealloc_end: &mut u64, enabled: &mut bool, block: bool) {
        let (Some(target), Some(shared)) = (self.pending, &self.shared) else {
            return;
        };
        let mut state = shared.state.lock().unwrap_or_else(|e| e.into_inner());

        let result = loop {
            if let Some(result) = state.result.take() {
                break result;
            }

            if !block {
                return;
            }

            state = shared
                .changed
                .wait(state)
                .unwrap_or_else(|e| e.into_inner());
        };

        drop(state);
        self.pending = None;

        if result {
            *prealloc_end = target;
        } else {
            // Not supported here or the disk refused: write as the file grows, like before.
            *enabled = false;
        }
    }

    /// Waits for a running fill so the caller may truncate the file.
    fn finish(&mut self, prealloc_end: &mut u64) {
        let mut enabled = true;

        self.poll(prealloc_end, &mut enabled, true);
    }

    fn reset(&mut self) {
        self.pending = None;
    }
}

fn prefill_thread(shared: &PrefillShared) {
    let zeros = vec![0u8; 1 << 20];

    loop {
        let job = {
            let mut state = shared.state.lock().unwrap_or_else(|e| e.into_inner());

            loop {
                if state.stop {
                    return;
                }

                if let Some(job) = state.job.take() {
                    break job;
                }

                state = shared
                    .changed
                    .wait(state)
                    .unwrap_or_else(|e| e.into_inner());
            }
        };
        let ok = match fill_with_zeros(&job, &zeros) {
            Ok(filled) => filled,
            Err(error) => {
                if let Some(sync_error) = &job.sync_error {
                    sync_error
                        .lock()
                        .unwrap_or_else(|e| e.into_inner())
                        .get_or_insert(error);
                }
                false
            }
        };
        let mut state = shared.state.lock().unwrap_or_else(|e| e.into_inner());

        state.result = Some(ok);
        shared.changed.notify_all();
    }
}

/// `fallocate`, then really write zeros so the blocks are "written" extents, then sync them once.
/// `Ok(false)`: preallocation is unsupported or refused. `Err`: the sync failed.
fn fill_with_zeros(job: &PrefillJob, zeros: &[u8]) -> Result<bool, String> {
    let (file, start, length) = (&job.file, job.start, job.length);

    if !preallocate(file, start, length) {
        return Ok(false);
    }

    // Unwritten (fallocate-only) extents make the first append's fdatasync wait on an ext4
    // extent-conversion commit, so the blocks are written here on the helper thread.
    let mut done = 0u64;

    while done < length {
        let piece = usize::try_from((length - done).min(zeros.len() as u64)).unwrap_or(zeros.len());

        if write_all_at(file, &zeros[..piece], start + done).is_err() {
            return Ok(false);
        }

        done += piece as u64;
    }

    #[cfg(test)]
    if job.fail_sync {
        return Err("injected prefill sync failure".into());
    }

    file.sync_data()
        .map(|()| true)
        .map_err(|error| error.to_string())
}

/// A new open file description of `file`, so its syncs keep their own error cursor.
#[cfg(target_os = "linux")]
fn reopen(file: &File) -> io::Result<File> {
    use std::{
        io::Write,
        os::{fd::AsRawFd, unix::ffi::OsStrExt},
    };

    // Formatted on the stack: a fill is requested from the write path, which allocates nothing.
    let mut path = [0u8; 32];
    let mut cursor = io::Cursor::new(&mut path[..]);
    write!(cursor, "/proc/self/fd/{}", file.as_raw_fd())?;
    let length = cursor.position() as usize;

    OpenOptions::new()
        .write(true)
        .open(std::ffi::OsStr::from_bytes(&path[..length]))
}

#[cfg(not(target_os = "linux"))]
fn reopen(file: &File) -> io::Result<File> {
    file.try_clone()
}

#[cfg(unix)]
fn write_all_at(file: &File, buffer: &[u8], offset: u64) -> io::Result<()> {
    std::os::unix::fs::FileExt::write_all_at(file, buffer, offset)
}

#[cfg(windows)]
fn write_all_at(file: &File, buffer: &[u8], offset: u64) -> io::Result<()> {
    let mut done = 0;

    while done < buffer.len() {
        match std::os::windows::fs::FileExt::seek_write(
            file,
            &buffer[done..],
            offset + done as u64,
        )? {
            0 => return Err(io::ErrorKind::WriteZero.into()),
            count => done += count,
        }
    }

    Ok(())
}

/// Extend `file` by `length` bytes from `offset` (size grows too). Linux only; `false` means unsupported or failed.
#[cfg(target_os = "linux")]
#[allow(unsafe_code)]
fn preallocate(file: &File, offset: u64, length: u64) -> bool {
    use std::os::fd::AsRawFd;

    unsafe extern "C" {
        fn fallocate(fd: i32, mode: i32, offset: i64, length: i64) -> i32;
    }

    let (Ok(offset), Ok(length)) = (i64::try_from(offset), i64::try_from(length)) else {
        return false;
    };

    // SAFETY: `fallocate` only reads its integer arguments; the descriptor is valid for the
    // whole call because `file` is borrowed.
    unsafe { fallocate(file.as_raw_fd(), 0, offset, length) == 0 }
}

#[cfg(not(target_os = "linux"))]
fn preallocate(_file: &File, _offset: u64, _length: u64) -> bool {
    false
}

/// The first record after a damaged one at `damaged` (offset and sequence) whose checksum
/// holds and whose sequence continues the log (`>= expected`). Reads the rest of the segment
/// once; only startup with a damaged last segment comes here.
fn intact_record_after(
    file: &mut File,
    damaged: u64,
    end: u64,
    shard_id: u32,
    limits: ProtocolLimits,
    expected: u64,
) -> Result<Option<(u64, u64)>, PersistenceError> {
    let mut rest = Vec::new();

    file.seek(SeekFrom::Start(damaged))?;
    file.take(end.saturating_sub(damaged))
        .read_to_end(&mut rest)?;

    for start in 1..rest.len().saturating_sub(RECORD_MIN_LEN - 1) {
        let length = u32::from_le_bytes(rest[start..start + 4].try_into().unwrap()) as usize;

        if length < RECORD_MIN_LEN || length > rest.len() - start {
            continue;
        }

        // Most offsets fail on the header; only the rest pay for a checksum.
        let frame = &rest[start..start + length];
        if !record_header_plausible(frame, shard_id, expected) {
            continue;
        }

        if let Ok(record) = decode_record(frame, shard_id, limits)
            && record.sequence >= expected
        {
            return Ok(Some((damaged + start as u64, record.sequence)));
        }
    }

    Ok(None)
}

/// True when every byte of `file` from `from` to its end is zero (an unwritten preallocated tail).
fn rest_is_zero(file: &mut File, from: u64) -> io::Result<bool> {
    file.seek(SeekFrom::Start(from))?;
    let mut buffer = [0u8; 8192];

    loop {
        let count = file.read(&mut buffer)?;

        if count == 0 {
            return Ok(true);
        }

        if buffer[..count].iter().any(|byte| *byte != 0) {
            return Ok(false);
        }
    }
}

fn read_partial(reader: &mut impl Read, buffer: &mut [u8]) -> io::Result<usize> {
    let mut read = 0;

    while read < buffer.len() {
        match reader.read(&mut buffer[read..])? {
            0 => break,
            count => read += count,
        }
    }

    Ok(read)
}

#[cfg(test)]
mod tests {
    use std::{borrow::Cow, fs, io::Write, time::Duration};

    use crate::persistence::codec::encode_record;

    use tempfile::tempdir;

    use super::*;

    fn config(path: &Path, profile: DurabilityProfile) -> PersistenceConfig {
        PersistenceConfig {
            profile,
            data_dir: path.to_path_buf(),
            wal_segment_bytes: 4096,
            sync_interval: std::time::Duration::from_millis(1),
            ..PersistenceConfig::default()
        }
    }

    /// Many appends with one sync at the end. `Always` would fsync every record.
    fn bulk_config(path: &Path) -> PersistenceConfig {
        PersistenceConfig {
            sync_interval: std::time::Duration::from_secs(3600),
            ..config(path, DurabilityProfile::Everysec)
        }
    }

    #[test]
    fn sync_totals_count_inline_syncs() {
        let directory = tempdir().unwrap();
        let config = config(directory.path(), DurabilityProfile::Always);
        let (mut sink, _, _) = WalSink::open(config, 0, ProtocolLimits::default()).unwrap();
        let before = sink.sync_totals().0;

        for index in 0..3u8 {
            sink.begin_group();
            sink.record(Mutation::SetBytes {
                key: b"k",
                value: &[index; 16],
            })
            .unwrap();
            sink.end_group().unwrap();
            sink.sync_pending().unwrap();
        }

        let (count, nanos) = sink.sync_totals();

        assert_eq!(count - before, 3);
        assert!(nanos > 0);
    }

    /// A checkpoint whose snapshot point is past `always` records not synced yet (a batch
    /// between its group's end and its sync) syncs them first, and is refused when that
    /// sync fails: the batch's clients are refused and its records cut.
    #[test]
    fn a_checkpoint_past_unsynced_always_records_syncs_them_or_is_refused() {
        let directory = tempdir().unwrap();
        let config = config(directory.path(), DurabilityProfile::Always);
        let (mut sink, _, _) = WalSink::open(config.clone(), 0, ProtocolLimits::default()).unwrap();
        let write = |sink: &mut WalSink, key: &[u8]| {
            sink.begin_group();
            sink.record(Mutation::SetBytes { key, value: b"v" })
                .unwrap();
            sink.end_group().unwrap();
        };

        write(&mut sink, b"synced");
        sink.sync_for_checkpoint(sink.last_sequence()).unwrap();
        assert_eq!(sink.durable.load(Ordering::Acquire), sink.last_sequence());

        write(&mut sink, b"refused");
        assert!(sink.checkpoint_may_cover(sink.last_sequence()).is_ok());
        FAILING_SYNC_DIRS
            .lock()
            .unwrap()
            .push(directory.path().to_path_buf());
        let checkpoint = sink.sync_for_checkpoint(sink.last_sequence());
        let batch = sink.sync_batch(1);
        FAILING_SYNC_DIRS
            .lock()
            .unwrap()
            .retain(|dir| dir != directory.path());

        assert!(checkpoint.is_err());
        assert!(batch.is_err());
        assert!(sink.checkpoint_may_cover(sink.last_sequence()).is_err());
        drop(sink);
        let (_sink, mutations, _) = WalSink::open(config, 0, ProtocolLimits::default()).unwrap();
        assert_eq!(mutations.len(), 1);
    }

    /// `SAVE` and rotation flush the log on the shard thread, possibly between a batch's
    /// group end and its sync. When that flush's sync fails, the batch's clients are refused
    /// by their own sync, so its records are cut and a restart does not revive them.
    #[test]
    fn a_failed_flush_cuts_always_records_whose_batch_is_refused() {
        let directory = tempdir().unwrap();
        let config = config(directory.path(), DurabilityProfile::Always);
        let (mut sink, _, _) = WalSink::open(config.clone(), 0, ProtocolLimits::default()).unwrap();

        sink.begin_group();
        sink.record(Mutation::SetBytes {
            key: b"acknowledged",
            value: b"v",
        })
        .unwrap();
        sink.end_group().unwrap();
        sink.sync_pending().unwrap();
        let start = sink.last_sequence();

        sink.begin_group();
        sink.record(Mutation::SetBytes {
            key: b"refused",
            value: b"v",
        })
        .unwrap();
        sink.end_group().unwrap();
        FAILING_SYNC_DIRS
            .lock()
            .unwrap()
            .push(directory.path().to_path_buf());
        let flushed = sink.flush();
        let batch = sink.sync_batch(start);
        FAILING_SYNC_DIRS
            .lock()
            .unwrap()
            .retain(|dir| dir != directory.path());

        assert!(flushed.is_err());
        assert!(batch.is_err());
        drop(sink);
        let (_sink, mutations, _) = WalSink::open(config, 0, ProtocolLimits::default()).unwrap();
        assert_eq!(mutations.len(), 1);
    }

    /// A sync on another descriptor of the segment (the preallocation fill) failed: the
    /// failure is picked up before this batch's own sync runs, and the batch's clients are
    /// refused, so its records are cut as when its own sync fails.
    #[test]
    fn a_failed_fill_sync_cuts_always_records_whose_batch_is_refused() {
        let directory = tempdir().unwrap();
        let config = config(directory.path(), DurabilityProfile::Always);
        let (mut sink, _, _) = WalSink::open(config.clone(), 0, ProtocolLimits::default()).unwrap();
        let write = |sink: &mut WalSink, key: &[u8]| {
            let start = sink.last_sequence();

            sink.begin_group();
            sink.record(Mutation::SetBytes { key, value: b"v" })
                .unwrap();
            sink.end_group().unwrap();
            start
        };

        let start = write(&mut sink, b"acknowledged");
        sink.sync_batch(start).unwrap();
        let start = write(&mut sink, b"refused");
        *sink.sync_error.lock().unwrap() = Some("injected fill sync failure".into());

        assert!(sink.sync_batch(start).is_err());
        drop(sink);
        let (_sink, mutations, _) = WalSink::open(config, 0, ProtocolLimits::default()).unwrap();
        assert_eq!(mutations.len(), 1);
    }

    #[test]
    fn steady_writes_through_the_sync_worker_keep_unsynced_bytes_bounded() {
        let directory = tempdir().unwrap();
        let mut config = config(directory.path(), DurabilityProfile::Always);
        config.sync_worker_active = true;
        // One segment for the whole run: a rotation would reset the count.
        config.wal_segment_bytes = 64 << 20;
        let (mut sink, _, _) = WalSink::open(config, 0, ProtocolLimits::default()).unwrap();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        // Each barrier is prepared after the next record is written, so the check never
        // finds the whole log durable.
        for index in 0..200u8 {
            sink.begin_group();
            sink.record(Mutation::SetBytes {
                key: b"k",
                value: &[index; 1000],
            })
            .unwrap();
            sink.end_group().unwrap();
            let barrier = sink.prepare_sync_barrier().unwrap().unwrap();
            block_on_bounded(&runtime, barrier.wait()).unwrap();
        }

        assert!(
            sink.dirty_bytes() < 3 * 1100,
            "unsynced bytes grew to {}",
            sink.dirty_bytes()
        );
    }

    #[test]
    fn sync_worker_barriers_survive_rotation_and_recovery() {
        let directory = tempdir().unwrap();
        let mut config = config(directory.path(), DurabilityProfile::Always);
        config.sync_worker_active = true;
        let (mut sink, _, _) = WalSink::open(config.clone(), 0, ProtocolLimits::default()).unwrap();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        for index in 0..30 {
            sink.begin_group();
            sink.record(Mutation::SetBytes {
                key: b"k",
                value: &[index; 512],
            })
            .unwrap();
            sink.end_group().unwrap();
            let sequence = sink.last_sequence();
            let barrier = sink.prepare_sync_barrier().unwrap().unwrap();
            block_on_bounded(&runtime, barrier.wait()).unwrap();
            assert!(sink.durable.load(Ordering::Acquire) >= sequence);
        }
        sink.flush().unwrap();
        drop(sink);
        let (_, records, report) = WalSink::open(config, 0, ProtocolLimits::default()).unwrap();
        assert_eq!(records.len(), 30);
        assert_eq!(report.last_sequence, 30);
    }

    #[test]
    fn shutdown_flush_waits_for_a_stalled_sync_worker() {
        let directory = tempdir().unwrap();
        let mut config = config(directory.path(), DurabilityProfile::Always);
        config.sync_worker_active = true;
        let (mut sink, _, _) = WalSink::open(config, 0, ProtocolLimits::default()).unwrap();
        let release = sink.sync_worker.as_ref().unwrap().pause_handle();
        release(true);
        sink.begin_group();
        sink.record(Mutation::SetBytes {
            key: b"k",
            value: b"v",
        })
        .unwrap();
        sink.end_group().unwrap();
        let _barrier = sink.prepare_sync_barrier().unwrap().unwrap();
        let wait_started = std::time::Instant::now();
        std::thread::scope(|scope| {
            scope.spawn(|| {
                std::thread::sleep(Duration::from_millis(150));
                release(false);
            });
            sink.flush().unwrap();
        });
        assert!(wait_started.elapsed() >= Duration::from_millis(150));
        assert_eq!(sink.durable.load(Ordering::Acquire), sink.last_sequence());
    }

    #[test]
    fn observed_read_barrier_waits_for_another_clients_unsynced_write() {
        let directory = tempdir().unwrap();
        let mut config = config(directory.path(), DurabilityProfile::Always);
        config.sync_worker_active = true;
        let (mut sink, _, _) = WalSink::open(config, 0, ProtocolLimits::default()).unwrap();
        sink.sync_worker.as_ref().unwrap().pause(true);
        sink.begin_group();
        sink.record(Mutation::SetBytes {
            key: b"k",
            value: b"v",
        })
        .unwrap();
        sink.end_group().unwrap();
        let writer = sink.prepare_sync_barrier().unwrap().unwrap();
        // A GET emits no new record, but must wait for the same observed sequence.
        let reader = sink.prepare_sync_barrier().unwrap().unwrap();
        assert_eq!(sink.durable.load(Ordering::Acquire), 0);
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async {
            let release = async {
                tokio::task::yield_now().await;
                sink.sync_worker.as_ref().unwrap().pause(false);
            };
            let (a, b, _) = tokio::join!(writer.wait(), reader.wait(), release);
            a.unwrap();
            b.unwrap();
        });
        assert_eq!(sink.durable.load(Ordering::Acquire), 1);
    }

    #[test]
    fn worker_failure_is_visible_before_next_append_and_rejects_mutations() {
        let directory = tempdir().unwrap();
        let mut config = config(directory.path(), DurabilityProfile::Always);
        config.sync_worker_active = true;
        let (mut sink, _, _) = WalSink::open(config, 0, ProtocolLimits::default()).unwrap();
        sink.sync_worker.as_ref().unwrap().fail_sync_for_test();
        sink.begin_group();
        sink.record(Mutation::SetBytes {
            key: b"k",
            value: b"v",
        })
        .unwrap();
        sink.end_group().unwrap();
        let barrier = sink.prepare_sync_barrier().unwrap().unwrap();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        assert!(block_on_bounded(&runtime, barrier.wait()).is_err());
        assert!(sink.failure().is_some());
        assert_eq!(sink.durable.load(Ordering::Acquire), 0);
        let sequence = sink.last_sequence();
        assert!(
            sink.record(Mutation::SetBytes {
                key: b"next",
                value: b"v"
            })
            .is_err()
        );
        assert_eq!(sink.last_sequence(), sequence);
        assert!(sink.flush().is_err());
    }

    #[test]
    fn group_bytes_reset_per_group_while_worker_owns_dirty_bytes() {
        let directory = tempdir().unwrap();
        let mut config = config(directory.path(), DurabilityProfile::Always);
        config.sync_worker_active = true;
        config.wal_segment_bytes = 1 << 20;
        let (mut sink, _, _) = WalSink::open(config, 0, ProtocolLimits::default()).unwrap();
        let value = vec![b'v'; 40 * 1024];
        for _ in 0..2 {
            sink.begin_group();
            sink.record(Mutation::SetBytes {
                key: b"k",
                value: &value,
            })
            .unwrap();
            assert!(sink.group_bytes() >= 40 * 1024);
            assert!(sink.group_bytes() < 64 * 1024);
            sink.end_group().unwrap();
            assert_eq!(sink.group_bytes(), 0);
            drop(sink.prepare_sync_barrier().unwrap().unwrap());
        }
        assert!(sink.dirty_bytes() >= 64 * 1024);
    }

    #[test]
    fn base_replacement_resets_worker_submission_cursor() {
        let directory = tempdir().unwrap();
        let mut config = config(directory.path(), DurabilityProfile::Always);
        config.sync_worker_active = true;
        let (mut sink, _, _) = WalSink::open(config, 0, ProtocolLimits::default()).unwrap();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        sink.begin_group();
        for _ in 0..3 {
            sink.record(Mutation::SetBytes {
                key: b"k",
                value: b"v",
            })
            .unwrap();
        }
        sink.end_group().unwrap();
        let barrier = sink.prepare_sync_barrier().unwrap().unwrap();
        block_on_bounded(&runtime, barrier.wait()).unwrap();
        sink.install_base(0).unwrap();
        sink.begin_group();
        sink.record(Mutation::SetBytes {
            key: b"new",
            value: b"v",
        })
        .unwrap();
        sink.end_group().unwrap();
        let barrier = sink.prepare_sync_barrier().unwrap().unwrap();
        runtime.block_on(async {
            tokio::time::timeout(std::time::Duration::from_secs(2), barrier.wait())
                .await
                .unwrap()
                .unwrap();
        });
        assert_eq!(sink.durable.load(Ordering::Acquire), 1);
    }

    #[test]
    fn idempotency_torn_completion_recovers_pending_and_success_recovers_result() {
        use crate::engine::{Response, execute_in_database};
        use crate::protocol::Command;
        let directory = tempdir().unwrap();
        let config = config(directory.path(), DurabilityProfile::Always);
        let limits = ProtocolLimits::default();
        let (mut sink, _, _) = WalSink::open(config.clone(), 0, limits).unwrap();
        let clock = crate::time::TestClock::new(4_000_000_000_000);
        let mut shard = crate::KvShard::with_clock(clock);
        let begin = Command::IdempotencyBegin {
            key: Cow::Borrowed(b"request"),
            fingerprint: Cow::Borrowed(b"fp"),
            owner: Cow::Borrowed(b"owner"),
            ttl_ms: 60_000,
        };
        let complete = Command::IdempotencyComplete {
            key: Cow::Borrowed(b"request"),
            fingerprint: Cow::Borrowed(b"fp"),
            owner: Cow::Borrowed(b"owner"),
            result: Cow::Borrowed(b"\0\xffsaved"),
        };
        execute_in_database(&mut shard, begin.clone(), &mut sink, &mut (), 2).unwrap();
        super::super::crash::arm_io_fault(super::super::crash::CrashSite::Append);
        assert!(execute_in_database(&mut shard, complete.clone(), &mut sink, &mut (), 2).is_err());
        drop(sink);
        let (mut sink, records, report) = WalSink::open(config.clone(), 0, limits).unwrap();
        assert_eq!(records.len(), 1);
        assert!(report.torn_tail_bytes > 0);
        let mut restored = crate::KvShard::new();
        for (database, mutation) in records {
            crate::persistence::apply_mutation(&mut restored, database, &mutation).unwrap();
        }
        assert_eq!(
            execute_in_database(&mut restored, begin.clone(), &mut sink, &mut (), 2).unwrap(),
            Response::Array(vec![Some(Cow::Borrowed(b"pending"))])
        );
        assert_eq!(
            restored.expires_at(b"request"),
            Some(crate::UnixMillis::new(4_000_000_060_000))
        );
        assert_eq!(
            execute_in_database(&mut restored, complete, &mut sink, &mut (), 2).unwrap(),
            Response::Integer(1)
        );
        drop(sink);
        let (_sink, records, _) = WalSink::open(config, 0, limits).unwrap();
        assert_eq!(records.len(), 2);
        let mut restored = crate::KvShard::new();
        for (database, mutation) in records {
            assert_eq!(database, 2);
            crate::persistence::apply_mutation(&mut restored, database, &mutation).unwrap();
        }
        assert_eq!(
            execute_in_database(
                &mut restored,
                begin,
                &mut crate::mutation::NoopMutationSink,
                &mut (),
                2
            )
            .unwrap(),
            Response::Array(vec![
                Some(Cow::Borrowed(b"completed")),
                Some(Cow::Borrowed(b"\0\xffsaved"))
            ])
        );
        assert_eq!(
            restored.expires_at(b"request"),
            Some(crate::UnixMillis::new(4_000_000_060_000))
        );
    }

    fn config_at(config: &PersistenceConfig, data_dir: &Path) -> PersistenceConfig {
        PersistenceConfig {
            data_dir: data_dir.to_path_buf(),
            ..config.clone()
        }
    }

    fn keys_of(mutations: &RecoveredMutations) -> Vec<Vec<u8>> {
        mutations
            .iter()
            .map(|(_, mutation)| match mutation {
                OwnedMutation::SetBytes { key, .. } => key.clone(),
                other => panic!("unexpected mutation {other:?}"),
            })
            .collect()
    }

    #[test]
    fn full_disk_during_grouped_write_keeps_wal_sequences_contiguous() {
        let directory = tempdir().unwrap();
        let config = bulk_config(directory.path());
        let limits = ProtocolLimits::default();
        let (mut sink, _, _) = WalSink::open(config.clone(), 0, limits).unwrap();

        sink.record(Mutation::SetBytes {
            key: b"kept",
            value: b"1",
        })
        .unwrap();
        sink.begin_group();
        for key in [&b"grouped-a"[..], b"grouped-b"] {
            sink.record(Mutation::SetBytes { key, value: b"2" })
                .unwrap();
        }
        super::super::crash::arm_io_fault(super::super::crash::CrashSite::Append);
        assert!(sink.end_group().is_err());
        assert!(sink.failure().is_some());

        // The disk accepts writes again: the batch that failed to reach it is written first.
        sink.record(Mutation::SetBytes {
            key: b"later",
            value: b"3",
        })
        .unwrap();
        assert!(sink.failure().is_none());
        sink.flush().unwrap();
        drop(sink);

        let (_sink, mutations, report) = WalSink::open(config, 0, limits).unwrap();

        assert_eq!(report.last_sequence, 4);
        assert_eq!(
            keys_of(&mutations),
            [&b"kept"[..], b"grouped-a", b"grouped-b", b"later"]
        );
    }

    #[test]
    fn full_disk_during_first_grouped_write_reuses_the_empty_segment() {
        let directory = tempdir().unwrap();
        let config = bulk_config(directory.path());
        let limits = ProtocolLimits::default();
        let (mut sink, _, _) = WalSink::open(config.clone(), 0, limits).unwrap();

        sink.begin_group();
        sink.record(Mutation::SetBytes {
            key: b"grouped",
            value: b"1",
        })
        .unwrap();
        super::super::crash::arm_io_fault(super::super::crash::CrashSite::Append);
        assert!(sink.end_group().is_err());
        // While failed, a later group that wrote nothing ends cleanly and must not append the
        // kept batch after the torn bytes.
        sink.begin_group();
        sink.end_group().unwrap();
        assert!(sink.failure().is_some());
        sink.record(Mutation::SetBytes {
            key: b"later",
            value: b"2",
        })
        .unwrap();
        sink.flush().unwrap();
        drop(sink);

        let (_sink, mutations, report) = WalSink::open(config, 0, limits).unwrap();

        assert_eq!(report.torn_tail_bytes, 0);
        assert_eq!(keys_of(&mutations), [&b"grouped"[..], b"later"]);
    }

    #[test]
    fn replica_frame_append_recovers_after_a_full_disk() {
        let directory = tempdir().unwrap();
        let config = config(directory.path(), DurabilityProfile::Always);
        let limits = ProtocolLimits::default();
        let (mut sink, _, _) = WalSink::open(config.clone(), 0, limits).unwrap();
        let frame = |sequence, key: &[u8]| {
            encode_record(
                0,
                sequence,
                0,
                &OwnedMutation::SetBytes {
                    key: key.to_vec(),
                    value: b"v".to_vec(),
                },
            )
            .unwrap()
        };

        sink.append_frame(&frame(1, b"one")).unwrap();
        super::super::crash::arm_io_fault(super::super::crash::CrashSite::Append);
        assert!(sink.append_frame(&frame(2, b"two")).is_err());
        sink.append_frame(&frame(2, b"two")).unwrap();
        assert!(sink.failure().is_none());
        drop(sink);

        let (_sink, mutations, _) = WalSink::open(config, 0, limits).unwrap();

        assert_eq!(keys_of(&mutations), [&b"one"[..], b"two"]);
    }

    #[test]
    fn failed_segment_creation_leaves_no_file_that_blocks_a_retry() {
        let directory = tempdir().unwrap();
        let shard_dir = shard_wal_directory(directory.path(), 0);

        super::super::crash::arm_io_fault(super::super::crash::CrashSite::Rotate);
        assert!(create_segment(&shard_dir, 0, 7).is_err());
        let (_, _, first, length) = create_segment(&shard_dir, 0, 7).unwrap();

        assert_eq!((first, length), (7, WAL_HEADER_LEN as u64));

        // A header-only leftover the cleanup could not remove is reused, not an EEXIST forever.
        let path = segment_path(&shard_dir, 9);

        fs::write(&path, &encode_segment_header(0, 9, 9)[..10]).unwrap();
        let (_, _, first, _) = create_segment(&shard_dir, 0, 9).unwrap();

        assert_eq!(first, 9);
        assert_eq!(fs::metadata(&path).unwrap().len(), WAL_HEADER_LEN as u64);
    }

    #[test]
    fn rotation_failure_enters_the_failed_state_and_recovers() {
        let directory = tempdir().unwrap();
        let config = config(directory.path(), DurabilityProfile::Always);
        let limits = ProtocolLimits::default();
        let (mut sink, _, _) = WalSink::open(config.clone(), 0, limits).unwrap();
        let value = vec![b'x'; 2_000];

        sink.record(Mutation::SetBytes {
            key: b"first",
            value: &value,
        })
        .unwrap();
        super::super::crash::arm_io_fault(super::super::crash::CrashSite::Rotate);
        assert!(
            sink.record(Mutation::SetBytes {
                key: b"second",
                value: &value,
            })
            .is_err()
        );
        assert!(sink.failure().is_some());
        sink.record(Mutation::SetBytes {
            key: b"second",
            value: &value,
        })
        .unwrap();
        assert!(sink.failure().is_none());
        drop(sink);

        let (_sink, mutations, _) = WalSink::open(config, 0, limits).unwrap();

        assert_eq!(keys_of(&mutations), [&b"first"[..], b"second"]);
    }

    #[test]
    fn everysec_flush_reports_a_background_sync_that_fails_late() {
        let directory = tempdir().unwrap();
        let config = config(directory.path(), DurabilityProfile::Everysec);
        let (mut sink, _, _) = WalSink::open(config, 0, ProtocolLimits::default()).unwrap();

        // Past the 1 ms interval, so this append starts a background sync.
        std::thread::sleep(std::time::Duration::from_millis(5));
        // The background sync started by this append fails a little later.
        super::super::crash::arm_io_fault(super::super::crash::CrashSite::Sync);
        sink.record(Mutation::SetBytes {
            key: b"k",
            value: b"v",
        })
        .unwrap();

        assert!(sink.flush().is_err());
        assert!(sink.dirty_bytes() > 0);
        assert_eq!(sink.durable.load(Ordering::Acquire), 0);
    }

    #[test]
    fn open_after_a_checkpoint_past_the_log_end_starts_a_new_segment() {
        let directory = tempdir().unwrap();
        let config = config(directory.path(), DurabilityProfile::Always);
        let limits = ProtocolLimits::default();
        let (mut sink, _, _) = WalSink::open(config.clone(), 0, limits).unwrap();

        for key in [&b"one"[..], b"two"] {
            sink.record(Mutation::SetBytes { key, value: b"v" })
                .unwrap();
        }
        drop(sink);

        // The checkpoint covers sequence 5; records 3..=5 never reached this log.
        let (mut sink, mutations, _) = WalSink::open_after(config.clone(), 0, limits, 5).unwrap();

        assert!(mutations.is_empty());
        assert_eq!(sink.last_sequence(), 5);
        sink.record(Mutation::SetBytes {
            key: b"six",
            value: b"v",
        })
        .unwrap();
        drop(sink);

        let (sink, mutations, _) = WalSink::open_after(config, 0, limits, 5).unwrap();

        assert_eq!(keys_of(&mutations), [&b"six"[..]]);
        assert_eq!(sink.last_sequence(), 6);
    }

    #[test]
    fn always_sync_failure_does_not_revive_the_refused_record() {
        let directory = tempdir().unwrap();
        let config = config(directory.path(), DurabilityProfile::Always);
        let limits = ProtocolLimits::default();
        let (mut sink, _, _) = WalSink::open(config.clone(), 0, limits).unwrap();

        sink.record(Mutation::SetBytes {
            key: b"one",
            value: b"v",
        })
        .unwrap();
        // The record reaches the file but its sync fails: the write is refused (not applied).
        super::super::crash::arm_io_fault(super::super::crash::CrashSite::Sync);
        assert!(
            sink.record(Mutation::SetBytes {
                key: b"refused",
                value: b"v",
            })
            .is_err()
        );
        // A restart before any recovery must not replay it either.
        let path = segment_path(&shard_wal_directory(directory.path(), 0), 1);
        let copy = directory.path().join("restart-copy");

        fs::create_dir_all(shard_wal_directory(&copy, 0)).unwrap();
        fs::copy(&path, segment_path(&shard_wal_directory(&copy, 0), 1)).unwrap();
        let (_copy_sink, mutations, _) =
            WalSink::open(config_at(&config, &copy), 0, limits).unwrap();

        assert_eq!(keys_of(&mutations), [&b"one"[..]]);

        // A failed sync is not retried in place: writes stay refused until a restart.
        sink.last_recovery = None;
        assert!(
            sink.record(Mutation::SetBytes {
                key: b"three",
                value: b"v",
            })
            .is_err()
        );
        drop(sink);

        let (mut sink, mutations, _) = WalSink::open(config, 0, limits).unwrap();

        assert_eq!(keys_of(&mutations), [&b"one"[..]]);
        sink.record(Mutation::SetBytes {
            key: b"three",
            value: b"v",
        })
        .unwrap();
    }

    /// Runs `future` on `runtime`, failing the test after 10 s instead of hanging.
    fn block_on_bounded<F: std::future::Future>(
        runtime: &tokio::runtime::Runtime,
        future: F,
    ) -> F::Output {
        runtime.block_on(async {
            tokio::time::timeout(std::time::Duration::from_secs(10), future)
                .await
                .expect("test wait exceeded 10 s")
        })
    }

    /// `WalSink::wait_background_sync` with a 10 s deadline, for tests.
    fn wait_background_sync_bounded(sink: &WalSink) {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);

        while sink.sync_running.load(Ordering::Acquire) {
            assert!(
                std::time::Instant::now() < deadline,
                "background sync still running after 10 s"
            );
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
    }

    fn set(sink: &mut WalSink, key: &[u8], value: &[u8]) -> Result<(), PersistenceError> {
        sink.record(Mutation::SetBytes { key, value })
    }

    fn segment_sizes(directory: &Path) -> Vec<u64> {
        discover_segments(&shard_wal_directory(directory, 0))
            .unwrap()
            .iter()
            .map(|path| fs::metadata(path).unwrap().len())
            .collect()
    }

    #[test]
    fn background_sync_failure_keeps_writes_refused_until_restart() {
        let directory = tempdir().unwrap();
        let config = config(directory.path(), DurabilityProfile::Everysec);
        let limits = ProtocolLimits::default();
        let (mut sink, _, _) = WalSink::open(config.clone(), 0, limits).unwrap();

        set(&mut sink, b"one", b"v").unwrap();
        // The sync this append started must be done, or the next append would not start one.
        wait_background_sync_bounded(&sink);
        std::thread::sleep(std::time::Duration::from_millis(5));
        // The background sync this append starts fails: the pages it covered may be gone
        // while a later sync reports success.
        super::super::crash::arm_io_fault(super::super::crash::CrashSite::Sync);
        set(&mut sink, b"two", b"v").unwrap();
        assert!(sink.sync_running.load(Ordering::Acquire));
        assert!(sink.flush().is_err());
        sink.shrink_to_written();
        let sizes = segment_sizes(directory.path());

        for _ in 0..2 {
            sink.last_recovery = None;
            assert!(set(&mut sink, b"three", b"v").is_err());
            assert!(sink.flush_for_shutdown().is_err());
            assert!(
                sink.prepare_sync_barrier()
                    .is_ok_and(|barrier| barrier.is_none())
            );
        }
        assert!(sink.failure().is_some());
        // Nothing written before the failure was cut, moved or rewritten.
        assert_eq!(segment_sizes(directory.path()), sizes);
        assert!(sink.durable.load(Ordering::Acquire) < 2);
        drop(sink);

        // Startup decides from what is on disk.
        let (mut sink, mutations, _) = WalSink::open(config, 0, limits).unwrap();

        assert_eq!(keys_of(&mutations), [&b"one"[..], b"two"]);
        set(&mut sink, b"three", b"v").unwrap();
    }

    #[test]
    fn write_failure_while_a_background_sync_fails_is_not_recovered() {
        let directory = tempdir().unwrap();
        let config = config(directory.path(), DurabilityProfile::Everysec);
        let limits = ProtocolLimits::default();
        let (mut sink, _, _) = WalSink::open(config.clone(), 0, limits).unwrap();

        set(&mut sink, b"one", b"v").unwrap();
        // The sync this append started must be done, or the next append would not start one.
        wait_background_sync_bounded(&sink);
        std::thread::sleep(std::time::Duration::from_millis(5));
        // This append starts a background sync that fails ~30 ms later.
        super::super::crash::arm_io_fault(super::super::crash::CrashSite::Sync);
        set(&mut sink, b"two", b"v").unwrap();
        assert!(sink.sync_running.load(Ordering::Acquire));
        // Meanwhile a write fails (a full disk): an ordinary, recoverable failure.
        super::super::crash::arm_io_fault(super::super::crash::CrashSite::Append);
        assert!(set(&mut sink, b"three", b"v").is_err());
        assert!(!sink.sync_failed);
        // Recovery's own sync would report clean pages the failed sync may have lost.
        sink.last_recovery = None;
        assert!(set(&mut sink, b"four", b"v").is_err());
        assert!(sink.sync_failed);
        sink.last_recovery = None;
        assert!(set(&mut sink, b"four", b"v").is_err());
        assert!(sink.durable.load(Ordering::Acquire) < 2);
    }

    #[test]
    fn failed_prefill_sync_is_a_failed_sync_of_the_segment() {
        let directory = tempdir().unwrap();
        let config = PersistenceConfig {
            wal_segment_bytes: 1 << 20,
            ..config(directory.path(), DurabilityProfile::Always)
        };
        let limits = ProtocolLimits::default();
        let (mut sink, _, _) = WalSink::open(config.clone(), 0, limits).unwrap();

        let probe = File::create(directory.path().join("probe")).unwrap();
        if !sink.prealloc || !preallocate(&probe, 0, 4096) {
            // `fallocate` is unsupported here: no fill, no sync to fail.
            return;
        }
        // The first append preallocates the segment; that fill's sync fails.
        super::super::crash::arm_io_fault(super::super::crash::CrashSite::PrefillSync);
        assert!(set(&mut sink, b"one", b"v").is_err());
        assert!(sink.sync_failed);
        sink.last_recovery = None;
        assert!(set(&mut sink, b"two", b"v").is_err());
        drop(sink);

        let (_sink, mutations, _) = WalSink::open(config, 0, limits).unwrap();
        assert!(mutations.is_empty());
    }

    #[test]
    fn always_group_refused_by_a_failed_sync_is_not_replayed() {
        let directory = tempdir().unwrap();
        let config = config(directory.path(), DurabilityProfile::Always);
        let limits = ProtocolLimits::default();
        let (mut sink, _, _) = WalSink::open(config.clone(), 0, limits).unwrap();

        set(&mut sink, b"one", b"v").unwrap();
        sink.begin_group();
        set(&mut sink, b"ga", b"v").unwrap();
        set(&mut sink, b"gb", b"v").unwrap();
        sink.end_group().unwrap();
        // The batch's sync fails: its clients get an error.
        super::super::crash::arm_io_fault(super::super::crash::CrashSite::Sync);
        assert!(sink.sync_pending().is_err());
        drop(sink);

        let (_sink, mutations, _) = WalSink::open(config, 0, limits).unwrap();
        assert_eq!(keys_of(&mutations), [&b"one"[..]]);
    }

    #[test]
    fn probe_second_batch_acked_after_cut() {
        let directory = tempdir().unwrap();
        let config = config(directory.path(), DurabilityProfile::Always);
        let limits = ProtocolLimits::default();
        let (mut sink, _, _) = WalSink::open(config.clone(), 0, limits).unwrap();
        let published = sink.published();

        set(&mut sink, b"one", b"v").unwrap();
        // Two batches end before either syncs; the first sync covers both and fails.
        for key in [&b"a"[..], b"b"] {
            sink.begin_group();
            set(&mut sink, key, b"v").unwrap();
            sink.end_group().unwrap();
        }
        super::super::crash::arm_io_fault(super::super::crash::CrashSite::Sync);
        assert!(sink.sync_pending().is_err());
        // The second batch's records were cut too: its clients must not get OK.
        assert!(sink.sync_pending().is_err());
        assert_eq!(published.load(Ordering::Acquire), 1);
        drop(sink);

        let (_sink, mutations, _) = WalSink::open(config, 0, limits).unwrap();
        assert_eq!(keys_of(&mutations), [&b"one"[..]]);
    }

    #[test]
    fn probe_group_synced_after_inline_failure() {
        let directory = tempdir().unwrap();
        let config = config(directory.path(), DurabilityProfile::Always);
        let limits = ProtocolLimits::default();
        let (mut sink, _, _) = WalSink::open(config.clone(), 0, limits).unwrap();
        let published = sink.published();

        set(&mut sink, b"one", b"v").unwrap();
        sink.begin_group();
        set(&mut sink, b"batch", b"v").unwrap();
        sink.end_group().unwrap();
        // Another client's single write fails its sync before the batch syncs. The kernel
        // reported the error once; a later sync of the same file comes back clean.
        super::super::crash::arm_io_fault(super::super::crash::CrashSite::Sync);
        assert!(set(&mut sink, b"single", b"v").is_err());
        assert!(sink.sync_pending().is_err());
        assert_eq!(published.load(Ordering::Acquire), 1);
        assert!(sink.durable.load(Ordering::Acquire) < 2);
        drop(sink);

        // Neither the refused write nor the batch whose clients got an error comes back.
        let (_sink, mutations, _) = WalSink::open(config, 0, limits).unwrap();
        assert_eq!(keys_of(&mutations), [&b"one"[..]]);
    }

    #[test]
    fn records_rewritten_by_recovery_are_published_after_their_sync() {
        let directory = tempdir().unwrap();
        let config = config(directory.path(), DurabilityProfile::Always);
        let limits = ProtocolLimits::default();
        let (mut sink, _, _) = WalSink::open(config, 0, limits).unwrap();
        let published = sink.published();

        set(&mut sink, b"one", b"v").unwrap();
        sink.begin_group();
        set(&mut sink, b"a", b"v").unwrap();
        // The batch's write fails (a full disk): its record stays staged for recovery.
        super::super::crash::arm_io_fault(super::super::crash::CrashSite::Append);
        assert!(sink.end_group().is_err());
        sink.last_recovery = None;
        sink.begin_group();
        set(&mut sink, b"b", b"v").unwrap();
        // Recovery wrote the staged record again, but no sync covers it yet.
        assert_eq!(published.load(Ordering::Acquire), 1);
        sink.end_group().unwrap();
        sink.sync_pending().unwrap();
        assert_eq!(published.load(Ordering::Acquire), 3);
    }

    #[test]
    fn no_background_sync_vouches_for_records_after_a_failed_sync() {
        let directory = tempdir().unwrap();
        let config = config(directory.path(), DurabilityProfile::Everysec);
        let limits = ProtocolLimits::default();
        let (mut sink, _, _) = WalSink::open(config, 0, limits).unwrap();

        sink.begin_group();
        set(&mut sink, b"a", b"v").unwrap();
        // A sync of the batch's bytes fails; the kernel may drop their pages.
        super::super::crash::arm_io_fault(super::super::crash::CrashSite::Sync);
        assert!(sink.flush().is_err());
        assert!(sink.sync_failed);
        std::thread::sleep(std::time::Duration::from_millis(5));
        let _ = sink.end_group();
        wait_background_sync_bounded(&sink);
        // A later background sync would come back clean for those pages.
        assert_eq!(sink.durable.load(Ordering::Acquire), 0);
    }

    #[test]
    fn abandoned_always_group_is_synced_and_published() {
        let directory = tempdir().unwrap();
        let config = config(directory.path(), DurabilityProfile::Always);
        let limits = ProtocolLimits::default();
        let (mut sink, _, _) = WalSink::open(config, 0, limits).unwrap();
        let published = sink.published();

        sink.begin_group();
        let start = sink.last_sequence();
        set(&mut sink, b"a", b"v").unwrap();
        // The client went away before the commit: no one else may sync this shard soon.
        sink.abandon_group(start).unwrap();
        assert_eq!(published.load(Ordering::Acquire), 1);
        assert_eq!(sink.durable.load(Ordering::Acquire), 1);
        assert_eq!(sink.dirty_bytes(), 0);
    }

    #[test]
    fn always_group_reaches_replicas_only_once_durable() {
        let directory = tempdir().unwrap();
        let config = config(directory.path(), DurabilityProfile::Always);
        let limits = ProtocolLimits::default();
        let (mut sink, _, _) = WalSink::open(config, 0, limits).unwrap();
        let published = sink.published();

        set(&mut sink, b"one", b"v").unwrap();
        assert_eq!(published.load(Ordering::Acquire), 1);
        sink.begin_group();
        set(&mut sink, b"ga", b"v").unwrap();
        sink.end_group().unwrap();
        // Not synced yet: a shipped record could be cut below, and a restarted primary
        // would reuse its sequence for another write.
        assert_eq!(published.load(Ordering::Acquire), 1);
        super::super::crash::arm_io_fault(super::super::crash::CrashSite::Sync);
        assert!(sink.sync_pending().is_err());
        assert_eq!(published.load(Ordering::Acquire), 1);
    }

    #[test]
    fn always_group_is_published_after_its_sync() {
        let directory = tempdir().unwrap();
        let config = config(directory.path(), DurabilityProfile::Always);
        let limits = ProtocolLimits::default();
        let (mut sink, _, _) = WalSink::open(config, 0, limits).unwrap();
        let published = sink.published();

        sink.begin_group();
        set(&mut sink, b"ga", b"v").unwrap();
        set(&mut sink, b"gb", b"v").unwrap();
        sink.end_group().unwrap();
        sink.sync_pending().unwrap();
        assert_eq!(published.load(Ordering::Acquire), 2);
        // A group that wrote nothing has nothing to wait for.
        sink.begin_group();
        sink.end_group().unwrap();
        sink.sync_pending().unwrap();
        assert_eq!(published.load(Ordering::Acquire), 2);
    }

    #[test]
    fn sync_worker_failure_is_not_recovered_in_place() {
        let directory = tempdir().unwrap();
        let mut config = config(directory.path(), DurabilityProfile::Always);
        config.sync_worker_active = true;
        let limits = ProtocolLimits::default();
        let (mut sink, _, _) = WalSink::open(config.clone(), 0, limits).unwrap();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        sink.begin_group();
        set(&mut sink, b"one", b"v").unwrap();
        sink.end_group().unwrap();
        let barrier = sink.prepare_sync_barrier().unwrap().unwrap();
        block_on_bounded(&runtime, barrier.wait()).unwrap();
        sink.sync_worker.as_ref().unwrap().fail_sync_for_test();
        sink.begin_group();
        set(&mut sink, b"two", b"v").unwrap();
        sink.end_group().unwrap();
        let barrier = sink.prepare_sync_barrier().unwrap().unwrap();
        assert!(block_on_bounded(&runtime, barrier.wait()).is_err());
        sink.last_recovery = None;
        assert!(set(&mut sink, b"three", b"v").is_err());
        assert!(sink.sync_failed);
        drop(sink);

        let (_sink, mutations, _) = WalSink::open(config, 0, limits).unwrap();

        assert_eq!(keys_of(&mutations), [&b"one"[..], b"two"]);
    }

    #[test]
    fn recovery_sync_failure_is_not_retried_in_place() {
        let directory = tempdir().unwrap();
        let config = bulk_config(directory.path());
        let limits = ProtocolLimits::default();
        let (mut sink, _, _) = WalSink::open(config.clone(), 0, limits).unwrap();

        set(&mut sink, b"one", b"v").unwrap();
        sink.begin_group();
        set(&mut sink, b"two", b"v").unwrap();
        super::super::crash::arm_io_fault(super::super::crash::CrashSite::Append);
        assert!(sink.end_group().is_err());
        // Recovery syncs "one" before continuing; that sync fails.
        super::super::crash::arm_io_fault(super::super::crash::CrashSite::Sync);
        assert!(set(&mut sink, b"three", b"v").is_err());
        assert!(sink.sync_failed);
        // A retry would sync again and could see a clean result over lost pages.
        sink.last_recovery = None;
        assert!(set(&mut sink, b"three", b"v").is_err());
        drop(sink);

        let (_sink, mutations, _) = WalSink::open(config, 0, limits).unwrap();

        assert_eq!(keys_of(&mutations), [&b"one"[..]]);
    }

    #[test]
    fn always_group_write_failure_and_failed_recovery_write_keep_synced_records() {
        let directory = tempdir().unwrap();
        let config = config(directory.path(), DurabilityProfile::Always);
        let limits = ProtocolLimits::default();
        let (mut sink, _, _) = WalSink::open(config.clone(), 0, limits).unwrap();

        set(&mut sink, b"k1", b"v").unwrap();
        set(&mut sink, b"k2", b"v").unwrap();
        sink.begin_group();
        set(&mut sink, b"ga", b"v").unwrap();
        set(&mut sink, b"gb", b"v").unwrap();
        super::super::crash::arm_io_fault(super::super::crash::CrashSite::Append);
        assert!(sink.end_group().is_err());
        // The next record triggers recovery, whose own write of the kept batch fails too.
        super::super::crash::arm_io_fault(super::super::crash::CrashSite::Append);
        assert!(set(&mut sink, b"k3", b"v").is_err());
        assert!(!sink.sync_failed);

        // A restart now still has the synced records.
        let copy = directory.path().join("restart-copy");
        let source = shard_wal_directory(directory.path(), 0);
        fs::create_dir_all(shard_wal_directory(&copy, 0)).unwrap();
        for path in discover_segments(&source).unwrap() {
            fs::copy(
                &path,
                shard_wal_directory(&copy, 0).join(path.file_name().unwrap()),
            )
            .unwrap();
        }
        let (_copy_sink, mutations, _) =
            WalSink::open(config_at(&config, &copy), 0, limits).unwrap();
        // The torn half of the batch may hold whole records; those replay too.
        assert_eq!(keys_of(&mutations)[..2], [&b"k1"[..], b"k2"]);

        // Once the disk accepts writes, the batch is written before the next record.
        sink.last_recovery = None;
        set(&mut sink, b"k3", b"v").unwrap();
        drop(sink);

        let (_sink, mutations, _) = WalSink::open(config, 0, limits).unwrap();
        assert_eq!(
            keys_of(&mutations),
            [&b"k1"[..], b"k2", b"ga", b"gb", b"k3"]
        );
    }

    /// Two connections close their groups in one scheduler turn: the first to resume writes
    /// both records with one `write`, and the second finds its record already written.
    #[test]
    fn everysec_tick_hold_writes_the_turns_records_once() {
        let directory = tempdir().unwrap();
        let config = config(directory.path(), DurabilityProfile::Everysec);
        let limits = ProtocolLimits::default();
        let (mut sink, _, _) = WalSink::open(config.clone(), 0, limits).unwrap();
        let mut tickets = Vec::new();

        for key in [&b"a"[..], b"b"] {
            sink.begin_group();
            set(&mut sink, key, b"v").unwrap();
            assert!(sink.reply_waits_for_publish());
            sink.hold_for_tick();
            sink.end_group().unwrap();
            tickets.push(sink.last_sequence());
        }
        // An expiry's record, written outside any group, waits for the turn too.
        set(&mut sink, b"c", b"v").unwrap();
        assert_eq!(sink.write_pos, WAL_HEADER_LEN as u64);

        sink.publish_through(tickets[0]).unwrap();
        let written = sink.write_pos;

        assert!(written > WAL_HEADER_LEN as u64);
        assert!(sink.staged.is_empty());
        sink.publish_through(tickets[1]).unwrap();
        assert_eq!(sink.write_pos, written);
        assert_eq!(sink.group, 0);
        drop(sink);

        let (_sink, mutations, _) = WalSink::open(config, 0, limits).unwrap();
        assert_eq!(keys_of(&mutations), [&b"a"[..], b"b", b"c"]);
    }

    /// The turn's shared `write` fails: every connection whose record it carried gets the
    /// error (no reply), and the records stay staged for recovery, as with each
    /// connection's own failed `write` before.
    #[test]
    fn everysec_tick_hold_failure_cuts_every_waiter() {
        let directory = tempdir().unwrap();
        let config = config(directory.path(), DurabilityProfile::Everysec);
        let limits = ProtocolLimits::default();
        let (mut sink, _, _) = WalSink::open(config.clone(), 0, limits).unwrap();
        let mut tickets = Vec::new();

        for key in [&b"a"[..], b"b"] {
            sink.begin_group();
            set(&mut sink, key, b"v").unwrap();
            sink.hold_for_tick();
            sink.end_group().unwrap();
            tickets.push(sink.last_sequence());
        }
        super::super::crash::arm_io_fault(super::super::crash::CrashSite::Append);
        assert!(sink.publish_through(tickets[0]).is_err());
        assert!(sink.publish_through(tickets[1]).is_err());
        // A later reply, after the failure, does not wait and is not refused.
        assert!(!sink.reply_waits_for_publish());
        assert_eq!(sink.group, 0);

        sink.last_recovery = None;
        set(&mut sink, b"c", b"v").unwrap();
        assert!(sink.failure().is_none());
        drop(sink);

        let (_sink, mutations, _) = WalSink::open(config, 0, limits).unwrap();
        assert_eq!(keys_of(&mutations), [&b"a"[..], b"b", b"c"]);
    }

    #[test]
    fn staged_records_of_mixed_sizes_recover_after_a_failed_write() {
        let directory = tempdir().unwrap();
        let config = PersistenceConfig {
            wal_segment_bytes: 1 << 20,
            ..config(directory.path(), DurabilityProfile::Always)
        };
        let limits = ProtocolLimits::default();
        let (mut sink, _, _) = WalSink::open(config.clone(), 0, limits).unwrap();

        set(&mut sink, b"k1", b"v").unwrap();
        set(&mut sink, b"k2", &[b'x'; 300]).unwrap();
        sink.begin_group();
        set(&mut sink, b"ga", &[b'y'; 1_777]).unwrap();
        set(&mut sink, b"gb", b"v").unwrap();
        super::super::crash::arm_io_fault(super::super::crash::CrashSite::Append);
        assert!(sink.end_group().is_err());
        for attempt in 0..3 {
            sink.last_recovery = None;
            if attempt < 2 {
                super::super::crash::arm_io_fault(super::super::crash::CrashSite::Append);
                assert!(set(&mut sink, b"k3", &[b'z'; 33]).is_err());
            } else {
                set(&mut sink, b"k3", &[b'z'; 33]).unwrap();
            }
        }
        assert!(sink.failure().is_none());
        drop(sink);

        let (_sink, mutations, report) = WalSink::open(config, 0, limits).unwrap();
        assert_eq!(
            keys_of(&mutations),
            [&b"k1"[..], b"k2", b"ga", b"gb", b"k3"]
        );
        assert_eq!(report.last_sequence, 5);
    }

    #[test]
    fn write_failure_after_synced_but_unsettled_bytes_keeps_them() {
        // Background `everysec` sync finished but not yet settled, and sync-worker barriers
        // whose bytes are still counted dirty: recovery must cut nothing that was written.
        for worker in [false, true] {
            let directory = tempdir().unwrap();
            let mut config = config(
                directory.path(),
                if worker {
                    DurabilityProfile::Always
                } else {
                    DurabilityProfile::Everysec
                },
            );
            config.sync_worker_active = worker;
            config.wal_segment_bytes = 1 << 20;
            let limits = ProtocolLimits::default();
            let (mut sink, _, _) = WalSink::open(config.clone(), 0, limits).unwrap();
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();

            for key in [&b"k1"[..], b"k2"] {
                sink.begin_group();
                set(&mut sink, key, &[b'v'; 700]).unwrap();
                std::thread::sleep(std::time::Duration::from_millis(5));
                sink.end_group().unwrap();
                if worker {
                    let barrier = sink.prepare_sync_barrier().unwrap().unwrap();
                    block_on_bounded(&runtime, barrier.wait()).unwrap();
                } else {
                    wait_background_sync_bounded(&sink);
                }
            }
            assert!(sink.dirty_bytes() > 0, "worker {worker}");
            sink.begin_group();
            set(&mut sink, b"ga", &[b'g'; 123]).unwrap();
            super::super::crash::arm_io_fault(super::super::crash::CrashSite::Append);
            assert!(sink.end_group().is_err());
            set(&mut sink, b"k3", b"v").unwrap();
            assert!(sink.failure().is_none(), "worker {worker}");
            sink.flush().unwrap();
            drop(sink);

            let (_sink, mutations, _) = WalSink::open(config, 0, limits).unwrap();
            assert_eq!(
                keys_of(&mutations),
                [&b"k1"[..], b"k2", b"ga", b"k3"],
                "worker {worker}"
            );
        }
    }

    #[test]
    fn records_written_under_a_larger_command_limit_still_replay_and_ship() {
        let directory = tempdir().unwrap();
        let mut config = config(directory.path(), DurabilityProfile::Always);

        config.wal_segment_bytes = 1 << 20;
        let roomy = ProtocolLimits::default();
        let (mut sink, _, _) = WalSink::open(config.clone(), 0, roomy).unwrap();
        let value = vec![b'v'; 64 * 1024];

        sink.record(Mutation::SetBytes {
            key: b"big",
            value: &value,
        })
        .unwrap();
        drop(sink);

        // The operator lowers the limits afterwards (or a replica runs with smaller ones).
        let tight = ProtocolLimits {
            max_command_bytes: 1024,
            max_value_bytes: 512,
            max_connection_buffer_bytes: 1024,
            ..roomy
        };
        let (_sink, mutations, _) = WalSink::open(config.clone(), 0, tight).unwrap();

        assert_eq!(keys_of(&mutations), [&b"big"[..]]);
        let FrameRead::Frames(frames) = read_frames(directory.path(), 0, 0, 1, tight).unwrap()
        else {
            panic!("frames expected");
        };
        let replica = tempdir().unwrap();
        let (mut replica_sink, _, _) =
            WalSink::open(config_at(&config, replica.path()), 0, tight).unwrap();

        replica_sink.append_frame(&frames[0]).unwrap();
    }

    #[test]
    fn full_disk_during_append_keeps_the_acked_prefix_and_resumes() {
        let directory = tempdir().unwrap();
        let config = config(directory.path(), DurabilityProfile::Always);
        let limits = ProtocolLimits::default();
        let (mut sink, _, _) = WalSink::open(config.clone(), 0, limits).unwrap();

        sink.record(Mutation::SetBytes {
            key: b"kept",
            value: b"1",
        })
        .unwrap();
        super::super::crash::arm_io_fault(super::super::crash::CrashSite::Append);
        let denied = sink.record(Mutation::SetBytes {
            key: b"lost",
            value: b"2",
        });

        assert!(
            matches!(denied, Err(PersistenceError::Failed(ref message)) if message.contains("space")),
            "{denied:?}"
        );
        assert!(sink.failure().is_some());

        // The disk accepts writes again: the torn tail is dropped and logging continues.
        sink.record(Mutation::SetBytes {
            key: b"later",
            value: b"3",
        })
        .unwrap();
        assert!(sink.failure().is_none());
        drop(sink);

        let (_sink, mutations, report) = WalSink::open(config, 0, limits).unwrap();

        assert_eq!(mutations.len(), 2);
        assert_eq!(report.torn_tail_bytes, 0);
        assert!(matches!(
            &mutations[0].1,
            OwnedMutation::SetBytes { key, value } if key == b"kept" && value == b"1"
        ));
        assert!(matches!(
            &mutations[1].1,
            OwnedMutation::SetBytes { key, value } if key == b"later" && value == b"3"
        ));
    }

    #[test]
    fn readonly_wal_directory_blocks_rotation_and_keeps_acked_records() {
        let directory = tempdir().unwrap();
        let mut config = config(directory.path(), DurabilityProfile::Always);

        config.wal_segment_bytes = 4096;
        let limits = ProtocolLimits::default();
        let (mut sink, _, _) = WalSink::open(config.clone(), 0, limits).unwrap();
        let value = vec![b'x'; 2_000];

        sink.record(Mutation::SetBytes {
            key: b"kept",
            value: &value,
        })
        .unwrap();

        let shard_dir = shard_wal_directory(directory.path(), 0);
        let original = fs::metadata(&shard_dir).unwrap().permissions();
        let mut permissions = original.clone();

        permissions.set_readonly(true);
        fs::set_permissions(&shard_dir, permissions).unwrap();

        let probe = shard_dir.join("permission-probe");

        if fs::File::create(&probe).is_ok() {
            // A privileged user (for example root in a container) ignores directory modes.
            fs::remove_file(&probe).unwrap();
            fs::set_permissions(&shard_dir, original).unwrap();
            return;
        }

        let denied = sink.record(Mutation::SetBytes {
            key: b"lost",
            value: &value,
        });

        fs::set_permissions(&shard_dir, original).unwrap();
        assert!(
            matches!(denied, Err(PersistenceError::Io(ref error)) if error.kind() == std::io::ErrorKind::PermissionDenied),
            "{denied:?}"
        );
        drop(sink);

        let (_sink, mutations, _) = WalSink::open(config, 0, limits).unwrap();

        assert_eq!(mutations.len(), 1);
        assert!(matches!(
            &mutations[0].1,
            OwnedMutation::SetBytes { key, .. } if key == b"kept"
        ));
    }

    #[test]
    fn appends_recovers_and_rotates_canonical_mutations() {
        let directory = tempdir().unwrap();
        let config = bulk_config(directory.path());
        let limits = ProtocolLimits::default();
        let (mut sink, mutations, report) = WalSink::open(config.clone(), 0, limits).unwrap();

        assert!(mutations.is_empty());
        assert_eq!(report.last_sequence, 0);

        for index in 0..100 {
            sink.record(Mutation::SetBytes {
                key: b"key",
                value: &[index; 64],
            })
            .unwrap();
        }

        sink.flush().unwrap();

        sink.flush().unwrap();
        drop(sink);

        let (_sink, mutations, report) = WalSink::open(config, 0, limits).unwrap();

        assert_eq!(mutations.len(), 100);
        assert_eq!(report.last_sequence, 100);
        assert!(
            fs::read_dir(shard_wal_directory(directory.path(), 0))
                .unwrap()
                .count()
                > 1
        );
    }

    #[test]
    fn a_segment_from_an_older_minor_is_not_appended_to() {
        let directory = tempdir().unwrap();
        let config = config(directory.path(), DurabilityProfile::Everysec);
        let limits = ProtocolLimits::default();
        let delete = |sink: &mut WalSink, key: &'static [u8]| {
            sink.record(Mutation::Delete {
                keys: &[Cow::Borrowed(key)],
            })
            .unwrap();
            sink.flush().unwrap();
        };
        let (mut sink, _, _) = WalSink::open(config.clone(), 0, limits).unwrap();

        delete(&mut sink, b"a");
        delete(&mut sink, b"b");
        drop(sink);

        let wal_dir = shard_wal_directory(directory.path(), 0);
        let old = segment_path(&wal_dir, 1);
        let mut bytes = fs::read(&old).unwrap();
        let header = super::super::codec::encode_segment_header_at(
            1,
            FORMAT_MINOR - 1,
            0,
            1,
            1,
            u64::from_le_bytes(bytes[36..44].try_into().unwrap()),
            0,
        );
        bytes[..WAL_HEADER_LEN].copy_from_slice(&header);
        fs::write(&old, &bytes).unwrap();

        let (mut sink, mutations, _) = WalSink::open(config.clone(), 0, limits).unwrap();
        assert_eq!(mutations.len(), 2);
        delete(&mut sink, b"c");
        drop(sink);

        let minor = |path: &Path| {
            let bytes = fs::read(path).unwrap();
            u16::from_le_bytes(bytes[10..12].try_into().unwrap())
        };
        assert_eq!(minor(&old), FORMAT_MINOR - 1);
        assert_eq!(minor(&segment_path(&wal_dir, 3)), FORMAT_MINOR);

        let (_sink, mutations, report) = WalSink::open(config, 0, limits).unwrap();
        assert_eq!(mutations.len(), 3);
        assert_eq!(report.last_sequence, 3);
    }

    #[test]
    fn preallocated_tail_is_not_a_torn_record_and_a_torn_record_in_it_is_dropped() {
        let directory = tempdir().unwrap();
        let config = config(directory.path(), DurabilityProfile::Everysec);
        let limits = ProtocolLimits::default();
        let (mut sink, _, _) = WalSink::open(config.clone(), 0, limits).unwrap();

        for key in [&b"a"[..], b"b", b"c"] {
            sink.record(Mutation::Delete {
                keys: &[Cow::Borrowed(key)],
            })
            .unwrap();
        }
        sink.flush().unwrap();

        // Crash image taken while the sink is alive: on Linux the file still has its zero tail.
        let image = tempdir().unwrap();
        let live = segment_path(&shard_wal_directory(directory.path(), 0), 1);
        let copy_dir = shard_wal_directory(image.path(), 0);
        fs::create_dir_all(&copy_dir).unwrap();
        let copy = copy_dir.join(live.file_name().unwrap());
        fs::copy(&live, &copy).unwrap();
        let exact = sink.write_pos;
        drop(sink);

        let (sink, mutations, report) = WalSink::open(config.clone(), 0, limits).unwrap();
        assert_eq!(mutations.len(), 3);
        assert_eq!(report.torn_tail_bytes, 0);
        drop(sink);
        assert_eq!(fs::metadata(&live).unwrap().len(), exact);

        let mut image_config = config.clone();
        image_config.data_dir = image.path().to_path_buf();
        let (sink, mutations, report) = WalSink::open(image_config.clone(), 0, limits).unwrap();
        assert_eq!(mutations.len(), 3);
        assert_eq!(report.torn_tail_bytes, 0);
        drop(sink);
        assert_eq!(fs::metadata(&copy).unwrap().len(), exact);

        // A record cut in the middle, with zeros after it as in a preallocated file.
        let mut bytes = fs::read(&copy).unwrap();
        let cut = bytes.len() - 3;
        bytes[cut] ^= 0xff;
        bytes.extend_from_slice(&[0; 512]);
        fs::write(&copy, &bytes).unwrap();
        let (_sink, mutations, report) = WalSink::open(image_config, 0, limits).unwrap();
        assert_eq!(mutations.len(), 2);
        assert!(report.torn_tail_bytes > 0);
    }

    #[test]
    fn truncates_only_a_torn_final_record() {
        let directory = tempdir().unwrap();
        let config = config(directory.path(), DurabilityProfile::Everysec);
        let limits = ProtocolLimits::default();
        let (mut sink, _, _) = WalSink::open(config.clone(), 0, limits).unwrap();

        sink.record(Mutation::Delete {
            keys: &[Cow::Borrowed(b"key")],
        })
        .unwrap();
        drop(sink);

        let path = segment_path(&shard_wal_directory(directory.path(), 0), 1);
        let original = fs::metadata(&path).unwrap().len();

        OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap()
            .write_all(b"\x20\x00")
            .unwrap();

        let (_sink, mutations, report) = WalSink::open(config, 0, limits).unwrap();

        assert_eq!(mutations.len(), 1);
        assert_eq!(report.torn_tail_bytes, 2);
        assert_eq!(fs::metadata(path).unwrap().len(), original);
    }

    #[test]
    fn discards_a_truncated_header_on_the_final_segment() {
        let directory = tempdir().unwrap();
        let config = config(directory.path(), DurabilityProfile::Always);
        let limits = ProtocolLimits::default();
        let (mut sink, _, _) = WalSink::open(config.clone(), 0, limits).unwrap();

        sink.record(Mutation::SetBytes {
            key: b"kept",
            value: b"yes",
        })
        .unwrap();
        drop(sink);

        let abandoned = segment_path(&shard_wal_directory(directory.path(), 0), 2);

        fs::write(&abandoned, b"RUVWAL").unwrap();

        let (_sink, mutations, report) = WalSink::open(config, 0, limits).unwrap();

        assert_eq!(mutations.len(), 1);
        assert_eq!(report.torn_tail_bytes, 6);
        assert!(!abandoned.exists());
    }

    /// A power cut between writing a new segment's header and its sync can leave the file with
    /// its length but zeros for content. No record was ever acknowledged in it (`create_segment`
    /// syncs the header before any record is appended), so recovery drops it like a short header.
    #[test]
    fn discards_a_never_synced_zero_header_on_the_final_segment() {
        for length in [WAL_HEADER_LEN, WAL_HEADER_LEN + 64] {
            let directory = tempdir().unwrap();
            let config = config(directory.path(), DurabilityProfile::Always);
            let limits = ProtocolLimits::default();
            let (mut sink, _, _) = WalSink::open(config.clone(), 0, limits).unwrap();

            sink.record(Mutation::SetBytes {
                key: b"kept",
                value: b"yes",
            })
            .unwrap();
            drop(sink);

            let abandoned = segment_path(&shard_wal_directory(directory.path(), 0), 2);

            fs::write(&abandoned, vec![0u8; length]).unwrap();

            let (mut sink, mutations, report) = WalSink::open(config.clone(), 0, limits).unwrap();

            assert_eq!(mutations.len(), 1);
            assert_eq!(report.torn_tail_bytes, length as u64);
            assert!(!abandoned.exists());

            // The next record takes the abandoned segment's sequence and survives a restart.
            sink.record(Mutation::SetBytes {
                key: b"next",
                value: b"yes",
            })
            .unwrap();
            drop(sink);

            let (_sink, mutations, _) = WalSink::open(config, 0, limits).unwrap();

            assert_eq!(mutations.len(), 2);
        }
    }

    #[test]
    fn a_zero_header_with_data_after_it_or_before_the_last_segment_is_corruption() {
        let directory = tempdir().unwrap();
        let config = config(directory.path(), DurabilityProfile::Always);
        let limits = ProtocolLimits::default();
        let (mut sink, _, _) = WalSink::open(config.clone(), 0, limits).unwrap();

        sink.record(Mutation::SetBytes {
            key: b"kept",
            value: b"yes",
        })
        .unwrap();
        drop(sink);

        let shard_dir = shard_wal_directory(directory.path(), 0);
        let zeroed = segment_path(&shard_dir, 2);
        let mut bytes = vec![0u8; WAL_HEADER_LEN + 16];

        bytes[WAL_HEADER_LEN + 3] = 7;
        fs::write(&zeroed, &bytes).unwrap();

        assert!(matches!(
            WalSink::open(config.clone(), 0, limits),
            Err(PersistenceError::Corruption(_))
        ));

        // A zeroed header followed by a later segment is not a torn creation either.
        fs::write(&zeroed, vec![0u8; WAL_HEADER_LEN]).unwrap();
        fs::write(segment_path(&shard_dir, 3), encode_segment_header(0, 3, 3)).unwrap();

        assert!(matches!(
            WalSink::open(config, 0, limits),
            Err(PersistenceError::Corruption(_))
        ));
    }

    /// Inside an `always` group, `record_now` writes the records staged ahead of it first and
    /// then its own, so the log keeps sequence order and a failed write reaches the script or
    /// transaction that can still roll back. Its sync still waits for the group's commit.
    /// Other profiles stage it like `record`.
    #[test]
    fn record_now_writes_the_staged_records_before_its_own() {
        let limits = ProtocolLimits::default();
        let batch = [OwnedMutation::SetBytes {
            key: b"scripted".to_vec(),
            value: b"2".to_vec(),
        }];

        for profile in [DurabilityProfile::Always, DurabilityProfile::Everysec] {
            let directory = tempdir().unwrap();
            let config = config(directory.path(), profile);
            let (mut sink, _, _) = WalSink::open(config.clone(), 0, limits).unwrap();
            let on_disk = || {
                inspect_shard_wal(directory.path(), 0, limits, config.wal_segment_bytes, 0)
                    .unwrap()
                    .0
            };

            sink.begin_group();
            set(&mut sink, b"staged", b"1").unwrap();
            assert!(on_disk().is_empty(), "{profile:?}");
            sink.record_now(Mutation::Transaction { mutations: &batch })
                .unwrap();

            if profile == DurabilityProfile::Always {
                assert!(sink.staged.is_empty());
                assert_eq!(
                    on_disk(),
                    [
                        (
                            0,
                            OwnedMutation::SetBytes {
                                key: b"staged".to_vec(),
                                value: b"1".to_vec(),
                            }
                        ),
                        // A batch of one write is stored as that write.
                        (0, batch[0].clone()),
                    ]
                );
                // Written, not yet synced: the group's commit syncs it.
                assert!(sink.durable_sequence() < sink.last_sequence());
            } else {
                assert!(on_disk().is_empty());
            }

            sink.end_group().unwrap();
            sink.sync_pending().unwrap();
            assert_eq!(on_disk().len(), 2, "{profile:?}");
        }
    }

    #[test]
    fn inspect_does_not_truncate_a_torn_final_record() {
        let directory = tempdir().unwrap();
        let config = config(directory.path(), DurabilityProfile::Everysec);
        let limits = ProtocolLimits::default();
        let (mut sink, _, _) = WalSink::open(config.clone(), 0, limits).unwrap();

        sink.record(Mutation::Delete {
            keys: &[Cow::Borrowed(b"key")],
        })
        .unwrap();
        drop(sink);

        let path = segment_path(&shard_wal_directory(directory.path(), 0), 1);

        OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap()
            .write_all(b"\x20\x00")
            .unwrap();
        let torn_length = fs::metadata(&path).unwrap().len();

        let (mutations, report) =
            inspect_shard_wal(directory.path(), 0, limits, config.wal_segment_bytes, 0).unwrap();

        assert_eq!(mutations.len(), 1);
        assert_eq!(report.torn_tail_bytes, 2);
        assert_eq!(fs::metadata(path).unwrap().len(), torn_length);
    }

    #[test]
    fn zeroed_record_length_before_data_in_the_last_segment_is_a_torn_tail() {
        let directory = tempdir().unwrap();
        let config = config(directory.path(), DurabilityProfile::Always);
        let limits = ProtocolLimits::default();
        let (mut sink, _, _) = WalSink::open(config.clone(), 0, limits).unwrap();

        set(&mut sink, b"one", b"v").unwrap();
        let cut_at = sink.write_pos;
        set(&mut sink, b"two", b"v").unwrap();
        drop(sink);

        // The last record's length is gone and the rest of it is garbage: a torn tail.
        let path = segment_path(&shard_wal_directory(directory.path(), 0), 1);
        let mut bytes = fs::read(&path).unwrap();
        bytes[cut_at as usize..cut_at as usize + 4].fill(0);
        bytes[cut_at as usize + 10] ^= 0xff;
        fs::write(&path, &bytes).unwrap();

        let (_, inspected) = inspect_shard_wal(directory.path(), 0, limits, 4096, 0).unwrap();
        assert_eq!(inspected.last_sequence, 1);
        assert_eq!(fs::metadata(&path).unwrap().len(), bytes.len() as u64);

        let (mut sink, mutations, report) = WalSink::open(config.clone(), 0, limits).unwrap();
        assert_eq!(keys_of(&mutations), [&b"one"[..]]);
        assert_eq!(report.torn_tail_bytes, bytes.len() as u64 - cut_at);
        assert_eq!(fs::metadata(&path).unwrap().len(), cut_at);
        set(&mut sink, b"four", b"v").unwrap();
        drop(sink);
        let (_sink, mutations, _) = WalSink::open(config, 0, limits).unwrap();
        assert_eq!(keys_of(&mutations), [&b"one"[..], b"four"]);
    }

    #[test]
    fn zeroed_record_length_before_intact_records_refuses_to_start() {
        let directory = tempdir().unwrap();
        let config = config(directory.path(), DurabilityProfile::Always);
        let limits = ProtocolLimits::default();
        let (mut sink, _, _) = WalSink::open(config.clone(), 0, limits).unwrap();

        set(&mut sink, b"one", b"v").unwrap();
        let cut_at = sink.write_pos;
        set(&mut sink, b"two", b"v").unwrap();
        let three_at = sink.write_pos;
        set(&mut sink, b"three", b"v").unwrap();
        drop(sink);

        // Mid-segment damage: synced records follow the zeroed length.
        let path = segment_path(&shard_wal_directory(directory.path(), 0), 1);
        let mut bytes = fs::read(&path).unwrap();
        bytes[cut_at as usize..cut_at as usize + 4].fill(0);
        fs::write(&path, &bytes).unwrap();

        let Err(PersistenceError::Corruption(message)) = WalSink::open(config.clone(), 0, limits)
        else {
            panic!("startup must refuse a segment with intact records after the damage");
        };
        assert!(message.contains(&format!("offset {cut_at}")), "{message}");
        assert!(
            message.contains(&format!("follows at offset {three_at}")),
            "{message}"
        );
        // Nothing was cut.
        assert_eq!(fs::read(&path).unwrap(), bytes);
    }

    #[test]
    fn zeroed_record_length_before_data_in_an_earlier_segment_is_corruption() {
        let directory = tempdir().unwrap();
        let config = config(directory.path(), DurabilityProfile::Always);
        let limits = ProtocolLimits::default();
        let (mut sink, _, _) = WalSink::open(config.clone(), 0, limits).unwrap();

        set(&mut sink, b"one", b"v").unwrap();
        let cut_at = sink.write_pos;
        set(&mut sink, b"two", b"v").unwrap();
        sink.rotate().unwrap();
        set(&mut sink, b"three", b"v").unwrap();
        drop(sink);

        let path = segment_path(&shard_wal_directory(directory.path(), 0), 1);
        let mut bytes = fs::read(&path).unwrap();
        bytes[cut_at as usize..cut_at as usize + 4].fill(0);
        fs::write(&path, &bytes).unwrap();

        assert!(matches!(
            WalSink::open(config, 0, limits),
            Err(PersistenceError::Corruption(_))
        ));
    }

    #[test]
    fn rejects_mid_log_checksum_corruption() {
        let directory = tempdir().unwrap();
        let config = config(directory.path(), DurabilityProfile::Always);
        let limits = ProtocolLimits::default();
        let (mut sink, _, _) = WalSink::open(config.clone(), 0, limits).unwrap();

        for key in [b"a".as_slice(), b"b".as_slice()] {
            sink.record(Mutation::SetBytes {
                key,
                value: b"value",
            })
            .unwrap();
        }

        drop(sink);

        let path = segment_path(&shard_wal_directory(directory.path(), 0), 1);
        let mut bytes = fs::read(&path).unwrap();

        bytes[WAL_HEADER_LEN + RECORD_MIN_LEN] ^= 1;
        fs::write(path, bytes).unwrap();

        assert!(matches!(
            WalSink::open(config, 0, limits),
            Err(PersistenceError::Corruption(_))
        ));
    }

    #[test]
    fn replays_every_supported_value_type() {
        let directory = tempdir().unwrap();
        let config = config(directory.path(), DurabilityProfile::Always);
        let limits = ProtocolLimits::default();
        let (mut sink, _, _) = WalSink::open(config.clone(), 0, limits).unwrap();

        sink.record(Mutation::SetBytes {
            key: b"bytes",
            value: b"value",
        })
        .unwrap();
        sink.record(Mutation::SetInteger {
            key: b"integer",
            value: 42,
        })
        .unwrap();
        sink.record(Mutation::ListPushFront {
            key: b"list",
            values: &[Cow::Borrowed(b"a"), Cow::Borrowed(b"b")],
        })
        .unwrap();
        sink.record(Mutation::SetAdd {
            key: b"set",
            members: &[Cow::Borrowed(b"a"), Cow::Borrowed(b"b")],
        })
        .unwrap();
        sink.record(Mutation::HashSet {
            key: b"hash",
            pairs: &[(Cow::Borrowed(b"field"), Cow::Borrowed(b"value"))],
        })
        .unwrap();
        sink.record(Mutation::SortedSetAdd {
            key: b"sorted",
            pairs: &[(1.0, Cow::Borrowed(b"member"))],
        })
        .unwrap();
        sink.record(Mutation::BloomReserve {
            key: b"bloom",
            error_rate: 0.01,
            capacity: 100,
            expansion: 2,
        })
        .unwrap();
        sink.record(Mutation::BloomAdd {
            key: b"bloom",
            value: b"member",
        })
        .unwrap();
        sink.record(Mutation::Transaction {
            mutations: &[
                OwnedMutation::SetBytes {
                    key: b"tx-a".to_vec(),
                    value: b"1".to_vec(),
                },
                OwnedMutation::SetInteger {
                    key: b"tx-i".to_vec(),
                    value: 2,
                },
            ],
        })
        .unwrap();
        drop(sink);

        let (_sink, mutations, report) = WalSink::open(config, 0, limits).unwrap();
        let mut shard = crate::KvShard::new();

        for (database, mutation) in &mutations {
            super::super::apply_mutation(&mut shard, *database, mutation).unwrap();
        }

        assert_eq!(report.records_replayed, 9);
        assert_eq!(
            shard.get_bytes(b"bytes").unwrap(),
            Some(b"value".as_slice())
        );
        assert_eq!(
            shard.copied_value(b"integer").as_deref(),
            Some(b"42".as_slice())
        );
        assert_eq!(shard.list_range(b"list", 0, -1).unwrap(), [b"b", b"a"]);
        assert!(shard.set_contains(b"set", b"a").unwrap());
        assert_eq!(
            shard.hash_get(b"hash", b"field").unwrap(),
            Some(b"value".as_slice())
        );
        assert_eq!(
            shard.sorted_set_range(b"sorted", 0, -1).unwrap(),
            [b"member"]
        );
        assert!(shard.bloom_contains(b"bloom", b"member").unwrap());
        assert_eq!(
            shard.copied_value(b"tx-a").as_deref(),
            Some(b"1".as_slice())
        );
        assert_eq!(
            shard.copied_value(b"tx-i").as_deref(),
            Some(b"2".as_slice())
        );
    }

    #[test]
    fn missing_earlier_wal_segment_is_rejected() {
        let directory = tempdir().unwrap();
        let mut config = bulk_config(directory.path());

        config.wal_segment_bytes = 4096;
        let limits = ProtocolLimits::default();
        let (mut sink, _, _) = WalSink::open(config.clone(), 0, limits).unwrap();

        for index in 0..200 {
            let value = vec![index as u8; 64];

            sink.record(Mutation::SetBytes {
                key: b"key",
                value: &value,
            })
            .unwrap();
        }

        sink.flush().unwrap();

        drop(sink);
        let wal_dir = shard_wal_directory(directory.path(), 0);
        let mut segments: Vec<_> = fs::read_dir(&wal_dir)
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .collect();

        segments.sort();
        assert!(segments.len() >= 2);
        fs::remove_file(&segments[0]).unwrap();
        assert!(matches!(
            WalSink::open(config, 0, limits),
            Err(PersistenceError::Corruption(_))
        ));
    }

    #[test]
    fn open_after_replays_only_sequences_after_a_checkpoint() {
        let directory = tempdir().unwrap();
        let config = config(directory.path(), DurabilityProfile::Always);
        let limits = ProtocolLimits::default();
        let (mut sink, _, _) = WalSink::open(config.clone(), 0, limits).unwrap();

        for index in 0..10 {
            sink.record(Mutation::SetBytes {
                key: b"key",
                value: &[index; 8],
            })
            .unwrap();
        }

        drop(sink);

        let (_sink, mutations, report) = WalSink::open_after(config, 0, limits, 4).unwrap();

        assert_eq!(mutations.len(), 6);
        assert_eq!(report.records_replayed, 6);
        assert_eq!(report.last_sequence, 10);
    }

    #[test]
    fn install_base_drops_old_records_and_keeps_the_checkpoint_sequence() {
        let directory = tempdir().unwrap();
        let config = config(directory.path(), DurabilityProfile::Always);
        let limits = ProtocolLimits::default();
        let (mut sink, _, _) = WalSink::open(config.clone(), 0, limits).unwrap();

        sink.record(Mutation::SetBytes {
            key: b"k",
            value: b"old",
        })
        .unwrap();
        sink.install_base(4).unwrap();
        assert_eq!(sink.last_sequence(), 4);
        drop(sink);

        let (_sink, mutations, report) = WalSink::open_after(config, 0, limits, 4).unwrap();

        assert!(mutations.is_empty());
        assert_eq!(report.last_sequence, 4);
    }

    #[test]
    fn compact_keeps_the_change_stream_window_and_nothing_more() {
        let wal_size = |config: &PersistenceConfig| -> (u64, usize) {
            let directory = shard_wal_directory(&config.data_dir, 0);
            let sizes: Vec<u64> = fs::read_dir(directory)
                .unwrap()
                .map(|entry| entry.unwrap().metadata().unwrap().len())
                .collect();

            (sizes.iter().sum(), sizes.len())
        };
        let run = |retain: u64| {
            let directory = tempdir().unwrap();
            let mut config = bulk_config(directory.path());
            config.cdc_retain_bytes = retain;
            let (mut sink, _, _) =
                WalSink::open(config.clone(), 0, ProtocolLimits::default()).unwrap();

            for index in 0..200 {
                sink.record(Mutation::SetBytes {
                    key: b"key",
                    value: &[index as u8; 64],
                })
                .unwrap();
            }

            sink.flush().unwrap();
            let before = wal_size(&config);
            sink.compact(200).unwrap();

            (before, wal_size(&config), config.wal_segment_bytes)
        };

        let (before, off, _) = run(0);
        let (_, everything, _) = run(u64::MAX);
        let (_, windowed, segment_bytes) = run(2048);

        assert!(before.1 > 2, "test needs several segments");
        assert!(off.1 < before.1, "no window: covered segments go");
        assert!(
            everything.1 >= before.1,
            "a huge window keeps every segment"
        );
        assert!(windowed.1 > off.1, "a window holds back some segments");
        assert!(windowed.1 < before.1, "but not all of them");
        assert!(windowed.0 <= 2048 + segment_bytes);
    }

    #[test]
    fn compact_deletes_segments_covered_by_a_checkpoint() {
        let directory = tempdir().unwrap();
        let config = bulk_config(directory.path());
        let limits = ProtocolLimits::default();
        let (mut sink, _, _) = WalSink::open(config.clone(), 0, limits).unwrap();

        for index in 0..200 {
            let value = vec![index as u8; 64];

            sink.record(Mutation::SetBytes {
                key: b"key",
                value: &value,
            })
            .unwrap();
        }

        sink.flush().unwrap();

        let covered = 50;

        sink.compact(covered).unwrap();
        drop(sink);

        let wal_dir = shard_wal_directory(directory.path(), 0);
        let mut segments: Vec<_> = fs::read_dir(&wal_dir)
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .collect();

        segments.sort();
        assert!(!segments.is_empty());
        let first = segment_first_sequence(&segments[0]).unwrap();

        assert!(first > 1, "covered prefix segments should be deleted");

        let (_sink, mutations, report) =
            WalSink::open_after(config.clone(), 0, limits, covered).unwrap();

        assert_eq!(report.last_sequence, 200);
        assert_eq!(mutations.len(), 150);
        assert!(matches!(
            WalSink::open(config, 0, limits),
            Err(PersistenceError::Corruption(_))
        ));
    }

    #[test]
    fn compact_keeps_segments_a_replica_has_not_received() {
        let directory = tempdir().unwrap();
        let config = bulk_config(directory.path());
        let limits = ProtocolLimits::default();
        let (mut sink, _, _) = WalSink::open(config, 0, limits).unwrap();

        for index in 0..200 {
            let value = vec![index as u8; 64];

            sink.record(Mutation::SetBytes {
                key: b"key",
                value: &value,
            })
            .unwrap();
        }

        sink.flush().unwrap();

        let wal_dir = shard_wal_directory(directory.path(), 0);
        let before = fs::read_dir(&wal_dir).unwrap().count();

        sink.backlog_floor().store(0, Ordering::Release);
        sink.compact(sink.last_sequence()).unwrap();

        assert_eq!(fs::read_dir(&wal_dir).unwrap().count(), before);

        sink.backlog_floor().store(u64::MAX, Ordering::Release);
        sink.compact(50).unwrap();

        let mut segments: Vec<_> = fs::read_dir(&wal_dir)
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .collect();

        segments.sort();
        let first = segment_first_sequence(&segments[0]).unwrap();

        assert!(first > 1, "prefix is deleted once no replica holds it");
    }

    #[test]
    fn compact_drops_a_replica_hold_that_exceeds_the_budget() {
        let directory = tempdir().unwrap();
        let mut config = bulk_config(directory.path());
        config.replica_retain_max_bytes = 1024;
        let limits = ProtocolLimits::default();
        let (mut sink, _, _) = WalSink::open(config, 0, limits).unwrap();

        for index in 0..400 {
            let value = vec![index as u8; 64];

            sink.record(Mutation::SetBytes {
                key: b"key",
                value: &value,
            })
            .unwrap();
        }

        sink.flush().unwrap();

        let wal_dir = shard_wal_directory(directory.path(), 0);
        let before = fs::read_dir(&wal_dir).unwrap().count();
        let dropped = REPLICA_HOLDS_DROPPED.load(Ordering::Relaxed);

        sink.backlog_floor().store(0, Ordering::Release);
        sink.compact(sink.last_sequence()).unwrap();

        assert!(fs::read_dir(&wal_dir).unwrap().count() < before);
        assert!(REPLICA_HOLDS_DROPPED.load(Ordering::Relaxed) > dropped);
    }

    #[test]
    fn read_frames_skips_segments_before_the_requested_sequence() {
        let directory = tempdir().unwrap();
        let config = bulk_config(directory.path());
        let limits = ProtocolLimits::default();
        let (mut sink, _, _) = WalSink::open(config, 0, limits).unwrap();

        for index in 0..200 {
            let value = vec![index as u8; 64];

            sink.record(Mutation::SetBytes {
                key: b"key",
                value: &value,
            })
            .unwrap();
        }

        sink.flush().unwrap();

        let wal_dir = shard_wal_directory(directory.path(), 0);
        let mut segments: Vec<_> = fs::read_dir(&wal_dir)
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .collect();

        segments.sort();
        assert!(
            segments.len() > 1,
            "the log should span more than one segment"
        );

        let second = segment_first_sequence(&segments[1]).unwrap();

        fs::write(&segments[0], b"not a wal segment").unwrap();

        let read = read_frames(
            directory.path(),
            0,
            second.saturating_sub(1),
            sink.last_sequence(),
            limits,
        )
        .unwrap();

        assert!(matches!(read, FrameRead::Frames(frames) if !frames.is_empty()));
    }

    #[test]
    fn read_frames_chunk_advances_across_segments_without_collecting_the_whole_backlog() {
        let directory = tempdir().unwrap();
        let limits = ProtocolLimits::default();
        let (mut sink, _, _) = WalSink::open(bulk_config(directory.path()), 0, limits).unwrap();
        for index in 0..200 {
            sink.record(Mutation::SetBytes {
                key: b"key",
                value: &[index as u8; 64],
            })
            .unwrap();
        }
        sink.flush().unwrap();

        let mut cursor = 0;
        let mut chunks = 0;
        while cursor < sink.last_sequence() {
            let FrameRead::Frames(frames) = read_frames_chunk(
                directory.path(),
                0,
                cursor,
                sink.last_sequence(),
                limits,
                256,
            )
            .unwrap() else {
                panic!("missing WAL history");
            };
            let bytes: usize = frames.iter().map(Vec::len).sum();
            assert!(bytes - frames.last().unwrap().len() < 256);
            assert!(!frames.is_empty());
            for frame in frames {
                let record = decode_record(&frame, 0, limits).unwrap();
                assert_eq!(record.sequence, cursor + 1);
                cursor = record.sequence;
            }
            chunks += 1;
        }
        assert!(chunks > 1);
        assert_eq!(cursor, 200);
    }

    #[test]
    fn a_resumed_reader_continues_where_it_stopped_instead_of_rescanning() {
        let directory = tempdir().unwrap();
        let limits = ProtocolLimits::default();
        let (mut sink, _, _) = WalSink::open(bulk_config(directory.path()), 0, limits).unwrap();
        let write = |sink: &mut WalSink, count: u8| {
            for index in 0..count {
                sink.record(Mutation::SetBytes {
                    key: b"key",
                    value: &[index; 64],
                })
                .unwrap();
            }
            sink.flush().unwrap();
        };
        write(&mut sink, 20);

        let mut reader = FrameCursor::default();
        let mut last = 0;
        let follow = |reader: &mut FrameCursor, last: &mut u64, through: u64| {
            while *last < through {
                let FrameRead::Frames(frames) =
                    read_frames_resuming(reader, directory.path(), 0, *last, through, limits, 256)
                        .unwrap()
                else {
                    panic!("missing WAL history after {last}");
                };
                assert!(!frames.is_empty());
                for frame in frames {
                    let record = decode_record(&frame, 0, limits).unwrap();
                    assert_eq!(record.sequence, *last + 1);
                    *last = record.sequence;
                }
            }
        };
        follow(&mut reader, &mut last, 10);

        // Damage a record the reader already returned. A rescan from the segment start
        // trips over it; a reader that continues from its offset never looks at it again.
        let segment =
            discover_segments(&shard_wal_directory(directory.path(), 0)).unwrap()[0].clone();
        let mut bytes = fs::read(&segment).unwrap();
        bytes[WAL_HEADER_LEN + 12] ^= 0xff;
        fs::write(&segment, &bytes).unwrap();
        assert!(matches!(
            read_frames_chunk(directory.path(), 0, last, 20, limits, 256).unwrap(),
            FrameRead::Local
        ));
        follow(&mut reader, &mut last, 20);
        assert_eq!(last, 20);

        // Records appended after the reader reached the end of the live data are picked up
        // from the same offset.
        write(&mut sink, 5);
        follow(&mut reader, &mut last, 25);
        assert_eq!(last, 25);

        // A request that does not continue the cursor scans; the damaged record is in the
        // scanned range, so the reader reports the log unusable rather than skipping it.
        assert!(matches!(
            read_frames_resuming(&mut reader, directory.path(), 0, 2, 25, limits, 256).unwrap(),
            FrameRead::Local
        ));
    }

    #[test]
    fn a_resumed_reader_follows_rotation_and_rescans_when_its_segment_is_gone() {
        let directory = tempdir().unwrap();
        let limits = ProtocolLimits::default();
        let mut config = bulk_config(directory.path());
        config.wal_segment_bytes = 4096;
        let (mut sink, _, _) = WalSink::open(config, 0, limits).unwrap();
        for index in 0..200u8 {
            sink.record(Mutation::SetBytes {
                key: b"key",
                value: &[index; 64],
            })
            .unwrap();
        }
        sink.flush().unwrap();
        let wal = shard_wal_directory(directory.path(), 0);
        assert!(discover_segments(&wal).unwrap().len() > 3);

        let mut reader = FrameCursor::default();
        let mut resumed = Vec::new();
        let mut last = 0;
        while last < 100 {
            let FrameRead::Frames(frames) =
                read_frames_resuming(&mut reader, directory.path(), 0, last, 100, limits, 300)
                    .unwrap()
            else {
                panic!("missing WAL history");
            };
            for frame in frames {
                last = decode_record(&frame, 0, limits).unwrap().sequence;
                resumed.push(frame);
            }
        }
        let FrameRead::Frames(whole) = read_frames(directory.path(), 0, 0, 100, limits).unwrap()
        else {
            panic!("missing WAL history");
        };
        assert_eq!(resumed, whole);

        // The segment under the cursor is compacted away: the next chunk rescans and
        // answers exactly as a reader without a cursor would.
        let (held, _) = reader.position.clone().unwrap();
        let held_first = segment_first_sequence(&held).unwrap();
        for path in discover_segments(&wal).unwrap() {
            if segment_first_sequence(&path).unwrap() <= held_first {
                fs::remove_file(path).unwrap();
            }
        }
        let resumed =
            read_frames_resuming(&mut reader, directory.path(), 0, last, 200, limits, 300);
        let scanned = read_frames_chunk(directory.path(), 0, last, 200, limits, 300);
        assert_eq!(format!("{resumed:?}"), format!("{scanned:?}"));
        assert!(matches!(resumed, Ok(FrameRead::Gap)));
    }

    #[test]
    fn compact_of_the_full_log_leaves_recovery_from_the_covered_sequence() {
        let directory = tempdir().unwrap();
        let config = bulk_config(directory.path());
        let limits = ProtocolLimits::default();
        let (mut sink, _, _) = WalSink::open(config.clone(), 0, limits).unwrap();

        for index in 0..50 {
            sink.record(Mutation::SetBytes {
                key: b"key",
                value: &[index; 8],
            })
            .unwrap();
        }

        sink.flush().unwrap();

        let covered = sink.last_sequence();

        sink.compact(covered).unwrap();
        drop(sink);

        let (_sink, mutations, report) = WalSink::open_after(config, 0, limits, covered).unwrap();

        assert!(mutations.is_empty());
        assert_eq!(report.last_sequence, covered);
        assert_eq!(report.records_replayed, 0);
    }

    #[test]
    fn published_sequence_reads_the_exact_wal_frame() {
        let directory = tempdir().unwrap();
        let primary_config = config(directory.path(), DurabilityProfile::Everysec);
        let limits = ProtocolLimits::default();
        let shard_id = 0;
        let (mut sink, _, _) = WalSink::open(primary_config, shard_id, limits).unwrap();
        let owned = OwnedMutation::SetBytes {
            key: b"k".to_vec(),
            value: b"v".to_vec(),
        };

        sink.append(Mutation::SetBytes {
            key: b"k",
            value: b"v",
        })
        .unwrap();

        assert_eq!(sink.published().load(Ordering::Acquire), 1);

        let frame = encode_record(shard_id, 1, 0, &owned).unwrap();
        let read = read_frames(directory.path(), shard_id, 0, 1, limits).unwrap();

        assert_eq!(read, FrameRead::Frames(vec![frame.clone()]));
        assert_eq!(
            read_frames(directory.path(), shard_id, 1, 1, limits).unwrap(),
            FrameRead::Frames(Vec::new())
        );
        assert_eq!(
            read_frames(directory.path(), shard_id, 2, 1, limits).unwrap(),
            FrameRead::Ahead
        );

        let segment = segment_path(&shard_wal_directory(directory.path(), shard_id), 1);

        drop(sink);
        fs::remove_file(segment).unwrap();
        assert_eq!(
            read_frames(directory.path(), shard_id, 0, 1, limits).unwrap(),
            FrameRead::Gap
        );

        let replica = tempdir().unwrap();
        let replica_config = config(replica.path(), DurabilityProfile::Everysec);
        let (mut replica_sink, _, _) = WalSink::open(replica_config, shard_id, limits).unwrap();

        replica_sink.append_frame(&frame).unwrap();
        assert_eq!(
            read_frames(replica.path(), shard_id, 0, 1, limits).unwrap(),
            FrameRead::Frames(vec![frame.clone()])
        );

        let published = replica_sink.published().load(Ordering::Acquire);
        let skipped = encode_record(shard_id, published.saturating_add(2), 0, &owned).unwrap();
        let rejected = replica_sink.append_frame(&skipped);

        assert!(rejected.is_err(), "{rejected:?}");
        assert_eq!(replica_sink.published().load(Ordering::Acquire), published);
    }

    #[test]
    fn everysec_keeps_an_unsynced_record_off_the_durable_sequence() {
        let directory = tempdir().unwrap();
        let mut config = config(directory.path(), DurabilityProfile::Everysec);

        config.sync_interval = std::time::Duration::from_secs(3600);
        let limits = ProtocolLimits::default();
        let (mut sink, _, _) = WalSink::open(config, 0, limits).unwrap();
        let owned = OwnedMutation::SetBytes {
            key: b"k".to_vec(),
            value: b"v".to_vec(),
        };

        sink.record(Mutation::SetBytes {
            key: b"k",
            value: b"v",
        })
        .unwrap();

        assert_eq!(sink.published().load(Ordering::Acquire), 1);
        assert_eq!(sink.durable().load(Ordering::Acquire), 0);
        assert_eq!(
            read_frames(directory.path(), 0, 0, 0, limits).unwrap(),
            FrameRead::Frames(Vec::new())
        );

        sink.flush().unwrap();
        assert_eq!(sink.durable().load(Ordering::Acquire), 1);

        let frame = encode_record(0, 1, 0, &owned).unwrap();

        assert_eq!(
            read_frames(directory.path(), 0, 0, 1, limits).unwrap(),
            FrameRead::Frames(vec![frame])
        );
    }

    #[test]
    fn always_group_commit_syncs_every_record_in_the_batch() {
        let directory = tempdir().unwrap();
        let config = config(directory.path(), DurabilityProfile::Always);
        let limits = ProtocolLimits::default();
        let (mut sink, _, _) = WalSink::open(config.clone(), 0, limits).unwrap();

        sink.begin_group();

        for index in 0..8 {
            sink.record(Mutation::SetBytes {
                key: b"key",
                value: &[index],
            })
            .unwrap();
        }

        assert_eq!(sink.durable().load(Ordering::Acquire), 0);
        assert_eq!(sink.published().load(Ordering::Acquire), 0);
        sink.end_group().unwrap();
        assert_eq!(sink.published().load(Ordering::Acquire), 0);
        assert_eq!(sink.durable().load(Ordering::Acquire), 0);
        sink.sync_pending().unwrap();
        assert_eq!(sink.durable().load(Ordering::Acquire), 8);
        assert_eq!(sink.published().load(Ordering::Acquire), 8);
        drop(sink);

        let (_sink, mutations, _) = WalSink::open(config, 0, limits).unwrap();

        assert_eq!(mutations.len(), 8);
    }

    #[test]
    fn everysec_group_is_readable_before_the_background_sync() {
        let directory = tempdir().unwrap();
        let mut config = config(directory.path(), DurabilityProfile::Everysec);

        config.sync_interval = std::time::Duration::from_secs(3600);
        let limits = ProtocolLimits::default();
        let (mut sink, _, _) = WalSink::open(config, 0, limits).unwrap();

        sink.begin_group();
        sink.record(Mutation::SetBytes {
            key: b"k",
            value: b"v",
        })
        .unwrap();
        assert_eq!(sink.published().load(Ordering::Acquire), 0);
        sink.end_group().unwrap();
        assert_eq!(sink.published().load(Ordering::Acquire), 1);
        assert_eq!(sink.durable().load(Ordering::Acquire), 0);
        assert!(matches!(
            read_frames(directory.path(), 0, 0, 1, limits).unwrap(),
            FrameRead::Frames(frames) if frames.len() == 1
        ));
    }
}
