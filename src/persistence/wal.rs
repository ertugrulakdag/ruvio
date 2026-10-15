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
mod tests;
