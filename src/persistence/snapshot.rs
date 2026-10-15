use std::{
    collections::BTreeSet,
    fs::{self, File, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

use crate::{
    protocol::ProtocolLimits,
    store::{SnapshotEntry, SnapshotValue},
    time::UnixMillis,
};

use super::wal::{FrameRead, read_frames_chunk};

use super::{DurabilityProfile, PersistenceConfig, PersistenceError};

/// Data directories whose carried checkpoint copies take this long (a large shard file).
#[cfg(test)]
pub(crate) static CARRY_DELAYS: std::sync::Mutex<Vec<(PathBuf, std::time::Duration)>> =
    std::sync::Mutex::new(Vec::new());

/// Data directories whose next round commits fail before `CURRENT` changes, as many as the
/// count says (the round is not published).
#[cfg(test)]
pub(crate) static FAILING_COMMITS: std::sync::Mutex<Vec<(PathBuf, u32)>> =
    std::sync::Mutex::new(Vec::new());

/// Shards of a data directory whose new (not carried) checkpoint files take this much
/// longer to finish, to order the submissions of a round.
#[cfg(test)]
pub(crate) static SHARD_FILE_DELAYS: std::sync::Mutex<Vec<(PathBuf, u32, std::time::Duration)>> =
    std::sync::Mutex::new(Vec::new());

#[cfg(test)]
fn delay_shard_file(path: &Path, shard_id: u32) {
    let delay = SHARD_FILE_DELAYS
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .iter()
        .find(|(dir, shard, _)| path.starts_with(dir) && *shard == shard_id)
        .map(|(_, _, delay)| *delay);

    if let Some(delay) = delay {
        std::thread::sleep(delay);
    }
}

const SNAPSHOT_MAGIC: &[u8; 8] = b"RUVSNP01";
const SNAPSHOT_FOOTER_MAGIC: &[u8; 8] = b"RUVEND01";
const SNAPSHOT_HEADER_LEN: usize = 64;
/// Minor version 1 streams entries before their count is known: the header holds
/// `COUNT_IN_FOOTER` and the footer holds the real count.
const SNAPSHOT_MINOR_STREAMED: u16 = 1;
const COUNT_IN_FOOTER: u64 = u64::MAX;
const SNAPSHOT_FOOTER_LEN: usize = 60;
const MANIFEST_MAGIC: &[u8; 8] = b"RUVMAN01";
const CURRENT_MAGIC: &[u8; 8] = b"RUVCUR01";
const CLOCK_MAGIC: &[u8; 8] = b"RUVCLK01";

pub(crate) struct ShardCheckpoint {
    pub shard_id: u32,
    pub last_sequence: u64,
    pub entries: Vec<SnapshotEntry>,
}

pub(crate) struct Checkpoint {
    pub generation: u64,
    pub created_at: UnixMillis,
    pub shards: Vec<ShardCheckpoint>,
}

/// A published checkpoint whose entries have not been read yet.
pub(crate) struct CheckpointPlan {
    pub generation: u64,
    pub created_at: UnixMillis,
    directory: PathBuf,
    items: Vec<ManifestItem>,
    limits: ProtocolLimits,
    maximum_bytes: u64,
}

impl CheckpointPlan {
    pub(crate) fn shard_count(&self) -> usize {
        self.items.len()
    }

    pub(crate) fn last_sequence(&self, shard_id: u32) -> Option<u64> {
        self.items
            .iter()
            .find(|item| item.shard_id == shard_id)
            .map(|item| item.last_sequence)
    }

    /// Verifies one shard's file and passes its entries to `sink` one at a time.
    pub(crate) fn stream_shard(
        &self,
        shard_id: u32,
        sink: impl FnMut(SnapshotEntry) -> Result<(), PersistenceError>,
    ) -> Result<(), PersistenceError> {
        let item = self
            .items
            .iter()
            .find(|item| item.shard_id == shard_id)
            .ok_or_else(|| PersistenceError::Corruption("snapshot shard missing".into()))?;
        let path = self.directory.join(&item.file_name);
        // Streamed: recovery memory follows the store being filled, not the file size too.
        let (last_sequence, _) = stream_snapshot_file(
            &path,
            item.byte_length,
            &item.digest,
            self.generation,
            item.shard_id,
            self.limits,
            self.maximum_bytes,
            sink,
        )?;

        if last_sequence != item.last_sequence {
            return Err(PersistenceError::Corruption(
                "manifest snapshot sequence mismatch".into(),
            ));
        }

        Ok(())
    }
}

pub(crate) struct CheckpointManager {
    config: PersistenceConfig,
    limits: ProtocolLimits,
    /// Highest generation any round used, published or not; `None` until first read from
    /// disk. A generation is never handed out twice.
    last_generation: std::sync::Mutex<Option<u64>>,
}

impl CheckpointManager {
    pub(crate) fn new(config: PersistenceConfig, limits: ProtocolLimits) -> Self {
        Self {
            config,
            limits,
            last_generation: std::sync::Mutex::new(None),
        }
    }

    /// A generation newer than `floor`, every published one and every round directory ever
    /// started. A round that failed after `CURRENT` may already name it, so it is not reused.
    pub(crate) fn allocate_generation(&self, floor: u64) -> Result<u64, PersistenceError> {
        let mut last = self
            .last_generation
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let known = match *last {
            Some(known) => known,
            None => self.highest_generation_on_disk()?,
        };
        let next = known
            .max(floor)
            .checked_add(1)
            .ok_or_else(|| PersistenceError::Failed("checkpoint generation exhausted".into()))?;

        *last = Some(next);
        Ok(next)
    }

    fn highest_generation_on_disk(&self) -> Result<u64, PersistenceError> {
        let mut highest = self.current_generation()?.unwrap_or(0);

        if let Ok(entries) = fs::read_dir(self.config.data_dir.join("checkpoints")) {
            for entry in entries.flatten() {
                if let Some(generation) = entry
                    .file_name()
                    .to_str()
                    .and_then(|name| name.parse::<u64>().ok())
                {
                    highest = highest.max(generation);
                }
            }
        }

        Ok(highest)
    }

    /// The generation `CURRENT` names, if any.
    fn current_generation(&self) -> Result<Option<u64>, PersistenceError> {
        match fs::read(self.config.data_dir.join("CURRENT")) {
            Ok(bytes) => decode_current(&bytes).map(Some),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(error.into()),
        }
    }

    /// Reads the manifest and each snapshot header, not the entries. Recovery then streams
    /// only the files its shard owns, so memory follows one shard's data, not the whole
    /// checkpoint.
    pub(crate) fn plan(&self) -> Result<Option<CheckpointPlan>, PersistenceError> {
        let current_path = self.config.data_dir.join("CURRENT");
        let current = match fs::read(&current_path) {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error.into()),
        };
        let generation = decode_current(&current)?;
        let directory = self
            .config
            .data_dir
            .join("checkpoints")
            .join(format!("{generation:020}"));
        let manifest = decode_manifest(&fs::read(directory.join("MANIFEST"))?, generation)?;
        let mut created_at = 0u64;
        let mut ids: Vec<u32> = manifest.iter().map(|item| item.shard_id).collect();

        ids.sort_unstable();
        if ids
            .iter()
            .enumerate()
            .any(|(expected, id)| *id as usize != expected)
        {
            return Err(PersistenceError::Corruption(
                "checkpoint shard set is not contiguous".into(),
            ));
        }

        for item in &manifest {
            let mut header = [0u8; SNAPSHOT_HEADER_LEN];
            let mut file = fs::File::open(directory.join(&item.file_name))?;

            std::io::Read::read_exact(&mut file, &mut header)?;

            if &header[..8] != SNAPSHOT_MAGIC
                || crc32c::crc32c(&header[..60]) != u32_at(&header, 60)?
            {
                return Err(PersistenceError::Corruption(
                    "invalid snapshot header".into(),
                ));
            }

            created_at = created_at.max(u64_at(&header, 48)?);
        }

        Ok(Some(CheckpointPlan {
            generation,
            created_at: UnixMillis::new(created_at),
            directory,
            items: manifest,
            limits: self.limits,
            maximum_bytes: self.config.snapshot_max_bytes,
        }))
    }

    pub(crate) fn load(&self) -> Result<Option<Checkpoint>, PersistenceError> {
        self.load_with_budget(None)
    }

    #[cfg(test)]
    pub(crate) fn load_bounded_history(
        &self,
        max_bytes: u64,
    ) -> Result<Option<Checkpoint>, PersistenceError> {
        self.load_with_budget(Some(max_bytes))
    }

    fn load_with_budget(
        &self,
        budget: Option<u64>,
    ) -> Result<Option<Checkpoint>, PersistenceError> {
        let current_path = self.config.data_dir.join("CURRENT");
        let current = match fs::read(&current_path) {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error.into()),
        };

        let generation = decode_current(&current)?;
        let directory = self
            .config
            .data_dir
            .join("checkpoints")
            .join(format!("{generation:020}"));
        let manifest_path = directory.join("MANIFEST");
        let mut remaining = budget.unwrap_or(u64::MAX);
        if budget.is_some() {
            let length = fs::metadata(&manifest_path)?.len();
            remaining = remaining.checked_sub(length).ok_or_else(|| {
                PersistenceError::Corruption("JSON validation snapshot budget exceeded".into())
            })?;
        }
        let manifest_bytes = fs::read(manifest_path)?;
        let manifest = decode_manifest(&manifest_bytes, generation)?;
        let mut shards = Vec::with_capacity(manifest.len());
        let mut created_at = 0u64;

        for item in manifest {
            let path = directory.join(&item.file_name);
            if budget.is_some() {
                let length = fs::metadata(&path)?.len();
                remaining = remaining.checked_sub(length).ok_or_else(|| {
                    PersistenceError::Corruption("JSON validation snapshot budget exceeded".into())
                })?;
            }
            let bytes = fs::read(&path)?;

            if bytes.len() as u64 != item.byte_length
                || blake3::hash(&bytes).as_bytes() != &item.digest
            {
                return Err(PersistenceError::Corruption(format!(
                    "snapshot digest mismatch: {}",
                    path.display()
                )));
            }

            let (snapshot, snapshot_created) = decode_snapshot(
                &bytes,
                generation,
                item.shard_id,
                self.limits,
                self.config.snapshot_max_bytes,
            )?;

            created_at = created_at.max(snapshot_created);

            if snapshot.last_sequence != item.last_sequence {
                return Err(PersistenceError::Corruption(
                    "manifest snapshot sequence mismatch".into(),
                ));
            }

            shards.push(snapshot);
        }

        shards.sort_by_key(|snapshot| snapshot.shard_id);

        for (expected, shard) in shards.iter().enumerate() {
            if shard.shard_id as usize != expected {
                return Err(PersistenceError::Corruption(
                    "checkpoint shard set is not contiguous".into(),
                ));
            }
        }

        Ok(Some(Checkpoint {
            generation,
            created_at: UnixMillis::new(created_at),
            shards,
        }))
    }

    pub(crate) fn publish(
        &self,
        generation: u64,
        shards: &[ShardCheckpoint],
    ) -> Result<(), PersistenceError> {
        if shards.is_empty() {
            return Err(PersistenceError::Failed(
                "checkpoint requires at least one shard".into(),
            ));
        }

        self.begin_round(generation)?;

        let mut files = Vec::with_capacity(shards.len());

        for shard in shards {
            files.push(self.write_shard_file(generation, shards.len() as u32, shard)?);
        }

        self.commit_round(generation, &files)
    }

    fn generation_directory(&self, generation: u64) -> PathBuf {
        self.config
            .data_dir
            .join("checkpoints")
            .join(format!("{generation:020}"))
    }

    /// Creates the directory a checkpoint round writes its shard files into. A leftover
    /// directory of the same generation was never selected by `CURRENT`, so it is stale.
    pub(crate) fn begin_round(&self, generation: u64) -> Result<(), PersistenceError> {
        // Never clear the directory a published `CURRENT` points to.
        if self
            .current_generation()?
            .is_some_and(|current| current >= generation)
        {
            return Err(PersistenceError::Failed(format!(
                "checkpoint generation {generation} is already published"
            )));
        }

        let checkpoints = self.config.data_dir.join("checkpoints");

        if !checkpoints.exists() {
            fs::create_dir_all(&checkpoints)?;
            sync_directory(&self.config.data_dir)?;
        }
        let directory = self.generation_directory(generation);

        if let Some(last) = self
            .last_generation
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .as_mut()
        {
            *last = (*last).max(generation);
        }

        match fs::remove_dir_all(&directory) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }

        fs::create_dir(&directory)?;
        // The round's directory entry must be durable before CURRENT can name it.
        sync_directory(&checkpoints)?;
        Ok(())
    }

    /// Removes the directory of a round that was abandoned before it was published. A
    /// published generation (or an older one) is never touched.
    pub(crate) fn discard_round(&self, generation: u64) -> Result<(), PersistenceError> {
        if self
            .current_generation()?
            .is_some_and(|current| current >= generation)
        {
            return Ok(());
        }

        match fs::remove_dir_all(self.generation_directory(generation)) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(error.into()),
        }
    }

    /// Starts a streamed snapshot file for one shard of a round.
    pub(crate) fn shard_writer(
        &self,
        generation: u64,
        shard_id: u32,
        shard_count: u32,
        last_sequence: u64,
    ) -> Result<SnapshotWriter, PersistenceError> {
        SnapshotWriter::create(
            &self.generation_directory(generation),
            generation,
            shard_id,
            shard_count,
            last_sequence,
            self.config.snapshot_max_bytes,
        )
    }

    /// Writes an in-memory checkpoint as one shard file of a round.
    pub(crate) fn write_shard_file(
        &self,
        generation: u64,
        shard_count: u32,
        shard: &ShardCheckpoint,
    ) -> Result<ShardFile, PersistenceError> {
        let bytes = encode_snapshot(generation, shard_count, shard)?;

        if bytes.len() as u64 > self.config.snapshot_max_bytes {
            return Err(PersistenceError::Failed(
                "snapshot exceeds configured size limit".into(),
            ));
        }

        let file_name = format!("shard-{:04}.snap", shard.shard_id);

        #[cfg(test)]
        delay_shard_file(&self.config.data_dir, shard.shard_id);
        atomic_write_file(&self.generation_directory(generation), &file_name, &bytes)?;
        Ok(ShardFile {
            shard_id: shard.shard_id,
            last_sequence: shard.last_sequence,
            byte_length: bytes.len() as u64,
            digest: *blake3::hash(&bytes).as_bytes(),
            file_name,
        })
    }

    /// Copies shard `shard_id`'s file of published round `from` into round `to`, unchanged
    /// but for the generation in its header (and the checksums that cover it). A shard
    /// whose log failed cannot take a new file of memory that may hold refused writes; it
    /// hands each new round its last published one instead, so the other shards' rounds
    /// still publish and their logs are compacted. The copy keeps its snapshot point, so
    /// that shard's log is kept from there.
    pub(crate) fn carry_shard_file(
        &self,
        from: u64,
        to: u64,
        shard_id: u32,
    ) -> Result<ShardFile, PersistenceError> {
        use std::io::Read;

        let file_name = format!("shard-{shard_id:04}.snap");
        let source = self.generation_directory(from).join(&file_name);
        let mut input = File::open(&source)?;
        let length = input.metadata()?.len();
        let body_length = length
            .checked_sub((SNAPSHOT_HEADER_LEN + SNAPSHOT_FOOTER_LEN) as u64)
            .ok_or_else(|| PersistenceError::Corruption("snapshot file is truncated".into()))?;
        let mut header = [0u8; SNAPSHOT_HEADER_LEN];

        input.read_exact(&mut header)?;
        if &header[..8] != SNAPSHOT_MAGIC
            || crc32c::crc32c(&header[..60]) != u32_at(&header, 60)?
            || u64_at(&header, 16)? != from
            || u32_at(&header, 24)? != shard_id
        {
            return Err(PersistenceError::Corruption(
                "carried snapshot header does not match its round".into(),
            ));
        }

        let directory = self.generation_directory(to);
        let temporary = directory.join(carry_temporary_name(&file_name));
        #[cfg(test)]
        if let Some((_, delay)) = CARRY_DELAYS
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .iter()
            .find(|(dir, _)| *dir == self.config.data_dir)
        {
            std::thread::sleep(*delay);
        }
        let copied = (|| -> Result<ShardFile, PersistenceError> {
            let mut output = OpenOptions::new()
                .create(true)
                .truncate(true)
                .write(true)
                .open(&temporary)?;
            let mut old_hash = blake3::Hasher::new();
            let mut new_hash = blake3::Hasher::new();

            old_hash.update(&header);
            header[16..24].copy_from_slice(&to.to_le_bytes());
            let checksum = crc32c::crc32c(&header[..60]);
            header[60..64].copy_from_slice(&checksum.to_le_bytes());
            new_hash.update(&header);
            output.write_all(&header)?;

            let mut buffer = vec![0u8; SNAPSHOT_READ_BUFFER];
            let mut left = body_length;

            while left > 0 {
                let take = left.min(buffer.len() as u64) as usize;

                input.read_exact(&mut buffer[..take])?;
                old_hash.update(&buffer[..take]);
                new_hash.update(&buffer[..take]);
                output.write_all(&buffer[..take])?;
                left -= take as u64;
            }

            let mut footer = [0u8; SNAPSHOT_FOOTER_LEN];

            input.read_exact(&mut footer)?;
            if &footer[..8] != SNAPSHOT_FOOTER_MAGIC
                || crc32c::crc32c(&footer[..56]) != u32_at(&footer, 56)?
                || footer[24..56] != *old_hash.finalize().as_bytes()
            {
                return Err(PersistenceError::Corruption(
                    "carried snapshot does not match its digest".into(),
                ));
            }

            footer[24..56].copy_from_slice(new_hash.finalize().as_bytes());
            let checksum = crc32c::crc32c(&footer[..56]);
            footer[56..60].copy_from_slice(&checksum.to_le_bytes());
            new_hash.update(&footer);
            output.write_all(&footer)?;
            output.sync_all()?;
            drop(output);
            fs::rename(&temporary, directory.join(&file_name))?;
            sync_directory(&directory)?;
            Ok(ShardFile {
                shard_id,
                last_sequence: u64_at(&header, 32)?,
                byte_length: length,
                digest: *new_hash.finalize().as_bytes(),
                file_name: file_name.clone(),
            })
        })();

        // A failed copy leaves no partial file behind (the round is retried with a new one).
        if copied.is_err() {
            let _ = fs::remove_file(&temporary);
        }

        copied
    }

    /// A file holding no entries at sequence 0, for a round of a shard whose log failed when
    /// no round was ever published: its log holds every record from the first, so it
    /// replays all of them over the empty file.
    pub(crate) fn empty_shard_file(
        &self,
        generation: u64,
        shard_id: u32,
        shard_count: u32,
    ) -> Result<ShardFile, PersistenceError> {
        let shard = ShardCheckpoint {
            shard_id,
            last_sequence: 0,
            entries: Vec::new(),
        };
        let bytes = encode_snapshot(generation, shard_count, &shard)?;
        let file_name = format!("shard-{shard_id:04}.snap");
        let directory = self.generation_directory(generation);

        write_through_temporary(
            &directory.join(carry_temporary_name(&file_name)),
            &directory.join(&file_name),
            &bytes,
        )?;
        sync_directory(&directory)?;
        Ok(ShardFile {
            shard_id,
            last_sequence: 0,
            byte_length: bytes.len() as u64,
            digest: *blake3::hash(&bytes).as_bytes(),
            file_name,
        })
    }

    /// Commits a round: the manifest, then `CURRENT`. Only a complete set of shard files
    /// may be committed.
    pub(crate) fn commit_round(
        &self,
        generation: u64,
        files: &[ShardFile],
    ) -> Result<(), PersistenceError> {
        #[cfg(test)]
        if let Some((_, left)) = FAILING_COMMITS
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .iter_mut()
            .find(|(dir, left)| *dir == self.config.data_dir && *left > 0)
        {
            *left -= 1;
            return Err(PersistenceError::Failed("injected commit failure".into()));
        }
        let directory = self.generation_directory(generation);
        let manifest: Vec<ManifestItem> = files
            .iter()
            .map(|file| ManifestItem {
                shard_id: file.shard_id,
                last_sequence: file.last_sequence,
                byte_length: file.byte_length,
                digest: file.digest,
                file_name: file.file_name.clone(),
            })
            .collect();
        let manifest_bytes = encode_manifest(generation, &manifest)?;

        super::crash::hit(super::crash::CrashSite::Manifest);
        atomic_write_file(&directory, "MANIFEST", &manifest_bytes)?;
        sync_directory(&directory)?;
        let current = encode_current(generation);

        atomic_write_file(&self.config.data_dir, "CURRENT", &current)?;
        #[cfg(test)]
        if let Some(error) = super::crash::take_io_fault(super::crash::CrashSite::Manifest) {
            return Err(error.into());
        }
        sync_directory(&self.config.data_dir)?;
        retain_previous_generations(&self.config.data_dir.join("checkpoints"), generation);
        Ok(())
    }

    pub(crate) fn persist_clock(&self, now: UnixMillis) -> Result<(), PersistenceError> {
        if self.config.profile == DurabilityProfile::Memory {
            return Ok(());
        }

        // Every shard persists the clock: serialize the read-modify-write and its shared temp
        // file, or two writers can lose the rename (ENOENT) or move the floor backwards.
        static CLOCK_WRITE: std::sync::Mutex<()> = std::sync::Mutex::new(());
        let _serialized = CLOCK_WRITE
            .lock()
            .unwrap_or_else(|error| error.into_inner());

        fs::create_dir_all(&self.config.data_dir)?;
        let floor = self.load_clock()?.unwrap_or(UnixMillis::new(0));
        let value = UnixMillis::new(floor.get().max(now.get()));

        atomic_write_file(&self.config.data_dir, "CLOCK", &encode_clock(value.get()))?;
        Ok(())
    }

    pub(crate) fn load_clock(&self) -> Result<Option<UnixMillis>, PersistenceError> {
        let path = self.config.data_dir.join("CLOCK");
        let bytes = match fs::read(&path) {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error.into()),
        };

        Ok(Some(UnixMillis::new(decode_clock(&bytes)?)))
    }

    pub(crate) fn recovery_clock_floor(
        &self,
        checkpoint_created_at: UnixMillis,
    ) -> Result<UnixMillis, PersistenceError> {
        let persisted = self.load_clock()?.unwrap_or(UnixMillis::new(0));

        Ok(UnixMillis::new(
            persisted.get().max(checkpoint_created_at.get()),
        ))
    }
}

#[cfg_attr(not(test), allow(dead_code))]
pub(crate) struct ResyncBytes {
    pub last_sequence: u64,
    pub shard: Vec<u8>,
    pub hot: Option<Vec<u8>>,
}

#[cfg_attr(not(test), allow(dead_code))]
pub(crate) fn decode_snapshot_blob(
    bytes: &[u8],
    limits: ProtocolLimits,
    maximum_bytes: u64,
) -> Result<ShardCheckpoint, PersistenceError> {
    let generation = u64_at(bytes, 16)?;
    let shard_id = u32_at(bytes, 24)?;

    decode_snapshot(bytes, generation, shard_id, limits, maximum_bytes)
        .map(|(checkpoint, _created_at)| checkpoint)
}

#[cfg_attr(not(test), allow(dead_code))]
pub(crate) fn load_resync_bytes(
    data_dir: &Path,
    limits: ProtocolLimits,
    snapshot_max_bytes: u64,
    shard_id: u32,
    shard_count: u32,
    after_sequence: u64,
    durable_sequence: u64,
) -> Result<Option<ResyncBytes>, PersistenceError> {
    let config = PersistenceConfig {
        data_dir: data_dir.to_path_buf(),
        snapshot_max_bytes,
        ..PersistenceConfig::default()
    };

    let manager = CheckpointManager::new(config, limits);
    let current_path = manager.config.data_dir.join("CURRENT");
    let current = match fs::read(&current_path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };

    let generation = match decode_current(&current) {
        Ok(generation) => generation,
        Err(_) => return Ok(None),
    };

    let directory = manager
        .config
        .data_dir
        .join("checkpoints")
        .join(format!("{generation:020}"));
    let manifest_bytes = match fs::read(directory.join("MANIFEST")) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };

    let manifest = match decode_manifest(&manifest_bytes, generation) {
        Ok(manifest) => manifest,
        Err(_) => return Ok(None),
    };

    let mut files = Vec::with_capacity(manifest.len());

    for item in &manifest {
        let Some(bytes) = read_verified_snapshot(&directory, item, snapshot_max_bytes)? else {
            return Ok(None);
        };

        let file_generation = match u64_at(&bytes, 16) {
            Ok(file_generation) => file_generation,
            Err(_) => return Ok(None),
        };

        if file_generation != generation {
            return Ok(None);
        }

        let checkpoint = match decode_snapshot_blob(&bytes, manager.limits, snapshot_max_bytes) {
            Ok(checkpoint) if checkpoint.shard_id == item.shard_id => checkpoint,
            _ => return Ok(None),
        };

        if checkpoint.last_sequence != item.last_sequence {
            return Ok(None);
        }

        files.push(VerifiedSnapshot {
            shard_id: item.shard_id,
            last_sequence: checkpoint.last_sequence,
            bytes,
        });
    }

    let Some(index) = files.iter().position(|file| file.shard_id == shard_id) else {
        return Ok(None);
    };

    let chosen = files.swap_remove(index);

    if chosen.last_sequence < after_sequence || chosen.last_sequence > durable_sequence {
        return Ok(None);
    }

    let mut cursor = chosen.last_sequence;
    while cursor < durable_sequence {
        match read_frames_chunk(
            data_dir,
            shard_id,
            cursor,
            durable_sequence,
            limits,
            16 * 1024 * 1024,
        ) {
            Ok(FrameRead::Frames(frames)) => {
                let Some(frame) = frames.last() else {
                    return Ok(None);
                };
                let Ok(record) = super::decode_record(frame, shard_id, limits) else {
                    return Ok(None);
                };
                cursor = record.sequence;
            }
            Ok(_) => return Ok(None),
            Err(PersistenceError::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => {
                return Ok(None);
            }
            Err(PersistenceError::Io(error)) => return Err(error.into()),
            Err(_) => return Ok(None),
        }
    }

    let hot = if shard_id == 0 {
        if shard_count == shard_id {
            Some(chosen.bytes.clone())
        } else {
            files
                .into_iter()
                .find(|file| file.shard_id == shard_count)
                .map(|file| file.bytes)
        }
    } else {
        None
    };

    Ok(Some(ResyncBytes {
        last_sequence: chosen.last_sequence,
        shard: chosen.bytes,
        hot,
    }))
}

#[cfg_attr(not(test), allow(dead_code))]
struct VerifiedSnapshot {
    shard_id: u32,
    last_sequence: u64,
    bytes: Vec<u8>,
}

#[cfg_attr(not(test), allow(dead_code))]
fn read_verified_snapshot(
    directory: &Path,
    item: &ManifestItem,
    maximum_bytes: u64,
) -> Result<Option<Vec<u8>>, PersistenceError> {
    let path = directory.join(&item.file_name);
    let length = match fs::metadata(&path) {
        Ok(metadata) => metadata.len(),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };

    if length > maximum_bytes || length != item.byte_length {
        return Ok(None);
    }

    let bytes = match fs::read(&path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };

    if bytes.len() as u64 != length || blake3::hash(&bytes).as_bytes() != &item.digest {
        return Ok(None);
    }

    Ok(Some(bytes))
}

/// One finished shard file of a checkpoint round.
#[derive(Clone, Debug)]
pub(crate) struct ShardFile {
    pub shard_id: u32,
    pub last_sequence: u64,
    pub byte_length: u64,
    pub digest: [u8; 32],
    pub file_name: String,
}

/// Writes one shard snapshot in the streamed layout (format minor 1). The file is
/// built under a temporary name and renamed into place by `finish`.
pub(crate) struct SnapshotWriter {
    directory: PathBuf,
    file_name: String,
    temporary: PathBuf,
    file: File,
    hasher: blake3::Hasher,
    written: u64,
    entries: u64,
    last_sequence: u64,
    shard_id: u32,
    maximum_bytes: u64,
}

impl SnapshotWriter {
    pub(crate) fn create(
        directory: &Path,
        generation: u64,
        shard_id: u32,
        shard_count: u32,
        last_sequence: u64,
        maximum_bytes: u64,
    ) -> Result<Self, PersistenceError> {
        Self::create_at(
            directory,
            generation,
            shard_id,
            shard_count,
            last_sequence,
            maximum_bytes,
            unix_millis(),
        )
    }

    fn create_at(
        directory: &Path,
        generation: u64,
        shard_id: u32,
        shard_count: u32,
        last_sequence: u64,
        maximum_bytes: u64,
        created_at: u64,
    ) -> Result<Self, PersistenceError> {
        let file_name = format!("shard-{shard_id:04}.snap");
        let temporary = directory.join(format!(".{file_name}.tmp"));
        let file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&temporary)?;
        let mut writer = Self {
            directory: directory.to_path_buf(),
            file_name,
            temporary,
            file,
            hasher: blake3::Hasher::new(),
            written: 0,
            entries: 0,
            last_sequence,
            shard_id,
            maximum_bytes,
        };
        let mut header = Vec::with_capacity(SNAPSHOT_HEADER_LEN);

        header.extend_from_slice(SNAPSHOT_MAGIC);
        put_u16(&mut header, 1);
        put_u16(&mut header, SNAPSHOT_MINOR_STREAMED);
        put_u32(&mut header, SNAPSHOT_HEADER_LEN as u32);
        put_u64(&mut header, generation);
        put_u32(&mut header, shard_id);
        put_u32(&mut header, shard_count);
        put_u64(&mut header, last_sequence);
        put_u64(&mut header, COUNT_IN_FOOTER);
        put_u64(&mut header, created_at);
        put_u32(&mut header, 0);
        let checksum = crc32c::crc32c(&header);

        put_u32(&mut header, checksum);
        debug_assert_eq!(header.len(), SNAPSHOT_HEADER_LEN);
        writer.write_raw(&header)?;
        Ok(writer)
    }

    fn write_raw(&mut self, bytes: &[u8]) -> Result<(), PersistenceError> {
        let total = self.written.saturating_add(bytes.len() as u64);

        if total.saturating_add(SNAPSHOT_FOOTER_LEN as u64) > self.maximum_bytes {
            return Err(PersistenceError::Failed(
                "snapshot exceeds configured size limit".into(),
            ));
        }

        super::crash::hit_write(super::crash::CrashSite::Snapshot, &mut self.file, bytes)?;
        self.hasher.update(bytes);
        self.written = total;
        Ok(())
    }

    /// Appends `bytes`, which hold `entries` complete encoded entries.
    pub(crate) fn write_chunk(
        &mut self,
        bytes: &[u8],
        entries: u64,
    ) -> Result<(), PersistenceError> {
        self.write_raw(bytes)?;
        self.entries += entries;
        Ok(())
    }

    /// Writes the footer, syncs, and renames the file into place.
    pub(crate) fn finish(mut self) -> Result<ShardFile, PersistenceError> {
        #[cfg(test)]
        delay_shard_file(&self.directory, self.shard_id);
        let digest = *self.hasher.finalize().as_bytes();
        let mut footer = Vec::with_capacity(SNAPSHOT_FOOTER_LEN);

        footer.extend_from_slice(SNAPSHOT_FOOTER_MAGIC);
        put_u64(&mut footer, self.entries);
        put_u64(&mut footer, self.last_sequence);
        footer.extend_from_slice(&digest);
        let checksum = crc32c::crc32c(&footer);

        put_u32(&mut footer, checksum);
        self.file.write_all(&footer)?;
        self.hasher.update(&footer);
        self.written += footer.len() as u64;
        self.file.sync_all()?;
        super::crash::hit(super::crash::CrashSite::Rename);
        fs::rename(&self.temporary, self.directory.join(&self.file_name))?;
        sync_directory(&self.directory)?;
        Ok(ShardFile {
            shard_id: self.shard_id,
            last_sequence: self.last_sequence,
            byte_length: self.written,
            digest: *self.hasher.finalize().as_bytes(),
            file_name: self.file_name.clone(),
        })
    }

    /// Drops the partial file.
    pub(crate) fn abort(self) {
        let _ = fs::remove_file(&self.temporary);
    }
}

struct ManifestItem {
    shard_id: u32,
    last_sequence: u64,
    byte_length: u64,
    digest: [u8; 32],
    file_name: String,
}

fn encode_snapshot(
    generation: u64,
    shard_count: u32,
    checkpoint: &ShardCheckpoint,
) -> Result<Vec<u8>, PersistenceError> {
    encode_snapshot_at(generation, shard_count, checkpoint, unix_millis())
}

fn encode_snapshot_at(
    generation: u64,
    shard_count: u32,
    checkpoint: &ShardCheckpoint,
    created_at: u64,
) -> Result<Vec<u8>, PersistenceError> {
    let mut bytes = Vec::new();

    bytes.extend_from_slice(SNAPSHOT_MAGIC);
    put_u16(&mut bytes, 1);
    put_u16(&mut bytes, 0);
    put_u32(&mut bytes, SNAPSHOT_HEADER_LEN as u32);
    put_u64(&mut bytes, generation);
    put_u32(&mut bytes, checkpoint.shard_id);
    put_u32(&mut bytes, shard_count);
    put_u64(&mut bytes, checkpoint.last_sequence);
    put_u64(&mut bytes, checkpoint.entries.len() as u64);
    put_u64(&mut bytes, created_at);
    put_u32(&mut bytes, 0);
    let header_checksum = crc32c::crc32c(&bytes);

    put_u32(&mut bytes, header_checksum);
    debug_assert_eq!(bytes.len(), SNAPSHOT_HEADER_LEN);

    for entry in &checkpoint.entries {
        encode_entry(&mut bytes, entry)?;
    }

    let digest = *blake3::hash(&bytes).as_bytes();

    bytes.extend_from_slice(SNAPSHOT_FOOTER_MAGIC);
    put_u64(&mut bytes, checkpoint.entries.len() as u64);
    put_u64(&mut bytes, checkpoint.last_sequence);
    bytes.extend_from_slice(&digest);
    let footer_start = bytes.len() - 56;
    let footer_checksum = crc32c::crc32c(&bytes[footer_start..]);

    put_u32(&mut bytes, footer_checksum);
    Ok(bytes)
}

fn decode_snapshot(
    bytes: &[u8],
    generation: u64,
    shard_id: u32,
    limits: ProtocolLimits,
    maximum_bytes: u64,
) -> Result<(ShardCheckpoint, u64), PersistenceError> {
    let mut entries = Vec::new();
    let mut keys = BTreeSet::new();
    let (last_sequence, created_at) = decode_snapshot_with(
        bytes,
        generation,
        shard_id,
        limits,
        maximum_bytes,
        |entry| {
            if !keys.insert((entry.database, entry.key.clone())) {
                return Err(PersistenceError::Corruption(
                    "duplicate key in snapshot".into(),
                ));
            }

            entries.push(entry);
            Ok(())
        },
    )?;

    Ok((
        ShardCheckpoint {
            shard_id,
            last_sequence,
            entries,
        },
        created_at,
    ))
}

/// Validates a snapshot file and hands each entry to `sink` as it is decoded, so a caller
/// that restores entries straight into a store never holds them all at once. Returns the
/// last sequence and the creation time. Duplicate keys are the sink's to detect.
fn decode_snapshot_with(
    bytes: &[u8],
    generation: u64,
    shard_id: u32,
    limits: ProtocolLimits,
    maximum_bytes: u64,
    mut sink: impl FnMut(SnapshotEntry) -> Result<(), PersistenceError>,
) -> Result<(u64, u64), PersistenceError> {
    if bytes.len() < SNAPSHOT_HEADER_LEN + SNAPSHOT_FOOTER_LEN {
        return Err(PersistenceError::Corruption(
            "invalid snapshot file length".into(),
        ));
    }

    let footer_start = bytes.len() - SNAPSHOT_FOOTER_LEN;
    let frame = SnapshotFrame::validate(
        &bytes[..SNAPSHOT_HEADER_LEN],
        &bytes[footer_start..],
        bytes.len() as u64,
        blake3::hash(&bytes[..footer_start]),
        generation,
        shard_id,
        limits,
        maximum_bytes,
    )?;
    let mut count = 0u64;
    let mut position = SNAPSHOT_HEADER_LEN;

    while position < footer_start {
        let (entry, consumed) = decode_entry(&bytes[position..footer_start], frame.limits)?;

        sink(entry)?;
        count += 1;
        position += consumed;
    }

    frame.check_count(count)?;
    Ok((frame.last_sequence, frame.created_at))
}

/// Header and footer facts of a snapshot file whose framing has been verified.
struct SnapshotFrame {
    last_sequence: u64,
    created_at: u64,
    entry_count: u64,
    /// Entry limits: one entry can be as large as the whole file.
    limits: ProtocolLimits,
}

impl SnapshotFrame {
    /// Checks the header, the footer and `prefix_digest` (BLAKE3 of everything before the
    /// footer) of a `file_length`-byte snapshot.
    #[allow(clippy::too_many_arguments)]
    fn validate(
        header: &[u8],
        footer: &[u8],
        file_length: u64,
        prefix_digest: blake3::Hash,
        generation: u64,
        shard_id: u32,
        limits: ProtocolLimits,
        maximum_bytes: u64,
    ) -> Result<Self, PersistenceError> {
        if file_length < (SNAPSHOT_HEADER_LEN + SNAPSHOT_FOOTER_LEN) as u64
            || file_length > maximum_bytes
        {
            return Err(PersistenceError::Corruption(
                "invalid snapshot file length".into(),
            ));
        }

        if &header[..8] != SNAPSHOT_MAGIC {
            return Err(PersistenceError::Corruption(
                "invalid snapshot magic".into(),
            ));
        }

        if u16_at(header, 8)? != 1 {
            return Err(PersistenceError::Unsupported(
                "snapshot major version".into(),
            ));
        }

        let minor = u16_at(header, 10)?;

        if minor > SNAPSHOT_MINOR_STREAMED {
            return Err(PersistenceError::Unsupported(
                "snapshot minor version".into(),
            ));
        }

        if u32_at(header, 12)? as usize != SNAPSHOT_HEADER_LEN
            || u64_at(header, 16)? != generation
            || u32_at(header, 24)? != shard_id
            || u32_at(header, 56)? != 0
            || crc32c::crc32c(&header[..60]) != u32_at(header, 60)?
        {
            return Err(PersistenceError::Corruption(
                "invalid snapshot header".into(),
            ));
        }

        // Entries were checked against the limits in force when they were written; lowered
        // limits must not make a valid checkpoint unreadable. The file size bounds an entry.
        let limits = ProtocolLimits {
            max_arguments: usize::MAX,
            max_key_bytes: usize::MAX,
            max_value_bytes: usize::MAX,
            max_command_bytes: usize::try_from(file_length).unwrap_or(usize::MAX),
            ..limits
        };
        let last_sequence = u64_at(header, 32)?;
        let header_count = u64_at(header, 40)?;
        let created_at = u64_at(header, 48)?;
        let entry_count = u64_at(footer, 8)?;

        // Minor 0 declares its count up front; minor 1 streams entries and leaves the
        // header at the sentinel.
        if (minor == 0 && header_count != entry_count)
            || (minor == SNAPSHOT_MINOR_STREAMED && header_count != COUNT_IN_FOOTER)
        {
            return Err(PersistenceError::Corruption(
                "snapshot entry count mismatch".into(),
            ));
        }

        if &footer[..8] != SNAPSHOT_FOOTER_MAGIC {
            return Err(PersistenceError::Corruption(
                "missing snapshot footer".into(),
            ));
        }

        if u64_at(footer, 16)? != last_sequence
            || crc32c::crc32c(&footer[..SNAPSHOT_FOOTER_LEN - 4])
                != u32_at(footer, SNAPSHOT_FOOTER_LEN - 4)?
            || prefix_digest.as_bytes() != &footer[24..56]
        {
            return Err(PersistenceError::Corruption(
                "snapshot footer validation failed".into(),
            ));
        }

        usize::try_from(entry_count)
            .map_err(|_| PersistenceError::Corruption("snapshot entry count overflow".into()))?;

        Ok(Self {
            last_sequence,
            created_at,
            entry_count,
            limits,
        })
    }

    fn check_count(&self, count: u64) -> Result<(), PersistenceError> {
        if count != self.entry_count {
            return Err(PersistenceError::Corruption(
                "snapshot entry count mismatch".into(),
            ));
        }

        Ok(())
    }
}

/// Read buffer of the streamed snapshot decoder.
const SNAPSHOT_READ_BUFFER: usize = 1024 * 1024;

/// Verifies a snapshot file and passes its entries to `sink` without holding the file in
/// memory: one streamed pass checks the digests and framing, a second decodes the entries
/// (each still CRC-checked). Returns `(last_sequence, created_at)`.
#[allow(clippy::too_many_arguments)]
fn stream_snapshot_file(
    path: &Path,
    expected_length: u64,
    expected_digest: &[u8; 32],
    generation: u64,
    shard_id: u32,
    limits: ProtocolLimits,
    maximum_bytes: u64,
    mut sink: impl FnMut(SnapshotEntry) -> Result<(), PersistenceError>,
) -> Result<(u64, u64), PersistenceError> {
    use std::io::{BufReader, Read, Seek, SeekFrom};

    let mismatch =
        || PersistenceError::Corruption(format!("snapshot digest mismatch: {}", path.display()));
    let mut file = File::open(path)?;
    let file_length = file.metadata()?.len();

    if file_length != expected_length {
        return Err(mismatch());
    }
    if file_length < (SNAPSHOT_HEADER_LEN + SNAPSHOT_FOOTER_LEN) as u64
        || file_length > maximum_bytes
    {
        return Err(PersistenceError::Corruption(
            "invalid snapshot file length".into(),
        ));
    }

    let footer_start = file_length - SNAPSHOT_FOOTER_LEN as u64;
    let mut header = [0u8; SNAPSHOT_HEADER_LEN];
    let mut footer = [0u8; SNAPSHOT_FOOTER_LEN];
    let mut whole = blake3::Hasher::new();
    let mut prefix = blake3::Hasher::new();
    let mut buffer = vec![0u8; SNAPSHOT_READ_BUFFER];
    let mut offset = 0u64;

    loop {
        let read = match file.read(&mut buffer) {
            Ok(0) => break,
            Ok(read) => read,
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(error) => return Err(error.into()),
        };
        let chunk = &buffer[..read];
        let end = offset + read as u64;

        if end > file_length {
            // The file grew while it was read: not the file the manifest describes.
            return Err(mismatch());
        }

        whole.update(chunk);
        if offset < footer_start {
            let take = (footer_start.min(end) - offset) as usize;

            prefix.update(&chunk[..take]);
        }
        if offset < SNAPSHOT_HEADER_LEN as u64 {
            let start = offset as usize;
            let take = (SNAPSHOT_HEADER_LEN - start).min(read);

            header[start..start + take].copy_from_slice(&chunk[..take]);
        }
        if end > footer_start {
            let from = footer_start.max(offset);
            let into = (from - footer_start) as usize;
            let chunk_from = (from - offset) as usize;

            footer[into..into + (end - from) as usize].copy_from_slice(&chunk[chunk_from..]);
        }
        offset = end;
    }

    if offset != expected_length || whole.finalize().as_bytes() != expected_digest {
        return Err(mismatch());
    }

    let frame = SnapshotFrame::validate(
        &header,
        &footer,
        file_length,
        prefix.finalize(),
        generation,
        shard_id,
        limits,
        maximum_bytes,
    )?;

    drop(buffer);
    file.seek(SeekFrom::Start(SNAPSHOT_HEADER_LEN as u64))?;
    let mut reader = BufReader::with_capacity(SNAPSHOT_READ_BUFFER, file);
    let mut entry = Vec::new();
    let mut position = SNAPSHOT_HEADER_LEN as u64;
    let mut count = 0u64;

    while position < footer_start {
        let remaining = footer_start - position;
        let mut length = [0u8; 4];

        if remaining < 4 {
            return Err(PersistenceError::Corruption(
                "truncated snapshot entry".into(),
            ));
        }
        reader.read_exact(&mut length)?;
        let length = u32::from_le_bytes(length) as usize;

        if length < 24 || length as u64 > remaining || length > frame.limits.max_command_bytes {
            return Err(PersistenceError::Corruption(
                "invalid snapshot entry length".into(),
            ));
        }

        entry.clear();
        entry.extend_from_slice(&(length as u32).to_le_bytes());
        entry.resize(length, 0);
        reader.read_exact(&mut entry[4..])?;

        let (decoded, consumed) = decode_entry(&entry, frame.limits)?;

        debug_assert_eq!(consumed, length);
        sink(decoded)?;
        count += 1;
        position += length as u64;
        if entry.capacity() > SNAPSHOT_READ_BUFFER {
            // Do not keep one huge entry's buffer for the rest of the file.
            entry = Vec::new();
        }
    }

    frame.check_count(count)?;
    Ok((frame.last_sequence, frame.created_at))
}

pub(crate) fn encode_entry(
    output: &mut Vec<u8>,
    entry: &SnapshotEntry,
) -> Result<(), PersistenceError> {
    encode_entry_with(
        output,
        entry.database,
        &entry.key,
        entry.expires_at,
        0,
        |payload| encode_value_into(payload, &entry.value),
    )
}

/// Appends one entry frame whose payload `write_payload` writes straight into `output`,
/// returning the value type: no copy of the value is made on the way. `payload_len` is
/// reserved up front when known (0 otherwise). On error `output` is left as it was.
pub(crate) fn encode_entry_with(
    output: &mut Vec<u8>,
    database: u8,
    key: &[u8],
    expires_at: Option<UnixMillis>,
    payload_len: usize,
    write_payload: impl FnOnce(&mut Vec<u8>) -> Result<u8, PersistenceError>,
) -> Result<(), PersistenceError> {
    let start = output.len();
    let written = (|| {
        // Length, key, expiry, type, database and payload length, the payload, then CRC.
        let frame = payload_len.saturating_add(key.len()).saturating_add(28);

        // A frame past the length field's range fails anyway; refuse it before reserving.
        if u32::try_from(frame).is_err() {
            return Err(PersistenceError::Failed(
                "snapshot entry exceeds 4 GiB".into(),
            ));
        }

        // A value larger than the buffer gets exactly its size, not a doubled buffer.
        if frame > output.capacity() {
            output.reserve_exact(frame);
        } else {
            output.reserve(frame);
        }
        put_u32(output, 0);
        put_bytes(output, key)?;
        match expires_at {
            Some(deadline) => {
                output.push(1);
                put_u64(output, deadline.get());
            }

            None => {
                output.push(0);
                put_u64(output, 0);
            }
        }

        let type_at = output.len();

        output.push(0);
        put_u16(output, u16::from(database));
        put_u32(output, 0);

        let payload_start = output.len();

        output[type_at] = write_payload(output)?;
        let payload = u32::try_from(output.len() - payload_start)
            .map_err(|_| PersistenceError::Failed("snapshot value exceeds 4 GiB".into()))?;
        let frame_length = u32::try_from(output.len() - start + 4)
            .map_err(|_| PersistenceError::Failed("snapshot entry exceeds 4 GiB".into()))?;

        output[payload_start - 4..payload_start].copy_from_slice(&payload.to_le_bytes());
        output[start..start + 4].copy_from_slice(&frame_length.to_le_bytes());
        let checksum = crc32c::crc32c(&output[start + 4..]);

        put_u32(output, checksum);
        Ok(())
    })();

    if written.is_err() {
        output.truncate(start);
    }

    written
}

fn decode_entry(
    bytes: &[u8],
    limits: ProtocolLimits,
) -> Result<(SnapshotEntry, usize), PersistenceError> {
    if bytes.len() < 4 {
        return Err(PersistenceError::Corruption(
            "truncated snapshot entry".into(),
        ));
    }

    let length = u32_at(bytes, 0)? as usize;

    if length < 24 || length > bytes.len() || length > limits.max_command_bytes {
        return Err(PersistenceError::Corruption(
            "invalid snapshot entry length".into(),
        ));
    }

    let checksum_offset = length - 4;

    if crc32c::crc32c(&bytes[4..checksum_offset]) != u32_at(bytes, checksum_offset)? {
        return Err(PersistenceError::Corruption(
            "snapshot entry checksum mismatch".into(),
        ));
    }

    let mut decoder = Decoder::new(&bytes[4..checksum_offset]);
    let key = decoder.bytes(limits.max_key_bytes)?;
    let expiration_flag = decoder.u8()?;
    let expiration_value = decoder.u64()?;
    let expires_at = match expiration_flag {
        0 if expiration_value == 0 => None,
        1 => Some(UnixMillis::new(expiration_value)),
        _ => {
            return Err(PersistenceError::Corruption(
                "invalid snapshot expiration flag".into(),
            ));
        }
    };

    let value_type = decoder.u8()?;
    let database = u8::try_from(decoder.u16()?)
        .map_err(|_| PersistenceError::Unsupported("snapshot entry database".into()))?;

    let payload_length = decoder.u32()? as usize;
    let payload = decoder.take(payload_length)?;

    if !decoder.is_empty() {
        return Err(PersistenceError::Corruption(
            "trailing snapshot entry bytes".into(),
        ));
    }

    let value = decode_value(value_type, payload, limits)?;

    Ok((
        SnapshotEntry {
            database,
            key,
            expires_at,
            value,
        },
        length,
    ))
}

#[cfg(test)]
fn encode_value(value: &SnapshotValue) -> Result<(u8, Vec<u8>), PersistenceError> {
    let mut payload = Vec::new();
    let kind = encode_value_into(&mut payload, value)?;

    Ok((kind, payload))
}

/// Appends the payload of `value` to `payload` and returns its value type.
fn encode_value_into(payload: &mut Vec<u8>, value: &SnapshotValue) -> Result<u8, PersistenceError> {
    let kind = match value {
        SnapshotValue::Bytes(value) => {
            payload.extend_from_slice(value);
            1
        }

        SnapshotValue::Json(value) => {
            payload.extend_from_slice(value);
            15
        }

        SnapshotValue::List(values) => {
            put_byte_list(payload, values)?;
            2
        }

        SnapshotValue::Set(values) => {
            put_byte_list(payload, values)?;
            3
        }

        SnapshotValue::Hash(pairs) => {
            put_u32(
                payload,
                u32::try_from(pairs.len())
                    .map_err(|_| PersistenceError::Failed("too many hash fields".into()))?,
            );

            for (field, value) in pairs {
                put_bytes(payload, field)?;
                put_bytes(payload, value)?;
            }

            6
        }

        SnapshotValue::HashTtl(fields) => {
            put_u32(
                payload,
                u32::try_from(fields.len())
                    .map_err(|_| PersistenceError::Failed("too many hash fields".into()))?,
            );

            for (field, value, at) in fields {
                put_bytes(payload, field)?;
                put_bytes(payload, value)?;
                put_u64(payload, *at);
            }

            13
        }

        SnapshotValue::SortedSet(values) => {
            put_u32(
                payload,
                u32::try_from(values.len())
                    .map_err(|_| PersistenceError::Failed("too many sorted-set members".into()))?,
            );

            for (score, member) in values {
                put_u64(payload, score.to_bits());
                put_bytes(payload, member)?;
            }

            4
        }

        SnapshotValue::BloomFilter { words, hash_count } => {
            payload.push(*hash_count);
            payload.extend_from_slice(&[0; 3]);
            put_u32(
                payload,
                u32::try_from(words.len())
                    .map_err(|_| PersistenceError::Failed("Bloom snapshot too large".into()))?,
            );

            for word in words {
                put_u64(payload, *word);
            }

            5
        }

        SnapshotValue::BloomLayers {
            layers,
            error_rate,
            expansion,
        } => {
            put_u64(payload, error_rate.to_bits());
            put_u32(payload, *expansion);
            put_u32(
                payload,
                u32::try_from(layers.len())
                    .map_err(|_| PersistenceError::Failed("too many Bloom layers".into()))?,
            );

            for layer in layers {
                payload.push(layer.hash_count);
                put_u64(payload, layer.capacity);
                put_u64(payload, layer.count);
                put_u32(
                    payload,
                    u32::try_from(layer.words.len())
                        .map_err(|_| PersistenceError::Failed("Bloom snapshot too large".into()))?,
                );

                for word in layer.words.iter() {
                    put_u64(payload, *word);
                }
            }

            16
        }

        SnapshotValue::Lease { token, expires_at } => {
            put_u64(payload, *token);
            put_u64(payload, *expires_at);
            8
        }

        SnapshotValue::Limit { used } => {
            put_u64(payload, *used);
            12
        }

        SnapshotValue::Once => 11,

        SnapshotValue::Semaphore {
            next_token,
            holders,
        } => {
            put_u64(payload, *next_token);
            put_u32(
                payload,
                u32::try_from(holders.len())
                    .map_err(|_| PersistenceError::Failed("too many semaphore holders".into()))?,
            );

            for (token, expires_at) in holders {
                put_u64(payload, *token);
                put_u64(payload, *expires_at);
            }

            14
        }

        SnapshotValue::Versioned { version, bytes } => {
            put_u64(payload, *version);
            payload.extend_from_slice(bytes);
            9
        }

        SnapshotValue::Stream {
            started,
            last_millis,
            last_sequence,
            entries,
            groups,
            delayed,
        } => {
            payload.push(u8::from(*started));
            put_u64(payload, *last_millis);
            put_u64(payload, *last_sequence);
            put_u32(
                payload,
                u32::try_from(entries.len())
                    .map_err(|_| PersistenceError::Failed("too many stream entries".into()))?,
            );

            for entry in entries {
                put_u64(payload, entry.millis);
                put_u64(payload, entry.sequence);
                put_u32(
                    payload,
                    u32::try_from(entry.fields.len())
                        .map_err(|_| PersistenceError::Failed("too many stream fields".into()))?,
                );

                for (field, value) in &entry.fields {
                    put_bytes(payload, field)?;
                    put_bytes(payload, value)?;
                }
            }

            put_u32(
                payload,
                u32::try_from(groups.len())
                    .map_err(|_| PersistenceError::Failed("too many stream groups".into()))?,
            );

            for group in groups {
                put_bytes(payload, &group.name)?;
                put_u64(payload, group.last_millis);
                put_u64(payload, group.last_sequence);
                put_u32(
                    payload,
                    u32::try_from(group.pending.len()).map_err(|_| {
                        PersistenceError::Failed("too many stream pending entries".into())
                    })?,
                );

                for delivery in &group.pending {
                    put_u64(payload, delivery.millis);
                    put_u64(payload, delivery.sequence);
                    put_bytes(payload, &delivery.consumer)?;
                    put_u64(payload, delivery.delivered_at);
                    put_u64(payload, delivery.deliveries);
                }
            }

            let idle_groups: Vec<_> = groups
                .iter()
                .filter(|group| !group.idle.is_empty())
                .collect();

            if !idle_groups.is_empty() || !delayed.is_empty() {
                put_u32(
                    payload,
                    u32::try_from(idle_groups.len()).map_err(|_| {
                        PersistenceError::Failed("too many idle consumer groups".into())
                    })?,
                );

                for group in idle_groups {
                    put_bytes(payload, &group.name)?;
                    put_u32(
                        payload,
                        u32::try_from(group.idle.len()).map_err(|_| {
                            PersistenceError::Failed("too many idle consumers".into())
                        })?,
                    );

                    for name in &group.idle {
                        put_bytes(payload, name)?;
                    }
                }
            }

            if !delayed.is_empty() {
                put_u32(
                    payload,
                    u32::try_from(delayed.len()).map_err(|_| {
                        PersistenceError::Failed("too many delayed stream entries".into())
                    })?,
                );

                for (millis, sequence, ready_at) in delayed {
                    put_u64(payload, *millis);
                    put_u64(payload, *sequence);
                    put_u64(payload, *ready_at);
                }
            }

            7
        }
    };

    Ok(kind)
}

fn decode_value(
    kind: u8,
    payload: &[u8],
    limits: ProtocolLimits,
) -> Result<SnapshotValue, PersistenceError> {
    if kind == 1 || kind == 15 {
        if payload.len() > limits.max_value_bytes {
            return Err(PersistenceError::Corruption(
                "snapshot bytes value exceeds configured limit".into(),
            ));
        }

        return Ok(if kind == 1 {
            SnapshotValue::Bytes(payload.to_vec())
        } else {
            SnapshotValue::Json(payload.to_vec())
        });
    }

    let mut decoder = Decoder::new(payload);
    let value = match kind {
        2 => SnapshotValue::List(decoder.byte_list(limits.max_command_bytes)?),
        3 => SnapshotValue::Set(decoder.byte_list(limits.max_command_bytes)?),
        6 => {
            let count = decoder.u32()? as usize;

            if count > limits.max_command_bytes / 4 {
                return Err(PersistenceError::Corruption(
                    "snapshot hash field count is excessive".into(),
                ));
            }

            let mut pairs = Vec::with_capacity(count.min(1024));

            for _ in 0..count {
                pairs.push((
                    decoder.bytes(limits.max_value_bytes)?,
                    decoder.bytes(limits.max_value_bytes)?,
                ));
            }

            SnapshotValue::Hash(pairs)
        }

        13 => {
            let count = decoder.u32()? as usize;

            if count > limits.max_command_bytes / 4 {
                return Err(PersistenceError::Corruption(
                    "snapshot hash field count is excessive".into(),
                ));
            }

            let mut fields = Vec::with_capacity(count.min(1024));

            for _ in 0..count {
                fields.push((
                    decoder.bytes(limits.max_value_bytes)?,
                    decoder.bytes(limits.max_value_bytes)?,
                    decoder.u64()?,
                ));
            }

            SnapshotValue::HashTtl(fields)
        }

        4 => {
            let count = decoder.u32()? as usize;
            let mut values = Vec::with_capacity(count.min(1024));

            for _ in 0..count {
                values.push((
                    f64::from_bits(decoder.u64()?),
                    decoder.bytes(limits.max_value_bytes)?,
                ));
            }

            SnapshotValue::SortedSet(values)
        }

        5 => {
            let hash_count = decoder.u8()?;

            if decoder.take(3)? != [0; 3] {
                return Err(PersistenceError::Unsupported("Bloom snapshot flags".into()));
            }

            let count = decoder.u32()? as usize;

            if count == 0 || count > limits.max_command_bytes / 8 {
                return Err(PersistenceError::Corruption(
                    "invalid Bloom snapshot word count".into(),
                ));
            }

            let mut words = Vec::with_capacity(count);

            for _ in 0..count {
                words.push(decoder.u64()?);
            }

            SnapshotValue::BloomFilter { words, hash_count }
        }

        16 => {
            let error_rate = f64::from_bits(decoder.u64()?);
            let expansion = decoder.u32()?;
            let layer_count = decoder.u32()? as usize;

            if layer_count == 0 || layer_count > 64 {
                return Err(PersistenceError::Corruption(
                    "invalid Bloom snapshot layer count".into(),
                ));
            }

            let mut layers = Vec::with_capacity(layer_count);

            for _ in 0..layer_count {
                let hash_count = decoder.u8()?;
                let capacity = decoder.u64()?;
                let count = decoder.u64()?;
                let word_count = decoder.u32()? as usize;

                if word_count == 0 || word_count > limits.max_command_bytes / 8 {
                    return Err(PersistenceError::Corruption(
                        "invalid Bloom snapshot word count".into(),
                    ));
                }

                let mut words = Vec::with_capacity(word_count);

                for _ in 0..word_count {
                    words.push(decoder.u64()?);
                }

                layers.push(crate::store::BloomLayer {
                    words: words.into_boxed_slice(),
                    hash_count,
                    capacity,
                    count,
                });
            }

            SnapshotValue::BloomLayers {
                layers,
                error_rate,
                expansion,
            }
        }

        7 => {
            let started = match decoder.u8()? {
                0 => false,
                1 => true,
                _ => {
                    return Err(PersistenceError::Corruption(
                        "invalid stream snapshot flag".into(),
                    ));
                }
            };

            let last_millis = decoder.u64()?;
            let last_sequence = decoder.u64()?;
            let count = decoder.u32()? as usize;

            if count > limits.max_command_bytes / 16 {
                return Err(PersistenceError::Corruption(
                    "snapshot stream entry count is excessive".into(),
                ));
            }

            let mut entries = Vec::with_capacity(count.min(1024));

            for _ in 0..count {
                let millis = decoder.u64()?;
                let sequence = decoder.u64()?;
                let field_count = decoder.u32()? as usize;

                if field_count == 0 || field_count > limits.max_arguments {
                    return Err(PersistenceError::Corruption(
                        "invalid stream field count in snapshot".into(),
                    ));
                }

                let mut fields = Vec::with_capacity(field_count);

                for _ in 0..field_count {
                    fields.push((
                        decoder.bytes(limits.max_value_bytes)?,
                        decoder.bytes(limits.max_value_bytes)?,
                    ));
                }

                entries.push(crate::stream::StreamEntry {
                    millis,
                    sequence,
                    fields: crate::stream::StreamFields::from_pairs(
                        fields
                            .iter()
                            .map(|(field, value)| (field.as_slice(), value.as_slice())),
                    ),
                });
            }

            let (groups, delayed) = if decoder.is_empty() {
                (Vec::new(), Vec::new())
            } else {
                let count = decoder.u32()? as usize;

                if count > limits.max_arguments {
                    return Err(PersistenceError::Corruption(
                        "snapshot stream group count is excessive".into(),
                    ));
                }

                let mut groups = Vec::with_capacity(count);

                for _ in 0..count {
                    let name = decoder.bytes(limits.max_value_bytes)?;
                    let last_millis = decoder.u64()?;
                    let last_sequence = decoder.u64()?;
                    let pending_count = decoder.u32()? as usize;

                    if pending_count > limits.max_arguments {
                        return Err(PersistenceError::Corruption(
                            "snapshot stream pending count is excessive".into(),
                        ));
                    }

                    let mut pending = Vec::with_capacity(pending_count);

                    for _ in 0..pending_count {
                        pending.push(crate::stream::PendingDelivery {
                            millis: decoder.u64()?,
                            sequence: decoder.u64()?,
                            consumer: decoder.bytes(limits.max_value_bytes)?,
                            delivered_at: decoder.u64()?,
                            deliveries: decoder.u64()?,
                        });
                    }

                    groups.push(crate::stream::ConsumerGroup {
                        name,
                        last_millis,
                        last_sequence,
                        pending,
                        idle: Vec::new(),
                    });
                }

                let mut delayed = Vec::new();

                if !decoder.is_empty() {
                    let idle_groups = decoder.u32()? as usize;

                    for _ in 0..idle_groups {
                        let name = decoder.bytes(limits.max_value_bytes)?;
                        let count = decoder.u32()? as usize;
                        let group = groups
                            .iter_mut()
                            .find(|group| group.name == name)
                            .ok_or_else(|| {
                                PersistenceError::Corruption(
                                    "snapshot idle consumers name a missing group".into(),
                                )
                            })?;

                        for _ in 0..count {
                            group.idle.push(decoder.bytes(limits.max_value_bytes)?);
                        }
                    }

                    if !decoder.is_empty() {
                        let count = decoder.u32()? as usize;

                        if count > limits.max_arguments {
                            return Err(PersistenceError::Corruption(
                                "snapshot delayed stream count is excessive".into(),
                            ));
                        }

                        for _ in 0..count {
                            delayed.push((decoder.u64()?, decoder.u64()?, decoder.u64()?));
                        }
                    }
                }

                (groups, delayed)
            };

            SnapshotValue::Stream {
                started,
                last_millis,
                last_sequence,
                entries,
                groups,
                delayed,
            }
        }

        8 => SnapshotValue::Lease {
            token: decoder.u64()?,
            expires_at: decoder.u64()?,
        },
        // Older snapshots also stored the window end here. The entry deadline already holds it.
        10 => {
            let used = decoder.u64()?;
            let _ = decoder.u64()?;

            SnapshotValue::Limit { used }
        }
        12 => SnapshotValue::Limit {
            used: decoder.u64()?,
        },
        11 => SnapshotValue::Once,
        14 => {
            let next_token = decoder.u64()?;
            let count = decoder.u32()? as usize;

            if count > limits.max_command_bytes / 16 {
                return Err(PersistenceError::Corruption(
                    "snapshot semaphore holder count is excessive".into(),
                ));
            }

            let mut holders = Vec::with_capacity(count.min(1024));

            for _ in 0..count {
                holders.push((decoder.u64()?, decoder.u64()?));
            }

            SnapshotValue::Semaphore {
                next_token,
                holders,
            }
        }
        9 => {
            let version = decoder.u64()?;
            let bytes = decoder.take_rest();

            SnapshotValue::Versioned { version, bytes }
        }
        _ => {
            return Err(PersistenceError::Unsupported(format!(
                "snapshot value type {kind}"
            )));
        }
    };

    if !decoder.is_empty() {
        return Err(PersistenceError::Corruption(
            "trailing snapshot value bytes".into(),
        ));
    }

    Ok(value)
}

fn encode_manifest(generation: u64, items: &[ManifestItem]) -> Result<Vec<u8>, PersistenceError> {
    let mut bytes = Vec::new();

    bytes.extend_from_slice(MANIFEST_MAGIC);
    put_u16(&mut bytes, 1);
    put_u16(&mut bytes, 0);
    put_u64(&mut bytes, generation);
    put_u32(
        &mut bytes,
        u32::try_from(items.len())
            .map_err(|_| PersistenceError::Failed("too many checkpoint shards".into()))?,
    );

    for item in items {
        put_u32(&mut bytes, item.shard_id);
        put_u64(&mut bytes, item.last_sequence);
        put_u64(&mut bytes, item.byte_length);
        bytes.extend_from_slice(&item.digest);
        put_u16(
            &mut bytes,
            u16::try_from(item.file_name.len())
                .map_err(|_| PersistenceError::Failed("snapshot filename too long".into()))?,
        );
        bytes.extend_from_slice(item.file_name.as_bytes());
    }

    let digest = *blake3::hash(&bytes).as_bytes();

    bytes.extend_from_slice(&digest);
    let checksum = crc32c::crc32c(&bytes);

    put_u32(&mut bytes, checksum);
    Ok(bytes)
}

fn decode_manifest(bytes: &[u8], generation: u64) -> Result<Vec<ManifestItem>, PersistenceError> {
    if bytes.len() < 60 || &bytes[..8] != MANIFEST_MAGIC {
        return Err(PersistenceError::Corruption("invalid manifest".into()));
    }

    if u16_at(bytes, 8)? != 1 || u16_at(bytes, 10)? != 0 {
        return Err(PersistenceError::Unsupported(
            "manifest format version".into(),
        ));
    }

    if u64_at(bytes, 12)? != generation {
        return Err(PersistenceError::Corruption(
            "manifest generation mismatch".into(),
        ));
    }

    let digest_offset = bytes.len() - 36;

    if blake3::hash(&bytes[..digest_offset]).as_bytes() != &bytes[digest_offset..digest_offset + 32]
        || crc32c::crc32c(&bytes[..bytes.len() - 4]) != u32_at(bytes, bytes.len() - 4)?
    {
        return Err(PersistenceError::Corruption(
            "manifest digest mismatch".into(),
        ));
    }

    let count = u32_at(bytes, 20)? as usize;
    let mut decoder = Decoder::new(&bytes[24..digest_offset]);
    let mut items = Vec::with_capacity(count);

    for expected_shard in 0..count {
        let shard_id = decoder.u32()?;

        if shard_id as usize != expected_shard {
            return Err(PersistenceError::Corruption(
                "manifest shard set is not ordered".into(),
            ));
        }

        let last_sequence = decoder.u64()?;
        let byte_length = decoder.u64()?;
        let digest = decoder.take(32)?.try_into().unwrap();
        let name_length = decoder.u16()? as usize;
        let file_name = std::str::from_utf8(decoder.take(name_length)?)
            .map_err(|_| PersistenceError::Corruption("invalid snapshot filename".into()))?
            .to_owned();

        if file_name != format!("shard-{shard_id:04}.snap") {
            return Err(PersistenceError::Corruption(
                "unsafe or unexpected snapshot filename".into(),
            ));
        }

        items.push(ManifestItem {
            shard_id,
            last_sequence,
            byte_length,
            digest,
            file_name,
        });
    }

    if !decoder.is_empty() {
        return Err(PersistenceError::Corruption(
            "trailing manifest bytes".into(),
        ));
    }

    Ok(items)
}

fn encode_current(generation: u64) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(20);

    bytes.extend_from_slice(CURRENT_MAGIC);
    put_u64(&mut bytes, generation);
    let checksum = crc32c::crc32c(&bytes);

    put_u32(&mut bytes, checksum);
    bytes
}

fn decode_current(bytes: &[u8]) -> Result<u64, PersistenceError> {
    if bytes.len() != 20
        || &bytes[..8] != CURRENT_MAGIC
        || crc32c::crc32c(&bytes[..16]) != u32_at(bytes, 16)?
    {
        return Err(PersistenceError::Corruption(
            "invalid CURRENT pointer".into(),
        ));
    }

    u64_at(bytes, 8)
}

fn encode_clock(unix_millis: u64) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(20);

    bytes.extend_from_slice(CLOCK_MAGIC);
    put_u64(&mut bytes, unix_millis);
    let checksum = crc32c::crc32c(&bytes);

    put_u32(&mut bytes, checksum);
    bytes
}

fn decode_clock(bytes: &[u8]) -> Result<u64, PersistenceError> {
    if bytes.len() != 20
        || &bytes[..8] != CLOCK_MAGIC
        || crc32c::crc32c(&bytes[..16]) != u32_at(bytes, 16)?
    {
        return Err(PersistenceError::Corruption("invalid CLOCK file".into()));
    }

    u64_at(bytes, 8)
}

fn retain_previous_generations(directory: &Path, generation: u64) {
    let keep_from = generation.saturating_sub(1);
    let Ok(entries) = fs::read_dir(directory) else {
        return;
    };

    for entry in entries.flatten() {
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            continue;
        };

        if let Ok(existing) = name.parse::<u64>()
            && existing < keep_from
        {
            let _ = fs::remove_dir_all(entry.path());
        }
    }
}

/// Removes `.<name>.tmp` files an interrupted write left in the data directory and in every
/// checkpoint generation. Call only while holding the data directory lock.
pub(crate) fn sweep_temporary_files(data_dir: &Path) {
    let mut directories = vec![data_dir.to_path_buf()];

    if let Ok(entries) = fs::read_dir(data_dir.join("checkpoints")) {
        directories.extend(
            entries
                .flatten()
                .filter(|entry| entry.file_type().is_ok_and(|kind| kind.is_dir()))
                .map(|entry| entry.path()),
        );
    }

    for directory in directories {
        let Ok(entries) = fs::read_dir(&directory) else {
            continue;
        };

        for entry in entries.flatten() {
            let name = entry.file_name();
            let is_temporary = name
                .to_str()
                .is_some_and(|name| name.starts_with('.') && name.ends_with(".tmp"));

            if is_temporary && entry.file_type().is_ok_and(|kind| kind.is_file()) {
                match fs::remove_file(entry.path()) {
                    Ok(()) => {
                        tracing::info!(path = %entry.path().display(), "removed leftover temporary file")
                    }
                    Err(error) => {
                        tracing::warn!(path = %entry.path().display(), %error, "cannot remove leftover temporary file")
                    }
                }
            }
        }
    }
}

/// Writes a temp file, syncs it, then renames it over the final name. A failed or interrupted
/// write leaves the previous file of that name intact, so the old checkpoint stays valid.
fn atomic_write_file(directory: &Path, name: &str, bytes: &[u8]) -> Result<(), PersistenceError> {
    write_through_temporary(
        &directory.join(format!(".{name}.tmp")),
        &directory.join(name),
        bytes,
    )?;
    sync_directory(directory)?;
    Ok(())
}

/// The temporary name of a carried copy (see `CheckpointManager::carry_shard_file`): not
/// the one a checkpoint of the same shard writes, so neither can replace or remove the
/// other's partial file. Startup cleanup removes it like any `.<name>.tmp`.
fn carry_temporary_name(file_name: &str) -> String {
    format!(".{file_name}.carry.tmp")
}

/// [`atomic_write_file`] without the directory sync.
fn write_through_temporary(
    temporary_path: &Path,
    final_path: &Path,
    bytes: &[u8],
) -> Result<(), PersistenceError> {
    // A leftover temp file from an interrupted write is overwritten, never fatal.
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(temporary_path)?;

    super::crash::hit_write(super::crash::CrashSite::Snapshot, &mut file, bytes)?;
    file.sync_all()?;
    drop(file);
    super::crash::hit(super::crash::CrashSite::Rename);
    fs::rename(temporary_path, final_path)?;
    Ok(())
}

pub(super) fn sync_directory(directory: &Path) -> Result<(), PersistenceError> {
    #[cfg(unix)]
    File::open(directory)?.sync_all()?;
    #[cfg(not(unix))]
    let _ = directory;
    Ok(())
}

fn unix_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}

fn put_byte_list(output: &mut Vec<u8>, values: &[Vec<u8>]) -> Result<(), PersistenceError> {
    put_u32(
        output,
        u32::try_from(values.len())
            .map_err(|_| PersistenceError::Failed("snapshot collection too large".into()))?,
    );

    for value in values {
        put_bytes(output, value)?;
    }

    Ok(())
}

fn put_bytes(output: &mut Vec<u8>, value: &[u8]) -> Result<(), PersistenceError> {
    put_u32(
        output,
        u32::try_from(value.len())
            .map_err(|_| PersistenceError::Failed("snapshot byte string too large".into()))?,
    );
    output.extend_from_slice(value);
    Ok(())
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

fn u16_at(bytes: &[u8], offset: usize) -> Result<u16, PersistenceError> {
    let value = bytes
        .get(offset..offset + 2)
        .ok_or_else(|| PersistenceError::Corruption("truncated integer".into()))?;

    Ok(u16::from_le_bytes(value.try_into().unwrap()))
}

fn u32_at(bytes: &[u8], offset: usize) -> Result<u32, PersistenceError> {
    let value = bytes
        .get(offset..offset + 4)
        .ok_or_else(|| PersistenceError::Corruption("truncated integer".into()))?;

    Ok(u32::from_le_bytes(value.try_into().unwrap()))
}

fn u64_at(bytes: &[u8], offset: usize) -> Result<u64, PersistenceError> {
    let value = bytes
        .get(offset..offset + 8)
        .ok_or_else(|| PersistenceError::Corruption("truncated integer".into()))?;

    Ok(u64::from_le_bytes(value.try_into().unwrap()))
}

struct Decoder<'a> {
    remaining: &'a [u8],
}

impl<'a> Decoder<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { remaining: bytes }
    }

    fn take(&mut self, length: usize) -> Result<&'a [u8], PersistenceError> {
        if length > self.remaining.len() {
            return Err(PersistenceError::Corruption(
                "truncated persistence payload".into(),
            ));
        }

        let (value, remaining) = self.remaining.split_at(length);

        self.remaining = remaining;
        Ok(value)
    }

    fn is_empty(&self) -> bool {
        self.remaining.is_empty()
    }

    fn take_rest(&mut self) -> Vec<u8> {
        let bytes = self.remaining.to_vec();

        self.remaining = &[];
        bytes
    }

    fn u8(&mut self) -> Result<u8, PersistenceError> {
        Ok(self.take(1)?[0])
    }

    fn u16(&mut self) -> Result<u16, PersistenceError> {
        Ok(u16::from_le_bytes(self.take(2)?.try_into().unwrap()))
    }

    fn u32(&mut self) -> Result<u32, PersistenceError> {
        Ok(u32::from_le_bytes(self.take(4)?.try_into().unwrap()))
    }

    fn u64(&mut self) -> Result<u64, PersistenceError> {
        Ok(u64::from_le_bytes(self.take(8)?.try_into().unwrap()))
    }

    fn bytes(&mut self, maximum: usize) -> Result<Vec<u8>, PersistenceError> {
        let length = self.u32()? as usize;

        if length > maximum {
            return Err(PersistenceError::Corruption(
                "persistence byte string exceeds configured limit".into(),
            ));
        }

        Ok(self.take(length)?.to_vec())
    }

    fn byte_list(&mut self, maximum_bytes: usize) -> Result<Vec<Vec<u8>>, PersistenceError> {
        let count = self.u32()? as usize;

        if count > maximum_bytes / 4 {
            return Err(PersistenceError::Corruption(
                "snapshot collection count is excessive".into(),
            ));
        }

        let mut values = Vec::with_capacity(count.min(1024));

        for _ in 0..count {
            values.push(self.bytes(maximum_bytes)?);
        }

        Ok(values)
    }
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use super::*;

    /// Value kinds 1-16 exist at snapshot minor 1. A reader that meets a later kind refuses
    /// the file, so a new kind also needs a snapshot minor bump and a row here.
    const LAST_SNAPSHOT_KIND: u8 = 16;

    #[test]
    fn every_value_kind_the_decoder_knows_is_counted() {
        let limits = ProtocolLimits::default();

        for kind in 0..=u8::MAX {
            let known = !matches!(
                decode_value(kind, &[], limits),
                Err(PersistenceError::Unsupported(_))
            );

            assert_eq!(
                known,
                (1..=LAST_SNAPSHOT_KIND).contains(&kind),
                "snapshot value kind {kind}: a new kind needs LAST_SNAPSHOT_KIND and a \
                 snapshot minor bump"
            );
        }
    }

    fn config(path: &Path) -> PersistenceConfig {
        PersistenceConfig {
            data_dir: path.to_path_buf(),
            snapshot_max_bytes: 1024 * 1024,
            ..PersistenceConfig::default()
        }
    }

    fn snapshot_config(path: &Path) -> PersistenceConfig {
        config(path)
    }

    #[test]
    fn load_resync_bytes_returns_a_newer_checkpoint_and_refuses_an_older_one() {
        let directory = tempdir().unwrap();
        let config = snapshot_config(directory.path());
        let limits = ProtocolLimits::default();
        let manager = CheckpointManager::new(config, limits);

        manager
            .publish(
                1,
                &[
                    ShardCheckpoint {
                        shard_id: 0,
                        last_sequence: 4,
                        entries: vec![SnapshotEntry {
                            database: 0,
                            key: b"k".to_vec(),
                            expires_at: None,
                            value: SnapshotValue::Bytes(b"kept".to_vec()),
                        }],
                    },
                    ShardCheckpoint {
                        shard_id: 1,
                        last_sequence: 4,
                        entries: Vec::new(),
                    },
                ],
            )
            .unwrap();

        let newer = load_resync_bytes(directory.path(), limits, 1024 * 1024, 0, 1, 1, 4)
            .unwrap()
            .expect("checkpoint covers sequence 1");

        assert_eq!(newer.last_sequence, 4);
        assert!(newer.hot.is_some());
        assert!(newer.shard.starts_with(b"RUVSNP01"));
        assert!(
            load_resync_bytes(directory.path(), limits, 1024 * 1024, 0, 1, 5, 4)
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn atomically_publishes_and_loads_a_checkpoint() {
        let directory = tempdir().unwrap();
        let manager = CheckpointManager::new(config(directory.path()), ProtocolLimits::default());
        let checkpoint = ShardCheckpoint {
            shard_id: 0,
            last_sequence: 7,
            entries: vec![SnapshotEntry {
                database: 0,
                key: b"key".to_vec(),
                expires_at: Some(UnixMillis::new(42)),
                value: SnapshotValue::List(vec![b"a".to_vec(), b"b".to_vec()]),
            }],
        };

        manager.publish(1, &[checkpoint]).unwrap();

        let loaded = manager.load().unwrap().unwrap();

        assert_eq!(loaded.generation, 1);
        assert_eq!(loaded.shards[0].last_sequence, 7);
        assert_eq!(loaded.shards[0].entries.len(), 1);
    }

    #[test]
    fn atomic_write_replaces_a_leftover_temporary_file() {
        let directory = tempdir().unwrap();

        std::fs::write(directory.path().join(".CURRENT.tmp"), vec![b'x'; 4096]).unwrap();
        atomic_write_file(directory.path(), "CURRENT", b"fresh").unwrap();

        assert_eq!(
            std::fs::read(directory.path().join("CURRENT")).unwrap(),
            b"fresh"
        );
        assert!(!directory.path().join(".CURRENT.tmp").exists());
    }

    #[test]
    fn plan_streams_only_the_requested_shard() {
        let directory = tempdir().unwrap();
        let manager = CheckpointManager::new(config(directory.path()), ProtocolLimits::default());
        let entry = |key: &[u8]| SnapshotEntry {
            database: 0,
            key: key.to_vec(),
            expires_at: None,
            value: SnapshotValue::Bytes(b"v".to_vec()),
        };

        manager
            .publish(
                3,
                &[
                    ShardCheckpoint {
                        shard_id: 0,
                        last_sequence: 5,
                        entries: vec![entry(b"a"), entry(b"b")],
                    },
                    ShardCheckpoint {
                        shard_id: 1,
                        last_sequence: 9,
                        entries: vec![entry(b"c")],
                    },
                ],
            )
            .unwrap();

        let plan = manager.plan().unwrap().unwrap();
        let mut keys = Vec::new();

        assert_eq!(plan.generation, 3);
        assert_eq!(plan.shard_count(), 2);
        assert_eq!(plan.last_sequence(1), Some(9));
        assert_eq!(plan.last_sequence(7), None);
        plan.stream_shard(0, |entry| {
            keys.push(entry.key);
            Ok(())
        })
        .unwrap();
        assert_eq!(keys, vec![b"a".to_vec(), b"b".to_vec()]);
        assert!(plan.stream_shard(7, |_| Ok(())).is_err());
        assert!(
            CheckpointManager::new(config(tempdir().unwrap().path()), ProtocolLimits::default())
                .plan()
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn streamed_recovery_reads_entries_across_buffer_boundaries_and_rejects_corruption() {
        let directory = tempdir().unwrap();
        let mut config = config(directory.path());

        config.snapshot_max_bytes = 64 * 1024 * 1024;
        let manager = CheckpointManager::new(config, ProtocolLimits::default());
        let large = vec![b'L'; 3 * SNAPSHOT_READ_BUFFER + 17];
        let mut entries: Vec<SnapshotEntry> = (0..20_000u32)
            .map(|n| SnapshotEntry {
                database: (n % 3) as u8,
                key: format!("key-{n}").into_bytes(),
                expires_at: (n % 7 == 0).then(|| UnixMillis::new(4_000_000_000_000)),
                value: SnapshotValue::Bytes(vec![b'v'; (n % 200) as usize]),
            })
            .collect();

        entries.insert(
            7_000,
            SnapshotEntry {
                database: 0,
                key: b"large".to_vec(),
                expires_at: None,
                value: SnapshotValue::Bytes(large.clone()),
            },
        );
        manager
            .publish(
                2,
                &[ShardCheckpoint {
                    shard_id: 0,
                    last_sequence: 11,
                    entries: entries.clone(),
                }],
            )
            .unwrap();

        let plan = manager.plan().unwrap().unwrap();
        let mut streamed = Vec::new();

        plan.stream_shard(0, |entry| {
            streamed.push(entry);
            Ok(())
        })
        .unwrap();
        assert_eq!(streamed.len(), entries.len());
        for (streamed, written) in streamed.iter().zip(&entries) {
            assert_eq!(streamed.key, written.key);
            assert_eq!(streamed.database, written.database);
            assert_eq!(streamed.expires_at, written.expires_at);
            assert!(matches!(
                (&streamed.value, &written.value),
                (SnapshotValue::Bytes(left), SnapshotValue::Bytes(right)) if left == right
            ));
        }

        // Limits lowered after the checkpoint was written do not make it unreadable.
        let tight = ProtocolLimits {
            max_value_bytes: 64,
            max_command_bytes: 1024,
            max_connection_buffer_bytes: 1024,
            ..ProtocolLimits::default()
        };
        let tight_config = PersistenceConfig {
            data_dir: directory.path().to_path_buf(),
            snapshot_max_bytes: 64 * 1024 * 1024,
            ..PersistenceConfig::default()
        };
        let tight_plan = CheckpointManager::new(tight_config, tight)
            .plan()
            .unwrap()
            .unwrap();
        let mut count = 0;

        tight_plan
            .stream_shard(0, |_| {
                count += 1;
                Ok(())
            })
            .unwrap();
        assert_eq!(count, entries.len());

        let file = directory
            .path()
            .join("checkpoints/00000000000000000002/shard-0000.snap");
        let mut bytes = fs::read(&file).unwrap();
        let middle = bytes.len() / 2;

        bytes[middle] ^= 1;
        fs::write(&file, &bytes).unwrap();
        let error = plan.stream_shard(0, |_| Ok(())).unwrap_err();

        assert!(error.to_string().contains("digest mismatch"), "{error}");
    }

    #[test]
    fn structured_json_checkpoint_round_trips_and_restores_tree() {
        let mut shard = crate::store::KvShard::new();
        let value = serde_json::to_vec(&serde_json::json!({
            "bulk": "x".repeat(crate::json::TREE_THRESHOLD), "nested": {"n": 1}
        }))
        .unwrap();
        shard.put_json(b"document", value).unwrap();
        let deadline = crate::time::UnixMillis::new(4_000_000_000_000);
        shard.expire_at(b"document", deadline);
        shard
            .apply_json_delta(b"document", b"$.nested.n", Some(b"2"), false)
            .unwrap();
        let checkpoint = ShardCheckpoint {
            shard_id: 0,
            last_sequence: 3,
            entries: shard.snapshot_entries(),
        };
        let bytes = encode_snapshot(1, 1, &checkpoint).unwrap();
        let (decoded, _) =
            decode_snapshot(&bytes, 1, 0, ProtocolLimits::default(), u64::MAX).unwrap();
        let mut restored = crate::store::KvShard::new();
        for entry in decoded.entries {
            restored.restore_snapshot_entry(entry).unwrap();
        }
        assert!(restored.json_tree(b"document").is_some());
        assert_eq!(
            restored
                .json_tree(b"document")
                .unwrap()
                .get(b"$.nested.n")
                .unwrap(),
            Some(b"2".to_vec())
        );
        assert_eq!(restored.expires_at(b"document"), Some(deadline));
        assert_eq!(restored.snapshot_entries(), shard.snapshot_entries());
    }

    #[test]
    fn json_snapshot_round_trips_with_its_type_and_ttl() {
        let value = SnapshotValue::Json(br#"{"nested":[true,null,42]}"#.to_vec());
        let (kind, payload) = encode_value(&value).unwrap();
        assert_eq!(kind, 15);
        assert_eq!(
            decode_value(kind, &payload, ProtocolLimits::default()).unwrap(),
            value
        );
        let limits = ProtocolLimits {
            max_value_bytes: 8,
            ..ProtocolLimits::default()
        };
        assert!(decode_value(kind, &payload, limits).is_err());

        let entry = SnapshotEntry {
            database: 3,
            key: b"document".to_vec(),
            expires_at: Some(UnixMillis::new(4_000_000_000_000)),
            value,
        };
        let mut bytes = Vec::new();
        encode_entry(&mut bytes, &entry).unwrap();
        let (decoded, consumed) = decode_entry(&bytes, ProtocolLimits::default()).unwrap();
        assert_eq!(consumed, bytes.len());
        assert_eq!(decoded, entry);
    }

    #[test]
    fn snapshot_entries_keep_their_database() {
        let directory = tempdir().unwrap();
        let manager = CheckpointManager::new(config(directory.path()), ProtocolLimits::default());
        let entry = |database, value: &[u8]| SnapshotEntry {
            database,
            key: b"k".to_vec(),
            expires_at: None,
            value: SnapshotValue::Bytes(value.to_vec()),
        };

        manager
            .publish(
                1,
                &[ShardCheckpoint {
                    shard_id: 0,
                    last_sequence: 2,
                    entries: vec![entry(0, b"zero"), entry(255, b"last")],
                }],
            )
            .unwrap();

        let loaded = manager.load().unwrap().unwrap();

        assert_eq!(
            loaded.shards[0].entries,
            vec![entry(0, b"zero"), entry(255, b"last")]
        );
    }

    #[test]
    fn rejects_a_key_twice_in_one_database() {
        let directory = tempdir().unwrap();
        let manager = CheckpointManager::new(config(directory.path()), ProtocolLimits::default());
        let entry = SnapshotEntry {
            database: 4,
            key: b"k".to_vec(),
            expires_at: None,
            value: SnapshotValue::Bytes(b"v".to_vec()),
        };

        manager
            .publish(
                1,
                &[ShardCheckpoint {
                    shard_id: 0,
                    last_sequence: 2,
                    entries: vec![entry.clone(), entry],
                }],
            )
            .unwrap();

        assert!(matches!(
            manager.load(),
            Err(PersistenceError::Corruption(message)) if message.contains("duplicate key")
        ));
    }

    #[test]
    fn keeps_previous_current_when_new_generation_is_incomplete() {
        let directory = tempdir().unwrap();
        let manager = CheckpointManager::new(config(directory.path()), ProtocolLimits::default());

        manager
            .publish(
                1,
                &[ShardCheckpoint {
                    shard_id: 0,
                    last_sequence: 0,
                    entries: Vec::new(),
                }],
            )
            .unwrap();
        fs::create_dir_all(directory.path().join("checkpoints/00000000000000000002")).unwrap();

        assert_eq!(manager.load().unwrap().unwrap().generation, 1);
    }

    #[test]
    fn rejects_snapshot_corruption_before_returning_entries() {
        let directory = tempdir().unwrap();
        let manager = CheckpointManager::new(config(directory.path()), ProtocolLimits::default());

        manager
            .publish(
                1,
                &[ShardCheckpoint {
                    shard_id: 0,
                    last_sequence: 0,
                    entries: Vec::new(),
                }],
            )
            .unwrap();
        let path = directory
            .path()
            .join("checkpoints/00000000000000000001/shard-0000.snap");
        let mut bytes = fs::read(&path).unwrap();

        bytes[20] ^= 1;
        fs::write(path, bytes).unwrap();
        assert!(matches!(
            manager.load(),
            Err(PersistenceError::Corruption(_))
        ));
    }

    #[test]
    fn keeps_the_previous_generation_until_a_newer_replacement_is_durable() {
        let directory = tempdir().unwrap();
        let manager = CheckpointManager::new(config(directory.path()), ProtocolLimits::default());
        let empty = || ShardCheckpoint {
            shard_id: 0,
            last_sequence: 0,
            entries: Vec::new(),
        };

        manager.publish(1, &[empty()]).unwrap();
        manager.publish(2, &[empty()]).unwrap();
        assert!(
            directory
                .path()
                .join("checkpoints/00000000000000000001")
                .exists()
        );
        manager.publish(3, &[empty()]).unwrap();
        assert!(
            !directory
                .path()
                .join("checkpoints/00000000000000000001")
                .exists()
        );
        assert!(
            directory
                .path()
                .join("checkpoints/00000000000000000002")
                .exists()
        );
        assert_eq!(manager.load().unwrap().unwrap().generation, 3);
    }

    #[test]
    fn truncated_current_pointer_is_rejected() {
        let directory = tempdir().unwrap();
        let manager = CheckpointManager::new(config(directory.path()), ProtocolLimits::default());

        manager
            .publish(
                1,
                &[ShardCheckpoint {
                    shard_id: 0,
                    last_sequence: 0,
                    entries: Vec::new(),
                }],
            )
            .unwrap();
        fs::write(directory.path().join("CURRENT"), b"short").unwrap();
        assert!(matches!(
            manager.load(),
            Err(PersistenceError::Corruption(_))
        ));
    }

    #[test]
    fn missing_manifest_fails_load() {
        let directory = tempdir().unwrap();
        let manager = CheckpointManager::new(config(directory.path()), ProtocolLimits::default());

        manager
            .publish(
                1,
                &[ShardCheckpoint {
                    shard_id: 0,
                    last_sequence: 0,
                    entries: Vec::new(),
                }],
            )
            .unwrap();
        fs::remove_file(
            directory
                .path()
                .join("checkpoints/00000000000000000001/MANIFEST"),
        )
        .unwrap();
        assert!(manager.load().is_err());
    }

    #[test]
    fn rejects_snapshots_over_the_configured_size_limit() {
        let directory = tempdir().unwrap();
        let mut persistence = config(directory.path());

        persistence.snapshot_max_bytes = 4096;
        let manager = CheckpointManager::new(persistence, ProtocolLimits::default());
        let entries = vec![SnapshotEntry {
            database: 0,
            key: b"key".to_vec(),
            expires_at: None,
            value: SnapshotValue::Bytes(vec![0; 8192]),
        }];

        assert!(matches!(
            manager.publish(
                1,
                &[ShardCheckpoint {
                    shard_id: 0,
                    last_sequence: 1,
                    entries,
                }],
            ),
            Err(PersistenceError::Failed(_))
        ));
        assert!(manager.load().unwrap().is_none());
    }

    #[test]
    fn persists_a_monotonic_clock_floor() {
        let directory = tempdir().unwrap();
        let mut persistence = config(directory.path());

        persistence.profile = DurabilityProfile::Snapshot;
        let manager = CheckpointManager::new(persistence, ProtocolLimits::default());

        manager.persist_clock(UnixMillis::new(1_000)).unwrap();
        manager.persist_clock(UnixMillis::new(500)).unwrap();
        assert_eq!(manager.load_clock().unwrap(), Some(UnixMillis::new(1_000)));
        assert_eq!(
            manager
                .recovery_clock_floor(UnixMillis::new(2_000))
                .unwrap(),
            UnixMillis::new(2_000)
        );
        assert_eq!(
            manager.recovery_clock_floor(UnixMillis::new(100)).unwrap(),
            UnixMillis::new(1_000)
        );
    }

    #[test]
    fn rejects_corrupt_clock_files() {
        let directory = tempdir().unwrap();
        let mut persistence = config(directory.path());

        persistence.profile = DurabilityProfile::Snapshot;
        let manager = CheckpointManager::new(persistence, ProtocolLimits::default());

        manager.persist_clock(UnixMillis::new(1)).unwrap();
        fs::write(directory.path().join("CLOCK"), b"not-a-clock-file!!!!").unwrap();
        assert!(matches!(
            manager.load_clock(),
            Err(PersistenceError::Corruption(_))
        ));
    }

    #[test]
    fn rejects_snapshot_filenames_outside_the_checkpoint_directory() {
        let manifest = encode_manifest(
            1,
            &[ManifestItem {
                shard_id: 0,
                last_sequence: 0,
                byte_length: 0,
                digest: [0; 32],
                file_name: "../etc/passwd".into(),
            }],
        )
        .unwrap();

        match decode_manifest(&manifest, 1) {
            Err(PersistenceError::Corruption(message)) => {
                assert!(message.contains("unsafe"), "{message}");
            }

            Err(error) => panic!("expected unsafe filename corruption, got {error}"),
            Ok(_) => panic!("expected unsafe filename corruption"),
        }
    }

    #[test]
    fn stream_snapshot_keeps_the_assigned_id() {
        let mut shard = crate::store::KvShard::new();

        shard
            .stream_append(
                b"events",
                5,
                1,
                &[(b"field".as_slice(), b"value".as_slice())],
            )
            .unwrap();
        assert!(shard.stream_delete(b"events", 5, 1).unwrap());
        let checkpoint = ShardCheckpoint {
            shard_id: 0,
            last_sequence: 1,
            entries: shard.snapshot_entries(),
        };

        let bytes = encode_snapshot(1, 1, &checkpoint).unwrap();
        let (decoded, _) =
            decode_snapshot(&bytes, 1, 0, ProtocolLimits::default(), u64::MAX).unwrap();
        let mut restored = crate::store::KvShard::new();

        restored
            .restore_snapshot_entry(decoded.entries.into_iter().next().unwrap())
            .unwrap();
        assert_eq!(restored.stream_len(b"events").unwrap(), 0);
        assert_eq!(restored.stream_next_id(b"events", 5).unwrap(), (5, 2));

        let entry = shard.snapshot_entries().into_iter().next().unwrap();
        let (kind, payload) = encode_value(&entry.value).unwrap();
        let legacy = decode_value(
            kind,
            &payload[..payload.len() - 4],
            ProtocolLimits::default(),
        )
        .unwrap();
        let crate::store::SnapshotValue::Stream {
            groups, entries, ..
        } = legacy
        else {
            panic!("expected a stream");
        };

        assert!(groups.is_empty());
        assert!(entries.is_empty());
    }

    #[test]
    fn limit_snapshot_stores_the_count_and_reads_the_older_payload() {
        let (kind, payload) = encode_value(&SnapshotValue::Limit { used: 3 }).unwrap();

        assert_eq!((kind, payload.len()), (12, 8));
        assert_eq!(
            decode_value(kind, &payload, ProtocolLimits::default()).unwrap(),
            SnapshotValue::Limit { used: 3 }
        );

        let mut legacy = Vec::new();

        put_u64(&mut legacy, 3);
        put_u64(&mut legacy, 4_000_000_000_000);

        let value = decode_value(10, &legacy, ProtocolLimits::default()).unwrap();

        assert_eq!(value, SnapshotValue::Limit { used: 3 });

        let mut restored = crate::store::KvShard::new();

        restored
            .restore_snapshot_entry(crate::store::SnapshotEntry {
                database: 0,
                key: b"login".to_vec(),
                expires_at: Some(UnixMillis::new(4_000_000_000_000)),
                value,
            })
            .unwrap();
        assert_eq!(
            restored.value_kind(b"login"),
            Some(crate::store::ValueKind::Limit)
        );
        assert_eq!(
            restored.expires_at(b"login"),
            Some(UnixMillis::new(4_000_000_000_000))
        );
    }

    #[test]
    fn a_hash_with_field_ttls_keeps_them_through_the_snapshot() {
        let mut shard = crate::store::KvShard::new();

        shard
            .hash_set(b"h", &[(b"a".as_slice(), b"1".as_slice()), (b"b", b"2")])
            .unwrap();
        shard
            .hash_expire_fields(b"h", &[b"a"], 4_000_000_000_000)
            .unwrap();
        shard
            .hash_set(b"plain", &[(b"f".as_slice(), b"v".as_slice())])
            .unwrap();

        let mut restored = crate::store::KvShard::new();

        for entry in shard.snapshot_entries() {
            let (kind, payload) = encode_value(&entry.value).unwrap();

            assert_eq!(kind, if entry.key == b"h" { 13 } else { 6 });

            let mut round = entry;

            round.value = decode_value(kind, &payload, ProtocolLimits::default()).unwrap();
            restored.restore_snapshot_entry(round).unwrap();
        }

        assert_eq!(
            restored.field_deadline(b"h", b"a"),
            Ok(Some(4_000_000_000_000))
        );
        assert_eq!(restored.field_deadline(b"h", b"b"), Ok(None));
        assert_eq!(restored.hash_get(b"plain", b"f"), Ok(Some(&b"v"[..])));
        assert!(restored.has_field_ttls());
    }

    #[test]
    fn a_semaphore_keeps_its_live_holders_through_the_snapshot() {
        let value = SnapshotValue::Semaphore {
            next_token: 7,
            holders: vec![(5, 4_000_000_000_000), (6, 1)],
        };
        let (kind, payload) = encode_value(&value).unwrap();

        assert_eq!((kind, payload.len()), (14, 8 + 4 + 32));

        let decoded = decode_value(kind, &payload, ProtocolLimits::default()).unwrap();

        assert_eq!(decoded, value);

        let mut restored = crate::store::KvShard::new();

        restored
            .restore_snapshot_entry(crate::store::SnapshotEntry {
                database: 0,
                key: b"jobs".to_vec(),
                expires_at: Some(UnixMillis::new(4_000_000_000_000)),
                value: decoded,
            })
            .unwrap();
        assert!(restored.semaphore_holds(b"jobs", 5));
        assert!(
            !restored.semaphore_holds(b"jobs", 6),
            "an expired holder is dropped"
        );
    }

    #[test]
    fn delayed_stream_entry_survives_the_snapshot() {
        let mut shard = crate::store::KvShard::new();

        shard
            .stream_append(
                b"events",
                5,
                1,
                &[(b"field".as_slice(), b"value".as_slice())],
            )
            .unwrap();
        shard.stream_hold_until(b"events", 5, 1, 9_000).unwrap();

        let entry = shard.snapshot_entries().into_iter().next().unwrap();
        let (kind, payload) = encode_value(&entry.value).unwrap();
        let decoded = decode_value(kind, &payload, ProtocolLimits::default()).unwrap();
        let mut round = entry;

        round.value = decoded;

        let mut restored = crate::store::KvShard::new();

        restored.restore_snapshot_entry(round).unwrap();

        let again = restored.snapshot_entries().into_iter().next().unwrap();
        let crate::store::SnapshotValue::Stream { delayed, .. } = again.value else {
            panic!("expected a stream");
        };

        assert_eq!(delayed, vec![(5, 1, 9_000)]);
    }

    #[test]
    fn stream_group_cursor_survives_the_snapshot() {
        let mut shard = crate::store::KvShard::new();

        shard
            .stream_append(
                b"events",
                5,
                1,
                &[(b"field".as_slice(), b"value".as_slice())],
            )
            .unwrap();
        shard
            .stream_create_group(b"events", b"g", 0, 0, false)
            .unwrap();
        assert_eq!(
            shard.stream_group_new(b"events", b"g", 10).unwrap().len(),
            1
        );
        shard
            .stream_group_claim(b"events", b"g", b"c1", 5, 1, 1_000)
            .unwrap();

        let checkpoint = ShardCheckpoint {
            shard_id: 0,
            last_sequence: 1,
            entries: shard.snapshot_entries(),
        };

        let bytes = encode_snapshot(1, 1, &checkpoint).unwrap();
        let (decoded, _) =
            decode_snapshot(&bytes, 1, 0, ProtocolLimits::default(), u64::MAX).unwrap();
        let mut restored = crate::store::KvShard::new();

        restored
            .restore_snapshot_entry(decoded.entries.into_iter().next().unwrap())
            .unwrap();
        assert!(
            restored
                .stream_group_new(b"events", b"g", 10)
                .unwrap()
                .is_empty()
        );
        assert!(
            restored
                .stream_group_pending(b"events", b"g", 5, 1)
                .unwrap()
        );
        assert!(restored.stream_group_ack(b"events", b"g", 5, 1).unwrap());
        assert!(
            !restored
                .stream_group_pending(b"events", b"g", 5, 1)
                .unwrap()
        );
    }

    #[test]
    fn version_one_meta_and_snapshot_fixtures_remain_stable() {
        assert_eq!(encode_current(1), load_hex_fixture("current.hex"));
        assert_eq!(decode_current(&load_hex_fixture("current.hex")).unwrap(), 1);
        assert_eq!(
            encode_clock(1_700_000_000_000),
            load_hex_fixture("clock.hex")
        );
        assert_eq!(
            decode_clock(&load_hex_fixture("clock.hex")).unwrap(),
            1_700_000_000_000
        );

        let empty = empty_snapshot();

        assert_eq!(
            encode_snapshot_at(1, 1, &empty, 1_700_000_000_000).unwrap(),
            load_hex_fixture("snapshot-empty.hex")
        );
        let (decoded_empty, created) = decode_snapshot(
            &load_hex_fixture("snapshot-empty.hex"),
            1,
            0,
            ProtocolLimits::default(),
            1024 * 1024,
        )
        .unwrap();

        assert_eq!(created, 1_700_000_000_000);
        assert!(decoded_empty.entries.is_empty());

        let populated = populated_snapshot();
        let encoded = encode_snapshot_at(1, 1, &populated, 1_700_000_000_000).unwrap();

        assert_eq!(encoded, load_hex_fixture("snapshot-all-types.hex"));
        let (decoded, _) =
            decode_snapshot(&encoded, 1, 0, ProtocolLimits::default(), 1024 * 1024).unwrap();

        assert_eq!(decoded.entries.len(), populated.entries.len());

        let manifest = encode_manifest(
            1,
            &[ManifestItem {
                shard_id: 0,
                last_sequence: populated.last_sequence,
                byte_length: encoded.len() as u64,
                digest: *blake3::hash(&encoded).as_bytes(),
                file_name: "shard-0000.snap".into(),
            }],
        )
        .unwrap();

        assert_eq!(manifest, load_hex_fixture("manifest.hex"));
        assert_eq!(decode_manifest(&manifest, 1).unwrap().len(), 1);

        let mut shard = crate::KvShard::new();

        for entry in decoded.entries {
            shard.restore_snapshot_entry(entry).unwrap();
        }

        assert_eq!(shard.get_bytes(b"bytes").unwrap(), Some(b"v".as_slice()));
        assert_eq!(shard.list_len(b"list").unwrap(), 2);
        assert_eq!(shard.set_card(b"set").unwrap(), 1);
        assert_eq!(
            shard.hash_get(b"hash", b"f").unwrap(),
            Some(b"v".as_slice())
        );
        assert_eq!(shard.sorted_set_score(b"zset", b"m").unwrap(), Some(1.5));
        assert!(shard.bloom_contains(b"bloom", b"m").is_ok());
        assert!(shard.get_bytes(b"ttl").unwrap().is_none());
    }

    #[test]
    fn frozen_v1_directory_opens_as_a_prior_release() {
        let directory = tempfile::tempdir().unwrap();
        let generation = directory
            .path()
            .join("checkpoints")
            .join(format!("{:020}", 1u64));

        std::fs::create_dir_all(&generation).unwrap();
        std::fs::write(
            directory.path().join("CURRENT"),
            load_hex_fixture("current.hex"),
        )
        .unwrap();
        std::fs::write(
            directory.path().join("CLOCK"),
            load_hex_fixture("clock.hex"),
        )
        .unwrap();
        std::fs::write(
            generation.join("MANIFEST"),
            load_hex_fixture("manifest.hex"),
        )
        .unwrap();
        std::fs::write(
            generation.join("shard-0000.snap"),
            load_hex_fixture("snapshot-all-types.hex"),
        )
        .unwrap();

        let checkpoints = CheckpointManager::new(
            PersistenceConfig {
                profile: crate::DurabilityProfile::Always,
                data_dir: directory.path().to_path_buf(),
                ..PersistenceConfig::default()
            },
            ProtocolLimits::default(),
        );
        let checkpoint = checkpoints.load().unwrap().unwrap();
        let mut shard =
            crate::KvShard::with_clock(crate::SystemClock::with_floor(checkpoint.created_at));

        for entry in checkpoint.shards.into_iter().next().unwrap().entries {
            shard.restore_snapshot_entry(entry).unwrap();
        }

        assert_eq!(shard.get_bytes(b"bytes").unwrap(), Some(b"v".as_slice()));
        assert_eq!(shard.list_len(b"list").unwrap(), 2);
        assert_eq!(shard.set_card(b"set").unwrap(), 1);
        assert_eq!(
            shard.hash_get(b"hash", b"f").unwrap(),
            Some(b"v".as_slice())
        );
        assert_eq!(shard.sorted_set_score(b"zset", b"m").unwrap(), Some(1.5));
        assert!(shard.bloom_contains(b"bloom", b"m").is_ok());
        assert!(shard.get_bytes(b"ttl").unwrap().is_none());
    }

    #[test]
    fn writes_v1_snapshot_fixtures_when_requested() {
        if std::env::var_os("RUVIO_WRITE_FIXTURES").is_none() {
            return;
        }

        write_hex_fixture("current.hex", &encode_current(1));
        write_hex_fixture("clock.hex", &encode_clock(1_700_000_000_000));
        let empty = empty_snapshot();

        write_hex_fixture(
            "snapshot-empty.hex",
            &encode_snapshot_at(1, 1, &empty, 1_700_000_000_000).unwrap(),
        );
        let populated = populated_snapshot();
        let snapshot = encode_snapshot_at(1, 1, &populated, 1_700_000_000_000).unwrap();

        write_hex_fixture("snapshot-all-types.hex", &snapshot);
        write_hex_fixture(
            "manifest.hex",
            &encode_manifest(
                1,
                &[ManifestItem {
                    shard_id: 0,
                    last_sequence: populated.last_sequence,
                    byte_length: snapshot.len() as u64,
                    digest: *blake3::hash(&snapshot).as_bytes(),
                    file_name: "shard-0000.snap".into(),
                }],
            )
            .unwrap(),
        );
    }

    fn empty_snapshot() -> ShardCheckpoint {
        ShardCheckpoint {
            shard_id: 0,
            last_sequence: 0,
            entries: Vec::new(),
        }
    }

    #[test]
    fn a_scaled_bloom_filter_round_trips_and_a_legacy_one_still_loads() {
        let mut shard = crate::KvShard::new();

        for index in 0..2_000 {
            shard
                .bloom_add(b"layered", format!("item-{index}").as_bytes())
                .unwrap();
        }

        let before = shard.bloom_info(b"layered").unwrap().unwrap();

        assert!(before.filters > 1);

        let mut restored = crate::KvShard::new();

        for entry in shard.snapshot_entries() {
            let mut bytes = Vec::new();

            encode_entry(&mut bytes, &entry).unwrap();

            let (decoded, _) = decode_entry(&bytes, ProtocolLimits::default()).unwrap();

            assert_eq!(decoded, entry);
            restored.restore_snapshot_entry(decoded).unwrap();
        }

        assert_eq!(restored.bloom_info(b"layered").unwrap().unwrap(), before);

        for index in (0..2_000).step_by(37) {
            assert!(
                restored
                    .bloom_contains(b"layered", format!("item-{index}").as_bytes())
                    .unwrap()
            );
        }

        // A filter written before layers existed: capacity and item count are rebuilt from the bits.
        let mut old = crate::KvShard::new();
        let mut words = vec![0u64; 16];

        for index in 0..40u64 {
            let (first, second) = (
                index.wrapping_mul(0x9e37_79b9_7f4a_7c15u64),
                0x1234_5679u64 | 1,
            );
            let mut position = first;

            for _ in 0..7 {
                let bit = (position % 1024) as usize;

                words[bit / 64] |= 1 << (bit % 64);
                position = position.wrapping_add(second);
            }
        }

        old.restore_snapshot_entry(SnapshotEntry {
            database: 0,
            key: b"old".to_vec(),
            expires_at: None,
            value: SnapshotValue::BloomFilter {
                words,
                hash_count: 7,
            },
        })
        .unwrap();

        let info = old.bloom_info(b"old").unwrap().unwrap();

        assert_eq!(info.filters, 1);
        assert!(info.capacity > 0 && info.items > 0, "{info:?}");
        assert_eq!(info.expansion, Some(2), "an old filter now scales");
    }

    fn populated_snapshot() -> ShardCheckpoint {
        ShardCheckpoint {
            shard_id: 0,
            last_sequence: 9,
            entries: vec![
                SnapshotEntry {
                    database: 0,
                    key: b"bytes".to_vec(),
                    expires_at: None,
                    value: SnapshotValue::Bytes(b"v".to_vec()),
                },
                SnapshotEntry {
                    database: 0,
                    key: b"list".to_vec(),
                    expires_at: None,
                    value: SnapshotValue::List(vec![b"a".to_vec(), b"b".to_vec()]),
                },
                SnapshotEntry {
                    database: 0,
                    key: b"set".to_vec(),
                    expires_at: None,
                    value: SnapshotValue::Set(vec![b"a".to_vec()]),
                },
                SnapshotEntry {
                    database: 0,
                    key: b"hash".to_vec(),
                    expires_at: None,
                    value: SnapshotValue::Hash(vec![(b"f".to_vec(), b"v".to_vec())]),
                },
                SnapshotEntry {
                    database: 0,
                    key: b"zset".to_vec(),
                    expires_at: None,
                    value: SnapshotValue::SortedSet(vec![(1.5, b"m".to_vec())]),
                },
                SnapshotEntry {
                    database: 0,
                    key: b"bloom".to_vec(),
                    expires_at: None,
                    value: SnapshotValue::BloomFilter {
                        words: vec![1],
                        hash_count: 2,
                    },
                },
                SnapshotEntry {
                    database: 0,
                    key: b"ttl".to_vec(),
                    expires_at: Some(UnixMillis::new(1)),
                    value: SnapshotValue::Bytes(b"expired".to_vec()),
                },
            ],
        }
    }

    fn fixture_dir() -> std::path::PathBuf {
        std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/persistence/v1")
    }

    fn load_hex_fixture(name: &str) -> Vec<u8> {
        let text = std::fs::read_to_string(fixture_dir().join(name))
            .unwrap_or_else(|error| panic!("missing v1 fixture {name}: {error}"));
        let compact: String = text.chars().filter(|ch| !ch.is_whitespace()).collect();

        compact
            .as_bytes()
            .as_chunks::<2>()
            .0
            .iter()
            .map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap())
            .collect()
    }

    fn write_hex_fixture(name: &str, bytes: &[u8]) {
        let directory = fixture_dir();

        std::fs::create_dir_all(&directory).unwrap();
        let mut text = String::with_capacity(bytes.len() * 2 + 1);

        for byte in bytes {
            text.push_str(&format!("{byte:02x}"));
        }

        text.push('\n');
        std::fs::write(directory.join(name), text).unwrap();
    }
    fn sample_entries() -> Vec<SnapshotEntry> {
        vec![
            SnapshotEntry {
                database: 0,
                key: b"a".to_vec(),
                expires_at: None,
                value: SnapshotValue::Bytes(b"one".to_vec()),
            },
            SnapshotEntry {
                database: 2,
                key: b"set".to_vec(),
                expires_at: Some(UnixMillis::new(99)),
                value: SnapshotValue::Set(vec![b"x".to_vec(), b"y".to_vec()]),
            },
        ]
    }

    fn streamed_file(
        directory: &Path,
        entries: &[SnapshotEntry],
        maximum: u64,
    ) -> Result<ShardFile, PersistenceError> {
        std::fs::create_dir_all(directory)?;
        let mut writer = SnapshotWriter::create_at(directory, 1, 0, 1, 7, maximum, 1_000)?;

        for chunk in entries.chunks(1) {
            let mut bytes = Vec::new();

            for entry in chunk {
                encode_entry(&mut bytes, entry)?;
            }

            writer.write_chunk(&bytes, chunk.len() as u64)?;
        }

        writer.finish()
    }

    #[test]
    fn streamed_snapshot_loads_through_the_manager() {
        let directory = tempdir().unwrap();
        let manager = CheckpointManager::new(config(directory.path()), ProtocolLimits::default());

        manager.begin_round(1).unwrap();
        let mut writer = manager.shard_writer(1, 0, 1, 7).unwrap();
        let entries = sample_entries();
        let mut bytes = Vec::new();

        for entry in &entries {
            encode_entry(&mut bytes, entry).unwrap();
        }

        writer.write_chunk(&bytes, entries.len() as u64).unwrap();
        let file = writer.finish().unwrap();

        manager.commit_round(1, &[file]).unwrap();
        let loaded = manager.load().unwrap().expect("checkpoint");

        assert_eq!(loaded.generation, 1);
        assert_eq!(loaded.shards[0].last_sequence, 7);
        assert_eq!(loaded.shards[0].entries, entries);
    }

    #[test]
    fn streamed_snapshot_matches_the_fixture_and_rejects_damage() {
        let directory = tempdir().unwrap();
        let file = streamed_file(directory.path(), &sample_entries(), 1 << 20).unwrap();
        let bytes = std::fs::read(directory.path().join(&file.file_name)).unwrap();

        if std::env::var_os("RUVIO_WRITE_FIXTURES").is_some() {
            write_hex_fixture("snapshot-streamed.hex", &bytes);
        }

        assert_eq!(bytes, load_hex_fixture("snapshot-streamed.hex"));
        assert_eq!(file.byte_length, bytes.len() as u64);
        assert_eq!(&file.digest, blake3::hash(&bytes).as_bytes());
        assert_eq!(u16_at(&bytes, 10).unwrap(), 1);
        assert_eq!(u64_at(&bytes, 40).unwrap(), u64::MAX);

        let limits = ProtocolLimits::default();
        let (decoded, created_at) = decode_snapshot(&bytes, 1, 0, limits, 1 << 20).unwrap();

        assert_eq!(decoded.entries, sample_entries());
        assert_eq!(created_at, 1_000);

        // A header count other than the sentinel on a minor 1 file is damage.
        let mut counted = bytes.clone();

        counted[40..48].copy_from_slice(&2u64.to_le_bytes());
        let checksum = crc32c::crc32c(&counted[..60]);

        counted[60..64].copy_from_slice(&checksum.to_le_bytes());
        assert!(decode_snapshot(&counted, 1, 0, limits, 1 << 20).is_err());

        // Footer count that disagrees with the entries present.
        let mut short = bytes.clone();
        let footer = short.len() - SNAPSHOT_FOOTER_LEN;

        short[footer + 8..footer + 16].copy_from_slice(&3u64.to_le_bytes());
        assert!(decode_snapshot(&short, 1, 0, limits, 1 << 20).is_err());

        // A minor from the future is refused.
        let mut future = bytes;

        future[10..12].copy_from_slice(&2u16.to_le_bytes());
        assert!(decode_snapshot(&future, 1, 0, limits, 1 << 20).is_err());
    }

    #[test]
    fn streamed_writer_enforces_the_size_limit_and_abort_removes_the_file() {
        let directory = tempdir().unwrap();
        let error = streamed_file(directory.path(), &sample_entries(), 120).unwrap_err();

        assert!(error.to_string().contains("size limit"));

        let mut writer =
            SnapshotWriter::create_at(directory.path(), 1, 0, 1, 7, 1 << 20, 1_000).unwrap();
        let temporary = directory.path().join(".shard-0000.snap.tmp");

        writer.write_chunk(&[], 0).unwrap();
        assert!(temporary.exists());
        writer.abort();
        assert!(!temporary.exists());
    }

    #[test]
    fn concurrent_clock_writes_never_fail_and_keep_the_highest_floor() {
        let directory = tempdir().unwrap();
        let mut config = config(directory.path());

        config.profile = DurabilityProfile::Snapshot;
        let manager =
            std::sync::Arc::new(CheckpointManager::new(config, ProtocolLimits::default()));
        let writers: Vec<_> = (0..8u64)
            .map(|writer| {
                let manager = std::sync::Arc::clone(&manager);

                std::thread::spawn(move || {
                    for round in 0..50u64 {
                        manager
                            .persist_clock(UnixMillis::new(1_000 + round * 8 + writer))
                            .unwrap();
                    }
                })
            })
            .collect();

        for writer in writers {
            writer.join().unwrap();
        }
        assert_eq!(
            manager.load_clock().unwrap(),
            Some(UnixMillis::new(1_000 + 49 * 8 + 7))
        );
    }

    #[test]
    fn startup_sweep_removes_leftover_temporary_files_only() {
        let directory = tempdir().unwrap();
        let generation = directory.path().join("checkpoints/00000000000000000003");

        std::fs::create_dir_all(&generation).unwrap();
        for path in [
            directory.path().join(".CLOCK.tmp"),
            generation.join(".shard-0000.snap.tmp"),
            generation.join("shard-0000.snap"),
            directory.path().join("CURRENT"),
        ] {
            std::fs::write(path, b"x").unwrap();
        }

        let mut config = config(directory.path());

        config.profile = DurabilityProfile::Snapshot;
        let _lock = super::super::DataDirectoryLock::acquire(&config).unwrap();

        assert!(!directory.path().join(".CLOCK.tmp").exists());
        assert!(!generation.join(".shard-0000.snap.tmp").exists());
        assert!(generation.join("shard-0000.snap").exists());
        assert!(directory.path().join("CURRENT").exists());
    }

    #[test]
    fn a_round_that_failed_after_current_never_reuses_its_generation() {
        let directory = tempdir().unwrap();
        let mut config = config(directory.path());

        config.profile = DurabilityProfile::Snapshot;
        let manager = CheckpointManager::new(config.clone(), ProtocolLimits::default());
        let shard = |last_sequence| ShardCheckpoint {
            shard_id: 0,
            last_sequence,
            entries: vec![SnapshotEntry {
                database: 0,
                key: b"k".to_vec(),
                expires_at: None,
                value: SnapshotValue::Bytes(b"v".to_vec()),
            }],
        };

        manager.publish(1, &[shard(1)]).unwrap();
        let generation = manager.allocate_generation(1).unwrap();

        assert_eq!(generation, 2);
        manager.begin_round(generation).unwrap();
        let file = manager.write_shard_file(generation, 1, &shard(2)).unwrap();

        // CURRENT is renamed, then the directory sync fails: the caller sees an error.
        super::super::crash::arm_io_fault(super::super::crash::CrashSite::Manifest);
        assert!(manager.commit_round(generation, &[file]).is_err());

        // A caller whose published counter still says 1 must not clear what CURRENT names.
        assert!(manager.begin_round(2).is_err());
        assert_eq!(manager.allocate_generation(1).unwrap(), 3);
        assert_eq!(manager.plan().unwrap().unwrap().generation, 2);
        manager
            .plan()
            .unwrap()
            .unwrap()
            .stream_shard(0, |_| Ok(()))
            .unwrap();
        // After a restart too.
        assert_eq!(
            CheckpointManager::new(config, ProtocolLimits::default())
                .allocate_generation(0)
                .unwrap(),
            3
        );
    }

    #[test]
    fn begin_round_discards_a_stale_directory_of_the_same_generation() {
        let directory = tempdir().unwrap();
        let manager = CheckpointManager::new(config(directory.path()), ProtocolLimits::default());

        manager.begin_round(5).unwrap();
        let stale = directory
            .path()
            .join("checkpoints/00000000000000000005/leftover");

        std::fs::write(&stale, b"x").unwrap();
        manager.begin_round(5).unwrap();
        assert!(!stale.exists());
    }

    #[test]
    fn an_abandoned_round_directory_is_removed_but_a_published_one_is_kept() {
        let directory = tempdir().unwrap();
        let manager = CheckpointManager::new(config(directory.path()), ProtocolLimits::default());
        let generation = manager.allocate_generation(0).unwrap();

        manager.begin_round(generation).unwrap();
        let file = manager
            .write_shard_file(
                generation,
                1,
                &ShardCheckpoint {
                    shard_id: 0,
                    last_sequence: 0,
                    entries: Vec::new(),
                },
            )
            .unwrap();
        manager.commit_round(generation, &[file]).unwrap();

        let abandoned = manager.allocate_generation(generation).unwrap();
        manager.begin_round(abandoned).unwrap();
        manager.discard_round(abandoned).unwrap();
        manager.discard_round(generation).unwrap();

        assert!(!manager.generation_directory(abandoned).exists());
        assert!(manager.generation_directory(generation).exists());
        assert!(manager.load().unwrap().is_some());
    }

    /// A copy that fails (here, the source does not match its digest) leaves no partial
    /// file in the new round.
    #[test]
    fn a_failed_carry_leaves_no_temporary_file() {
        let directory = tempdir().unwrap();
        let manager = CheckpointManager::new(config(directory.path()), ProtocolLimits::default());

        manager.begin_round(1).unwrap();
        let file = manager
            .write_shard_file(
                1,
                1,
                &ShardCheckpoint {
                    shard_id: 0,
                    last_sequence: 7,
                    entries: Vec::new(),
                },
            )
            .unwrap();
        manager.commit_round(1, &[file]).unwrap();
        let source = manager.generation_directory(1).join("shard-0000.snap");
        let mut bytes = std::fs::read(&source).unwrap();
        let digest = bytes.len() - 10;

        bytes[digest] ^= 0xff;
        std::fs::write(&source, &bytes).unwrap();
        manager.begin_round(2).unwrap();

        assert!(manager.carry_shard_file(1, 2, 0).is_err());
        let left: Vec<_> = std::fs::read_dir(manager.generation_directory(2))
            .unwrap()
            .map(|entry| entry.unwrap().file_name())
            .collect();
        assert!(left.is_empty(), "{left:?}");
    }
}
