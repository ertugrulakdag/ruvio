//! Reading and validating snapshot files, whole or streamed, and the replica resync bytes.

use super::*;

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

pub(super) fn decode_snapshot(
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
pub(super) const SNAPSHOT_READ_BUFFER: usize = 1024 * 1024;

/// Verifies a snapshot file and passes its entries to `sink` without holding the file in
/// memory: one streamed pass checks the digests and framing, a second decodes the entries
/// (each still CRC-checked). Returns `(last_sequence, created_at)`.
#[allow(clippy::too_many_arguments)]
pub(super) fn stream_snapshot_file(
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
