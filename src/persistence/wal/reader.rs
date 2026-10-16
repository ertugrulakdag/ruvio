//! Reading WAL frames back for replication, CDC and bounded history.

use super::*;

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
