use std::time::{SystemTime, UNIX_EPOCH};

use crate::{OwnedMutation, mutation::Mutation, protocol::ProtocolLimits};

use super::PersistenceError;

pub(crate) const WAL_MAGIC: &[u8; 8] = b"RUVWAL01";
pub(crate) const WAL_HEADER_LEN: usize = 52;
pub(crate) const RECORD_HEADER_LEN: usize = 28;
pub(crate) const RECORD_MIN_LEN: usize = 32;
const FORMAT_MAJOR: u16 = 1;
/// Minor 1 adds SetRange (58) and ListSet/ListInsert/ListRemove (59-61). A minor-0
/// reader refuses these files instead of misreading them. Minor 2 adds BloomReserve with an
/// explicit expansion (62) and BloomAddMany (63); a reserve with the default expansion still
/// uses record 9. Minor 3 adds SetBytesExpiring (64).
pub(crate) const FORMAT_MINOR: u16 = 3;
/// The first record type of each minor. A new record type goes in a new last row with the
/// next minor, and `FORMAT_MINOR` follows it; the codec tests refuse a decoder that knows a
/// record type this table does not.
#[cfg(test)]
const RECORD_MINORS: [(u16, u16); 4] = [(1, 0), (58, 1), (62, 2), (64, 3)];
#[cfg(test)]
const LAST_RECORD_TYPE: u16 = 64;

pub(crate) struct SegmentHeader {
    pub minor: u16,
    pub shard_id: u32,
    pub segment_id: u64,
    pub first_sequence: u64,
}

pub(crate) struct DecodedRecord {
    pub sequence: u64,
    pub database: u8,
    pub mutation: OwnedMutation,
}

/// Record flags bits 0..8 hold the logical database. Database 0 keeps the flags zero, so
/// its records are byte-identical to files written before databases existed.
const RECORD_DATABASE_MASK: u32 = 0xff;

pub(crate) fn encode_segment_header(
    shard_id: u32,
    segment_id: u64,
    first_sequence: u64,
) -> Vec<u8> {
    let created = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX);

    encode_segment_header_at(
        FORMAT_MAJOR,
        FORMAT_MINOR,
        shard_id,
        segment_id,
        first_sequence,
        created,
        0,
    )
}

pub(crate) fn encode_segment_header_at(
    major: u16,
    minor: u16,
    shard_id: u32,
    segment_id: u64,
    first_sequence: u64,
    created: u64,
    flags: u32,
) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(WAL_HEADER_LEN);

    bytes.extend_from_slice(WAL_MAGIC);
    put_u16(&mut bytes, major);
    put_u16(&mut bytes, minor);
    put_u32(&mut bytes, WAL_HEADER_LEN as u32);
    put_u32(&mut bytes, shard_id);
    put_u64(&mut bytes, segment_id);
    put_u64(&mut bytes, first_sequence);
    put_u64(&mut bytes, created);
    put_u32(&mut bytes, flags);
    let checksum = crc32c::crc32c(&bytes);

    put_u32(&mut bytes, checksum);
    debug_assert_eq!(bytes.len(), WAL_HEADER_LEN);
    bytes
}

pub(crate) fn decode_segment_header(bytes: &[u8]) -> Result<SegmentHeader, PersistenceError> {
    if bytes.len() != WAL_HEADER_LEN {
        return Err(PersistenceError::Corruption(
            "truncated WAL segment header".into(),
        ));
    }

    if &bytes[..8] != WAL_MAGIC {
        return Err(PersistenceError::Corruption("invalid WAL magic".into()));
    }

    let expected = u32::from_le_bytes(bytes[48..52].try_into().unwrap());

    if crc32c::crc32c(&bytes[..48]) != expected {
        return Err(PersistenceError::Corruption(
            "WAL header checksum mismatch".into(),
        ));
    }

    let major = u16::from_le_bytes(bytes[8..10].try_into().unwrap());
    let minor = u16::from_le_bytes(bytes[10..12].try_into().unwrap());

    if major != FORMAT_MAJOR {
        return Err(PersistenceError::Unsupported(format!(
            "WAL major version {major}"
        )));
    }

    if minor > FORMAT_MINOR {
        return Err(PersistenceError::Unsupported(format!(
            "WAL minor version {minor}"
        )));
    }

    if u32::from_le_bytes(bytes[12..16].try_into().unwrap()) as usize != WAL_HEADER_LEN {
        return Err(PersistenceError::Corruption(
            "invalid WAL header length".into(),
        ));
    }

    if u32::from_le_bytes(bytes[44..48].try_into().unwrap()) != 0 {
        return Err(PersistenceError::Unsupported(
            "WAL required feature flags".into(),
        ));
    }

    Ok(SegmentHeader {
        minor,
        shard_id: u32::from_le_bytes(bytes[16..20].try_into().unwrap()),
        segment_id: u64::from_le_bytes(bytes[20..28].try_into().unwrap()),
        first_sequence: u64::from_le_bytes(bytes[28..36].try_into().unwrap()),
    })
}

#[cfg(test)]
pub(crate) fn encode_record(
    shard_id: u32,
    sequence: u64,
    database: u8,
    mutation: &OwnedMutation,
) -> Result<Vec<u8>, PersistenceError> {
    let mut bytes = Vec::new();

    encode_record_into(&mut bytes, shard_id, sequence, database, mutation)?;
    Ok(bytes)
}

pub(crate) fn encode_record_into(
    bytes: &mut Vec<u8>,
    shard_id: u32,
    sequence: u64,
    database: u8,
    mutation: &OwnedMutation,
) -> Result<(), PersistenceError> {
    bytes.clear();
    bytes.resize(RECORD_HEADER_LEN, 0);
    let record_type = encode_payload(mutation, bytes)?;
    finish_record(bytes, shard_id, sequence, database, record_type)
}

/// Encodes a borrowed mutation without first copying it into an `OwnedMutation`.
/// Variants outside the hot write set fall back to the owned encoder, so the
/// bytes are identical either way.
pub(crate) fn encode_borrowed_record_into(
    bytes: &mut Vec<u8>,
    shard_id: u32,
    sequence: u64,
    database: u8,
    mutation: &Mutation<'_>,
) -> Result<(), PersistenceError> {
    bytes.clear();
    bytes.resize(RECORD_HEADER_LEN, 0);

    let record_type = match encode_borrowed_payload(mutation, bytes)? {
        Some(record_type) => record_type,
        None => encode_payload(&Mutation::to_owned(*mutation), bytes)?,
    };

    finish_record(bytes, shard_id, sequence, database, record_type)
}

fn encode_borrowed_payload(
    mutation: &Mutation<'_>,
    payload: &mut Vec<u8>,
) -> Result<Option<u16>, PersistenceError> {
    let record_type = match mutation {
        Mutation::SetBytes { key, value } => {
            put_bytes(payload, key)?;
            put_bytes(payload, value)?;
            1
        }
        Mutation::SetBytesExpiring {
            key,
            value,
            expires_at,
        } => {
            put_bytes(payload, key)?;
            put_bytes(payload, value)?;
            put_u64(payload, *expires_at);
            64
        }
        Mutation::SetInteger { key, value } => {
            put_bytes(payload, key)?;
            payload.extend_from_slice(&value.to_le_bytes());
            2
        }
        Mutation::Delete { keys } => {
            put_byte_list(payload, keys)?;
            3
        }
        Mutation::ListPushFront { key, values } => {
            put_bytes(payload, key)?;
            put_byte_list(payload, values)?;
            4
        }
        Mutation::ListPopFront { key } => {
            put_bytes(payload, key)?;
            5
        }
        Mutation::ListPushBack { key, values } => {
            put_bytes(payload, key)?;
            put_byte_list(payload, values)?;
            14
        }
        Mutation::ListPopBack { key } => {
            put_bytes(payload, key)?;
            15
        }
        Mutation::SetAdd { key, members } => {
            put_bytes(payload, key)?;
            put_byte_list(payload, members)?;
            6
        }
        Mutation::SetRemove { key, members } => {
            put_bytes(payload, key)?;
            put_byte_list(payload, members)?;
            7
        }
        Mutation::HashSet { key, pairs } => {
            put_bytes(payload, key)?;
            put_pair_list(payload, pairs)?;
            19
        }
        Mutation::HashDelete { key, fields } => {
            put_bytes(payload, key)?;
            put_byte_list(payload, fields)?;
            20
        }
        Mutation::SortedSetAdd { key, pairs } => {
            put_bytes(payload, key)?;

            if let [(score, member)] = pairs {
                put_u64(payload, score.to_bits());
                put_bytes(payload, member)?;
                8
            } else {
                put_score_member_list(payload, pairs)?;
                21
            }
        }
        Mutation::SortedSetRemove { key, members } => {
            put_bytes(payload, key)?;
            put_byte_list(payload, members)?;
            22
        }
        Mutation::ExpireAt { key, unix_millis } => {
            put_bytes(payload, key)?;
            put_u64(payload, *unix_millis);
            11
        }
        Mutation::Persist { key } => {
            put_bytes(payload, key)?;
            12
        }
        Mutation::SetVersioned {
            key,
            value,
            version,
        } => {
            put_bytes(payload, key)?;
            put_u64(payload, *version);
            put_bytes(payload, value)?;
            45
        }
        // Scripts and EXEC hand over their buffered batch; encode it in place, not via a
        // deep copy of every key and value.
        Mutation::Transaction { mutations } => encode_transaction(mutations, payload)?,
        _ => return Ok(None),
    };

    Ok(Some(record_type))
}

fn finish_record(
    bytes: &mut Vec<u8>,
    shard_id: u32,
    sequence: u64,
    database: u8,
    record_type: u16,
) -> Result<(), PersistenceError> {
    let payload_len = bytes.len() - RECORD_HEADER_LEN;
    let total_length = u32::try_from(bytes.len() + 4)
        .map_err(|_| PersistenceError::Failed("WAL record exceeds 4 GiB".into()))?;

    bytes[..4].copy_from_slice(&total_length.to_le_bytes());
    bytes[4..6].copy_from_slice(&1u16.to_le_bytes());
    bytes[6..8].copy_from_slice(&record_type.to_le_bytes());
    bytes[8..12].copy_from_slice(&u32::from(database).to_le_bytes());
    bytes[12..16].copy_from_slice(&shard_id.to_le_bytes());
    bytes[16..24].copy_from_slice(&sequence.to_le_bytes());
    bytes[24..28].copy_from_slice(
        &u32::try_from(payload_len)
            .map_err(|_| PersistenceError::Failed("WAL payload exceeds 4 GiB".into()))?
            .to_le_bytes(),
    );
    let checksum = crc32c::crc32c(&bytes[4..]);

    put_u32(bytes, checksum);
    Ok(())
}

/// One value mutation as a self-checking frame, for `DUMP`.
pub(crate) fn encode_dump_record(mutation: &OwnedMutation) -> Result<Vec<u8>, PersistenceError> {
    let mut bytes = Vec::new();

    encode_record_into(&mut bytes, 0, 0, 0, mutation)?;
    Ok(bytes)
}

/// The inverse of [`encode_dump_record`]; the frame's length and CRC are verified.
pub(crate) fn decode_dump_record(frame: &[u8]) -> Result<OwnedMutation, PersistenceError> {
    let limits = ProtocolLimits {
        max_command_bytes: usize::MAX,
        ..ProtocolLimits::default()
    };

    decode_record(frame, 0, limits).map(|record| record.mutation)
}

/// Largest record any reader accepts: the length field is 32 bits.
pub(crate) const WAL_RECORD_MAX_BYTES: usize = u32::MAX as usize;

/// A logged record was validated against the limits in force when it was written. Reading it
/// back must not depend on today's command and size limits (lowered since, or smaller on a
/// replica), or recovery and replication refuse a valid log. Only the database count stays.
fn record_read_limits(limits: ProtocolLimits) -> ProtocolLimits {
    ProtocolLimits {
        max_arguments: usize::MAX,
        max_key_bytes: usize::MAX,
        max_value_bytes: usize::MAX,
        max_command_bytes: WAL_RECORD_MAX_BYTES,
        ..limits
    }
}

/// A cheap look at `frame`'s fixed header, without its checksum or payload: the length,
/// version, flags, shard and payload length agree, and the sequence is at least
/// `min_sequence`. Scanning damaged bytes for a record checks this before a checksum.
pub(crate) fn record_header_plausible(
    frame: &[u8],
    expected_shard: u32,
    min_sequence: u64,
) -> bool {
    if frame.len() < RECORD_MIN_LEN {
        return false;
    }
    let field = |at: usize| u32::from_le_bytes(frame[at..at + 4].try_into().unwrap());

    field(0) as usize == frame.len()
        && u16::from_le_bytes(frame[4..6].try_into().unwrap()) == 1
        && field(8) & !RECORD_DATABASE_MASK == 0
        && field(12) == expected_shard
        && u64::from_le_bytes(frame[16..24].try_into().unwrap()) >= min_sequence
        && field(24) as usize == frame.len() - RECORD_MIN_LEN
}

pub(crate) fn decode_record(
    frame: &[u8],
    expected_shard: u32,
    limits: ProtocolLimits,
) -> Result<DecodedRecord, PersistenceError> {
    let limits = record_read_limits(limits);

    if frame.len() < RECORD_MIN_LEN {
        return Err(PersistenceError::Corruption(
            "WAL record is shorter than its header".into(),
        ));
    }

    let declared = u32::from_le_bytes(frame[..4].try_into().unwrap()) as usize;

    if declared != frame.len() {
        return Err(PersistenceError::Corruption(
            "WAL record length mismatch".into(),
        ));
    }

    if frame.len() > limits.max_command_bytes {
        return Err(PersistenceError::Corruption(
            "WAL record exceeds configured command limit".into(),
        ));
    }

    let checksum_offset = frame.len() - 4;
    let expected_checksum = u32::from_le_bytes(frame[checksum_offset..].try_into().unwrap());

    if crc32c::crc32c(&frame[4..checksum_offset]) != expected_checksum {
        return Err(PersistenceError::Corruption(
            "WAL record checksum mismatch".into(),
        ));
    }

    if u16::from_le_bytes(frame[4..6].try_into().unwrap()) != 1 {
        return Err(PersistenceError::Unsupported("WAL record version".into()));
    }

    let record_type = u16::from_le_bytes(frame[6..8].try_into().unwrap());

    let flags = u32::from_le_bytes(frame[8..12].try_into().unwrap());

    if flags & !RECORD_DATABASE_MASK != 0 {
        return Err(PersistenceError::Unsupported(
            "WAL record feature flags".into(),
        ));
    }

    let database = (flags & RECORD_DATABASE_MASK) as u8;

    let shard_id = u32::from_le_bytes(frame[12..16].try_into().unwrap());

    if shard_id != expected_shard {
        return Err(PersistenceError::Corruption(
            "WAL record shard mismatch".into(),
        ));
    }

    let sequence = u64::from_le_bytes(frame[16..24].try_into().unwrap());
    let payload_length = u32::from_le_bytes(frame[24..28].try_into().unwrap()) as usize;

    if payload_length != checksum_offset - RECORD_HEADER_LEN {
        return Err(PersistenceError::Corruption(
            "WAL payload length mismatch".into(),
        ));
    }

    let mutation = decode_payload(record_type, &frame[28..checksum_offset], limits)?;

    Ok(DecodedRecord {
        sequence,
        database,
        mutation,
    })
}

fn encode_payload(
    mutation: &OwnedMutation,
    payload: &mut Vec<u8>,
) -> Result<u16, PersistenceError> {
    let record_type = match mutation {
        OwnedMutation::SetBytes { key, value } => {
            put_bytes(payload, key)?;
            put_bytes(payload, value)?;
            1
        }
        OwnedMutation::SetBytesExpiring {
            key,
            value,
            expires_at,
        } => {
            put_bytes(payload, key)?;
            put_bytes(payload, value)?;
            put_u64(payload, *expires_at);
            64
        }

        OwnedMutation::JsonPut { key, value } => {
            put_bytes(payload, key)?;
            put_bytes(payload, value)?;
            54
        }

        OwnedMutation::JsonSetPath { key, path, value } => {
            put_bytes(payload, key)?;
            put_bytes(payload, path)?;
            put_bytes(payload, value)?;
            55
        }

        OwnedMutation::JsonMergePath { key, path, value } => {
            put_bytes(payload, key)?;
            put_bytes(payload, path)?;
            put_bytes(payload, value)?;
            56
        }

        OwnedMutation::JsonDelPath { key, path } => {
            put_bytes(payload, key)?;
            put_bytes(payload, path)?;
            57
        }

        OwnedMutation::SetInteger { key, value } => {
            put_bytes(payload, key)?;
            payload.extend_from_slice(&value.to_le_bytes());
            2
        }

        OwnedMutation::Delete { keys } => {
            put_byte_list(payload, keys)?;
            3
        }

        OwnedMutation::ListPushFront { key, values } => {
            put_bytes(payload, key)?;
            put_byte_list(payload, values)?;
            4
        }

        OwnedMutation::ListPopFront { key } => {
            put_bytes(payload, key)?;
            5
        }

        OwnedMutation::ListPushBack { key, values } => {
            put_bytes(payload, key)?;
            put_byte_list(payload, values)?;
            14
        }

        OwnedMutation::ListPopBack { key } => {
            put_bytes(payload, key)?;
            15
        }

        OwnedMutation::ListTrim { key, start, stop } => {
            put_bytes(payload, key)?;
            payload.extend_from_slice(&start.to_le_bytes());
            payload.extend_from_slice(&stop.to_le_bytes());
            16
        }

        OwnedMutation::SetRange { key, offset, bytes } => {
            put_bytes(payload, key)?;
            put_u64(payload, *offset);
            put_bytes(payload, bytes)?;
            58
        }

        OwnedMutation::ListSet { key, index, value } => {
            put_bytes(payload, key)?;
            payload.extend_from_slice(&index.to_le_bytes());
            put_bytes(payload, value)?;
            59
        }

        OwnedMutation::ListInsert {
            key,
            before,
            pivot,
            value,
        } => {
            put_bytes(payload, key)?;
            payload.push(u8::from(*before));
            put_bytes(payload, pivot)?;
            put_bytes(payload, value)?;
            60
        }

        OwnedMutation::ListRemove { key, count, value } => {
            put_bytes(payload, key)?;
            payload.extend_from_slice(&count.to_le_bytes());
            put_bytes(payload, value)?;
            61
        }

        OwnedMutation::SetAdd { key, members } => {
            put_bytes(payload, key)?;
            put_byte_list(payload, members)?;
            6
        }

        OwnedMutation::SetRemove { key, members } => {
            put_bytes(payload, key)?;
            put_byte_list(payload, members)?;
            7
        }

        OwnedMutation::SetStore { key, members } => {
            put_bytes(payload, key)?;
            put_byte_list(payload, members)?;
            17
        }

        OwnedMutation::SetMove {
            source,
            destination,
            member,
        } => {
            put_bytes(payload, source)?;
            put_bytes(payload, destination)?;
            put_bytes(payload, member)?;
            18
        }

        OwnedMutation::HashSet { key, pairs } => {
            put_bytes(payload, key)?;
            put_pair_list(payload, pairs)?;
            19
        }

        OwnedMutation::HashDelete { key, fields } => {
            put_bytes(payload, key)?;
            put_byte_list(payload, fields)?;
            20
        }

        OwnedMutation::HashFieldExpire {
            key,
            fields,
            unix_millis,
        } => {
            put_bytes(payload, key)?;
            put_u64(payload, *unix_millis);
            put_byte_list(payload, fields)?;
            49
        }

        OwnedMutation::HashFieldPersist { key, fields } => {
            put_bytes(payload, key)?;
            put_byte_list(payload, fields)?;
            50
        }

        OwnedMutation::SemaphoreAcquire {
            key,
            token,
            expires_at,
        } => {
            put_bytes(payload, key)?;
            put_u64(payload, *token);
            put_u64(payload, *expires_at);
            51
        }

        OwnedMutation::SemaphoreRelease { key, token } => {
            put_bytes(payload, key)?;
            put_u64(payload, *token);
            52
        }

        OwnedMutation::SortedSetAdd { key, pairs } => {
            put_bytes(payload, key)?;

            if let [(score, member)] = pairs.as_slice() {
                put_u64(payload, score.to_bits());
                put_bytes(payload, member)?;
                8
            } else {
                put_score_member_list(payload, pairs)?;
                21
            }
        }

        OwnedMutation::SortedSetRemove { key, members } => {
            put_bytes(payload, key)?;
            put_byte_list(payload, members)?;
            22
        }

        OwnedMutation::BloomReserve {
            key,
            error_rate,
            capacity,
            expansion,
        } => {
            put_bytes(payload, key)?;
            put_u64(payload, error_rate.to_bits());
            put_u64(payload, *capacity);

            if *expansion == crate::store::BLOOM_DEFAULT_EXPANSION {
                9
            } else {
                put_u32(payload, *expansion);
                62
            }
        }

        OwnedMutation::BloomAddMany { key, values } => {
            put_bytes(payload, key)?;
            put_byte_list(payload, values)?;
            63
        }

        OwnedMutation::BloomAdd { key, value } => {
            put_bytes(payload, key)?;
            put_bytes(payload, value)?;
            10
        }

        OwnedMutation::StreamAppend {
            key,
            millis,
            sequence,
            fields,
        } => {
            put_bytes(payload, key)?;
            put_u64(payload, *millis);
            put_u64(payload, *sequence);
            put_pair_list(payload, fields)?;
            24
        }

        OwnedMutation::StreamDelete {
            key,
            millis,
            sequence,
        } => {
            put_bytes(payload, key)?;
            put_u64(payload, *millis);
            put_u64(payload, *sequence);
            25
        }

        OwnedMutation::StreamTrim { key, max_len } => {
            put_bytes(payload, key)?;
            put_u64(payload, *max_len);
            26
        }

        OwnedMutation::StreamTrimMinId {
            key,
            millis,
            sequence,
        } => {
            put_bytes(payload, key)?;
            put_u64(payload, *millis);
            put_u64(payload, *sequence);
            41
        }

        OwnedMutation::StreamSetId {
            key,
            millis,
            sequence,
        } => {
            put_bytes(payload, key)?;
            put_u64(payload, *millis);
            put_u64(payload, *sequence);
            40
        }

        OwnedMutation::StreamGroupCreate {
            key,
            group,
            millis,
            sequence,
            mkstream,
        } => {
            put_bytes(payload, key)?;
            put_bytes(payload, group)?;
            put_u64(payload, *millis);
            put_u64(payload, *sequence);
            put_u64(payload, u64::from(*mkstream));
            27
        }

        OwnedMutation::StreamClaim {
            key,
            group,
            consumer,
            millis,
            sequence,
            delivered_at,
        } => {
            put_bytes(payload, key)?;
            put_bytes(payload, group)?;
            put_bytes(payload, consumer)?;
            put_u64(payload, *millis);
            put_u64(payload, *sequence);
            put_u64(payload, *delivered_at);
            28
        }

        OwnedMutation::StreamReclaim {
            key,
            group,
            consumer,
            millis,
            sequence,
            delivered_at,
            deliveries,
        } => {
            put_bytes(payload, key)?;
            put_bytes(payload, group)?;
            put_bytes(payload, consumer)?;
            put_u64(payload, *millis);
            put_u64(payload, *sequence);
            put_u64(payload, *delivered_at);
            put_u64(payload, *deliveries);
            30
        }

        OwnedMutation::StreamForceClaim {
            key,
            group,
            consumer,
            millis,
            sequence,
            delivered_at,
            deliveries,
        } => {
            put_bytes(payload, key)?;
            put_bytes(payload, group)?;
            put_bytes(payload, consumer)?;
            put_u64(payload, *millis);
            put_u64(payload, *sequence);
            put_u64(payload, *delivered_at);
            put_u64(payload, *deliveries);
            35
        }

        OwnedMutation::StreamAck {
            key,
            group,
            millis,
            sequence,
        } => {
            put_bytes(payload, key)?;
            put_bytes(payload, group)?;
            put_u64(payload, *millis);
            put_u64(payload, *sequence);
            29
        }

        OwnedMutation::StreamGroupDestroy { key, group } => {
            put_bytes(payload, key)?;
            put_bytes(payload, group)?;
            31
        }

        OwnedMutation::StreamGroupSetId {
            key,
            group,
            millis,
            sequence,
        } => {
            put_bytes(payload, key)?;
            put_bytes(payload, group)?;
            put_u64(payload, *millis);
            put_u64(payload, *sequence);
            32
        }

        OwnedMutation::StreamGroupDelConsumer {
            key,
            group,
            consumer,
        } => {
            put_bytes(payload, key)?;
            put_bytes(payload, group)?;
            put_bytes(payload, consumer)?;
            33
        }

        OwnedMutation::StreamGroupCreateConsumer {
            key,
            group,
            consumer,
        } => {
            put_bytes(payload, key)?;
            put_bytes(payload, group)?;
            put_bytes(payload, consumer)?;
            42
        }

        OwnedMutation::StreamHold {
            key,
            millis,
            sequence,
            ready_at,
        } => {
            put_bytes(payload, key)?;
            put_u64(payload, *millis);
            put_u64(payload, *sequence);
            put_u64(payload, *ready_at);
            43
        }

        OwnedMutation::LeasePut {
            key,
            token,
            expires_at,
        } => {
            put_bytes(payload, key)?;
            put_u64(payload, *token);
            put_u64(payload, *expires_at);
            44
        }

        OwnedMutation::SetVersioned {
            key,
            value,
            version,
        } => {
            put_bytes(payload, key)?;
            put_u64(payload, *version);
            put_bytes(payload, value)?;
            45
        }

        OwnedMutation::LimitPut {
            key,
            used,
            expires_at,
        } => {
            put_bytes(payload, key)?;
            put_u64(payload, *used);

            match expires_at {
                Some(expires_at) => {
                    put_u64(payload, *expires_at);
                    46
                }

                None => 48,
            }
        }

        OwnedMutation::IdempotencyPut {
            key,
            value,
            expires_at,
        } => {
            put_bytes(payload, key)?;
            put_bytes(payload, value)?;
            put_u64(payload, *expires_at);
            53
        }
        OwnedMutation::OncePut { key, expires_at } => {
            put_bytes(payload, key)?;
            put_u64(payload, *expires_at);
            47
        }

        OwnedMutation::StreamGroupAdvance {
            key,
            group,
            millis,
            sequence,
        } => {
            put_bytes(payload, key)?;
            put_bytes(payload, group)?;
            put_u64(payload, *millis);
            put_u64(payload, *sequence);
            34
        }

        OwnedMutation::ExpireAt { key, unix_millis } => {
            put_bytes(payload, key)?;
            put_u64(payload, *unix_millis);
            11
        }

        OwnedMutation::Persist { key } => {
            put_bytes(payload, key)?;
            12
        }

        OwnedMutation::Rename {
            source,
            destination,
        } => {
            put_bytes(payload, source)?;
            put_bytes(payload, destination)?;
            13
        }

        OwnedMutation::FlushDatabase => 36,
        OwnedMutation::FlushAll => 37,
        OwnedMutation::SwapDatabases { first, second } => {
            payload.push(*first);
            payload.push(*second);
            38
        }

        OwnedMutation::MoveKey { key, destination } => {
            put_bytes(payload, key)?;
            payload.push(*destination);
            39
        }

        OwnedMutation::Transaction { mutations } => encode_transaction(mutations, payload)?,
    };

    Ok(record_type)
}

/// A batch that must replay all or nothing. A batch of one write is stored as that write's
/// own record: replaying one record is already atomic, and it skips the wrapper's framing.
fn encode_transaction(
    mutations: &[OwnedMutation],
    payload: &mut Vec<u8>,
) -> Result<u16, PersistenceError> {
    if mutations
        .iter()
        .any(|mutation| matches!(mutation, OwnedMutation::Transaction { .. }))
    {
        return Err(PersistenceError::Failed(
            "nested WAL transactions are not supported".into(),
        ));
    }

    if let [only] = mutations {
        return encode_payload(only, payload);
    }

    put_u32(
        payload,
        u32::try_from(mutations.len())
            .map_err(|_| PersistenceError::Failed("too many transaction mutations".into()))?,
    );

    for mutation in mutations {
        let mark = payload.len();

        payload.extend_from_slice(&[0; 6]);
        let record_type = encode_payload(mutation, payload)?;
        let inner_len = u32::try_from(payload.len() - mark - 6)
            .map_err(|_| PersistenceError::Failed("transaction mutation exceeds 4 GiB".into()))?;

        payload[mark..mark + 2].copy_from_slice(&record_type.to_le_bytes());
        payload[mark + 2..mark + 6].copy_from_slice(&inner_len.to_le_bytes());
    }

    Ok(23)
}

fn decode_payload(
    record_type: u16,
    payload: &[u8],
    limits: ProtocolLimits,
) -> Result<OwnedMutation, PersistenceError> {
    let mut decoder = Decoder::new(payload);
    let mutation = match record_type {
        1 => OwnedMutation::SetBytes {
            key: decoder.key(limits)?,
            value: decoder.value(limits)?,
        },
        // Additive record type: older readers reject it, so upgrade replicas and recovery
        // tools first.
        64 => OwnedMutation::SetBytesExpiring {
            key: decoder.key(limits)?,
            value: decoder.value(limits)?,
            expires_at: decoder.u64()?,
        },
        // Additive record type: older readers reject it, so upgrade replicas and recovery
        // tools before writing JSON values.
        54 => OwnedMutation::JsonPut {
            key: decoder.key(limits)?,
            value: decoder.value(limits)?,
        },
        55 => OwnedMutation::JsonSetPath {
            key: decoder.key(limits)?,
            path: decoder.value(limits)?,
            value: decoder.value(limits)?,
        },
        56 => OwnedMutation::JsonMergePath {
            key: decoder.key(limits)?,
            path: decoder.value(limits)?,
            value: decoder.value(limits)?,
        },
        57 => OwnedMutation::JsonDelPath {
            key: decoder.key(limits)?,
            path: decoder.value(limits)?,
        },
        2 => OwnedMutation::SetInteger {
            key: decoder.key(limits)?,
            value: decoder.i64()?,
        },
        3 => OwnedMutation::Delete {
            keys: decoder.byte_list(limits.max_arguments, limits.max_key_bytes)?,
        },
        4 => OwnedMutation::ListPushFront {
            key: decoder.key(limits)?,
            values: decoder.byte_list(limits.max_arguments, limits.max_value_bytes)?,
        },
        5 => OwnedMutation::ListPopFront {
            key: decoder.key(limits)?,
        },
        6 => OwnedMutation::SetAdd {
            key: decoder.key(limits)?,
            members: decoder.byte_list(limits.max_arguments, limits.max_value_bytes)?,
        },
        7 => OwnedMutation::SetRemove {
            key: decoder.key(limits)?,
            members: decoder.byte_list(limits.max_arguments, limits.max_value_bytes)?,
        },
        8 => OwnedMutation::SortedSetAdd {
            key: decoder.key(limits)?,
            pairs: vec![(f64::from_bits(decoder.u64()?), decoder.value(limits)?)],
        },
        21 => OwnedMutation::SortedSetAdd {
            key: decoder.key(limits)?,
            pairs: decoder.score_member_list(limits.max_arguments, limits.max_value_bytes)?,
        },
        22 => OwnedMutation::SortedSetRemove {
            key: decoder.key(limits)?,
            members: decoder.byte_list(limits.max_arguments, limits.max_value_bytes)?,
        },
        23 => {
            let count = decoder.u32()? as usize;

            if count > limits.max_arguments.saturating_mul(16) {
                return Err(PersistenceError::Corruption(
                    "WAL transaction mutation count exceeds configured limit".into(),
                ));
            }

            let mut mutations = Vec::with_capacity(count.min(1024));

            for _ in 0..count {
                let record_type = decoder.u16()?;
                let length = decoder.u32()? as usize;
                let payload = decoder.take(length)?;
                let mutation = decode_payload(record_type, payload, limits)?;

                if matches!(mutation, OwnedMutation::Transaction { .. }) {
                    return Err(PersistenceError::Corruption(
                        "nested WAL transactions are not supported".into(),
                    ));
                }

                mutations.push(mutation);
            }

            OwnedMutation::Transaction { mutations }
        }

        9 => OwnedMutation::BloomReserve {
            key: decoder.key(limits)?,
            error_rate: f64::from_bits(decoder.u64()?),
            capacity: decoder.u64()?,
            expansion: crate::store::BLOOM_DEFAULT_EXPANSION,
        },
        62 => OwnedMutation::BloomReserve {
            key: decoder.key(limits)?,
            error_rate: f64::from_bits(decoder.u64()?),
            capacity: decoder.u64()?,
            expansion: decoder.u32()?,
        },
        63 => OwnedMutation::BloomAddMany {
            key: decoder.key(limits)?,
            values: decoder.byte_list(limits.max_arguments, limits.max_value_bytes)?,
        },
        10 => OwnedMutation::BloomAdd {
            key: decoder.key(limits)?,
            value: decoder.value(limits)?,
        },
        24 => OwnedMutation::StreamAppend {
            key: decoder.key(limits)?,
            millis: decoder.u64()?,
            sequence: decoder.u64()?,
            fields: decoder.pair_list(limits.max_arguments, limits.max_value_bytes)?,
        },
        25 => OwnedMutation::StreamDelete {
            key: decoder.key(limits)?,
            millis: decoder.u64()?,
            sequence: decoder.u64()?,
        },
        26 => OwnedMutation::StreamTrim {
            key: decoder.key(limits)?,
            max_len: decoder.u64()?,
        },
        41 => OwnedMutation::StreamTrimMinId {
            key: decoder.key(limits)?,
            millis: decoder.u64()?,
            sequence: decoder.u64()?,
        },
        40 => OwnedMutation::StreamSetId {
            key: decoder.key(limits)?,
            millis: decoder.u64()?,
            sequence: decoder.u64()?,
        },
        27 => OwnedMutation::StreamGroupCreate {
            key: decoder.key(limits)?,
            group: decoder.value(limits)?,
            millis: decoder.u64()?,
            sequence: decoder.u64()?,
            mkstream: match decoder.u64()? {
                0 => false,
                1 => true,
                _ => {
                    return Err(PersistenceError::Corruption(
                        "invalid stream group create flag".into(),
                    ));
                }
            },
        },
        28 => {
            let key = decoder.key(limits)?;
            let group = decoder.value(limits)?;
            let consumer = decoder.value(limits)?;
            let millis = decoder.u64()?;
            let sequence = decoder.u64()?;
            let delivered_at = if decoder.is_empty() {
                0
            } else {
                decoder.u64()?
            };

            OwnedMutation::StreamClaim {
                key,
                group,
                consumer,
                millis,
                sequence,
                delivered_at,
            }
        }

        30 => OwnedMutation::StreamReclaim {
            key: decoder.key(limits)?,
            group: decoder.value(limits)?,
            consumer: decoder.value(limits)?,
            millis: decoder.u64()?,
            sequence: decoder.u64()?,
            delivered_at: decoder.u64()?,
            deliveries: decoder.u64()?,
        },
        31 => OwnedMutation::StreamGroupDestroy {
            key: decoder.key(limits)?,
            group: decoder.value(limits)?,
        },
        32 => OwnedMutation::StreamGroupSetId {
            key: decoder.key(limits)?,
            group: decoder.value(limits)?,
            millis: decoder.u64()?,
            sequence: decoder.u64()?,
        },
        42 => OwnedMutation::StreamGroupCreateConsumer {
            key: decoder.key(limits)?,
            group: decoder.value(limits)?,
            consumer: decoder.value(limits)?,
        },
        43 => OwnedMutation::StreamHold {
            key: decoder.key(limits)?,
            millis: decoder.u64()?,
            sequence: decoder.u64()?,
            ready_at: decoder.u64()?,
        },
        44 => OwnedMutation::LeasePut {
            key: decoder.key(limits)?,
            token: decoder.u64()?,
            expires_at: decoder.u64()?,
        },
        45 => OwnedMutation::SetVersioned {
            key: decoder.key(limits)?,
            version: decoder.u64()?,
            value: decoder.value(limits)?,
        },
        46 => OwnedMutation::LimitPut {
            key: decoder.key(limits)?,
            used: decoder.u64()?,
            expires_at: Some(decoder.u64()?),
        },
        48 => OwnedMutation::LimitPut {
            key: decoder.key(limits)?,
            used: decoder.u64()?,
            expires_at: None,
        },
        // Additive record type: older readers reject it, so upgrade replicas and recovery
        // tools first.
        53 => OwnedMutation::IdempotencyPut {
            key: decoder.key(limits)?,
            value: decoder.value(limits)?,
            expires_at: decoder.u64()?,
        },
        47 => OwnedMutation::OncePut {
            key: decoder.key(limits)?,
            expires_at: decoder.u64()?,
        },
        33 => OwnedMutation::StreamGroupDelConsumer {
            key: decoder.key(limits)?,
            group: decoder.value(limits)?,
            consumer: decoder.value(limits)?,
        },
        34 => OwnedMutation::StreamGroupAdvance {
            key: decoder.key(limits)?,
            group: decoder.value(limits)?,
            millis: decoder.u64()?,
            sequence: decoder.u64()?,
        },
        35 => OwnedMutation::StreamForceClaim {
            key: decoder.key(limits)?,
            group: decoder.value(limits)?,
            consumer: decoder.value(limits)?,
            millis: decoder.u64()?,
            sequence: decoder.u64()?,
            delivered_at: decoder.u64()?,
            deliveries: decoder.u64()?,
        },
        29 => OwnedMutation::StreamAck {
            key: decoder.key(limits)?,
            group: decoder.value(limits)?,
            millis: decoder.u64()?,
            sequence: decoder.u64()?,
        },
        11 => OwnedMutation::ExpireAt {
            key: decoder.key(limits)?,
            unix_millis: decoder.u64()?,
        },
        12 => OwnedMutation::Persist {
            key: decoder.key(limits)?,
        },
        13 => OwnedMutation::Rename {
            source: decoder.key(limits)?,
            destination: decoder.key(limits)?,
        },
        36 => OwnedMutation::FlushDatabase,
        37 => OwnedMutation::FlushAll,
        38 => OwnedMutation::SwapDatabases {
            first: decoder.u8()?,
            second: decoder.u8()?,
        },
        39 => OwnedMutation::MoveKey {
            key: decoder.key(limits)?,
            destination: decoder.u8()?,
        },
        14 => OwnedMutation::ListPushBack {
            key: decoder.key(limits)?,
            values: decoder.byte_list(limits.max_arguments, limits.max_value_bytes)?,
        },
        15 => OwnedMutation::ListPopBack {
            key: decoder.key(limits)?,
        },
        16 => OwnedMutation::ListTrim {
            key: decoder.key(limits)?,
            start: decoder.i64()?,
            stop: decoder.i64()?,
        },
        58 => OwnedMutation::SetRange {
            key: decoder.key(limits)?,
            offset: decoder.u64()?,
            bytes: decoder.value(limits)?,
        },
        59 => OwnedMutation::ListSet {
            key: decoder.key(limits)?,
            index: decoder.i64()?,
            value: decoder.value(limits)?,
        },
        60 => OwnedMutation::ListInsert {
            key: decoder.key(limits)?,
            before: match decoder.u8()? {
                0 => false,
                1 => true,
                _ => return Err(PersistenceError::Corruption("invalid LINSERT side".into())),
            },
            pivot: decoder.value(limits)?,
            value: decoder.value(limits)?,
        },
        61 => OwnedMutation::ListRemove {
            key: decoder.key(limits)?,
            count: decoder.i64()?,
            value: decoder.value(limits)?,
        },
        17 => OwnedMutation::SetStore {
            key: decoder.key(limits)?,
            members: decoder.byte_list(limits.max_arguments, limits.max_value_bytes)?,
        },
        18 => OwnedMutation::SetMove {
            source: decoder.key(limits)?,
            destination: decoder.key(limits)?,
            member: decoder.value(limits)?,
        },
        19 => OwnedMutation::HashSet {
            key: decoder.key(limits)?,
            pairs: decoder.pair_list(limits.max_arguments, limits.max_value_bytes)?,
        },
        20 => OwnedMutation::HashDelete {
            key: decoder.key(limits)?,
            fields: decoder.byte_list(limits.max_arguments, limits.max_value_bytes)?,
        },
        49 => OwnedMutation::HashFieldExpire {
            key: decoder.key(limits)?,
            unix_millis: decoder.u64()?,
            fields: decoder.byte_list(limits.max_arguments, limits.max_value_bytes)?,
        },
        50 => OwnedMutation::HashFieldPersist {
            key: decoder.key(limits)?,
            fields: decoder.byte_list(limits.max_arguments, limits.max_value_bytes)?,
        },
        51 => OwnedMutation::SemaphoreAcquire {
            key: decoder.key(limits)?,
            token: decoder.u64()?,
            expires_at: decoder.u64()?,
        },
        52 => OwnedMutation::SemaphoreRelease {
            key: decoder.key(limits)?,
            token: decoder.u64()?,
        },
        _ => {
            return Err(PersistenceError::Unsupported(format!(
                "WAL record type {record_type}"
            )));
        }
    };

    if !decoder.is_empty() {
        return Err(PersistenceError::Corruption(
            "trailing bytes in WAL mutation payload".into(),
        ));
    }

    Ok(mutation)
}

type OwnedBytePair = (Vec<u8>, Vec<u8>);
type OwnedScoreMember = (f64, Vec<u8>);

fn put_score_member_list<M: AsRef<[u8]>>(
    output: &mut Vec<u8>,
    pairs: &[(f64, M)],
) -> Result<(), PersistenceError> {
    put_u32(
        output,
        u32::try_from(pairs.len())
            .map_err(|_| PersistenceError::Failed("too many sorted-set pairs".into()))?,
    );

    for (score, member) in pairs {
        put_u64(output, score.to_bits());
        put_bytes(output, member.as_ref())?;
    }

    Ok(())
}

fn put_pair_list<A: AsRef<[u8]>, B: AsRef<[u8]>>(
    output: &mut Vec<u8>,
    pairs: &[(A, B)],
) -> Result<(), PersistenceError> {
    put_u32(
        output,
        u32::try_from(pairs.len())
            .map_err(|_| PersistenceError::Failed("too many mutation pairs".into()))?,
    );

    for (field, value) in pairs {
        put_bytes(output, field.as_ref())?;
        put_bytes(output, value.as_ref())?;
    }

    Ok(())
}

fn put_byte_list<T: AsRef<[u8]>>(
    output: &mut Vec<u8>,
    values: &[T],
) -> Result<(), PersistenceError> {
    put_u32(
        output,
        u32::try_from(values.len())
            .map_err(|_| PersistenceError::Failed("too many mutation values".into()))?,
    );

    for value in values {
        put_bytes(output, value.as_ref())?;
    }

    Ok(())
}

fn put_bytes(output: &mut Vec<u8>, value: &[u8]) -> Result<(), PersistenceError> {
    put_u32(
        output,
        u32::try_from(value.len())
            .map_err(|_| PersistenceError::Failed("mutation value exceeds 4 GiB".into()))?,
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

struct Decoder<'a> {
    remaining: &'a [u8],
}

impl<'a> Decoder<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { remaining: bytes }
    }

    fn is_empty(&self) -> bool {
        self.remaining.is_empty()
    }

    fn take(&mut self, length: usize) -> Result<&'a [u8], PersistenceError> {
        if length > self.remaining.len() {
            return Err(PersistenceError::Corruption(
                "truncated WAL mutation payload".into(),
            ));
        }

        let (value, remaining) = self.remaining.split_at(length);

        self.remaining = remaining;
        Ok(value)
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

    fn i64(&mut self) -> Result<i64, PersistenceError> {
        Ok(i64::from_le_bytes(self.take(8)?.try_into().unwrap()))
    }

    fn bytes(&mut self, maximum: usize) -> Result<Vec<u8>, PersistenceError> {
        let length = self.u32()? as usize;

        // Checked before any allocation: a corrupt length must not drive memory use.
        if length > maximum {
            return Err(PersistenceError::Corruption(
                "WAL byte string exceeds configured limit".into(),
            ));
        }

        Ok(self.take(length)?.to_vec())
    }

    fn key(&mut self, limits: ProtocolLimits) -> Result<Vec<u8>, PersistenceError> {
        self.bytes(limits.max_key_bytes)
    }

    fn value(&mut self, limits: ProtocolLimits) -> Result<Vec<u8>, PersistenceError> {
        self.bytes(limits.max_value_bytes)
    }

    fn byte_list(
        &mut self,
        maximum_count: usize,
        maximum_bytes: usize,
    ) -> Result<Vec<Vec<u8>>, PersistenceError> {
        let count = self.u32()? as usize;

        if count > maximum_count {
            return Err(PersistenceError::Corruption(
                "WAL collection count exceeds configured limit".into(),
            ));
        }

        let mut values = Vec::with_capacity(count);

        for _ in 0..count {
            values.push(self.bytes(maximum_bytes)?);
        }

        Ok(values)
    }

    fn score_member_list(
        &mut self,
        maximum_count: usize,
        maximum_bytes: usize,
    ) -> Result<Vec<OwnedScoreMember>, PersistenceError> {
        let count = self.u32()? as usize;

        if count > maximum_count {
            return Err(PersistenceError::Corruption(
                "WAL sorted-set pair count exceeds configured limit".into(),
            ));
        }

        let mut pairs = Vec::with_capacity(count);

        for _ in 0..count {
            pairs.push((f64::from_bits(self.u64()?), self.bytes(maximum_bytes)?));
        }

        Ok(pairs)
    }

    fn pair_list(
        &mut self,
        maximum_count: usize,
        maximum_bytes: usize,
    ) -> Result<Vec<OwnedBytePair>, PersistenceError> {
        let count = self.u32()? as usize;

        if count > maximum_count {
            return Err(PersistenceError::Corruption(
                "WAL pair count exceeds configured limit".into(),
            ));
        }

        let mut pairs = Vec::with_capacity(count);

        for _ in 0..count {
            pairs.push((self.bytes(maximum_bytes)?, self.bytes(maximum_bytes)?));
        }

        Ok(pairs)
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn borrowed_encoding_matches_owned_encoding() {
        use std::borrow::Cow;

        let values = [Cow::Borrowed(&b"a"[..]), Cow::Borrowed(&b"bc"[..])];
        let pairs = [(Cow::Borrowed(&b"f"[..]), Cow::Borrowed(&b"v"[..]))];
        let one_score = [(1.5f64, Cow::Borrowed(&b"m"[..]))];
        let two_scores = [
            (1.5f64, Cow::Borrowed(&b"m"[..])),
            (-2.0, Cow::Borrowed(&b"n"[..])),
        ];
        let owned = [OwnedMutation::FlushAll];
        let borrowed = [
            Mutation::SetBytes {
                key: b"k",
                value: b"v",
            },
            Mutation::SetInteger {
                key: b"k",
                value: -7,
            },
            Mutation::Delete { keys: &values },
            Mutation::ListPushFront {
                key: b"k",
                values: &values,
            },
            Mutation::ListPopFront { key: b"k" },
            Mutation::ListPushBack {
                key: b"k",
                values: &values,
            },
            Mutation::ListPopBack { key: b"k" },
            Mutation::SetAdd {
                key: b"k",
                members: &values,
            },
            Mutation::SetRemove {
                key: b"k",
                members: &values,
            },
            Mutation::HashSet {
                key: b"k",
                pairs: &pairs,
            },
            Mutation::HashDelete {
                key: b"k",
                fields: &values,
            },
            Mutation::SortedSetAdd {
                key: b"k",
                pairs: &one_score,
            },
            Mutation::SortedSetAdd {
                key: b"k",
                pairs: &two_scores,
            },
            Mutation::SortedSetRemove {
                key: b"k",
                members: &values,
            },
            Mutation::ExpireAt {
                key: b"k",
                unix_millis: 99,
            },
            Mutation::Persist { key: b"k" },
            Mutation::SetVersioned {
                key: b"k",
                value: b"v",
                version: 3,
            },
            Mutation::Rename {
                source: b"a",
                destination: b"b",
            },
            Mutation::Transaction { mutations: &owned },
            Mutation::FlushDatabase,
        ];

        for mutation in borrowed {
            let mut fast = Vec::new();

            encode_borrowed_record_into(&mut fast, 2, 9, 1, &mutation).unwrap();
            assert_eq!(
                fast,
                encode_record(2, 9, 1, &mutation.to_owned()).unwrap(),
                "{mutation:?}"
            );
        }
    }

    use super::*;

    #[test]
    fn round_trips_every_wal_mutation_type() {
        let mutations = vec![
            OwnedMutation::SetBytes {
                key: b"k".to_vec(),
                value: b"v".to_vec(),
            },
            OwnedMutation::SetInteger {
                key: b"i".to_vec(),
                value: -42,
            },
            OwnedMutation::Delete {
                keys: vec![b"a".to_vec(), b"b".to_vec()],
            },
            OwnedMutation::ListPushFront {
                key: b"l".to_vec(),
                values: vec![b"a".to_vec()],
            },
            OwnedMutation::ListPopFront { key: b"l".to_vec() },
            OwnedMutation::SetAdd {
                key: b"s".to_vec(),
                members: vec![b"a".to_vec()],
            },
            OwnedMutation::SetRemove {
                key: b"s".to_vec(),
                members: vec![b"a".to_vec()],
            },
            OwnedMutation::SortedSetAdd {
                key: b"z".to_vec(),
                pairs: vec![(1.5, b"m".to_vec())],
            },
            OwnedMutation::SortedSetAdd {
                key: b"z".to_vec(),
                pairs: vec![(1.0, b"a".to_vec()), (2.0, b"b".to_vec())],
            },
            OwnedMutation::SortedSetRemove {
                key: b"z".to_vec(),
                members: vec![b"a".to_vec()],
            },
            OwnedMutation::Transaction {
                mutations: vec![
                    OwnedMutation::SetBytes {
                        key: b"a".to_vec(),
                        value: b"1".to_vec(),
                    },
                    OwnedMutation::SetInteger {
                        key: b"i".to_vec(),
                        value: 2,
                    },
                ],
            },
            OwnedMutation::BloomReserve {
                key: b"b".to_vec(),
                error_rate: 0.01,
                capacity: 100,
                expansion: 2,
            },
            OwnedMutation::BloomReserve {
                key: b"b".to_vec(),
                error_rate: 0.01,
                capacity: 100,
                expansion: 0,
            },
            OwnedMutation::BloomAdd {
                key: b"b".to_vec(),
                value: b"m".to_vec(),
            },
            OwnedMutation::BloomAddMany {
                key: b"b".to_vec(),
                values: vec![b"m".to_vec(), b"n".to_vec()],
            },
            OwnedMutation::StreamAppend {
                key: b"stream".to_vec(),
                millis: 10,
                sequence: 0,
                fields: vec![(b"field".to_vec(), b"value".to_vec())],
            },
            OwnedMutation::StreamDelete {
                key: b"stream".to_vec(),
                millis: 10,
                sequence: 0,
            },
            OwnedMutation::StreamTrim {
                key: b"stream".to_vec(),
                max_len: 1,
            },
            OwnedMutation::StreamTrimMinId {
                key: b"stream".to_vec(),
                millis: 2,
                sequence: 0,
            },
            OwnedMutation::StreamSetId {
                key: b"stream".to_vec(),
                millis: 10,
                sequence: 1,
            },
            OwnedMutation::StreamGroupCreate {
                key: b"stream".to_vec(),
                group: b"workers".to_vec(),
                millis: 0,
                sequence: 0,
                mkstream: true,
            },
            OwnedMutation::StreamClaim {
                key: b"stream".to_vec(),
                group: b"workers".to_vec(),
                consumer: b"c1".to_vec(),
                millis: 10,
                sequence: 0,
                delivered_at: 50,
            },
            OwnedMutation::StreamReclaim {
                key: b"stream".to_vec(),
                group: b"workers".to_vec(),
                consumer: b"c2".to_vec(),
                millis: 10,
                sequence: 0,
                delivered_at: 80,
                deliveries: 2,
            },
            OwnedMutation::StreamForceClaim {
                key: b"stream".to_vec(),
                group: b"workers".to_vec(),
                consumer: b"c3".to_vec(),
                millis: 11,
                sequence: 0,
                delivered_at: 90,
                deliveries: 1,
            },
            OwnedMutation::StreamAck {
                key: b"stream".to_vec(),
                group: b"workers".to_vec(),
                millis: 10,
                sequence: 0,
            },
            OwnedMutation::StreamGroupDestroy {
                key: b"stream".to_vec(),
                group: b"workers".to_vec(),
            },
            OwnedMutation::StreamGroupSetId {
                key: b"stream".to_vec(),
                group: b"workers".to_vec(),
                millis: 20,
                sequence: 1,
            },
            OwnedMutation::StreamGroupDelConsumer {
                key: b"stream".to_vec(),
                group: b"workers".to_vec(),
                consumer: b"c1".to_vec(),
            },
            OwnedMutation::StreamGroupCreateConsumer {
                key: b"stream".to_vec(),
                group: b"workers".to_vec(),
                consumer: b"c2".to_vec(),
            },
            OwnedMutation::StreamHold {
                key: b"stream".to_vec(),
                millis: 30,
                sequence: 1,
                ready_at: 9_000,
            },
            OwnedMutation::LeasePut {
                key: b"lock".to_vec(),
                token: 2,
                expires_at: 5_000,
            },
            OwnedMutation::SetVersioned {
                key: b"doc".to_vec(),
                value: b"12".to_vec(),
                version: 2,
            },
            OwnedMutation::LimitPut {
                key: b"login".to_vec(),
                used: 2,
                expires_at: Some(5_000),
            },
            OwnedMutation::LimitPut {
                key: b"login".to_vec(),
                used: 3,
                expires_at: None,
            },
            OwnedMutation::OncePut {
                key: b"pay".to_vec(),
                expires_at: 9_000,
            },
            OwnedMutation::IdempotencyPut {
                key: b"request".to_vec(),
                value: b"RVIDEM01\x01\x01\x01fo\x00\xff".to_vec(),
                expires_at: 9_000,
            },
            OwnedMutation::StreamGroupAdvance {
                key: b"stream".to_vec(),
                group: b"workers".to_vec(),
                millis: 30,
                sequence: 0,
            },
            OwnedMutation::ExpireAt {
                key: b"k".to_vec(),
                unix_millis: 42,
            },
            OwnedMutation::Persist { key: b"k".to_vec() },
            OwnedMutation::Rename {
                source: b"old".to_vec(),
                destination: b"new".to_vec(),
            },
            OwnedMutation::ListPushBack {
                key: b"l".to_vec(),
                values: vec![b"z".to_vec()],
            },
            OwnedMutation::ListPopBack { key: b"l".to_vec() },
            OwnedMutation::ListTrim {
                key: b"l".to_vec(),
                start: 0,
                stop: 1,
            },
            OwnedMutation::SetRange {
                key: b"bits".to_vec(),
                offset: 7,
                bytes: b"xy".to_vec(),
            },
            OwnedMutation::ListSet {
                key: b"l".to_vec(),
                index: -1,
                value: b"v".to_vec(),
            },
            OwnedMutation::ListInsert {
                key: b"l".to_vec(),
                before: true,
                pivot: b"p".to_vec(),
                value: b"v".to_vec(),
            },
            OwnedMutation::ListRemove {
                key: b"l".to_vec(),
                count: -2,
                value: b"v".to_vec(),
            },
            OwnedMutation::SetStore {
                key: b"s".to_vec(),
                members: vec![b"a".to_vec(), b"c".to_vec()],
            },
            OwnedMutation::SetMove {
                source: b"from".to_vec(),
                destination: b"to".to_vec(),
                member: b"a".to_vec(),
            },
            OwnedMutation::HashSet {
                key: b"h".to_vec(),
                pairs: vec![(b"f".to_vec(), b"v".to_vec())],
            },
            OwnedMutation::HashDelete {
                key: b"h".to_vec(),
                fields: vec![b"f".to_vec()],
            },
            OwnedMutation::HashFieldExpire {
                key: b"h".to_vec(),
                fields: vec![b"f".to_vec(), b"g".to_vec()],
                unix_millis: 9_000,
            },
            OwnedMutation::HashFieldPersist {
                key: b"h".to_vec(),
                fields: vec![b"f".to_vec()],
            },
            OwnedMutation::SemaphoreAcquire {
                key: b"jobs".to_vec(),
                token: 1 << 20,
                expires_at: 9_000,
            },
            OwnedMutation::SemaphoreRelease {
                key: b"jobs".to_vec(),
                token: 1 << 20,
            },
        ];

        for (index, mutation) in mutations.into_iter().enumerate() {
            let encoded = encode_record(3, index as u64 + 1, 0, &mutation).unwrap();
            let decoded = decode_record(&encoded, 3, ProtocolLimits::default()).unwrap();

            assert_eq!(decoded.sequence, index as u64 + 1);
            assert_eq!(decoded.mutation, mutation);
        }
    }

    #[test]
    fn json_put_round_trips_with_a_distinct_opcode_and_the_set_bytes_payload() {
        let key = b"document".to_vec();
        let value = br#"{"nested":[true,null,42]}"#.to_vec();
        let json = crate::mutation::Mutation::JsonPut {
            key: &key,
            value: &value,
        }
        .to_owned();
        assert_eq!(json.write_keys(), vec![key.clone()]);
        let bytes = OwnedMutation::SetBytes { key, value };
        let encoded = encode_record(2, 7, 3, &json).unwrap();
        let encoded_bytes = encode_record(2, 7, 3, &bytes).unwrap();

        assert_eq!(u16::from_le_bytes(encoded[6..8].try_into().unwrap()), 54);
        assert_eq!(
            &encoded[RECORD_HEADER_LEN..encoded.len() - 4],
            &encoded_bytes[RECORD_HEADER_LEN..encoded_bytes.len() - 4]
        );
        let decoded = decode_record(&encoded, 2, ProtocolLimits::default()).unwrap();
        assert_eq!(decoded.database, 3);
        assert_eq!(decoded.mutation, json);

        let transaction = OwnedMutation::Transaction {
            mutations: vec![json, bytes],
        };
        let encoded = encode_record(2, 8, 3, &transaction).unwrap();
        assert_eq!(
            decode_record(&encoded, 2, ProtocolLimits::default())
                .unwrap()
                .mutation,
            transaction
        );
    }

    #[test]
    fn a_batch_of_one_write_is_encoded_as_that_write() {
        let write = OwnedMutation::SetBytes {
            key: b"key".to_vec(),
            value: b"value".to_vec(),
        };
        let single = [write.clone()];
        let mut borrowed = Vec::new();

        encode_borrowed_record_into(
            &mut borrowed,
            2,
            9,
            1,
            &crate::mutation::Mutation::Transaction { mutations: &single },
        )
        .unwrap();
        assert_eq!(borrowed, encode_record(2, 9, 1, &write).unwrap());
        assert_eq!(
            encode_record(
                2,
                9,
                1,
                &OwnedMutation::Transaction {
                    mutations: single.to_vec()
                }
            )
            .unwrap(),
            borrowed
        );

        let pair = [write.clone(), write];
        let transaction = OwnedMutation::Transaction {
            mutations: pair.to_vec(),
        };

        encode_borrowed_record_into(
            &mut borrowed,
            2,
            9,
            1,
            &crate::mutation::Mutation::Transaction { mutations: &pair },
        )
        .unwrap();
        assert_eq!(borrowed, encode_record(2, 9, 1, &transaction).unwrap());
        assert_eq!(
            decode_record(&borrowed, 2, ProtocolLimits::default())
                .unwrap()
                .mutation,
            transaction
        );
    }

    #[test]
    fn json_path_deltas_round_trip_with_distinct_opcodes_and_write_keys() {
        let key = b"document";
        let path = b"$.nested[0]";
        let value = br#"{"answer":42}"#;
        let mutations = [
            (
                55,
                crate::mutation::Mutation::JsonSetPath { key, path, value }.to_owned(),
            ),
            (
                56,
                crate::mutation::Mutation::JsonMergePath { key, path, value }.to_owned(),
            ),
            (
                57,
                crate::mutation::Mutation::JsonDelPath { key, path }.to_owned(),
            ),
        ];

        for (opcode, mutation) in &mutations {
            assert_eq!(mutation.write_keys(), vec![key.to_vec()]);
            let encoded = encode_record(2, 7, 3, mutation).unwrap();
            assert_eq!(
                u16::from_le_bytes(encoded[6..8].try_into().unwrap()),
                *opcode
            );
            let decoded = decode_record(&encoded, 2, ProtocolLimits::default()).unwrap();
            assert_eq!(decoded.database, 3);
            assert_eq!(&decoded.mutation, mutation);
        }

        let transaction = OwnedMutation::Transaction {
            mutations: mutations
                .iter()
                .map(|(_, mutation)| mutation.clone())
                .collect(),
        };
        assert_eq!(transaction.write_keys(), vec![key.to_vec(); 3]);
        let encoded = encode_record(2, 8, 3, &transaction).unwrap();
        assert_eq!(
            decode_record(&encoded, 2, ProtocolLimits::default())
                .unwrap()
                .mutation,
            transaction
        );
    }

    #[test]
    fn json_path_delta_decoder_rejects_oversize_and_malformed_payloads() {
        let limits = ProtocolLimits {
            max_key_bytes: 3,
            max_value_bytes: 4,
            ..ProtocolLimits::default()
        };
        for opcode in [55, 56, 57] {
            let mut payload = Vec::new();
            put_bytes(&mut payload, b"long").unwrap();
            put_bytes(&mut payload, b"$").unwrap();
            if opcode != 57 {
                put_bytes(&mut payload, b"null").unwrap();
            }
            assert!(matches!(
                decode_payload(opcode, &payload, limits),
                Err(PersistenceError::Corruption(_))
            ));

            payload.clear();
            put_bytes(&mut payload, b"key").unwrap();
            put_bytes(&mut payload, b"$.long").unwrap();
            if opcode != 57 {
                put_bytes(&mut payload, b"null").unwrap();
            }
            assert!(matches!(
                decode_payload(opcode, &payload, limits),
                Err(PersistenceError::Corruption(_))
            ));

            payload.clear();
            put_bytes(&mut payload, b"key").unwrap();
            put_bytes(&mut payload, b"$").unwrap();
            if opcode != 57 {
                put_bytes(&mut payload, b"12345").unwrap();
                assert!(matches!(
                    decode_payload(opcode, &payload, limits),
                    Err(PersistenceError::Corruption(_))
                ));
                payload.clear();
                put_bytes(&mut payload, b"key").unwrap();
                put_bytes(&mut payload, b"$").unwrap();
                put_bytes(&mut payload, b"null").unwrap();
            }
            assert!(decode_payload(opcode, &payload[..payload.len() - 1], limits).is_err());
            assert!(decode_payload(opcode, &payload, limits).is_ok());
            payload.push(0);
            assert!(decode_payload(opcode, &payload, limits).is_err());
        }
    }

    #[test]
    fn records_carry_their_database_in_the_flags() {
        let set = OwnedMutation::SetBytes {
            key: b"k".to_vec(),
            value: b"v".to_vec(),
        };

        assert_eq!(
            encode_record(0, 1, 0, &set).unwrap(),
            load_hex_fixture("wal-set-bytes-record.hex"),
            "database 0 records keep the pre-database bytes"
        );

        let mutations = [
            set,
            OwnedMutation::FlushDatabase,
            OwnedMutation::FlushAll,
            OwnedMutation::SwapDatabases {
                first: 0,
                second: 255,
            },
            OwnedMutation::MoveKey {
                key: b"k".to_vec(),
                destination: 9,
            },
            OwnedMutation::Transaction {
                mutations: vec![
                    OwnedMutation::FlushDatabase,
                    OwnedMutation::SwapDatabases {
                        first: 1,
                        second: 2,
                    },
                ],
            },
        ];

        for (index, mutation) in mutations.into_iter().enumerate() {
            let sequence = index as u64 + 1;
            let encoded = encode_record(0, sequence, 7, &mutation).unwrap();
            let decoded = decode_record(&encoded, 0, ProtocolLimits::default()).unwrap();

            assert_eq!(decoded.database, 7);
            assert_eq!(decoded.mutation, mutation);
        }
    }

    #[test]
    fn rejects_unknown_record_flags() {
        let mut record = encode_record(0, 1, 3, &OwnedMutation::FlushDatabase).unwrap();

        record[9] = 1;
        let end = record.len() - 4;
        let checksum = crc32c::crc32c(&record[4..end]);

        record[end..].copy_from_slice(&checksum.to_le_bytes());
        assert!(decode_record(&record, 0, ProtocolLimits::default()).is_err());
    }

    #[test]
    fn record_header_check_agrees_with_the_decoder_on_header_fields() {
        let mutation = OwnedMutation::Persist { key: b"k".to_vec() };
        let record = encode_record(2, 7, 1, &mutation).unwrap();

        assert!(record_header_plausible(&record, 2, 7));
        assert!(!record_header_plausible(&record, 3, 7));
        assert!(!record_header_plausible(&record, 2, 8));
        assert!(!record_header_plausible(&record[..record.len() - 1], 2, 7));
        for (at, flip) in [(4, 1u8), (11, 1), (24, 1)] {
            let mut changed = record.clone();

            changed[at] ^= flip;
            assert!(!record_header_plausible(&changed, 2, 7), "byte {at}");
        }
        // The payload and checksum are left to the decoder.
        let mut payload = record.clone();
        payload[28] ^= 1;
        assert!(record_header_plausible(&payload, 2, 7));
        assert!(decode_record(&payload, 2, ProtocolLimits::default()).is_err());
    }

    #[test]
    fn detects_header_and_record_corruption() {
        let mut header = encode_segment_header(1, 2, 3);

        header[20] ^= 1;
        assert!(matches!(
            decode_segment_header(&header),
            Err(PersistenceError::Corruption(_))
        ));

        let mutation = OwnedMutation::Persist { key: b"k".to_vec() };

        let mut record = encode_record(1, 1, 0, &mutation).unwrap();

        record[28] ^= 1;
        assert!(matches!(
            decode_record(&record, 1, ProtocolLimits::default()),
            Err(PersistenceError::Corruption(_))
        ));
    }

    #[test]
    fn version_one_set_bytes_fixture_remains_stable() {
        assert_v1_record_fixture(
            "wal-set-bytes-record.hex",
            0,
            1,
            &OwnedMutation::SetBytes {
                key: b"k".to_vec(),
                value: b"v".to_vec(),
            },
        );
    }

    #[test]
    fn value_with_deadline_record_round_trips_from_the_borrowed_encoder() {
        let borrowed = Mutation::SetBytesExpiring {
            key: b"lock",
            value: b"token",
            expires_at: 1_700_000_030_000,
        };
        let mut bytes = Vec::new();

        encode_borrowed_record_into(&mut bytes, 0, 9, 2, &borrowed).unwrap();
        assert_eq!(bytes, encode_record(0, 9, 2, &borrowed.to_owned()).unwrap());

        let record = decode_record(&bytes, 0, ProtocolLimits::default()).unwrap();

        assert_eq!(record.sequence, 9);
        assert_eq!(record.database, 2);
        assert_eq!(record.mutation, borrowed.to_owned());
    }

    #[test]
    fn version_one_wal_record_fixtures_cover_every_mutation() {
        for (name, shard, sequence, mutation) in v1_wal_records() {
            assert_v1_record_fixture(name, shard, sequence, &mutation);
            cover_owned_mutation(&mutation);
        }

        assert_eq!(v1_wal_records().len(), 23);
    }

    #[test]
    fn every_record_type_the_decoder_knows_has_a_format_minor() {
        let limits = ProtocolLimits::default();

        for record_type in 0..=u16::from(u8::MAX) {
            let known = !matches!(
                decode_payload(record_type, &[], limits),
                Err(PersistenceError::Unsupported(_))
            );

            assert_eq!(
                known,
                (1..=LAST_RECORD_TYPE).contains(&record_type),
                "record type {record_type}: a new record type needs a RECORD_MINORS row, \
                 LAST_RECORD_TYPE and a FORMAT_MINOR bump"
            );
        }

        assert!(
            RECORD_MINORS
                .windows(2)
                .all(|pair| pair[0].0 < pair[1].0 && pair[1].1 == pair[0].1 + 1)
        );
        assert!(RECORD_MINORS.last().unwrap().0 <= LAST_RECORD_TYPE);
        assert_eq!(RECORD_MINORS.last().unwrap().1, FORMAT_MINOR);

        let future = encode_segment_header_at(1, FORMAT_MINOR + 1, 0, 0, 1, 1, 0);
        assert!(matches!(
            decode_segment_header(&future),
            Err(PersistenceError::Unsupported(_))
        ));
        let current = encode_segment_header_at(1, FORMAT_MINOR, 0, 0, 1, 1, 0);
        assert!(decode_segment_header(&current).is_ok());
    }

    #[test]
    fn version_one_wal_header_fixtures_decode_and_reject_future_major() {
        let header = load_hex_fixture("wal-header.hex");

        assert_eq!(
            encode_segment_header_at(1, 0, 0, 0, 1, 1_700_000_000_000, 0),
            header
        );
        let decoded = decode_segment_header(&header).unwrap();

        assert_eq!(decoded.shard_id, 0);
        assert_eq!(decoded.segment_id, 0);
        assert_eq!(decoded.first_sequence, 1);

        let unsupported = load_hex_fixture("wal-unsupported-major.hex");

        assert_eq!(
            encode_segment_header_at(2, 0, 0, 0, 1, 1_700_000_000_000, 0),
            unsupported
        );
        assert!(matches!(
            decode_segment_header(&unsupported),
            Err(PersistenceError::Unsupported(_))
        ));
    }

    #[test]
    fn version_one_wal_fixtures_apply_without_reencoding() {
        let mut shard = crate::KvShard::new();

        crate::persistence::apply_mutation(
            &mut shard,
            0,
            &decode_record(
                &load_hex_fixture("wal-set-bytes-record.hex"),
                0,
                ProtocolLimits::default(),
            )
            .unwrap()
            .mutation,
        )
        .unwrap();
        assert_eq!(shard.get_bytes(b"k").unwrap(), Some(b"v".as_slice()));

        crate::persistence::apply_mutation(
            &mut shard,
            0,
            &decode_record(
                &load_hex_fixture("wal-transaction.hex"),
                0,
                ProtocolLimits::default(),
            )
            .unwrap()
            .mutation,
        )
        .unwrap();
        assert_eq!(shard.copied_value(b"a").as_deref(), Some(b"1".as_slice()));
        assert_eq!(shard.copied_value(b"i").as_deref(), Some(b"2".as_slice()));
    }

    #[test]
    fn writes_v1_wal_fixtures_when_requested() {
        if std::env::var_os("RUVIO_WRITE_FIXTURES").is_none() {
            return;
        }

        write_hex_fixture(
            "wal-header.hex",
            &encode_segment_header_at(1, 0, 0, 0, 1, 1_700_000_000_000, 0),
        );
        write_hex_fixture(
            "wal-unsupported-major.hex",
            &encode_segment_header_at(2, 0, 0, 0, 1, 1_700_000_000_000, 0),
        );

        for (name, shard, sequence, mutation) in v1_wal_records() {
            write_hex_fixture(name, &encode_record(shard, sequence, 0, &mutation).unwrap());
        }
    }

    fn v1_wal_records() -> Vec<(&'static str, u32, u64, OwnedMutation)> {
        vec![
            (
                "wal-set-bytes-record.hex",
                0,
                1,
                OwnedMutation::SetBytes {
                    key: b"k".to_vec(),
                    value: b"v".to_vec(),
                },
            ),
            (
                "wal-set-integer.hex",
                0,
                1,
                OwnedMutation::SetInteger {
                    key: b"i".to_vec(),
                    value: -42,
                },
            ),
            (
                "wal-delete.hex",
                0,
                1,
                OwnedMutation::Delete {
                    keys: vec![b"gone".to_vec()],
                },
            ),
            (
                "wal-list-push-front.hex",
                0,
                1,
                OwnedMutation::ListPushFront {
                    key: b"l".to_vec(),
                    values: vec![b"a".to_vec()],
                },
            ),
            (
                "wal-list-pop-front.hex",
                0,
                1,
                OwnedMutation::ListPopFront { key: b"l".to_vec() },
            ),
            (
                "wal-list-push-back.hex",
                0,
                1,
                OwnedMutation::ListPushBack {
                    key: b"l".to_vec(),
                    values: vec![b"z".to_vec()],
                },
            ),
            (
                "wal-list-pop-back.hex",
                0,
                1,
                OwnedMutation::ListPopBack { key: b"l".to_vec() },
            ),
            (
                "wal-list-trim.hex",
                0,
                1,
                OwnedMutation::ListTrim {
                    key: b"l".to_vec(),
                    start: 0,
                    stop: 1,
                },
            ),
            (
                "wal-set-add.hex",
                0,
                1,
                OwnedMutation::SetAdd {
                    key: b"s".to_vec(),
                    members: vec![b"a".to_vec()],
                },
            ),
            (
                "wal-set-remove.hex",
                0,
                1,
                OwnedMutation::SetRemove {
                    key: b"s".to_vec(),
                    members: vec![b"a".to_vec()],
                },
            ),
            (
                "wal-set-store.hex",
                0,
                1,
                OwnedMutation::SetStore {
                    key: b"s".to_vec(),
                    members: vec![b"a".to_vec(), b"c".to_vec()],
                },
            ),
            (
                "wal-set-move.hex",
                0,
                1,
                OwnedMutation::SetMove {
                    source: b"from".to_vec(),
                    destination: b"to".to_vec(),
                    member: b"a".to_vec(),
                },
            ),
            (
                "wal-hash-set.hex",
                0,
                1,
                OwnedMutation::HashSet {
                    key: b"h".to_vec(),
                    pairs: vec![(b"f".to_vec(), b"v".to_vec())],
                },
            ),
            (
                "wal-hash-delete.hex",
                0,
                1,
                OwnedMutation::HashDelete {
                    key: b"h".to_vec(),
                    fields: vec![b"f".to_vec()],
                },
            ),
            (
                "wal-zadd-single.hex",
                0,
                1,
                OwnedMutation::SortedSetAdd {
                    key: b"z".to_vec(),
                    pairs: vec![(1.5, b"m".to_vec())],
                },
            ),
            (
                "wal-zadd-multi.hex",
                0,
                1,
                OwnedMutation::SortedSetAdd {
                    key: b"z".to_vec(),
                    pairs: vec![(1.0, b"a".to_vec()), (2.0, b"b".to_vec())],
                },
            ),
            (
                "wal-zrem.hex",
                0,
                1,
                OwnedMutation::SortedSetRemove {
                    key: b"z".to_vec(),
                    members: vec![b"a".to_vec()],
                },
            ),
            (
                "wal-bloom-reserve.hex",
                0,
                1,
                OwnedMutation::BloomReserve {
                    key: b"b".to_vec(),
                    error_rate: 0.01,
                    capacity: 100,
                    expansion: 2,
                },
            ),
            (
                "wal-bloom-add.hex",
                0,
                1,
                OwnedMutation::BloomAdd {
                    key: b"b".to_vec(),
                    value: b"m".to_vec(),
                },
            ),
            (
                "wal-expire-at.hex",
                0,
                1,
                OwnedMutation::ExpireAt {
                    key: b"k".to_vec(),
                    unix_millis: 42,
                },
            ),
            (
                "wal-persist.hex",
                0,
                1,
                OwnedMutation::Persist { key: b"k".to_vec() },
            ),
            (
                "wal-rename.hex",
                0,
                1,
                OwnedMutation::Rename {
                    source: b"old".to_vec(),
                    destination: b"new".to_vec(),
                },
            ),
            (
                "wal-transaction.hex",
                0,
                1,
                OwnedMutation::Transaction {
                    mutations: vec![
                        OwnedMutation::SetBytes {
                            key: b"a".to_vec(),
                            value: b"1".to_vec(),
                        },
                        OwnedMutation::SetInteger {
                            key: b"i".to_vec(),
                            value: 2,
                        },
                    ],
                },
            ),
        ]
    }

    fn cover_owned_mutation(mutation: &OwnedMutation) {
        match mutation {
            OwnedMutation::SetBytes { .. }
            | OwnedMutation::SetBytesExpiring { .. }
            | OwnedMutation::JsonPut { .. }
            | OwnedMutation::JsonSetPath { .. }
            | OwnedMutation::JsonMergePath { .. }
            | OwnedMutation::JsonDelPath { .. }
            | OwnedMutation::SetInteger { .. }
            | OwnedMutation::Delete { .. }
            | OwnedMutation::ListPushFront { .. }
            | OwnedMutation::ListPopFront { .. }
            | OwnedMutation::ListPushBack { .. }
            | OwnedMutation::ListPopBack { .. }
            | OwnedMutation::ListTrim { .. }
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
            | OwnedMutation::LeasePut { .. }
            | OwnedMutation::SetVersioned { .. }
            | OwnedMutation::LimitPut { .. }
            | OwnedMutation::OncePut { .. }
            | OwnedMutation::IdempotencyPut { .. }
            | OwnedMutation::StreamGroupAdvance { .. }
            | OwnedMutation::ExpireAt { .. }
            | OwnedMutation::Persist { .. }
            | OwnedMutation::Rename { .. }
            | OwnedMutation::Transaction { .. } => {}
            OwnedMutation::FlushDatabase
            | OwnedMutation::FlushAll
            | OwnedMutation::SwapDatabases { .. }
            | OwnedMutation::MoveKey { .. }
            | OwnedMutation::SetRange { .. }
            | OwnedMutation::ListSet { .. }
            | OwnedMutation::ListInsert { .. }
            | OwnedMutation::ListRemove { .. } => {
                panic!("these records are newer than the v1 fixtures")
            }
        }
    }

    fn assert_v1_record_fixture(name: &str, shard: u32, sequence: u64, mutation: &OwnedMutation) {
        let expected = load_hex_fixture(name);

        assert_eq!(
            encode_record(shard, sequence, 0, mutation).unwrap(),
            expected,
            "{name} encoding drifted"
        );
        let decoded = decode_record(&expected, shard, ProtocolLimits::default()).unwrap();

        assert_eq!(decoded.sequence, sequence);
        assert_eq!(decoded.mutation, *mutation);
    }

    fn fixture_dir() -> std::path::PathBuf {
        std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/persistence/v1")
    }

    fn load_hex_fixture(name: &str) -> Vec<u8> {
        let text = std::fs::read_to_string(fixture_dir().join(name))
            .unwrap_or_else(|error| panic!("missing v1 fixture {name}: {error}"));

        parse_hex(&text)
    }

    fn write_hex_fixture(name: &str, bytes: &[u8]) {
        let directory = fixture_dir();

        std::fs::create_dir_all(&directory).unwrap();
        std::fs::write(directory.join(name), to_hex(bytes)).unwrap();
    }

    fn parse_hex(text: &str) -> Vec<u8> {
        let compact: String = text.chars().filter(|ch| !ch.is_whitespace()).collect();

        compact
            .as_bytes()
            .as_chunks::<2>()
            .0
            .iter()
            .map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap())
            .collect()
    }

    fn to_hex(bytes: &[u8]) -> String {
        let mut text = String::with_capacity(bytes.len() * 2 + 1);

        for byte in bytes {
            text.push_str(&format!("{byte:02x}"));
        }

        text.push('\n');
        text
    }
}
