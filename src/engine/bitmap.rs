use super::*;
use crate::protocol::BitOperation;

/// Largest bitmap, in bytes. Matches the default `max_value_bytes`.
pub(crate) const MAX_BITMAP_BYTES: usize = 16 * 1024 * 1024;
const OUT_OF_RANGE: &[u8] = b"ERR bit offset is not an integer or out of range";

pub(super) fn current_bytes<C: Clock, I: StorageIndex>(
    shard: &KvShard<C, I>,
    key: &[u8],
) -> Result<Option<Vec<u8>>, ()> {
    match shard.value_view(key) {
        Ok(Some(value)) => Ok(Some(value.to_vec())),
        Ok(None) => Ok(None),
        Err(_) => Err(()),
    }
}

/// Runs `read` over the string under `key` without copying it; an integer is formatted
/// on the stack. `Ok(None)` for a missing key, `Err` for another type.
pub(super) fn with_bytes<C: Clock, I: StorageIndex, R>(
    shard: &KvShard<C, I>,
    key: &[u8],
    read: impl FnOnce(&[u8]) -> R,
) -> Result<Option<R>, ()> {
    match shard.value_view(key) {
        Ok(Some(ByteView::Borrowed(bytes))) => Ok(Some(read(bytes))),
        Ok(Some(ByteView::Integer(value))) => {
            let mut digits = [0u8; 20];
            let len = crate::store::format_integer(value, &mut digits);

            Ok(Some(read(&digits[..len])))
        }
        Ok(None) => Ok(None),
        Err(_) => Err(()),
    }
}

/// Overwrites `patch` at byte `offset` (zero-padding a shorter value), keeping the TTL,
/// and logs only those bytes. The caller has already run `before_mutation`.
pub(super) fn patch_bytes<C: Clock, I: StorageIndex, S: MutationSink>(
    shard: &mut KvShard<C, I>,
    sink: &mut S,
    key: &[u8],
    offset: usize,
    patch: &[u8],
) -> Result<usize, S::Error> {
    sink.record(Mutation::SetRange {
        key,
        offset: offset as u64,
        bytes: patch,
    })?;

    Ok(shard.set_range(key, offset, patch).unwrap_or(0))
}

/// Store `value` under `key`, keeping its TTL, and log the change.
pub(super) fn store_bytes<C: Clock, I: StorageIndex, S: MutationSink>(
    shard: &mut KvShard<C, I>,
    sink: &mut S,
    key: &[u8],
    value: &[u8],
) -> Result<(), S::Error> {
    let deadline = shard.expires_at(key);

    // One record: a log cut between the value and its deadline would drop the TTL.
    match deadline {
        Some(expires_at) => sink.record(Mutation::SetBytesExpiring {
            key,
            value,
            expires_at: expires_at.get(),
        })?,
        None => sink.record(Mutation::SetBytes { key, value })?,
    }

    shard.set_bytes(key, value);

    if let Some(expires_at) = deadline {
        shard.expire_at(key, expires_at);
    }

    Ok(())
}

pub(super) fn set_bit<'a, C: Clock, I: StorageIndex, S: MutationSink>(
    shard: &'a mut KvShard<C, I>,
    sink: &mut S,
    key: crate::protocol::Bytes<'a>,
    offset: u64,
    value: bool,
) -> Result<Response<'a>, S::Error> {
    let byte = (offset / 8) as usize;

    if byte >= MAX_BITMAP_BYTES {
        return Ok(Response::Error(OUT_OF_RANGE));
    }

    // (current byte, string length) without copying the string.
    let Ok(existing) = with_bytes(shard, &key, |bytes| (bytes.get(byte).copied(), bytes.len()))
    else {
        return Ok(wrong_type());
    };

    let created = existing.is_none();
    let (current, length) = existing.unwrap_or((None, 0));
    let current = current.unwrap_or(0);
    let grow = (byte + 1).saturating_sub(length);
    let mask = 0x80u8 >> (offset % 8);
    let previous = current & mask != 0;

    if previous == value && grow == 0 {
        return Ok(Response::Integer(i64::from(previous)));
    }

    let extra = grow + if created { key.len() } else { 0 } + ADMISSION_OVERHEAD_BYTES;

    if let Some(rejected) = before_mutation(shard, sink, [key.as_ref()], extra)? {
        return Ok(rejected);
    }

    let updated = if value {
        current | mask
    } else {
        current & !mask
    };

    patch_bytes(shard, sink, &key, byte, &[updated])?;
    Ok(Response::Integer(i64::from(previous)))
}

pub(super) fn get_bit<'a, C: Clock, I: StorageIndex>(
    shard: &'a KvShard<C, I>,
    key: &[u8],
    offset: u64,
) -> Response<'a> {
    let byte = (offset / 8) as usize;

    match with_bytes(shard, key, |bytes| {
        bytes
            .get(byte)
            .is_some_and(|&b| b & (0x80u8 >> (offset % 8)) != 0)
    }) {
        Ok(Some(set)) => Response::Integer(i64::from(set)),
        Ok(None) => Response::Integer(0),
        Err(()) => wrong_type(),
    }
}

/// Resolve a Redis `start end` pair (negative = from the end) against `length` units.
/// `None` when the range is empty.
pub(super) fn resolve_range(start: i64, end: i64, length: i64) -> Option<(usize, usize)> {
    let start = if start < 0 {
        (length + start).max(0)
    } else {
        start
    };
    let end = if end < 0 {
        length + end
    } else {
        end.min(length - 1)
    };

    (length > 0 && start <= end && start < length).then_some((start as usize, end as usize))
}

pub(super) fn bit_count<'a, C: Clock, I: StorageIndex>(
    shard: &'a KvShard<C, I>,
    key: &[u8],
    range: Option<(i64, i64, bool)>,
) -> Response<'a> {
    match with_bytes(shard, key, |bytes| count_bits(bytes, range)) {
        Ok(Some(response)) => response,
        Ok(None) => Response::Integer(0),
        Err(()) => wrong_type(),
    }
}

/// Set bits in `bytes`, eight bytes per `count_ones`.
fn popcount(bytes: &[u8]) -> u64 {
    let (chunks, tail) = bytes.as_chunks::<8>();
    let mut count: u64 = chunks
        .iter()
        .map(|chunk| u64::from(u64::from_ne_bytes(*chunk).count_ones()))
        .sum();

    count += tail
        .iter()
        .map(|byte| u64::from(byte.count_ones()))
        .sum::<u64>();
    count
}

fn count_bits(bytes: &[u8], range: Option<(i64, i64, bool)>) -> Response<'static> {
    let (start, end, bits) = range.unwrap_or((0, -1, false));
    let unit_length = if bits {
        bytes.len() as i64 * 8
    } else {
        bytes.len() as i64
    };

    let Some((start, end)) = resolve_range(start, end, unit_length) else {
        return Response::Integer(0);
    };

    if !bits {
        return Response::Integer(popcount(&bytes[start..=end]) as i64);
    }

    let count = (start..=end)
        .filter(|&bit| bytes[bit / 8] & (0x80u8 >> (bit % 8)) != 0)
        .count();

    Response::Integer(count as i64)
}

pub(super) fn bit_pos<'a, C: Clock, I: StorageIndex>(
    shard: &'a KvShard<C, I>,
    key: &[u8],
    bit: bool,
    range: Option<(i64, Option<i64>, bool)>,
) -> Response<'a> {
    match with_bytes(shard, key, |bytes| find_bit(bytes, bit, range)) {
        Ok(Some(response)) => response,
        Ok(None) => Response::Integer(if bit { -1 } else { 0 }),
        Err(()) => wrong_type(),
    }
}

fn find_bit(bytes: &[u8], bit: bool, range: Option<(i64, Option<i64>, bool)>) -> Response<'static> {
    let (start, end, bits) = range.unwrap_or((0, None, false));
    let end_given = end.is_some();
    let unit_length = if bits {
        bytes.len() as i64 * 8
    } else {
        bytes.len() as i64
    };

    let Some((start, end)) = resolve_range(start, end.unwrap_or(-1), unit_length) else {
        return Response::Integer(-1);
    };

    let (first_bit, last_bit) = if bits {
        (start, end)
    } else {
        (start * 8, end * 8 + 7)
    };

    let found = (first_bit..=last_bit)
        .find(|&position| (bytes[position / 8] & (0x80u8 >> (position % 8)) != 0) == bit);

    match found {
        Some(position) => Response::Integer(position as i64),
        // Looking for a clear bit past an all-ones string with no explicit end: the string
        // is treated as padded with zeros on the right.
        None if !bit && !end_given => Response::Integer((last_bit + 1) as i64),
        None => Response::Integer(-1),
    }
}

/// `BITOP` over zero-padded sources, one slice pass per source so the byte loops vectorize.
fn combine_bitmaps(operation: BitOperation, sources: &[Vec<u8>], length: usize) -> Vec<u8> {
    let mut result = vec![0u8; length];
    let Some((first, rest)) = sources.split_first() else {
        return result;
    };

    result[..first.len()].copy_from_slice(first);

    match operation {
        BitOperation::Not => {
            for byte in &mut result[..first.len()] {
                *byte = !*byte;
            }
            result[first.len()..].fill(0xFF);
        }
        BitOperation::And => {
            for source in rest {
                for (slot, byte) in result.iter_mut().zip(source) {
                    *slot &= byte;
                }
                // Past a shorter source the missing bytes are zero.
                if source.len() < length {
                    result[source.len()..].fill(0);
                }
            }
        }
        BitOperation::Or => {
            for source in rest {
                for (slot, byte) in result.iter_mut().zip(source) {
                    *slot |= byte;
                }
            }
        }
        BitOperation::Xor => {
            for source in rest {
                for (slot, byte) in result.iter_mut().zip(source) {
                    *slot ^= byte;
                }
            }
        }
    }

    result
}

pub(super) fn bit_op<'a, C: Clock, I: StorageIndex, S: MutationSink>(
    shard: &'a mut KvShard<C, I>,
    sink: &mut S,
    operation: BitOperation,
    destination: crate::protocol::Bytes<'a>,
    keys: Vec<crate::protocol::Bytes<'a>>,
) -> Result<Response<'a>, S::Error> {
    if operation == BitOperation::Not && keys.len() != 1 {
        return Ok(Response::Error(
            b"ERR BITOP NOT must be called with a single source key.",
        ));
    }

    let mut sources = Vec::with_capacity(keys.len());

    for key in &keys {
        match current_bytes(shard, key) {
            Ok(bytes) => sources.push(bytes.unwrap_or_default()),
            Err(()) => return Ok(wrong_type()),
        }
    }

    let length = sources.iter().map(Vec::len).max().unwrap_or(0);

    if length > MAX_BITMAP_BYTES {
        return Ok(Response::Error(OUT_OF_RANGE));
    }

    let result = combine_bitmaps(operation, &sources, length);

    let mut touched: Vec<&[u8]> = vec![destination.as_ref()];
    touched.extend(keys.iter().map(AsRef::as_ref));

    if let Some(rejected) = before_mutation(
        shard,
        sink,
        touched,
        length + destination.len() + ADMISSION_OVERHEAD_BYTES,
    )? {
        return Ok(rejected);
    }

    if result.is_empty() {
        if shard.contains_key(&destination) {
            sink.record(Mutation::Delete {
                keys: std::slice::from_ref(&destination),
            })?;
            shard.delete(&destination);
        }
    } else {
        // A fresh destination loses any old TTL, as with `SET`.
        sink.record(Mutation::SetBytes {
            key: &destination,
            value: &result,
        })?;
        shard.set_bytes(&destination, &result);
    }

    Ok(Response::Integer(length as i64))
}

#[cfg(test)]
mod bitop_tests {
    use super::*;

    fn naive(operation: BitOperation, sources: &[Vec<u8>], length: usize) -> Vec<u8> {
        (0..length)
            .map(|index| {
                let mut bytes = sources
                    .iter()
                    .map(|source| source.get(index).copied().unwrap_or(0));
                let first = bytes.next().unwrap_or(0);
                match operation {
                    BitOperation::And => bytes.fold(first, |acc, b| acc & b),
                    BitOperation::Or => bytes.fold(first, |acc, b| acc | b),
                    BitOperation::Xor => bytes.fold(first, |acc, b| acc ^ b),
                    BitOperation::Not => !first,
                }
            })
            .collect()
    }

    #[test]
    fn slice_passes_match_the_per_byte_definition() {
        let mut seed = 0x2545_f491_4f6c_dd1du64;
        let mut next = move || {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            seed
        };
        for _ in 0..300 {
            let count = 1 + (next() % 4) as usize;
            let sources: Vec<Vec<u8>> = (0..count)
                .map(|_| (0..(next() % 40) as usize).map(|_| next() as u8).collect())
                .collect();
            let length = sources.iter().map(Vec::len).max().unwrap_or(0);
            for operation in [BitOperation::And, BitOperation::Or, BitOperation::Xor] {
                assert_eq!(
                    combine_bitmaps(operation, &sources, length),
                    naive(operation, &sources, length)
                );
            }
            let single = &sources[..1];
            assert_eq!(
                combine_bitmaps(BitOperation::Not, single, single[0].len()),
                naive(BitOperation::Not, single, single[0].len())
            );
        }
    }
}
