//! `BITFIELD` / `BITFIELD_RO`: typed integer fields inside a string.

use super::*;
use crate::protocol::{BitFieldOp, BitFieldOverflow, BitFieldType};

fn field_mask(bits: u8) -> u128 {
    if bits == 64 {
        u128::from(u64::MAX)
    } else {
        (1u128 << bits) - 1
    }
}

/// Raw bits of a field, most significant first. Bytes past the end read as zero.
fn read_raw(bytes: &[u8], offset: u64, bits: u8) -> u64 {
    // At most nine bytes hold a field of up to 64 bits at any bit shift.
    let first = (offset / 8) as usize;
    let shift = (offset % 8) as u32;
    let span = (shift + u32::from(bits)).div_ceil(8) as usize;
    let mut window = 0u128;

    for index in 0..span {
        window = (window << 8) | u128::from(bytes.get(first + index).copied().unwrap_or(0));
    }

    ((window >> (span as u32 * 8 - shift - u32::from(bits))) & field_mask(bits)) as u64
}

/// Stores the low `bits` of `value` in the field: the bytes it touches are read as one
/// window, the field is replaced, and only those bytes are written back.
fn write_raw(bytes: &mut [u8], offset: u64, bits: u8, value: u64) {
    let first = (offset / 8) as usize;
    let shift = (offset % 8) as u32;
    let span = (shift + u32::from(bits)).div_ceil(8) as usize;
    let mut window = 0u128;

    for index in 0..span {
        window = (window << 8) | u128::from(bytes[first + index]);
    }

    let below = span as u32 * 8 - shift - u32::from(bits);
    let mask = field_mask(bits);

    window = (window & !(mask << below)) | ((u128::from(value) & mask) << below);

    for index in (0..span).rev() {
        bytes[first + index] = window as u8;
        window >>= 8;
    }
}

fn limits(ty: BitFieldType) -> (i128, i128) {
    if ty.signed {
        (-(1i128 << (ty.bits - 1)), (1i128 << (ty.bits - 1)) - 1)
    } else {
        (0, (1i128 << ty.bits) - 1)
    }
}

/// The field's value as the type reads it back: sign-extended for `i`, plain for `u`.
fn decode(raw: u64, ty: BitFieldType) -> i64 {
    if ty.signed && ty.bits < 64 && raw & (1 << (ty.bits - 1)) != 0 {
        (raw | (u64::MAX << ty.bits)) as i64
    } else {
        raw as i64
    }
}

/// Fits `value` to the type: the value to store, or `None` when `FAIL` refuses it.
fn fit(value: i128, ty: BitFieldType, overflow: BitFieldOverflow) -> Option<i128> {
    let (min, max) = limits(ty);

    if (min..=max).contains(&value) {
        return Some(value);
    }

    match overflow {
        BitFieldOverflow::Fail => None,
        BitFieldOverflow::Sat => Some(value.clamp(min, max)),
        BitFieldOverflow::Wrap => {
            let modulus = 1i128 << ty.bits;
            let wrapped = value.rem_euclid(modulus);

            Some(if ty.signed && wrapped > max {
                wrapped - modulus
            } else {
                wrapped
            })
        }
    }
}

pub(super) fn bit_field<'a, C: Clock, I: StorageIndex, S: MutationSink>(
    shard: &mut KvShard<C, I>,
    sink: &mut S,
    key: crate::protocol::Bytes<'a>,
    ops: &[BitFieldOp],
) -> Result<Response<'a>, S::Error> {
    // Only the bytes the fields touch are read and written: [low, high) in bytes. A write
    // extends the string to cover its field (`reach`), even when the value does not change.
    let (mut low, mut high, mut reach) = (u64::MAX, 0u64, None::<u64>);

    for op in ops {
        let (ty, offset, writes) = match *op {
            BitFieldOp::Get { ty, offset } => (ty, offset, false),
            BitFieldOp::Set { ty, offset, .. } | BitFieldOp::IncrBy { ty, offset, .. } => {
                (ty, offset, true)
            }
        };
        let end = (offset + u64::from(ty.bits)).div_ceil(8);

        low = low.min(offset / 8);
        high = high.max(end);

        if writes {
            reach = Some(reach.map_or(end, |reach| reach.max(end)));
        }
    }

    let (low, high) = (low.min(high) as usize, high as usize);
    let mut inline = [0u8; 16];
    let mut spilled = Vec::new();
    let window: &mut [u8] = if high - low <= inline.len() {
        &mut inline[..high - low]
    } else {
        spilled.resize(high - low, 0);
        &mut spilled
    };

    // A plain string is found once; the write at the end goes to the same address.
    let loc = if shard.addresses_survive_admission() {
        shard.log_string_loc(&key)
    } else {
        None
    };
    let copy_window = |bytes: &[u8], window: &mut [u8]| {
        let copied = bytes.len().min(high);

        if copied > low {
            window[..copied - low].copy_from_slice(&bytes[low..copied]);
        }

        bytes.len()
    };
    let length = match loc {
        Some((at, len)) => Ok(Some(copy_window(shard.log_bytes(at, len), window))),
        None => with_bytes(shard, &key, |bytes| copy_window(bytes, window)),
    };
    let Ok(length) = length else {
        return Ok(wrong_type());
    };

    let created = length.is_none();
    let length = length.unwrap_or(0);
    let base = low as u64 * 8;
    let new_length = reach.map_or(length, |reach| length.max(reach as usize));
    let mut changed = false;
    let mut replies = Vec::with_capacity(if ops.len() == 1 { 0 } else { ops.len() });
    let mut last = None;

    for op in ops {
        let reply = match *op {
            BitFieldOp::Get { ty, offset } => {
                Response::Integer(decode(read_raw(window, offset - base, ty.bits), ty))
            }
            BitFieldOp::Set {
                ty,
                offset,
                value,
                overflow,
            } => {
                let previous = read_raw(window, offset - base, ty.bits);
                // Redis checks an unsigned field against the value's raw 64 bits.
                let wanted = if ty.signed {
                    i128::from(value)
                } else {
                    i128::from(value as u64)
                };

                match fit(wanted, ty, overflow) {
                    Some(stored) => {
                        write_raw(window, offset - base, ty.bits, stored as u64);
                        changed |= read_raw(window, offset - base, ty.bits) != previous;
                        Response::Integer(decode(previous, ty))
                    }
                    None => Response::Bulk(None),
                }
            }
            BitFieldOp::IncrBy {
                ty,
                offset,
                delta,
                overflow,
            } => {
                let previous = read_raw(window, offset - base, ty.bits);
                let current = i128::from(decode(previous, ty));

                match fit(current + i128::from(delta), ty, overflow) {
                    Some(stored) => {
                        write_raw(window, offset - base, ty.bits, stored as u64);
                        let now = read_raw(window, offset - base, ty.bits);

                        changed |= now != previous;
                        Response::Integer(decode(now, ty))
                    }
                    None => Response::Bulk(None),
                }
            }
        };

        if ops.len() == 1 {
            last = Some(match reply {
                Response::Integer(value) => Some(value),
                _ => None,
            });
        } else {
            replies.push(reply);
        }
    }

    if changed || new_length > length {
        let grow = new_length - length;
        let extra = grow + if created { key.len() } else { 0 } + ADMISSION_OVERHEAD_BYTES;

        if let Some(rejected) = before_mutation(shard, sink, [key.as_ref()], extra)? {
            return Ok(rejected);
        }

        // Window bytes past the new end were only read (GET past the end), never written.
        let end = high.min(new_length).max(low);

        match loc {
            Some((at, len)) if end <= len => {
                sink.record(Mutation::SetRange {
                    key: &key,
                    offset: low as u64,
                    bytes: &window[..end - low],
                })?;
                shard.write_log_range(&key, at, low, &window[..end - low]);
            }
            _ => {
                patch_bytes(shard, sink, &key, low, &window[..end - low])?;
            }
        }
    }

    Ok(match last {
        Some(only) => Response::OneInteger(only),
        None => Response::Exec(replies),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bit_at(bytes: &[u8], position: u64) -> u64 {
        bytes
            .get((position / 8) as usize)
            .map_or(0, |byte| u64::from(byte >> (7 - position % 8) & 1))
    }

    /// Bit-by-bit reference for `read_raw` and `write_raw`.
    #[test]
    fn word_access_matches_bit_by_bit() {
        let mut seed = 0x9E37_79B9_7F4A_7C15u64;
        let mut next = move || {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            seed
        };

        for _ in 0..20_000 {
            let bits = (next() % 64 + 1) as u8;
            let offset = next() % 40;
            let mut fast: Vec<u8> = (0..16).map(|_| next() as u8).collect();
            let mut slow = fast.clone();
            let value = next();

            let expected = (0..u64::from(bits)).fold(0u64, |raw, index| {
                (raw << 1) | bit_at(&fast, offset + index)
            });

            assert_eq!(
                read_raw(&fast, offset, bits),
                expected,
                "read {bits}@{offset}"
            );

            write_raw(&mut fast, offset, bits, value);

            for index in 0..u64::from(bits) {
                let position = offset + index;
                let mask = 0x80u8 >> (position % 8);
                let byte = &mut slow[(position / 8) as usize];

                if value >> (u64::from(bits) - 1 - index) & 1 == 1 {
                    *byte |= mask;
                } else {
                    *byte &= !mask;
                }
            }

            assert_eq!(fast, slow, "write {bits}@{offset}");
        }
    }
}
