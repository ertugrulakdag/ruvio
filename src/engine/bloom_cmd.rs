use super::*;
use crate::protocol::{BloomInfoField, Bytes};
use crate::store::BloomAdd;

const FILTER_FULL: &[u8] = b"ERR non scaling filter is full";

pub(super) fn bloom_reserve_key<'a, C: Clock, I: StorageIndex, S: MutationSink>(
    shard: &'a mut KvShard<C, I>,
    sink: &mut S,
    key: Bytes<'a>,
    error_rate: f64,
    capacity: usize,
    expansion: u32,
) -> Result<Response<'a>, S::Error> {
    if shard.contains_key(&key) {
        return Ok(bloom_error(BloomError::KeyExists));
    }

    let Some(filter_bytes) = KvShard::<C, I>::bloom_config_bytes(error_rate, capacity) else {
        return Ok(bloom_error(BloomError::InvalidConfiguration));
    };

    if let Some(rejected) = before_mutation(
        shard,
        sink,
        [key.as_ref()],
        key.len() + filter_bytes + ADMISSION_OVERHEAD_BYTES,
    )? {
        return Ok(rejected);
    }

    sink.record(Mutation::BloomReserve {
        key: &key,
        error_rate,
        capacity,
        expansion,
    })?;

    match shard.bloom_reserve(&key, error_rate, capacity, expansion) {
        Ok(()) => Ok(Response::Simple(b"OK")),
        Err(error) => {
            debug_assert!(false, "Bloom reserve failed after validation");
            Ok(bloom_error(error))
        }
    }
}

fn add_reply(outcome: BloomAdd) -> Response<'static> {
    match outcome {
        BloomAdd::Added => Response::Integer(1),
        BloomAdd::Exists => Response::Integer(0),
        BloomAdd::Full => Response::Error(FILTER_FULL),
    }
}

pub(super) fn bloom_add_key<'a, C: Clock, I: StorageIndex, S: MutationSink>(
    shard: &'a mut KvShard<C, I>,
    sink: &mut S,
    key: Bytes<'a>,
    value: Bytes<'a>,
) -> Result<Response<'a>, S::Error> {
    let growth = match shard.bloom_growth_bytes(&key, 1) {
        Ok(growth) => growth,
        Err(error) => return Ok(bloom_error(error)),
    };

    if let Some(rejected) = before_mutation(
        shard,
        sink,
        [key.as_ref()],
        key.len() + value.len() + ADMISSION_OVERHEAD_BYTES + growth,
    )? {
        return Ok(rejected);
    }

    sink.record(Mutation::BloomAdd {
        key: &key,
        value: &value,
    })?;

    match shard.bloom_add(&key, &value) {
        Ok(outcome) => Ok(add_reply(outcome)),
        Err(error) => {
            debug_assert!(false, "Bloom add failed after validation");
            Ok(bloom_error(error))
        }
    }
}

/// The write shared by `BF.MADD` and `BF.INSERT`: one WAL record for all items, one lookup.
fn add_items<'a, C: Clock, I: StorageIndex, S: MutationSink>(
    shard: &mut KvShard<C, I>,
    sink: &mut S,
    key: &Bytes<'a>,
    items: &[Bytes<'a>],
) -> Result<Response<'a>, S::Error> {
    sink.record(Mutation::BloomAddMany { key, values: items })?;

    let mut replies = Vec::with_capacity(items.len());

    Ok(
        match shard.bloom_add_each(key, items, |outcome| replies.push(add_reply(outcome))) {
            Ok(()) => Response::Exec(replies),
            Err(error) => {
                debug_assert!(false, "Bloom add failed after validation");
                bloom_error(error)
            }
        },
    )
}

pub(super) fn bloom_add_many_key<'a, C: Clock, I: StorageIndex, S: MutationSink>(
    shard: &'a mut KvShard<C, I>,
    sink: &mut S,
    key: Bytes<'a>,
    items: Vec<Bytes<'a>>,
) -> Result<Response<'a>, S::Error> {
    let growth = match shard.bloom_growth_bytes(&key, items.len()) {
        Ok(growth) => growth,
        Err(error) => return Ok(bloom_error(error)),
    };
    let extra = key.len()
        + items.iter().map(|item| item.len()).sum::<usize>()
        + ADMISSION_OVERHEAD_BYTES
        + growth;

    if let Some(rejected) = before_mutation(shard, sink, [key.as_ref()], extra)? {
        return Ok(rejected);
    }

    add_items(shard, sink, &key, &items)
}

#[allow(clippy::too_many_arguments)]
pub(super) fn bloom_insert_key<'a, C: Clock, I: StorageIndex, S: MutationSink>(
    shard: &'a mut KvShard<C, I>,
    sink: &mut S,
    key: Bytes<'a>,
    error_rate: f64,
    capacity: usize,
    expansion: u32,
    no_create: bool,
    items: Vec<Bytes<'a>>,
) -> Result<Response<'a>, S::Error> {
    let growth = match shard.bloom_growth_bytes(&key, items.len()) {
        Ok(growth) => growth,
        Err(error) => return Ok(bloom_error(error)),
    };
    let create = !shard.contains_key(&key);
    let mut extra = key.len()
        + items.iter().map(|item| item.len()).sum::<usize>()
        + ADMISSION_OVERHEAD_BYTES
        + growth;

    if create {
        if no_create {
            return Ok(Response::Error(b"ERR not found"));
        }

        let Some(filter_bytes) = KvShard::<C, I>::bloom_config_bytes(error_rate, capacity) else {
            return Ok(bloom_error(BloomError::InvalidConfiguration));
        };

        extra += filter_bytes;
    }

    if let Some(rejected) = before_mutation(shard, sink, [key.as_ref()], extra)? {
        return Ok(rejected);
    }

    if create {
        sink.record(Mutation::BloomReserve {
            key: &key,
            error_rate,
            capacity,
            expansion,
        })?;

        if shard
            .bloom_reserve(&key, error_rate, capacity, expansion)
            .is_err()
        {
            debug_assert!(false, "Bloom reserve failed after validation");
            return Ok(bloom_error(BloomError::InvalidConfiguration));
        }
    }

    add_items(shard, sink, &key, &items)
}

pub(super) fn bloom_info<'a, C: Clock, I: StorageIndex>(
    shard: &KvShard<C, I>,
    key: &[u8],
    field: Option<BloomInfoField>,
) -> Response<'a> {
    let info = match shard.bloom_info(key) {
        Ok(Some(info)) => info,
        Ok(None) => return Response::Error(b"ERR not found"),
        Err(error) => return bloom_error(error),
    };
    let expansion = || info.expansion.map(i64::from);
    let integer = |value: i64| Response::Integer(value);

    match field {
        Some(BloomInfoField::Capacity) => integer(info.capacity as i64),
        Some(BloomInfoField::Size) => integer(info.size_bytes as i64),
        Some(BloomInfoField::Filters) => integer(info.filters as i64),
        Some(BloomInfoField::Items) => integer(info.items as i64),
        Some(BloomInfoField::Expansion) => expansion().map_or(Response::Bulk(None), integer),
        None => {
            let name = |text: &'static [u8]| Response::Bulk(Some(Cow::Borrowed(text)));

            Response::Exec(vec![
                name(b"Capacity"),
                integer(info.capacity as i64),
                name(b"Size"),
                integer(info.size_bytes as i64),
                name(b"Number of filters"),
                integer(info.filters as i64),
                name(b"Number of items inserted"),
                integer(info.items as i64),
                name(b"Expansion rate"),
                expansion().map_or(Response::Bulk(None), integer),
            ])
        }
    }
}
