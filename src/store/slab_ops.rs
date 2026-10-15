//! In-place edits of small lists and hashes inside their index slab slot.
//!
//! A slot is `[subkind, item count, used length, packed bytes...]` (see `index::coll_parts`).
//! These helpers change the packed bytes without building a `ListValue`/`HashValue`. Each
//! returns `None` when the edit does not fit the slot or its limits, and then touches nothing,
//! so the caller falls back to the general path.

use super::hash::{PACKED_LONG_LEN, find_packed_pair, packed_len_size, read_packed};
use super::list::{list_item_size, list_item_start, next_list_item};
use crate::index::{COLL_HASH, COLL_LIST};

const HEAD: usize = 3;

fn put_len(buf: &mut [u8], at: usize, len: usize) -> usize {
    if len < PACKED_LONG_LEN as usize {
        buf[at] = len as u8;
        at + 1
    } else {
        buf[at] = PACKED_LONG_LEN;
        buf[at + 1..at + 5].copy_from_slice(&(len as u32).to_le_bytes());
        at + 5
    }
}

fn put_packed_at(buf: &mut [u8], at: usize, item: &[u8]) -> usize {
    let at = put_len(buf, at, item.len());

    buf[at..at + item.len()].copy_from_slice(item);
    at + item.len()
}

fn put_list_item_at(buf: &mut [u8], at: usize, item: &[u8]) -> usize {
    let at = put_packed_at(buf, at, item);

    if item.len() < PACKED_LONG_LEN as usize {
        buf[at] = item.len() as u8;
        at + 1
    } else {
        buf[at..at + 4].copy_from_slice(&(item.len() as u32).to_le_bytes());
        buf[at + 4] = PACKED_LONG_LEN;
        at + 5
    }
}

/// `HSET field value`. `Some(true)` when the field is new, `Some(false)` when replaced.
pub(super) fn hash_set(
    slot: &mut [u8],
    field: &[u8],
    value: &[u8],
    max_entries: usize,
    max_value_bytes: usize,
) -> Option<bool> {
    if slot[0] != COLL_HASH || field.len() > max_value_bytes || value.len() > max_value_bytes {
        return None;
    }

    let room = slot.len() - HEAD;
    let (head, data) = slot.split_at_mut(HEAD);
    let used = usize::from(head[2]);

    if let Some(pair) = find_packed_pair(&data[..used], field) {
        let (old, _) = read_packed(&data[..used], pair.value_at);

        if old.len() == value.len() {
            let start = pair.end - old.len();

            data[start..pair.end].copy_from_slice(value);

            return Some(false);
        }

        let encoded = packed_len_size(value.len()) + value.len();
        let new_used = used - (pair.end - pair.value_at) + encoded;

        if new_used > room {
            return None;
        }

        data.copy_within(pair.end..used, pair.value_at + encoded);
        put_packed_at(data, pair.value_at, value);
        head[2] = new_used as u8;

        return Some(false);
    }

    let size =
        packed_len_size(field.len()) + field.len() + packed_len_size(value.len()) + value.len();

    if usize::from(head[1]) >= max_entries || used + size > room {
        return None;
    }

    let at = put_packed_at(data, used, field);

    put_packed_at(data, at, value);
    head[1] += 1;
    head[2] = (used + size) as u8;

    Some(true)
}

/// `HDEL field`. `Some(true)` when removed, `Some(false)` when the field is absent. `None` when it
/// is the last field (the caller deletes the key) or the record is not a hash.
pub(super) fn hash_del(slot: &mut [u8], field: &[u8]) -> Option<bool> {
    if slot[0] != COLL_HASH {
        return None;
    }

    let (head, data) = slot.split_at_mut(HEAD);
    let used = usize::from(head[2]);
    let Some(pair) = find_packed_pair(&data[..used], field) else {
        return Some(false);
    };

    if head[1] <= 1 {
        return None;
    }

    data.copy_within(pair.end..used, pair.start);
    head[1] -= 1;
    head[2] = (used - (pair.end - pair.start)) as u8;

    Some(true)
}

/// `LPUSH`/`RPUSH` of one value.
pub(super) fn list_push(
    slot: &mut [u8],
    value: &[u8],
    front: bool,
    max_entries: usize,
    max_item_bytes: usize,
) -> Option<()> {
    if slot[0] != COLL_LIST || value.len() > max_item_bytes || usize::from(slot[1]) >= max_entries {
        return None;
    }

    let used = usize::from(slot[2]);
    let size = list_item_size(value.len());

    if used + size > slot.len() - HEAD {
        return None;
    }

    let data = &mut slot[HEAD..];

    if front {
        data.copy_within(0..used, size);
        put_list_item_at(data, 0, value);
    } else {
        put_list_item_at(data, used, value);
    }

    slot[1] += 1;
    slot[2] = (used + size) as u8;

    Some(())
}

/// `LPOP`/`RPOP` of one item. `None` when the list would become empty (the caller deletes the
/// key) or is not a list.
pub(super) fn list_pop(slot: &mut [u8], front: bool) -> Option<Box<[u8]>> {
    if slot[0] != COLL_LIST || slot[1] <= 1 {
        return None;
    }

    let used = usize::from(slot[2]);
    let data = &mut slot[HEAD..];
    let (item, start, end) = if front {
        let (item, end) = next_list_item(&data[..used], 0);

        (Box::<[u8]>::from(item), 0, end)
    } else {
        let start = list_item_start(&data[..used], used);
        let (item, _) = next_list_item(&data[..used], start);

        (Box::<[u8]>::from(item), start, used)
    };

    data.copy_within(end..used, start);
    slot[1] -= 1;
    slot[2] = (used - (end - start)) as u8;

    Some(item)
}
