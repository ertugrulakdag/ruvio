//! JSON write and read helpers the dispatcher calls: `JSON.SET`, `JSON.ARRAPPEND`, path reads.

use super::*;

/// The compact JSON bytes selected by `path`, or `None` when the key or path is absent.
/// `JSON.ARRAPPEND`: each item is written as `path[length]`, a small path record, so the
/// WAL and replicas see the item and not the whole array.
pub(super) fn arr_append<C: Clock, I: StorageIndex, S: MutationSink, W: ListWaiterSink>(
    shard: &mut KvShard<C, I>,
    sink: &mut S,
    waiters: &mut W,
    database: u8,
    key: crate::protocol::Bytes<'_>,
    path: crate::protocol::Bytes<'_>,
    args: Vec<crate::protocol::Bytes<'_>>,
) -> Result<Response<'static>, S::Error> {
    use crate::json::ArrayLen;

    let length = match shard.json_tree(&key) {
        Some(tree) => tree.array_len(&path),
        None => match shard.json_view(&key) {
            Ok(Some(document)) => crate::json::array_len(document, &path),
            Ok(None) if crate::json::validate_path(&path).is_err() => {
                return Ok(Response::Error(b"ERR invalid JSON path"));
            }
            Ok(None) => return Ok(Response::Bulk(None)),
            Err(_) => return Ok(wrong_type()),
        },
    };
    let length = match length {
        Ok(ArrayLen::Len(length)) => length,
        Ok(ArrayLen::Missing) => return Ok(Response::Bulk(None)),
        Ok(ArrayLen::NotArray) => {
            return Ok(Response::Error(b"WRONGTYPE JSON value is not an array"));
        }
        Err(_) => return Ok(Response::Error(b"ERR invalid JSON path")),
    };
    if args.is_empty() {
        return Ok(Response::Error(b"WRONGTYPE JSON value is not an array"));
    }
    let mut items = Vec::with_capacity(args.len());
    for arg in &args {
        match crate::json::set_root(arg) {
            Ok(canonical) => items.push(canonical),
            Err(_) => return Ok(Response::Error(b"ERR invalid JSON value")),
        }
    }
    let path_text = String::from_utf8_lossy(&path).into_owned();
    for (offset, item) in items.into_iter().enumerate() {
        let written = execute_in_database(
            &mut *shard,
            Command::JsonSet {
                key: Cow::Owned(key.to_vec()),
                path: Cow::Owned(format!("{path_text}[{}]", length + offset).into_bytes()),
                value: Cow::Owned(item),
            },
            sink,
            waiters,
            database,
        )?;
        match written {
            Response::Simple(_) => {}
            Response::Error(message) => return Ok(Response::Error(message)),
            _ => return Ok(Response::Bulk(None)),
        }
    }
    Ok(Response::Integer((length + args.len()) as i64))
}

/// `JSON.SET`. `trusted` marks a replacement the engine built itself from stored (canonical)
/// bytes, so it is spliced in without being parsed and encoded again.
pub(super) fn json_set_at<C: Clock, I: StorageIndex, S: MutationSink>(
    shard: &mut KvShard<C, I>,
    sink: &mut S,
    key: crate::protocol::Bytes<'_>,
    path: crate::protocol::Bytes<'_>,
    value: crate::protocol::Bytes<'_>,
    trusted: bool,
    on_oom: OnOom,
) -> Result<Response<'static>, S::Error> {
    if path.as_ref() != b"$"
        && let Some(tree) = shard.json_tree(&key)
    {
        let edit = match tree.prepare(&path, Some(&value), false) {
            Ok(Some(edit)) => edit,
            Ok(None) | Err(crate::json::JsonError::MissingRoot) => {
                return Ok(Response::Bulk(None));
            }
            Err(crate::json::JsonError::Syntax) => {
                return Ok(Response::Error(b"ERR invalid JSON value"));
            }
            Err(crate::json::JsonError::Path) => {
                return Ok(Response::Error(b"ERR invalid JSON path"));
            }
        };
        if edit.len() > crate::protocol::DEFAULT_MAX_VALUE_BYTES {
            return Ok(Response::Error(
                b"ERR JSON document exceeds maximum value size",
            ));
        }
        let extra = edit.charge().saturating_sub(tree.charge());
        if let Some(rejected) = admit(shard, sink, [key.as_ref()], extra, on_oom)? {
            return Ok(rejected);
        }
        sink.record(Mutation::JsonSetPath {
            key: &key,
            path: &path,
            value: &value,
        })?;
        shard.apply_json_tree_edit(&key, edit);
        return Ok(Response::Simple(b"OK"));
    }
    let tree_len = shard.json_tree(&key).map(|tree| tree.len());
    let current = match shard.json_view(&key) {
        Err(_) if tree_len.is_some() => None,
        Ok(current) => current,
        Err(_) => return Ok(wrong_type()),
    };
    let previous_len = tree_len.unwrap_or_else(|| current.map_or(0, <[u8]>::len));
    let new_key = current.is_none() && tree_len.is_none();
    // A small path edit of a large compact document keeps it compact; repeating the
    // tree eligibility parse on every write would make each edit cost the document.
    let small_edit = path.as_ref() != b"$"
        && value.len() <= crate::store::SMALL_JSON_EDIT
        && tree_len.is_none()
        && current.is_some_and(|document| document.len() >= crate::json::TREE_THRESHOLD);
    let mut root_tree = None;
    let updated = if path.as_ref() == b"$" {
        crate::json::set_root_tree(&value).map(|(document, tree)| {
            root_tree = Some(tree);
            Some(document)
        })
    } else {
        crate::json::set_with(current, &path, &value, trusted)
    };
    let document = match updated {
        Ok(Some(document)) => document,
        Ok(None) | Err(crate::json::JsonError::MissingRoot) => {
            return Ok(Response::Bulk(None));
        }
        Err(crate::json::JsonError::Syntax) => {
            return Ok(Response::Error(b"ERR invalid JSON value"));
        }
        Err(crate::json::JsonError::Path) => {
            return Ok(Response::Error(b"ERR invalid JSON path"));
        }
    };
    if document.len() > crate::protocol::DEFAULT_MAX_VALUE_BYTES {
        return Ok(Response::Error(
            b"ERR JSON document exceeds maximum value size",
        ));
    }
    let stored_charge = shard
        .json_tree(&key)
        .map_or(previous_len, |tree| tree.charge());
    // Built once: admission is sized from this tree and the store keeps it.
    let candidate_tree = if small_edit {
        None
    } else if let Some(evaluated) = root_tree {
        evaluated
    } else {
        crate::json::TreeDocument::from_bytes(&document)
    };
    let evaluated = (!small_edit).then_some(());
    let candidate_charge = candidate_tree
        .as_ref()
        .map_or(document.capacity(), |tree| tree.charge());
    let extra = candidate_charge.saturating_sub(stored_charge)
        + if new_key {
            key.len() + ADMISSION_OVERHEAD_BYTES
        } else {
            0
        };
    if let Some(rejected) = admit(shard, sink, [key.as_ref()], extra, on_oom)? {
        return Ok(rejected);
    }
    // A path write to an existing document logs the small request, not the
    // whole result; replay runs the same deterministic edit.
    if path.as_ref() != b"$" && !new_key && tree_len.is_none() {
        sink.record(Mutation::JsonSetPath {
            key: &key,
            path: &path,
            value: &value,
        })?;
    } else {
        sink.record(Mutation::JsonPut {
            key: &key,
            value: &document,
        })?;
    }
    shard
        .put_json_edit_with(
            &key,
            document,
            small_edit,
            evaluated.map(|()| candidate_tree),
        )
        .expect("JSON type validated before recording");
    Ok(Response::Simple(b"OK"))
}

pub(super) fn json_value<C: Clock, I: StorageIndex>(
    shard: &KvShard<C, I>,
    key: &[u8],
    path: &[u8],
) -> Result<Option<Vec<u8>>, Response<'static>> {
    let bad_path = || Response::Error(b"ERR invalid JSON path");
    if let Some(tree) = shard.json_tree(key) {
        return match tree.get(path) {
            Ok(value) => Ok(value),
            Err(crate::json::JsonError::MissingRoot) => Ok(None),
            Err(_) => Err(bad_path()),
        };
    }
    match shard.json_view(key) {
        Ok(None) if crate::json::validate_path(path).is_err() => Err(bad_path()),
        Ok(None) => Ok(None),
        Ok(Some(document)) => crate::json::get_stored(document, path).map_err(|_| bad_path()),
        Err(_) => Err(wrong_type()),
    }
}
