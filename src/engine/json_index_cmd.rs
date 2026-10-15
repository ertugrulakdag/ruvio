//! `JSON.INDEX CREATE|DROP|LIST` and `JSON.FIND`.

use std::borrow::Cow;

use super::*;
use crate::json_index::{Definition, FieldType, IndexState};
use crate::protocol::{Bytes, JsonIndexKind};
use crate::store::json_indexing::{FindError, definition_key};

const MAX_NAME_BYTES: usize = 64;
const DEFAULT_COUNT: usize = 100;
const MAX_COUNT: usize = 1000;

fn error(message: &'static [u8]) -> Response<'static> {
    Response::Error(message)
}

pub(super) fn run<C: Clock, I: StorageIndex, S: MutationSink>(
    shard: &mut KvShard<C, I>,
    sink: &mut S,
    database: u8,
    op: JsonIndexKind,
    args: &[Bytes<'_>],
) -> Result<Response<'static>, S::Error> {
    Ok(match op {
        JsonIndexKind::Create => return create(shard, sink, database, args),
        JsonIndexKind::Drop => return drop_index(shard, sink, database, args),
        JsonIndexKind::List => list(shard, args),
        JsonIndexKind::Find => find(shard, args),
    })
}

fn name_of(raw: &[u8]) -> Option<&str> {
    std::str::from_utf8(raw)
        .ok()
        .filter(|name| !name.is_empty() && name.len() <= MAX_NAME_BYTES && !name.contains('\0'))
}

fn create<C: Clock, I: StorageIndex, S: MutationSink>(
    shard: &mut KvShard<C, I>,
    sink: &mut S,
    database: u8,
    args: &[Bytes<'_>],
) -> Result<Response<'static>, S::Error> {
    const SYNTAX: &[u8] =
        b"ERR syntax: JSON.INDEX CREATE name PREFIX prefix PATH $.field [TYPE string|number|bool]";

    let Some(name) = args.first().and_then(|raw| name_of(raw)) else {
        return Ok(error(b"ERR invalid index name"));
    };
    let (mut prefix, mut path, mut field) = (None, None, FieldType::String);
    let (pairs, leftover) = args[1..].as_chunks::<2>();

    for pair in pairs {
        let option = &pair[0];

        if option.eq_ignore_ascii_case(b"PREFIX") {
            prefix = Some(pair[1].to_vec());
        } else if option.eq_ignore_ascii_case(b"PATH") {
            path = Some(pair[1].to_vec());
        } else if option.eq_ignore_ascii_case(b"TYPE") {
            let Some(parsed) = FieldType::parse(&pair[1]) else {
                return Ok(error(b"ERR TYPE must be string, number or bool"));
            };
            field = parsed;
        } else {
            return Ok(error(SYNTAX));
        }
    }

    let (Some(prefix), Some(path)) = (prefix, path) else {
        return Ok(error(SYNTAX));
    };

    if !leftover.is_empty() {
        return Ok(error(SYNTAX));
    }

    if path.as_slice() == b"$" || crate::json::validate_path(&path).is_err() {
        return Ok(error(b"ERR invalid JSON path"));
    }

    if database != 0 {
        return Ok(error(b"ERR JSON.INDEX CREATE must run in database 0"));
    }

    if shard
        .json_index_list()
        .iter()
        .any(|(d, _, _, _)| d.name == name)
    {
        return Ok(error(b"ERR index already exists"));
    }

    let definition = Definition {
        name: name.to_owned(),
        prefix,
        path,
        field,
    };
    let key = definition_key(name);
    let value = definition.encode();

    if let Some(rejected) = before_mutation(
        shard,
        sink,
        [key.as_slice()],
        key.len() + value.len() + ADMISSION_OVERHEAD_BYTES,
    )? {
        return Ok(own(rejected));
    }

    sink.record(Mutation::SetBytes {
        key: &key,
        value: &value,
    })?;
    shard.set_bytes(&key, &value);
    shard.json_index_create(definition);
    Ok(Response::Simple(b"OK"))
}

fn drop_index<C: Clock, I: StorageIndex, S: MutationSink>(
    shard: &mut KvShard<C, I>,
    sink: &mut S,
    database: u8,
    args: &[Bytes<'_>],
) -> Result<Response<'static>, S::Error> {
    let [raw] = args else {
        return Ok(error(b"ERR syntax: JSON.INDEX DROP name"));
    };
    let Some(name) = name_of(raw) else {
        return Ok(error(b"ERR invalid index name"));
    };

    if database != 0 {
        return Ok(error(b"ERR JSON.INDEX DROP must run in database 0"));
    }

    if !shard
        .json_index_list()
        .iter()
        .any(|(d, _, _, _)| d.name == name)
    {
        return Ok(Response::Integer(0));
    }

    let keys: [Bytes<'_>; 1] = [Cow::Owned(definition_key(name))];

    before_mutation_allow_oom(shard, sink, [keys[0].as_ref()], 0)?;
    sink.record(Mutation::Delete { keys: &keys })?;
    shard.delete(&keys[0]);
    shard.json_index_drop(name);
    Ok(Response::Integer(1))
}

fn list<C: Clock, I: StorageIndex>(shard: &KvShard<C, I>, args: &[Bytes<'_>]) -> Response<'static> {
    if !args.is_empty() {
        return error(b"ERR syntax: JSON.INDEX LIST");
    }

    Response::Array(
        shard
            .json_index_list()
            .into_iter()
            .map(|(definition, state, entries, bytes)| {
                let line = format!(
                    "{} prefix={} path={} type={} state={} keys={} memory={}",
                    definition.name,
                    String::from_utf8_lossy(&definition.prefix),
                    String::from_utf8_lossy(&definition.path),
                    definition.field.name(),
                    if state == IndexState::Ready {
                        "ready"
                    } else {
                        "building"
                    },
                    entries,
                    bytes,
                );

                Some(Cow::Owned(line.into_bytes()))
            })
            .collect(),
    )
}

fn find<C: Clock, I: StorageIndex>(
    shard: &mut KvShard<C, I>,
    args: &[Bytes<'_>],
) -> Response<'static> {
    const SYNTAX: &[u8] = b"ERR syntax: JSON.FIND name VALUE value [COUNT n] [AFTER key]";

    let Some(name) = args.first().and_then(|raw| name_of(raw)) else {
        return error(b"ERR invalid index name");
    };
    let (mut value, mut count, mut after) = (None, DEFAULT_COUNT, None);
    let (pairs, leftover) = args[1..].as_chunks::<2>();

    for pair in pairs {
        let option = &pair[0];

        if option.eq_ignore_ascii_case(b"VALUE") {
            value = Some(&pair[1]);
        } else if option.eq_ignore_ascii_case(b"COUNT") {
            match std::str::from_utf8(&pair[1])
                .ok()
                .and_then(|text| text.parse::<usize>().ok())
                .filter(|count| (1..=MAX_COUNT).contains(count))
            {
                Some(parsed) => count = parsed,
                None => return error(b"ERR COUNT must be between 1 and 1000"),
            }
        } else if option.eq_ignore_ascii_case(b"AFTER") {
            after = Some(pair[1].to_vec());
        } else {
            return error(SYNTAX);
        }
    }

    let Some(value) = value else {
        return error(SYNTAX);
    };

    if !leftover.is_empty() {
        return error(SYNTAX);
    }

    match shard.json_index_find(name, value, after.as_deref(), count) {
        Ok(keys) => Response::Array(keys.into_iter().map(|key| Some(Cow::Owned(key))).collect()),
        Err(FindError::NoSuchIndex) => error(b"ERR no such index"),
        Err(FindError::Building) => error(b"ERR index is building"),
        Err(FindError::BadValue) => error(b"ERR VALUE does not match the index type"),
    }
}

fn own(response: Response<'_>) -> Response<'static> {
    match response {
        Response::Error(message) => Response::Error(message),
        _ => Response::Error(b"ERR OOM command not allowed when used memory > 'maxmemory'"),
    }
}
