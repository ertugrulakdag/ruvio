//! Per-command parsers carried over from the former `parse_arguments` chain. The table
//! (`protocol/table.rs`) dispatches to them; each takes the command name (for the
//! several that share a body) and the argument vector.

use super::*;

pub(in crate::protocol) fn cmd_wait<'a>(
    _name: &[u8],
    arguments: Vec<Bytes<'a>>,
) -> Result<Command<'a>, ProtocolError> {
    if arguments.len() != 2 {
        return Err(ProtocolError::Invalid(
            "WAIT expects a replica count and a timeout",
        ));
    }

    let replicas = parse_i64(&arguments[0])?;
    let timeout = parse_i64(&arguments[1])?;

    if replicas < 0 {
        return Err(ProtocolError::Invalid("WAIT replica count is out of range"));
    }

    if timeout < 0 {
        return Err(ProtocolError::Invalid("WAIT timeout is negative"));
    }

    Ok(Command::Wait {
        replicas: replicas as u64,
        timeout_ms: timeout as u64,
    })
}

pub(in crate::protocol) fn cmd_ruvio<'a>(
    _name: &[u8],
    mut arguments: Vec<Bytes<'a>>,
) -> Result<Command<'a>, ProtocolError> {
    match arguments.len() {
        0 => Ok(Command::Ruvio(None)),
        1 => Ok(Command::Ruvio(arguments.pop())),
        _ => Err(ProtocolError::Invalid("RUVIO expects zero or one argument")),
    }
}

pub(in crate::protocol) fn cmd_cluster<'a>(
    _name: &[u8],
    mut arguments: Vec<Bytes<'a>>,
) -> Result<Command<'a>, ProtocolError> {
    match arguments.as_slice() {
        [subcommand] if subcommand.eq_ignore_ascii_case(b"SLOTS") => Ok(Command::ClusterSlots),
        [subcommand] if subcommand.eq_ignore_ascii_case(b"NODES") => Ok(Command::ClusterNodes),
        [subcommand] if subcommand.eq_ignore_ascii_case(b"INFO") => Ok(Command::ClusterInfo),
        [subcommand] if subcommand.eq_ignore_ascii_case(b"SHARDS") => Ok(Command::ClusterShards),

        [subcommand] if subcommand.eq_ignore_ascii_case(b"MYID") => Ok(Command::ClusterMyId),
        [subcommand, _] if subcommand.eq_ignore_ascii_case(b"KEYSLOT") => Ok(
            Command::ClusterKeySlot(arguments.pop().expect("length checked")),
        ),
        _ => Err(ProtocolError::Invalid(
            "only CLUSTER SLOTS, NODES, SHARDS, INFO, MYID, and KEYSLOT are supported",
        )),
    }
}

pub(in crate::protocol) fn cmd_readonly<'a>(
    name: &[u8],
    arguments: Vec<Bytes<'a>>,
) -> Result<Command<'a>, ProtocolError> {
    if !arguments.is_empty() {
        return Err(ProtocolError::Invalid(
            "READONLY and READWRITE take no arguments",
        ));
    }

    Ok(if name.eq_ignore_ascii_case(b"READONLY") {
        Command::ReadOnly
    } else {
        Command::ReadWrite
    })
}

pub(in crate::protocol) fn cmd_config<'a>(
    _name: &[u8],
    mut arguments: Vec<Bytes<'a>>,
) -> Result<Command<'a>, ProtocolError> {
    if arguments.len() == 2 && arguments[0].eq_ignore_ascii_case(b"GET") {
        return Ok(Command::ConfigGet(arguments.pop().expect("length checked")));
    }

    Err(ProtocolError::Invalid("only CONFIG GET is supported"))
}

pub(in crate::protocol) fn cmd_info<'a>(
    _name: &[u8],
    mut arguments: Vec<Bytes<'a>>,
) -> Result<Command<'a>, ProtocolError> {
    match arguments.len() {
        0 => Ok(Command::Info(None)),
        1 => Ok(Command::Info(arguments.pop())),
        _ => Err(ProtocolError::Invalid("INFO expects zero or one argument")),
    }
}

pub(in crate::protocol) fn cmd_save<'a>(
    _name: &[u8],
    arguments: Vec<Bytes<'a>>,
) -> Result<Command<'a>, ProtocolError> {
    match arguments.len() {
        0 => Ok(Command::Save),
        _ => Err(ProtocolError::Invalid("SAVE expects no arguments")),
    }
}

pub(in crate::protocol) fn cmd_bgsave<'a>(
    _name: &[u8],
    arguments: Vec<Bytes<'a>>,
) -> Result<Command<'a>, ProtocolError> {
    match arguments.len() {
        0 => Ok(Command::Bgsave),
        _ => Err(ProtocolError::Invalid("BGSAVE expects no arguments")),
    }
}

/// RedisJSON's legacy paths start at `.` or `[` (or are just `.`); store them as `$` paths.
fn json_path(path: Bytes<'_>) -> Bytes<'_> {
    match path.first() {
        Some(b'.') if path.len() == 1 => Cow::Borrowed(b"$"),
        Some(b'.' | b'[') => {
            let mut normalized = Vec::with_capacity(path.len() + 1);
            normalized.push(b'$');
            normalized.extend_from_slice(&path);
            Cow::Owned(normalized)
        }
        _ => path,
    }
}

pub(in crate::protocol) fn cmd_json_op<'a>(
    name: &[u8],
    mut arguments: Vec<Bytes<'a>>,
) -> Result<Command<'a>, ProtocolError> {
    use crate::protocol::JsonOpKind as Op;
    let op = [
        (&b"JSON.TYPE"[..], Op::Type),
        (b"JSON.STRLEN", Op::StrLen),
        (b"JSON.ARRLEN", Op::ArrLen),
        (b"JSON.OBJKEYS", Op::ObjKeys),
        (b"JSON.NUMINCRBY", Op::NumIncrBy),
        (b"JSON.TOGGLE", Op::Toggle),
        (b"JSON.ARRAPPEND", Op::ArrAppend),
        (b"JSON.CLEAR", Op::Clear),
        (b"JSON.OBJLEN", Op::ObjLen),
        (b"JSON.STRAPPEND", Op::StrAppend),
        (b"JSON.NUMMULTBY", Op::NumMultBy),
        (b"JSON.ARRINSERT", Op::ArrInsert),
        (b"JSON.ARRPOP", Op::ArrPop),
        (b"JSON.ARRTRIM", Op::ArrTrim),
        (b"JSON.ARRINDEX", Op::ArrIndex),
    ]
    .into_iter()
    .find(|(candidate, _)| name.eq_ignore_ascii_case(candidate))
    .map(|(_, op)| op)
    .ok_or(ProtocolError::Invalid("unknown JSON command"))?;
    let (minimum, maximum) = match op {
        Op::NumIncrBy | Op::NumMultBy | Op::StrAppend => (3, 3),
        Op::ArrAppend => (3, usize::MAX),
        Op::ArrInsert => (4, usize::MAX),
        Op::ArrTrim => (4, 4),
        Op::ArrIndex => (3, 5),
        Op::ArrPop => (1, 3),
        _ => (1, 2),
    };
    if arguments.len() < minimum || arguments.len() > maximum {
        return Err(ProtocolError::Invalid(
            "JSON command has the wrong number of arguments",
        ));
    }
    let key = arguments.remove(0);
    let path = if arguments.is_empty() {
        Cow::Borrowed(&b"$"[..])
    } else {
        json_path(arguments.remove(0))
    };
    Ok(Command::JsonOp {
        op,
        key,
        path,
        args: arguments,
    })
}

pub(in crate::protocol) fn cmd_json_index<'a>(
    name: &[u8],
    mut arguments: Vec<Bytes<'a>>,
) -> Result<Command<'a>, ProtocolError> {
    use crate::protocol::JsonIndexKind;

    let op = if name.eq_ignore_ascii_case(b"JSON.FIND") {
        if arguments.len() < 3 {
            return Err(ProtocolError::Invalid(
                "wrong number of arguments for 'json.find' command",
            ));
        }
        JsonIndexKind::Find
    } else {
        if arguments.is_empty() {
            return Err(ProtocolError::Invalid("JSON.INDEX expects a subcommand"));
        }
        let sub = arguments.remove(0);
        if sub.eq_ignore_ascii_case(b"CREATE") {
            JsonIndexKind::Create
        } else if sub.eq_ignore_ascii_case(b"DROP") {
            JsonIndexKind::Drop
        } else if sub.eq_ignore_ascii_case(b"LIST") {
            JsonIndexKind::List
        } else {
            return Err(ProtocolError::Invalid(
                "JSON.INDEX subcommand must be CREATE, DROP or LIST",
            ));
        }
    };

    Ok(Command::JsonIndex {
        op,
        args: arguments,
    })
}

pub(in crate::protocol) fn cmd_json_get<'a>(
    name: &[u8],
    mut arguments: Vec<Bytes<'a>>,
) -> Result<Command<'a>, ProtocolError> {
    match arguments.len() {
        1 | 2 => {
            let key = arguments.remove(0);
            let path = json_path(arguments.pop().unwrap_or(Cow::Borrowed(b"$")));
            if name.eq_ignore_ascii_case(b"JSON.GET") {
                Ok(Command::JsonGet { key, path })
            } else {
                Ok(Command::JsonDel { key, path })
            }
        }
        _ => Err(ProtocolError::Invalid(
            "JSON.GET/JSON.DEL expects key and optional path",
        )),
    }
}

pub(in crate::protocol) fn cmd_json_mget<'a>(
    _name: &[u8],
    mut arguments: Vec<Bytes<'a>>,
) -> Result<Command<'a>, ProtocolError> {
    if arguments.len() < 2 {
        return Err(ProtocolError::Invalid(
            "JSON.MGET expects at least one key and a path",
        ));
    }

    let path = json_path(arguments.pop().unwrap_or(Cow::Borrowed(b"$")));

    Ok(Command::JsonMGet {
        keys: arguments,
        path,
    })
}

pub(in crate::protocol) fn cmd_json_set<'a>(
    _name: &[u8],
    mut arguments: Vec<Bytes<'a>>,
) -> Result<Command<'a>, ProtocolError> {
    match arguments.len() {
        3 => Ok(Command::JsonSet {
            key: arguments.remove(0),
            path: json_path(arguments.remove(0)),
            value: arguments.remove(0),
        }),
        4 => {
            let condition = arguments.pop().expect("length checked");
            let must_exist = if condition.eq_ignore_ascii_case(b"XX") {
                true
            } else if condition.eq_ignore_ascii_case(b"NX") {
                false
            } else {
                return Err(ProtocolError::Invalid("syntax error"));
            };

            Ok(Command::JsonSetIf {
                key: arguments.remove(0),
                path: json_path(arguments.remove(0)),
                value: arguments.remove(0),
                must_exist,
            })
        }
        _ => Err(ProtocolError::Invalid(
            "JSON.SET expects key, path, JSON value and an optional NX or XX",
        )),
    }
}

pub(in crate::protocol) fn cmd_json_merge<'a>(
    _name: &[u8],
    mut arguments: Vec<Bytes<'a>>,
) -> Result<Command<'a>, ProtocolError> {
    match arguments.len() {
        3 => Ok(Command::JsonMerge {
            key: arguments.remove(0),
            path: json_path(arguments.remove(0)),
            value: arguments.remove(0),
        }),
        _ => Err(ProtocolError::Invalid(
            "JSON.MERGE expects key, path, and JSON patch",
        )),
    }
}

pub(in crate::protocol) fn cmd_lease<'a>(
    _name: &[u8],
    mut arguments: Vec<Bytes<'a>>,
) -> Result<Command<'a>, ProtocolError> {
    if arguments.len() != 2 {
        return Err(ProtocolError::Invalid(
            "LEASE expects a key and a TTL in milliseconds",
        ));
    }

    let key = arguments.remove(0);
    let ttl_ms = parse_u64(arguments.remove(0).as_ref())
        .map_err(|_| ProtocolError::Invalid("LEASE TTL must be a positive integer"))?;

    if ttl_ms == 0 {
        return Err(ProtocolError::Invalid(
            "LEASE TTL must be a positive integer",
        ));
    }

    Ok(Command::Lease { key, ttl_ms })
}

pub(in crate::protocol) fn cmd_semaphore<'a>(
    _name: &[u8],
    mut arguments: Vec<Bytes<'a>>,
) -> Result<Command<'a>, ProtocolError> {
    if arguments.len() != 3 {
        return Err(ProtocolError::Invalid(
            "SEMAPHORE expects a key, a maximum, and a TTL in milliseconds",
        ));
    }

    let key = arguments.remove(0);
    let max = parse_u64(arguments.remove(0).as_ref())
        .map_err(|_| ProtocolError::Invalid("SEMAPHORE maximum must be a positive integer"))?;
    let ttl_ms = parse_u64(arguments.remove(0).as_ref())
        .map_err(|_| ProtocolError::Invalid("SEMAPHORE TTL must be a positive integer"))?;

    if max == 0 || max > u64::from(u32::MAX) || ttl_ms == 0 {
        return Err(ProtocolError::Invalid(
            "SEMAPHORE maximum and TTL must be positive integers",
        ));
    }

    Ok(Command::Semaphore { key, max, ttl_ms })
}

pub(in crate::protocol) fn cmd_limit<'a>(
    _name: &[u8],
    mut arguments: Vec<Bytes<'a>>,
) -> Result<Command<'a>, ProtocolError> {
    if arguments.len() != 3 {
        return Err(ProtocolError::Invalid(
            "LIMIT expects a key, a maximum, and a window in milliseconds",
        ));
    }

    let key = arguments.remove(0);
    let max = parse_u64(arguments.remove(0).as_ref())
        .map_err(|_| ProtocolError::Invalid("LIMIT maximum must be a positive integer"))?;
    let window_ms = parse_u64(arguments.remove(0).as_ref())
        .map_err(|_| ProtocolError::Invalid("LIMIT window must be a positive integer"))?;

    if max == 0 || window_ms == 0 {
        return Err(ProtocolError::Invalid(
            "LIMIT maximum and window must be positive integers",
        ));
    }

    Ok(Command::Limit {
        key,
        max,
        window_ms,
    })
}

pub(in crate::protocol) fn cmd_idem<'a>(
    _name: &[u8],
    mut arguments: Vec<Bytes<'a>>,
) -> Result<Command<'a>, ProtocolError> {
    if arguments.len() != 5 {
        return Err(ProtocolError::Invalid(
            "IDEM expects key BEGIN fingerprint owner ttl_ms or key COMPLETE fingerprint owner result",
        ));
    }
    let key = arguments.remove(0);
    let action = arguments.remove(0);
    let fingerprint = arguments.remove(0);
    let owner = arguments.remove(0);
    if fingerprint.is_empty() || fingerprint.len() > 128 || owner.is_empty() || owner.len() > 128 {
        return Err(ProtocolError::Invalid(
            "IDEM fingerprint and owner must contain 1-128 bytes",
        ));
    }
    let value = arguments.remove(0);
    if action.eq_ignore_ascii_case(b"BEGIN") {
        let ttl_ms = parse_u64(&value)?;
        if ttl_ms == 0 || ttl_ms > MAX_IDEMPOTENCY_TTL_MS {
            return Err(ProtocolError::Invalid(
                "IDEM TTL must be 1-604800000 milliseconds",
            ));
        }
        return Ok(Command::IdempotencyBegin {
            key,
            fingerprint,
            owner,
            ttl_ms,
        });
    }
    if action.eq_ignore_ascii_case(b"COMPLETE") {
        if value.len() > MAX_IDEMPOTENCY_RESULT_BYTES {
            return Err(ProtocolError::Invalid("IDEM result exceeds 4 MiB"));
        }
        return Ok(Command::IdempotencyComplete {
            key,
            fingerprint,
            owner,
            result: value,
        });
    }
    Err(ProtocolError::Invalid(
        "IDEM action must be BEGIN or COMPLETE",
    ))
}

pub(in crate::protocol) fn cmd_once<'a>(
    _name: &[u8],
    mut arguments: Vec<Bytes<'a>>,
) -> Result<Command<'a>, ProtocolError> {
    if arguments.len() != 2 {
        return Err(ProtocolError::Invalid(
            "ONCE expects a key and a TTL in milliseconds",
        ));
    }

    let key = arguments.remove(0);
    let ttl_ms = parse_u64(arguments.remove(0).as_ref())
        .map_err(|_| ProtocolError::Invalid("ONCE TTL must be a positive integer"))?;

    if ttl_ms == 0 {
        return Err(ProtocolError::Invalid(
            "ONCE TTL must be a positive integer",
        ));
    }

    Ok(Command::Once { key, ttl_ms })
}

pub(in crate::protocol) fn cmd_take<'a>(
    _name: &[u8],
    mut arguments: Vec<Bytes<'a>>,
) -> Result<Command<'a>, ProtocolError> {
    if arguments.len() != 2 {
        return Err(ProtocolError::Invalid(
            "TAKE expects a key and a positive count",
        ));
    }

    let key = arguments.remove(0);
    let count = parse_i64(arguments.remove(0).as_ref())
        .map_err(|_| ProtocolError::Invalid("TAKE count must be a positive integer"))?;

    if count <= 0 {
        return Err(ProtocolError::Invalid(
            "TAKE count must be a positive integer",
        ));
    }

    Ok(Command::Take { key, count })
}

pub(in crate::protocol) fn cmd_getex<'a>(
    _name: &[u8],
    mut arguments: Vec<Bytes<'a>>,
) -> Result<Command<'a>, ProtocolError> {
    if arguments.len() != 2 {
        return Err(ProtocolError::Invalid(
            "GETEX expects a key and a TTL in milliseconds",
        ));
    }

    let key = arguments.remove(0);
    let ttl_ms = parse_u64(arguments.remove(0).as_ref())
        .map_err(|_| ProtocolError::Invalid("GETEX TTL must be a positive integer"))?;

    if ttl_ms == 0 {
        return Err(ProtocolError::Invalid(
            "GETEX TTL must be a positive integer",
        ));
    }

    Ok(Command::GetEx { key, ttl_ms })
}

pub(in crate::protocol) fn cmd_release<'a>(
    _name: &[u8],
    mut arguments: Vec<Bytes<'a>>,
) -> Result<Command<'a>, ProtocolError> {
    if arguments.len() != 2 {
        return Err(ProtocolError::Invalid("RELEASE expects a key and a token"));
    }

    let key = arguments.remove(0);
    let token = parse_u64(arguments.remove(0).as_ref())
        .map_err(|_| ProtocolError::Invalid("RELEASE token must be an integer"))?;

    Ok(Command::Release { key, token })
}

pub(in crate::protocol) fn cmd_setv<'a>(
    _name: &[u8],
    mut arguments: Vec<Bytes<'a>>,
) -> Result<Command<'a>, ProtocolError> {
    if arguments.len() != 2 && arguments.len() != 3 {
        return Err(ProtocolError::Invalid(
            "SETV expects a key, a value, and an optional version",
        ));
    }

    let key = arguments.remove(0);
    let value = arguments.remove(0);
    let expected = if arguments.is_empty() {
        None
    } else {
        Some(
            parse_u64(arguments.remove(0).as_ref())
                .map_err(|_| ProtocolError::Invalid("SETV version must be an integer"))?,
        )
    };

    Ok(Command::SetVersioned {
        key,
        value,
        expected,
    })
}

pub(in crate::protocol) fn cmd_changes<'a>(
    _name: &[u8],
    arguments: Vec<Bytes<'a>>,
) -> Result<Command<'a>, ProtocolError> {
    if arguments
        .first()
        .is_some_and(|word| word.as_ref().eq_ignore_ascii_case(b"SUBSCRIBE"))
    {
        let options: Vec<&[u8]> = arguments[1..].iter().map(AsRef::as_ref).collect();

        return Ok(Command::ChangesSubscribe(
            crate::changes::ChangeSubscription::parse(&options)?,
        ));
    }

    match arguments.as_slice() {
        [word] if word.as_ref().eq_ignore_ascii_case(b"START") => Ok(Command::Changes(None)),
        [sequence] => Ok(Command::Changes(Some(
            parse_u64(sequence.as_ref())
                .map_err(|_| ProtocolError::Invalid("CHANGES expects START or a cursor"))?,
        ))),
        _ => Err(ProtocolError::Invalid("CHANGES expects START or a cursor")),
    }
}

pub(in crate::protocol) fn cmd_scan<'a>(
    _name: &[u8],
    arguments: Vec<Bytes<'a>>,
) -> Result<Command<'a>, ProtocolError> {
    parse_scan(arguments)
}

pub(in crate::protocol) fn cmd_flushdb<'a>(
    name: &[u8],
    arguments: Vec<Bytes<'a>>,
) -> Result<Command<'a>, ProtocolError> {
    let all = name.eq_ignore_ascii_case(b"FLUSHALL");

    match arguments.as_slice() {
        [] => Ok(if all {
            Command::FlushAll
        } else {
            Command::FlushDb
        }),
        [mode] if mode.eq_ignore_ascii_case(b"ASYNC") || mode.eq_ignore_ascii_case(b"SYNC") => {
            Ok(if all {
                Command::FlushAll
            } else {
                Command::FlushDb
            })
        }

        _ => Err(ProtocolError::Invalid("syntax error")),
    }
}

pub(in crate::protocol) fn cmd_swapdb<'a>(
    _name: &[u8],
    arguments: Vec<Bytes<'a>>,
) -> Result<Command<'a>, ProtocolError> {
    match arguments.as_slice() {
        [left, right] => Ok(Command::SwapDb(parse_i64(left)?, parse_i64(right)?)),
        _ => Err(ProtocolError::Invalid(
            "SWAPDB expects two database indexes",
        )),
    }
}

pub(in crate::protocol) fn cmd_move<'a>(
    _name: &[u8],
    arguments: Vec<Bytes<'a>>,
) -> Result<Command<'a>, ProtocolError> {
    match arguments.len() {
        2 => {
            let db = parse_i64(&arguments[1])?;
            let mut arguments = arguments.into_iter();

            Ok(Command::Move {
                key: arguments.next().expect("two arguments"),
                db,
            })
        }

        _ => Err(ProtocolError::Invalid(
            "MOVE expects a key and a database index",
        )),
    }
}

pub(in crate::protocol) fn cmd_subscribe<'a>(
    _name: &[u8],
    arguments: Vec<Bytes<'a>>,
) -> Result<Command<'a>, ProtocolError> {
    if arguments.is_empty() {
        Err(ProtocolError::Invalid(
            "SUBSCRIBE expects at least one channel",
        ))
    } else {
        Ok(Command::Subscribe(arguments))
    }
}

pub(in crate::protocol) fn cmd_unsubscribe<'a>(
    _name: &[u8],
    arguments: Vec<Bytes<'a>>,
) -> Result<Command<'a>, ProtocolError> {
    Ok(Command::Unsubscribe(arguments))
}

pub(in crate::protocol) fn cmd_psubscribe<'a>(
    _name: &[u8],
    arguments: Vec<Bytes<'a>>,
) -> Result<Command<'a>, ProtocolError> {
    if arguments.is_empty() {
        Err(ProtocolError::Invalid(
            "PSUBSCRIBE expects at least one pattern",
        ))
    } else {
        Ok(Command::PSubscribe(arguments))
    }
}

pub(in crate::protocol) fn cmd_punsubscribe<'a>(
    _name: &[u8],
    arguments: Vec<Bytes<'a>>,
) -> Result<Command<'a>, ProtocolError> {
    Ok(Command::PUnsubscribe(arguments))
}

pub(in crate::protocol) fn cmd_spublish<'a>(
    _name: &[u8],
    mut arguments: Vec<Bytes<'a>>,
) -> Result<Command<'a>, ProtocolError> {
    match arguments.len() {
        2 => {
            let message = arguments.pop().expect("length checked");
            let channel = arguments.pop().expect("length checked");

            Ok(Command::ShardPublish { channel, message })
        }

        _ => Err(ProtocolError::Invalid(
            "SPUBLISH expects a channel and a message",
        )),
    }
}

pub(in crate::protocol) fn cmd_ssubscribe<'a>(
    _name: &[u8],
    arguments: Vec<Bytes<'a>>,
) -> Result<Command<'a>, ProtocolError> {
    if arguments.is_empty() {
        Err(ProtocolError::Invalid(
            "SSUBSCRIBE expects at least one channel",
        ))
    } else {
        Ok(Command::ShardSubscribe(arguments))
    }
}

pub(in crate::protocol) fn cmd_sunsubscribe<'a>(
    _name: &[u8],
    arguments: Vec<Bytes<'a>>,
) -> Result<Command<'a>, ProtocolError> {
    Ok(Command::ShardUnsubscribe(arguments))
}

pub(in crate::protocol) fn cmd_hello<'a>(
    _name: &[u8],
    arguments: Vec<Bytes<'a>>,
) -> Result<Command<'a>, ProtocolError> {
    parse_hello(arguments)
}

pub(in crate::protocol) fn cmd_client<'a>(
    _name: &[u8],
    arguments: Vec<Bytes<'a>>,
) -> Result<Command<'a>, ProtocolError> {
    parse_client(arguments)
}

pub(in crate::protocol) fn cmd_auth<'a>(
    _name: &[u8],
    mut arguments: Vec<Bytes<'a>>,
) -> Result<Command<'a>, ProtocolError> {
    match arguments.len() {
        1 => Ok(Command::Auth {
            username: None,
            password: arguments.pop().expect("length checked"),
        }),
        2 => {
            let password = arguments.pop().expect("length checked");
            let username = arguments.pop().expect("length checked");

            Ok(Command::Auth {
                username: Some(username),
                password,
            })
        }

        _ => Err(ProtocolError::Invalid("AUTH expects a password")),
    }
}

pub(in crate::protocol) fn cmd_acl<'a>(
    _name: &[u8],
    arguments: Vec<Bytes<'a>>,
) -> Result<Command<'a>, ProtocolError> {
    parse_acl_command(arguments)
}

pub(in crate::protocol) fn cmd_blpop<'a>(
    name: &[u8],
    arguments: Vec<Bytes<'a>>,
) -> Result<Command<'a>, ProtocolError> {
    parse_list_block(arguments, name.eq_ignore_ascii_case(b"BRPOP"))
}

pub(in crate::protocol) fn cmd_sinter<'a>(
    _name: &[u8],
    arguments: Vec<Bytes<'a>>,
) -> Result<Command<'a>, ProtocolError> {
    parse_set_algebra(arguments, SetOp::Inter, false)
}

pub(in crate::protocol) fn cmd_sunion<'a>(
    _name: &[u8],
    arguments: Vec<Bytes<'a>>,
) -> Result<Command<'a>, ProtocolError> {
    parse_set_algebra(arguments, SetOp::Union, false)
}

pub(in crate::protocol) fn cmd_sdiff<'a>(
    _name: &[u8],
    arguments: Vec<Bytes<'a>>,
) -> Result<Command<'a>, ProtocolError> {
    parse_set_algebra(arguments, SetOp::Diff, false)
}

pub(in crate::protocol) fn cmd_sinterstore<'a>(
    _name: &[u8],
    arguments: Vec<Bytes<'a>>,
) -> Result<Command<'a>, ProtocolError> {
    parse_set_algebra(arguments, SetOp::Inter, true)
}

pub(in crate::protocol) fn cmd_sunionstore<'a>(
    _name: &[u8],
    arguments: Vec<Bytes<'a>>,
) -> Result<Command<'a>, ProtocolError> {
    parse_set_algebra(arguments, SetOp::Union, true)
}

pub(in crate::protocol) fn cmd_sdiffstore<'a>(
    _name: &[u8],
    arguments: Vec<Bytes<'a>>,
) -> Result<Command<'a>, ProtocolError> {
    parse_set_algebra(arguments, SetOp::Diff, true)
}

pub(in crate::protocol) fn cmd_smove<'a>(
    _name: &[u8],
    mut arguments: Vec<Bytes<'a>>,
) -> Result<Command<'a>, ProtocolError> {
    match arguments.len() {
        3 => {
            let member = arguments.pop().expect("length checked");
            let destination = arguments.pop().expect("length checked");
            let source = arguments.pop().expect("length checked");

            Ok(Command::SetMove {
                source,
                destination,
                member,
            })
        }

        _ => Err(ProtocolError::Invalid(
            "SMOVE expects a source, destination, and member",
        )),
    }
}

pub(in crate::protocol) fn cmd_hscan<'a>(
    _name: &[u8],
    arguments: Vec<Bytes<'a>>,
) -> Result<Command<'a>, ProtocolError> {
    parse_hash_scan(arguments)
}

pub(in crate::protocol) fn cmd_sscan<'a>(
    _name: &[u8],
    arguments: Vec<Bytes<'a>>,
) -> Result<Command<'a>, ProtocolError> {
    parse_collection_scan(arguments, CollectionScanKind::Set)
}

pub(in crate::protocol) fn cmd_zscan<'a>(
    _name: &[u8],
    arguments: Vec<Bytes<'a>>,
) -> Result<Command<'a>, ProtocolError> {
    parse_collection_scan(arguments, CollectionScanKind::SortedSet)
}

pub(in crate::protocol) fn cmd_hexpire<'a>(
    name: &[u8],
    arguments: Vec<Bytes<'a>>,
) -> Result<Command<'a>, ProtocolError> {
    parse_hash_expire(arguments, name)
}

pub(in crate::protocol) fn cmd_httl<'a>(
    name: &[u8],
    arguments: Vec<Bytes<'a>>,
) -> Result<Command<'a>, ProtocolError> {
    parse_hash_field_read(arguments, name)
}

pub(in crate::protocol) fn cmd_zrange<'a>(
    name: &[u8],
    arguments: Vec<Bytes<'a>>,
) -> Result<Command<'a>, ProtocolError> {
    parse_sorted_set_range(arguments, name.eq_ignore_ascii_case(b"ZREVRANGE"))
}

pub(in crate::protocol) fn cmd_zrangebyscore<'a>(
    _name: &[u8],
    arguments: Vec<Bytes<'a>>,
) -> Result<Command<'a>, ProtocolError> {
    parse_sorted_set_range_by_score(arguments)
}

pub(in crate::protocol) fn cmd_zrangebylex<'a>(
    name: &[u8],
    arguments: Vec<Bytes<'a>>,
) -> Result<Command<'a>, ProtocolError> {
    parse_sorted_set_range_by_lex(arguments, name.eq_ignore_ascii_case(b"ZREVRANGEBYLEX"))
}

pub(in crate::protocol) fn cmd_zpopmin<'a>(
    name: &[u8],
    arguments: Vec<Bytes<'a>>,
) -> Result<Command<'a>, ProtocolError> {
    parse_sorted_set_pop(arguments, name.eq_ignore_ascii_case(b"ZPOPMAX"))
}

pub(in crate::protocol) fn cmd_bzpopmin<'a>(
    name: &[u8],
    mut arguments: Vec<Bytes<'a>>,
) -> Result<Command<'a>, ProtocolError> {
    let max = name.eq_ignore_ascii_case(b"BZPOPMAX");

    if arguments.len() < 2 {
        return Err(ProtocolError::Invalid(if max {
            "wrong number of arguments for 'bzpopmax' command"
        } else {
            "wrong number of arguments for 'bzpopmin' command"
        }));
    }

    let timeout_ms = parse_timeout_ms(&arguments.pop().expect("length checked"))?;

    Ok(Command::SortedSetBlockPop {
        keys: arguments,
        timeout_ms,
        max,
    })
}

pub(in crate::protocol) fn cmd_zremrangebyrank<'a>(
    name: &[u8],
    arguments: Vec<Bytes<'a>>,
) -> Result<Command<'a>, ProtocolError> {
    parse_sorted_set_remove_range(arguments, name)
}

pub(in crate::protocol) fn cmd_geoadd<'a>(
    _name: &[u8],
    arguments: Vec<Bytes<'a>>,
) -> Result<Command<'a>, ProtocolError> {
    parse_geo_add(arguments)
}

pub(in crate::protocol) fn cmd_geodist<'a>(
    _name: &[u8],
    arguments: Vec<Bytes<'a>>,
) -> Result<Command<'a>, ProtocolError> {
    parse_geo_dist(arguments)
}

pub(in crate::protocol) fn cmd_geopos<'a>(
    _name: &[u8],
    arguments: Vec<Bytes<'a>>,
) -> Result<Command<'a>, ProtocolError> {
    parse_geo_pos(arguments)
}

pub(in crate::protocol) fn cmd_geohash<'a>(
    _name: &[u8],
    arguments: Vec<Bytes<'a>>,
) -> Result<Command<'a>, ProtocolError> {
    parse_geo_hash(arguments)
}

pub(in crate::protocol) fn cmd_geosearch<'a>(
    _name: &[u8],
    arguments: Vec<Bytes<'a>>,
) -> Result<Command<'a>, ProtocolError> {
    parse_geo_search(arguments, false)
}

pub(in crate::protocol) fn cmd_geosearchstore<'a>(
    _name: &[u8],
    arguments: Vec<Bytes<'a>>,
) -> Result<Command<'a>, ProtocolError> {
    parse_geo_search(arguments, true)
}

pub(in crate::protocol) fn cmd_georadius<'a>(
    _name: &[u8],
    arguments: Vec<Bytes<'a>>,
) -> Result<Command<'a>, ProtocolError> {
    parse_geo_radius(arguments, false)
}

pub(in crate::protocol) fn cmd_georadiusbymember<'a>(
    _name: &[u8],
    arguments: Vec<Bytes<'a>>,
) -> Result<Command<'a>, ProtocolError> {
    parse_geo_radius(arguments, true)
}

const BLOOM_DEFAULT_ERROR_RATE: f64 = 0.01;
const BLOOM_DEFAULT_CAPACITY: usize = 100;

/// `EXPANSION n` (at least 1) or `NONSCALING` at `arguments[index]`; `Ok(false)` when it is
/// neither. `NONSCALING` stores `0`, and the two exclude each other.
fn parse_bloom_growth(
    arguments: &[Bytes<'_>],
    index: usize,
    expansion: &mut Option<u32>,
) -> Result<bool, ProtocolError> {
    let name = arguments[index].as_ref();

    if name.eq_ignore_ascii_case(b"NONSCALING") {
        if expansion.is_some() {
            return Err(ProtocolError::Invalid("syntax error"));
        }

        *expansion = Some(0);
        return Ok(true);
    }

    if name.eq_ignore_ascii_case(b"EXPANSION") {
        let value = arguments
            .get(index + 1)
            .ok_or(ProtocolError::Invalid("syntax error"))?;
        let value = u32::try_from(parse_u64(value)?)
            .ok()
            .filter(|value| *value >= 1)
            .ok_or(ProtocolError::Invalid(
                "EXPANSION must be a positive integer",
            ))?;

        if expansion.is_some() {
            return Err(ProtocolError::Invalid("syntax error"));
        }

        *expansion = Some(value);
        return Ok(true);
    }

    Ok(false)
}

pub(in crate::protocol) fn cmd_bf_reserve<'a>(
    _name: &[u8],
    mut arguments: Vec<Bytes<'a>>,
) -> Result<Command<'a>, ProtocolError> {
    if arguments.len() < 3 {
        return Err(ProtocolError::Invalid(
            "BF.RESERVE expects key, error rate, and capacity",
        ));
    }

    let mut expansion = None;
    let mut index = 3;

    while index < arguments.len() {
        if !parse_bloom_growth(&arguments, index, &mut expansion)? {
            return Err(ProtocolError::Invalid("syntax error"));
        }

        index += if arguments[index].eq_ignore_ascii_case(b"EXPANSION") {
            2
        } else {
            1
        };
    }

    let capacity = parse_usize(&arguments[2])?;
    let error_rate = parse_f64(&arguments[1])?;

    arguments.truncate(1);

    Ok(Command::BloomReserve(
        arguments.pop().expect("length checked"),
        error_rate,
        capacity,
        expansion.unwrap_or(crate::store::BLOOM_DEFAULT_EXPANSION),
    ))
}

pub(in crate::protocol) fn cmd_bf_add<'a>(
    name: &[u8],
    mut arguments: Vec<Bytes<'a>>,
) -> Result<Command<'a>, ProtocolError> {
    match arguments.len() {
        2 => {
            let value = arguments.pop().expect("length checked");
            let key = arguments.pop().expect("length checked");

            if name.eq_ignore_ascii_case(b"BF.ADD") {
                Ok(Command::BloomAdd(key, value))
            } else {
                Ok(Command::BloomContains(key, value))
            }
        }

        _ => Err(ProtocolError::Invalid(
            "Bloom command expects key and value",
        )),
    }
}

/// `BF.MADD` and `BF.MEXISTS`: a key and at least one item.
pub(in crate::protocol) fn cmd_bf_many<'a>(
    name: &[u8],
    mut arguments: Vec<Bytes<'a>>,
) -> Result<Command<'a>, ProtocolError> {
    if arguments.len() < 2 {
        return Err(ProtocolError::Invalid(
            "Bloom command expects a key and at least one item",
        ));
    }

    let key = arguments.remove(0);

    if name.eq_ignore_ascii_case(b"BF.MADD") {
        Ok(Command::BloomAddMany(key, arguments))
    } else {
        Ok(Command::BloomContainsMany(key, arguments))
    }
}

pub(in crate::protocol) fn cmd_bf_insert<'a>(
    _name: &[u8],
    mut arguments: Vec<Bytes<'a>>,
) -> Result<Command<'a>, ProtocolError> {
    let mut error_rate = BLOOM_DEFAULT_ERROR_RATE;
    let mut capacity = BLOOM_DEFAULT_CAPACITY;
    let mut expansion = None;
    let mut no_create = false;
    let mut index = 1;
    let mut items_at = None;

    while index < arguments.len() {
        let name = arguments[index].as_ref();

        if name.eq_ignore_ascii_case(b"ITEMS") {
            items_at = Some(index + 1);
            break;
        }

        if name.eq_ignore_ascii_case(b"NOCREATE") {
            no_create = true;
            index += 1;
        } else if name.eq_ignore_ascii_case(b"CAPACITY") || name.eq_ignore_ascii_case(b"ERROR") {
            let value = arguments
                .get(index + 1)
                .ok_or(ProtocolError::Invalid("syntax error"))?;

            if name.eq_ignore_ascii_case(b"CAPACITY") {
                capacity = parse_usize(value)?;
            } else {
                error_rate = parse_f64(value)?;
            }

            index += 2;
        } else if parse_bloom_growth(&arguments, index, &mut expansion)? {
            index += if name.eq_ignore_ascii_case(b"EXPANSION") {
                2
            } else {
                1
            };
        } else {
            return Err(ProtocolError::Invalid("syntax error"));
        }
    }

    let Some(items_at) = items_at.filter(|at| *at < arguments.len()) else {
        return Err(ProtocolError::Invalid(
            "BF.INSERT expects ITEMS and at least one item",
        ));
    };

    let items = arguments.split_off(items_at);
    let key = arguments.remove(0);

    Ok(Command::BloomInsert {
        key,
        error_rate,
        capacity,
        expansion: expansion.unwrap_or(crate::store::BLOOM_DEFAULT_EXPANSION),
        no_create,
        items,
    })
}

pub(in crate::protocol) fn cmd_bf_info<'a>(
    name: &[u8],
    mut arguments: Vec<Bytes<'a>>,
) -> Result<Command<'a>, ProtocolError> {
    if name.eq_ignore_ascii_case(b"BF.CARD") {
        if arguments.len() != 1 {
            return Err(ProtocolError::Invalid("BF.CARD expects a key"));
        }

        return Ok(Command::BloomCard(arguments.remove(0)));
    }

    let field = match arguments.len() {
        1 => None,
        2 => {
            let field = arguments.pop().expect("length checked");

            Some(
                [
                    (&b"CAPACITY"[..], BloomInfoField::Capacity),
                    (b"SIZE", BloomInfoField::Size),
                    (b"FILTERS", BloomInfoField::Filters),
                    (b"ITEMS", BloomInfoField::Items),
                    (b"EXPANSION", BloomInfoField::Expansion),
                ]
                .into_iter()
                .find(|(candidate, _)| field.eq_ignore_ascii_case(candidate))
                .map(|(_, field)| field)
                .ok_or(ProtocolError::Invalid("unknown BF.INFO field"))?,
            )
        }
        _ => {
            return Err(ProtocolError::Invalid(
                "BF.INFO expects a key and an optional field",
            ));
        }
    };

    Ok(Command::BloomInfo(arguments.remove(0), field))
}

pub(in crate::protocol) fn cmd_xadd<'a>(
    _name: &[u8],
    arguments: Vec<Bytes<'a>>,
) -> Result<Command<'a>, ProtocolError> {
    parse_xadd(arguments)
}

pub(in crate::protocol) fn cmd_xrange<'a>(
    name: &[u8],
    arguments: Vec<Bytes<'a>>,
) -> Result<Command<'a>, ProtocolError> {
    parse_xrange(name.eq_ignore_ascii_case(b"XREVRANGE"), arguments)
}

pub(in crate::protocol) fn cmd_xread<'a>(
    _name: &[u8],
    arguments: Vec<Bytes<'a>>,
) -> Result<Command<'a>, ProtocolError> {
    parse_xread(arguments)
}

pub(in crate::protocol) fn cmd_xdel<'a>(
    _name: &[u8],
    arguments: Vec<Bytes<'a>>,
) -> Result<Command<'a>, ProtocolError> {
    parse_xdel(arguments)
}

pub(in crate::protocol) fn cmd_xtrim<'a>(
    _name: &[u8],
    arguments: Vec<Bytes<'a>>,
) -> Result<Command<'a>, ProtocolError> {
    parse_xtrim(arguments)
}

pub(in crate::protocol) fn cmd_xsetid<'a>(
    _name: &[u8],
    arguments: Vec<Bytes<'a>>,
) -> Result<Command<'a>, ProtocolError> {
    parse_xsetid(arguments)
}

pub(in crate::protocol) fn cmd_xinfo<'a>(
    _name: &[u8],
    arguments: Vec<Bytes<'a>>,
) -> Result<Command<'a>, ProtocolError> {
    parse_xinfo(arguments)
}

pub(in crate::protocol) fn cmd_xgroup<'a>(
    _name: &[u8],
    arguments: Vec<Bytes<'a>>,
) -> Result<Command<'a>, ProtocolError> {
    parse_xgroup(arguments)
}

pub(in crate::protocol) fn cmd_xreadgroup<'a>(
    _name: &[u8],
    arguments: Vec<Bytes<'a>>,
) -> Result<Command<'a>, ProtocolError> {
    parse_xreadgroup(arguments)
}

pub(in crate::protocol) fn cmd_xack<'a>(
    _name: &[u8],
    arguments: Vec<Bytes<'a>>,
) -> Result<Command<'a>, ProtocolError> {
    parse_xack(arguments)
}

pub(in crate::protocol) fn cmd_xpending<'a>(
    _name: &[u8],
    arguments: Vec<Bytes<'a>>,
) -> Result<Command<'a>, ProtocolError> {
    parse_xpending(arguments)
}

pub(in crate::protocol) fn cmd_xclaim<'a>(
    _name: &[u8],
    arguments: Vec<Bytes<'a>>,
) -> Result<Command<'a>, ProtocolError> {
    parse_xclaim(arguments)
}

pub(in crate::protocol) fn cmd_xautoclaim<'a>(
    _name: &[u8],
    arguments: Vec<Bytes<'a>>,
) -> Result<Command<'a>, ProtocolError> {
    parse_xautoclaim(arguments)
}

pub(in crate::protocol) fn cmd_multi<'a>(
    _name: &[u8],
    arguments: Vec<Bytes<'a>>,
) -> Result<Command<'a>, ProtocolError> {
    match arguments.len() {
        0 => Ok(Command::Multi),
        _ => Err(ProtocolError::Invalid("MULTI expects no arguments")),
    }
}

pub(in crate::protocol) fn cmd_exec<'a>(
    _name: &[u8],
    arguments: Vec<Bytes<'a>>,
) -> Result<Command<'a>, ProtocolError> {
    match arguments.len() {
        0 => Ok(Command::Exec),
        _ => Err(ProtocolError::Invalid("EXEC expects no arguments")),
    }
}

pub(in crate::protocol) fn cmd_discard<'a>(
    _name: &[u8],
    arguments: Vec<Bytes<'a>>,
) -> Result<Command<'a>, ProtocolError> {
    match arguments.len() {
        0 => Ok(Command::Discard),
        _ => Err(ProtocolError::Invalid("DISCARD expects no arguments")),
    }
}

pub(in crate::protocol) fn cmd_watch<'a>(
    _name: &[u8],
    arguments: Vec<Bytes<'a>>,
) -> Result<Command<'a>, ProtocolError> {
    if arguments.is_empty() {
        return Err(ProtocolError::Invalid("WATCH expects at least one key"));
    }

    Ok(Command::Watch(arguments))
}

pub(in crate::protocol) fn cmd_unwatch<'a>(
    _name: &[u8],
    arguments: Vec<Bytes<'a>>,
) -> Result<Command<'a>, ProtocolError> {
    match arguments.len() {
        0 => Ok(Command::Unwatch),
        _ => Err(ProtocolError::Invalid("UNWATCH expects no arguments")),
    }
}

pub(in crate::protocol) fn cmd_eval<'a>(
    name: &[u8],
    arguments: Vec<Bytes<'a>>,
) -> Result<Command<'a>, ProtocolError> {
    parse_eval(name.eq_ignore_ascii_case(b"EVALSHA"), arguments)
}

pub(in crate::protocol) fn cmd_fcall<'a>(
    _name: &[u8],
    arguments: Vec<Bytes<'a>>,
) -> Result<Command<'a>, ProtocolError> {
    parse_fcall(arguments)
}

pub(in crate::protocol) fn cmd_script<'a>(
    _name: &[u8],
    arguments: Vec<Bytes<'a>>,
) -> Result<Command<'a>, ProtocolError> {
    parse_script(arguments)
}

pub(in crate::protocol) fn cmd_function<'a>(
    _name: &[u8],
    arguments: Vec<Bytes<'a>>,
) -> Result<Command<'a>, ProtocolError> {
    parse_function(arguments)
}
