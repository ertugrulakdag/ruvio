use super::*;

/// `XINFO STREAM`, `XINFO GROUPS`, and `XINFO CONSUMERS`.
pub(in crate::protocol) fn parse_xinfo(
    mut arguments: Vec<Bytes<'_>>,
) -> Result<Command<'_>, ProtocolError> {
    let Some(kind) = arguments.first().cloned() else {
        return Err(ProtocolError::Invalid(
            "XINFO expects STREAM, GROUPS, or CONSUMERS",
        ));
    };

    arguments.remove(0);

    if kind.eq_ignore_ascii_case(b"STREAM") || kind.eq_ignore_ascii_case(b"GROUPS") {
        if arguments.len() != 1 {
            return Err(ProtocolError::Invalid("XINFO expects a key"));
        }

        return Ok(Command::StreamInfo {
            key: arguments.remove(0),
            group: None,
            kind: if kind.eq_ignore_ascii_case(b"STREAM") {
                StreamInfoKind::Stream
            } else {
                StreamInfoKind::Groups
            },
        });
    }

    if kind.eq_ignore_ascii_case(b"CONSUMERS") {
        if arguments.len() != 2 {
            return Err(ProtocolError::Invalid(
                "XINFO CONSUMERS expects a key and a group",
            ));
        }

        let group = arguments.pop().expect("length checked");
        let key = arguments.pop().expect("length checked");

        if group.as_ref().is_empty() {
            return Err(ProtocolError::Invalid(
                "XINFO CONSUMERS group must not be empty",
            ));
        }

        return Ok(Command::StreamInfo {
            key,
            group: Some(group),
            kind: StreamInfoKind::Consumers,
        });
    }

    Err(ProtocolError::Invalid(
        "XINFO expects STREAM, GROUPS, or CONSUMERS",
    ))
}

pub(in crate::protocol) fn parse_stream_add_id(bytes: &[u8]) -> Result<StreamAddId, ProtocolError> {
    if bytes == b"*" {
        return Ok(StreamAddId::Auto);
    }

    if let Some(millis) = bytes.strip_suffix(b"-*") {
        if millis.is_empty() || !millis.iter().all(u8::is_ascii_digit) {
            return Err(ProtocolError::Invalid("XADD id must be *, ms-*, or ms-seq"));
        }

        let millis = std::str::from_utf8(millis)
            .ok()
            .and_then(|text| text.parse().ok())
            .ok_or(ProtocolError::Invalid("XADD id must be *, ms-*, or ms-seq"))?;

        return Ok(StreamAddId::AutoSequence(millis));
    }

    let (millis, sequence) = crate::stream::parse_id(bytes)
        .ok_or(ProtocolError::Invalid("XADD id must be *, ms-*, or ms-seq"))?;

    Ok(StreamAddId::Explicit(millis, sequence))
}

pub(in crate::protocol) fn parse_xadd(
    mut arguments: Vec<Bytes<'_>>,
) -> Result<Command<'_>, ProtocolError> {
    if arguments.len() < 4 {
        return Err(ProtocolError::Invalid(
            "XADD expects a key, id, and at least one field",
        ));
    }

    let key = arguments.remove(0);
    let mut max_len = None;
    let mut min_id = None;
    let mut delay_ms = None;
    let mut nomkstream = false;

    while let Some(argument) = arguments.first() {
        if argument.as_ref().eq_ignore_ascii_case(b"DELAY") {
            if delay_ms.is_some() {
                return Err(ProtocolError::Invalid("XADD DELAY was repeated"));
            }

            arguments.remove(0);
            let raw = arguments
                .first()
                .ok_or(ProtocolError::Invalid("XADD DELAY expects milliseconds"))?;

            delay_ms = Some(
                parse_u64(raw.as_ref())
                    .map_err(|_| ProtocolError::Invalid("XADD DELAY expects milliseconds"))?,
            );

            arguments.remove(0);

            continue;
        }

        if argument.as_ref().eq_ignore_ascii_case(b"NOMKSTREAM") {
            if nomkstream {
                return Err(ProtocolError::Invalid("XADD NOMKSTREAM was repeated"));
            }

            nomkstream = true;
            arguments.remove(0);

            continue;
        }

        if argument.as_ref().eq_ignore_ascii_case(b"MAXLEN") {
            if max_len.is_some() {
                return Err(ProtocolError::Invalid("XADD MAXLEN was repeated"));
            }

            if min_id.is_some() {
                return Err(ProtocolError::Invalid(
                    "XADD MAXLEN and MINID cannot be used together",
                ));
            }

            arguments.remove(0);

            if arguments
                .first()
                .is_some_and(|argument| argument.as_ref() == b"~")
            {
                arguments.remove(0);
            }

            let count = arguments
                .first()
                .ok_or(ProtocolError::Invalid("XADD MAXLEN expects a count"))?;

            max_len = Some(parse_usize(count)?);
            arguments.remove(0);

            continue;
        }

        if argument.as_ref().eq_ignore_ascii_case(b"MINID") {
            if min_id.is_some() {
                return Err(ProtocolError::Invalid("XADD MINID was repeated"));
            }

            if max_len.is_some() {
                return Err(ProtocolError::Invalid(
                    "XADD MAXLEN and MINID cannot be used together",
                ));
            }

            arguments.remove(0);
            min_id = Some(parse_min_id(&mut arguments)?);

            continue;
        }

        break;
    }

    let id_bytes = arguments
        .first()
        .ok_or(ProtocolError::Invalid("XADD expects an id"))?;
    let id = parse_stream_add_id(id_bytes.as_ref())?;

    arguments.remove(0);

    if arguments.is_empty() || !arguments.len().is_multiple_of(2) {
        return Err(ProtocolError::Invalid("XADD expects field and value pairs"));
    }

    let mut fields = Vec::with_capacity(arguments.len() / 2);

    while arguments.len() >= 2 {
        let field = arguments.remove(0);
        let value = arguments.remove(0);

        fields.push((field, value));
    }

    Ok(Command::StreamAdd {
        key,
        id,
        max_len,
        min_id,
        delay_ms,
        nomkstream,
        fields,
    })
}

pub(in crate::protocol) fn parse_xrange(
    reverse: bool,
    mut arguments: Vec<Bytes<'_>>,
) -> Result<Command<'_>, ProtocolError> {
    if arguments.len() < 3 {
        return Err(ProtocolError::Invalid(
            "XRANGE expects a key, start, and end",
        ));
    }

    let key = arguments.remove(0);
    let first = parse_stream_bound(&arguments.remove(0))?;
    let second = parse_stream_bound(&arguments.remove(0))?;
    let (start, end) = if reverse {
        (second, first)
    } else {
        (first, second)
    };

    let count = parse_stream_count(&arguments)?;

    Ok(Command::StreamRange {
        key,
        start,
        end,
        count,
        reverse,
    })
}

pub(in crate::protocol) fn parse_xread(
    mut arguments: Vec<Bytes<'_>>,
) -> Result<Command<'_>, ProtocolError> {
    let mut count = None;
    let mut block = None;
    let mut saw_streams = false;

    while let Some(argument) = arguments.first() {
        if argument.as_ref().eq_ignore_ascii_case(b"BLOCK") {
            arguments.remove(0);
            let raw = arguments
                .first()
                .ok_or(ProtocolError::Invalid("XREAD BLOCK expects milliseconds"))?;

            block = Some(parse_block_millis(raw.as_ref())?);
            arguments.remove(0);

            continue;
        }

        if argument.as_ref().eq_ignore_ascii_case(b"COUNT") {
            arguments.remove(0);
            let raw = arguments.first().ok_or(ProtocolError::Invalid(
                "XREAD COUNT expects a positive integer",
            ))?;

            count = Some(parse_positive_count(raw)?);
            arguments.remove(0);

            continue;
        }

        if argument.as_ref().eq_ignore_ascii_case(b"STREAMS") {
            arguments.remove(0);
            saw_streams = true;

            break;
        }

        return Err(ProtocolError::Invalid("XREAD expects COUNT and STREAMS"));
    }

    if !saw_streams || arguments.is_empty() || !arguments.len().is_multiple_of(2) {
        return Err(ProtocolError::Invalid(
            "XREAD expects STREAMS followed by key and id pairs",
        ));
    }

    let half = arguments.len() / 2;
    let ids = arguments.split_off(half);
    let mut keys = Vec::with_capacity(half);

    for (key, id) in arguments.into_iter().zip(ids) {
        keys.push((key, parse_stream_cursor(&id)?));
    }

    Ok(Command::StreamRead { count, block, keys })
}

pub(in crate::protocol) fn parse_block_millis(bytes: &[u8]) -> Result<Option<u64>, ProtocolError> {
    let millis = parse_u64(bytes)?;

    if millis == 0 {
        return Ok(None);
    }

    Ok(Some(millis))
}

pub(in crate::protocol) fn parse_xdel(
    mut arguments: Vec<Bytes<'_>>,
) -> Result<Command<'_>, ProtocolError> {
    if arguments.len() < 2 {
        return Err(ProtocolError::Invalid("XDEL expects a key and an id"));
    }

    let key = arguments.remove(0);
    let mut ids = Vec::with_capacity(arguments.len());

    for id in arguments {
        let (millis, sequence) = crate::stream::parse_id(id.as_ref()).ok_or(
            ProtocolError::Invalid("XDEL expects ids of the form ms-seq"),
        )?;

        ids.push((millis, sequence));
    }

    Ok(Command::StreamDelete { key, ids })
}

pub(in crate::protocol) fn parse_stream_count(
    arguments: &[Bytes<'_>],
) -> Result<Option<usize>, ProtocolError> {
    if arguments.is_empty() {
        return Ok(None);
    }

    if arguments.len() == 2 && arguments[0].as_ref().eq_ignore_ascii_case(b"COUNT") {
        return Ok(Some(parse_positive_count(&arguments[1])?));
    }

    Err(ProtocolError::Invalid("stream range only supports COUNT"))
}

pub(in crate::protocol) fn parse_positive_count(bytes: &Bytes<'_>) -> Result<usize, ProtocolError> {
    let count = parse_usize(bytes.as_ref())?;

    if count == 0 {
        return Err(ProtocolError::Invalid("COUNT must be positive"));
    }

    Ok(count)
}

pub(in crate::protocol) fn parse_stream_cursor(
    bytes: &Bytes<'_>,
) -> Result<crate::stream::StreamCursor, ProtocolError> {
    if bytes.as_ref() == b"$" {
        return Ok(crate::stream::StreamCursor::End);
    }

    if bytes.as_ref() == b"0" {
        return Ok(crate::stream::StreamCursor::After(0, 0));
    }

    let (millis, sequence) = crate::stream::parse_id(bytes.as_ref())
        .ok_or(ProtocolError::Invalid("XREAD id must be $, 0, or ms-seq"))?;

    Ok(crate::stream::StreamCursor::After(millis, sequence))
}

pub(in crate::protocol) fn parse_min_id(
    arguments: &mut Vec<Bytes<'_>>,
) -> Result<(u64, u64), ProtocolError> {
    if arguments
        .first()
        .is_some_and(|argument| argument.as_ref() == b"~" || argument.as_ref() == b"=")
    {
        arguments.remove(0);
    }

    let raw = arguments
        .first()
        .ok_or(ProtocolError::Invalid("MINID expects an id"))?;

    if raw.as_ref().eq_ignore_ascii_case(b"LIMIT") {
        return Err(ProtocolError::Invalid("LIMIT is not supported"));
    }

    let id = crate::stream::parse_id(raw.as_ref())
        .ok_or(ProtocolError::Invalid("MINID id must be ms-seq"))?;

    arguments.remove(0);

    if arguments
        .first()
        .is_some_and(|argument| argument.as_ref().eq_ignore_ascii_case(b"LIMIT"))
    {
        return Err(ProtocolError::Invalid("LIMIT is not supported"));
    }

    Ok(id)
}

pub(in crate::protocol) fn parse_xsetid(
    mut arguments: Vec<Bytes<'_>>,
) -> Result<Command<'_>, ProtocolError> {
    if arguments.len() != 2 {
        return Err(ProtocolError::Invalid("XSETID expects a key and an id"));
    }

    let key = arguments.remove(0);
    let id = crate::stream::parse_id(arguments.remove(0).as_ref())
        .ok_or(ProtocolError::Invalid("XSETID id must be ms-seq"))?;

    Ok(Command::StreamSetId { key, id })
}

pub(in crate::protocol) fn parse_xtrim(
    mut arguments: Vec<Bytes<'_>>,
) -> Result<Command<'_>, ProtocolError> {
    if arguments.len() < 3 {
        return Err(ProtocolError::Invalid(
            "XTRIM expects a key and MAXLEN or MINID",
        ));
    }

    let key = arguments.remove(0);
    let strategy = arguments
        .first()
        .ok_or(ProtocolError::Invalid("XTRIM expects MAXLEN or MINID"))?;

    if strategy.as_ref().eq_ignore_ascii_case(b"MINID") {
        arguments.remove(0);
        let id = parse_min_id(&mut arguments)?;

        if !arguments.is_empty() {
            return Err(ProtocolError::Invalid("XTRIM MINID expects one id"));
        }

        return Ok(Command::StreamTrimMinId { key, id });
    }

    if !strategy.as_ref().eq_ignore_ascii_case(b"MAXLEN") {
        return Err(ProtocolError::Invalid(
            "XTRIM only supports MAXLEN and MINID",
        ));
    }

    arguments.remove(0);

    if arguments
        .first()
        .is_some_and(|argument| argument.as_ref() == b"~" || argument.as_ref() == b"=")
    {
        arguments.remove(0);
    }

    if arguments.len() != 1 {
        return Err(ProtocolError::Invalid("XTRIM expects MAXLEN and a count"));
    }

    Ok(Command::StreamTrim {
        key,
        max_len: parse_usize(&arguments.pop().expect("length checked"))?,
    })
}

pub(in crate::protocol) fn parse_group_start(
    bytes: &Bytes<'_>,
) -> Result<crate::stream::GroupStart, ProtocolError> {
    if bytes.as_ref() == b"$" {
        return Ok(crate::stream::GroupStart::End);
    }

    if bytes.as_ref() == b"0" {
        return Ok(crate::stream::GroupStart::Id(0, 0));
    }

    let (millis, sequence) = crate::stream::parse_id(bytes.as_ref())
        .ok_or(ProtocolError::Invalid("XGROUP id must be $, 0, or ms-seq"))?;

    Ok(crate::stream::GroupStart::Id(millis, sequence))
}

pub(in crate::protocol) fn parse_xgroup(
    mut arguments: Vec<Bytes<'_>>,
) -> Result<Command<'_>, ProtocolError> {
    let Some(subcommand) = arguments.first().cloned() else {
        return Err(ProtocolError::Invalid(
            "XGROUP expects CREATE, CREATECONSUMER, DESTROY, SETID, or DELCONSUMER",
        ));
    };

    arguments.remove(0);

    if subcommand.as_ref().eq_ignore_ascii_case(b"DESTROY") {
        return parse_xgroup_destroy(arguments);
    }

    if subcommand.as_ref().eq_ignore_ascii_case(b"SETID") {
        return parse_xgroup_setid(arguments);
    }

    if subcommand.as_ref().eq_ignore_ascii_case(b"DELCONSUMER") {
        return parse_xgroup_delconsumer(arguments);
    }

    if subcommand.as_ref().eq_ignore_ascii_case(b"CREATECONSUMER") {
        return parse_xgroup_createconsumer(arguments);
    }

    if !subcommand.as_ref().eq_ignore_ascii_case(b"CREATE") {
        return Err(ProtocolError::Invalid(
            "XGROUP expects CREATE, CREATECONSUMER, DESTROY, SETID, or DELCONSUMER",
        ));
    }

    if arguments.len() < 3 || arguments.len() > 4 {
        return Err(ProtocolError::Invalid(
            "XGROUP CREATE expects a key, group, id, and optional MKSTREAM",
        ));
    }

    let key = arguments.remove(0);
    let group = arguments.remove(0);
    let id = arguments.remove(0);

    if group.as_ref().is_empty() {
        return Err(ProtocolError::Invalid(
            "XGROUP group name must not be empty",
        ));
    }

    let mkstream = if let Some(flag) = arguments.first() {
        if flag.as_ref().eq_ignore_ascii_case(b"MKSTREAM") {
            true
        } else {
            return Err(ProtocolError::Invalid(
                "XGROUP CREATE only supports MKSTREAM",
            ));
        }
    } else {
        false
    };

    Ok(Command::StreamGroupCreate {
        key,
        group,
        start: parse_group_start(&id)?,
        mkstream,
    })
}

pub(in crate::protocol) fn parse_xreadgroup(
    mut arguments: Vec<Bytes<'_>>,
) -> Result<Command<'_>, ProtocolError> {
    if arguments
        .first()
        .is_none_or(|argument| !argument.as_ref().eq_ignore_ascii_case(b"GROUP"))
    {
        return Err(ProtocolError::Invalid("XREADGROUP expects GROUP"));
    }

    arguments.remove(0);

    if arguments.len() < 4 {
        return Err(ProtocolError::Invalid(
            "XREADGROUP expects a group, consumer, and one stream",
        ));
    }

    let group = arguments.remove(0);
    let consumer = arguments.remove(0);

    if group.as_ref().is_empty() || consumer.as_ref().is_empty() {
        return Err(ProtocolError::Invalid(
            "XREADGROUP group and consumer must not be empty",
        ));
    }

    let mut count = None;
    let mut block = None;
    let mut noack = false;

    while arguments
        .first()
        .is_some_and(|argument| !argument.as_ref().eq_ignore_ascii_case(b"STREAMS"))
    {
        let option = arguments.remove(0);

        if option.as_ref().eq_ignore_ascii_case(b"COUNT") {
            let Some(value) = arguments.first() else {
                return Err(ProtocolError::Invalid("XREADGROUP COUNT expects a number"));
            };

            count = Some(parse_positive_count(value)?);
            arguments.remove(0);
        } else if option.as_ref().eq_ignore_ascii_case(b"BLOCK") {
            let Some(value) = arguments.first() else {
                return Err(ProtocolError::Invalid(
                    "XREADGROUP BLOCK expects milliseconds",
                ));
            };

            block = Some(parse_block_millis(value)?);
            arguments.remove(0);
        } else if option.as_ref().eq_ignore_ascii_case(b"NOACK") {
            noack = true;
        } else {
            return Err(ProtocolError::Invalid(
                "XREADGROUP only supports COUNT, BLOCK, and NOACK before STREAMS",
            ));
        }
    }

    if arguments
        .first()
        .is_none_or(|argument| !argument.as_ref().eq_ignore_ascii_case(b"STREAMS"))
    {
        return Err(ProtocolError::Invalid("XREADGROUP expects STREAMS"));
    }

    arguments.remove(0);

    if arguments.len() != 2 {
        return Err(ProtocolError::Invalid("XREADGROUP reads one stream"));
    }

    let key = arguments.remove(0);
    let id = arguments.remove(0);

    if id.as_ref() != b">" {
        return Err(ProtocolError::Invalid(
            "XREADGROUP only reads new messages with >",
        ));
    }

    Ok(Command::StreamGroupRead {
        key,
        group,
        consumer,
        count,
        block,
        noack,
    })
}

pub(in crate::protocol) fn parse_xgroup_destroy(
    mut arguments: Vec<Bytes<'_>>,
) -> Result<Command<'_>, ProtocolError> {
    if arguments.len() != 2 {
        return Err(ProtocolError::Invalid(
            "XGROUP DESTROY expects a key and a group",
        ));
    }

    let key = arguments.remove(0);
    let group = arguments.remove(0);

    if group.as_ref().is_empty() {
        return Err(ProtocolError::Invalid(
            "XGROUP group name must not be empty",
        ));
    }

    Ok(Command::StreamGroupDestroy { key, group })
}

pub(in crate::protocol) fn parse_xgroup_setid(
    mut arguments: Vec<Bytes<'_>>,
) -> Result<Command<'_>, ProtocolError> {
    if arguments.len() != 3 {
        return Err(ProtocolError::Invalid(
            "XGROUP SETID expects a key, group, and id",
        ));
    }

    let key = arguments.remove(0);
    let group = arguments.remove(0);
    let id = arguments.remove(0);

    if group.as_ref().is_empty() {
        return Err(ProtocolError::Invalid(
            "XGROUP group name must not be empty",
        ));
    }

    Ok(Command::StreamGroupSetId {
        key,
        group,
        start: parse_group_start(&id)?,
    })
}

pub(in crate::protocol) fn parse_xgroup_createconsumer(
    mut arguments: Vec<Bytes<'_>>,
) -> Result<Command<'_>, ProtocolError> {
    if arguments.len() != 3 {
        return Err(ProtocolError::Invalid(
            "XGROUP CREATECONSUMER expects a key, group, and consumer",
        ));
    }

    let key = arguments.remove(0);
    let group = arguments.remove(0);
    let consumer = arguments.remove(0);

    if group.as_ref().is_empty() || consumer.as_ref().is_empty() {
        return Err(ProtocolError::Invalid(
            "XGROUP group and consumer must not be empty",
        ));
    }

    Ok(Command::StreamGroupCreateConsumer {
        key,
        group,
        consumer,
    })
}

pub(in crate::protocol) fn parse_xgroup_delconsumer(
    mut arguments: Vec<Bytes<'_>>,
) -> Result<Command<'_>, ProtocolError> {
    if arguments.len() != 3 {
        return Err(ProtocolError::Invalid(
            "XGROUP DELCONSUMER expects a key, group, and consumer",
        ));
    }

    let key = arguments.remove(0);
    let group = arguments.remove(0);
    let consumer = arguments.remove(0);

    if group.as_ref().is_empty() || consumer.as_ref().is_empty() {
        return Err(ProtocolError::Invalid(
            "XGROUP group and consumer must not be empty",
        ));
    }

    Ok(Command::StreamGroupDelConsumer {
        key,
        group,
        consumer,
    })
}

pub(in crate::protocol) fn parse_xack(
    mut arguments: Vec<Bytes<'_>>,
) -> Result<Command<'_>, ProtocolError> {
    if arguments.len() < 3 {
        return Err(ProtocolError::Invalid(
            "XACK expects a key, group, and at least one id",
        ));
    }

    let key = arguments.remove(0);
    let group = arguments.remove(0);

    if group.as_ref().is_empty() {
        return Err(ProtocolError::Invalid("XACK group name must not be empty"));
    }

    let mut ids = Vec::with_capacity(arguments.len());

    for id in arguments {
        let (millis, sequence) = crate::stream::parse_id(id.as_ref()).ok_or(
            ProtocolError::Invalid("XACK expects ids of the form ms-seq"),
        )?;

        ids.push((millis, sequence));
    }

    Ok(Command::StreamAck { key, group, ids })
}

pub(in crate::protocol) fn parse_xpending(
    mut arguments: Vec<Bytes<'_>>,
) -> Result<Command<'_>, ProtocolError> {
    if arguments.len() < 2 {
        return Err(ProtocolError::Invalid("XPENDING expects a key and a group"));
    }

    let key = arguments.remove(0);
    let group = arguments.remove(0);

    if group.as_ref().is_empty() {
        return Err(ProtocolError::Invalid(
            "XPENDING group name must not be empty",
        ));
    }

    if arguments.is_empty() {
        return Ok(Command::StreamPending { key, group });
    }

    let mut min_idle = None;

    if arguments[0].as_ref().eq_ignore_ascii_case(b"IDLE") {
        arguments.remove(0);

        if arguments.is_empty() {
            return Err(ProtocolError::Invalid(
                "XPENDING IDLE expects a millisecond count",
            ));
        }

        min_idle = Some(parse_u64(arguments.remove(0).as_ref())?);
    }

    if !(3..=4).contains(&arguments.len()) {
        return Err(ProtocolError::Invalid(
            "XPENDING expects a start id, an end id, a count, and an optional consumer",
        ));
    }

    let start = parse_stream_bound(&arguments.remove(0))?;
    let end = parse_stream_bound(&arguments.remove(0))?;
    let count = parse_usize(arguments.remove(0).as_ref())?;
    let consumer = if arguments.is_empty() {
        None
    } else {
        let consumer = arguments.remove(0);

        if consumer.as_ref().is_empty() {
            return Err(ProtocolError::Invalid(
                "XPENDING consumer must not be empty",
            ));
        }

        Some(consumer)
    };

    Ok(Command::StreamPendingRange {
        key,
        group,
        min_idle,
        start,
        end,
        count,
        consumer,
    })
}

pub(in crate::protocol) fn parse_xautoclaim(
    mut arguments: Vec<Bytes<'_>>,
) -> Result<Command<'_>, ProtocolError> {
    if arguments.len() < 5 {
        return Err(ProtocolError::Invalid(
            "XAUTOCLAIM expects a key, group, consumer, min-idle time, and start id",
        ));
    }

    let key = arguments.remove(0);
    let group = arguments.remove(0);
    let consumer = arguments.remove(0);
    let min_idle = parse_u64(arguments.remove(0).as_ref())?;
    let start = crate::stream::parse_id(arguments.remove(0).as_ref())
        .ok_or(ProtocolError::Invalid("XAUTOCLAIM start id must be ms-seq"))?;

    if group.as_ref().is_empty() || consumer.as_ref().is_empty() {
        return Err(ProtocolError::Invalid(
            "XAUTOCLAIM group and consumer must not be empty",
        ));
    }

    let mut count = None;
    let mut just_id = false;

    while !arguments.is_empty() {
        let name = arguments.remove(0);

        if name.as_ref().eq_ignore_ascii_case(b"COUNT") {
            if count.is_some() || arguments.is_empty() {
                return Err(ProtocolError::Invalid("XAUTOCLAIM COUNT needs one integer"));
            }

            let value = parse_u64(arguments.remove(0).as_ref())?;

            if value == 0 {
                return Err(ProtocolError::Invalid(
                    "XAUTOCLAIM COUNT must be at least 1",
                ));
            }

            count = Some(value as usize);
            continue;
        }

        if name.as_ref().eq_ignore_ascii_case(b"JUSTID") {
            if just_id {
                return Err(ProtocolError::Invalid("XAUTOCLAIM JUSTID was repeated"));
            }

            just_id = true;
            continue;
        }

        return Err(ProtocolError::Invalid("syntax error"));
    }

    Ok(Command::StreamAutoclaim {
        key,
        group,
        consumer,
        min_idle,
        start,
        count,
        just_id,
    })
}

pub(in crate::protocol) fn parse_xclaim(
    mut arguments: Vec<Bytes<'_>>,
) -> Result<Command<'_>, ProtocolError> {
    if arguments.len() < 5 {
        return Err(ProtocolError::Invalid(
            "XCLAIM expects a key, group, consumer, min-idle time, and at least one id",
        ));
    }

    let key = arguments.remove(0);
    let group = arguments.remove(0);
    let consumer = arguments.remove(0);
    let min_idle = parse_u64(arguments.remove(0).as_ref())?;

    if group.as_ref().is_empty() || consumer.as_ref().is_empty() {
        return Err(ProtocolError::Invalid(
            "XCLAIM group and consumer must not be empty",
        ));
    }

    let mut ids = Vec::new();
    let mut options = ClaimOptions::plain();
    let mut parsing_options = false;

    while !arguments.is_empty() {
        let token = arguments[0].as_ref();

        if !parsing_options && !xclaim_option(token) {
            let (millis, sequence) = crate::stream::parse_id(token).ok_or(
                ProtocolError::Invalid("XCLAIM expects ids of the form ms-seq"),
            )?;

            ids.push((millis, sequence));
            arguments.remove(0);
            continue;
        }

        parsing_options = true;
        let name = arguments.remove(0);

        if name.as_ref().eq_ignore_ascii_case(b"IDLE") {
            if options.idle.is_some() {
                return Err(ProtocolError::Invalid("XCLAIM IDLE was repeated"));
            }

            if options.time.is_some() {
                return Err(ProtocolError::Invalid(
                    "XCLAIM accepts only one of IDLE or TIME",
                ));
            }

            options.idle = Some(parse_xclaim_u64(&mut arguments, b"IDLE")?);
        } else if name.as_ref().eq_ignore_ascii_case(b"TIME") {
            if options.time.is_some() {
                return Err(ProtocolError::Invalid("XCLAIM TIME was repeated"));
            }

            if options.idle.is_some() {
                return Err(ProtocolError::Invalid(
                    "XCLAIM accepts only one of IDLE or TIME",
                ));
            }

            options.time = Some(parse_xclaim_u64(&mut arguments, b"TIME")?);
        } else if name.as_ref().eq_ignore_ascii_case(b"RETRYCOUNT") {
            if options.retry_count.is_some() {
                return Err(ProtocolError::Invalid("XCLAIM RETRYCOUNT was repeated"));
            }

            options.retry_count = Some(parse_xclaim_u64(&mut arguments, b"RETRYCOUNT")?);
        } else if name.as_ref().eq_ignore_ascii_case(b"FORCE") {
            if options.force {
                return Err(ProtocolError::Invalid("XCLAIM FORCE was repeated"));
            }

            options.force = true;
        } else if name.as_ref().eq_ignore_ascii_case(b"JUSTID") {
            if options.just_id {
                return Err(ProtocolError::Invalid("XCLAIM JUSTID was repeated"));
            }

            options.just_id = true;
        } else if name.as_ref().eq_ignore_ascii_case(b"LASTID") {
            if options.last_id.is_some() {
                return Err(ProtocolError::Invalid("XCLAIM LASTID was repeated"));
            }

            let Some(id) = arguments.first() else {
                return Err(ProtocolError::Invalid(
                    "XCLAIM LASTID expects an id of the form ms-seq",
                ));
            };

            let parsed = crate::stream::parse_id(id.as_ref()).ok_or(ProtocolError::Invalid(
                "XCLAIM LASTID expects an id of the form ms-seq",
            ))?;

            arguments.remove(0);
            options.last_id = Some(parsed);
        } else {
            return Err(ProtocolError::Invalid("XCLAIM option is not supported"));
        }
    }

    if ids.is_empty() {
        return Err(ProtocolError::Invalid(
            "XCLAIM expects a key, group, consumer, min-idle time, and at least one id",
        ));
    }

    Ok(Command::StreamClaimPending {
        key,
        group,
        consumer,
        min_idle,
        ids,
        options,
    })
}

pub(in crate::protocol) fn xclaim_option(token: &[u8]) -> bool {
    token.eq_ignore_ascii_case(b"IDLE")
        || token.eq_ignore_ascii_case(b"TIME")
        || token.eq_ignore_ascii_case(b"RETRYCOUNT")
        || token.eq_ignore_ascii_case(b"FORCE")
        || token.eq_ignore_ascii_case(b"JUSTID")
        || token.eq_ignore_ascii_case(b"LASTID")
}

pub(in crate::protocol) fn parse_xclaim_u64(
    arguments: &mut Vec<Bytes<'_>>,
    name: &[u8],
) -> Result<u64, ProtocolError> {
    let Some(value) = arguments.first() else {
        return Err(ProtocolError::Invalid(match name {
            b"IDLE" => "XCLAIM IDLE expects a millisecond count",
            b"TIME" => "XCLAIM TIME expects a unix millisecond time",
            _ => "XCLAIM RETRYCOUNT expects a count",
        }));
    };

    let parsed = parse_u64(value.as_ref())?;

    arguments.remove(0);
    Ok(parsed)
}

pub(in crate::protocol) fn parse_stream_bound(
    bytes: &Bytes<'_>,
) -> Result<crate::stream::StreamBound, ProtocolError> {
    if bytes.as_ref() == b"-" {
        return Ok(crate::stream::StreamBound::Min);
    }

    if bytes.as_ref() == b"+" {
        return Ok(crate::stream::StreamBound::Max);
    }

    let (millis, sequence) = crate::stream::parse_id(bytes.as_ref())
        .ok_or(ProtocolError::Invalid("stream id must be -, +, or ms-seq"))?;

    Ok(crate::stream::StreamBound::Id(millis, sequence))
}
