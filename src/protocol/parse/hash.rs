use super::*;

/// Largest absolute field deadline Redis accepts: 2^46 - 1 milliseconds.
pub(crate) const MAX_FIELD_EXPIRE_MS: u64 = (1 << 46) - 1;

pub(in crate::protocol) fn hash_expire_name(name: &[u8]) -> &'static str {
    if name.eq_ignore_ascii_case(b"HEXPIRE") {
        "hexpire"
    } else if name.eq_ignore_ascii_case(b"HPEXPIRE") {
        "hpexpire"
    } else if name.eq_ignore_ascii_case(b"HEXPIREAT") {
        "hexpireat"
    } else {
        "hpexpireat"
    }
}

/// Reads `FIELDS numfields field ...` starting at `index`. Every argument must be consumed.
pub(in crate::protocol) fn parse_field_block<'a>(
    arguments: &mut Vec<Bytes<'a>>,
    index: usize,
    wrong_arity: &'static str,
) -> Result<Vec<Bytes<'a>>, ProtocolError> {
    if arguments.len() < index + 3 {
        return Err(ProtocolError::Invalid(wrong_arity));
    }

    if !arguments[index].eq_ignore_ascii_case(b"FIELDS") {
        return Err(ProtocolError::Invalid("syntax error"));
    }

    let count = parse_usize(&arguments[index + 1])?;

    if count == 0 {
        return Err(ProtocolError::Invalid(wrong_arity));
    }

    let fields = arguments.split_off(index + 2);

    match fields.len().cmp(&count) {
        std::cmp::Ordering::Less => Err(ProtocolError::Invalid("wrong number of arguments")),
        std::cmp::Ordering::Greater => Err(ProtocolError::Invalid("unknown argument")),
        std::cmp::Ordering::Equal => Ok(fields),
    }
}

pub(in crate::protocol) fn parse_hash_expire<'a>(
    mut arguments: Vec<Bytes<'a>>,
    name: &[u8],
) -> Result<Command<'a>, ProtocolError> {
    let command = hash_expire_name(name);
    let wrong_arity = match command {
        "hexpire" => "wrong number of arguments for 'hexpire' command",
        "hpexpire" => "wrong number of arguments for 'hpexpire' command",
        "hexpireat" => "wrong number of arguments for 'hexpireat' command",
        _ => "wrong number of arguments for 'hpexpireat' command",
    };

    if arguments.len() < 5 {
        return Err(ProtocolError::Invalid(wrong_arity));
    }

    let raw = parse_i64(&arguments[1])?;

    if raw < 0 {
        return Err(ProtocolError::Invalid("invalid expire time, must be >= 0"));
    }

    let millis = if command == "hexpire" || command == "hexpireat" {
        (raw as u64).checked_mul(1_000)
    } else {
        Some(raw as u64)
    };

    let invalid = match command {
        "hexpire" => "invalid expire time in 'hexpire' command",
        "hpexpire" => "invalid expire time in 'hpexpire' command",
        "hexpireat" => "invalid expire time in 'hexpireat' command",
        _ => "invalid expire time in 'hpexpireat' command",
    };

    let millis = millis
        .filter(|millis| *millis <= MAX_FIELD_EXPIRE_MS)
        .ok_or(ProtocolError::Invalid(invalid))?;
    let time = if command.ends_with("at") {
        FieldExpireTime::AtMillis(millis)
    } else {
        FieldExpireTime::InMillis(millis)
    };

    let mut condition = FieldExpireCondition::Always;
    let mut index = 2;

    while index < arguments.len() && !arguments[index].eq_ignore_ascii_case(b"FIELDS") {
        let flag = arguments[index].as_ref();
        let parsed = if flag.eq_ignore_ascii_case(b"NX") {
            FieldExpireCondition::Nx
        } else if flag.eq_ignore_ascii_case(b"XX") {
            FieldExpireCondition::Xx
        } else if flag.eq_ignore_ascii_case(b"GT") {
            FieldExpireCondition::Gt
        } else if flag.eq_ignore_ascii_case(b"LT") {
            FieldExpireCondition::Lt
        } else {
            return Err(ProtocolError::Invalid("unknown argument"));
        };

        if condition != FieldExpireCondition::Always {
            return Err(ProtocolError::Invalid("Multiple condition flags specified"));
        }

        condition = parsed;
        index += 1;
    }

    let fields = parse_field_block(&mut arguments, index, wrong_arity)?;
    let key = arguments.into_iter().next().expect("length checked");

    Ok(Command::HashExpire {
        key,
        time,
        condition,
        fields,
        name: command,
    })
}

pub(in crate::protocol) fn parse_hash_field_read<'a>(
    mut arguments: Vec<Bytes<'a>>,
    name: &[u8],
) -> Result<Command<'a>, ProtocolError> {
    let (reply, wrong_arity) = if name.eq_ignore_ascii_case(b"HTTL") {
        (
            Some(FieldTtlReply::Seconds),
            "wrong number of arguments for 'httl' command",
        )
    } else if name.eq_ignore_ascii_case(b"HPTTL") {
        (
            Some(FieldTtlReply::Millis),
            "wrong number of arguments for 'hpttl' command",
        )
    } else if name.eq_ignore_ascii_case(b"HEXPIRETIME") {
        (
            Some(FieldTtlReply::AtSeconds),
            "wrong number of arguments for 'hexpiretime' command",
        )
    } else if name.eq_ignore_ascii_case(b"HPEXPIRETIME") {
        (
            Some(FieldTtlReply::AtMillis),
            "wrong number of arguments for 'hpexpiretime' command",
        )
    } else {
        (None, "wrong number of arguments for 'hpersist' command")
    };

    if arguments.len() < 4 {
        return Err(ProtocolError::Invalid(wrong_arity));
    }

    let fields = parse_field_block(&mut arguments, 1, wrong_arity)?;
    let key = arguments.into_iter().next().expect("length checked");

    Ok(match reply {
        Some(reply) => Command::HashTtl { key, reply, fields },
        None => Command::HashPersist { key, fields },
    })
}

pub(in crate::protocol) fn parse_hash_scan(
    arguments: Vec<Bytes<'_>>,
) -> Result<Command<'_>, ProtocolError> {
    let (key, cursor, pattern, count) =
        parse_scan_arguments(arguments, "HSCAN expects a key and a cursor")?;

    Ok(Command::HashScan {
        key,
        cursor,
        pattern,
        count,
    })
}

pub(in crate::protocol) fn parse_collection_scan(
    arguments: Vec<Bytes<'_>>,
    kind: CollectionScanKind,
) -> Result<Command<'_>, ProtocolError> {
    let usage = match kind {
        CollectionScanKind::Set => "SSCAN expects a key and a cursor",
        CollectionScanKind::SortedSet => "ZSCAN expects a key and a cursor",
    };
    let (key, cursor, pattern, count) = parse_scan_arguments(arguments, usage)?;

    Ok(Command::CollectionScan {
        kind,
        key,
        cursor,
        pattern,
        count,
    })
}

type ScanArguments<'a> = (Bytes<'a>, u64, Option<Bytes<'a>>, usize);

fn parse_scan_arguments<'a>(
    mut arguments: Vec<Bytes<'a>>,
    usage: &'static str,
) -> Result<ScanArguments<'a>, ProtocolError> {
    if arguments.len() < 2 {
        return Err(ProtocolError::Invalid(usage));
    }

    let key = arguments.remove(0);
    let cursor = parse_u64(&arguments.remove(0))?;
    let mut pattern = None;
    let mut count = 10;
    let mut index = 0;

    while index < arguments.len() {
        let name = arguments[index].as_ref();
        let Some(value) = arguments.get(index + 1) else {
            return Err(ProtocolError::Invalid("syntax error"));
        };

        if name.eq_ignore_ascii_case(b"MATCH") {
            pattern = Some(value.clone());
        } else if name.eq_ignore_ascii_case(b"COUNT") {
            let parsed = parse_i64(value)?;

            count = usize::try_from(parsed)
                .map_err(|_| ProtocolError::Invalid("value is not an integer"))?;
        } else {
            return Err(ProtocolError::Invalid("syntax error"));
        }

        index += 2;
    }

    Ok((key, cursor, pattern, count))
}
