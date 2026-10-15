use super::*;

pub(in crate::protocol) fn parse_sorted_set_range(
    mut arguments: Vec<Bytes<'_>>,
    legacy_reverse: bool,
) -> Result<Command<'_>, ProtocolError> {
    if arguments.len() < 3 {
        return Err(ProtocolError::Invalid(
            "ZRANGE expects start, stop, and optional WITHSCORES",
        ));
    }

    let key = arguments.remove(0);
    let min_arg = arguments.remove(0);
    let max_arg = arguments.remove(0);
    let options = parse_range_options(&arguments, legacy_reverse)?;

    if legacy_reverse && (options.by_score || options.by_lex || options.limit.is_some()) {
        return Err(ProtocolError::Invalid("syntax error"));
    }

    if options.limit.is_some() && !options.by_score && !options.by_lex {
        return Err(ProtocolError::Invalid(
            "syntax error, LIMIT is only supported in combination with either BYSCORE or BYLEX",
        ));
    }

    // With REV, Redis takes the high bound first for BYSCORE and BYLEX.
    let (min_arg, max_arg) = if options.reverse && (options.by_score || options.by_lex) {
        (max_arg, min_arg)
    } else {
        (min_arg, max_arg)
    };

    if options.by_lex {
        return Ok(Command::SortedSetRangeByLex {
            key,
            min: parse_lex_bound(min_arg)?,
            max: parse_lex_bound(max_arg)?,
            with_scores: options.with_scores,
            reverse: options.reverse,
            limit: options.limit,
            invocation: LexInvocation::ZRange,
        });
    }

    if options.by_score {
        return Ok(Command::SortedSetRangeByScore {
            key,
            min: parse_score_bound(&min_arg)?,
            max: parse_score_bound(&max_arg)?,
            with_scores: options.with_scores,
            reverse: options.reverse,
            limit: options.limit,
            via_zrange: true,
        });
    }

    Ok(Command::SortedSetRange {
        key,
        start: parse_isize(&min_arg)?,
        stop: parse_isize(&max_arg)?,
        with_scores: options.with_scores,
        reverse: options.reverse,
    })
}

pub(in crate::protocol) fn parse_sorted_set_range_by_score(
    mut arguments: Vec<Bytes<'_>>,
) -> Result<Command<'_>, ProtocolError> {
    if arguments.len() < 3 {
        return Err(ProtocolError::Invalid(
            "ZRANGEBYSCORE expects min, max, and optional WITHSCORES",
        ));
    }

    let key = arguments.remove(0);
    let min = parse_score_bound(&arguments.remove(0))?;
    let max = parse_score_bound(&arguments.remove(0))?;
    let options = parse_range_options(&arguments, false)?;

    if options.by_score || options.by_lex || options.reverse {
        return Err(ProtocolError::Invalid("syntax error"));
    }

    Ok(Command::SortedSetRangeByScore {
        key,
        min,
        max,
        with_scores: options.with_scores,
        reverse: false,
        limit: options.limit,
        via_zrange: false,
    })
}

pub(in crate::protocol) fn parse_sorted_set_range_by_lex(
    mut arguments: Vec<Bytes<'_>>,
    reverse: bool,
) -> Result<Command<'_>, ProtocolError> {
    if arguments.len() < 3 {
        return Err(ProtocolError::Invalid(
            "ZRANGEBYLEX expects min, max, and optional LIMIT",
        ));
    }

    let key = arguments.remove(0);
    let first = parse_lex_bound(arguments.remove(0))?;
    let second = parse_lex_bound(arguments.remove(0))?;
    let options = parse_range_options(&arguments, false)?;

    if options.by_score || options.by_lex || options.reverse || options.with_scores {
        return Err(ProtocolError::Invalid("syntax error"));
    }

    let (min, max) = if reverse {
        (second, first)
    } else {
        (first, second)
    };

    Ok(Command::SortedSetRangeByLex {
        key,
        min,
        max,
        with_scores: false,
        reverse,
        limit: options.limit,
        invocation: if reverse {
            LexInvocation::ZRevRangeByLex
        } else {
            LexInvocation::ZRangeByLex
        },
    })
}

pub(in crate::protocol) struct RangeOptions {
    pub(in crate::protocol) by_score: bool,
    pub(in crate::protocol) by_lex: bool,
    pub(in crate::protocol) reverse: bool,
    pub(in crate::protocol) with_scores: bool,
    pub(in crate::protocol) limit: Option<RangeLimit>,
}

pub(in crate::protocol) fn parse_range_options(
    arguments: &[Bytes<'_>],
    legacy_reverse: bool,
) -> Result<RangeOptions, ProtocolError> {
    let mut options = RangeOptions {
        by_score: false,
        by_lex: false,
        reverse: legacy_reverse,
        with_scores: false,
        limit: None,
    };

    let mut index = 0;

    while index < arguments.len() {
        let token = arguments[index].as_ref();

        if token.eq_ignore_ascii_case(b"WITHSCORES") {
            if options.with_scores {
                return Err(ProtocolError::Invalid("syntax error"));
            }

            options.with_scores = true;
            index += 1;
            continue;
        }

        if token.eq_ignore_ascii_case(b"BYSCORE") {
            if options.by_score || options.by_lex {
                return Err(ProtocolError::Invalid("syntax error"));
            }

            options.by_score = true;
            index += 1;
            continue;
        }

        if token.eq_ignore_ascii_case(b"BYLEX") {
            if options.by_score || options.by_lex {
                return Err(ProtocolError::Invalid("syntax error"));
            }

            options.by_lex = true;
            index += 1;
            continue;
        }

        if token.eq_ignore_ascii_case(b"REV") {
            if legacy_reverse || options.reverse {
                return Err(ProtocolError::Invalid("syntax error"));
            }

            options.reverse = true;
            index += 1;
            continue;
        }

        if token.eq_ignore_ascii_case(b"LIMIT") {
            if options.limit.is_some() || index + 2 >= arguments.len() {
                return Err(ProtocolError::Invalid("syntax error"));
            }

            let offset = parse_limit_int(arguments[index + 1].as_ref())?;
            let count = parse_limit_int(arguments[index + 2].as_ref())?;

            if offset < 0 {
                return Err(ProtocolError::Invalid("LIMIT offset must be >= 0"));
            }

            options.limit = Some(RangeLimit {
                offset: offset as usize,
                count: (count >= 0).then_some(count as usize),
            });

            index += 3;
            continue;
        }

        return Err(ProtocolError::Invalid("syntax error"));
    }

    Ok(options)
}

pub(in crate::protocol) fn parse_limit_int(bytes: &[u8]) -> Result<isize, ProtocolError> {
    std::str::from_utf8(bytes)
        .ok()
        .and_then(|text| text.parse().ok())
        .ok_or(ProtocolError::Invalid("limit is not an integer"))
}

pub(in crate::protocol) fn parse_lex_bound(
    bytes: Bytes<'_>,
) -> Result<LexBound<'_>, ProtocolError> {
    if bytes.as_ref() == b"-" {
        return Ok(LexBound::NegInf);
    }

    if bytes.as_ref() == b"+" {
        return Ok(LexBound::PosInf);
    }

    if let Some(member) = bytes.as_ref().strip_prefix(b"[") {
        return Ok(LexBound::Inclusive(Cow::Owned(member.to_vec())));
    }

    if let Some(member) = bytes.as_ref().strip_prefix(b"(") {
        return Ok(LexBound::Exclusive(Cow::Owned(member.to_vec())));
    }

    Err(ProtocolError::Invalid(
        "min or max not valid string range item",
    ))
}

pub(in crate::protocol) fn parse_score_bound(bytes: &[u8]) -> Result<ScoreBound, ProtocolError> {
    if bytes.eq_ignore_ascii_case(b"-inf") {
        return Ok(ScoreBound::NegInf);
    }

    if bytes.eq_ignore_ascii_case(b"+inf") || bytes.eq_ignore_ascii_case(b"inf") {
        return Ok(ScoreBound::PosInf);
    }

    if let Some(rest) = bytes.strip_prefix(b"(") {
        return Ok(ScoreBound::Exclusive(parse_f64(rest)?));
    }

    Ok(ScoreBound::Inclusive(parse_f64(bytes)?))
}

pub(in crate::protocol) fn parse_set_algebra(
    mut arguments: Vec<Bytes<'_>>,
    op: SetOp,
    store: bool,
) -> Result<Command<'_>, ProtocolError> {
    if store {
        if arguments.len() < 2 {
            return Err(ProtocolError::Invalid(
                "set STORE commands expect a destination and at least one key",
            ));
        }

        let destination = Some(arguments.remove(0));

        return Ok(Command::SetAlgebra {
            keys: arguments,
            op,
            destination,
        });
    }

    if arguments.is_empty() {
        return Err(ProtocolError::Invalid(
            "set algebra commands expect at least one key",
        ));
    }

    Ok(Command::SetAlgebra {
        keys: arguments,
        op,
        destination: None,
    })
}

pub(in crate::protocol) fn parse_sorted_set_pop(
    mut arguments: Vec<Bytes<'_>>,
    max: bool,
) -> Result<Command<'_>, ProtocolError> {
    let count =
        match arguments.len() {
            1 => None,
            2 => {
                let parsed = parse_i64(&arguments.pop().expect("length checked"))?;

                Some(usize::try_from(parsed).map_err(|_| {
                    ProtocolError::Invalid("value is out of range, must be positive")
                })?)
            }

            0 => {
                return Err(ProtocolError::Invalid(if max {
                    "wrong number of arguments for 'zpopmax' command"
                } else {
                    "wrong number of arguments for 'zpopmin' command"
                }));
            }

            _ => return Err(ProtocolError::Invalid("syntax error")),
        };

    Ok(Command::SortedSetPop {
        key: arguments.pop().expect("length checked"),
        count,
        max,
    })
}

pub(in crate::protocol) fn parse_sorted_set_remove_range<'a>(
    mut arguments: Vec<Bytes<'a>>,
    name: &[u8],
) -> Result<Command<'a>, ProtocolError> {
    if arguments.len() != 3 {
        return Err(ProtocolError::Invalid(
            if name.eq_ignore_ascii_case(b"ZREMRANGEBYRANK") {
                "wrong number of arguments for 'zremrangebyrank' command"
            } else if name.eq_ignore_ascii_case(b"ZREMRANGEBYSCORE") {
                "wrong number of arguments for 'zremrangebyscore' command"
            } else {
                "wrong number of arguments for 'zremrangebylex' command"
            },
        ));
    }

    let max = arguments.pop().expect("length checked");
    let min = arguments.pop().expect("length checked");
    let key = arguments.pop().expect("length checked");
    let range = if name.eq_ignore_ascii_case(b"ZREMRANGEBYRANK") {
        let not_integer = |_| ProtocolError::Invalid("value is not an integer or out of range");

        SortedRemoveRange::Rank(
            parse_isize(&min).map_err(not_integer)?,
            parse_isize(&max).map_err(not_integer)?,
        )
    } else if name.eq_ignore_ascii_case(b"ZREMRANGEBYSCORE") {
        let not_float = |_| ProtocolError::Invalid("min or max is not a float");

        SortedRemoveRange::Score(
            parse_score_bound(&min).map_err(not_float)?,
            parse_score_bound(&max).map_err(not_float)?,
        )
    } else {
        SortedRemoveRange::Lex(parse_lex_bound(min)?, parse_lex_bound(max)?)
    };

    Ok(Command::SortedSetRemoveRange { key, range })
}
