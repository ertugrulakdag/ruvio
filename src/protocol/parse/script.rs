use super::*;

pub(in crate::protocol) fn parse_fcall<'a>(args: &Args<'a, '_>) -> Parsed<'a> {
    if args.len() < 2 {
        return Err(ProtocolError::Invalid(
            "FCALL expects a function name, a key count, optional keys, and optional arguments",
        ));
    }

    let numkeys = parse_usize(args.bytes(1))?;

    if args.len() - 2 < numkeys {
        return Err(ProtocolError::Invalid(
            "FCALL key count exceeds the number of remaining arguments",
        ));
    }

    let keys_end = 2 + numkeys;

    Ok(Command::FCall {
        name: args.arg(0),
        keys: (2..keys_end).map(|index| args.arg(index)).collect(),
        args: args.rest(keys_end),
    })
}

pub(in crate::protocol) fn parse_function<'a>(args: &Args<'a, '_>) -> Parsed<'a> {
    if args.len() == 0 {
        return Err(ProtocolError::Invalid("FUNCTION expects a subcommand"));
    }

    let subcommand = args.bytes(0);
    // Arguments after the subcommand.
    let rest = args.len() - 1;

    if subcommand.eq_ignore_ascii_case(b"LOAD") {
        return match rest {
            2 => Ok(Command::FunctionLoad {
                name: args.arg(1),
                body: args.arg(2),
            }),

            _ => Err(ProtocolError::Invalid(
                "FUNCTION LOAD expects a name and a body",
            )),
        };
    }

    if subcommand.eq_ignore_ascii_case(b"DELETE") {
        return match rest {
            1 => Ok(Command::FunctionDelete(args.arg(1))),
            _ => Err(ProtocolError::Invalid("FUNCTION DELETE expects a name")),
        };
    }

    if subcommand.eq_ignore_ascii_case(b"LIST") {
        return match rest {
            0 => Ok(Command::FunctionList),
            _ => Err(ProtocolError::Invalid("FUNCTION LIST expects no arguments")),
        };
    }

    Err(ProtocolError::Invalid(
        "only FUNCTION LOAD, LIST, and DELETE are supported",
    ))
}

pub(in crate::protocol) fn parse_eval<'a>(sha: bool, args: &Args<'a, '_>) -> Parsed<'a> {
    if args.len() < 2 {
        return Err(ProtocolError::Invalid(
            "EVAL expects a script, a key count, optional keys, and optional arguments",
        ));
    }

    let numkeys = parse_usize(args.bytes(1))?;

    if args.len() - 2 < numkeys {
        return Err(ProtocolError::Invalid(
            "EVAL key count exceeds the number of remaining arguments",
        ));
    }

    let script = args.arg(0);
    let keys_end = 2 + numkeys;
    let keys = (2..keys_end).map(|index| args.arg(index)).collect();
    let rest = args.rest(keys_end);

    if sha {
        Ok(Command::EvalSha {
            sha: script,
            keys,
            args: rest,
        })
    } else {
        Ok(Command::Eval {
            script,
            keys,
            args: rest,
        })
    }
}

pub(in crate::protocol) fn parse_script<'a>(args: &Args<'a, '_>) -> Parsed<'a> {
    if args.len() == 0 {
        return Err(ProtocolError::Invalid("SCRIPT expects a subcommand"));
    }

    let subcommand = args.bytes(0);
    // Arguments after the subcommand.
    let rest = args.len() - 1;

    if subcommand.eq_ignore_ascii_case(b"LOAD") {
        return match rest {
            1 => Ok(Command::ScriptLoad(args.arg(1))),
            _ => Err(ProtocolError::Invalid("SCRIPT LOAD expects a script")),
        };
    }

    if subcommand.eq_ignore_ascii_case(b"EXISTS") {
        if rest == 0 {
            return Err(ProtocolError::Invalid(
                "SCRIPT EXISTS expects one or more SHA1 digests",
            ));
        }

        return Ok(Command::ScriptExists(args.rest(1)));
    }

    if subcommand.eq_ignore_ascii_case(b"FLUSH") {
        let mode = |mode: &[u8]| args.bytes(1).eq_ignore_ascii_case(mode);

        return match rest {
            0 => Ok(Command::ScriptFlush),
            1 if mode(b"SYNC") || mode(b"ASYNC") => Ok(Command::ScriptFlush),

            _ => Err(ProtocolError::Invalid(
                "SCRIPT FLUSH expects no arguments or SYNC/ASYNC",
            )),
        };
    }

    if subcommand.eq_ignore_ascii_case(b"KILL") {
        return match rest {
            0 => Ok(Command::ScriptKill),
            _ => Err(ProtocolError::Invalid("SCRIPT KILL expects no arguments")),
        };
    }

    Err(ProtocolError::Invalid("unknown SCRIPT subcommand"))
}
