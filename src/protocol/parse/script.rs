use super::*;

pub(in crate::protocol) fn parse_fcall(
    mut arguments: Vec<Bytes<'_>>,
) -> Result<Command<'_>, ProtocolError> {
    if arguments.len() < 2 {
        return Err(ProtocolError::Invalid(
            "FCALL expects a function name, a key count, optional keys, and optional arguments",
        ));
    }

    let name = arguments.remove(0);
    let numkeys = parse_usize(&arguments.remove(0))?;

    if arguments.len() < numkeys {
        return Err(ProtocolError::Invalid(
            "FCALL key count exceeds the number of remaining arguments",
        ));
    }

    let keys = arguments.drain(..numkeys).collect();

    Ok(Command::FCall {
        name,
        keys,
        args: arguments,
    })
}

pub(in crate::protocol) fn parse_function(
    mut arguments: Vec<Bytes<'_>>,
) -> Result<Command<'_>, ProtocolError> {
    if arguments.is_empty() {
        return Err(ProtocolError::Invalid("FUNCTION expects a subcommand"));
    }

    let subcommand = arguments.remove(0);

    if subcommand.eq_ignore_ascii_case(b"LOAD") {
        return match arguments.len() {
            2 => {
                let body = arguments.pop().expect("length checked");
                let name = arguments.pop().expect("length checked");

                Ok(Command::FunctionLoad { name, body })
            }

            _ => Err(ProtocolError::Invalid(
                "FUNCTION LOAD expects a name and a body",
            )),
        };
    }

    if subcommand.eq_ignore_ascii_case(b"DELETE") {
        return match arguments.len() {
            1 => Ok(Command::FunctionDelete(
                arguments.pop().expect("length checked"),
            )),
            _ => Err(ProtocolError::Invalid("FUNCTION DELETE expects a name")),
        };
    }

    if subcommand.eq_ignore_ascii_case(b"LIST") {
        return match arguments.len() {
            0 => Ok(Command::FunctionList),
            _ => Err(ProtocolError::Invalid("FUNCTION LIST expects no arguments")),
        };
    }

    Err(ProtocolError::Invalid(
        "only FUNCTION LOAD, LIST, and DELETE are supported",
    ))
}

pub(in crate::protocol) fn parse_eval(
    sha: bool,
    mut arguments: Vec<Bytes<'_>>,
) -> Result<Command<'_>, ProtocolError> {
    if arguments.len() < 2 {
        return Err(ProtocolError::Invalid(
            "EVAL expects a script, a key count, optional keys, and optional arguments",
        ));
    }

    let script = arguments.remove(0);
    let numkeys = parse_usize(&arguments.remove(0))?;

    if arguments.len() < numkeys {
        return Err(ProtocolError::Invalid(
            "EVAL key count exceeds the number of remaining arguments",
        ));
    }

    let keys = arguments.drain(..numkeys).collect();

    if sha {
        Ok(Command::EvalSha {
            sha: script,
            keys,
            args: arguments,
        })
    } else {
        Ok(Command::Eval {
            script,
            keys,
            args: arguments,
        })
    }
}

pub(in crate::protocol) fn parse_script(
    mut arguments: Vec<Bytes<'_>>,
) -> Result<Command<'_>, ProtocolError> {
    if arguments.is_empty() {
        return Err(ProtocolError::Invalid("SCRIPT expects a subcommand"));
    }

    let subcommand = arguments.remove(0);

    if subcommand.eq_ignore_ascii_case(b"LOAD") {
        return match arguments.len() {
            1 => Ok(Command::ScriptLoad(
                arguments.pop().expect("length checked"),
            )),
            _ => Err(ProtocolError::Invalid("SCRIPT LOAD expects a script")),
        };
    }

    if subcommand.eq_ignore_ascii_case(b"EXISTS") {
        if arguments.is_empty() {
            return Err(ProtocolError::Invalid(
                "SCRIPT EXISTS expects one or more SHA1 digests",
            ));
        }

        return Ok(Command::ScriptExists(arguments));
    }

    if subcommand.eq_ignore_ascii_case(b"FLUSH") {
        return match arguments.as_slice() {
            [] => Ok(Command::ScriptFlush),
            [mode] if mode.eq_ignore_ascii_case(b"SYNC") || mode.eq_ignore_ascii_case(b"ASYNC") => {
                Ok(Command::ScriptFlush)
            }

            _ => Err(ProtocolError::Invalid(
                "SCRIPT FLUSH expects no arguments or SYNC/ASYNC",
            )),
        };
    }

    if subcommand.eq_ignore_ascii_case(b"KILL") {
        return match arguments.len() {
            0 => Ok(Command::ScriptKill),
            _ => Err(ProtocolError::Invalid("SCRIPT KILL expects no arguments")),
        };
    }

    Err(ProtocolError::Invalid("unknown SCRIPT subcommand"))
}
