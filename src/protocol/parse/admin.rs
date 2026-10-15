use super::*;

pub(in crate::protocol) fn parse_hello(
    mut arguments: Vec<Bytes<'_>>,
) -> Result<Command<'_>, ProtocolError> {
    if arguments.is_empty() {
        return Ok(Command::Hello {
            protocol: None,
            auth: None,
            client_name: None,
        });
    }

    let protocol = Some(parse_i64(&arguments.remove(0))?);
    let mut auth = None;
    let mut client_name = None;

    while let Some(option) = arguments.first() {
        if option.as_ref().eq_ignore_ascii_case(b"AUTH") {
            arguments.remove(0);

            if arguments.len() < 2 {
                return Err(ProtocolError::Invalid(
                    "HELLO AUTH expects a username and a password",
                ));
            }

            let username = arguments.remove(0);
            let password = arguments.remove(0);

            auth = Some((username, password));
        } else if option.as_ref().eq_ignore_ascii_case(b"SETNAME") {
            arguments.remove(0);

            if arguments.is_empty() {
                return Err(ProtocolError::Invalid("HELLO SETNAME expects a name"));
            }

            client_name = Some(arguments.remove(0));
        } else {
            return Err(ProtocolError::Invalid(
                "HELLO only supports protocol version 2 or 3, AUTH, and SETNAME",
            ));
        }
    }

    Ok(Command::Hello {
        protocol,
        auth,
        client_name,
    })
}

pub(in crate::protocol) fn parse_client(
    mut arguments: Vec<Bytes<'_>>,
) -> Result<Command<'_>, ProtocolError> {
    let Some(subcommand) = arguments.first().cloned() else {
        return Err(ProtocolError::Invalid(
            "CLIENT expects GETNAME, SETNAME, or TRACKING",
        ));
    };

    arguments.remove(0);

    if subcommand.as_ref().eq_ignore_ascii_case(b"GETNAME") {
        if !arguments.is_empty() {
            return Err(ProtocolError::Invalid(
                "CLIENT GETNAME expects no arguments",
            ));
        }

        return Ok(Command::ClientGetName);
    }

    if subcommand.as_ref().eq_ignore_ascii_case(b"ID") {
        if !arguments.is_empty() {
            return Err(ProtocolError::Invalid(
                "wrong number of arguments for 'client|id' command",
            ));
        }

        return Ok(Command::ClientId);
    }

    if subcommand.as_ref().eq_ignore_ascii_case(b"INFO") {
        if !arguments.is_empty() {
            return Err(ProtocolError::Invalid(
                "wrong number of arguments for 'client|info' command",
            ));
        }

        return Ok(Command::ClientInfo);
    }

    if subcommand.as_ref().eq_ignore_ascii_case(b"LIST") {
        return parse_client_list(arguments);
    }

    if subcommand.as_ref().eq_ignore_ascii_case(b"KILL") {
        return parse_client_kill(arguments);
    }

    if subcommand.as_ref().eq_ignore_ascii_case(b"SETNAME") {
        if arguments.len() != 1 {
            return Err(ProtocolError::Invalid("CLIENT SETNAME expects a name"));
        }

        return Ok(Command::ClientSetName(arguments.remove(0)));
    }

    if subcommand.as_ref().eq_ignore_ascii_case(b"TRACKING") {
        let Some(mode) = arguments.first() else {
            return Err(ProtocolError::Invalid("CLIENT TRACKING expects ON or OFF"));
        };

        if arguments.len() != 1 {
            return Err(ProtocolError::Invalid(
                "CLIENT TRACKING only supports ON or OFF",
            ));
        }

        if mode.as_ref().eq_ignore_ascii_case(b"ON") {
            return Ok(Command::ClientTracking(true));
        }

        if mode.as_ref().eq_ignore_ascii_case(b"OFF") {
            return Ok(Command::ClientTracking(false));
        }

        return Err(ProtocolError::Invalid(
            "CLIENT TRACKING only supports ON or OFF",
        ));
    }

    Err(ProtocolError::Invalid(
        "CLIENT expects GETNAME, SETNAME, or TRACKING",
    ))
}

/// `ACL LOAD` and `ACL SAVE` read and write `security.acl_file` only.
pub(in crate::protocol) fn parse_acl_command(
    mut arguments: Vec<Bytes<'_>>,
) -> Result<Command<'_>, ProtocolError> {
    if arguments
        .first()
        .is_some_and(|subcommand| subcommand.eq_ignore_ascii_case(b"SETUSER"))
    {
        arguments.remove(0);

        let Some(name) = arguments.first() else {
            return Err(ProtocolError::Invalid("ACL SETUSER expects a username"));
        };

        if name.as_ref().is_empty() {
            return Err(ProtocolError::Invalid(
                "ACL SETUSER username must not be empty",
            ));
        }

        let name = arguments.remove(0);

        return Ok(Command::Acl(AclCommand::SetUser {
            name,
            rules: arguments,
        }));
    }

    match arguments.as_slice() {
        [subcommand] if subcommand.eq_ignore_ascii_case(b"WHOAMI") => {
            Ok(Command::Acl(AclCommand::WhoAmI))
        }

        [subcommand] if subcommand.eq_ignore_ascii_case(b"USERS") => {
            Ok(Command::Acl(AclCommand::Users))
        }

        [subcommand] if subcommand.eq_ignore_ascii_case(b"LIST") => {
            Ok(Command::Acl(AclCommand::List))
        }

        [subcommand, name] if subcommand.eq_ignore_ascii_case(b"GETUSER") => {
            Ok(Command::Acl(AclCommand::GetUser(name.clone())))
        }

        [subcommand] if subcommand.eq_ignore_ascii_case(b"CAT") => {
            Ok(Command::Acl(AclCommand::Cat(None)))
        }

        [subcommand, name] if subcommand.eq_ignore_ascii_case(b"CAT") => {
            Ok(Command::Acl(AclCommand::Cat(Some(name.clone()))))
        }

        [subcommand] if subcommand.eq_ignore_ascii_case(b"LOAD") => {
            Ok(Command::Acl(AclCommand::Load))
        }

        [subcommand] if subcommand.eq_ignore_ascii_case(b"SAVE") => {
            Ok(Command::Acl(AclCommand::Save))
        }

        _ => Err(ProtocolError::Invalid(
            "only ACL WHOAMI, USERS, LIST, GETUSER, CAT, SETUSER, LOAD, and SAVE are supported",
        )),
    }
}

fn client_id(bytes: &[u8]) -> Result<u64, ProtocolError> {
    std::str::from_utf8(bytes)
        .ok()
        .and_then(|text| text.parse::<u64>().ok())
        .filter(|id| *id > 0)
        .ok_or(ProtocolError::Invalid("Invalid client ID"))
}

/// `CLIENT LIST [TYPE normal|master|replica|pubsub] [ID id ...]`.
fn parse_client_list(arguments: Vec<Bytes<'_>>) -> Result<Command<'_>, ProtocolError> {
    let mut ids = Vec::new();

    match arguments.first() {
        None => {}
        Some(option) if option.eq_ignore_ascii_case(b"ID") && arguments.len() > 1 => {
            for id in &arguments[1..] {
                ids.push(client_id(id)?);
            }
        }
        Some(option) if option.eq_ignore_ascii_case(b"TYPE") && arguments.len() == 2 => {
            let kind = arguments[1].as_ref();

            if !["normal", "master", "replica", "slave", "pubsub"]
                .iter()
                .any(|name| kind.eq_ignore_ascii_case(name.as_bytes()))
            {
                return Err(ProtocolError::Invalid("Unknown client type"));
            }
        }
        Some(_) => return Err(ProtocolError::Invalid("syntax error")),
    }

    Ok(Command::ClientList { ids })
}

/// `CLIENT KILL addr` or `CLIENT KILL [ID id] [ADDR addr] [SKIPME yes|no]`.
fn parse_client_kill(mut arguments: Vec<Bytes<'_>>) -> Result<Command<'_>, ProtocolError> {
    if arguments.len() == 1 {
        return Ok(Command::ClientKill {
            id: None,
            addr: Some(arguments.remove(0)),
            skip_me: false,
            legacy: true,
        });
    }

    if arguments.is_empty() || !arguments.len().is_multiple_of(2) {
        return Err(ProtocolError::Invalid("syntax error"));
    }

    let mut id = None;
    let mut addr = None;
    let mut skip_me = true;

    while !arguments.is_empty() {
        let name = arguments.remove(0);
        let value = arguments.remove(0);

        if name.eq_ignore_ascii_case(b"ID") {
            id = Some(client_id(&value)?);
        } else if name.eq_ignore_ascii_case(b"ADDR") {
            addr = Some(value);
        } else if name.eq_ignore_ascii_case(b"SKIPME") {
            skip_me = if value.eq_ignore_ascii_case(b"yes") {
                true
            } else if value.eq_ignore_ascii_case(b"no") {
                false
            } else {
                return Err(ProtocolError::Invalid("syntax error"));
            };
        } else {
            return Err(ProtocolError::Invalid("syntax error"));
        }
    }

    Ok(Command::ClientKill {
        id,
        addr,
        skip_me,
        legacy: false,
    })
}
