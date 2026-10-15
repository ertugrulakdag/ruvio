//! `COMMAND`, `COMMAND COUNT`, `COMMAND INFO` and `COMMAND DOCS`, answered from the
//! command table in `protocol/table.rs`.

use std::borrow::Cow;

use super::Response;
use crate::protocol::{Bytes, CommandMetaKind, CommandSpec, all_commands, command_spec};

pub(super) fn respond<'a>(kind: CommandMetaKind, names: &[Bytes<'_>]) -> Response<'a> {
    match kind {
        CommandMetaKind::Count => Response::Integer(all_commands().len() as i64),
        CommandMetaKind::List => Response::Exec(all_commands().iter().map(info).collect()),
        CommandMetaKind::Info if names.is_empty() => {
            Response::Exec(all_commands().iter().map(info).collect())
        }
        CommandMetaKind::Info => Response::Exec(
            names
                .iter()
                .map(|name| command_spec(name).map_or(Response::Bulk(None), info))
                .collect(),
        ),
        CommandMetaKind::Docs => {
            let selected: Vec<&CommandSpec> = if names.is_empty() {
                all_commands().iter().collect()
            } else {
                names.iter().filter_map(|name| command_spec(name)).collect()
            };

            Response::Map(
                selected
                    .into_iter()
                    .map(|spec| {
                        (
                            Cow::Borrowed(spec.name.as_bytes()),
                            Response::Map(Vec::new()),
                        )
                    })
                    .collect(),
            )
        }
    }
}

/// One `COMMAND INFO` entry in the Redis 7 shape: name, arity, flags, first, last, step,
/// ACL categories, then empty tips, key specs and subcommands.
fn info<'a>(spec: &CommandSpec) -> Response<'a> {
    Response::Exec(vec![
        Response::Bulk(Some(Cow::Borrowed(spec.name.as_bytes()))),
        Response::Integer(i64::from(spec.arity)),
        Response::Exec(
            spec.flag_names()
                .into_iter()
                .map(|flag| Response::Simple(flag.as_bytes()))
                .collect(),
        ),
        Response::Integer(i64::from(spec.first_key)),
        Response::Integer(i64::from(spec.last_key)),
        Response::Integer(i64::from(spec.step)),
        Response::Exec(
            spec.categories
                .iter()
                .map(|category| Response::OwnedSimple(format!("@{category}").into_bytes()))
                .collect(),
        ),
        Response::Exec(Vec::new()),
        Response::Exec(Vec::new()),
        Response::Exec(Vec::new()),
    ])
}
