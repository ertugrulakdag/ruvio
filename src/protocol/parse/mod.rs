use super::*;

mod admin;
pub(in crate::protocol) mod commands;
mod geo;
mod hash;
mod list;
mod script;
mod stream;
mod util;
mod zset;

pub(in crate::protocol) use admin::*;
pub(in crate::protocol) use geo::*;
pub(crate) use hash::*;
pub(in crate::protocol) use list::*;
pub(in crate::protocol) use script::*;
pub(in crate::protocol) use stream::*;
pub(crate) use util::*;
pub(in crate::protocol) use zset::*;

pub(crate) fn command_rejection(message: &'static str) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(4 + message.len());

    bytes.extend_from_slice(b"ERR ");
    bytes.extend_from_slice(message.as_bytes());
    bytes
}

pub(crate) fn parse_arguments(mut arguments: Vec<Bytes<'_>>) -> Result<Command<'_>, ProtocolError> {
    let name = arguments.remove(0);

    match super::table::lookup(&name).and_then(|spec| spec.parse) {
        Some(parse) => parse(&super::table::Args::from_slice(&arguments)),
        None => Err(ProtocolError::Invalid("unknown command")),
    }
}
