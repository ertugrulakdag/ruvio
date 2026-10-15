use super::*;

pub(in crate::protocol) fn parse_list_block(
    mut arguments: Vec<Bytes<'_>>,
    back: bool,
) -> Result<Command<'_>, ProtocolError> {
    if arguments.len() < 2 {
        return Err(ProtocolError::Invalid(
            "BLPOP and BRPOP expect at least one key and a timeout",
        ));
    }

    let timeout_ms = parse_timeout_ms(&arguments.pop().expect("length checked"))?;

    Ok(Command::ListBlock {
        keys: arguments,
        timeout_ms,
        back,
    })
}
