use super::*;

pub(in crate::protocol) fn parse_list_block<'a>(args: &Args<'a, '_>, back: bool) -> Parsed<'a> {
    if args.len() < 2 {
        return Err(ProtocolError::Invalid(
            "BLPOP and BRPOP expect at least one key and a timeout",
        ));
    }

    let last = args.len() - 1;
    let timeout_ms = parse_timeout_ms(args.bytes(last))?;

    Ok(Command::ListBlock {
        keys: (0..last).map(|index| args.arg(index)).collect(),
        timeout_ms,
        back,
    })
}
