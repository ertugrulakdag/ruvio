use super::*;

/// Longest inline command line, terminator included. Redis uses the same 64 KiB.
pub const MAX_INLINE_BYTES: usize = 64 * 1024;

/// Split one inline command line like Redis `sdssplitargs`: whitespace separates
/// arguments, `"..."` understands `\n \r \t \b \a \xHH` and `\\`, `'...'` only `\'`.
/// A closing quote must be followed by whitespace or the end of the line.
pub(super) fn split_inline_arguments(line: &[u8]) -> Result<Vec<Vec<u8>>, ProtocolError> {
    const UNBALANCED: ProtocolError = ProtocolError::Invalid("unbalanced quotes in request");

    let mut arguments = Vec::new();
    let mut index = 0;

    loop {
        while index < line.len() && line[index].is_ascii_whitespace() {
            index += 1;
        }

        if index >= line.len() {
            return Ok(arguments);
        }

        let mut current = Vec::new();
        let mut in_double = false;
        let mut in_single = false;

        loop {
            let byte = line.get(index).copied();

            if in_double {
                match byte {
                    None => return Err(UNBALANCED),
                    Some(b'\\') if line.get(index + 1) == Some(&b'x') => {
                        let high = line.get(index + 2).and_then(|b| (*b as char).to_digit(16));
                        let low = line.get(index + 3).and_then(|b| (*b as char).to_digit(16));

                        if let (Some(high), Some(low)) = (high, low) {
                            current.push((high * 16 + low) as u8);
                            index += 3;
                        } else {
                            current.push(b'x');
                            index += 1;
                        }
                    }
                    Some(b'\\') if index + 1 < line.len() => {
                        index += 1;
                        current.push(match line[index] {
                            b'n' => b'\n',
                            b'r' => b'\r',
                            b't' => b'\t',
                            b'b' => 8,
                            b'a' => 7,
                            other => other,
                        });
                    }
                    Some(b'"') => {
                        if line
                            .get(index + 1)
                            .is_some_and(|b| !b.is_ascii_whitespace())
                        {
                            return Err(UNBALANCED);
                        }
                        index += 1;
                        break;
                    }
                    Some(other) => current.push(other),
                }
            } else if in_single {
                match byte {
                    None => return Err(UNBALANCED),
                    Some(b'\\') if line.get(index + 1) == Some(&b'\'') => {
                        index += 1;
                        current.push(b'\'');
                    }
                    Some(b'\'') => {
                        if line
                            .get(index + 1)
                            .is_some_and(|b| !b.is_ascii_whitespace())
                        {
                            return Err(UNBALANCED);
                        }
                        index += 1;
                        break;
                    }
                    Some(other) => current.push(other),
                }
            } else {
                match byte {
                    None => break,
                    Some(byte) if byte.is_ascii_whitespace() => break,
                    Some(b'"') => in_double = true,
                    Some(b'\'') => in_single = true,
                    Some(other) => current.push(other),
                }
            }

            index += 1;
        }

        arguments.push(current);
    }
}

/// Parse one inline command line from the start of `bytes`.
pub(super) fn parse_inline_command(
    bytes: &[u8],
    limits: ProtocolLimits,
) -> Result<Option<(Command<'_>, usize)>, ProtocolError> {
    let window = &bytes[..bytes.len().min(MAX_INLINE_BYTES)];

    let Some(newline) = window.iter().position(|&byte| byte == b'\n') else {
        if bytes.len() >= MAX_INLINE_BYTES {
            return Err(ProtocolError::LimitExceeded("too big inline request"));
        }
        return Ok(None);
    };

    let consumed = newline + 1;
    let line = bytes[..newline]
        .strip_suffix(b"\r")
        .unwrap_or(&bytes[..newline]);

    let arguments = match split_inline_arguments(line) {
        Ok(arguments) => arguments,
        Err(ProtocolError::Invalid(message)) => {
            return Err(ProtocolError::Rejected { message, consumed });
        }
        Err(error) => return Err(error),
    };

    if arguments.is_empty() {
        return Err(ProtocolError::Rejected {
            message: "empty inline command",
            consumed,
        });
    }

    if arguments.len() > limits.max_arguments {
        return Err(ProtocolError::LimitExceeded("too many arguments"));
    }

    let command = match parse_arguments(arguments.into_iter().map(Cow::Owned).collect()) {
        Ok(command) => command,
        Err(ProtocolError::Invalid(message)) => {
            return Err(ProtocolError::Rejected { message, consumed });
        }
        Err(error) => return Err(error),
    };

    validate_command_payloads(&command, limits)?;
    Ok(Some((command, consumed)))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn split(line: &str) -> Result<Vec<Vec<u8>>, ProtocolError> {
        split_inline_arguments(line.as_bytes())
    }

    #[test]
    fn splits_plain_and_quoted_arguments() {
        assert_eq!(
            split("SET  a b").unwrap(),
            vec![b"SET".to_vec(), b"a".to_vec(), b"b".to_vec()]
        );
        assert_eq!(
            split(r#"SET k "a b\n\x41""#).unwrap()[2],
            b"a b\nA".to_vec()
        );
        assert_eq!(split(r#"SET k 'it\'s'"#).unwrap()[2], b"it's".to_vec());
        assert!(split(r#"SET k "abc"#).is_err());
        assert!(split(r#"SET k "abc"x"#).is_err());
        assert!(split("   ").unwrap().is_empty());
    }

    #[test]
    fn parses_inline_frames_and_reports_consumed_bytes() {
        let limits = ProtocolLimits::default();
        let (command, consumed) = parse_inline_command(b"PING\r\nPING\r\n", limits)
            .unwrap()
            .unwrap();
        assert_eq!(consumed, 6);
        assert!(matches!(command, Command::Ping(None)));
        assert!(parse_inline_command(b"PIN", limits).unwrap().is_none());
        assert!(matches!(
            parse_inline_command(b"\r\n", limits),
            Err(ProtocolError::Rejected { consumed: 2, .. })
        ));
        let long = vec![b'a'; MAX_INLINE_BYTES];
        assert!(matches!(
            parse_inline_command(&long, limits),
            Err(ProtocolError::LimitExceeded(_))
        ));
    }
}
