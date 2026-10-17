use std::fmt;

/// Every way a call can fail.
///
/// Match on the variant to decide what to do: [`Error::Server`] is a normal refusal by the
/// server (the connection is still usable), [`Error::Io`] means the socket failed, and
/// [`Error::Protocol`] means the reply or an argument was malformed. The `Display` text is
/// the bare message, e.g. `WRONGTYPE Operation against a key holding the wrong kind of value`.
///
/// # Examples
///
/// ```no_run
/// # use ruvio_client::*;
/// # use std::time::Duration;
/// # fn run(client: &mut Client) -> Result<(), Error> {
/// match client.get("counter") {
///     Ok(value) => println!("{value:?}"),
///     Err(Error::Server(message)) if message.starts_with("WRONGTYPE") => {
///         println!("not a string");
///     }
///     Err(other) => return Err(other),
/// }
/// # Ok(())
/// # }
/// ```
#[derive(Debug)]
pub enum Error {
    /// The TCP connection failed, timed out, or was closed (connect error, reset, broken pipe,
    /// or a read timeout set with `Client::set_read_timeout`). After a failed command the
    /// outcome is unknown; with reconnect on the next command opens a new socket.
    Io(std::io::Error),
    /// The bytes on the socket were not a complete RESP2 value, a reply had an unexpected shape
    /// for the command, or the client rejected an argument before sending (for example an empty
    /// host, an empty key list for `DEL`, or a non-UTF-8 text reply read as a `String`).
    Protocol(String),
    /// Ruvio returned a RESP error reply; the text is the error message including its code
    /// (`ERR ...`, `WRONGTYPE ...`, `NOAUTH ...`, `CROSSSLOT ...`, `NOPERM ...`). The connection
    /// can still run the next command.
    Server(String),
}

impl fmt::Display for Error {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Io(error) => write!(formatter, "{error}"),
            Error::Protocol(message) | Error::Server(message) => write!(formatter, "{message}"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Io(error) => Some(error),
            Error::Protocol(_) | Error::Server(_) => None,
        }
    }
}

impl From<std::io::Error> for Error {
    fn from(error: std::io::Error) -> Self {
        Error::Io(error)
    }
}

#[cfg(test)]
mod tests {
    use super::Error;
    use std::error::Error as _;
    use std::io::{self, ErrorKind};

    #[test]
    fn display_shows_the_message_of_every_variant() {
        assert_eq!(Error::Protocol("bad frame".into()).to_string(), "bad frame");
        assert_eq!(Error::Server("ERR nope".into()).to_string(), "ERR nope");
        assert_eq!(
            Error::Io(io::Error::new(ErrorKind::BrokenPipe, "pipe gone")).to_string(),
            "pipe gone"
        );
    }

    #[test]
    fn source_is_the_io_error_only() {
        let io_error = Error::Io(io::Error::new(ErrorKind::TimedOut, "slow"));
        assert_eq!(io_error.source().unwrap().to_string(), "slow");
        assert!(Error::Protocol("x".into()).source().is_none());
        assert!(Error::Server("x".into()).source().is_none());
    }

    #[test]
    fn io_errors_convert_with_from_and_question_mark() {
        fn run(fail: bool) -> Result<u8, Error> {
            if fail {
                Err(io::Error::new(ErrorKind::UnexpectedEof, "eof"))?;
            }

            Ok(7)
        }

        assert_eq!(run(false).unwrap(), 7);
        match run(true) {
            Err(Error::Io(error)) => assert_eq!(error.kind(), ErrorKind::UnexpectedEof),
            other => panic!("expected Io, got {other:?}"),
        }
        assert!(format!("{:?}", Error::Server("s".into())).contains("Server"));
    }
}
