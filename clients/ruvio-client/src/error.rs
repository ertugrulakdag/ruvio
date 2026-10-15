use std::fmt;

/// A transport failure, a broken RESP frame, or a Ruvio error reply.
#[derive(Debug)]
pub enum Error {
    /// The TCP connection failed or closed.
    Io(std::io::Error),
    /// The bytes on the socket were not a complete RESP2 value.
    Protocol(String),
    /// Ruvio returned a RESP error. The connection can still run the next command.
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
