/// One RESP2 value, as returned by [`Client::execute`](crate::Client::execute) and the
/// low-level methods such as `eval` and `command_info`. Most typed methods decode it for you.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RespValue {
    /// A simple string such as `OK` or `PONG`.
    Simple(String),
    /// An error reply. `Client::execute` converts it to [`Error::Server`](crate::Error::Server);
    /// it only appears as a value inside pipeline results and nested arrays.
    Error(String),
    /// A signed 64-bit integer.
    Integer(i64),
    /// A bulk string as raw bytes, including the empty bulk. Not necessarily UTF-8.
    Bulk(Vec<u8>),
    /// An array of values (possibly nested).
    Array(Vec<RespValue>),
    /// A null bulk or null array: the key, field or result does not exist.
    Null,
}

impl RespValue {
    /// UTF-8 text of a simple string, error or bulk string; an integer becomes its decimal
    /// text. `Ok(None)` for null or an array (including a Pub/Sub push). Copies the bytes; use
    /// [`Self::into_string`] to avoid that.
    ///
    /// # Errors
    /// [`Error::Protocol`](crate::Error::Protocol) when a bulk string is not valid UTF-8.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use ruvio_client::*;
    /// # use std::time::Duration;
    /// # fn run(client: &mut Client) -> Result<(), Error> {
    /// let text = RespValue::Bulk(b"hi".to_vec()).as_string()?;
    /// assert_eq!(text.as_deref(), Some("hi"));
    /// # Ok(())
    /// # }
    /// ```
    pub fn as_string(&self) -> Result<Option<String>, crate::Error> {
        match self {
            RespValue::Null | RespValue::Array(_) => Ok(None),
            RespValue::Integer(value) => Ok(Some(value.to_string())),
            RespValue::Simple(text) | RespValue::Error(text) => Ok(Some(text.clone())),
            RespValue::Bulk(bytes) => {
                let text = String::from_utf8(bytes.clone())
                    .map_err(|_| crate::Error::Protocol("bulk string is not UTF-8".to_owned()))?;

                Ok(Some(text))
            }
        }
    }

    /// Like [`Self::as_string`] but consumes the value and moves a bulk's bytes into the
    /// `String` instead of copying them.
    pub fn into_string(self) -> Result<Option<String>, crate::Error> {
        match self {
            RespValue::Null | RespValue::Array(_) => Ok(None),
            RespValue::Integer(value) => Ok(Some(value.to_string())),
            RespValue::Simple(text) | RespValue::Error(text) => Ok(Some(text)),
            RespValue::Bulk(bytes) => String::from_utf8(bytes)
                .map(Some)
                .map_err(|_| crate::Error::Protocol("bulk string is not UTF-8".to_owned())),
        }
    }

    /// The raw bytes of a bulk (or simple) string, or `None` for any other kind, including
    /// null. Does not copy.
    pub fn into_bytes(self) -> Option<Vec<u8>> {
        match self {
            RespValue::Bulk(bytes) => Some(bytes),
            RespValue::Simple(text) => Some(text.into_bytes()),
            _ => None,
        }
    }

    /// The integer, or `None` when the value is not an integer.
    pub fn as_integer(&self) -> Option<i64> {
        match self {
            RespValue::Integer(value) => Some(*value),
            _ => None,
        }
    }

    /// The array items, or `None` when the value is not an array.
    pub fn as_array(&self) -> Option<&[RespValue]> {
        match self {
            RespValue::Array(items) => Some(items),
            _ => None,
        }
    }

    /// True for a null bulk or null array.
    pub fn is_null(&self) -> bool {
        matches!(self, RespValue::Null)
    }
}

#[cfg(test)]
mod tests {
    use super::RespValue;

    #[test]
    fn as_string_does_not_fail_on_an_array_or_integer() {
        let message = RespValue::Array(vec![
            RespValue::Bulk(b"message".to_vec()),
            RespValue::Bulk(b"news".to_vec()),
            RespValue::Bulk(b"hello".to_vec()),
        ]);

        assert_eq!(message.as_string().unwrap(), None);
        assert_eq!(
            message.as_array().unwrap()[2]
                .as_string()
                .unwrap()
                .as_deref(),
            Some("hello")
        );
        assert_eq!(
            RespValue::Integer(3).as_string().unwrap().as_deref(),
            Some("3")
        );
    }

    #[test]
    fn string_accessors_cover_every_kind_and_reject_non_utf8() {
        let bad = || RespValue::Bulk(vec![0xff, 0xfe]);
        assert!(bad().as_string().is_err());
        assert!(bad().into_string().is_err());
        assert_eq!(RespValue::Null.as_string().unwrap(), None);
        assert_eq!(RespValue::Null.into_string().unwrap(), None);
        assert_eq!(RespValue::Array(vec![]).into_string().unwrap(), None);
        assert_eq!(
            RespValue::Simple("OK".into())
                .as_string()
                .unwrap()
                .as_deref(),
            Some("OK")
        );
        assert_eq!(
            RespValue::Error("ERR x".into())
                .as_string()
                .unwrap()
                .as_deref(),
            Some("ERR x")
        );
        assert_eq!(
            RespValue::Bulk(b"hi".to_vec())
                .as_string()
                .unwrap()
                .as_deref(),
            Some("hi")
        );
        assert_eq!(
            RespValue::Integer(-4).into_string().unwrap().as_deref(),
            Some("-4")
        );
        assert_eq!(
            RespValue::Simple("s".into())
                .into_string()
                .unwrap()
                .as_deref(),
            Some("s")
        );
        assert_eq!(
            RespValue::Error("e".into())
                .into_string()
                .unwrap()
                .as_deref(),
            Some("e")
        );
        assert_eq!(
            RespValue::Bulk(b"b".to_vec())
                .into_string()
                .unwrap()
                .as_deref(),
            Some("b")
        );
    }

    #[test]
    fn bytes_integer_array_and_null_accessors() {
        assert_eq!(
            RespValue::Bulk(b"ab".to_vec()).into_bytes(),
            Some(b"ab".to_vec())
        );
        assert_eq!(
            RespValue::Simple("OK".into()).into_bytes(),
            Some(b"OK".to_vec())
        );
        assert_eq!(RespValue::Integer(1).into_bytes(), None);
        assert_eq!(RespValue::Null.into_bytes(), None);
        assert_eq!(RespValue::Integer(9).as_integer(), Some(9));
        assert_eq!(RespValue::Bulk(b"9".to_vec()).as_integer(), None);
        assert!(RespValue::Integer(1).as_array().is_none());
        assert_eq!(RespValue::Array(vec![]).as_array().unwrap().len(), 0);
        assert!(RespValue::Null.is_null());
        assert!(!RespValue::Array(vec![]).is_null());
        assert!(!RespValue::Bulk(vec![]).is_null());
    }
}
