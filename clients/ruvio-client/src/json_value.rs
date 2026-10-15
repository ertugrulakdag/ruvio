//! Typed `JSON.*` helpers (feature `serde`): pass any `Serialize` value instead of JSON text.
//!
//! Serializer settings live on the value's serde attributes (`rename_all`, `skip_serializing_if`, ...).
//! To use a different serializer entirely, serialize yourself and call the raw `json_set`,
//! `json_merge` and `json_arrappend`, which take JSON text.

use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::client::Client;
use crate::error::Error;

fn to_json<T: Serialize + ?Sized>(value: &T) -> Result<String, Error> {
    serde_json::to_string(value)
        .map_err(|error| Error::Protocol(format!("JSON serialize failed: {error}")))
}

impl Client {
    /// Serializes `value` and sets it at `path`. `&"izmir"` is sent as `"izmir"` with quotes.
    /// `false` when the path cannot be reached.
    pub fn json_set_value<T: Serialize + ?Sized>(
        &mut self,
        key: &str,
        path: &str,
        value: &T,
    ) -> Result<bool, Error> {
        self.json_set(key, path, &to_json(value)?)
    }

    /// Serializes `patch` and sends it as a `JSON.MERGE` (RFC 7396) patch.
    pub fn json_merge_value<T: Serialize + ?Sized>(
        &mut self,
        key: &str,
        path: &str,
        patch: &T,
    ) -> Result<bool, Error> {
        self.json_merge(key, path, &to_json(patch)?)
    }

    /// Serializes `value` and appends it to the array at `path`; returns the new length.
    pub fn json_arrappend_value<T: Serialize + ?Sized>(
        &mut self,
        key: &str,
        path: &str,
        value: &T,
    ) -> Result<Option<i64>, Error> {
        self.json_arrappend(key, path, &to_json(value)?)
    }

    /// Reads a JSON value and deserializes it. `None` when the key or path is absent.
    pub fn json_get_value<T: DeserializeOwned>(
        &mut self,
        key: &str,
        path: &str,
    ) -> Result<Option<T>, Error> {
        match self.json_get(key, path)? {
            None => Ok(None),
            Some(text) => serde_json::from_str(&text)
                .map(Some)
                .map_err(|error| Error::Protocol(format!("JSON deserialize failed: {error}"))),
        }
    }
}

impl Client {
    /// Pops an array element and deserializes it. `None` for an empty array or missing key.
    pub fn json_arrpop_value<T: DeserializeOwned>(
        &mut self,
        key: &str,
        path: &str,
        index: Option<i64>,
    ) -> Result<Option<T>, Error> {
        match self.json_arrpop(key, path, index)? {
            None => Ok(None),
            Some(text) => serde_json::from_str(&text)
                .map(Some)
                .map_err(|error| Error::Protocol(format!("JSON deserialize failed: {error}"))),
        }
    }

    /// Serializes `value` and sets it only when `condition` holds (`NX` / `XX`).
    pub fn json_set_value_with<T: Serialize + ?Sized>(
        &mut self,
        key: &str,
        path: &str,
        value: &T,
        condition: crate::models::JsonSetCondition,
    ) -> Result<bool, Error> {
        self.json_set_with(key, path, &to_json(value)?, condition)
    }

    /// `JSON.FIND` for a typed value: serializes `value` (a string is sent without quotes,
    /// as the index stores it) and returns the matching keys.
    pub fn json_find_value<T: Serialize + ?Sized>(
        &mut self,
        index: &str,
        value: &T,
        count: Option<u64>,
        after: Option<&str>,
    ) -> Result<Vec<String>, Error> {
        let json = serde_json::to_value(value)
            .map_err(|error| Error::Protocol(format!("JSON serialize failed: {error}")))?;
        let text = match json {
            serde_json::Value::String(text) => text,
            other => other.to_string(),
        };

        self.json_find(index, &text, count, after)
    }
}
