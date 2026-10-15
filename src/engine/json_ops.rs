//! Single-key JSON commands built on the stored document: reads answer from the selected
//! value, writes compute a replacement and run it as an ordinary path `JSON.SET`, so the
//! WAL record, TTL handling and replica replay are the ones `JSON.SET` already has.

use std::borrow::Cow;

use serde_json::value::RawValue;

use super::response::Response;
use crate::protocol::{Bytes, JsonOpKind};

pub(super) enum Reply {
    Integer(i64),
    Bulk(Vec<u8>),
}

pub(super) enum Plan {
    Respond(Response<'static>),
    /// Replace the selected value with these JSON bytes, then answer with the reply.
    Write {
        value: Vec<u8>,
        reply: Reply,
    },
    /// Remove element `index` of the selected array, then answer with `popped`.
    Delete {
        index: usize,
        popped: Vec<u8>,
    },
}

const NOT_STRING: &[u8] = b"WRONGTYPE JSON value is not a string";
const NOT_ARRAY: &[u8] = b"WRONGTYPE JSON value is not an array";
const NOT_OBJECT: &[u8] = b"WRONGTYPE JSON value is not an object";
const NOT_NUMBER: &[u8] = b"WRONGTYPE JSON value is not a number";
const NOT_BOOL: &[u8] = b"WRONGTYPE JSON value is not a boolean";

fn error(message: &'static [u8]) -> Plan {
    Plan::Respond(Response::Error(message))
}

pub(super) fn plan(op: JsonOpKind, value: &[u8], args: &[Bytes<'_>]) -> Plan {
    let first = value.first().copied().unwrap_or(b'n');
    match op {
        JsonOpKind::Type => {
            let name: &'static [u8] = match first {
                b'{' => b"object",
                b'[' => b"array",
                b'"' => b"string",
                b't' | b'f' => b"boolean",
                b'n' => b"null",
                _ if value.iter().any(|byte| matches!(byte, b'.' | b'e' | b'E')) => b"number",
                _ => b"integer",
            };
            Plan::Respond(Response::Bulk(Some(Cow::Borrowed(name))))
        }
        JsonOpKind::StrLen => match serde_json::from_slice::<String>(value) {
            Ok(text) if first == b'"' => Plan::Respond(Response::Integer(text.len() as i64)),
            _ => error(NOT_STRING),
        },
        JsonOpKind::ArrLen => {
            if first == b'[' {
                Plan::Respond(Response::Integer(
                    crate::json::count_array_items(value) as i64
                ))
            } else {
                error(NOT_ARRAY)
            }
        }
        JsonOpKind::ObjKeys => {
            if first != b'{' {
                return error(NOT_OBJECT);
            }

            Plan::Respond(Response::Array(
                crate::json::object_members(value)
                    .map(|(key, _)| Some(Cow::Owned(decode_key(&value[key]))))
                    .collect(),
            ))
        }
        JsonOpKind::NumIncrBy | JsonOpKind::NumMultBy => {
            let multiply = op == JsonOpKind::NumMultBy;
            let (Ok(old), Some(Ok(by))) = (
                serde_json::from_slice::<serde_json::Number>(value),
                args.first()
                    .map(|arg| serde_json::from_slice::<serde_json::Number>(arg)),
            ) else {
                return error(NOT_NUMBER);
            };
            let exact = match (old.as_i64(), by.as_i64()) {
                (Some(a), Some(b)) if multiply => a.checked_mul(b),
                (Some(a), Some(b)) => a.checked_add(b),
                _ => None,
            }
            .map(serde_json::Number::from);
            let sum = exact.or_else(|| {
                let (a, b) = (old.as_f64()?, by.as_f64()?);

                serde_json::Number::from_f64(if multiply { a * b } else { a + b })
            });
            match sum {
                Some(sum) => {
                    let text = sum.to_string().into_bytes();
                    Plan::Write {
                        value: text.clone(),
                        reply: Reply::Bulk(text),
                    }
                }
                None => error(b"ERR result is not a finite number"),
            }
        }
        JsonOpKind::ObjLen => {
            if first != b'{' {
                return error(NOT_OBJECT);
            }

            Plan::Respond(Response::Integer(
                crate::json::object_members(value).count() as i64,
            ))
        }
        JsonOpKind::StrAppend => {
            let (Ok(mut text), Some(Ok(suffix))) = (
                serde_json::from_slice::<String>(value),
                args.first()
                    .map(|arg| serde_json::from_slice::<String>(arg)),
            ) else {
                return error(NOT_STRING);
            };

            text.push_str(&suffix);

            Plan::Write {
                value: serde_json::to_vec(&text).expect("a string serializes"),
                reply: Reply::Integer(text.len() as i64),
            }
        }
        JsonOpKind::ArrInsert => {
            if first != b'[' {
                return error(NOT_ARRAY);
            }
            let length = crate::json::count_array_items(value);
            let Some(position) = array_position(&args[0], length) else {
                return error(b"ERR value is not an integer or out of range");
            };
            if position > length {
                return error(INDEX_RANGE);
            }

            let mut inserted = Vec::with_capacity(args.len() - 1);
            for arg in &args[1..] {
                let Ok(canonical) = crate::json::set_root(arg) else {
                    return error(b"ERR invalid JSON value");
                };
                inserted.push(canonical);
            }

            let mut joined = Vec::with_capacity(value.len() + 16);
            joined.push(b'[');
            let mut wrote = false;
            let mut push = |bytes: &[u8]| {
                if wrote {
                    joined.push(b',');
                }
                wrote = true;
                joined.extend_from_slice(bytes);
            };
            let mut items = crate::json::array_items(value);
            for item in items.by_ref().take(position) {
                push(&value[item]);
            }
            for canonical in &inserted {
                push(canonical);
            }
            for item in items {
                push(&value[item]);
            }
            joined.push(b']');
            Plan::Write {
                value: joined,
                reply: Reply::Integer((length + inserted.len()) as i64),
            }
        }
        JsonOpKind::ArrTrim => {
            if first != b'[' {
                return error(NOT_ARRAY);
            }
            let (Some(start), Some(stop)) = (parse_index(&args[0]), parse_index(&args[1])) else {
                return error(b"ERR value is not an integer or out of range");
            };
            let length = crate::json::count_array_items(value) as i64;
            let start = if start < 0 {
                (length + start).max(0)
            } else {
                start
            };
            let stop = if stop < 0 {
                length + stop
            } else {
                stop.min(length - 1)
            };
            let mut joined = Vec::with_capacity(value.len());
            joined.push(b'[');
            let mut kept = 0usize;
            if start < length && start <= stop && stop >= 0 {
                for item in crate::json::array_items(value)
                    .skip(start as usize)
                    .take((stop - start + 1) as usize)
                {
                    if kept > 0 {
                        joined.push(b',');
                    }
                    kept += 1;
                    joined.extend_from_slice(&value[item]);
                }
            }
            joined.push(b']');
            Plan::Write {
                value: joined,
                reply: Reply::Integer(kept as i64),
            }
        }
        JsonOpKind::ArrIndex => {
            if first != b'[' {
                return error(NOT_ARRAY);
            }
            let Ok(target) = serde_json::from_slice::<serde_json::Value>(&args[0]) else {
                return error(b"ERR invalid JSON value");
            };
            if target.is_array() || target.is_object() {
                return error(b"ERR JSON value is not a scalar");
            }
            let length = crate::json::count_array_items(value) as i64;
            let bound = |position: Option<&Bytes<'_>>, default: i64| -> Option<i64> {
                let Some(raw) = position else {
                    return Some(default);
                };
                let index = parse_index(raw)?;

                Some(if index < 0 {
                    (length + index).max(0)
                } else {
                    index
                })
            };
            let (Some(start), Some(stop)) = (bound(args.get(1), 0), bound(args.get(2), 0)) else {
                return error(b"ERR value is not an integer or out of range");
            };
            // A stop of 0 means the end of the array.
            let stop = if stop == 0 { length } else { stop.min(length) };
            // Stored values are canonical, so equal scalars have equal text. Only a form that
            // canonical text never takes (an exponent, an escape) is parsed to compare.
            let canonical = target.to_string().into_bytes();
            let class = target_class(&target);
            let found = crate::json::array_items(value)
                .enumerate()
                .skip(start as usize)
                .take_while(|(index, _)| (*index as i64) < stop)
                .find(|(_, item)| {
                    let raw = &value[item.clone()];

                    raw == canonical.as_slice()
                        || (raw_class(raw) == class
                            && raw.iter().any(|byte| matches!(byte, b'e' | b'E' | b'\\'))
                            && serde_json::from_slice::<serde_json::Value>(raw)
                                .is_ok_and(|parsed| parsed == target))
                })
                .map(|(index, _)| index as i64);
            Plan::Respond(Response::Integer(found.unwrap_or(-1)))
        }
        JsonOpKind::ArrPop => {
            if first != b'[' {
                return error(NOT_ARRAY);
            }
            let length = crate::json::count_array_items(value) as i64;
            if length == 0 {
                return Plan::Respond(Response::Bulk(None));
            }
            let Some(index) = args.first().map_or(Some(-1), |raw| parse_index(raw)) else {
                return error(b"ERR value is not an integer or out of range");
            };
            let index =
                (if index < 0 { length + index } else { index }).clamp(0, length - 1) as usize;
            // One scan to the element; the removal itself is a small `JSON.DEL` of its path.
            let popped = crate::json::array_items(value)
                .nth(index)
                .map_or(&value[0..0], |item| &value[item]);
            Plan::Delete {
                index,
                popped: popped.to_vec(),
            }
        }
        JsonOpKind::Toggle => match first {
            b't' | b'f' => {
                let now = first == b'f';
                Plan::Write {
                    value: if now {
                        b"true".to_vec()
                    } else {
                        b"false".to_vec()
                    },
                    reply: Reply::Integer(i64::from(now)),
                }
            }
            _ => error(NOT_BOOL),
        },
        JsonOpKind::ArrAppend => {
            let Ok(items) = serde_json::from_slice::<Vec<&RawValue>>(value) else {
                return error(NOT_ARRAY);
            };
            if first != b'[' || args.is_empty() {
                return error(NOT_ARRAY);
            }
            let mut joined = Vec::with_capacity(value.len() + 16);
            joined.extend_from_slice(&value[..value.len() - 1]);
            for arg in args {
                let Ok(canonical) = crate::json::set_root(arg) else {
                    return error(b"ERR invalid JSON value");
                };
                if !items.is_empty() || joined.len() > 1 {
                    joined.push(b',');
                }
                joined.extend_from_slice(&canonical);
            }
            joined.push(b']');
            Plan::Write {
                value: joined,
                reply: Reply::Integer((items.len() + args.len()) as i64),
            }
        }
        JsonOpKind::Clear => {
            let empty: Option<&[u8]> = match first {
                b'[' => Some(b"[]"),
                b'{' => Some(b"{}"),
                b'-' | b'0'..=b'9' => Some(b"0"),
                _ => None,
            };
            match empty {
                Some(empty) if value != empty => Plan::Write {
                    value: empty.to_vec(),
                    reply: Reply::Integer(1),
                },
                _ => Plan::Respond(Response::Integer(0)),
            }
        }
    }
}

const INDEX_RANGE: &[u8] = b"ERR index out of range";

fn parse_index(raw: &[u8]) -> Option<i64> {
    std::str::from_utf8(raw).ok()?.parse().ok()
}

/// An insert position: a negative index counts from the end.
fn array_position(raw: &[u8], length: usize) -> Option<usize> {
    let index = parse_index(raw)?;
    let position = if index < 0 {
        length as i64 + index
    } else {
        index
    };

    // A position before the start can never be in range; report it as one past the end.
    Some(usize::try_from(position).unwrap_or(usize::MAX))
}

/// An object key without its quotes; a key with escapes is decoded.
fn decode_key(raw: &[u8]) -> Vec<u8> {
    let inner = &raw[1..raw.len() - 1];

    if inner.contains(&b'\\')
        && let Ok(decoded) = serde_json::from_slice::<String>(raw)
    {
        return decoded.into_bytes();
    }

    inner.to_vec()
}

/// What kind of scalar a value is, from its first byte: the cheap filter before comparing.
fn raw_class(raw: &[u8]) -> u8 {
    match raw.first() {
        Some(b'"') => b's',
        Some(b't' | b'f') => b'b',
        Some(b'n') => b'n',
        Some(b'{' | b'[') => b'c',
        _ => b'0',
    }
}

fn target_class(target: &serde_json::Value) -> u8 {
    match target {
        serde_json::Value::String(_) => b's',
        serde_json::Value::Bool(_) => b'b',
        serde_json::Value::Null => b'n',
        _ => b'0',
    }
}

impl Reply {
    pub(super) fn into_response(self) -> Response<'static> {
        match self {
            Self::Integer(value) => Response::Integer(value),
            Self::Bulk(bytes) => Response::Bulk(Some(Cow::Owned(bytes))),
        }
    }
}
