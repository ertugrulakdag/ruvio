//! The RESP wire vectors in `clients/testdata/resp.json` are shared by every client, so the six
//! encoders and parsers cannot drift apart.

use std::io::{Cursor, ErrorKind};
use std::path::Path;

use ruvio_client::__bench::{encode, encode_bytes, parse};
use ruvio_client::{Error, RespValue};
use serde_json::Value;

fn vectors() -> Value {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("../testdata/resp.json");
    let text = std::fs::read_to_string(&path).expect("resp.json");
    serde_json::from_str(&text).expect("resp.json is JSON")
}

fn unhex(text: &str) -> Vec<u8> {
    (0..text.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&text[i..i + 2], 16).expect("hex"))
        .collect()
}

/// The wire bytes of a vector: `bytes` (UTF-8 text) or `bytes_hex`.
fn wire(entry: &Value) -> Vec<u8> {
    match entry.get("bytes_hex") {
        Some(hex) => unhex(hex.as_str().expect("bytes_hex")),
        None => entry["bytes"].as_str().expect("bytes").as_bytes().to_vec(),
    }
}

fn expected(value: &Value) -> RespValue {
    let (kind, inner) = value
        .as_object()
        .and_then(|object| object.iter().next())
        .expect("value is a one-key object");

    match kind.as_str() {
        "simple" => RespValue::Simple(inner.as_str().expect("simple").to_owned()),
        "error" => RespValue::Error(inner.as_str().expect("error").to_owned()),
        "int" => RespValue::Integer(match inner.as_str() {
            Some(text) => text.parse().expect("int text"),
            None => inner.as_i64().expect("int"),
        }),
        "bulk" => RespValue::Bulk(inner.as_str().expect("bulk").as_bytes().to_vec()),
        "bulk_hex" => RespValue::Bulk(unhex(inner.as_str().expect("bulk_hex"))),
        "null" => RespValue::Null,
        "array" => RespValue::Array(
            inner
                .as_array()
                .expect("array")
                .iter()
                .map(expected)
                .collect(),
        ),
        other => panic!("unknown value kind {other}"),
    }
}

#[test]
fn encoder_writes_the_shared_vectors() {
    let vectors = vectors();
    let list = vectors["encode"].as_array().expect("encode");
    assert!(!list.is_empty());

    for entry in list {
        let name = entry["name"].as_str().expect("name");
        let want = wire(entry);

        let args: Vec<Vec<u8>> = match entry.get("args") {
            Some(args) => {
                let texts: Vec<&str> = args
                    .as_array()
                    .expect("args")
                    .iter()
                    .map(|a| a.as_str().expect("arg"))
                    .collect();
                let mut payload = Vec::new();
                encode(&mut payload, &texts).expect("encode");
                assert_eq!(payload, want, "{name}: text encoder");
                texts.iter().map(|t| t.as_bytes().to_vec()).collect()
            }
            None => entry["args_hex"]
                .as_array()
                .expect("args_hex")
                .iter()
                .map(|a| unhex(a.as_str().expect("arg")))
                .collect(),
        };

        let slices: Vec<&[u8]> = args.iter().map(Vec::as_slice).collect();
        let mut payload = Vec::new();
        encode_bytes(&mut payload, &slices).expect("encode");
        assert_eq!(payload, want, "{name}: binary encoder");
    }
}

#[test]
fn parser_reads_the_shared_vectors() {
    let vectors = vectors();
    let list = vectors["decode"].as_array().expect("decode");
    assert!(!list.is_empty());

    for entry in list {
        let name = entry["name"].as_str().expect("name");
        let bytes = wire(entry);
        let mut reader = Cursor::new(bytes.as_slice());

        let value = parse(&mut reader).unwrap_or_else(|e| panic!("{name}: {e:?}"));
        assert_eq!(value, expected(&entry["value"]), "{name}");
        assert_eq!(
            reader.position() as usize,
            bytes.len(),
            "{name}: bytes left over"
        );
    }
}

/// The reader blocks for more bytes; over a finished source that shows as end of input.
#[test]
fn parser_waits_for_the_rest_of_an_incomplete_reply() {
    let vectors = vectors();
    let list = vectors["incomplete"].as_array().expect("incomplete");
    assert!(!list.is_empty());

    for entry in list {
        let name = entry["name"].as_str().expect("name");
        let bytes = wire(entry);

        match parse(&mut Cursor::new(bytes.as_slice())) {
            Err(Error::Io(e)) if e.kind() == ErrorKind::UnexpectedEof => {}
            Err(Error::Protocol(message)) if message == "connection closed" => {}
            other => panic!("{name}: expected end of input, got {other:?}"),
        }
    }
}
