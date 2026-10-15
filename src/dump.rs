//! `DUMP` / `RESTORE` payloads: `"RVD1"`, then one WAL record frame holding the value as
//! the mutation that would rebuild it. The frame carries its own length and CRC32C, so a
//! truncated or flipped payload is refused. Only Ruvio can read these; they are not RDB.

use crate::mutation::OwnedMutation;
use crate::persistence::{decode_dump_record, encode_dump_record};

const MAGIC: &[u8; 4] = b"RVD1";

pub const BAD_PAYLOAD: &[u8] = b"ERR DUMP payload version or checksum are wrong";

/// The value mutation (written for the empty key) as a payload.
pub fn encode(value: &OwnedMutation) -> Option<Vec<u8>> {
    let frame = encode_dump_record(value).ok()?;
    let mut payload = Vec::with_capacity(MAGIC.len() + frame.len());

    payload.extend_from_slice(MAGIC);
    payload.extend_from_slice(&frame);
    Some(payload)
}

pub fn decode(payload: &[u8]) -> Result<OwnedMutation, &'static [u8]> {
    let frame = payload.strip_prefix(MAGIC.as_slice()).ok_or(BAD_PAYLOAD)?;

    decode_dump_record(frame).map_err(|_| BAD_PAYLOAD)
}

/// The same mutation aimed at `key`; `None` for kinds a payload may not carry.
pub fn rekey(value: OwnedMutation, key: &[u8]) -> Option<OwnedMutation> {
    let key = key.to_vec();

    Some(match value {
        OwnedMutation::SetBytes { value, .. } => OwnedMutation::SetBytes { key, value },
        OwnedMutation::JsonPut { value, .. } => OwnedMutation::JsonPut { key, value },
        OwnedMutation::ListPushBack { values, .. } => OwnedMutation::ListPushBack { key, values },
        OwnedMutation::SetStore { members, .. } => OwnedMutation::SetStore { key, members },
        OwnedMutation::HashSet { pairs, .. } => OwnedMutation::HashSet { key, pairs },
        OwnedMutation::SortedSetAdd { pairs, .. } => OwnedMutation::SortedSetAdd { key, pairs },
        _ => return None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> OwnedMutation {
        OwnedMutation::HashSet {
            key: Vec::new(),
            pairs: vec![(b"f".to_vec(), b"v".to_vec())],
        }
    }

    #[test]
    fn round_trips_and_rekeys() {
        let payload = encode(&sample()).unwrap();
        let back = decode(&payload).unwrap();

        assert_eq!(
            rekey(back, b"k"),
            Some(OwnedMutation::HashSet {
                key: b"k".to_vec(),
                pairs: vec![(b"f".to_vec(), b"v".to_vec())],
            })
        );
    }

    #[test]
    fn refuses_flipped_truncated_and_foreign_payloads() {
        let payload = encode(&sample()).unwrap();
        let mut flipped = payload.clone();
        let last = flipped.len() - 1;

        flipped[last / 2 + 3] ^= 0x40;
        assert_eq!(decode(&flipped), Err(BAD_PAYLOAD));
        assert_eq!(decode(&payload[..payload.len() - 1]), Err(BAD_PAYLOAD));
        assert_eq!(decode(b"RVD2whatever"), Err(BAD_PAYLOAD));
        assert_eq!(decode(b""), Err(BAD_PAYLOAD));
    }

    #[test]
    fn other_mutation_kinds_cannot_be_rekeyed() {
        assert_eq!(
            rekey(OwnedMutation::Delete { keys: Vec::new() }, b"k"),
            None
        );
    }
}
