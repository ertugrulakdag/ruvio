use zerocopy::{
    FromBytes, Immutable, KnownLayout, Ref,
    byteorder::{NetworkEndian, U32},
};

#[repr(C)]
#[derive(FromBytes, Immutable, KnownLayout, Copy, Clone, Debug, PartialEq, Eq)]
struct WireRecord {
    // Zerocopy byte-order integers have alignment 1, so arbitrary network buffers are valid.
    key_len: U32<NetworkEndian>,
    value_len: U32<NetworkEndian>,
}

#[derive(Debug, PartialEq, Eq)]
struct RecordView<'a> {
    header: &'a WireRecord,
    key: &'a [u8],
    value: &'a [u8],
}

fn main() {
    // Header (3-byte key, 5-byte value) and payload live in one receive-like buffer.
    let frame = b"\0\0\0\x03\0\0\0\x05keyvalue";
    let record = decode_frame(frame).expect("valid frame");

    println!("key={:?} value={:?}", record.key, record.value);
}

fn decode_frame(bytes: &[u8]) -> Option<RecordView<'_>> {
    // `Ref` exposes the header directly from `bytes`; no decoded header is allocated or copied.
    let (header, payload) = Ref::<_, WireRecord>::from_prefix(bytes).ok()?;
    let header = Ref::into_ref(header);
    let key_len = usize::try_from(header.key_len.get()).ok()?;
    let value_len = usize::try_from(header.value_len.get()).ok()?;
    let frame_len = key_len.checked_add(value_len)?;

    if payload.len() != frame_len {
        return None;
    }

    let (key, value) = payload.split_at(key_len);

    Some(RecordView { header, key, value })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decodes_views_into_the_original_buffer() {
        let frame = b"\0\0\0\x03\0\0\0\x05keyvalue";
        let record = decode_frame(frame).expect("valid frame");

        assert_eq!(record.header.key_len.get(), 3);
        assert_eq!(record.key, b"key");
        assert_eq!(record.value, b"value");
        assert_eq!(record.key.as_ptr(), frame[8..].as_ptr());
    }

    #[test]
    fn rejects_truncated_payloads() {
        let frame = b"\0\0\0\x03\0\0\0\x05key";

        assert!(decode_frame(frame).is_none());
    }
}
