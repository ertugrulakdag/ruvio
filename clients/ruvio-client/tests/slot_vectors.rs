//! The key to hash slot vectors in `clients/testdata/slots.json` are shared by the server and
//! every client.

use std::path::Path;

#[test]
fn hash_slot_matches_the_shared_vectors() {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("../testdata/slots.json");
    let text = std::fs::read_to_string(&path).expect("slots.json");
    let vectors: serde_json::Value = serde_json::from_str(&text).expect("slots.json is JSON");

    for entry in vectors["keys"].as_array().expect("keys") {
        let key = entry["key"].as_str().expect("key");
        let slot = entry["slot"].as_u64().expect("slot");

        assert_eq!(u64::from(ruvio_client::hash_slot(key)), slot, "{key:?}");
        assert_eq!(
            u64::from(ruvio_client::hash_slot_bytes(key.as_bytes())),
            slot,
            "{key:?}"
        );
    }
}
