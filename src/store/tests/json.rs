use super::*;

#[test]
fn json_overwrite_reuses_document_header() {
    let mut shard = KvShard::new();
    shard.put_json(b"doc", b"{}".as_slice()).unwrap();
    let before = match &shard.entries.object_mut(b"doc").unwrap().value {
        Value::Json(value) => &**value as *const Vec<u8>,
        _ => unreachable!(),
    };
    shard
        .put_json(b"doc", br#"{"name":"Ada","score":42}"#.as_slice())
        .unwrap();
    let after = match &shard.entries.object_mut(b"doc").unwrap().value {
        Value::Json(value) => &**value as *const Vec<u8>,
        _ => unreachable!(),
    };
    assert_eq!(before, after);
    assert_eq!(
        shard.json_view(b"doc").unwrap(),
        Some(br#"{"name":"Ada","score":42}"#.as_slice())
    );
}

#[test]
fn large_json_demotes_to_compact_after_shrinking() {
    let mut shard = KvShard::new();
    let value = serde_json::to_vec(&serde_json::json!({
        "bulk": "x".repeat(crate::json::TREE_THRESHOLD), "small": 1
    }))
    .unwrap();
    shard.put_json(b"doc", value).unwrap();
    assert!(shard.json_tree(b"doc").is_some());
    shard
        .apply_json_delta(b"doc", b"$.bulk", Some(br#""short""#), false)
        .unwrap();
    assert!(shard.json_tree(b"doc").is_none());
    assert_eq!(
        shard.json_view(b"doc"),
        Ok(Some(br#"{"bulk":"short","small":1}"#.as_slice()))
    );
}

#[test]
fn structured_json_charges_and_expires_without_affecting_plain_values() {
    let clock = TestClock::new(1_000);
    let mut shard = KvShard::with_clock(clock.clone());
    let value = serde_json::to_vec(&serde_json::json!({
        "bulk": "x".repeat(crate::json::TREE_THRESHOLD), "nested": {"x": 1}
    }))
    .unwrap();
    shard.set_bytes(b"plain", b"unchanged");
    shard.put_json(b"doc", value).unwrap();
    assert!(shard.json_tree(b"doc").is_some());
    assert!(shard.expire_at(b"doc", UnixMillis::new(2_000)));
    let before = shard.entry_charge(b"doc");
    let edit = shard
        .json_tree(b"doc")
        .unwrap()
        .prepare(b"$.nested.x", Some(b"987654"), false)
        .unwrap()
        .unwrap();
    shard.apply_json_tree_edit(b"doc", edit);
    assert!(shard.entry_charge(b"doc").value_bytes >= before.value_bytes);
    let check = |shard: &KvShard<TestClock>| {
        let expected: usize = shard
            .database_maps()
            .map(|(_, map)| {
                map.entries()
                    .map(|(key, hit)| charge_of(key, hit).value_bytes)
                    .sum::<usize>()
            })
            .sum();
        assert_eq!(shard.memory_usage().value_bytes, expected);
    };
    check(&shard);
    assert_eq!(shard.pttl(b"doc"), 1_000);
    let snapshots = shard.snapshot_entries();
    let mut restored = KvShard::with_clock(clock.clone());
    for snapshot in snapshots {
        restored.restore_snapshot_entry(snapshot).unwrap();
    }
    assert!(restored.json_tree(b"doc").is_some());
    check(&restored);
    clock.set(2_000);
    assert_eq!(restored.pttl(b"doc"), -2);
    assert!(restored.json_tree(b"doc").is_none());
    assert_eq!(restored.get(b"plain"), Some(b"unchanged".as_slice()));
    check(&restored);
}
