use ruvio::KvShard;

#[test]
fn supports_binary_keys_and_values_end_to_end() {
    let mut shard = KvShard::with_capacity(4);
    let key = [0x00, 0xff, 0x10];
    let value = [0xde, 0xad, 0xbe, 0xef];

    assert!(shard.is_empty());
    assert_eq!(shard.insert(&key, &value), None);
    assert_eq!(shard.get(&key), Some(value.as_slice()));
    assert_eq!(shard.len(), 1);
    assert_eq!(shard.remove(&key).as_deref(), Some(value.as_slice()));
    assert!(shard.is_empty());
}

#[test]
fn keeps_owned_data_after_input_buffers_are_reused() {
    let mut shard = KvShard::new();
    let mut key = b"key".to_vec();
    let mut value = b"value".to_vec();

    shard.insert(&key, &value);
    key.fill(0);
    value.fill(0);

    assert_eq!(shard.get(b"key"), Some(&b"value"[..]));
}
