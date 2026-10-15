use std::collections::HashMap;

use proptest::prelude::*;
use ruvio::KvShard;

proptest! {
    #[test]
    fn byte_value_mutations_match_a_reference_map(
        operations in proptest::collection::vec((any::<bool>(), any::<u8>(), any::<u8>()), 0..256)
    ) {
        let mut shard = KvShard::new();
        let mut reference = HashMap::new();


        for (insert, key, value) in operations {
            let key = [key];

            if insert {
                shard.set_bytes(&key, &[value]);
                reference.insert(key, vec![value]);
            } else {
                shard.delete(&key);
                reference.remove(&key);
            }


            for (key, expected) in &reference {
                prop_assert_eq!(shard.get_bytes(key).unwrap(), Some(expected.as_slice()));
            }

            prop_assert_eq!(shard.memory_usage().entries, reference.len());
        }
    }
}

proptest! {
    #[test]
    fn every_value_kind_rejects_an_incompatible_transition(kind in 0u8..7) {
        let mut shard = KvShard::new();

        match kind {
            0 => {
                shard.set_bytes(b"key", b"value");
                prop_assert!(shard.list_push_front(b"key", &[b"item"]).is_err());
            }

            1 => {
                shard.increment(b"key", 1).unwrap();
                prop_assert!(shard.set_add(b"key", &[b"member"]).is_err());
            }

            2 => {
                shard.list_push_front(b"key", &[b"item"]).unwrap();
                prop_assert!(shard.get_bytes(b"key").is_err());
            }

            3 => {
                shard.set_add(b"key", &[b"member"]).unwrap();
                prop_assert!(shard.get_bytes(b"key").is_err());
            }

            4 => {
                shard.sorted_set_add(b"key", 1.0, b"member").unwrap();
                prop_assert!(shard.get_bytes(b"key").is_err());
            }

            5 => {
                shard.hash_set(b"key", &[(b"f".as_slice(), b"v".as_slice())]).unwrap();
                prop_assert!(shard.get_bytes(b"key").is_err());
            }

            _ => {
                shard.bloom_add(b"key", b"member").unwrap();
                prop_assert!(shard.get_bytes(b"key").is_err());
            }
        }
    }
}

proptest! {
    #[test]
    fn list_set_sorted_set_and_bloom_preserve_core_invariants(
        values in proptest::collection::vec(any::<u8>(), 1..64)
    ) {
        let mut shard = KvShard::new();


        for value in &values {
            shard.list_push_front(b"list", &[[*value]]).unwrap();
            shard.set_add(b"set", &[[*value]]).unwrap();
            shard
                .sorted_set_add(b"sorted", f64::from(*value), &[*value])
                .unwrap();
            shard.bloom_add(b"bloom", &[*value]).unwrap();
            let field = [*value];

            shard.hash_set(b"hash", &[(&field, &field)]).unwrap();
        }


        let expected_list: Vec<_> = values.iter().rev().copied().collect();
        let actual_list: Vec<_> = shard
            .list_range(b"list", 0, -1)
            .unwrap()
            .into_iter()
            .map(|value| value[0])
            .collect();

        prop_assert_eq!(actual_list, expected_list);

        for value in &values {
            prop_assert!(shard.set_contains(b"set", &[*value]).unwrap());
            prop_assert!(shard.bloom_contains(b"bloom", &[*value]).unwrap());
            prop_assert_eq!(shard.hash_get(b"hash", &[*value]).unwrap(), Some(&[*value][..]));
        }


        let mut expected_sorted = values.clone();

        expected_sorted.sort_unstable();
        expected_sorted.dedup();
        let actual_sorted: Vec<_> = shard
            .sorted_set_range(b"sorted", 0, -1)
            .unwrap()
            .into_iter()
            .map(|value| value[0])
            .collect();

        prop_assert_eq!(actual_sorted, expected_sorted);
    }
}
