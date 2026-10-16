use super::*;

#[test]
fn stores_lists_with_redis_lpush_ordering() {
    let mut shard = KvShard::new();
    let values = vec![b"first".to_vec(), b"second".to_vec()];

    assert_eq!(shard.list_push_front(b"items", &values), Ok(2));
    assert_eq!(
        shard.list_range(b"items", 0, -1).unwrap(),
        vec![&b"second"[..], &b"first"[..]]
    );
    assert_eq!(
        shard.list_pop_front(b"items").unwrap().as_deref(),
        Some(&b"second"[..])
    );
    assert_eq!(
        shard.list_pop_front(b"items").unwrap().as_deref(),
        Some(&b"first"[..])
    );
}

#[test]
fn rpush_lindex_and_ltrim_follow_redis_list_indexes() {
    let mut shard = KvShard::new();

    assert_eq!(
        shard.list_push_back(b"items", &[b"a".to_vec(), b"b".to_vec(), b"c".to_vec()]),
        Ok(3)
    );
    assert_eq!(shard.list_len(b"items"), Ok(3));
    assert_eq!(shard.list_index(b"items", 1), Ok(Some(&b"b"[..])));
    assert_eq!(shard.list_index(b"items", -1), Ok(Some(&b"c"[..])));
    assert_eq!(
        shard.list_pop_back(b"items").unwrap().as_deref(),
        Some(&b"c"[..])
    );
    assert!(shard.list_trim(b"items", 1, 1).is_ok());
    assert_eq!(shard.list_range(b"items", 0, -1).unwrap(), vec![&b"b"[..]]);
}

const L: CollectionLimits = CollectionLimits {
    set_max_intset_entries: 512,
    list_max_entries: 128,
    list_max_item_bytes: 64,
};

#[test]
fn packed_list_matches_a_deque_through_promotion_and_demotion() {
    let mut list = ListValue::default();
    let mut model: VecDeque<Vec<u8>> = VecDeque::new();
    let mut seed = 0x9e37_79b9_u32;

    for step in 0..4_000usize {
        seed ^= seed << 13;
        seed ^= seed >> 17;
        seed ^= seed << 5;
        let len = match seed % 23 {
            0 => 300,
            1 => 254,
            2 => 255,
            _ => (seed as usize >> 8) % 40,
        };
        let item = vec![(step % 251) as u8; len];
        let grow = step % 600 < 400;

        match seed % 4 {
            0 if grow => {
                list.push_front(&item, L);
                model.push_front(item);
            }
            1 if grow => {
                list.push_back(&item, L);
                model.push_back(item);
            }
            0 | 2 => assert_eq!(list.pop_front(L).map(Vec::from), model.pop_front()),
            _ => assert_eq!(list.pop_back(L).map(Vec::from), model.pop_back()),
        }

        assert_eq!(list.len(), model.len());
        assert_eq!(
            list.payload_bytes,
            model.iter().map(Vec::len).sum::<usize>()
        );

        if step % 97 == 0 {
            assert!(list.iter().eq(model.iter().map(Vec::as_slice)));

            for index in [
                0,
                model.len() / 2,
                model.len().saturating_sub(1),
                model.len(),
            ] {
                assert_eq!(list.get(index), model.get(index).map(Vec::as_slice));
            }
        }
    }
}

#[test]
fn quicklist_spans_many_nodes_and_matches_a_model() {
    let mut list = ListValue::default();
    let mut model: VecDeque<Vec<u8>> = VecDeque::new();
    let mut seed = 0x1234_5678_u32;

    for step in 0..6_000usize {
        seed ^= seed << 13;
        seed ^= seed >> 17;
        seed ^= seed << 5;
        let len = match seed % 37 {
            0 => 9_000,
            1 => 4_000,
            _ => (seed as usize >> 8) % 90,
        };
        let item = vec![(step % 251) as u8; len];

        match seed % 5 {
            0 => {
                list.push_front(&item, L);
                model.push_front(item);
            }
            1 | 2 => {
                list.push_back(&item, L);
                model.push_back(item);
            }
            3 => assert_eq!(list.pop_front(L).map(Vec::from), model.pop_front()),
            _ => assert_eq!(list.pop_back(L).map(Vec::from), model.pop_back()),
        }

        if step % 211 == 0 {
            assert_eq!(list.len(), model.len());
            assert!(list.iter().eq(model.iter().map(Vec::as_slice)));

            for index in (0..=model.len()).step_by(7) {
                assert_eq!(list.get(index), model.get(index).map(Vec::as_slice));
                assert!(
                    list.iter_from(index)
                        .eq(model.iter().skip(index).map(Vec::as_slice))
                );
            }

            let copy = list.clone();

            assert_eq!(copy, list);
            assert!(copy.capacity_bytes() <= list.capacity_bytes());
        }

        if let ListBody::Quick {
            nodes,
            counts,
            len,
            node_capacity,
        } = &list.body
        {
            assert_eq!(*len, model.len());
            assert!(
                counts
                    .iter()
                    .copied()
                    .eq(nodes.iter().map(|node| node.count))
            );
            assert_eq!(
                *node_capacity,
                nodes
                    .iter()
                    .map(|node| node.bytes.capacity())
                    .sum::<usize>()
            );
            assert!(nodes.iter().all(|node| node.count > 0));
        }
    }

    assert!(matches!(list.body, ListBody::Quick { .. }));

    let kept: Vec<Vec<u8>> = model
        .iter()
        .skip(40)
        .take(model.len() - 90)
        .cloned()
        .collect();

    list.retain_range(40, model.len() - 51, L);
    assert!(list.iter().eq(kept.iter().map(Vec::as_slice)));
    assert_eq!(list.len(), kept.len());
    assert_eq!(list.payload_bytes, kept.iter().map(Vec::len).sum::<usize>());
}

#[test]
fn packed_list_retain_range_keeps_the_middle() {
    let values: Vec<Vec<u8>> = (0..20u8).map(|n| vec![n; usize::from(n) * 20]).collect();
    let mut list = ListValue::from_values(values.clone(), L);

    assert!(matches!(list.body, ListBody::Quick { .. }));
    list.retain_range(3, 8, L);
    assert!(list.iter().eq(values[3..=8].iter().map(Vec::as_slice)));
    assert_eq!(
        list.payload_bytes,
        values[3..=8].iter().map(Vec::len).sum::<usize>()
    );

    let small: Vec<Vec<u8>> = (0..10u8).map(|n| vec![n; 3]).collect();
    let mut list = ListValue::from_values(small.clone(), L);

    assert!(matches!(list.body, ListBody::Packed { .. }));
    list.retain_range(2, 5, L);
    assert!(list.iter().eq(small[2..=5].iter().map(Vec::as_slice)));
    assert_eq!(list.len(), 4);
}

#[test]
fn memory_limits_choose_the_set_and_list_encodings() {
    let mut shard = KvShard::new();

    shard.apply_memory_limits(MemoryLimits {
        set_max_intset_entries: 0,
        list_max_compact_entries: 4,
        list_max_compact_value_bytes: 8,
        ..MemoryLimits::default()
    });

    for value in 0..200 {
        shard
            .set_add(b"ints", &[value.to_string().into_bytes()])
            .unwrap();
    }

    for value in 0..6 {
        shard
            .list_push_back(b"list", &[value.to_string().into_bytes()])
            .unwrap();
    }

    let Some(IndexRef::Object(entry)) = shard.entries.lookup(b"ints") else {
        panic!("a large set is an object");
    };
    let Value::Set(set) = &entry.value else {
        panic!("set")
    };

    assert!(
        matches!(set.body, SetBody::Arena(_)),
        "intset is switched off"
    );

    let Some(IndexRef::Object(entry)) = shard.entries.lookup(b"list") else {
        panic!("a list is an object");
    };
    let Value::List(list) = &entry.value else {
        panic!("list")
    };

    assert!(
        matches!(list.body, ListBody::Quick { .. }),
        "5th item leaves the packed form"
    );
    assert_eq!(shard.list_len(b"list"), Ok(6));
    assert_eq!(shard.set_card(b"ints"), Ok(200));
}
