use super::*;

#[test]
fn hash_family_uses_compact_pairs_until_promotion() {
    let mut shard = KvShard::new();

    assert_eq!(
        shard.hash_set(b"user", &[(b"name".as_slice(), b"ada".as_slice())]),
        Ok(1)
    );
    assert_eq!(
        shard.hash_set(b"user", &[(b"name".as_slice(), b"grace".as_slice())]),
        Ok(0)
    );
    assert_eq!(shard.hash_get(b"user", b"name"), Ok(Some(&b"grace"[..])));
    assert_eq!(shard.hash_exists(b"user", b"name"), Ok(true));
    assert_eq!(shard.hash_len(b"user"), Ok(1));
    assert_eq!(shard.hash_is_compact(b"user"), Ok(Some(true)));
    assert_eq!(
        shard.hash_set(
            b"user",
            &[
                (b"city".as_slice(), b"london".as_slice()),
                (b"lang".as_slice(), b"rust".as_slice()),
            ]
        ),
        Ok(2)
    );
    assert_eq!(
        shard.hash_get_all(b"user").unwrap(),
        vec![
            (&b"city"[..], &b"london"[..]),
            (&b"lang"[..], &b"rust"[..]),
            (&b"name"[..], &b"grace"[..]),
        ]
    );
    assert_eq!(
        shard.hash_mget(b"user", &[b"lang".as_slice(), b"missing".as_slice()]),
        Ok(vec![Some(&b"rust"[..]), None])
    );
    assert_eq!(shard.hash_delete(b"user", &[b"city".as_slice()]), Ok(1));
    assert_eq!(
        shard.hash_keys(b"user").unwrap(),
        vec![&b"lang"[..], &b"name"[..]]
    );

    let large = vec![b'x'; MemoryLimits::default().hash_max_compact_value_bytes + 1];

    assert_eq!(
        shard.hash_set(b"user", &[(b"bio".as_slice(), large.as_slice())]),
        Ok(1)
    );
    assert_eq!(shard.hash_is_compact(b"user"), Ok(Some(false)));
    assert_eq!(shard.hash_delete(b"user", &[b"bio".as_slice()]), Ok(1));
    assert_eq!(shard.hash_is_compact(b"user"), Ok(Some(true)));

    let mut many = Vec::new();
    let compact_entries = MemoryLimits::default().hash_max_compact_entries;

    for index in 0..=compact_entries {
        let field = format!("f{index:02}");

        many.push((field, b"v".to_vec()));
    }

    let refs: Vec<(&[u8], &[u8])> = many
        .iter()
        .map(|(field, value)| (field.as_bytes(), value.as_slice()))
        .collect();

    assert_eq!(shard.hash_set(b"wide", &refs), Ok(compact_entries + 1));
    assert_eq!(shard.hash_is_compact(b"wide"), Ok(Some(false)));
    shard.set_bytes(b"string", b"value");
    assert_eq!(
        shard.hash_set(b"string", &[(b"a".as_slice(), b"b".as_slice())]),
        Err(WrongType)
    );
    assert_eq!(shard.hash_get(b"string", b"a"), Err(WrongType));
}

#[test]
fn arena_hash_matches_a_map_through_overwrites_removals_and_demotion() {
    use std::collections::BTreeMap;

    let limits = CompactLimits {
        max_entries: 16,
        max_value_bytes: 32,
    };
    let mut hash = HashValue::default();
    let mut model: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();
    let mut seed = 0x9e37_79b9_u32;
    let mut saw_arena = false;

    for step in 0..40_000usize {
        seed ^= seed << 13;
        seed ^= seed >> 17;
        seed ^= seed << 5;
        let field = format!("field-{}", seed % 900).into_bytes();
        let value = vec![(step % 251) as u8; (seed as usize >> 12) % 80];
        let growing = (step / 2_000) % 2 == 0;

        if growing || seed.is_multiple_of(7) {
            let fresh = model.insert(field.clone(), value.clone()).is_none();

            assert_eq!(hash.insert(&field, &value, limits), fresh);
        } else {
            assert_eq!(hash.remove(&field, limits), model.remove(&field).is_some());
        }

        saw_arena |= matches!(hash.inner, HashInner::Arena(_));
        assert_eq!(hash.len(), model.len());
        assert_eq!(
            hash.payload_bytes(),
            model.iter().map(|(f, v)| f.len() + v.len()).sum::<usize>()
        );

        if step % 199 == 0 {
            let mut all: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();

            hash.visit_pairs(|(f, v)| all.push((f.to_vec(), v.to_vec())));
            all.sort();
            assert!(
                all.iter()
                    .map(|(f, v)| (&f[..], &v[..]))
                    .eq(model.iter().map(|(f, v)| (&f[..], &v[..])))
            );

            for (field, value) in model.iter().take(25) {
                assert_eq!(hash.get(field), Some(value.as_slice()));
            }

            assert_eq!(hash.get(b"absent"), None);
        }
    }

    assert!(saw_arena);
}

#[test]
fn packed_hash_overwrites_removes_and_promotes() {
    let limits = CompactLimits {
        max_entries: 8,
        max_value_bytes: 300,
    };
    let mut hash = HashValue::default();

    assert!(hash.insert(b"a", b"1", limits));
    assert!(hash.insert(b"b", &[7; 260], limits));
    assert!(!hash.insert(b"a", b"22", limits));
    assert!(!hash.insert(b"b", b"x", limits));
    assert_eq!(hash.get(b"a"), Some(&b"22"[..]));
    assert_eq!(hash.get(b"b"), Some(&b"x"[..]));
    assert_eq!(hash.payload_bytes(), 1 + 2 + 1 + 1);
    assert!(hash.remove(b"a", limits));
    assert!(!hash.contains(b"a"));
    assert!(hash.is_compact());

    for n in 0..10u8 {
        hash.insert(&[b'f', n], &[n], limits);
    }

    assert!(!hash.is_compact());
    assert_eq!(hash.len(), 11);

    for n in 0..5u8 {
        hash.remove(&[b'f', n], limits);
    }

    assert!(hash.is_compact());
    assert_eq!(hash.get(b"f\x07"), Some(&[7u8][..]));
    assert_eq!(hash.iter_sorted().len(), 6);
    assert!(!hash.insert(b"b", &[1; 301], limits));
    assert!(!hash.is_compact());
    assert_eq!(hash.get(b"b").map(<[u8]>::len), Some(301));
}

#[test]
fn hash_scan_walks_every_field_and_filters_by_pattern() {
    let mut shard = KvShard::new();
    for id in 0..512 {
        shard
            .hash_set(b"hash", &[(format!("match:{id:03}"), "v")])
            .unwrap();
        shard
            .hash_set(b"hash", &[(format!("other:{id:03}"), "v")])
            .unwrap();
    }

    for count in [0, 1, 7, 60, 3000] {
        let mut seen = std::collections::HashSet::new();
        let mut cursor = 0;
        loop {
            let (next, pairs) = shard
                .hash_scan(b"hash", cursor, count, Some(b"match:*"))
                .unwrap();
            assert!(
                pairs
                    .iter()
                    .all(|(field, value)| field.starts_with(b"match:") && *value == b"v")
            );
            seen.extend(pairs.iter().map(|(field, _)| field.to_vec()));
            cursor = next;
            if cursor == 0 {
                break;
            }
        }
        assert_eq!(seen.len(), 512, "count={count}");
    }
}

#[test]
fn hash_random_fields_are_distinct_or_repeat_by_sign() {
    let mut shard = KvShard::new();
    for id in 0..400 {
        shard
            .hash_set(b"h", &[(format!("f{id:03}"), format!("v{id:03}"))])
            .unwrap();
    }

    assert_eq!(shard.hash_random_pairs(b"h", None).unwrap().len(), 1);
    assert!(shard.hash_random_pairs(b"h", Some(0)).unwrap().is_empty());
    assert_eq!(
        shard.hash_random_pairs(b"h", Some(-700)).unwrap().len(),
        700
    );

    let picked = shard.hash_random_pairs(b"h", Some(50)).unwrap();
    let unique: std::collections::HashSet<_> = picked.iter().map(|(field, _)| field).collect();
    assert_eq!(unique.len(), 50);
    assert!(picked.iter().all(|(field, value)| field[1..] == value[1..]));
    assert_eq!(shard.hash_random_pairs(b"h", Some(900)).unwrap().len(), 400);
}

#[test]
fn hash_compact_threshold_follows_the_shard_limit() {
    let mut shard = KvShard::new();
    let limits = MemoryLimits {
        hash_max_compact_entries: 2,
        hash_max_compact_value_bytes: 8,
        ..MemoryLimits::default()
    };

    shard.apply_memory_limits(limits);
    assert_eq!(
        shard.hash_set(
            b"h",
            &[
                (b"a".as_slice(), b"1".as_slice()),
                (b"b".as_slice(), b"2".as_slice())
            ]
        ),
        Ok(2)
    );
    assert_eq!(shard.hash_is_compact(b"h"), Ok(Some(true)));
    assert_eq!(
        shard.hash_set(b"h", &[(b"c".as_slice(), b"3".as_slice())]),
        Ok(1)
    );
    assert_eq!(shard.hash_is_compact(b"h"), Ok(Some(false)));
    assert_eq!(
        shard.hash_set(b"wide", &[(b"f".as_slice(), b"012345678".as_slice())]),
        Ok(1)
    );
    assert_eq!(shard.hash_is_compact(b"wide"), Ok(Some(false)));
}

#[test]
fn hash_order_pages_end_and_miss_nothing_while_the_table_changes() {
    let mut members: ahash::AHashSet<Box<[u8]>> = (0..5_000u32)
        .map(|index| format!("m{index}").into_bytes().into())
        .collect();
    let hasher = members.hasher().clone();
    let (mut low, mut high) = (0u32, 5_000u32);
    let mut seen = std::collections::HashSet::new();
    let mut cursor = 0;
    let mut pages = 0;

    loop {
        let (next, page) = scan_by_hash(
            members.iter().map(|member| member.to_vec()),
            |member| hasher.hash_one(member.as_slice()),
            cursor,
            100,
        );

        seen.extend(page);
        pages += 1;
        assert!(pages < 1_000, "the walk did not end");
        if next == 0 {
            break;
        }
        cursor = next;

        // A rolling window, and a growth that rehashes the table, between pages.
        for _ in 0..50 {
            members.insert(format!("m{high}").into_bytes().into());
            members.remove(format!("m{low}").as_bytes());
            high += 1;
            low += 1;
        }
        if pages == 5 {
            members.extend((0..20_000u32).map(|index| format!("x{index}").into_bytes().into()));
        }
    }

    for index in low..5_000 {
        assert!(
            seen.contains(format!("m{index}").as_bytes()),
            "m{index} was skipped"
        );
    }
    assert!(pages > 10);
}
