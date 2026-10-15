use super::*;
use crate::time::TestClock;
use std::collections::BTreeMap;

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

#[test]
fn string_value_is_not_sized_like_a_sorted_set() {
    assert_eq!(size_of::<Value>(), size_of::<BytesValue>());
    assert_eq!(size_of::<BytesValue>(), size_of::<ByteBody>());
    assert_eq!(size_of::<Entry>(), size_of::<Value>());
    assert_eq!(size_of::<ByteBody>(), 16);
    assert!(size_of::<Value>() < size_of::<SortedSet>());
}

/// Native runs use thousands of entries to show the cost curve. Miri
/// interprets every step, so it keeps the same 4× ratio at a small size.
fn benchmark_counts() -> (usize, usize) {
    if cfg!(miri) {
        (32, 128)
    } else {
        (2_000, 8_000)
    }
}

/// Enough entries to grow past the reclaim threshold, then a delete that
/// leaves the table under a quarter full. Miri stays just above that threshold.
fn shrink_counts() -> (usize, usize) {
    if cfg!(miri) {
        (128, 120)
    } else {
        (4_000, 3_900)
    }
}

fn compact_hash_counts() -> (usize, usize) {
    if cfg!(miri) {
        (128, 120)
    } else {
        (2_000, 1_900)
    }
}

#[test]
fn insert_replaces_and_remove_returns_owned_values() {
    let mut shard = KvShard::new();

    assert_eq!(shard.insert(b"answer", b"41"), None);
    assert_eq!(shard.insert(b"answer", b"42").as_deref(), Some(&b"41"[..]));
    assert_eq!(shard.get(b"answer"), Some(&b"42"[..]));
    assert_eq!(shard.remove(b"answer").as_deref(), Some(&b"42"[..]));
    assert!(shard.is_empty());
}

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

#[test]
fn set_add_counts_only_new_members() {
    let mut shard = KvShard::new();
    let members = vec![b"a".to_vec(), b"b".to_vec(), b"a".to_vec()];

    assert_eq!(shard.set_add(b"members", &members), Ok(2));
    assert_eq!(shard.set_contains(b"members", b"b"), Ok(true));
    assert_eq!(shard.set_add(b"members", &[b"b".to_vec()]), Ok(0));
    assert_eq!(shard.set_remove(b"members", &[b"a".to_vec()]), Ok(1));
    assert_eq!(shard.set_contains(b"members", b"a"), Ok(false));
    assert_eq!(
        shard.set_add(b"left", &[b"a".to_vec(), b"b".to_vec()]),
        Ok(2)
    );
    assert_eq!(
        shard.set_add(b"right", &[b"b".to_vec(), b"c".to_vec()]),
        Ok(2)
    );
    assert_eq!(shard.set_card(b"left"), Ok(2));
    assert_eq!(
        shard.set_members(b"left").unwrap(),
        vec![&b"a"[..], &b"b"[..]]
    );
    assert_eq!(shard.set_store(b"out", &[b"b".to_vec()]), 1);
    assert_eq!(shard.set_members(b"out").unwrap(), vec![&b"b"[..]]);
    assert_eq!(shard.set_move(b"left", b"right", b"a"), Ok(true));
    assert_eq!(shard.set_contains(b"left", b"a"), Ok(false));
    assert_eq!(shard.set_contains(b"right", b"a"), Ok(true));
}

#[test]
fn a_hash_without_field_ttls_pays_nothing_for_them() {
    assert_eq!(
        size_of::<HashValue>(),
        48,
        "a plain hash keeps its main layout (48: up to 30 packed bytes live inside it)"
    );

    let clock = TestClock::new(1_000);
    let mut shard = KvShard::with_clock(clock.clone());
    let pairs = [(b"a".as_slice(), b"1".as_slice()), (b"b", b"2")];

    shard.hash_set(b"plain", &pairs).unwrap();
    shard.hash_set(b"timed", &pairs).unwrap();

    let plain = shard.entry_charge(b"plain");

    assert!(!shard.has_field_ttls());
    assert_eq!(
        shard.hash_expire_fields(b"timed", &[b"a", b"x"], 5_000),
        Ok(1)
    );
    assert!(shard.has_field_ttls());
    assert!(shard.entry_charge(b"timed").collection_bytes > plain.collection_bytes);
    assert_eq!(shard.hash_persist_fields(b"timed", &[b"a"]), Ok(1));
    assert_eq!(shard.entry_charge(b"timed"), plain);
    assert!(!shard.has_field_ttls(), "the last TTL frees the sweep");
}

#[test]
fn field_deadlines_move_from_a_vector_to_a_map_and_keep_the_earliest() {
    let clock = TestClock::new(1_000);
    let mut shard = KvShard::with_clock(clock.clone());
    let fields: Vec<Vec<u8>> = (0..40)
        .map(|index| format!("f{index}").into_bytes())
        .collect();
    let pairs: Vec<(&[u8], &[u8])> = fields
        .iter()
        .map(|field| (field.as_slice(), b"v".as_slice()))
        .collect();

    shard.hash_set(b"wide", &pairs).unwrap();

    for (index, field) in fields.iter().enumerate() {
        let at = 10_000 - index as u64;

        assert_eq!(shard.hash_expire_fields(b"wide", &[field], at), Ok(1));
    }

    assert_eq!(shard.field_deadline(b"wide", b"f0"), Ok(Some(10_000)));
    assert_eq!(shard.field_deadline(b"wide", b"f39"), Ok(Some(9_961)));
    assert_eq!(shard.hash_persist_fields(b"wide", &[b"f39"]), Ok(1));

    clock.set(9_962);
    assert_eq!(shard.expire_hash_fields_due(), 1);
    assert_eq!(shard.hash_len(b"wide"), Ok(39));
    assert_eq!(shard.hash_get(b"wide", b"f39"), Ok(Some(&b"v"[..])));
    assert_eq!(shard.hash_get(b"wide", b"f38"), Ok(None));
}

#[test]
fn wide_timed_hashes_expire_in_capped_passes_and_keep_deadlines_ordered() {
    let clock = TestClock::new(1_000);
    let mut shard = KvShard::with_clock(clock.clone());
    let fields: Vec<Vec<u8>> = (0..3_000)
        .map(|index| format!("f{index}").into_bytes())
        .collect();
    let pairs: Vec<(&[u8], &[u8])> = fields
        .iter()
        .map(|field| (field.as_slice(), b"v".as_slice()))
        .collect();
    let names: Vec<&[u8]> = fields.iter().map(Vec::as_slice).collect();

    shard.active_expire_keys = 1;
    shard.hash_set(b"wide", &pairs).unwrap();
    // Every field falls due at the same moment, except the last ten, which stay later.
    assert_eq!(
        shard.hash_expire_fields(b"wide", &names[..2_990], 2_000),
        Ok(2_990)
    );
    assert_eq!(
        shard.hash_expire_fields(b"wide", &names[2_990..], 9_000),
        Ok(10)
    );

    // Postponing and dropping deadlines keeps the earliest one exact.
    assert_eq!(shard.hash_expire_fields(b"wide", &[b"f0"], 5_000), Ok(1));
    assert_eq!(shard.hash_persist_fields(b"wide", &[b"f1"]), Ok(1));
    assert_eq!(shard.field_deadline(b"wide", b"f0"), Ok(Some(5_000)));
    assert_eq!(shard.field_deadline(b"wide", b"f1"), Ok(None));

    clock.set(2_000);
    let mut passes = 0;

    while shard.hash_len(b"wide").unwrap() > 12 {
        assert_eq!(shard.expire_hash_fields_due(), 1);
        passes += 1;
        assert!(passes < 10, "every capped pass makes progress");
    }

    assert_eq!(passes, 3, "2,988 due fields take three passes of 1,024");
    assert_eq!(shard.expire_hash_fields_due(), 0);
    assert_eq!(shard.hash_get(b"wide", b"f0"), Ok(Some(&b"v"[..])));
    assert_eq!(shard.hash_get(b"wide", b"f1"), Ok(Some(&b"v"[..])));

    clock.set(5_000);
    assert_eq!(shard.expire_hash_fields_due(), 1);
    assert_eq!(shard.hash_get(b"wide", b"f0"), Ok(None));

    clock.set(9_000);
    shard.expire_hash_fields(b"wide");
    assert_eq!(
        shard.hash_len(b"wide"),
        Ok(1),
        "a read drops every due field at once"
    );
    assert!(!shard.has_field_ttls());
}

#[test]
fn the_field_sweep_trims_due_fields_and_follows_renamed_hashes() {
    let clock = TestClock::new(1_000);
    let mut shard = KvShard::with_clock(clock.clone());

    shard
        .hash_set(b"h", &[(b"a".as_slice(), b"1".as_slice()), (b"b", b"2")])
        .unwrap();
    shard
        .hash_set(b"only", &[(b"f".as_slice(), b"v".as_slice())])
        .unwrap();
    shard.hash_expire_fields(b"h", &[b"a"], 2_000).unwrap();
    shard.hash_expire_fields(b"only", &[b"f"], 2_000).unwrap();
    assert!(shard.rename(b"h", b"moved"));

    clock.set(1_999);
    assert_eq!(shard.expire_hash_fields_due(), 0);

    clock.set(2_000);
    assert_eq!(shard.expire_hash_fields_due(), 2);
    assert_eq!(shard.hash_get(b"moved", b"a"), Ok(None));
    assert_eq!(shard.hash_get(b"moved", b"b"), Ok(Some(&b"2"[..])));
    assert_eq!(
        shard.value_kind(b"only"),
        None,
        "the last field takes the key"
    );
    assert!(!shard.has_field_ttls());

    shard
        .hash_set(b"swap", &[(b"f".as_slice(), b"v".as_slice())])
        .unwrap();
    shard.hash_expire_fields(b"swap", &[b"f"], 3_000).unwrap();
    shard.swap_databases(0, 1);
    clock.set(3_000);
    assert_eq!(shard.expire_hash_fields_due(), 1);
    shard.enter(1);
    assert_eq!(shard.value_kind(b"swap"), None);
    shard.enter(0);
    assert!(!shard.has_field_ttls());
}

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
fn small_sets_stay_in_a_compact_record() {
    let mut shard = KvShard::new();

    for index in 0..1_000 {
        let key = format!("s:{index}");

        for slot in 0..4 {
            shard
                .set_add(key.as_bytes(), &[format!("m{slot:08}")])
                .unwrap();
        }
    }

    for index in 0..1_000 {
        let key = format!("z:{index}");

        for slot in 0..4 {
            shard
                .sorted_set_add(
                    key.as_bytes(),
                    slot as f64,
                    format!("m{slot:08}").as_bytes(),
                )
                .unwrap();
        }
    }

    let usage = shard.memory_usage();

    assert!(
        usage.value_bytes < 1_000 * 160,
        "value {} index {} collection {}",
        usage.value_bytes,
        usage.index_bytes,
        usage.collection_bytes
    );
    assert!(
        usage.collection_bytes < 1_000 * 320,
        "value {} index {} collection {}",
        usage.value_bytes,
        usage.index_bytes,
        usage.collection_bytes
    );
}

#[test]
fn indexed_sorted_range_stops_early_in_score_member_order() {
    let mut set = SortedSet::default();
    let mut expected = Vec::new();

    for at in 0..400u32 {
        let score = f64::from(at % 37);
        let member = format!("m{:03}", (at * 151) % 400).into_bytes();

        set.insert(score, &member).unwrap();
        expected.push((score, member));
    }

    assert!(!set.is_flat());
    expected.sort_by(|left, right| {
        left.0
            .total_cmp(&right.0)
            .then_with(|| left.1.cmp(&right.1))
    });

    let owned = |entries: Vec<SortedMemberScore<'_>>| -> Vec<(f64, Vec<u8>)> {
        entries
            .into_iter()
            .map(|(member, score)| (score, member.to_vec()))
            .collect()
    };

    for (start, stop) in [
        (0, 0),
        (0, 4),
        (3, 17),
        (10, 10),
        (395, 500),
        (-5, -1),
        (0, -1),
    ] {
        let length = expected.len() as isize;
        let from = if start < 0 { length + start } else { start }.max(0) as usize;
        let to = (if stop < 0 { length + stop } else { stop }).min(length - 1) as usize;
        let forward = expected[from..=to].to_vec();
        let mut backward = expected.clone();

        backward.reverse();
        let backward = backward[from..=to].to_vec();

        assert_eq!(owned(set.range_entries(start, stop, false)), forward);
        assert_eq!(owned(set.range_entries(start, stop, true)), backward);
    }
}

#[test]
fn narrow_ranges_in_large_tie_groups_keep_score_and_member_order() {
    let mut set = SortedSet::default();
    set.insert(0.0, b"before").unwrap();
    for id in (0..512).rev() {
        set.insert(1.0, format!("m{id:04}").as_bytes()).unwrap();
    }
    set.insert(2.0, b"after").unwrap();
    assert!(!set.is_flat());

    let mut expected = vec![(b"before".to_vec(), 0.0)];
    expected.extend((0..512).map(|id| (format!("m{id:04}").into_bytes(), 1.0)));
    expected.push((b"after".to_vec(), 2.0));

    for reverse in [false, true] {
        let mut ordered = expected.clone();
        if reverse {
            ordered.reverse();
        }
        for (from, to) in [
            (0, 0),
            (1, 1),
            (3, 7),
            (255, 255),
            (511, 513),
            (513, 513),
            (-1, -1),
            (0, -1),
        ] {
            let (start, stop) = normalize_range(ordered.len(), from, to).unwrap();
            let actual: Vec<_> = set
                .range_entries(from, to, reverse)
                .into_iter()
                .map(|(member, score)| (member.to_vec(), score))
                .collect();
            assert_eq!(
                actual,
                ordered[start..=stop],
                "reverse={reverse} {from}..{to}"
            );
        }
    }
}

#[test]
fn sorted_set_orders_by_score_then_member() {
    let mut shard = KvShard::new();

    assert_eq!(shard.sorted_set_add(b"scores", 2.0, b"b"), Ok(true));
    assert_eq!(shard.sorted_set_add(b"scores", 1.0, b"c"), Ok(true));
    assert_eq!(shard.sorted_set_add(b"scores", 1.0, b"a"), Ok(true));
    assert_eq!(shard.sorted_set_add(b"scores", 0.5, b"b"), Ok(false));
    assert_eq!(
        shard.sorted_set_range(b"scores", 0, 2).unwrap(),
        vec![&b"b"[..], &b"a"[..], &b"c"[..]]
    );
    assert_eq!(shard.sorted_set_card(b"scores"), Ok(3));
    assert_eq!(shard.sorted_set_score(b"scores", b"a"), Ok(Some(1.0)));
    assert_eq!(shard.sorted_set_rank(b"scores", b"a", false), Ok(Some(1)));
    assert_eq!(shard.sorted_set_rank(b"scores", b"a", true), Ok(Some(1)));
    assert_eq!(
        shard.sorted_set_add_pairs(b"scores", &[(3.0, b"d".as_slice()), (4.0, b"e".as_slice())]),
        Ok(2)
    );
    assert_eq!(shard.sorted_set_increment(b"scores", b"b", 2.0), Ok(2.5));
    assert_eq!(
        shard
            .sorted_set_range_by_score(
                b"scores",
                crate::protocol::ScoreBound::Exclusive(1.0),
                crate::protocol::ScoreBound::Inclusive(3.0)
            )
            .unwrap()
            .into_iter()
            .map(|(member, _)| member)
            .collect::<Vec<_>>(),
        vec![&b"b"[..], &b"d"[..]]
    );
    assert_eq!(
        shard.sorted_set_remove(b"scores", &[b"c".as_slice(), b"missing".as_slice()]),
        Ok(1)
    );
}

#[test]
fn sorted_set_order_survives_the_flat_member_limit() {
    let mut shard = KvShard::new();
    let members: Vec<_> = (0..70).map(|index| format!("m{index:02}")).collect();

    for (index, member) in members.iter().enumerate() {
        assert_eq!(
            shard.sorted_set_add(b"wide", index as f64, member.as_bytes()),
            Ok(true)
        );
    }

    assert_eq!(shard.sorted_set_rank(b"wide", b"m00", false), Ok(Some(0)));
    assert_eq!(shard.sorted_set_rank(b"wide", b"m69", false), Ok(Some(69)));
    assert_eq!(shard.sorted_set_add(b"wide", 100.0, b"m00"), Ok(false));
    assert_eq!(shard.sorted_set_rank(b"wide", b"m00", false), Ok(Some(69)));

    for member in &members[2..] {
        shard
            .sorted_set_remove(b"wide", &[member.as_bytes()])
            .unwrap();
    }

    assert_eq!(shard.sorted_set_rank(b"wide", b"m01", false), Ok(Some(0)));
    assert_eq!(shard.sorted_set_rank(b"wide", b"m00", false), Ok(Some(1)));
}

#[test]
fn indexed_sorted_rank_counts_ties_without_sorting() {
    let mut shard = KvShard::new();
    for index in (0..100).rev() {
        let member = format!("m{index:03}");
        let score = if index < 10 { 0.0 } else { 1.0 };
        assert_eq!(
            shard.sorted_set_add(b"ties", score, member.as_bytes()),
            Ok(true)
        );
    }

    for index in [0, 9, 10, 50, 99] {
        let member = format!("m{index:03}");
        assert_eq!(
            shard.sorted_set_rank(b"ties", member.as_bytes(), false),
            Ok(Some(index))
        );
        assert_eq!(
            shard.sorted_set_rank(b"ties", member.as_bytes(), true),
            Ok(Some(99 - index))
        );
    }
    assert_eq!(shard.sorted_set_rank(b"ties", b"missing", false), Ok(None));
    assert_eq!(shard.sorted_set_add(b"ties", 0.0, b"m050"), Ok(false));
    assert_eq!(shard.sorted_set_rank(b"ties", b"m050", false), Ok(Some(10)));
    assert_eq!(shard.sorted_set_remove(b"ties", &[b"m009"]), Ok(1));
    assert_eq!(shard.sorted_set_rank(b"ties", b"m050", false), Ok(Some(9)));
}

#[test]
fn bloom_filter_tracks_inserted_values() {
    let mut shard = KvShard::new();

    assert_eq!(shard.bloom_reserve(b"seen", 0.01, 1_000, 2), Ok(()));
    assert_eq!(shard.bloom_add(b"seen", b"alpha"), Ok(BloomAdd::Added));
    assert_eq!(shard.bloom_add(b"seen", b"alpha"), Ok(BloomAdd::Exists));
    assert_eq!(shard.bloom_contains(b"seen", b"alpha"), Ok(true));
    assert_eq!(shard.bloom_contains(b"seen", b"missing"), Ok(false));
}

#[test]
fn reports_wrong_type_without_replacing_data() {
    let mut shard = KvShard::new();

    shard.insert(b"key", b"value");

    assert_eq!(
        shard.list_push_front(b"key", &[b"item".to_vec()]),
        Err(WrongType)
    );
    assert_eq!(shard.get(b"key"), Some(&b"value"[..]));
}

#[test]
fn increments_missing_and_existing_integer_values() {
    let mut shard = KvShard::new();

    assert_eq!(shard.increment(b"counter", 1), Ok(1));
    assert_eq!(shard.increment(b"counter", 1), Ok(2));
    assert_eq!(shard.increment(b"counter", -1), Ok(1));
    assert_eq!(shard.copied_value(b"counter").as_deref(), Some(&b"1"[..]));
}

#[test]
fn rename_moves_a_value_and_its_deadline() {
    let clock = TestClock::new(10);
    let mut shard = KvShard::with_clock(clock);

    shard.set_bytes(b"old", b"value");
    assert!(shard.expire_at(b"old", UnixMillis::new(2_000)));
    shard.set_bytes(b"dest", b"replaced");

    assert!(shard.rename(b"old", b"dest"));
    assert_eq!(shard.get(b"old"), None);
    assert_eq!(shard.get(b"dest"), Some(&b"value"[..]));
    assert_eq!(shard.expires_at(b"dest"), Some(UnixMillis::new(2_000)));
}

#[test]
fn increment_preserves_an_existing_deadline() {
    let clock = TestClock::new(10);
    let mut shard = KvShard::with_clock(clock);

    shard.set_bytes(b"counter", b"1");
    assert!(shard.expire_at(b"counter", UnixMillis::new(2_000)));

    assert_eq!(shard.increment(b"counter", 1), Ok(2));
    assert_eq!(shard.copied_value(b"counter").as_deref(), Some(&b"2"[..]));
    assert_eq!(shard.expires_at(b"counter"), Some(UnixMillis::new(2_000)));
}

#[test]
fn failed_increment_does_not_modify_the_value() {
    let mut shard = KvShard::new();

    shard.insert(b"text", b"not-an-integer");
    shard.insert(b"maximum", i64::MAX.to_string().as_bytes());
    shard.insert(b"minimum", i64::MIN.to_string().as_bytes());
    shard.list_push_front(b"list", &[b"item".to_vec()]).unwrap();

    assert_eq!(
        shard.increment(b"text", 1),
        Err(IncrementError::NotIntegerOrOverflow)
    );
    assert_eq!(
        shard.increment(b"maximum", 1),
        Err(IncrementError::NotIntegerOrOverflow)
    );
    assert_eq!(
        shard.increment(b"minimum", -1),
        Err(IncrementError::NotIntegerOrOverflow)
    );
    assert_eq!(shard.increment(b"list", 1), Err(IncrementError::WrongType));
    assert_eq!(shard.get(b"text"), Some(&b"not-an-integer"[..]));
    assert_eq!(shard.get(b"maximum"), Some(i64::MAX.to_string().as_bytes()));
    assert_eq!(shard.get(b"minimum"), Some(i64::MIN.to_string().as_bytes()));
}

#[test]
fn recycles_deleted_byte_buffers_within_a_bounded_pool() {
    let mut shard = KvShard::new();

    shard.set_bytes(b"first", &[7; 4_096]);
    assert!(shard.delete(b"first"));
    assert!(shard.pooled_value_bytes() >= 4_096);

    shard.set_bytes(b"second", &[8; 4_096]);
    assert_eq!(shard.pooled_value_bytes(), 0);
    assert_eq!(shard.get(b"second").unwrap(), &[8; 4_096]);
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
fn set_moves_between_flat_int_and_arena_encodings() {
    use std::collections::BTreeSet;

    let mut set = SetValue::default();
    let mut model: BTreeSet<Vec<u8>> = BTreeSet::new();
    let mut seed = 0x2545_f491_u32;
    let mut saw = [false; 3];

    for step in 0..30_000usize {
        seed ^= seed << 13;
        seed ^= seed >> 17;
        seed ^= seed << 5;
        let member: Vec<u8> = match (step / 3_000, seed % 9) {
            (0 | 2 | 4 | 6 | 8, _) => ((seed >> 4) % 900).to_string().into_bytes(),
            (_, 0) => format!("007{}", seed % 50).into_bytes(),
            (_, 1) => vec![b'x'; 70_000],
            (_, 2) => (-i64::from(seed % 70_000) * 100_000)
                .to_string()
                .into_bytes(),
            _ => format!("m{}", seed % 700).into_bytes(),
        };
        let grow = (step / 1_500) % 2 == 0;

        if grow {
            assert_eq!(set.insert(&member, 512), model.insert(member.clone()));
        } else {
            assert_eq!(set.remove(&member), model.remove(&member));
        }

        match &set.body {
            SetBody::Flat(_) => saw[0] = true,
            SetBody::Int(_) => saw[1] = true,
            SetBody::Arena(_) => saw[2] = true,
            SetBody::Table(_) => panic!("the overflow table is not expected here"),
        }

        assert_eq!(set.len(), model.len());
        assert_eq!(set.payload_bytes, model.iter().map(Vec::len).sum::<usize>());

        if step % 251 == 0 {
            let mut all: Vec<Vec<u8>> = set.iter().map(|m| m.into_owned()).collect();

            all.sort();
            assert!(all.iter().eq(model.iter()));

            for member in model.iter().take(20) {
                assert!(set.contains(member));
            }

            assert!(!set.contains(b"not-there"));
        }
    }

    assert_eq!(saw, [true; 3]);
}

#[test]
fn integer_members_use_the_int_array_until_a_text_member_arrives() {
    let mut set = SetValue::default();

    for value in 0..300 {
        assert!(set.insert(value.to_string().as_bytes(), 512));
    }

    assert!(matches!(set.body, SetBody::Int(_)));
    assert!(!set.insert(b"299", 512));
    assert!(set.contains(b"0") && !set.contains(b"00"));
    assert!(set.insert(b"007", 512));
    assert!(matches!(set.body, SetBody::Arena(_)));
    assert_eq!(set.len(), 301);
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
fn value_log_free_maps_stay_in_sync_and_reuse_spans() {
    let mut log = ValueLog::default();
    let mut live = Vec::new();
    let mut seed = 0x2545_f491_u32;

    for step in 0..20_000usize {
        seed ^= seed << 13;
        seed ^= seed >> 17;
        seed ^= seed << 5;

        if step % 3 == 2 && !live.is_empty() {
            let (offset, len) = live.swap_remove(seed as usize % live.len());

            log.free(offset, len);
        } else {
            let value = vec![step as u8; 20 + seed as usize % 300];
            let (offset, len) = log.alloc(&value);

            assert_eq!(log.slice(offset, len), &value[..]);
            live.push((offset, len));
        }
    }

    log.check_invariants();
    let end = log.end();
    let (offset, _) = log.alloc(&[1; 20]);

    assert!(offset < end);
}

#[test]
fn flush_all_drops_value_log_pages() {
    let mut shard = KvShard::new();

    for index in 0..5_000 {
        shard.insert(format!("k{index}").as_bytes(), b"0123456789abcdef0123");
    }

    assert!(!shard.value_log.extents.is_empty());
    shard.flush_all();
    assert!(shard.value_log.extents.is_empty());
    shard.insert(b"after", b"value-after-flush");
    assert_eq!(shard.get(b"after"), Some(&b"value-after-flush"[..]));
}

#[test]
fn byte_pool_enforces_class_and_size_limits() {
    let mut shard = KvShard::new();

    for index in 0..(MAX_BUFFERS_PER_CLASS + 8) {
        shard.set_bytes(format!("key:{index}").as_bytes(), &[1; 1_024]);
    }

    for index in 0..(MAX_BUFFERS_PER_CLASS + 8) {
        assert!(shard.delete(format!("key:{index}").as_bytes()));
    }

    assert_eq!(shard.pooled_value_bytes(), MAX_BUFFERS_PER_CLASS * 1_024);

    shard.set_bytes(b"large", &[2; MAX_POOLED_VALUE_BYTES + 1]);
    assert!(shard.delete(b"large"));
    assert_eq!(shard.pooled_value_bytes(), MAX_BUFFERS_PER_CLASS * 1_024);
}

#[test]
fn expiration_uses_persistable_absolute_time() {
    let clock = TestClock::new(1_000);
    let mut shard = KvShard::with_clock(clock.clone());

    shard.set_bytes(b"session", b"value");

    assert!(shard.expire_at(b"session", UnixMillis::new(2_000)));
    assert_eq!(shard.expires_at(b"session"), Some(UnixMillis::new(2_000)));
    clock.set(1_999);
    assert_eq!(shard.get(b"session"), Some(&b"value"[..]));
    assert_eq!(shard.len(), 1);

    clock.set(2_000);
    assert_eq!(shard.get(b"session"), None);
    assert_eq!(shard.expires_at(b"session"), None);
    assert_eq!(shard.len(), 1);
    assert_eq!(shard.expire_due(), 1);
    assert!(shard.is_empty());
}

#[test]
fn expired_values_behave_as_missing_for_future_mutations() {
    let clock = TestClock::new(10);
    let mut shard = KvShard::with_clock(clock.clone());

    shard.list_push_front(b"key", &[b"item".to_vec()]).unwrap();
    assert!(shard.expire_at(b"key", UnixMillis::new(20)));

    clock.set(20);
    assert_eq!(shard.increment(b"key", 1), Ok(1));
    assert_eq!(shard.copied_value(b"key").as_deref(), Some(&b"1"[..]));
    assert_eq!(shard.expires_at(b"key"), None);
}

#[test]
fn clock_is_not_read_for_values_without_expiration() {
    let clock = TestClock::new(1_000);
    let mut shard = KvShard::with_clock(clock.clone());

    shard.set_bytes(b"key", b"value");

    assert_eq!(shard.get(b"key"), Some(&b"value"[..]));
    assert_eq!(shard.len(), 1);
    assert_eq!(clock.reads(), 0);

    assert!(shard.expire_at(b"key", UnixMillis::new(2_000)));
    assert_eq!(shard.get(b"key"), Some(&b"value"[..]));
    assert!(clock.reads() > 0);
}

#[test]
fn persist_clears_an_existing_deadline() {
    let clock = TestClock::new(100);
    let mut shard = KvShard::with_clock(clock.clone());

    shard.set_bytes(b"key", b"value");
    assert!(shard.expire_at(b"key", UnixMillis::new(200)));

    assert!(shard.persist(b"key"));
    clock.set(500);
    assert_eq!(shard.get(b"key"), Some(&b"value"[..]));
    assert!(!shard.persist(b"key"));
}

#[test]
fn tracks_per_mutation_memory_deltas_and_pool_transfers() {
    let mut shard = KvShard::new();
    let empty = shard.memory_usage();

    shard.set_bytes(b"key", &[7; 4_096]);
    let inserted = shard.memory_usage();
    let insert_delta = inserted.delta_from(empty);

    // Keys are charged with their key-log record: header and slot id.
    assert_eq!(insert_delta.entries, 1);
    assert_eq!(insert_delta.key_bytes, 3 + 8);
    assert!(insert_delta.value_bytes >= 4_096);
    assert!(insert_delta.charged_bytes() > 4_096);

    assert!(shard.delete(b"key"));
    let deleted = shard.memory_usage();
    let delete_delta = deleted.delta_from(inserted);

    assert_eq!(delete_delta.entries, -1);
    assert_eq!(delete_delta.key_bytes, -(3 + 8));
    assert!(delete_delta.value_bytes <= -4_096);
    assert!(delete_delta.pooled_bytes >= 4_096);

    shard.set_bytes(b"next", &[8; 4_096]);
    let reused = shard.memory_usage();
    let reuse_delta = reused.delta_from(deleted);

    assert_eq!(reuse_delta.entries, 1);
    assert_eq!(reuse_delta.key_bytes, 4 + 8);
    assert!(reuse_delta.value_bytes >= 4_096);
    assert!(reuse_delta.pooled_bytes <= -4_096);
}

#[test]
fn tracks_collection_payload_and_capacity_charges() {
    let mut shard = KvShard::new();
    let before = shard.memory_usage();

    shard
        .list_push_front(b"jobs", &[b"first".to_vec(), b"second".to_vec()])
        .unwrap();
    let list_usage = shard.memory_usage();

    assert_eq!(list_usage.entries, 1);
    // The key and its key-log record: header and slab slot id.
    assert_eq!(list_usage.key_bytes, 4 + 8);
    assert!(list_usage.value_bytes >= b"firstsecond".len());
    // A small list lives in an index slab record: its framing is part of `value_bytes`.
    assert_eq!(list_usage.collection_bytes, 0);

    shard
        .hash_set(b"user", &[(b"name".as_slice(), b"ada".as_slice())])
        .unwrap();
    let hash_usage = shard.memory_usage();

    assert!(hash_usage.value_bytes >= b"nameada".len());
    assert_eq!(hash_usage.collection_bytes, list_usage.collection_bytes);

    shard.delete(b"jobs");
    shard.delete(b"user");
    let after = shard.memory_usage();
    let total_delta = after.delta_from(before);

    assert_eq!(total_delta.entries, 0);
    assert_eq!(total_delta.key_bytes, 0);
    assert_eq!(total_delta.value_bytes, 0);
    assert_eq!(total_delta.collection_bytes, 0);
}

#[test]
fn deadline_index_charges_owned_key_bytes_until_addressed() {
    let mut deadlines = DeadlineDb::default();
    let key = b"unaddressed";
    assert_eq!(
        deadlines.set(key, None, UnixMillis::new(100), |_| None),
        None
    );
    let id = deadlines.id_of(key, |_| None).unwrap();
    let with_owned = deadlines.charged_bytes();
    deadlines.note_address(id, 1);
    assert_eq!(deadlines.charged_bytes(), with_owned - key.len());
    assert!(deadlines.remove(key, |_| Some(&key[..])));
    assert_eq!(deadlines.charged_bytes(), 0);
}

#[test]
fn deadline_index_charges_track_capacity_renewal_and_release() {
    let clock = TestClock::new(1_000);
    let mut shard = KvShard::with_clock(clock.clone());
    let baseline = shard.memory_usage().index_bytes;
    for id in 0..1_000 {
        let key = format!("key:{id:04}");
        shard.set_bytes(key.as_bytes(), b"v");
    }
    let untimed = shard.memory_usage().index_bytes;
    for id in 0..1_000 {
        let key = format!("key:{id:04}");
        assert!(shard.expire_at(key.as_bytes(), UnixMillis::new(10_000)));
    }
    let timed = shard.memory_usage().index_bytes;
    assert!(timed > untimed);
    let capacity = shard.deadlines[0].charged_bytes();
    assert!(capacity >= 1_000 * size_of::<DeadlineRecord>());
    assert_eq!(timed, untimed + capacity);

    for id in 0..1_000 {
        let key = format!("key:{id:04}");
        assert!(shard.expire_at(key.as_bytes(), UnixMillis::new(20_000)));
    }
    assert_eq!(shard.memory_usage().index_bytes, timed);
    shard.set_memory_limit(Some(shard.memory_usage().charged_bytes()));
    assert!(shard.memory_admission_would_fail(1));

    clock.set(20_000);
    while shard.expire_cycle(100) > 0 {}
    assert_eq!(shard.memory_usage().index_bytes, baseline);
    assert_eq!(shard.expiration_stats().keys_with_ttl, 0);
}

#[test]
fn watch_index_tracks_shared_keys_databases_and_unwatch() {
    let mut shard = KvShard::new();
    assert!(shard.watchers_by_key.is_none());
    shard.watch(1, 0, &[b"shared", b"shared"]);
    shard.watch(2, 0, &[b"shared"]);
    shard.watch(2, 1, &[b"shared"]);
    shard.watch(3, 1, &[b"other"]);
    assert_eq!(shard.watches[&1].len(), 1);
    assert!(Arc::ptr_eq(
        &shard.watches[&1][0].1,
        &shard.watches[&2][0].1
    ));
    assert_eq!(
        shard.watchers_by_key.as_ref().unwrap()[&0][b"shared".as_slice()].len(),
        2
    );

    shard.set_bytes(b"unrelated", b"v");
    assert!(!shard.watches_dirty(1));
    shard.enter(1);
    shard.set_bytes(b"shared", b"v");
    assert!(!shard.watches_dirty(1));
    assert!(shard.watches_dirty(2));
    assert!(!shard.watches_dirty(3));

    shard.unwatch(2);
    assert!(!shard.watches_dirty(2));
    shard.enter(0);
    shard.set_bytes(b"shared", b"v");
    assert!(shard.watches_dirty(1));
    assert!(!shard.watches_dirty(2));
    shard.unwatch(1);
    assert!(!shard.watchers_by_key.as_ref().unwrap().contains_key(&0));
    shard.enter(1);
    shard.set_bytes(b"other", b"v");
    assert!(shard.watches_dirty(3));
    shard.unwatch(3);
    assert!(shard.watchers_by_key.is_none());

    shard.watch(2, 0, &[b"shared"]);
    shard.enter(0);
    shard.set_bytes(b"shared", b"new");
    assert!(shard.watches_dirty(2));
    shard.unwatch(2);
    assert!(shard.watchers_by_key.is_none());
}

#[test]
fn set_sampling_returns_distinct_members_or_repeats_by_sign() {
    let mut shard = KvShard::new();
    let members: Vec<Vec<u8>> = (0..256)
        .map(|id| format!("m{id:03}").into_bytes())
        .collect();
    shard.set_add(b"sample", &members).unwrap();

    let single = shard.set_random_members(b"sample", None).unwrap();
    assert_eq!(single.len(), 1);
    assert!(members.contains(&single[0]));
    assert!(
        shard
            .set_random_members(b"sample", Some(0))
            .unwrap()
            .is_empty()
    );

    for (count, len, distinct) in [
        (9, 9, true),
        (-9, 9, false),
        (256, 256, true),
        (400, 256, true),
        (-260, 260, false),
    ] {
        let sampled = shard.set_random_members(b"sample", Some(count)).unwrap();
        assert_eq!(sampled.len(), len, "count={count}");
        assert!(sampled.iter().all(|member| members.contains(member)));

        if distinct {
            let unique: std::collections::HashSet<_> = sampled.iter().collect();
            assert_eq!(unique.len(), len, "count={count}");
        }
    }

    let mut seen = std::collections::HashSet::new();
    for _ in 0..2000 {
        seen.insert(shard.set_random_members(b"sample", None).unwrap().remove(0));
    }
    assert!(
        seen.len() > 128,
        "single draws cover the set: {}",
        seen.len()
    );
}

#[test]
fn set_scan_survives_growth_and_returns_every_member() {
    let mut shard = KvShard::new();
    let base: Vec<Vec<u8>> = (0..2000)
        .map(|id| format!("base:{id:05}").into_bytes())
        .collect();
    shard.set_add(b"s", &base).unwrap();

    let mut seen = std::collections::HashSet::new();
    let mut cursor = 0;
    let mut grown = 0;
    loop {
        let (next, page) = shard.set_scan(b"s", cursor, 50, None).unwrap();
        seen.extend(page.iter().map(|member| member.to_vec()));
        // Writes between the first pages, enough to rebuild the table several times.
        if grown < 12 {
            let extra: Vec<Vec<u8>> = (0..300)
                .map(|id| format!("extra:{grown:03}:{id}").into_bytes())
                .collect();
            shard.set_add(b"s", &extra).unwrap();
        }
        grown += 1;
        assert!(grown < 10_000, "scan must end once writes stop");
        cursor = next;
        if cursor == 0 {
            break;
        }
    }
    assert!(base.iter().all(|member| seen.contains(member)));

    let (_, matching) = shard
        .set_scan(b"s", 0, 100_000, Some(b"base:0000*"))
        .unwrap();
    assert!(
        matching
            .iter()
            .all(|member| member.starts_with(b"base:0000"))
    );
}

#[test]
fn set_pop_takes_distinct_members_and_covers_the_set() {
    let mut shard = KvShard::new();
    let members: Vec<Vec<u8>> = (0..300)
        .map(|id| format!("p{id:03}").into_bytes())
        .collect();
    shard.set_add(b"pop", &members).unwrap();

    let first = shard.set_pop_members(b"pop", 20).unwrap();
    let unique: std::collections::HashSet<_> = first.iter().collect();
    assert_eq!(first.len(), 20);
    assert_eq!(unique.len(), 20);

    let mut firsts = std::collections::HashSet::new();
    for _ in 0..200 {
        firsts.insert(shard.set_pop_members(b"pop", 1).unwrap().remove(0));
    }
    assert!(
        firsts.len() > 30,
        "single pops are not stuck on one bucket: {}",
        firsts.len()
    );
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
fn sorted_set_scan_and_random_walk_by_rank() {
    let mut shard = KvShard::new();
    let pairs: Vec<(f64, Vec<u8>)> = (0..600)
        .map(|id| (f64::from(id), format!("z{id:03}").into_bytes()))
        .collect();
    shard.sorted_set_add_pairs(b"z", &pairs).unwrap();

    let mut seen = Vec::new();
    let mut cursor = 0;
    loop {
        let (next, page) = shard
            .sorted_set_scan(b"z", cursor, 70, Some(b"z*"))
            .unwrap();
        seen.extend(page.iter().map(|(member, _)| member.to_vec()));
        cursor = next;
        if cursor == 0 {
            break;
        }
    }
    assert_eq!(seen.len(), 600);

    let sparse = shard.sorted_set_scan(b"z", 0, 5, Some(b"z59*")).unwrap().1;
    assert!(sparse.iter().all(|(member, _)| member.starts_with(b"z59")));

    assert_eq!(
        shard.sorted_set_random_entries(b"z", None).unwrap().len(),
        1
    );
    assert_eq!(
        shard
            .sorted_set_random_entries(b"z", Some(-1000))
            .unwrap()
            .len(),
        1000
    );
    let distinct = shard.sorted_set_random_entries(b"z", Some(40)).unwrap();
    assert_eq!(
        distinct
            .iter()
            .map(|(m, _)| m)
            .collect::<std::collections::HashSet<_>>()
            .len(),
        40
    );
    assert_eq!(
        shard
            .sorted_set_random_entries(b"z", Some(5000))
            .unwrap()
            .len(),
        600
    );
}

#[test]
fn scan_visits_every_live_key_once_with_filters() {
    let clock = TestClock::new(1_000);
    let mut shard = KvShard::with_clock(clock.clone());
    let expected: Vec<Vec<u8>> = (0..256)
        .filter(|id| *id != 42)
        .map(|id| format!("match:{id:03}").into_bytes())
        .collect();
    for id in 0..256 {
        shard.set_bytes(format!("match:{id:03}").as_bytes(), b"value");
        shard.set_bytes(format!("other:{id:03}").as_bytes(), b"value");
    }
    shard
        .list_push_front(b"match:list", &[b"value".to_vec()])
        .unwrap();
    assert!(shard.expire_at(b"match:042", UnixMillis::new(1_500)));
    clock.set(1_600);

    for count in [0, 1, 7, 60, 300] {
        let mut seen: Vec<Vec<u8>> = Vec::new();
        let mut cursor = 0;

        loop {
            let (next, keys) = shard.scan(cursor, count, Some(b"match:*"), Some(ValueKind::Bytes));
            // COUNT is a hint: a page ends after the block that reached it, and a
            // table under 64 buckets is one block.
            assert!(keys.len() < count.max(1) + 64, "count={count}");
            seen.extend(keys.into_iter().map(<[u8]>::to_vec));
            cursor = next;
            if cursor == 0 {
                break;
            }
        }

        seen.sort();
        assert_eq!(seen, expected, "count={count}");
    }

    // A cursor from a stale or hostile client never panics and still terminates.
    for cursor in [1, 12, 255, u64::MAX] {
        let (next, _) = shard.scan(cursor, 10, None, None);
        assert!(next == 0 || next != cursor);
    }

    shard.enter(1);
    shard.set_bytes(b"match:other-db", b"v");
    let (mut cursor, mut keys) = (0, Vec::new());
    loop {
        let (next, page) = shard.scan(cursor, 10, None, None);
        keys.extend(page);
        cursor = next;
        if cursor == 0 {
            break;
        }
    }
    assert_eq!(keys, vec![b"match:other-db".as_slice()]);
}

#[test]
fn keyspace_counts_live_keys_without_materializing_key_copies() {
    let clock = TestClock::new(1_000);
    let mut shard = KvShard::with_clock(clock.clone());
    shard.set_bytes(b"live", b"v");
    assert!(shard.expire_at(b"live", UnixMillis::new(2_000)));
    shard.set_bytes(b"plain", b"v");
    shard.enter(3);
    shard.set_bytes(b"expired", b"v");
    assert!(shard.expire_at(b"expired", UnixMillis::new(1_500)));
    clock.set(1_600);

    // Constant-time counts: the expired key in database 3 is listed until it is reaped.
    assert_eq!(shard.keyspace(), vec![(0, 2, 1, 400), (3, 1, 1, 0)]);
    assert_eq!(shard.database_len(), 1);
}

#[test]
fn deadline_charge_cache_tracks_multiple_databases_and_rewrites() {
    let mut shard = KvShard::with_clock(TestClock::new(1_000));
    let check = |shard: &KvShard<TestClock>| {
        let total: usize = shard.deadlines.iter().map(DeadlineDb::charged_bytes).sum();
        assert_eq!(shard.deadline_index_bytes, total);
        assert_eq!(
            shard.memory_usage().index_bytes,
            shard.entries.charged_bytes() + shard.parked_index_bytes + total
        );
    };
    shard.enter(255);
    shard.set_bytes(b"timed", b"value");
    assert!(shard.expire_at(b"timed", UnixMillis::new(10_000)));
    check(&shard);
    shard.enter(1);
    shard.set_bytes(b"other", b"value");
    assert!(shard.expire_at(b"other", UnixMillis::new(20_000)));
    check(&shard);
    shard.enter(0);
    for id in 0..100 {
        shard.set_bytes(format!("plain:{id}").as_bytes(), b"value");
    }
    check(&shard);
    shard.swap_databases(1, 255);
    check(&shard);
    let detached = shard.take_database(1);
    check(&shard);
    shard.restore_database(detached);
    check(&shard);
    shard.enter(1);
    assert!(shard.persist(b"timed"));
    check(&shard);
    shard.flush_all();
    check(&shard);
}

#[test]
fn detached_databases_keep_their_deadline_index_and_flushes_clear_every_charge() {
    let clock = TestClock::new(1_000);
    let mut shard = KvShard::with_clock(clock.clone());

    for id in 0..500 {
        let key = format!("timed:{id}");

        shard.set_bytes(key.as_bytes(), b"value");
        assert!(shard.expire_at(key.as_bytes(), UnixMillis::new(5_000 + id)));
    }

    shard.set_bytes(b"plain", b"value");
    let usage = shard.memory_usage();

    // The only database of the shard is detached on the running totals and comes back
    // with its deadlines still pointing at its keys.
    let detached = shard.take_database(0);

    assert_eq!(shard.memory_usage().key_bytes, 0);
    assert_eq!(shard.memory_usage().value_bytes, 0);
    shard.restore_database(detached);
    assert_eq!(shard.memory_usage().key_bytes, usage.key_bytes);
    assert_eq!(shard.memory_usage().value_bytes, usage.value_bytes);
    assert_eq!(shard.deadline(0, b"timed:7"), Some(UnixMillis::new(5_007)));
    clock.set(5_250);
    assert_eq!(shard.get(b"timed:200"), None);
    assert_eq!(shard.get(b"timed:300"), Some(&b"value"[..]));

    // A second database: the walk still charges the one taken out alone.
    shard.enter(2);
    shard.set_bytes(b"other", b"value");
    assert!(shard.expire_at(b"other", UnixMillis::new(9_000)));
    shard.enter(0);
    let before = shard.memory_usage().key_bytes;
    let detached = shard.take_database(2);

    // The key and its record: a header and a value-log span.
    assert_eq!(shard.memory_usage().key_bytes, before - b"other".len() - 10);
    shard.restore_database(detached);
    assert_eq!(shard.memory_usage().key_bytes, before);

    shard.flush_all();
    let after = shard.memory_usage();

    assert_eq!(
        (after.key_bytes, after.value_bytes, after.collection_bytes),
        (0, 0, 0)
    );
    assert_eq!(shard.expiration.keys_with_ttl, 0);
    shard.enter(2);
    assert_eq!(shard.deadline(2, b"other"), None);
}

#[test]
fn storage_charges_match_entries_across_expiry_databases_and_snapshot() {
    let clock = TestClock::new(1_000);
    let mut shard = KvShard::with_clock(clock.clone());
    shard.apply_memory_limits(MemoryLimits {
        hash_max_compact_entries: 1,
        ..MemoryLimits::default()
    });
    let check = |shard: &KvShard<TestClock>| {
        let mut entries = 0;
        let mut keys = 0;
        let mut values = 0;
        let mut collections = 0;
        let mut index_bytes = 0;
        for (_, map) in shard.database_maps() {
            entries += map.len();
            index_bytes += map.charged_bytes();
            for (key, hit) in map.entries() {
                let charge = charge_of(key, hit);
                keys += charge.key_bytes;
                values += charge.value_bytes;
                collections += charge.collection_bytes;
            }
        }
        let usage = shard.memory_usage();
        assert_eq!(usage.entries, entries);
        assert_eq!(usage.key_bytes, keys);
        assert_eq!(usage.value_bytes, values);
        assert_eq!(usage.collection_bytes, collections);
        let deadline_bytes: usize = shard.deadlines.iter().map(DeadlineDb::charged_bytes).sum();
        assert_eq!(shard.deadline_index_bytes, deadline_bytes);
        assert_eq!(usage.index_bytes, index_bytes + deadline_bytes);
        assert_eq!(usage.pooled_bytes, shard.byte_pool.cached_bytes);
    };
    check(&shard);
    shard.set_bytes(b"reused", &[3; 4_096]);
    shard.set_bytes(b"reused", &[7; 8_192]);
    shard.set_bytes(b"reused", b"tiny");
    check(&shard);
    shard.set_bytes(b"long", &[1; 100]);
    shard.set_range(b"long", 150, b"tail").unwrap();
    check(&shard);
    shard.enter(1);
    shard
        .hash_set(b"fields", &[(b"a", b"v"), (b"b", b"v")])
        .unwrap();
    assert_eq!(shard.hash_is_compact(b"fields"), Ok(Some(false)));
    shard.list_push_back(b"jobs", &[b"item"]).unwrap();
    assert!(shard.expire_at(b"jobs", UnixMillis::new(2_000)));
    check(&shard);
    shard.begin_snapshot(16_384).unwrap();
    shard.capture_snapshot_key(b"fields");
    let db1_journal = shard.memory_usage().snapshot_journal_bytes;
    assert!(db1_journal > 0);
    check(&shard);
    shard.enter(0);
    shard.capture_snapshot_key(b"reused");
    assert!(shard.memory_usage().snapshot_journal_bytes > db1_journal);
    check(&shard);
    shard.delete(b"reused");
    check(&shard);
    clock.set(2_000);
    assert_eq!(shard.expire_cycle(8), 1);
    check(&shard);
    let snapshot = shard.finish_snapshot();
    assert_eq!(shard.memory_usage().snapshot_journal_bytes, 0);
    assert!(
        snapshot
            .iter()
            .any(|entry| entry.database == 1 && entry.key == b"fields")
    );
    assert!(
        snapshot
            .iter()
            .any(|entry| entry.database == 0 && entry.key == b"reused")
    );
    check(&shard);
    shard.apply_eviction(1, b"fields");
    assert_eq!(shard.memory_stats().evicted_keys, 1);
    check(&shard);
    shard.flush_all();
    check(&shard);
    let usage = shard.memory_usage();
    assert_eq!(usage.entries, 0);
    assert_eq!(
        usage.key_bytes + usage.value_bytes + usage.collection_bytes + usage.index_bytes,
        0
    );
}

#[test]
fn evicted_and_deleted_short_strings_free_their_log_spans() {
    let mut shard = KvShard::new();

    shard.set_bytes(b"kept", b"value-kept");
    let live = shard.value_log.live_bytes();

    shard.set_bytes(b"evicted", &[7; 48]);
    shard.apply_eviction(0, b"evicted");
    assert_eq!(shard.value_log.live_bytes(), live);

    assert!(shard.put_versioned(b"versioned", &[9; 40], 3).is_ok());
    assert!(shard.delete(b"versioned"));
    assert_eq!(shard.value_log.live_bytes(), live);

    shard.set_bytes(b"removed", &[5; 32]);
    assert_eq!(shard.remove(b"removed").as_deref(), Some(&[5; 32][..]));
    assert_eq!(shard.value_log.live_bytes(), live);
}

#[test]
fn a_rolled_back_versioned_value_keeps_its_own_bytes() {
    let mut shard = KvShard::new();

    assert!(shard.put_versioned(b"k", b"abc", 1).is_ok());
    let live = shard.value_log.live_bytes();
    let saved = shard.clone_entry(b"k");

    assert!(shard.delete(b"k"));
    shard.restore_entry(b"k", saved);
    assert_eq!(shard.value_log.live_bytes(), live);

    // The span the delete freed is reused; the restored value must not share it.
    shard.set_bytes(b"other", b"xyz");
    assert_eq!(
        shard.value_view(b"k").unwrap(),
        Some(ByteView::Borrowed(b"abc"))
    );
    assert_eq!(shard.version_of(b"k"), Some(1));

    // Overwriting a versioned value inside a rolled-back batch: same rule.
    let saved = shard.clone_entry(b"k");

    assert!(shard.put_versioned(b"k", b"defg", 2).is_ok());
    shard.restore_entry(b"k", saved);
    shard.set_bytes(b"third", b"wxyz");
    assert_eq!(
        shard.value_view(b"k").unwrap(),
        Some(ByteView::Borrowed(b"abc"))
    );
    assert!(shard.delete(b"k"));
    assert!(shard.delete(b"other"));
    assert!(shard.delete(b"third"));
    assert_eq!(shard.value_log.live_bytes(), 0);
}

#[test]
fn flushing_one_database_frees_its_versioned_spans() {
    let mut shard = KvShard::new();

    shard.set_bytes(b"kept", b"value");
    let live = shard.value_log.live_bytes();

    shard.enter(1);
    assert!(shard.put_versioned(b"versioned", &[4; 30], 1).is_ok());
    shard.enter(0);
    shard.flush_database(1);
    assert_eq!(shard.value_log.live_bytes(), live);
}

#[test]
fn storing_a_set_over_a_versioned_value_frees_its_span() {
    let mut shard = KvShard::new();

    shard.set_bytes(b"kept", b"value");
    let live = shard.value_log.live_bytes();

    assert!(shard.put_versioned(b"dest", &[6; 20], 1).is_ok());
    assert_eq!(shard.set_store(b"dest", &[b"a".as_slice()]), 1);
    assert_eq!(shard.value_log.live_bytes(), live);

    assert!(shard.delete(b"dest"));
    assert!(shard.put_versioned(b"dest", &[6; 20], 2).is_ok());
    assert_eq!(shard.set_store::<&[u8]>(b"dest", &[]), 0);
    assert_eq!(shard.value_log.live_bytes(), live);
}

#[test]
fn reclaiming_logs_keeps_a_parked_database_charge_exact() {
    let mut shard = KvShard::with_clock(TestClock::new(1_000));

    shard.set_eviction_policy(crate::EvictionPolicy::AllKeysLru);
    shard.enter(1);
    for index in 0..4_000 {
        let key = format!("key:{index:05}");

        shard.set_bytes(key.as_bytes(), b"v");
    }
    // Sparse key-log pages, and recency cells on the keys that stay.
    for index in 0..4_000 {
        let key = format!("key:{index:05}");

        if index % 8 == 0 {
            shard.get(key.as_bytes());
        } else {
            assert!(shard.delete(key.as_bytes()));
        }
    }
    shard.enter(0);
    shard.set_bytes(b"active", b"v");

    let mut done = false;

    for _ in 0..1_000 {
        let migrated = shard.migrate_indexes(std::time::Duration::from_millis(5));

        done = migrated && shard.reclaim_logs(std::time::Duration::from_millis(5));
        if done {
            break;
        }
    }
    assert!(done, "reclaim did not finish");
    charges_match_a_recount(&shard);
    shard.enter(1);
    charges_match_a_recount(&shard);
}

#[test]
fn switching_the_eviction_policy_keeps_parked_charges_exact() {
    let mut shard = KvShard::with_clock(TestClock::new(1_000));

    shard.set_eviction_policy(crate::EvictionPolicy::AllKeysLru);
    shard.enter(1);
    for index in 0..100 {
        let key = format!("key:{index}");

        shard.set_bytes(key.as_bytes(), b"v");
        shard.get(key.as_bytes());
    }
    shard.enter(0);
    shard.set_bytes(b"active", b"v");

    for _ in 0..3 {
        shard.set_eviction_policy(crate::EvictionPolicy::AllKeysLfu);
        charges_match_a_recount(&shard);
        shard.set_eviction_policy(crate::EvictionPolicy::AllKeysLru);
        charges_match_a_recount(&shard);
    }

    shard.enter(1);
    charges_match_a_recount(&shard);
    shard.enter(0);
    charges_match_a_recount(&shard);
}

#[test]
fn string_charges_count_records_and_allocator_blocks() {
    let mut shard = KvShard::new();
    let key = b"0123456789abcdef";

    shard.set_bytes(key, &[1; 64]);
    assert_eq!(shard.key_memory_usage(key), Some(16 + 10 + 64));

    shard.set_bytes(key, &[1; 100]);
    assert_eq!(shard.key_memory_usage(key), Some(16 + 8 + 112 + 24));

    // A shorter value reuses the large buffer, which stays charged.
    shard.set_bytes(key, &[2; 100_000]);
    let large = shard.key_memory_usage(key).unwrap();

    assert_eq!(large, 16 + 8 + alloc_size(100_000) + 24);
    shard.set_bytes(key, &[3; 100]);
    assert_eq!(shard.key_memory_usage(key), Some(large));
    assert!(shard.delete(key));

    shard.set_bytes(key, b"12345");
    assert_eq!(shard.key_memory_usage(key), Some(16 + 10 + 5));

    assert!(shard.delete(key));
    shard.increment(key, 5).unwrap();
    assert_eq!(shard.key_memory_usage(key), Some(16 + 12));
}

#[test]
fn repeated_list_pushes_charge_payload_without_rescanning() {
    let value = vec![b'x'; 64];
    let push = |count: usize| {
        let mut shard = KvShard::new();
        let started = std::time::Instant::now();

        for _ in 0..count {
            shard
                .list_push_front(b"jobs", std::slice::from_ref(&value))
                .unwrap();
        }

        let elapsed = started.elapsed();
        assert_eq!(shard.memory_usage().value_bytes, count * value.len());

        for _ in 0..count {
            assert_eq!(shard.list_pop_front(b"jobs").unwrap().unwrap().len(), 64);
        }

        assert_eq!(shard.memory_usage().entries, 0);
        assert_eq!(shard.memory_usage().value_bytes, 0);
        assert_eq!(shard.memory_usage().collection_bytes, 0);
        elapsed
    };

    // Four times the elements. A full recount grows about sixteenfold;
    // an incremental charge stays near fourfold.
    let (small_count, large_count) = benchmark_counts();
    let small = push(small_count);
    let large = push(large_count);

    assert!(
        large < small.saturating_mul(8),
        "list push scaled too fast: {small:?} -> {large:?}"
    );
}

#[test]
fn repeated_set_and_sorted_set_inserts_do_not_rescan_members() {
    let member = |index: usize| format!("member-{index:04}").into_bytes();
    let time_set = |count: usize| {
        let mut shard = KvShard::new();
        let started = std::time::Instant::now();

        for index in 0..count {
            assert_eq!(shard.set_add(b"tags", &[member(index)]).unwrap(), 1);
        }

        let elapsed = started.elapsed();
        let payload: usize = (0..count).map(|index| member(index).len()).sum();
        assert_eq!(shard.memory_usage().value_bytes, payload);
        elapsed
    };

    let time_sorted = |count: usize| {
        let mut shard = KvShard::new();
        let started = std::time::Instant::now();

        for index in 0..count {
            assert!(
                shard
                    .sorted_set_add(b"board", index as f64, &member(index))
                    .unwrap()
            );
        }

        let elapsed = started.elapsed();
        let payload: usize = (0..count).map(|index| member(index).len()).sum();
        assert_eq!(shard.memory_usage().value_bytes, payload);
        elapsed
    };

    let (small_count, large_count) = benchmark_counts();
    let small_set = time_set(small_count);
    let large_set = time_set(large_count);

    assert!(
        large_set < small_set.saturating_mul(8),
        "set insert scaled too fast: {small_set:?} -> {large_set:?}"
    );
    let small_sorted = time_sorted(small_count);
    let large_sorted = time_sorted(large_count);

    assert!(
        large_sorted < small_sorted.saturating_mul(8),
        "sorted set insert scaled too fast: {small_sorted:?} -> {large_sorted:?}"
    );
}

#[test]
fn physical_memory_accounting_retains_passively_expired_entries() {
    let clock = TestClock::new(10);
    let mut shard = KvShard::with_clock(clock.clone());

    shard.set_bytes(b"key", b"value");
    shard.expire_at(b"key", UnixMillis::new(20));
    let live_usage = shard.memory_usage();

    clock.set(20);
    assert_eq!(shard.get(b"key"), None);
    assert_eq!(shard.memory_usage(), live_usage);

    shard.set_bytes(b"key", b"new");
    assert_eq!(shard.memory_usage().entries, 1);
    assert_eq!(shard.get(b"key"), Some(&b"new"[..]));
}

#[test]
fn shard_semantics_work_with_an_alternate_static_index() {
    let clock = TestClock::new(100);
    let index = crate::BTreeIndex::with_capacity(8);
    let mut shard = KvShard::with_index_and_clock(index, clock.clone());

    shard.set_bytes(b"counter", b"41");
    assert_eq!(shard.increment(b"counter", 1), Ok(42));
    shard
        .set_add(b"members", &[b"a".to_vec(), b"b".to_vec()])
        .unwrap();
    assert_eq!(shard.set_contains(b"members", b"b"), Ok(true));
    assert!(shard.expire_at(b"members", UnixMillis::new(200)));
    assert_eq!(shard.memory_usage().entries, 2);

    clock.set(200);
    assert_eq!(shard.set_contains(b"members", b"b"), Ok(false));
    assert_eq!(shard.copied_value(b"counter").as_deref(), Some(&b"42"[..]));
}

#[test]
fn copy_on_write_snapshot_preserves_pre_mutation_values() {
    let mut shard = KvShard::new();

    shard.set_bytes(b"keep", b"original");
    shard.set_bytes(b"mutate", b"before");
    shard.begin_snapshot(1024).unwrap();
    shard.capture_snapshot_key(b"mutate");
    shard.set_bytes(b"mutate", b"after");
    shard.capture_snapshot_key(b"created");
    shard.set_bytes(b"created", b"new");
    shard.capture_snapshot_key(b"keep");
    shard.delete(b"keep");
    let snapshot = shard.finish_snapshot();

    assert_eq!(
        snapshot,
        vec![
            SnapshotEntry {
                database: 0,
                key: b"keep".to_vec(),
                expires_at: None,
                value: SnapshotValue::Bytes(b"original".to_vec()),
            },
            SnapshotEntry {
                database: 0,
                key: b"mutate".to_vec(),
                expires_at: None,
                value: SnapshotValue::Bytes(b"before".to_vec()),
            },
        ]
    );
    assert_eq!(shard.get(b"mutate"), Some(&b"after"[..]));
    assert_eq!(shard.get(b"created"), Some(&b"new"[..]));
    assert!(shard.get(b"keep").is_none());
    assert!(
        shard
            .snapshot_entries()
            .iter()
            .any(|entry| entry.key == b"created")
    );
}

#[test]
fn snapshot_captured_keys_only_shadow_their_own_database() {
    let mut shard = KvShard::new();
    shard.set_bytes(b"shared", b"db0-before");
    shard.set_bytes(b"uncaptured", b"db0");
    shard.enter(1);
    shard.set_bytes(b"shared", b"db1-before");
    shard.begin_snapshot(1024).unwrap();
    shard.enter(0);
    shard.capture_snapshot_key(b"shared");
    shard.set_bytes(b"shared", b"db0-after");
    shard.capture_snapshot_key(b"created");
    shard.set_bytes(b"created", b"new");

    let snapshot = shard.finish_snapshot();
    assert_eq!(
        snapshot
            .into_iter()
            .map(|entry| (entry.database, entry.key, entry.value))
            .collect::<Vec<_>>(),
        vec![
            (
                0,
                b"shared".to_vec(),
                SnapshotValue::Bytes(b"db0-before".to_vec())
            ),
            (
                0,
                b"uncaptured".to_vec(),
                SnapshotValue::Bytes(b"db0".to_vec())
            ),
            (
                1,
                b"shared".to_vec(),
                SnapshotValue::Bytes(b"db1-before".to_vec())
            ),
        ]
    );
}

#[test]
fn snapshot_journal_beyond_the_configured_bound_aborts_the_snapshot() {
    let mut shard = KvShard::new();

    shard.set_bytes(b"key", &[7; 64]);
    shard.begin_snapshot(16).unwrap();
    shard.capture_snapshot_key(b"key");
    assert!(!shard.journal_is_open());
    assert_eq!(shard.snapshot_aborts(), 1);
    assert_eq!(shard.get(b"key"), Some(&[7; 64][..]));
}

#[test]
fn a_capture_past_the_journal_bound_aborts_before_copying_the_value() {
    let mut shard = KvShard::new();
    let members: Vec<Vec<u8>> = (0..4_096u32)
        .map(|index| format!("member-{index:08}").into_bytes())
        .collect();

    shard.set_add(b"big", &members).unwrap();
    shard.begin_snapshot(1024).unwrap();
    super::snapshot::LIVE_SNAPSHOT_COPIES.with(|copies| copies.set(0));
    shard.capture_snapshot_key(b"big");

    assert!(!shard.journal_is_open());
    assert_eq!(shard.snapshot_aborts(), 1);
    assert_eq!(
        super::snapshot::LIVE_SNAPSHOT_COPIES.with(std::cell::Cell::get),
        0,
        "the value was copied only to be thrown away"
    );
}

#[test]
fn ttl_reports_missing_persistent_and_remaining_deadlines() {
    let clock = TestClock::new(1_000);
    let mut shard = KvShard::with_clock(clock.clone());

    assert_eq!(shard.pttl(b"missing"), -2);
    shard.set_bytes(b"key", b"value");
    assert_eq!(shard.ttl_seconds(b"key"), -1);
    assert!(shard.expire_at(b"key", UnixMillis::new(3_500)));
    assert_eq!(shard.pttl(b"key"), 2_500);
    assert_eq!(
        shard.ttl_seconds(b"key"),
        3,
        "Redis rounds to the nearest second"
    );
    clock.set(3_500);
    assert_eq!(shard.pttl(b"key"), -2);
}

#[test]
fn bounded_active_expiry_removes_a_budgeted_number_of_stale_keys() {
    let clock = TestClock::new(10);
    let mut shard = KvShard::with_clock(clock.clone());

    shard.set_bytes(b"a", b"1");
    shard.set_bytes(b"b", b"2");
    shard.set_bytes(b"c", b"3");
    shard.expire_at(b"a", UnixMillis::new(20));
    shard.expire_at(b"b", UnixMillis::new(20));
    shard.expire_at(b"c", UnixMillis::new(40));
    clock.set(20);
    assert_eq!(shard.len(), 3);
    assert_eq!(shard.memory_usage().entries, 3);
    assert_eq!(shard.expiration_stats().keys_with_ttl, 3);
    assert_eq!(shard.expire_cycle(1), 1);
    assert_eq!(shard.memory_usage().entries, 2);
    assert_eq!(shard.expiration_stats().expired_keys_active, 1);
    assert_eq!(shard.expiration_stats().keys_with_ttl, 2);
    assert_eq!(shard.expire_cycle(8), 1);
    assert_eq!(shard.memory_usage().entries, 1);
    assert_eq!(shard.expiration_stats().expired_keys_active, 2);
    assert_eq!(shard.expiration_stats().keys_with_ttl, 1);
    assert_eq!(shard.get(b"c"), Some(&b"3"[..]));
}

#[test]
fn deadline_heap_matches_reference_across_replacements_and_removals() {
    let mut deadlines = DeadlineDb::default();
    let mut reference = BTreeMap::new();
    let mut seed = 0x9e37_79b9_7f4a_7c15_u64;

    for _ in 0..5_000 {
        seed ^= seed << 13;
        seed ^= seed >> 7;
        seed ^= seed << 17;
        let key = format!("key-{}", seed % 256);
        if seed & 3 == 0 {
            assert_eq!(
                deadlines.remove(key.as_bytes(), |_| None),
                reference.remove(key.as_bytes()).is_some()
            );
        } else {
            let at = UnixMillis::new(1_000 + (seed >> 8) % 4_000);
            let previous = deadlines.set(key.as_bytes(), None, at, |_| None);
            assert_eq!(previous, reference.insert(key.into_bytes(), at));
        }
        assert_eq!(deadlines.len, reference.len());
        assert_eq!(deadlines.heap.len(), reference.len());
        // `min_at` is a lower bound until `first_due` re-places moved deadlines.
        assert!(deadlines.min_at() <= reference.values().min().copied());
        for (position, &id) in deadlines.heap.iter().enumerate() {
            assert_eq!(
                deadlines.records[id as usize].as_ref().unwrap().position() as usize,
                position
            );
        }
    }

    for at in 1_000..5_000 {
        while let Some(key) = deadlines.first_due(UnixMillis::new(at), |_| unreachable!()) {
            assert_eq!(reference.remove(key.as_ref()), Some(UnixMillis::new(at)));
            assert!(deadlines.remove(&key, |_| None));
        }
    }
    assert!(deadlines.heap.is_empty());
    assert!(reference.is_empty());
}

#[test]
fn active_expiry_prioritizes_due_keys_behind_future_deadlines() {
    let clock = TestClock::new(1_000);
    let mut shard = KvShard::with_clock(clock.clone());
    for index in 0..128 {
        let key = format!("future-{index}");
        shard.set_bytes(key.as_bytes(), b"v");
        assert!(shard.expire_at(key.as_bytes(), UnixMillis::new(10_000)));
    }
    shard.set_bytes(b"due", b"v");
    assert!(shard.expire_at(b"due", UnixMillis::new(2_000)));
    clock.set(2_000);

    assert_eq!(shard.expire_cycle(1), 1);
    assert_eq!(shard.expiration_stats().keys_with_ttl, 128);
    assert_eq!(shard.expiration_stats().expired_keys_active, 1);
    assert_eq!(shard.get(b"due"), None);
    assert_eq!(shard.get(b"future-0"), Some(&b"v"[..]));
}

#[test]
fn active_expiry_reorders_extended_and_shortened_deadlines() {
    let clock = TestClock::new(1_000);
    let mut shard = KvShard::with_clock(clock.clone());
    for key in [b"extended".as_slice(), b"shortened", b"other"] {
        shard.set_bytes(key, b"v");
        assert!(shard.expire_at(key, UnixMillis::new(3_000)));
    }
    assert!(shard.expire_at(b"extended", UnixMillis::new(10_000)));
    assert!(shard.expire_at(b"shortened", UnixMillis::new(2_000)));
    clock.set(2_000);
    assert_eq!(shard.expire_cycle(1), 1);
    assert_eq!(shard.get(b"shortened"), None);
    assert_eq!(shard.get(b"other"), Some(&b"v"[..]));
    assert_eq!(shard.get(b"extended"), Some(&b"v"[..]));
    clock.set(3_000);
    assert_eq!(shard.expire_cycle(1), 1);
    assert_eq!(shard.expiration_stats().keys_with_ttl, 1);
    assert_eq!(shard.get(b"other"), None);
    clock.set(10_000);
    assert_eq!(shard.expire_cycle(1), 1);
    assert_eq!(shard.expiration_stats().keys_with_ttl, 0);
}

#[test]
fn active_expiry_reaches_later_databases_when_an_earlier_one_fills_the_budget() {
    let clock = TestClock::new(1_000);
    let mut shard = KvShard::with_clock(clock.clone());

    for index in 0..8 {
        let key = format!("future-{index}");
        shard.set_bytes(key.as_bytes(), b"v");
        assert!(shard.expire_at(key.as_bytes(), UnixMillis::new(10_000)));
    }

    shard.enter(1);
    shard.set_bytes(b"due", b"v");
    assert!(shard.expire_at(b"due", UnixMillis::new(2_000)));
    shard.enter(0);
    clock.set(2_000);

    let removed = (0..2).map(|_| shard.expire_cycle(1)).sum::<usize>();
    assert_eq!(removed, 1);
    assert_eq!(shard.expiration_stats().expired_keys_active, 1);
    shard.enter(1);
    assert_eq!(shard.get(b"due"), None);
}

#[test]
fn active_expiry_respects_replaced_and_persisted_deadlines_across_databases() {
    let clock = TestClock::new(1_000);
    let mut shard = KvShard::with_clock(clock.clone());
    shard.set_bytes(b"past", b"v");
    assert!(shard.expire_at(b"past", UnixMillis::new(999)));
    shard.set_bytes(b"kept", b"v");
    assert!(shard.expire_at(b"kept", UnixMillis::new(2_000)));
    assert!(shard.persist(b"kept"));

    shard.enter(1);
    shard.set_bytes(b"later", b"v");
    assert!(shard.expire_at(b"later", UnixMillis::new(2_000)));
    assert!(shard.expire_at(b"later", UnixMillis::new(3_000)));
    shard.set_bytes(b"equal", b"v");
    assert!(shard.expire_at(b"equal", UnixMillis::new(2_000)));
    shard.enter(0);

    assert_eq!(shard.expire_cycle(1), 1);
    assert_eq!(shard.expiration_stats().expired_keys_active, 1);
    clock.set(1_999);
    assert_eq!(shard.expire_cycle(8), 0);
    clock.set(2_000);
    assert_eq!(shard.expire_cycle(8), 1);
    shard.enter(1);
    assert_eq!(shard.get(b"equal"), None);
    assert_eq!(shard.get(b"later"), Some(&b"v"[..]));
    shard.enter(0);
    assert_eq!(shard.get(b"kept"), Some(&b"v"[..]));
    clock.set(3_000);
    assert_eq!(shard.expire_cycle(8), 1);
    assert_eq!(shard.expiration_stats().expired_keys_active, 3);
    assert_eq!(shard.expiration_stats().expired_keys_passive, 0);
    assert_eq!(shard.expiration_stats().keys_with_ttl, 0);
}

#[test]
fn expired_values_are_hidden_from_reads_and_mutations_before_active_cleanup() {
    let clock = TestClock::new(1_000);
    let mut shard = KvShard::with_clock(clock.clone());
    shard.enter(1);
    shard.set_bytes(b"string", b"old");
    shard.list_push_back(b"list", &[b"old"]).unwrap();
    shard.set_add(b"set", &[b"old"]).unwrap();
    shard.hash_set(b"hash", &[(b"field", b"old")]).unwrap();
    shard.sorted_set_add(b"sorted", 1.0, b"old").unwrap();
    shard
        .stream_append(b"stream", 1, 0, &[(b"field", b"old")])
        .unwrap();
    shard.bloom_reserve(b"bloom", 0.01, 1_000, 2).unwrap();
    shard.bloom_add(b"bloom", b"old").unwrap();
    for key in [
        b"string".as_slice(),
        b"list",
        b"set",
        b"hash",
        b"sorted",
        b"stream",
        b"bloom",
    ] {
        assert!(shard.expire_at(key, UnixMillis::new(2_000)));
    }
    clock.set(2_000);
    assert_eq!(shard.expiration_stats().expired_keys_active, 0);
    assert_eq!(shard.get(b"string"), None);
    assert_eq!(shard.list_index(b"list", 0), Ok(None));
    assert_eq!(shard.set_contains(b"set", b"old"), Ok(false));
    assert_eq!(shard.hash_get(b"hash", b"field"), Ok(None));
    assert_eq!(shard.sorted_set_score(b"sorted", b"old"), Ok(None));
    assert_eq!(shard.bloom_contains(b"bloom", b"old"), Ok(false));
    assert_eq!(shard.stream_len(b"stream"), Ok(0));
    assert_eq!(shard.increment(b"string", 1), Ok(1));
    assert_eq!(shard.list_push_back(b"list", &[b"new"]), Ok(1));
    assert_eq!(shard.set_add(b"set", &[b"new"]), Ok(1));
    assert_eq!(shard.hash_set(b"hash", &[(b"field", b"new")]), Ok(1));
    assert_eq!(shard.sorted_set_add(b"sorted", 2.0, b"new"), Ok(true));
    assert_eq!(shard.bloom_add(b"bloom", b"new"), Ok(BloomAdd::Added));
    assert_eq!(shard.copied_value(b"string"), Some(b"1".to_vec()));
    assert_eq!(shard.list_index(b"list", 0), Ok(Some(&b"new"[..])));
    assert_eq!(shard.set_members(b"set").unwrap(), vec![&b"new"[..]]);
    assert_eq!(shard.hash_get(b"hash", b"field"), Ok(Some(&b"new"[..])));
    assert_eq!(shard.sorted_set_score(b"sorted", b"old"), Ok(None));
    assert_eq!(shard.expiration_stats().expired_keys_passive, 7);
    assert_eq!(shard.expire_cycle(16), 0);
}

#[test]
fn equal_deadline_storm_drains_with_a_fixed_budget_and_keeps_other_keys_live() {
    let clock = TestClock::new(1_000);
    let mut shard = KvShard::with_clock(clock.clone());
    shard.set_bytes(b"persistent", b"ok");

    for db in 0..2 {
        shard.enter(db);
        for index in 0..64 {
            let key = format!("due-{index}");
            shard.set_bytes(key.as_bytes(), b"v");
            assert!(shard.expire_at(key.as_bytes(), UnixMillis::new(2_000)));
        }
    }

    shard.enter(0);
    clock.set(2_000);
    for _ in 0..16 {
        assert_eq!(shard.expire_cycle(8), 8);
        assert_eq!(shard.get(b"persistent"), Some(&b"ok"[..]));
    }
    assert_eq!(shard.expire_cycle(8), 0);
    assert_eq!(shard.expiration_stats().expired_keys_active, 128);
    assert_eq!(shard.expiration_stats().keys_with_ttl, 0);
    shard.enter(1);
    assert_eq!(shard.len(), 0);
}

#[test]
fn equal_deadline_hash_fields_drain_in_bounded_ticks() {
    let clock = TestClock::new(1_000);
    let mut shard = KvShard::with_clock(clock.clone());
    shard.apply_memory_limits(MemoryLimits {
        active_expire_keys: 8,
        ..MemoryLimits::default()
    });
    shard.enter(1);

    for index in 0..64 {
        let key = format!("hash-{index}");
        shard
            .hash_set(
                key.as_bytes(),
                &[(b"field".as_slice(), b"value".as_slice())],
            )
            .unwrap();
        assert_eq!(
            shard.hash_expire_fields(key.as_bytes(), &[b"field"], 2_000),
            Ok(1)
        );
    }

    clock.set(2_000);
    // At least `active_expire_keys` hashes a tick, more while the tick's time budget lasts.
    let mut drained = 0;

    for _ in 0..8 {
        let trimmed = shard.expire_hash_fields_due();

        assert!(trimmed >= 8 || drained + trimmed == 64, "{trimmed}");
        drained += trimmed;
    }
    assert_eq!(drained, 64);
    assert_eq!(shard.expire_hash_fields_due(), 0);
    assert!(!shard.has_field_ttls());
    assert_eq!(shard.len(), 0);
}

#[test]
fn sampled_time_stays_fixed_until_the_batch_ends() {
    let clock = TestClock::new(1_000);
    let shard = KvShard::with_clock(clock.clone());

    shard.sample_time();
    clock.set(5_000);
    assert_eq!(shard.now_unix_millis(), UnixMillis::new(1_000));
    shard.clear_time_sample();
    assert_eq!(shard.now_unix_millis(), UnixMillis::new(5_000));
}

#[test]
fn clock_jumps_and_restart_cannot_resurrect_an_expired_snapshot_entry() {
    let wall = crate::time::TestWallClock::new(1_000, 1_000);
    let mut shard = KvShard::with_clock(wall.clone());
    shard.set_bytes(b"soon", b"value");
    assert!(shard.expire_at(b"soon", UnixMillis::new(1_500)));
    let snapshot = shard.snapshot_entries();
    assert_eq!(shard.pttl(b"soon"), 500);

    wall.set(1_500);
    assert_eq!(shard.pttl(b"soon"), -2);
    wall.set(500);
    assert_eq!(shard.get(b"soon"), None);

    let mut restarted = KvShard::with_clock(crate::time::TestWallClock::new(500, 1_500));
    for entry in snapshot {
        restarted.restore_snapshot_entry(entry).unwrap();
    }
    assert_eq!(restarted.get(b"soon"), None);
    assert_eq!(restarted.pttl(b"soon"), -2);
}

#[test]
fn a_lagging_replica_needs_the_primary_clock_floor_before_promotion() {
    let primary_wall = crate::time::TestWallClock::new(1_000, 1_000);
    let mut primary = KvShard::with_clock(primary_wall.clone());
    primary.set_bytes(b"soon", b"value");
    assert!(primary.expire_at(b"soon", UnixMillis::new(1_250)));
    let transferred = primary.snapshot_entries();

    primary_wall.set(1_500);
    assert_eq!(primary.get(b"soon"), None);
    let replica_wall = crate::time::TestWallClock::new(900, 900);
    let mut replica = KvShard::with_clock(replica_wall.clone());
    for entry in transferred.clone() {
        replica.restore_snapshot_entry(entry).unwrap();
    }
    assert_eq!(replica.get(b"soon"), Some(b"value".as_slice()));

    let mut promoted = KvShard::with_clock(crate::time::TestWallClock::new(900, 1_500));
    for entry in transferred {
        promoted.restore_snapshot_entry(entry).unwrap();
    }
    assert_eq!(promoted.get(b"soon"), None);
}

#[test]
fn large_lease_and_limit_deadlines_saturate_without_wrapping() {
    let clock = TestClock::new(1);
    let mut shard = KvShard::with_clock(clock.clone());
    let (token, lease_end) = shard.lease_plan(b"lease", u64::MAX).unwrap().unwrap();
    assert_eq!(lease_end, u64::MAX);
    shard
        .lease_put(b"lease", token, lease_end, "LEASE")
        .unwrap();
    assert_eq!(shard.pttl(b"lease"), i64::MAX);

    clock.set(u64::MAX - 5);
    assert_eq!(shard.pttl(b"lease"), 5);
    let LimitDecision::Allow {
        window_end: Some(end),
        ..
    } = shard.plan_limit(b"limit", 2, 20).unwrap()
    else {
        panic!("expected an open rate-limit window");
    };
    assert_eq!(end, u64::MAX);
    shard.put_limit(b"limit", 1).unwrap();
    assert!(shard.expire_at(b"limit", UnixMillis::new(end)));
    assert_eq!(shard.pttl(b"limit"), 5);
    clock.set(u64::MAX);
    assert_eq!(shard.pttl(b"lease"), -1);
    assert_eq!(shard.pttl(b"limit"), -2);
}

#[test]
fn future_deadlines_skip_the_active_expire_scan() {
    let clock = TestClock::new(1_000);
    let mut shard = KvShard::with_clock(clock.clone());

    for index in 0..1_000 {
        let key = index.to_string();

        shard.set_bytes(key.as_bytes(), b"v");
        assert!(shard.expire_at(key.as_bytes(), UnixMillis::new(50_000)));
    }

    assert_eq!(shard.expire_cycle(16), 0);
    assert_eq!(shard.expiration_stats().keys_with_ttl, 1_000);
    assert_eq!(shard.len(), 1_000);

    clock.set(50_000);
    assert_eq!(shard.expire_cycle(16), 16);
    assert_eq!(shard.expiration_stats().keys_with_ttl, 984);
    assert_eq!(shard.expire_cycle(16), 16);
    assert_eq!(shard.expiration_stats().keys_with_ttl, 968);
}

#[test]
fn deleting_the_earliest_deadline_keeps_a_later_key_until_it_is_due() {
    let clock = TestClock::new(1_000);
    let mut shard = KvShard::with_clock(clock.clone());

    shard.set_bytes(b"early", b"1");
    shard.set_bytes(b"later", b"2");
    assert!(shard.expire_at(b"early", UnixMillis::new(2_000)));
    assert!(shard.expire_at(b"later", UnixMillis::new(5_000)));
    assert!(shard.delete(b"early"));

    clock.set(3_000);
    assert_eq!(shard.expire_cycle(16), 0);
    assert_eq!(shard.get(b"later"), Some(&b"2"[..]));

    clock.set(5_000);
    assert_eq!(shard.expire_cycle(16), 1);
    assert_eq!(shard.get(b"later"), None);
}

#[test]
fn expiration_metrics_separate_passive_active_and_ttl_gauge() {
    let clock = TestClock::new(10);
    let mut shard = KvShard::with_clock(clock.clone());

    shard.set_bytes(b"session", b"token");
    shard.set_bytes(b"cache", b"v");
    shard.set_bytes(b"keep", b"alive");
    assert!(shard.expire_at(b"session", UnixMillis::new(20)));
    assert!(shard.expire_at(b"cache", UnixMillis::new(20)));
    assert!(shard.expire_at(b"keep", UnixMillis::new(40)));
    assert_eq!(
        shard.expiration_stats(),
        ExpirationStats {
            expired_keys: 0,
            expired_keys_passive: 0,
            expired_keys_active: 0,
            keys_with_ttl: 3,
        }
    );

    assert!(shard.persist(b"keep"));
    assert_eq!(shard.expiration_stats().keys_with_ttl, 2);

    clock.set(20);
    assert_eq!(shard.len(), 3);
    assert_eq!(shard.expiration_stats().keys_with_ttl, 2);
    assert_eq!(shard.expiration_stats().expired_keys, 0);

    assert!(!shard.delete(b"session"));
    assert_eq!(
        shard.expiration_stats(),
        ExpirationStats {
            expired_keys: 1,
            expired_keys_passive: 1,
            expired_keys_active: 0,
            keys_with_ttl: 1,
        }
    );

    assert_eq!(shard.expire_cycle(8), 1);
    assert_eq!(
        shard.expiration_stats(),
        ExpirationStats {
            expired_keys: 2,
            expired_keys_passive: 1,
            expired_keys_active: 1,
            keys_with_ttl: 0,
        }
    );
    assert_eq!(shard.get(b"keep"), Some(&b"alive"[..]));

    shard.set_bytes(b"fresh", b"1");
    assert!(shard.expire_at(b"fresh", UnixMillis::new(50)));
    shard.set_bytes(b"fresh", b"2");
    assert_eq!(shard.expiration_stats().keys_with_ttl, 0);
    assert_eq!(shard.pttl(b"fresh"), -1);
}

#[test]
fn noeviction_admission_uses_the_configured_shard_budget() {
    let mut shard = KvShard::new();

    shard.set_bytes(b"key", b"value");
    let used = shard.memory_usage().charged_bytes();

    shard.set_memory_limit(Some(used));
    assert!(shard.memory_admission_would_fail(1));
    assert!(!shard.memory_admission_would_fail(0));
}

#[test]
fn eviction_policies_pick_lru_lfu_and_nearest_ttl_victims() {
    let clock = TestClock::new(1_000);
    let mut shard = KvShard::with_clock(clock.clone());

    shard.set_eviction_policy(EvictionPolicy::AllKeysLru);
    shard.set_bytes(b"a", b"1");
    shard.set_bytes(b"b", b"2");
    shard.set_bytes(b"c", b"3");
    // Writes stamp the keys too; reads later make "a" and "c" the recent ones.
    clock.set(10_000);
    shard.touch(b"a");
    shard.touch(b"c");
    assert_eq!(
        shard
            .pick_eviction_victim(&[], true)
            .map(|(_, key)| key)
            .as_deref(),
        Some(b"b".as_slice())
    );

    let mut shard = KvShard::new();

    shard.set_eviction_policy(EvictionPolicy::AllKeysLfu);
    shard.set_bytes(b"a", b"1");
    shard.set_bytes(b"b", b"2");
    shard.set_bytes(b"c", b"3");
    shard.touch(b"a");
    shard.touch(b"a");
    shard.touch(b"b");
    assert_eq!(
        shard
            .pick_eviction_victim(&[], true)
            .map(|(_, key)| key)
            .as_deref(),
        Some(b"c".as_slice())
    );

    let clock = TestClock::new(10);
    let mut shard = KvShard::with_clock(clock);

    shard.set_eviction_policy(EvictionPolicy::VolatileTtl);
    shard.set_bytes(b"persist", b"1");
    shard.set_bytes(b"soon", b"2");
    shard.set_bytes(b"later", b"3");
    assert!(shard.expire_at(b"soon", UnixMillis::new(20)));
    assert!(shard.expire_at(b"later", UnixMillis::new(40)));
    assert_eq!(
        shard
            .pick_eviction_victim(&[], true)
            .map(|(_, key)| key)
            .as_deref(),
        Some(b"soon".as_slice())
    );

    let clock = TestClock::new(10);
    let mut shard = KvShard::with_clock(clock);

    shard.set_eviction_policy(EvictionPolicy::VolatileLru);
    shard.set_bytes(b"persist", b"1");
    shard.set_bytes(b"volatile", b"2");
    assert!(shard.expire_at(b"volatile", UnixMillis::new(50)));
    shard.touch(b"volatile");
    assert_eq!(
        shard
            .pick_eviction_victim(&[], true)
            .map(|(_, key)| key)
            .as_deref(),
        Some(b"volatile".as_slice())
    );
}

#[test]
fn eviction_is_bounded_and_skips_protected_write_keys() {
    let mut shard = KvShard::new();

    shard.set_eviction_policy(EvictionPolicy::AllKeysLru);

    for index in 0..20 {
        shard.set_bytes(format!("k{index:02}").as_bytes(), b"v");
    }

    let used = shard.memory_usage().charged_bytes();

    shard.set_memory_limit(Some(used / 2));
    let mut evicted = 0;

    for _ in 0..EVICTION_TURN_BUDGET {
        let Some((database, victim)) = shard.pick_eviction_victim(&[b"k00".as_slice()], true)
        else {
            break;
        };

        assert_ne!(victim.as_ref(), b"k00");
        shard.apply_eviction(database, &victim);
        evicted += 1;
    }

    assert_eq!(evicted, EVICTION_TURN_BUDGET);
    assert!(shard.get(b"k00").is_some());
    assert_eq!(
        shard.memory_stats().evicted_keys,
        EVICTION_TURN_BUDGET as u64
    );
    assert!(shard.memory_stats().allocation_events > 0);
    assert!(shard.write_exceeds_shard_budget(used + 1));
}

#[test]
fn deleting_the_last_key_releases_index_charge() {
    let mut shard = KvShard::new();
    shard.set_bytes(b"key", b"value");
    assert!(shard.memory_usage().index_bytes > 0);
    assert!(shard.delete(b"key"));
    assert_eq!(shard.len(), 0);
    assert_eq!(shard.memory_usage().index_bytes, 0);
    assert_eq!(shard.memory_usage().key_bytes, 0);
    assert_eq!(shard.memory_usage().value_bytes, 0);
    assert_eq!(shard.memory_usage().collection_bytes, 0);
}

#[test]
fn deleting_most_keys_shrinks_the_index() {
    let mut shard = KvShard::new();
    let (inserted, removed) = shrink_counts();

    for index in 0..inserted {
        shard.set_bytes(format!("k{index:04}").as_bytes(), b"v");
    }

    let grown = shard.memory_usage().index_bytes;

    for index in 0..removed {
        assert!(shard.delete(format!("k{index:04}").as_bytes()));
    }

    assert_eq!(shard.len(), inserted - removed);
    let shrunk = shard.memory_usage().index_bytes;

    assert!(
        shrunk.saturating_mul(4) < grown,
        "index stayed large after deletes: {shrunk} vs {grown}"
    );
}

#[test]
fn removing_most_set_members_shrinks_the_table() {
    let mut shard = KvShard::new();
    let (inserted, removed) = shrink_counts();
    let members: Vec<Vec<u8>> = (0..inserted)
        .map(|index| format!("m{index:04}").into_bytes())
        .collect();

    assert_eq!(shard.set_add(b"s", &members), Ok(inserted));
    let grown = shard.memory_usage().collection_bytes;

    assert_eq!(shard.set_remove(b"s", &members[..removed]), Ok(removed));
    assert_eq!(shard.set_card(b"s"), Ok(inserted - removed));
    let shrunk = shard.memory_usage().collection_bytes;

    assert!(
        shrunk.saturating_mul(4) < grown,
        "set table stayed large after deletes: {shrunk} vs {grown}"
    );
}

#[test]
fn removing_most_hash_fields_shrinks_the_table() {
    let mut shard = KvShard::new();
    let (inserted, removed) = shrink_counts();
    let fields: Vec<(Vec<u8>, Vec<u8>)> = (0..inserted)
        .map(|index| (format!("f{index:04}").into_bytes(), b"v".to_vec()))
        .collect();
    let pairs: Vec<(&[u8], &[u8])> = fields
        .iter()
        .map(|(field, value)| (field.as_slice(), value.as_slice()))
        .collect();

    assert_eq!(shard.hash_set(b"h", &pairs), Ok(inserted));
    let grown = shard.memory_usage().collection_bytes;
    let remove: Vec<&[u8]> = fields[..removed]
        .iter()
        .map(|(field, _)| field.as_slice())
        .collect();

    assert_eq!(shard.hash_delete(b"h", &remove), Ok(removed));
    assert_eq!(shard.hash_len(b"h"), Ok(inserted - removed));
    let shrunk = shard.memory_usage().collection_bytes;

    assert!(
        shrunk.saturating_mul(4) < grown,
        "hash table stayed large after deletes: {shrunk} vs {grown}"
    );
}

#[test]
fn eviction_sample_size_stops_after_the_configured_candidates() {
    let mut limits = MemoryLimits {
        eviction_policy: EvictionPolicy::AllKeysLru,
        maxmemory_samples: 1,
        ..MemoryLimits::default()
    };

    let clock = TestClock::new(1);
    let mut shard =
        KvShard::with_index_and_clock(crate::index::BTreeIndex::default(), clock.clone());

    shard.apply_memory_limits(limits);
    shard.set_bytes(b"c", b"1");
    shard.set_bytes(b"a", b"1");
    shard.set_bytes(b"b", b"1");
    clock.set(5_000);
    shard.touch(b"a");
    shard.touch(b"b");
    assert_eq!(
        shard
            .pick_eviction_victim(&[], true)
            .map(|(_, key)| key)
            .as_deref(),
        Some(b"a".as_slice())
    );
    limits.maxmemory_samples = 16;
    shard.apply_memory_limits(limits);
    assert_eq!(
        shard
            .pick_eviction_victim(&[], true)
            .map(|(_, key)| key)
            .as_deref(),
        Some(b"c".as_slice())
    );
}

#[test]
fn eviction_rotates_across_cold_keys_and_parked_databases() {
    let clock = TestClock::new(1);
    let mut shard =
        KvShard::with_index_and_clock(crate::index::BTreeIndex::default(), clock.clone());
    shard.apply_memory_limits(MemoryLimits {
        eviction_policy: EvictionPolicy::AllKeysLru,
        maxmemory_samples: 1,
        ..MemoryLimits::default()
    });
    shard.enter(1);
    shard.set_bytes(b"parked-cold", b"v");
    shard.enter(0);
    for key in [b"a", b"b", b"c", b"d"] {
        shard.set_bytes(key, b"v");
    }
    clock.set(5_000);
    shard.touch(b"a");
    shard.touch(b"b");

    let victims: Vec<_> = (0..5)
        .map(|_| shard.pick_eviction_victim(&[], true).unwrap())
        .collect();
    assert_eq!(
        victims,
        vec![
            (0, Box::from(b"a".as_slice())),
            (0, Box::from(b"b".as_slice())),
            (0, Box::from(b"c".as_slice())),
            (0, Box::from(b"d".as_slice())),
            (1, Box::from(b"parked-cold".as_slice())),
        ]
    );

    // Raising the sample size exposes the untouched keys hidden by the first window.
    shard.apply_memory_limits(MemoryLimits {
        eviction_policy: EvictionPolicy::AllKeysLru,
        maxmemory_samples: 16,
        ..MemoryLimits::default()
    });
    let (db, key) = shard.pick_eviction_victim(&[], true).unwrap();
    assert!(db == 1 || key.as_ref() == b"c" || key.as_ref() == b"d");
}

#[test]
fn eviction_hash_bucket_cursor_covers_all_keys_and_survives_resize() {
    let mut shard = KvShard::with_clock(TestClock::new(1));
    shard.apply_memory_limits(MemoryLimits {
        eviction_policy: EvictionPolicy::AllKeysLru,
        maxmemory_samples: 1,
        ..MemoryLimits::default()
    });
    for index in 0..128 {
        shard.set_bytes(format!("k{index:04}").as_bytes(), b"v");
    }
    let victims: AHashSet<_> = (0..128)
        .map(|_| shard.pick_eviction_victim(&[], false).unwrap())
        .collect();
    assert_eq!(victims.len(), 128);
    let stats = shard.memory_stats();
    assert_eq!(stats.eviction_scans, 128);
    // One rotation visits each bucket at most once, empty buckets included.
    assert!(stats.eviction_slots_inspected >= 128);
    assert!(stats.eviction_slots_inspected <= shard.entries.eviction_slots() as u64);

    for index in 0..127 {
        shard.delete(format!("k{index:04}").as_bytes());
    }
    assert_eq!(
        shard.pick_eviction_victim(&[], false),
        Some((0, Box::from(b"k0127".as_slice())))
    );
    assert_eq!(shard.pick_eviction_victim(&[b"k0127"], false), None);
    shard.delete(b"k0127");
    assert_eq!(shard.pick_eviction_victim(&[], false), None);
    shard.set_bytes(b"new", b"v");
    assert_eq!(
        shard.pick_eviction_victim(&[], false),
        Some((0, Box::from(b"new".as_slice())))
    );
}

#[test]
fn eviction_filters_expired_and_protected_keys_without_cross_database_exclusion() {
    let clock = TestClock::new(1);
    let mut shard =
        KvShard::with_index_and_clock(crate::index::BTreeIndex::default(), clock.clone());
    shard.apply_memory_limits(MemoryLimits {
        eviction_policy: EvictionPolicy::VolatileLru,
        maxmemory_samples: 1,
        ..MemoryLimits::default()
    });
    shard.set_bytes(b"expired", b"v");
    shard.expire_at(b"expired", UnixMillis::new(2));
    shard.set_bytes(b"protected", b"v");
    shard.expire_at(b"protected", UnixMillis::new(1000));
    shard.set_bytes(b"persistent", b"v");
    shard.enter(1);
    shard.set_bytes(b"protected", b"v");
    shard.expire_at(b"protected", UnixMillis::new(1000));
    shard.enter(0);
    clock.set(2);
    assert_eq!(shard.pick_eviction_victim(&[b"protected"], false), None);
    assert_eq!(
        shard.pick_eviction_victim(&[b"protected"], true),
        Some((1, Box::from(b"protected".as_slice())))
    );
    shard.apply_eviction(1, b"protected");
    assert_eq!(shard.active, 0);
    assert_eq!(shard.memory_stats().evicted_keys, 1);
    assert_eq!(shard.pick_eviction_victim(&[b"protected"], true), None);
    assert_eq!(shard.get(b"persistent"), Some(b"v".as_slice()));
}

#[test]
fn eviction_scan_counts_expired_protected_and_parked_database_slots() {
    for policy in [EvictionPolicy::AllKeysLru, EvictionPolicy::VolatileLru] {
        let clock = TestClock::new(1);
        let mut shard = KvShard::with_clock(clock.clone());
        shard.apply_memory_limits(MemoryLimits {
            eviction_policy: policy,
            maxmemory_samples: 1,
            active_expire_keys: 0,
            ..MemoryLimits::default()
        });
        shard.set_bytes(b"expired", b"v");
        assert!(shard.expire_at(b"expired", UnixMillis::new(2)));
        shard.set_bytes(b"protected", b"v");
        assert!(shard.expire_at(b"protected", UnixMillis::new(1_000)));
        shard.enter(1);
        shard.set_bytes(b"parked", b"v");
        assert!(shard.expire_at(b"parked", UnixMillis::new(1_000)));
        shard.enter(0);
        clock.set(2);

        let active_slots = if policy.volatile_only() {
            shard.deadlines[0].lookup.num_buckets()
        } else {
            shard.entries.eviction_slots()
        } as u64;
        let parked_slots = if policy.volatile_only() {
            shard.deadlines[1].lookup.num_buckets()
        } else {
            shard.databases[1].as_ref().unwrap().eviction_slots()
        } as u64;

        let before = shard.memory_stats();
        assert_eq!(shard.pick_eviction_victim(&[b"protected"], false), None);
        let after = shard.memory_stats();
        assert_eq!(after.eviction_scans - before.eviction_scans, 1);
        assert_eq!(
            after.eviction_slots_inspected - before.eviction_slots_inspected,
            active_slots
        );
        assert_eq!(
            shard.pick_eviction_victim(&[b"protected"], true),
            Some((1, Box::from(b"parked".as_slice())))
        );
        let final_stats = shard.memory_stats();
        let inspected = final_stats.eviction_slots_inspected - after.eviction_slots_inspected;
        assert!((1..=active_slots + parked_slots).contains(&inspected));
        assert_eq!(final_stats.eviction_scans - before.eviction_scans, 2);
        assert_eq!(shard.active_database(), 0);
    }
}

#[derive(Debug, Default)]
struct CountingEvictionIndex {
    inner: crate::index::BTreeIndex,
    inspected: Cell<usize>,
}

impl StorageIndex for CountingEvictionIndex {
    fn with_capacity(capacity: usize) -> Self {
        Self {
            inner: crate::index::BTreeIndex::with_capacity(capacity),
            inspected: Cell::new(0),
        }
    }

    fn lookup(&self, key: &[u8]) -> Option<IndexRef<'_>> {
        self.inner.lookup(key)
    }

    fn object_mut(&mut self, key: &[u8]) -> Option<&mut Entry> {
        self.inner.object_mut(key)
    }

    fn get_key_value(&self, key: &[u8]) -> Option<(&[u8], IndexRef<'_>)> {
        self.inner.get_key_value(key)
    }

    fn insert(&mut self, key: &[u8], entry: Entry) -> Option<Entry> {
        self.inner.insert(key, entry)
    }

    fn remove(&mut self, key: &[u8]) -> Option<Entry> {
        self.inner.remove(key)
    }

    fn access_get(&self, key: &[u8]) -> u32 {
        self.inner.access_get(key)
    }

    fn access_mut(&mut self, key: &[u8]) -> Option<&mut u32> {
        self.inner.access_mut(key)
    }

    fn reset_access(&mut self) {
        self.inner.reset_access();
    }

    fn get_or_insert_with(
        &mut self,
        key: &[u8],
        create: impl FnOnce() -> Entry,
    ) -> Option<&mut Entry> {
        self.inner.get_or_insert_with(key, create)
    }

    fn len(&self) -> usize {
        self.inner.len()
    }

    fn entries(&self) -> impl Iterator<Item = (&[u8], IndexRef<'_>)> {
        self.inner.entries().inspect(|_| {
            self.inspected.set(self.inspected.get() + 1);
        })
    }

    fn into_values(self) -> impl Iterator<Item = Entry> {
        self.inner.into_values()
    }

    fn charged_bytes(&self) -> usize {
        self.inner.charged_bytes()
    }
}

#[test]
fn volatile_eviction_samples_deadlines_instead_of_the_key_index() {
    for count in [32, 128] {
        let mut shard =
            KvShard::with_index_and_clock(CountingEvictionIndex::default(), TestClock::new(1));
        shard.apply_memory_limits(MemoryLimits {
            eviction_policy: EvictionPolicy::VolatileLru,
            maxmemory_samples: 1,
            ..MemoryLimits::default()
        });
        for index in 0..count {
            shard.set_bytes(format!("k{index:04}").as_bytes(), b"v");
        }
        let slots = |shard: &KvShard<TestClock, CountingEvictionIndex>| {
            shard.memory_stats().eviction_slots_inspected
        };

        shard.entries.inspected.set(0);
        assert_eq!(shard.pick_eviction_victim(&[], false), None);
        assert_eq!(slots(&shard), 0);

        let last = format!("k{:04}", count - 1);
        assert!(shard.expire_at(last.as_bytes(), UnixMillis::new(1000)));
        assert_eq!(
            shard.pick_eviction_victim(&[], false),
            Some((0, last.clone().into_bytes().into_boxed_slice()))
        );
        // One pass over the small deadline lookup, independent of the key count.
        let buckets = shard.deadlines[0].lookup.num_buckets() as u64;
        assert!((1..=buckets).contains(&slots(&shard)));

        let before = slots(&shard);
        assert_eq!(shard.pick_eviction_victim(&[last.as_bytes()], false), None);
        assert_eq!(slots(&shard) - before, buckets);
        assert_eq!(shard.entries.inspected.get(), 0);
        assert_eq!(shard.memory_stats().eviction_scans, 3);
    }
}

#[test]
fn deadline_lookup_shrinks_after_most_timed_keys_are_removed() {
    let mut shard = KvShard::with_clock(TestClock::new(1));
    for index in 0..1_000 {
        let key = format!("t{index:04}");
        shard.set_bytes(key.as_bytes(), b"v");
        assert!(shard.expire_at(key.as_bytes(), UnixMillis::new(100_000)));
    }
    let grown = shard.deadlines[0].lookup.num_buckets();
    for index in 0..990 {
        assert!(shard.delete(format!("t{index:04}").as_bytes()));
    }

    assert!(shard.deadlines[0].lookup.num_buckets() * 16 <= grown);
    for index in 990..1_000 {
        assert!(shard.ttl_seconds(format!("t{index:04}").as_bytes()) > 0);
    }
}

#[test]
fn volatile_eviction_rotates_all_timed_keys_and_survives_deletes() {
    let mut shard = KvShard::with_clock(TestClock::new(1));
    shard.apply_memory_limits(MemoryLimits {
        eviction_policy: EvictionPolicy::VolatileTtl,
        maxmemory_samples: 1,
        ..MemoryLimits::default()
    });
    let mut timed = AHashSet::new();
    for db in [0u8, 1] {
        shard.enter(db);
        for index in 0..6 {
            let key = format!("t{db}{index}");
            shard.set_bytes(key.as_bytes(), b"v");
            assert!(shard.expire_at(key.as_bytes(), UnixMillis::new(1_000 + index)));
            timed.insert((db, Box::from(key.as_bytes())));
            shard.set_bytes(format!("p{db}{index}").as_bytes(), b"v");
        }
    }
    shard.enter(0);

    let victims: AHashSet<_> = (0..timed.len())
        .map(|_| shard.pick_eviction_victim(&[], true).unwrap())
        .collect();
    assert_eq!(victims, timed);
    let only_active: AHashSet<_> = (0..12)
        .map(|_| shard.pick_eviction_victim(&[], false).unwrap())
        .collect();
    assert!(only_active.iter().all(|(db, _)| *db == 0));
    assert_eq!(only_active.len(), 6);

    // Deletes and new deadlines change the lookup; sampling must stay valid.
    let next = shard.pick_eviction_victim(&[], false).unwrap();
    shard.apply_eviction(next.0, &next.1);
    let other = (0..6)
        .map(|index| format!("t0{index}"))
        .find(|key| key.as_bytes() != next.1.as_ref())
        .unwrap();
    assert!(shard.delete(other.as_bytes()));
    shard.set_bytes(b"fresh", b"v");
    assert!(shard.expire_at(b"fresh", UnixMillis::new(5)));
    let mut seen = AHashSet::new();
    for _ in 0..16 {
        seen.insert(shard.pick_eviction_victim(&[], false).unwrap());
    }
    assert!(seen.contains(&(0, Box::from(b"fresh".as_slice()))));
    assert!(!seen.contains(&next));
    assert!(!seen.contains(&(0, Box::from(other.as_bytes()))));
    assert_eq!(seen.len(), 5);
}

#[test]
fn active_expire_budget_limits_one_cycle() {
    let clock = TestClock::new(10);
    let mut shard = KvShard::with_clock(clock.clone());
    let limits = MemoryLimits {
        active_expire_keys: 1,
        ..MemoryLimits::default()
    };

    shard.apply_memory_limits(limits);
    shard.set_bytes(b"a", b"1");
    shard.set_bytes(b"b", b"2");
    assert!(shard.expire_at(b"a", UnixMillis::new(20)));
    assert!(shard.expire_at(b"b", UnixMillis::new(20)));
    clock.set(20);
    assert_eq!(shard.memory_usage().entries, 2);
    assert_eq!(shard.expire_due(), 1);
    assert_eq!(shard.memory_usage().entries, 1);
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
fn overwriting_a_string_with_a_much_smaller_value_releases_capacity() {
    let mut shard = KvShard::new();
    let large = vec![b'x'; 64 * 1024];

    shard.set_bytes(b"k", &large);
    let grown = shard.memory_usage().value_bytes;

    shard.set_bytes(b"k", b"v");
    let shrunk = shard.memory_usage().value_bytes;

    assert!(
        shrunk.saturating_mul(4) < grown,
        "string buffer stayed large after a smaller SET: {shrunk} vs {grown}"
    );
    assert_eq!(shard.get(b"k"), Some(&b"v"[..]));
}

#[test]
fn emptied_log_page_is_released_and_its_index_reused() {
    let mut log = ValueLog::default();
    let first: Vec<_> = (0..20).map(|_| log.alloc(&[1; 200])).collect();
    let second: Vec<_> = (0..20).map(|_| log.alloc(&[2; 200])).collect();

    assert_eq!(log.pages_in_use(), 2);

    for (offset, len) in first {
        log.free(offset, len);
    }

    // Page 0 is not the bump page any more, so it is released.
    assert!(!log.page_open(0));
    assert_eq!(log.pages_in_use(), 1);
    assert_eq!(log.stats().pages_released, 1);
    log.check_invariants();

    // The next page the bump needs reuses index 0 instead of growing the log.
    let end = log.end();

    for _ in 0..15 {
        log.alloc(&[3; 200]);
    }

    assert_eq!(log.end(), end);
    assert!(log.page_open(0));

    for (offset, len) in second {
        assert_eq!(log.slice(offset, len), &[2; 200][..]);
    }

    log.check_invariants();
}

#[test]
fn emptied_log_extent_goes_back_to_the_allocator() {
    let mut log = ValueLog::default();
    // Two pages per 20 spans; the test build uses 8-page extents.
    let spans: Vec<_> = (0..400).map(|_| log.alloc(&[5; 200])).collect();

    assert!(log.extents.len() >= 2);

    for &(offset, len) in &spans[..160] {
        log.free(offset, len);
    }

    assert!(log.extents[0].bytes.is_none());
    log.check_invariants();

    for &(offset, len) in &spans[160..] {
        assert_eq!(log.slice(offset, len), &[5; 200][..]);
    }

    // The released extent is the first one reused.
    for _ in 0..200 {
        log.alloc(&[6; 200]);
    }

    assert!(log.extents[0].bytes.is_some());
    log.check_invariants();
}

#[test]
fn value_log_matches_a_model_under_random_alloc_and_free() {
    let mut log = ValueLog::default();
    let mut live: Vec<(u32, u16, u8)> = Vec::new();
    let mut seed = 0x9e37_79b9_u32;

    for step in 0..30_000usize {
        seed ^= seed << 13;
        seed ^= seed >> 17;
        seed ^= seed << 5;

        if seed % 5 < 2 && !live.is_empty() {
            let (offset, len, fill) = live.swap_remove(seed as usize % live.len());

            assert!(log.slice(offset, len).iter().all(|&byte| byte == fill));
            log.free(offset, len);
        } else {
            let len = 1 + (seed >> 8) as usize % if step % 7 == 0 { 900 } else { 70 };
            let fill = step as u8;
            let (offset, len) = log.alloc(&vec![fill; len]);

            live.push((offset, len, fill));
        }

        if step % 997 == 0 {
            log.check_invariants();

            let bytes: u64 = live.iter().map(|span| u64::from(span.1)).sum();

            assert_eq!(log.live_bytes(), bytes);
        }
    }

    for (offset, len, fill) in &live {
        assert!(log.slice(*offset, *len).iter().all(|byte| byte == fill));
    }

    for (offset, len, _) in live.drain(..) {
        log.free(offset, len);
    }

    log.check_invariants();
    assert_eq!(log.live_bytes(), 0);
    assert!(log.pages_in_use() <= 1, "only the bump page may stay");
}

#[test]
fn draining_moves_spans_off_sparse_pages_and_releases_them() {
    let mut log = ValueLog::default();
    let all: Vec<(u32, u16, u8)> = (0..4_000usize)
        .map(|index| {
            let (offset, len) = log.alloc(&[index as u8; 40]);

            (offset, len, index as u8)
        })
        .collect();
    let mut live = Vec::new();

    for (index, span) in all.into_iter().enumerate() {
        if index % 3 == 0 {
            live.push(span);
        } else {
            log.free(span.0, span.1);
        }
    }

    // Two of every three spans are gone: no page empties, but most of the log is dead.
    let before = log.pages_in_use();

    assert!(log.plan_drain() > 0);
    log.check_invariants();

    for (offset, len, _) in &mut live {
        if let Some(moved) = log.relocate_value(*offset, *len) {
            *offset = moved;
        }
    }

    log.end_drain();
    log.check_invariants();

    for (offset, len, fill) in &live {
        assert!(log.slice(*offset, *len).iter().all(|byte| byte == fill));
    }

    assert!(
        log.pages_in_use() < before,
        "{} vs {before}",
        log.pages_in_use()
    );
    assert!(log.stats().relocated_bytes > 0);
}

#[test]
fn free_spans_do_not_merge_across_a_page() {
    let mut log = ValueLog::default();
    let (head, head_len) = log.alloc(&[1; 4090]);
    let (next, next_len) = log.alloc(&[2; 16]);

    log.free(head, head_len);
    log.free(next, next_len);

    for _ in 0..64 {
        let (offset, len) = log.alloc(b"0123456789");

        assert_eq!(log.slice(offset, len), b"0123456789");
    }
}

#[test]
fn popping_most_of_a_list_shrinks_the_ring() {
    let mut shard = KvShard::new();
    let (inserted, removed) = shrink_counts();
    let values: Vec<Vec<u8>> = (0..inserted)
        .map(|index| format!("v{index:04}").into_bytes())
        .collect();

    assert_eq!(shard.list_push_front(b"l", &values), Ok(inserted));
    let grown = shard.memory_usage().collection_bytes;

    for _ in 0..removed {
        assert!(shard.list_pop_front(b"l").unwrap().is_some());
    }

    assert_eq!(shard.list_len(b"l"), Ok(inserted - removed));
    let shrunk = shard.memory_usage().collection_bytes;

    assert!(
        shrunk.saturating_mul(4) < grown,
        "list ring stayed large after pops: {shrunk} vs {grown}"
    );
}

#[test]
fn removing_most_compact_hash_fields_shrinks_the_buffer() {
    let mut shard = KvShard::new();
    let limits = MemoryLimits {
        hash_max_compact_entries: 4_096,
        hash_max_compact_value_bytes: 64,
        ..MemoryLimits::default()
    };

    shard.apply_memory_limits(limits);
    let (inserted, removed) = compact_hash_counts();
    let fields: Vec<(Vec<u8>, Vec<u8>)> = (0..inserted)
        .map(|index| (format!("f{index:04}").into_bytes(), b"v".to_vec()))
        .collect();
    let pairs: Vec<(&[u8], &[u8])> = fields
        .iter()
        .map(|(field, value)| (field.as_slice(), value.as_slice()))
        .collect();

    assert_eq!(shard.hash_set(b"h", &pairs), Ok(inserted));
    assert_eq!(shard.hash_is_compact(b"h"), Ok(Some(true)));
    let usage = shard.memory_usage();
    let grown = usage.value_bytes + usage.collection_bytes;
    let remove: Vec<&[u8]> = fields[..removed]
        .iter()
        .map(|(field, _)| field.as_slice())
        .collect();

    assert_eq!(shard.hash_delete(b"h", &remove), Ok(removed));
    assert_eq!(shard.hash_is_compact(b"h"), Ok(Some(true)));
    let usage = shard.memory_usage();
    let shrunk = usage.value_bytes + usage.collection_bytes;

    assert!(
        shrunk.saturating_mul(4) < grown,
        "compact hash buffer stayed large after deletes: {shrunk} vs {grown}"
    );
}

#[test]
fn key_notices_stay_empty_until_the_gate_is_open() {
    let mut shard = KvShard::new();

    shard.set_notify_mask(crate::notify::parse_mask("Eg$e").unwrap());
    shard.set_bytes(b"live", b"1");
    assert!(shard.delete(b"live"));
    assert!(shard.take_notices().is_empty());

    shard.set_notify_listeners(1);
    shard.set_bytes(b"live", b"1");
    assert_eq!(shard.get(b"live"), Some(&b"1"[..]));
    let pending = shard.take_notices();

    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].2, crate::notify::KeyEvent::Set);

    assert!(shard.delete(b"live"));
    let deleted = shard.take_notices();

    assert_eq!(deleted.len(), 1);
    assert_eq!(deleted[0].2, crate::notify::KeyEvent::Del);

    shard.set_bytes(b"gone", b"1");
    shard.take_notices();
    shard.apply_eviction(0, b"gone");
    let evicted = shard.take_notices();

    assert_eq!(evicted.len(), 1);
    assert_eq!(evicted[0].2, crate::notify::KeyEvent::Evicted);
    let frames = crate::notify::notices(
        shard.notify_mask(),
        evicted[0].0,
        &evicted[0].1,
        evicted[0].2,
    );

    assert_eq!(frames.len(), 1);
    assert_eq!(frames[0].channel, b"__keyevent@0__:evicted");
    assert_eq!(frames[0].payload, b"gone");
}

#[test]
fn deleting_an_expired_key_records_expired_not_del() {
    let clock = crate::time::TestClock::new(1_000);
    let mut shard = KvShard::with_clock(clock.clone());

    shard.set_notify_mask(crate::notify::parse_mask("Egx").unwrap());
    shard.set_notify_listeners(1);
    shard.set_bytes(b"temp", b"1");
    assert!(shard.take_notices().is_empty());
    assert!(shard.expire_at(b"temp", UnixMillis::new(1_500)));
    clock.set(1_500);
    assert!(!shard.delete(b"temp"));
    let pending = shard.take_notices();

    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].1.as_ref(), b"temp");
    assert_eq!(pending[0].2, crate::notify::KeyEvent::Expired);
    let frames = crate::notify::notices(
        shard.notify_mask(),
        pending[0].0,
        &pending[0].1,
        pending[0].2,
    );

    assert_eq!(frames[0].channel, b"__keyevent@0__:expired");
    assert_eq!(frames[0].payload, b"temp");
}

#[test]
fn value_log_refuses_admission_near_limit_and_recovers_after_frees() {
    let mut log = ValueLog::default();

    log.set_limit_for_test(64 * LOG_PAGE as u32);
    assert!(!log.near_limit());

    let mut live = Vec::new();

    while !log.near_limit() {
        let value = [live.len() as u8; 200];

        live.push(log.alloc(&value));
    }

    // Stopped before the cursor reached the limit, and everything written is intact.
    assert!(log.end() < log.limit);
    for (index, &(offset, len)) in live.iter().enumerate() {
        assert_eq!(log.slice(offset, len), &[index as u8; 200][..]);
    }

    // Freeing enough adjacent spans produces a page-sized hole and admission reopens.
    for (offset, len) in live.drain(..40) {
        log.free(offset, len);
    }

    assert!(!log.near_limit());
}

#[test]
fn value_log_offsets_never_reach_the_large_block_bit() {
    let mut log = ValueLog::default();

    assert_eq!(LOG_LIMIT, LOG_LARGE);
    log.set_limit_for_test(u32::MAX);
    assert_eq!(log.limit, LOG_LIMIT);
}

#[test]
#[should_panic(expected = "value log exhausted")]
fn value_log_bump_past_limit_panics_instead_of_wrapping() {
    let mut log = ValueLog::default();

    log.set_limit_for_test(2 * LOG_PAGE as u32);
    for _ in 0..100 {
        log.alloc(&[7; 1000]);
    }
}

#[test]
fn shard_writes_get_oom_admission_when_log_is_near_limit() {
    let mut shard = KvShard::new();

    shard.value_log.set_limit_for_test(64 * LOG_PAGE as u32);
    assert!(!shard.memory_admission_would_fail(0));

    let mut written = 0usize;

    while !shard.memory_admission_would_fail(0) {
        shard.set_bytes(format!("key:{written}").as_bytes(), &[b'v'; 64]);
        written += 1;
        assert!(written < 100_000, "admission never tripped");
    }

    for index in 0..written {
        assert_eq!(
            shard.get(format!("key:{index}").as_bytes()),
            Some(&[b'v'; 64][..])
        );
    }

    // Deleting keys frees spans, and the shard accepts writes again.
    for index in 0..written / 2 {
        shard.delete(format!("key:{index}").as_bytes());
    }

    assert!(!shard.memory_admission_would_fail(0));
}

fn lru_shard(clock: &TestClock) -> KvShard<TestClock> {
    let mut shard = KvShard::with_clock(clock.clone());

    shard.apply_memory_limits(MemoryLimits {
        eviction_policy: EvictionPolicy::AllKeysLru,
        maxmemory_samples: 16,
        ..MemoryLimits::default()
    });
    shard
}

#[test]
fn lru_clock_wrap_does_not_make_new_keys_look_oldest() {
    let clock = TestClock::new(1_000);
    let mut shard = lru_shard(&clock);

    shard.set_bytes(b"old", b"1");
    shard.touch(b"old");
    shard.set_bytes(b"new", b"1");
    shard.touch(b"new");

    // 2^24 seconds later the stamps have wrapped to a smaller number than "old"'s.
    let wrap_ms = (1u64 << 24) * 1000;

    clock.set(1_000 + wrap_ms - 5_000);
    shard.touch(b"new");
    clock.set(1_000 + wrap_ms + 3_000);
    shard.touch(b"new");

    let victim = shard.pick_eviction_victim(&[], true).map(|(_, key)| key);

    assert_eq!(victim.as_deref(), Some(b"old".as_slice()));
}

#[test]
fn lru_idle_age_is_seconds_not_access_count() {
    let clock = TestClock::new(0);
    let mut shard = lru_shard(&clock);

    shard.set_bytes(b"busy", b"1");
    shard.set_bytes(b"quiet", b"1");
    shard.touch(b"quiet");

    for second in 1..=50u64 {
        clock.set(second * 1000);
        shard.touch(b"busy");
    }

    // Many touches of "busy" must not make "quiet" look newer than it is.
    let victim = shard.pick_eviction_victim(&[], true).map(|(_, key)| key);

    assert_eq!(victim.as_deref(), Some(b"quiet".as_slice()));
}

#[test]
fn lfu_counter_decays_after_a_minute_without_access() {
    let clock = TestClock::new(0);
    let mut shard = KvShard::with_clock(clock.clone());

    shard.apply_memory_limits(MemoryLimits {
        eviction_policy: EvictionPolicy::AllKeysLfu,
        maxmemory_samples: 16,
        ..MemoryLimits::default()
    });
    shard.set_bytes(b"hot", b"1");
    shard.set_bytes(b"cold", b"1");

    for _ in 0..5_000 {
        shard.touch(b"hot");
    }
    shard.touch(b"cold");

    let fresh = shard.eviction_score(0, b"hot", None);

    assert!(fresh < shard.eviction_score(0, b"cold", None));

    // Left alone for ten minutes the hot counter decays by about ten steps.
    clock.set(10 * 60_000);

    let aged = shard.eviction_score(0, b"hot", None);

    assert!(aged > fresh, "counter did not decay: {aged} <= {fresh}");

    // Touching "cold" now resets its idle time while "hot" keeps decaying.
    for _ in 0..3 {
        shard.touch(b"cold");
    }
    clock.set(400 * 60_000);

    assert_eq!(
        shard.eviction_score(0, b"hot", None) >> 32,
        u64::from(u8::MAX),
        "counter should be fully decayed"
    );
}

#[test]
fn eviction_pool_keeps_good_candidates_between_calls() {
    let clock = TestClock::new(0);
    let mut shard = lru_shard(&clock);

    shard.apply_memory_limits(MemoryLimits {
        eviction_policy: EvictionPolicy::AllKeysLru,
        maxmemory_samples: 4,
        ..MemoryLimits::default()
    });

    for index in 0..64u64 {
        clock.set(index * 1000);
        let key = format!("k{index:02}");

        shard.set_bytes(key.as_bytes(), b"v");
        shard.touch(key.as_bytes());
    }

    clock.set(100_000);

    let mut ages = Vec::new();

    for _ in 0..8 {
        let (_, key) = shard.pick_eviction_victim(&[], true).unwrap();
        let index: u64 = std::str::from_utf8(&key[1..]).unwrap().parse().unwrap();

        ages.push(index);
        shard.delete(&key);
    }

    // With a pool, picks stay near the oldest keys even though each call samples only 4.
    assert!(ages.iter().all(|&index| index < 40), "{ages:?}");
}

#[test]
fn eviction_pool_rescores_its_leader_before_taking_it() {
    let clock = TestClock::new(0);
    let mut shard = lru_shard(&clock);

    shard.set_bytes(b"cold", b"v");
    shard.set_bytes(b"warm", b"v");
    shard.set_bytes(b"hot", b"v");
    shard.touch(b"cold");
    clock.set(40_000);
    shard.touch(b"warm");
    clock.set(100_000);
    shard.touch(b"hot");

    // Pooled scores as of an earlier call: "hot" looked the oldest then and was read
    // since; slot 3 no longer holds a key.
    let keys: [&[u8]; 3] = [b"cold", b"warm", b"hot"];
    let resolve = |_: u8, slot: usize| keys.get(slot).map(|key| (*key, None));

    shard.offer_eviction_candidate(0, 2, 1_000);
    shard.offer_eviction_candidate(0, 3, 900);
    shard.offer_eviction_candidate(0, 0, 100);
    shard.offer_eviction_candidate(0, 1, 60);

    let take = |exclude: &[&[u8]]| {
        shard
            .take_eviction_victim(exclude, true, resolve)
            .map(|(_, key)| key.into_vec())
    };

    // An excluded key is dropped from the pool, not taken.
    assert_eq!(take(&[b"cold"]), Some(b"warm".to_vec()));
    assert_eq!(take(&[]), Some(b"hot".to_vec()));
    assert_eq!(take(&[]), None);
}

#[test]
fn full_eviction_pool_turns_away_weaker_candidates() {
    let clock = TestClock::new(0);
    let mut shard = lru_shard(&clock);
    let keys: Vec<Vec<u8>> = (0..20)
        .map(|index| format!("k{index:02}").into_bytes())
        .collect();

    for key in &keys {
        shard.set_bytes(key, b"v");
    }
    // Every key now scores above its stored score, so rescoring the leader keeps it the
    // leader and the stored scores decide the order.
    clock.set(1_000_000);
    for slot in 0..16 {
        shard.offer_eviction_candidate(0, slot, 100 + slot as u64);
    }
    shard.offer_eviction_candidate(0, 16, 50);
    shard.offer_eviction_candidate(0, 17, 500);
    shard.offer_eviction_candidate(0, 3, 400);

    let resolve = |_: u8, slot: usize| keys.get(slot).map(|key| (key.as_slice(), None));
    let mut taken = Vec::new();

    while let Some((_, key)) = shard.take_eviction_victim(&[], true, resolve) {
        taken.push(String::from_utf8(key.into_vec()).unwrap());
    }

    assert_eq!(taken.len(), 16, "{taken:?}");
    assert_eq!(&taken[..2], ["k17", "k03"]);
    assert!(
        !taken.contains(&"k16".to_owned()),
        "below the weakest entry"
    );
    assert!(
        !taken.contains(&"k00".to_owned()),
        "replaced as the weakest"
    );
}

#[test]
fn eviction_sampling_without_deadlines_never_picks_an_excluded_key() {
    let clock = TestClock::new(0);
    let mut shard = lru_shard(&clock);

    for index in 0..40 {
        shard.set_bytes(format!("k{index:02}").as_bytes(), b"v");
        if index % 3 == 0 {
            shard.touch(format!("k{index:02}").as_bytes());
        }
    }
    clock.set(50_000);

    let mut evicted = 0;

    while let Some((_, key)) = shard.pick_eviction_victim(&[b"k07"], false) {
        assert_ne!(&*key, b"k07");
        assert!(shard.delete(&key));
        evicted += 1;
    }

    assert_eq!(evicted, 39);
    assert_eq!(shard.len(), 1);
}

fn eviction_order(shard: &mut KvShard<TestClock>) -> Vec<Vec<u8>> {
    let mut order = Vec::new();

    while let Some((_, key)) = shard.pick_eviction_victim(&[], false) {
        assert!(shard.delete(&key));
        order.push(key.into_vec());
    }
    order
}

fn written_in_two_generations(policy: EvictionPolicy, later_ms: u64) -> Vec<Vec<u8>> {
    let clock = TestClock::new(1_000);
    let mut shard = lru_shard(&clock);

    shard.apply_memory_limits(MemoryLimits {
        eviction_policy: policy,
        maxmemory_samples: 16,
        ..MemoryLimits::default()
    });
    for index in 0..40 {
        shard.set_bytes(format!("a{index:02}").as_bytes(), b"old");
    }
    // Overwriting a key restamps it: "a00" joins the newer generation.
    clock.set(later_ms);
    shard.set_bytes(b"a00", b"rewritten");
    for index in 0..40 {
        shard.set_bytes(format!("b{index:02}").as_bytes(), b"new");
    }
    clock.set(later_ms + 1_000);

    eviction_order(&mut shard)
}

fn assert_older_writes_go_first(order: &[Vec<u8>]) {
    let older = order
        .iter()
        .take_while(|key| key[0] == b'a' && key.as_slice() != b"a00")
        .count();

    assert_eq!(order.len(), 80);
    assert_eq!(older, 39, "{:?}", &order[..older + 1]);
}

#[test]
fn allkeys_lru_evicts_keys_written_earlier_first() {
    assert_older_writes_go_first(&written_in_two_generations(
        EvictionPolicy::AllKeysLru,
        100_000,
    ));
}

#[test]
fn allkeys_lfu_starts_written_keys_at_the_initial_counter() {
    // Ten minutes on, the older keys' counters have decayed below the initial value
    // the newer writes start with.
    assert_older_writes_go_first(&written_in_two_generations(
        EvictionPolicy::AllKeysLfu,
        10 * 60_000,
    ));
}

#[test]
fn writes_leave_no_access_metadata_without_a_recency_policy() {
    let clock = TestClock::new(1_000);
    let mut shard = KvShard::with_clock(clock.clone());

    shard.set_bytes(b"key", b"value");
    shard.set_integer(b"counter", 7).unwrap();

    assert_eq!(shard.entries.access_get(b"key"), 0);
    assert_eq!(shard.entries.access_get(b"counter"), 0);
}

#[test]
fn keys_a_write_protects_do_not_starve_eviction_of_the_others() {
    let clock = TestClock::new(1_000);
    let mut shard = lru_shard(&clock);

    shard.apply_memory_limits(MemoryLimits {
        eviction_policy: EvictionPolicy::AllKeysLru,
        maxmemory_samples: 5,
        ..MemoryLimits::default()
    });
    let cold: Vec<Vec<u8>> = (0..24)
        .map(|i| format!("cold{i:02}").into_bytes())
        .collect();

    for key in &cold {
        shard.set_bytes(key, b"v");
    }
    clock.set(100_000);
    for index in 0..400 {
        shard.set_bytes(format!("warm{index:03}").as_bytes(), b"v");
    }
    clock.set(200_000);

    // An MSET over the cold keys protects every one of them.
    let protected: Vec<&[u8]> = cold.iter().map(Vec::as_slice).collect();
    let mut evicted = 0;

    while let Some((_, key)) = shard.pick_eviction_victim(&protected, false) {
        assert!(!protected.contains(&&*key));
        assert!(shard.delete(&key));
        evicted += 1;
    }

    assert_eq!(evicted, 400);
    assert_eq!(shard.len(), 24);
}

#[test]
fn a_pick_limited_to_the_active_database_never_takes_another_databases_key() {
    let clock = TestClock::new(1_000);
    let mut shard = lru_shard(&clock);

    shard.enter(1);
    for index in 0..20 {
        shard.set_bytes(format!("parked{index:02}").as_bytes(), b"v");
    }
    shard.enter(0);
    clock.set(100_000);
    for index in 0..3 {
        shard.set_bytes(format!("active{index}").as_bytes(), b"v");
    }
    clock.set(200_000);

    // A pick over every database pools the colder keys of database 1.
    let (db, _) = shard.pick_eviction_victim(&[], true).unwrap();

    assert_eq!(db, 1);

    // A transaction buffer logs deletes against its own database: its picks stay there.
    for _ in 0..3 {
        let (db, key) = shard.pick_eviction_victim(&[], false).unwrap();

        assert_eq!(db, 0, "{}", String::from_utf8_lossy(&key));
        assert!(shard.delete(&key));
    }
    assert!(shard.pick_eviction_victim(&[], false).is_none());
}

#[test]
fn hash_field_expiry_does_not_count_as_an_access() {
    let clock = TestClock::new(1_000);
    let mut shard = lru_shard(&clock);

    shard.apply_memory_limits(MemoryLimits {
        eviction_policy: EvictionPolicy::AllKeysLru,
        maxmemory_samples: 16,
        active_expire_keys: 16,
        ..MemoryLimits::default()
    });
    shard
        .hash_set(b"hash", &[(&b"due"[..], &b"v"[..]), (b"kept", b"v")])
        .unwrap();
    assert_eq!(shard.hash_expire_fields(b"hash", &[b"due"], 60_000), Ok(1));
    clock.set(50_000);
    shard.set_bytes(b"newer", b"v");
    clock.set(100_000);

    assert_eq!(shard.expire_hash_fields_due(), 1);
    assert_eq!(shard.hash_len(b"hash"), Ok(1));
    // A read lazily purging due fields is the read's access, not the purge's.
    assert_eq!(
        shard
            .pick_eviction_victim(&[], false)
            .map(|(_, key)| key.into_vec()),
        Some(b"hash".to_vec())
    );
}

mod scan_properties {
    use super::*;
    use proptest::prelude::*;
    use std::collections::BTreeSet;

    #[derive(Debug, Clone)]
    enum Step {
        Insert(u16),
        Delete(u16),
        Scan(u8),
    }

    fn steps() -> impl Strategy<Value = Vec<Step>> {
        prop::collection::vec(
            prop_oneof![
                4 => (0u16..4_000).prop_map(Step::Insert),
                2 => (0u16..4_000).prop_map(Step::Delete),
                3 => (1u8..40).prop_map(Step::Scan),
            ],
            1..400,
        )
    }

    fn key(id: u16) -> Vec<u8> {
        format!("k:{id}").into_bytes()
    }

    proptest! {
        // Keys present at the start and never deleted come back at least once,
        // whatever grows or rehashes the table between SCAN calls.
        #[test]
        fn untouched_keys_survive_writes_between_scan_calls(
            initial in prop::collection::btree_set(0u16..4_000, 0..600),
            steps in steps(),
        ) {
            let mut shard = KvShard::new();
            for id in &initial {
                shard.set_bytes(&key(*id), b"v");
            }

            let mut survivors: BTreeSet<u16> = initial.clone();
            let mut returned: BTreeSet<Vec<u8>> = BTreeSet::new();
            let mut cursor = 0u64;
            let mut finished = false;

            for step in steps {
                match step {
                    Step::Insert(id) => {
                        shard.set_bytes(&key(id), b"v");
                    }
                    Step::Delete(id) => {
                        if !finished {
                            survivors.remove(&id);
                        }
                        shard.delete(&key(id));
                    }
                    Step::Scan(count) if !finished => {
                        let (next, keys) = shard.scan(cursor, usize::from(count), None, None);
                        returned.extend(keys.into_iter().map(<[u8]>::to_vec));
                        cursor = next;
                        finished = cursor == 0;
                    }
                    Step::Scan(_) => {}
                }
            }

            // Finish the traversal with no further writes.
            let mut guard = 0usize;
            while !finished {
                let (next, keys) = shard.scan(cursor, 25, None, None);
                returned.extend(keys.into_iter().map(<[u8]>::to_vec));
                cursor = next;
                finished = cursor == 0;
                guard += 1;
                prop_assert!(guard < 100_000, "scan did not terminate");
            }

            for id in survivors {
                prop_assert!(returned.contains(&key(id)), "lost {id}");
            }
        }
    }
}

#[test]
fn deleting_a_large_collection_frees_it_off_the_shard_thread() {
    let mut shard = KvShard::new();
    let members: Vec<Vec<u8>> = (0..20_000u32).map(|id| id.to_be_bytes().to_vec()).collect();

    shard.set_add(b"big", &members).unwrap();
    shard.set_bytes(b"small", b"v");
    let charged = shard.memory_usage();
    assert!(charged.collection_bytes + charged.value_bytes >= crate::lazy_free::MIN_DEFERRED_BYTES);

    assert!(shard.delete(b"big"));
    assert_eq!(shard.set_card(b"big"), Ok(0));
    assert_eq!(shard.get(b"small"), Some(&b"v"[..]));
    assert!(shard.memory_usage().collection_bytes < charged.collection_bytes);

    let start = std::time::Instant::now();
    while crate::lazy_free::pending_bytes() != 0 {
        assert!(start.elapsed() < std::time::Duration::from_secs(5));
        std::thread::yield_now();
    }
}

#[test]
fn flush_all_hands_the_index_and_log_to_the_lazy_free_thread() {
    let mut shard = KvShard::new();

    for id in 0..5_000u32 {
        shard.set_bytes(format!("a:{id}").as_bytes(), &[7u8; 200]);
    }
    shard.enter(2);
    shard
        .set_add(b"set", &[b"x".to_vec(), b"y".to_vec()])
        .unwrap();
    shard.enter(0);

    shard.flush_all();

    assert_eq!(shard.len(), 0);
    assert_eq!(shard.memory_usage().value_bytes, 0);
    assert_eq!(shard.memory_usage().collection_bytes, 0);
    shard.set_bytes(b"after", b"flush");
    assert_eq!(shard.get(b"after"), Some(&b"flush"[..]));
    shard.enter(2);
    assert_eq!(shard.set_card(b"set"), Ok(0));
}

#[test]
fn incremental_snapshot_matches_the_point_in_time_state_under_concurrent_writes() {
    let mut shard = KvShard::new();
    let mut expected = std::collections::BTreeMap::new();

    for index in 0..3_000u32 {
        let key = format!("key:{index}").into_bytes();
        let value = format!("v{index}").into_bytes();

        shard.set_bytes(&key, &value);
        expected.insert(key, value);
    }

    shard.begin_snapshot(1 << 24).unwrap();

    let mut written = Vec::new();
    let mut round = 0u32;

    loop {
        let step = shard.snapshot_step(100, &mut |entry| written.push(entry.to_entry()));

        // Writes between slices: overwrite, delete and create (enough to grow the table).
        for offset in 0..40u32 {
            let key = format!("key:{}", (round * 97 + offset * 31) % 3_000).into_bytes();

            shard.capture_snapshot_key(&key);

            if offset % 3 == 0 {
                shard.delete(&key);
            } else {
                shard.set_bytes(&key, b"changed");
            }

            let fresh = format!("fresh:{round}:{offset}").into_bytes();

            shard.capture_snapshot_key(&fresh);
            shard.set_bytes(&fresh, b"new");
        }

        round += 1;
        // Log reclaim may run between slices: values move, key records stay put.
        shard.reclaim_logs(std::time::Duration::from_millis(1));

        match step {
            SnapshotStep::Done => break,
            SnapshotStep::More => {}
            SnapshotStep::Waiting => {
                shard.migrate_indexes(std::time::Duration::from_millis(5));
            }
        }
    }

    written.extend(shard.finish_snapshot_journal());

    let mut actual = std::collections::BTreeMap::new();

    for entry in written {
        let SnapshotValue::Bytes(value) = entry.value else {
            panic!("unexpected value type");
        };

        assert!(
            actual.insert(entry.key, value).is_none(),
            "key written twice"
        );
    }

    assert_eq!(actual, expected);
}

mod ttl_hint_properties {
    use super::*;
    use proptest::prelude::*;

    #[derive(Debug, Clone)]
    enum Op {
        Set(u8),
        Expire(u8),
        Persist(u8),
        Increment(u8),
        AddMembers(u8, u8),
        AddScores(u8, u8),
        Rename(u8, u8),
        Delete(u8),
        Enter(u8),
        Swap(u8, u8),
    }

    fn ops() -> impl Strategy<Value = Vec<Op>> {
        prop::collection::vec(
            prop_oneof![
                3 => (0u8..8).prop_map(Op::Set),
                4 => (0u8..8).prop_map(Op::Expire),
                2 => (0u8..8).prop_map(Op::Persist),
                1 => (0u8..8).prop_map(Op::Increment),
                3 => (0u8..8, 1u8..200).prop_map(|(key, count)| Op::AddMembers(key, count)),
                3 => (0u8..8, 1u8..120).prop_map(|(key, count)| Op::AddScores(key, count)),
                2 => (0u8..8, 0u8..8).prop_map(|(from, to)| Op::Rename(from, to)),
                2 => (0u8..8).prop_map(Op::Delete),
                1 => (0u8..3).prop_map(Op::Enter),
                1 => (0u8..3, 0u8..3).prop_map(|(left, right)| Op::Swap(left, right)),
            ],
            1..300,
        )
    }

    fn key(id: u8) -> Vec<u8> {
        format!("key:{id}").into_bytes()
    }

    /// A key with a deadline record must carry the index's TTL bit.
    fn assert_hint_covers_deadlines(shard: &KvShard) {
        for (db, index) in shard.database_maps() {
            for (key, _) in index.entries() {
                if shard.deadline(db, key).is_some() {
                    let (_, hint) = index.lookup_ttl(key).unwrap();

                    assert!(
                        hint,
                        "db {db} key {key:?} has a deadline but a clear TTL bit"
                    );
                }
            }
        }
    }

    proptest! {
        #[test]
        fn clear_ttl_bit_means_no_deadline(ops in ops()) {
            let mut shard = KvShard::new();
            let far = UnixMillis::new(shard.now_unix_millis().get() + 10_000_000_000);

            for op in ops {
                match op {
                    Op::Set(id) => shard.set_bytes(&key(id), b"v"),
                    Op::Expire(id) => {
                        shard.expire_at(&key(id), far);
                    }
                    Op::Persist(id) => {
                        shard.persist(&key(id));
                    }
                    Op::Increment(id) => {
                        let _ = shard.increment(&key(id), 1);
                    }
                    Op::AddMembers(id, count) => {
                        let members: Vec<Vec<u8>> =
                            (0..count).map(|n| vec![n, 7, 7, 7, 7, 7, 7, 7]).collect();

                        let _ = shard.set_add(&key(id), &members);
                    }
                    Op::AddScores(id, count) => {
                        for n in 0..count {
                            let _ = shard.sorted_set_add(&key(id), f64::from(n), &[n, 9, 9, 9, 9, 9, 9, 9]);
                        }
                    }
                    Op::Rename(from, to) => {
                        shard.rename(&key(from), &key(to));
                    }
                    Op::Delete(id) => {
                        shard.delete(&key(id));
                    }
                    Op::Enter(db) => shard.enter(db),
                    Op::Swap(left, right) => shard.swap_databases(left, right),
                }

                assert_hint_covers_deadlines(&shard);
            }
        }
    }
}

#[test]
fn reclaim_logs_drains_sparse_pages_and_keeps_values_and_deadlines() {
    let clock = TestClock::new(1_000);
    let mut shard = KvShard::with_clock(clock.clone());
    let key = |db: u8, index: u64| format!("key:{db}:{index}").into_bytes();
    let value = |index: u64| format!("value-{index:032}").into_bytes();

    for db in [0u8, 3] {
        shard.enter(db);

        for index in 0..6_000u64 {
            shard.set_bytes(&key(db, index), &value(index));

            if index % 7 == 0 {
                assert!(shard.expire_at(&key(db, index), UnixMillis::new(100_000 + index)));
            }
        }

        for index in (0..6_000u64).filter(|index| index % 3 != 0) {
            assert!(shard.delete(&key(db, index)));
        }
    }

    shard.enter(0);
    let before = shard.log_stats();
    let mut calls = 0;

    // Rounds repeat while the logs stay sparse, and stop once they are dense.
    loop {
        let moved = shard.log_stats().relocated_bytes;
        // As maintenance does: the shrink the deletes started finishes first.
        let migrated = shard.migrate_indexes(std::time::Duration::from_micros(200));
        let done = migrated && shard.reclaim_logs(std::time::Duration::from_micros(200));

        calls += 1;
        assert!(calls < 10_000, "reclaim does not converge");

        if done && shard.log_stats().relocated_bytes == moved {
            break;
        }
    }

    let after = shard.log_stats();

    assert!(after.relocated_bytes > 0);
    assert!(
        after.pages * 2 < before.pages,
        "pages {} -> {}",
        before.pages,
        after.pages
    );
    shard.value_log.check_invariants();

    for db in [0u8, 3] {
        shard.enter(db);

        for index in 0..6_000u64 {
            let found = shard.get(&key(db, index));

            if index % 3 == 0 {
                assert_eq!(found, Some(&value(index)[..]), "db {db} key {index}");

                let expected = if index % 7 == 0 {
                    (100_000 + index - 1_000) as i64
                } else {
                    -1
                };

                assert_eq!(shard.pttl(&key(db, index)), expected);
            } else {
                assert_eq!(found, None);
            }
        }
    }

    // Deadlines still find their moved key records.
    clock.set(200_000);

    for db in [0u8, 3] {
        shard.enter(db);
        while shard.expire_cycle(100) > 0 {}

        for index in (0..6_000u64).filter(|index| index % 3 == 0) {
            assert_eq!(shard.get(&key(db, index)).is_some(), index % 7 != 0);
        }
    }
}

#[test]
fn reclaim_logs_leaves_key_records_alone_during_a_snapshot() {
    let mut shard = KvShard::new();

    for index in 0..6_000u32 {
        shard.set_bytes(format!("k{index}").as_bytes(), &[b'v'; 40]);
    }

    for index in (0..6_000u32).filter(|index| index % 4 != 0) {
        shard.delete(format!("k{index}").as_bytes());
    }

    let addresses: Vec<_> = (0..6_000u32)
        .step_by(4)
        .map(|index| shard.entries.key_address(format!("k{index}").as_bytes()))
        .collect();

    shard.begin_snapshot(usize::MAX).unwrap();
    while !shard.reclaim_logs(std::time::Duration::from_micros(200)) {}

    let moved: Vec<_> = (0..6_000u32)
        .step_by(4)
        .map(|index| shard.entries.key_address(format!("k{index}").as_bytes()))
        .collect();

    assert_eq!(addresses, moved);
    assert!(shard.log_stats().relocated_bytes > 0, "values still move");

    for index in (0..6_000u32).step_by(4) {
        assert_eq!(
            shard.get(format!("k{index}").as_bytes()),
            Some(&[b'v'; 40][..])
        );
    }

    shard.abort_snapshot();
}

#[test]
fn slab_pages_are_released_when_their_sets_go() {
    let mut shard = KvShard::new();

    for index in 0..6_000u32 {
        shard
            .set_add(format!("s{index}").as_bytes(), &[b"a".as_slice(), b"b"])
            .unwrap();
    }

    let before = shard.entries.slab_pages_in_use();

    for index in 0..6_000u32 {
        shard.delete(format!("s{index}").as_bytes());
    }

    assert!(before > 10);
    assert_eq!(shard.entries.slab_pages_in_use(), 1);
}

#[test]
fn deadlines_moved_later_still_expire_in_order() {
    let mut deadlines = DeadlineDb::default();
    let mut reference = BTreeMap::new();
    let mut seed = 0x2545_f491_4f6c_dd1d_u64;

    for _ in 0..4_000 {
        seed ^= seed << 13;
        seed ^= seed >> 7;
        seed ^= seed << 17;
        let key = format!("key-{}", seed % 300);
        let at = UnixMillis::new(1_000 + (seed >> 8) % 4_000);
        deadlines.set(key.as_bytes(), None, at, |_| None);
        reference.insert(key.into_bytes(), at);
    }

    // Reap step by step: every due key comes out, none early, each exactly once.
    for now in (1_000..=5_000).step_by(250) {
        let now = UnixMillis::new(now);

        while let Some(key) = deadlines.first_due(now, |_| unreachable!("owned keys")) {
            let at = reference.remove(&*key).expect("reaped once");
            assert!(at <= now);
            assert!(deadlines.remove(&key, |_| None));
        }

        assert!(reference.values().all(|&at| at > now));
        assert!(deadlines.min_at().is_none_or(|lowest| lowest > now));
    }

    assert!(reference.is_empty());
    assert_eq!(size_of::<Option<DeadlineRecord>>(), 32);
}

#[test]
fn slab_stats_count_pages_and_records_per_class() {
    let mut shard = KvShard::new();

    assert!(
        shard
            .slab_stats()
            .iter()
            .all(|class| class.pages == 0 && class.live == 0)
    );

    for index in 0..300u32 {
        shard
            .list_push_back(format!("l{index}").as_bytes(), &[b"x".as_slice()])
            .unwrap();
    }

    let stats = shard.slab_stats();
    let records: u64 = stats.iter().map(|class| class.live).sum();
    let pages: u64 = stats.iter().map(|class| class.pages).sum();

    assert_eq!(records, 300);
    assert!(pages >= 1);
    assert!(
        stats
            .windows(2)
            .all(|pair| pair[0].slot_bytes < pair[1].slot_bytes)
    );
}

#[test]
fn active_expiry_drops_a_stale_deadline_instead_of_spinning() {
    let clock = TestClock::new(1_000);
    let mut shard = KvShard::with_clock(clock.clone());

    for key in [b"stale".as_slice(), b"due"] {
        shard.set_bytes(key, b"v");
        assert!(shard.expire_at(key, UnixMillis::new(2_000)));
    }

    // A deadline record whose key no longer says it may expire: the reaper cannot
    // remove the key through it.
    let active = shard.active;
    shard.mark_ttl_hint(active, b"stale", false);
    clock.set(2_000);

    assert_eq!(shard.expire_cycle(10), 1, "only the real expiry counts");
    assert_eq!(shard.get(b"due"), None);
    assert_eq!(shard.expiration_stats().keys_with_ttl, 0);
    assert!(
        !shard.soonest_deadline_due(),
        "no backlog is left to spin on"
    );
    assert_eq!(shard.expire_cycle(10), 0);
    assert_eq!(shard.get(b"stale"), Some(&b"v"[..]));
}

/// Members of `key` drawn by `draws` single SRANDMEMBER calls.
fn single_draws(shard: &KvShard, key: &[u8], draws: usize) -> Vec<Vec<u8>> {
    (0..draws)
        .map(|_| shard.set_random_members(key, None).unwrap().remove(0))
        .collect()
}

fn assert_spread(draws: &[Vec<u8>], members: usize, what: &str) {
    let mut counts: AHashMap<&[u8], usize> = AHashMap::new();

    for draw in draws {
        *counts.entry(draw).or_default() += 1;
    }

    let expected = draws.len() / members;

    assert_eq!(counts.len(), members, "{what}: every member is drawn");
    assert!(
        counts
            .values()
            .all(|&count| count > expected / 3 && count < expected * 3),
        "{what}: counts {counts:?}"
    );
    // A rotation repeats with the set's period; random draws do not.
    let periodic = draws
        .iter()
        .zip(draws.iter().skip(members))
        .filter(|(left, right)| left == right)
        .count();
    assert!(
        periodic < draws.len() / 2,
        "{what}: draws repeat with period {members} ({periodic} times)"
    );
    let first_window = &draws[..members];
    assert!(
        draws
            .chunks(members)
            .filter(|window| *window == first_window)
            .count()
            < 3,
        "{what}: draws cycle"
    );
}

#[test]
fn set_random_and_pop_are_random_for_every_encoding() {
    let mut shard = KvShard::new();
    let ints: Vec<Vec<u8>> = (0..10).map(|id| id.to_string().into_bytes()).collect();
    let small: Vec<Vec<u8>> = (0..10).map(|id| format!("s{id}").into_bytes()).collect();
    let flat: Vec<Vec<u8>> = (0..60)
        .map(|id| format!("flat-member-{id:03}").into_bytes())
        .collect();
    let arena: Vec<Vec<u8>> = (0..400)
        .map(|id| format!("a{id:03}").into_bytes())
        .collect();

    shard.set_add(b"ints", &ints).unwrap();
    shard.set_add(b"small", &small).unwrap();
    shard.set_add(b"flat", &flat).unwrap();
    shard.set_add(b"arena", &arena).unwrap();

    for (key, members) in [
        (b"ints".as_slice(), 10),
        (b"small", 10),
        (b"flat", 60),
        (b"arena", 400),
    ] {
        let what = String::from_utf8_lossy(key).into_owned();

        assert_spread(&single_draws(&shard, key, members * 200), members, &what);

        // SPOP candidates: not always the smallest or the same members.
        let firsts: std::collections::HashSet<Vec<u8>> = (0..200)
            .map(|_| shard.set_pop_members(key, 1).unwrap().remove(0))
            .collect();
        assert!(
            firsts.len() > members.min(30) / 2,
            "{what}: pops {}",
            firsts.len()
        );
    }
}

#[test]
fn set_random_count_draws_independent_positions() {
    let mut shard = KvShard::new();
    let members: Vec<Vec<u8>> = (0..200)
        .map(|id| format!("m{id:03}").into_bytes())
        .collect();

    shard.set_add(b"s", &members).unwrap();

    // A run of neighbouring buckets pairs each member with the same few partners; random
    // positions pair it with most of the set.
    let mut pairs = std::collections::HashSet::new();

    for _ in 0..3_000 {
        let mut drawn = shard.set_random_members(b"s", Some(2)).unwrap();

        assert_eq!(drawn.len(), 2);
        assert_ne!(drawn[0], drawn[1]);
        drawn.sort();
        pairs.insert(drawn);
    }

    assert!(pairs.len() > 2_000, "distinct pairs {}", pairs.len());

    let mut shard = KvShard::new();
    let mut hash = Vec::new();

    for id in 0..200 {
        hash.push((format!("f{id:03}"), format!("v{id:03}")));
    }

    shard.hash_set(b"h", &hash).unwrap();
    let mut pairs = std::collections::HashSet::new();

    for _ in 0..3_000 {
        let mut drawn: Vec<Vec<u8>> = shard
            .hash_random_pairs(b"h", Some(2))
            .unwrap()
            .into_iter()
            .map(|(field, _)| field)
            .collect();

        drawn.sort();
        assert_ne!(drawn[0], drawn[1]);
        pairs.insert(drawn);
    }

    assert!(pairs.len() > 2_000, "distinct hash pairs {}", pairs.len());
}

#[test]
fn spilled_set_samples_and_keeps_scanning() {
    let _limit = crate::arena::limit_chunks_for_test(1);
    let mut shard = KvShard::new();
    let member = |id: usize| {
        let mut member = format!("m{id:05}").into_bytes();

        member.resize(300, b'.');
        member
    };
    let base: Vec<Vec<u8>> = (0..150).map(member).collect();

    shard.set_add(b"s", &base).unwrap();

    let (mut cursor, first) = shard.set_scan(b"s", 0, 20, None).unwrap();
    let mut seen: std::collections::HashSet<Vec<u8>> =
        first.iter().map(|member| member.to_vec()).collect();

    assert_ne!(cursor, 0);

    // One chunk holds about 200 such members; the rest spill the set to the boxed table.
    let extra: Vec<Vec<u8>> = (1_000..1_300).map(member).collect();

    shard.set_add(b"s", &extra).unwrap();
    assert!(matches!(
        shard.entries.lookup(b"s"),
        Some(IndexRef::Object(Entry { value: Value::Set(set), .. })) if matches!(set.body, SetBody::Table(_))
    ));

    for _ in 0..1_000 {
        let (next, page) = shard.set_scan(b"s", cursor, 20, None).unwrap();

        seen.extend(page.iter().map(|member| member.to_vec()));
        cursor = next;

        if cursor == 0 {
            break;
        }
    }

    assert_eq!(cursor, 0);
    assert!(base.iter().all(|member| seen.contains(member)));
    assert_spread(&single_draws(&shard, b"s", 450 * 40), 450, "spilled");
}

#[test]
fn zscan_does_not_skip_members_when_others_move() {
    let mut shard = KvShard::new();
    let pairs: Vec<(f64, Vec<u8>)> = (0..2_000)
        .map(|id| (f64::from(id), format!("z{id:04}").into_bytes()))
        .collect();

    shard.sorted_set_add_pairs(b"z", &pairs).unwrap();

    let mut seen = std::collections::HashSet::new();
    let mut cursor = 0;
    let mut round = 0usize;

    loop {
        let (next, page) = shard.sorted_set_scan(b"z", cursor, 25, None).unwrap();

        seen.extend(page.iter().map(|(member, _)| member.to_vec()));
        cursor = next;

        // Remove low-ranked members (ids below 1000, odd) and move others to new
        // scores between pages; members with ids from 1000 stay for the whole scan.
        for id in (round * 20..round * 20 + 20).filter(|id| id % 2 == 1 && *id < 1_000) {
            shard
                .sorted_set_remove(b"z", &[format!("z{id:04}").into_bytes()])
                .unwrap();
        }
        for id in (round * 7..round * 7 + 7).filter(|id| *id < 1_000) {
            let member = format!("z{id:04}").into_bytes();

            if shard.sorted_set_score(b"z", &member).unwrap().is_some() {
                shard
                    .sorted_set_add(b"z", 5_000.0 + id as f64, &member)
                    .unwrap();
            }
        }

        round += 1;
        assert!(round < 10_000, "scan ends");

        if cursor == 0 {
            break;
        }
    }

    for id in 1_000..2_000 {
        assert!(
            seen.contains(format!("z{id:04}").as_bytes()),
            "z{id:04} was present for the whole scan"
        );
    }
}

struct CountingWaker(std::sync::atomic::AtomicUsize);

impl std::task::Wake for CountingWaker {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }
}

#[test]
fn a_change_to_a_watched_key_wakes_the_task_waiting_on_it() {
    let mut shard = KvShard::new();
    let counter = Arc::new(CountingWaker(Default::default()));
    let waker = std::task::Waker::from(Arc::clone(&counter));
    let woken = || counter.0.load(std::sync::atomic::Ordering::SeqCst);
    let id = 1 << 63;

    shard.watch(id, 0, &[b"list"]);
    shard.wake_on_watch(id, &waker);
    shard.set_bytes(b"other", b"v");
    assert_eq!(woken(), 0);
    assert!(!shard.take_watch_change(id));

    shard
        .list_push_back(b"list", &[std::borrow::Cow::Borrowed(b"x".as_slice())])
        .unwrap();
    assert_eq!(woken(), 1);
    assert!(shard.take_watch_change(id));
    assert!(!shard.take_watch_change(id), "the change was taken");

    // A change made before the task waits again wakes it at once.
    shard
        .list_push_back(b"list", &[std::borrow::Cow::Borrowed(b"y".as_slice())])
        .unwrap();
    shard.wake_on_watch(id, &waker);
    assert_eq!(woken(), 2);

    shard.take_watch_change(id);
    shard.wake_on_watch(id, &waker);
    shard.unwatch(id);
    shard
        .list_push_back(b"list", &[std::borrow::Cow::Borrowed(b"z".as_slice())])
        .unwrap();
    assert_eq!(woken(), 2, "an unwatched task is not woken");
}

#[test]
fn volatile_victim_search_is_bounded_when_most_deadlines_have_passed() {
    let clock = TestClock::new(1);
    let mut shard = KvShard::with_clock(clock.clone());

    shard.apply_memory_limits(MemoryLimits {
        eviction_policy: EvictionPolicy::VolatileLru,
        active_expire_keys: 0,
        ..MemoryLimits::default()
    });
    for index in 0..50_000u32 {
        let key = format!("gone:{index}");

        shard.set_bytes(key.as_bytes(), b"v");
        assert!(shard.expire_at(key.as_bytes(), UnixMillis::new(2)));
    }
    shard.set_bytes(b"live", b"v");
    assert!(shard.expire_at(b"live", UnixMillis::new(1_000_000)));
    clock.set(3);

    let slots = shard.deadlines[0].lookup.num_buckets() as u64;
    let before = shard.memory_stats();
    let _ = shard.pick_eviction_victim(&[], false);
    let inspected = shard.memory_stats().eviction_slots_inspected - before.eviction_slots_inspected;

    assert!(
        inspected <= super::evict::VOLATILE_SCAN_SLOTS as u64,
        "one search inspected {inspected} of {slots} slots"
    );
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

#[test]
fn keyspace_scan_ends_and_misses_no_stable_key_while_the_index_grows_and_shrinks() {
    let mut shard = KvShard::new();
    let stable: Vec<Vec<u8>> = (0..5_000u32)
        .map(|index| format!("s{index}").into_bytes())
        .collect();

    for key in &stable {
        shard.set_bytes(key, b"v");
    }

    let mut seen = std::collections::HashSet::new();
    let (mut cursor, mut pages) = (0u64, 0usize);
    let (mut low, mut high) = (0u32, 0u32);

    loop {
        let (next, keys) = shard.scan(cursor, 100, None, None);
        seen.extend(keys.into_iter().map(<[u8]>::to_vec));
        cursor = next;
        pages += 1;
        assert!(pages < 1_000, "the scan did not end");

        if cursor == 0 {
            break;
        }

        // Steady churn, plus a burst that grows the index (starting a migration) and
        // a removal that shrinks it again.
        for _ in 0..60 {
            shard.set_bytes(format!("c{high}").as_bytes(), b"v");
            high += 1;
        }
        for _ in 0..60.min(high - low) {
            shard.remove(format!("c{low}").as_bytes());
            low += 1;
        }
        match pages % 20 {
            5 => {
                for index in 0..20_000u32 {
                    shard.set_bytes(format!("x{index}").as_bytes(), b"v");
                }
            }
            15 => {
                for index in 0..20_000u32 {
                    shard.remove(format!("x{index}").as_bytes());
                }
            }
            _ => {}
        }
    }

    let missed = stable.iter().filter(|key| !seen.contains(*key)).count();
    assert_eq!(missed, 0, "missed stable keys after {pages} pages");
}

fn finish_background_work(shard: &mut KvShard<TestClock>) {
    let mut rounds = 0;

    while !(shard.migrate_indexes(std::time::Duration::from_micros(100))
        & shard.step_collection_work(std::time::Duration::from_micros(100)))
    {
        rounds += 1;
        assert!(rounds < 100_000, "background work does not converge");
    }
}

#[test]
fn mass_delete_shrinks_the_key_index_incrementally() {
    let mut shard = KvShard::with_clock(TestClock::new(1));
    let key = |index: u32| format!("key:{index:06}").into_bytes();

    for index in 0..60_000 {
        shard.set_bytes(&key(index), b"v");
    }
    finish_background_work(&mut shard);
    let grown = shard.entries.charged_bytes();
    let mut saw_migration = false;

    for index in 0..58_000 {
        assert!(shard.delete(&key(index)));
        saw_migration |= shard.entries.is_growing();

        if index % 4_000 == 0 {
            // Mid-shrink, every remaining key is reachable and a new one lands.
            let fresh = format!("fresh:{index}").into_bytes();
            shard.set_bytes(&fresh, b"n");
            assert!(
                (index + 1..60_000)
                    .step_by(97)
                    .all(|i| shard.get(&key(i)).is_some())
            );
            assert_eq!(shard.get(&fresh), Some(&b"n"[..]));
            assert!(shard.delete(&fresh));
        }
    }

    assert!(saw_migration, "the index shrank in one step");
    finish_background_work(&mut shard);
    assert!(!shard.entries.is_growing());
    assert!(shard.entries.charged_bytes() * 8 <= grown);
    assert_eq!(shard.memory_usage().entries, 2_000);
    assert!((58_000..60_000).all(|i| shard.get(&key(i)) == Some(&b"v"[..])));
}

#[test]
fn mass_ttl_removal_shrinks_the_deadline_table_incrementally() {
    let mut shard = KvShard::with_clock(TestClock::new(1));
    let key = |index: u32| format!("ttl:{index:06}").into_bytes();

    for index in 0..40_000 {
        shard.set_bytes(&key(index), b"v");
        assert!(shard.expire_at(&key(index), UnixMillis::new(1_000_000 + u64::from(index))));
    }
    finish_background_work(&mut shard);
    let grown = shard.deadlines[0].lookup.num_buckets();
    let mut saw_migration = false;

    for index in 0..39_000 {
        assert!(shard.delete(&key(index)));
        saw_migration |= shard.deadlines[0].lookup.is_migrating();

        if index % 3_000 == 0 {
            assert!(
                (index + 1..40_000)
                    .step_by(101)
                    .all(|i| shard.deadline(0, &key(i)).is_some())
            );
        }
    }

    assert!(saw_migration, "the deadline table shrank in one step");
    finish_background_work(&mut shard);
    assert!(shard.deadlines[0].lookup.num_buckets() * 16 <= grown);
    assert!((39_000..40_000).all(|i| shard.ttl_seconds(&key(i)) > 0));
}

#[test]
fn quiet_collections_finish_their_shrink_and_compaction_in_maintenance() {
    let mut shard = KvShard::with_clock(TestClock::new(1));
    let member = |index: u32| format!("member-{index:08}-with-some-padding").into_bytes();
    let members: Vec<Vec<u8>> = (0..60_000).map(member).collect();

    shard.set_add(b"set", &members).unwrap();
    let pairs: Vec<(Vec<u8>, Vec<u8>)> = members
        .iter()
        .map(|field| (field.clone(), b"value".to_vec()))
        .collect();
    shard.hash_set(b"hash", &pairs).unwrap();
    finish_background_work(&mut shard);
    let full = shard.memory_usage().collection_bytes;

    // Remove most members, then stop writing while work is still under way.
    shard.set_remove(b"set", &members[..57_000]).unwrap();
    shard.hash_delete(b"hash", &members[..57_000]).unwrap();
    assert!(
        !shard.collection_work.is_empty(),
        "no unfinished collection work"
    );

    let tracked = shard.collection_work.len();
    finish_background_work(&mut shard);
    assert!(shard.collection_work.is_empty());
    assert!(tracked >= 1);

    // The charge follows the released memory exactly.
    let charged = shard.memory_usage().collection_bytes;
    let recomputed: usize = [&b"set"[..], &b"hash"[..]]
        .iter()
        .map(|key| shard.entry_charge(key).collection_bytes)
        .sum();
    assert_eq!(charged, recomputed);
    assert!(charged * 4 < full, "{charged} of {full} still charged");
    assert_eq!(shard.set_card(b"set").unwrap(), 3_000);
    assert_eq!(shard.hash_len(b"hash").unwrap(), 3_000);
    assert!(
        members[57_000..]
            .iter()
            .all(|m| shard.set_contains(b"set", m).unwrap())
    );
    assert!(
        members[57_000..]
            .iter()
            .all(|m| shard.hash_get(b"hash", m).unwrap() == Some(&b"value"[..]))
    );
}

#[test]
fn the_snapshot_walk_encodes_large_values_without_copying_them() {
    let mut shard = KvShard::new();
    let pairs: Vec<(f64, Vec<u8>)> = (0..5_000u32)
        .map(|index| (f64::from(index), format!("member-{index:08}").into_bytes()))
        .collect();
    let members: Vec<Vec<u8>> = pairs.iter().map(|(_, member)| member.clone()).collect();

    shard.sorted_set_add_pairs(b"zset", &pairs).unwrap();
    shard.set_add(b"set", &members).unwrap();
    shard.set_bytes(b"string", &[b's'; 100_000]);
    shard.begin_snapshot(1 << 24).unwrap();
    super::snapshot::LIVE_SNAPSHOT_COPIES.with(|copies| copies.set(0));
    super::snapshot::LIVE_SNAPSHOT_ENCODES.with(|encodes| encodes.set(0));

    let mut frames = 0;

    while !matches!(
        shard.snapshot_step(100, &mut |entry| {
            let mut frame = Vec::new();

            entry.encode_into(&mut frame).unwrap();
            // Sized once: no spare room from growing the buffer while encoding.
            assert_eq!(frame.capacity(), frame.len());
            frames += 1;
        }),
        SnapshotStep::Done
    ) {}

    assert_eq!(
        super::snapshot::LIVE_SNAPSHOT_COPIES.with(std::cell::Cell::get),
        0
    );
    assert_eq!(
        super::snapshot::LIVE_SNAPSHOT_ENCODES.with(std::cell::Cell::get),
        3
    );
    assert_eq!(frames, 3);
}

fn timed_value<C: Clock>(shard: &KvShard<C>, key: &[u8]) -> Option<TimedHash> {
    match shard.entries.lookup(key) {
        Some(IndexRef::Object(entry)) => match &entry.value {
            Value::TimedHash(timed) => Some(timed.as_ref().clone()),
            _ => None,
        },
        _ => None,
    }
}

fn wide_hash(shard: &mut KvShard<TestClock>, key: &[u8], count: usize) -> Vec<Vec<u8>> {
    let fields: Vec<Vec<u8>> = (0..count)
        .map(|index| format!("field:{index}").into_bytes())
        .collect();

    for chunk in fields.chunks(4_096) {
        let pairs: Vec<(&[u8], &[u8])> = chunk
            .iter()
            .map(|field| (field.as_slice(), b"vvvvvvvv".as_slice()))
            .collect();

        shard.hash_set(key, &pairs).unwrap();
    }

    fields
}

#[test]
fn wide_timed_hash_charges_about_two_dozen_bytes_per_timed_field() {
    let clock = TestClock::new(1_000);
    let mut shard = KvShard::with_clock(clock);
    let count = 50_000;
    let fields = wide_hash(&mut shard, b"h", count);
    let charged = |shard: &KvShard<TestClock>| {
        let charge = shard.entry_charge(b"h");

        charge.value_bytes + charge.collection_bytes
    };
    let plain = charged(&shard);

    for (index, chunk) in fields.chunks(1_000).enumerate() {
        let at = 1_000_000 + index as u64;

        assert_eq!(shard.hash_expire_fields(b"h", chunk, at), Ok(chunk.len()));
    }

    let timed = charged(&shard);
    let per_field = (timed - plain) as f64 / count as f64;

    // A heap slot is 16 bytes and the position index about 5 bytes per bucket; the name-keyed
    // map and B-tree this replaced cost about 139.
    assert!(per_field <= 30.0, "{per_field:.1} bytes per timed field");
    timed_value(&shard, b"h").unwrap().check();
}

#[test]
fn field_ttl_commands_on_wide_hashes_stay_logarithmic() {
    // Per-operation time of HDEL of the soonest field, HSET of it back and HEXPIRE, at two
    // sizes 32 times apart: a linear scan per command would grow the ratio to about 32.
    let per_op = |count: usize| {
        let clock = TestClock::new(1_000);
        let mut shard = KvShard::with_clock(clock);
        let fields = wide_hash(&mut shard, b"h", count);

        for (index, field) in fields.iter().enumerate() {
            shard
                .hash_expire_fields(b"h", &[field], 10_000_000 + index as u64)
                .unwrap();
        }

        let mut best = f64::MAX;

        for round in 0..3 {
            let started = std::time::Instant::now();

            for step in 0..2_000 {
                let field = &fields[(round * 2_000 + step) % 64];

                shard.hash_delete(b"h", &[field]).unwrap();
                shard.hash_set(b"h", &[(field, b"v")]).unwrap();
                shard.hash_expire_fields(b"h", &[field], 5_000).unwrap();
                assert_eq!(shard.field_deadline(b"h", field), Ok(Some(5_000)));
            }

            best = best.min(started.elapsed().as_secs_f64());
        }

        best
    };
    let small = per_op(2_048);
    let large = per_op(65_536);

    assert!(
        large / small < 8.0,
        "2k fields {small:.4}s, 64k fields {large:.4}s"
    );
}

#[test]
fn mass_field_expiry_runs_in_capped_passes() {
    let clock = TestClock::new(1_000);
    let mut shard = KvShard::with_clock(clock.clone());
    let count = 20_000;
    let fields = wide_hash(&mut shard, b"h", count);

    shard.active_expire_keys = 1;

    // One field without a TTL keeps the hash alive: a hash with every field due is removed
    // whole, like an expired key.
    for chunk in fields[..count - 1].chunks(1_000) {
        shard.hash_expire_fields(b"h", chunk, 2_000).unwrap();
    }

    clock.set(2_000);

    let mut passes = 0;

    while shard.hash_len(b"h").unwrap_or(0) > 1 {
        let before = shard.hash_len(b"h").unwrap();

        assert_eq!(shard.expire_hash_fields_due(), 1);

        let after = shard.hash_len(b"h").unwrap_or(0);

        assert_eq!(before - after, (before - 1).min(FIELDS_PER_SWEEP));
        passes += 1;
    }

    assert_eq!(passes, (count - 1).div_ceil(FIELDS_PER_SWEEP));
    assert!(!shard.has_field_ttls());
}

#[test]
fn wide_timed_hash_survives_snapshot_copy_and_rename() {
    let clock = TestClock::new(1_000);
    let mut shard = KvShard::with_clock(clock.clone());
    let fields = wide_hash(&mut shard, b"h", 500);

    for (index, field) in fields
        .iter()
        .enumerate()
        .filter(|(index, _)| index % 3 != 0)
    {
        shard
            .hash_expire_fields(b"h", &[field], 50_000 + index as u64)
            .unwrap();
    }

    assert!(shard.rename(b"h", b"moved"));

    let mut restored = KvShard::with_clock(clock.clone());

    for entry in shard.snapshot_entries() {
        restored.restore_snapshot_entry(entry).unwrap();
    }

    for key in [b"moved".as_slice()] {
        for (index, field) in fields.iter().enumerate() {
            let expected = (index % 3 != 0).then(|| 50_000 + index as u64);

            assert_eq!(shard.field_deadline(key, field), Ok(expected));
            assert_eq!(restored.field_deadline(key, field), Ok(expected));
        }

        let original = timed_value(&shard, key).unwrap();

        assert_eq!(original, timed_value(&restored, key).unwrap());
        assert_eq!(original, original.clone());
        timed_value(&restored, key).unwrap().check();
    }

    clock.set(50_200);
    restored.expire_hash_fields(b"moved");
    assert_eq!(restored.hash_len(b"moved"), Ok(500 - 134));
}

mod field_ttl_properties {
    use super::*;
    use proptest::prelude::*;

    #[derive(Debug, Clone)]
    enum Op {
        Set(u8, u8),
        SetKeep(u8, u8),
        Delete(u8),
        Expire(u8, u16),
        Persist(u8),
        Purge(u16, u8),
        Read(u8),
    }

    fn ops(fields: u8) -> impl Strategy<Value = Vec<Op>> {
        prop::collection::vec(
            prop_oneof![
                6 => (0..fields, any::<u8>()).prop_map(|(f, l)| Op::Set(f, l)),
                1 => (0..fields, any::<u8>()).prop_map(|(f, l)| Op::SetKeep(f, l)),
                2 => (0..fields).prop_map(Op::Delete),
                6 => (0..fields, 0u16..2_000).prop_map(|(f, d)| Op::Expire(f, d)),
                1 => (0..fields).prop_map(Op::Persist),
                1 => (0u16..400, any::<u8>()).prop_map(|(a, m)| Op::Purge(a, m)),
                1 => (0..fields).prop_map(Op::Read),
            ],
            1..700,
        )
    }

    type Model = BTreeMap<Vec<u8>, (Vec<u8>, Option<u64>)>;

    fn check(value: &Value, model: &Model, now: u64) {
        let hash = value.as_hash().expect("a hash value");
        let dead =
            !model.is_empty() && model.values().all(|(_, at)| at.is_some_and(|at| at <= now));

        assert_eq!(value.hash_fields_all_due(now), dead);

        assert_eq!(hash.len(), model.len());

        for (field, (stored, at)) in model {
            assert_eq!(hash.get(field), Some(stored.as_slice()));
            assert_eq!(value.field_deadline(field), *at, "{field:?}");
        }

        let soonest = model.values().filter_map(|(_, at)| *at).min();

        match value {
            Value::TimedHash(timed) => {
                assert_eq!(Some(timed.soonest), soonest);
                timed.check();
            }
            _ => assert_eq!(soonest, None),
        }

        assert_eq!(value.field_deadline(b"absent"), None);
    }

    fn run(ops: Vec<Op>, limits: CompactLimits, scale: usize) {
        let mut value = Value::Hash(Box::default());
        let mut model = Model::new();
        let mut now = 1_000u64;

        for op in ops {
            match op {
                Op::Set(id, len) => {
                    let field = format!("f{id}").into_bytes();
                    let stored = vec![b'v'; usize::from(len) * scale];
                    let added = value.hash_insert(&field, &stored, limits);

                    assert_eq!(added, !model.contains_key(&field));
                    model.insert(field, (stored, None));
                }

                Op::SetKeep(id, len) => {
                    let field = format!("f{id}").into_bytes();
                    let stored = vec![b'k'; usize::from(len) * scale];

                    if let Some(entry) = model.get_mut(&field) {
                        assert!(!value.hash_insert_keep_deadline(&field, &stored, limits));
                        entry.0 = stored;
                    }
                }

                Op::Delete(id) => {
                    let field = format!("f{id}").into_bytes();

                    assert_eq!(
                        value.hash_remove(&field, limits),
                        model.remove(&field).is_some()
                    );
                }

                Op::Expire(id, delta) => {
                    let field = format!("f{id}").into_bytes();

                    if let Some(entry) = model.get_mut(&field) {
                        let at = now + u64::from(delta);

                        value.set_field_deadline(&field, at);
                        entry.1 = Some(at);
                    }
                }

                Op::Persist(id) => {
                    let field = format!("f{id}").into_bytes();
                    let had = model.get_mut(&field).and_then(|entry| entry.1.take());

                    assert_eq!(value.clear_field_deadline(&field), had.is_some());
                }

                Op::Purge(advance, max) => {
                    now += u64::from(advance);

                    let max = usize::from(max % 40) + 1;
                    let due = model
                        .values()
                        .filter(|(_, at)| at.is_some_and(|at| at <= now))
                        .count();
                    let removed = value.purge_due_fields(now, max, limits);

                    assert_eq!(removed, due.min(max));

                    let hash = value.as_hash().unwrap();

                    model.retain(|field, (_, at)| {
                        let kept = hash.contains(field);

                        assert!(kept || at.is_some_and(|at| at <= now));
                        kept
                    });
                }

                Op::Read(id) => {
                    let field = format!("f{id}").into_bytes();

                    assert_eq!(
                        value.field_deadline(&field),
                        model.get(&field).and_then(|entry| entry.1)
                    );
                }
            }

            check(&value, &model, now);
        }
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(64))]

        // Wide hashes cross the vector-to-heap switch and compact/arena promote and demote.
        #[test]
        fn field_ttls_follow_a_model_across_encodings(ops in ops(90)) {
            run(ops, CompactLimits { max_entries: 40, max_value_bytes: 64 }, 1);
        }

        // Handles reduced to two bits collide all the time in a compact hash.
        #[test]
        fn field_ttls_follow_a_model_when_handles_collide(ops in ops(90)) {
            let _mask = mask_handles_for_test(0b11);

            run(ops, CompactLimits { max_entries: 80, max_value_bytes: 300 }, 1);
        }

        // Values of up to 5 KiB fill a one-chunk arena, which spills to the boxed map.
        #[test]
        fn field_ttls_follow_a_model_across_a_spill(ops in ops(70)) {
            let _limit = crate::arena::limit_chunks_for_test(1);

            run(ops, CompactLimits { max_entries: 8, max_value_bytes: 64 }, 20);
        }
    }
}

/// A large set left with a shrink and compaction under way by removing most members.
fn set_with_unfinished_work(shard: &mut KvShard<TestClock>, key: &[u8]) -> Vec<Vec<u8>> {
    let members: Vec<Vec<u8>> = (0..60_000u32)
        .map(|index| format!("member-{index:08}-with-some-padding").into_bytes())
        .collect();

    shard.set_add(key, &members).unwrap();
    while !shard.step_collection_work(std::time::Duration::from_secs(1)) {}
    // Stop removing while a resize or compaction is under way.
    let mut stop = 0;
    while stop < 50_000 || !collection_has_work(shard, key) {
        shard.set_remove(key, &members[stop..stop + 1]).unwrap();
        stop += 1;
    }
    members[stop..].to_vec()
}

fn collection_has_work(shard: &KvShard<TestClock>, key: &[u8]) -> bool {
    matches!(shard.entries.lookup(key), Some(IndexRef::Object(entry))
        if arena_table::has_background_work(&entry.value))
}

fn charges_match_a_recount(shard: &KvShard<TestClock>) {
    let index: usize = std::iter::once(&shard.entries)
        .chain(shard.databases.iter().flatten())
        .map(|map| map.charged_bytes())
        .sum();
    let deadlines: usize = shard.deadlines.iter().map(DeadlineDb::charged_bytes).sum();

    assert_eq!(shard.memory_usage.index_bytes, index + deadlines);
}

#[test]
fn renamed_moved_and_swapped_collections_still_finish_their_work() {
    let mut shard = KvShard::with_clock(TestClock::new(1));

    let kept = set_with_unfinished_work(&mut shard, b"renamed");
    assert!(shard.rename(b"renamed", b"destination"));
    set_with_unfinished_work(&mut shard, b"moved");
    assert!(shard.move_key(b"moved", 3));
    set_with_unfinished_work(&mut shard, b"swapped");
    shard.swap_databases(0, 5);

    finish_background_work(&mut shard);
    shard.enter(5);
    assert!(!collection_has_work(&shard, b"swapped"));
    assert!(!collection_has_work(&shard, b"destination"));
    shard.enter(3);
    assert!(!collection_has_work(&shard, b"moved"));
    shard.enter(5);
    assert!(
        kept.iter()
            .all(|m| shard.set_contains(b"destination", m).unwrap())
    );
}

#[test]
fn collection_work_does_not_promote_a_small_value_under_the_same_key() {
    let mut shard = KvShard::with_clock(TestClock::new(1));

    set_with_unfinished_work(&mut shard, b"key");
    assert!(shard.delete(b"key"));
    shard.hash_set(b"key", &[(b"f", b"v")]).unwrap();
    assert!(matches!(
        shard.entries.lookup(b"key"),
        Some(IndexRef::CollSlab(_))
    ));

    finish_background_work(&mut shard);
    assert!(matches!(
        shard.entries.lookup(b"key"),
        Some(IndexRef::CollSlab(_))
    ));
    assert!(shard.collection_work.is_empty());
}

#[test]
fn collection_work_does_not_hold_back_the_index() {
    let mut shard = KvShard::with_clock(TestClock::new(1));

    set_with_unfinished_work(&mut shard, b"set");
    // Index work is done, so log reclaim may run while the collection is unfinished.
    assert!(shard.migrate_indexes(std::time::Duration::from_millis(50)));
    assert!(collection_has_work(&shard, b"set"));
}

#[test]
fn a_shrinking_index_is_charged_for_both_tables_without_drift() {
    let mut shard = KvShard::with_clock(TestClock::new(1));
    let key = |index: u32| format!("key:{index:06}").into_bytes();

    for index in 0..60_000 {
        shard.set_bytes(&key(index), b"v");
        assert!(shard.expire_at(&key(index), UnixMillis::new(1_000_000)));
    }
    finish_background_work(&mut shard);
    let full = shard.entries.charged_bytes();

    let mut index = 0;
    while !shard.entries.is_growing() {
        assert!(shard.delete(&key(index)));
        index += 1;
    }
    // The draining table is still allocated and still charged.
    assert!(shard.entries.charged_bytes() > full);
    assert!(shard.deadlines[0].lookup.is_migrating() || shard.deadlines[0].charged_bytes() > 0);
    charges_match_a_recount(&shard);

    // Park the map mid-shrink, let maintenance finish it, then return.
    for index in index..59_990 {
        assert!(shard.delete(&key(index)));
    }
    shard.enter(1);
    finish_background_work(&mut shard);
    charges_match_a_recount(&shard);
    // A parked map's owed shrink runs too.
    assert!(shard.databases[0].as_ref().unwrap().charged_bytes() * 64 <= full);
    shard.enter(0);
    charges_match_a_recount(&shard);
    assert!(shard.entries.charged_bytes() * 64 <= full);
}

#[test]
fn a_timed_hash_counts_exactly_one_deadline_per_timed_field() {
    fn run(shard: &mut KvShard<TestClock>, parts: &[&[u8]]) -> crate::engine::Response<'static> {
        let mut frame = format!("*{}\r\n", parts.len()).into_bytes();

        for part in parts {
            frame.extend(format!("${}\r\n", part.len()).bytes());
            frame.extend_from_slice(part);
            frame.extend_from_slice(b"\r\n");
        }

        let (command, _) = crate::protocol::parse_command(&frame)
            .unwrap()
            .expect("complete frame");

        crate::engine::execute(shard, command).into_owned()
    }
    fn checked(shard: &KvShard<TestClock>, key: &[u8]) {
        let timed = timed_value(shard, key).expect("a timed hash");

        timed.check();
    }

    // Three fields keep the vector form; forty make the heap.
    for width in [3usize, 40] {
        let clock = TestClock::new(1_000);
        let mut shard = KvShard::with_clock(clock.clone());
        let fields: Vec<Vec<u8>> = (0..width).map(|i| format!("f{i}").into_bytes()).collect();
        let count = width.to_string();

        for field in &fields {
            run(&mut shard, &[b"HSET", b"h", field, b"v"]);
        }
        let mut expire: Vec<&[u8]> =
            vec![b"HPEXPIREAT", b"h", b"2000", b"FIELDS", count.as_bytes()];

        expire.extend(fields.iter().map(Vec::as_slice));
        run(&mut shard, &expire);
        checked(&shard, b"h");

        // Each step leaves one more field without a deadline, or removes one with it.
        run(&mut shard, &[b"HDEL", b"h", &fields[0]]);
        checked(&shard, b"h");
        run(&mut shard, &[b"HSET", b"h", &fields[1], b"w"]);
        checked(&shard, b"h");
        run(
            &mut shard,
            &[b"HPERSIST", b"h", b"FIELDS", b"1", &fields[2]],
        );
        if width > 3 {
            checked(&shard, b"h");
        }
        run(&mut shard, &[b"RENAME", b"h", b"r"]);

        // DUMP refuses a hash with field TTLs; a snapshot carries them.
        let mut restored = KvShard::with_clock(clock.clone());

        for entry in shard.snapshot_entries() {
            restored.restore_snapshot_entry(entry).unwrap();
        }
        assert!(restored.rename(b"r", b"copy"));
        if width > 3 {
            checked(&shard, b"r");
            checked(&restored, b"copy");
        }

        // Fields 1 and 2 have no deadline: due fields never make the hash read as missing.
        clock.set(2_000);
        for (shard, key) in [(&mut shard, &b"r"[..]), (&mut restored, b"copy")] {
            assert_eq!(
                run(shard, &[b"EXISTS", key]),
                crate::engine::Response::Integer(1),
                "width {width}"
            );
            assert_eq!(
                run(shard, &[b"HLEN", key]),
                crate::engine::Response::Integer(2)
            );
        }
    }
}

/// Removing popped members with one lookup leaves the store as the general remove does:
/// same members, a charge that matches a fresh count, same record kind (slab or object),
/// the key gone once empty. Covers a slab set, a flat object and an indexed set, from both ends.
#[test]
fn removing_popped_members_matches_the_general_remove() {
    fn kind(shard: &KvShard<TestClock>, key: &[u8]) -> &'static str {
        match shard.entries.lookup(key) {
            None => "none",
            Some(IndexRef::SortedSlab(_)) => "slab",
            Some(IndexRef::Object(_)) => "object",
            Some(_) => "other",
        }
    }

    for size in [4usize, 90, 600] {
        let mut popped = KvShard::with_clock(TestClock::new(1));
        let mut general = KvShard::with_clock(TestClock::new(1));
        let pairs: Vec<(f64, Vec<u8>)> = (0..size)
            .map(|index| {
                let pad = "x".repeat(index % 7);

                ((index % 11) as f64, format!("m{index}{pad}").into_bytes())
            })
            .collect();

        for shard in [&mut popped, &mut general] {
            shard.sorted_set_add_pairs(b"z", &pairs).unwrap();
        }

        let mut round = 0usize;

        while kind(&popped, b"z") != "none" {
            let max = round % 2 == 1;
            let count = 1 + round % 3;
            let mut members = Vec::new();

            popped
                .sorted_set_visit_ends(b"z", count, max, |member, _| members.push(member.to_vec()))
                .unwrap();

            let listed: Vec<Vec<u8>> = popped
                .sorted_set_range_entries(b"z", 0, count as isize - 1, max)
                .unwrap()
                .into_iter()
                .map(|(member, _)| member.to_vec())
                .collect();

            assert_eq!(members, listed, "size {size} round {round}");
            assert_eq!(
                popped.sorted_set_remove_popped(b"z", &members),
                Ok(members.len())
            );
            assert_eq!(general.sorted_set_remove(b"z", &members), Ok(members.len()));
            // Each set has its own random hasher, so collection bytes may differ between
            // the two shards; each shard's running charge must match a fresh count.
            for shard in [&popped, &general] {
                let usage = shard.memory_usage;

                assert_eq!(
                    usage.key_bytes + usage.value_bytes + usage.collection_bytes,
                    shard.key_memory_usage(b"z").unwrap_or(0),
                    "size {size} round {round}"
                );
            }
            assert_eq!(
                popped.memory_usage.value_bytes,
                general.memory_usage.value_bytes
            );
            assert_eq!(popped.memory_usage.entries, general.memory_usage.entries);
            assert_eq!(kind(&popped, b"z"), kind(&general, b"z"));
            assert_eq!(
                popped.sorted_set_range_entries(b"z", 0, -1, false),
                general.sorted_set_range_entries(b"z", 0, -1, false)
            );
            round += 1;
        }

        assert_eq!(kind(&general, b"z"), "none");
        charges_match_a_recount(&popped);
    }
}

/// A sorted set with a TTL, popped from both ends: while members are left the TTL stays
/// and the charge matches a fresh count, including after the set shrinks from an indexed
/// object to a flat one and into a slab record; popped empty, the key and its TTL go.
#[test]
fn a_sorted_set_with_a_ttl_keeps_it_through_demotion_and_loses_it_with_the_last_pop() {
    fn kind(shard: &KvShard<TestClock>, key: &[u8]) -> &'static str {
        match shard.entries.lookup(key) {
            None => "none",
            Some(IndexRef::SortedSlab(_)) => "slab",
            Some(IndexRef::Object(_)) => "object",
            Some(_) => "other",
        }
    }

    let mut shard = KvShard::with_clock(TestClock::new(1));
    let pairs: Vec<(f64, Vec<u8>)> = (0..600usize)
        .map(|index| (index as f64, format!("m{index:04}").into_bytes()))
        .collect();

    shard.sorted_set_add_pairs(b"z", &pairs).unwrap();
    assert!(shard.expire_at(b"z", UnixMillis::new(500_000)));

    let mut kinds = Vec::new();
    let mut left = pairs.len();
    let mut round = 0usize;

    while left > 0 {
        let count = (1 + round % 9).min(left);
        let mut members = Vec::new();

        shard
            .sorted_set_visit_ends(b"z", count, round % 2 == 1, |member, _| {
                members.push(member.to_vec())
            })
            .unwrap();
        assert_eq!(
            shard.sorted_set_remove_popped(b"z", &members),
            Ok(members.len())
        );
        left -= members.len();
        round += 1;

        let usage = shard.memory_usage;

        assert_eq!(
            usage.key_bytes + usage.value_bytes + usage.collection_bytes,
            shard.key_memory_usage(b"z").unwrap_or(0),
            "round {round}"
        );
        charges_match_a_recount(&shard);

        if left > 0 {
            assert_eq!(shard.pttl(b"z"), 500_000 - 1, "round {round}");
            assert_eq!(shard.sorted_set_card(b"z"), Ok(left));

            if kinds.last() != Some(&kind(&shard, b"z")) {
                kinds.push(kind(&shard, b"z"));
            }
        }
    }

    // It went through a slab record before it emptied.
    assert_eq!(kinds.first(), Some(&"object"));
    assert_eq!(kinds.last(), Some(&"slab"));
    assert_eq!(kind(&shard, b"z"), "none");
    assert_eq!(shard.pttl(b"z"), -2);
    assert_eq!(shard.memory_usage.entries, 0);
    charges_match_a_recount(&shard);

    // A fresh key of the same name has no TTL.
    shard.sorted_set_add_pairs(b"z", &pairs[..3]).unwrap();
    assert_eq!(shard.pttl(b"z"), -1);
}

#[test]
fn visiting_the_ends_of_a_missing_key_or_another_type() {
    let mut shard = KvShard::with_clock(TestClock::new(1));
    let mut seen = 0;

    assert_eq!(
        shard.sorted_set_visit_ends(b"none", 3, false, |_, _| seen += 1),
        Ok(())
    );
    shard.set_bytes(b"s", b"v");
    assert_eq!(
        shard.sorted_set_visit_ends(b"s", 3, true, |_, _| seen += 1),
        Err(WrongType)
    );
    assert_eq!(shard.sorted_set_remove_popped(b"none", &[b"m"]), Ok(0));
    assert_eq!(seen, 0);
}

#[test]
fn deadline_changes_dirty_watches() {
    let mut shard = KvShard::new();
    shard.set_bytes(b"k", b"v");
    shard.watch(1, 0, &[b"k"]);
    let later = UnixMillis::new(shard.now_unix_millis().get() + 60_000);
    assert!(shard.expire_at(b"k", later));
    assert!(shard.take_watch_change(1), "EXPIRE must dirty WATCH");
    assert!(shard.persist(b"k"));
    assert!(shard.take_watch_change(1), "PERSIST must dirty WATCH");
    shard.keep_for(b"k", later.get());
    assert!(shard.take_watch_change(1), "GETEX must dirty WATCH");
}
