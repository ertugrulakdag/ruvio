use super::*;

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
