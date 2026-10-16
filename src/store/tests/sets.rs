use super::*;

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
