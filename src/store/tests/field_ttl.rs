use super::*;

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
