use super::*;

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
fn deleting_a_large_collection_frees_it_off_the_shard_thread() {
    let mut shard = KvShard::new();
    let members: Vec<Vec<u8>> = (0..20_000u32).map(|id| id.to_be_bytes().to_vec()).collect();

    shard.set_add(b"big", &members).unwrap();
    shard.set_bytes(b"small", b"v");
    let charged = shard.memory_usage();
    assert!(charged.collection_bytes + charged.value_bytes >= crate::lazy_free::MIN_DEFERRED_BYTES);

    // A queue another test filled would make the delete drop the set in place.
    let _serial = crate::lazy_free::tests::QUEUE_BOUNDS
        .lock()
        .unwrap_or_else(|error| error.into_inner());
    let _ = crate::lazy_free::take_last_deferred();
    assert!(shard.delete(b"big"));
    assert_eq!(shard.set_card(b"big"), Ok(0));
    assert_eq!(shard.get(b"small"), Some(&b"v"[..]));
    assert!(shard.memory_usage().collection_bytes < charged.collection_bytes);

    // This test's own value: the shared queue also holds what parallel tests delete and flush.
    let freed = crate::lazy_free::take_last_deferred().expect("the set was handed over");
    let start = std::time::Instant::now();
    while !freed.load(std::sync::atomic::Ordering::Acquire) {
        assert!(
            start.elapsed() < std::time::Duration::from_secs(30),
            "the deleted set was not freed within 30 s"
        );
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
