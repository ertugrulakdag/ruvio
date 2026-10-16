use super::*;

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
