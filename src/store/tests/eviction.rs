use super::*;

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
