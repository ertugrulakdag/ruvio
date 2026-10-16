use super::*;

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
