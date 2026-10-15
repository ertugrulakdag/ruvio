use super::*;

#[test]
fn transaction_records_one_batch_and_rolls_back_on_sink_failure() {
    let mut shard = KvShard::new();

    shard.set_bytes(b"{t}a", b"old");
    let mut sink = RecordingSink::default();
    let response = execute_transaction(
        &mut shard,
        &mut sink,
        &mut (),
        1,
        vec![
            Command::set(owned(b"{t}a"), owned(b"new")),
            Command::Increment(owned(b"{t}i"), 1),
        ],
        0,
    )
    .unwrap();

    assert_eq!(
        response,
        Response::Exec(vec![Response::Simple(b"OK"), Response::Integer(1)])
    );
    assert_eq!(sink.mutations, vec![RecordedMutation::Other]);
    assert_eq!(shard.get(b"{t}a"), Some(&b"new"[..]));
    assert_eq!(shard.copied_value(b"{t}i").as_deref(), Some(&b"1"[..]));

    let mut shard = KvShard::new();

    shard.set_bytes(b"{t}a", b"old");
    assert_eq!(
        execute_transaction(
            &mut shard,
            &mut FailingSink,
            &mut (),
            1,
            vec![Command::set(owned(b"{t}a"), owned(b"new"))],
            0
        ),
        Err("persistence unavailable")
    );
    assert_eq!(shard.get(b"{t}a"), Some(&b"old"[..]));
}

#[test]
fn transaction_aborts_when_a_watched_key_is_dirty() {
    let mut shard = KvShard::new();

    shard.set_bytes(b"a", b"old");
    shard.watch(7, 0, &[b"a"]);
    shard.set_bytes(b"a", b"changed");
    let response = execute_transaction(
        &mut shard,
        &mut NoopMutationSink,
        &mut (),
        7,
        vec![Command::Get(owned(b"a"))],
        0,
    )
    .unwrap();

    assert_eq!(response, Response::NullArray);
    assert!(!shard.watches_dirty(7));
}

#[test]
fn empty_transaction_returns_an_empty_array() {
    let mut shard = KvShard::new();

    assert_eq!(
        execute_transaction(&mut shard, &mut NoopMutationSink, &mut (), 1, Vec::new(), 0).unwrap(),
        Response::Exec(Vec::new())
    );
}

#[test]
fn transaction_rejects_cross_slot_queues() {
    let mut left = b"left".to_vec();
    let mut right = b"right".to_vec();

    left.push(0);
    while crate::routing::hash_slot(&left) == crate::routing::hash_slot(&right) {
        right.push(1);
    }

    let mut shard = KvShard::new();
    let response = execute_transaction(
        &mut shard,
        &mut NoopMutationSink,
        &mut (),
        1,
        vec![
            Command::set(Cow::Owned(left), owned(b"1")),
            Command::set(Cow::Owned(right), owned(b"2")),
        ],
        0,
    )
    .unwrap();

    assert_eq!(
        response,
        Response::Error(b"CROSSSLOT Keys in request do not hash to the same slot")
    );
    assert_eq!(shard.len(), 0);
}

fn undo_copies() -> usize {
    crate::engine::UNDO_COPIES.get()
}

fn run_transaction<S: MutationSink>(
    shard: &mut KvShard,
    sink: &mut S,
    commands: &[&[&[u8]]],
) -> Result<Response<'static>, S::Error> {
    execute_transaction(
        shard,
        sink,
        &mut (),
        1,
        commands.iter().map(|parts| command_of(parts)).collect(),
        0,
    )
}

fn big_list(shard: &mut KvShard) {
    for index in 0..2_000u32 {
        execute(
            shard,
            command_of(&[b"RPUSH", b"{t}list", format!("v{index}").as_bytes()]),
        );
    }
}

#[test]
fn a_read_only_transaction_copies_no_key() {
    let mut shard = KvShard::new();

    big_list(&mut shard);
    crate::engine::UNDO_COPIES.set(0);
    let reply = run_transaction(
        &mut shard,
        &mut RecordingSink::default(),
        &[&[b"LLEN", b"{t}list"], &[b"LRANGE", b"{t}list", b"0", b"2"]],
    )
    .unwrap();

    assert!(matches!(reply, Response::Exec(_)));
    assert_eq!(undo_copies(), 0);
}

#[test]
fn a_transaction_into_a_sink_that_keeps_nothing_copies_no_key() {
    let mut shard = KvShard::new();

    big_list(&mut shard);
    crate::engine::UNDO_COPIES.set(0);
    run_transaction(
        &mut shard,
        &mut NoopMutationSink,
        &[&[b"LPUSH", b"{t}list", b"head"], &[b"SET", b"{t}s", b"v"]],
    )
    .unwrap();

    assert_eq!(undo_copies(), 0);
    assert_eq!(shard.list_len(b"{t}list"), Ok(2_001));
    assert_eq!(shard.get(b"{t}s"), Some(&b"v"[..]));
}

#[test]
fn a_refused_transaction_restores_keys_first_written_after_reads() {
    let mut shard = KvShard::new();

    big_list(&mut shard);
    shard.set_bytes(b"{t}a", b"old");
    let before = shard.snapshot_entries();
    crate::engine::UNDO_COPIES.set(0);

    let refused = run_transaction(
        &mut shard,
        &mut FailingSink,
        &[
            &[b"GET", b"{t}a"],
            &[b"LLEN", b"{t}list"],
            &[b"SET", b"{t}a", b"new"],
            &[b"LPUSH", b"{t}list", b"head"],
            &[b"APPEND", b"{t}a", b"-more"],
            &[b"RPOP", b"{t}list"],
            &[b"SET", b"{t}created", b"v"],
        ],
    );

    assert_eq!(refused, Err("persistence unavailable"));
    // Each written key is copied once, before its first write.
    assert_eq!(undo_copies(), 3);
    assert_eq!(shard.snapshot_entries(), before);
}

#[test]
fn a_store_command_copies_only_its_destination() {
    let mut shard = KvShard::new();

    for index in 0..500u32 {
        let member = format!("m{index}");

        execute(
            &mut shard,
            command_of(&[b"SADD", b"{t}left", member.as_bytes()]),
        );
        execute(
            &mut shard,
            command_of(&[b"SADD", b"{t}right", member.as_bytes()]),
        );
    }
    shard.set_bytes(b"{t}source", b"copied");
    execute(&mut shard, command_of(&[b"SADD", b"{t}dest", b"old"]));
    let before = shard.snapshot_entries();
    crate::engine::UNDO_COPIES.set(0);

    let refused = run_transaction(
        &mut shard,
        &mut FailingSink,
        &[
            &[b"SUNIONSTORE", b"{t}dest", b"{t}left", b"{t}right"],
            &[b"ZUNIONSTORE", b"{t}zdest", b"2", b"{t}left", b"{t}right"],
            &[b"ZUNION", b"2", b"{t}left", b"{t}right"],
            &[b"COPY", b"{t}source", b"{t}copy"],
        ],
    );

    assert_eq!(refused, Err("persistence unavailable"));
    assert_eq!(undo_copies(), 3, "only the three destinations are copied");
    assert_eq!(shard.snapshot_entries(), before);
}

#[test]
fn a_refused_transaction_with_an_oom_reply_in_the_middle_restores_every_write() {
    let mut shard = KvShard::new();

    shard.set_eviction_policy(crate::EvictionPolicy::NoEviction);
    shard.set_bytes(b"{t}a", b"old");
    big_list(&mut shard);
    shard.set_memory_limit(Some(shard.memory_usage().charged_bytes() + 4_096));
    let before = shard.snapshot_entries();
    let huge = vec![b'x'; 64 * 1024];

    let refused = run_transaction(
        &mut shard,
        &mut FailingSink,
        &[
            &[b"SET", b"{t}a", b"new"],
            &[b"SET", b"{t}huge", &huge],
            &[b"LPUSH", b"{t}list", b"head"],
        ],
    );

    assert_eq!(refused, Err("persistence unavailable"));
    assert_eq!(shard.snapshot_entries(), before);

    // Accepted, the same batch keeps both small writes and refuses only the large one.
    let Ok(Response::Exec(replies)) = run_transaction(
        &mut shard,
        &mut RecordingSink::default(),
        &[
            &[b"SET", b"{t}a", b"new"],
            &[b"SET", b"{t}huge", &huge],
            &[b"LPUSH", b"{t}list", b"head"],
        ],
    ) else {
        panic!("the transaction ran");
    };

    assert!(matches!(replies[1], Response::Error(message) if message.starts_with(b"OOM")));
    assert_eq!(shard.get(b"{t}a"), Some(&b"new"[..]));
    assert_eq!(shard.get(b"{t}huge"), None);
    assert_eq!(shard.list_len(b"{t}list"), Ok(2_001));
}

#[test]
fn a_transaction_on_a_full_noeviction_store_runs_its_deletes_and_refuses_its_growth() {
    let mut shard = KvShard::new();

    shard.set_eviction_policy(crate::EvictionPolicy::NoEviction);
    for parts in [
        &[&b"SET"[..], b"{t}a", &[1; 256]][..],
        &[b"SET", b"{t}b", &[2; 256]],
        &[b"HSET", b"{t}hash", b"field", b"value"],
        &[b"SADD", b"{t}set", b"member", b"other"],
        &[b"RPUSH", b"{t}list", b"head", b"tail"],
    ] {
        execute(&mut shard, command_of(parts));
    }
    shard.set_memory_limit(Some(shard.memory_usage().charged_bytes() - 1));
    let big = vec![b'x'; 256];

    let Ok(Response::Exec(replies)) = run_transaction(
        &mut shard,
        &mut RecordingSink::default(),
        &[
            &[b"SET", b"{t}new", &big],
            &[b"HSET", b"{t}hash", b"grown", &big],
            &[b"SADD", b"{t}set", &big],
            &[b"DEL", b"{t}a"],
            &[b"UNLINK", b"{t}b"],
            &[b"HDEL", b"{t}hash", b"field"],
            &[b"SREM", b"{t}set", b"member"],
            &[b"LPOP", b"{t}list"],
        ],
    ) else {
        panic!("the transaction ran");
    };

    for reply in &replies[..3] {
        assert!(
            matches!(reply, Response::Error(message) if message.starts_with(b"OOM")),
            "{reply:?}"
        );
    }
    assert_eq!(
        &replies[3..],
        &[
            Response::Integer(1),
            Response::Integer(1),
            Response::Integer(1),
            Response::Integer(1),
            Response::Bulk(Some(Cow::Borrowed(b"head"))),
        ]
    );
    assert_eq!(shard.get(b"{t}a"), None);
    assert_eq!(shard.get(b"{t}b"), None);
    assert_eq!(shard.get(b"{t}new"), None);
    assert_eq!(shard.list_len(b"{t}list"), Ok(1));
}
