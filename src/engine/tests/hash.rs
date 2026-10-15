use super::*;

#[test]
fn hash_field_ttl_records_once_and_replays_the_same_deadlines() {
    // HEXPIRE HPEXPIREAT HTTL HPTTL HEXPIRETIME HPEXPIRETIME HPERSIST
    let mut shard = KvShard::new();
    let mut sink = DatabaseLog::default();
    let at = 4_000_000_000_000;
    let later = at + 5_000;
    let run = |shard: &mut KvShard, sink: &mut DatabaseLog, command| {
        execute_with_sink(shard, command, sink)
            .unwrap()
            .into_owned()
    };

    run(
        &mut shard,
        &mut sink,
        Command::HashSet {
            key: owned(b"h"),
            pairs: vec![
                (owned(b"a"), owned(b"1")),
                (owned(b"b"), owned(b"2")),
                (owned(b"c"), owned(b"3")),
            ],
        },
    );
    assert_eq!(
        run(
            &mut shard,
            &mut sink,
            field_expire(b"h", at, FieldExpireCondition::Always, &[b"a", b"b", b"x"])
        ),
        integers(&[1, 1, -2])
    );

    let records = sink.records.len();

    assert_eq!(
        run(
            &mut shard,
            &mut sink,
            field_expire(b"h", later, FieldExpireCondition::Nx, &[b"a", b"x"])
        ),
        integers(&[0, -2])
    );
    assert_eq!(
        run(
            &mut shard,
            &mut sink,
            field_expire(b"missing", later, FieldExpireCondition::Always, &[b"a"])
        ),
        integers(&[-2])
    );
    assert_eq!(
        sink.records.len(),
        records,
        "a rejected expire writes no record"
    );

    assert_eq!(
        run(
            &mut shard,
            &mut sink,
            Command::HashIncrement {
                key: owned(b"h"),
                field: owned(b"a"),
                delta: 4,
            }
        ),
        Response::Integer(5)
    );
    run(
        &mut shard,
        &mut sink,
        Command::HashSet {
            key: owned(b"h"),
            pairs: vec![(owned(b"b"), owned(b"9"))],
        },
    );
    assert_eq!(
        run(
            &mut shard,
            &mut sink,
            field_ttl(b"h", &[b"a", b"b", b"c", b"x"])
        ),
        integers(&[at as i64, -1, -1, -2])
    );
    assert_eq!(
        run(
            &mut shard,
            &mut sink,
            field_expire(b"h", 1, FieldExpireCondition::Always, &[b"c"])
        ),
        integers(&[2])
    );
    run(
        &mut shard,
        &mut sink,
        field_expire(b"h", later, FieldExpireCondition::Always, &[b"b"]),
    );
    assert_eq!(
        run(
            &mut shard,
            &mut sink,
            Command::HashPersist {
                key: owned(b"h"),
                fields: vec![owned(b"b"), owned(b"b"), owned(b"c")],
            }
        ),
        integers(&[1, -1, -2])
    );

    assert!(sink.records.iter().any(|(_, mutation)| matches!(
        mutation,
        OwnedMutation::HashFieldExpire { unix_millis, .. } if *unix_millis == at
    )));
    assert!(sink.records.iter().any(|(_, mutation)| matches!(
        mutation,
        OwnedMutation::HashDelete { fields, .. } if fields == &vec![b"c".to_vec()]
    )));

    let mut restored = replayed(&sink);
    let mut quiet = NoopMutationSink;

    assert_eq!(
        execute_with_sink(
            &mut restored,
            field_ttl(b"h", &[b"a", b"b", b"c"]),
            &mut quiet
        )
        .unwrap()
        .into_owned(),
        integers(&[at as i64, -1, -2])
    );
    assert_eq!(
        execute_with_sink(
            &mut restored,
            Command::HashGet {
                key: owned(b"h"),
                field: owned(b"a"),
            },
            &mut quiet,
        )
        .unwrap()
        .into_owned(),
        Response::Bulk(Some(Cow::Owned(b"5".to_vec())))
    );
}

#[test]
fn hsetnx_writes_only_a_missing_field_and_srandmember_keeps_members() {
    // HSETNX HSTRLEN SRANDMEMBER
    let mut shard = KvShard::new();
    let mut sink = RecordingSink::default();

    assert_eq!(
        execute_with_sink(
            &mut shard,
            Command::HashSetNx {
                key: owned(b"user"),
                field: owned(b"name"),
                value: owned(b"ada"),
            },
            &mut sink,
        )
        .unwrap(),
        Response::Integer(1)
    );
    assert_eq!(sink.mutations.len(), 1);
    assert_eq!(
        execute_with_sink(
            &mut shard,
            Command::HashSetNx {
                key: owned(b"user"),
                field: owned(b"name"),
                value: owned(b"other"),
            },
            &mut sink,
        )
        .unwrap(),
        Response::Integer(0)
    );
    assert_eq!(sink.mutations.len(), 1);
    assert_eq!(
        execute(
            &mut shard,
            Command::HashGet {
                key: owned(b"user"),
                field: owned(b"name"),
            }
        ),
        Response::Bulk(Some(Cow::Borrowed(b"ada")))
    );
    assert_eq!(
        execute(
            &mut shard,
            Command::HashStrLen {
                key: owned(b"user"),
                field: owned(b"name"),
            }
        ),
        Response::Integer(3)
    );
    assert_eq!(
        execute(
            &mut shard,
            Command::HashStrLen {
                key: owned(b"user"),
                field: owned(b"missing"),
            }
        ),
        Response::Integer(0)
    );
    assert_eq!(
        execute(
            &mut shard,
            Command::HashStrLen {
                key: owned(b"absent"),
                field: owned(b"name"),
            }
        ),
        Response::Integer(0)
    );
    execute(&mut shard, Command::set(owned(b"str"), owned(b"x")));
    assert_eq!(
        execute(
            &mut shard,
            Command::HashSetNx {
                key: owned(b"str"),
                field: owned(b"f"),
                value: owned(b"v"),
            }
        ),
        Response::Error(b"WRONGTYPE Operation against a key holding the wrong kind of value")
    );
    assert_eq!(
        execute(
            &mut shard,
            Command::HashStrLen {
                key: owned(b"str"),
                field: owned(b"f"),
            }
        ),
        Response::Error(b"WRONGTYPE Operation against a key holding the wrong kind of value")
    );
    execute(
        &mut shard,
        Command::SetAdd(owned(b"tags"), vec![owned(b"a")]),
    );
    assert_eq!(
        execute(
            &mut shard,
            Command::SetRandom {
                key: owned(b"tags"),
                count: None,
            }
        ),
        Response::Bulk(Some(Cow::Owned(b"a".to_vec())))
    );
    assert_eq!(
        execute(&mut shard, Command::SetCard(owned(b"tags"))),
        Response::Integer(1)
    );
    assert_eq!(
        execute(
            &mut shard,
            Command::SetRandom {
                key: owned(b"tags"),
                count: Some(-3),
            }
        ),
        Response::Array(vec![
            Some(Cow::Owned(b"a".to_vec())),
            Some(Cow::Owned(b"a".to_vec())),
            Some(Cow::Owned(b"a".to_vec())),
        ])
    );
    assert_eq!(
        execute(
            &mut shard,
            Command::SetRandom {
                key: owned(b"tags"),
                count: Some(0),
            }
        ),
        Response::Array(Vec::new())
    );
    assert_eq!(
        execute(
            &mut shard,
            Command::SetRandom {
                key: owned(b"missing"),
                count: None,
            }
        ),
        Response::Bulk(None)
    );
    assert_eq!(
        execute(
            &mut shard,
            Command::SetRandom {
                key: owned(b"missing"),
                count: Some(2),
            }
        ),
        Response::Array(Vec::new())
    );
    execute(
        &mut shard,
        Command::SetAdd(owned(b"tags"), vec![owned(b"b"), owned(b"c")]),
    );
    let Response::Array(sampled) = execute(
        &mut shard,
        Command::SetRandom {
            key: owned(b"tags"),
            count: Some(3),
        },
    ) else {
        panic!("expected an array");
    };

    let mut got: Vec<Vec<u8>> = sampled
        .into_iter()
        .map(|item| item.expect("member").into_owned())
        .collect();

    got.sort();
    assert_eq!(got, vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()]);
    assert_eq!(
        execute(&mut shard, Command::SetCard(owned(b"tags"))),
        Response::Integer(3)
    );
    assert_eq!(
        execute(
            &mut shard,
            Command::SetRandom {
                key: owned(b"str"),
                count: None,
            }
        ),
        Response::Error(b"WRONGTYPE Operation against a key holding the wrong kind of value")
    );
}

#[test]
fn long_field_lists_with_repeats_decide_each_repeat_after_the_previous_one() {
    let mut shard = KvShard::new();
    let mut sink = DatabaseLog::default();
    let at = 4_000_000_000_000;
    let mut run = |shard: &mut KvShard, command| {
        execute_with_sink(shard, command, &mut sink)
            .unwrap()
            .into_owned()
    };

    run(
        &mut shard,
        Command::HashSet {
            key: owned(b"h"),
            pairs: vec![(owned(b"a"), owned(b"1")), (owned(b"b"), owned(b"2"))],
        },
    );

    // Past eight fields the repeats are found through a map; the replies must match the
    // short-list rule: a repeat sees the decision made for its previous occurrence.
    let fields: [&[u8]; 10] = [b"a", b"x", b"a", b"b", b"a", b"x", b"b", b"y", b"z", b"a"];

    assert_eq!(
        run(
            &mut shard,
            field_expire(b"h", at, FieldExpireCondition::Nx, &fields)
        ),
        integers(&[1, -2, 0, 1, 0, -2, 0, -2, -2, 0])
    );
    assert_eq!(
        run(
            &mut shard,
            Command::HashPersist {
                key: owned(b"h"),
                fields: fields.iter().map(|field| owned(field)).collect(),
            }
        ),
        integers(&[1, -2, -1, 1, -1, -2, -1, -2, -2, -1])
    );
}
