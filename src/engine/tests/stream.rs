use super::*;

/// XINFO STREAM, XINFO GROUPS, and XINFO CONSUMERS read the stream that is already stored.
#[test]
fn stream_info_reads_length_groups_and_pending_consumers() {
    let mut shard = KvShard::new();
    let mut sink = RecordingSink::default();

    execute_with_sink(
        &mut shard,
        Command::StreamAdd {
            key: owned(b"events"),
            id: StreamAddId::Explicit(1, 0),
            max_len: None,
            nomkstream: false,
            min_id: None,
            delay_ms: None,
            fields: vec![(owned(b"field"), owned(b"a"))],
        },
        &mut sink,
    )
    .unwrap();
    execute_with_sink(
        &mut shard,
        Command::StreamGroupCreate {
            key: owned(b"events"),
            group: owned(b"workers"),
            start: crate::stream::GroupStart::Id(0, 0),
            mkstream: false,
        },
        &mut sink,
    )
    .unwrap();
    execute_with_sink(
        &mut shard,
        Command::StreamGroupRead {
            key: owned(b"events"),
            group: owned(b"workers"),
            consumer: owned(b"ada"),
            count: None,
            block: None,
            noack: false,
        },
        &mut sink,
    )
    .unwrap();

    match execute(
        &mut shard,
        Command::StreamInfo {
            key: owned(b"events"),
            group: None,
            kind: crate::protocol::StreamInfoKind::Stream,
        },
    ) {
        Response::Exec(items) => assert!(
            items
                .iter()
                .any(|item| matches!(item, Response::Integer(1)))
        ),
        other => panic!("{other:?}"),
    }

    match execute(
        &mut shard,
        Command::StreamInfo {
            key: owned(b"events"),
            group: None,
            kind: crate::protocol::StreamInfoKind::Groups,
        },
    ) {
        Response::Exec(groups) => assert_eq!(groups.len(), 1),
        other => panic!("{other:?}"),
    }

    match execute(
        &mut shard,
        Command::StreamInfo {
            key: owned(b"events"),
            group: Some(owned(b"workers")),
            kind: crate::protocol::StreamInfoKind::Consumers,
        },
    ) {
        Response::Exec(consumers) => assert_eq!(consumers.len(), 1),
        other => panic!("{other:?}"),
    }

    assert_eq!(
        execute(
            &mut shard,
            Command::StreamInfo {
                key: owned(b"missing"),
                group: None,
                kind: crate::protocol::StreamInfoKind::Stream,
            },
        ),
        Response::Error(b"ERR The XGROUP subcommand requires the key to exist")
    );
}

/// XADD, XLEN, XRANGE, XDEL, and XTRIM keep assigned ids. `~` is an exact MAXLEN.
#[test]
fn stream_commands_assign_ids_trim_and_reject_a_smaller_id() {
    let mut shard = KvShard::new();
    let mut sink = RecordingSink::default();
    let first = execute_with_sink(
        &mut shard,
        Command::StreamAdd {
            key: owned(b"events"),
            id: StreamAddId::Auto,
            max_len: None,
            nomkstream: false,
            min_id: None,
            delay_ms: None,
            fields: vec![(owned(b"field"), owned(b"a"))],
        },
        &mut sink,
    )
    .unwrap();
    let Response::Bulk(Some(first_id)) = first else {
        panic!("expected a bulk id");
    };

    let first_id = first_id.into_owned();
    let second = execute_with_sink(
        &mut shard,
        Command::StreamAdd {
            key: owned(b"events"),
            id: StreamAddId::Auto,
            max_len: Some(1),
            nomkstream: false,
            min_id: None,
            delay_ms: None,
            fields: vec![(owned(b"field"), owned(b"b"))],
        },
        &mut sink,
    )
    .unwrap();
    let Response::Bulk(Some(second_id)) = second else {
        panic!("expected a bulk id");
    };

    let second_id = second_id.into_owned();

    assert_ne!(first_id.as_slice(), second_id.as_slice());
    assert!(sink.mutations.len() >= 2);
    assert_eq!(
        execute(&mut shard, Command::StreamLen(owned(b"events"))),
        Response::Integer(1)
    );
    assert_eq!(
        execute(&mut shard, Command::Type(owned(b"events"))),
        Response::Simple(b"stream")
    );

    let (millis, sequence) = crate::stream::parse_id(&second_id).expect("id");
    let duplicate = execute_with_sink(
        &mut shard,
        Command::StreamAdd {
            key: owned(b"events"),
            id: StreamAddId::Explicit(millis, sequence),
            max_len: None,
            nomkstream: false,
            min_id: None,
            delay_ms: None,
            fields: vec![(owned(b"field"), owned(b"c"))],
        },
        &mut sink,
    )
    .unwrap();

    assert!(matches!(duplicate, Response::Error(_)));
    assert_eq!(
        execute(
            &mut shard,
            Command::StreamDelete {
                key: owned(b"events"),
                ids: vec![(millis, sequence)],
            },
        ),
        Response::Integer(1)
    );
    assert_eq!(
        execute(&mut shard, Command::StreamLen(owned(b"events"))),
        Response::Integer(0)
    );
    assert_eq!(
        execute(
            &mut shard,
            Command::StreamRange {
                key: owned(b"events"),
                start: crate::stream::StreamBound::Min,
                end: crate::stream::StreamBound::Max,
                count: None,
                reverse: false,
            },
        ),
        Response::Exec(Vec::new())
    );
    shard.set_bytes(b"plain", b"value");
    assert_eq!(
        execute(&mut shard, Command::StreamLen(owned(b"plain"))),
        wrong_type()
    );
}

/// XREAD, XREVRANGE, and XDEL. `XREAD BLOCK` is rejected. `$` yields nothing.
#[test]
fn stream_read_walks_forward_and_delete_accepts_many_ids() {
    let mut shard = KvShard::new();

    execute(
        &mut shard,
        Command::StreamAdd {
            key: owned(b"events"),
            id: StreamAddId::Explicit(1, 0),
            max_len: None,
            nomkstream: false,
            min_id: None,
            delay_ms: None,
            fields: vec![(owned(b"field"), owned(b"a"))],
        },
    );
    execute(
        &mut shard,
        Command::StreamAdd {
            key: owned(b"events"),
            id: StreamAddId::Explicit(2, 0),
            max_len: None,
            nomkstream: false,
            min_id: None,
            delay_ms: None,
            fields: vec![(owned(b"field"), owned(b"b"))],
        },
    );

    let newest = execute(
        &mut shard,
        Command::StreamRange {
            key: owned(b"events"),
            start: crate::stream::StreamBound::Min,
            end: crate::stream::StreamBound::Max,
            count: Some(1),
            reverse: true,
        },
    );
    let Response::Exec(entries) = newest else {
        panic!("expected a range");
    };

    let Response::Exec(parts) = &entries[0] else {
        panic!("expected one entry");
    };

    let Response::Bulk(Some(id)) = &parts[0] else {
        panic!("expected an id");
    };

    assert_eq!(id.as_ref(), b"2-0");
    assert_eq!(
        execute(
            &mut shard,
            Command::StreamRead {
                count: None,
                block: None,
                keys: vec![(owned(b"events"), crate::stream::StreamCursor::End)],
            },
        ),
        Response::NullArray
    );

    let forward = execute(
        &mut shard,
        Command::StreamRead {
            count: Some(1),
            block: None,
            keys: vec![(owned(b"events"), crate::stream::StreamCursor::After(0, 0))],
        },
    );
    let Response::Exec(groups) = forward else {
        panic!("expected a read");
    };

    assert_eq!(groups.len(), 1);
    assert_eq!(
        execute(
            &mut shard,
            Command::StreamDelete {
                key: owned(b"events"),
                ids: vec![(1, 0), (2, 0), (1, 0)],
            },
        ),
        Response::Integer(2)
    );
    assert_eq!(
        execute(&mut shard, Command::StreamLen(owned(b"events"))),
        Response::Integer(0)
    );
}

/// XREAD BLOCK parks until a later XADD. The entry stays in the stream.
#[test]
fn xread_block_wakes_on_xadd_without_removing_the_entry() {
    let mut shard = KvShard::new();
    let parked = execute(
        &mut shard,
        Command::StreamRead {
            count: None,
            block: Some(Some(10)),
            keys: vec![(owned(b"events"), crate::stream::StreamCursor::End)],
        },
    );
    let Response::BlockStream {
        cursors,
        timeout_ms: Some(10),
        ..
    } = parked
    else {
        panic!("expected to park");
    };

    assert_eq!(cursors, vec![(0, 0)]);

    let mut waiters = crate::list_waiters::ListWaiters::new(4);
    let (reply, receiver) = std::sync::mpsc::sync_channel(1);

    waiters
        .register_stream(
            vec![tagged(0, b"events")],
            vec![(0, 0)],
            None,
            reply,
            b"*-1\r\n".to_vec(),
        )
        .unwrap();
    execute_with_waiters(
        &mut shard,
        Command::StreamAdd {
            key: owned(b"events"),
            id: StreamAddId::Explicit(1, 0),
            max_len: None,
            nomkstream: false,
            min_id: None,
            delay_ms: None,
            fields: vec![(owned(b"field"), owned(b"a"))],
        },
        &mut NoopMutationSink,
        &mut waiters,
    )
    .unwrap();

    let frame = receiver.try_recv().unwrap();

    assert!(frame.windows(3).any(|window| window == b"1-0"));
    assert_eq!(
        execute(&mut shard, Command::StreamLen(owned(b"events"))),
        Response::Integer(1)
    );
    assert!(waiters.is_empty());
}

/// Every `XREAD BLOCK` reader behind a new entry gets it, not only the oldest one, on the
/// primary and on a replica that applies the primary's record.
#[test]
fn a_new_entry_wakes_every_blocked_reader() {
    type Parked = (
        crate::list_waiters::ListWaiters<std::sync::mpsc::SyncSender<Vec<u8>>>,
        Vec<std::sync::mpsc::Receiver<Vec<u8>>>,
    );

    fn parked_readers(count: usize) -> Parked {
        let mut waiters = crate::list_waiters::ListWaiters::new(8);
        let mut receivers = Vec::new();

        for _ in 0..count {
            let (reply, receiver) = std::sync::mpsc::sync_channel(1);

            waiters
                .register_stream(
                    vec![tagged(0, b"events")],
                    vec![(0, 0)],
                    None,
                    reply,
                    b"*-1\r\n".to_vec(),
                )
                .unwrap();
            receivers.push(receiver);
        }
        (waiters, receivers)
    }

    let mut shard = KvShard::new();
    let (mut waiters, receivers) = parked_readers(3);

    execute_with_waiters(
        &mut shard,
        Command::StreamAdd {
            key: owned(b"events"),
            id: StreamAddId::Explicit(1, 0),
            max_len: None,
            nomkstream: false,
            min_id: None,
            delay_ms: None,
            fields: vec![(owned(b"field"), owned(b"a"))],
        },
        &mut NoopMutationSink,
        &mut waiters,
    )
    .unwrap();

    for receiver in &receivers {
        assert!(
            receiver
                .try_recv()
                .unwrap()
                .windows(3)
                .any(|window| window == b"1-0")
        );
    }
    assert!(waiters.is_empty());

    // A replica: the record is applied, then its readers are woken from it.
    let (mut waiters, receivers) = parked_readers(2);
    let record = crate::OwnedMutation::Transaction {
        mutations: vec![crate::OwnedMutation::StreamAppend {
            key: b"events".to_vec(),
            millis: 2,
            sequence: 0,
            fields: vec![(b"field".to_vec(), b"b".to_vec())],
        }],
    };

    crate::engine::wake_replicated_readers(&mut waiters, 0, &record, &mut |_, _, _| true);
    for receiver in &receivers {
        assert!(
            receiver
                .try_recv()
                .unwrap()
                .windows(3)
                .any(|window| window == b"2-0")
        );
    }

    // An entry a trim in the same record already dropped wakes nobody.
    let (mut waiters, receivers) = parked_readers(1);

    crate::engine::wake_replicated_readers(&mut waiters, 0, &record, &mut |_, _, _| false);
    assert!(receivers[0].try_recv().is_err());
}

/// XREADGROUP BLOCK claims the new id for the oldest consumer and leaves it stored.
#[test]
fn xreadgroup_block_claims_one_consumer_and_leaves_the_entry() {
    let mut shard = KvShard::new();

    execute(
        &mut shard,
        Command::StreamGroupCreate {
            key: owned(b"events"),
            group: owned(b"g"),
            start: crate::stream::GroupStart::End,
            mkstream: true,
        },
    );

    let parked = execute(
        &mut shard,
        Command::StreamGroupRead {
            key: owned(b"events"),
            group: owned(b"g"),
            consumer: owned(b"c1"),
            count: None,
            block: Some(Some(10)),
            noack: false,
        },
    );
    let Response::BlockGroup {
        cursor,
        timeout_ms: Some(10),
        ..
    } = parked
    else {
        panic!("expected to park");
    };

    assert_eq!(cursor, (0, 0));

    let mut waiters = crate::list_waiters::ListWaiters::new(4);
    let (stream_reply, stream_receiver) = std::sync::mpsc::sync_channel(1);
    let (first_reply, first_receiver) = std::sync::mpsc::sync_channel(1);
    let (second_reply, second_receiver) = std::sync::mpsc::sync_channel(1);

    waiters
        .register_stream(
            vec![tagged(0, b"events")],
            vec![(0, 0)],
            None,
            stream_reply,
            b"*-1\r\n".to_vec(),
        )
        .unwrap();
    waiters
        .register_group(
            crate::list_waiters::GroupRegistration {
                key: tagged(0, b"events"),
                group: b"g".to_vec(),
                consumer: b"c1".to_vec(),
                cursor: (0, 0),
                noack: false,
            },
            None,
            first_reply,
            b"*-1\r\n".to_vec(),
        )
        .unwrap();
    waiters
        .register_group(
            crate::list_waiters::GroupRegistration {
                key: tagged(0, b"events"),
                group: b"g".to_vec(),
                consumer: b"c2".to_vec(),
                cursor: (0, 0),
                noack: false,
            },
            None,
            second_reply,
            b"*-1\r\n".to_vec(),
        )
        .unwrap();
    execute_with_waiters(
        &mut shard,
        Command::StreamAdd {
            key: owned(b"events"),
            id: StreamAddId::Explicit(1, 0),
            max_len: None,
            nomkstream: false,
            min_id: None,
            delay_ms: None,
            fields: vec![(owned(b"field"), owned(b"a"))],
        },
        &mut NoopMutationSink,
        &mut waiters,
    )
    .unwrap();

    let stream_frame = stream_receiver.try_recv().unwrap();
    let first_frame = first_receiver.try_recv().unwrap();

    assert!(stream_frame.windows(3).any(|window| window == b"1-0"));
    assert!(first_frame.windows(3).any(|window| window == b"1-0"));
    assert!(second_receiver.try_recv().is_err());
    assert_eq!(waiters.len(), 1);
    assert_eq!(
        execute(
            &mut shard,
            Command::StreamPending {
                key: owned(b"events"),
                group: owned(b"g"),
            },
        ),
        Response::Exec(vec![
            Response::Integer(1),
            Response::Bulk(Some(Cow::Owned(b"1-0".to_vec()))),
            Response::Bulk(Some(Cow::Owned(b"1-0".to_vec()))),
            Response::Exec(vec![Response::Exec(vec![
                Response::Bulk(Some(Cow::Owned(b"c1".to_vec()))),
                Response::Integer(1),
            ])]),
        ])
    );
    execute_with_waiters(
        &mut shard,
        Command::StreamAdd {
            key: owned(b"events"),
            id: StreamAddId::Explicit(2, 0),
            max_len: None,
            nomkstream: false,
            min_id: None,
            delay_ms: None,
            fields: vec![(owned(b"field"), owned(b"b"))],
        },
        &mut NoopMutationSink,
        &mut waiters,
    )
    .unwrap();

    let second_frame = second_receiver.try_recv().unwrap();

    assert!(second_frame.windows(3).any(|window| window == b"2-0"));
    assert_eq!(
        execute(&mut shard, Command::StreamLen(owned(b"events"))),
        Response::Integer(2)
    );
    assert!(waiters.is_empty());
}

/// XGROUP CREATECONSUMER keeps a consumer that has no pending entry.
#[test]
fn xgroup_createconsumer_is_visible_before_a_delivery() {
    let mut shard = KvShard::new();

    execute(
        &mut shard,
        Command::StreamAdd {
            key: owned(b"events"),
            id: StreamAddId::Explicit(1, 0),
            max_len: None,
            min_id: None,
            delay_ms: None,
            nomkstream: false,
            fields: vec![(owned(b"field"), owned(b"a"))],
        },
    );
    execute(
        &mut shard,
        Command::StreamGroupCreate {
            key: owned(b"events"),
            group: owned(b"g"),
            start: crate::stream::GroupStart::Id(0, 0),
            mkstream: false,
        },
    );
    assert_eq!(
        execute(
            &mut shard,
            Command::StreamGroupCreateConsumer {
                key: owned(b"events"),
                group: owned(b"g"),
                consumer: owned(b"c1"),
            },
        ),
        Response::Integer(1)
    );
    assert_eq!(
        execute(
            &mut shard,
            Command::StreamGroupCreateConsumer {
                key: owned(b"events"),
                group: owned(b"g"),
                consumer: owned(b"c1"),
            },
        ),
        Response::Integer(0)
    );

    let rows = shard
        .stream_consumer_overviews(b"events", b"g", 0)
        .expect("group");

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].name, b"c1");
    assert_eq!(rows[0].pending, 0);
    assert_eq!(
        execute(
            &mut shard,
            Command::StreamGroupDelConsumer {
                key: owned(b"events"),
                group: owned(b"g"),
                consumer: owned(b"c1"),
            },
        ),
        Response::Integer(0)
    );
    assert!(
        shard
            .stream_consumer_overviews(b"events", b"g", 0)
            .expect("group")
            .is_empty()
    );
}

/// XGROUP DESTROY, XGROUP SETID, and XGROUP DELCONSUMER update the group. NOACK skips the pending list.
#[test]
fn xgroup_destroy_setid_delconsumer_and_noack() {
    let mut shard = KvShard::new();

    execute(
        &mut shard,
        Command::StreamAdd {
            key: owned(b"events"),
            id: StreamAddId::Explicit(1, 0),
            max_len: None,
            nomkstream: false,
            min_id: None,
            delay_ms: None,
            fields: vec![(owned(b"field"), owned(b"a"))],
        },
    );
    execute(
        &mut shard,
        Command::StreamAdd {
            key: owned(b"events"),
            id: StreamAddId::Explicit(2, 0),
            max_len: None,
            nomkstream: false,
            min_id: None,
            delay_ms: None,
            fields: vec![(owned(b"field"), owned(b"b"))],
        },
    );
    execute(
        &mut shard,
        Command::StreamGroupCreate {
            key: owned(b"events"),
            group: owned(b"g"),
            start: crate::stream::GroupStart::Id(0, 0),
            mkstream: false,
        },
    );

    let first = execute(
        &mut shard,
        Command::StreamGroupRead {
            key: owned(b"events"),
            group: owned(b"g"),
            consumer: owned(b"c1"),
            count: Some(1),
            block: None,
            noack: true,
        },
    );
    let Response::Exec(_) = first else {
        panic!("expected a noack delivery");
    };

    assert_eq!(
        execute(
            &mut shard,
            Command::StreamPending {
                key: owned(b"events"),
                group: owned(b"g"),
            },
        ),
        Response::Exec(vec![
            Response::Integer(0),
            Response::Bulk(None),
            Response::Bulk(None),
            Response::Exec(Vec::new()),
        ])
    );
    assert_eq!(
        execute(
            &mut shard,
            Command::StreamGroupSetId {
                key: owned(b"events"),
                group: owned(b"g"),
                start: crate::stream::GroupStart::End,
            },
        ),
        Response::Simple(b"OK")
    );
    assert_eq!(
        execute(
            &mut shard,
            Command::StreamGroupRead {
                key: owned(b"events"),
                group: owned(b"g"),
                consumer: owned(b"c1"),
                count: None,
                block: None,
                noack: false,
            },
        ),
        Response::NullArray
    );
    execute(
        &mut shard,
        Command::StreamGroupSetId {
            key: owned(b"events"),
            group: owned(b"g"),
            start: crate::stream::GroupStart::Id(0, 0),
        },
    );
    execute(
        &mut shard,
        Command::StreamGroupRead {
            key: owned(b"events"),
            group: owned(b"g"),
            consumer: owned(b"c1"),
            count: None,
            block: None,
            noack: false,
        },
    );
    assert_eq!(
        execute(
            &mut shard,
            Command::StreamGroupDelConsumer {
                key: owned(b"events"),
                group: owned(b"g"),
                consumer: owned(b"c1"),
            },
        ),
        Response::Integer(2)
    );
    assert_eq!(
        execute(
            &mut shard,
            Command::StreamGroupDelConsumer {
                key: owned(b"events"),
                group: owned(b"g"),
                consumer: owned(b"missing"),
            },
        ),
        Response::Integer(0)
    );

    let parked = execute(
        &mut shard,
        Command::StreamGroupRead {
            key: owned(b"events"),
            group: owned(b"g"),
            consumer: owned(b"c2"),
            count: None,
            block: Some(None),
            noack: false,
        },
    );
    let Response::BlockGroup {
        key,
        group,
        consumer,
        cursor,
        timeout_ms: None,
        noack: false,
    } = parked
    else {
        panic!("expected a parked consumer");
    };

    let mut waiters = crate::list_waiters::ListWaiters::new(4);
    let (reply, receiver) = std::sync::mpsc::sync_channel(1);

    waiters
        .park(
            crate::list_waiters::ParkRequest::Group {
                key: key.into_owned(),
                group: group.into_owned(),
                consumer: consumer.into_owned(),
                cursor,
                noack: false,
                timeout_ms: None,
            },
            reply,
            b"*-1\r\n".to_vec(),
        )
        .unwrap();
    assert_eq!(
        execute_with_waiters(
            &mut shard,
            Command::StreamGroupDestroy {
                key: owned(b"events"),
                group: owned(b"g"),
            },
            &mut NoopMutationSink,
            &mut waiters,
        )
        .unwrap(),
        Response::Integer(1)
    );
    assert_eq!(
        receiver.try_recv().unwrap(),
        b"-NOGROUP No such key or consumer group\r\n"
    );
    assert!(waiters.is_empty());
    assert_eq!(
        execute(
            &mut shard,
            Command::StreamGroupDestroy {
                key: owned(b"events"),
                group: owned(b"g"),
            },
        ),
        Response::Integer(0)
    );
    assert_eq!(
        execute(
            &mut shard,
            Command::StreamGroupSetId {
                key: owned(b"events"),
                group: owned(b"g"),
                start: crate::stream::GroupStart::Id(0, 0),
            },
        ),
        Response::Error(b"NOGROUP No such key or consumer group")
    );
}

/// XGROUP CREATE, XREADGROUP and XACK keep one delivery until it is acknowledged.
#[test]
fn stream_group_delivers_each_new_id_once() {
    let mut shard = KvShard::new();

    assert_eq!(
        execute(
            &mut shard,
            Command::StreamGroupCreate {
                key: owned(b"missing"),
                group: owned(b"g"),
                start: crate::stream::GroupStart::Id(0, 0),
                mkstream: false,
            },
        ),
        Response::Error(b"ERR The XGROUP subcommand requires the key to exist")
    );
    execute(
        &mut shard,
        Command::StreamAdd {
            key: owned(b"events"),
            id: StreamAddId::Explicit(1, 0),
            max_len: None,
            nomkstream: false,
            min_id: None,
            delay_ms: None,
            fields: vec![(owned(b"field"), owned(b"a"))],
        },
    );
    assert_eq!(
        execute(
            &mut shard,
            Command::StreamGroupCreate {
                key: owned(b"events"),
                group: owned(b"g"),
                start: crate::stream::GroupStart::End,
                mkstream: false,
            },
        ),
        Response::Simple(b"OK")
    );
    assert_eq!(
        execute(
            &mut shard,
            Command::StreamGroupCreate {
                key: owned(b"events"),
                group: owned(b"g"),
                start: crate::stream::GroupStart::End,
                mkstream: false,
            },
        ),
        Response::Error(b"BUSYGROUP Consumer Group name already exists")
    );
    execute(
        &mut shard,
        Command::StreamAdd {
            key: owned(b"events"),
            id: StreamAddId::Explicit(2, 0),
            max_len: None,
            nomkstream: false,
            min_id: None,
            delay_ms: None,
            fields: vec![(owned(b"field"), owned(b"b"))],
        },
    );
    execute(
        &mut shard,
        Command::StreamAdd {
            key: owned(b"events"),
            id: StreamAddId::Explicit(3, 0),
            max_len: None,
            nomkstream: false,
            min_id: None,
            delay_ms: None,
            fields: vec![(owned(b"field"), owned(b"c"))],
        },
    );

    let first = execute(
        &mut shard,
        Command::StreamGroupRead {
            key: owned(b"events"),
            group: owned(b"g"),
            consumer: owned(b"c1"),
            count: Some(1),
            block: None,
            noack: false,
        },
    );
    let Response::Exec(groups) = &first else {
        panic!("expected a group read");
    };

    let Response::Exec(entries) = &groups[0] else {
        panic!("expected one stream");
    };

    let Response::Exec(entry) = &entries[1] else {
        panic!("expected entries");
    };

    let Response::Exec(fields) = &entry[0] else {
        panic!("expected one entry");
    };

    let Response::Bulk(Some(id)) = &fields[0] else {
        panic!("expected an id");
    };

    assert_eq!(id.as_ref(), b"2-0");

    let second = execute(
        &mut shard,
        Command::StreamGroupRead {
            key: owned(b"events"),
            group: owned(b"g"),
            consumer: owned(b"c1"),
            count: None,
            block: None,
            noack: false,
        },
    );
    let Response::Exec(groups) = &second else {
        panic!("expected the rest");
    };

    let Response::Exec(entries) = &groups[0] else {
        panic!("expected one stream");
    };

    let Response::Exec(entry) = &entries[1] else {
        panic!("expected entries");
    };

    let Response::Exec(fields) = &entry[0] else {
        panic!("expected one entry");
    };

    let Response::Bulk(Some(id)) = &fields[0] else {
        panic!("expected an id");
    };

    assert_eq!(id.as_ref(), b"3-0");
    assert_eq!(
        execute(
            &mut shard,
            Command::StreamGroupRead {
                key: owned(b"events"),
                group: owned(b"g"),
                consumer: owned(b"c1"),
                count: None,
                block: None,
                noack: false,
            },
        ),
        Response::NullArray
    );
    assert_eq!(
        execute(
            &mut shard,
            Command::StreamAck {
                key: owned(b"events"),
                group: owned(b"g"),
                ids: vec![(2, 0), (3, 0), (2, 0)],
            },
        ),
        Response::Integer(2)
    );
    assert_eq!(
        execute(
            &mut shard,
            Command::StreamAck {
                key: owned(b"events"),
                group: owned(b"g"),
                ids: vec![(3, 0)],
            },
        ),
        Response::Integer(0)
    );
    assert_eq!(
        execute(&mut shard, Command::StreamLen(owned(b"events"))),
        Response::Integer(3)
    );

    let mut restored = KvShard::new();

    for entry in shard.snapshot_entries() {
        restored.restore_snapshot_entry(entry).unwrap();
    }

    assert_eq!(
        execute(
            &mut restored,
            Command::StreamGroupRead {
                key: owned(b"events"),
                group: owned(b"g"),
                consumer: owned(b"c1"),
                count: None,
                block: None,
                noack: false,
            },
        ),
        Response::NullArray
    );
    execute(
        &mut restored,
        Command::StreamAdd {
            key: owned(b"events"),
            id: StreamAddId::Explicit(4, 0),
            max_len: None,
            nomkstream: false,
            min_id: None,
            delay_ms: None,
            fields: vec![(owned(b"field"), owned(b"d"))],
        },
    );

    let after = execute(
        &mut restored,
        Command::StreamGroupRead {
            key: owned(b"events"),
            group: owned(b"g"),
            consumer: owned(b"c1"),
            count: None,
            block: None,
            noack: false,
        },
    );
    let Response::Exec(groups) = &after else {
        panic!("expected the id after the snapshot");
    };

    let Response::Exec(entries) = &groups[0] else {
        panic!("expected one stream");
    };

    let Response::Exec(entry) = &entries[1] else {
        panic!("expected entries");
    };

    let Response::Exec(fields) = &entry[0] else {
        panic!("expected one entry");
    };

    let Response::Bulk(Some(id)) = &fields[0] else {
        panic!("expected an id");
    };

    assert_eq!(id.as_ref(), b"4-0");
}

/// XPENDING returns the pending count, the id span, and one row per consumer.
#[test]
fn xpending_summarizes_claimed_ids() {
    let mut shard = KvShard::new();

    execute(
        &mut shard,
        Command::StreamAdd {
            key: owned(b"events"),
            id: StreamAddId::Explicit(1, 0),
            max_len: None,
            nomkstream: false,
            min_id: None,
            delay_ms: None,
            fields: vec![(owned(b"field"), owned(b"a"))],
        },
    );
    execute(
        &mut shard,
        Command::StreamGroupCreate {
            key: owned(b"events"),
            group: owned(b"g"),
            start: crate::stream::GroupStart::End,
            mkstream: false,
        },
    );
    assert_eq!(
        execute(
            &mut shard,
            Command::StreamPending {
                key: owned(b"events"),
                group: owned(b"g"),
            },
        ),
        Response::Exec(vec![
            Response::Integer(0),
            Response::Bulk(None),
            Response::Bulk(None),
            Response::Exec(Vec::new()),
        ])
    );
    execute(
        &mut shard,
        Command::StreamAdd {
            key: owned(b"events"),
            id: StreamAddId::Explicit(2, 0),
            max_len: None,
            nomkstream: false,
            min_id: None,
            delay_ms: None,
            fields: vec![(owned(b"field"), owned(b"b"))],
        },
    );
    execute(
        &mut shard,
        Command::StreamAdd {
            key: owned(b"events"),
            id: StreamAddId::Explicit(3, 0),
            max_len: None,
            nomkstream: false,
            min_id: None,
            delay_ms: None,
            fields: vec![(owned(b"field"), owned(b"c"))],
        },
    );
    execute(
        &mut shard,
        Command::StreamGroupRead {
            key: owned(b"events"),
            group: owned(b"g"),
            consumer: owned(b"c1"),
            count: Some(1),
            block: None,
            noack: false,
        },
    );
    execute(
        &mut shard,
        Command::StreamGroupRead {
            key: owned(b"events"),
            group: owned(b"g"),
            consumer: owned(b"c2"),
            count: None,
            block: None,
            noack: false,
        },
    );
    assert_eq!(
        execute(
            &mut shard,
            Command::StreamPending {
                key: owned(b"events"),
                group: owned(b"g"),
            },
        ),
        Response::Exec(vec![
            Response::Integer(2),
            Response::Bulk(Some(Cow::Owned(b"2-0".to_vec()))),
            Response::Bulk(Some(Cow::Owned(b"3-0".to_vec()))),
            Response::Exec(vec![
                Response::Exec(vec![
                    Response::Bulk(Some(Cow::Owned(b"c1".to_vec()))),
                    Response::Integer(1),
                ]),
                Response::Exec(vec![
                    Response::Bulk(Some(Cow::Owned(b"c2".to_vec()))),
                    Response::Integer(1),
                ]),
            ]),
        ])
    );
    execute(
        &mut shard,
        Command::StreamAck {
            key: owned(b"events"),
            group: owned(b"g"),
            ids: vec![(2, 0), (3, 0)],
        },
    );
    assert_eq!(
        execute(
            &mut shard,
            Command::StreamPending {
                key: owned(b"events"),
                group: owned(b"g"),
            },
        ),
        Response::Exec(vec![
            Response::Integer(0),
            Response::Bulk(None),
            Response::Bulk(None),
            Response::Exec(Vec::new()),
        ])
    );
    assert_eq!(
        execute(
            &mut shard,
            Command::StreamPending {
                key: owned(b"missing"),
                group: owned(b"g"),
            },
        ),
        Response::Error(b"NOGROUP No such key or consumer group")
    );
}

/// XAUTOCLAIM scans the pending list from a start id and claims idle entries.
#[test]
fn xautoclaim_claims_one_idle_entry_and_returns_the_next_start() {
    let clock = crate::time::TestClock::new(1_000);
    let mut shard = KvShard::with_clock(clock.clone());

    execute(
        &mut shard,
        Command::StreamAdd {
            key: owned(b"events"),
            id: StreamAddId::Explicit(1, 0),
            max_len: None,
            nomkstream: false,
            min_id: None,
            delay_ms: None,
            fields: vec![(owned(b"field"), owned(b"a"))],
        },
    );
    execute(
        &mut shard,
        Command::StreamAdd {
            key: owned(b"events"),
            id: StreamAddId::Explicit(2, 0),
            max_len: None,
            nomkstream: false,
            min_id: None,
            delay_ms: None,
            fields: vec![(owned(b"field"), owned(b"b"))],
        },
    );
    execute(
        &mut shard,
        Command::StreamGroupCreate {
            key: owned(b"events"),
            group: owned(b"g"),
            start: crate::stream::GroupStart::Id(0, 0),
            mkstream: false,
        },
    );
    execute(
        &mut shard,
        Command::StreamGroupRead {
            key: owned(b"events"),
            group: owned(b"g"),
            consumer: owned(b"c1"),
            count: None,
            block: None,
            noack: false,
        },
    );
    clock.set(1_500);

    match execute(
        &mut shard,
        Command::StreamAutoclaim {
            key: owned(b"events"),
            group: owned(b"g"),
            consumer: owned(b"c2"),
            min_idle: 400,
            start: (0, 0),
            count: Some(1),
            just_id: true,
        },
    ) {
        Response::Exec(parts) => {
            assert_eq!(parts.len(), 3);
            assert_eq!(parts[0], Response::Bulk(Some(Cow::Owned(b"1-0".to_vec()))));
            assert_eq!(
                parts[1],
                Response::Exec(vec![Response::Bulk(Some(Cow::Owned(b"1-0".to_vec())))])
            );
        }

        other => panic!("{other:?}"),
    }
}

/// XTRIM MINID drops entries strictly below the threshold. `~` is exact.
#[test]
fn xtrim_minid_drops_entries_below_the_threshold() {
    let mut shard = KvShard::new();

    for (millis, value) in [(1, b"a" as &[u8]), (2, b"b"), (3, b"c")] {
        execute(
            &mut shard,
            Command::StreamAdd {
                key: owned(b"events"),
                id: StreamAddId::Explicit(millis, 0),
                max_len: None,
                min_id: None,
                delay_ms: None,
                nomkstream: false,
                fields: vec![(owned(b"field"), owned(value))],
            },
        );
    }

    assert_eq!(
        execute(
            &mut shard,
            Command::StreamTrimMinId {
                key: owned(b"events"),
                id: (2, 0),
            },
        ),
        Response::Integer(1)
    );
    assert_eq!(
        execute(&mut shard, Command::StreamLen(owned(b"events"))),
        Response::Integer(2)
    );
    assert_eq!(
        execute(
            &mut shard,
            Command::StreamAdd {
                key: owned(b"events"),
                id: StreamAddId::Explicit(4, 0),
                max_len: None,
                min_id: Some((3, 0)),
                delay_ms: None,
                nomkstream: false,
                fields: vec![(owned(b"field"), owned(b"d"))],
            },
        ),
        Response::Bulk(Some(Cow::Owned(b"4-0".to_vec())))
    );
    assert_eq!(
        execute(&mut shard, Command::StreamLen(owned(b"events"))),
        Response::Integer(2)
    );
}

/// XADD NOMKSTREAM leaves a missing key absent and returns a null bulk.
#[test]
fn xadd_nomkstream_does_not_create_a_missing_key() {
    let mut shard = KvShard::new();

    assert_eq!(
        execute(
            &mut shard,
            Command::StreamAdd {
                key: owned(b"events"),
                id: StreamAddId::Auto,
                max_len: None,
                nomkstream: true,
                min_id: None,
                delay_ms: None,
                fields: vec![(owned(b"field"), owned(b"a"))],
            },
        ),
        Response::Bulk(None)
    );
    assert_eq!(
        execute(&mut shard, Command::Type(owned(b"events"))),
        Response::Simple(b"none")
    );
    execute(
        &mut shard,
        Command::StreamAdd {
            key: owned(b"events"),
            id: StreamAddId::Explicit(1, 0),
            max_len: None,
            nomkstream: false,
            min_id: None,
            delay_ms: None,
            fields: vec![(owned(b"field"), owned(b"a"))],
        },
    );
    assert_eq!(
        execute(
            &mut shard,
            Command::StreamAdd {
                key: owned(b"events"),
                id: StreamAddId::Explicit(2, 0),
                max_len: None,
                nomkstream: true,
                min_id: None,
                delay_ms: None,
                fields: vec![(owned(b"field"), owned(b"b"))],
            },
        ),
        Response::Bulk(Some(Cow::Owned(b"2-0".to_vec())))
    );
    execute(&mut shard, Command::set(owned(b"plain"), owned(b"x")));
    assert_eq!(
        execute(
            &mut shard,
            Command::StreamAdd {
                key: owned(b"plain"),
                id: StreamAddId::Auto,
                max_len: None,
                nomkstream: true,
                min_id: None,
                delay_ms: None,
                fields: vec![(owned(b"field"), owned(b"a"))],
            },
        ),
        wrong_type()
    );
}

/// XADD `<millis>-*` keeps that millisecond and assigns the next sequence.
#[test]
fn xadd_auto_sequence_stays_on_the_given_millisecond() {
    let clock = crate::time::TestClock::new(1_000);
    let mut shard = KvShard::with_clock(clock);

    assert_eq!(
        execute(
            &mut shard,
            Command::StreamAdd {
                key: owned(b"events"),
                id: StreamAddId::AutoSequence(1_500),
                max_len: None,
                nomkstream: false,
                min_id: None,
                delay_ms: None,
                fields: vec![(owned(b"field"), owned(b"a"))],
            },
        ),
        Response::Bulk(Some(Cow::Owned(b"1500-0".to_vec())))
    );
    assert_eq!(
        execute(
            &mut shard,
            Command::StreamAdd {
                key: owned(b"events"),
                id: StreamAddId::AutoSequence(1_500),
                max_len: None,
                nomkstream: false,
                min_id: None,
                delay_ms: None,
                fields: vec![(owned(b"field"), owned(b"b"))],
            },
        ),
        Response::Bulk(Some(Cow::Owned(b"1500-1".to_vec())))
    );
    assert_eq!(
        execute(
            &mut shard,
            Command::StreamAdd {
                key: owned(b"events"),
                id: StreamAddId::AutoSequence(900),
                max_len: None,
                nomkstream: false,
                min_id: None,
                delay_ms: None,
                fields: vec![(owned(b"field"), owned(b"c"))],
            },
        ),
        Response::Error(
            b"ERR The ID specified in XADD is equal or smaller than the target stream top item",
        )
    );
}

/// XSETID moves the last generated id and does not store another entry.
#[test]
fn xsetid_moves_the_next_auto_id_and_refuses_a_smaller_one() {
    let clock = crate::time::TestClock::new(1_000);
    let mut shard = KvShard::with_clock(clock);

    execute(
        &mut shard,
        Command::StreamAdd {
            key: owned(b"events"),
            id: StreamAddId::Explicit(1, 0),
            max_len: None,
            nomkstream: false,
            min_id: None,
            delay_ms: None,
            fields: vec![(owned(b"field"), owned(b"a"))],
        },
    );
    assert_eq!(
        execute(
            &mut shard,
            Command::StreamSetId {
                key: owned(b"events"),
                id: (2_000, 3),
            },
        ),
        Response::Simple(b"OK")
    );
    assert_eq!(
        execute(
            &mut shard,
            Command::StreamAdd {
                key: owned(b"events"),
                id: StreamAddId::Auto,
                max_len: None,
                nomkstream: false,
                min_id: None,
                delay_ms: None,
                fields: vec![(owned(b"field"), owned(b"b"))],
            },
        ),
        Response::Bulk(Some(Cow::Owned(b"2000-4".to_vec())))
    );
    assert_eq!(
        execute(&mut shard, Command::StreamLen(owned(b"events"))),
        Response::Integer(2)
    );
    assert_eq!(
        execute(
            &mut shard,
            Command::StreamSetId {
                key: owned(b"events"),
                id: (2_000, 3),
            },
        ),
        Response::Error(
            b"ERR The ID specified in XSETID is smaller than the target stream top item",
        )
    );
    assert_eq!(
        execute(
            &mut shard,
            Command::StreamSetId {
                key: owned(b"missing"),
                id: (1, 0),
            },
        ),
        Response::Error(b"ERR no such key")
    );
    execute(&mut shard, Command::set(owned(b"plain"), owned(b"x")));
    assert_eq!(
        execute(
            &mut shard,
            Command::StreamSetId {
                key: owned(b"plain"),
                id: (1, 0),
            },
        ),
        wrong_type()
    );
}

/// XCLAIM moves a pending id to another consumer once it has been idle long enough.
#[test]
fn xclaim_waits_for_idle_and_drops_a_deleted_entry() {
    let clock = crate::time::TestClock::new(1_000);
    let mut shard = KvShard::with_clock(clock.clone());

    execute(
        &mut shard,
        Command::StreamAdd {
            key: owned(b"events"),
            id: StreamAddId::Explicit(1, 0),
            max_len: None,
            nomkstream: false,
            min_id: None,
            delay_ms: None,
            fields: vec![(owned(b"field"), owned(b"a"))],
        },
    );
    execute(
        &mut shard,
        Command::StreamAdd {
            key: owned(b"events"),
            id: StreamAddId::Explicit(2, 0),
            max_len: None,
            nomkstream: false,
            min_id: None,
            delay_ms: None,
            fields: vec![(owned(b"field"), owned(b"b"))],
        },
    );
    execute(
        &mut shard,
        Command::StreamGroupCreate {
            key: owned(b"events"),
            group: owned(b"g"),
            start: crate::stream::GroupStart::Id(0, 0),
            mkstream: false,
        },
    );
    execute(
        &mut shard,
        Command::StreamGroupRead {
            key: owned(b"events"),
            group: owned(b"g"),
            consumer: owned(b"c1"),
            count: None,
            block: None,
            noack: false,
        },
    );
    assert_eq!(
        execute(
            &mut shard,
            Command::StreamClaimPending {
                key: owned(b"events"),
                group: owned(b"g"),
                consumer: owned(b"c2"),
                min_idle: 1,
                ids: vec![(1, 0)],
                options: ClaimOptions::plain(),
            },
        ),
        Response::Exec(Vec::new())
    );
    clock.set(1_500);
    assert_eq!(
        execute(
            &mut shard,
            Command::StreamClaimPending {
                key: owned(b"events"),
                group: owned(b"g"),
                consumer: owned(b"c2"),
                min_idle: 500,
                ids: vec![(1, 0), (1, 0), (9, 0)],
                options: ClaimOptions::plain(),
            },
        ),
        Response::Exec(vec![Response::Exec(vec![
            Response::Bulk(Some(Cow::Owned(b"1-0".to_vec()))),
            Response::Array(vec![
                Some(Cow::Owned(b"field".to_vec())),
                Some(Cow::Owned(b"a".to_vec())),
            ]),
        ])])
    );
    assert_eq!(
        execute(
            &mut shard,
            Command::StreamPending {
                key: owned(b"events"),
                group: owned(b"g"),
            },
        ),
        Response::Exec(vec![
            Response::Integer(2),
            Response::Bulk(Some(Cow::Owned(b"1-0".to_vec()))),
            Response::Bulk(Some(Cow::Owned(b"2-0".to_vec()))),
            Response::Exec(vec![
                Response::Exec(vec![
                    Response::Bulk(Some(Cow::Owned(b"c1".to_vec()))),
                    Response::Integer(1),
                ]),
                Response::Exec(vec![
                    Response::Bulk(Some(Cow::Owned(b"c2".to_vec()))),
                    Response::Integer(1),
                ]),
            ]),
        ])
    );
    execute(
        &mut shard,
        Command::StreamDelete {
            key: owned(b"events"),
            ids: vec![(2, 0)],
        },
    );
    assert_eq!(
        execute(
            &mut shard,
            Command::StreamClaimPending {
                key: owned(b"events"),
                group: owned(b"g"),
                consumer: owned(b"c2"),
                min_idle: 0,
                ids: vec![(2, 0)],
                options: ClaimOptions::plain(),
            },
        ),
        Response::Exec(Vec::new())
    );
    assert_eq!(
        execute(
            &mut shard,
            Command::StreamPending {
                key: owned(b"events"),
                group: owned(b"g"),
            },
        ),
        Response::Exec(vec![
            Response::Integer(1),
            Response::Bulk(Some(Cow::Owned(b"1-0".to_vec()))),
            Response::Bulk(Some(Cow::Owned(b"1-0".to_vec()))),
            Response::Exec(vec![Response::Exec(vec![
                Response::Bulk(Some(Cow::Owned(b"c2".to_vec()))),
                Response::Integer(1),
            ])]),
        ])
    );
}

/// XPENDING can list idle rows. XCLAIM accepts IDLE, TIME, RETRYCOUNT, FORCE, JUSTID, and LASTID.
#[test]
fn xpending_lists_rows_and_xclaim_applies_its_options() {
    let clock = crate::time::TestClock::new(1_000);
    let mut shard = KvShard::with_clock(clock.clone());

    for (id, value) in [((1, 0), b"a" as &[u8]), ((2, 0), b"b")] {
        execute(
            &mut shard,
            Command::StreamAdd {
                key: owned(b"events"),
                id: StreamAddId::Explicit(id.0, id.1),
                max_len: None,
                nomkstream: false,
                min_id: None,
                delay_ms: None,
                fields: vec![(owned(b"field"), owned(value))],
            },
        );
    }

    execute(
        &mut shard,
        Command::StreamGroupCreate {
            key: owned(b"events"),
            group: owned(b"g"),
            start: crate::stream::GroupStart::Id(0, 0),
            mkstream: false,
        },
    );
    execute(
        &mut shard,
        Command::StreamGroupRead {
            key: owned(b"events"),
            group: owned(b"g"),
            consumer: owned(b"c1"),
            count: None,
            block: None,
            noack: false,
        },
    );
    clock.set(1_500);
    assert_eq!(
        execute(
            &mut shard,
            Command::StreamPendingRange {
                key: owned(b"events"),
                group: owned(b"g"),
                min_idle: Some(500),
                start: crate::stream::StreamBound::Min,
                end: crate::stream::StreamBound::Max,
                count: 1,
                consumer: None,
            },
        ),
        Response::Exec(vec![pending_row(b"1-0", b"c1", 500, 1)])
    );
    assert_eq!(
        execute(
            &mut shard,
            Command::StreamPendingRange {
                key: owned(b"events"),
                group: owned(b"g"),
                min_idle: Some(600),
                start: crate::stream::StreamBound::Min,
                end: crate::stream::StreamBound::Max,
                count: 10,
                consumer: Some(owned(b"c1")),
            },
        ),
        Response::Exec(Vec::new())
    );
    assert_eq!(
        execute(
            &mut shard,
            Command::StreamClaimPending {
                key: owned(b"events"),
                group: owned(b"g"),
                consumer: owned(b"c2"),
                min_idle: 500,
                ids: vec![(1, 0)],
                options: ClaimOptions {
                    just_id: true,
                    ..ClaimOptions::plain()
                },
            },
        ),
        Response::Exec(vec![Response::Bulk(Some(Cow::Owned(b"1-0".to_vec())))])
    );
    clock.set(2_000);
    execute(
        &mut shard,
        Command::StreamClaimPending {
            key: owned(b"events"),
            group: owned(b"g"),
            consumer: owned(b"c2"),
            min_idle: 0,
            ids: vec![(1, 0)],
            options: ClaimOptions {
                idle: Some(250),
                retry_count: Some(9),
                ..ClaimOptions::plain()
            },
        },
    );
    assert_eq!(
        execute(
            &mut shard,
            Command::StreamPendingRange {
                key: owned(b"events"),
                group: owned(b"g"),
                min_idle: None,
                start: crate::stream::StreamBound::Id(1, 0),
                end: crate::stream::StreamBound::Id(1, 0),
                count: 10,
                consumer: Some(owned(b"c2")),
            },
        ),
        Response::Exec(vec![pending_row(b"1-0", b"c2", 250, 9)])
    );
    execute(
        &mut shard,
        Command::StreamClaimPending {
            key: owned(b"events"),
            group: owned(b"g"),
            consumer: owned(b"c2"),
            min_idle: 0,
            ids: vec![(1, 0)],
            options: ClaimOptions {
                time: Some(1_000),
                ..ClaimOptions::plain()
            },
        },
    );
    assert_eq!(
        execute(
            &mut shard,
            Command::StreamPendingRange {
                key: owned(b"events"),
                group: owned(b"g"),
                min_idle: None,
                start: crate::stream::StreamBound::Min,
                end: crate::stream::StreamBound::Max,
                count: 10,
                consumer: Some(owned(b"c2")),
            },
        ),
        Response::Exec(vec![pending_row(b"1-0", b"c2", 1_000, 10)])
    );
    execute(
        &mut shard,
        Command::StreamAck {
            key: owned(b"events"),
            group: owned(b"g"),
            ids: vec![(2, 0)],
        },
    );
    assert_eq!(
        execute(
            &mut shard,
            Command::StreamClaimPending {
                key: owned(b"events"),
                group: owned(b"g"),
                consumer: owned(b"c2"),
                min_idle: 1_000_000,
                ids: vec![(2, 0), (9, 0)],
                options: ClaimOptions {
                    force: true,
                    ..ClaimOptions::plain()
                },
            },
        ),
        Response::Exec(vec![Response::Exec(vec![
            Response::Bulk(Some(Cow::Owned(b"2-0".to_vec()))),
            Response::Array(vec![
                Some(Cow::Owned(b"field".to_vec())),
                Some(Cow::Owned(b"b".to_vec())),
            ]),
        ])])
    );
    execute(
        &mut shard,
        Command::StreamClaimPending {
            key: owned(b"events"),
            group: owned(b"g"),
            consumer: owned(b"c2"),
            min_idle: 0,
            ids: vec![(1, 0)],
            options: ClaimOptions {
                last_id: Some((5, 0)),
                ..ClaimOptions::plain()
            },
        },
    );
    assert_eq!(shard.stream_group_cursor(b"events", b"g").unwrap(), (5, 0));
}

/// Pending deliveries are charged to the stream's memory and released when acknowledged
/// or dropped with their consumer.
#[test]
fn pending_deliveries_are_charged_and_released() {
    let mut shard = KvShard::new();
    let mut sink = RecordingSink::default();
    let mut run = |shard: &mut KvShard, parts: &[&[u8]]| {
        execute_with_sink(shard, command_of(parts), &mut sink).unwrap();
    };

    for millis in 1..=100u32 {
        let id = format!("{millis}-0");

        run(&mut shard, &[b"XADD", b"s", id.as_bytes(), b"f", b"v"]);
    }
    run(&mut shard, &[b"XGROUP", b"CREATE", b"s", b"g", b"0"]);

    let base = shard.memory_usage().collection_bytes;

    run(
        &mut shard,
        &[
            b"XREADGROUP",
            b"GROUP",
            b"g",
            b"c",
            b"COUNT",
            b"100",
            b"STREAMS",
            b"s",
            b">",
        ],
    );
    let read = shard.memory_usage().collection_bytes;

    assert_eq!(read - base, 100 * crate::stream::PENDING_ROW_BYTES);

    let ids: Vec<String> = (1..=50).map(|millis| format!("{millis}-0")).collect();
    let mut ack: Vec<&[u8]> = vec![b"XACK", b"s", b"g"];

    ack.extend(ids.iter().map(|id| id.as_bytes()));
    run(&mut shard, &ack);
    assert_eq!(
        read - shard.memory_usage().collection_bytes,
        50 * crate::stream::PENDING_ROW_BYTES
    );

    run(&mut shard, &[b"XGROUP", b"DELCONSUMER", b"s", b"g", b"c"]);
    assert_eq!(shard.memory_usage().collection_bytes, base);
}

/// XAUTOCLAIM looks at no more than ten pending entries per one asked for, as Redis does,
/// and returns where it stopped.
#[test]
fn xautoclaim_scans_at_most_ten_pending_entries_per_count() {
    let mut shard = KvShard::new();
    let mut sink = RecordingSink::default();
    let mut run = |shard: &mut KvShard, parts: &[&[u8]]| {
        execute_with_sink(shard, command_of(parts), &mut sink)
            .unwrap()
            .into_owned()
    };

    for millis in 1..=30u32 {
        let id = format!("{millis}-0");

        run(&mut shard, &[b"XADD", b"s", id.as_bytes(), b"f", b"v"]);
    }
    run(&mut shard, &[b"XGROUP", b"CREATE", b"s", b"g", b"0"]);
    run(
        &mut shard,
        &[b"XREADGROUP", b"GROUP", b"g", b"c", b"STREAMS", b"s", b">"],
    );

    // Nothing is idle long enough: the call stops after ten entries.
    let reply = run(
        &mut shard,
        &[
            b"XAUTOCLAIM",
            b"s",
            b"g",
            b"d",
            b"3600000",
            b"0-0",
            b"COUNT",
            b"1",
        ],
    );
    let Response::Exec(parts) = reply else {
        panic!("unexpected reply {reply:?}");
    };

    assert_eq!(parts[0], Response::Bulk(Some(owned(b"10-0"))));
    assert_eq!(parts[1], Response::Exec(Vec::new()));
}
