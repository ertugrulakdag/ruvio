use super::*;

#[test]
fn spop_removes_the_returned_members() {
    let mut shard = KvShard::new();

    reply(&mut shard, b"*3\r\n$4\r\nSADD\r\n$1\r\ns\r\n$4\r\nonly\r\n");
    assert_eq!(
        reply(&mut shard, b"*2\r\n$4\r\nSPOP\r\n$1\r\ns\r\n"),
        Reply::Bulk(b"only".to_vec())
    );
    assert_eq!(
        reply(&mut shard, b"*2\r\n$5\r\nSCARD\r\n$1\r\ns\r\n"),
        Reply::Integer(0)
    );
    assert_eq!(
        reply(&mut shard, b"*2\r\n$4\r\nSPOP\r\n$1\r\ns\r\n"),
        Reply::Nil
    );
    reply(
        &mut shard,
        b"*4\r\n$4\r\nSADD\r\n$1\r\ns\r\n$1\r\na\r\n$1\r\nb\r\n",
    );
    match reply(&mut shard, b"*3\r\n$4\r\nSPOP\r\n$1\r\ns\r\n$1\r\n2\r\n") {
        Reply::Array(mut members) => {
            members.sort();
            assert_eq!(members, vec![Some(b"a".to_vec()), Some(b"b".to_vec())]);
        }

        other => panic!("unexpected {other:?}"),
    }

    assert_eq!(
        reply(&mut shard, b"*2\r\n$5\r\nSCARD\r\n$1\r\ns\r\n"),
        Reply::Integer(0)
    );
    assert_eq!(
        reply(
            &mut shard,
            b"*3\r\n$4\r\nSPOP\r\n$7\r\nmissing\r\n$1\r\n3\r\n"
        ),
        Reply::Array(Vec::new())
    );
    reply(&mut shard, b"*3\r\n$3\r\nSET\r\n$3\r\nstr\r\n$1\r\nv\r\n");
    assert_eq!(
        reply(&mut shard, b"*2\r\n$4\r\nSPOP\r\n$3\r\nstr\r\n"),
        Reply::Error(b"WRONGTYPE Operation against a key holding the wrong kind of value")
    );
    assert!(parse_command(b"*3\r\n$4\r\nSPOP\r\n$1\r\ns\r\n$2\r\n-1\r\n").is_err());
}

#[test]
fn hincrby_stores_the_canonical_integer() {
    let mut shard = KvShard::new();

    assert_eq!(
        reply(
            &mut shard,
            b"*4\r\n$7\r\nHINCRBY\r\n$1\r\nh\r\n$1\r\nf\r\n$1\r\n2\r\n"
        ),
        Reply::Integer(2)
    );
    assert_eq!(
        reply(
            &mut shard,
            b"*4\r\n$7\r\nHINCRBY\r\n$1\r\nh\r\n$1\r\nf\r\n$1\r\n3\r\n"
        ),
        Reply::Integer(5)
    );
    assert_eq!(
        reply(&mut shard, b"*3\r\n$4\r\nHGET\r\n$1\r\nh\r\n$1\r\nf\r\n"),
        Reply::Bulk(b"5".to_vec())
    );
    reply(
        &mut shard,
        b"*4\r\n$4\r\nHSET\r\n$1\r\nh\r\n$1\r\nf\r\n$1\r\nx\r\n",
    );
    assert_eq!(
        reply(
            &mut shard,
            b"*4\r\n$7\r\nHINCRBY\r\n$1\r\nh\r\n$1\r\nf\r\n$1\r\n1\r\n"
        ),
        Reply::Error(b"ERR value is not an integer or out of range")
    );
    assert_eq!(
        reply(&mut shard, b"*3\r\n$4\r\nHGET\r\n$1\r\nh\r\n$1\r\nf\r\n"),
        Reply::Bulk(b"x".to_vec())
    );
    reply(&mut shard, b"*3\r\n$3\r\nSET\r\n$3\r\nstr\r\n$1\r\nv\r\n");
    assert_eq!(
        reply(
            &mut shard,
            b"*4\r\n$7\r\nHINCRBY\r\n$3\r\nstr\r\n$1\r\nf\r\n$1\r\n1\r\n"
        ),
        Reply::Error(b"WRONGTYPE Operation against a key holding the wrong kind of value")
    );
    assert_eq!(
        reply(
            &mut shard,
            b"*4\r\n$7\r\nHINCRBY\r\n$3\r\nnew\r\n$1\r\nf\r\n$2\r\n-1\r\n"
        ),
        Reply::Integer(-1)
    );
}

#[test]
fn zadd_nx_and_xx_skip_the_other_case() {
    let mut shard = KvShard::new();

    assert_eq!(
        reply(
            &mut shard,
            b"*5\r\n$4\r\nZADD\r\n$1\r\nz\r\n$2\r\nNX\r\n$1\r\n1\r\n$1\r\na\r\n"
        ),
        Reply::Integer(1)
    );
    assert_eq!(
        reply(&mut shard, b"*3\r\n$6\r\nZSCORE\r\n$1\r\nz\r\n$1\r\na\r\n"),
        Reply::Bulk(b"1".to_vec())
    );
    assert_eq!(
        reply(
            &mut shard,
            b"*5\r\n$4\r\nZADD\r\n$1\r\nz\r\n$2\r\nNX\r\n$1\r\n2\r\n$1\r\na\r\n"
        ),
        Reply::Integer(0)
    );
    assert_eq!(
        reply(&mut shard, b"*3\r\n$6\r\nZSCORE\r\n$1\r\nz\r\n$1\r\na\r\n"),
        Reply::Bulk(b"1".to_vec())
    );
    assert_eq!(
        reply(
            &mut shard,
            b"*5\r\n$4\r\nZADD\r\n$1\r\nz\r\n$2\r\nXX\r\n$1\r\n3\r\n$1\r\nb\r\n"
        ),
        Reply::Integer(0)
    );
    assert_eq!(
        reply(&mut shard, b"*3\r\n$6\r\nZSCORE\r\n$1\r\nz\r\n$1\r\nb\r\n"),
        Reply::Nil
    );
    assert_eq!(
        reply(
            &mut shard,
            b"*5\r\n$4\r\nZADD\r\n$1\r\nz\r\n$2\r\nXX\r\n$1\r\n4\r\n$1\r\na\r\n"
        ),
        Reply::Integer(0)
    );
    assert_eq!(
        reply(&mut shard, b"*3\r\n$6\r\nZSCORE\r\n$1\r\nz\r\n$1\r\na\r\n"),
        Reply::Bulk(b"4".to_vec())
    );
    assert_eq!(
        reply(
            &mut shard,
            b"*6\r\n$4\r\nZADD\r\n$1\r\nz\r\n$1\r\n5\r\n$1\r\na\r\n$1\r\n6\r\n$1\r\nc\r\n"
        ),
        Reply::Integer(1)
    );
    assert!(
        parse_command(
            b"*6\r\n$4\r\nZADD\r\n$1\r\nz\r\n$2\r\nNX\r\n$2\r\nXX\r\n$1\r\n1\r\n$1\r\na\r\n"
        )
        .is_err()
    );
}

#[test]
fn lpop_count_zadd_bounds_and_hscan_page_fields() {
    // LPOP RPOP HSCAN
    let mut shard = KvShard::new();

    reply(
        &mut shard,
        b"*5\r\n$5\r\nRPUSH\r\n$1\r\nl\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n",
    );
    assert_eq!(
        reply(&mut shard, b"*3\r\n$4\r\nLPOP\r\n$1\r\nl\r\n$1\r\n2\r\n"),
        Reply::Array(vec![Some(b"a".to_vec()), Some(b"b".to_vec())])
    );
    assert_eq!(
        reply(&mut shard, b"*3\r\n$4\r\nLPOP\r\n$1\r\nl\r\n$1\r\n5\r\n"),
        Reply::Array(vec![Some(b"c".to_vec())])
    );
    assert_eq!(
        reply(&mut shard, b"*3\r\n$4\r\nRPOP\r\n$1\r\nl\r\n$1\r\n1\r\n"),
        Reply::Nil
    );
    assert!(parse_command(b"*3\r\n$4\r\nLPOP\r\n$1\r\nl\r\n$1\r\n0\r\n").is_err());

    reply(
        &mut shard,
        b"*4\r\n$4\r\nZADD\r\n$1\r\nz\r\n$1\r\n1\r\n$1\r\na\r\n",
    );
    assert_eq!(
        reply(
            &mut shard,
            b"*6\r\n$4\r\nZADD\r\n$1\r\nz\r\n$2\r\nCH\r\n$2\r\nGT\r\n$1\r\n2\r\n$1\r\na\r\n"
        ),
        Reply::Integer(1)
    );
    assert_eq!(
        reply(
            &mut shard,
            b"*6\r\n$4\r\nZADD\r\n$1\r\nz\r\n$2\r\nCH\r\n$2\r\nGT\r\n$1\r\n2\r\n$1\r\na\r\n"
        ),
        Reply::Integer(0)
    );
    assert_eq!(
        reply(
            &mut shard,
            b"*5\r\n$4\r\nZADD\r\n$1\r\nz\r\n$2\r\nLT\r\n$1\r\n9\r\n$1\r\na\r\n"
        ),
        Reply::Integer(0)
    );
    assert_eq!(
        reply(&mut shard, b"*3\r\n$6\r\nZSCORE\r\n$1\r\nz\r\n$1\r\na\r\n"),
        Reply::Bulk(b"2".to_vec())
    );
    assert_eq!(
        reply(
            &mut shard,
            b"*6\r\n$4\r\nZADD\r\n$1\r\nz\r\n$4\r\nINCR\r\n$2\r\nNX\r\n$1\r\n1\r\n$1\r\na\r\n"
        ),
        Reply::Nil
    );
    assert_eq!(
        reply(
            &mut shard,
            b"*5\r\n$4\r\nZADD\r\n$1\r\nz\r\n$4\r\nINCR\r\n$1\r\n3\r\n$1\r\nb\r\n"
        ),
        Reply::Bulk(b"3".to_vec())
    );
    assert!(
        parse_command(
            b"*6\r\n$4\r\nZADD\r\n$1\r\nz\r\n$2\r\nNX\r\n$2\r\nGT\r\n$1\r\n1\r\n$1\r\nc\r\n"
        )
        .is_err()
    );

    reply(
        &mut shard,
        b"*6\r\n$4\r\nHSET\r\n$1\r\nh\r\n$1\r\na\r\n$1\r\n1\r\n$1\r\nb\r\n$1\r\n2\r\n",
    );
    let (command, _) =
        parse_command(b"*5\r\n$5\r\nHSCAN\r\n$1\r\nh\r\n$1\r\n0\r\n$5\r\nCOUNT\r\n$1\r\n1\r\n")
            .unwrap()
            .expect("frame");
    let Response::Scan { cursor, keys } = execute(&mut shard, command.into_owned()) else {
        panic!("expected a scan");
    };

    // A compact hash comes back whole, as in Redis, whatever COUNT says.
    assert_eq!(cursor, 0);
    assert_eq!(
        keys.iter().map(|key| key.as_ref()).collect::<Vec<_>>(),
        [b"a".as_slice(), b"1", b"b", b"2"]
    );
}

#[test]
fn sscan_and_zscan_page_through_members() {
    let mut shard = KvShard::new();

    reply(
        &mut shard,
        b"*5\r\n$4\r\nSADD\r\n$1\r\ns\r\n$1\r\nb\r\n$1\r\na\r\n$1\r\nc\r\n",
    );
    reply(
        &mut shard,
        b"*6\r\n$4\r\nZADD\r\n$1\r\nz\r\n$1\r\n2\r\n$1\r\nb\r\n$1\r\n1\r\n$1\r\na\r\n",
    );

    let scan = |shard: &mut KvShard, frame: &[u8]| {
        let (command, _) = parse_command(frame).unwrap().expect("frame");
        let Response::Scan { cursor, keys } = execute(shard, command.into_owned()) else {
            panic!("expected a scan");
        };

        (
            cursor,
            keys.iter().map(|key| key.to_vec()).collect::<Vec<_>>(),
        )
    };

    let (cursor, keys) = scan(
        &mut shard,
        b"*5\r\n$5\r\nSSCAN\r\n$1\r\ns\r\n$1\r\n0\r\n$5\r\nCOUNT\r\n$1\r\n2\r\n",
    );
    // A small set comes back whole, as in Redis, whatever COUNT says.
    assert_eq!(
        (cursor, keys),
        (0, vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()])
    );

    let (cursor, keys) = scan(&mut shard, b"*3\r\n$5\r\nZSCAN\r\n$1\r\nz\r\n$1\r\n0\r\n");
    assert_eq!(
        (cursor, keys),
        (
            0,
            vec![b"a".to_vec(), b"1".to_vec(), b"b".to_vec(), b"2".to_vec()]
        )
    );
    assert_eq!(
        scan(
            &mut shard,
            b"*3\r\n$5\r\nSSCAN\r\n$7\r\nmissing\r\n$1\r\n0\r\n"
        ),
        (0, Vec::new())
    );
}

fn wire(shard: &mut KvShard, frame: &[u8]) -> Vec<u8> {
    let (command, consumed) = parse_command(frame).unwrap().expect("complete frame");
    let mut output = Vec::new();

    assert_eq!(consumed, frame.len());
    write_response(&mut output, execute(shard, command.into_owned())).unwrap();
    output
}

#[test]
fn command_replies_come_from_the_command_table() {
    let mut shard = KvShard::new();

    assert_eq!(
        reply(&mut shard, b"*2\r\n$7\r\nCOMMAND\r\n$5\r\nCOUNT\r\n"),
        Reply::Integer(crate::protocol::all_commands().len() as i64)
    );
    assert_eq!(
        wire(
            &mut shard,
            b"*4\r\n$7\r\nCOMMAND\r\n$4\r\nINFO\r\n$3\r\nGeT\r\n$4\r\nnope\r\n"
        ),
        b"*2\r\n*10\r\n$3\r\nget\r\n:2\r\n*2\r\n+readonly\r\n+fast\r\n:1\r\n:1\r\n:1\r\n\
          *3\r\n+@read\r\n+@string\r\n+@fast\r\n*0\r\n*0\r\n*0\r\n$-1\r\n"
            .to_vec()
    );

    let list = wire(&mut shard, b"*1\r\n$7\r\nCOMMAND\r\n");
    let header = format!("*{}\r\n*10\r\n", crate::protocol::all_commands().len());

    assert!(list.starts_with(header.as_bytes()));
    assert!(
        wire(
            &mut shard,
            b"*3\r\n$7\r\nCOMMAND\r\n$4\r\nDOCS\r\n$3\r\nget\r\n"
        )
        .len()
            > 4
    );
    assert!(parse_command(b"*2\r\n$7\r\nCOMMAND\r\n$3\r\nFOO\r\n").is_err());
}

#[test]
fn randomkey_gives_up_after_bounded_tries_on_an_expired_keyspace() {
    let clock = crate::time::TestClock::new(1_000);
    let mut shard = KvShard::with_clock(clock.clone());

    for id in 0..200_000 {
        let key = format!("gone:{id}");

        shard.set_bytes(key.as_bytes(), b"v");
        assert!(shard.expire_at(key.as_bytes(), crate::time::UnixMillis::new(2_000)));
    }

    clock.set(3_000);

    // Every key is past its deadline and not reaped yet: a bounded look finds none.
    assert_eq!(
        execute(&mut shard, Command::RandomKey),
        Response::Bulk(None)
    );

    let mut small = KvShard::new();

    small.set_bytes(b"only", b"v");
    assert_eq!(
        execute(&mut small, Command::RandomKey),
        Response::Bulk(Some(Cow::Borrowed(&b"only"[..])))
    );
}

#[test]
fn walked_entries_encode_the_same_bytes_as_their_copies() {
    let mut shard = KvShard::new();
    let run = |shard: &mut KvShard, parts: &[&[u8]]| {
        let reply = execute(shard, command_of(parts));

        assert!(!matches!(reply, Response::Error(_)), "{parts:?}: {reply:?}");
    };

    run(&mut shard, &[b"SET", b"small", b"value"]);
    run(&mut shard, &[b"SET", b"counter", b"42"]);
    run(&mut shard, &[b"INCR", b"counter"]);
    run(&mut shard, &[b"SET", b"large", &[b'x'; 70_000]]);
    run(&mut shard, &[b"SET", b"expiring", b"v", b"PX", b"900000"]);
    run(&mut shard, &[b"SADD", b"small-set", b"a", b"b"]);
    run(&mut shard, &[b"SADD", b"int-set", b"1", b"2", b"300"]);
    run(
        &mut shard,
        &[b"ZADD", b"small-zset", b"1.5", b"a", b"-2", b"b"],
    );
    run(&mut shard, &[b"RPUSH", b"small-list", b"a", b"b"]);
    run(&mut shard, &[b"HSET", b"small-hash", b"f", b"v"]);
    for index in 0..3_000u32 {
        let member = format!("member-{index:05}");
        let score = format!("{}", f64::from(index % 97) * 0.5);

        run(&mut shard, &[b"SADD", b"set", member.as_bytes()]);
        run(
            &mut shard,
            &[b"ZADD", b"zset", score.as_bytes(), member.as_bytes()],
        );
        run(&mut shard, &[b"RPUSH", b"list", member.as_bytes()]);
        run(
            &mut shard,
            &[b"HSET", b"hash", member.as_bytes(), &[b'v'; 40]],
        );
    }
    run(
        &mut shard,
        &[b"JSON.SET", b"doc", b"$", br#"{"a":[1,2,{"b":null}]}"#],
    );
    run(&mut shard, &[b"SETV", b"versioned", b"body"]);
    run(&mut shard, &[b"LEASE", b"lease", b"900000"]);
    run(&mut shard, &[b"LIMIT", b"limit", b"5", b"900000"]);
    run(&mut shard, &[b"ONCE", b"once", b"900000"]);
    run(&mut shard, &[b"SEMAPHORE", b"semaphore", b"2", b"900000"]);
    run(&mut shard, &[b"BF.ADD", b"bloom", b"item"]);
    run(&mut shard, &[b"XADD", b"stream", b"*", b"f", b"v"]);
    run(
        &mut shard,
        &[b"HSET", b"timed-hash", b"f", b"v", b"g", b"w"],
    );
    run(
        &mut shard,
        &[b"HPEXPIRE", b"timed-hash", b"900000", b"FIELDS", b"1", b"f"],
    );

    shard.begin_snapshot(1 << 24).unwrap();
    let mut compared = 0;

    loop {
        let step = shard.snapshot_step(usize::MAX / 20, &mut |entry| {
            let mut direct = vec![0xAA];
            let mut copied = vec![0xAA];

            entry.encode_into(&mut direct).unwrap();
            crate::persistence::encode_entry(&mut copied, &entry.to_entry()).unwrap();
            assert_eq!(direct, copied, "{:?}", entry.to_entry().key);
            compared += 1;
        });

        if matches!(step, crate::store::SnapshotStep::Done) {
            break;
        }
    }

    let _ = shard.finish_snapshot_journal();
    assert_eq!(compared, shard.len());
}

/// One command that creates a key `k` of `kind`. Every round-trip test below builds its
/// values from this table, so a new kind fails to compile here until it has a sample and
/// then runs through each of them.
fn sample_command(kind: crate::store::ValueKind) -> &'static [&'static [u8]] {
    use crate::store::ValueKind;

    match kind {
        ValueKind::Bytes => &[b"SET", b"k", b"v"],
        ValueKind::Json => &[b"JSON.SET", b"k", b"$", b"{\"a\":1}"],
        ValueKind::List => &[b"RPUSH", b"k", b"a"],
        ValueKind::Set => &[b"SADD", b"k", b"a"],
        ValueKind::Hash => &[b"HSET", b"k", b"f", b"v"],
        ValueKind::SortedSet => &[b"ZADD", b"k", b"1", b"m"],
        ValueKind::BloomFilter => &[b"BF.ADD", b"k", b"x"],
        ValueKind::Stream => &[b"XADD", b"k", b"1-1", b"f", b"v"],
        ValueKind::Lease => &[b"LEASE", b"k", b"100000"],
        ValueKind::Versioned => &[b"SETV", b"k", b"v"],
        ValueKind::Limit => &[b"LIMIT", b"k", b"1", b"100000"],
        ValueKind::Once => &[b"ONCE", b"k", b"100000"],
        ValueKind::Semaphore => &[b"SEMAPHORE", b"k", b"2", b"100000"],
    }
}

/// Every kind answers `TYPE` with its name, and `SCAN ... TYPE <that name>` finds it. A
/// new kind fails to compile here until it has a sample command.
#[test]
fn scan_type_finds_every_kind_by_the_name_type_reports() {
    use crate::store::ValueKind;

    for kind in ValueKind::ALL {
        let sample = sample_command(kind);
        let mut shard = KvShard::new();
        let name = kind.type_name();

        execute(&mut shard, command_of(sample));
        assert_eq!(
            execute_owned(execute(&mut shard, command_of(&[b"TYPE", b"k"]))),
            Response::Simple(name),
            "{kind:?}"
        );
        match execute(&mut shard, command_of(&[b"SCAN", b"0", b"TYPE", name])) {
            Response::Scan { keys, .. } => {
                assert_eq!(
                    keys.len(),
                    1,
                    "SCAN TYPE {} for {kind:?}",
                    String::from_utf8_lossy(name)
                )
            }
            other => panic!(
                "SCAN TYPE {} for {kind:?}: {other:?}",
                String::from_utf8_lossy(name)
            ),
        }
    }
}

/// Whether `DUMP` carries `kind`; a kind it does not carry must say so, not write a payload
/// `RESTORE` would misread.
fn dump_carries(kind: crate::store::ValueKind) -> bool {
    use crate::store::ValueKind;

    match kind {
        ValueKind::Bytes
        | ValueKind::Json
        | ValueKind::List
        | ValueKind::Set
        | ValueKind::Hash
        | ValueKind::SortedSet => true,
        ValueKind::BloomFilter
        | ValueKind::Stream
        | ValueKind::Lease
        | ValueKind::Versioned
        | ValueKind::Limit
        | ValueKind::Once
        | ValueKind::Semaphore => false,
    }
}

/// Every key of database 0 with its stored value and deadline.
fn stored(shard: &mut KvShard) -> Vec<(Vec<u8>, Option<crate::store::Entry>, Option<UnixMillis>)> {
    let mut keys: Vec<Vec<u8>> = shard
        .snapshot_entries()
        .into_iter()
        .filter(|entry| entry.database == 0)
        .map(|entry| entry.key)
        .collect();

    keys.sort();
    keys.into_iter()
        .map(|key| {
            let entry = shard.clone_entry(&key);
            let deadline = shard.deadline(0, &key);

            (key, entry, deadline)
        })
        .collect()
}

/// A value of every kind comes back unchanged through each path that stores or moves it: the
/// write-ahead log replayed into an empty shard, a checkpoint file written and read back,
/// and `DUMP` then `RESTORE`. The values come from `sample_command`, so a new kind is
/// covered by all three the moment it has a sample.
#[test]
fn every_value_kind_survives_log_replay_checkpoint_and_dump() {
    use crate::persistence::{CheckpointManager, PersistenceConfig, ShardCheckpoint};
    use crate::store::ValueKind;

    for kind in ValueKind::ALL {
        let mut primary = KvShard::new();
        let mut log = DatabaseLog::default();

        in_db(&mut primary, &mut log, 0, command_of(sample_command(kind)));
        assert!(
            !log.records.is_empty(),
            "{kind:?}: the sample logged nothing"
        );

        let expected = stored(&mut primary);

        assert_eq!(expected.len(), 1, "{kind:?}: the sample makes one key");

        let mut replica = replayed(&log);

        assert_eq!(stored(&mut replica), expected, "{kind:?}: log replay");

        let directory = tempfile::tempdir().unwrap();
        let manager = CheckpointManager::new(
            PersistenceConfig {
                data_dir: directory.path().to_path_buf(),
                snapshot_max_bytes: 1 << 20,
                ..PersistenceConfig::default()
            },
            crate::protocol::ProtocolLimits::default(),
        );

        manager
            .publish(
                1,
                &[ShardCheckpoint {
                    shard_id: 0,
                    last_sequence: 1,
                    entries: primary.snapshot_entries(),
                }],
            )
            .unwrap();

        let mut loaded = KvShard::new();
        let checkpoint = manager.load().unwrap().expect("a published checkpoint");

        for shard_file in checkpoint.shards {
            for entry in shard_file.entries {
                loaded.restore_snapshot_entry(entry).unwrap();
            }
        }
        assert_eq!(stored(&mut loaded), expected, "{kind:?}: checkpoint file");

        let dumped = execute_owned(execute(&mut primary, command_of(&[b"DUMP", b"k"])));

        match dumped {
            Response::Bulk(Some(payload)) if dump_carries(kind) => {
                let mut restored = KvShard::new();
                let reply = execute_owned(execute(
                    &mut restored,
                    command_of(&[b"RESTORE", b"k", b"0", &payload]),
                ));

                assert_eq!(reply, Response::Simple(b"OK"), "{kind:?}: RESTORE");
                assert_eq!(stored(&mut restored), expected, "{kind:?}: DUMP/RESTORE");
            }
            Response::Error(_) if !dump_carries(kind) => {}
            other => panic!(
                "{kind:?}: DUMP answered {other:?}, carries = {}",
                dump_carries(kind)
            ),
        }
    }
}
