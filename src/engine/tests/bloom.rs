use super::strings::words;
use super::*;

fn render(response: Response<'_>) -> String {
    match response {
        Response::Integer(value) => value.to_string(),
        Response::Simple(text) => String::from_utf8_lossy(text).into_owned(),
        Response::Bulk(Some(bytes)) => String::from_utf8_lossy(&bytes).into_owned(),
        Response::Bulk(None) => "nil".to_owned(),
        Response::Error(message) => String::from_utf8_lossy(message).into_owned(),
        Response::Exec(items) => {
            let items: Vec<String> = items.into_iter().map(render).collect();
            format!("[{}]", items.join(","))
        }
        other => format!("{other:?}"),
    }
}

fn run(shard: &mut KvShard, sink: &mut DatabaseLog, parts: &[&str]) -> String {
    let frame = words(parts);
    let (command, _) = parse_command(&frame).unwrap().unwrap();

    render(in_db(shard, sink, 0, command.into_owned()))
}

fn info(shard: &mut KvShard, key: &str, field: &str) -> i64 {
    run(shard, &mut DatabaseLog::default(), &["BF.INFO", key, field])
        .parse()
        .unwrap()
}

#[test]
fn a_default_filter_grows_instead_of_degrading() {
    let mut shard = KvShard::new();
    let mut sink = DatabaseLog::default();

    for index in 0..3_000 {
        run(
            &mut shard,
            &mut sink,
            &["BF.ADD", "seen", &format!("item-{index}")],
        );
    }

    for index in 0..3_000 {
        assert_eq!(
            run(
                &mut shard,
                &mut sink,
                &["BF.EXISTS", "seen", &format!("item-{index}")]
            ),
            "1",
            "no false negatives: item-{index}"
        );
    }

    assert!(info(&mut shard, "seen", "FILTERS") > 1, "the filter scaled");
    assert!(info(&mut shard, "seen", "CAPACITY") >= 3_000);
    let items = info(&mut shard, "seen", "ITEMS");
    assert!((2_900..=3_000).contains(&items), "items {items}");
    assert_eq!(
        run(&mut shard, &mut sink, &["BF.CARD", "seen"]),
        items.to_string()
    );

    // The rate stays near the configured 1% even though 30x the first capacity went in.
    let false_positives = (0..10_000)
        .filter(|index| {
            run(
                &mut shard,
                &mut sink,
                &["BF.EXISTS", "seen", &format!("other-{index}")],
            ) == "1"
        })
        .count();
    assert!(
        false_positives < 300,
        "false positives {false_positives}/10000"
    );

    let mut recovered = replayed(&sink);
    assert_eq!(
        info(&mut recovered, "seen", "FILTERS"),
        info(&mut shard, "seen", "FILTERS")
    );
    assert_eq!(info(&mut recovered, "seen", "ITEMS"), items);

    for index in (0..3_000).step_by(97) {
        assert_eq!(
            run(
                &mut recovered,
                &mut sink,
                &["BF.EXISTS", "seen", &format!("item-{index}")]
            ),
            "1"
        );
    }
}

#[test]
fn a_non_scaling_filter_refuses_items_once_full() {
    let mut shard = KvShard::new();
    let mut sink = DatabaseLog::default();

    assert_eq!(
        run(
            &mut shard,
            &mut sink,
            &["BF.RESERVE", "f", "0.01", "10", "NONSCALING"]
        ),
        "OK"
    );
    assert_eq!(
        run(&mut shard, &mut sink, &["BF.RESERVE", "f", "0.01", "10"]),
        "ERR item exists"
    );

    let mut added = 0;
    let mut refused = 0;

    for index in 0..30 {
        match run(
            &mut shard,
            &mut sink,
            &["BF.ADD", "f", &format!("v{index}")],
        )
        .as_str()
        {
            "1" => added += 1,
            "ERR non scaling filter is full" => refused += 1,
            "0" => {}
            other => panic!("unexpected {other}"),
        }
    }

    assert_eq!(added, 10);
    assert!(refused >= 15, "refused {refused}");
    assert_eq!(info(&mut shard, "f", "FILTERS"), 1);
    assert_eq!(
        run(&mut shard, &mut sink, &["BF.INFO", "f", "EXPANSION"]),
        "nil"
    );

    let mut recovered = replayed(&sink);
    assert_eq!(info(&mut recovered, "f", "ITEMS"), 10);
    assert_eq!(
        run(&mut recovered, &mut sink, &["BF.INFO", "f", "EXPANSION"]),
        "nil"
    );
}

#[test]
fn madd_mexists_and_insert_share_one_lookup_and_one_record() {
    let mut shard = KvShard::new();
    let mut sink = DatabaseLog::default();

    assert_eq!(
        run(&mut shard, &mut sink, &["BF.MADD", "m", "a", "b", "a"]),
        "[1,1,0]"
    );
    assert_eq!(sink.records.len(), 1, "one record for the whole batch");
    assert_eq!(
        run(&mut shard, &mut sink, &["BF.MEXISTS", "m", "a", "z", "b"]),
        "[1,0,1]"
    );
    assert_eq!(
        run(&mut shard, &mut sink, &["BF.MEXISTS", "nokey", "a", "b"]),
        "[0,0]"
    );

    assert_eq!(
        run(
            &mut shard,
            &mut sink,
            &["BF.INSERT", "i", "NOCREATE", "ITEMS", "x"]
        ),
        "ERR not found"
    );
    assert_eq!(shard.value_kind(b"i"), None);
    assert_eq!(
        run(
            &mut shard,
            &mut sink,
            &[
                "BF.INSERT",
                "i",
                "CAPACITY",
                "50",
                "ERROR",
                "0.001",
                "EXPANSION",
                "4",
                "ITEMS",
                "x",
                "y"
            ]
        ),
        "[1,1]"
    );
    assert_eq!(info(&mut shard, "i", "EXPANSION"), 4);
    assert_eq!(info(&mut shard, "i", "CAPACITY"), 50);
    // An existing filter ignores the creation options.
    assert_eq!(
        run(
            &mut shard,
            &mut sink,
            &["BF.INSERT", "i", "CAPACITY", "9999", "ITEMS", "x", "w"]
        ),
        "[0,1]"
    );
    assert_eq!(info(&mut shard, "i", "CAPACITY"), 50);

    assert_eq!(
        run(&mut shard, &mut sink, &["BF.INFO", "m"]),
        "[Capacity,100,Size,".to_owned()
            + &info(&mut shard, "m", "SIZE").to_string()
            + ",Number of filters,1,Number of items inserted,2,Expansion rate,2]"
    );
    assert_eq!(
        run(&mut shard, &mut sink, &["BF.INFO", "missing"]),
        "ERR not found"
    );
    assert_eq!(run(&mut shard, &mut sink, &["BF.CARD", "missing"]), "0");

    run(&mut shard, &mut sink, &["SET", "s", "v"]);
    for command in [
        &["BF.MADD", "s", "a"][..],
        &["BF.MEXISTS", "s", "a"][..],
        &["BF.INSERT", "s", "ITEMS", "a"][..],
        &["BF.INFO", "s"][..],
        &["BF.CARD", "s"][..],
        &["BF.ADD", "s", "a"][..],
    ] {
        assert!(
            run(&mut shard, &mut sink, command).starts_with("WRONGTYPE"),
            "{command:?}"
        );
    }

    for bad in [
        &["BF.RESERVE", "k", "0.01", "10", "EXPANSION"][..],
        &["BF.RESERVE", "k", "0.01", "10", "EXPANSION", "0"][..],
        &[
            "BF.RESERVE",
            "k",
            "0.01",
            "10",
            "NONSCALING",
            "EXPANSION",
            "2",
        ][..],
        &["BF.INSERT", "k", "ITEMS"][..],
        &["BF.INSERT", "k", "x"][..],
        &["BF.INSERT", "k", "CAPACITY", "ITEMS", "x"][..],
        &["BF.INFO", "k", "BOGUS"][..],
        &["BF.MADD", "k"][..],
    ] {
        assert!(parse_command(&words(bad)).is_err(), "{bad:?}");
    }

    let mut recovered = replayed(&sink);
    assert_eq!(info(&mut recovered, "i", "ITEMS"), 3);
    assert_eq!(info(&mut recovered, "i", "EXPANSION"), 4);
    assert_eq!(
        run(
            &mut recovered,
            &mut sink,
            &["BF.MEXISTS", "m", "a", "b", "z"]
        ),
        "[1,1,0]"
    );
}

#[test]
fn an_unrestorable_configuration_is_refused() {
    let mut shard = KvShard::new();
    let mut sink = DatabaseLog::default();

    for config in [
        &["0", "100"][..],
        &["1", "100"][..],
        &["0.01", "0"][..],
        &["0.01", "99999999999"][..],
    ] {
        let mut command = vec!["BF.RESERVE", "k"];
        command.extend_from_slice(config);
        assert_eq!(
            run(&mut shard, &mut sink, &command),
            "ERR invalid Bloom configuration",
            "{config:?}"
        );
    }

    assert_eq!(shard.value_kind(b"k"), None);
}
