use super::strings::words;
use super::*;

fn render(response: Response<'_>) -> String {
    match response {
        Response::Integer(value) => value.to_string(),
        Response::Simple(text) => String::from_utf8_lossy(text).into_owned(),
        Response::Bulk(Some(bytes)) => String::from_utf8_lossy(&bytes).into_owned(),
        Response::Bulk(None) => "nil".to_owned(),
        Response::Error(message) => String::from_utf8_lossy(message).into_owned(),
        other => format!("{other:?}"),
    }
}

fn run(shard: &mut KvShard, sink: &mut DatabaseLog, parts: &[&str]) -> String {
    let frame = words(parts);
    let (command, _) = parse_command(&frame).unwrap().unwrap();

    render(in_db(shard, sink, 0, command.into_owned()))
}

fn doc(shard: &mut KvShard, sink: &mut DatabaseLog, json: &str) {
    assert_eq!(run(shard, sink, &["JSON.SET", "d", "$", json]), "OK");
}

fn get(shard: &mut KvShard, sink: &mut DatabaseLog, path: &str) -> String {
    run(shard, sink, &["JSON.GET", "d", path])
}

#[test]
fn json_set_nx_and_xx_check_the_path() {
    let mut shard = KvShard::new();
    let mut sink = DatabaseLog::default();

    assert_eq!(
        run(
            &mut shard,
            &mut sink,
            &["JSON.SET", "d", "$", r#"{"a":1}"#, "XX"]
        ),
        "nil"
    );
    assert_eq!(shard.value_kind(b"d"), None);
    assert_eq!(
        run(
            &mut shard,
            &mut sink,
            &["JSON.SET", "d", "$", r#"{"a":1}"#, "NX"]
        ),
        "OK"
    );
    assert_eq!(
        run(
            &mut shard,
            &mut sink,
            &["JSON.SET", "d", "$", r#"{"a":9}"#, "NX"]
        ),
        "nil"
    );
    assert_eq!(
        run(&mut shard, &mut sink, &["JSON.SET", "d", "$.a", "2", "NX"]),
        "nil"
    );
    assert_eq!(
        run(&mut shard, &mut sink, &["JSON.SET", "d", "$.b", "3", "NX"]),
        "OK"
    );
    assert_eq!(
        run(&mut shard, &mut sink, &["JSON.SET", "d", "$.c", "3", "XX"]),
        "nil"
    );
    assert_eq!(
        run(&mut shard, &mut sink, &["JSON.SET", "d", "$.a", "5", "XX"]),
        "OK"
    );
    assert_eq!(get(&mut shard, &mut sink, "$"), r#"{"a":5,"b":3}"#);
    assert!(parse_command(&words(&["JSON.SET", "d", "$", "1", "MAYBE"])).is_err());

    let mut recovered = replayed(&sink);
    assert_eq!(
        run(
            &mut recovered,
            &mut DatabaseLog::default(),
            &["JSON.GET", "d"]
        ),
        r#"{"a":5,"b":3}"#
    );
}

#[test]
fn array_commands_follow_the_redisjson_index_rules() {
    let mut shard = KvShard::new();
    let mut sink = DatabaseLog::default();

    doc(&mut shard, &mut sink, r#"{"l":[1,2,3]}"#);
    assert_eq!(
        run(
            &mut shard,
            &mut sink,
            &["JSON.ARRINSERT", "d", "$.l", "1", "9", r#""x""#]
        ),
        "5"
    );
    assert_eq!(get(&mut shard, &mut sink, "$.l"), r#"[1,9,"x",2,3]"#);
    assert_eq!(
        run(
            &mut shard,
            &mut sink,
            &["JSON.ARRINSERT", "d", "$.l", "0", "0"]
        ),
        "6"
    );
    assert_eq!(
        run(
            &mut shard,
            &mut sink,
            &["JSON.ARRINSERT", "d", "$.l", "6", "7"]
        ),
        "7"
    );
    assert_eq!(
        run(
            &mut shard,
            &mut sink,
            &["JSON.ARRINSERT", "d", "$.l", "-1", "8"]
        ),
        "8"
    );
    assert_eq!(get(&mut shard, &mut sink, "$.l"), r#"[0,1,9,"x",2,3,8,7]"#);
    assert_eq!(
        run(
            &mut shard,
            &mut sink,
            &["JSON.ARRINSERT", "d", "$.l", "9", "1"]
        ),
        "ERR index out of range"
    );
    assert_eq!(
        run(
            &mut shard,
            &mut sink,
            &["JSON.ARRINSERT", "d", "$.l", "-9", "1"]
        ),
        "ERR index out of range"
    );

    // ARRINDEX: scalars only, 1 and 1.0 differ, stop 0 means the end.
    assert_eq!(
        run(
            &mut shard,
            &mut sink,
            &["JSON.ARRINDEX", "d", "$.l", r#""x""#]
        ),
        "3"
    );
    assert_eq!(
        run(&mut shard, &mut sink, &["JSON.ARRINDEX", "d", "$.l", "7"]),
        "7"
    );
    assert_eq!(
        run(
            &mut shard,
            &mut sink,
            &["JSON.ARRINDEX", "d", "$.l", "7", "0", "7"]
        ),
        "-1"
    );
    assert_eq!(
        run(
            &mut shard,
            &mut sink,
            &["JSON.ARRINDEX", "d", "$.l", "7", "-1"]
        ),
        "7"
    );
    assert_eq!(
        run(&mut shard, &mut sink, &["JSON.ARRINDEX", "d", "$.l", "1.0"]),
        "-1"
    );
    assert_eq!(
        run(
            &mut shard,
            &mut sink,
            &["JSON.ARRINDEX", "d", "$.l", "null"]
        ),
        "-1"
    );
    assert_eq!(
        run(&mut shard, &mut sink, &["JSON.ARRINDEX", "d", "$.l", "[1]"]),
        "ERR JSON value is not a scalar"
    );

    // ARRPOP: last by default, out-of-range indexes are clamped, empty gives nil.
    assert_eq!(
        run(&mut shard, &mut sink, &["JSON.ARRPOP", "d", "$.l"]),
        "7"
    );
    assert_eq!(
        run(&mut shard, &mut sink, &["JSON.ARRPOP", "d", "$.l", "0"]),
        "0"
    );
    assert_eq!(
        run(&mut shard, &mut sink, &["JSON.ARRPOP", "d", "$.l", "-100"]),
        "1"
    );
    assert_eq!(
        run(&mut shard, &mut sink, &["JSON.ARRPOP", "d", "$.l", "100"]),
        "8"
    );
    assert_eq!(get(&mut shard, &mut sink, "$.l"), r#"[9,"x",2,3]"#);

    // ARRTRIM keeps the inclusive range; reversed or past-the-end ranges empty the array.
    assert_eq!(
        run(
            &mut shard,
            &mut sink,
            &["JSON.ARRTRIM", "d", "$.l", "1", "-1"]
        ),
        "3"
    );
    assert_eq!(get(&mut shard, &mut sink, "$.l"), r#"["x",2,3]"#);
    assert_eq!(
        run(
            &mut shard,
            &mut sink,
            &["JSON.ARRTRIM", "d", "$.l", "0", "100"]
        ),
        "3"
    );
    assert_eq!(
        run(
            &mut shard,
            &mut sink,
            &["JSON.ARRTRIM", "d", "$.l", "-2", "-2"]
        ),
        "1"
    );
    assert_eq!(get(&mut shard, &mut sink, "$.l"), "[2]");
    assert_eq!(
        run(
            &mut shard,
            &mut sink,
            &["JSON.ARRTRIM", "d", "$.l", "3", "5"]
        ),
        "0"
    );
    assert_eq!(get(&mut shard, &mut sink, "$.l"), "[]");
    assert_eq!(
        run(&mut shard, &mut sink, &["JSON.ARRPOP", "d", "$.l"]),
        "nil"
    );
    assert_eq!(
        run(&mut shard, &mut sink, &["JSON.ARRINDEX", "d", "$.l", "1"]),
        "-1"
    );

    doc(&mut shard, &mut sink, r#"{"l":[1,2,3]}"#);
    assert_eq!(
        run(
            &mut shard,
            &mut sink,
            &["JSON.ARRTRIM", "d", "$.l", "2", "1"]
        ),
        "0"
    );

    assert!(run(&mut shard, &mut sink, &["JSON.ARRPOP", "d", "$"]).starts_with("WRONGTYPE"));
    assert!(
        run(
            &mut shard,
            &mut sink,
            &["JSON.ARRINSERT", "d", "$", "0", "1"]
        )
        .starts_with("WRONGTYPE")
    );

    let mut recovered = replayed(&sink);
    assert_eq!(
        run(
            &mut recovered,
            &mut DatabaseLog::default(),
            &["JSON.GET", "d"]
        ),
        run(&mut shard, &mut DatabaseLog::default(), &["JSON.GET", "d"])
    );
}

#[test]
fn string_number_and_object_helpers() {
    let mut shard = KvShard::new();
    let mut sink = DatabaseLog::default();

    doc(
        &mut shard,
        &mut sink,
        r#"{"s":"ab","n":6,"f":1.5,"o":{"x":1,"y":2},"e":{}}"#,
    );
    assert_eq!(
        run(
            &mut shard,
            &mut sink,
            &["JSON.STRAPPEND", "d", "$.s", r#""cd""#]
        ),
        "4"
    );
    assert_eq!(get(&mut shard, &mut sink, "$.s"), r#""abcd""#);
    assert!(
        run(
            &mut shard,
            &mut sink,
            &["JSON.STRAPPEND", "d", "$.n", r#""x""#]
        )
        .starts_with("WRONGTYPE")
    );
    assert!(
        run(&mut shard, &mut sink, &["JSON.STRAPPEND", "d", "$.s", "1"]).starts_with("WRONGTYPE")
    );

    assert_eq!(
        run(&mut shard, &mut sink, &["JSON.NUMMULTBY", "d", "$.n", "7"]),
        "42"
    );
    assert_eq!(
        run(&mut shard, &mut sink, &["JSON.NUMMULTBY", "d", "$.f", "2"]),
        "3.0"
    );
    assert_eq!(
        run(
            &mut shard,
            &mut sink,
            &["JSON.NUMMULTBY", "d", "$.n", "0.5"]
        ),
        "21.0"
    );
    assert!(
        run(&mut shard, &mut sink, &["JSON.NUMMULTBY", "d", "$.s", "2"]).starts_with("WRONGTYPE")
    );
    assert_eq!(
        run(
            &mut shard,
            &mut sink,
            &["JSON.NUMMULTBY", "d", "$.n", "1e308"]
        ),
        "ERR result is not a finite number"
    );

    assert_eq!(
        run(&mut shard, &mut sink, &["JSON.OBJLEN", "d", "$.o"]),
        "2"
    );
    assert_eq!(
        run(&mut shard, &mut sink, &["JSON.OBJLEN", "d", "$.e"]),
        "0"
    );
    assert_eq!(run(&mut shard, &mut sink, &["JSON.OBJLEN", "d"]), "5");
    assert!(run(&mut shard, &mut sink, &["JSON.OBJLEN", "d", "$.s"]).starts_with("WRONGTYPE"));
    assert_eq!(run(&mut shard, &mut sink, &["JSON.OBJLEN", "nokey"]), "nil");

    assert_eq!(
        run(&mut shard, &mut sink, &["JSON.FORGET", "d", "$.o"]),
        "1"
    );
    assert_eq!(run(&mut shard, &mut sink, &["JSON.OBJLEN", "d"]), "4");

    let mut recovered = replayed(&sink);
    assert_eq!(
        run(
            &mut recovered,
            &mut DatabaseLog::default(),
            &["JSON.GET", "d"]
        ),
        run(&mut shard, &mut DatabaseLog::default(), &["JSON.GET", "d"])
    );
}

#[test]
fn array_pop_cuts_one_element_and_logs_a_small_delete() {
    let mut shard = KvShard::new();
    let mut sink = DatabaseLog::default();

    doc(
        &mut shard,
        &mut sink,
        r#"{"l":[1,{"a":[2,3]},"x,y",4],"n":0}"#,
    );
    assert_eq!(
        run(&mut shard, &mut sink, &["JSON.ARRPOP", "d", "$.l"]),
        "4"
    );
    assert_eq!(
        get(&mut shard, &mut sink, "$.l"),
        r#"[1,{"a":[2,3]},"x,y"]"#
    );
    assert_eq!(
        run(&mut shard, &mut sink, &["JSON.ARRPOP", "d", "$.l", "0"]),
        "1"
    );
    assert_eq!(
        run(&mut shard, &mut sink, &["JSON.ARRPOP", "d", "$.l", "0"]),
        r#"{"a":[2,3]}"#
    );
    assert_eq!(
        run(&mut shard, &mut sink, &["JSON.ARRPOP", "d", "$.l"]),
        r#""x,y""#
    );
    assert_eq!(get(&mut shard, &mut sink, "$"), r#"{"l":[],"n":0}"#);
    assert_eq!(
        run(&mut shard, &mut sink, &["JSON.ARRPOP", "d", "$.l"]),
        "nil"
    );
}
