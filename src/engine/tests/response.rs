use super::*;

#[test]
fn write_response_encodes_exec_and_watch_abort() {
    let mut output = Vec::new();

    write_response(
        &mut output,
        Response::Exec(vec![Response::Simple(b"OK"), Response::Integer(1)]),
    )
    .unwrap();
    assert_eq!(output, b"*2\r\n+OK\r\n:1\r\n");

    let mut output = Vec::new();

    write_response(&mut output, Response::NullArray).unwrap();
    assert_eq!(output, b"*-1\r\n");
}

#[test]
fn resp3_encoding_keeps_resp2_bytes_for_the_same_values() {
    let map = Response::Map(vec![(
        Cow::Borrowed(b"f"),
        Response::Bulk(Some(Cow::Borrowed(b"v"))),
    )]);
    let set = Response::Set(vec![Cow::Borrowed(b"m")]);
    let mut v2 = Vec::new();

    write_response_in(&mut v2, Response::Bulk(None), RespVersion::V2).unwrap();
    write_response_in(&mut v2, map.clone(), RespVersion::V2).unwrap();
    write_response_in(&mut v2, set.clone(), RespVersion::V2).unwrap();
    write_response_in(&mut v2, Response::Double(1.5), RespVersion::V2).unwrap();
    write_response_in(&mut v2, Response::Bool(true), RespVersion::V2).unwrap();
    write_response_in(&mut v2, Response::Bool(false), RespVersion::V2).unwrap();
    write_response_in(&mut v2, Response::NullArray, RespVersion::V2).unwrap();
    assert_eq!(
        v2,
        b"$-1\r\n*2\r\n$1\r\nf\r\n$1\r\nv\r\n*1\r\n$1\r\nm\r\n$3\r\n1.5\r\n:1\r\n:0\r\n*-1\r\n"
    );

    let mut v3 = Vec::new();

    write_response_in(&mut v3, Response::Bulk(None), RespVersion::V3).unwrap();
    write_response_in(&mut v3, map, RespVersion::V3).unwrap();
    write_response_in(&mut v3, set, RespVersion::V3).unwrap();
    write_response_in(&mut v3, Response::Double(1.5), RespVersion::V3).unwrap();
    write_response_in(&mut v3, Response::Bool(true), RespVersion::V3).unwrap();
    write_response_in(&mut v3, Response::Bool(false), RespVersion::V3).unwrap();
    write_response_in(&mut v3, Response::NullArray, RespVersion::V3).unwrap();
    assert_eq!(
        v3,
        b"_\r\n%1\r\n$1\r\nf\r\n$1\r\nv\r\n~1\r\n$1\r\nm\r\n,1.5\r\n#t\r\n#f\r\n_\r\n"
    );
}
