use super::*;

#[test]
fn geo_commands_use_one_sorted_set_and_the_redis_sicily_values() {
    let mut shard = KvShard::new();

    assert_eq!(
            reply(
                &mut shard,
                b"*8\r\n$6\r\nGEOADD\r\n$6\r\nSicily\r\n$9\r\n13.361389\r\n$9\r\n38.115556\r\n$7\r\nPalermo\r\n$9\r\n15.087269\r\n$9\r\n37.502669\r\n$7\r\nCatania\r\n"
            ),
            Reply::Integer(2)
        );
    assert_eq!(
        reply(&mut shard, b"*2\r\n$4\r\nTYPE\r\n$6\r\nSicily\r\n"),
        Reply::Simple(b"zset")
    );

    match reply(
        &mut shard,
        b"*5\r\n$7\r\nGEODIST\r\n$6\r\nSicily\r\n$7\r\nPalermo\r\n$7\r\nCatania\r\n$1\r\nm\r\n",
    ) {
        Reply::Bulk(bytes) => {
            let meters: f64 = std::str::from_utf8(&bytes).unwrap().parse().unwrap();

            assert!((meters - 166_274.151_6).abs() < 0.001, "{meters}");
        }

        other => panic!("distance {other:?}"),
    }

    assert_eq!(
        execute(
            &mut shard,
            parse_command(
                b"*4\r\n$7\r\nGEOHASH\r\n$6\r\nSicily\r\n$7\r\nPalermo\r\n$7\r\nCatania\r\n"
            )
            .unwrap()
            .unwrap()
            .0
            .into_owned()
        ),
        Response::Exec(vec![
            Response::Bulk(Some(Cow::Owned(b"sqc8b49rny0".to_vec()))),
            Response::Bulk(Some(Cow::Owned(b"sqdtr74hyu0".to_vec()))),
        ])
    );
    assert_eq!(
            execute(
                &mut shard,
                parse_command(
                    b"*9\r\n$9\r\nGEOSEARCH\r\n$6\r\nSicily\r\n$10\r\nFROMLONLAT\r\n$2\r\n15\r\n$2\r\n37\r\n$8\r\nBYRADIUS\r\n$3\r\n200\r\n$2\r\nkm\r\n$3\r\nASC\r\n"
                )
                .unwrap()
                .unwrap()
                .0
                .into_owned()
            ),
            Response::Exec(vec![
                Response::Bulk(Some(Cow::Owned(b"Catania".to_vec()))),
                Response::Bulk(Some(Cow::Owned(b"Palermo".to_vec()))),
            ])
        );
    assert_eq!(
            reply(
                &mut shard,
                b"*8\r\n$14\r\nGEOSEARCHSTORE\r\n$4\r\ndest\r\n$6\r\nSicily\r\n$10\r\nFROMMEMBER\r\n$7\r\nPalermo\r\n$8\r\nBYRADIUS\r\n$3\r\n100\r\n$2\r\nkm\r\n"
            ),
            Reply::Integer(1)
        );
    assert_eq!(
        reply(&mut shard, b"*2\r\n$5\r\nZCARD\r\n$4\r\ndest\r\n"),
        Reply::Integer(1)
    );
    assert_eq!(
        execute(
            &mut shard,
            parse_command(
                b"*5\r\n$6\r\nGEOADD\r\n$3\r\nbad\r\n$3\r\n200\r\n$2\r\n10\r\n$1\r\na\r\n"
            )
            .unwrap()
            .unwrap()
            .0
            .into_owned()
        ),
        Response::OwnedError(b"ERR invalid longitude,latitude pair 200,10".to_vec())
    );
}
