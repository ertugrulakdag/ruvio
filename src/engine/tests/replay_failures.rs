//! Replay must fail loudly when a logged write cannot be applied. WAL recovery and replica
//! frames share this path, so a silent skip would let a restarted server or a replica differ
//! from the state that wrote the record.

use super::*;

#[test]
fn a_logged_sorted_set_write_with_no_room_fails_replay_instead_of_being_skipped() {
    let pairs: Vec<(f64, Vec<u8>)> = (0..2_000u32)
        .map(|index| (f64::from(index), format!("{index:0>100}").into_bytes()))
        .collect();
    let mutation = OwnedMutation::SortedSetAdd {
        key: b"z".to_vec(),
        pairs,
    };

    // With room, the record applies.
    let mut roomy = KvShard::new();
    crate::persistence::apply_mutation(&mut roomy, 0, &mutation).unwrap();
    assert!(roomy.value_kind(b"z").is_some());

    // Without room, the record is an error, not a warning and a skipped write.
    let small = crate::arena::limit_chunks_for_test(2);
    let mut cramped = KvShard::new();
    let outcome = crate::persistence::apply_mutation(&mut cramped, 0, &mutation);
    drop(small);

    assert!(
        matches!(outcome, Err(crate::PersistenceError::Corruption(_))),
        "a write replay cannot hold was not reported: {outcome:?}"
    );
}
