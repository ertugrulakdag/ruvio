//! Redis set, sorted-set and hash commands that rebuild from existing store reads.

use std::sync::atomic::{AtomicU64, Ordering};

use super::string_ext::float_text;
use super::*;
use crate::protocol::{LexBound, ScoreBound, SortedAggregate};

static DRAW: AtomicU64 = AtomicU64::new(0x9e37_79b9_7f4a_7c15);

/// Cheap pseudo-random draw below `bound` (xorshift over a time-seeded counter).
pub(super) fn draw(bound: usize) -> usize {
    let mut state = DRAW.load(Ordering::Relaxed);

    if state == 0x9e37_79b9_7f4a_7c15 {
        state ^= std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(1, |elapsed| elapsed.as_nanos() as u64)
            | 1;
    }

    state ^= state << 13;
    state ^= state >> 7;
    state ^= state << 17;
    DRAW.store(state, Ordering::Relaxed);
    (state % bound.max(1) as u64) as usize
}

pub(super) fn set_mismember<C: Clock, I: StorageIndex>(
    shard: &KvShard<C, I>,
    key: &[u8],
    members: &[crate::protocol::Bytes<'_>],
) -> Response<'static> {
    let mut answers = Vec::with_capacity(members.len());

    for member in members {
        match shard.set_contains(key, member) {
            Ok(found) => answers.push(Response::Integer(i64::from(found))),
            Err(_) => return wrong_type(),
        }
    }

    Response::Exec(answers)
}

pub(super) fn set_inter_card<C: Clock, I: StorageIndex>(
    shard: &KvShard<C, I>,
    keys: &[crate::protocol::Bytes<'_>],
    limit: u64,
) -> Response<'static> {
    let mut sets = Vec::with_capacity(keys.len());

    for key in keys {
        match shard.live_set(key) {
            Ok(Some(set)) => sets.push(set),
            Ok(None) => {
                // A missing key empties the intersection, but a wrong type elsewhere still errors.
                for other in keys {
                    if shard.live_set(other).is_err() {
                        return wrong_type();
                    }
                }

                return Response::Integer(0);
            }
            Err(_) => return wrong_type(),
        }
    }

    let Some(smallest) = sets.iter().min_by_key(|set| set.len()) else {
        return Response::Integer(0);
    };
    let mut count = 0u64;
    let _ = smallest.visit::<()>(|member| {
        if sets.iter().all(|set| set.contains(member)) {
            count += 1;
        }

        if limit != 0 && count >= limit {
            Err(())
        } else {
            Ok(())
        }
    });

    Response::Integer(count as i64)
}

pub(super) fn sorted_count<C: Clock, I: StorageIndex>(
    shard: &KvShard<C, I>,
    key: &[u8],
    min: ScoreBound,
    max: ScoreBound,
) -> Response<'static> {
    let mut count = 0i64;

    match shard.sorted_set_visit_scores(key, min, max, |_, _| {
        count += 1;
        true
    }) {
        Ok(()) => Response::Integer(count),
        Err(_) => wrong_type(),
    }
}

pub(super) fn sorted_lex_count<C: Clock, I: StorageIndex>(
    shard: &KvShard<C, I>,
    key: &[u8],
    min: &LexBound<'_>,
    max: &LexBound<'_>,
) -> Response<'static> {
    match shard.sorted_set_range_by_lex(key, min, max) {
        Ok(entries) => Response::Integer(entries.len() as i64),
        Err(_) => wrong_type(),
    }
}

pub(super) fn sorted_mscore<C: Clock, I: StorageIndex>(
    shard: &KvShard<C, I>,
    key: &[u8],
    members: &[crate::protocol::Bytes<'_>],
) -> Response<'static> {
    let mut scores = Vec::with_capacity(members.len());

    for member in members {
        match shard.sorted_set_score(key, member) {
            Ok(Some(score)) => scores.push(Response::Double(score)),
            Ok(None) => scores.push(Response::Bulk(None)),
            Err(_) => return wrong_type(),
        }
    }

    Response::Exec(scores)
}

/// Entries of a sorted set, or of a plain set at score 1 (Redis accepts both as inputs).
fn algebra_input<C: Clock, I: StorageIndex>(
    shard: &KvShard<C, I>,
    key: &[u8],
) -> Result<Vec<(Vec<u8>, f64)>, ()> {
    if let Ok(entries) = shard.sorted_set_range_entries(key, 0, -1, false) {
        return Ok(entries
            .into_iter()
            .map(|(member, score)| (member.to_vec(), score))
            .collect());
    }

    match shard.live_set(key) {
        Ok(Some(set)) => {
            let mut members = Vec::with_capacity(set.len());
            let _ = set.visit::<()>(|member| {
                members.push((member.to_vec(), 1.0));
                Ok(())
            });

            Ok(members)
        }
        Ok(None) => Ok(Vec::new()),
        Err(_) => Err(()),
    }
}

fn combine(aggregate: SortedAggregate, left: f64, right: f64) -> f64 {
    let merged = match aggregate {
        SortedAggregate::Sum => left + right,
        SortedAggregate::Min => left.min(right),
        SortedAggregate::Max => left.max(right),
    };

    // Redis: infinities of opposite sign cancel to 0 rather than NaN.
    if merged.is_nan() { 0.0 } else { merged }
}

#[allow(clippy::too_many_arguments)]
pub(super) fn sorted_algebra<'a, C: Clock, I: StorageIndex, S: MutationSink>(
    shard: &mut KvShard<C, I>,
    sink: &mut S,
    op: SetOp,
    keys: &[crate::protocol::Bytes<'a>],
    weights: &[f64],
    aggregate: SortedAggregate,
    with_scores: bool,
    destination: Option<&crate::protocol::Bytes<'a>>,
) -> Result<Response<'static>, S::Error> {
    let mut inputs = Vec::with_capacity(keys.len());

    for (index, key) in keys.iter().enumerate() {
        let Ok(mut entries) = algebra_input(shard, key) else {
            return Ok(wrong_type());
        };
        let weight = weights.get(index).copied().unwrap_or(1.0);

        if weight != 1.0 {
            for entry in &mut entries {
                // 0 * inf is NaN in IEEE; Redis defines it as 0.
                entry.1 = if weight == 0.0 { 0.0 } else { entry.1 * weight };
            }
        }

        inputs.push(entries);
    }

    let mut merged: ahash::AHashMap<Vec<u8>, f64> = ahash::AHashMap::new();

    match op {
        SetOp::Union => {
            for entries in inputs {
                for (member, score) in entries {
                    merged
                        .entry(member)
                        .and_modify(|current| *current = combine(aggregate, *current, score))
                        .or_insert(score);
                }
            }
        }
        SetOp::Inter => {
            let mut rest = inputs.into_iter();

            if let Some(first) = rest.next() {
                merged.extend(first);

                for entries in rest {
                    let other: ahash::AHashMap<Vec<u8>, f64> = entries.into_iter().collect();

                    merged = merged
                        .into_iter()
                        .filter_map(|(member, score)| {
                            other
                                .get(&member)
                                .map(|right| (member, combine(aggregate, score, *right)))
                        })
                        .collect();
                }
            }
        }
        SetOp::Diff => {
            let mut rest = inputs.into_iter();

            if let Some(first) = rest.next() {
                merged.extend(first);

                for entries in rest {
                    for (member, _) in entries {
                        merged.remove(&member);
                    }
                }
            }
        }
    }

    let mut ordered: Vec<(Vec<u8>, f64)> = merged.into_iter().collect();

    ordered.sort_by(|left, right| {
        left.1
            .partial_cmp(&right.1)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| left.0.cmp(&right.0))
    });

    let Some(destination) = destination else {
        let mut values = Vec::with_capacity(ordered.len() * (1 + usize::from(with_scores)));

        for (member, score) in ordered {
            values.push(Some(Cow::Owned(member)));

            if with_scores {
                values.push(Some(Cow::Owned(format_score(score))));
            }
        }

        return Ok(Response::Array(values));
    };

    let stored = ordered.len();
    let extra = ordered
        .iter()
        .map(|(member, _)| member.len() + 16)
        .sum::<usize>()
        + destination.len()
        + ADMISSION_OVERHEAD_BYTES;

    if let Some(rejected) = before_mutation(shard, sink, [destination.as_ref()], extra)? {
        return Ok(rejected);
    }

    if !shard.sorted_set_make_room(
        destination,
        true,
        ordered.iter().map(|(member, _)| member.as_slice()),
    ) {
        return Ok(sorted_set_error(SortedSetError::Full));
    }

    if shard.contains_key(destination) {
        sink.record(Mutation::Delete {
            keys: std::slice::from_ref(destination),
        })?;
        shard.delete(destination);
    }

    if !ordered.is_empty() {
        let pairs: Vec<(f64, crate::protocol::Bytes<'_>)> = ordered
            .iter()
            .map(|(member, score)| (*score, Cow::Borrowed(member.as_slice())))
            .collect();

        sink.record(Mutation::SortedSetAdd {
            key: destination,
            pairs: &pairs,
        })?;
        if let Err(error) = shard.sorted_set_add_pairs(destination, &pairs) {
            debug_assert!(false, "sorted-set store failed after validation");
            return Ok(sorted_set_error(error));
        }
    }

    Ok(Response::Integer(stored as i64))
}

pub(super) fn sorted_rand_member<C: Clock, I: StorageIndex>(
    shard: &KvShard<C, I>,
    key: &[u8],
    count: Option<i64>,
    with_scores: bool,
) -> Response<'static> {
    let entries = match shard.sorted_set_random_entries(key, count) {
        Ok(entries) => entries,
        Err(_) => return wrong_type(),
    };

    if count.is_none() {
        return Response::Bulk(
            entries
                .into_iter()
                .next()
                .map(|(member, _)| Cow::Owned(member)),
        );
    }

    let mut values = Vec::with_capacity(entries.len() * 2);

    for (member, score) in entries {
        values.push(Some(Cow::Owned(member)));

        if with_scores {
            values.push(Some(Cow::Owned(format_score(score))));
        }
    }

    Response::Array(values)
}

pub(super) fn hash_rand_field<C: Clock, I: StorageIndex>(
    shard: &mut KvShard<C, I>,
    key: &[u8],
    count: Option<i64>,
    with_values: bool,
) -> Response<'static> {
    shard.expire_hash_fields(key);

    let entries = match shard.hash_random_pairs(key, count) {
        Ok(entries) => entries,
        Err(_) => return wrong_type(),
    };

    if count.is_none() {
        return Response::Bulk(
            entries
                .into_iter()
                .next()
                .map(|(field, _)| Cow::Owned(field)),
        );
    }

    let mut values = Vec::with_capacity(entries.len() * 2);

    for (field, value) in entries {
        values.push(Some(Cow::Owned(field)));

        if with_values {
            values.push(Some(Cow::Owned(value)));
        }
    }

    Response::Array(values)
}

pub(super) fn hash_increment_float<'a, C: Clock, I: StorageIndex, S: MutationSink>(
    shard: &'a mut KvShard<C, I>,
    sink: &mut S,
    key: crate::protocol::Bytes<'a>,
    field: crate::protocol::Bytes<'a>,
    delta: f64,
) -> Result<Response<'a>, S::Error> {
    shard.expire_hash_fields(&key);

    let base = match shard.hash_get(&key, &field) {
        Ok(None) => 0.0,
        Ok(Some(bytes)) => match std::str::from_utf8(bytes)
            .ok()
            .and_then(|text| text.parse::<f64>().ok())
            .filter(|value| value.is_finite())
        {
            Some(value) => value,
            None => return Ok(Response::Error(b"ERR hash value is not a float")),
        },
        Err(_) => return Ok(wrong_type()),
    };
    let sum = base + delta;

    if !sum.is_finite() {
        return Ok(Response::Error(b"ERR value is NaN or Infinity"));
    }

    let text = float_text(sum).into_bytes();

    if let Some(rejected) = before_mutation(
        shard,
        sink,
        [key.as_ref()],
        key.len() + field.len() + text.len() + ADMISSION_OVERHEAD_BYTES,
    )? {
        return Ok(rejected);
    }

    let pairs = [(field, Cow::Owned(text.clone()))];
    let kept = if shard.has_field_ttls() {
        shard
            .field_deadline(&key, &pairs[0].0)
            .expect("hash type was validated")
    } else {
        None
    };

    match kept {
        // The kept field TTL goes out with the value in one record: a torn WAL tail
        // or a replica never sees the field without it.
        Some(at) => sink.record(Mutation::Transaction {
            mutations: &[
                Mutation::HashSet {
                    key: &key,
                    pairs: &pairs,
                }
                .to_owned(),
                Mutation::HashFieldExpire {
                    key: &key,
                    fields: std::slice::from_ref(&pairs[0].0),
                    unix_millis: at,
                }
                .to_owned(),
            ],
        })?,
        None => sink.record(Mutation::HashSet {
            key: &key,
            pairs: &pairs,
        })?,
    }

    match kept {
        Some(_) => {
            shard
                .hash_set_keep_ttl(&key, &pairs[0].0, &pairs[0].1)
                .expect("hash type was validated before recording");
        }
        None => {
            shard
                .hash_set(&key, &pairs)
                .expect("hash type was validated before recording");
        }
    }

    Ok(Response::Bulk(Some(Cow::Owned(text))))
}
