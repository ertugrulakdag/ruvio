use super::*;

pub(super) fn sorted_set_add<'a, C: Clock, I: StorageIndex, S: MutationSink, W: ListWaiterSink>(
    shard: &'a mut KvShard<C, I>,
    sink: &mut S,
    waiters: &mut W,
    key: crate::protocol::Bytes<'a>,
    pairs: Vec<(f64, crate::protocol::Bytes<'a>)>,
    options: crate::protocol::SortedSetAddOptions,
) -> Result<Response<'a>, S::Error> {
    if pairs.iter().any(|(score, _)| score.is_nan()) {
        return Ok(sorted_set_error(SortedSetError::InvalidScore));
    }

    if !accepts_type(shard, &key, ValueKind::SortedSet) {
        return Ok(wrong_type());
    }

    if options.increment {
        return sorted_set_increment_add(
            shard,
            sink,
            waiters,
            key,
            &pairs[0].1,
            pairs[0].0,
            options,
        );
    }

    let mut added = 0i64;
    let mut changed = 0i64;
    // Scores this command already set, by member: one lookup per pair, so a ZADD of
    // many members stays linear.
    let mut overlay: ahash::AHashMap<&[u8], f64> = ahash::AHashMap::new();
    let mut kept = vec![false; pairs.len()];

    for (index, (score, member)) in pairs.iter().enumerate() {
        let current = overlay.get(member.as_ref()).copied().or_else(|| {
            shard
                .sorted_set_score(&key, member)
                .expect("sorted-set type was validated")
        });

        if current == Some(*score) || !score_update_applies(current, *score, options) {
            continue;
        }

        if current.is_none() {
            added += 1;
        }

        changed += 1;
        overlay.insert(member.as_ref(), *score);
        kept[index] = true;
    }
    drop(overlay);

    let selected: Vec<_> = pairs
        .into_iter()
        .zip(kept)
        .filter_map(|(pair, kept)| kept.then_some(pair))
        .collect();

    if selected.is_empty() {
        return Ok(Response::Integer(0));
    }

    let extra = selected
        .iter()
        .map(|(_, member)| member.len())
        .sum::<usize>()
        + key.len()
        + ADMISSION_OVERHEAD_BYTES;

    if let Some(rejected) = before_mutation(shard, sink, [key.as_ref()], extra)? {
        return Ok(rejected);
    }

    if !shard.sorted_set_make_room(&key, false, selected.iter().map(|(_, m)| m.as_ref())) {
        return Ok(sorted_set_error(SortedSetError::Full));
    }

    sink.record(Mutation::SortedSetAdd {
        key: &key,
        pairs: &selected,
    })?;

    if let Err(error) = shard.sorted_set_add_pairs(&key, &selected) {
        debug_assert!(false, "sorted-set add failed after validation");
        return Ok(sorted_set_error(error));
    }
    wake_sorted_waiters(shard, sink, waiters, &key)?;

    Ok(Response::Integer(if options.changed {
        changed
    } else {
        added
    }))
}

pub(super) fn sorted_set_increment_add<
    'a,
    C: Clock,
    I: StorageIndex,
    S: MutationSink,
    W: ListWaiterSink,
>(
    shard: &'a mut KvShard<C, I>,
    sink: &mut S,
    waiters: &mut W,
    key: crate::protocol::Bytes<'a>,
    member: &crate::protocol::Bytes<'a>,
    delta: f64,
    options: crate::protocol::SortedSetAddOptions,
) -> Result<Response<'a>, S::Error> {
    let current = shard
        .sorted_set_score(&key, member)
        .expect("sorted-set type was validated");
    let next = current.unwrap_or(0.0) + delta;

    if next.is_nan() {
        return Ok(sorted_set_error(SortedSetError::InvalidIncrement));
    }

    if !score_update_applies(current, next, options) {
        return Ok(Response::Bulk(None));
    }

    if current == Some(next) {
        return Ok(Response::Double(next));
    }

    if let Some(rejected) = before_mutation(
        shard,
        sink,
        [key.as_ref()],
        key.len() + member.len() + ADMISSION_OVERHEAD_BYTES,
    )? {
        return Ok(rejected);
    }

    if !shard.sorted_set_make_room(&key, false, std::iter::once(member.as_ref())) {
        return Ok(sorted_set_error(SortedSetError::Full));
    }

    let pair = [(next, member.clone())];

    sink.record(Mutation::SortedSetAdd {
        key: &key,
        pairs: &pair,
    })?;

    if let Err(error) = shard.sorted_set_add(&key, next, member) {
        debug_assert!(false, "sorted-set increment failed after validation");
        return Ok(sorted_set_error(error));
    }
    wake_sorted_waiters(shard, sink, waiters, &key)?;

    Ok(Response::Double(next))
}

pub(super) fn score_update_applies(
    current: Option<f64>,
    score: f64,
    options: crate::protocol::SortedSetAddOptions,
) -> bool {
    match (options.condition, current) {
        (crate::protocol::SortedSetCondition::NotExists, Some(_)) => return false,
        (crate::protocol::SortedSetCondition::Exists, None) => return false,
        _ => {}
    }

    let Some(current) = current else {
        return true;
    };

    if options.greater {
        return score > current;
    }

    if options.less {
        return score < current;
    }

    true
}

/// Pops for parked `BZPOPMIN`/`BZPOPMAX` clients after a write added members to `key`.
#[inline]
pub(super) fn wake_sorted_waiters<C: Clock, I: StorageIndex, S: MutationSink, W: ListWaiterSink>(
    shard: &mut KvShard<C, I>,
    sink: &mut S,
    waiters: &mut W,
    key: &[u8],
) -> Result<(), S::Error> {
    if !waiters.has_waiters() {
        return Ok(());
    }

    serve_sorted_waiters(shard, sink, waiters, key)
}

#[cold]
pub(super) fn serve_sorted_waiters<
    C: Clock,
    I: StorageIndex,
    S: MutationSink,
    W: ListWaiterSink,
>(
    shard: &mut KvShard<C, I>,
    sink: &mut S,
    waiters: &mut W,
    key: &[u8],
) -> Result<(), S::Error> {
    let database = shard.active_database();

    while let Some(max) = waiters.sorted_waiter(database, key) {
        let Some((member, score)) = shard
            .sorted_set_range_entries(key, 0, 0, max)
            .ok()
            .and_then(|entries| {
                entries
                    .first()
                    .map(|(member, score)| (member.to_vec(), *score))
            })
        else {
            break;
        };

        before_mutation_allow_oom(shard, sink, [key], 0)?;

        let members = [Cow::Owned(member)];

        sink.record(Mutation::SortedSetRemove {
            key,
            members: &members,
        })?;

        shard
            .sorted_set_remove_popped(key, &members)
            .expect("sorted-set type was validated before recording");
        waiters.served_by(sink.written_sequence());
        waiters.deliver_sorted(database, key, &members[0], score);
    }

    Ok(())
}

/// Records one `SortedSetRemove` for `members`, then removes them. Empty input writes nothing.
pub(super) fn remove_sorted_members<C: Clock, I: StorageIndex, S: MutationSink>(
    shard: &mut KvShard<C, I>,
    sink: &mut S,
    key: &[u8],
    members: &[crate::protocol::Bytes<'_>],
) -> Result<(), S::Error> {
    if members.is_empty() {
        return Ok(());
    }

    before_mutation_allow_oom(shard, sink, [key], 0)?;

    sink.record(Mutation::SortedSetRemove { key, members })?;

    shard
        .sorted_set_remove(key, members)
        .expect("sorted-set type was validated before recording");
    Ok(())
}

/// What a pop of `count` would take from the low (`max` false) or high end of `key`, with
/// one index lookup. `None` when `key` holds another type.
fn peek_popped<C: Clock, I: StorageIndex>(
    shard: &KvShard<C, I>,
    key: &[u8],
    count: usize,
    max: bool,
) -> Option<PoppedEntries> {
    let mut popped = PoppedEntries::default();

    shard
        .sorted_set_visit_ends(key, count, max, |member, score| popped.push(member, score))
        .ok()?;

    Some(popped)
}

/// Records one `SortedSetRemove` for the entries `peek_popped` read, then removes them.
/// The record lists the members in pop order, as a remove of those members would.
fn remove_popped<C: Clock, I: StorageIndex, S: MutationSink>(
    shard: &mut KvShard<C, I>,
    sink: &mut S,
    key: &[u8],
    popped: &PoppedEntries,
) -> Result<(), S::Error> {
    let mut entries = popped.iter();

    match popped.len() {
        0 => Ok(()),
        1 => {
            let (member, _) = entries.next().expect("one popped entry");

            apply_popped(shard, sink, key, &[Cow::Borrowed(member)])
        }
        _ => {
            let members: Vec<crate::protocol::Bytes<'_>> =
                entries.map(|(member, _)| Cow::Borrowed(member)).collect();

            apply_popped(shard, sink, key, &members)
        }
    }
}

fn apply_popped<C: Clock, I: StorageIndex, S: MutationSink>(
    shard: &mut KvShard<C, I>,
    sink: &mut S,
    key: &[u8],
    members: &[crate::protocol::Bytes<'_>],
) -> Result<(), S::Error> {
    before_mutation_allow_oom(shard, sink, [key], 0)?;

    sink.record(Mutation::SortedSetRemove { key, members })?;

    shard
        .sorted_set_remove_popped(key, members)
        .expect("sorted-set type was validated before recording");
    Ok(())
}

pub(super) fn pop_sorted<'a, C: Clock, I: StorageIndex, S: MutationSink>(
    shard: &'a mut KvShard<C, I>,
    sink: &mut S,
    key: crate::protocol::Bytes<'a>,
    count: Option<usize>,
    max: bool,
) -> Result<Response<'a>, S::Error> {
    let Some(popped) = peek_popped(shard, &key, count.unwrap_or(1), max) else {
        return Ok(wrong_type());
    };

    remove_popped(shard, sink, &key, &popped)?;

    Ok(Response::Popped(None, popped))
}

pub(super) fn remove_sorted_range<'a, C: Clock, I: StorageIndex, S: MutationSink>(
    shard: &'a mut KvShard<C, I>,
    sink: &mut S,
    key: crate::protocol::Bytes<'a>,
    range: crate::protocol::SortedRemoveRange<'a>,
) -> Result<Response<'a>, S::Error> {
    use crate::protocol::SortedRemoveRange;

    let entries = match &range {
        SortedRemoveRange::Rank(start, stop) => {
            shard.sorted_set_range_entries(&key, *start, *stop, false)
        }

        SortedRemoveRange::Score(min, max) => shard.sorted_set_range_by_score(&key, *min, *max),
        SortedRemoveRange::Lex(min, max) => shard.sorted_set_range_by_lex(&key, min, max),
    };

    let Ok(entries) = entries else {
        return Ok(wrong_type());
    };

    let members: Vec<crate::protocol::Bytes<'static>> = entries
        .into_iter()
        .map(|(member, _)| Cow::Owned(member.to_vec()))
        .collect();

    remove_sorted_members(shard, sink, &key, &members)?;

    Ok(Response::Integer(members.len() as i64))
}

pub(super) fn block_pop_sorted<'a, C: Clock, I: StorageIndex, S: MutationSink>(
    shard: &'a mut KvShard<C, I>,
    sink: &mut S,
    mut keys: Vec<crate::protocol::Bytes<'a>>,
    timeout_ms: Option<u64>,
    max: bool,
) -> Result<Response<'a>, S::Error> {
    for key in &keys {
        if !accepts_type(shard, key, ValueKind::SortedSet) {
            return Ok(wrong_type());
        }
    }

    for index in 0..keys.len() {
        let popped = peek_popped(shard, &keys[index], 1, max).unwrap_or_default();

        if popped.is_empty() {
            continue;
        }

        remove_popped(shard, sink, &keys[index], &popped)?;

        return Ok(Response::Popped(Some(keys.swap_remove(index)), popped));
    }

    let database = shard.active_database();

    Ok(Response::BlockList {
        keys: keys
            .iter()
            .map(|key| Cow::Owned(tagged(database, key)))
            .collect(),
        timeout_ms,
        back: false,
        sorted: Some(max),
    })
}
