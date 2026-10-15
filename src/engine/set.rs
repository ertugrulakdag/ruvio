use super::*;

pub(super) fn pop_set<'a, C: Clock, I: StorageIndex, S: MutationSink>(
    shard: &'a mut KvShard<C, I>,
    sink: &mut S,
    key: crate::protocol::Bytes<'a>,
    count: Option<usize>,
) -> Result<Response<'a>, S::Error> {
    if !accepts_type(shard, &key, ValueKind::Set) {
        return Ok(wrong_type());
    }

    let members = shard
        .set_pop_members(&key, count.unwrap_or(1))
        .expect("set type was validated");

    if members.is_empty() {
        return Ok(if count.is_some() {
            Response::Array(Vec::new())
        } else {
            Response::Bulk(None)
        });
    }

    before_mutation_allow_oom(shard, sink, [key.as_ref()], 0)?;

    let recorded: Vec<crate::protocol::Bytes<'a>> = members
        .iter()
        .map(|member| Cow::Owned(member.clone()))
        .collect();

    sink.record(Mutation::SetRemove {
        key: &key,
        members: &recorded,
    })?;

    shard
        .set_remove(&key, &members)
        .expect("set type was validated");

    if count.is_none() {
        return Ok(Response::Bulk(Some(Cow::Owned(
            members.into_iter().next().expect("non-empty pop"),
        ))));
    }

    Ok(Response::Array(
        members
            .into_iter()
            .map(|member| Some(Cow::Owned(member)))
            .collect(),
    ))
}

pub(super) fn sample_set<'a, C: Clock, I: StorageIndex, S: MutationSink>(
    shard: &mut KvShard<C, I>,
    key: crate::protocol::Bytes<'a>,
    count: Option<i64>,
) -> Result<Response<'a>, S::Error> {
    match shard.set_random_members(&key, count) {
        Ok(members) if count.is_none() => {
            Ok(Response::Bulk(members.into_iter().next().map(Cow::Owned)))
        }

        Ok(members) => Ok(Response::Array(
            members
                .into_iter()
                .map(|member| Some(Cow::Owned(member)))
                .collect(),
        )),
        Err(_) => Ok(wrong_type()),
    }
}

pub(super) fn set_algebra<'a, C: Clock, I: StorageIndex, S: MutationSink>(
    shard: &mut KvShard<C, I>,
    sink: &mut S,
    keys: &[crate::protocol::Bytes<'a>],
    op: SetOp,
    destination: Option<&[u8]>,
) -> Result<Response<'static>, S::Error> {
    let mut sets = Vec::with_capacity(keys.len());

    for key in keys {
        match shard.live_set(key) {
            Ok(set) => sets.push(set),
            Err(_) => return Ok(wrong_type()),
        }
    }

    let members: Vec<Cow<'_, [u8]>> = match op {
        SetOp::Union => sets
            .iter()
            .flatten()
            .flat_map(|set| set.iter())
            .collect::<ahash::AHashSet<_>>()
            .into_iter()
            .collect(),
        SetOp::Inter => match sets
            .iter()
            .min_by_key(|set| set.as_ref().map_or(0, |set| set.len()))
        {
            Some(Some(smallest)) => smallest
                .iter()
                .filter(|member| {
                    sets.iter()
                        .all(|set| set.as_ref().is_some_and(|set| set.contains(member)))
                })
                .collect(),
            _ => Vec::new(),
        },
        SetOp::Diff => sets
            .first()
            .and_then(Option::as_ref)
            .map(|first| {
                first
                    .iter()
                    .filter(|member| {
                        sets[1..]
                            .iter()
                            .all(|set| !set.as_ref().is_some_and(|set| set.contains(member)))
                    })
                    .collect()
            })
            .unwrap_or_default(),
    };

    let mut members: Vec<Vec<u8>> = members.into_iter().map(Cow::into_owned).collect();

    if let Some(destination) = destination {
        let extra = members.iter().map(Vec::len).sum::<usize>()
            + destination.len()
            + ADMISSION_OVERHEAD_BYTES;

        if let Some(rejected) = before_mutation(shard, sink, [destination], extra)? {
            return Ok(rejected);
        }

        let stored: Vec<Cow<'_, [u8]>> = members
            .iter()
            .map(|member| Cow::Borrowed(member.as_slice()))
            .collect();

        sink.record(Mutation::SetStore {
            key: destination,
            members: &stored,
        })?;

        let count = shard.set_store(destination, &members);

        return Ok(Response::Integer(count as i64));
    }

    // The matrix promises sorted replies, as SMEMBERS gives; union walks a hash set.
    members.sort_unstable();

    Ok(owned_byte_set(members))
}

pub(super) fn set_move<C: Clock, I: StorageIndex, S: MutationSink>(
    shard: &mut KvShard<C, I>,
    sink: &mut S,
    source: &[u8],
    destination: &[u8],
    member: &[u8],
) -> Result<Response<'static>, S::Error> {
    if shard.set_card(source).is_err() || shard.set_card(destination).is_err() {
        return Ok(wrong_type());
    }

    match shard.set_contains(source, member) {
        Ok(true) if source == destination => return Ok(Response::Integer(1)),
        Ok(false) => return Ok(Response::Integer(0)),
        Ok(true) => {}
        Err(_) => return Ok(wrong_type()),
    }

    // Not exempt from OOM like Redis: the member can create the destination or turn it
    // from a compact encoding into a larger one.
    if let Some(rejected) = before_mutation(
        shard,
        sink,
        [source, destination],
        member.len() + ADMISSION_OVERHEAD_BYTES,
    )? {
        return Ok(rejected);
    }

    sink.record(Mutation::SetMove {
        source,
        destination,
        member,
    })?;

    Ok(Response::Integer(i64::from(
        shard
            .set_move(source, destination, member)
            .expect("set move was validated before recording"),
    )))
}
