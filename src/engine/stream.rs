use super::*;

pub(super) fn stream_write_error(error: crate::store::StreamWriteError) -> Response<'static> {
    match error {
        crate::store::StreamWriteError::WrongType => wrong_type(),
        crate::store::StreamWriteError::Id => Response::Error(
            b"ERR The ID specified in XADD is equal or smaller than the target stream top item",
        ),
    }
}

pub(super) fn stream_info<C: crate::time::Clock, I: crate::StorageIndex>(
    shard: &mut crate::store::KvShard<C, I>,
    key: &[u8],
    group: Option<&[u8]>,
    kind: crate::protocol::StreamInfoKind,
) -> Response<'static> {
    use crate::protocol::StreamInfoKind;

    let result = match kind {
        StreamInfoKind::Stream => shard.stream_overview(key).map(stream_overview_response),
        StreamInfoKind::Groups => shard
            .stream_group_overviews(key)
            .map(stream_groups_response),
        StreamInfoKind::Consumers => {
            let Some(group) = group else {
                return Response::Error(b"ERR XINFO CONSUMERS expects a group");
            };

            let now = shard.now_unix_millis().get();

            shard
                .stream_consumer_overviews(key, group, now)
                .map(stream_consumers_response)
        }
    };

    result.unwrap_or_else(stream_group_error)
}

pub(super) fn stream_overview_response(info: crate::stream::StreamOverview) -> Response<'static> {
    let last_id = match info.last_millis {
        Some(millis) => crate::stream::format_id(millis, info.last_sequence),
        None => b"0-0".to_vec(),
    };

    flat_pairs(vec![
        bulk_static(b"length"),
        Response::Integer(info.length as i64),
        bulk_static(b"last-generated-id"),
        bulk_owned(last_id),
        bulk_static(b"groups"),
        Response::Integer(info.groups as i64),
        bulk_static(b"first-entry"),
        stream_edge(info.first),
        bulk_static(b"last-entry"),
        stream_edge(info.last),
    ])
}

pub(super) fn stream_groups_response(
    groups: Vec<crate::stream::GroupOverview>,
) -> Response<'static> {
    Response::Exec(
        groups
            .into_iter()
            .map(|group| {
                flat_pairs(vec![
                    bulk_static(b"name"),
                    bulk_owned(group.name),
                    bulk_static(b"consumers"),
                    Response::Integer(group.consumers as i64),
                    bulk_static(b"pending"),
                    Response::Integer(group.pending as i64),
                    bulk_static(b"last-delivered-id"),
                    bulk_owned(crate::stream::format_id(
                        group.last_millis,
                        group.last_sequence,
                    )),
                    bulk_static(b"lag"),
                    Response::Integer(group.lag as i64),
                ])
            })
            .collect(),
    )
}

pub(super) fn stream_consumers_response(
    consumers: Vec<crate::stream::ConsumerOverview>,
) -> Response<'static> {
    Response::Exec(
        consumers
            .into_iter()
            .map(|consumer| {
                flat_pairs(vec![
                    bulk_static(b"name"),
                    bulk_owned(consumer.name),
                    bulk_static(b"pending"),
                    Response::Integer(consumer.pending as i64),
                    bulk_static(b"idle"),
                    Response::Integer(consumer.idle as i64),
                ])
            })
            .collect(),
    )
}

pub(super) fn stream_edge(entry: Option<crate::stream::StreamEntry>) -> Response<'static> {
    let Some(entry) = entry else {
        return Response::Bulk(None);
    };

    let mut fields = Vec::with_capacity(entry.fields.len().saturating_mul(2));

    for (field, value) in &entry.fields {
        fields.push(bulk_owned(field.to_vec()));
        fields.push(bulk_owned(value.to_vec()));
    }

    Response::Exec(vec![
        bulk_owned(crate::stream::format_id(entry.millis, entry.sequence)),
        flat_pairs(fields),
    ])
}

pub(super) fn flat_pairs(items: Vec<Response<'static>>) -> Response<'static> {
    Response::Exec(items)
}

pub(super) fn bulk_static(bytes: &'static [u8]) -> Response<'static> {
    Response::Bulk(Some(Cow::Borrowed(bytes)))
}

pub(super) fn bulk_owned(bytes: Vec<u8>) -> Response<'static> {
    Response::Bulk(Some(Cow::Owned(bytes)))
}

pub(super) fn stream_group_error(error: crate::store::StreamGroupError) -> Response<'static> {
    match error {
        crate::store::StreamGroupError::WrongType => wrong_type(),
        crate::store::StreamGroupError::MissingKey => {
            Response::Error(b"ERR The XGROUP subcommand requires the key to exist")
        }

        crate::store::StreamGroupError::Busy => {
            Response::Error(b"BUSYGROUP Consumer Group name already exists")
        }

        crate::store::StreamGroupError::NoGroup => {
            Response::Error(b"NOGROUP No such key or consumer group")
        }
    }
}

pub(super) fn pending_rows_response(rows: Vec<crate::stream::PendingRow>) -> Response<'static> {
    Response::Exec(
        rows.into_iter()
            .map(|row| {
                Response::Exec(vec![
                    Response::Bulk(Some(Cow::Owned(crate::stream::format_id(
                        row.millis,
                        row.sequence,
                    )))),
                    Response::Bulk(Some(Cow::Owned(row.consumer))),
                    Response::Integer(i64::try_from(row.idle).unwrap_or(i64::MAX)),
                    Response::Integer(i64::try_from(row.deliveries).unwrap_or(i64::MAX)),
                ])
            })
            .collect(),
    )
}

pub(super) fn pending_summary_response(
    summary: crate::stream::PendingSummary,
) -> Response<'static> {
    let id_bulk = |id: Option<(u64, u64)>| match id {
        Some((millis, sequence)) => {
            Response::Bulk(Some(Cow::Owned(crate::stream::format_id(millis, sequence))))
        }
        None => Response::Bulk(None),
    };

    let consumers = summary
        .consumers
        .into_iter()
        .map(|(name, count)| {
            Response::Exec(vec![
                Response::Bulk(Some(Cow::Owned(name))),
                Response::Integer(count),
            ])
        })
        .collect();

    Response::Exec(vec![
        Response::Integer(summary.count as i64),
        id_bulk(summary.smallest),
        id_bulk(summary.greatest),
        Response::Exec(consumers),
    ])
}

pub(super) fn stream_range_response(entries: Vec<crate::stream::StreamEntry>) -> Response<'static> {
    Response::Exec(
        entries
            .into_iter()
            .map(|entry| {
                let mut fields = Vec::with_capacity(entry.fields.len().saturating_mul(2));

                for (field, value) in &entry.fields {
                    fields.push(Some(Cow::Owned(field.to_vec())));
                    fields.push(Some(Cow::Owned(value.to_vec())));
                }

                Response::Exec(vec![
                    Response::Bulk(Some(Cow::Owned(crate::stream::format_id(
                        entry.millis,
                        entry.sequence,
                    )))),
                    Response::Array(fields),
                ])
            })
            .collect(),
    )
}

#[inline(always)]
#[allow(clippy::too_many_arguments)]
pub(super) fn stream_add<'a, C: Clock, I: StorageIndex, S: MutationSink, W: ListWaiterSink>(
    shard: &'a mut KvShard<C, I>,
    sink: &mut S,
    waiters: &mut W,
    database: u8,
    key: crate::protocol::Bytes<'a>,
    id: StreamAddId,
    max_len: Option<usize>,
    min_id: Option<(u64, u64)>,
    delay_ms: Option<u64>,
    nomkstream: bool,
    fields: Vec<(crate::protocol::Bytes<'a>, crate::protocol::Bytes<'a>)>,
) -> Result<Response<'a>, S::Error> {
    if !accepts_type(shard, &key, ValueKind::Stream) {
        return Ok(wrong_type());
    }

    if nomkstream && shard.value_kind(&key).is_none() {
        return Ok(Response::Bulk(None));
    }

    let now = shard.now_unix_millis().get();
    let (millis, sequence) = match id {
        StreamAddId::Explicit(millis, sequence) => (millis, sequence),
        StreamAddId::Auto => match shard.stream_next_id(&key, now) {
            Ok(id) => id,
            Err(_) => return Ok(wrong_type()),
        },
        StreamAddId::AutoSequence(millis) => match shard.stream_next_at(&key, millis) {
            Ok(id) => id,
            Err(error) => return Ok(stream_write_error(error)),
        },
    };

    if let Err(error) = shard.stream_can_append(&key, millis, sequence) {
        return Ok(stream_write_error(error));
    }

    let bytes = fields.iter().fold(key.len(), |total, (field, value)| {
        total
            .saturating_add(field.len())
            .saturating_add(value.len())
    });

    if let Some(rejected) = before_mutation(
        shard,
        sink,
        [key.as_ref()],
        bytes + ADMISSION_OVERHEAD_BYTES,
    )? {
        return Ok(rejected);
    }

    let ready_at = delay_ms
        .filter(|delay| *delay > 0)
        .map(|delay| shard.now_unix_millis().get().saturating_add(delay));

    if let Some(ready_at) = ready_at {
        // A held entry goes out with its hold in one record: a torn WAL tail or a
        // replica never sees it released early.
        let mut together = vec![
            Mutation::StreamAppend {
                key: &key,
                millis,
                sequence,
                fields: &fields,
            }
            .to_owned(),
        ];

        if let Some(max_len) = max_len {
            together.push(Mutation::StreamTrim { key: &key, max_len }.to_owned());
        }
        together.push(
            Mutation::StreamHold {
                key: &key,
                millis,
                sequence,
                ready_at,
            }
            .to_owned(),
        );
        if let Some((millis, sequence)) = min_id {
            together.push(
                Mutation::StreamTrimMinId {
                    key: &key,
                    millis,
                    sequence,
                }
                .to_owned(),
            );
        }
        sink.record(Mutation::Transaction {
            mutations: &together,
        })?;
    } else {
        sink.record(Mutation::StreamAppend {
            key: &key,
            millis,
            sequence,
            fields: &fields,
        })?;
    }

    if let Err(error) = shard.stream_append(&key, millis, sequence, &fields) {
        debug_assert!(false, "stream append failed after validation");

        return Ok(stream_write_error(error));
    }

    if let Some(max_len) = max_len {
        if ready_at.is_none() {
            sink.record(Mutation::StreamTrim { key: &key, max_len })?;
        }

        if shard.stream_trim(&key, max_len).is_err() {
            debug_assert!(false, "stream trim failed after append");

            return Ok(wrong_type());
        }
    }

    if let Some(ready_at) = ready_at
        && shard
            .stream_hold_until(&key, millis, sequence, ready_at)
            .is_err()
    {
        debug_assert!(false, "stream hold failed after append");

        return Ok(wrong_type());
    }

    if let Some((millis, sequence)) = min_id {
        if ready_at.is_none() {
            sink.record(Mutation::StreamTrimMinId {
                key: &key,
                millis,
                sequence,
            })?;
        }

        if shard.stream_trim_min_id(&key, millis, sequence).is_err() {
            debug_assert!(false, "stream minid trim failed after append");

            return Ok(wrong_type());
        }
    }

    // With no client parked anywhere there is nobody to hand the entry to.
    if waiters.has_waiters()
        && shard
            .stream_contains(&key, millis, sequence)
            .unwrap_or(false)
    {
        let pairs: Vec<(&[u8], &[u8])> = fields
            .iter()
            .map(|(field, value)| (field.as_ref(), value.as_ref()))
            .collect();

        let served_by = sink.written_sequence();

        wake_stream_readers(waiters, served_by, database, &key, millis, sequence, &pairs);
        waiters.served_by(served_by);
        let mut claimed_groups = Vec::new();

        while let Some(blocked) =
            waiters.next_blocked_group(database, &key, millis, sequence, &claimed_groups)
        {
            if blocked.noack {
                sink.record(Mutation::StreamGroupAdvance {
                    key: &key,
                    group: &blocked.group,
                    millis,
                    sequence,
                })?;

                if shard
                    .stream_group_advance(&key, &blocked.group, millis, sequence)
                    .is_err()
                {
                    debug_assert!(false, "group advance failed after append");
                    break;
                }
            } else {
                sink.record(Mutation::StreamClaim {
                    key: &key,
                    group: &blocked.group,
                    consumer: &blocked.consumer,
                    millis,
                    sequence,
                    delivered_at: now,
                })?;

                if shard
                    .stream_group_claim(
                        &key,
                        &blocked.group,
                        &blocked.consumer,
                        millis,
                        sequence,
                        now,
                    )
                    .is_err()
                {
                    debug_assert!(false, "group claim failed after append");
                    break;
                }
            }

            waiters.served_by(sink.written_sequence());
            waiters.deliver_group(database, &key, &blocked.group, millis, sequence, &pairs);
            claimed_groups.push(blocked.group);
        }
    }

    Ok(Response::Bulk(Some(Cow::Owned(crate::stream::format_id(
        millis, sequence,
    )))))
}

#[inline(always)]
pub(super) fn stream_read<'a, C: Clock, I: StorageIndex, S: MutationSink>(
    shard: &'a mut KvShard<C, I>,
    database: u8,
    count: Option<usize>,
    block: Option<Option<u64>>,
    keys: Vec<(crate::protocol::Bytes<'a>, crate::stream::StreamCursor)>,
) -> Result<Response<'a>, S::Error> {
    for (key, _) in &keys {
        if !accepts_type(shard, key, ValueKind::Stream) {
            return Ok(wrong_type());
        }
    }

    let limit = count.unwrap_or(usize::MAX);
    let mut groups = Vec::new();

    for (key, cursor) in &keys {
        let entries = match shard.stream_read(key, *cursor, limit) {
            Ok(entries) => entries,
            Err(_) => return Ok(wrong_type()),
        };

        if entries.is_empty() {
            continue;
        }

        groups.push(Response::Exec(vec![
            Response::Bulk(Some(Cow::Owned(key.to_vec()))),
            stream_range_response(entries),
        ]));
    }

    if !groups.is_empty() {
        return Ok(Response::Exec(groups));
    }

    let Some(timeout_ms) = block else {
        return Ok(Response::NullArray);
    };

    let mut resolved = Vec::with_capacity(keys.len());

    for (key, cursor) in &keys {
        let cursor = match cursor {
            crate::stream::StreamCursor::After(millis, sequence) => (*millis, *sequence),
            crate::stream::StreamCursor::End => match shard.stream_end(key) {
                Ok(end) => end,
                Err(_) => return Ok(wrong_type()),
            },
        };

        resolved.push(cursor);
    }

    Ok(Response::BlockStream {
        keys: keys
            .iter()
            .map(|(key, _)| Cow::Owned(tagged(database, key)))
            .collect(),
        cursors: resolved,
        timeout_ms,
    })
}

#[inline(always)]
pub(super) fn stream_set_id<'a, C: Clock, I: StorageIndex, S: MutationSink>(
    shard: &'a mut KvShard<C, I>,
    sink: &mut S,
    key: crate::protocol::Bytes<'a>,
    id: (u64, u64),
) -> Result<Response<'a>, S::Error> {
    let (millis, sequence) = id;

    match shard.stream_set_id_check(&key, millis, sequence) {
        Err(crate::store::StreamSetIdError::Missing) => {
            return Ok(Response::Error(b"ERR no such key"));
        }

        Err(crate::store::StreamSetIdError::WrongType) => return Ok(wrong_type()),

        Err(crate::store::StreamSetIdError::TooSmall) => {
            return Ok(Response::Error(
                b"ERR The ID specified in XSETID is smaller than the target stream top item",
            ));
        }

        Ok(()) => {}
    }

    if let Some(rejected) = before_mutation(
        shard,
        sink,
        [key.as_ref()],
        key.len() + ADMISSION_OVERHEAD_BYTES,
    )? {
        return Ok(rejected);
    }

    sink.record(Mutation::StreamSetId {
        key: &key,
        millis,
        sequence,
    })?;

    match shard.stream_set_id(&key, millis, sequence) {
        Ok(()) => Ok(Response::Simple(b"OK")),
        Err(crate::store::StreamSetIdError::WrongType) => Ok(wrong_type()),
        Err(crate::store::StreamSetIdError::Missing) => Ok(Response::Error(b"ERR no such key")),

        Err(crate::store::StreamSetIdError::TooSmall) => Ok(Response::Error(
            b"ERR The ID specified in XSETID is smaller than the target stream top item",
        )),
    }
}

#[inline(always)]
pub(super) fn stream_group_create<'a, C: Clock, I: StorageIndex, S: MutationSink>(
    shard: &'a mut KvShard<C, I>,
    sink: &mut S,
    key: crate::protocol::Bytes<'a>,
    group: crate::protocol::Bytes<'a>,
    start: crate::stream::GroupStart,
    mkstream: bool,
) -> Result<Response<'a>, S::Error> {
    if let Err(error) = shard.stream_group_can_create(&key, group.as_ref(), mkstream) {
        return Ok(stream_group_error(error));
    }

    let (millis, sequence) = match start {
        crate::stream::GroupStart::Id(millis, sequence) => (millis, sequence),
        crate::stream::GroupStart::End => match shard.stream_end(&key) {
            Ok(end) => end,
            Err(_) => return Ok(wrong_type()),
        },
    };

    if let Some(rejected) = before_mutation(
        shard,
        sink,
        [key.as_ref()],
        key.len() + ADMISSION_OVERHEAD_BYTES,
    )? {
        return Ok(rejected);
    }

    sink.record(Mutation::StreamGroupCreate {
        key: key.as_ref(),
        group: group.as_ref(),
        millis,
        sequence,
        mkstream,
    })?;

    match shard.stream_create_group(&key, group.as_ref(), millis, sequence, mkstream) {
        Ok(()) => Ok(Response::Simple(b"OK")),
        Err(error) => Ok(stream_group_error(error)),
    }
}

#[inline(always)]
#[allow(clippy::too_many_arguments)]
pub(super) fn stream_group_read<'a, C: Clock, I: StorageIndex, S: MutationSink>(
    shard: &'a mut KvShard<C, I>,
    sink: &mut S,
    database: u8,
    key: crate::protocol::Bytes<'a>,
    group: crate::protocol::Bytes<'a>,
    consumer: crate::protocol::Bytes<'a>,
    count: Option<usize>,
    block: Option<Option<u64>>,
    noack: bool,
) -> Result<Response<'a>, S::Error> {
    let limit = count.unwrap_or(usize::MAX);
    let entries = match shard.stream_group_new(&key, group.as_ref(), limit) {
        Ok(entries) => entries,
        Err(error) => return Ok(stream_group_error(error)),
    };

    if entries.is_empty() {
        let Some(timeout_ms) = block else {
            return Ok(Response::NullArray);
        };

        let cursor = match shard.stream_group_cursor(&key, group.as_ref()) {
            Ok(cursor) => cursor,
            Err(error) => return Ok(stream_group_error(error)),
        };

        return Ok(Response::BlockGroup {
            key: Cow::Owned(tagged(database, &key)),
            group,
            consumer,
            cursor,
            timeout_ms,
            noack,
        });
    }

    if let Some(rejected) = before_mutation(
        shard,
        sink,
        [key.as_ref()],
        key.len() + ADMISSION_OVERHEAD_BYTES,
    )? {
        return Ok(rejected);
    }

    let delivered_at = shard.now_unix_millis().get();

    for entry in &entries {
        if noack {
            sink.record(Mutation::StreamGroupAdvance {
                key: key.as_ref(),
                group: group.as_ref(),
                millis: entry.millis,
                sequence: entry.sequence,
            })?;

            if let Err(error) =
                shard.stream_group_advance(&key, group.as_ref(), entry.millis, entry.sequence)
            {
                return Ok(stream_group_error(error));
            }
        } else {
            sink.record(Mutation::StreamClaim {
                key: key.as_ref(),
                group: group.as_ref(),
                consumer: consumer.as_ref(),
                millis: entry.millis,
                sequence: entry.sequence,
                delivered_at,
            })?;

            if let Err(error) = shard.stream_group_claim(
                &key,
                group.as_ref(),
                consumer.as_ref(),
                entry.millis,
                entry.sequence,
                delivered_at,
            ) {
                return Ok(stream_group_error(error));
            }
        }
    }

    Ok(Response::Exec(vec![Response::Exec(vec![
        Response::Bulk(Some(Cow::Owned(key.to_vec()))),
        stream_range_response(entries),
    ])]))
}

#[inline(always)]
pub(super) fn stream_group_create_consumer<'a, C: Clock, I: StorageIndex, S: MutationSink>(
    shard: &'a mut KvShard<C, I>,
    sink: &mut S,
    key: crate::protocol::Bytes<'a>,
    group: crate::protocol::Bytes<'a>,
    consumer: crate::protocol::Bytes<'a>,
) -> Result<Response<'a>, S::Error> {
    if let Err(error) = shard.stream_group_summary(&key, group.as_ref()) {
        return Ok(stream_group_error(error));
    }

    let already = shard
        .stream_consumer_overviews(&key, group.as_ref(), 0)
        .map(|rows| {
            rows.iter()
                .any(|row| row.name.as_slice() == consumer.as_ref())
        })
        .unwrap_or(false);

    if already {
        return Ok(Response::Integer(0));
    }

    if let Some(rejected) = before_mutation(
        shard,
        sink,
        [key.as_ref()],
        key.len() + ADMISSION_OVERHEAD_BYTES,
    )? {
        return Ok(rejected);
    }

    sink.record(Mutation::StreamGroupCreateConsumer {
        key: key.as_ref(),
        group: group.as_ref(),
        consumer: consumer.as_ref(),
    })?;

    match shard.stream_create_consumer(key.as_ref(), group.as_ref(), consumer.as_ref()) {
        Ok(true) => Ok(Response::Integer(1)),
        Ok(false) => Ok(Response::Integer(0)),
        Err(error) => Ok(stream_group_error(error)),
    }
}

#[inline(always)]
pub(super) fn stream_group_del_consumer<'a, C: Clock, I: StorageIndex, S: MutationSink>(
    shard: &'a mut KvShard<C, I>,
    sink: &mut S,
    key: crate::protocol::Bytes<'a>,
    group: crate::protocol::Bytes<'a>,
    consumer: crate::protocol::Bytes<'a>,
) -> Result<Response<'a>, S::Error> {
    let pending = match shard.stream_group_summary(&key, group.as_ref()) {
        Ok(summary) => summary
            .consumers
            .iter()
            .find(|(name, _)| name.as_slice() == consumer.as_ref())
            .map(|(_, count)| *count)
            .unwrap_or(0),
        Err(error) => return Ok(stream_group_error(error)),
    };

    let registered = shard
        .stream_consumer_overviews(&key, group.as_ref(), 0)
        .map(|rows| {
            rows.iter()
                .any(|row| row.name.as_slice() == consumer.as_ref())
        })
        .unwrap_or(false);

    if pending == 0 && !registered {
        return Ok(Response::Integer(0));
    }

    before_mutation_allow_oom(
        shard,
        sink,
        [key.as_ref()],
        key.len() + ADMISSION_OVERHEAD_BYTES,
    )?;

    sink.record(Mutation::StreamGroupDelConsumer {
        key: key.as_ref(),
        group: group.as_ref(),
        consumer: consumer.as_ref(),
    })?;

    match shard.stream_delete_consumer(&key, group.as_ref(), consumer.as_ref()) {
        Ok(removed) => Ok(Response::Integer(removed as i64)),
        Err(error) => Ok(stream_group_error(error)),
    }
}

#[inline(always)]
pub(super) fn stream_ack<'a, C: Clock, I: StorageIndex, S: MutationSink>(
    shard: &'a mut KvShard<C, I>,
    sink: &mut S,
    key: crate::protocol::Bytes<'a>,
    group: crate::protocol::Bytes<'a>,
    ids: Vec<(u64, u64)>,
) -> Result<Response<'a>, S::Error> {
    let mut acked = 0i64;

    for (millis, sequence) in ids {
        let present = match shard.stream_group_pending(&key, group.as_ref(), millis, sequence) {
            Ok(present) => present,
            Err(error) => return Ok(stream_group_error(error)),
        };

        if !present {
            continue;
        }

        if acked == 0 {
            before_mutation_allow_oom(
                shard,
                sink,
                [key.as_ref()],
                key.len() + ADMISSION_OVERHEAD_BYTES,
            )?;
        }

        sink.record(Mutation::StreamAck {
            key: key.as_ref(),
            group: group.as_ref(),
            millis,
            sequence,
        })?;

        match shard.stream_group_ack(&key, group.as_ref(), millis, sequence) {
            Ok(true) => acked += 1,
            Ok(false) => {}
            Err(error) => return Ok(stream_group_error(error)),
        }
    }

    Ok(Response::Integer(acked))
}

#[inline(always)]
#[allow(clippy::too_many_arguments)]
pub(super) fn stream_claim_pending<'a, C: Clock, I: StorageIndex, S: MutationSink>(
    shard: &'a mut KvShard<C, I>,
    sink: &mut S,
    key: crate::protocol::Bytes<'a>,
    group: crate::protocol::Bytes<'a>,
    consumer: crate::protocol::Bytes<'a>,
    min_idle: u64,
    ids: Vec<(u64, u64)>,
    options: crate::protocol::ClaimOptions,
) -> Result<Response<'a>, S::Error> {
    let now = shard.now_unix_millis().get();
    let delivered_at = match (options.idle, options.time) {
        (Some(idle), None) => now.saturating_sub(idle),
        (None, Some(time)) => time,
        (None, None) => now,
        (Some(_), Some(_)) => {
            return Ok(Response::Error(
                b"ERR XCLAIM accepts only one of IDLE or TIME",
            ));
        }
    };

    let mut claimed = Vec::new();
    let mut recorded = false;

    for (millis, sequence) in ids {
        if claimed.iter().any(|entry: &crate::stream::StreamEntry| {
            entry.millis == millis && entry.sequence == sequence
        }) {
            continue;
        }

        let pending = match shard.stream_pending_delivery(&key, group.as_ref(), millis, sequence) {
            Ok(pending) => pending,
            Err(error) => return Ok(stream_group_error(error)),
        };

        let entry = match shard.stream_entry(&key, millis, sequence) {
            Ok(entry) => entry,
            Err(_) => return Ok(wrong_type()),
        };

        let Some(pending) = pending else {
            if !options.force {
                continue;
            }

            let Some(entry) = entry else {
                continue;
            };

            let deliveries = options.retry_count.unwrap_or(1);

            if let Some(rejected) =
                admit_stream_write(shard, sink, key.as_ref(), key.len(), &mut recorded)?
            {
                return Ok(rejected);
            }

            sink.record(Mutation::StreamForceClaim {
                key: key.as_ref(),
                group: group.as_ref(),
                consumer: consumer.as_ref(),
                millis,
                sequence,
                delivered_at,
                deliveries,
            })?;

            if let Err(error) = shard.stream_group_force_claim(
                &key,
                group.as_ref(),
                consumer.as_ref(),
                crate::stream::ReclaimUpdate {
                    millis,
                    sequence,
                    delivered_at,
                    deliveries,
                },
            ) {
                return Ok(stream_group_error(error));
            }

            claimed.push(entry);
            continue;
        };

        let Some(entry) = entry else {
            if let Some(rejected) =
                admit_stream_write(shard, sink, key.as_ref(), key.len(), &mut recorded)?
            {
                return Ok(rejected);
            }

            sink.record(Mutation::StreamAck {
                key: key.as_ref(),
                group: group.as_ref(),
                millis,
                sequence,
            })?;

            if let Err(error) = shard.stream_group_ack(&key, group.as_ref(), millis, sequence) {
                return Ok(stream_group_error(error));
            }

            continue;
        };

        if now.saturating_sub(pending.delivered_at) < min_idle {
            continue;
        }

        let deliveries = options
            .retry_count
            .unwrap_or_else(|| pending.deliveries.saturating_add(1));

        if let Some(rejected) =
            admit_stream_write(shard, sink, key.as_ref(), key.len(), &mut recorded)?
        {
            return Ok(rejected);
        }

        sink.record(Mutation::StreamReclaim {
            key: key.as_ref(),
            group: group.as_ref(),
            consumer: consumer.as_ref(),
            millis,
            sequence,
            delivered_at,
            deliveries,
        })?;

        if let Err(error) = shard.stream_group_reclaim(
            &key,
            group.as_ref(),
            consumer.as_ref(),
            crate::stream::ReclaimUpdate {
                millis,
                sequence,
                delivered_at,
                deliveries,
            },
        ) {
            return Ok(stream_group_error(error));
        }

        claimed.push(entry);
    }

    if let Some((millis, sequence)) = options.last_id {
        if let Some(rejected) =
            admit_stream_write(shard, sink, key.as_ref(), key.len(), &mut recorded)?
        {
            return Ok(rejected);
        }

        sink.record(Mutation::StreamGroupSetId {
            key: key.as_ref(),
            group: group.as_ref(),
            millis,
            sequence,
        })?;

        if let Err(error) = shard.stream_set_group_id(&key, group.as_ref(), millis, sequence) {
            return Ok(stream_group_error(error));
        }
    }

    if options.just_id {
        return Ok(Response::Exec(
            claimed
                .into_iter()
                .map(|entry| {
                    Response::Bulk(Some(Cow::Owned(crate::stream::format_id(
                        entry.millis,
                        entry.sequence,
                    ))))
                })
                .collect(),
        ));
    }

    Ok(stream_range_response(claimed))
}

#[inline(always)]
#[allow(clippy::too_many_arguments)]
pub(super) fn stream_autoclaim<'a, C: Clock, I: StorageIndex, S: MutationSink>(
    shard: &'a mut KvShard<C, I>,
    sink: &mut S,
    key: crate::protocol::Bytes<'a>,
    group: crate::protocol::Bytes<'a>,
    consumer: crate::protocol::Bytes<'a>,
    min_idle: u64,
    start: (u64, u64),
    count: Option<usize>,
    just_id: bool,
) -> Result<Response<'a>, S::Error> {
    let now = shard.now_unix_millis().get();
    let plan = match shard.stream_autoclaim_plan(
        &key,
        group.as_ref(),
        start,
        min_idle,
        // Redis claims up to 100 entries when COUNT is not given.
        count.unwrap_or(100),
        now,
    ) {
        Ok(plan) => plan,
        Err(error) => return Ok(stream_group_error(error)),
    };

    let mut claimed = Vec::new();
    let mut deleted = Vec::new();
    let mut recorded = false;

    for step in plan.steps {
        match step {
            crate::stream::AutoclaimStep::Drop { millis, sequence } => {
                if let Some(rejected) =
                    admit_stream_write(shard, sink, key.as_ref(), key.len(), &mut recorded)?
                {
                    return Ok(rejected);
                }

                sink.record(Mutation::StreamAck {
                    key: key.as_ref(),
                    group: group.as_ref(),
                    millis,
                    sequence,
                })?;

                if let Err(error) = shard.stream_group_ack(&key, group.as_ref(), millis, sequence) {
                    return Ok(stream_group_error(error));
                }

                deleted.push((millis, sequence));
            }

            crate::stream::AutoclaimStep::Claim {
                millis,
                sequence,
                deliveries,
            } => {
                let Some(entry) = (match shard.stream_entry(&key, millis, sequence) {
                    Ok(entry) => entry,
                    Err(_) => return Ok(wrong_type()),
                }) else {
                    continue;
                };

                if let Some(rejected) =
                    admit_stream_write(shard, sink, key.as_ref(), key.len(), &mut recorded)?
                {
                    return Ok(rejected);
                }

                sink.record(Mutation::StreamReclaim {
                    key: key.as_ref(),
                    group: group.as_ref(),
                    consumer: consumer.as_ref(),
                    millis,
                    sequence,
                    delivered_at: now,
                    deliveries,
                })?;

                if let Err(error) = shard.stream_group_reclaim(
                    &key,
                    group.as_ref(),
                    consumer.as_ref(),
                    crate::stream::ReclaimUpdate {
                        millis,
                        sequence,
                        delivered_at: now,
                        deliveries,
                    },
                ) {
                    return Ok(stream_group_error(error));
                }

                claimed.push(entry);
            }
        }
    }

    let messages = if just_id {
        Response::Exec(
            claimed
                .into_iter()
                .map(|entry| {
                    Response::Bulk(Some(Cow::Owned(crate::stream::format_id(
                        entry.millis,
                        entry.sequence,
                    ))))
                })
                .collect(),
        )
    } else {
        stream_range_response(claimed)
    };

    Ok(Response::Exec(vec![
        Response::Bulk(Some(Cow::Owned(crate::stream::format_id(
            plan.next.0,
            plan.next.1,
        )))),
        messages,
        Response::Exec(
            deleted
                .into_iter()
                .map(|(millis, sequence)| {
                    Response::Bulk(Some(Cow::Owned(crate::stream::format_id(millis, sequence))))
                })
                .collect(),
        ),
    ]))
}
