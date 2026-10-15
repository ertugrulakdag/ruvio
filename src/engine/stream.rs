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
