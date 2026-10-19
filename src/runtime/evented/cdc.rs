//! `CHANGES SUBSCRIBE`: streams WAL records to one connection through its outbound queue.

use super::*;
use crate::{
    changes::{self, ChangeCursor, ChangeSubscription},
    persistence::{FrameCursor, FrameRead, decode_record, read_frames_resuming},
};

const READ_BYTES: usize = 64 * 1024;
/// The queue holds `PUBSUB_QUEUE` frames; stop reading well before it fills.
const QUEUE_HIGH_WATER: usize = crate::pubsub::PUBSUB_QUEUE / 2;
const IDLE_POLL: Duration = Duration::from_millis(20);
const HEARTBEAT: Duration = Duration::from_secs(1);

/// Validates the request and starts the stream; returns the cursor the stream begins after.
pub(super) fn start(
    state: &Rc<ShardState>,
    outbound: Outbound,
    subscription: ChangeSubscription,
    resp3: bool,
    shutdown: ShutdownToken,
) -> Result<ChangeCursor, &'static [u8]> {
    if state.role.get() == crate::config::ReplicationRole::Replica {
        return Err(b"ERR CHANGES SUBSCRIBE is only available on a primary");
    }

    let (profile, shard, durable) = {
        let wal = state
            .persistence
            .lock()
            .unwrap_or_else(|error| error.into_inner());

        (wal.profile(), wal.shard_id(), wal.durable())
    };

    if !profile.uses_wal() {
        return Err(b"ERR CHANGES SUBSCRIBE needs a durability profile that keeps a WAL");
    }

    let now = durable.load(Ordering::Acquire);
    let start = match subscription.from {
        None => ChangeCursor {
            shard,
            sequence: now,
        },
        Some(cursor) if cursor.shard != shard => {
            return Err(b"ERR change cursor belongs to a different shard");
        }
        Some(cursor) if cursor.sequence > now => {
            return Err(b"ERR change cursor is ahead of this server");
        }
        Some(cursor) => cursor,
    };

    tokio::task::spawn_local(stream(
        Rc::clone(state),
        outbound,
        subscription,
        resp3,
        start,
        durable,
        shutdown,
    ));

    Ok(start)
}

async fn stream(
    state: Rc<ShardState>,
    outbound: Outbound,
    subscription: ChangeSubscription,
    resp3: bool,
    start: ChangeCursor,
    durable: Arc<AtomicU64>,
    shutdown: ShutdownToken,
) {
    let data_dir = state.published_config.persistence.data_dir.clone();
    let limits = state.published_config.protocol;
    let shard = start.shard;
    let mut position = start.sequence;
    let mut reported = position;
    // Where the last chunk stopped in the log, so the next one does not rescan the segment.
    let mut reader = FrameCursor::default();
    let mut last_report = Instant::now();

    loop {
        if outbound.closed.get() || shutdown.is_requested() {
            return;
        }

        let through = durable.load(Ordering::Acquire);

        if position >= through || outbound.queue.borrow().len() >= QUEUE_HIGH_WATER {
            if position != reported && last_report.elapsed() >= HEARTBEAT {
                let mut frame = Vec::new();

                changes::encode_cursor(
                    &mut frame,
                    resp3,
                    ChangeCursor {
                        shard,
                        sequence: position,
                    },
                );
                reported = position;
                last_report = Instant::now();

                if !outbound.try_push(frame.into()) {
                    outbound.close();
                    return;
                }
            }

            tokio::time::sleep(IDLE_POLL).await;
            continue;
        }

        let directory = data_dir.clone();
        let after = position;
        let mut cursor = std::mem::take(&mut reader);
        let read = tokio::task::spawn_blocking(move || {
            let read = read_frames_resuming(
                &mut cursor,
                &directory,
                shard,
                after,
                through,
                limits,
                READ_BYTES,
            );
            (read, cursor)
        })
        .await
        .map(|(read, cursor)| {
            reader = cursor;
            read
        });

        let frames = match read {
            Ok(Ok(FrameRead::Frames(frames))) => frames,
            Ok(Ok(FrameRead::Gap | FrameRead::Ahead | FrameRead::Local)) => {
                resync(&outbound, resp3, "cursor is no longer retained");
                return;
            }
            _ => {
                resync(&outbound, resp3, "change log is unreadable");
                return;
            }
        };

        if frames.is_empty() {
            tokio::time::sleep(IDLE_POLL).await;
            continue;
        }

        let mut batch = Vec::new();

        for frame in &frames {
            let Ok(record) = decode_record(frame, shard, limits) else {
                resync(&outbound, resp3, "change log is unreadable");
                return;
            };
            let cursor = ChangeCursor {
                shard,
                sequence: record.sequence,
            };

            changes::describe(&record.mutation, &mut |event| {
                if subscription.accepts(event.kind, event.key) {
                    // A move's value is the cursor to continue from, never optional.
                    let value = (subscription.values || event.kind == changes::ChangeKind::Moved)
                        .then_some(event.value)
                        .flatten();

                    changes::encode_event(
                        &mut batch,
                        resp3,
                        cursor,
                        record.database,
                        event.kind,
                        event.key,
                        value.as_deref(),
                    );
                }
            });
            position = record.sequence;
        }

        if !batch.is_empty() {
            reported = position;
            last_report = Instant::now();

            if !outbound.try_push(batch.into()) {
                outbound.close();
                return;
            }
        }
    }
}

fn resync(outbound: &Outbound, resp3: bool, reason: &str) {
    let mut frame = Vec::new();

    changes::encode_resync(&mut frame, resp3, reason);
    let _ = outbound.try_push(frame.into());
    // The connection writes queued frames before it honours `closed`, so the notice is sent.
    outbound.close();
}
