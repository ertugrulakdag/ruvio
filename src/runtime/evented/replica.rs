use super::*;

pub(super) fn attach_replication(
    state: &ShardState,
    shard_id: u32,
) -> io::Result<Option<crate::replication::ReplicationThread>> {
    match state.published_config.replication.role {
        crate::config::ReplicationRole::Off => Ok(None),
        crate::config::ReplicationRole::Primary => {
            let Some(listen) = state.published_config.replication.listen else {
                return Ok(None);
            };

            let published = state
                .persistence
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .durable();
            let backlog_floor = state
                .persistence
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .backlog_floor();
            let replica_count = state
                .persistence
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .replica_count();

            Ok(Some(crate::replication::spawn_primary(
                crate::replication::Shipment {
                    listen: SocketAddr::new(
                        listen.ip(),
                        listen.port().wrapping_add(shard_id as u16),
                    ),
                    shard_id,
                    published,
                    data_dir: state.published_config.persistence.data_dir.clone(),
                    requirepass: state
                        .published_config
                        .security
                        .replication_secret()
                        .to_vec(),
                    limits: state.published_config.protocol,
                    max_replicas: crate::replication::MAX_REPLICAS,
                    backlog_floor,
                    replica_count,
                    acks: Arc::clone(&state.durable_acks),
                    fanout: state
                        .publish_fanout
                        .clone()
                        .unwrap_or_else(|| Arc::new(crate::replication::PublishFanout::new())),
                    snapshot_max_bytes: state.published_config.persistence.snapshot_max_bytes,
                    shard_count: state.published_config.shards as u32,
                    history: crate::replication::load_or_create_history(
                        &state.published_config.persistence.data_dir,
                    )?,
                    snapshot_requests: Arc::clone(&state.snapshot_requests),
                },
            )?))
        }

        crate::config::ReplicationRole::Replica => {
            let Some(primary) = state.published_config.replication.primary.or_else(|| {
                state
                    .published_config
                    .replication
                    .primary_name
                    .as_ref()
                    .map(|(_, port)| SocketAddr::new(std::net::Ipv4Addr::UNSPECIFIED.into(), *port))
            }) else {
                return Ok(None);
            };

            let primary_name = state.published_config.replication.primary_name.clone();

            let (inbound, outbound) = mpsc::sync_channel(crate::replication::QUEUE_FRAMES);
            let thread =
                crate::replication::spawn_replica_reader(crate::replication::ReplicaLink {
                    primary: SocketAddr::new(
                        primary.ip(),
                        primary.port().wrapping_add(shard_id as u16),
                    ),
                    primary_name,
                    shard_id,
                    wal: Arc::clone(&state.persistence),
                    inbound,
                    password: state
                        .published_config
                        .security
                        .replication_secret()
                        .to_vec(),
                    limits: state.published_config.protocol,
                    snapshot_max_bytes: state.published_config.persistence.snapshot_max_bytes,
                    resync_sequence: Arc::clone(&state.replica_resync_sequence),
                    data_dir: state.published_config.persistence.data_dir.clone(),
                    shard_count: state.published_config.shards as u32,
                    status: Arc::clone(&state.replica_link),
                    wake_shard: Arc::clone(&state.replica_wake),
                });

            *state.replica_frames.borrow_mut() = Some(outbound);
            Ok(Some(thread))
        }
    }
}

pub(super) fn install_replica_snapshot(
    state: &ShardState,
    shard: &[u8],
    hot: Option<&[u8]>,
    catchup_through: u64,
    history: &crate::replication::HistoryId,
) -> bool {
    let limits = state.published_config.protocol;
    let maximum = state.published_config.persistence.snapshot_max_bytes;
    let Ok(shard_checkpoint) = crate::persistence::decode_snapshot_blob(shard, limits, maximum)
    else {
        return false;
    };

    let hot_checkpoint = match hot {
        Some(bytes) => {
            let Ok(checkpoint) = crate::persistence::decode_snapshot_blob(bytes, limits, maximum)
            else {
                return false;
            };

            Some(checkpoint)
        }

        None => None,
    };

    let mut fresh = KvShard::with_clock(SystemClock::with_floor(
        state.store.borrow().now_unix_millis(),
    ));

    fresh.apply_memory_limits(state.published_config.shard_memory_limits());
    fresh.follow_primary(state.store.borrow().follows_primary());
    let replay_clock = fresh.pause_expiration_for_replay();

    let memory_entries = match crate::persistence::decode_snapshot_blob(shard, limits, maximum) {
        Ok(checkpoint) => checkpoint.entries,
        Err(_) => return false,
    };

    for entry in memory_entries {
        if fresh.restore_snapshot_entry(entry).is_err() {
            return false;
        }
    }
    fresh.resume_expiration_after_replay(replay_clock);

    let sequence = shard_checkpoint.last_sequence;
    let requested = state.replica_resync_sequence.load(Ordering::Acquire);
    if catchup_through < sequence
        || (requested != 0 && requested != u64::MAX && sequence < requested)
    {
        return false;
    }
    abandon_checkpoint_for_install(state);
    let mut published = vec![shard_checkpoint];

    if let Some(hot_checkpoint) = hot_checkpoint {
        published.push(hot_checkpoint);
    }

    // A round allocates its generation; with several shards the coordinator's round does.
    let mut generation = match &state.cluster {
        None => match state
            .checkpoints
            .allocate_generation(state.checkpoint_generation.get())
        {
            Ok(generation) => generation,
            Err(_) => return false,
        },
        Some(coordinator) => coordinator.published_generation().saturating_add(1),
    };
    let data_dir = &state.published_config.persistence.data_dir;
    let shard_id = state
        .route
        .as_ref()
        .map_or(0, |route| route.shard_id as u32);
    state.replica_incomplete.set(true);
    state.replica_caught_up.set(false);
    state.catchup_wait_started.set(Instant::now());
    if replica_guard::write(data_dir, shard_id, CatchupMarker::Preparing).is_err() {
        return false;
    }

    // From here the marker forces a full resync after any crash, so the log may claim the
    // primary's history before the new base replaces it.
    if crate::replication::record_upstream_history(data_dir, shard_id, history).is_err() {
        return false;
    }

    if state.cluster.is_none() && state.checkpoints.publish(generation, &published).is_err() {
        return false;
    }

    if state
        .persistence
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .install_base(sequence)
        .is_err()
    {
        return false;
    }
    // The primary's hot keys are in place before this shard's file can complete a round:
    // the coordinator writes the round's hot partition from the shared hot store.
    if let Some(store) = &state.hot_store
        && let Some(checkpoint) = published.get(1)
    {
        store.restore(checkpoint.entries.clone());
    }
    if let Some(coordinator) = &state.cluster {
        // A carried copy writes this shard's file of the round: it goes in first.
        super::checkpoint::collect_carry(state, true);
        let now = state.store.borrow().now_unix_millis();
        let shard_checkpoint = published.swap_remove(0);

        match coordinator.submit(shard_checkpoint, now) {
            Ok(round) => generation = round,
            Err(error) => {
                warn!(%error, "replica could not add its installed copy to a checkpoint round");
                return false;
            }
        }
    }

    if replica_guard::write(
        data_dir,
        shard_id,
        CatchupMarker::Installed {
            generation,
            snapshot: sequence,
            through: catchup_through,
        },
    )
    .is_err()
    {
        return false;
    }
    state.checkpoint_generation.set(generation);
    state.replica_catchup_through.set(catchup_through);
    *state.store.borrow_mut() = fresh;

    if requested != 0 {
        state.replica_resync_sequence.store(0, Ordering::Release);
    }
    true
}

pub(super) fn finish_replica_catchup(state: &ShardState) -> io::Result<()> {
    if !state.replica_caught_up.get() || state.replica_resync_sequence.load(Ordering::Acquire) != 0
    {
        return Ok(());
    }
    let shard_id = state
        .route
        .as_ref()
        .map_or(0, |route| route.shard_id as u32);
    let data_dir = &state.published_config.persistence.data_dir;
    let target = state.replica_catchup_through.get();
    let marker = replica_guard::read(data_dir, shard_id)?;
    let Some(CatchupMarker::Installed {
        generation,
        through,
        ..
    }) = marker
    else {
        return Err(io::Error::other(
            "replica catch-up marker is missing or uninstalled",
        ));
    };
    if generation != state.checkpoint_generation.get() || through != target {
        return Err(io::Error::other("replica catch-up checkpoint mismatch"));
    }
    if state
        .cluster
        .as_ref()
        .is_some_and(|coordinator| coordinator.published_generation() != generation)
    {
        return Ok(());
    }
    {
        let mut wal = state
            .persistence
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if wal.last_sequence() < target {
            return Ok(());
        }
        wal.flush()
            .map_err(|error| io::Error::other(error.to_string()))?;
    }
    replica_guard::clear(data_dir, shard_id)?;
    state.replica_catchup_through.set(0);
    state.replica_incomplete.set(false);
    state.replica_caught_up.set(false);
    Ok(())
}

pub(super) struct ReplicaReplayClock<'a> {
    pub(super) store: &'a RefCell<KvShard>,
    pub(super) previous: Option<u64>,
}

impl Drop for ReplicaReplayClock<'_> {
    fn drop(&mut self) {
        self.store
            .borrow()
            .resume_expiration_after_replay(self.previous);
    }
}

/// Every generic command calls this; on a primary there is no inbound stream, so the
/// check stays inline and the draining loop stays out of the dispatcher's code.
#[inline]
pub(super) fn drain_replicated(state: &ShardState) {
    if state.replica_frames.borrow().is_some() {
        drain_replicated_frames(state);
    }
}

/// Frames applied per drain. The reader thread refills the queue while it drains, so
/// without a bound a fast primary would keep the shard away from its own clients.
const REPLICA_DRAIN_BUDGET: usize = 256;

#[inline(never)]
fn drain_replicated_frames(state: &ShardState) {
    let taken = drain_replicated_budget(state, REPLICA_DRAIN_BUDGET);

    if taken == 0 {
        return;
    }

    if taken >= REPLICA_DRAIN_BUDGET {
        // More may be waiting: come back once other ready work has had its turn.
        state.replica_wake.notify_one();
    }

    if let Some(reader) = state.replica_reader.borrow().as_ref() {
        reader.unpark();
    }
}

/// Takes at most `budget` deliveries and returns how many it took.
fn drain_replicated_budget(state: &ShardState, budget: usize) -> usize {
    let mut taken = 0;

    while taken < budget {
        let delivery = {
            let mut slot = state.replica_frames.borrow_mut();
            let Some(inbound) = slot.as_mut() else {
                return taken;
            };

            match inbound.try_recv() {
                Ok(delivery) => delivery,
                Err(mpsc::TryRecvError::Empty) => return taken,
                Err(mpsc::TryRecvError::Disconnected) => {
                    *slot = None;

                    return taken;
                }
            }
        };
        taken += 1;

        let frame = match delivery {
            crate::replication::ReplicaDelivery::Snapshot {
                shard,
                hot,
                catchup_through,
                history,
            } => {
                if !install_replica_snapshot(
                    state,
                    &shard,
                    hot.as_deref(),
                    catchup_through,
                    &history,
                ) {
                    if state.replica_incomplete.get() {
                        state
                            .replica_resync_sequence
                            .store(u64::MAX, Ordering::Release);
                    }
                    if state.replica_resync_sequence.load(Ordering::Acquire) != 0 {
                        continue;
                    }
                    // Dropping the queue would end replication until a restart: ask for a
                    // newer copy instead, which skips the frames queued behind this one.
                    warn!("replica could not install the primary's snapshot; requesting another");
                    request_replica_resync(state);
                    continue;
                }

                continue;
            }

            crate::replication::ReplicaDelivery::CaughtUp(sequence) => {
                let target = state.replica_catchup_through.get();
                let applied = state
                    .persistence
                    .lock()
                    .unwrap_or_else(|error| error.into_inner())
                    .last_sequence();
                if state.replica_incomplete.get()
                    && state.replica_resync_sequence.load(Ordering::Acquire) == 0
                    && sequence >= target
                    && applied >= target
                {
                    state.replica_caught_up.set(true);
                    if let Err(error) = finish_replica_catchup(state) {
                        warn!(%error, "replica catch-up remains incomplete");
                    }
                }
                continue;
            }

            crate::replication::ReplicaDelivery::Frame(frame) => {
                if state.replica_resync_sequence.load(Ordering::Acquire) != 0 {
                    continue;
                }
                frame
            }
        };

        if let Some((sharded, channel, message)) = crate::replication::decode_copied_publish(&frame)
        {
            if sharded {
                publish_sharded(state, &channel, &message);
            } else {
                publish_classic(state, &channel, &message);
            }

            continue;
        }

        // Frames apply as the primary wrote them: a key past this replica's clock is
        // still there for the primary's next deadline, persist or delete.
        let _replay_clock = ReplicaReplayClock {
            store: &state.store,
            previous: state.store.borrow().pause_expiration_for_replay(),
        };
        let limits = state.published_config.protocol;
        let applied = {
            let mut wal = state
                .persistence
                .lock()
                .unwrap_or_else(|error| error.into_inner());

            crate::replication::accept_replica_frame(
                &mut state.store.borrow_mut(),
                &mut wal,
                &state.checkpoints,
                &state.published_config.persistence,
                &frame,
                limits,
            )
        };

        match applied {
            Ok((database, mutation, skip_expired)) => {
                let track_plan = crate::tracking::TrackPlan::replicated(database, &mutation);
                let hot = database == 0
                    && state.hot_store.as_ref().is_some_and(|store| {
                        crate::hot_store::apply_owned(
                            store,
                            &state.published_config.hot_keys,
                            &mutation,
                        )
                    });

                if !hot
                    && !skip_expired
                    && let Err(error) =
                        apply_mutation(&mut state.store.borrow_mut(), database, &mutation)
                {
                    // The frame is already in this replica's log but not in memory: the
                    // two no longer agree with the primary. Restarting the link would
                    // resume after the frame and keep the difference, so take a full copy
                    // instead, and mark the shard so a crash before it lands resyncs too.
                    warn!(%error, "replica WAL frame could not be applied; requesting a full resync");
                    let shard_id = state
                        .route
                        .as_ref()
                        .map_or(0, |route| route.shard_id as u32);
                    let _ = replica_guard::write(
                        &state.published_config.persistence.data_dir,
                        shard_id,
                        CatchupMarker::Preparing,
                    );
                    state.replica_incomplete.set(true);
                    state.replica_caught_up.set(false);
                    state
                        .replica_resync_sequence
                        .store(u64::MAX, Ordering::Release);
                    continue;
                }

                if !state.waiters.borrow().is_empty() {
                    let mut store = state.store.borrow_mut();
                    let previous = store.active_database();

                    store.enter(database);
                    crate::engine::wake_replicated_readers(
                        &mut *state.waiters.borrow_mut(),
                        database,
                        &mutation,
                        &mut |key, millis, sequence| {
                            store
                                .stream_contains(key, millis, sequence)
                                .unwrap_or(false)
                        },
                    );
                    store.enter(previous);
                }
                bump_mutations(state);
                flush_key_notices(state);
                observe_tracking(state, 0, false, track_plan, true);
            }

            Err(crate::replication::ApplyFailure::Persist(error)) => {
                // The frame is not applied and the next one requests a snapshot. After a
                // failed fsync this repeats until a restart while reads serve older data: say so, at most
                // every ten seconds (INFO also reports the link down).
                if state
                    .replica_persist_warned
                    .get()
                    .is_none_or(|at| at.elapsed() >= Duration::from_secs(10))
                {
                    state.replica_persist_warned.set(Some(Instant::now()));
                    warn!(
                        %error,
                        "replica cannot log the primary's frames; it serves older data until \
                         the log recovers (after a failed fsync: until restart)"
                    );
                }
            }
            // A frame an earlier link queued, sent again by the link that replaced it.
            Err(crate::replication::ApplyFailure::Applied) => {}
            Err(crate::replication::ApplyFailure::NeedsSnapshot) => {
                let sequence = request_replica_resync(state);
                warn!(
                    sequence,
                    "replica lacks the key a frame edits (JSON path, deadline or PERSIST); \
                     requesting a newer primary snapshot"
                );
            }
            Err(failure) => {
                // A gap (a frame refused before it, such as after a failed log write), a bad
                // frame or another shard's frame: this log cannot continue from the stream.
                // Resuming would hit the same frame again, and dropping the queue would end
                // replication for good, so take a copy that covers the next sequence.
                let sequence = request_replica_resync(state);
                warn!(
                    ?failure,
                    sequence, "replica frame refused; requesting a newer primary snapshot"
                );
            }
        }
    }

    taken
}

/// Asks the reader for a primary snapshot that covers the next sequence; the frames queued
/// until it arrives are skipped. Returns that sequence.
fn request_replica_resync(state: &ShardState) -> u64 {
    let sequence = state
        .persistence
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .last_sequence()
        .saturating_add(1);
    // The reader may have asked for a full copy (u64::MAX) meanwhile; keep any request
    // already pending instead of narrowing it.
    match state.replica_resync_sequence.compare_exchange(
        0,
        sequence,
        Ordering::AcqRel,
        Ordering::Acquire,
    ) {
        Ok(_) => sequence,
        Err(pending) => pending,
    }
}

pub(super) async fn wait_for_durable_replicas(
    state: &ShardState,
    forwarded_shards: u64,
    replicas: u64,
    timeout_ms: u64,
    shutdown: &ShutdownToken,
) -> u64 {
    if state.published_config.replication.role != crate::config::ReplicationRole::Primary {
        return 0;
    }

    // In single-port mode a client's writes may sit on any shard: wait for each shard's
    // replicas to reach that shard's last sequence, and count the replicas all of them reached.
    // In per-shard mode a cluster client sends WAIT to the shard it wrote on, as with Redis
    // Cluster, so that shard's replicas count, plus those of every shard this connection had
    // commands forwarded to (a client that is not cluster-aware writes through one port).
    let shards = match state.forwarding.as_ref() {
        Some(forwarding) if state.single_port => forwarding.fabric.durability(),
        Some(forwarding) if forwarded_shards != 0 => {
            let mut shards = forwarding.fabric.durability_of(forwarded_shards & !(1 << forwarding.me));

            shards.push(ShardDurability {
                wal: Arc::clone(&state.persistence),
                acks: Arc::clone(&state.durable_acks),
            });
            shards
        }
        _ => vec![ShardDurability {
            wal: Arc::clone(&state.persistence),
            acks: Arc::clone(&state.durable_acks),
        }],
    };
    let targets: Vec<(Arc<crate::replication::DurableAcks>, u64)> = shards
        .into_iter()
        .map(|shard| {
            let target = shard
                .wal
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .last_sequence();

            (shard.acks, target)
        })
        .collect();
    let reached = || {
        targets
            .iter()
            .map(|(acks, target)| acks.reached(*target))
            .min()
            .unwrap_or(0)
    };
    let deadline = (timeout_ms > 0).then(|| Instant::now() + Duration::from_millis(timeout_ms));

    loop {
        if shutdown.is_requested() {
            return reached();
        }

        let reached_now = reached();

        if reached_now >= replicas {
            return reached_now;
        }

        if let Some(deadline) = deadline
            && Instant::now() >= deadline
        {
            return reached();
        }

        tokio::select! {
            _ = state.durable_acks.notified() => {}
            _ = tokio::time::sleep(Duration::from_millis(20)) => {}
        }
    }
}
