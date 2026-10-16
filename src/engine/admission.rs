//! Write-path admission: type checks, memory limits, expiry logging and eviction.

use super::*;

pub(super) const ADMISSION_OVERHEAD_BYTES: usize = 64;

/// The reply to a write refused for lack of memory under `maxmemory`.
pub(crate) const OOM_ERROR: &[u8] = b"OOM command not allowed when used memory > 'maxmemory'.";

fn oom() -> Response<'static> {
    Response::Error(OOM_ERROR)
}

pub(super) fn admit_stream_write<C: Clock, I: StorageIndex, S: MutationSink>(
    shard: &mut KvShard<C, I>,
    sink: &mut S,
    key: &[u8],
    key_len: usize,
    recorded: &mut bool,
) -> Result<Option<Response<'static>>, S::Error> {
    if *recorded {
        return Ok(None);
    }

    if let Some(rejected) = before_mutation(shard, sink, [key], key_len + ADMISSION_OVERHEAD_BYTES)?
    {
        return Ok(Some(rejected));
    }

    *recorded = true;
    Ok(None)
}

pub(super) fn before_mutation<'a, C: Clock, I: StorageIndex, S: MutationSink>(
    shard: &mut KvShard<C, I>,
    sink: &mut S,
    keys: impl IntoIterator<Item = &'a [u8], IntoIter: Clone>,
    extra_bytes: usize,
) -> Result<Option<Response<'static>>, S::Error> {
    let keys = keys.into_iter();

    // A written key that is stored but expired is treated as absent; replay keeps expired
    // keys, so its delete is logged before the write that follows.
    if sink.persists() && shard.write_may_find_expired() {
        for key in keys.clone() {
            shard.reap_expired(key);
        }
    }

    let admitted = admit_mutation(shard, sink, keys, extra_bytes)?;

    // Expiry may also have run while making room; log those deletes before the write too.
    if admitted.is_none() {
        record_removals(shard, sink)?;
    }

    Ok(admitted)
}

/// Whether a full store refuses a write: the `denyoom` flag of the command table.
#[derive(Clone, Copy)]
pub(super) enum OnOom {
    Refuse,
    Allow,
}

/// [`before_mutation`] or [`before_mutation_allow_oom`], picked by `on_oom`.
pub(super) fn admit<'a, C: Clock, I: StorageIndex, S: MutationSink>(
    shard: &mut KvShard<C, I>,
    sink: &mut S,
    keys: impl IntoIterator<Item = &'a [u8], IntoIter: Clone>,
    extra_bytes: usize,
    on_oom: OnOom,
) -> Result<Option<Response<'static>>, S::Error> {
    match on_oom {
        OnOom::Refuse => before_mutation(shard, sink, keys, extra_bytes),
        OnOom::Allow => before_mutation_allow_oom(shard, sink, keys, extra_bytes).map(|()| None),
    }
}

/// [`before_mutation`] for a command Redis does not flag `denyoom`: one that removes data
/// (`DEL`, `HDEL`, `LPOP`, ...) or only moves or rewrites it (`RENAME`, `TAKE`). Expired keys
/// are reaped, snapshot copies taken and an evicting policy still makes room first, but a full
/// store never refuses it: deleting is how a `noeviction` store frees memory. Commands that
/// set a TTL or move a member (`SMOVE` may create the destination) keep
/// [`before_mutation`].
pub(super) fn before_mutation_allow_oom<'a, C: Clock, I: StorageIndex, S: MutationSink>(
    shard: &mut KvShard<C, I>,
    sink: &mut S,
    keys: impl IntoIterator<Item = &'a [u8], IntoIter: Clone>,
    extra_bytes: usize,
) -> Result<(), S::Error> {
    let keys = keys.into_iter();

    if sink.persists() && shard.write_may_find_expired() {
        for key in keys.clone() {
            shard.reap_expired(key);
        }
    }

    if shard.eviction_policy().evicts() {
        // The refusal is dropped; the eviction it may have run is kept, as in Redis.
        let _ = admit_mutation(shard, sink, keys, extra_bytes)?;
    } else {
        shard.prepare_index_insert();
        shard.reclaim_pool_for_admission(extra_bytes);

        for key in keys {
            shard.capture_snapshot_key(key);
        }
    }

    record_removals(shard, sink)
}

fn admit_mutation<'a, C: Clock, I: StorageIndex, S: MutationSink>(
    shard: &mut KvShard<C, I>,
    sink: &mut S,
    keys: impl Iterator<Item = &'a [u8]>,
    extra_bytes: usize,
) -> Result<Option<Response<'static>>, S::Error> {
    shard.prepare_index_insert();
    shard.reclaim_pool_for_admission(extra_bytes);

    if !shard.eviction_policy().evicts() {
        if (shard.write_exceeds_shard_budget(extra_bytes)
            || shard.memory_admission_would_fail(extra_bytes))
            && !shard.follows_primary()
        {
            return Ok(Some(oom()));
        }

        if !shard.journal_is_open() {
            return Ok(None);
        }
    }

    let protected: Vec<&[u8]> = keys.into_iter().collect();

    for key in &protected {
        shard.capture_snapshot_key(key);
    }

    if shard.write_exceeds_shard_budget(extra_bytes) && !shard.follows_primary() {
        return Ok(Some(oom()));
    }

    if shard.memory_admission_would_fail(extra_bytes) {
        // A replica's memory follows the primary's deletes: it never evicts on its own
        // or refuses what the primary already wrote (Redis `replica-ignore-maxmemory`).
        if shard.follows_primary() {
            return Ok(None);
        }

        if !shard.eviction_policy().evicts() {
            return Ok(Some(oom()));
        }

        shard.expire_cycle(EVICTION_TURN_BUDGET);
        shard.reclaim_pool_for_admission(extra_bytes);

        if !shard.memory_admission_would_fail(extra_bytes) {
            return Ok(None);
        }

        let all_keys = !shard.eviction_policy().volatile_only();

        // A write the stored data cannot make room for is refused before any key goes.
        if all_keys && shard.bytes_over_limit(extra_bytes) > shard.evictable_bytes(&protected) {
            return Ok(Some(oom()));
        }

        let deadline = std::time::Instant::now() + EVICTION_TIME_BUDGET;
        // While maintenance pays a debt off in slices, a write within the bound frees
        // only its own size: the debt does not grow, and a pipeline of writes does not
        // spend the whole time budget on each one.
        let paying = all_keys
            && shard.eviction_debt()
            && !shard.logs_near_limit()
            && shard.eviction_debt_within_bound(EVICTION_DEBT_DIVISOR);
        let goal = shard
            .memory_usage()
            .charged_bytes()
            .saturating_sub(extra_bytes);
        let outcome = evict_until(shard, sink, &protected, deadline, |shard| {
            !shard.memory_admission_would_fail(extra_bytes)
                || (paying && shard.memory_usage().charged_bytes() <= goal)
        })?;

        if outcome == EvictOutcome::Fits {
            return Ok(None);
        }

        // Out of time with room still owed. Like Redis, an all-keys policy admits the
        // write and leaves the rest to maintenance, so one large write does not stall the
        // shard. The debt is bounded: a write is refused while the shard is already over
        // its limit by more than a share of it, so a burst of large writes cannot outrun
        // eviction without end; the overshoot stays under that share plus one write.
        if outcome == EvictOutcome::OutOfTime
            && all_keys
            && !shard.logs_near_limit()
            && shard.eviction_debt_within_bound(EVICTION_DEBT_DIVISOR)
        {
            shard.owe_eviction(protected.first().copied().unwrap_or_default());
            return Ok(None);
        }

        return Ok(Some(oom()));
    }

    Ok(None)
}

/// Logs a delete for each key expiry removed without one (see `KvShard::drain_removals`).
/// A transaction buffer takes those of its own database and holds them until it is
/// committed or rolled back.
#[inline]
pub(crate) fn record_removals<C: Clock, I: StorageIndex, S: MutationSink>(
    shard: &mut KvShard<C, I>,
    sink: &mut S,
) -> Result<(), S::Error> {
    if !sink.persists() || !shard.has_unlogged_removals() {
        return Ok(());
    }

    let active = shard.active_database();
    let buffered = !sink.switches_databases();
    let recorded = shard.drain_removals(buffered.then_some(active), buffered, |database, key| {
        let keys = [Cow::Borrowed(key)];

        sink.select_database(database);
        sink.record(Mutation::Delete { keys: &keys })
    });

    sink.select_database(active);
    recorded
}

/// A write whose eviction ran out of time is still admitted while the shard is at most
/// `limit / EVICTION_DEBT_DIVISOR` over its limit. A quarter keeps a burst's overshoot in
/// the order of the default reserved headroom, and maintenance pays a debt that size off
/// in well under a second.
pub(super) const EVICTION_DEBT_DIVISOR: usize = 4;

/// Per-write eviction stops after this long; what is still owed becomes an eviction
/// debt that maintenance pays off between requests (see [`evict_debt`]).
const EVICTION_TIME_BUDGET: std::time::Duration = std::time::Duration::from_millis(2);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum EvictOutcome {
    Fits,
    OutOfTime,
    NoVictim,
}

/// Evicts until `done` holds (`Fits`) or `deadline` passes. The clock is read once per
/// `EVICTION_TURN_BUDGET` victims.
fn evict_until<C: Clock, I: StorageIndex, S: MutationSink>(
    shard: &mut KvShard<C, I>,
    sink: &mut S,
    protected: &[&[u8]],
    deadline: std::time::Instant,
    done: impl Fn(&KvShard<C, I>) -> bool,
) -> Result<EvictOutcome, S::Error> {
    let mut evicted = 0usize;

    loop {
        if done(shard) {
            return Ok(EvictOutcome::Fits);
        }

        if evicted > 0
            && evicted.is_multiple_of(EVICTION_TURN_BUDGET)
            && std::time::Instant::now() >= deadline
        {
            return Ok(EvictOutcome::OutOfTime);
        }

        evicted += 1;

        let Some((database, key)) =
            shard.pick_eviction_victim(protected, sink.switches_databases())
        else {
            // A volatile search stops early in a backlog of passed deadlines; reaping
            // them frees memory too, and the next search starts past them.
            if shard.eviction_policy().volatile_only()
                && shard.expire_cycle(EVICTION_TURN_BUDGET) > 0
            {
                continue;
            }
            return Ok(EvictOutcome::NoVictim);
        };

        let keys = [Cow::Borrowed(key.as_ref())];
        let active = shard.active_database();

        sink.select_database(database);
        let recorded = sink.record(Mutation::Delete { keys: &keys });

        sink.select_database(active);
        recorded?;
        if !sink.switches_databases() {
            // A transaction buffer may be rolled back; the eviction stands either way,
            // so its delete is then logged on its own.
            shard.hold_removal(database, &key);
        }
        shard.apply_eviction(database, &key);
    }
}

/// Pays off an eviction debt for up to `max_time`. Returns true while some is still owed,
/// so the caller can come back as soon as requests have had their turn.
pub fn evict_debt<C: Clock, I: StorageIndex, S: MutationSink>(
    shard: &mut KvShard<C, I>,
    sink: &mut S,
    max_time: std::time::Duration,
) -> Result<bool, S::Error> {
    if !shard.eviction_debt() {
        return Ok(false);
    }

    if shard.follows_primary() {
        shard.clear_eviction_debt();
        return Ok(false);
    }

    let written = shard.eviction_debt_key().map(<[u8]>::to_vec);
    let protected: Vec<&[u8]> = written.as_deref().into_iter().collect();
    let deadline = std::time::Instant::now() + max_time;
    let owed = evict_until(shard, sink, &protected, deadline, |shard| {
        !shard.memory_admission_would_fail(0)
    })? == EvictOutcome::OutOfTime;

    if !owed {
        shard.clear_eviction_debt();
    }

    Ok(owed)
}

pub(super) fn accepts_type<C: Clock, I: StorageIndex>(
    shard: &KvShard<C, I>,
    key: &[u8],
    expected: ValueKind,
) -> bool {
    shard
        .value_kind(key)
        .is_none_or(|actual| actual == expected)
}
