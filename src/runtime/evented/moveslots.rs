//! `RUVIO MOVESLOTS`: moves a slot range between the shards of this process (ADR 0014).
//!
//! The source shard copies the range to the target in bounded steps while it keeps serving
//! it, then sends the keys written meanwhile again until few are left. The hand-over is one
//! turn of the source's thread: it sends the last changes and a flip message, then waits
//! (bounded) while the target applies them, records the move and points the slot table at
//! itself. Nothing on the source runs in between, so no write can land behind the copy.
//! Commands that were routed to the source before the flip and run there later are checked
//! where they run (see [`SlotMover::moved`]). The source then drops its copy of the range in
//! bounded steps, without logging it: recovery routes by the `SLOTS` file.

use super::*;
use crate::protocol::SlotRangeMove;
use crate::routing::hash_slot;
use crate::slot_moves::SlotMoves;
use crate::store::SnapshotEntry;
use std::sync::Condvar;
use std::sync::atomic::AtomicBool;

/// Keys per copy message.
const COPY_BATCH_KEYS: usize = 256;
/// Index positions one walk step visits.
const WALK_STEP: usize = 1024;
/// Copy messages the target may have queued before the source waits.
const IN_FLIGHT: u64 = 4;
/// Catch-up rounds before the hand-over goes ahead whatever is still changing.
const CATCH_UP_ROUNDS: usize = 32;
/// Changed keys few enough to send in the hand-over turn itself.
const HAND_OVER_KEYS: usize = 512;
/// The longest the source waits for the target in the hand-over; past it the move is
/// cancelled.
const HAND_OVER_LIMIT: Duration = Duration::from_secs(1);
/// Keys one clean-up step removes.
const CLEAN_UP_STEP: usize = 1024;

pub(super) const UNBLOCKED_ERROR: &[u8] =
    b"UNBLOCKED the key's slot moved to another shard while the client was blocked";
const UNBLOCKED_REPLY: &[u8] =
    b"-UNBLOCKED the key's slot moved to another shard while the client was blocked\r\n";
pub(super) const TRYAGAIN_REPLY: &[u8] =
    b"-TRYAGAIN the key's slot moved to another shard; send the command again\r\n";

/// Moves of this process: the one running, the last one, and the durable history.
#[derive(Default)]
pub(super) struct SlotMover {
    inner: Mutex<MoverState>,
    /// Set by the first hand-over: from then on a request routed before a hand-over may
    /// reach a shard that no longer owns its keys, so shards check forwarded requests and
    /// commands that waited. Never set in a process that moved nothing.
    moved: AtomicBool,
}

#[derive(Default)]
struct MoverState {
    /// The `SLOTS` history and the directory it lives in; `None` without durability.
    durable: Option<(SlotMoves, std::path::PathBuf)>,
    current: Option<Arc<MoveSession>>,
    last: Option<Report>,
}

impl SlotMover {
    /// Keeps the history loaded at start, so moves are recorded in `data_dir`.
    pub(super) fn set_durable(&self, history: SlotMoves, data_dir: std::path::PathBuf) {
        self.lock().durable = Some((history, data_dir));
    }

    /// Whether any hand-over happened in this process.
    #[inline]
    pub(super) fn moved(&self) -> bool {
        self.moved.load(Ordering::Acquire)
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, MoverState> {
        self.inner.lock().unwrap_or_else(|error| error.into_inner())
    }
}

/// One move, shared by its source and target shards.
pub(super) struct MoveSession {
    first: u16,
    last: u16,
    from: usize,
    to: usize,
    started: Instant,
    /// Copy messages sent and applied (the opening message counts as one).
    sent: AtomicU64,
    applied: AtomicU64,
    keys: AtomicU64,
    rounds: AtomicU64,
    progress: tokio::sync::Notify,
    outcome: Mutex<Outcome>,
    handed_over: Condvar,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum Outcome {
    Copying,
    Failed(String),
    /// The target owns the range; the source is dropping its copy.
    HandedOver {
        freeze: Duration,
    },
}

/// What `RUVIO MOVESLOTS STATUS` reports about a finished move.
#[derive(Clone, Debug)]
struct Report {
    first: u16,
    last: u16,
    from: usize,
    to: usize,
    state: &'static str,
    keys: u64,
    rounds: u64,
    freeze_ms: Option<f64>,
    elapsed_ms: f64,
    error: Option<String>,
}

impl MoveSession {
    fn outcome(&self) -> Outcome {
        self.outcome
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .clone()
    }

    /// Records a failure unless the move already ended; true when this call ended it.
    fn fail(&self, reason: impl Into<String>) -> bool {
        let mut outcome = self
            .outcome
            .lock()
            .unwrap_or_else(|error| error.into_inner());

        if *outcome != Outcome::Copying {
            return false;
        }

        *outcome = Outcome::Failed(reason.into());
        drop(outcome);
        self.handed_over.notify_all();
        self.progress.notify_one();
        true
    }

    fn contains(&self, key: &[u8]) -> bool {
        (self.first..=self.last).contains(&hash_slot(key))
    }

    fn report(&self, state: &'static str, error: Option<String>) -> Report {
        let freeze_ms = match self.outcome() {
            Outcome::HandedOver { freeze } => Some(freeze.as_secs_f64() * 1000.0),
            _ => None,
        };

        Report {
            first: self.first,
            last: self.last,
            from: self.from,
            to: self.to,
            state,
            keys: self.keys.load(Ordering::Acquire),
            rounds: self.rounds.load(Ordering::Acquire),
            freeze_ms,
            elapsed_ms: self.started.elapsed().as_secs_f64() * 1000.0,
            error,
        }
    }
}

/// One key's values in every database that holds it; an empty list removes the key.
pub(super) struct MovedKey {
    key: Box<[u8]>,
    entries: Vec<SnapshotEntry>,
}

pub(super) enum MoveMessage {
    /// The target records that a copy begins.
    Open(Arc<MoveSession>),
    Copy(Arc<MoveSession>, Vec<MovedKey>),
    /// The last copy is in; the target takes the range.
    Flip(Arc<MoveSession>),
    /// The move failed: the target drops what it copied.
    Cancel(Arc<MoveSession>),
    /// Starts a move on its source shard.
    Start(Arc<MoveSession>),
}

/// `RUVIO MOVESLOTS ...` on any shard of a multi-shard process.
pub(super) fn command(state: &Rc<ShardState>, target: Option<SlotRangeMove>) -> Response<'static> {
    let Some(forwarding) = state.forwarding.as_ref() else {
        return Response::Error(b"ERR RUVIO MOVESLOTS needs more than one shard");
    };
    let fabric = &forwarding.fabric;

    let Some(target) = target else {
        return status(&fabric.mover);
    };

    match start(state, forwarding, target) {
        Ok(()) => Response::Simple(b"OK"),
        Err(error) => Response::Error(error),
    }
}

fn start(
    state: &Rc<ShardState>,
    forwarding: &Forwarding,
    target: SlotRangeMove,
) -> Result<(), &'static [u8]> {
    let fabric = &forwarding.fabric;
    let to = usize::from(target.shard);

    if to >= fabric.shard_count() {
        return Err(b"ERR RUVIO MOVESLOTS no such shard");
    }
    if state.role.get() != crate::config::ReplicationRole::Off {
        return Err(b"ERR RUVIO MOVESLOTS is not supported while replication is on");
    }
    if state
        .persistence
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .profile()
        .uses_wal()
    {
        return Err(b"ERR RUVIO MOVESLOTS does not yet support a durability profile with a log");
    }

    let from = fabric.slot_owner(target.first);

    if (target.first..=target.last).any(|slot| fabric.slot_owner(slot) != from) {
        return Err(b"ERR RUVIO MOVESLOTS range has more than one owner; move each part");
    }
    if from == to {
        return Err(b"ERR RUVIO MOVESLOTS range is already on that shard");
    }

    let session = Arc::new(MoveSession {
        first: target.first,
        last: target.last,
        from,
        to,
        started: Instant::now(),
        sent: AtomicU64::new(0),
        applied: AtomicU64::new(0),
        keys: AtomicU64::new(0),
        rounds: AtomicU64::new(0),
        progress: tokio::sync::Notify::new(),
        outcome: Mutex::new(Outcome::Copying),
        handed_over: Condvar::new(),
    });

    {
        let mut mover = fabric.mover.lock();

        if mover.current.is_some() {
            return Err(b"ERR RUVIO MOVESLOTS another move is in progress");
        }
        mover.current = Some(Arc::clone(&session));
    }

    if from == forwarding.me {
        tokio::task::spawn_local(run_source(Rc::clone(state), session));
    } else {
        fabric.send(
            forwarding.me,
            from,
            Message::Move(MoveMessage::Start(session)),
        );
    }

    Ok(())
}

fn status(mover: &SlotMover) -> Response<'static> {
    let state = mover.lock();
    let report = match (&state.current, &state.last) {
        (Some(session), _) => Some(match session.outcome() {
            Outcome::Copying => session.report("copying", None),
            Outcome::HandedOver { .. } => session.report("cleaning", None),
            Outcome::Failed(reason) => session.report("cancelling", Some(reason)),
        }),
        (None, Some(last)) => Some(last.clone()),
        (None, None) => None,
    };
    let Some(report) = report else {
        return Response::Map(vec![(
            std::borrow::Cow::Borrowed(b"state".as_slice()),
            Response::Bulk(Some(std::borrow::Cow::Borrowed(b"none"))),
        )]);
    };
    let text = |value: String| Response::Bulk(Some(std::borrow::Cow::Owned(value.into_bytes())));
    let mut pairs = vec![
        ("state", text(report.state.to_string())),
        ("first", Response::Integer(i64::from(report.first))),
        ("last", Response::Integer(i64::from(report.last))),
        ("from", Response::Integer(report.from as i64)),
        ("to", Response::Integer(report.to as i64)),
        ("keys", Response::Integer(report.keys as i64)),
        ("rounds", Response::Integer(report.rounds as i64)),
        ("elapsed_ms", text(format!("{:.3}", report.elapsed_ms))),
    ];

    if let Some(freeze) = report.freeze_ms {
        pairs.push(("freeze_ms", text(format!("{freeze:.3}"))));
    }
    if let Some(error) = report.error {
        pairs.push(("error", text(error)));
    }

    Response::Map(
        pairs
            .into_iter()
            .map(|(name, value)| (std::borrow::Cow::Borrowed(name.as_bytes()), value))
            .collect(),
    )
}

/// A move message arriving on this shard's fabric loop.
pub(super) fn on_message(state: &Rc<ShardState>, message: MoveMessage) {
    match message {
        MoveMessage::Start(session) => {
            tokio::task::spawn_local(run_source(Rc::clone(state), session));
        }
        MoveMessage::Open(session) => applied(&session),
        MoveMessage::Copy(session, keys) => {
            if session.outcome() == Outcome::Copying {
                apply_copies(state, &session, keys);
            }
            applied(&session);
        }
        MoveMessage::Flip(session) => take_over(state, &session),
        MoveMessage::Cancel(session) => {
            tokio::task::spawn_local(forget_range(Rc::clone(state), session, "cancelled"));
        }
    }
}

fn applied(session: &MoveSession) {
    session.applied.fetch_add(1, Ordering::AcqRel);
    session.progress.notify_one();
}

/// Target: replaces each key with the source's values.
fn apply_copies(state: &ShardState, session: &MoveSession, keys: Vec<MovedKey>) {
    let mut store = state.store.borrow_mut();
    let databases = store.database_ids();
    let count = keys.len() as u64;

    for MovedKey { key, entries } in keys {
        for database in &databases {
            store.forget_key(*database, &key);
        }
        for entry in entries {
            if let Err(error) = store.restore_snapshot_entry(entry) {
                drop(store);
                session.fail(format!("target could not take a key: {error}"));
                return;
            }
        }
    }

    if store.memory_admission_would_fail(0) {
        drop(store);
        session.fail("the range does not fit under the target shard's memory limit");
        return;
    }

    session.keys.fetch_add(count, Ordering::AcqRel);
}

/// Target, in the hand-over: takes the range unless the source gave up meanwhile.
fn take_over(state: &Rc<ShardState>, session: &Arc<MoveSession>) {
    let Some(forwarding) = state.forwarding.as_ref() else {
        return;
    };
    let mut outcome = session
        .outcome
        .lock()
        .unwrap_or_else(|error| error.into_inner());

    if *outcome != Outcome::Copying {
        // The source timed out or the copy failed: the source sends the cancel.
        return;
    }

    let table = forwarding.fabric.slot_table();

    for slot in session.first..=session.last {
        table.assign(slot, session.to);
    }
    forwarding.fabric.mover.moved.store(true, Ordering::Release);
    *outcome = Outcome::HandedOver {
        freeze: Duration::ZERO,
    };
    drop(outcome);
    session.handed_over.notify_all();
}

/// Source: copies the range, hands it over, then drops its copy.
async fn run_source(state: Rc<ShardState>, session: Arc<MoveSession>) {
    let Some(forwarding) = state.forwarding.as_ref() else {
        return;
    };
    let fabric = Arc::clone(&forwarding.fabric);

    state
        .store
        .borrow_mut()
        .watch_slots(session.first, session.last);

    let result = copy_and_hand_over(&state, &fabric, &session).await;

    state.store.borrow_mut().unwatch_slots();

    match result {
        Ok(()) => {
            release_range_clients(&state, &session);
            forget_range(state, session, "done").await;
        }
        Err(reason) => {
            session.fail(reason);
            fabric.send(
                session.from,
                session.to,
                Message::Move(MoveMessage::Cancel(session)),
            );
        }
    }
}

async fn copy_and_hand_over(
    state: &Rc<ShardState>,
    fabric: &Arc<Fabric>,
    session: &Arc<MoveSession>,
) -> Result<(), String> {
    send(fabric, session, MoveMessage::Open(Arc::clone(session)));
    settle(session, 0).await?;

    // Every key of the range, database by database, in bounded steps.
    let databases = state.store.borrow().database_ids();
    let mut batch: Vec<MovedKey> = Vec::new();

    for database in databases {
        let mut cursor = 0;

        loop {
            let mut keys: Vec<Box<[u8]>> = Vec::new();

            cursor = state.store.borrow().walk_slots(
                database,
                session.first,
                session.last,
                cursor,
                WALK_STEP,
                &mut |key| keys.push(key.into()),
            );

            {
                let store = state.store.borrow();

                batch.extend(keys.into_iter().map(|key| MovedKey {
                    entries: store.key_entries(&key),
                    key,
                }));
            }

            if batch.len() >= COPY_BATCH_KEYS {
                send_copies(fabric, session, std::mem::take(&mut batch)).await?;
            }
            if cursor == 0 {
                break;
            }
            tokio::task::yield_now().await;
        }
    }
    send_copies(fabric, session, batch).await?;

    // Keys written since they were copied, until few are left.
    for _ in 0..CATCH_UP_ROUNDS {
        settle(session, 0).await?;

        let changes = state.store.borrow_mut().take_slot_changes();

        if changes.rewritten {
            return Err("a flush or SWAPDB ran during the move".into());
        }
        if changes.keys.len() <= HAND_OVER_KEYS {
            return hand_over(state, fabric, session, changes.keys);
        }

        session.rounds.fetch_add(1, Ordering::AcqRel);
        for chunk in changes.keys.chunks(COPY_BATCH_KEYS) {
            let copies = moved_keys(state, chunk);

            send_copies(fabric, session, copies).await?;
        }
    }

    settle(session, 0).await?;
    let changes = state.store.borrow_mut().take_slot_changes();

    if changes.rewritten {
        return Err("a flush or SWAPDB ran during the move".into());
    }
    hand_over(state, fabric, session, changes.keys)
}

fn moved_keys(state: &ShardState, keys: &[Box<[u8]>]) -> Vec<MovedKey> {
    let store = state.store.borrow();

    keys.iter()
        .map(|key| MovedKey {
            entries: store.key_entries(key),
            key: key.clone(),
        })
        .collect()
}

/// One synchronous turn: the last changes, the flip, and a bounded wait for the target.
/// No other task of this shard runs until it returns.
fn hand_over(
    state: &ShardState,
    fabric: &Arc<Fabric>,
    session: &Arc<MoveSession>,
    keys: Vec<Box<[u8]>>,
) -> Result<(), String> {
    let started = Instant::now();

    if !keys.is_empty() {
        let copies = moved_keys(state, &keys);

        send(
            fabric,
            session,
            MoveMessage::Copy(Arc::clone(session), copies),
        );
    }
    fabric.send(
        session.from,
        session.to,
        Message::Move(MoveMessage::Flip(Arc::clone(session))),
    );

    let outcome = session
        .outcome
        .lock()
        .unwrap_or_else(|error| error.into_inner());
    let (mut outcome, _) = session
        .handed_over
        .wait_timeout_while(outcome, HAND_OVER_LIMIT, |outcome| {
            *outcome == Outcome::Copying
        })
        .unwrap_or_else(|error| error.into_inner());

    match &mut *outcome {
        Outcome::HandedOver { freeze } => {
            *freeze = started.elapsed();
            Ok(())
        }
        Outcome::Failed(reason) => Err(reason.clone()),
        Outcome::Copying => {
            // Under the lock, so the target can no longer take the range.
            *outcome = Outcome::Failed("the target did not take the range in time".into());
            Err("the target did not take the range in time".into())
        }
    }
}

fn send(fabric: &Fabric, session: &Arc<MoveSession>, message: MoveMessage) {
    session.sent.fetch_add(1, Ordering::AcqRel);
    fabric.send(session.from, session.to, Message::Move(message));
}

async fn send_copies(
    fabric: &Fabric,
    session: &Arc<MoveSession>,
    keys: Vec<MovedKey>,
) -> Result<(), String> {
    if keys.is_empty() {
        return Ok(());
    }

    settle(session, IN_FLIGHT).await?;
    send(
        fabric,
        session,
        MoveMessage::Copy(Arc::clone(session), keys),
    );
    Ok(())
}

/// Waits until at most `behind` copy messages are still queued at the target.
async fn settle(session: &MoveSession, behind: u64) -> Result<(), String> {
    loop {
        let notified = session.progress.notified();

        if let Outcome::Failed(reason) = session.outcome() {
            return Err(reason);
        }
        if session.applied.load(Ordering::Acquire) + behind >= session.sent.load(Ordering::Acquire)
        {
            return Ok(());
        }
        notified.await;
    }
}

/// Source, after the hand-over: clients waiting on keys of the range here would wait for
/// writes that now go to the target.
fn release_range_clients(state: &ShardState, session: &MoveSession) {
    let in_range = |key: &[u8]| session.contains(key);

    for waiter in state.waiters.borrow_mut().take_where(in_range) {
        waiter.fill(UNBLOCKED_REPLY.to_vec());
    }

    // A WATCH on a moved key fails its EXEC, and a retried blocking command wakes to find
    // its key gone from this shard.
    state
        .store
        .borrow_mut()
        .dirty_watches_in_slots(session.first, session.last);

    let tracked = state
        .tracker
        .borrow()
        .tracked_where(|key| session.contains(key));

    if !tracked.is_empty() {
        let invalidations = state.tracker.borrow_mut().invalidate(&tracked);

        super::hot::push_invalidations(state, invalidations);
    }
}

/// Removes this shard's keys of the range in bounded steps, then ends the move: the source
/// after a hand-over, the target after a cancel.
async fn forget_range(state: Rc<ShardState>, session: Arc<MoveSession>, ended: &'static str) {
    let databases = state.store.borrow().database_ids();

    for database in databases {
        let mut cursor = 0;

        loop {
            let mut keys: Vec<Box<[u8]>> = Vec::new();

            cursor = state.store.borrow().walk_slots(
                database,
                session.first,
                session.last,
                cursor,
                CLEAN_UP_STEP,
                &mut |key| keys.push(key.into()),
            );

            {
                let mut store = state.store.borrow_mut();

                for key in &keys {
                    store.forget_key(database, key);
                }
            }

            if cursor == 0 {
                break;
            }
            tokio::task::yield_now().await;
        }
    }

    let Some(forwarding) = state.forwarding.as_ref() else {
        return;
    };
    let error = match session.outcome() {
        Outcome::Failed(reason) => Some(reason),
        _ => None,
    };
    let mut mover = forwarding.fabric.mover.lock();

    mover.last = Some(session.report(ended, error));
    if mover
        .current
        .as_ref()
        .is_some_and(|current| Arc::ptr_eq(current, &session))
    {
        mover.current = None;
    }
}

/// With [`SlotMover::moved`] set: whether `command`, about to run on this shard, belongs to
/// a slot another shard took over since it was routed.
pub(super) fn runs_elsewhere(
    state: &ShardState,
    session: &TxSession,
    command: &Command<'_>,
) -> bool {
    let Some(forwarding) = state.forwarding.as_ref() else {
        return false;
    };
    let slot = if matches!(command, Command::Exec) {
        match transaction_slot(session.queued()) {
            Ok(Some(slot)) => Some(slot),
            _ => session.watch_slot(),
        }
    } else if session.is_multi() {
        // Queued, not run: EXEC decides.
        None
    } else {
        // As routing decides it: a hot key never leaves, a split stays refused here.
        return matches!(
            super::forward::command_owner(state, forwarding, command).0,
            super::forward::Owner::Remote(_)
        );
    };

    slot.is_some_and(|slot| forwarding.fabric.slot_owner(slot) != forwarding.me)
}

/// The shard every keyed frame of a forwarded request now belongs to, when that is not
/// this one: the request goes there whole. `None` runs it here.
pub(super) fn request_owner(state: &ShardState, frames: &[u8], count: u32) -> Option<usize> {
    let forwarding = state.forwarding.as_ref()?;
    let limits = state.published_config.protocol;
    let mut consumed = 0;
    let mut owner = None;

    for _ in 0..count {
        let (command, length) = parse_command_with_limits(&frames[consumed..], limits).ok()??;

        consumed += length;
        let Some(slot) = command_slot(&command).ok().flatten() else {
            continue;
        };
        let found = forwarding.fabric.slot_owner(slot);

        match owner {
            None => owner = Some(found),
            Some(previous) if previous != found => return None,
            Some(_) => {}
        }
    }

    owner.filter(|owner| *owner != forwarding.me)
}
