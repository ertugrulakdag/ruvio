use super::*;

pub trait ListWaiterSink {
    fn deliver(&mut self, database: u8, key: &[u8], value: &[u8]) -> bool;

    /// The last WAL record of the write whose deliveries follow, or `None` when it wrote
    /// none. A served client whose element came with a record that never reaches the disk
    /// must be refused with that write, so its reply waits for that record's sync.
    fn served_by(&mut self, _sequence: Option<u64>) {}

    /// False when no client is parked at all, so a write can skip the waiter lookup.
    fn has_waiters(&self) -> bool {
        true
    }

    /// `Some(max)` when the oldest `BZPOPMIN`/`BZPOPMAX` client parked on `key` wants the
    /// lowest (`false`) or highest (`true`) member.
    fn sorted_waiter(&self, database: u8, key: &[u8]) -> Option<bool>;

    fn deliver_sorted(&mut self, database: u8, key: &[u8], member: &[u8], score: f64) -> bool;

    fn deliver_stream(
        &mut self,
        database: u8,
        key: &[u8],
        millis: u64,
        sequence: u64,
        fields: &[(&[u8], &[u8])],
    ) -> bool;

    fn next_blocked_group(
        &self,
        database: u8,
        key: &[u8],
        millis: u64,
        sequence: u64,
        claimed: &[Vec<u8>],
    ) -> Option<crate::list_waiters::BlockedGroup>;

    fn deliver_group(
        &mut self,
        database: u8,
        key: &[u8],
        group: &[u8],
        millis: u64,
        sequence: u64,
        fields: &[(&[u8], &[u8])],
    ) -> bool;

    fn release_group(&mut self, database: u8, key: &[u8], group: &[u8]);

    /// Holds data handed to blocked clients from here until `release_deliveries`, so a
    /// write whose record fails delivers nothing.
    fn hold_deliveries(&mut self) {}

    /// Sends what the matching hold kept (`delivered`), or parks its clients again.
    fn release_deliveries(&mut self, _delivered: bool) {}
}

impl ListWaiterSink for () {
    fn deliver(&mut self, _database: u8, _key: &[u8], _value: &[u8]) -> bool {
        false
    }

    fn has_waiters(&self) -> bool {
        false
    }

    fn sorted_waiter(&self, _database: u8, _key: &[u8]) -> Option<bool> {
        None
    }

    fn deliver_sorted(&mut self, _database: u8, _key: &[u8], _member: &[u8], _score: f64) -> bool {
        false
    }

    fn deliver_stream(
        &mut self,
        _database: u8,
        _key: &[u8],
        _millis: u64,
        _sequence: u64,
        _fields: &[(&[u8], &[u8])],
    ) -> bool {
        false
    }

    fn next_blocked_group(
        &self,
        _database: u8,
        _key: &[u8],
        _millis: u64,
        _sequence: u64,
        _claimed: &[Vec<u8>],
    ) -> Option<crate::list_waiters::BlockedGroup> {
        None
    }

    fn deliver_group(
        &mut self,
        _database: u8,
        _key: &[u8],
        _group: &[u8],
        _millis: u64,
        _sequence: u64,
        _fields: &[(&[u8], &[u8])],
    ) -> bool {
        false
    }

    fn release_group(&mut self, _database: u8, _key: &[u8], _group: &[u8]) {}
}

impl ListWaiterSink for crate::list_waiters::ListWaiters<SyncSender<Vec<u8>>> {
    fn deliver(&mut self, database: u8, key: &[u8], value: &[u8]) -> bool {
        if self.is_empty() {
            return false;
        }

        let waiter = self.take_for_key(&tagged(database, key));

        hand_over(self, waiter, || block_hit_response(key, value))
    }

    fn has_waiters(&self) -> bool {
        !self.is_empty()
    }

    fn sorted_waiter(&self, database: u8, key: &[u8]) -> Option<bool> {
        if self.is_empty() {
            return None;
        }

        self.oldest_sorted(&tagged(database, key))
    }

    fn deliver_sorted(&mut self, database: u8, key: &[u8], member: &[u8], score: f64) -> bool {
        let waiter = self.take_sorted_for_key(&tagged(database, key));

        hand_over(self, waiter, || sorted_block_hit(key, member, score))
    }

    fn deliver_stream(
        &mut self,
        database: u8,
        key: &[u8],
        millis: u64,
        sequence: u64,
        fields: &[(&[u8], &[u8])],
    ) -> bool {
        if self.is_empty() {
            return false;
        }

        let waiter = self.take_stream_for_append(&tagged(database, key), millis, sequence);

        hand_over(self, waiter, || {
            stream_block_hit(key, millis, sequence, fields)
        })
    }

    fn next_blocked_group(
        &self,
        database: u8,
        key: &[u8],
        millis: u64,
        sequence: u64,
        claimed: &[Vec<u8>],
    ) -> Option<crate::list_waiters::BlockedGroup> {
        if self.is_empty() {
            return None;
        }

        self.oldest_blocked_group(&tagged(database, key), millis, sequence, claimed)
    }

    fn deliver_group(
        &mut self,
        database: u8,
        key: &[u8],
        group: &[u8],
        millis: u64,
        sequence: u64,
        fields: &[(&[u8], &[u8])],
    ) -> bool {
        if self.is_empty() {
            return false;
        }

        let waiter = self.take_group(&tagged(database, key), group);

        hand_over(self, waiter, || {
            stream_block_hit(key, millis, sequence, fields)
        })
    }

    fn release_group(&mut self, database: u8, key: &[u8], group: &[u8]) {
        release_blocked_group(self, &tagged(database, key), group);
    }

    fn hold_deliveries(&mut self) {
        self.hold();
    }

    fn release_deliveries(&mut self, delivered: bool) {
        for (reply, payload) in self.release(delivered) {
            reply.complete(payload);
        }
    }
}

impl ListWaiterSink for crate::list_waiters::ListWaiters<crate::list_waiters::EncodedReplySlot> {
    fn deliver(&mut self, database: u8, key: &[u8], value: &[u8]) -> bool {
        if self.is_empty() {
            return false;
        }

        let waiter = self.take_for_key(&tagged(database, key));

        hand_over(self, waiter, || block_hit_response(key, value))
    }

    fn has_waiters(&self) -> bool {
        !self.is_empty()
    }

    fn sorted_waiter(&self, database: u8, key: &[u8]) -> Option<bool> {
        if self.is_empty() {
            return None;
        }

        self.oldest_sorted(&tagged(database, key))
    }

    fn deliver_sorted(&mut self, database: u8, key: &[u8], member: &[u8], score: f64) -> bool {
        let waiter = self.take_sorted_for_key(&tagged(database, key));

        hand_over(self, waiter, || sorted_block_hit(key, member, score))
    }

    fn deliver_stream(
        &mut self,
        database: u8,
        key: &[u8],
        millis: u64,
        sequence: u64,
        fields: &[(&[u8], &[u8])],
    ) -> bool {
        if self.is_empty() {
            return false;
        }

        let waiter = self.take_stream_for_append(&tagged(database, key), millis, sequence);

        hand_over(self, waiter, || {
            stream_block_hit(key, millis, sequence, fields)
        })
    }

    fn next_blocked_group(
        &self,
        database: u8,
        key: &[u8],
        millis: u64,
        sequence: u64,
        claimed: &[Vec<u8>],
    ) -> Option<crate::list_waiters::BlockedGroup> {
        if self.is_empty() {
            return None;
        }

        self.oldest_blocked_group(&tagged(database, key), millis, sequence, claimed)
    }

    fn deliver_group(
        &mut self,
        database: u8,
        key: &[u8],
        group: &[u8],
        millis: u64,
        sequence: u64,
        fields: &[(&[u8], &[u8])],
    ) -> bool {
        if self.is_empty() {
            return false;
        }

        let waiter = self.take_group(&tagged(database, key), group);

        hand_over(self, waiter, || {
            stream_block_hit(key, millis, sequence, fields)
        })
    }

    fn release_group(&mut self, database: u8, key: &[u8], group: &[u8]) {
        release_blocked_group(self, &tagged(database, key), group);
    }

    fn hold_deliveries(&mut self) {
        self.hold();
    }

    fn release_deliveries(&mut self, delivered: bool) {
        let served_by = self.take_serving_sequence();
        let released = self.release(delivered);

        if !released.is_empty() {
            self.note_delivered(served_by);
        }
        for (reply, payload) in released {
            reply.mark_served_by(served_by);
            reply.complete(payload);
        }
    }

    fn served_by(&mut self, sequence: Option<u64>) {
        self.set_serving_sequence(sequence);
    }
}

/// Hands a new stream entry to every `XREAD BLOCK` client parked on `key` whose cursor is
/// behind it; each reply waits for the record `served_by` like the write's own. Consumer
/// groups are served apart, one reader per group.
pub fn wake_stream_readers<W: ListWaiterSink>(
    waiters: &mut W,
    served_by: Option<u64>,
    database: u8,
    key: &[u8],
    millis: u64,
    sequence: u64,
    fields: &[(&[u8], &[u8])],
) {
    loop {
        waiters.served_by(served_by);
        if !waiters.deliver_stream(database, key, millis, sequence, fields) {
            break;
        }
    }
}

/// Wakes the readers a replicated record serves. A replica refuses writes, so its parked
/// clients are `XREAD BLOCK` readers only, and the primary's appends reach them here.
/// `stored` says whether an entry is still in its stream once the whole record applied (a
/// trim in the same record may have dropped it), as the primary checks before serving.
pub fn wake_replicated_readers<W: ListWaiterSink>(
    waiters: &mut W,
    database: u8,
    mutation: &crate::OwnedMutation,
    stored: &mut impl FnMut(&[u8], u64, u64) -> bool,
) {
    match mutation {
        crate::OwnedMutation::StreamAppend {
            key,
            millis,
            sequence,
            fields,
        } if stored(key, *millis, *sequence) => {
            let pairs: Vec<(&[u8], &[u8])> = fields
                .iter()
                .map(|(field, value)| (field.as_slice(), value.as_slice()))
                .collect();

            wake_stream_readers(waiters, None, database, key, *millis, *sequence, &pairs);
        }
        crate::OwnedMutation::Transaction { mutations } => {
            for mutation in mutations {
                wake_replicated_readers(waiters, database, mutation, stored);
            }
        }
        _ => {}
    }
}

/// The record a write ended on, when it wrote one since `before` (`written_sequence`).
pub(crate) fn written_since(before: Option<u64>, after: Option<u64>) -> Option<u64> {
    after.filter(|after| before != Some(*after))
}

/// Sends `waiter` its reply, or keeps it while deliveries are held.
fn hand_over<R: CompleteListWaiter>(
    waiters: &mut crate::list_waiters::ListWaiters<R>,
    waiter: Option<crate::list_waiters::ListWaiter<R>>,
    payload: impl FnOnce() -> Vec<u8>,
) -> bool {
    let Some(waiter) = waiter else {
        return false;
    };

    if waiters.holding() {
        waiters.hold_delivery(waiter, payload());
    } else {
        let served_by = waiters.take_serving_sequence();

        waiters.note_delivered(served_by);
        waiter.reply.stamp(served_by);
        waiter.reply.complete(payload());
    }
    true
}

pub(super) fn release_blocked_group<R: CompleteListWaiter>(
    waiters: &mut crate::list_waiters::ListWaiters<R>,
    key: &[u8],
    group: &[u8],
) {
    while let Some(waiter) = waiters.take_group(key, group) {
        waiter
            .reply
            .complete(b"-NOGROUP No such key or consumer group\r\n".to_vec());
    }
}

pub(super) trait CompleteListWaiter {
    fn complete(self, payload: Vec<u8>);

    fn stamp(&self, _served_by: Option<u64>) {}
}

impl CompleteListWaiter for SyncSender<Vec<u8>> {
    fn complete(self, payload: Vec<u8>) {
        let _ = self.send(payload);
    }
}

impl CompleteListWaiter for crate::list_waiters::EncodedReplySlot {
    fn complete(self, payload: Vec<u8>) {
        self.fill(payload);
    }

    fn stamp(&self, served_by: Option<u64>) {
        self.mark_served_by(served_by);
    }
}

pub fn encode_null_array() -> Vec<u8> {
    b"*-1\r\n".to_vec()
}

pub(super) fn sorted_block_hit(key: &[u8], member: &[u8], score: f64) -> Vec<u8> {
    let mut output = Vec::new();

    write_response(
        &mut output,
        Response::Array(vec![
            Some(Cow::Borrowed(key)),
            Some(Cow::Borrowed(member)),
            Some(Cow::Owned(format_score(score))),
        ]),
    )
    .expect("writing to a Vec cannot fail");
    output
}

pub fn block_hit_response(key: &[u8], value: &[u8]) -> Vec<u8> {
    let mut output = Vec::new();

    write_response(
        &mut output,
        Response::Array(vec![
            Some(Cow::Owned(key.to_vec())),
            Some(Cow::Owned(value.to_vec())),
        ]),
    )
    .expect("encoding a two-bulk array into memory cannot fail");
    output
}

pub fn stream_block_hit(
    key: &[u8],
    millis: u64,
    sequence: u64,
    fields: &[(&[u8], &[u8])],
) -> Vec<u8> {
    let mut field_bulks = Vec::with_capacity(fields.len().saturating_mul(2));

    for (field, value) in fields {
        field_bulks.push(Some(Cow::Owned((*field).to_vec())));
        field_bulks.push(Some(Cow::Owned((*value).to_vec())));
    }

    let response = Response::Exec(vec![Response::Exec(vec![
        Response::Bulk(Some(Cow::Owned(key.to_vec()))),
        // The entries of the key form a list, as in a reply that did not block.
        Response::Exec(vec![Response::Exec(vec![
            Response::Bulk(Some(Cow::Owned(crate::stream::format_id(millis, sequence)))),
            Response::Array(field_bulks),
        ])]),
    ])]);
    let mut output = Vec::new();

    write_response(&mut output, response).expect("encoding a stream read into memory cannot fail");
    output
}
