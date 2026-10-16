use super::*;

impl<C: Clock, I: StorageIndex> KvShard<C, I> {
    pub fn stream_next_id(&mut self, key: &[u8], now_millis: u64) -> Result<(u64, u64), WrongType> {
        self.remove_expired(key);

        match self.unexpired_object_value(key)? {
            None => Ok(crate::stream::StreamValue::new().next_auto_id(now_millis)),
            Some(Value::Stream(stream)) => Ok(stream.next_auto_id(now_millis)),
            Some(_) => Err(WrongType),
        }
    }

    pub fn stream_next_at(
        &mut self,
        key: &[u8],
        millis: u64,
    ) -> Result<(u64, u64), StreamWriteError> {
        self.remove_expired(key);

        match self.unexpired_object_value(key)? {
            None => Ok((millis, 0)),
            Some(Value::Stream(stream)) => stream
                .next_seq_at(millis)
                .map(|sequence| (millis, sequence))
                .ok_or(StreamWriteError::Id),
            Some(_) => Err(StreamWriteError::WrongType),
        }
    }

    pub fn stream_can_append(
        &mut self,
        key: &[u8],
        millis: u64,
        sequence: u64,
    ) -> Result<(), StreamWriteError> {
        self.remove_expired(key);

        match self.unexpired_object_value(key)? {
            None => Ok(()),
            Some(Value::Stream(stream)) => {
                if stream.accepts(millis, sequence) {
                    Ok(())
                } else {
                    Err(StreamWriteError::Id)
                }
            }

            Some(_) => Err(StreamWriteError::WrongType),
        }
    }

    pub fn stream_contains(
        &mut self,
        key: &[u8],
        millis: u64,
        sequence: u64,
    ) -> Result<bool, WrongType> {
        self.remove_expired(key);

        match self.unexpired_object_value(key)? {
            None => Ok(false),
            Some(Value::Stream(stream)) => Ok(stream.contains(millis, sequence)),
            Some(_) => Err(WrongType),
        }
    }

    pub fn stream_append(
        &mut self,
        key: &[u8],
        millis: u64,
        sequence: u64,
        fields: &[(impl AsRef<[u8]>, impl AsRef<[u8]>)],
    ) -> Result<(), StreamWriteError> {
        self.remove_expired(key);
        let before = self.entry_charge(key);
        let result = match self.entries.object_mut(key).map(|entry| &mut entry.value) {
            None => {
                let mut stream = crate::stream::StreamValue::new();

                stream
                    .append(millis, sequence, fields)
                    .map_err(|_| StreamWriteError::Id)?;
                self.insert_entry(key, Entry::fresh(Value::Stream(Box::new(stream))));
                Ok(())
            }

            Some(Value::Stream(stream)) => stream
                .append(millis, sequence, fields)
                .map_err(|_| StreamWriteError::Id),
            Some(_) => Err(StreamWriteError::WrongType),
        };

        self.reconcile_entry_charge(before, key);
        result
    }

    pub fn stream_delete(
        &mut self,
        key: &[u8],
        millis: u64,
        sequence: u64,
    ) -> Result<bool, WrongType> {
        self.remove_expired(key);
        let before = self.entry_charge(key);
        let deleted = match self.entries.object_mut(key).map(|entry| &mut entry.value) {
            None => return Ok(false),
            Some(Value::Stream(stream)) => stream.delete(millis, sequence),
            Some(_) => return Err(WrongType),
        };

        self.reconcile_entry_charge(before, key);
        Ok(deleted)
    }

    pub fn stream_trim(&mut self, key: &[u8], max_len: usize) -> Result<usize, WrongType> {
        self.remove_expired(key);
        let before = self.entry_charge(key);
        let removed = match self.entries.object_mut(key).map(|entry| &mut entry.value) {
            None => return Ok(0),
            Some(Value::Stream(stream)) => {
                let before_len = stream.len();

                stream.trim(max_len);
                before_len.saturating_sub(stream.len())
            }

            Some(_) => return Err(WrongType),
        };

        self.reconcile_entry_charge(before, key);
        Ok(removed)
    }

    pub fn stream_trim_min_id(
        &mut self,
        key: &[u8],
        millis: u64,
        sequence: u64,
    ) -> Result<usize, WrongType> {
        self.remove_expired(key);
        let before = self.entry_charge(key);
        let removed = match self.entries.object_mut(key).map(|entry| &mut entry.value) {
            None => return Ok(0),
            Some(Value::Stream(stream)) => stream.trim_before(millis, sequence),
            Some(_) => return Err(WrongType),
        };

        self.reconcile_entry_charge(before, key);
        Ok(removed)
    }

    pub(crate) fn stream_set_id_check(
        &mut self,
        key: &[u8],
        millis: u64,
        sequence: u64,
    ) -> Result<(), StreamSetIdError> {
        self.remove_expired(key);

        match self.unexpired_object_value(key) {
            Ok(None) => Err(StreamSetIdError::Missing),
            Ok(Some(Value::Stream(stream))) if stream.last_id_is_too_small(millis, sequence) => {
                Err(StreamSetIdError::TooSmall)
            }

            Ok(Some(Value::Stream(_))) => Ok(()),
            Ok(Some(_)) | Err(_) => Err(StreamSetIdError::WrongType),
        }
    }

    /// Runs `edit` on the stream at `key` through `edit_object`: a successful edit is charged
    /// and marks the key changed like any other write, so no stream operation can skip that.
    /// `None` for a missing or expired key.
    fn edit_stream<R, E>(
        &mut self,
        key: &[u8],
        wrong_type: E,
        edit: impl FnOnce(&mut crate::stream::StreamValue) -> Result<R, E>,
    ) -> Option<Result<R, E>> {
        self.edit_object(key, |entry| match &mut entry.value {
            Value::Stream(stream) => {
                let result = edit(stream);
                let changed = result.is_ok();

                (result, changed)
            }
            _ => (Err(wrong_type), false),
        })
    }

    pub(crate) fn stream_set_id(
        &mut self,
        key: &[u8],
        millis: u64,
        sequence: u64,
    ) -> Result<(), StreamSetIdError> {
        self.edit_stream(key, StreamSetIdError::WrongType, |stream| {
            stream
                .set_last_id(millis, sequence)
                .map_err(|_| StreamSetIdError::TooSmall)
        })
        .unwrap_or(Err(StreamSetIdError::Missing))
    }

    pub(crate) fn stream_overview(
        &mut self,
        key: &[u8],
    ) -> Result<crate::stream::StreamOverview, StreamGroupError> {
        self.stream_view(key, |stream| stream.overview())
    }

    pub(crate) fn stream_group_overviews(
        &mut self,
        key: &[u8],
    ) -> Result<Vec<crate::stream::GroupOverview>, StreamGroupError> {
        self.stream_view(key, |stream| stream.group_overviews())
    }

    pub(crate) fn stream_consumer_overviews(
        &mut self,
        key: &[u8],
        group: &[u8],
        now: u64,
    ) -> Result<Vec<crate::stream::ConsumerOverview>, StreamGroupError> {
        self.remove_expired(key);

        match self.unexpired_object_value(key) {
            Ok(None) => Err(StreamGroupError::MissingKey),
            Ok(Some(Value::Stream(stream))) => stream
                .consumer_overviews(group, now)
                .map_err(|_| StreamGroupError::NoGroup),
            Ok(Some(_)) | Err(WrongType) => Err(StreamGroupError::WrongType),
        }
    }

    pub(super) fn stream_view<T>(
        &mut self,
        key: &[u8],
        view: impl FnOnce(&crate::stream::StreamValue) -> T,
    ) -> Result<T, StreamGroupError> {
        self.remove_expired(key);

        match self.unexpired_object_value(key) {
            Ok(None) => Err(StreamGroupError::MissingKey),
            Ok(Some(Value::Stream(stream))) => Ok(view(stream)),
            Ok(Some(_)) | Err(WrongType) => Err(StreamGroupError::WrongType),
        }
    }

    pub fn stream_len(&mut self, key: &[u8]) -> Result<usize, WrongType> {
        self.remove_expired(key);
        match self.unexpired_object_value(key)? {
            None => Ok(0),
            Some(Value::Stream(stream)) => Ok(stream.len()),
            Some(_) => Err(WrongType),
        }
    }

    pub fn stream_range(
        &mut self,
        key: &[u8],
        start: crate::stream::StreamBound,
        end: crate::stream::StreamBound,
        count: Option<usize>,
        reverse: bool,
    ) -> Result<Vec<crate::stream::StreamEntry>, WrongType> {
        self.remove_expired(key);
        match self.unexpired_object_value(key)? {
            None => Ok(Vec::new()),
            Some(Value::Stream(stream)) => {
                Ok(stream.range_page(start, end, count.unwrap_or(usize::MAX), reverse))
            }

            Some(_) => Err(WrongType),
        }
    }

    pub fn stream_read(
        &mut self,
        key: &[u8],
        cursor: crate::stream::StreamCursor,
        limit: usize,
    ) -> Result<Vec<crate::stream::StreamEntry>, WrongType> {
        self.remove_expired(key);

        match self.unexpired_object_value(key)? {
            None => Ok(Vec::new()),
            Some(Value::Stream(stream)) => Ok(stream.read_after(cursor, limit)),
            Some(_) => Err(WrongType),
        }
    }

    pub fn stream_end(&mut self, key: &[u8]) -> Result<(u64, u64), WrongType> {
        self.remove_expired(key);

        match self.unexpired_object_value(key)? {
            None => Ok((0, 0)),
            Some(Value::Stream(stream)) => {
                let (started, millis, sequence) = stream.high_water();

                if started {
                    Ok((millis, sequence))
                } else {
                    Ok((0, 0))
                }
            }

            Some(_) => Err(WrongType),
        }
    }

    pub fn stream_group_can_create(
        &mut self,
        key: &[u8],
        name: &[u8],
        mkstream: bool,
    ) -> Result<(), StreamGroupError> {
        self.remove_expired(key);

        match self.unexpired_object_value(key)? {
            None => {
                if mkstream {
                    Ok(())
                } else {
                    Err(StreamGroupError::MissingKey)
                }
            }

            Some(Value::Stream(stream)) => {
                if stream.has_group(name) {
                    Err(StreamGroupError::Busy)
                } else {
                    Ok(())
                }
            }

            Some(_) => Err(StreamGroupError::WrongType),
        }
    }

    pub fn stream_create_group(
        &mut self,
        key: &[u8],
        name: &[u8],
        last_millis: u64,
        last_sequence: u64,
        mkstream: bool,
    ) -> Result<(), StreamGroupError> {
        self.remove_expired(key);
        let before = self.entry_charge(key);
        let result = match self.entries.object_mut(key).map(|entry| &mut entry.value) {
            None => {
                if !mkstream {
                    Err(StreamGroupError::MissingKey)
                } else {
                    let mut stream = crate::stream::StreamValue::new();

                    stream
                        .create_group(name, last_millis, last_sequence)
                        .map_err(stream_group_error)?;
                    self.insert_entry(key, Entry::fresh(Value::Stream(Box::new(stream))));
                    Ok(())
                }
            }

            Some(Value::Stream(stream)) => stream
                .create_group(name, last_millis, last_sequence)
                .map_err(stream_group_error),
            Some(_) => Err(StreamGroupError::WrongType),
        };

        self.reconcile_entry_charge(before, key);
        result
    }

    pub(crate) fn stream_destroy_group(
        &mut self,
        key: &[u8],
        name: &[u8],
    ) -> Result<bool, StreamGroupError> {
        self.remove_expired(key);
        let before = self.entry_charge(key);
        let result = match self.entries.object_mut(key).map(|entry| &mut entry.value) {
            None => Ok(false),
            Some(Value::Stream(stream)) => Ok(stream.destroy_group(name)),
            Some(_) => Err(StreamGroupError::WrongType),
        };

        self.reconcile_entry_charge(before, key);
        result
    }

    pub(crate) fn stream_set_group_id(
        &mut self,
        key: &[u8],
        name: &[u8],
        millis: u64,
        sequence: u64,
    ) -> Result<(), StreamGroupError> {
        self.edit_stream(key, StreamGroupError::WrongType, |stream| {
            stream
                .set_group_id(name, millis, sequence)
                .map_err(stream_group_error)
        })
        .unwrap_or(Err(StreamGroupError::NoGroup))
    }

    pub(crate) fn stream_create_consumer(
        &mut self,
        key: &[u8],
        name: &[u8],
        consumer: &[u8],
    ) -> Result<bool, StreamGroupError> {
        self.remove_expired(key);
        let before = self.entry_charge(key);
        let result = match self.entries.object_mut(key).map(|entry| &mut entry.value) {
            None => Err(StreamGroupError::NoGroup),
            Some(Value::Stream(stream)) => stream
                .create_consumer(name, consumer)
                .map_err(stream_group_error),
            Some(_) => Err(StreamGroupError::WrongType),
        };

        self.reconcile_entry_charge(before, key);
        result
    }

    pub(crate) fn stream_delete_consumer(
        &mut self,
        key: &[u8],
        name: &[u8],
        consumer: &[u8],
    ) -> Result<usize, StreamGroupError> {
        self.remove_expired(key);
        let before = self.entry_charge(key);
        let result = match self.entries.object_mut(key).map(|entry| &mut entry.value) {
            None => Err(StreamGroupError::NoGroup),
            Some(Value::Stream(stream)) => stream
                .delete_consumer(name, consumer)
                .map_err(stream_group_error),
            Some(_) => Err(StreamGroupError::WrongType),
        };

        self.reconcile_entry_charge(before, key);
        result
    }

    pub(crate) fn stream_group_advance(
        &mut self,
        key: &[u8],
        name: &[u8],
        millis: u64,
        sequence: u64,
    ) -> Result<(), StreamGroupError> {
        self.edit_stream(key, StreamGroupError::WrongType, |stream| {
            stream
                .advance_group(name, millis, sequence)
                .map_err(stream_group_error)
        })
        .unwrap_or(Err(StreamGroupError::NoGroup))
    }

    pub fn stream_group_new(
        &mut self,
        key: &[u8],
        name: &[u8],
        limit: usize,
    ) -> Result<Vec<crate::stream::StreamEntry>, StreamGroupError> {
        let now = self.now_unix_millis().get();

        // A read that delivers nothing and drops no elapsed hold changes nothing: it must
        // not dirty a WATCH on the stream.
        self.edit_object(key, |entry| match &mut entry.value {
            Value::Stream(stream) => {
                let held = stream.held_count();
                let result = stream
                    .new_for_group(name, limit, now)
                    .map_err(stream_group_error);
                let changed = result.as_ref().is_ok_and(|entries| !entries.is_empty())
                    || stream.held_count() != held;

                (result, changed)
            }
            _ => (Err(StreamGroupError::WrongType), false),
        })
        .unwrap_or(Err(StreamGroupError::NoGroup))
    }

    pub(crate) fn stream_hold_until(
        &mut self,
        key: &[u8],
        millis: u64,
        sequence: u64,
        ready_at: u64,
    ) -> Result<(), WrongType> {
        self.edit_stream(key, WrongType, |stream| {
            stream.hold_until(millis, sequence, ready_at);
            Ok(())
        })
        .unwrap_or(Err(WrongType))
    }

    pub fn stream_group_claim(
        &mut self,
        key: &[u8],
        name: &[u8],
        consumer: &[u8],
        millis: u64,
        sequence: u64,
        delivered_at: u64,
    ) -> Result<(), StreamGroupError> {
        self.remove_expired(key);
        let before = self.entry_charge(key);
        let result = match self.entries.object_mut(key).map(|entry| &mut entry.value) {
            None => Err(StreamGroupError::NoGroup),
            Some(Value::Stream(stream)) => stream
                .claim(name, consumer, millis, sequence, delivered_at)
                .map_err(stream_group_error),
            Some(_) => Err(StreamGroupError::WrongType),
        };

        self.reconcile_entry_charge(before, key);
        result
    }

    pub(crate) fn stream_group_reclaim(
        &mut self,
        key: &[u8],
        name: &[u8],
        consumer: &[u8],
        update: crate::stream::ReclaimUpdate,
    ) -> Result<bool, StreamGroupError> {
        self.remove_expired(key);
        let before = self.entry_charge(key);
        let result = match self.entries.object_mut(key).map(|entry| &mut entry.value) {
            None => Err(StreamGroupError::NoGroup),
            Some(Value::Stream(stream)) => stream
                .reclaim(name, consumer, update)
                .map_err(stream_group_error),

            Some(_) => Err(StreamGroupError::WrongType),
        };

        self.reconcile_entry_charge(before, key);
        result
    }

    pub(crate) fn stream_group_force_claim(
        &mut self,
        key: &[u8],
        name: &[u8],
        consumer: &[u8],
        update: crate::stream::ReclaimUpdate,
    ) -> Result<(), StreamGroupError> {
        self.remove_expired(key);
        let before = self.entry_charge(key);
        let result = match self.entries.object_mut(key).map(|entry| &mut entry.value) {
            None => Err(StreamGroupError::NoGroup),
            Some(Value::Stream(stream)) => stream
                .upsert_pending(name, consumer, update)
                .map_err(stream_group_error),

            Some(_) => Err(StreamGroupError::WrongType),
        };

        self.reconcile_entry_charge(before, key);
        result
    }

    pub(crate) fn stream_pending_range(
        &mut self,
        key: &[u8],
        name: &[u8],
        filter: crate::stream::PendingFilter<'_>,
    ) -> Result<Vec<crate::stream::PendingRow>, StreamGroupError> {
        self.remove_expired(key);

        match self.unexpired_object_value(key)? {
            None => Err(StreamGroupError::NoGroup),
            Some(Value::Stream(stream)) => stream
                .pending_rows(name, filter)
                .map_err(stream_group_error),

            Some(_) => Err(StreamGroupError::WrongType),
        }
    }

    pub(crate) fn stream_autoclaim_plan(
        &mut self,
        key: &[u8],
        group: &[u8],
        start: (u64, u64),
        min_idle: u64,
        count: usize,
        now: u64,
    ) -> Result<crate::stream::AutoclaimPlan, StreamGroupError> {
        self.remove_expired(key);

        match self.unexpired_object_value(key)? {
            None => Err(StreamGroupError::NoGroup),
            Some(Value::Stream(stream)) => stream
                .autoclaim_plan(group, start, min_idle, count, now)
                .map_err(stream_group_error),
            Some(_) => Err(StreamGroupError::WrongType),
        }
    }

    pub(crate) fn stream_pending_delivery(
        &mut self,
        key: &[u8],
        name: &[u8],
        millis: u64,
        sequence: u64,
    ) -> Result<Option<crate::stream::PendingDelivery>, StreamGroupError> {
        self.remove_expired(key);

        match self.unexpired_object_value(key)? {
            None => Err(StreamGroupError::NoGroup),
            Some(Value::Stream(stream)) => stream
                .pending_delivery(name, millis, sequence)
                .map_err(|_| StreamGroupError::NoGroup),

            Some(_) => Err(StreamGroupError::WrongType),
        }
    }

    pub(crate) fn stream_entry(
        &mut self,
        key: &[u8],
        millis: u64,
        sequence: u64,
    ) -> Result<Option<crate::stream::StreamEntry>, WrongType> {
        self.remove_expired(key);

        match self.unexpired_object_value(key)? {
            None => Ok(None),
            Some(Value::Stream(stream)) => Ok(stream.entry(millis, sequence)),
            Some(_) => Err(WrongType),
        }
    }

    pub fn stream_group_pending(
        &mut self,
        key: &[u8],
        name: &[u8],
        millis: u64,
        sequence: u64,
    ) -> Result<bool, StreamGroupError> {
        self.remove_expired(key);

        match self.unexpired_object_value(key)? {
            None => Err(StreamGroupError::NoGroup),
            Some(Value::Stream(stream)) => stream
                .is_pending(name, millis, sequence)
                .map_err(|_| StreamGroupError::NoGroup),

            Some(_) => Err(StreamGroupError::WrongType),
        }
    }

    pub(crate) fn stream_group_cursor(
        &mut self,
        key: &[u8],
        name: &[u8],
    ) -> Result<(u64, u64), StreamGroupError> {
        self.remove_expired(key);

        match self.unexpired_object_value(key)? {
            None => Err(StreamGroupError::NoGroup),
            Some(Value::Stream(stream)) => stream.group_cursor(name).map_err(stream_group_error),
            Some(_) => Err(StreamGroupError::WrongType),
        }
    }

    pub(crate) fn stream_group_summary(
        &mut self,
        key: &[u8],
        name: &[u8],
    ) -> Result<crate::stream::PendingSummary, StreamGroupError> {
        self.remove_expired(key);

        match self.unexpired_object_value(key)? {
            None => Err(StreamGroupError::NoGroup),
            Some(Value::Stream(stream)) => stream.pending_summary(name).map_err(stream_group_error),
            Some(_) => Err(StreamGroupError::WrongType),
        }
    }

    pub fn stream_group_ack(
        &mut self,
        key: &[u8],
        name: &[u8],
        millis: u64,
        sequence: u64,
    ) -> Result<bool, StreamGroupError> {
        self.remove_expired(key);
        let before = self.entry_charge(key);
        let result = match self.entries.object_mut(key).map(|entry| &mut entry.value) {
            None => Err(StreamGroupError::NoGroup),
            Some(Value::Stream(stream)) => stream
                .ack(name, millis, sequence)
                .map_err(stream_group_error),

            Some(_) => Err(StreamGroupError::WrongType),
        };

        self.reconcile_entry_charge(before, key);
        result
    }
}
