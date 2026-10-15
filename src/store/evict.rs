use super::*;

/// Most deadline slots one volatile victim search inspects. Deadlines already past are
/// not victims, so with a large expiry backlog a search would otherwise walk them all.
pub(super) const VOLATILE_SCAN_SLOTS: usize = 4096;

impl<C: Clock, I: StorageIndex> KvShard<C, I> {
    /// Under a memory limit, a tombstone-saturated index is rehashed in place
    /// instead of letting the next insert double its allocation.
    pub(crate) fn prepare_index_insert(&mut self) {
        if self.memory_limit.is_some() {
            self.entries.reclaim_before_growth();
        }
    }

    pub(crate) fn reclaim_pool_for_admission(&mut self, extra_bytes: usize) {
        if self.byte_pool.cached_bytes > 0 && self.memory_admission_would_fail(extra_bytes) {
            self.byte_pool.clear();
            self.memory_usage.pooled_bytes = 0;
        }
    }

    pub(crate) fn write_exceeds_shard_budget(&self, extra_bytes: usize) -> bool {
        self.memory_limit.is_some_and(|limit| extra_bytes > limit)
    }

    /// Rotates across database/index slots; sparse candidates may require a full pass.
    /// `exclude` names keys of the active database.
    pub(crate) fn pick_eviction_victim(
        &self,
        exclude: &[&[u8]],
        other_databases: bool,
    ) -> Option<DatabaseKey> {
        if !self.eviction_policy.evicts() {
            return None;
        }

        // Every sampled and pooled candidate is scored against the current time: read
        // the clock once per pick, not once per candidate.
        let sampled = self.cached_now.get().is_none();
        if sampled {
            self.sample_time();
        }
        if !other_databases {
            self.keep_pool_database();
        }

        let victim = if self.eviction_policy.volatile_only() {
            self.pick_volatile_victim(exclude, other_databases)
        } else {
            self.pick_any_victim(exclude, other_databases)
        };

        if sampled {
            self.clear_time_sample();
        }
        victim
    }

    fn pick_any_victim(&self, exclude: &[&[u8]], other_databases: bool) -> Option<DatabaseKey> {
        let timed = self.expiration.keys_with_ttl > 0;
        let maps = || {
            self.database_maps()
                .take(if other_databases { usize::MAX } else { 1 })
        };
        let total: usize = maps().map(|(_, map)| map.eviction_slots()).sum();
        if total == 0 {
            return None;
        }
        let start = self.eviction_cursor.get() % total;
        let mut inspected = 0;
        let mut sampled = 0;

        'scan: for (mut skip, mut remaining) in [(start, total - start), (0, start)] {
            for (db, map) in maps() {
                let slots = map.eviction_slots();
                if skip >= slots {
                    skip -= slots;
                    continue;
                }
                let take = remaining.min(slots - skip);
                let complete = if timed {
                    self.offer_samples(
                        db,
                        skip,
                        map.eviction_keys(skip, take).map(|key| {
                            let key = key?;
                            if self.deadline_passed(db, key)
                                || (db == self.active && exclude.contains(&key))
                            {
                                return None;
                            }
                            Some(self.eviction_score(db, key, self.deadline(db, key)))
                        }),
                        &mut inspected,
                        &mut sampled,
                    )
                } else {
                    // Without deadlines a slot is scored from its metadata alone, with no
                    // key read.
                    let excluded = if db == self.active { exclude } else { &[] };

                    self.offer_samples(
                        db,
                        skip,
                        map.eviction_access(skip, take, excluded)
                            .map(|access| access.map(|access| self.access_score(access, None))),
                        &mut inspected,
                        &mut sampled,
                    )
                };
                if complete {
                    break 'scan;
                }
                remaining -= take;
                skip = 0;
                if remaining == 0 {
                    break;
                }
            }
        }

        self.eviction_cursor
            .set(start.wrapping_add(inspected) % total);
        self.record_eviction_scan(inspected);
        self.take_eviction_victim(exclude, other_databases, |db, slot| {
            let (_, map) = self.database_maps().find(|&(id, _)| id == db)?;
            let key = map.eviction_keys(slot, 1).next().flatten()?;

            Some((key, self.deadline(db, key)))
        })
    }

    /// Offers the scored slots of `scores` (from slot `skip` of `db`; `None` for a slot
    /// that holds no candidate) to the pool. True once the sample is complete.
    fn offer_samples(
        &self,
        db: u8,
        skip: usize,
        scores: impl Iterator<Item = Option<u64>>,
        inspected: &mut usize,
        sampled: &mut usize,
    ) -> bool {
        for (offset, score) in scores.enumerate() {
            *inspected += 1;
            let Some(score) = score else { continue };
            self.offer_eviction_candidate(db, skip + offset, score);
            *sampled += 1;
            if *sampled == self.eviction_sample_size {
                return true;
            }
        }
        false
    }

    /// Volatile policies sample the per-database deadline lookups, so the cost
    /// depends on timed keys rather than the key index. Hash bucket order keeps
    /// samples uncorrelated with insertion order.
    pub(super) fn pick_volatile_victim(
        &self,
        exclude: &[&[u8]],
        other_databases: bool,
    ) -> Option<DatabaseKey> {
        if self.expiration.keys_with_ttl == 0 {
            self.record_eviction_scan(0);
            return None;
        }

        let rings: Vec<(u8, &DeadlineDb)> = self
            .database_maps()
            .take(if other_databases { usize::MAX } else { 1 })
            .filter_map(|(db, _)| {
                self.deadlines
                    .get(db as usize)
                    .filter(|deadlines| deadlines.len > 0)
                    .map(|deadlines| (db, deadlines))
            })
            .collect();
        let total: usize = rings
            .iter()
            .map(|(_, deadlines)| deadlines.lookup.num_buckets())
            .sum();
        if total == 0 {
            self.record_eviction_scan(0);
            return None;
        }
        let start = self.eviction_cursor.get() % total;
        let now = self.now_unix_millis();
        let mut inspected = 0;
        let mut sampled = 0;

        'scan: for (mut skip, mut remaining) in [(start, total - start), (0, start)] {
            for &(db, deadlines) in &rings {
                let slots = deadlines.lookup.num_buckets();
                if skip >= slots {
                    skip -= slots;
                    continue;
                }
                let take = remaining.min(slots - skip);
                for slot in skip..skip + take {
                    if inspected == VOLATILE_SCAN_SLOTS {
                        break 'scan;
                    }
                    inspected += 1;
                    let Some(&id) = deadlines.lookup.get_bucket(slot) else {
                        continue;
                    };
                    let record = deadlines.records[id as usize]
                        .as_ref()
                        .expect("live deadline");
                    if record.at <= now {
                        continue;
                    }
                    let key = match deadlines.owned_key(id) {
                        Some(owned) => owned,
                        None => match index_key_bytes(
                            &self.entries,
                            &self.databases,
                            self.active,
                            db,
                            record.address,
                        ) {
                            Some(key) => key,
                            None => continue,
                        },
                    };
                    if db == self.active && exclude.contains(&key) {
                        continue;
                    }
                    let score = self.eviction_score(db, key, Some(record.at));
                    self.offer_eviction_candidate(db, slot, score);
                    sampled += 1;
                    if sampled == self.eviction_sample_size {
                        break 'scan;
                    }
                }
                remaining -= take;
                skip = 0;
                if remaining == 0 {
                    break;
                }
            }
        }

        self.eviction_cursor
            .set(start.wrapping_add(inspected) % total);
        self.record_eviction_scan(inspected);
        self.take_eviction_victim(exclude, other_databases, |db, slot| {
            let deadlines = self.deadlines.get(db as usize)?;
            let &id = deadlines.lookup.get_bucket(slot)?;
            let record = deadlines.records[id as usize].as_ref()?;
            let key = match deadlines.owned_key(id) {
                Some(owned) => owned,
                None => index_key_bytes(
                    &self.entries,
                    &self.databases,
                    self.active,
                    db,
                    record.address,
                )?,
            };

            Some((key, Some(record.at)))
        })
    }

    fn record_eviction_scan(&self, inspected: usize) {
        let (scans, slots) = self.eviction_scan.get();

        self.eviction_scan.set((
            scans.saturating_add(1),
            slots.saturating_add(inspected as u64),
        ));
    }

    pub(crate) fn apply_eviction(&mut self, database: u8, key: &[u8]) {
        let previous = self.active;

        self.enter(database);

        if self.delete_internal(key, false, crate::notify::KeyEvent::Evicted) {
            self.memory_stats.evicted_keys = self.memory_stats.evicted_keys.saturating_add(1);
        }

        self.enter(previous);
    }

    pub fn set_aux_memory(
        &mut self,
        connection_bytes: usize,
        output_bytes: usize,
        persistence_bytes: usize,
    ) {
        self.memory_usage.connection_bytes = connection_bytes;
        self.memory_usage.output_bytes = output_bytes;
        self.memory_usage.persistence_bytes = persistence_bytes;
    }

    /// Bytes to free before a write of `extra_bytes` fits the memory limit.
    pub(crate) fn bytes_over_limit(&self, extra_bytes: usize) -> usize {
        self.memory_limit.map_or(0, |limit| {
            self.memory_usage
                .charged_bytes()
                .saturating_add(extra_bytes)
                .saturating_sub(limit)
        })
    }

    /// Bytes an all-keys policy could free: every stored key and value except `protected`
    /// (keys of the active database). Index and buffer bytes do not shrink with eviction.
    pub(crate) fn evictable_bytes(&self, protected: &[&[u8]]) -> usize {
        let usage = self.memory_usage;
        let held: usize = protected
            .iter()
            .filter_map(|key| self.key_memory_usage(key))
            .sum();

        usage
            .key_bytes
            .saturating_add(usage.value_bytes)
            .saturating_add(usage.collection_bytes)
            .saturating_sub(held)
    }

    /// True when a value or key log is close to its address limit; evicting small keys
    /// may not open the page-sized span the next write needs.
    pub(crate) fn logs_near_limit(&self) -> bool {
        self.value_log.near_limit() || self.entries.log_near_limit()
    }

    /// True while the bytes over the memory limit are at most `limit / divisor`.
    pub(crate) fn eviction_debt_within_bound(&self, divisor: usize) -> bool {
        self.memory_limit
            .is_none_or(|limit| self.bytes_over_limit(0) <= limit / divisor.max(1))
    }

    pub(crate) fn eviction_debt(&self) -> bool {
        self.eviction_debt.is_some()
    }

    /// The key whose write left the debt, when it is in the active database: paying the
    /// debt must not evict the value that was just admitted.
    pub(crate) fn eviction_debt_key(&self) -> Option<&[u8]> {
        self.eviction_debt
            .as_ref()
            .filter(|(db, _)| *db == self.active)
            .map(|(_, key)| &**key)
    }

    /// Records a debt left by a write to `key` in the active database.
    pub(crate) fn owe_eviction(&mut self, key: &[u8]) {
        self.eviction_debt = Some((self.active, key.into()));
    }

    pub(crate) fn clear_eviction_debt(&mut self) {
        self.eviction_debt = None;
    }

    pub(crate) fn memory_admission_would_fail(&self, extra_bytes: usize) -> bool {
        if self.value_log.near_limit() || self.entries.log_near_limit() {
            return true;
        }

        let Some(limit) = self.memory_limit else {
            return false;
        };

        self.memory_usage
            .charged_bytes()
            .saturating_add(extra_bytes)
            > limit
    }
}
