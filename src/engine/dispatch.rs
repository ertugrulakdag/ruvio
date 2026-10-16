//! The command entry points: `execute*` and the per-command dispatch of `execute_in_database`.

use super::*;

pub fn execute<'a, C: Clock, I: StorageIndex>(
    shard: &'a mut KvShard<C, I>,
    command: Command<'a>,
) -> Response<'a> {
    let mut sink = NoopMutationSink;

    match execute_with_sink(shard, command, &mut sink) {
        Ok(response) => response,
        Err(error) => match error {},
    }
}

pub fn execute_with_sink<'a, C: Clock, I: StorageIndex, S: MutationSink>(
    shard: &'a mut KvShard<C, I>,
    command: Command<'a>,
    sink: &mut S,
) -> Result<Response<'a>, S::Error> {
    execute_with_waiters(shard, command, sink, &mut ())
}

pub fn execute_with_waiters<'a, C: Clock, I: StorageIndex, S: MutationSink, W: ListWaiterSink>(
    shard: &'a mut KvShard<C, I>,
    command: Command<'a>,
    sink: &mut S,
    waiters: &mut W,
) -> Result<Response<'a>, S::Error> {
    execute_in_database(shard, command, sink, waiters, 0)
}

/// Counts every key a read names as accessed (see `execute_in_database`).
#[inline(never)]
fn touch_read_keys<C: Clock, I: StorageIndex>(shard: &mut KvShard<C, I>, command: &Command<'_>) {
    for key in crate::routing::command_keys(command) {
        shard.touch(key);
    }
}

pub fn execute_in_database<'a, C: Clock, I: StorageIndex, S: MutationSink, W: ListWaiterSink>(
    shard: &'a mut KvShard<C, I>,
    command: Command<'a>,
    sink: &mut S,
    waiters: &mut W,
    database: u8,
) -> Result<Response<'a>, S::Error> {
    shard.expire_due();
    shard.enter(database);
    sink.select_database(database);
    // Best effort: a read must not fail on the log; a write retries in `before_mutation`.
    let _ = record_removals(shard, sink);
    // The one place a read counts as an access for LRU/LFU: writes count through the
    // store's change path, so no command lists its own keys for either.
    if shard.eviction_policy().tracks_recency() && !crate::protocol::command_mutates_store(&command)
    {
        touch_read_keys(shard, &command);
    }
    match command {
        Command::Wait { .. } => Ok(Response::Integer(0)),
        Command::CommandMeta { kind, names } => Ok(command_meta::respond(kind, &names)),
        Command::Ping(message) => match message {
            Some(message) => Ok(Response::Bulk(Some(message))),
            None => Ok(Response::Simple(b"PONG")),
        },
        Command::Ruvio(message) => match message {
            None => Ok(Response::Simple(b"PONG is taken")),
            Some(word) if word.as_ref().eq_ignore_ascii_case(b"owner") => Ok(Response::Simple(
                b"the core that owns the slot does the work",
            )),

            Some(_) => Ok(Response::Error(
                b"ERR RUVIO expects zero arguments or OWNER",
            )),
        },
        Command::Auth { .. } => Ok(Response::Error(
            b"ERR AUTH <password> called without any password configured",
        )),
        Command::Acl(_) => Ok(Response::Error(b"ERR ACL command requires a connection")),
        Command::ClusterSlots => Ok(Response::Error(b"ERR cluster mode is disabled")),
        Command::ClusterNodes
        | Command::ClusterInfo
        | Command::ClusterShards
        | Command::ClusterMyId
        | Command::ReadOnly
        | Command::ReadWrite => Ok(Response::Error(b"ERR cluster mode is disabled")),
        Command::ClusterKeySlot(key) => Ok(Response::Integer(i64::from(
            crate::routing::hash_slot(key.as_ref()),
        ))),
        Command::ConfigGet(pattern) => Ok(config_get_response(&AppConfig::default(), &pattern)),
        Command::Info(_) => Ok(Response::Bulk(Some(Cow::Borrowed(
            b"# Server\r\nruvio_backend:portable\r\n",
        )))),
        Command::Save | Command::Bgsave => {
            Ok(Response::Error(b"ERR SAVE is handled by the server"))
        }

        Command::Multi
        | Command::Exec
        | Command::Discard
        | Command::Watch(_)
        | Command::Unwatch => Ok(Response::Error(
            b"ERR transaction command requires a connection",
        )),
        Command::Eval { .. }
        | Command::EvalSha { .. }
        | Command::ScriptLoad(_)
        | Command::ScriptExists(_)
        | Command::ScriptFlush
        | Command::ScriptKill
        | Command::FunctionLoad { .. }
        | Command::FunctionList
        | Command::FunctionDelete(_)
        | Command::FCall { .. } => Ok(Response::Error(b"ERR script command requires a connection")),
        Command::Hello { .. }
        | Command::ClientGetName
        | Command::ClientId
        | Command::ClientList { .. }
        | Command::ClientInfo
        | Command::ClientKill { .. }
        | Command::Slowlog(_)
        | Command::LatencyLatest
        | Command::ClientSetName(_)
        | Command::ClientTracking(_)
        | Command::Select(_) => Ok(Response::Error(b"ERR command is handled by the connection")),

        Command::ShardPublish { .. }
        | Command::ShardSubscribe(_)
        | Command::ShardUnsubscribe(_)
        | Command::Publish { .. }
        | Command::Subscribe(_)
        | Command::Unsubscribe(_)
        | Command::PSubscribe(_)
        | Command::PUnsubscribe(_) => {
            Ok(Response::Error(b"ERR pubsub is handled by the connection"))
        }

        Command::Get(key) => match shard.value_view(&key) {
            Ok(value) => Ok(Response::Bulk(value.map(byte_view_to_cow))),
            Err(_) => Ok(wrong_type()),
        },

        Command::JsonGet { key, path } => {
            if let Some(tree) = shard.json_tree(&key) {
                if path.as_ref() == b"$" {
                    return Ok(match tree.compact_view() {
                        Some(bytes) => Response::Bulk(Some(Cow::Borrowed(bytes))),
                        None => Response::JsonTree(tree),
                    });
                }
                return match tree.get(&path) {
                    Ok(value) => Ok(Response::Bulk(value.map(Cow::Owned))),
                    Err(crate::json::JsonError::MissingRoot) => Ok(Response::Bulk(None)),
                    Err(_) => Ok(Response::Error(b"ERR invalid JSON path")),
                };
            }
            match shard.json_view(&key) {
                Ok(None) if crate::json::validate_path(&path).is_err() => {
                    Ok(Response::Error(b"ERR invalid JSON path"))
                }
                Ok(None) => Ok(Response::Bulk(None)),
                Ok(Some(document)) if path.as_ref() == b"$" => {
                    Ok(Response::Bulk(Some(Cow::Borrowed(document))))
                }
                Ok(Some(document)) => match crate::json::get(document, &path) {
                    Ok(result) => Ok(Response::Bulk(result.map(Cow::Owned))),
                    Err(_) => Ok(Response::Error(b"ERR invalid JSON path")),
                },
                Err(_) => Ok(wrong_type()),
            }
        }
        Command::JsonMGet { keys, path } => {
            let mut values = Vec::with_capacity(keys.len());

            for key in &keys {
                values.push(match json_value(shard, key, &path) {
                    Ok(value) => value.map(Cow::Owned),
                    Err(rejected) if rejected == Response::Error(b"ERR invalid JSON path") => {
                        return Ok(rejected);
                    }
                    Err(_) => None,
                });
            }

            Ok(Response::Array(values))
        }
        Command::JsonIndex { op, args } => json_index_cmd::run(shard, sink, database, op, &args),
        Command::JsonOp {
            op,
            key,
            path,
            args,
        } => {
            if op == crate::protocol::JsonOpKind::ArrAppend {
                return arr_append(shard, sink, waiters, database, key, path, args);
            }
            let current = match json_value(shard, &key, &path) {
                Ok(Some(current)) => current,
                Ok(None) => return Ok(Response::Bulk(None)),
                Err(response) => return Ok(response),
            };
            match json_ops::plan(op, &current, &args) {
                json_ops::Plan::Respond(response) => Ok(response),
                json_ops::Plan::Delete { index, popped } => {
                    let mut element = path.to_vec();

                    element.extend_from_slice(format!("[{index}]").as_bytes());

                    let removed = execute_in_database(
                        shard,
                        Command::JsonDel {
                            key,
                            path: Cow::Owned(element),
                        },
                        sink,
                        waiters,
                        database,
                    )?;
                    Ok(match removed {
                        Response::Integer(_) => Response::Bulk(Some(Cow::Owned(popped))),
                        other => other,
                    })
                }
                json_ops::Plan::Write { value, reply } => {
                    // CLEAR and ARRTRIM only shrink the document; Redis does not flag them
                    // `denyoom`, so a full store still runs them.
                    let on_oom = if matches!(
                        op,
                        crate::protocol::JsonOpKind::Clear | crate::protocol::JsonOpKind::ArrTrim
                    ) {
                        OnOom::Allow
                    } else {
                        OnOom::Refuse
                    };
                    let written =
                        json_set_at(shard, sink, key, path, Cow::Owned(value), true, on_oom)?;
                    Ok(match written {
                        Response::Simple(_) => reply.into_response(),
                        other => other,
                    })
                }
            }
        }

        Command::JsonSetIf {
            key,
            path,
            value,
            must_exist,
        } => {
            let exists = match json_value(shard, &key, &path) {
                Ok(current) => current.is_some(),
                Err(response) => return Ok(response),
            };
            if exists != must_exist {
                return Ok(Response::Bulk(None));
            }
            execute_in_database(
                shard,
                Command::JsonSet { key, path, value },
                sink,
                waiters,
                database,
            )
        }
        Command::JsonSet { key, path, value } => {
            json_set_at(shard, sink, key, path, value, false, OnOom::Refuse)
        }
        Command::JsonMerge { key, path, value } => {
            if path.as_ref() != b"$"
                && let Some(tree) = shard.json_tree(&key)
            {
                let edit = match tree.prepare(&path, Some(&value), true) {
                    Ok(Some(edit)) => edit,
                    Ok(None) => return Ok(Response::Bulk(None)),
                    Err(crate::json::JsonError::MissingRoot) => {
                        return Ok(Response::Bulk(None));
                    }
                    Err(crate::json::JsonError::Syntax) => {
                        return Ok(Response::Error(b"ERR invalid JSON value"));
                    }
                    Err(crate::json::JsonError::Path) => {
                        return Ok(Response::Error(b"ERR invalid JSON path"));
                    }
                };
                if edit.len() > crate::protocol::DEFAULT_MAX_VALUE_BYTES {
                    return Ok(Response::Error(
                        b"ERR JSON document exceeds maximum value size",
                    ));
                }
                let extra = edit.charge().saturating_sub(tree.charge());
                if let Some(rejected) = before_mutation(shard, sink, [key.as_ref()], extra)? {
                    return Ok(rejected);
                }
                sink.record(Mutation::JsonMergePath {
                    key: &key,
                    path: &path,
                    value: &value,
                })?;
                shard.apply_json_tree_edit(&key, edit);
                return Ok(Response::Simple(b"OK"));
            }
            let tree_document = shard.json_tree(&key).map(|tree| tree.to_vec());
            let current = match shard.json_view(&key) {
                Err(_) if tree_document.is_some() => tree_document.as_deref(),
                Ok(current) => current,
                Err(_) => return Ok(wrong_type()),
            };
            let previous_len = current.map_or(0, <[u8]>::len);
            let existing = current.is_some();
            let document = match crate::json::merge(current, &path, &value) {
                Ok(Some(document)) => document,
                Ok(None) => return Ok(Response::Bulk(None)),
                Err(crate::json::JsonError::MissingRoot) => {
                    return Ok(Response::Error(
                        b"ERR new JSON documents must be created at root",
                    ));
                }
                Err(crate::json::JsonError::Syntax) => {
                    return Ok(Response::Error(b"ERR invalid JSON value"));
                }
                Err(crate::json::JsonError::Path) => {
                    return Ok(Response::Error(b"ERR invalid JSON path"));
                }
            };
            if document.len() > crate::protocol::DEFAULT_MAX_VALUE_BYTES {
                return Ok(Response::Error(
                    b"ERR JSON document exceeds maximum value size",
                ));
            }
            let stored_charge = shard
                .json_tree(&key)
                .map_or(previous_len, |tree| tree.charge());
            // Same rule replay applies to the recorded path merge: a small merge into a
            // large compact document keeps it compact.
            let small_edit = path.as_ref() != b"$"
                && value.len() <= crate::store::SMALL_JSON_EDIT
                && tree_document.is_none()
                && previous_len >= crate::json::TREE_THRESHOLD;
            let candidate_tree = if small_edit {
                None
            } else {
                crate::json::TreeDocument::from_bytes(&document)
            };
            let candidate_charge = candidate_tree
                .as_ref()
                .map_or(document.capacity(), |tree| tree.charge());
            let extra = candidate_charge.saturating_sub(stored_charge)
                + if current.is_none() {
                    key.len() + ADMISSION_OVERHEAD_BYTES
                } else {
                    0
                };
            if let Some(rejected) = before_mutation(shard, sink, [key.as_ref()], extra)? {
                return Ok(rejected);
            }
            if path.as_ref() != b"$" && existing {
                sink.record(Mutation::JsonMergePath {
                    key: &key,
                    path: &path,
                    value: &value,
                })?;
            } else {
                sink.record(Mutation::JsonPut {
                    key: &key,
                    value: &document,
                })?;
            }
            shard
                .put_json_edit_with(
                    &key,
                    document,
                    small_edit,
                    (!small_edit).then_some(candidate_tree),
                )
                .expect("JSON type validated before recording");
            Ok(Response::Simple(b"OK"))
        }
        Command::JsonDel { key, path } => {
            if let Some(tree) = shard.json_tree(&key) {
                if path.as_ref() == b"$" {
                    before_mutation_allow_oom(shard, sink, [key.as_ref()], 0)?;
                    sink.record(Mutation::Delete {
                        keys: std::slice::from_ref(&key),
                    })?;
                    shard.delete(&key);
                    return Ok(Response::Integer(1));
                }
                let edit = match tree.prepare(&path, None, false) {
                    Ok(Some(edit)) => edit,
                    Ok(None) | Err(crate::json::JsonError::MissingRoot) => {
                        return Ok(Response::Integer(0));
                    }
                    Err(_) => return Ok(Response::Error(b"ERR invalid JSON path")),
                };
                before_mutation_allow_oom(shard, sink, [key.as_ref()], 0)?;
                sink.record(Mutation::JsonDelPath {
                    key: &key,
                    path: &path,
                })?;
                shard.apply_json_tree_edit(&key, edit);
                return Ok(Response::Integer(1));
            }
            let current = match shard.json_view(&key) {
                Ok(None) if crate::json::validate_path(&path).is_err() => {
                    return Ok(Response::Error(b"ERR invalid JSON path"));
                }
                Ok(None) => return Ok(Response::Integer(0)),
                Ok(Some(current)) => current,
                Err(_) => return Ok(wrong_type()),
            };
            let root = path.as_ref() == b"$";
            let document = if root {
                None
            } else {
                match crate::json::del(current, &path) {
                    Ok(Some(document)) => Some(document),
                    Ok(None) => return Ok(Response::Integer(0)),
                    Err(_) => return Ok(Response::Error(b"ERR invalid JSON path")),
                }
            };
            before_mutation_allow_oom(shard, sink, [key.as_ref()], 0)?;
            if let Some(document) = document {
                sink.record(Mutation::JsonDelPath {
                    key: &key,
                    path: &path,
                })?;
                // A delete must not turn a large compact document into a tree: that can
                // double its size on a store too full for anything but deletes.
                shard
                    .put_json_edit(&key, document, true)
                    .expect("JSON type validated before recording");
            } else {
                sink.record(Mutation::Delete {
                    keys: std::slice::from_ref(&key),
                })?;
                shard.delete(&key);
            }
            Ok(Response::Integer(1))
        }
        Command::Set(key, value, options) => set_key(shard, sink, key, value, options),
        Command::IdempotencyBegin {
            key,
            fingerprint,
            owner,
            ttl_ms,
        } => idempotency::begin(shard, sink, &key, &fingerprint, &owner, ttl_ms),
        Command::IdempotencyComplete {
            key,
            fingerprint,
            owner,
            result,
        } => idempotency::complete(shard, sink, &key, &fingerprint, &owner, &result),
        Command::Lease { key, ttl_ms } => {
            let planned = match shard.lease_plan(&key, ttl_ms) {
                Ok(planned) => planned,
                Err(_) => return Ok(wrong_type()),
            };

            let Some((token, expires_at)) = planned else {
                return Ok(Response::Bulk(None));
            };

            if let Some(rejected) = before_mutation(
                shard,
                sink,
                [key.as_ref()],
                key.len() + ADMISSION_OVERHEAD_BYTES,
            )? {
                return Ok(rejected);
            }

            sink.record(Mutation::LeasePut {
                key: &key,
                token,
                expires_at,
            })?;

            if shard.lease_put(&key, token, expires_at, "LEASE").is_err() {
                debug_assert!(false, "lease put failed after validation");

                return Ok(wrong_type());
            }

            Ok(Response::Integer(token as i64))
        }

        Command::Release { key, token }
            if shard.value_kind(&key) == Some(crate::store::ValueKind::Semaphore) =>
        {
            if !shard.semaphore_holds(&key, token) {
                return Ok(Response::Integer(0));
            }

            // Releasing only frees memory, but a running snapshot still needs the holder.
            before_mutation_allow_oom(shard, sink, [key.as_ref()], 0)?;

            sink.record(Mutation::SemaphoreRelease { key: &key, token })?;

            if shard.semaphore_release(&key, token).is_err() {
                debug_assert!(false, "semaphore release failed after validation");

                return Ok(wrong_type());
            }

            Ok(Response::Integer(1))
        }

        Command::Semaphore { key, max, ttl_ms } => {
            let (token, expires_at, fresh) = match shard.semaphore_plan(&key, max, ttl_ms) {
                Ok(Some(planned)) => planned,
                Ok(None) => return Ok(Response::Bulk(None)),
                Err(_) => return Ok(wrong_type()),
            };

            let extra = if fresh {
                key.len() + ADMISSION_OVERHEAD_BYTES
            } else {
                16
            };

            if let Some(rejected) = before_mutation(shard, sink, [key.as_ref()], extra)? {
                return Ok(rejected);
            }

            sink.record(Mutation::SemaphoreAcquire {
                key: &key,
                token,
                expires_at,
            })?;

            if shard.semaphore_put(&key, token, expires_at).is_err() {
                debug_assert!(false, "semaphore put failed after validation");

                return Ok(wrong_type());
            }

            Ok(Response::Integer(i64::try_from(token).unwrap_or(i64::MAX)))
        }

        Command::Release { key, token } => {
            let expires_at = match shard.lease_release_plan(&key, token) {
                Ok(Some(expires_at)) => expires_at,
                Ok(None) => return Ok(Response::Integer(0)),
                Err(_) => return Ok(wrong_type()),
            };

            // Releasing rewrites the lease in place: a running snapshot keeps the held one.
            before_mutation_allow_oom(shard, sink, [key.as_ref()], 0)?;

            sink.record(Mutation::LeasePut {
                key: &key,
                token,
                expires_at,
            })?;

            if shard.lease_put(&key, token, expires_at, "RELEASE").is_err() {
                debug_assert!(false, "lease release failed after validation");

                return Ok(wrong_type());
            }

            Ok(Response::Integer(1))
        }

        Command::SetVersioned {
            key,
            value,
            expected,
        } => {
            let version = match shard.plan_versioned(&key, expected) {
                Ok(version) => version,
                Err(crate::store::SetVersionError::Mismatch) => {
                    return Ok(Response::Error(b"ERR version mismatch"));
                }

                Err(crate::store::SetVersionError::WrongType) => return Ok(wrong_type()),
            };

            if let Some(rejected) = before_mutation(
                shard,
                sink,
                [key.as_ref()],
                key.len() + value.len() + ADMISSION_OVERHEAD_BYTES,
            )? {
                return Ok(rejected);
            }

            sink.record(Mutation::SetVersioned {
                key: &key,
                value: &value,
                version,
            })?;

            if shard.put_versioned(&key, &value, version).is_err() {
                debug_assert!(false, "versioned put failed after validation");

                return Ok(wrong_type());
            }

            Ok(Response::Integer(version as i64))
        }

        Command::Limit {
            key,
            max,
            window_ms,
        } => {
            let planned = match shard.plan_limit(&key, max, window_ms) {
                Ok(planned) => planned,
                Err(_) => return Ok(wrong_type()),
            };

            let crate::store::LimitDecision::Allow {
                used,
                window_end,
                remaining,
            } = planned
            else {
                return Ok(Response::Bulk(None));
            };

            let fresh = used == 1;

            if let Some(rejected) = before_mutation(
                shard,
                sink,
                [key.as_ref()],
                if fresh {
                    key.len() + ADMISSION_OVERHEAD_BYTES
                } else {
                    0
                },
            )? {
                return Ok(rejected);
            }

            match window_end {
                // A new window: the counter and its end in one record.
                Some(expires_at) => sink.record(Mutation::Transaction {
                    mutations: &[
                        OwnedMutation::LimitPut {
                            key: key.to_vec(),
                            used,
                            expires_at: None,
                        },
                        OwnedMutation::ExpireAt {
                            key: key.to_vec(),
                            unix_millis: expires_at,
                        },
                    ],
                })?,
                None => sink.record(Mutation::LimitPut { key: &key, used })?,
            }

            if shard.put_limit(&key, used).is_err() {
                debug_assert!(false, "limit put failed after validation");

                return Ok(wrong_type());
            }

            if let Some(expires_at) = window_end {
                shard.expire_at(&key, UnixMillis::new(expires_at));
            }

            Ok(Response::Integer(remaining as i64))
        }

        Command::Once { key, ttl_ms } => {
            let expires_at = match shard.plan_once(&key, ttl_ms) {
                Ok(Some(expires_at)) => expires_at,
                Ok(None) => return Ok(Response::Integer(0)),
                Err(_) => return Ok(wrong_type()),
            };

            if let Some(rejected) = before_mutation(
                shard,
                sink,
                [key.as_ref()],
                key.len() + ADMISSION_OVERHEAD_BYTES,
            )? {
                return Ok(rejected);
            }

            sink.record(Mutation::OncePut {
                key: &key,
                expires_at,
            })?;

            if shard.put_once(&key, expires_at).is_err() {
                debug_assert!(false, "once put failed after validation");

                return Ok(wrong_type());
            }

            Ok(Response::Integer(1))
        }

        Command::ChangesSubscribe(_) => Ok(Response::Error(
            b"ERR CHANGES SUBSCRIBE needs a client connection on the evented runtime",
        )),

        Command::Changes(after) => {
            if after.is_none() {
                let cursor = shard.changes_start();

                return Ok(Response::Integer(cursor as i64));
            }

            let notes = shard.changes_after(after.unwrap_or(0));
            let rows = notes
                .into_iter()
                .map(|note| {
                    Response::Exec(vec![
                        Response::Integer(note.sequence as i64),
                        Response::Bulk(Some(Cow::Owned(note.text.into_bytes()))),
                    ])
                })
                .collect();

            Ok(Response::Exec(rows))
        }

        Command::Increment(key, delta) => {
            let updated = match shard.preview_increment(&key, delta) {
                Ok(value) => value,
                Err(error) => return Ok(increment_error(error)),
            };

            if let Some(rejected) = before_mutation(
                shard,
                sink,
                [key.as_ref()],
                key.len() + ADMISSION_OVERHEAD_BYTES,
            )? {
                return Ok(rejected);
            }

            sink.record(Mutation::SetInteger {
                key: &key,
                value: updated,
            })?;

            shard
                .set_integer(&key, updated)
                .expect("increment type was validated before recording");
            Ok(Response::Integer(updated))
        }

        Command::IncrementCapped { key, delta, limit } => {
            let updated = match shard.preview_increment(&key, delta) {
                Ok(value) => value,
                Err(error) => return Ok(increment_error(error)),
            };

            if updated > limit {
                return Ok(Response::Bulk(None));
            }

            if let Some(rejected) = before_mutation(
                shard,
                sink,
                [key.as_ref()],
                key.len() + ADMISSION_OVERHEAD_BYTES,
            )? {
                return Ok(rejected);
            }

            sink.record(Mutation::SetInteger {
                key: &key,
                value: updated,
            })?;

            shard
                .set_integer(&key, updated)
                .expect("increment type was validated before recording");

            Ok(Response::Integer(updated))
        }

        Command::SetIfEq {
            key,
            value,
            expected,
            options,
        } => set_if_eq_key(shard, sink, key, value, &expected, options),
        Command::DelIfEq { key, expected } => del_if_eq_key(shard, sink, key, &expected),
        Command::ListPushCapped {
            key,
            cap,
            values,
            back,
        } => push_list_capped(shard, sink, waiters, key, cap, values, back),
        Command::Take { key, count } => {
            let updated = match shard.preview_take(&key, count) {
                Ok(Some(updated)) => updated,
                Ok(None) => return Ok(Response::Bulk(None)),
                Err(error) => return Ok(increment_error(error)),
            };

            before_mutation_allow_oom(shard, sink, [key.as_ref()], 0)?;

            sink.record(Mutation::SetInteger {
                key: &key,
                value: updated,
            })?;

            shard
                .set_integer(&key, updated)
                .expect("take type was validated before recording");

            Ok(Response::Integer(updated))
        }

        Command::GetEx { key, ttl_ms } => {
            let bytes = match shard.copy_string(&key) {
                Ok(Some(bytes)) => bytes,
                Ok(None) => return Ok(Response::Bulk(None)),
                Err(_) => return Ok(wrong_type()),
            };

            let expires_at = shard.now_unix_millis().get().saturating_add(ttl_ms);
            let extra = if shard.deadline_present(&key) {
                0
            } else {
                key.len()
            };

            if let Some(rejected) = before_mutation(shard, sink, [key.as_ref()], extra)? {
                return Ok(rejected);
            }

            sink.record(Mutation::ExpireAt {
                key: &key,
                unix_millis: expires_at,
            })?;

            shard.keep_for(&key, expires_at);

            Ok(Response::Bulk(Some(Cow::Owned(bytes))))
        }

        Command::MGet(keys) => {
            let mut values = Vec::with_capacity(keys.len());

            for key in keys {
                values.push(match shard.value_view(&key) {
                    Ok(value) => value.map(byte_view_to_cow),
                    Err(_) => None,
                });
            }

            Ok(Response::Array(values))
        }

        Command::MSet(pairs) => {
            let extra = pairs
                .iter()
                .map(|(key, value)| key.len() + value.len() + ADMISSION_OVERHEAD_BYTES)
                .sum();

            if let Some(rejected) = before_mutation(
                shard,
                sink,
                pairs.iter().map(|(key, _)| key.as_ref()),
                extra,
            )? {
                return Ok(rejected);
            }

            for (key, value) in &pairs {
                sink.record(Mutation::SetBytes { key, value })?;
            }

            shard.set_many_bytes(&pairs);

            Ok(Response::Simple(b"OK"))
        }

        Command::GetSet(key, value) => {
            let previous = match shard.get_bytes(&key) {
                Ok(Some(current)) => Some(Cow::Owned(current.to_vec())),
                Ok(None) => None,
                Err(_) => return Ok(wrong_type()),
            };

            if let Some(rejected) = before_mutation(
                shard,
                sink,
                [key.as_ref()],
                key.len() + value.len() + ADMISSION_OVERHEAD_BYTES,
            )? {
                return Ok(rejected);
            }

            sink.record(Mutation::SetBytes {
                key: &key,
                value: &value,
            })?;

            shard.set_bytes(&key, &value);
            Ok(Response::Bulk(previous))
        }

        Command::Append(key, suffix) => append_key(shard, sink, key, suffix),
        Command::SetNx(key, value) => set_nx(shard, sink, key, value),
        Command::GetDel(key) => get_del(shard, sink, key),
        Command::MSetNx(pairs) => mset_nx(shard, sink, pairs),
        Command::IncrementFloat(key, delta) => increment_float(shard, sink, key, delta),
        Command::GetRange { key, start, end } => Ok(get_range(shard, &key, start, end)),
        Command::SetRange { key, offset, value } => set_range(shard, sink, key, offset, value),
        Command::SetBit { key, offset, value } => set_bit(shard, sink, key, offset, value),
        Command::GetBit(key, offset) => Ok(get_bit(shard, &key, offset)),
        Command::BitField { key, ops, .. } => bit_field(shard, sink, key, &ops),
        Command::BitCount { key, range } => Ok(bit_count(shard, &key, range)),
        Command::BitPos { key, bit, range } => Ok(bit_pos(shard, &key, bit, range)),
        Command::FillGet {
            key,
            fill_ms,
            wait_ms,
        } => Ok(fill_get(shard, &key, fill_ms, wait_ms)),
        Command::FillSet {
            key,
            value,
            ttl_ms,
            token,
            swr_ms,
        } => fill_set(shard, sink, key, value, ttl_ms, token, swr_ms),
        Command::PfAdd { key, elements } => pf_add(shard, sink, key, elements),
        Command::PfCount(keys) => Ok(pf_count(shard, &keys)),
        Command::PfMerge {
            destination,
            sources,
        } => pf_merge(shard, sink, destination, sources),
        Command::BitOp {
            operation,
            destination,
            keys,
        } => bit_op(shard, sink, operation, destination, keys),
        Command::StrLen(key) => match shard.value_view(&key) {
            Ok(Some(value)) => Ok(Response::Integer(value.text_len() as i64)),
            Ok(None) => Ok(Response::Integer(0)),
            Err(_) => Ok(wrong_type()),
        },
        Command::Del(keys) => {
            if !keys.iter().any(|key| shard.contains_key(key)) {
                return Ok(Response::Integer(0));
            }

            before_mutation_allow_oom(shard, sink, keys.iter().map(|key| key.as_ref()), 0)?;

            sink.record(Mutation::Delete { keys: &keys })?;

            Ok(Response::Integer(
                keys.iter().filter(|key| shard.delete(key)).count() as i64,
            ))
        }

        Command::ListPush(key, values) => push_list(shard, sink, waiters, key, values, false),
        Command::ListPushBack(key, values) => push_list(shard, sink, waiters, key, values, true),
        Command::ListPop { key, count } => pop_list(shard, sink, key, count, false),
        Command::ListPopBack { key, count } => pop_list(shard, sink, key, count, true),
        Command::ListLen(key) => shard.list_len(&key).map_or_else(
            |_| Ok(wrong_type()),
            |len| Ok(Response::Integer(len as i64)),
        ),
        Command::ListIndex(key, index) => match shard.list_index(&key, index) {
            Ok(value) => Ok(Response::Bulk(value.map(Cow::Borrowed))),
            Err(_) => Ok(wrong_type()),
        },
        Command::ListTrim(key, start, stop) => trim_list(shard, sink, key, start, stop),
        Command::ListInsert {
            key,
            before,
            pivot,
            value,
        } => list_insert(shard, sink, key, before, pivot, value),
        Command::ListSet { key, index, value } => list_set(shard, sink, key, index, value),
        Command::ListRemove { key, count, value } => list_remove(shard, sink, key, count, value),
        Command::ListPos {
            key,
            element,
            rank,
            count,
            max_len,
        } => Ok(list_pos(shard, &key, &element, rank, count, max_len)),
        Command::ListPushX { key, values, back } => {
            list_push_existing(shard, sink, waiters, key, values, back)
        }
        Command::ListMove {
            source,
            destination,
            from_left,
            to_left,
            block,
        } => list_move(
            shard,
            sink,
            waiters,
            source,
            destination,
            from_left,
            to_left,
            block,
        ),
        Command::ListMPop {
            keys,
            from_left,
            count,
            block,
        } => list_mpop(shard, sink, keys, from_left, count, block),
        Command::ListBlock {
            keys,
            timeout_ms,
            back,
        } => block_pop_list(shard, sink, keys, timeout_ms, back),
        Command::ListRange(key, start, stop) => shard
            .list_range_stream(&key, start, stop)
            .map_or_else(|_| Ok(wrong_type()), |items| Ok(Response::ListSlice(items))),
        Command::SetAdd(key, members) => {
            if !accepts_type(shard, &key, ValueKind::Set) {
                return Ok(wrong_type());
            }

            let extra = members.iter().map(|member| member.len()).sum::<usize>()
                + key.len()
                + ADMISSION_OVERHEAD_BYTES;

            if let Some(rejected) = before_mutation(shard, sink, [key.as_ref()], extra)? {
                return Ok(rejected);
            }

            sink.record(Mutation::SetAdd {
                key: &key,
                members: &members,
            })?;

            Ok(Response::Integer(
                shard
                    .set_add(&key, &members)
                    .expect("set type was validated before recording") as i64,
            ))
        }

        Command::SetRemove(key, members) => {
            if !accepts_type(shard, &key, ValueKind::Set) {
                return Ok(wrong_type());
            }

            if !shard.contains_key(&key) {
                return Ok(Response::Integer(0));
            }

            before_mutation_allow_oom(shard, sink, [key.as_ref()], 0)?;

            sink.record(Mutation::SetRemove {
                key: &key,
                members: &members,
            })?;

            Ok(Response::Integer(
                shard
                    .set_remove(&key, &members)
                    .expect("set type was validated before recording") as i64,
            ))
        }

        Command::SetContains(key, member) => shard.set_contains(&key, &member).map_or_else(
            |_| Ok(wrong_type()),
            |exists| Ok(Response::Integer(i64::from(exists))),
        ),
        Command::SetCard(key) => shard.set_card(&key).map_or_else(
            |_| Ok(wrong_type()),
            |count| Ok(Response::Integer(count as i64)),
        ),
        Command::SetMembers(key) => shard.set_member_stream(&key).map_or_else(
            |_| Ok(wrong_type()),
            |members| Ok(Response::Members(members)),
        ),
        Command::SetPop { key, count } => pop_set(shard, sink, key, count),
        Command::SetRandom { key, count } => sample_set::<C, I, S>(shard, key, count),
        Command::SetAlgebra {
            keys,
            op,
            destination,
        } => set_algebra(shard, sink, &keys, op, destination.as_deref()),
        Command::SetMIsMember { key, members } => Ok(set_mismember(shard, &key, &members)),
        Command::SetInterCard { keys, limit } => Ok(set_inter_card(shard, &keys, limit)),
        Command::SortedSetCount { key, min, max } => Ok(sorted_count(shard, &key, min, max)),
        Command::SortedSetLexCount { key, min, max } => {
            Ok(sorted_lex_count(shard, &key, &min, &max))
        }
        Command::SortedSetMScore { key, members } => Ok(sorted_mscore(shard, &key, &members)),
        Command::SortedSetAlgebra {
            op,
            keys,
            weights,
            aggregate,
            with_scores,
            destination,
        } => sorted_algebra(
            shard,
            sink,
            op,
            &keys,
            &weights,
            aggregate,
            with_scores,
            destination.as_ref(),
        ),
        Command::SortedSetRandMember {
            key,
            count,
            with_scores,
        } => Ok(sorted_rand_member(shard, &key, count, with_scores)),
        Command::HashRandField {
            key,
            count,
            with_values,
        } => Ok(hash_rand_field(shard, &key, count, with_values)),
        Command::HashIncrementFloat { key, field, delta } => {
            hash_increment_float(shard, sink, key, field, delta)
        }
        Command::SetMove {
            source,
            destination,
            member,
        } => set_move(shard, sink, &source, &destination, &member),
        Command::HashSet { key, pairs } => {
            shard.expire_hash_fields(&key);

            if !accepts_type(shard, &key, ValueKind::Hash) {
                return Ok(wrong_type());
            }

            let extra = pairs
                .iter()
                .map(|(field, value)| field.len().saturating_add(value.len()))
                .sum::<usize>()
                + key.len()
                + ADMISSION_OVERHEAD_BYTES;

            if let Some(rejected) = before_mutation(shard, sink, [key.as_ref()], extra)? {
                return Ok(rejected);
            }

            sink.record(Mutation::HashSet {
                key: &key,
                pairs: &pairs,
            })?;

            Ok(Response::Integer(
                shard
                    .hash_set(&key, &pairs)
                    .expect("hash type was validated before recording") as i64,
            ))
        }

        Command::HashDelete { key, fields } => {
            shard.expire_hash_fields(&key);

            if !accepts_type(shard, &key, ValueKind::Hash) {
                return Ok(wrong_type());
            }

            if !shard.contains_key(&key) {
                return Ok(Response::Integer(0));
            }

            before_mutation_allow_oom(shard, sink, [key.as_ref()], 0)?;

            sink.record(Mutation::HashDelete {
                key: &key,
                fields: &fields,
            })?;

            Ok(Response::Integer(
                shard
                    .hash_delete(&key, &fields)
                    .expect("hash type was validated before recording") as i64,
            ))
        }

        Command::HashGet { key, field } => {
            shard.expire_hash_fields(&key);

            shard.hash_get(&key, &field).map_or_else(
                |_| Ok(wrong_type()),
                |value| Ok(Response::Bulk(value.map(Cow::Borrowed))),
            )
        }

        Command::HashMGet { key, fields } => {
            shard.expire_hash_fields(&key);

            match shard.hash_mget(&key, &fields) {
                Ok(values) => Ok(Response::Array(
                    values
                        .into_iter()
                        .map(|value| value.map(Cow::Borrowed))
                        .collect(),
                )),
                Err(_) => Ok(wrong_type()),
            }
        }

        Command::HashLen(key) => {
            shard.expire_hash_fields(&key);

            shard.hash_len(&key).map_or_else(
                |_| Ok(wrong_type()),
                |len| Ok(Response::Integer(len as i64)),
            )
        }

        Command::HashGetAll(key) => {
            shard.expire_hash_fields(&key);

            shard.hash_stream(&key).map_or_else(
                |_| Ok(wrong_type()),
                |hash| Ok(Response::HashEntries(hash, HashStreamKind::Pairs)),
            )
        }

        Command::HashExists { key, field } => {
            shard.expire_hash_fields(&key);

            shard.hash_exists(&key, &field).map_or_else(
                |_| Ok(wrong_type()),
                |exists| Ok(Response::Integer(i64::from(exists))),
            )
        }

        Command::HashKeys(key) => {
            shard.expire_hash_fields(&key);

            shard.hash_stream(&key).map_or_else(
                |_| Ok(wrong_type()),
                |hash| Ok(Response::HashEntries(hash, HashStreamKind::Fields)),
            )
        }

        Command::HashValues(key) => {
            shard.expire_hash_fields(&key);

            shard.hash_stream(&key).map_or_else(
                |_| Ok(wrong_type()),
                |hash| Ok(Response::HashEntries(hash, HashStreamKind::Values)),
            )
        }

        Command::HashIncrement { key, field, delta } => {
            shard.expire_hash_fields(&key);

            hash_increment(shard, sink, key, field, delta)
        }

        Command::HashSetNx { key, field, value } => {
            shard.expire_hash_fields(&key);

            hash_set_nx(shard, sink, key, field, value)
        }

        Command::HashStrLen { key, field } => {
            shard.expire_hash_fields(&key);

            match shard.hash_strlen(&key, &field) {
                Ok(length) => Ok(Response::Integer(i64::try_from(length).unwrap_or(i64::MAX))),
                Err(_) => Ok(wrong_type()),
            }
        }

        Command::HashScan {
            key,
            cursor,
            pattern,
            count,
        } => {
            shard.expire_hash_fields(&key);

            match shard.hash_scan(&key, cursor, count, pattern.as_deref()) {
                Ok((cursor, pairs)) => {
                    let mut keys = Vec::with_capacity(pairs.len().saturating_mul(2));

                    for (field, value) in pairs {
                        keys.push(Cow::Borrowed(field));
                        keys.push(Cow::Borrowed(value));
                    }

                    Ok(Response::Scan { cursor, keys })
                }

                Err(_) => Ok(wrong_type()),
            }
        }

        Command::CollectionScan {
            kind,
            key,
            cursor,
            pattern,
            count,
        } => Ok(keyspace::collection_scan(
            shard,
            kind,
            &key,
            cursor,
            count,
            pattern.as_deref(),
        )),

        Command::SortedSetAdd {
            key,
            pairs,
            options,
        } => sorted_set_add(shard, sink, waiters, key, pairs, options),

        Command::SortedSetRange {
            key,
            start,
            stop,
            with_scores,
            reverse,
        } => match shard.sorted_set_range_entries(&key, start, stop, reverse) {
            Ok(entries) => Ok(sorted_set_range_response(entries, with_scores)),
            Err(_) => Ok(wrong_type()),
        },
        Command::SortedSetRangeByScore {
            key,
            min,
            max,
            with_scores,
            reverse,
            limit,
            via_zrange: _,
        } => match shard.sorted_set_range_by_score(&key, min, max) {
            Ok(entries) => Ok(sorted_set_range_response(
                apply_range_window(entries, reverse, limit),
                with_scores,
            )),
            Err(_) => Ok(wrong_type()),
        },
        Command::SortedSetRangeByLex {
            key,
            min,
            max,
            with_scores,
            reverse,
            limit,
            invocation: _,
        } => match shard.sorted_set_range_by_lex(&key, &min, &max) {
            Ok(entries) => Ok(sorted_set_range_response(
                apply_range_window(entries, reverse, limit),
                with_scores,
            )),
            Err(_) => Ok(wrong_type()),
        },
        Command::SortedSetRemove { key, members } => {
            if !accepts_type(shard, &key, ValueKind::SortedSet) {
                return Ok(wrong_type());
            }

            if !shard.contains_key(&key) {
                return Ok(Response::Integer(0));
            }

            before_mutation_allow_oom(shard, sink, [key.as_ref()], 0)?;

            sink.record(Mutation::SortedSetRemove {
                key: &key,
                members: &members,
            })?;

            Ok(Response::Integer(
                shard
                    .sorted_set_remove(&key, &members)
                    .expect("sorted-set type was validated before recording")
                    as i64,
            ))
        }

        Command::SortedSetIncr { key, delta, member } => {
            if delta.is_nan() {
                return Ok(sorted_set_error(SortedSetError::InvalidIncrement));
            }

            if !accepts_type(shard, &key, ValueKind::SortedSet) {
                return Ok(wrong_type());
            }

            let current = shard
                .sorted_set_score(&key, &member)
                .expect("type was validated");
            let next = current.unwrap_or(0.0) + delta;

            if next.is_nan() {
                return Ok(sorted_set_error(SortedSetError::InvalidIncrement));
            }

            if let Some(rejected) = before_mutation(
                shard,
                sink,
                [key.as_ref()],
                key.len() + member.len() + ADMISSION_OVERHEAD_BYTES,
            )? {
                return Ok(rejected);
            }

            if !shard.sorted_set_make_room(&key, false, std::iter::once(member.as_ref())) {
                return Ok(sorted_set_error(SortedSetError::Full));
            }

            let pair = [(next, member)];

            sink.record(Mutation::SortedSetAdd {
                key: &key,
                pairs: &pair,
            })?;

            match shard.sorted_set_add(&key, next, &pair[0].1) {
                Ok(_) => {
                    wake_sorted_waiters(shard, sink, waiters, &key)?;

                    Ok(Response::Double(next))
                }

                Err(error) => {
                    debug_assert!(false, "sorted-set increment failed after validation");
                    Ok(sorted_set_error(error))
                }
            }
        }

        Command::SortedSetCard(key) => shard.sorted_set_card(&key).map_or_else(
            |_| Ok(wrong_type()),
            |count| Ok(Response::Integer(count as i64)),
        ),
        Command::SortedSetScore { key, member } => match shard.sorted_set_score(&key, &member) {
            Ok(Some(score)) => Ok(Response::Double(score)),
            Ok(None) => Ok(Response::Bulk(None)),
            Err(_) => Ok(wrong_type()),
        },
        Command::SortedSetRank {
            key,
            member,
            reverse,
        } => match shard.sorted_set_rank(&key, &member, reverse) {
            Ok(Some(rank)) => Ok(Response::Integer(rank as i64)),
            Ok(None) => Ok(Response::Bulk(None)),
            Err(_) => Ok(wrong_type()),
        },
        Command::SortedSetPop { key, count, max } => pop_sorted(shard, sink, key, count, max),
        Command::SortedSetBlockPop {
            keys,
            timeout_ms,
            max,
        } => block_pop_sorted(shard, sink, keys, timeout_ms, max),
        Command::SortedSetRemoveRange { key, range } => {
            remove_sorted_range(shard, sink, key, range)
        }

        Command::HashExpire {
            key,
            time,
            condition,
            fields,
            name,
        } => hash_field_expire(shard, sink, key, time, condition, fields, name),
        Command::HashTtl { key, reply, fields } => {
            shard.expire_hash_fields(&key);
            Ok(field_ttl_reply(shard, &key, reply, &fields))
        }

        Command::HashPersist { key, fields } => persist_hash_fields(shard, sink, key, fields),
        Command::GeoAdd {
            key,
            members,
            options,
        } => geo_add(shard, sink, waiters, key, members, options),
        Command::GeoDist {
            key,
            from,
            to,
            unit,
        } => Ok(geo_dist(shard, key, from, to, unit)),
        Command::GeoPos { key, members } => Ok(geo_pos(shard, key, members)),
        Command::GeoHash { key, members } => Ok(geo_hash(shard, key, members)),
        Command::GeoSearch {
            key,
            center,
            area,
            unit,
            descending,
            ordered,
            count,
            any,
            with_dist,
            with_hash,
            with_coord,
            store,
            query: _,
        } => geo_search(
            shard, sink, key, center, area, unit, descending, ordered, count, any, with_dist,
            with_hash, with_coord, store,
        ),
        Command::StreamAdd {
            key,
            id,
            max_len,
            min_id,
            delay_ms,
            nomkstream,
            fields,
        } => {
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

        Command::StreamLen(key) => shard.stream_len(&key).map_or_else(
            |_| Ok(wrong_type()),
            |len| Ok(Response::Integer(len as i64)),
        ),
        Command::StreamInfo { key, group, kind } => {
            Ok(stream_info(shard, &key, group.as_deref(), kind))
        }

        Command::StreamRange {
            key,
            start,
            end,
            count,
            reverse,
        } => match shard.stream_range(&key, start, end, count, reverse) {
            Ok(entries) => Ok(stream_range_response(entries)),

            Err(_) => Ok(wrong_type()),
        },
        Command::StreamRead { count, block, keys } => {
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

        Command::StreamDelete { key, ids } => {
            let mut deleted = 0i64;

            for (millis, sequence) in ids {
                let present = match shard.stream_contains(&key, millis, sequence) {
                    Ok(present) => present,
                    Err(_) => return Ok(wrong_type()),
                };

                if !present {
                    continue;
                }

                if deleted == 0 {
                    before_mutation_allow_oom(
                        shard,
                        sink,
                        [key.as_ref()],
                        key.len() + ADMISSION_OVERHEAD_BYTES,
                    )?;
                }

                sink.record(Mutation::StreamDelete {
                    key: &key,
                    millis,
                    sequence,
                })?;

                match shard.stream_delete(&key, millis, sequence) {
                    Ok(true) => deleted += 1,
                    Ok(false) => {}
                    Err(_) => return Ok(wrong_type()),
                }
            }

            Ok(Response::Integer(deleted))
        }

        Command::StreamTrim { key, max_len } => {
            match shard.value_kind(&key) {
                None => return Ok(Response::Integer(0)),
                Some(ValueKind::Stream) => {}
                Some(_) => return Ok(wrong_type()),
            }

            before_mutation_allow_oom(
                shard,
                sink,
                [key.as_ref()],
                key.len() + ADMISSION_OVERHEAD_BYTES,
            )?;

            sink.record(Mutation::StreamTrim { key: &key, max_len })?;

            match shard.stream_trim(&key, max_len) {
                Ok(removed) => Ok(Response::Integer(removed as i64)),
                Err(_) => Ok(wrong_type()),
            }
        }

        Command::StreamTrimMinId { key, id } => {
            let (millis, sequence) = id;

            match shard.value_kind(&key) {
                None => return Ok(Response::Integer(0)),
                Some(ValueKind::Stream) => {}
                Some(_) => return Ok(wrong_type()),
            }

            before_mutation_allow_oom(
                shard,
                sink,
                [key.as_ref()],
                key.len() + ADMISSION_OVERHEAD_BYTES,
            )?;

            sink.record(Mutation::StreamTrimMinId {
                key: &key,
                millis,
                sequence,
            })?;

            match shard.stream_trim_min_id(&key, millis, sequence) {
                Ok(removed) => Ok(Response::Integer(removed as i64)),
                Err(_) => Ok(wrong_type()),
            }
        }

        Command::StreamSetId { key, id } => {
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
                Err(crate::store::StreamSetIdError::Missing) => {
                    Ok(Response::Error(b"ERR no such key"))
                }

                Err(crate::store::StreamSetIdError::TooSmall) => Ok(Response::Error(
                    b"ERR The ID specified in XSETID is smaller than the target stream top item",
                )),
            }
        }

        Command::StreamGroupCreate {
            key,
            group,
            start,
            mkstream,
        } => {
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

        Command::StreamGroupRead {
            key,
            group,
            consumer,
            count,
            block,
            noack,
        } => {
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

                    if let Err(error) = shard.stream_group_advance(
                        &key,
                        group.as_ref(),
                        entry.millis,
                        entry.sequence,
                    ) {
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

        Command::StreamGroupDestroy { key, group } => {
            match shard.stream_group_cursor(&key, group.as_ref()) {
                Err(crate::store::StreamGroupError::NoGroup) => return Ok(Response::Integer(0)),
                Err(error) => return Ok(stream_group_error(error)),
                Ok(_) => {}
            }

            before_mutation_allow_oom(
                shard,
                sink,
                [key.as_ref()],
                key.len() + ADMISSION_OVERHEAD_BYTES,
            )?;

            sink.record(Mutation::StreamGroupDestroy {
                key: key.as_ref(),
                group: group.as_ref(),
            })?;

            match shard.stream_destroy_group(&key, group.as_ref()) {
                Ok(true) => {
                    waiters.release_group(database, key.as_ref(), group.as_ref());
                    Ok(Response::Integer(1))
                }

                Ok(false) => Ok(Response::Integer(0)),
                Err(error) => Ok(stream_group_error(error)),
            }
        }

        Command::StreamGroupSetId { key, group, start } => {
            let (millis, sequence) = match start {
                crate::stream::GroupStart::Id(millis, sequence) => (millis, sequence),
                crate::stream::GroupStart::End => match shard.stream_end(&key) {
                    Ok(end) => end,
                    Err(_) => return Ok(wrong_type()),
                },
            };

            if let Err(error) = shard.stream_group_cursor(&key, group.as_ref()) {
                return Ok(stream_group_error(error));
            }

            before_mutation_allow_oom(
                shard,
                sink,
                [key.as_ref()],
                key.len() + ADMISSION_OVERHEAD_BYTES,
            )?;

            sink.record(Mutation::StreamGroupSetId {
                key: key.as_ref(),
                group: group.as_ref(),
                millis,
                sequence,
            })?;

            match shard.stream_set_group_id(&key, group.as_ref(), millis, sequence) {
                Ok(()) => Ok(Response::Simple(b"OK")),
                Err(error) => Ok(stream_group_error(error)),
            }
        }

        Command::StreamGroupCreateConsumer {
            key,
            group,
            consumer,
        } => {
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

        Command::StreamGroupDelConsumer {
            key,
            group,
            consumer,
        } => {
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

        Command::StreamAck { key, group, ids } => {
            let mut acked = 0i64;

            for (millis, sequence) in ids {
                let present =
                    match shard.stream_group_pending(&key, group.as_ref(), millis, sequence) {
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

        Command::StreamPending { key, group } => {
            match shard.stream_group_summary(&key, group.as_ref()) {
                Ok(summary) => Ok(pending_summary_response(summary)),
                Err(error) => Ok(stream_group_error(error)),
            }
        }

        Command::StreamPendingRange {
            key,
            group,
            min_idle,
            start,
            end,
            count,
            consumer,
        } => {
            match shard.stream_pending_range(
                &key,
                group.as_ref(),
                crate::stream::PendingFilter {
                    start,
                    end,
                    min_idle,
                    consumer: consumer.as_deref(),
                    count,
                    now: shard.now_unix_millis().get(),
                },
            ) {
                Ok(rows) => Ok(pending_rows_response(rows)),
                Err(error) => Ok(stream_group_error(error)),
            }
        }

        Command::StreamClaimPending {
            key,
            group,
            consumer,
            min_idle,
            ids,
            options,
        } => {
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

                let pending =
                    match shard.stream_pending_delivery(&key, group.as_ref(), millis, sequence) {
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

                    if let Err(error) =
                        shard.stream_group_ack(&key, group.as_ref(), millis, sequence)
                    {
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

                if let Err(error) =
                    shard.stream_set_group_id(&key, group.as_ref(), millis, sequence)
                {
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

        Command::StreamAutoclaim {
            key,
            group,
            consumer,
            min_idle,
            start,
            count,
            just_id,
        } => {
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

                        if let Err(error) =
                            shard.stream_group_ack(&key, group.as_ref(), millis, sequence)
                        {
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
                            Response::Bulk(Some(Cow::Owned(crate::stream::format_id(
                                millis, sequence,
                            ))))
                        })
                        .collect(),
                ),
            ]))
        }

        Command::BloomReserve(key, error_rate, capacity, expansion) => {
            bloom_reserve_key(shard, sink, key, error_rate, capacity, expansion)
        }
        Command::BloomAdd(key, value) => bloom_add_key(shard, sink, key, value),
        Command::BloomContains(key, value) => shard.bloom_contains(&key, &value).map_or_else(
            |error| Ok(bloom_error(error)),
            |exists| Ok(Response::Integer(i64::from(exists))),
        ),
        Command::BloomAddMany(key, values) => bloom_add_many_key(shard, sink, key, values),
        Command::BloomContainsMany(key, values) => {
            let mut answers = Vec::with_capacity(values.len());

            Ok(
                match shard.bloom_contains_each(&key, &values, |exists| {
                    answers.push(Response::Integer(i64::from(exists)));
                }) {
                    Ok(()) => Response::Exec(answers),
                    Err(error) => bloom_error(error),
                },
            )
        }
        Command::BloomInsert {
            key,
            error_rate,
            capacity,
            expansion,
            no_create,
            items,
        } => bloom_insert_key(
            shard, sink, key, error_rate, capacity, expansion, no_create, items,
        ),
        Command::BloomInfo(key, field) => Ok(bloom_info(shard, &key, field)),
        Command::BloomCard(key) => Ok(match shard.bloom_info(&key) {
            Ok(info) => Response::Integer(info.map_or(0, |info| info.items as i64)),
            Err(error) => bloom_error(error),
        }),
        Command::Expire { key, milliseconds } => expire_key(shard, sink, key, milliseconds, false),
        Command::PExpireAt { key, unix_millis } => expire_key(shard, sink, key, unix_millis, true),
        Command::Ttl(key) => Ok(Response::Integer(shard.ttl_seconds(&key))),
        Command::PTtl(key) => Ok(Response::Integer(shard.pttl(&key))),
        Command::Persist(key) => persist_key(shard, sink, key),
        Command::Exists(keys) => Ok(Response::Integer(
            keys.iter().filter(|key| shard.contains_key(key)).count() as i64,
        )),
        Command::Type(key) => Ok(Response::Simple(type_name(shard.value_kind(&key)))),
        Command::Rename {
            source,
            destination,
        } => rename_key(shard, sink, source, destination),
        Command::Echo(message) => Ok(Response::Bulk(Some(message))),
        Command::Object { kind, key } => Ok(keyspace_ext::object(shard, kind, &key)),
        Command::MemoryUsage(key) => Ok(keyspace_ext::memory_usage(shard, &key)),
        Command::Keys(pattern) => Ok(keyspace_ext::keys_matching(shard, &pattern)),
        Command::RandomKey => Ok(keyspace_ext::random_key(shard)),
        Command::Dump(key) => Ok(keyspace_ext::dump_key(shard, &key)),
        Command::Restore {
            key,
            ttl,
            payload,
            replace,
            absolute,
        } => keyspace_ext::restore_key(shard, sink, key, ttl, payload, replace, absolute),
        Command::Touch(keys) => Ok(keyspace_ext::touch(shard, &keys)),
        Command::RenameNx {
            source,
            destination,
        } => keyspace_ext::rename_nx(shard, sink, source, destination),
        Command::ExpireTime { key, millis } => Ok(keyspace_ext::expire_time(shard, &key, millis)),
        Command::Copy {
            source,
            destination,
            replace,
        } => keyspace_ext::copy_key(shard, sink, source, destination, replace),
        Command::Scan {
            cursor,
            pattern,
            count,
            type_name,
        } => Ok(scan_keys(
            shard,
            cursor,
            pattern.as_deref(),
            count,
            type_name.as_deref(),
        )),
        Command::DbSize => Ok(Response::Integer(shard.database_len() as i64)),
        Command::FlushDb => {
            shard.capture_snapshot_databases(&[database]);

            sink.record(Mutation::FlushDatabase)?;
            shard.flush_database(database);
            Ok(Response::Simple(b"OK"))
        }

        Command::FlushAll => {
            shard.capture_snapshot_databases(&shard.database_ids());

            sink.record(Mutation::FlushAll)?;
            shard.flush_all();
            Ok(Response::Simple(b"OK"))
        }

        Command::SwapDb(left, right) => match (u8::try_from(left), u8::try_from(right)) {
            (Ok(first), Ok(second)) => {
                shard.capture_snapshot_databases(&[first, second]);

                sink.record(Mutation::SwapDatabases { first, second })?;

                shard.swap_databases(first, second);
                Ok(Response::Simple(b"OK"))
            }

            _ => Ok(Response::Error(b"ERR DB index is out of range")),
        },
        Command::Move { key, db } => match u8::try_from(db) {
            Ok(target) if target == database => Ok(Response::Error(
                b"ERR source and destination objects are the same",
            )),
            Ok(target) => move_key(shard, sink, key, target),
            _ => Ok(Response::Error(b"ERR DB index is out of range")),
        },
    }
}
