// client_coverage: APPEND DEL EVAL EXEC EXPIRE GET HGET HGETALL HSET INCR
// LPUSH LRANGE MGET MSET MULTI PING SADD SET SMEMBERS STRLEN TTL ZADD ZRANGE

use std::collections::HashMap;

use std::time::{Duration, SystemTime};

use crate::client::{Client, duration_millis, duration_secs, join, unexpected, unix_millis};

use crate::error::Error;
use crate::models::{
    FieldExpireCondition, FillOutcome, HashScanPage, PubSubMessage, ScanPage, SortedSetAddOptions,
    SortedSetEntry, StreamClaimOptions, StreamEntry, StreamPendingEntry, StreamPendingFilter,
    StreamReadOptions, StreamReadResult,
};

use crate::args::Num;
use crate::value::RespValue;

macro_rules! args {
    ($($argument:expr),* $(,)?) => {
        vec![$($argument.to_string()),*]
    };
}

impl Client {
    /// Sends `MGET`. A missing key is `None`.
    pub fn mget(&mut self, keys: &[&str]) -> Result<Vec<Option<String>>, Error> {
        self.optional_strings(join("MGET", keys))
    }

    /// Sends `LIMIT`. `None` means this call did not fit in the window.
    /// `Some` is how many calls remain after this one.
    pub fn limit(&mut self, key: &str, max: u64, window_ms: u64) -> Result<Option<i64>, Error> {
        let value = self.run([
            "LIMIT",
            key,
            Num::uint(max).as_str(),
            Num::uint(window_ms).as_str(),
        ])?;

        if value.is_null() {
            return Ok(None);
        }

        value
            .as_integer()
            .map(Some)
            .ok_or_else(|| unexpected("LIMIT", &value))
    }

    /// Sends `ONCE`. `1` is the first call. `0` means the key is already taken.
    pub fn once(&mut self, key: &str, ttl_ms: u64) -> Result<i64, Error> {
        self.integer(["ONCE", key, Num::uint(ttl_ms).as_str()])
    }

    /// Native reservation: `[acquired]`, `[pending]`, `[conflict]`, or `[completed, binary result]`. See [`Client::idem_begin`] for a decoded form.
    pub fn idempotency_begin(
        &mut self,
        key: &str,
        fingerprint: &str,
        owner: &str,
        ttl_ms: u64,
    ) -> Result<RespValue, Error> {
        self.run([
            "IDEM",
            key,
            "BEGIN",
            fingerprint,
            owner,
            Num::uint(ttl_ms).as_str(),
        ])
    }

    /// Publishes a result only for the matching live owner, without renewing retention.
    pub fn idempotency_complete(
        &mut self,
        key: &str,
        fingerprint: &str,
        owner: &str,
        result: &str,
    ) -> Result<i64, Error> {
        self.integer(["IDEM", key, "COMPLETE", fingerprint, owner, result])
    }

    /// Sends `TAKE`. `None` means the key did not hold enough and was left unchanged.
    pub fn take(&mut self, key: &str, count: i64) -> Result<Option<i64>, Error> {
        let value = self.run(["TAKE", key, Num::int(count).as_str()])?;

        if value.is_null() {
            return Ok(None);
        }

        value
            .as_integer()
            .map(Some)
            .ok_or_else(|| unexpected("TAKE", &value))
    }

    /// Sends `GETEX`. Returns the string and sets its TTL to `ttl_ms`.
    pub fn getex(&mut self, key: &str, ttl_ms: u64) -> Result<Option<String>, Error> {
        self.bulk_or_null(["GETEX", key, Num::uint(ttl_ms).as_str()])
    }

    /// Sends `LEASE`. `None` means another holder still has the lease.
    pub fn lease(&mut self, key: &str, ttl_ms: u64) -> Result<Option<i64>, Error> {
        let value = self.run(["LEASE", key, Num::uint(ttl_ms).as_str()])?;

        if value.is_null() {
            return Ok(None);
        }

        value
            .as_integer()
            .map(Some)
            .ok_or_else(|| unexpected("LEASE", &value))
    }

    /// Sends `SEMAPHORE key max ttl_ms`. `Some(token)` while fewer than `max` holders are
    /// live; `None` when full. Pass the token to [`Client::release`].
    pub fn semaphore(&mut self, key: &str, max: u64, ttl_ms: u64) -> Result<Option<i64>, Error> {
        let value = self.run([
            "SEMAPHORE",
            key,
            Num::uint(max).as_str(),
            Num::uint(ttl_ms).as_str(),
        ])?;

        if value.is_null() {
            return Ok(None);
        }

        value
            .as_integer()
            .map(Some)
            .ok_or_else(|| unexpected("SEMAPHORE", &value))
    }

    /// Sends `INCRBY key delta MAX limit`. `None` means the result would pass `limit`, and the
    /// key was left unchanged.
    pub fn incr_by_max(&mut self, key: &str, delta: i64, limit: i64) -> Result<Option<i64>, Error> {
        let value = self.run([
            "INCRBY",
            key,
            Num::int(delta).as_str(),
            "MAX",
            Num::int(limit).as_str(),
        ])?;

        if value.is_null() {
            return Ok(None);
        }

        value
            .as_integer()
            .map(Some)
            .ok_or_else(|| unexpected("INCRBY", &value))
    }

    /// Sends `RELEASE`. Returns 1 when the token matched the holder of a lease or a semaphore.
    pub fn release(&mut self, key: &str, token: u64) -> Result<i64, Error> {
        self.integer(["RELEASE", key, Num::uint(token).as_str()])
    }

    /// Sends `SETV`. Omit `version` to create version 1.
    pub fn setv(&mut self, key: &str, value: &str, version: Option<u64>) -> Result<i64, Error> {
        let mut arguments = vec!["SETV".to_owned(), key.to_owned(), value.to_owned()];

        if let Some(version) = version {
            arguments.push(version.to_string());
        }

        self.integer(arguments)
    }

    /// Sends `CHANGES START` and returns the cursor to read after.
    pub fn changes_start(&mut self) -> Result<i64, Error> {
        self.integer(["CHANGES", "START"])
    }

    /// Sends `CHANGES` and returns `(sequence, text)` notes after `cursor`.
    pub fn changes(&mut self, cursor: u64) -> Result<Vec<(i64, String)>, Error> {
        let value = self.run(["CHANGES", Num::uint(cursor).as_str()])?;
        let Some(rows) = value.as_array() else {
            return Err(unexpected("CHANGES", &value));
        };
        let mut notes = Vec::new();

        for row in rows {
            let Some(parts) = row.as_array() else {
                return Err(unexpected("CHANGES", row));
            };
            let sequence = parts
                .first()
                .and_then(|part| part.as_integer())
                .ok_or_else(|| unexpected("CHANGES", row))?;
            let text = parts
                .get(1)
                .and_then(|part| part.as_string().ok().flatten())
                .unwrap_or_default();

            notes.push((sequence, text));
        }

        Ok(notes)
    }

    /// Sends `MSET`.
    pub fn mset(&mut self, pairs: &[(&str, &str)]) -> Result<(), Error> {
        self.ok(with_pairs(args!["MSET"], pairs))
    }

    /// Sends `GETSET` and returns the previous value.
    pub fn getset(&mut self, key: &str, value: &str) -> Result<Option<String>, Error> {
        self.bulk_or_null(["GETSET", key, value])
    }

    /// Sends `APPEND` and returns the new length.
    pub fn append(&mut self, key: &str, value: &str) -> Result<i64, Error> {
        self.integer(["APPEND", key, value])
    }

    /// Sends `STRLEN`.
    pub fn strlen(&mut self, key: &str) -> Result<i64, Error> {
        self.integer(["STRLEN", key])
    }

    /// Sends `DECR`.
    pub fn decr(&mut self, key: &str) -> Result<i64, Error> {
        self.integer(["DECR", key])
    }

    /// Sends `INCRBY`.
    pub fn incr_by(&mut self, key: &str, delta: i64) -> Result<i64, Error> {
        self.integer(["INCRBY", key, Num::int(delta).as_str()])
    }

    /// Sends `DECRBY`.
    pub fn decr_by(&mut self, key: &str, delta: i64) -> Result<i64, Error> {
        self.integer(["DECRBY", key, Num::int(delta).as_str()])
    }

    /// Sends `UNLINK` for one key.
    pub fn unlink_key(&mut self, key: &str) -> Result<i64, Error> {
        self.unlink(&[key])
    }

    /// Sends `UNLINK` and returns how many keys were removed.
    pub fn unlink(&mut self, keys: &[&str]) -> Result<i64, Error> {
        self.integer(join("UNLINK", keys))
    }

    /// Sends `EXISTS` for one key.
    pub fn exists_key(&mut self, key: &str) -> Result<i64, Error> {
        self.exists(&[key])
    }

    /// Sends `EXISTS` and returns how many of the keys exist.
    pub fn exists(&mut self, keys: &[&str]) -> Result<i64, Error> {
        self.integer(join("EXISTS", keys))
    }

    /// Sends `TYPE`: `string`, `list`, `set`, `hash`, `zset`, `stream`, or `none`.
    pub fn key_type(&mut self, key: &str) -> Result<String, Error> {
        self.text(["TYPE", key])
    }

    /// Sends `RENAME`.
    pub fn rename(&mut self, key: &str, new_key: &str) -> Result<(), Error> {
        self.ok(["RENAME", key, new_key])
    }

    /// Sends `SCAN` with an optional `MATCH` pattern and `COUNT` hint.
    pub fn scan(
        &mut self,
        cursor: u64,
        pattern: Option<&str>,
        count: Option<u64>,
    ) -> Result<ScanPage, Error> {
        let mut arguments = args!["SCAN", cursor];

        push_scan_options(&mut arguments, pattern, count);

        let (cursor, keys) = parse_scan("SCAN", self.run(arguments)?)?;

        Ok(ScanPage { cursor, keys })
    }

    /// Sends `DBSIZE` for the selected database.
    pub fn dbsize(&mut self) -> Result<i64, Error> {
        self.integer(["DBSIZE"])
    }

    /// Sends `EXPIREAT`, or `PEXPIREAT` when `when` is not a whole second.
    pub fn expire_at(&mut self, key: &str, when: SystemTime) -> Result<bool, Error> {
        let millis = unix_millis(when)?;

        if millis % 1000 == 0 {
            return self.flag(args!["EXPIREAT", key, millis / 1000]);
        }

        self.flag(args!["PEXPIREAT", key, millis])
    }

    /// Sends `PEXPIRE`. False when the key does not exist.
    pub fn pexpire(&mut self, key: &str, ttl: Duration) -> Result<bool, Error> {
        self.flag(args!["PEXPIRE", key, duration_millis(ttl)])
    }

    /// Sends `TTL`: seconds left, -1 without an expiry, -2 when the key is missing.
    pub fn ttl(&mut self, key: &str) -> Result<i64, Error> {
        self.integer(["TTL", key])
    }

    /// Sends `PTTL`: milliseconds left, -1 without an expiry, -2 when the key is missing.
    pub fn pttl(&mut self, key: &str) -> Result<i64, Error> {
        self.integer(["PTTL", key])
    }

    /// Sends `PERSIST`. False when the key is missing or has no expiry.
    pub fn persist(&mut self, key: &str) -> Result<bool, Error> {
        self.flag(["PERSIST", key])
    }

    /// Sends `LPUSH` and returns the new length.
    pub fn lpush(&mut self, key: &str, values: &[&str]) -> Result<i64, Error> {
        self.integer(with(args!["LPUSH", key], values))
    }

    /// Sends `RPUSH` and returns the new length.
    pub fn rpush(&mut self, key: &str, values: &[&str]) -> Result<i64, Error> {
        self.integer(with(args!["RPUSH", key], values))
    }

    /// Sends `LPOP` for one element.
    pub fn lpop(&mut self, key: &str) -> Result<Option<String>, Error> {
        self.bulk_or_null(["LPOP", key])
    }

    /// Sends `LPOP` with a count. A missing list returns an empty vector.
    pub fn lpop_count(&mut self, key: &str, count: u64) -> Result<Vec<String>, Error> {
        self.strings(args!["LPOP", key, count])
    }

    /// Sends `RPOP` for one element.
    pub fn rpop(&mut self, key: &str) -> Result<Option<String>, Error> {
        self.bulk_or_null(["RPOP", key])
    }

    /// Sends `RPOP` with a count. A missing list returns an empty vector.
    pub fn rpop_count(&mut self, key: &str, count: u64) -> Result<Vec<String>, Error> {
        self.strings(args!["RPOP", key, count])
    }

    /// Sends `LLEN`.
    pub fn llen(&mut self, key: &str) -> Result<i64, Error> {
        self.integer(["LLEN", key])
    }

    /// Sends `LINDEX`.
    pub fn lindex(&mut self, key: &str, index: i64) -> Result<Option<String>, Error> {
        self.bulk_or_null(args!["LINDEX", key, index])
    }

    /// Sends `LRANGE`. `stop` is inclusive and negative indexes count from the tail.
    pub fn lrange(&mut self, key: &str, start: i64, stop: i64) -> Result<Vec<String>, Error> {
        self.strings(args!["LRANGE", key, start, stop])
    }

    /// Sends `LTRIM`.
    pub fn ltrim(&mut self, key: &str, start: i64, stop: i64) -> Result<(), Error> {
        self.ok(args!["LTRIM", key, start, stop])
    }

    /// Sends `BLPOP` for one key. Returns `(key, value)`, or `None` when
    /// `timeout` passes. `Duration::ZERO` waits forever.
    pub fn blpop_key(
        &mut self,
        key: &str,
        timeout: Duration,
    ) -> Result<Option<(String, String)>, Error> {
        self.blpop(&[key], timeout)
    }

    /// Sends `BLPOP`. Returns `(key, value)`, or `None` when `timeout` passes.
    /// `Duration::ZERO` waits forever. The timeout rounds up to whole seconds.
    pub fn blpop(
        &mut self,
        keys: &[&str],
        timeout: Duration,
    ) -> Result<Option<(String, String)>, Error> {
        self.blocking_pop("BLPOP", keys, timeout)
    }

    /// Sends `BRPOP` for one key. Returns `(key, value)`, or `None` when
    /// `timeout` passes.
    pub fn brpop_key(
        &mut self,
        key: &str,
        timeout: Duration,
    ) -> Result<Option<(String, String)>, Error> {
        self.brpop(&[key], timeout)
    }

    /// Sends `BRPOP`. Returns `(key, value)`, or `None` when `timeout` passes.
    pub fn brpop(
        &mut self,
        keys: &[&str],
        timeout: Duration,
    ) -> Result<Option<(String, String)>, Error> {
        self.blocking_pop("BRPOP", keys, timeout)
    }

    /// Sends `SADD` and returns how many members were new.
    pub fn sadd(&mut self, key: &str, members: &[&str]) -> Result<i64, Error> {
        self.integer(with(args!["SADD", key], members))
    }

    /// Sends `SREM` and returns how many members were removed.
    pub fn srem(&mut self, key: &str, members: &[&str]) -> Result<i64, Error> {
        self.integer(with(args!["SREM", key], members))
    }

    /// Sends `SISMEMBER`.
    pub fn sismember(&mut self, key: &str, member: &str) -> Result<bool, Error> {
        self.flag(["SISMEMBER", key, member])
    }

    /// Sends `SCARD`.
    pub fn scard(&mut self, key: &str) -> Result<i64, Error> {
        self.integer(["SCARD", key])
    }

    /// Sends `SMEMBERS`.
    pub fn smembers(&mut self, key: &str) -> Result<Vec<String>, Error> {
        self.strings(["SMEMBERS", key])
    }

    /// Sends `SINTER`.
    pub fn sinter(&mut self, keys: &[&str]) -> Result<Vec<String>, Error> {
        self.strings(join("SINTER", keys))
    }

    /// Sends `SUNION`.
    pub fn sunion(&mut self, keys: &[&str]) -> Result<Vec<String>, Error> {
        self.strings(join("SUNION", keys))
    }

    /// Sends `SDIFF`.
    pub fn sdiff(&mut self, keys: &[&str]) -> Result<Vec<String>, Error> {
        self.strings(join("SDIFF", keys))
    }

    /// Sends `SINTERSTORE` and returns the size of `destination`.
    pub fn sinterstore(&mut self, destination: &str, keys: &[&str]) -> Result<i64, Error> {
        self.integer(with(args!["SINTERSTORE", destination], keys))
    }

    /// Sends `SUNIONSTORE` and returns the size of `destination`.
    pub fn sunionstore(&mut self, destination: &str, keys: &[&str]) -> Result<i64, Error> {
        self.integer(with(args!["SUNIONSTORE", destination], keys))
    }

    /// Sends `SDIFFSTORE` and returns the size of `destination`.
    pub fn sdiffstore(&mut self, destination: &str, keys: &[&str]) -> Result<i64, Error> {
        self.integer(with(args!["SDIFFSTORE", destination], keys))
    }

    /// Sends `SMOVE`. False when `member` was not in `source`.
    pub fn smove(&mut self, source: &str, destination: &str, member: &str) -> Result<bool, Error> {
        self.flag(["SMOVE", source, destination, member])
    }

    /// Sends `SPOP` for one member.
    pub fn spop(&mut self, key: &str) -> Result<Option<String>, Error> {
        self.bulk_or_null(["SPOP", key])
    }

    /// Sends `SPOP` with a count and returns the removed members.
    pub fn spop_count(&mut self, key: &str, count: u64) -> Result<Vec<String>, Error> {
        self.strings(args!["SPOP", key, count])
    }

    /// Sends `SRANDMEMBER` for one member.
    pub fn srandmember(&mut self, key: &str) -> Result<Option<String>, Error> {
        self.bulk_or_null(["SRANDMEMBER", key])
    }

    /// Sends `SRANDMEMBER` with a count. A positive count returns distinct members;
    /// a negative count may repeat members.
    pub fn srandmember_count(&mut self, key: &str, count: i64) -> Result<Vec<String>, Error> {
        self.strings(args!["SRANDMEMBER", key, count])
    }

    /// Sends `JSON.GET`. `None` when the key or path is absent. Pass `"$"` for the whole document.
    pub fn json_get(&mut self, key: &str, path: &str) -> Result<Option<String>, Error> {
        self.bulk_or_null(["JSON.GET", key, path])
    }

    /// Sends `JSON.MGET`: one path from several keys of one slot. A missing key is `None`.
    pub fn json_mget(&mut self, keys: &[&str], path: &str) -> Result<Vec<Option<String>>, Error> {
        let mut arguments = join("JSON.MGET", keys);

        arguments.push(path.to_owned());
        self.optional_strings(arguments)
    }

    /// Sends `JSON.SET`. `false` when the path cannot be reached and nothing was written.
    pub fn json_set(&mut self, key: &str, path: &str, json: &str) -> Result<bool, Error> {
        Ok(!matches!(
            self.run(["JSON.SET", key, path, json])?,
            RespValue::Null
        ))
    }

    /// Sends `JSON.MERGE` (RFC 7396). `false` when the path cannot be reached.
    pub fn json_merge(&mut self, key: &str, path: &str, patch: &str) -> Result<bool, Error> {
        Ok(!matches!(
            self.run(["JSON.MERGE", key, path, patch])?,
            RespValue::Null
        ))
    }

    /// Sends `JSON.DEL` and returns 1 when a field, element or the whole key was removed.
    pub fn json_del(&mut self, key: &str, path: &str) -> Result<i64, Error> {
        self.integer(["JSON.DEL", key, path])
    }

    /// Sends `JSON.TYPE`: object, array, string, integer, number, boolean or null; `None` when absent.
    pub fn json_type(&mut self, key: &str, path: &str) -> Result<Option<String>, Error> {
        self.bulk_or_null(["JSON.TYPE", key, path])
    }

    /// Sends `JSON.STRLEN`. `None` when the key or path is absent.
    pub fn json_strlen(&mut self, key: &str, path: &str) -> Result<Option<i64>, Error> {
        self.json_integer(["JSON.STRLEN", key, path])
    }

    /// Sends `JSON.ARRLEN`. `None` when the key or path is absent.
    pub fn json_arrlen(&mut self, key: &str, path: &str) -> Result<Option<i64>, Error> {
        self.json_integer(["JSON.ARRLEN", key, path])
    }

    /// Sends `JSON.OBJKEYS`. Empty when the key or path is absent.
    pub fn json_objkeys(&mut self, key: &str, path: &str) -> Result<Vec<String>, Error> {
        self.strings(["JSON.OBJKEYS", key, path])
    }

    /// Sends `JSON.NUMINCRBY` and returns the new number as JSON text; `None` when absent.
    pub fn json_numincrby(
        &mut self,
        key: &str,
        path: &str,
        by: f64,
    ) -> Result<Option<String>, Error> {
        self.bulk_or_null(["JSON.NUMINCRBY", key, path, Num::json_number(by).as_str()])
    }

    /// Sends `JSON.TOGGLE` and returns the new boolean; `None` when absent.
    pub fn json_toggle(&mut self, key: &str, path: &str) -> Result<Option<bool>, Error> {
        Ok(self
            .json_integer(["JSON.TOGGLE", key, path])?
            .map(|value| value != 0))
    }

    /// Sends `JSON.ARRAPPEND` with one JSON value and returns the new length; `None` when absent.
    pub fn json_arrappend(
        &mut self,
        key: &str,
        path: &str,
        json: &str,
    ) -> Result<Option<i64>, Error> {
        self.json_integer(["JSON.ARRAPPEND", key, path, json])
    }

    /// Sends `JSON.CLEAR` and returns how many values changed.
    pub fn json_clear(&mut self, key: &str, path: &str) -> Result<i64, Error> {
        Ok(self.json_integer(["JSON.CLEAR", key, path])?.unwrap_or(0))
    }

    fn json_integer<const N: usize>(&mut self, arguments: [&str; N]) -> Result<Option<i64>, Error> {
        let value = self.run(arguments)?;

        if value.is_null() {
            return Ok(None);
        }

        value
            .as_integer()
            .map(Some)
            .ok_or_else(|| unexpected("JSON", &value))
    }

    /// Sends `HSET` and returns how many fields were new.
    pub fn hset(&mut self, key: &str, fields: &[(&str, &str)]) -> Result<i64, Error> {
        self.integer(with_pairs(args!["HSET", key], fields))
    }

    /// Sends `HGET`.
    pub fn hget(&mut self, key: &str, field: &str) -> Result<Option<String>, Error> {
        self.bulk_or_null(["HGET", key, field])
    }

    /// Sends `HDEL` and returns how many fields were removed.
    pub fn hdel(&mut self, key: &str, fields: &[&str]) -> Result<i64, Error> {
        self.integer(with(args!["HDEL", key], fields))
    }

    /// Sends `HLEN`.
    pub fn hlen(&mut self, key: &str) -> Result<i64, Error> {
        self.integer(["HLEN", key])
    }

    /// Sends `HGETALL`.
    pub fn hgetall(&mut self, key: &str) -> Result<HashMap<String, String>, Error> {
        let pairs = pairs(&self.run(["HGETALL", key])?)?;

        Ok(pairs.into_iter().collect())
    }

    /// Sends `HMGET`. A missing field is `None`.
    pub fn hmget(&mut self, key: &str, fields: &[&str]) -> Result<Vec<Option<String>>, Error> {
        self.optional_strings(with(args!["HMGET", key], fields))
    }

    /// Sends `HEXISTS`.
    pub fn hexists(&mut self, key: &str, field: &str) -> Result<bool, Error> {
        self.flag(["HEXISTS", key, field])
    }

    /// Sends `HKEYS`.
    pub fn hkeys(&mut self, key: &str) -> Result<Vec<String>, Error> {
        self.strings(["HKEYS", key])
    }

    /// Sends `HVALS`.
    pub fn hvals(&mut self, key: &str) -> Result<Vec<String>, Error> {
        self.strings(["HVALS", key])
    }

    /// Sends `HINCRBY` and returns the new value.
    pub fn hincr_by(&mut self, key: &str, field: &str, delta: i64) -> Result<i64, Error> {
        self.integer(args!["HINCRBY", key, field, delta])
    }

    /// Sends `HSETNX`. False when the field already existed.
    pub fn hsetnx(&mut self, key: &str, field: &str, value: &str) -> Result<bool, Error> {
        self.flag(["HSETNX", key, field, value])
    }

    /// Sends `HSTRLEN`.
    pub fn hstrlen(&mut self, key: &str, field: &str) -> Result<i64, Error> {
        self.integer(["HSTRLEN", key, field])
    }

    /// Sets a TTL on hash fields. Sends `HEXPIRE` for whole seconds, otherwise `HPEXPIRE`.
    /// One reply per field: `1` set, `0` condition not met, `2` deleted by a zero TTL,
    /// `-2` no such field or key.
    ///
    /// ```no_run
    /// # use std::time::Duration;
    /// # let mut db = ruvio_client::Client::connect("127.0.0.1", 6379)?;
    /// db.hset("session:42", &[("token", "abc"), ("user", "ada")])?;
    /// db.hexpire("session:42", Duration::from_secs(900), &["token"])?;
    /// # Ok::<(), ruvio_client::Error>(())
    /// ```
    pub fn hexpire(
        &mut self,
        key: &str,
        ttl: Duration,
        fields: &[&str],
    ) -> Result<Vec<i64>, Error> {
        self.hexpire_with(key, ttl, FieldExpireCondition::Always, fields)
    }

    /// Like [`Client::hexpire`] with `NX`, `XX`, `GT`, or `LT`.
    pub fn hexpire_with(
        &mut self,
        key: &str,
        ttl: Duration,
        condition: FieldExpireCondition,
        fields: &[&str],
    ) -> Result<Vec<i64>, Error> {
        let arguments = if ttl.subsec_nanos() == 0 {
            args!["HEXPIRE", key, duration_secs(ttl)]
        } else {
            args!["HPEXPIRE", key, duration_millis(ttl)]
        };

        self.integers(field_block(arguments, condition, fields))
    }

    /// Sends `HPEXPIRE`: a TTL in milliseconds on hash fields.
    pub fn hpexpire(
        &mut self,
        key: &str,
        ttl: Duration,
        fields: &[&str],
    ) -> Result<Vec<i64>, Error> {
        self.integers(field_block(
            args!["HPEXPIRE", key, duration_millis(ttl)],
            FieldExpireCondition::Always,
            fields,
        ))
    }

    /// Sets an absolute deadline on hash fields. Sends `HEXPIREAT` for a whole second,
    /// otherwise `HPEXPIREAT`. A time in the past deletes the field (reply `2`).
    pub fn hexpire_at(
        &mut self,
        key: &str,
        when: SystemTime,
        fields: &[&str],
    ) -> Result<Vec<i64>, Error> {
        let millis = unix_millis(when)?;
        let arguments = if millis % 1000 == 0 {
            args!["HEXPIREAT", key, millis / 1000]
        } else {
            args!["HPEXPIREAT", key, millis]
        };

        self.integers(field_block(arguments, FieldExpireCondition::Always, fields))
    }

    /// Sends `HTTL`: seconds left per field, `-1` without a TTL, `-2` when missing.
    pub fn httl(&mut self, key: &str, fields: &[&str]) -> Result<Vec<i64>, Error> {
        self.integers(field_block(
            args!["HTTL", key],
            FieldExpireCondition::Always,
            fields,
        ))
    }

    /// Sends `HPTTL`: milliseconds left per field, `-1` without a TTL, `-2` when missing.
    pub fn hpttl(&mut self, key: &str, fields: &[&str]) -> Result<Vec<i64>, Error> {
        self.integers(field_block(
            args!["HPTTL", key],
            FieldExpireCondition::Always,
            fields,
        ))
    }

    /// Sends `HEXPIRETIME`: the Unix second each field expires at, `-1` or `-2` as `HTTL`.
    pub fn hexpiretime(&mut self, key: &str, fields: &[&str]) -> Result<Vec<i64>, Error> {
        self.integers(field_block(
            args!["HEXPIRETIME", key],
            FieldExpireCondition::Always,
            fields,
        ))
    }

    /// Sends `HPEXPIRETIME`: the Unix millisecond each field expires at.
    pub fn hpexpiretime(&mut self, key: &str, fields: &[&str]) -> Result<Vec<i64>, Error> {
        self.integers(field_block(
            args!["HPEXPIRETIME", key],
            FieldExpireCondition::Always,
            fields,
        ))
    }

    /// Sends `HPERSIST`: `1` removed a TTL, `-1` the field had none, `-2` it is missing.
    pub fn hpersist(&mut self, key: &str, fields: &[&str]) -> Result<Vec<i64>, Error> {
        self.integers(field_block(
            args!["HPERSIST", key],
            FieldExpireCondition::Always,
            fields,
        ))
    }

    /// Sends `HSCAN` with an optional `MATCH` pattern and `COUNT` hint.
    pub fn hscan(
        &mut self,
        key: &str,
        cursor: u64,
        pattern: Option<&str>,
        count: Option<u64>,
    ) -> Result<HashScanPage, Error> {
        let mut arguments = args!["HSCAN", key, cursor];

        push_scan_options(&mut arguments, pattern, count);

        let (cursor, flat) = parse_scan("HSCAN", self.run(arguments)?)?;
        let mut fields = Vec::with_capacity(flat.len() / 2);
        let mut items = flat.into_iter();

        while let (Some(field), Some(value)) = (items.next(), items.next()) {
            fields.push((field, value));
        }

        Ok(HashScanPage { cursor, fields })
    }

    /// Sends `ZADD` with `(member, score)` pairs and returns how many members were new.
    ///
    /// ```no_run
    /// # let mut db = ruvio_client::Client::connect("127.0.0.1", 6379)?;
    /// db.zadd("board", &[("ada", 100.0), ("bob", 50.0)])?;
    /// # Ok::<(), ruvio_client::Error>(())
    /// ```
    pub fn zadd(&mut self, key: &str, entries: &[(&str, f64)]) -> Result<i64, Error> {
        self.zadd_with(key, SortedSetAddOptions::default(), entries)
    }

    /// Sends `ZADD` with `NX`, `XX`, `GT`, `LT`, or `CH`.
    /// Without [`SortedSetAddOptions::changed`] the reply counts new members.
    pub fn zadd_with(
        &mut self,
        key: &str,
        options: SortedSetAddOptions,
        entries: &[(&str, f64)],
    ) -> Result<i64, Error> {
        let scores: Vec<Num> = entries
            .iter()
            .map(|(_, score)| Num::float(*score))
            .collect();
        let mut arguments: Vec<&str> = Vec::with_capacity(7 + entries.len() * 2);

        arguments.extend(["ZADD", key]);

        for (enabled, flag) in [
            (options.if_not_exists, "NX"),
            (options.if_exists, "XX"),
            (options.greater_than, "GT"),
            (options.less_than, "LT"),
            (options.changed, "CH"),
        ] {
            if enabled {
                arguments.push(flag);
            }
        }

        for (score, (member, _)) in scores.iter().zip(entries) {
            arguments.push(score.as_str());
            arguments.push(member);
        }

        self.integer(arguments)
    }

    /// Sends `ZADD ... INCR`. Returns the new score, or `None` when an option skipped the update.
    pub fn zadd_incr(
        &mut self,
        key: &str,
        member: &str,
        delta: f64,
        options: SortedSetAddOptions,
    ) -> Result<Option<f64>, Error> {
        let mut arguments = zadd_arguments(key, options);

        arguments.extend(args!["INCR", delta, member]);
        self.bulk_or_null(arguments)?
            .map(|text| parse_score(&text))
            .transpose()
    }

    /// Sends `ZINCRBY` and returns the new score.
    pub fn zincr_by(&mut self, key: &str, member: &str, delta: f64) -> Result<f64, Error> {
        parse_score(&self.text(args!["ZINCRBY", key, delta, member])?)
    }

    /// Sends `GEOADD`. Each place is longitude, latitude, then member.
    pub fn geo_add(&mut self, key: &str, places: &[(f64, f64, &str)]) -> Result<i64, Error> {
        let mut arguments = args!["GEOADD", key];

        for (longitude, latitude, member) in places {
            arguments.push(longitude.to_string());
            arguments.push(latitude.to_string());
            arguments.push((*member).to_owned());
        }

        self.integer(arguments)
    }

    /// Sends `GEODIST`. `unit` is `m`, `km`, `ft`, or `mi`. `None` asks for meters.
    pub fn geo_dist(
        &mut self,
        key: &str,
        from: &str,
        to: &str,
        unit: Option<&str>,
    ) -> Result<Option<f64>, Error> {
        let mut arguments = args!["GEODIST", key, from, to];

        if let Some(unit) = unit {
            arguments.push(unit.to_owned());
        }

        self.bulk_or_null(arguments)?
            .map(|text| parse_score(&text))
            .transpose()
    }

    /// Sends `GEOHASH`. A missing member is `None`.
    pub fn geo_hash(&mut self, key: &str, members: &[&str]) -> Result<Vec<Option<String>>, Error> {
        let mut arguments = args!["GEOHASH", key];

        arguments.extend(members.iter().map(|member| (*member).to_owned()));
        self.optional_strings(arguments)
    }

    /// Sends `GEOPOS`. A missing member is `None`.
    pub fn geo_pos(
        &mut self,
        key: &str,
        members: &[&str],
    ) -> Result<Vec<Option<(f64, f64)>>, Error> {
        let mut arguments = args!["GEOPOS", key];

        arguments.extend(members.iter().map(|member| (*member).to_owned()));

        let reply = self.run(arguments)?;
        let Some(items) = reply.as_array() else {
            return Err(unexpected("GEOPOS", &reply));
        };

        let mut points = Vec::with_capacity(items.len());

        for item in items {
            if item.is_null() {
                points.push(None);

                continue;
            }

            let Some(pair) = item.as_array() else {
                return Err(unexpected("GEOPOS", item));
            };

            if pair.len() != 2 {
                return Err(unexpected("GEOPOS", item));
            }

            let longitude = parse_score(&pair[0].as_string()?.unwrap_or_default())?;
            let latitude = parse_score(&pair[1].as_string()?.unwrap_or_default())?;

            points.push(Some((longitude, latitude)));
        }

        Ok(points)
    }

    /// Sends `GEOSEARCH ... FROMLONLAT ... BYRADIUS ... ASC`.
    pub fn geo_search(
        &mut self,
        key: &str,
        longitude: f64,
        latitude: f64,
        radius: f64,
        unit: &str,
    ) -> Result<Vec<String>, Error> {
        self.strings(args![
            "GEOSEARCH",
            key,
            "FROMLONLAT",
            longitude,
            latitude,
            "BYRADIUS",
            radius,
            unit,
            "ASC"
        ])
    }

    /// Sends `GEOSEARCHSTORE` from a member and a radius. Returns how many members were stored.
    pub fn geo_search_store(
        &mut self,
        destination: &str,
        source: &str,
        member: &str,
        radius: f64,
        unit: &str,
    ) -> Result<i64, Error> {
        self.integer(args![
            "GEOSEARCHSTORE",
            destination,
            source,
            "FROMMEMBER",
            member,
            "BYRADIUS",
            radius,
            unit
        ])
    }

    /// Sends `GEORADIUSBYMEMBER ... ASC`.
    pub fn geo_radius_by_member(
        &mut self,
        key: &str,
        member: &str,
        radius: f64,
        unit: &str,
    ) -> Result<Vec<String>, Error> {
        self.strings(args!["GEORADIUSBYMEMBER", key, member, radius, unit, "ASC"])
    }

    /// Sends `GEORADIUS ... ASC`.
    pub fn geo_radius(
        &mut self,
        key: &str,
        longitude: f64,
        latitude: f64,
        radius: f64,
        unit: &str,
    ) -> Result<Vec<String>, Error> {
        self.strings(args![
            "GEORADIUS",
            key,
            longitude,
            latitude,
            radius,
            unit,
            "ASC"
        ])
    }

    /// Sends `ZRANGE` by rank. `stop` is inclusive.
    pub fn zrange(&mut self, key: &str, start: i64, stop: i64) -> Result<Vec<String>, Error> {
        self.strings(args!["ZRANGE", key, start, stop])
    }

    /// Sends `ZREVRANGE` by rank. `stop` is inclusive.
    pub fn zrevrange(&mut self, key: &str, start: i64, stop: i64) -> Result<Vec<String>, Error> {
        self.strings(args!["ZREVRANGE", key, start, stop])
    }

    /// Sends `ZRANGE ... WITHSCORES`.
    pub fn zrange_with_scores(
        &mut self,
        key: &str,
        start: i64,
        stop: i64,
    ) -> Result<Vec<SortedSetEntry>, Error> {
        sorted_set(&self.run(args!["ZRANGE", key, start, stop, "WITHSCORES"])?)
    }

    /// Sends `ZREVRANGE ... WITHSCORES`.
    pub fn zrevrange_with_scores(
        &mut self,
        key: &str,
        start: i64,
        stop: i64,
    ) -> Result<Vec<SortedSetEntry>, Error> {
        sorted_set(&self.run(args!["ZREVRANGE", key, start, stop, "WITHSCORES"])?)
    }

    /// Sends `ZRANGE min max BYLEX`. Bounds are `-`, `+`, `[member`, or `(member`.
    pub fn zrange_by_lex(&mut self, key: &str, min: &str, max: &str) -> Result<Vec<String>, Error> {
        self.strings(["ZRANGE", key, min, max, "BYLEX"])
    }

    /// Sends `ZRANGEBYSCORE ... WITHSCORES`. `min` and `max` accept `-inf`, `+inf`, and `(` for exclusive bounds.
    pub fn zrange_by_score_with_scores(
        &mut self,
        key: &str,
        min: &str,
        max: &str,
    ) -> Result<Vec<SortedSetEntry>, Error> {
        sorted_set(&self.run(["ZRANGEBYSCORE", key, min, max, "WITHSCORES"])?)
    }

    /// Sends `ZREM` and returns how many members were removed.
    pub fn zrem(&mut self, key: &str, members: &[&str]) -> Result<i64, Error> {
        self.integer(with(args!["ZREM", key], members))
    }

    /// Sends `ZPOPMIN key count`: removes and returns up to `count` lowest-scored members.
    pub fn zpopmin(&mut self, key: &str, count: u64) -> Result<Vec<SortedSetEntry>, Error> {
        sorted_set(&self.run(args!["ZPOPMIN", key, count])?)
    }

    /// Sends `ZPOPMAX key count`: removes and returns up to `count` highest-scored members.
    pub fn zpopmax(&mut self, key: &str, count: u64) -> Result<Vec<SortedSetEntry>, Error> {
        sorted_set(&self.run(args!["ZPOPMAX", key, count])?)
    }

    /// Sends `BZPOPMIN`. Returns the key and the popped member, or `None` when `timeout`
    /// passes. `Duration::ZERO` waits forever. The timeout rounds up to whole seconds.
    pub fn bzpopmin(
        &mut self,
        keys: &[&str],
        timeout: Duration,
    ) -> Result<Option<(String, SortedSetEntry)>, Error> {
        self.blocking_sorted_pop("BZPOPMIN", keys, timeout)
    }

    /// Sends `BZPOPMAX`. Like [`Client::bzpopmin`], highest score first.
    pub fn bzpopmax(
        &mut self,
        keys: &[&str],
        timeout: Duration,
    ) -> Result<Option<(String, SortedSetEntry)>, Error> {
        self.blocking_sorted_pop("BZPOPMAX", keys, timeout)
    }

    /// Sends `ZREMRANGEBYRANK`. `stop` is inclusive; negative ranks count from the end.
    pub fn zremrangebyrank(&mut self, key: &str, start: i64, stop: i64) -> Result<i64, Error> {
        self.integer(args!["ZREMRANGEBYRANK", key, start, stop])
    }

    /// Sends `ZREMRANGEBYSCORE`. Bounds accept `-inf`, `+inf`, and `(` for exclusive.
    pub fn zremrangebyscore(&mut self, key: &str, min: &str, max: &str) -> Result<i64, Error> {
        self.integer(["ZREMRANGEBYSCORE", key, min, max])
    }

    /// Sends `ZREMRANGEBYLEX`. Bounds are `-`, `+`, `[member`, or `(member`.
    pub fn zremrangebylex(&mut self, key: &str, min: &str, max: &str) -> Result<i64, Error> {
        self.integer(["ZREMRANGEBYLEX", key, min, max])
    }

    /// Sends `ZCARD`.
    pub fn zcard(&mut self, key: &str) -> Result<i64, Error> {
        self.integer(["ZCARD", key])
    }

    /// Sends `ZSCORE`.
    pub fn zscore(&mut self, key: &str, member: &str) -> Result<Option<f64>, Error> {
        self.bulk_or_null(["ZSCORE", key, member])?
            .map(|text| parse_score(&text))
            .transpose()
    }

    /// Sends `ZRANK`: the 0-based rank from the lowest score.
    pub fn zrank(&mut self, key: &str, member: &str) -> Result<Option<i64>, Error> {
        self.optional_integer(["ZRANK", key, member])
    }

    /// Sends `ZREVRANK`: the 0-based rank from the highest score.
    pub fn zrevrank(&mut self, key: &str, member: &str) -> Result<Option<i64>, Error> {
        self.optional_integer(["ZREVRANK", key, member])
    }

    /// Sends `BF.RESERVE`.
    pub fn bf_reserve(&mut self, key: &str, error_rate: f64, capacity: u64) -> Result<(), Error> {
        self.ok(args!["BF.RESERVE", key, error_rate, capacity])
    }

    /// Sends `BF.ADD`. False when the item may already have been added.
    pub fn bf_add(&mut self, key: &str, item: &str) -> Result<bool, Error> {
        self.flag(["BF.ADD", key, item])
    }

    /// Sends `BF.EXISTS`. True means "maybe added"; false means "never added".
    pub fn bf_exists(&mut self, key: &str, item: &str) -> Result<bool, Error> {
        self.flag(["BF.EXISTS", key, item])
    }

    /// Sends `XADD NOMKSTREAM`. A missing key stays absent and the reply is nil.
    pub fn xadd_nomkstream(
        &mut self,
        key: &str,
        id: &str,
        fields: &[(&str, &str)],
    ) -> Result<Option<String>, Error> {
        self.bulk_or_null(with_pairs(args!["XADD", key, "NOMKSTREAM", id], fields))
    }

    /// Sends `XADD key DELAY ms`. The entry stays hidden from `XREADGROUP` until the delay passes.
    pub fn xadd_delay(
        &mut self,
        key: &str,
        delay_ms: u64,
        id: &str,
        fields: &[(&str, &str)],
    ) -> Result<String, Error> {
        self.text(with_pairs(
            args!["XADD", key, "DELAY", delay_ms, id],
            fields,
        ))
    }

    /// Sends `XADD` and returns the entry id. Use `*` for `id` to let the server pick one. `ms-*` fixes the millisecond.
    pub fn xadd(&mut self, key: &str, id: &str, fields: &[(&str, &str)]) -> Result<String, Error> {
        self.text(with_pairs(args!["XADD", key, id], fields))
    }

    /// Sends `XADD key MAXLEN n`: the stream keeps exactly the newest `max_len` entries.
    pub fn xadd_maxlen(
        &mut self,
        key: &str,
        max_len: u64,
        id: &str,
        fields: &[(&str, &str)],
    ) -> Result<String, Error> {
        self.text(with_pairs(
            args!["XADD", key, "MAXLEN", max_len, id],
            fields,
        ))
    }

    /// Sends `XLEN`.
    pub fn xlen(&mut self, key: &str) -> Result<i64, Error> {
        self.integer(["XLEN", key])
    }

    /// Sends `XINFO STREAM`.
    pub fn xinfo_stream(&mut self, key: &str) -> Result<RespValue, Error> {
        self.run(["XINFO", "STREAM", key])
    }

    /// Sends `XINFO GROUPS`.
    pub fn xinfo_groups(&mut self, key: &str) -> Result<RespValue, Error> {
        self.run(["XINFO", "GROUPS", key])
    }

    /// Sends `XINFO CONSUMERS`.
    pub fn xinfo_consumers(&mut self, key: &str, group: &str) -> Result<RespValue, Error> {
        self.run(["XINFO", "CONSUMERS", key, group])
    }

    /// Sends `XSETID`. The id must be `ms-seq` and at least the stream's current top id.
    pub fn xsetid(&mut self, key: &str, id: &str) -> Result<(), Error> {
        self.ok(["XSETID", key, id])
    }

    /// Sends `XAUTOCLAIM`. `start` is exclusive (`0-0` scans from the beginning).
    pub fn xautoclaim(
        &mut self,
        key: &str,
        group: &str,
        consumer: &str,
        min_idle_ms: u64,
        start: &str,
    ) -> Result<RespValue, Error> {
        self.run([
            "XAUTOCLAIM",
            key,
            group,
            consumer,
            Num::uint(min_idle_ms).as_str(),
            start,
        ])
    }

    /// Sends `XRANGE` from `start` to `end` (`-` and `+` for the ends), with an optional `COUNT`.
    pub fn xrange(
        &mut self,
        key: &str,
        start: &str,
        end: &str,
        count: Option<u64>,
    ) -> Result<Vec<StreamEntry>, Error> {
        let mut arguments = args!["XRANGE", key, start, end];

        if let Some(count) = count {
            arguments.extend(args!["COUNT", count]);
        }

        stream_entries(&self.run(arguments)?)
    }

    /// Sends `XREVRANGE` from `end` down to `start`, with an optional `COUNT`.
    pub fn xrevrange(
        &mut self,
        key: &str,
        end: &str,
        start: &str,
        count: Option<u64>,
    ) -> Result<Vec<StreamEntry>, Error> {
        let mut arguments = args!["XREVRANGE", key, end, start];

        if let Some(count) = count {
            arguments.extend(args!["COUNT", count]);
        }

        stream_entries(&self.run(arguments)?)
    }

    /// Sends `XDEL` and returns how many entries were removed.
    pub fn xdel(&mut self, key: &str, ids: &[&str]) -> Result<i64, Error> {
        self.integer(with(args!["XDEL", key], ids))
    }

    /// Sends `XTRIM MINID`. Entries below `id` are removed. `~` is trimmed exactly.
    pub fn xtrim_minid(&mut self, key: &str, id: &str) -> Result<i64, Error> {
        self.integer(args!["XTRIM", key, "MINID", id])
    }

    /// Sends `XTRIM MAXLEN` and returns how many entries were removed.
    pub fn xtrim_maxlen(&mut self, key: &str, max_len: u64) -> Result<i64, Error> {
        self.integer(args!["XTRIM", key, "MAXLEN", max_len])
    }

    /// Sends `XREAD` for `(key, id)` pairs. A block that times out returns an empty vector.
    pub fn xread(
        &mut self,
        options: &StreamReadOptions,
        streams: &[(&str, &str)],
    ) -> Result<Vec<StreamReadResult>, Error> {
        if options.no_ack {
            return Err(Error::Protocol(
                "NOACK applies only to XREADGROUP".to_owned(),
            ));
        }

        let mut arguments = args!["XREAD"];

        push_stream_read_options(&mut arguments, options);
        push_streams(&mut arguments, streams);
        stream_read(&self.run(arguments)?)
    }

    /// Sends `XGROUP CREATECONSUMER`. Returns 1 when the consumer name is new.
    pub fn xgroup_create_consumer(
        &mut self,
        key: &str,
        group: &str,
        consumer: &str,
    ) -> Result<i64, Error> {
        self.integer(args!["XGROUP", "CREATECONSUMER", key, group, consumer])
    }

    /// Sends `XGROUP CREATE`. With `make_stream` it adds `MKSTREAM`.
    pub fn xgroup_create(
        &mut self,
        key: &str,
        group: &str,
        id: &str,
        make_stream: bool,
    ) -> Result<(), Error> {
        let mut arguments = args!["XGROUP", "CREATE", key, group, id];

        if make_stream {
            arguments.push("MKSTREAM".to_owned());
        }

        self.ok(arguments)
    }

    /// Sends `XREADGROUP GROUP` with `COUNT`, `BLOCK`, and `NOACK` from `options`.
    /// Use `>` as the id for entries never delivered to the group.
    pub fn xreadgroup(
        &mut self,
        group: &str,
        consumer: &str,
        options: &StreamReadOptions,
        streams: &[(&str, &str)],
    ) -> Result<Vec<StreamReadResult>, Error> {
        let mut arguments = args!["XREADGROUP", "GROUP", group, consumer];

        push_stream_read_options(&mut arguments, options);

        if options.no_ack {
            arguments.push("NOACK".to_owned());
        }

        push_streams(&mut arguments, streams);
        stream_read(&self.run(arguments)?)
    }

    /// Sends `XGROUP DESTROY`. False when the group did not exist.
    pub fn xgroup_destroy(&mut self, key: &str, group: &str) -> Result<bool, Error> {
        self.flag(["XGROUP", "DESTROY", key, group])
    }

    /// Sends `XGROUP SETID`.
    pub fn xgroup_setid(&mut self, key: &str, group: &str, id: &str) -> Result<(), Error> {
        self.ok(["XGROUP", "SETID", key, group, id])
    }

    /// Sends `XGROUP DELCONSUMER` and returns how many pending entries the consumer had.
    pub fn xgroup_delconsumer(
        &mut self,
        key: &str,
        group: &str,
        consumer: &str,
    ) -> Result<i64, Error> {
        self.integer(["XGROUP", "DELCONSUMER", key, group, consumer])
    }

    /// Sends `XACK` and returns how many entries were acknowledged.
    pub fn xack(&mut self, key: &str, group: &str, ids: &[&str]) -> Result<i64, Error> {
        self.integer(with(args!["XACK", key, group], ids))
    }

    /// Sends the summary form of `XPENDING`: count, smallest id, largest id, and per-consumer counts.
    pub fn xpending_summary(&mut self, key: &str, group: &str) -> Result<RespValue, Error> {
        self.run(["XPENDING", key, group])
    }

    /// Sends the extended `XPENDING` form: ids from `start` to `end` (inclusive), at most `count`,
    /// narrowed by `filter`.
    pub fn xpending(
        &mut self,
        key: &str,
        group: &str,
        start: &str,
        end: &str,
        count: u64,
        filter: &StreamPendingFilter,
    ) -> Result<Vec<StreamPendingEntry>, Error> {
        let mut arguments = args!["XPENDING", key, group];

        if let Some(idle) = filter.min_idle {
            arguments.extend(args!["IDLE", duration_millis(idle)]);
        }

        arguments.extend(args![start, end, count]);

        if let Some(consumer) = &filter.consumer {
            arguments.push(consumer.clone());
        }

        let reply = self.run(arguments)?;
        let mut rows = Vec::new();

        for row in reply.as_array().unwrap_or_default() {
            let Some([id, owner, idle, deliveries]) = row.as_array() else {
                return Err(unexpected("XPENDING", row));
            };

            rows.push(StreamPendingEntry {
                id: id.as_string()?.unwrap_or_default(),
                consumer: owner.as_string()?.unwrap_or_default(),
                idle_millis: idle.as_integer().unwrap_or_default(),
                delivery_count: deliveries.as_integer().unwrap_or_default(),
            });
        }

        Ok(rows)
    }

    /// Sends `XCLAIM` with `IDLE`, `TIME`, `RETRYCOUNT`, `FORCE`, or `LASTID` from `options`.
    pub fn xclaim(
        &mut self,
        key: &str,
        group: &str,
        consumer: &str,
        min_idle: Duration,
        ids: &[&str],
        options: &StreamClaimOptions,
    ) -> Result<Vec<StreamEntry>, Error> {
        let arguments = claim_arguments(key, group, consumer, min_idle, ids, options)?;

        stream_entries(&self.run(arguments)?)
    }

    /// Sends `XCLAIM ... JUSTID` and returns only the claimed ids.
    pub fn xclaim_ids(
        &mut self,
        key: &str,
        group: &str,
        consumer: &str,
        min_idle: Duration,
        ids: &[&str],
        options: &StreamClaimOptions,
    ) -> Result<Vec<String>, Error> {
        let mut arguments = claim_arguments(key, group, consumer, min_idle, ids, options)?;

        arguments.push("JUSTID".to_owned());
        self.strings(arguments)
    }

    /// Sends `INFO`, or `INFO section`. The `prometheus` section returns Prometheus text.
    pub fn info(&mut self, section: Option<&str>) -> Result<String, Error> {
        match section {
            Some(section) => self.text(["INFO", section]),
            None => self.text(["INFO"]),
        }
    }

    /// Sends `CONFIG GET` and returns `(parameter, value)` pairs.
    pub fn config_get(&mut self, parameter: &str) -> Result<Vec<(String, String)>, Error> {
        pairs(&self.run(["CONFIG", "GET", parameter])?)
    }

    /// Sends `SAVE`.
    pub fn save(&mut self) -> Result<(), Error> {
        self.ok(["SAVE"])
    }

    /// Sends `BGSAVE`.
    pub fn bgsave(&mut self) -> Result<(), Error> {
        self.ok(["BGSAVE"])
    }

    /// Sends `FLUSHDB`. Only the selected database is emptied.
    pub fn flushdb(&mut self) -> Result<(), Error> {
        self.ok(["FLUSHDB"])
    }

    /// Sends `FLUSHALL`. Every database is emptied.
    pub fn flushall(&mut self) -> Result<(), Error> {
        self.ok(["FLUSHALL"])
    }

    /// Sends `SWAPDB`.
    pub fn swapdb(&mut self, first: u32, second: u32) -> Result<(), Error> {
        self.ok(args!["SWAPDB", first, second])
    }

    /// Sends `MOVE`. False when the key is missing or already exists in `database`.
    pub fn move_key(&mut self, key: &str, database: u32) -> Result<bool, Error> {
        self.flag(args!["MOVE", key, database])
    }

    /// Sends `MULTI`. Until [`Self::exec`], the server answers `QUEUED`, so queue commands
    /// with [`Client::execute`] rather than the typed methods.
    pub fn multi(&mut self) -> Result<(), Error> {
        self.ok(["MULTI"])
    }

    /// Sends `EXEC`. `None` when a watched key changed and the transaction was aborted.
    pub fn exec(&mut self) -> Result<Option<Vec<RespValue>>, Error> {
        match self.run(["EXEC"])? {
            RespValue::Null => Ok(None),
            RespValue::Array(items) => Ok(Some(items)),
            other => Err(unexpected("EXEC", &other)),
        }
    }

    /// Sends `DISCARD`.
    pub fn discard(&mut self) -> Result<(), Error> {
        self.ok(["DISCARD"])
    }

    /// Sends `WATCH` for one key.
    pub fn watch_key(&mut self, key: &str) -> Result<(), Error> {
        self.watch(&[key])
    }

    /// Sends `WATCH`.
    pub fn watch(&mut self, keys: &[&str]) -> Result<(), Error> {
        self.ok(join("WATCH", keys))
    }

    /// Sends `UNWATCH`.
    pub fn unwatch(&mut self) -> Result<(), Error> {
        self.ok(["UNWATCH"])
    }

    /// Sends `EVAL`. The key count is `keys.len()`.
    pub fn eval(
        &mut self,
        script: &str,
        keys: &[&str],
        arguments: &[&str],
    ) -> Result<RespValue, Error> {
        self.run(script_arguments("EVAL", script, keys, arguments))
    }

    /// Sends `EVALSHA`. The key count is `keys.len()`.
    pub fn evalsha(
        &mut self,
        sha: &str,
        keys: &[&str],
        arguments: &[&str],
    ) -> Result<RespValue, Error> {
        self.run(script_arguments("EVALSHA", sha, keys, arguments))
    }

    /// Sends `SCRIPT LOAD` and returns the SHA1 for [`Self::evalsha`].
    pub fn script_load(&mut self, script: &str) -> Result<String, Error> {
        self.text(["SCRIPT", "LOAD", script])
    }

    /// Sends `SCRIPT EXISTS`, one flag per hash.
    pub fn script_exists(&mut self, hashes: &[&str]) -> Result<Vec<bool>, Error> {
        let reply = self.run(with(args!["SCRIPT", "EXISTS"], hashes))?;

        Ok(reply
            .as_array()
            .unwrap_or_default()
            .iter()
            .map(|item| item.as_integer().unwrap_or_default() > 0)
            .collect())
    }

    /// Sends `SCRIPT FLUSH`.
    pub fn script_flush(&mut self) -> Result<(), Error> {
        self.ok(["SCRIPT", "FLUSH"])
    }

    /// Sends `SCRIPT KILL`.
    pub fn script_kill(&mut self) -> Result<(), Error> {
        self.ok(["SCRIPT", "KILL"])
    }

    /// Sends `FCALL`. The key count is `keys.len()`.
    pub fn fcall(
        &mut self,
        name: &str,
        keys: &[&str],
        arguments: &[&str],
    ) -> Result<RespValue, Error> {
        self.run(script_arguments("FCALL", name, keys, arguments))
    }

    /// Sends `FUNCTION LOAD name body`. The body is plain Lua, not a Redis library shebang,
    /// and is lost when the server restarts.
    pub fn function_load(&mut self, name: &str, body: &str) -> Result<(), Error> {
        self.ok(["FUNCTION", "LOAD", name, body])
    }

    /// Sends `FUNCTION LIST`.
    pub fn function_list(&mut self) -> Result<RespValue, Error> {
        self.run(["FUNCTION", "LIST"])
    }

    /// Sends `FUNCTION DELETE`. False when no function had that name.
    pub fn function_delete(&mut self, name: &str) -> Result<bool, Error> {
        self.flag(["FUNCTION", "DELETE", name])
    }

    /// Sends `ACL WHOAMI`.
    pub fn acl_whoami(&mut self) -> Result<String, Error> {
        self.text(["ACL", "WHOAMI"])
    }

    /// Sends `ACL USERS`.
    pub fn acl_users(&mut self) -> Result<Vec<String>, Error> {
        self.strings(["ACL", "USERS"])
    }

    /// Sends `ACL LIST`.
    pub fn acl_list(&mut self) -> Result<Vec<String>, Error> {
        self.strings(["ACL", "LIST"])
    }

    /// Sends `ACL GETUSER`.
    pub fn acl_getuser(&mut self, user: &str) -> Result<RespValue, Error> {
        self.run(["ACL", "GETUSER", user])
    }

    /// Sends `ACL CAT`.
    pub fn acl_cat(&mut self) -> Result<Vec<String>, Error> {
        self.strings(["ACL", "CAT"])
    }

    /// Sends `ACL SETUSER user rule...`.
    pub fn acl_setuser(&mut self, user: &str, rules: &[&str]) -> Result<(), Error> {
        self.ok(with(args!["ACL", "SETUSER", user], rules))
    }

    /// Sends `ACL LOAD`.
    pub fn acl_load(&mut self) -> Result<(), Error> {
        self.ok(["ACL", "LOAD"])
    }

    /// Sends `ACL SAVE`.
    pub fn acl_save(&mut self) -> Result<(), Error> {
        self.ok(["ACL", "SAVE"])
    }

    /// Sends `CLUSTER SLOTS`.
    pub fn cluster_slots(&mut self) -> Result<RespValue, Error> {
        self.run(["CLUSTER", "SLOTS"])
    }

    /// Sends `CLUSTER NODES`.
    pub fn cluster_nodes(&mut self) -> Result<String, Error> {
        self.text(["CLUSTER", "NODES"])
    }

    /// Sends `CLUSTER SHARDS`.
    pub fn cluster_shards(&mut self) -> Result<RespValue, Error> {
        self.run(["CLUSTER", "SHARDS"])
    }

    /// Sends `CLUSTER INFO`.
    pub fn cluster_info(&mut self) -> Result<String, Error> {
        self.text(["CLUSTER", "INFO"])
    }

    /// Sends `CLUSTER MYID`.
    pub fn cluster_myid(&mut self) -> Result<String, Error> {
        self.text(["CLUSTER", "MYID"])
    }

    /// Sends `CLUSTER KEYSLOT`.
    pub fn cluster_keyslot(&mut self, key: &str) -> Result<i64, Error> {
        self.integer(["CLUSTER", "KEYSLOT", key])
    }

    /// Sends `READONLY`. Ruvio has no replicas; the reply is still `OK` in cluster mode.
    pub fn readonly(&mut self) -> Result<(), Error> {
        self.ok(["READONLY"])
    }

    /// Sends `READWRITE`.
    pub fn readwrite(&mut self) -> Result<(), Error> {
        self.ok(["READWRITE"])
    }

    /// Sends `CLIENT GETNAME`.
    pub fn client_getname(&mut self) -> Result<Option<String>, Error> {
        self.bulk_or_null(["CLIENT", "GETNAME"])
    }

    /// Sends `CLIENT SETNAME`.
    pub fn client_setname(&mut self, name: &str) -> Result<(), Error> {
        self.ok(["CLIENT", "SETNAME", name])
    }

    /// Sends `CLIENT TRACKING ON` or `OFF`.
    pub fn client_tracking(&mut self, enabled: bool) -> Result<(), Error> {
        self.ok(["CLIENT", "TRACKING", if enabled { "ON" } else { "OFF" }])
    }

    /// Sends `HELLO protocol`. Connecting itself does not send this, and this client
    /// only decodes RESP2 replies.
    pub fn hello(&mut self, protocol: u8) -> Result<RespValue, Error> {
        self.run(args!["HELLO", protocol])
    }

    /// Sends `PUBLISH` and returns how many subscribers received the message.
    pub fn publish(&mut self, channel: &str, message: &str) -> Result<i64, Error> {
        self.integer(["PUBLISH", channel, message])
    }

    /// Sends `SPUBLISH` and returns how many subscribers received the message.
    pub fn spublish(&mut self, channel: &str, message: &str) -> Result<i64, Error> {
        self.integer(["SPUBLISH", channel, message])
    }

    /// Sends `SUBSCRIBE` and reads one confirmation per channel.
    /// Opens a dedicated Pub/Sub socket so `PING` and `PUBLISH` stay on the
    /// command connection. Then read deliveries with [`Self::next_message`].
    pub fn subscribe(&mut self, channels: &[&str]) -> Result<Vec<RespValue>, Error> {
        self.subscription("SUBSCRIBE", channels, false)
    }

    /// Sends `UNSUBSCRIBE`. No channels drops every exact subscription on this connection.
    pub fn unsubscribe(&mut self, channels: &[&str]) -> Result<Vec<RespValue>, Error> {
        self.subscription("UNSUBSCRIBE", channels, true)
    }

    /// Sends `PSUBSCRIBE` and reads one confirmation per pattern.
    pub fn psubscribe(&mut self, patterns: &[&str]) -> Result<Vec<RespValue>, Error> {
        self.subscription("PSUBSCRIBE", patterns, false)
    }

    /// Sends `PUNSUBSCRIBE`. No patterns drops every pattern subscription on this connection.
    pub fn punsubscribe(&mut self, patterns: &[&str]) -> Result<Vec<RespValue>, Error> {
        self.subscription("PUNSUBSCRIBE", patterns, true)
    }

    /// Sends `SSUBSCRIBE` and reads one confirmation per channel.
    pub fn ssubscribe(&mut self, channels: &[&str]) -> Result<Vec<RespValue>, Error> {
        self.subscription("SSUBSCRIBE", channels, false)
    }

    /// Sends `SUNSUBSCRIBE`. No channels drops every sharded subscription on this connection.
    pub fn sunsubscribe(&mut self, channels: &[&str]) -> Result<Vec<RespValue>, Error> {
        self.subscription("SUNSUBSCRIBE", channels, true)
    }

    /// Reads replies until a `message`, `pmessage`, or `smessage` arrives, skipping
    /// subscribe confirmations. Blocks until [`Self::set_read_timeout`] passes.
    pub fn next_message(&mut self) -> Result<PubSubMessage, Error> {
        loop {
            let reply = self.read_message()?;

            if let Some(message) = pubsub_message(&reply)? {
                return Ok(message);
            }
        }
    }

    /// Calls `on_message` for each delivery. Safe on the command client after
    /// `subscribe`; deliveries are read from the dedicated Pub/Sub socket.
    pub fn listen<F>(&mut self, mut on_message: F) -> Result<(), Error>
    where
        F: FnMut(PubSubMessage),
    {
        loop {
            on_message(self.next_message()?);
        }
    }

    fn subscription(
        &mut self,
        command: &str,
        names: &[&str],
        allow_empty: bool,
    ) -> Result<Vec<RespValue>, Error> {
        if names.is_empty() && !allow_empty {
            return Err(Error::Protocol(format!(
                "{command} needs at least one channel or pattern"
            )));
        }

        self.run_replies(join(command, names), names.len().max(1))
    }

    pub(crate) fn integers<A: crate::args::Args>(
        &mut self,
        arguments: A,
    ) -> Result<Vec<i64>, Error> {
        self.run_with(arguments, |command, reply| integer_array(command, &reply))
    }

    fn blocking_sorted_pop(
        &mut self,
        command: &str,
        keys: &[&str],
        timeout: Duration,
    ) -> Result<Option<(String, SortedSetEntry)>, Error> {
        let mut arguments = join(command, keys);

        arguments.push(duration_secs(timeout).to_string());

        match self.run(arguments)? {
            RespValue::Null => Ok(None),
            RespValue::Array(items) if items.len() == 3 => Ok(Some((
                items[0].as_string()?.unwrap_or_default(),
                SortedSetEntry::new(
                    items[1].as_string()?.unwrap_or_default(),
                    parse_score(&items[2].as_string()?.unwrap_or_default())?,
                ),
            ))),
            other => Err(unexpected(command, &other)),
        }
    }

    fn blocking_pop(
        &mut self,
        command: &str,
        keys: &[&str],
        timeout: Duration,
    ) -> Result<Option<(String, String)>, Error> {
        let mut arguments = join(command, keys);

        arguments.push(duration_secs(timeout).to_string());

        match self.run(arguments)? {
            RespValue::Null => Ok(None),
            RespValue::Array(items) if items.len() == 2 => Ok(Some((
                items[0].as_string()?.unwrap_or_default(),
                items[1].as_string()?.unwrap_or_default(),
            ))),
            other => Err(unexpected(command, &other)),
        }
    }
}

impl Client {
    /// Sends `KEYS`. The server refuses to return more than 100,000 keys.
    pub fn keys(&mut self, pattern: &str) -> Result<Vec<String>, Error> {
        self.strings(["KEYS", pattern])
    }

    /// Sends `RANDOMKEY`. `None` when the database is empty.
    pub fn random_key(&mut self) -> Result<Option<String>, Error> {
        self.bulk_or_null(["RANDOMKEY"])
    }

    /// Sends `TOUCH` and returns how many of the keys exist.
    pub fn touch(&mut self, keys: &[&str]) -> Result<i64, Error> {
        self.integer(join("TOUCH", keys))
    }

    /// Sends `RENAMENX`. False when `new_key` already exists.
    pub fn rename_nx(&mut self, key: &str, new_key: &str) -> Result<bool, Error> {
        self.flag(["RENAMENX", key, new_key])
    }

    /// Sends `EXPIRETIME`: unix seconds, -1 without a TTL, -2 for a missing key.
    pub fn expire_time(&mut self, key: &str) -> Result<i64, Error> {
        self.integer(["EXPIRETIME", key])
    }

    /// Sends `PEXPIRETIME`: unix milliseconds, -1 without a TTL, -2 for a missing key.
    pub fn pexpire_time(&mut self, key: &str) -> Result<i64, Error> {
        self.integer(["PEXPIRETIME", key])
    }

    /// Sends `COPY`. False when the source is missing or the target exists and
    /// `replace` is false.
    pub fn copy(&mut self, source: &str, destination: &str, replace: bool) -> Result<bool, Error> {
        let mut arguments = args!["COPY", source, destination];

        if replace {
            arguments.push("REPLACE".to_owned());
        }

        self.flag(arguments)
    }

    /// Sends `ECHO`.
    pub fn echo(&mut self, message: &str) -> Result<String, Error> {
        self.text(["ECHO", message])
    }

    /// Sends `OBJECT ENCODING`. `None` for a missing key.
    pub fn object_encoding(&mut self, key: &str) -> Result<Option<String>, Error> {
        self.bulk_or_null(["OBJECT", "ENCODING", key])
    }

    /// Sends `OBJECT IDLETIME` in seconds. `None` for a missing key.
    pub fn object_idletime(&mut self, key: &str) -> Result<Option<i64>, Error> {
        self.optional_integer(["OBJECT", "IDLETIME", key])
    }

    /// Sends `OBJECT FREQ`. `None` for a missing key.
    pub fn object_freq(&mut self, key: &str) -> Result<Option<i64>, Error> {
        self.optional_integer(["OBJECT", "FREQ", key])
    }

    /// Sends `MEMORY USAGE`. `None` for a missing key.
    pub fn memory_usage(&mut self, key: &str) -> Result<Option<i64>, Error> {
        self.optional_integer(["MEMORY", "USAGE", key])
    }

    /// Sends `SLOWLOG LEN`.
    pub fn slowlog_len(&mut self) -> Result<i64, Error> {
        self.integer(["SLOWLOG", "LEN"])
    }

    /// Sends `SLOWLOG RESET`.
    pub fn slowlog_reset(&mut self) -> Result<(), Error> {
        self.ok(["SLOWLOG", "RESET"])
    }

    /// Sends `SLOWLOG GET` for up to `count` entries. Each entry is returned as the raw reply.
    pub fn slowlog_get(&mut self, count: u64) -> Result<Vec<RespValue>, Error> {
        match self.run(args!["SLOWLOG", "GET", count])? {
            RespValue::Array(items) => Ok(items),
            other => Err(unexpected("SLOWLOG", &other)),
        }
    }

    /// Sends `CLIENT ID`.
    pub fn client_id(&mut self) -> Result<i64, Error> {
        self.integer(["CLIENT", "ID"])
    }

    /// Sends `CLIENT INFO`: one line for this connection.
    pub fn client_info(&mut self) -> Result<String, Error> {
        self.text(["CLIENT", "INFO"])
    }

    /// Sends `CLIENT LIST`: one line per connection on the answering shard.
    pub fn client_list(&mut self) -> Result<String, Error> {
        self.text(["CLIENT", "LIST"])
    }

    /// Sends `CLIENT KILL ID` and returns how many connections were closed.
    pub fn client_kill_id(&mut self, id: i64) -> Result<i64, Error> {
        self.integer(args!["CLIENT", "KILL", "ID", id])
    }

    /// Sends `LINSERT`. Returns the new length, -1 when the pivot is missing, 0 for a missing list.
    pub fn linsert(
        &mut self,
        key: &str,
        before: bool,
        pivot: &str,
        value: &str,
    ) -> Result<i64, Error> {
        let side = if before { "BEFORE" } else { "AFTER" };

        self.integer(["LINSERT", key, side, pivot, value])
    }

    /// Sends `LSET`.
    pub fn lset(&mut self, key: &str, index: i64, value: &str) -> Result<(), Error> {
        self.ok(args!["LSET", key, index, value])
    }

    /// Sends `LREM` and returns how many elements were removed.
    pub fn lrem(&mut self, key: &str, count: i64, value: &str) -> Result<i64, Error> {
        self.integer(args!["LREM", key, count, value])
    }

    /// Sends `LPOS` for the first match. `None` when the value is not in the list.
    pub fn lpos(&mut self, key: &str, value: &str) -> Result<Option<i64>, Error> {
        self.optional_integer(["LPOS", key, value])
    }

    /// Sends `LPUSHX`. Returns 0 and writes nothing when the list does not exist.
    pub fn lpushx(&mut self, key: &str, values: &[&str]) -> Result<i64, Error> {
        self.integer(with(args!["LPUSHX", key], values))
    }

    /// Sends `RPUSHX`. Returns 0 and writes nothing when the list does not exist.
    pub fn rpushx(&mut self, key: &str, values: &[&str]) -> Result<i64, Error> {
        self.integer(with(args!["RPUSHX", key], values))
    }

    /// Sends `LMOVE`. `None` when the source is empty.
    pub fn lmove(
        &mut self,
        source: &str,
        destination: &str,
        from_left: bool,
        to_left: bool,
    ) -> Result<Option<String>, Error> {
        self.bulk_or_null(["LMOVE", source, destination, side(from_left), side(to_left)])
    }

    /// Sends `RPOPLPUSH`. `None` when the source is empty.
    pub fn rpoplpush(&mut self, source: &str, destination: &str) -> Result<Option<String>, Error> {
        self.bulk_or_null(["RPOPLPUSH", source, destination])
    }

    /// Sends `BLMOVE`. `None` when `timeout` passes. `Duration::ZERO` waits forever.
    pub fn blmove(
        &mut self,
        source: &str,
        destination: &str,
        from_left: bool,
        to_left: bool,
        timeout: Duration,
    ) -> Result<Option<String>, Error> {
        self.bulk_or_null(args![
            "BLMOVE",
            source,
            destination,
            side(from_left),
            side(to_left),
            duration_secs(timeout)
        ])
    }

    /// Sends `LMPOP` over `keys`. Returns the key that was popped and its elements,
    /// or `None` when every list is empty.
    pub fn lmpop(
        &mut self,
        keys: &[&str],
        from_left: bool,
        count: u64,
    ) -> Result<Option<(String, Vec<String>)>, Error> {
        let mut arguments = args!["LMPOP", keys.len()];

        arguments.extend(keys.iter().map(|key| (*key).to_owned()));
        arguments.extend(args![side(from_left), "COUNT", count]);

        pop_many("LMPOP", self.run(arguments)?)
    }

    /// Sends `BITFIELD key operation...`. `operations` are the subcommand words, for example
    /// `["OVERFLOW", "SAT", "INCRBY", "u8", "0", "10", "GET", "u8", "0"]`. Each reply is a number,
    /// or `None` when `OVERFLOW FAIL` refused the write.
    pub fn bitfield(&mut self, key: &str, operations: &[&str]) -> Result<Vec<Option<i64>>, Error> {
        self.bitfield_command("BITFIELD", key, operations)
    }

    /// Sends `BITFIELD_RO key GET type offset ...`.
    pub fn bitfield_ro(
        &mut self,
        key: &str,
        operations: &[&str],
    ) -> Result<Vec<Option<i64>>, Error> {
        self.bitfield_command("BITFIELD_RO", key, operations)
    }

    fn bitfield_command(
        &mut self,
        name: &str,
        key: &str,
        operations: &[&str],
    ) -> Result<Vec<Option<i64>>, Error> {
        let mut arguments = args![name, key];

        arguments.extend(operations.iter().map(|word| (*word).to_owned()));

        match self.run(arguments)? {
            RespValue::Array(items) => items
                .iter()
                .map(|item| match item {
                    RespValue::Null => Ok(None),
                    RespValue::Integer(value) => Ok(Some(*value)),
                    other => Err(unexpected(name, other)),
                })
                .collect(),
            other => Err(unexpected(name, &other)),
        }
    }

    /// Sends `FILLGET`: one caller gets `Fill` and builds the value, the rest get
    /// `Hit`, `Busy`, or `Stale`. `fill` is how long the fill marker lives.
    pub fn fill_get(&mut self, key: &str, fill: Duration) -> Result<FillOutcome, Error> {
        parse_fill(self.run(args!["FILLGET", key, duration_millis(fill)])?)
    }

    /// Sends `FILLSET`. True when `token` was valid and the value was written.
    /// `stale` keeps the old value readable for that long after `ttl` (stale-while-revalidate).
    pub fn fill_set(
        &mut self,
        key: &str,
        value: &str,
        ttl: Duration,
        token: &str,
        stale: Option<Duration>,
    ) -> Result<bool, Error> {
        let mut arguments = args!["FILLSET", key, value, duration_millis(ttl), token];

        if let Some(stale) = stale {
            arguments.extend(args!["SWR", duration_millis(stale)]);
        }

        self.flag(arguments)
    }
}

pub(crate) fn side(left: bool) -> &'static str {
    if left { "LEFT" } else { "RIGHT" }
}

pub(crate) fn pop_many(
    command: &str,
    reply: RespValue,
) -> Result<Option<(String, Vec<String>)>, Error> {
    match reply {
        RespValue::Null => Ok(None),
        RespValue::Array(items) if items.len() == 2 => {
            let key = items[0].as_string()?.unwrap_or_default();
            let elements = match &items[1] {
                RespValue::Array(values) => values
                    .iter()
                    .map(|value| Ok(value.as_string()?.unwrap_or_default()))
                    .collect::<Result<Vec<_>, Error>>()?,
                other => return Err(unexpected(command, other)),
            };

            Ok(Some((key, elements)))
        }
        other => Err(unexpected(command, &other)),
    }
}

pub(crate) fn parse_fill(reply: RespValue) -> Result<FillOutcome, Error> {
    let items = match &reply {
        RespValue::Array(items) if items.len() >= 2 => items,
        other => return Err(unexpected("FILLGET", other)),
    };
    let state = items[0].as_string()?.unwrap_or_default();
    let text = |value: &RespValue| value.as_string();

    match (state.as_str(), items.len()) {
        ("HIT", 2) => Ok(FillOutcome::Hit(text(&items[1])?.unwrap_or_default())),
        ("FILL", 2) => Ok(FillOutcome::Fill(text(&items[1])?.unwrap_or_default())),
        ("BUSY", 2) => Ok(FillOutcome::Busy),
        ("STALE", 3) => Ok(FillOutcome::Stale {
            value: text(&items[1])?.unwrap_or_default(),
            token: text(&items[2])?,
        }),
        _ => Err(unexpected("FILLGET", &reply)),
    }
}

/// Appends an optional `NX`/`XX`/`GT`/`LT`, then `FIELDS numfields field ...`.
fn field_block(
    mut arguments: Vec<String>,
    condition: FieldExpireCondition,
    fields: &[&str],
) -> Vec<String> {
    let flag = match condition {
        FieldExpireCondition::Always => None,
        FieldExpireCondition::IfNoTtl => Some("NX"),
        FieldExpireCondition::IfTtl => Some("XX"),
        FieldExpireCondition::IfGreater => Some("GT"),
        FieldExpireCondition::IfLess => Some("LT"),
    };

    arguments.extend(flag.map(str::to_owned));
    arguments.push("FIELDS".to_owned());
    arguments.push(fields.len().to_string());

    with(arguments, fields)
}

fn integer_array(command: &str, reply: &RespValue) -> Result<Vec<i64>, Error> {
    let Some(items) = reply.as_array() else {
        return Err(unexpected(command, reply));
    };

    items
        .iter()
        .map(|item| item.as_integer().ok_or_else(|| unexpected(command, item)))
        .collect()
}

fn with(mut arguments: Vec<String>, rest: &[&str]) -> Vec<String> {
    arguments.extend(rest.iter().map(|argument| (*argument).to_owned()));

    arguments
}

fn with_pairs(mut arguments: Vec<String>, pairs: &[(&str, &str)]) -> Vec<String> {
    for (left, right) in pairs {
        arguments.push((*left).to_owned());
        arguments.push((*right).to_owned());
    }

    arguments
}

fn push_scan_options(arguments: &mut Vec<String>, pattern: Option<&str>, count: Option<u64>) {
    if let Some(pattern) = pattern {
        arguments.extend(args!["MATCH", pattern]);
    }

    if let Some(count) = count {
        arguments.extend(args!["COUNT", count]);
    }
}

fn push_stream_read_options(arguments: &mut Vec<String>, options: &StreamReadOptions) {
    if let Some(count) = options.count {
        arguments.extend(args!["COUNT", count]);
    }

    if let Some(block) = options.block {
        arguments.extend(args!["BLOCK", duration_millis(block)]);
    }
}

fn push_streams(arguments: &mut Vec<String>, streams: &[(&str, &str)]) {
    arguments.push("STREAMS".to_owned());
    arguments.extend(streams.iter().map(|(key, _)| (*key).to_owned()));
    arguments.extend(streams.iter().map(|(_, id)| (*id).to_owned()));
}

fn zadd_arguments(key: &str, options: SortedSetAddOptions) -> Vec<String> {
    let mut arguments = args!["ZADD", key];
    let flags = [
        (options.if_not_exists, "NX"),
        (options.if_exists, "XX"),
        (options.greater_than, "GT"),
        (options.less_than, "LT"),
        (options.changed, "CH"),
    ];

    for (enabled, flag) in flags {
        if enabled {
            arguments.push(flag.to_owned());
        }
    }

    arguments
}

fn claim_arguments(
    key: &str,
    group: &str,
    consumer: &str,
    min_idle: Duration,
    ids: &[&str],
    options: &StreamClaimOptions,
) -> Result<Vec<String>, Error> {
    if options.idle.is_some() && options.time.is_some() {
        return Err(Error::Protocol(
            "IDLE and TIME cannot be combined".to_owned(),
        ));
    }

    let mut arguments = with(
        args!["XCLAIM", key, group, consumer, duration_millis(min_idle)],
        ids,
    );

    if let Some(idle) = options.idle {
        arguments.extend(args!["IDLE", duration_millis(idle)]);
    }

    if let Some(time) = options.time {
        arguments.extend(args!["TIME", unix_millis(time)?]);
    }

    if let Some(retry_count) = options.retry_count {
        arguments.extend(args!["RETRYCOUNT", retry_count]);
    }

    if options.force {
        arguments.push("FORCE".to_owned());
    }

    if let Some(last_id) = &options.last_id {
        arguments.extend(args!["LASTID", last_id]);
    }

    Ok(arguments)
}

fn script_arguments(command: &str, script: &str, keys: &[&str], arguments: &[&str]) -> Vec<String> {
    let values = with(args![command, script, keys.len()], keys);

    with(values, arguments)
}

fn parse_scan(command: &str, reply: RespValue) -> Result<(u64, Vec<String>), Error> {
    let Some([cursor, items]) = reply.as_array() else {
        return Err(unexpected(command, &reply));
    };

    let cursor = cursor
        .as_string()?
        .unwrap_or_default()
        .parse()
        .map_err(|_| Error::Protocol(format!("{command} returned a bad cursor")))?;
    let mut keys = Vec::new();

    for item in items.as_array().unwrap_or_default() {
        keys.push(item.as_string()?.unwrap_or_default());
    }

    Ok((cursor, keys))
}

pub(crate) fn parse_score(text: &str) -> Result<f64, Error> {
    text.parse()
        .map_err(|_| Error::Protocol(format!("{text:?} is not a score")))
}

fn pairs(reply: &RespValue) -> Result<Vec<(String, String)>, Error> {
    let items = reply.as_array().unwrap_or_default();
    let mut values = Vec::with_capacity(items.len() / 2);

    for pair in items.chunks_exact(2) {
        values.push((
            pair[0].as_string()?.unwrap_or_default(),
            pair[1].as_string()?.unwrap_or_default(),
        ));
    }

    Ok(values)
}

fn sorted_set(reply: &RespValue) -> Result<Vec<SortedSetEntry>, Error> {
    pairs(reply)?
        .into_iter()
        .map(|(member, score)| Ok(SortedSetEntry::new(member, parse_score(&score)?)))
        .collect()
}

fn stream_entries(reply: &RespValue) -> Result<Vec<StreamEntry>, Error> {
    let mut entries = Vec::new();

    for item in reply.as_array().unwrap_or_default() {
        let Some([id, fields]) = item.as_array() else {
            continue;
        };

        entries.push(StreamEntry {
            id: id.as_string()?.unwrap_or_default(),
            fields: pairs(fields)?,
        });
    }

    Ok(entries)
}

fn stream_read(reply: &RespValue) -> Result<Vec<StreamReadResult>, Error> {
    let mut results = Vec::new();

    for item in reply.as_array().unwrap_or_default() {
        let Some([key, entries]) = item.as_array() else {
            continue;
        };

        results.push(StreamReadResult {
            key: key.as_string()?.unwrap_or_default(),
            entries: stream_entries(entries)?,
        });
    }

    Ok(results)
}

fn pubsub_message(reply: &RespValue) -> Result<Option<PubSubMessage>, Error> {
    let items = reply.as_array().unwrap_or_default();
    let kind = match items.first() {
        Some(first) => first.as_string()?.unwrap_or_default(),
        None => return Ok(None),
    };

    let (pattern, channel, payload) = match (kind.as_str(), items) {
        ("message" | "smessage", [_, channel, payload]) => (None, channel, payload),
        ("pmessage", [_, pattern, channel, payload]) => (pattern.as_string()?, channel, payload),

        _ => return Ok(None),
    };

    let payload = match payload {
        RespValue::Bulk(bytes) => bytes.clone(),
        other => other.as_string()?.unwrap_or_default().into_bytes(),
    };

    Ok(Some(PubSubMessage {
        kind,
        pattern,
        channel: channel.as_string()?.unwrap_or_default(),
        payload,
    }))
}

#[cfg(test)]
mod tests {
    use super::{field_block, integer_array, parse_fill, pop_many, pubsub_message};
    use crate::models::{FieldExpireCondition, FillOutcome};
    use crate::value::RespValue;

    #[test]
    fn field_block_puts_the_condition_before_fields() {
        assert_eq!(
            field_block(
                vec!["HEXPIRE".into(), "h".into(), "10".into()],
                FieldExpireCondition::IfGreater,
                &["a", "b"],
            ),
            ["HEXPIRE", "h", "10", "GT", "FIELDS", "2", "a", "b"]
        );
        assert_eq!(
            field_block(
                vec!["HTTL".into(), "h".into()],
                FieldExpireCondition::Always,
                &["a"]
            ),
            ["HTTL", "h", "FIELDS", "1", "a"]
        );
        assert_eq!(
            integer_array(
                "HTTL",
                &RespValue::Array(vec![RespValue::Integer(-2), RespValue::Integer(5)])
            )
            .unwrap(),
            vec![-2, 5]
        );
    }

    #[test]
    fn fill_replies_parse_into_outcomes() {
        let bulk = |text: &str| RespValue::Bulk(text.as_bytes().to_vec());
        let reply = |items: Vec<RespValue>| RespValue::Array(items);

        assert_eq!(
            parse_fill(reply(vec![bulk("HIT"), bulk("v")])).unwrap(),
            FillOutcome::Hit("v".to_owned())
        );
        assert_eq!(
            parse_fill(reply(vec![bulk("FILL"), bulk("t1")])).unwrap(),
            FillOutcome::Fill("t1".to_owned())
        );
        assert_eq!(
            parse_fill(reply(vec![bulk("BUSY"), RespValue::Null])).unwrap(),
            FillOutcome::Busy
        );
        assert_eq!(
            parse_fill(reply(vec![bulk("STALE"), bulk("old"), RespValue::Null])).unwrap(),
            FillOutcome::Stale {
                value: "old".to_owned(),
                token: None
            }
        );
        assert!(parse_fill(RespValue::Integer(1)).is_err());
    }

    #[test]
    fn lmpop_reply_parses_key_and_elements() {
        let bulk = |text: &str| RespValue::Bulk(text.as_bytes().to_vec());
        let reply = RespValue::Array(vec![
            bulk("l"),
            RespValue::Array(vec![bulk("a"), bulk("b")]),
        ]);

        assert_eq!(
            pop_many("LMPOP", reply).unwrap(),
            Some(("l".to_owned(), vec!["a".to_owned(), "b".to_owned()]))
        );
        assert_eq!(pop_many("LMPOP", RespValue::Null).unwrap(), None);
    }

    #[test]
    fn pubsub_message_reads_payload_from_an_array() {
        let reply = RespValue::Array(vec![
            RespValue::Bulk(b"message".to_vec()),
            RespValue::Bulk(b"news".to_vec()),
            RespValue::Bulk(b"hello-ruvio".to_vec()),
        ]);

        let parsed = pubsub_message(&reply).unwrap().unwrap();

        assert_eq!(parsed.kind, "message");
        assert_eq!(parsed.channel, "news");
        assert_eq!(parsed.payload, b"hello-ruvio");
        assert!(
            pubsub_message(&RespValue::Array(vec![
                RespValue::Bulk(b"subscribe".to_vec()),
                RespValue::Bulk(b"news".to_vec()),
                RespValue::Integer(1),
            ]))
            .unwrap()
            .is_none()
        );
    }
}
