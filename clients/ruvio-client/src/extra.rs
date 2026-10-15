//! Typed methods for the rest of the compatibility matrix: Ruvio extensions (`DELEX`,
//! `LPUSHCAP`, `JSON.INDEX`, ...), bitmaps, HyperLogLog, the remaining string, set, sorted-set
//! and Bloom commands, and admin commands.
//!
//! Fixed-arity commands build their argument list as a stack array and format numbers with
//! [`Num`], so they allocate nothing but the reply. Variadic commands allocate one
//! `Vec<&str>` of borrowed pointers, never owned copies of the arguments.

use std::time::Duration;

use crate::args::Num;
use crate::client::{Client, duration_millis, duration_secs, unexpected};
use crate::commands::{parse_fill, parse_score, pop_many, side};
use crate::error::Error;
use crate::models::{
    Aggregate, BitOperation, BitUnit, BloomInfo, BloomInsertOptions, BloomReserveOptions,
    FillOutcome, GeoFrom, GeoMatch, GeoSearch, GeoShape, IdemState, JsonIndexType,
    JsonSetCondition, ListPositionOptions, RangeBy, RangeOptions, RestoreOptions, SetScanPage,
    SortedSetAddOptions, SortedSetCombine, SortedSetEntry, SortedSetScanPage,
};
use crate::value::RespValue;

impl Client {
    // ---- connection and server ------------------------------------------------------------

    /// Sends `RUVIO` or `RUVIO word` (`OWNER`, `HOTKEYS`, `BIGKEYS`) and returns the raw reply.
    pub fn ruvio(&mut self, word: Option<&str>) -> Result<RespValue, Error> {
        match word {
            Some(word) => self.execute(&["RUVIO", word]),
            None => self.execute(&["RUVIO"]),
        }
    }

    /// Sends `AUTH password` on the current connection. Prefer [`crate::ClientOptions::password`],
    /// which is also replayed after a reconnect.
    pub fn auth(&mut self, password: &str) -> Result<(), Error> {
        self.ok(["AUTH", password])
    }

    /// Sends `AUTH username password` on the current connection.
    pub fn auth_user(&mut self, username: &str, password: &str) -> Result<(), Error> {
        self.ok(["AUTH", username, password])
    }

    /// Sends `WAIT replicas timeout_ms`: how many replicas synced this shard's WAL position.
    /// `Duration::ZERO` waits until shutdown. Blocks this connection.
    pub fn wait(&mut self, replicas: u64, timeout: Duration) -> Result<i64, Error> {
        let replicas = Num::uint(replicas);
        let timeout = Num::wide(duration_millis(timeout));

        self.integer(["WAIT", replicas.as_str(), timeout.as_str()])
    }

    /// Sends `COMMAND`: the whole command table as the raw reply.
    pub fn command_list(&mut self) -> Result<RespValue, Error> {
        self.execute(&["COMMAND"])
    }

    /// Sends `COMMAND COUNT`.
    pub fn command_count(&mut self) -> Result<i64, Error> {
        self.integer(["COMMAND", "COUNT"])
    }

    /// Sends `COMMAND INFO name...` (Redis 7 shape, one entry or nil per name).
    pub fn command_info(&mut self, names: &[&str]) -> Result<RespValue, Error> {
        self.execute(&prefixed(&["COMMAND", "INFO"], names))
    }

    /// Sends `COMMAND DOCS name...`. Ruvio answers empty maps.
    pub fn command_docs(&mut self, names: &[&str]) -> Result<RespValue, Error> {
        self.execute(&prefixed(&["COMMAND", "DOCS"], names))
    }

    /// Sends `LATENCY LATEST`. Ruvio records no spike events, so this is always empty.
    pub fn latency_latest(&mut self) -> Result<RespValue, Error> {
        self.execute(&["LATENCY", "LATEST"])
    }

    /// Sends `CLIENT KILL ADDR ip:port [SKIPME yes|no]` and returns how many connections closed.
    pub fn client_kill_addr(&mut self, address: &str, skip_me: Option<bool>) -> Result<i64, Error> {
        match skip_me {
            Some(skip) => self.integer([
                "CLIENT",
                "KILL",
                "ADDR",
                address,
                "SKIPME",
                if skip { "yes" } else { "no" },
            ]),
            None => self.integer(["CLIENT", "KILL", "ADDR", address]),
        }
    }

    /// Sends `MEMORY USAGE key SAMPLES n`. `None` for a missing key.
    pub fn memory_usage_samples(&mut self, key: &str, samples: u64) -> Result<Option<i64>, Error> {
        let samples = Num::uint(samples);

        self.optional_integer(["MEMORY", "USAGE", key, "SAMPLES", samples.as_str()])
    }

    /// Sends `DUMP` and returns Ruvio's binary payload (`RVD1...`), or `None` for a missing key.
    /// Only Ruvio's [`Self::restore`] reads it; the TTL is not part of it.
    pub fn dump(&mut self, key: &str) -> Result<Option<Vec<u8>>, Error> {
        self.bytes_or_null(["DUMP", key])
    }

    /// Sends `RESTORE key ttl payload [REPLACE] [ABSTTL]` with a binary payload from [`Self::dump`].
    /// `ttl_ms` 0 means no TTL; with `absolute_ttl` it is a Unix-millisecond deadline.
    pub fn restore(
        &mut self,
        key: &str,
        ttl_ms: u64,
        payload: &[u8],
        options: RestoreOptions,
    ) -> Result<(), Error> {
        let ttl = Num::uint(ttl_ms);
        let mut arguments: [&[u8]; 6] = [
            b"RESTORE",
            key.as_bytes(),
            ttl.as_str().as_bytes(),
            payload,
            b"",
            b"",
        ];
        let mut count = 4;

        if options.replace {
            arguments[count] = b"REPLACE";
            count += 1;
        }

        if options.absolute_ttl {
            arguments[count] = b"ABSTTL";
            count += 1;
        }

        self.execute_binary(&arguments[..count])?;

        Ok(())
    }

    // ---- strings ---------------------------------------------------------------------------

    /// Sends `DELEX key IFEQ expected` (Ruvio extension): deletes the string only when it equals
    /// `expected`. True when it was deleted. This is the safe lock release.
    pub fn delex(&mut self, key: &str, expected: &str) -> Result<bool, Error> {
        self.flag(["DELEX", key, "IFEQ", expected])
    }

    /// Sends `SETEX key seconds value`.
    pub fn setex(&mut self, key: &str, seconds: u64, value: &str) -> Result<(), Error> {
        let seconds = Num::uint(seconds);

        self.ok(["SETEX", key, seconds.as_str(), value])
    }

    /// Sends `PSETEX key milliseconds value`.
    pub fn psetex(&mut self, key: &str, ttl: Duration, value: &str) -> Result<(), Error> {
        let millis = Num::wide(duration_millis(ttl));

        self.ok(["PSETEX", key, millis.as_str(), value])
    }

    /// Sends `SETNX`. True when the key was written.
    pub fn setnx(&mut self, key: &str, value: &str) -> Result<bool, Error> {
        self.flag(["SETNX", key, value])
    }

    /// Sends `GETDEL`: the string, removed. `None` for a missing key.
    pub fn getdel(&mut self, key: &str) -> Result<Option<String>, Error> {
        self.bulk_or_null(["GETDEL", key])
    }

    /// Sends `MSETNX`: writes every pair only when none of the keys exist. Keys share a slot.
    pub fn msetnx(&mut self, pairs: &[(&str, &str)]) -> Result<bool, Error> {
        let mut arguments = Vec::with_capacity(1 + pairs.len() * 2);

        arguments.push("MSETNX");

        for (key, value) in pairs {
            arguments.push(*key);
            arguments.push(*value);
        }

        self.flag(arguments)
    }

    /// Sends `INCRBYFLOAT` and returns the new value (printed with 15 significant digits).
    pub fn incr_by_float(&mut self, key: &str, delta: f64) -> Result<f64, Error> {
        let delta = Num::float(delta);

        parse_score(&self.text(["INCRBYFLOAT", key, delta.as_str()])?)
    }

    /// Sends `GETRANGE key start end` (inclusive, negative from the end).
    pub fn getrange(&mut self, key: &str, start: i64, end: i64) -> Result<String, Error> {
        let (start, end) = (Num::int(start), Num::int(end));

        self.text(["GETRANGE", key, start.as_str(), end.as_str()])
    }

    /// Sends `SUBSTR`, the old name of `GETRANGE`.
    pub fn substr(&mut self, key: &str, start: i64, end: i64) -> Result<String, Error> {
        let (start, end) = (Num::int(start), Num::int(end));

        self.text(["SUBSTR", key, start.as_str(), end.as_str()])
    }

    /// Sends `SETRANGE` and returns the new length.
    pub fn setrange(&mut self, key: &str, offset: u64, value: &str) -> Result<i64, Error> {
        let offset = Num::uint(offset);

        self.integer(["SETRANGE", key, offset.as_str(), value])
    }

    /// Sends `FILLGET key fill_ms WAIT wait_ms`: a caller that would get `Busy` waits up to
    /// `wait` for the filler. The wait parks this connection, so prefer
    /// [`Self::get_or_create`] (short client-side polling) on a shared connection.
    pub fn fill_get_wait(
        &mut self,
        key: &str,
        fill: Duration,
        wait: Duration,
    ) -> Result<FillOutcome, Error> {
        let fill = Num::wide(duration_millis(fill));
        let wait = Num::wide(duration_millis(wait));

        parse_fill(self.execute(&["FILLGET", key, fill.as_str(), "WAIT", wait.as_str()])?)
    }

    // ---- lists -----------------------------------------------------------------------------

    /// Sends `LPUSHCAP key cap value...` (Ruvio extension): `LPUSH`, then trims the tail so at
    /// most `cap` items remain. Returns the new length.
    pub fn lpushcap(&mut self, key: &str, cap: u64, values: &[&str]) -> Result<i64, Error> {
        let cap = Num::uint(cap);

        self.integer(prefixed(&["LPUSHCAP", key, cap.as_str()], values))
    }

    /// Sends `RPUSHCAP key cap value...`: `RPUSH`, then trims the head to `cap` items.
    pub fn rpushcap(&mut self, key: &str, cap: u64, values: &[&str]) -> Result<i64, Error> {
        let cap = Num::uint(cap);

        self.integer(prefixed(&["RPUSHCAP", key, cap.as_str()], values))
    }

    /// Sends `BLMPOP timeout numkeys key... LEFT|RIGHT COUNT n`. `None` when `timeout` passes.
    pub fn blmpop(
        &mut self,
        timeout: Duration,
        keys: &[&str],
        from_left: bool,
        count: u64,
    ) -> Result<Option<(String, Vec<String>)>, Error> {
        let timeout = Num::uint(duration_secs(timeout));
        let numkeys = Num::uint(keys.len() as u64);
        let count = Num::uint(count);
        let mut arguments = prefixed(&["BLMPOP", timeout.as_str(), numkeys.as_str()], keys);

        arguments.extend([side(from_left), "COUNT", count.as_str()]);
        pop_many("BLMPOP", self.run(arguments)?)
    }

    /// Sends `LPOS key value [RANK r] [MAXLEN n]`. `None` when there is no match.
    pub fn lpos_with(
        &mut self,
        key: &str,
        value: &str,
        options: ListPositionOptions,
    ) -> Result<Option<i64>, Error> {
        let rank = options.rank.map(Num::int);
        let max_len = options.max_len.map(Num::uint);
        let mut arguments = ["LPOS", key, value, "", "", "", ""];
        let count = push_lpos_options(&mut arguments, 3, rank.as_ref(), max_len.as_ref());

        self.optional_integer(&arguments[..count])
    }

    /// Sends `LPOS key value COUNT n [RANK r] [MAXLEN m]`. `count` 0 returns every match.
    pub fn lpos_count(
        &mut self,
        key: &str,
        value: &str,
        count: u64,
        options: ListPositionOptions,
    ) -> Result<Vec<i64>, Error> {
        let rank = options.rank.map(Num::int);
        let max_len = options.max_len.map(Num::uint);
        let wanted = Num::uint(count);
        let mut arguments = ["LPOS", key, value, "COUNT", wanted.as_str(), "", "", "", ""];
        let used = push_lpos_options(&mut arguments, 5, rank.as_ref(), max_len.as_ref());

        self.integers(&arguments[..used])
    }

    // ---- sets ------------------------------------------------------------------------------

    /// Sends `SSCAN key cursor [MATCH pattern] [COUNT n]`. See [`Self::sscan_iter`].
    pub fn sscan(
        &mut self,
        key: &str,
        cursor: u64,
        pattern: Option<&str>,
        count: Option<u64>,
    ) -> Result<SetScanPage, Error> {
        let (cursor, members) = self.scan_page("SSCAN", Some(key), cursor, pattern, count)?;

        Ok(SetScanPage { cursor, members })
    }

    /// Sends `SMISMEMBER`: one flag per member.
    pub fn smismember(&mut self, key: &str, members: &[&str]) -> Result<Vec<bool>, Error> {
        Ok(self
            .integers(prefixed(&["SMISMEMBER", key], members))?
            .into_iter()
            .map(|flag| flag != 0)
            .collect())
    }

    /// Sends `SINTERCARD numkeys key... [LIMIT n]`: the size of the intersection.
    pub fn sintercard(&mut self, keys: &[&str], limit: Option<u64>) -> Result<i64, Error> {
        let numkeys = Num::uint(keys.len() as u64);
        let limit = limit.map(Num::uint);
        let mut arguments = prefixed(&["SINTERCARD", numkeys.as_str()], keys);

        if let Some(limit) = &limit {
            arguments.extend(["LIMIT", limit.as_str()]);
        }

        self.integer(arguments)
    }

    // ---- sorted sets -----------------------------------------------------------------------

    /// Sends `ZRANGEBYLEX key min max [LIMIT offset count]`. Bounds: `-`, `+`, `[m`, `(m`.
    pub fn zrangebylex(
        &mut self,
        key: &str,
        min: &str,
        max: &str,
        limit: Option<(i64, i64)>,
    ) -> Result<Vec<String>, Error> {
        self.ranged("ZRANGEBYLEX", key, min, max, limit)
    }

    /// Sends `ZREVRANGEBYLEX key max min [LIMIT offset count]`: the high bound comes first.
    pub fn zrevrangebylex(
        &mut self,
        key: &str,
        max: &str,
        min: &str,
        limit: Option<(i64, i64)>,
    ) -> Result<Vec<String>, Error> {
        self.ranged("ZREVRANGEBYLEX", key, max, min, limit)
    }

    /// Sends `ZRANGEBYSCORE key min max [LIMIT offset count]` without scores.
    pub fn zrange_by_score(
        &mut self,
        key: &str,
        min: &str,
        max: &str,
        limit: Option<(i64, i64)>,
    ) -> Result<Vec<String>, Error> {
        self.ranged("ZRANGEBYSCORE", key, min, max, limit)
    }

    /// Sends `ZRANGE key start stop [BYSCORE|BYLEX] [REV] [LIMIT offset count]`.
    /// `start` and `stop` are sent as given.
    pub fn zrange_query(
        &mut self,
        key: &str,
        start: &str,
        stop: &str,
        options: RangeOptions,
    ) -> Result<Vec<String>, Error> {
        let offsets = options
            .limit
            .map(|(offset, count)| (Num::int(offset), Num::int(count)));
        let mut arguments = ["ZRANGE", key, start, stop, "", "", "", "", "", ""];
        let count = push_range_options(&mut arguments, options, offsets.as_ref(), false);

        self.strings(&arguments[..count])
    }

    /// [`Self::zrange_query`] with `WITHSCORES`.
    pub fn zrange_query_with_scores(
        &mut self,
        key: &str,
        start: &str,
        stop: &str,
        options: RangeOptions,
    ) -> Result<Vec<SortedSetEntry>, Error> {
        let offsets = options
            .limit
            .map(|(offset, count)| (Num::int(offset), Num::int(count)));
        let mut arguments = ["ZRANGE", key, start, stop, "", "", "", "", "", ""];
        let count = push_range_options(&mut arguments, options, offsets.as_ref(), true);

        scored(self.run(&arguments[..count])?)
    }

    /// Sends `ZSCAN key cursor [MATCH pattern] [COUNT n]`. See [`Self::zscan_iter`].
    pub fn zscan(
        &mut self,
        key: &str,
        cursor: u64,
        pattern: Option<&str>,
        count: Option<u64>,
    ) -> Result<SortedSetScanPage, Error> {
        let (cursor, flat) = self.scan_page("ZSCAN", Some(key), cursor, pattern, count)?;

        Ok(SortedSetScanPage {
            cursor,
            entries: entries_from_flat(flat)?,
        })
    }

    /// Sends `ZCOUNT key min max`. Bounds accept `-inf`, `+inf` and `(` for exclusive.
    pub fn zcount(&mut self, key: &str, min: &str, max: &str) -> Result<i64, Error> {
        self.integer(["ZCOUNT", key, min, max])
    }

    /// Sends `ZLEXCOUNT key min max`.
    pub fn zlexcount(&mut self, key: &str, min: &str, max: &str) -> Result<i64, Error> {
        self.integer(["ZLEXCOUNT", key, min, max])
    }

    /// Sends `ZMSCORE`: one score per member, `None` when missing.
    pub fn zmscore(&mut self, key: &str, members: &[&str]) -> Result<Vec<Option<f64>>, Error> {
        self.optional_strings(prefixed(&["ZMSCORE", key], members))?
            .into_iter()
            .map(|score| score.map(|text| parse_score(&text)).transpose())
            .collect()
    }

    /// Sends `ZUNIONSTORE destination numkeys key... [WEIGHTS ...] [AGGREGATE ...]`.
    pub fn zunionstore(
        &mut self,
        destination: &str,
        keys: &[&str],
        combine: SortedSetCombine<'_>,
    ) -> Result<i64, Error> {
        self.combine_store("ZUNIONSTORE", destination, keys, combine)
    }

    /// Sends `ZINTERSTORE destination numkeys key... [WEIGHTS ...] [AGGREGATE ...]`.
    pub fn zinterstore(
        &mut self,
        destination: &str,
        keys: &[&str],
        combine: SortedSetCombine<'_>,
    ) -> Result<i64, Error> {
        self.combine_store("ZINTERSTORE", destination, keys, combine)
    }

    /// Sends `ZDIFFSTORE destination numkeys key...`.
    pub fn zdiffstore(&mut self, destination: &str, keys: &[&str]) -> Result<i64, Error> {
        self.combine_store("ZDIFFSTORE", destination, keys, SortedSetCombine::default())
    }

    /// Sends `ZUNION numkeys key... [WEIGHTS ...] [AGGREGATE ...]` and returns the members.
    pub fn zunion(
        &mut self,
        keys: &[&str],
        combine: SortedSetCombine<'_>,
    ) -> Result<Vec<String>, Error> {
        self.combine_read("ZUNION", keys, combine, false)
            .and_then(members_only)
    }

    /// [`Self::zunion`] with `WITHSCORES`.
    pub fn zunion_with_scores(
        &mut self,
        keys: &[&str],
        combine: SortedSetCombine<'_>,
    ) -> Result<Vec<SortedSetEntry>, Error> {
        scored(self.combine_read("ZUNION", keys, combine, true)?)
    }

    /// Sends `ZINTER numkeys key... [WEIGHTS ...] [AGGREGATE ...]` and returns the members.
    pub fn zinter(
        &mut self,
        keys: &[&str],
        combine: SortedSetCombine<'_>,
    ) -> Result<Vec<String>, Error> {
        self.combine_read("ZINTER", keys, combine, false)
            .and_then(members_only)
    }

    /// [`Self::zinter`] with `WITHSCORES`.
    pub fn zinter_with_scores(
        &mut self,
        keys: &[&str],
        combine: SortedSetCombine<'_>,
    ) -> Result<Vec<SortedSetEntry>, Error> {
        scored(self.combine_read("ZINTER", keys, combine, true)?)
    }

    /// Sends `ZDIFF numkeys key...`: members of the first set missing from the rest.
    pub fn zdiff(&mut self, keys: &[&str]) -> Result<Vec<String>, Error> {
        self.combine_read("ZDIFF", keys, SortedSetCombine::default(), false)
            .and_then(members_only)
    }

    /// [`Self::zdiff`] with `WITHSCORES`.
    pub fn zdiff_with_scores(&mut self, keys: &[&str]) -> Result<Vec<SortedSetEntry>, Error> {
        scored(self.combine_read("ZDIFF", keys, SortedSetCombine::default(), true)?)
    }

    /// Sends `ZRANDMEMBER key`: one random member, `None` for a missing key.
    pub fn zrandmember(&mut self, key: &str) -> Result<Option<String>, Error> {
        self.bulk_or_null(["ZRANDMEMBER", key])
    }

    /// Sends `ZRANDMEMBER key count`. A negative count may repeat members.
    pub fn zrandmember_count(&mut self, key: &str, count: i64) -> Result<Vec<String>, Error> {
        let count = Num::int(count);

        self.strings(["ZRANDMEMBER", key, count.as_str()])
    }

    /// Sends `ZRANDMEMBER key count WITHSCORES`.
    pub fn zrandmember_with_scores(
        &mut self,
        key: &str,
        count: i64,
    ) -> Result<Vec<SortedSetEntry>, Error> {
        let count = Num::int(count);

        scored(self.execute(&["ZRANDMEMBER", key, count.as_str(), "WITHSCORES"])?)
    }

    // ---- hashes ----------------------------------------------------------------------------

    /// Sends `HRANDFIELD key`: one random field, `None` for a missing key.
    pub fn hrandfield(&mut self, key: &str) -> Result<Option<String>, Error> {
        self.bulk_or_null(["HRANDFIELD", key])
    }

    /// Sends `HRANDFIELD key count`. A negative count may repeat fields.
    pub fn hrandfield_count(&mut self, key: &str, count: i64) -> Result<Vec<String>, Error> {
        let count = Num::int(count);

        self.strings(["HRANDFIELD", key, count.as_str()])
    }

    /// Sends `HRANDFIELD key count WITHVALUES`.
    pub fn hrandfield_with_values(
        &mut self,
        key: &str,
        count: i64,
    ) -> Result<Vec<(String, String)>, Error> {
        let count = Num::int(count);
        let flat = self.strings(["HRANDFIELD", key, count.as_str(), "WITHVALUES"])?;

        Ok(pair_up(flat))
    }

    /// Sends `HINCRBYFLOAT` and returns the new value.
    pub fn hincr_by_float(&mut self, key: &str, field: &str, delta: f64) -> Result<f64, Error> {
        let delta = Num::float(delta);

        parse_score(&self.text(["HINCRBYFLOAT", key, field, delta.as_str()])?)
    }

    // ---- bitmaps ---------------------------------------------------------------------------

    /// Sends `SETBIT` and returns the previous bit.
    pub fn setbit(&mut self, key: &str, offset: u64, value: bool) -> Result<bool, Error> {
        let offset = Num::uint(offset);

        self.flag([
            "SETBIT",
            key,
            offset.as_str(),
            if value { "1" } else { "0" },
        ])
    }

    /// Sends `GETBIT`.
    pub fn getbit(&mut self, key: &str, offset: u64) -> Result<bool, Error> {
        let offset = Num::uint(offset);

        self.flag(["GETBIT", key, offset.as_str()])
    }

    /// Sends `BITCOUNT key`: set bits in the whole string.
    pub fn bitcount(&mut self, key: &str) -> Result<i64, Error> {
        self.integer(["BITCOUNT", key])
    }

    /// Sends `BITCOUNT key start end [BYTE|BIT]`.
    pub fn bitcount_range(
        &mut self,
        key: &str,
        start: i64,
        end: i64,
        unit: BitUnit,
    ) -> Result<i64, Error> {
        let (start, end) = (Num::int(start), Num::int(end));

        self.integer([
            "BITCOUNT",
            key,
            start.as_str(),
            end.as_str(),
            unit_word(unit),
        ])
    }

    /// Sends `BITPOS key bit`: the first bit set to `bit`, or -1.
    pub fn bitpos(&mut self, key: &str, bit: bool) -> Result<i64, Error> {
        self.integer(["BITPOS", key, if bit { "1" } else { "0" }])
    }

    /// Sends `BITPOS key bit start [end [BYTE|BIT]]`.
    pub fn bitpos_range(
        &mut self,
        key: &str,
        bit: bool,
        start: i64,
        end: Option<i64>,
        unit: BitUnit,
    ) -> Result<i64, Error> {
        let start = Num::int(start);
        let bit = if bit { "1" } else { "0" };

        match end.map(Num::int) {
            Some(end) => self.integer([
                "BITPOS",
                key,
                bit,
                start.as_str(),
                end.as_str(),
                unit_word(unit),
            ]),
            None => self.integer(["BITPOS", key, bit, start.as_str()]),
        }
    }

    /// Sends `BITOP operation destination key...` and returns the destination length.
    pub fn bitop(
        &mut self,
        operation: BitOperation,
        destination: &str,
        keys: &[&str],
    ) -> Result<i64, Error> {
        let operation = match operation {
            BitOperation::And => "AND",
            BitOperation::Or => "OR",
            BitOperation::Xor => "XOR",
            BitOperation::Not => "NOT",
        };

        self.integer(prefixed(&["BITOP", operation, destination], keys))
    }

    // ---- HyperLogLog -----------------------------------------------------------------------

    /// Sends `PFADD`. True when the estimate changed.
    pub fn pfadd(&mut self, key: &str, elements: &[&str]) -> Result<bool, Error> {
        self.flag(prefixed(&["PFADD", key], elements))
    }

    /// Sends `PFCOUNT`: the estimate, or the union estimate of several keys (same slot).
    pub fn pfcount(&mut self, keys: &[&str]) -> Result<i64, Error> {
        self.integer(prefixed(&["PFCOUNT"], keys))
    }

    /// Sends `PFMERGE destination source...`.
    pub fn pfmerge(&mut self, destination: &str, sources: &[&str]) -> Result<(), Error> {
        self.ok(prefixed(&["PFMERGE", destination], sources))
    }

    // ---- Bloom filters ---------------------------------------------------------------------

    /// Sends `BF.RESERVE key error capacity [EXPANSION n] [NONSCALING]`.
    pub fn bf_reserve_with(
        &mut self,
        key: &str,
        error_rate: f64,
        capacity: u64,
        options: BloomReserveOptions,
    ) -> Result<(), Error> {
        let error_rate = Num::float(error_rate);
        let capacity = Num::uint(capacity);
        let expansion = options.expansion.map(Num::uint);
        let mut arguments = [
            "BF.RESERVE",
            key,
            error_rate.as_str(),
            capacity.as_str(),
            "",
            "",
            "",
        ];
        let mut count = 4;

        if let Some(expansion) = &expansion {
            arguments[count] = "EXPANSION";
            arguments[count + 1] = expansion.as_str();
            count += 2;
        }

        if options.non_scaling {
            arguments[count] = "NONSCALING";
            count += 1;
        }

        self.ok(&arguments[..count])
    }

    /// Sends `BF.MADD key item...`: per item, true when it was added (false: maybe present).
    pub fn bf_madd(&mut self, key: &str, items: &[&str]) -> Result<Vec<bool>, Error> {
        flags("BF.MADD", self.run(prefixed(&["BF.MADD", key], items))?)
    }

    /// Sends `BF.INSERT key [CAPACITY c] [ERROR e] [EXPANSION x] [NOCREATE] [NONSCALING] ITEMS item...`.
    pub fn bf_insert(
        &mut self,
        key: &str,
        options: &BloomInsertOptions,
        items: &[&str],
    ) -> Result<Vec<bool>, Error> {
        let capacity = options.capacity.map(Num::uint);
        let error_rate = options.error_rate.map(Num::float);
        let expansion = options.expansion.map(Num::uint);
        let mut arguments = Vec::with_capacity(11 + items.len());

        arguments.extend(["BF.INSERT", key]);

        if let Some(capacity) = &capacity {
            arguments.extend(["CAPACITY", capacity.as_str()]);
        }

        if let Some(error_rate) = &error_rate {
            arguments.extend(["ERROR", error_rate.as_str()]);
        }

        if let Some(expansion) = &expansion {
            arguments.extend(["EXPANSION", expansion.as_str()]);
        }

        if options.no_create {
            arguments.push("NOCREATE");
        }

        if options.non_scaling {
            arguments.push("NONSCALING");
        }

        arguments.push("ITEMS");
        arguments.extend_from_slice(items);

        flags("BF.INSERT", self.run(arguments)?)
    }

    /// Sends `BF.INFO key` and reads every field.
    pub fn bf_info(&mut self, key: &str) -> Result<BloomInfo, Error> {
        let reply = self.execute(&["BF.INFO", key])?;
        let Some(items) = reply.as_array() else {
            return Err(unexpected("BF.INFO", &reply));
        };
        let mut info = BloomInfo::default();

        for pair in items.chunks_exact(2) {
            let value = pair[1].as_integer();

            match pair[0].as_string()?.as_deref() {
                Some("Capacity") => info.capacity = value.unwrap_or_default(),
                Some("Size") => info.size = value.unwrap_or_default(),
                Some("Number of filters") => info.filters = value.unwrap_or_default(),
                Some("Number of items inserted") => info.items = value.unwrap_or_default(),
                Some("Expansion rate") => info.expansion = value,
                _ => {}
            }
        }

        Ok(info)
    }

    /// Sends `BF.INFO key field` (`CAPACITY`, `SIZE`, `FILTERS`, `ITEMS`, `EXPANSION`).
    /// `None` for the expansion of a non-scaling filter.
    pub fn bf_info_field(&mut self, key: &str, field: &str) -> Result<Option<i64>, Error> {
        self.run_with(["BF.INFO", key, field], |command, reply| match reply {
            RespValue::Null => Ok(None),
            RespValue::Integer(value) => Ok(Some(value)),
            RespValue::Array(mut items) if items.len() == 1 => match items.pop() {
                Some(RespValue::Integer(value)) => Ok(Some(value)),
                _ => Ok(None),
            },
            other => Err(unexpected(command, &other)),
        })
    }

    /// Sends `BF.CARD`: items added so far; 0 for a missing key.
    pub fn bf_card(&mut self, key: &str) -> Result<i64, Error> {
        self.integer(["BF.CARD", key])
    }

    /// Sends `BF.MEXISTS key item...`: per item, true for "maybe added".
    pub fn bf_mexists(&mut self, key: &str, items: &[&str]) -> Result<Vec<bool>, Error> {
        flags(
            "BF.MEXISTS",
            self.run(prefixed(&["BF.MEXISTS", key], items))?,
        )
    }

    // ---- JSON ------------------------------------------------------------------------------

    /// Sends `JSON.SET key path json [NX|XX]`. False when the condition or path stopped the write.
    pub fn json_set_with(
        &mut self,
        key: &str,
        path: &str,
        json: &str,
        condition: JsonSetCondition,
    ) -> Result<bool, Error> {
        let reply = match condition {
            JsonSetCondition::Always => self.execute(&["JSON.SET", key, path, json])?,
            JsonSetCondition::IfMissing => self.execute(&["JSON.SET", key, path, json, "NX"])?,
            JsonSetCondition::IfExists => self.execute(&["JSON.SET", key, path, json, "XX"])?,
        };

        Ok(!reply.is_null())
    }

    /// Sends `JSON.OBJLEN`. `None` when the key or path is missing.
    pub fn json_objlen(&mut self, key: &str, path: &str) -> Result<Option<i64>, Error> {
        self.optional_integer(["JSON.OBJLEN", key, path])
    }

    /// Sends `JSON.STRAPPEND key path json-string` (pass the quoted JSON string, e.g. `"\"ab\""`).
    /// Returns the new byte length.
    pub fn json_strappend(
        &mut self,
        key: &str,
        path: &str,
        json_string: &str,
    ) -> Result<Option<i64>, Error> {
        self.optional_integer(["JSON.STRAPPEND", key, path, json_string])
    }

    /// Sends `JSON.NUMMULTBY` and returns the new number as JSON text.
    pub fn json_nummultby(
        &mut self,
        key: &str,
        path: &str,
        by: f64,
    ) -> Result<Option<String>, Error> {
        let by = Num::json_number(by);

        self.bulk_or_null(["JSON.NUMMULTBY", key, path, by.as_str()])
    }

    /// Sends `JSON.ARRAPPEND key path json...` with several values; returns the new length.
    pub fn json_arrappend_many(
        &mut self,
        key: &str,
        path: &str,
        values: &[&str],
    ) -> Result<Option<i64>, Error> {
        self.optional_integer(prefixed(&["JSON.ARRAPPEND", key, path], values))
    }

    /// Sends `JSON.ARRINSERT key path index json...`; returns the new length.
    pub fn json_arrinsert(
        &mut self,
        key: &str,
        path: &str,
        index: i64,
        values: &[&str],
    ) -> Result<Option<i64>, Error> {
        let index = Num::int(index);

        self.optional_integer(prefixed(
            &["JSON.ARRINSERT", key, path, index.as_str()],
            values,
        ))
    }

    /// Sends `JSON.ARRPOP key path [index]` and returns the popped value as JSON text.
    /// `None` for an empty array or a missing key. `index` defaults to -1 (the last element).
    pub fn json_arrpop(
        &mut self,
        key: &str,
        path: &str,
        index: Option<i64>,
    ) -> Result<Option<String>, Error> {
        match index.map(Num::int) {
            Some(index) => self.bulk_or_null(["JSON.ARRPOP", key, path, index.as_str()]),
            None => self.bulk_or_null(["JSON.ARRPOP", key, path]),
        }
    }

    /// Sends `JSON.ARRTRIM key path start stop` (inclusive) and returns the new length.
    pub fn json_arrtrim(
        &mut self,
        key: &str,
        path: &str,
        start: i64,
        stop: i64,
    ) -> Result<Option<i64>, Error> {
        let (start, stop) = (Num::int(start), Num::int(stop));

        self.optional_integer(["JSON.ARRTRIM", key, path, start.as_str(), stop.as_str()])
    }

    /// Sends `JSON.ARRINDEX key path scalar [start [stop]]`. `scalar` is JSON text (`"\"a\""`, `1`).
    /// Returns the index or -1.
    pub fn json_arrindex(
        &mut self,
        key: &str,
        path: &str,
        scalar: &str,
        start: Option<i64>,
        stop: Option<i64>,
    ) -> Result<Option<i64>, Error> {
        let start = start.or(stop.map(|_| 0)).map(Num::int);
        let stop = stop.map(Num::int);

        match (&start, &stop) {
            (Some(start), Some(stop)) => self.optional_integer([
                "JSON.ARRINDEX",
                key,
                path,
                scalar,
                start.as_str(),
                stop.as_str(),
            ]),
            (Some(start), None) => {
                self.optional_integer(["JSON.ARRINDEX", key, path, scalar, start.as_str()])
            }
            _ => self.optional_integer(["JSON.ARRINDEX", key, path, scalar]),
        }
    }

    /// Sends `JSON.FORGET`, the alias of `JSON.DEL`.
    pub fn json_forget(&mut self, key: &str, path: &str) -> Result<i64, Error> {
        self.integer(["JSON.FORGET", key, path])
    }

    /// Sends `JMERGE`, the alias of `JSON.MERGE`.
    pub fn jmerge(&mut self, key: &str, path: &str, patch: &str) -> Result<bool, Error> {
        Ok(!self.execute(&["JMERGE", key, path, patch])?.is_null())
    }

    /// Sends `JSON.INDEX CREATE name PREFIX prefix PATH path [TYPE string|number|bool]`
    /// (Ruvio extension): an exact-equality index on one field, database 0 only.
    pub fn json_index_create(
        &mut self,
        name: &str,
        prefix: &str,
        path: &str,
        kind: Option<JsonIndexType>,
    ) -> Result<(), Error> {
        match kind {
            Some(kind) => self.ok([
                "JSON.INDEX",
                "CREATE",
                name,
                "PREFIX",
                prefix,
                "PATH",
                path,
                "TYPE",
                match kind {
                    JsonIndexType::String => "string",
                    JsonIndexType::Number => "number",
                    JsonIndexType::Bool => "bool",
                },
            ]),
            None => self.ok(["JSON.INDEX", "CREATE", name, "PREFIX", prefix, "PATH", path]),
        }
    }

    /// Sends `JSON.INDEX DROP name`. True when the index existed.
    pub fn json_index_drop(&mut self, name: &str) -> Result<bool, Error> {
        self.flag(["JSON.INDEX", "DROP", name])
    }

    /// Sends `JSON.INDEX LIST`: one description line per index
    /// (`name prefix=... path=... type=... state=... keys=... memory=...`).
    pub fn json_index_list(&mut self) -> Result<Vec<String>, Error> {
        self.strings(["JSON.INDEX", "LIST"])
    }

    /// Sends `JSON.FIND name VALUE value [COUNT n] [AFTER key]`: matching keys in ascending
    /// order. Page with `after` = the last key of the previous page. Errors with
    /// `index is building` until the first build finishes.
    pub fn json_find(
        &mut self,
        name: &str,
        value: &str,
        count: Option<u64>,
        after: Option<&str>,
    ) -> Result<Vec<String>, Error> {
        let count = count.map(Num::uint);
        let mut arguments = ["JSON.FIND", name, "VALUE", value, "", "", "", ""];
        let mut used = 4;

        if let Some(count) = &count {
            arguments[used] = "COUNT";
            arguments[used + 1] = count.as_str();
            used += 2;
        }

        if let Some(after) = after {
            arguments[used] = "AFTER";
            arguments[used + 1] = after;
            used += 2;
        }

        self.strings(&arguments[..used])
    }

    // ---- geo -------------------------------------------------------------------------------

    /// Sends `GEOADD key [NX|XX] [CH] lon lat member...`. Only `if_not_exists`, `if_exists`
    /// and `changed` of `options` apply.
    pub fn geo_add_with(
        &mut self,
        key: &str,
        options: SortedSetAddOptions,
        places: &[(f64, f64, &str)],
    ) -> Result<i64, Error> {
        let numbers: Vec<(Num, Num)> = places
            .iter()
            .map(|(longitude, latitude, _)| (Num::float(*longitude), Num::float(*latitude)))
            .collect();
        let mut arguments = Vec::with_capacity(4 + places.len() * 3);

        arguments.extend(["GEOADD", key]);

        if options.if_not_exists {
            arguments.push("NX");
        }

        if options.if_exists {
            arguments.push("XX");
        }

        if options.changed {
            arguments.push("CH");
        }

        for ((longitude, latitude), (_, _, member)) in numbers.iter().zip(places) {
            arguments.extend([longitude.as_str(), latitude.as_str(), *member]);
        }

        self.integer(arguments)
    }

    /// Sends a full `GEOSEARCH` (`FROMMEMBER`/`FROMLONLAT`, `BYRADIUS`/`BYBOX`, `ASC`/`DESC`,
    /// `COUNT [ANY]`, `WITHDIST`, `WITHHASH`, `WITHCOORD`).
    pub fn geo_search_with(
        &mut self,
        key: &str,
        search: &GeoSearch<'_>,
    ) -> Result<Vec<GeoMatch>, Error> {
        let (first, second) = match search.from {
            GeoFrom::Member(_) => (None, None),
            GeoFrom::LonLat(longitude, latitude) => {
                (Some(Num::float(longitude)), Some(Num::float(latitude)))
            }
        };
        let (width, height) = match search.shape {
            GeoShape::Radius(radius) => (Num::float(radius), None),
            GeoShape::Box(width, height) => (Num::float(width), Some(Num::float(height))),
        };
        let count = search.count.map(Num::uint);
        let mut arguments: Vec<&str> = Vec::with_capacity(16);

        arguments.extend(["GEOSEARCH", key]);

        match (search.from, &first, &second) {
            (GeoFrom::Member(member), _, _) => arguments.extend(["FROMMEMBER", member]),
            (_, Some(longitude), Some(latitude)) => {
                arguments.extend(["FROMLONLAT", longitude.as_str(), latitude.as_str()])
            }
            _ => {}
        }

        match &height {
            None => arguments.extend(["BYRADIUS", width.as_str(), search.unit]),
            Some(height) => {
                arguments.extend(["BYBOX", width.as_str(), height.as_str(), search.unit])
            }
        }

        match search.descending {
            Some(false) => arguments.push("ASC"),
            Some(true) => arguments.push("DESC"),
            None => {}
        }

        if let Some(count) = &count {
            arguments.extend(["COUNT", count.as_str()]);

            if search.any {
                arguments.push("ANY");
            }
        }

        if search.with_distance {
            arguments.push("WITHDIST");
        }

        if search.with_hash {
            arguments.push("WITHHASH");
        }

        if search.with_coordinates {
            arguments.push("WITHCOORD");
        }

        let reply = self.run(arguments)?;

        geo_matches(reply, search)
    }

    // ---- Ruvio jobs ------------------------------------------------------------------------

    /// Sends `IDEM key BEGIN fingerprint owner ttl_ms` and decodes the state.
    /// See [`Self::idempotent`] for the full run-once flow.
    pub fn idem_begin(
        &mut self,
        key: &str,
        fingerprint: &str,
        owner: &str,
        ttl: Duration,
    ) -> Result<IdemState, Error> {
        let ttl = Num::wide(duration_millis(ttl));
        let reply = self.execute(&["IDEM", key, "BEGIN", fingerprint, owner, ttl.as_str()])?;

        idem_state(reply)
    }

    /// Sends `IDEM key COMPLETE fingerprint owner result`. True when the result was published
    /// (or an identical result was already stored).
    pub fn idem_complete(
        &mut self,
        key: &str,
        fingerprint: &str,
        owner: &str,
        result: &[u8],
    ) -> Result<bool, Error> {
        let reply = self.execute_binary(&[
            b"IDEM",
            key.as_bytes(),
            b"COMPLETE",
            fingerprint.as_bytes(),
            owner.as_bytes(),
            result,
        ])?;

        match reply {
            RespValue::Integer(value) => Ok(value > 0),
            other => Err(unexpected("IDEM", &other)),
        }
    }

    // ---- shared plumbing -------------------------------------------------------------------

    pub(crate) fn scan_page(
        &mut self,
        command: &str,
        key: Option<&str>,
        cursor: u64,
        pattern: Option<&str>,
        count: Option<u64>,
    ) -> Result<(u64, Vec<String>), Error> {
        let cursor = Num::uint(cursor);
        let count = count.map(Num::uint);
        let mut arguments = [command, "", "", "", "", "", ""];
        let mut used = 1;

        if let Some(key) = key {
            arguments[used] = key;
            used += 1;
        }

        arguments[used] = cursor.as_str();
        used += 1;

        if let Some(pattern) = pattern {
            arguments[used] = "MATCH";
            arguments[used + 1] = pattern;
            used += 2;
        }

        if let Some(count) = &count {
            arguments[used] = "COUNT";
            arguments[used + 1] = count.as_str();
            used += 2;
        }

        let reply = self.execute(&arguments[..used])?;

        scan_reply(command, reply)
    }

    fn ranged(
        &mut self,
        command: &str,
        key: &str,
        first: &str,
        second: &str,
        limit: Option<(i64, i64)>,
    ) -> Result<Vec<String>, Error> {
        match limit {
            Some((offset, count)) => {
                let (offset, count) = (Num::int(offset), Num::int(count));

                self.strings([
                    command,
                    key,
                    first,
                    second,
                    "LIMIT",
                    offset.as_str(),
                    count.as_str(),
                ])
            }
            None => self.strings([command, key, first, second]),
        }
    }

    fn combine_store(
        &mut self,
        command: &str,
        destination: &str,
        keys: &[&str],
        combine: SortedSetCombine<'_>,
    ) -> Result<i64, Error> {
        let numkeys = Num::uint(keys.len() as u64);
        let weights: Vec<Num> = combine
            .weights
            .iter()
            .map(|weight| Num::float(*weight))
            .collect();
        let mut arguments = prefixed(&[command, destination, numkeys.as_str()], keys);

        push_combine(&mut arguments, &weights, combine.aggregate);
        self.integer(arguments)
    }

    fn combine_read(
        &mut self,
        command: &str,
        keys: &[&str],
        combine: SortedSetCombine<'_>,
        with_scores: bool,
    ) -> Result<RespValue, Error> {
        let numkeys = Num::uint(keys.len() as u64);
        let weights: Vec<Num> = combine
            .weights
            .iter()
            .map(|weight| Num::float(*weight))
            .collect();
        let mut arguments = prefixed(&[command, numkeys.as_str()], keys);

        push_combine(&mut arguments, &weights, combine.aggregate);

        if with_scores {
            arguments.push("WITHSCORES");
        }

        self.run(arguments)
    }
}

/// `head` followed by `rest`, in one allocation of borrowed pointers.
pub(crate) fn prefixed<'a>(head: &[&'a str], rest: &[&'a str]) -> Vec<&'a str> {
    let mut arguments = Vec::with_capacity(head.len() + rest.len() + 4);

    arguments.extend_from_slice(head);
    arguments.extend_from_slice(rest);

    arguments
}

fn push_combine<'a>(
    arguments: &mut Vec<&'a str>,
    weights: &'a [Num],
    aggregate: Option<Aggregate>,
) {
    if !weights.is_empty() {
        arguments.push("WEIGHTS");
        arguments.extend(weights.iter().map(Num::as_str));
    }

    if let Some(aggregate) = aggregate {
        arguments.push("AGGREGATE");
        arguments.push(match aggregate {
            Aggregate::Sum => "SUM",
            Aggregate::Min => "MIN",
            Aggregate::Max => "MAX",
        });
    }
}

fn push_lpos_options<'a, const N: usize>(
    arguments: &mut [&'a str; N],
    mut used: usize,
    rank: Option<&'a Num>,
    max_len: Option<&'a Num>,
) -> usize {
    if let Some(rank) = rank {
        arguments[used] = "RANK";
        arguments[used + 1] = rank.as_str();
        used += 2;
    }

    if let Some(max_len) = max_len {
        arguments[used] = "MAXLEN";
        arguments[used + 1] = max_len.as_str();
        used += 2;
    }

    used
}

fn push_range_options<'a>(
    arguments: &mut [&'a str; 10],
    options: RangeOptions,
    offsets: Option<&'a (Num, Num)>,
    with_scores: bool,
) -> usize {
    let mut used = 4;

    match options.by {
        RangeBy::Rank => {}
        RangeBy::Score => {
            arguments[used] = "BYSCORE";
            used += 1;
        }
        RangeBy::Lex => {
            arguments[used] = "BYLEX";
            used += 1;
        }
    }

    if options.rev {
        arguments[used] = "REV";
        used += 1;
    }

    if let Some((offset, count)) = offsets {
        arguments[used] = "LIMIT";
        arguments[used + 1] = offset.as_str();
        arguments[used + 2] = count.as_str();
        used += 3;
    }

    if with_scores {
        arguments[used] = "WITHSCORES";
        used += 1;
    }

    used
}

fn unit_word(unit: BitUnit) -> &'static str {
    match unit {
        BitUnit::Byte => "BYTE",
        BitUnit::Bit => "BIT",
    }
}

fn flags(command: &str, reply: RespValue) -> Result<Vec<bool>, Error> {
    let RespValue::Array(items) = reply else {
        return Err(unexpected(command, &reply));
    };

    items
        .into_iter()
        .map(|item| match item {
            RespValue::Integer(value) => Ok(value != 0),
            RespValue::Error(message) => Err(Error::Server(message)),
            other => Err(unexpected(command, &other)),
        })
        .collect()
}

/// Moves the strings of a flat array without copying them.
fn flat_strings(command: &str, reply: RespValue) -> Result<Vec<String>, Error> {
    match reply {
        RespValue::Null => Ok(Vec::new()),
        RespValue::Array(items) => items
            .into_iter()
            .map(|item| Ok(item.into_string()?.unwrap_or_default()))
            .collect(),
        other => Err(unexpected(command, &other)),
    }
}

fn members_only(reply: RespValue) -> Result<Vec<String>, Error> {
    flat_strings("ZRANGE", reply)
}

/// A flat `member, score, member, score` reply.
fn scored(reply: RespValue) -> Result<Vec<SortedSetEntry>, Error> {
    entries_from_flat(flat_strings("ZRANGE", reply)?)
}

pub(crate) fn entries_from_flat(flat: Vec<String>) -> Result<Vec<SortedSetEntry>, Error> {
    let mut entries = Vec::with_capacity(flat.len() / 2);
    let mut items = flat.into_iter();

    while let (Some(member), Some(score)) = (items.next(), items.next()) {
        entries.push(SortedSetEntry {
            score: parse_score(&score)?,
            member,
        });
    }

    Ok(entries)
}

pub(crate) fn pair_up(flat: Vec<String>) -> Vec<(String, String)> {
    let mut pairs = Vec::with_capacity(flat.len() / 2);
    let mut items = flat.into_iter();

    while let (Some(left), Some(right)) = (items.next(), items.next()) {
        pairs.push((left, right));
    }

    pairs
}

pub(crate) fn scan_reply(command: &str, reply: RespValue) -> Result<(u64, Vec<String>), Error> {
    let RespValue::Array(mut parts) = reply else {
        return Err(unexpected(command, &reply));
    };

    if parts.len() != 2 {
        return Err(Error::Protocol(format!(
            "{command} returned {} parts",
            parts.len()
        )));
    }

    let items = parts.pop().unwrap_or(RespValue::Null);
    let cursor = parts
        .pop()
        .and_then(|cursor| cursor.into_string().ok().flatten())
        .and_then(|text| text.parse().ok())
        .ok_or_else(|| Error::Protocol(format!("{command} returned a bad cursor")))?;

    Ok((cursor, flat_strings(command, items)?))
}

fn idem_state(reply: RespValue) -> Result<IdemState, Error> {
    let RespValue::Array(mut items) = reply else {
        return Err(unexpected("IDEM", &reply));
    };

    let result = if items.len() > 1 { items.pop() } else { None };
    let state = items
        .into_iter()
        .next()
        .and_then(|state| state.into_string().ok().flatten())
        .unwrap_or_default();

    match state.as_str() {
        "acquired" => Ok(IdemState::Acquired),
        "pending" => Ok(IdemState::Pending),
        "conflict" => Ok(IdemState::Conflict),
        "completed" => Ok(IdemState::Completed(
            result.and_then(RespValue::into_bytes).unwrap_or_default(),
        )),
        other => Err(Error::Protocol(format!("IDEM returned state {other:?}"))),
    }
}

fn geo_matches(reply: RespValue, search: &GeoSearch<'_>) -> Result<Vec<GeoMatch>, Error> {
    let RespValue::Array(rows) = reply else {
        return Err(unexpected("GEOSEARCH", &reply));
    };

    let mut matches = Vec::with_capacity(rows.len());

    for row in rows {
        let mut found = GeoMatch {
            member: String::new(),
            distance: None,
            hash: None,
            coordinates: None,
        };

        let RespValue::Array(parts) = row else {
            found.member = row.into_string()?.unwrap_or_default();
            matches.push(found);

            continue;
        };

        let mut parts = parts.into_iter();

        found.member = parts
            .next()
            .and_then(|member| member.into_string().ok().flatten())
            .unwrap_or_default();

        if search.with_distance {
            found.distance = parts
                .next()
                .and_then(|value| value.into_string().ok().flatten())
                .map(|text| parse_score(&text))
                .transpose()?;
        }

        if search.with_hash {
            found.hash = parts.next().and_then(|value| value.as_integer());
        }

        if search.with_coordinates {
            if let Some(RespValue::Array(pair)) = parts.next() {
                if let [longitude, latitude] = pair.as_slice() {
                    let longitude = parse_score(&longitude.as_string()?.unwrap_or_default())?;
                    let latitude = parse_score(&latitude.as_string()?.unwrap_or_default())?;

                    found.coordinates = Some((longitude, latitude));
                }
            }
        }

        matches.push(found);
    }

    Ok(matches)
}

#[cfg(test)]
mod tests {
    use super::{entries_from_flat, flags, idem_state, prefixed, scan_reply};
    use crate::models::IdemState;
    use crate::value::RespValue;

    fn bulk(text: &str) -> RespValue {
        RespValue::Bulk(text.as_bytes().to_vec())
    }

    #[test]
    fn idem_replies_decode_into_states() {
        assert_eq!(
            idem_state(RespValue::Array(vec![bulk("acquired")])).unwrap(),
            IdemState::Acquired
        );
        assert_eq!(
            idem_state(RespValue::Array(vec![bulk("completed"), bulk("r")])).unwrap(),
            IdemState::Completed(b"r".to_vec())
        );
        assert!(idem_state(RespValue::Integer(1)).is_err());
    }

    #[test]
    fn scan_and_flag_replies_decode() {
        let page = RespValue::Array(vec![
            bulk("7"),
            RespValue::Array(vec![bulk("a"), bulk("1")]),
        ]);

        assert_eq!(
            scan_reply("ZSCAN", page).unwrap(),
            (7, vec!["a".to_owned(), "1".to_owned()])
        );
        assert_eq!(
            entries_from_flat(vec!["a".into(), "1.5".into()]).unwrap()[0].score,
            1.5
        );
        assert_eq!(
            flags(
                "BF.MADD",
                RespValue::Array(vec![RespValue::Integer(1), RespValue::Integer(0)])
            )
            .unwrap(),
            vec![true, false]
        );
        assert!(
            flags(
                "BF.MADD",
                RespValue::Array(vec![RespValue::Error("ERR full".into())])
            )
            .is_err()
        );
        assert_eq!(prefixed(&["A", "b"], &["c"]), ["A", "b", "c"]);
    }
}
