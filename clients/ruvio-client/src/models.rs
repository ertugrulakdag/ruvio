use std::time::{Duration, SystemTime};

/// A sorted-set member and its score.
#[derive(Debug, Clone, PartialEq)]
pub struct SortedSetEntry {
    /// The member.
    pub member: String,
    /// The member's score.
    pub score: f64,
}

impl SortedSetEntry {
    /// A member and its score.
    pub fn new(member: impl Into<String>, score: f64) -> Self {
        Self {
            member: member.into(),
            score,
        }
    }
}

impl<S: Into<String>> From<(S, f64)> for SortedSetEntry {
    fn from((member, score): (S, f64)) -> Self {
        Self::new(member, score)
    }
}

/// When `HEXPIRE` and its siblings may change a field's TTL. A field without a TTL counts
/// as an infinite TTL for `IfGreater` and `IfLess`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum FieldExpireCondition {
    /// Always set the TTL.
    #[default]
    Always,
    /// Only when the field has no TTL (`NX`).
    IfNoTtl,
    /// Only when the field already has a TTL (`XX`).
    IfTtl,
    /// Only when the new deadline is later (`GT`).
    IfGreater,
    /// Only when the new deadline is earlier (`LT`).
    IfLess,
}

/// `ZADD` options. The server refuses `NX` with `XX`, `GT`, or `LT`, and `GT` with `LT`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SortedSetAddOptions {
    /// Only add new members (`NX`).
    pub if_not_exists: bool,
    /// Only update existing members (`XX`).
    pub if_exists: bool,
    /// Only update when the new score is greater (`GT`). A missing member is still added.
    pub greater_than: bool,
    /// Only update when the new score is lower (`LT`). A missing member is still added.
    pub less_than: bool,
    /// Count changed members as well as new ones (`CH`).
    pub changed: bool,
}

/// `XREAD` and `XREADGROUP` options.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct StreamReadOptions {
    /// Most entries to return per stream (`COUNT`).
    pub count: Option<u64>,
    /// How long to wait for a new entry (`BLOCK`). `Duration::ZERO` waits until an entry arrives.
    /// The connection sends nothing else while it waits.
    pub block: Option<Duration>,
    /// `XREADGROUP` only: advance the group without adding pending entries (`NOACK`).
    pub no_ack: bool,
}

/// `XCLAIM` options.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct StreamClaimOptions {
    /// Set the claimed entries' idle time (`IDLE`). Cannot be combined with [`Self::time`].
    pub idle: Option<Duration>,
    /// Set the claimed entries' last delivery time (`TIME`).
    pub time: Option<SystemTime>,
    /// Set the delivery count (`RETRYCOUNT`).
    pub retry_count: Option<u64>,
    /// Claim ids that are in the stream but not pending (`FORCE`).
    pub force: bool,
    /// Move the group's last delivered id (`LASTID`).
    pub last_id: Option<String>,
}

/// Optional narrowing for the extended `XPENDING` form.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct StreamPendingFilter {
    /// Only entries owned by this consumer.
    pub consumer: Option<String>,
    /// Only entries idle for at least this long (`IDLE`).
    pub min_idle: Option<Duration>,
}

/// One row of the extended `XPENDING` form.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamPendingEntry {
    /// Entry id.
    pub id: String,
    /// Consumer that owns the entry.
    pub consumer: String,
    /// Milliseconds since the entry was last delivered.
    pub idle_millis: i64,
    /// How many times the entry was delivered.
    pub delivery_count: i64,
}

/// One `SCAN` page. A cursor of 0 means the scan is complete.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScanPage {
    /// Cursor for the next call.
    pub cursor: u64,
    /// Keys on this page.
    pub keys: Vec<String>,
}

/// One `HSCAN` page. A cursor of 0 means the scan is complete.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HashScanPage {
    /// Cursor for the next call.
    pub cursor: u64,
    /// Field and value pairs on this page.
    pub fields: Vec<(String, String)>,
}

/// One stream entry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamEntry {
    /// Entry id.
    pub id: String,
    /// Field and value pairs in the order they were added.
    pub fields: Vec<(String, String)>,
}

/// Entries read from one stream key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamReadResult {
    /// Stream key.
    pub key: String,
    /// Entries read from the key.
    pub entries: Vec<StreamEntry>,
}

/// A Pub/Sub delivery: `message`, `pmessage`, or `smessage`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PubSubMessage {
    /// `message`, `pmessage`, or `smessage`.
    pub kind: String,
    /// The matching pattern for `pmessage`.
    pub pattern: Option<String>,
    /// Channel the message was published to.
    pub channel: String,
    /// Raw payload.
    pub payload: Vec<u8>,
}

/// The answer to `FILLGET`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FillOutcome {
    /// The value is stored and fresh.
    Hit(String),
    /// Nothing is stored and this caller must build the value, then call `FILLSET` with the token.
    Fill(String),
    /// Nothing is stored and another caller is building it.
    Busy,
    /// The value is past its soft TTL. `token` is `Some` when this caller must refresh it.
    Stale {
        /// The old value, still served.
        value: String,
        /// The refresh token, or `None` when someone else is refreshing.
        token: Option<String>,
    },
}

/// One `SSCAN` page. A cursor of 0 means the scan is complete.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SetScanPage {
    /// Cursor for the next call.
    pub cursor: u64,
    /// Members on this page.
    pub members: Vec<String>,
}

/// One `ZSCAN` page. A cursor of 0 means the scan is complete.
#[derive(Debug, Clone, PartialEq)]
pub struct SortedSetScanPage {
    /// Cursor for the next call.
    pub cursor: u64,
    /// Members and scores on this page.
    pub entries: Vec<SortedSetEntry>,
}

/// `BF.RESERVE` options beyond error rate and capacity.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct BloomReserveOptions {
    /// Growth factor of each new layer (`EXPANSION`).
    pub expansion: Option<u64>,
    /// One fixed layer; a full filter answers an error (`NONSCALING`).
    pub non_scaling: bool,
}

/// `BF.INSERT` options. The creation options apply only when the key is new.
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct BloomInsertOptions {
    /// `CAPACITY` for a new filter.
    pub capacity: Option<u64>,
    /// `ERROR` rate for a new filter.
    pub error_rate: Option<f64>,
    /// `EXPANSION` for a new filter.
    pub expansion: Option<u64>,
    /// `NOCREATE`: a missing key is an error instead of a new filter.
    pub no_create: bool,
    /// `NONSCALING` for a new filter.
    pub non_scaling: bool,
}

/// The `BF.INFO` reply.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct BloomInfo {
    /// Items the first layer was sized for.
    pub capacity: i64,
    /// Bytes: the bit arrays plus the filter header.
    pub size: i64,
    /// Number of layers.
    pub filters: i64,
    /// Items that set at least one new bit.
    pub items: i64,
    /// Growth factor; `None` for a non-scaling filter.
    pub expansion: Option<i64>,
}

/// Value type of a `JSON.INDEX` field.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JsonIndexType {
    /// `TYPE string`.
    String,
    /// `TYPE number`.
    Number,
    /// `TYPE bool`.
    Bool,
}

/// When `JSON.SET` may write.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum JsonSetCondition {
    /// Always write.
    #[default]
    Always,
    /// Only when the path is missing (`NX`).
    IfMissing,
    /// Only when the path exists (`XX`).
    IfExists,
}

/// The `BITOP` operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BitOperation {
    /// `AND`.
    And,
    /// `OR`.
    Or,
    /// `XOR`.
    Xor,
    /// `NOT` (one source key).
    Not,
}

/// The unit of a `BITCOUNT` / `BITPOS` range.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum BitUnit {
    /// Byte offsets (the default).
    #[default]
    Byte,
    /// Bit offsets (`BIT`).
    Bit,
}

/// How `ZUNION`, `ZINTER` and their `STORE` forms combine scores.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum Aggregate {
    /// Add the scores (the default).
    #[default]
    Sum,
    /// Keep the lowest score.
    Min,
    /// Keep the highest score.
    Max,
}

/// `WEIGHTS` and `AGGREGATE` for `ZUNION` / `ZINTER` / `ZUNIONSTORE` / `ZINTERSTORE`.
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct SortedSetCombine<'a> {
    /// One multiplier per input key; empty sends no `WEIGHTS`.
    pub weights: &'a [f64],
    /// `AGGREGATE`; `None` leaves the server default (`SUM`).
    pub aggregate: Option<Aggregate>,
}

/// What the bounds of [`crate::Client::zrange_query`] mean.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum RangeBy {
    /// Ranks (indexes).
    #[default]
    Rank,
    /// Scores (`BYSCORE`): `-inf`, `+inf`, `(` for exclusive.
    Score,
    /// Members (`BYLEX`): `-`, `+`, `[member`, `(member`.
    Lex,
}

/// `ZRANGE` options.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RangeOptions {
    /// What the bounds mean.
    pub by: RangeBy,
    /// Highest first (`REV`). Bounds are sent exactly as given.
    pub rev: bool,
    /// `LIMIT offset count` (only with `Score` or `Lex`). A negative count returns the rest.
    pub limit: Option<(i64, i64)>,
}

/// The answer to `IDEM key BEGIN`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IdemState {
    /// This caller owns the reservation and must run the work, then `COMPLETE`.
    Acquired,
    /// Another owner holds a live reservation. Do not run the work.
    Pending,
    /// The key holds a different fingerprint.
    Conflict,
    /// The work already finished; the stored result.
    Completed(Vec<u8>),
}

/// `RESTORE` options.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RestoreOptions {
    /// Overwrite an existing key (`REPLACE`).
    pub replace: bool,
    /// The TTL is a Unix-millisecond deadline (`ABSTTL`).
    pub absolute_ttl: bool,
}

/// Where a `GEOSEARCH` starts.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum GeoFrom<'a> {
    /// `FROMMEMBER member`.
    Member(&'a str),
    /// `FROMLONLAT longitude latitude`.
    LonLat(f64, f64),
}

/// The `GEOSEARCH` area.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum GeoShape {
    /// `BYRADIUS radius`.
    Radius(f64),
    /// `BYBOX width height`.
    Box(f64, f64),
}

/// A full `GEOSEARCH` request.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct GeoSearch<'a> {
    /// Center.
    pub from: GeoFrom<'a>,
    /// Area.
    pub shape: GeoShape,
    /// `m`, `km`, `ft`, or `mi`.
    pub unit: &'a str,
    /// `Some(false)` sends `ASC`, `Some(true)` sends `DESC`.
    pub descending: Option<bool>,
    /// `COUNT n`.
    pub count: Option<u64>,
    /// `ANY` (with `count`): stop at the first `count` matches.
    pub any: bool,
    /// `WITHDIST`.
    pub with_distance: bool,
    /// `WITHHASH`.
    pub with_hash: bool,
    /// `WITHCOORD`.
    pub with_coordinates: bool,
}

impl<'a> GeoSearch<'a> {
    /// A radius search around a point, nearest first, without extra fields.
    pub fn radius(from: GeoFrom<'a>, radius: f64, unit: &'a str) -> Self {
        Self {
            from,
            shape: GeoShape::Radius(radius),
            unit,
            descending: Some(false),
            count: None,
            any: false,
            with_distance: false,
            with_hash: false,
            with_coordinates: false,
        }
    }
}

/// One `GEOSEARCH` match. Fields are `Some` when the matching `WITH*` flag was set.
#[derive(Debug, Clone, PartialEq)]
pub struct GeoMatch {
    /// Member name.
    pub member: String,
    /// Distance in the request unit.
    pub distance: Option<f64>,
    /// 52-bit geohash score.
    pub hash: Option<i64>,
    /// Longitude and latitude.
    pub coordinates: Option<(f64, f64)>,
}

/// `LPOS` options.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ListPositionOptions {
    /// `RANK`: which match to start from; negative searches from the tail.
    pub rank: Option<i64>,
    /// `MAXLEN`: compare at most this many elements.
    pub max_len: Option<u64>,
}

/// The answer of [`crate::Client::rate_limit`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RateLimit {
    /// True when this call fit in the window.
    pub allowed: bool,
    /// Calls left in the window after this one (0 when refused).
    pub remaining: i64,
}
