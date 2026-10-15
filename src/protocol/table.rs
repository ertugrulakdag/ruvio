//! Compile-time command table. One row per command: dispatch, arity, flags and
//! key positions live here. Rows built with `CommandSpec::migrated` parse [`Args`] straight
//! from the input frame; rows built with `legacy!` still hand their `parse/commands.rs`
//! function an argument vector (`Args::rest`, one allocation) until they move over.

use super::parse::commands;
use super::*;

/// Command flags, as in `COMMAND INFO`.
pub(crate) mod flags {
    pub(crate) const WRITE: u32 = 1 << 0;
    pub(crate) const READONLY: u32 = 1 << 1;
    pub(crate) const FAST: u32 = 1 << 2;
    pub(crate) const DENYOOM: u32 = 1 << 3;
    pub(crate) const BLOCKING: u32 = 1 << 4;
}

/// Arguments after the command name. Either spans into the input frame (no allocation) or
/// an already-built argument vector (scripts, `read_command`, legacy callers).
pub(crate) struct Args<'a, 's> {
    source: ArgSource<'a, 's>,
}

enum ArgSource<'a, 's> {
    Frame {
        frame: &'a [u8],
        spans: &'s [(u32, u32)],
    },
    Slice(&'s [Bytes<'a>]),
}

impl<'a, 's> Args<'a, 's> {
    pub(crate) fn from_frame(frame: &'a [u8], spans: &'s [(u32, u32)]) -> Self {
        Self {
            source: ArgSource::Frame { frame, spans },
        }
    }

    pub(crate) fn from_slice(arguments: &'s [Bytes<'a>]) -> Self {
        Self {
            source: ArgSource::Slice(arguments),
        }
    }

    pub(crate) fn len(&self) -> usize {
        match &self.source {
            ArgSource::Frame { spans, .. } => spans.len(),
            ArgSource::Slice(arguments) => arguments.len(),
        }
    }

    /// Argument `index` for inspection or number parsing.
    pub(crate) fn bytes(&self, index: usize) -> &[u8] {
        match &self.source {
            ArgSource::Frame { frame, spans } => {
                let (start, end) = spans[index];

                &frame[start as usize..end as usize]
            }
            ArgSource::Slice(arguments) => &arguments[index],
        }
    }

    /// Argument `index` as a value a `Command` can keep. Frame arguments stay borrowed.
    pub(crate) fn arg(&self, index: usize) -> Bytes<'a> {
        match &self.source {
            ArgSource::Frame { frame, spans } => {
                let (start, end) = spans[index];

                Cow::Borrowed(&frame[start as usize..end as usize])
            }
            ArgSource::Slice(arguments) => arguments[index].clone(),
        }
    }

    /// Arguments `from..` as owned-or-borrowed values; one `Vec` for commands that keep a list.
    pub(crate) fn rest(&self, from: usize) -> Vec<Bytes<'a>> {
        (from..self.len()).map(|index| self.arg(index)).collect()
    }
}

pub(crate) type ParseFn = for<'a, 's> fn(&Args<'a, 's>) -> Result<Command<'a>, ProtocolError>;

pub(crate) struct CommandSpec {
    /// Lowercase ASCII, at most `MAX_NAME` bytes.
    pub(crate) name: &'static str,
    /// Redis arity: exact count including the name when positive, minimum when negative.
    pub(crate) arity: i8,
    pub(crate) flags: u32,
    pub(crate) first_key: i8,
    pub(crate) last_key: i8,
    pub(crate) step: i8,
    pub(crate) categories: &'static [&'static str],
    /// Every row has one today; see the module note for the two parser styles.
    pub(crate) parse: Option<ParseFn>,
}

impl CommandSpec {
    /// Flag names as `COMMAND INFO` reports them.
    pub(crate) fn flag_names(&self) -> Vec<&'static str> {
        [
            (WRITE, "write"),
            (READONLY, "readonly"),
            (FAST, "fast"),
            (DENYOOM, "denyoom"),
            (BLOCKING, "blocking"),
        ]
        .into_iter()
        .filter(|(flag, _)| self.flags & flag != 0)
        .map(|(_, name)| name)
        .collect()
    }

    const fn migrated(
        name: &'static str,
        arity: i8,
        flags: u32,
        keys: (i8, i8, i8),
        categories: &'static [&'static str],
        parse: ParseFn,
    ) -> Self {
        Self {
            name,
            arity,
            flags,
            first_key: keys.0,
            last_key: keys.1,
            step: keys.2,
            categories,
            parse: Some(parse),
        }
    }
}

/// A row for a command whose parser still takes the argument vector: `$parse` is the
/// per-command function in `parse/commands.rs`, `$name` the name it is called with.
macro_rules! legacy {
    ($parse:path, $name:literal) => {{
        fn parse<'a>(args: &Args<'a, '_>) -> Result<Command<'a>, ProtocolError> {
            $parse($name, args.rest(0))
        }

        parse
    }};
}

pub(crate) fn commands() -> &'static [CommandSpec] {
    COMMANDS
}

pub(crate) const MAX_NAME: usize = 32;

use flags::{BLOCKING, DENYOOM, FAST, READONLY, WRITE};

pub(crate) static COMMANDS: &[CommandSpec] = &[
    CommandSpec::migrated(
        "get",
        2,
        READONLY | FAST,
        (1, 1, 1),
        &["read", "string", "fast"],
        parse_get,
    ),
    CommandSpec::migrated(
        "set",
        -3,
        WRITE | DENYOOM,
        (1, 1, 1),
        &["write", "string", "slow"],
        parse_set,
    ),
    CommandSpec::migrated(
        "incr",
        2,
        WRITE | DENYOOM | FAST,
        (1, 1, 1),
        &["write", "string", "fast"],
        parse_incr,
    ),
    CommandSpec::migrated(
        "decr",
        2,
        WRITE | DENYOOM | FAST,
        (1, 1, 1),
        &["write", "string", "fast"],
        parse_decr,
    ),
    CommandSpec::migrated(
        "incrby",
        -3,
        WRITE | DENYOOM | FAST,
        (1, 1, 1),
        &["write", "string", "fast"],
        parse_incrby,
    ),
    CommandSpec::migrated(
        "decrby",
        3,
        WRITE | DENYOOM | FAST,
        (1, 1, 1),
        &["write", "string", "fast"],
        parse_decrby,
    ),
    CommandSpec::migrated(
        "mget",
        -2,
        READONLY | FAST,
        (1, -1, 1),
        &["read", "string", "fast"],
        parse_mget,
    ),
    CommandSpec::migrated(
        "getset",
        3,
        WRITE | DENYOOM | FAST,
        (1, 1, 1),
        &["write", "string", "slow"],
        parse_getset,
    ),
    CommandSpec::migrated(
        "append",
        3,
        WRITE | DENYOOM | FAST,
        (1, 1, 1),
        &["write", "string", "fast"],
        parse_append,
    ),
    CommandSpec::migrated(
        "setex",
        4,
        WRITE | DENYOOM,
        (1, 1, 1),
        &["write", "string", "slow"],
        parse_setex,
    ),
    CommandSpec::migrated(
        "psetex",
        4,
        WRITE | DENYOOM,
        (1, 1, 1),
        &["write", "string", "slow"],
        parse_psetex,
    ),
    CommandSpec::migrated(
        "setnx",
        3,
        WRITE | DENYOOM | FAST,
        (1, 1, 1),
        &["write", "string", "fast"],
        parse_setnx,
    ),
    CommandSpec::migrated(
        "getdel",
        2,
        WRITE | FAST,
        (1, 1, 1),
        &["write", "string", "fast"],
        parse_getdel,
    ),
    CommandSpec::migrated(
        "msetnx",
        -3,
        WRITE | DENYOOM,
        (1, -1, 2),
        &["write", "string", "slow"],
        parse_msetnx,
    ),
    CommandSpec::migrated(
        "incrbyfloat",
        3,
        WRITE | DENYOOM | FAST,
        (1, 1, 1),
        &["write", "string", "fast"],
        parse_incrbyfloat,
    ),
    CommandSpec::migrated(
        "getrange",
        4,
        READONLY,
        (1, 1, 1),
        &["read", "string", "slow"],
        parse_getrange,
    ),
    CommandSpec::migrated(
        "substr",
        4,
        READONLY,
        (1, 1, 1),
        &["read", "string", "slow"],
        parse_getrange,
    ),
    CommandSpec::migrated(
        "setrange",
        4,
        WRITE | DENYOOM,
        (1, 1, 1),
        &["write", "string", "slow"],
        parse_setrange,
    ),
    CommandSpec::migrated(
        "smismember",
        -3,
        READONLY | FAST,
        (1, 1, 1),
        &["read", "set", "fast"],
        parse_smismember,
    ),
    CommandSpec::migrated(
        "sintercard",
        -3,
        READONLY,
        (2, -1, 1),
        &["read", "set", "slow"],
        parse_sintercard,
    ),
    CommandSpec::migrated(
        "zcount",
        4,
        READONLY | FAST,
        (1, 1, 1),
        &["read", "sortedset", "fast"],
        parse_zcount,
    ),
    CommandSpec::migrated(
        "zlexcount",
        4,
        READONLY | FAST,
        (1, 1, 1),
        &["read", "sortedset", "fast"],
        parse_zlexcount,
    ),
    CommandSpec::migrated(
        "zmscore",
        -3,
        READONLY | FAST,
        (1, 1, 1),
        &["read", "sortedset", "fast"],
        parse_zmscore,
    ),
    CommandSpec::migrated(
        "zunionstore",
        -4,
        WRITE | DENYOOM,
        (1, 1, 1),
        &["write", "sortedset", "slow"],
        parse_zunionstore,
    ),
    CommandSpec::migrated(
        "zinterstore",
        -4,
        WRITE | DENYOOM,
        (1, 1, 1),
        &["write", "sortedset", "slow"],
        parse_zinterstore,
    ),
    CommandSpec::migrated(
        "zdiffstore",
        -4,
        WRITE | DENYOOM,
        (1, 1, 1),
        &["write", "sortedset", "slow"],
        parse_zdiffstore,
    ),
    CommandSpec::migrated(
        "zunion",
        -3,
        READONLY,
        (2, -1, 1),
        &["read", "sortedset", "slow"],
        parse_zunion,
    ),
    CommandSpec::migrated(
        "zinter",
        -3,
        READONLY,
        (2, -1, 1),
        &["read", "sortedset", "slow"],
        parse_zinter,
    ),
    CommandSpec::migrated(
        "zdiff",
        -3,
        READONLY,
        (2, -1, 1),
        &["read", "sortedset", "slow"],
        parse_zdiff,
    ),
    CommandSpec::migrated(
        "zrandmember",
        -2,
        READONLY,
        (1, 1, 1),
        &["read", "sortedset", "slow"],
        parse_zrandmember,
    ),
    CommandSpec::migrated(
        "hrandfield",
        -2,
        READONLY,
        (1, 1, 1),
        &["read", "hash", "slow"],
        parse_hrandfield,
    ),
    CommandSpec::migrated(
        "hincrbyfloat",
        4,
        WRITE | DENYOOM | FAST,
        (1, 1, 1),
        &["write", "hash", "fast"],
        parse_hincrbyfloat,
    ),
    CommandSpec::migrated(
        "linsert",
        -5,
        WRITE | DENYOOM,
        (1, 1, 1),
        &["write", "list", "slow"],
        parse_linsert,
    ),
    CommandSpec::migrated(
        "lset",
        4,
        WRITE | DENYOOM,
        (1, 1, 1),
        &["write", "list", "slow"],
        parse_lset,
    ),
    CommandSpec::migrated(
        "lrem",
        4,
        WRITE,
        (1, 1, 1),
        &["write", "list", "slow"],
        parse_lrem,
    ),
    CommandSpec::migrated(
        "lpos",
        -3,
        READONLY,
        (1, 1, 1),
        &["read", "list", "slow"],
        parse_lpos,
    ),
    CommandSpec::migrated(
        "lmove",
        5,
        WRITE | DENYOOM,
        (1, 2, 1),
        &["write", "list", "slow"],
        parse_lmove,
    ),
    CommandSpec::migrated(
        "blmove",
        6,
        WRITE | DENYOOM | BLOCKING,
        (1, 2, 1),
        &["write", "list", "slow", "blocking"],
        parse_blmove,
    ),
    CommandSpec::migrated(
        "rpoplpush",
        3,
        WRITE | DENYOOM,
        (1, 2, 1),
        &["write", "list", "slow"],
        parse_rpoplpush,
    ),
    CommandSpec::migrated(
        "lpushx",
        -3,
        WRITE | DENYOOM | FAST,
        (1, 1, 1),
        &["write", "list", "fast"],
        parse_lpushx,
    ),
    CommandSpec::migrated(
        "rpushx",
        -3,
        WRITE | DENYOOM | FAST,
        (1, 1, 1),
        &["write", "list", "fast"],
        parse_rpushx,
    ),
    CommandSpec::migrated(
        "lmpop",
        -4,
        WRITE,
        (0, 0, 0),
        &["write", "list", "slow"],
        parse_lmpop,
    ),
    CommandSpec::migrated(
        "blmpop",
        -5,
        WRITE | BLOCKING,
        (0, 0, 0),
        &["write", "list", "slow", "blocking"],
        parse_blmpop,
    ),
    CommandSpec::migrated(
        "setbit",
        4,
        WRITE | DENYOOM,
        (1, 1, 1),
        &["write", "bitmap", "slow"],
        parse_setbit,
    ),
    CommandSpec::migrated(
        "bitfield",
        -2,
        WRITE | DENYOOM,
        (1, 1, 1),
        &["write", "bitmap", "slow"],
        parse_bitfield,
    ),
    CommandSpec::migrated(
        "bitfield_ro",
        -2,
        READONLY | FAST,
        (1, 1, 1),
        &["read", "bitmap", "fast"],
        parse_bitfield_ro,
    ),
    CommandSpec::migrated(
        "getbit",
        3,
        READONLY | FAST,
        (1, 1, 1),
        &["read", "bitmap", "fast"],
        parse_getbit,
    ),
    CommandSpec::migrated(
        "bitcount",
        -2,
        READONLY,
        (1, 1, 1),
        &["read", "bitmap", "slow"],
        parse_bitcount,
    ),
    CommandSpec::migrated(
        "bitpos",
        -3,
        READONLY,
        (1, 1, 1),
        &["read", "bitmap", "slow"],
        parse_bitpos,
    ),
    CommandSpec::migrated(
        "bitop",
        -4,
        WRITE | DENYOOM,
        (2, -1, 1),
        &["write", "bitmap", "slow"],
        parse_bitop,
    ),
    CommandSpec::migrated(
        "pfadd",
        -2,
        WRITE | DENYOOM | FAST,
        (1, 1, 1),
        &["write", "hyperloglog", "fast"],
        parse_pfadd,
    ),
    CommandSpec::migrated(
        "pfcount",
        -2,
        READONLY,
        (1, -1, 1),
        &["read", "hyperloglog", "slow"],
        parse_pfcount,
    ),
    CommandSpec::migrated(
        "pfmerge",
        -2,
        WRITE | DENYOOM,
        (1, -1, 1),
        &["write", "hyperloglog", "slow"],
        parse_pfmerge,
    ),
    CommandSpec::migrated(
        "fillget",
        3,
        WRITE | FAST | BLOCKING,
        (1, 1, 1),
        &["write", "string", "fast"],
        parse_fillget,
    ),
    CommandSpec::migrated(
        "fillset",
        -5,
        WRITE | DENYOOM,
        (1, 1, 1),
        &["write", "string", "slow"],
        parse_fillset,
    ),
    CommandSpec::migrated(
        "strlen",
        2,
        READONLY | FAST,
        (1, 1, 1),
        &["read", "string", "fast"],
        parse_strlen,
    ),
    CommandSpec::migrated(
        "exists",
        -2,
        READONLY | FAST,
        (1, -1, 1),
        &["read", "keyspace", "fast"],
        parse_exists,
    ),
    CommandSpec::migrated(
        "keys",
        2,
        READONLY,
        (0, 0, 0),
        &["keyspace", "read", "slow", "dangerous"],
        parse_keys,
    ),
    CommandSpec::migrated(
        "randomkey",
        1,
        READONLY,
        (0, 0, 0),
        &["keyspace", "read", "slow"],
        parse_randomkey,
    ),
    CommandSpec::migrated(
        "touch",
        -2,
        READONLY | FAST,
        (1, -1, 1),
        &["keyspace", "read", "fast"],
        parse_touch,
    ),
    CommandSpec::migrated(
        "renamenx",
        3,
        WRITE | FAST,
        (1, 2, 1),
        &["keyspace", "write", "fast"],
        parse_renamenx,
    ),
    CommandSpec::migrated(
        "expiretime",
        2,
        READONLY | FAST,
        (1, 1, 1),
        &["keyspace", "read", "fast"],
        parse_expiretime,
    ),
    CommandSpec::migrated(
        "pexpiretime",
        2,
        READONLY | FAST,
        (1, 1, 1),
        &["keyspace", "read", "fast"],
        parse_pexpiretime,
    ),
    CommandSpec::migrated(
        "copy",
        -3,
        WRITE | DENYOOM,
        (1, 2, 1),
        &["keyspace", "write", "slow"],
        parse_copy,
    ),
    CommandSpec::migrated(
        "echo",
        2,
        FAST,
        (0, 0, 0),
        &["connection", "fast"],
        parse_echo,
    ),
    CommandSpec::migrated(
        "object",
        -2,
        READONLY,
        (2, 2, 1),
        &["keyspace", "read", "slow"],
        parse_object,
    ),
    CommandSpec::migrated(
        "memory",
        -2,
        READONLY,
        (2, 2, 1),
        &["read", "slow"],
        parse_memory,
    ),
    CommandSpec::migrated(
        "slowlog",
        -2,
        0,
        (0, 0, 0),
        &["admin", "slow", "dangerous"],
        parse_slowlog,
    ),
    CommandSpec::migrated(
        "latency",
        -2,
        0,
        (0, 0, 0),
        &["admin", "slow", "dangerous"],
        parse_latency,
    ),
    CommandSpec::migrated(
        "dump",
        2,
        READONLY,
        (1, 1, 1),
        &["keyspace", "read", "slow"],
        parse_dump,
    ),
    CommandSpec::migrated(
        "restore",
        -4,
        WRITE | DENYOOM,
        (1, 1, 1),
        &["keyspace", "write", "slow", "dangerous"],
        parse_restore,
    ),
    CommandSpec::migrated(
        "ping",
        -1,
        FAST,
        (0, 0, 0),
        &["connection", "fast"],
        direct::cmd_ping,
    ),
    CommandSpec::migrated(
        "wait",
        3,
        0,
        (0, 0, 0),
        &["connection", "slow"],
        legacy!(commands::cmd_wait, b"WAIT"),
    ),
    CommandSpec::migrated(
        "ruvio",
        -1,
        FAST,
        (0, 0, 0),
        &["connection", "fast"],
        legacy!(commands::cmd_ruvio, b"RUVIO"),
    ),
    CommandSpec::migrated(
        "cluster",
        -2,
        0,
        (0, 0, 0),
        &["slow"],
        legacy!(commands::cmd_cluster, b"CLUSTER"),
    ),
    CommandSpec::migrated(
        "readonly",
        1,
        FAST,
        (0, 0, 0),
        &["connection", "fast"],
        legacy!(commands::cmd_readonly, b"READONLY"),
    ),
    CommandSpec::migrated(
        "readwrite",
        1,
        FAST,
        (0, 0, 0),
        &["connection", "fast"],
        legacy!(commands::cmd_readonly, b"READWRITE"),
    ),
    CommandSpec::migrated(
        "config",
        -2,
        0,
        (0, 0, 0),
        &["admin", "slow", "dangerous"],
        legacy!(commands::cmd_config, b"CONFIG"),
    ),
    CommandSpec::migrated(
        "info",
        -1,
        0,
        (0, 0, 0),
        &["admin", "slow", "dangerous"],
        legacy!(commands::cmd_info, b"INFO"),
    ),
    CommandSpec::migrated(
        "save",
        1,
        0,
        (0, 0, 0),
        &["admin", "slow", "dangerous"],
        legacy!(commands::cmd_save, b"SAVE"),
    ),
    CommandSpec::migrated(
        "bgsave",
        -1,
        0,
        (0, 0, 0),
        &["admin", "slow", "dangerous"],
        legacy!(commands::cmd_bgsave, b"BGSAVE"),
    ),
    CommandSpec::migrated(
        "json.get",
        -2,
        READONLY,
        (1, 1, 1),
        &["read", "slow"],
        legacy!(commands::cmd_json_get, b"JSON.GET"),
    ),
    CommandSpec::migrated(
        "json.mget",
        -3,
        READONLY,
        (1, -2, 1),
        &["read", "slow"],
        legacy!(commands::cmd_json_mget, b"JSON.MGET"),
    ),
    CommandSpec::migrated(
        "json.del",
        -2,
        WRITE,
        (1, 1, 1),
        &["write", "slow"],
        legacy!(commands::cmd_json_get, b"JSON.DEL"),
    ),
    CommandSpec::migrated(
        "json.forget",
        -2,
        WRITE,
        (1, 1, 1),
        &["write", "slow"],
        legacy!(commands::cmd_json_get, b"JSON.FORGET"),
    ),
    CommandSpec::migrated(
        "json.set",
        4,
        WRITE | DENYOOM,
        (1, 1, 1),
        &["write", "slow"],
        legacy!(commands::cmd_json_set, b"JSON.SET"),
    ),
    CommandSpec::migrated(
        "json.merge",
        4,
        WRITE | DENYOOM,
        (1, 1, 1),
        &["write", "slow"],
        legacy!(commands::cmd_json_merge, b"JSON.MERGE"),
    ),
    CommandSpec::migrated(
        "json.index",
        -2,
        WRITE,
        (0, 0, 0),
        &["write", "slow"],
        legacy!(commands::cmd_json_index, b"JSON.INDEX"),
    ),
    CommandSpec::migrated(
        "json.find",
        -4,
        READONLY,
        (0, 0, 0),
        &["read", "slow"],
        legacy!(commands::cmd_json_index, b"JSON.FIND"),
    ),
    CommandSpec::migrated(
        "json.type",
        -2,
        READONLY,
        (1, 1, 1),
        &["read", "slow"],
        legacy!(commands::cmd_json_op, b"JSON.TYPE"),
    ),
    CommandSpec::migrated(
        "json.strlen",
        -2,
        READONLY,
        (1, 1, 1),
        &["read", "slow"],
        legacy!(commands::cmd_json_op, b"JSON.STRLEN"),
    ),
    CommandSpec::migrated(
        "json.arrlen",
        -2,
        READONLY,
        (1, 1, 1),
        &["read", "slow"],
        legacy!(commands::cmd_json_op, b"JSON.ARRLEN"),
    ),
    CommandSpec::migrated(
        "json.objkeys",
        -2,
        READONLY,
        (1, 1, 1),
        &["read", "slow"],
        legacy!(commands::cmd_json_op, b"JSON.OBJKEYS"),
    ),
    CommandSpec::migrated(
        "json.numincrby",
        4,
        WRITE | DENYOOM,
        (1, 1, 1),
        &["write", "slow"],
        legacy!(commands::cmd_json_op, b"JSON.NUMINCRBY"),
    ),
    CommandSpec::migrated(
        "json.toggle",
        -2,
        WRITE | DENYOOM,
        (1, 1, 1),
        &["write", "slow"],
        legacy!(commands::cmd_json_op, b"JSON.TOGGLE"),
    ),
    CommandSpec::migrated(
        "json.arrappend",
        -4,
        WRITE | DENYOOM,
        (1, 1, 1),
        &["write", "slow"],
        legacy!(commands::cmd_json_op, b"JSON.ARRAPPEND"),
    ),
    CommandSpec::migrated(
        "json.clear",
        -2,
        WRITE,
        (1, 1, 1),
        &["write", "slow"],
        legacy!(commands::cmd_json_op, b"JSON.CLEAR"),
    ),
    CommandSpec::migrated(
        "json.objlen",
        -2,
        READONLY,
        (1, 1, 1),
        &["read", "slow"],
        legacy!(commands::cmd_json_op, b"JSON.OBJLEN"),
    ),
    CommandSpec::migrated(
        "json.strappend",
        4,
        WRITE | DENYOOM,
        (1, 1, 1),
        &["write", "slow"],
        legacy!(commands::cmd_json_op, b"JSON.STRAPPEND"),
    ),
    CommandSpec::migrated(
        "json.nummultby",
        4,
        WRITE | DENYOOM,
        (1, 1, 1),
        &["write", "slow"],
        legacy!(commands::cmd_json_op, b"JSON.NUMMULTBY"),
    ),
    CommandSpec::migrated(
        "json.arrinsert",
        -5,
        WRITE | DENYOOM,
        (1, 1, 1),
        &["write", "slow"],
        legacy!(commands::cmd_json_op, b"JSON.ARRINSERT"),
    ),
    CommandSpec::migrated(
        "json.arrpop",
        -2,
        WRITE,
        (1, 1, 1),
        &["write", "slow"],
        legacy!(commands::cmd_json_op, b"JSON.ARRPOP"),
    ),
    CommandSpec::migrated(
        "json.arrtrim",
        5,
        WRITE,
        (1, 1, 1),
        &["write", "slow"],
        legacy!(commands::cmd_json_op, b"JSON.ARRTRIM"),
    ),
    CommandSpec::migrated(
        "json.arrindex",
        -4,
        READONLY,
        (1, 1, 1),
        &["read", "slow"],
        legacy!(commands::cmd_json_op, b"JSON.ARRINDEX"),
    ),
    CommandSpec::migrated(
        "jmerge",
        4,
        WRITE | DENYOOM,
        (1, 1, 1),
        &["write", "slow"],
        legacy!(commands::cmd_json_merge, b"JMERGE"),
    ),
    CommandSpec::migrated(
        "lease",
        -3,
        WRITE | DENYOOM | FAST,
        (1, 1, 1),
        &["keyspace", "write", "fast"],
        legacy!(commands::cmd_lease, b"LEASE"),
    ),
    CommandSpec::migrated(
        "semaphore",
        -3,
        WRITE | DENYOOM | FAST,
        (1, 1, 1),
        &["keyspace", "write", "fast"],
        legacy!(commands::cmd_semaphore, b"SEMAPHORE"),
    ),
    CommandSpec::migrated(
        "limit",
        -4,
        WRITE | DENYOOM | FAST,
        (1, 1, 1),
        &["keyspace", "write", "fast"],
        legacy!(commands::cmd_limit, b"LIMIT"),
    ),
    CommandSpec::migrated(
        "idem",
        -4,
        WRITE | DENYOOM,
        (1, 1, 1),
        &["keyspace", "write"],
        legacy!(commands::cmd_idem, b"IDEM"),
    ),
    CommandSpec::migrated(
        "once",
        -3,
        WRITE | DENYOOM | FAST,
        (1, 1, 1),
        &["keyspace", "write", "fast"],
        legacy!(commands::cmd_once, b"ONCE"),
    ),
    CommandSpec::migrated(
        "take",
        -2,
        WRITE | FAST,
        (1, 1, 1),
        &["string", "write", "fast"],
        legacy!(commands::cmd_take, b"TAKE"),
    ),
    CommandSpec::migrated(
        "getex",
        -2,
        WRITE | DENYOOM | FAST,
        (1, 1, 1),
        &["string", "write", "fast"],
        legacy!(commands::cmd_getex, b"GETEX"),
    ),
    CommandSpec::migrated(
        "release",
        -3,
        WRITE | FAST,
        (1, 1, 1),
        &["keyspace", "write", "fast"],
        legacy!(commands::cmd_release, b"RELEASE"),
    ),
    CommandSpec::migrated(
        "setv",
        -3,
        WRITE | DENYOOM | FAST,
        (1, 1, 1),
        &["string", "write", "fast"],
        legacy!(commands::cmd_setv, b"SETV"),
    ),
    CommandSpec::migrated(
        "changes",
        -2,
        READONLY,
        (0, 0, 0),
        &["keyspace", "read", "slow"],
        legacy!(commands::cmd_changes, b"CHANGES"),
    ),
    CommandSpec::migrated(
        "mset",
        -3,
        WRITE | DENYOOM,
        (1, -1, 2),
        &["write", "string", "slow"],
        direct::cmd_mset,
    ),
    CommandSpec::migrated(
        "type",
        2,
        READONLY | FAST,
        (1, 1, 1),
        &["keyspace", "read", "fast"],
        direct::cmd_type,
    ),
    CommandSpec::migrated(
        "unlink",
        -2,
        WRITE,
        (1, -1, 1),
        &["keyspace", "write", "slow"],
        direct::cmd_unlink,
    ),
    CommandSpec::migrated(
        "rename",
        3,
        WRITE | FAST,
        (1, 2, 1),
        &["keyspace", "write", "fast"],
        direct::cmd_rename,
    ),
    CommandSpec::migrated(
        "scan",
        -2,
        READONLY,
        (0, 0, 0),
        &["keyspace", "read", "slow"],
        legacy!(commands::cmd_scan, b"SCAN"),
    ),
    CommandSpec::migrated(
        "dbsize",
        1,
        READONLY | FAST,
        (0, 0, 0),
        &["keyspace", "read", "fast"],
        direct::cmd_dbsize,
    ),
    CommandSpec::migrated(
        "select",
        2,
        FAST,
        (0, 0, 0),
        &["connection", "fast"],
        direct::cmd_select,
    ),
    CommandSpec::migrated(
        "flushdb",
        -1,
        WRITE,
        (0, 0, 0),
        &["keyspace", "write", "slow", "dangerous"],
        legacy!(commands::cmd_flushdb, b"FLUSHDB"),
    ),
    CommandSpec::migrated(
        "flushall",
        -1,
        WRITE,
        (0, 0, 0),
        &["keyspace", "write", "slow", "dangerous"],
        legacy!(commands::cmd_flushdb, b"FLUSHALL"),
    ),
    CommandSpec::migrated(
        "swapdb",
        3,
        WRITE | FAST,
        (0, 0, 0),
        &["keyspace", "write", "fast", "dangerous"],
        legacy!(commands::cmd_swapdb, b"SWAPDB"),
    ),
    CommandSpec::migrated(
        "move",
        3,
        WRITE | FAST,
        (1, 1, 1),
        &["keyspace", "write", "fast"],
        legacy!(commands::cmd_move, b"MOVE"),
    ),
    CommandSpec::migrated(
        "del",
        -2,
        WRITE,
        (1, -1, 1),
        &["keyspace", "write", "slow"],
        direct::cmd_del,
    ),
    CommandSpec::migrated(
        "publish",
        3,
        FAST,
        (0, 0, 0),
        &["pubsub", "fast"],
        direct::cmd_publish,
    ),
    CommandSpec::migrated(
        "subscribe",
        -2,
        0,
        (0, 0, 0),
        &["pubsub", "slow"],
        legacy!(commands::cmd_subscribe, b"SUBSCRIBE"),
    ),
    CommandSpec::migrated(
        "unsubscribe",
        -1,
        0,
        (0, 0, 0),
        &["pubsub", "slow"],
        legacy!(commands::cmd_unsubscribe, b"UNSUBSCRIBE"),
    ),
    CommandSpec::migrated(
        "psubscribe",
        -2,
        0,
        (0, 0, 0),
        &["pubsub", "slow"],
        legacy!(commands::cmd_psubscribe, b"PSUBSCRIBE"),
    ),
    CommandSpec::migrated(
        "punsubscribe",
        -1,
        0,
        (0, 0, 0),
        &["pubsub", "slow"],
        legacy!(commands::cmd_punsubscribe, b"PUNSUBSCRIBE"),
    ),
    CommandSpec::migrated(
        "spublish",
        3,
        FAST,
        (1, 1, 1),
        &["pubsub", "fast"],
        legacy!(commands::cmd_spublish, b"SPUBLISH"),
    ),
    CommandSpec::migrated(
        "ssubscribe",
        -2,
        0,
        (1, -1, 1),
        &["pubsub", "slow"],
        legacy!(commands::cmd_ssubscribe, b"SSUBSCRIBE"),
    ),
    CommandSpec::migrated(
        "sunsubscribe",
        -1,
        0,
        (1, -1, 1),
        &["pubsub", "slow"],
        legacy!(commands::cmd_sunsubscribe, b"SUNSUBSCRIBE"),
    ),
    CommandSpec::migrated(
        "hello",
        -1,
        FAST,
        (0, 0, 0),
        &["connection", "fast"],
        legacy!(commands::cmd_hello, b"HELLO"),
    ),
    CommandSpec::migrated(
        "client",
        -2,
        0,
        (0, 0, 0),
        &["connection", "slow"],
        legacy!(commands::cmd_client, b"CLIENT"),
    ),
    CommandSpec::migrated(
        "auth",
        -2,
        FAST,
        (0, 0, 0),
        &["connection", "fast"],
        legacy!(commands::cmd_auth, b"AUTH"),
    ),
    CommandSpec::migrated(
        "acl",
        -2,
        0,
        (0, 0, 0),
        &["admin", "slow", "dangerous"],
        legacy!(commands::cmd_acl, b"ACL"),
    ),
    CommandSpec::migrated(
        "expire",
        -3,
        WRITE | DENYOOM | FAST,
        (1, 1, 1),
        &["keyspace", "write", "fast"],
        direct::cmd_expire,
    ),
    CommandSpec::migrated(
        "pexpire",
        -3,
        WRITE | DENYOOM | FAST,
        (1, 1, 1),
        &["keyspace", "write", "fast"],
        direct::cmd_pexpire,
    ),
    CommandSpec::migrated(
        "expireat",
        -3,
        WRITE | DENYOOM | FAST,
        (1, 1, 1),
        &["keyspace", "write", "fast"],
        direct::cmd_expireat,
    ),
    CommandSpec::migrated(
        "pexpireat",
        -3,
        WRITE | DENYOOM | FAST,
        (1, 1, 1),
        &["keyspace", "write", "fast"],
        direct::cmd_pexpireat,
    ),
    CommandSpec::migrated(
        "ttl",
        2,
        READONLY | FAST,
        (1, 1, 1),
        &["keyspace", "read", "fast"],
        direct::cmd_ttl,
    ),
    CommandSpec::migrated(
        "pttl",
        2,
        READONLY | FAST,
        (1, 1, 1),
        &["keyspace", "read", "fast"],
        direct::cmd_pttl,
    ),
    CommandSpec::migrated(
        "persist",
        2,
        WRITE | FAST,
        (1, 1, 1),
        &["keyspace", "write", "fast"],
        direct::cmd_persist,
    ),
    CommandSpec::migrated(
        "lpushcap",
        -4,
        WRITE | DENYOOM | FAST,
        (1, 1, 1),
        &["write", "list", "fast"],
        parse_lpushcap,
    ),
    CommandSpec::migrated(
        "rpushcap",
        -4,
        WRITE | DENYOOM | FAST,
        (1, 1, 1),
        &["write", "list", "fast"],
        parse_rpushcap,
    ),
    CommandSpec::migrated(
        "delex",
        4,
        WRITE | FAST,
        (1, 1, 1),
        &["string", "write", "fast"],
        parse_delex,
    ),
    CommandSpec::migrated(
        "lpush",
        -3,
        WRITE | DENYOOM | FAST,
        (1, 1, 1),
        &["write", "list", "fast"],
        parse_lpush,
    ),
    CommandSpec::migrated(
        "lpop",
        -2,
        WRITE | FAST,
        (1, 1, 1),
        &["write", "list", "fast"],
        direct::cmd_lpop,
    ),
    CommandSpec::migrated(
        "lrange",
        4,
        READONLY,
        (1, 1, 1),
        &["read", "list", "slow"],
        direct::cmd_lrange,
    ),
    CommandSpec::migrated(
        "rpush",
        -3,
        WRITE | DENYOOM | FAST,
        (1, 1, 1),
        &["write", "list", "fast"],
        parse_rpush,
    ),
    CommandSpec::migrated(
        "rpop",
        -2,
        WRITE | FAST,
        (1, 1, 1),
        &["write", "list", "fast"],
        direct::cmd_rpop,
    ),
    CommandSpec::migrated(
        "llen",
        2,
        READONLY | FAST,
        (1, 1, 1),
        &["read", "list", "fast"],
        direct::cmd_llen,
    ),
    CommandSpec::migrated(
        "lindex",
        3,
        READONLY,
        (1, 1, 1),
        &["read", "list", "slow"],
        direct::cmd_lindex,
    ),
    CommandSpec::migrated(
        "ltrim",
        4,
        WRITE,
        (1, 1, 1),
        &["write", "list", "slow"],
        direct::cmd_ltrim,
    ),
    CommandSpec::migrated(
        "blpop",
        -3,
        WRITE | BLOCKING,
        (1, -2, 1),
        &["write", "list", "slow", "blocking"],
        legacy!(commands::cmd_blpop, b"BLPOP"),
    ),
    CommandSpec::migrated(
        "brpop",
        -3,
        WRITE | BLOCKING,
        (1, -2, 1),
        &["write", "list", "slow", "blocking"],
        legacy!(commands::cmd_blpop, b"BRPOP"),
    ),
    CommandSpec::migrated(
        "sadd",
        -3,
        WRITE | DENYOOM | FAST,
        (1, 1, 1),
        &["write", "set", "fast"],
        direct::cmd_sadd,
    ),
    CommandSpec::migrated(
        "sismember",
        3,
        READONLY | FAST,
        (1, 1, 1),
        &["read", "set", "fast"],
        direct::cmd_sismember,
    ),
    CommandSpec::migrated(
        "srem",
        -3,
        WRITE | FAST,
        (1, 1, 1),
        &["write", "set", "fast"],
        direct::cmd_srem,
    ),
    CommandSpec::migrated(
        "scard",
        2,
        READONLY | FAST,
        (1, 1, 1),
        &["read", "set", "fast"],
        direct::cmd_scard,
    ),
    CommandSpec::migrated(
        "smembers",
        2,
        READONLY,
        (1, 1, 1),
        &["read", "set", "slow"],
        direct::cmd_smembers,
    ),
    CommandSpec::migrated(
        "sinter",
        -2,
        READONLY,
        (1, -1, 1),
        &["read", "set", "slow"],
        legacy!(commands::cmd_sinter, b"SINTER"),
    ),
    CommandSpec::migrated(
        "sunion",
        -2,
        READONLY,
        (1, -1, 1),
        &["read", "set", "slow"],
        legacy!(commands::cmd_sunion, b"SUNION"),
    ),
    CommandSpec::migrated(
        "sdiff",
        -2,
        READONLY,
        (1, -1, 1),
        &["read", "set", "slow"],
        legacy!(commands::cmd_sdiff, b"SDIFF"),
    ),
    CommandSpec::migrated(
        "sinterstore",
        -3,
        WRITE | DENYOOM,
        (1, -1, 1),
        &["write", "set", "slow"],
        legacy!(commands::cmd_sinterstore, b"SINTERSTORE"),
    ),
    CommandSpec::migrated(
        "sunionstore",
        -3,
        WRITE | DENYOOM,
        (1, -1, 1),
        &["write", "set", "slow"],
        legacy!(commands::cmd_sunionstore, b"SUNIONSTORE"),
    ),
    CommandSpec::migrated(
        "sdiffstore",
        -3,
        WRITE | DENYOOM,
        (1, -1, 1),
        &["write", "set", "slow"],
        legacy!(commands::cmd_sdiffstore, b"SDIFFSTORE"),
    ),
    CommandSpec::migrated(
        "spop",
        -2,
        WRITE | FAST,
        (1, 1, 1),
        &["write", "set", "fast"],
        direct::cmd_spop,
    ),
    CommandSpec::migrated(
        "srandmember",
        -2,
        READONLY,
        (1, 1, 1),
        &["read", "set", "slow"],
        direct::cmd_srandmember,
    ),
    CommandSpec::migrated(
        "smove",
        4,
        WRITE | DENYOOM | FAST,
        (1, 2, 1),
        &["write", "set", "fast"],
        legacy!(commands::cmd_smove, b"SMOVE"),
    ),
    CommandSpec::migrated(
        "hincrby",
        4,
        WRITE | DENYOOM | FAST,
        (1, 1, 1),
        &["write", "hash", "fast"],
        direct::cmd_hincrby,
    ),
    CommandSpec::migrated(
        "hsetnx",
        4,
        WRITE | DENYOOM | FAST,
        (1, 1, 1),
        &["write", "hash", "fast"],
        direct::cmd_hsetnx,
    ),
    CommandSpec::migrated(
        "hscan",
        -3,
        READONLY,
        (1, 1, 1),
        &["read", "hash", "slow"],
        legacy!(commands::cmd_hscan, b"HSCAN"),
    ),
    CommandSpec::migrated(
        "sscan",
        -3,
        READONLY,
        (1, 1, 1),
        &["read", "set", "slow"],
        legacy!(commands::cmd_sscan, b"SSCAN"),
    ),
    CommandSpec::migrated(
        "zscan",
        -3,
        READONLY,
        (1, 1, 1),
        &["read", "sortedset", "slow"],
        legacy!(commands::cmd_zscan, b"ZSCAN"),
    ),
    CommandSpec::migrated(
        "hstrlen",
        3,
        READONLY | FAST,
        (1, 1, 1),
        &["read", "hash", "fast"],
        direct::cmd_hstrlen,
    ),
    CommandSpec::migrated(
        "hset",
        -4,
        WRITE | DENYOOM | FAST,
        (1, 1, 1),
        &["write", "hash", "fast"],
        parse_hset,
    ),
    CommandSpec::migrated(
        "hdel",
        -3,
        WRITE | FAST,
        (1, 1, 1),
        &["write", "hash", "fast"],
        direct::cmd_hdel,
    ),
    CommandSpec::migrated(
        "hget",
        3,
        READONLY | FAST,
        (1, 1, 1),
        &["read", "hash", "fast"],
        direct::cmd_hget,
    ),
    CommandSpec::migrated(
        "hmget",
        -3,
        READONLY | FAST,
        (1, 1, 1),
        &["read", "hash", "fast"],
        direct::cmd_hmget,
    ),
    CommandSpec::migrated(
        "hlen",
        2,
        READONLY | FAST,
        (1, 1, 1),
        &["read", "hash", "fast"],
        direct::cmd_hlen,
    ),
    CommandSpec::migrated(
        "hgetall",
        2,
        READONLY,
        (1, 1, 1),
        &["read", "hash", "slow"],
        direct::cmd_hgetall,
    ),
    CommandSpec::migrated(
        "hexists",
        3,
        READONLY | FAST,
        (1, 1, 1),
        &["read", "hash", "fast"],
        direct::cmd_hexists,
    ),
    CommandSpec::migrated(
        "hkeys",
        2,
        READONLY,
        (1, 1, 1),
        &["read", "hash", "slow"],
        direct::cmd_hkeys,
    ),
    CommandSpec::migrated(
        "hvals",
        2,
        READONLY,
        (1, 1, 1),
        &["read", "hash", "slow"],
        direct::cmd_hvals,
    ),
    CommandSpec::migrated(
        "hexpire",
        -6,
        WRITE | DENYOOM | FAST,
        (1, 1, 1),
        &["write", "hash", "fast"],
        legacy!(commands::cmd_hexpire, b"HEXPIRE"),
    ),
    CommandSpec::migrated(
        "hpexpire",
        -6,
        WRITE | DENYOOM | FAST,
        (1, 1, 1),
        &["write", "hash", "fast"],
        legacy!(commands::cmd_hexpire, b"HPEXPIRE"),
    ),
    CommandSpec::migrated(
        "hexpireat",
        -6,
        WRITE | DENYOOM | FAST,
        (1, 1, 1),
        &["write", "hash", "fast"],
        legacy!(commands::cmd_hexpire, b"HEXPIREAT"),
    ),
    CommandSpec::migrated(
        "hpexpireat",
        -6,
        WRITE | DENYOOM | FAST,
        (1, 1, 1),
        &["write", "hash", "fast"],
        legacy!(commands::cmd_hexpire, b"HPEXPIREAT"),
    ),
    CommandSpec::migrated(
        "httl",
        -5,
        READONLY | FAST,
        (1, 1, 1),
        &["read", "hash", "fast"],
        legacy!(commands::cmd_httl, b"HTTL"),
    ),
    CommandSpec::migrated(
        "hpttl",
        -5,
        READONLY | FAST,
        (1, 1, 1),
        &["read", "hash", "fast"],
        legacy!(commands::cmd_httl, b"HPTTL"),
    ),
    CommandSpec::migrated(
        "hexpiretime",
        -5,
        READONLY | FAST,
        (1, 1, 1),
        &["read", "hash", "fast"],
        legacy!(commands::cmd_httl, b"HEXPIRETIME"),
    ),
    CommandSpec::migrated(
        "hpexpiretime",
        -5,
        READONLY | FAST,
        (1, 1, 1),
        &["read", "hash", "fast"],
        legacy!(commands::cmd_httl, b"HPEXPIRETIME"),
    ),
    CommandSpec::migrated(
        "hpersist",
        -5,
        WRITE | FAST,
        (1, 1, 1),
        &["write", "hash", "fast"],
        legacy!(commands::cmd_httl, b"HPERSIST"),
    ),
    CommandSpec::migrated(
        "zadd",
        -4,
        WRITE | DENYOOM | FAST,
        (1, 1, 1),
        &["write", "sortedset", "fast"],
        parse_zadd,
    ),
    CommandSpec::migrated(
        "zrange",
        -4,
        READONLY,
        (1, 1, 1),
        &["read", "sortedset", "slow"],
        legacy!(commands::cmd_zrange, b"ZRANGE"),
    ),
    CommandSpec::migrated(
        "zrevrange",
        -4,
        READONLY,
        (1, 1, 1),
        &["read", "sortedset", "slow"],
        legacy!(commands::cmd_zrange, b"ZREVRANGE"),
    ),
    CommandSpec::migrated(
        "zrangebyscore",
        -4,
        READONLY,
        (1, 1, 1),
        &["read", "sortedset", "slow"],
        legacy!(commands::cmd_zrangebyscore, b"ZRANGEBYSCORE"),
    ),
    CommandSpec::migrated(
        "zrangebylex",
        -4,
        READONLY,
        (1, 1, 1),
        &["read", "sortedset", "slow"],
        legacy!(commands::cmd_zrangebylex, b"ZRANGEBYLEX"),
    ),
    CommandSpec::migrated(
        "zrevrangebylex",
        -4,
        READONLY,
        (1, 1, 1),
        &["read", "sortedset", "slow"],
        legacy!(commands::cmd_zrangebylex, b"ZREVRANGEBYLEX"),
    ),
    CommandSpec::migrated(
        "zrem",
        -3,
        WRITE | FAST,
        (1, 1, 1),
        &["write", "sortedset", "fast"],
        direct::cmd_zrem,
    ),
    CommandSpec::migrated(
        "zincrby",
        4,
        WRITE | DENYOOM | FAST,
        (1, 1, 1),
        &["write", "sortedset", "fast"],
        direct::cmd_zincrby,
    ),
    CommandSpec::migrated(
        "zcard",
        2,
        READONLY | FAST,
        (1, 1, 1),
        &["read", "sortedset", "fast"],
        direct::cmd_zcard,
    ),
    CommandSpec::migrated(
        "zscore",
        3,
        READONLY | FAST,
        (1, 1, 1),
        &["read", "sortedset", "fast"],
        direct::cmd_zscore,
    ),
    CommandSpec::migrated(
        "zrank",
        -3,
        READONLY | FAST,
        (1, 1, 1),
        &["read", "sortedset", "fast"],
        direct::cmd_zrank,
    ),
    CommandSpec::migrated(
        "zrevrank",
        -3,
        READONLY | FAST,
        (1, 1, 1),
        &["read", "sortedset", "fast"],
        direct::cmd_zrevrank,
    ),
    CommandSpec::migrated(
        "zpopmin",
        -2,
        WRITE | FAST,
        (1, 1, 1),
        &["write", "sortedset", "fast"],
        legacy!(commands::cmd_zpopmin, b"ZPOPMIN"),
    ),
    CommandSpec::migrated(
        "zpopmax",
        -2,
        WRITE | FAST,
        (1, 1, 1),
        &["write", "sortedset", "fast"],
        legacy!(commands::cmd_zpopmin, b"ZPOPMAX"),
    ),
    CommandSpec::migrated(
        "bzpopmin",
        -3,
        WRITE | FAST | BLOCKING,
        (1, -2, 1),
        &["write", "sortedset", "fast", "blocking"],
        legacy!(commands::cmd_bzpopmin, b"BZPOPMIN"),
    ),
    CommandSpec::migrated(
        "bzpopmax",
        -3,
        WRITE | FAST | BLOCKING,
        (1, -2, 1),
        &["write", "sortedset", "fast", "blocking"],
        legacy!(commands::cmd_bzpopmin, b"BZPOPMAX"),
    ),
    CommandSpec::migrated(
        "zremrangebyrank",
        4,
        WRITE,
        (1, 1, 1),
        &["write", "sortedset", "slow"],
        legacy!(commands::cmd_zremrangebyrank, b"ZREMRANGEBYRANK"),
    ),
    CommandSpec::migrated(
        "zremrangebyscore",
        4,
        WRITE,
        (1, 1, 1),
        &["write", "sortedset", "slow"],
        legacy!(commands::cmd_zremrangebyrank, b"ZREMRANGEBYSCORE"),
    ),
    CommandSpec::migrated(
        "zremrangebylex",
        4,
        WRITE,
        (1, 1, 1),
        &["write", "sortedset", "slow"],
        legacy!(commands::cmd_zremrangebyrank, b"ZREMRANGEBYLEX"),
    ),
    CommandSpec::migrated(
        "geoadd",
        -5,
        WRITE | DENYOOM,
        (1, 1, 1),
        &["write", "geo", "slow"],
        legacy!(commands::cmd_geoadd, b"GEOADD"),
    ),
    CommandSpec::migrated(
        "geodist",
        -4,
        READONLY,
        (1, 1, 1),
        &["read", "geo", "slow"],
        legacy!(commands::cmd_geodist, b"GEODIST"),
    ),
    CommandSpec::migrated(
        "geopos",
        -2,
        READONLY,
        (1, 1, 1),
        &["read", "geo", "slow"],
        legacy!(commands::cmd_geopos, b"GEOPOS"),
    ),
    CommandSpec::migrated(
        "geohash",
        -2,
        READONLY,
        (1, 1, 1),
        &["read", "geo", "slow"],
        legacy!(commands::cmd_geohash, b"GEOHASH"),
    ),
    CommandSpec::migrated(
        "geosearch",
        -7,
        READONLY,
        (1, 1, 1),
        &["read", "geo", "slow"],
        legacy!(commands::cmd_geosearch, b"GEOSEARCH"),
    ),
    CommandSpec::migrated(
        "geosearchstore",
        -8,
        WRITE | DENYOOM,
        (1, 2, 1),
        &["write", "geo", "slow"],
        legacy!(commands::cmd_geosearchstore, b"GEOSEARCHSTORE"),
    ),
    CommandSpec::migrated(
        "georadius",
        -6,
        WRITE | DENYOOM,
        (1, 1, 1),
        &["read", "write", "geo", "slow"],
        legacy!(commands::cmd_georadius, b"GEORADIUS"),
    ),
    CommandSpec::migrated(
        "georadiusbymember",
        -5,
        WRITE | DENYOOM,
        (1, 1, 1),
        &["read", "write", "geo", "slow"],
        legacy!(commands::cmd_georadiusbymember, b"GEORADIUSBYMEMBER"),
    ),
    CommandSpec::migrated(
        "bf.reserve",
        -4,
        WRITE | DENYOOM,
        (1, 1, 1),
        &["write", "slow"],
        legacy!(commands::cmd_bf_reserve, b"BF.RESERVE"),
    ),
    CommandSpec::migrated(
        "bf.add",
        3,
        WRITE | DENYOOM | FAST,
        (1, 1, 1),
        &["write", "fast"],
        legacy!(commands::cmd_bf_add, b"BF.ADD"),
    ),
    CommandSpec::migrated(
        "bf.madd",
        -3,
        WRITE | DENYOOM,
        (1, 1, 1),
        &["write", "slow"],
        legacy!(commands::cmd_bf_many, b"BF.MADD"),
    ),
    CommandSpec::migrated(
        "bf.mexists",
        -3,
        READONLY,
        (1, 1, 1),
        &["read", "slow"],
        legacy!(commands::cmd_bf_many, b"BF.MEXISTS"),
    ),
    CommandSpec::migrated(
        "bf.insert",
        -4,
        WRITE | DENYOOM,
        (1, 1, 1),
        &["write", "slow"],
        legacy!(commands::cmd_bf_insert, b"BF.INSERT"),
    ),
    CommandSpec::migrated(
        "bf.info",
        -2,
        READONLY | FAST,
        (1, 1, 1),
        &["read", "fast"],
        legacy!(commands::cmd_bf_info, b"BF.INFO"),
    ),
    CommandSpec::migrated(
        "bf.card",
        2,
        READONLY | FAST,
        (1, 1, 1),
        &["read", "fast"],
        legacy!(commands::cmd_bf_info, b"BF.CARD"),
    ),
    CommandSpec::migrated(
        "bf.exists",
        3,
        READONLY | FAST,
        (1, 1, 1),
        &["read", "fast"],
        legacy!(commands::cmd_bf_add, b"BF.EXISTS"),
    ),
    CommandSpec::migrated(
        "xadd",
        -5,
        WRITE | DENYOOM | FAST,
        (1, 1, 1),
        &["stream", "write", "fast"],
        legacy!(commands::cmd_xadd, b"XADD"),
    ),
    CommandSpec::migrated(
        "xlen",
        2,
        READONLY | FAST,
        (1, 1, 1),
        &["stream", "read", "fast"],
        direct::cmd_xlen,
    ),
    CommandSpec::migrated(
        "xrange",
        -4,
        READONLY,
        (1, 1, 1),
        &["stream", "read", "slow"],
        legacy!(commands::cmd_xrange, b"XRANGE"),
    ),
    CommandSpec::migrated(
        "xrevrange",
        -4,
        READONLY,
        (1, 1, 1),
        &["stream", "read", "slow"],
        legacy!(commands::cmd_xrange, b"XREVRANGE"),
    ),
    CommandSpec::migrated(
        "xread",
        -4,
        READONLY | BLOCKING,
        (0, 0, 0),
        &["stream", "read", "slow"],
        legacy!(commands::cmd_xread, b"XREAD"),
    ),
    CommandSpec::migrated(
        "xdel",
        -3,
        WRITE | FAST,
        (1, 1, 1),
        &["stream", "write", "fast"],
        legacy!(commands::cmd_xdel, b"XDEL"),
    ),
    CommandSpec::migrated(
        "xtrim",
        -4,
        WRITE,
        (1, 1, 1),
        &["stream", "write", "slow"],
        legacy!(commands::cmd_xtrim, b"XTRIM"),
    ),
    CommandSpec::migrated(
        "xsetid",
        -3,
        WRITE | DENYOOM | FAST,
        (1, 1, 1),
        &["stream", "write", "fast"],
        legacy!(commands::cmd_xsetid, b"XSETID"),
    ),
    CommandSpec::migrated(
        "xinfo",
        -2,
        READONLY,
        (2, 2, 1),
        &["stream", "read", "slow"],
        legacy!(commands::cmd_xinfo, b"XINFO"),
    ),
    CommandSpec::migrated(
        "xgroup",
        -2,
        WRITE,
        (2, 2, 1),
        &["stream", "write", "slow"],
        legacy!(commands::cmd_xgroup, b"XGROUP"),
    ),
    CommandSpec::migrated(
        "xreadgroup",
        -7,
        WRITE | DENYOOM | BLOCKING,
        (0, 0, 0),
        &["stream", "write", "slow"],
        legacy!(commands::cmd_xreadgroup, b"XREADGROUP"),
    ),
    CommandSpec::migrated(
        "xack",
        -4,
        WRITE | FAST,
        (1, 1, 1),
        &["stream", "write", "fast"],
        legacy!(commands::cmd_xack, b"XACK"),
    ),
    CommandSpec::migrated(
        "xpending",
        -3,
        READONLY,
        (1, 1, 1),
        &["stream", "read", "slow"],
        legacy!(commands::cmd_xpending, b"XPENDING"),
    ),
    CommandSpec::migrated(
        "xclaim",
        -6,
        WRITE | DENYOOM | FAST,
        (1, 1, 1),
        &["stream", "write", "fast"],
        legacy!(commands::cmd_xclaim, b"XCLAIM"),
    ),
    CommandSpec::migrated(
        "xautoclaim",
        -6,
        WRITE | DENYOOM | FAST,
        (1, 1, 1),
        &["stream", "write", "fast"],
        legacy!(commands::cmd_xautoclaim, b"XAUTOCLAIM"),
    ),
    CommandSpec::migrated(
        "multi",
        1,
        FAST,
        (0, 0, 0),
        &["transaction", "fast"],
        legacy!(commands::cmd_multi, b"MULTI"),
    ),
    CommandSpec::migrated(
        "exec",
        1,
        0,
        (0, 0, 0),
        &["transaction", "slow"],
        legacy!(commands::cmd_exec, b"EXEC"),
    ),
    CommandSpec::migrated(
        "discard",
        1,
        FAST,
        (0, 0, 0),
        &["transaction", "fast"],
        legacy!(commands::cmd_discard, b"DISCARD"),
    ),
    CommandSpec::migrated(
        "watch",
        -2,
        FAST,
        (1, -1, 1),
        &["transaction", "fast"],
        legacy!(commands::cmd_watch, b"WATCH"),
    ),
    CommandSpec::migrated(
        "unwatch",
        1,
        FAST,
        (0, 0, 0),
        &["transaction", "fast"],
        legacy!(commands::cmd_unwatch, b"UNWATCH"),
    ),
    CommandSpec::migrated(
        "eval",
        -3,
        WRITE,
        (0, 0, 0),
        &["scripting", "slow", "write"],
        legacy!(commands::cmd_eval, b"EVAL"),
    ),
    CommandSpec::migrated(
        "evalsha",
        -3,
        WRITE,
        (0, 0, 0),
        &["scripting", "slow", "write"],
        legacy!(commands::cmd_eval, b"EVALSHA"),
    ),
    CommandSpec::migrated(
        "fcall",
        -3,
        WRITE,
        (0, 0, 0),
        &["scripting", "slow", "write"],
        legacy!(commands::cmd_fcall, b"FCALL"),
    ),
    CommandSpec::migrated(
        "script",
        -2,
        0,
        (0, 0, 0),
        &["scripting", "slow"],
        legacy!(commands::cmd_script, b"SCRIPT"),
    ),
    CommandSpec::migrated(
        "function",
        -2,
        0,
        (0, 0, 0),
        &["scripting", "slow"],
        legacy!(commands::cmd_function, b"FUNCTION"),
    ),
    CommandSpec::migrated(
        "command",
        -1,
        0,
        (0, 0, 0),
        &["connection", "slow"],
        parse_command_meta,
    ),
];

// A const-built open-addressed table instead of phf: no new dependency, so the deny.toml
// and SBOM surface stay the same. Duplicate names and long probe chains fail the build.
const SLOTS: usize = 1024;
const EMPTY: u16 = 0;

const fn hash(name: &[u8]) -> usize {
    let mut state: u32 = 0x811c_9dc5;
    let mut index = 0;

    while index < name.len() {
        state ^= name[index] as u32;
        state = state.wrapping_mul(0x0100_0193);
        index += 1;
    }

    (state as usize) & (SLOTS - 1)
}

const fn same_name(left: &[u8], right: &[u8]) -> bool {
    if left.len() != right.len() {
        return false;
    }

    let mut index = 0;

    while index < left.len() {
        if left[index] != right[index] {
            return false;
        }

        index += 1;
    }

    true
}

/// Open-addressed table of `COMMANDS` indexes plus one (zero is empty), built at compile
/// time. Panics the build on a duplicate name, an over-long or non-lowercase name, an
/// overfull table, or a probe sequence longer than `MAX_PROBES`.
const MAX_PROBES: usize = 8;

static TABLE: [u16; SLOTS] = build_table();

const fn build_table() -> [u16; SLOTS] {
    let mut table = [EMPTY; SLOTS];
    let mut command = 0;

    assert!(
        COMMANDS.len() * 2 <= SLOTS,
        "command table is over half full"
    );

    while command < COMMANDS.len() {
        let name = COMMANDS[command].name.as_bytes();

        assert!(name.len() <= MAX_NAME, "command name is too long");

        let mut check = 0;

        while check < name.len() {
            assert!(
                !name[check].is_ascii_uppercase(),
                "command names are lowercase"
            );
            check += 1;
        }

        let mut slot = hash(name);
        let mut probes = 0;

        while table[slot] != EMPTY {
            let other = COMMANDS[(table[slot] - 1) as usize].name.as_bytes();

            assert!(!same_name(name, other), "duplicate command name");
            slot = (slot + 1) & (SLOTS - 1);
            probes += 1;
            assert!(probes < MAX_PROBES, "command table probe chain is too long");
        }

        table[slot] = command as u16 + 1;
        command += 1;
    }

    table
}

/// The row for `name`, matched ASCII case-insensitively, or `None` for an unknown or
/// not-yet-tabled command.
/// Position of `spec` in the command table.
pub(crate) fn command_index(spec: &CommandSpec) -> usize {
    (spec as *const CommandSpec as usize - COMMANDS.as_ptr() as usize)
        / std::mem::size_of::<CommandSpec>()
}

pub(crate) fn lookup(name: &[u8]) -> Option<&'static CommandSpec> {
    if name.len() > MAX_NAME {
        return None;
    }

    // The same FNV-1a as `hash`, over the lowercased name, without a lowercase copy.
    let mut state: u32 = 0x811c_9dc5;

    for &byte in name {
        state ^= byte.to_ascii_lowercase() as u32;
        state = state.wrapping_mul(0x0100_0193);
    }

    let mut slot = (state as usize) & (SLOTS - 1);

    for _ in 0..MAX_PROBES {
        let entry = TABLE[slot];

        if entry == EMPTY {
            return None;
        }

        let spec = &COMMANDS[(entry - 1) as usize];

        // Table names are lowercase ASCII.
        let wanted = spec.name.as_bytes();

        if wanted.len() == name.len()
            && wanted
                .iter()
                .zip(name)
                .all(|(&wanted, &byte)| byte.to_ascii_lowercase() == wanted)
        {
            return Some(spec);
        }

        slot = (slot + 1) & (SLOTS - 1);
    }

    None
}

/// All arguments as keys; one key is kept without a `Vec`.
pub(super) fn keys_of<'a>(args: &Args<'a, '_>) -> Keys<'a> {
    if args.len() == 1 {
        Keys::One(args.arg(0))
    } else {
        Keys::Many(args.rest(0))
    }
}

fn parse_get<'a>(args: &Args<'a, '_>) -> Result<Command<'a>, ProtocolError> {
    match args.len() {
        1 => Ok(Command::Get(args.arg(0))),
        _ => Err(ProtocolError::Invalid("GET expects one argument")),
    }
}

fn parse_set<'a>(args: &Args<'a, '_>) -> Result<Command<'a>, ProtocolError> {
    if args.len() < 2 {
        return Err(ProtocolError::Invalid("SET expects a key and a value"));
    }

    if args.len() > 2 {
        if args.bytes(2).eq_ignore_ascii_case(b"IFEQ") {
            return parse_set_ifeq(args);
        }

        let options = parse_set_options(&args.rest(2))?;

        return Ok(Command::Set(args.arg(0), args.arg(1), options));
    }

    Ok(Command::Set(
        args.arg(0),
        args.arg(1),
        SetOptions::default(),
    ))
}

/// `SET key value IFEQ expected [EX|PX|EXAT|PXAT|KEEPTTL]`. `NX`, `XX` and `GET` do not combine
/// with `IFEQ`.
fn parse_set_ifeq<'a>(args: &Args<'a, '_>) -> Result<Command<'a>, ProtocolError> {
    if args.len() < 4 {
        return Err(ProtocolError::Invalid(
            "SET IFEQ expects a value to compare",
        ));
    }

    let options = parse_set_options(&args.rest(4))?;

    if options.get || options.condition != SetCondition::Always {
        return Err(ProtocolError::Invalid("syntax error"));
    }

    Ok(Command::SetIfEq {
        key: args.arg(0),
        value: args.arg(1),
        expected: args.arg(3),
        options,
    })
}

fn parse_delex<'a>(args: &Args<'a, '_>) -> Result<Command<'a>, ProtocolError> {
    if args.len() != 3 || !args.bytes(1).eq_ignore_ascii_case(b"IFEQ") {
        return Err(ProtocolError::Invalid("DELEX expects key IFEQ value"));
    }

    Ok(Command::DelIfEq {
        key: args.arg(0),
        expected: args.arg(2),
    })
}

fn parse_push_capped<'a>(
    args: &Args<'a, '_>,
    back: bool,
    name: &'static str,
) -> Result<Command<'a>, ProtocolError> {
    if args.len() < 3 {
        return Err(ProtocolError::Invalid(name));
    }

    let cap = parse_usize(args.bytes(1))
        .ok()
        .filter(|cap| *cap > 0)
        .ok_or(ProtocolError::Invalid("cap must be a positive integer"))?;

    Ok(Command::ListPushCapped {
        key: args.arg(0),
        cap,
        values: (2..args.len()).map(|index| args.arg(index)).collect(),
        back,
    })
}

fn parse_lpushcap<'a>(args: &Args<'a, '_>) -> Result<Command<'a>, ProtocolError> {
    parse_push_capped(
        args,
        false,
        "LPUSHCAP expects a key, a cap and at least one value",
    )
}

fn parse_rpushcap<'a>(args: &Args<'a, '_>) -> Result<Command<'a>, ProtocolError> {
    parse_push_capped(
        args,
        true,
        "RPUSHCAP expects a key, a cap and at least one value",
    )
}

/// Integer argument with Redis's wording for a bad one.
fn parse_integer_argument(bytes: &[u8]) -> Result<i64, ProtocolError> {
    super::parse::redis_integer(bytes).ok_or(ProtocolError::Invalid(
        "value is not an integer or out of range",
    ))
}

fn parse_expire_seconds(
    bytes: &[u8],
    unit_ms: i64,
    invalid: &'static str,
) -> Result<SetExpiry, ProtocolError> {
    let value = parse_integer_argument(bytes)?;

    if value <= 0 {
        return Err(ProtocolError::Invalid(invalid));
    }

    value
        .checked_mul(unit_ms)
        .map(SetExpiry::RelativeMs)
        .ok_or(ProtocolError::Invalid(invalid))
}

fn parse_setex<'a>(args: &Args<'a, '_>) -> Result<Command<'a>, ProtocolError> {
    if args.len() != 3 {
        return Err(ProtocolError::Invalid(
            "wrong number of arguments for 'setex' command",
        ));
    }

    let options = SetOptions {
        expiry: parse_expire_seconds(
            args.bytes(1),
            1000,
            "invalid expire time in 'setex' command",
        )?,
        ..SetOptions::default()
    };

    Ok(Command::Set(args.arg(0), args.arg(2), options))
}

fn parse_psetex<'a>(args: &Args<'a, '_>) -> Result<Command<'a>, ProtocolError> {
    if args.len() != 3 {
        return Err(ProtocolError::Invalid(
            "wrong number of arguments for 'psetex' command",
        ));
    }

    let options = SetOptions {
        expiry: parse_expire_seconds(args.bytes(1), 1, "invalid expire time in 'psetex' command")?,
        ..SetOptions::default()
    };

    Ok(Command::Set(args.arg(0), args.arg(2), options))
}

fn parse_setnx<'a>(args: &Args<'a, '_>) -> Result<Command<'a>, ProtocolError> {
    match args.len() {
        2 => Ok(Command::SetNx(args.arg(0), args.arg(1))),
        _ => Err(ProtocolError::Invalid(
            "wrong number of arguments for 'setnx' command",
        )),
    }
}

fn parse_getdel<'a>(args: &Args<'a, '_>) -> Result<Command<'a>, ProtocolError> {
    match args.len() {
        1 => Ok(Command::GetDel(args.arg(0))),
        _ => Err(ProtocolError::Invalid(
            "wrong number of arguments for 'getdel' command",
        )),
    }
}

fn parse_msetnx<'a>(args: &Args<'a, '_>) -> Result<Command<'a>, ProtocolError> {
    if args.len() < 2 || !args.len().is_multiple_of(2) {
        return Err(ProtocolError::Invalid(
            "wrong number of arguments for 'msetnx' command",
        ));
    }

    Ok(Command::MSetNx(
        (0..args.len() / 2)
            .map(|pair| (args.arg(pair * 2), args.arg(pair * 2 + 1)))
            .collect(),
    ))
}

/// Like Redis, `inf` parses and fails later as an overflow; only NaN or text is "not a float".
fn parse_increment_float(bytes: &[u8]) -> Result<f64, ProtocolError> {
    std::str::from_utf8(bytes)
        .ok()
        .and_then(|text| text.parse::<f64>().ok())
        .filter(|value| !value.is_nan())
        .ok_or(ProtocolError::Invalid("value is not a valid float"))
}

fn parse_incrbyfloat<'a>(args: &Args<'a, '_>) -> Result<Command<'a>, ProtocolError> {
    match args.len() {
        2 => Ok(Command::IncrementFloat(
            args.arg(0),
            parse_increment_float(args.bytes(1))?,
        )),
        _ => Err(ProtocolError::Invalid(
            "wrong number of arguments for 'incrbyfloat' command",
        )),
    }
}

fn parse_getrange<'a>(args: &Args<'a, '_>) -> Result<Command<'a>, ProtocolError> {
    match args.len() {
        3 => Ok(Command::GetRange {
            key: args.arg(0),
            start: parse_integer_argument(args.bytes(1))?,
            end: parse_integer_argument(args.bytes(2))?,
        }),
        _ => Err(ProtocolError::Invalid(
            "wrong number of arguments for 'getrange' command",
        )),
    }
}

fn parse_setrange<'a>(args: &Args<'a, '_>) -> Result<Command<'a>, ProtocolError> {
    if args.len() != 3 {
        return Err(ProtocolError::Invalid(
            "wrong number of arguments for 'setrange' command",
        ));
    }

    let offset = parse_integer_argument(args.bytes(1))?;

    if offset < 0 {
        return Err(ProtocolError::Invalid("offset is out of range"));
    }

    Ok(Command::SetRange {
        key: args.arg(0),
        offset: offset as u64,
        value: args.arg(2),
    })
}

fn arity_error(name: &'static str) -> ProtocolError {
    ProtocolError::Invalid(match name {
        "smismember" => "wrong number of arguments for 'smismember' command",
        "sintercard" => "wrong number of arguments for 'sintercard' command",
        "zcount" => "wrong number of arguments for 'zcount' command",
        "zlexcount" => "wrong number of arguments for 'zlexcount' command",
        "zmscore" => "wrong number of arguments for 'zmscore' command",
        "zrandmember" => "wrong number of arguments for 'zrandmember' command",
        "hrandfield" => "wrong number of arguments for 'hrandfield' command",
        "hincrbyfloat" => "wrong number of arguments for 'hincrbyfloat' command",
        _ => "wrong number of arguments",
    })
}

fn parse_smismember<'a>(args: &Args<'a, '_>) -> Result<Command<'a>, ProtocolError> {
    if args.len() < 2 {
        return Err(arity_error("smismember"));
    }

    Ok(Command::SetMIsMember {
        key: args.arg(0),
        members: (1..args.len()).map(|index| args.arg(index)).collect(),
    })
}

fn parse_sintercard<'a>(args: &Args<'a, '_>) -> Result<Command<'a>, ProtocolError> {
    if args.len() < 2 {
        return Err(arity_error("sintercard"));
    }

    let numkeys = parse_integer_argument(args.bytes(0))?;

    if numkeys < 1 {
        return Err(ProtocolError::Invalid("numkeys should be greater than 0"));
    }

    if numkeys as usize > args.len() - 1 {
        return Err(ProtocolError::Invalid(
            "Number of keys can't be greater than number of args",
        ));
    }

    let keys: Vec<Bytes<'a>> = (1..=numkeys as usize)
        .map(|index| args.arg(index))
        .collect();
    let mut limit = 0;
    let mut index = numkeys as usize + 1;

    while index < args.len() {
        if args.bytes(index).eq_ignore_ascii_case(b"LIMIT") && index + 1 < args.len() {
            let value = parse_integer_argument(args.bytes(index + 1))
                .map_err(|_| ProtocolError::Invalid("LIMIT can't be negative"))?;

            if value < 0 {
                return Err(ProtocolError::Invalid("LIMIT can't be negative"));
            }

            limit = value as u64;
            index += 2;
        } else {
            return Err(ProtocolError::Invalid("syntax error"));
        }
    }

    Ok(Command::SetInterCard { keys, limit })
}

fn parse_zcount<'a>(args: &Args<'a, '_>) -> Result<Command<'a>, ProtocolError> {
    if args.len() != 3 {
        return Err(arity_error("zcount"));
    }

    let bound = |index: usize| {
        parse_score_bound(args.bytes(index))
            .map_err(|_| ProtocolError::Invalid("min or max is not a float"))
    };

    Ok(Command::SortedSetCount {
        key: args.arg(0),
        min: bound(1)?,
        max: bound(2)?,
    })
}

fn parse_zlexcount<'a>(args: &Args<'a, '_>) -> Result<Command<'a>, ProtocolError> {
    if args.len() != 3 {
        return Err(arity_error("zlexcount"));
    }

    Ok(Command::SortedSetLexCount {
        key: args.arg(0),
        min: parse_lex_bound(args.arg(1))?,
        max: parse_lex_bound(args.arg(2))?,
    })
}

fn parse_zmscore<'a>(args: &Args<'a, '_>) -> Result<Command<'a>, ProtocolError> {
    if args.len() < 2 {
        return Err(arity_error("zmscore"));
    }

    Ok(Command::SortedSetMScore {
        key: args.arg(0),
        members: (1..args.len()).map(|index| args.arg(index)).collect(),
    })
}

/// `[dest] numkeys key... [WEIGHTS w...] [AGGREGATE SUM|MIN|MAX] [WITHSCORES]`.
fn parse_sorted_algebra<'a>(
    args: &Args<'a, '_>,
    op: SetOp,
    store: bool,
    name: &'static str,
    empty_keys: &'static str,
) -> Result<Command<'a>, ProtocolError> {
    let first = usize::from(store);

    if args.len() < first + 2 {
        return Err(ProtocolError::Invalid(name));
    }

    let numkeys = parse_integer_argument(args.bytes(first))?;

    if numkeys < 1 {
        return Err(ProtocolError::Invalid(empty_keys));
    }

    let keys_start = first + 1;
    let keys_end = keys_start + numkeys as usize;

    if keys_end > args.len() {
        return Err(ProtocolError::Invalid("syntax error"));
    }

    let mut weights = Vec::new();
    let mut aggregate = SortedAggregate::Sum;
    let mut with_scores = false;
    let mut index = keys_end;

    while index < args.len() {
        let word = args.bytes(index);

        if op != SetOp::Diff && word.eq_ignore_ascii_case(b"WEIGHTS") {
            if index + numkeys as usize >= args.len() {
                return Err(ProtocolError::Invalid("syntax error"));
            }

            weights = (1..=numkeys as usize)
                .map(|offset| {
                    std::str::from_utf8(args.bytes(index + offset))
                        .ok()
                        .and_then(|text| text.parse::<f64>().ok())
                        .filter(|weight| !weight.is_nan())
                        .ok_or(ProtocolError::Invalid("weight value is not a float"))
                })
                .collect::<Result<_, _>>()?;
            index += 1 + numkeys as usize;
        } else if op != SetOp::Diff
            && word.eq_ignore_ascii_case(b"AGGREGATE")
            && index + 1 < args.len()
        {
            let mode = args.bytes(index + 1);

            aggregate = if mode.eq_ignore_ascii_case(b"SUM") {
                SortedAggregate::Sum
            } else if mode.eq_ignore_ascii_case(b"MIN") {
                SortedAggregate::Min
            } else if mode.eq_ignore_ascii_case(b"MAX") {
                SortedAggregate::Max
            } else {
                return Err(ProtocolError::Invalid("syntax error"));
            };
            index += 2;
        } else if !store && word.eq_ignore_ascii_case(b"WITHSCORES") {
            with_scores = true;
            index += 1;
        } else {
            return Err(ProtocolError::Invalid("syntax error"));
        }
    }

    Ok(Command::SortedSetAlgebra {
        op,
        keys: (keys_start..keys_end)
            .map(|position| args.arg(position))
            .collect(),
        weights,
        aggregate,
        with_scores,
        destination: store.then(|| args.arg(0)),
    })
}

fn parse_zunionstore<'a>(args: &Args<'a, '_>) -> Result<Command<'a>, ProtocolError> {
    parse_sorted_algebra(
        args,
        SetOp::Union,
        true,
        "wrong number of arguments for 'zunionstore' command",
        "at least 1 input key is needed for 'zunionstore' command",
    )
}

fn parse_zinterstore<'a>(args: &Args<'a, '_>) -> Result<Command<'a>, ProtocolError> {
    parse_sorted_algebra(
        args,
        SetOp::Inter,
        true,
        "wrong number of arguments for 'zinterstore' command",
        "at least 1 input key is needed for 'zinterstore' command",
    )
}

fn parse_zdiffstore<'a>(args: &Args<'a, '_>) -> Result<Command<'a>, ProtocolError> {
    parse_sorted_algebra(
        args,
        SetOp::Diff,
        true,
        "wrong number of arguments for 'zdiffstore' command",
        "at least 1 input key is needed for 'zdiffstore' command",
    )
}

fn parse_zunion<'a>(args: &Args<'a, '_>) -> Result<Command<'a>, ProtocolError> {
    parse_sorted_algebra(
        args,
        SetOp::Union,
        false,
        "wrong number of arguments for 'zunion' command",
        "at least 1 input key is needed for 'zunion' command",
    )
}

fn parse_zinter<'a>(args: &Args<'a, '_>) -> Result<Command<'a>, ProtocolError> {
    parse_sorted_algebra(
        args,
        SetOp::Inter,
        false,
        "wrong number of arguments for 'zinter' command",
        "at least 1 input key is needed for 'zinter' command",
    )
}

fn parse_zdiff<'a>(args: &Args<'a, '_>) -> Result<Command<'a>, ProtocolError> {
    parse_sorted_algebra(
        args,
        SetOp::Diff,
        false,
        "wrong number of arguments for 'zdiff' command",
        "at least 1 input key is needed for 'zdiff' command",
    )
}

/// `key [count [WITHSCORES|WITHVALUES]]` shared by `ZRANDMEMBER` and `HRANDFIELD`.
fn parse_random_options<'a>(
    args: &Args<'a, '_>,
    flag: &[u8],
    name: &'static str,
) -> Result<(Bytes<'a>, Option<i64>, bool), ProtocolError> {
    match args.len() {
        1 => Ok((args.arg(0), None, false)),
        2 | 3 => {
            let count = check_random_count(parse_integer_argument(args.bytes(1))?)?;
            let with_extra = if args.len() == 3 {
                if !args.bytes(2).eq_ignore_ascii_case(flag) {
                    return Err(ProtocolError::Invalid("syntax error"));
                }

                true
            } else {
                false
            };

            Ok((args.arg(0), Some(count), with_extra))
        }
        _ => Err(arity_error(name)),
    }
}

fn parse_zrandmember<'a>(args: &Args<'a, '_>) -> Result<Command<'a>, ProtocolError> {
    let (key, count, with_scores) = parse_random_options(args, b"WITHSCORES", "zrandmember")?;

    Ok(Command::SortedSetRandMember {
        key,
        count,
        with_scores,
    })
}

fn parse_hrandfield<'a>(args: &Args<'a, '_>) -> Result<Command<'a>, ProtocolError> {
    let (key, count, with_values) = parse_random_options(args, b"WITHVALUES", "hrandfield")?;

    Ok(Command::HashRandField {
        key,
        count,
        with_values,
    })
}

fn parse_hincrbyfloat<'a>(args: &Args<'a, '_>) -> Result<Command<'a>, ProtocolError> {
    if args.len() != 3 {
        return Err(arity_error("hincrbyfloat"));
    }

    Ok(Command::HashIncrementFloat {
        key: args.arg(0),
        field: args.arg(1),
        delta: parse_increment_float(args.bytes(2))?,
    })
}

fn parse_linsert<'a>(args: &Args<'a, '_>) -> Result<Command<'a>, ProtocolError> {
    if args.len() != 4 {
        return Err(ProtocolError::Invalid(
            "wrong number of arguments for 'linsert' command",
        ));
    }

    let before = if args.bytes(1).eq_ignore_ascii_case(b"BEFORE") {
        true
    } else if args.bytes(1).eq_ignore_ascii_case(b"AFTER") {
        false
    } else {
        return Err(ProtocolError::Invalid("syntax error"));
    };

    Ok(Command::ListInsert {
        key: args.arg(0),
        before,
        pivot: args.arg(2),
        value: args.arg(3),
    })
}

fn parse_lset<'a>(args: &Args<'a, '_>) -> Result<Command<'a>, ProtocolError> {
    if args.len() != 3 {
        return Err(ProtocolError::Invalid(
            "wrong number of arguments for 'lset' command",
        ));
    }

    Ok(Command::ListSet {
        key: args.arg(0),
        index: parse_integer_argument(args.bytes(1))?,
        value: args.arg(2),
    })
}

fn parse_lrem<'a>(args: &Args<'a, '_>) -> Result<Command<'a>, ProtocolError> {
    if args.len() != 3 {
        return Err(ProtocolError::Invalid(
            "wrong number of arguments for 'lrem' command",
        ));
    }

    Ok(Command::ListRemove {
        key: args.arg(0),
        count: parse_integer_argument(args.bytes(1))?,
        value: args.arg(2),
    })
}

fn parse_lpos<'a>(args: &Args<'a, '_>) -> Result<Command<'a>, ProtocolError> {
    if args.len() < 2 {
        return Err(ProtocolError::Invalid(
            "wrong number of arguments for 'lpos' command",
        ));
    }

    let mut rank = 1i64;
    let mut count = None;
    let mut max_len = 0u64;
    let mut index = 2;

    while index < args.len() {
        let word = args.bytes(index);

        if index + 1 >= args.len() {
            return Err(ProtocolError::Invalid("syntax error"));
        }

        let value = parse_integer_argument(args.bytes(index + 1))?;

        if word.eq_ignore_ascii_case(b"RANK") {
            if value == 0 {
                return Err(ProtocolError::Invalid(
                    "RANK can't be zero: use 1 to start from the first match, 2 from the second ... or use negative to start from the end of the list",
                ));
            }

            if value == i64::MIN {
                return Err(ProtocolError::Invalid(
                    "value is out of range, value must between -9223372036854775807 and 9223372036854775807",
                ));
            }

            rank = value;
        } else if word.eq_ignore_ascii_case(b"COUNT") {
            if value < 0 {
                return Err(ProtocolError::Invalid("COUNT can't be negative"));
            }

            count = Some(value as u64);
        } else if word.eq_ignore_ascii_case(b"MAXLEN") {
            if value < 0 {
                return Err(ProtocolError::Invalid("MAXLEN can't be negative"));
            }

            max_len = value as u64;
        } else {
            return Err(ProtocolError::Invalid("syntax error"));
        }

        index += 2;
    }

    Ok(Command::ListPos {
        key: args.arg(0),
        element: args.arg(1),
        rank,
        count,
        max_len,
    })
}

fn parse_side(bytes: &[u8]) -> Result<bool, ProtocolError> {
    if bytes.eq_ignore_ascii_case(b"LEFT") {
        Ok(true)
    } else if bytes.eq_ignore_ascii_case(b"RIGHT") {
        Ok(false)
    } else {
        Err(ProtocolError::Invalid("syntax error"))
    }
}

fn parse_lmove<'a>(args: &Args<'a, '_>) -> Result<Command<'a>, ProtocolError> {
    if args.len() != 4 {
        return Err(ProtocolError::Invalid(
            "wrong number of arguments for 'lmove' command",
        ));
    }

    Ok(Command::ListMove {
        source: args.arg(0),
        destination: args.arg(1),
        from_left: parse_side(args.bytes(2))?,
        to_left: parse_side(args.bytes(3))?,
        block: None,
    })
}

fn parse_blmove<'a>(args: &Args<'a, '_>) -> Result<Command<'a>, ProtocolError> {
    if args.len() != 5 {
        return Err(ProtocolError::Invalid(
            "wrong number of arguments for 'blmove' command",
        ));
    }

    Ok(Command::ListMove {
        source: args.arg(0),
        destination: args.arg(1),
        from_left: parse_side(args.bytes(2))?,
        to_left: parse_side(args.bytes(3))?,
        block: Some(parse_timeout_ms(args.bytes(4))?),
    })
}

fn parse_rpoplpush<'a>(args: &Args<'a, '_>) -> Result<Command<'a>, ProtocolError> {
    if args.len() != 2 {
        return Err(ProtocolError::Invalid(
            "wrong number of arguments for 'rpoplpush' command",
        ));
    }

    Ok(Command::ListMove {
        source: args.arg(0),
        destination: args.arg(1),
        from_left: false,
        to_left: true,
        block: None,
    })
}

fn parse_pushx<'a>(
    args: &Args<'a, '_>,
    back: bool,
    name: &'static str,
) -> Result<Command<'a>, ProtocolError> {
    if args.len() < 2 {
        return Err(ProtocolError::Invalid(name));
    }

    Ok(Command::ListPushX {
        key: args.arg(0),
        values: (1..args.len()).map(|index| args.arg(index)).collect(),
        back,
    })
}

fn parse_lpushx<'a>(args: &Args<'a, '_>) -> Result<Command<'a>, ProtocolError> {
    parse_pushx(
        args,
        false,
        "wrong number of arguments for 'lpushx' command",
    )
}

fn parse_rpushx<'a>(args: &Args<'a, '_>) -> Result<Command<'a>, ProtocolError> {
    parse_pushx(args, true, "wrong number of arguments for 'rpushx' command")
}

/// `numkeys key... LEFT|RIGHT [COUNT n]` starting at argument `first`.
fn parse_mpop<'a>(
    args: &Args<'a, '_>,
    first: usize,
    block: Option<Option<u64>>,
    arity: &'static str,
) -> Result<Command<'a>, ProtocolError> {
    if args.len() < first + 3 {
        return Err(ProtocolError::Invalid(arity));
    }

    let numkeys = parse_integer_argument(args.bytes(first))?;

    if numkeys < 1 {
        return Err(ProtocolError::Invalid("numkeys should be greater than 0"));
    }

    let keys_end = first + 1 + numkeys as usize;

    if keys_end >= args.len() {
        return Err(ProtocolError::Invalid("syntax error"));
    }

    let from_left = parse_side(args.bytes(keys_end))?;
    let mut count = 1u64;
    let mut index = keys_end + 1;

    if index < args.len() {
        if index + 2 != args.len() || !args.bytes(index).eq_ignore_ascii_case(b"COUNT") {
            return Err(ProtocolError::Invalid("syntax error"));
        }

        let value = parse_integer_argument(args.bytes(index + 1))?;

        if value < 1 {
            return Err(ProtocolError::Invalid("count should be greater than 0"));
        }

        count = value as u64;
        index += 2;
    }

    debug_assert_eq!(index, args.len());

    Ok(Command::ListMPop {
        keys: (first + 1..keys_end)
            .map(|position| args.arg(position))
            .collect(),
        from_left,
        count,
        block,
    })
}

fn parse_lmpop<'a>(args: &Args<'a, '_>) -> Result<Command<'a>, ProtocolError> {
    parse_mpop(
        args,
        0,
        None,
        "wrong number of arguments for 'lmpop' command",
    )
}

fn parse_blmpop<'a>(args: &Args<'a, '_>) -> Result<Command<'a>, ProtocolError> {
    if args.len() == 0 {
        return Err(ProtocolError::Invalid(
            "wrong number of arguments for 'blmpop' command",
        ));
    }

    let timeout = parse_timeout_ms(args.bytes(0))?;

    parse_mpop(
        args,
        1,
        Some(timeout),
        "wrong number of arguments for 'blmpop' command",
    )
}

fn parse_incr<'a>(args: &Args<'a, '_>) -> Result<Command<'a>, ProtocolError> {
    match args.len() {
        1 => Ok(Command::Increment(args.arg(0), 1)),
        _ => Err(ProtocolError::Invalid("INCR and DECR expect one argument")),
    }
}

fn parse_decr<'a>(args: &Args<'a, '_>) -> Result<Command<'a>, ProtocolError> {
    match args.len() {
        1 => Ok(Command::Increment(args.arg(0), -1)),
        _ => Err(ProtocolError::Invalid("INCR and DECR expect one argument")),
    }
}

fn parse_incrby<'a>(args: &Args<'a, '_>) -> Result<Command<'a>, ProtocolError> {
    match args.len() {
        2 => Ok(Command::Increment(args.arg(0), parse_i64(args.bytes(1))?)),
        4 if args.bytes(2).eq_ignore_ascii_case(b"MAX") => Ok(Command::IncrementCapped {
            key: args.arg(0),
            delta: parse_i64(args.bytes(1))?,
            limit: parse_i64(args.bytes(3))?,
        }),
        _ => Err(ProtocolError::Invalid(
            "INCRBY and DECRBY expect a key and an integer",
        )),
    }
}

fn parse_decrby<'a>(args: &Args<'a, '_>) -> Result<Command<'a>, ProtocolError> {
    match args.len() {
        2 => {
            let delta = parse_i64(args.bytes(1))?
                .checked_neg()
                .ok_or(ProtocolError::Invalid(
                    "value is not an integer or out of range",
                ))?;

            Ok(Command::Increment(args.arg(0), delta))
        }
        _ => Err(ProtocolError::Invalid(
            "INCRBY and DECRBY expect a key and an integer",
        )),
    }
}

fn parse_mget<'a>(args: &Args<'a, '_>) -> Result<Command<'a>, ProtocolError> {
    if args.len() == 0 {
        Err(ProtocolError::Invalid("MGET expects at least one key"))
    } else {
        Ok(Command::MGet(args.rest(0)))
    }
}

fn parse_getset<'a>(args: &Args<'a, '_>) -> Result<Command<'a>, ProtocolError> {
    match args.len() {
        2 => Ok(Command::GetSet(args.arg(0), args.arg(1))),
        _ => Err(ProtocolError::Invalid("GETSET expects a key and a value")),
    }
}

fn parse_append<'a>(args: &Args<'a, '_>) -> Result<Command<'a>, ProtocolError> {
    match args.len() {
        2 => Ok(Command::Append(args.arg(0), args.arg(1))),
        _ => Err(ProtocolError::Invalid("APPEND expects a key and a value")),
    }
}

/// Highest bit offset `SETBIT` and `GETBIT` accept (Redis: 2^32 - 1; here bounded by the
/// 16 MiB bitmap limit in the engine).
const MAX_BIT_OFFSET: u64 = (1 << 32) - 1;

fn parse_bit_offset(bytes: &[u8]) -> Result<u64, ProtocolError> {
    std::str::from_utf8(bytes)
        .ok()
        .and_then(|text| text.parse::<u64>().ok())
        .filter(|&offset| offset <= MAX_BIT_OFFSET)
        .ok_or(ProtocolError::Invalid(
            "bit offset is not an integer or out of range",
        ))
}

fn parse_bit_value(bytes: &[u8]) -> Result<bool, ProtocolError> {
    match bytes {
        b"0" => Ok(false),
        b"1" => Ok(true),
        _ => Err(ProtocolError::Invalid(
            "bit is not an integer or out of range",
        )),
    }
}

/// Optional trailing `BYTE` or `BIT` unit; `true` means bits.
fn parse_bit_unit(bytes: &[u8]) -> Result<bool, ProtocolError> {
    if bytes.eq_ignore_ascii_case(b"BIT") {
        Ok(true)
    } else if bytes.eq_ignore_ascii_case(b"BYTE") {
        Ok(false)
    } else {
        Err(ProtocolError::Invalid("syntax error"))
    }
}

fn parse_setbit<'a>(args: &Args<'a, '_>) -> Result<Command<'a>, ProtocolError> {
    match args.len() {
        3 => Ok(Command::SetBit {
            key: args.arg(0),
            offset: parse_bit_offset(args.bytes(1))?,
            value: parse_bit_value(args.bytes(2))?,
        }),
        _ => Err(ProtocolError::Invalid(
            "SETBIT expects a key, an offset and a bit",
        )),
    }
}

/// Largest bit offset a `BITFIELD` field may end at (16 MiB bitmap).
const MAX_BITFIELD_BITS: u64 = 16 * 1024 * 1024 * 8;

/// Largest repeat count (a negative count) `SRANDMEMBER`, `HRANDFIELD` and `ZRANDMEMBER`
/// accept. Repeats are drawn into memory before any output limit applies, so an unbounded
/// count could exhaust memory. A positive count is bounded by the collection size.
pub(crate) const MAX_RANDOM_COUNT: u64 = 1 << 20;

/// Refuses a repeat count whose reply could not fit in memory.
pub(crate) fn check_random_count(count: i64) -> Result<i64, ProtocolError> {
    if count < 0 && count.unsigned_abs() > MAX_RANDOM_COUNT {
        return Err(ProtocolError::Invalid("count is out of range"));
    }

    Ok(count)
}

fn parse_bitfield_type(bytes: &[u8]) -> Result<BitFieldType, ProtocolError> {
    const INVALID: ProtocolError = ProtocolError::Invalid(
        "Invalid bitfield type. Use something like i16 u8. Note that u64 is not supported but i64 is.",
    );
    let (signed, digits) = match bytes.split_first() {
        Some((b'i', rest)) => (true, rest),
        Some((b'u', rest)) => (false, rest),
        _ => return Err(INVALID),
    };
    let bits: u8 = std::str::from_utf8(digits)
        .ok()
        .and_then(|text| text.parse().ok())
        .ok_or(INVALID)?;
    let limit = if signed { 64 } else { 63 };

    if bits == 0 || bits > limit {
        return Err(INVALID);
    }

    Ok(BitFieldType { signed, bits })
}

/// A bit offset, or `#n` for the n-th field of the type's width. The field must fit the bitmap limit.
fn parse_bitfield_offset(bytes: &[u8], ty: BitFieldType) -> Result<u64, ProtocolError> {
    const INVALID: ProtocolError =
        ProtocolError::Invalid("bit offset is not an integer or out of range");
    let (scaled, digits) = match bytes.split_first() {
        Some((b'#', rest)) => (true, rest),
        _ => (false, bytes),
    };
    let number: u64 = std::str::from_utf8(digits)
        .ok()
        .and_then(|text| text.parse().ok())
        .ok_or(INVALID)?;
    let offset = if scaled {
        number.checked_mul(u64::from(ty.bits)).ok_or(INVALID)?
    } else {
        number
    };

    if offset.saturating_add(u64::from(ty.bits)) > MAX_BITFIELD_BITS {
        return Err(INVALID);
    }

    Ok(offset)
}

fn parse_bitfield_ops(args: &Args<'_, '_>, read_only: bool) -> Result<BitFieldOps, ProtocolError> {
    if args.len() == 0 {
        return Err(ProtocolError::Invalid(
            "wrong number of arguments for 'bitfield' command",
        ));
    }

    const SYNTAX: ProtocolError = ProtocolError::Invalid("syntax error");
    let mut ops = BitFieldOps::new();
    let mut overflow = BitFieldOverflow::Wrap;
    let mut index = 1;

    while index < args.len() {
        let word = args.bytes(index);

        if word.eq_ignore_ascii_case(b"GET") && index + 2 < args.len() {
            let ty = parse_bitfield_type(args.bytes(index + 1))?;

            ops.push(BitFieldOp::Get {
                ty,
                offset: parse_bitfield_offset(args.bytes(index + 2), ty)?,
            });
            index += 3;
        } else if (word.eq_ignore_ascii_case(b"SET") || word.eq_ignore_ascii_case(b"INCRBY"))
            && index + 3 < args.len()
        {
            if read_only {
                return Err(ProtocolError::Invalid(
                    "BITFIELD_RO only supports the GET subcommand",
                ));
            }

            let ty = parse_bitfield_type(args.bytes(index + 1))?;
            let offset = parse_bitfield_offset(args.bytes(index + 2), ty)?;
            let number = super::parse::redis_integer(args.bytes(index + 3)).ok_or(
                ProtocolError::Invalid("value is not an integer or out of range"),
            )?;

            ops.push(if word.eq_ignore_ascii_case(b"SET") {
                BitFieldOp::Set {
                    ty,
                    offset,
                    value: number,
                    overflow,
                }
            } else {
                BitFieldOp::IncrBy {
                    ty,
                    offset,
                    delta: number,
                    overflow,
                }
            });
            index += 4;
        } else if word.eq_ignore_ascii_case(b"OVERFLOW") && index + 1 < args.len() {
            let mode = args.bytes(index + 1);

            overflow = if mode.eq_ignore_ascii_case(b"WRAP") {
                BitFieldOverflow::Wrap
            } else if mode.eq_ignore_ascii_case(b"SAT") {
                BitFieldOverflow::Sat
            } else if mode.eq_ignore_ascii_case(b"FAIL") {
                BitFieldOverflow::Fail
            } else {
                return Err(ProtocolError::Invalid("Invalid OVERFLOW type specified"));
            };
            index += 2;
        } else if read_only
            && (word.eq_ignore_ascii_case(b"SET") || word.eq_ignore_ascii_case(b"INCRBY"))
        {
            return Err(ProtocolError::Invalid(
                "BITFIELD_RO only supports the GET subcommand",
            ));
        } else {
            return Err(SYNTAX);
        }
    }

    Ok(ops)
}

fn parse_bitfield<'a>(args: &Args<'a, '_>) -> Result<Command<'a>, ProtocolError> {
    let ops = parse_bitfield_ops(args, false)?;

    Ok(Command::BitField {
        key: args.arg(0),
        ops,
        read_only_name: false,
    })
}

fn parse_bitfield_ro<'a>(args: &Args<'a, '_>) -> Result<Command<'a>, ProtocolError> {
    let ops = parse_bitfield_ops(args, true)?;

    Ok(Command::BitField {
        key: args.arg(0),
        ops,
        read_only_name: true,
    })
}

fn parse_getbit<'a>(args: &Args<'a, '_>) -> Result<Command<'a>, ProtocolError> {
    match args.len() {
        2 => Ok(Command::GetBit(
            args.arg(0),
            parse_bit_offset(args.bytes(1))?,
        )),
        _ => Err(ProtocolError::Invalid("GETBIT expects a key and an offset")),
    }
}

fn parse_bitcount<'a>(args: &Args<'a, '_>) -> Result<Command<'a>, ProtocolError> {
    let range = match args.len() {
        0 => return Err(ProtocolError::Invalid("BITCOUNT expects a key")),
        1 => None,
        3 => Some((parse_i64(args.bytes(1))?, parse_i64(args.bytes(2))?, false)),
        4 => Some((
            parse_i64(args.bytes(1))?,
            parse_i64(args.bytes(2))?,
            parse_bit_unit(args.bytes(3))?,
        )),
        _ => return Err(ProtocolError::Invalid("syntax error")),
    };

    Ok(Command::BitCount {
        key: args.arg(0),
        range,
    })
}

fn parse_bitpos<'a>(args: &Args<'a, '_>) -> Result<Command<'a>, ProtocolError> {
    if args.len() < 2 {
        return Err(ProtocolError::Invalid("BITPOS expects a key and a bit"));
    }

    let bit = parse_bit_value(args.bytes(1))?;
    let range = match args.len() {
        2 => None,
        3 => Some((parse_i64(args.bytes(2))?, None, false)),
        4 => Some((
            parse_i64(args.bytes(2))?,
            Some(parse_i64(args.bytes(3))?),
            false,
        )),
        5 => Some((
            parse_i64(args.bytes(2))?,
            Some(parse_i64(args.bytes(3))?),
            parse_bit_unit(args.bytes(4))?,
        )),
        _ => return Err(ProtocolError::Invalid("syntax error")),
    };

    Ok(Command::BitPos {
        key: args.arg(0),
        bit,
        range,
    })
}

fn parse_bitop<'a>(args: &Args<'a, '_>) -> Result<Command<'a>, ProtocolError> {
    if args.len() < 3 {
        return Err(ProtocolError::Invalid(
            "BITOP expects an operation, a destination and at least one source",
        ));
    }

    let operation = match args.bytes(0) {
        name if name.eq_ignore_ascii_case(b"AND") => BitOperation::And,
        name if name.eq_ignore_ascii_case(b"OR") => BitOperation::Or,
        name if name.eq_ignore_ascii_case(b"XOR") => BitOperation::Xor,
        name if name.eq_ignore_ascii_case(b"NOT") => BitOperation::Not,
        _ => return Err(ProtocolError::Invalid("syntax error")),
    };

    Ok(Command::BitOp {
        operation,
        destination: args.arg(1),
        keys: (2..args.len()).map(|index| args.arg(index)).collect(),
    })
}

fn parse_pfadd<'a>(args: &Args<'a, '_>) -> Result<Command<'a>, ProtocolError> {
    if args.len() == 0 {
        return Err(ProtocolError::Invalid("PFADD expects a key"));
    }

    Ok(Command::PfAdd {
        key: args.arg(0),
        elements: (1..args.len()).map(|index| args.arg(index)).collect(),
    })
}

fn parse_pfcount<'a>(args: &Args<'a, '_>) -> Result<Command<'a>, ProtocolError> {
    if args.len() == 0 {
        return Err(ProtocolError::Invalid("PFCOUNT expects at least one key"));
    }

    Ok(Command::PfCount(keys_of(args)))
}

fn parse_pfmerge<'a>(args: &Args<'a, '_>) -> Result<Command<'a>, ProtocolError> {
    if args.len() == 0 {
        return Err(ProtocolError::Invalid("PFMERGE expects a destination key"));
    }

    Ok(Command::PfMerge {
        destination: args.arg(0),
        sources: (1..args.len()).map(|index| args.arg(index)).collect(),
    })
}

fn parse_positive_u64(bytes: &[u8], what: &'static str) -> Result<u64, ProtocolError> {
    match parse_i64(bytes) {
        Ok(value) if value > 0 => Ok(value as u64),
        _ => Err(ProtocolError::Invalid(what)),
    }
}

fn parse_fillget<'a>(args: &Args<'a, '_>) -> Result<Command<'a>, ProtocolError> {
    let wait_ms = match args.len() {
        2 => None,
        4 if args.bytes(2).eq_ignore_ascii_case(b"WAIT") => Some(parse_positive_u64(
            args.bytes(3),
            "FILLGET wait time must be a positive integer",
        )?),
        _ => {
            return Err(ProtocolError::Invalid(
                "FILLGET expects a key, a fill time in milliseconds and optionally WAIT ms",
            ));
        }
    };

    Ok(Command::FillGet {
        key: args.arg(0),
        fill_ms: parse_positive_u64(
            args.bytes(1),
            "FILLGET fill time must be a positive integer",
        )?,
        wait_ms,
    })
}

fn parse_fillset<'a>(args: &Args<'a, '_>) -> Result<Command<'a>, ProtocolError> {
    let swr_ms = match args.len() {
        4 => None,
        6 if args.bytes(4).eq_ignore_ascii_case(b"SWR") => Some(parse_positive_u64(
            args.bytes(5),
            "FILLSET SWR must be a positive integer",
        )?),
        _ => {
            return Err(ProtocolError::Invalid(
                "FILLSET expects a key, a value, a ttl in milliseconds, a token and optionally SWR ms",
            ));
        }
    };

    Ok(Command::FillSet {
        key: args.arg(0),
        value: args.arg(1),
        ttl_ms: parse_positive_u64(args.bytes(2), "FILLSET ttl must be a positive integer")?,
        token: parse_positive_u64(args.bytes(3), "FILLSET token must be a positive integer")?,
        swr_ms,
    })
}

fn parse_strlen<'a>(args: &Args<'a, '_>) -> Result<Command<'a>, ProtocolError> {
    match args.len() {
        1 => Ok(Command::StrLen(args.arg(0))),
        _ => Err(ProtocolError::Invalid("STRLEN expects one argument")),
    }
}

fn arity_invalid(name: &'static str) -> ProtocolError {
    ProtocolError::Invalid(match name {
        "keys" => "wrong number of arguments for 'keys' command",
        "randomkey" => "wrong number of arguments for 'randomkey' command",
        "touch" => "wrong number of arguments for 'touch' command",
        "renamenx" => "wrong number of arguments for 'renamenx' command",
        "expiretime" => "wrong number of arguments for 'expiretime' command",
        "pexpiretime" => "wrong number of arguments for 'pexpiretime' command",
        _ => "wrong number of arguments for 'copy' command",
    })
}

fn parse_keys<'a>(args: &Args<'a, '_>) -> Result<Command<'a>, ProtocolError> {
    match args.len() {
        1 => Ok(Command::Keys(args.arg(0))),
        _ => Err(arity_invalid("keys")),
    }
}

fn parse_randomkey<'a>(args: &Args<'a, '_>) -> Result<Command<'a>, ProtocolError> {
    match args.len() {
        0 => Ok(Command::RandomKey),
        _ => Err(arity_invalid("randomkey")),
    }
}

fn parse_touch<'a>(args: &Args<'a, '_>) -> Result<Command<'a>, ProtocolError> {
    if args.len() == 0 {
        Err(arity_invalid("touch"))
    } else {
        Ok(Command::Touch(keys_of(args)))
    }
}

fn parse_renamenx<'a>(args: &Args<'a, '_>) -> Result<Command<'a>, ProtocolError> {
    match args.len() {
        2 => Ok(Command::RenameNx {
            source: args.arg(0),
            destination: args.arg(1),
        }),
        _ => Err(arity_invalid("renamenx")),
    }
}

fn parse_expiretime<'a>(args: &Args<'a, '_>) -> Result<Command<'a>, ProtocolError> {
    match args.len() {
        1 => Ok(Command::ExpireTime {
            key: args.arg(0),
            millis: false,
        }),
        _ => Err(arity_invalid("expiretime")),
    }
}

fn parse_pexpiretime<'a>(args: &Args<'a, '_>) -> Result<Command<'a>, ProtocolError> {
    match args.len() {
        1 => Ok(Command::ExpireTime {
            key: args.arg(0),
            millis: true,
        }),
        _ => Err(arity_invalid("pexpiretime")),
    }
}

/// `COPY source destination [REPLACE]`. `DB` is refused: a copy stays in the selected database.
fn parse_copy<'a>(args: &Args<'a, '_>) -> Result<Command<'a>, ProtocolError> {
    if args.len() < 2 {
        return Err(arity_invalid("copy"));
    }

    let mut replace = false;

    for index in 2..args.len() {
        let option = args.bytes(index);

        if option.eq_ignore_ascii_case(b"REPLACE") {
            replace = true;
        } else if option.eq_ignore_ascii_case(b"DB") {
            return Err(ProtocolError::Invalid("COPY with DB is not supported"));
        } else {
            return Err(ProtocolError::Invalid("syntax error"));
        }
    }

    Ok(Command::Copy {
        source: args.arg(0),
        destination: args.arg(1),
        replace,
    })
}

fn parse_echo<'a>(args: &Args<'a, '_>) -> Result<Command<'a>, ProtocolError> {
    match args.len() {
        1 => Ok(Command::Echo(args.arg(0))),
        _ => Err(ProtocolError::Invalid(
            "wrong number of arguments for 'echo' command",
        )),
    }
}

fn parse_object<'a>(args: &Args<'a, '_>) -> Result<Command<'a>, ProtocolError> {
    if args.len() == 0 {
        return Err(ProtocolError::Invalid(
            "wrong number of arguments for 'object' command",
        ));
    }

    let kind = match args.bytes(0) {
        name if name.eq_ignore_ascii_case(b"ENCODING") => ObjectKind::Encoding,
        name if name.eq_ignore_ascii_case(b"IDLETIME") => ObjectKind::IdleTime,
        name if name.eq_ignore_ascii_case(b"FREQ") => ObjectKind::Freq,
        _ => {
            return Err(ProtocolError::Invalid(
                "unknown subcommand or wrong number of arguments for 'OBJECT'. Try OBJECT HELP.",
            ));
        }
    };

    if args.len() != 2 {
        return Err(ProtocolError::Invalid(match kind {
            ObjectKind::Encoding => "wrong number of arguments for 'object|encoding' command",
            ObjectKind::IdleTime => "wrong number of arguments for 'object|idletime' command",
            ObjectKind::Freq => "wrong number of arguments for 'object|freq' command",
        }));
    }

    Ok(Command::Object {
        kind,
        key: args.arg(1),
    })
}

/// `MEMORY USAGE key [SAMPLES count]`; the sample count is accepted and ignored because the
/// size is always read exactly.
fn parse_memory<'a>(args: &Args<'a, '_>) -> Result<Command<'a>, ProtocolError> {
    const UNKNOWN: &str =
        "unknown subcommand or wrong number of arguments for 'MEMORY'. Try MEMORY HELP.";

    if args.len() == 0 {
        return Err(ProtocolError::Invalid(
            "wrong number of arguments for 'memory' command",
        ));
    }

    if !args.bytes(0).eq_ignore_ascii_case(b"USAGE") || args.len() < 2 {
        return Err(ProtocolError::Invalid(UNKNOWN));
    }

    match args.len() {
        2 => {}
        4 if args.bytes(2).eq_ignore_ascii_case(b"SAMPLES") => {
            parse_integer_argument(args.bytes(3))?;
        }
        _ => return Err(ProtocolError::Invalid("syntax error")),
    }

    Ok(Command::MemoryUsage(args.arg(1)))
}

fn parse_slowlog<'a>(args: &Args<'a, '_>) -> Result<Command<'a>, ProtocolError> {
    const UNKNOWN: &str =
        "unknown subcommand or wrong number of arguments for 'SLOWLOG'. Try SLOWLOG HELP.";

    if args.len() == 0 {
        return Err(ProtocolError::Invalid(
            "wrong number of arguments for 'slowlog' command",
        ));
    }

    let name = args.bytes(0);

    if name.eq_ignore_ascii_case(b"GET") && args.len() <= 2 {
        let count = if args.len() == 2 {
            match parse_integer_argument(args.bytes(1))? {
                -1 => usize::MAX,
                count if count >= 0 => count as usize,
                _ => {
                    return Err(ProtocolError::Invalid(
                        "count should be greater than or equal to -1",
                    ));
                }
            }
        } else {
            10
        };

        return Ok(Command::Slowlog(SlowlogOp::Get(count)));
    }

    if name.eq_ignore_ascii_case(b"LEN") && args.len() == 1 {
        return Ok(Command::Slowlog(SlowlogOp::Len));
    }

    if name.eq_ignore_ascii_case(b"RESET") && args.len() == 1 {
        return Ok(Command::Slowlog(SlowlogOp::Reset));
    }

    Err(ProtocolError::Invalid(UNKNOWN))
}

/// `LATENCY LATEST` only: Ruvio keeps no latency-spike events, so the list is always empty.
fn parse_latency<'a>(args: &Args<'a, '_>) -> Result<Command<'a>, ProtocolError> {
    if args.len() == 0 {
        return Err(ProtocolError::Invalid(
            "wrong number of arguments for 'latency' command",
        ));
    }

    if args.bytes(0).eq_ignore_ascii_case(b"LATEST") && args.len() == 1 {
        return Ok(Command::LatencyLatest);
    }

    Err(ProtocolError::Invalid(
        "unknown subcommand or wrong number of arguments for 'LATENCY'. Try LATENCY HELP.",
    ))
}

fn parse_dump<'a>(args: &Args<'a, '_>) -> Result<Command<'a>, ProtocolError> {
    match args.len() {
        1 => Ok(Command::Dump(args.arg(0))),
        _ => Err(ProtocolError::Invalid(
            "wrong number of arguments for 'dump' command",
        )),
    }
}

/// `RESTORE key ttl payload [REPLACE] [ABSTTL] [IDLETIME s] [FREQ f]`. The last two are
/// accepted and ignored: access data is not carried by `DUMP`.
fn parse_restore<'a>(args: &Args<'a, '_>) -> Result<Command<'a>, ProtocolError> {
    if args.len() < 3 {
        return Err(ProtocolError::Invalid(
            "wrong number of arguments for 'restore' command",
        ));
    }

    let ttl = parse_integer_argument(args.bytes(1))?;
    let mut replace = false;
    let mut absolute = false;
    let mut index = 3;

    while index < args.len() {
        let option = args.bytes(index);

        if option.eq_ignore_ascii_case(b"REPLACE") {
            replace = true;
        } else if option.eq_ignore_ascii_case(b"ABSTTL") {
            absolute = true;
        } else if (option.eq_ignore_ascii_case(b"IDLETIME") || option.eq_ignore_ascii_case(b"FREQ"))
            && index + 1 < args.len()
        {
            parse_integer_argument(args.bytes(index + 1))?;
            index += 1;
        } else {
            return Err(ProtocolError::Invalid("syntax error"));
        }

        index += 1;
    }

    Ok(Command::Restore {
        key: args.arg(0),
        ttl,
        payload: args.arg(2),
        replace,
        absolute,
    })
}

fn parse_exists<'a>(args: &Args<'a, '_>) -> Result<Command<'a>, ProtocolError> {
    if args.len() == 0 {
        Err(ProtocolError::Invalid("EXISTS expects at least one key"))
    } else {
        Ok(Command::Exists(keys_of(args)))
    }
}

fn parse_hset<'a>(args: &Args<'a, '_>) -> Result<Command<'a>, ProtocolError> {
    if args.len() < 3 || args.len().is_multiple_of(2) {
        return Err(ProtocolError::Invalid(
            "HSET expects a key and field/value pairs",
        ));
    }

    let pairs = (1..args.len())
        .step_by(2)
        .map(|index| (args.arg(index), args.arg(index + 1)))
        .collect();

    Ok(Command::HashSet {
        key: args.arg(0),
        pairs,
    })
}

fn parse_lpush<'a>(args: &Args<'a, '_>) -> Result<Command<'a>, ProtocolError> {
    if args.len() < 2 {
        return Err(ProtocolError::Invalid(
            "LPUSH expects a key and at least one value",
        ));
    }

    Ok(Command::ListPush(args.arg(0), args.rest(1)))
}

fn parse_rpush<'a>(args: &Args<'a, '_>) -> Result<Command<'a>, ProtocolError> {
    if args.len() < 2 {
        return Err(ProtocolError::Invalid(
            "RPUSH expects a key and at least one value",
        ));
    }

    Ok(Command::ListPushBack(args.arg(0), args.rest(1)))
}

fn parse_zadd<'a>(args: &Args<'a, '_>) -> Result<Command<'a>, ProtocolError> {
    if args.len() == 0 {
        return Err(ProtocolError::Invalid(
            "ZADD expects a key and score/member pairs",
        ));
    }

    let mut options = SortedSetAddOptions::default();
    let mut index = 1;

    while index < args.len() {
        let option = args.bytes(index);

        if option.eq_ignore_ascii_case(b"NX") {
            if options.condition != SortedSetCondition::Always || options.greater || options.less {
                return Err(ProtocolError::Invalid("syntax error"));
            }

            options.condition = SortedSetCondition::NotExists;
        } else if option.eq_ignore_ascii_case(b"XX") {
            if options.condition != SortedSetCondition::Always {
                return Err(ProtocolError::Invalid("syntax error"));
            }

            options.condition = SortedSetCondition::Exists;
        } else if option.eq_ignore_ascii_case(b"GT") || option.eq_ignore_ascii_case(b"LT") {
            if options.greater || options.less || options.condition == SortedSetCondition::NotExists
            {
                return Err(ProtocolError::Invalid("syntax error"));
            }

            if option.eq_ignore_ascii_case(b"GT") {
                options.greater = true;
            } else {
                options.less = true;
            }
        } else if option.eq_ignore_ascii_case(b"CH") {
            options.changed = true;
        } else if option.eq_ignore_ascii_case(b"INCR") {
            options.increment = true;
        } else {
            break;
        }

        index += 1;
    }

    let remaining = args.len() - index;

    if remaining < 2 || !remaining.is_multiple_of(2) {
        return Err(ProtocolError::Invalid(
            "ZADD expects a key and score/member pairs",
        ));
    }

    let mut pairs = Vec::with_capacity(remaining / 2);

    for position in (index..args.len()).step_by(2) {
        pairs.push((parse_f64(args.bytes(position))?, args.arg(position + 1)));
    }

    if options.increment && pairs.len() != 1 {
        return Err(ProtocolError::Invalid(
            "INCR option supports a single increment-element pair",
        ));
    }

    Ok(Command::SortedSetAdd {
        key: args.arg(0),
        pairs,
        options,
    })
}

/// Commands ACL rules treat as one: enabling or disabling the canonical name covers every
/// alias. Each alias is a row of its own in `COMMANDS`.
const ACL_ALIASES: &[(&str, &str)] = &[
    ("decr", "incr"),
    ("incrby", "incr"),
    ("decrby", "incr"),
    ("unlink", "del"),
    ("georadiusbymember", "georadius"),
    ("jmerge", "json.merge"),
    ("json.forget", "json.del"),
    ("pexpire", "expire"),
    ("expireat", "expire"),
    ("pexpireat", "expire"),
    // These parse to the canonical command, so a rule on the alias alone could not be
    // enforced on every path (MULTI, scripts, forwarding).
    ("setex", "set"),
    ("psetex", "set"),
    ("substr", "getrange"),
    ("rpoplpush", "lmove"),
];

/// Canonical ACL command for `name` (any case, alias or not), or `None` for an unknown command.
pub(crate) fn acl_canonical(name: &str) -> Option<&'static str> {
    let spec = lookup(name.as_bytes())?;

    Some(
        ACL_ALIASES
            .iter()
            .find(|(alias, _)| *alias == spec.name)
            .map_or(spec.name, |(_, canonical)| canonical),
    )
}

/// One row per ACL command: the table without alias rows.
pub(crate) fn acl_commands() -> impl Iterator<Item = &'static CommandSpec> {
    COMMANDS
        .iter()
        .filter(|spec| !ACL_ALIASES.iter().any(|(alias, _)| *alias == spec.name))
}

fn parse_command_meta<'a>(args: &Args<'a, '_>) -> Result<Command<'a>, ProtocolError> {
    if args.len() == 0 {
        return Ok(Command::CommandMeta {
            kind: CommandMetaKind::List,
            names: Vec::new(),
        });
    }

    let subcommand = args.bytes(0);

    if subcommand.eq_ignore_ascii_case(b"COUNT") {
        return if args.len() == 1 {
            Ok(Command::CommandMeta {
                kind: CommandMetaKind::Count,
                names: Vec::new(),
            })
        } else {
            Err(ProtocolError::Invalid("COMMAND COUNT expects no arguments"))
        };
    }

    let kind = if subcommand.eq_ignore_ascii_case(b"INFO") {
        CommandMetaKind::Info
    } else if subcommand.eq_ignore_ascii_case(b"DOCS") {
        CommandMetaKind::Docs
    } else {
        return Err(ProtocolError::Invalid(
            "COMMAND supports COUNT, INFO and DOCS",
        ));
    };

    Ok(Command::CommandMeta {
        kind,
        names: args.rest(1),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_row_is_found_by_its_name_in_any_case() {
        for spec in COMMANDS {
            assert!(spec.name.len() <= MAX_NAME);
            assert!(std::ptr::eq(lookup(spec.name.as_bytes()).unwrap(), spec));
            assert!(std::ptr::eq(
                lookup(spec.name.to_ascii_uppercase().as_bytes()).unwrap(),
                spec
            ));
        }

        assert!(lookup(b"definitely-not-a-command").is_none());
        assert!(lookup(&[b'x'; MAX_NAME + 1]).is_none());
    }

    #[test]
    fn arity_matches_what_the_parsers_accept() {
        // A positive arity is exact: the name plus that many minus one arguments must
        // parse for commands whose arguments need no particular shape.
        let free_form = [
            "get", "strlen", "type", "ttl", "pttl", "persist", "llen", "scard", "smembers", "hlen",
            "hgetall", "hkeys", "hvals", "zcard", "xlen",
        ];

        for name in free_form {
            let spec = lookup(name.as_bytes()).unwrap();
            let arguments: Vec<Bytes<'_>> =
                (1..spec.arity).map(|_| Cow::Borrowed(&b"k"[..])).collect();
            let parse = spec.parse.unwrap();

            assert!(parse(&Args::from_slice(&arguments)).is_ok(), "{name}");

            let mut wrong = arguments.clone();

            wrong.push(Cow::Borrowed(&b"extra"[..]));
            assert!(parse(&Args::from_slice(&wrong)).is_err(), "{name}");
        }
    }

    #[test]
    fn acl_aliases_resolve_to_a_canonical_row() {
        for (alias, canonical) in ACL_ALIASES {
            assert!(lookup(alias.as_bytes()).is_some(), "{alias}");
            assert!(lookup(canonical.as_bytes()).is_some(), "{canonical}");
            assert_eq!(acl_canonical(alias), Some(*canonical));
        }

        assert_eq!(acl_canonical("GET"), Some("get"));
        assert_eq!(acl_canonical("nope"), None);
    }
}
