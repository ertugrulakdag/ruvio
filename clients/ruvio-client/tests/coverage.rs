//! Fails when a command in `docs/compatibility-matrix.json` has no typed method.
//!
//! `TABLE` maps every matrix name to the typed method(s) that send it. The test checks that
//! the table and the matrix list the same commands and that each method is a real `pub fn`
//! of the crate; `methods_exist` makes the compiler check the same names.

use std::collections::BTreeSet;
use std::path::Path;

use ruvio_client::Client;

const TABLE: &[(&str, &[&str])] = &[
    ("RUVIO", &["ruvio"]),
    ("PING", &["ping", "ping_message"]),
    ("HELLO", &["hello"]),
    ("AUTH", &["auth", "auth_user"]),
    ("ACL WHOAMI", &["acl_whoami"]),
    ("ACL USERS", &["acl_users"]),
    ("ACL LIST", &["acl_list"]),
    ("ACL GETUSER", &["acl_getuser"]),
    ("ACL CAT", &["acl_cat"]),
    ("ACL SETUSER", &["acl_setuser"]),
    ("ACL LOAD", &["acl_load"]),
    ("ACL SAVE", &["acl_save"]),
    ("GET", &["get", "get_string"]),
    ("SETV", &["setv"]),
    ("TAKE", &["take"]),
    ("DELEX", &["delex", "release_lock"]),
    ("GETEX", &["getex"]),
    ("LIMIT", &["limit", "rate_limit"]),
    (
        "IDEM",
        &[
            "idempotency_begin",
            "idempotency_complete",
            "idem_begin",
            "idem_complete",
        ],
    ),
    ("ONCE", &["once"]),
    ("LEASE", &["lease", "lease_guard"]),
    ("SEMAPHORE", &["semaphore", "semaphore_permit"]),
    ("RELEASE", &["release"]),
    ("CHANGES", &["changes_start", "changes"]),
    (
        "SET",
        &[
            "set",
            "set_with",
            "set_expires_at",
            "set_keep_ttl",
            "set_and_get",
            "set_if_eq",
            "set_if_eq_keep_ttl",
            "set_ex",
            "set_exat",
            "set_bytes",
        ],
    ),
    ("MGET", &["mget"]),
    ("MSET", &["mset"]),
    ("GETSET", &["getset"]),
    ("APPEND", &["append"]),
    ("STRLEN", &["strlen"]),
    ("INCR", &["incr"]),
    ("DECR", &["decr"]),
    ("INCRBY", &["incr_by", "incr_by_max"]),
    ("DECRBY", &["decr_by"]),
    ("DEL", &["del", "del_key"]),
    ("UNLINK", &["unlink", "unlink_key"]),
    ("EXISTS", &["exists", "exists_key"]),
    ("TYPE", &["key_type"]),
    ("RENAME", &["rename"]),
    ("SCAN", &["scan", "scan_iter"]),
    ("DBSIZE", &["dbsize"]),
    ("SELECT", &["select"]),
    ("FLUSHDB", &["flushdb"]),
    ("FLUSHALL", &["flushall"]),
    ("SWAPDB", &["swapdb"]),
    ("MOVE", &["move_key"]),
    ("EXPIRE", &["expire"]),
    ("PEXPIRE", &["expire", "pexpire"]),
    ("EXPIREAT", &["expire_at"]),
    ("PEXPIREAT", &["expire_at"]),
    ("TTL", &["ttl"]),
    ("PTTL", &["pttl"]),
    ("PERSIST", &["persist"]),
    ("LPUSH", &["lpush"]),
    ("LPUSHCAP", &["lpushcap", "push_recent"]),
    ("RPUSHCAP", &["rpushcap"]),
    ("RPUSH", &["rpush"]),
    ("LPOP", &["lpop", "lpop_count"]),
    ("RPOP", &["rpop", "rpop_count"]),
    ("LLEN", &["llen"]),
    ("LINDEX", &["lindex"]),
    ("LRANGE", &["lrange"]),
    ("LTRIM", &["ltrim"]),
    ("BLPOP", &["blpop", "blpop_key"]),
    ("BRPOP", &["brpop", "brpop_key"]),
    ("SADD", &["sadd"]),
    ("SREM", &["srem"]),
    ("SISMEMBER", &["sismember"]),
    ("SCARD", &["scard"]),
    ("SMEMBERS", &["smembers"]),
    ("SINTER", &["sinter"]),
    ("SUNION", &["sunion"]),
    ("SDIFF", &["sdiff"]),
    ("SINTERSTORE", &["sinterstore"]),
    ("SUNIONSTORE", &["sunionstore"]),
    ("SDIFFSTORE", &["sdiffstore"]),
    ("SMOVE", &["smove"]),
    ("SPOP", &["spop", "spop_count"]),
    ("SRANDMEMBER", &["srandmember", "srandmember_count"]),
    ("HSET", &["hset"]),
    ("HGET", &["hget"]),
    ("HDEL", &["hdel"]),
    ("HLEN", &["hlen"]),
    ("HGETALL", &["hgetall"]),
    ("HMGET", &["hmget"]),
    ("HEXISTS", &["hexists"]),
    ("HKEYS", &["hkeys"]),
    ("HVALS", &["hvals"]),
    ("HINCRBY", &["hincr_by"]),
    ("HSETNX", &["hsetnx"]),
    ("HSTRLEN", &["hstrlen"]),
    ("HSCAN", &["hscan", "hscan_iter"]),
    ("HEXPIRE", &["hexpire_with"]),
    ("HPEXPIRE", &["hexpire_with", "hpexpire"]),
    ("HEXPIREAT", &["hexpire_at"]),
    ("HPEXPIREAT", &["hexpire_at"]),
    ("HTTL", &["httl"]),
    ("HPTTL", &["hpttl"]),
    ("HEXPIRETIME", &["hexpiretime"]),
    ("HPEXPIRETIME", &["hpexpiretime"]),
    ("HPERSIST", &["hpersist"]),
    ("ZADD", &["zadd", "zadd_with", "zadd_incr"]),
    (
        "ZRANGE",
        &[
            "zrange",
            "zrange_with_scores",
            "zrange_by_lex",
            "zrange_query",
            "zrange_query_with_scores",
        ],
    ),
    ("ZREVRANGE", &["zrevrange", "zrevrange_with_scores"]),
    (
        "ZRANGEBYSCORE",
        &["zrange_by_score_with_scores", "zrange_by_score"],
    ),
    ("ZRANGEBYLEX", &["zrangebylex"]),
    ("ZREVRANGEBYLEX", &["zrevrangebylex"]),
    ("ZREM", &["zrem"]),
    ("ZINCRBY", &["zincr_by"]),
    ("ZCARD", &["zcard"]),
    ("ZSCORE", &["zscore"]),
    ("ZRANK", &["zrank"]),
    ("ZREVRANK", &["zrevrank"]),
    ("ZPOPMIN", &["zpopmin"]),
    ("ZPOPMAX", &["zpopmax"]),
    ("BZPOPMIN", &["bzpopmin"]),
    ("BZPOPMAX", &["bzpopmax"]),
    ("ZREMRANGEBYRANK", &["zremrangebyrank"]),
    ("ZREMRANGEBYSCORE", &["zremrangebyscore"]),
    ("ZREMRANGEBYLEX", &["zremrangebylex"]),
    ("GEOADD", &["geo_add", "geo_add_with"]),
    ("GEODIST", &["geo_dist"]),
    ("GEOHASH", &["geo_hash"]),
    ("GEOPOS", &["geo_pos"]),
    ("GEOSEARCH", &["geo_search", "geo_search_with"]),
    ("GEOSEARCHSTORE", &["geo_search_store"]),
    ("GEORADIUS", &["geo_radius"]),
    ("GEORADIUSBYMEMBER", &["geo_radius_by_member"]),
    ("BF.RESERVE", &["bf_reserve", "bf_reserve_with"]),
    ("BF.ADD", &["bf_add"]),
    ("BF.MADD", &["bf_madd", "seen_before"]),
    ("BF.INSERT", &["bf_insert"]),
    ("BF.INFO", &["bf_info", "bf_info_field"]),
    ("BF.CARD", &["bf_card"]),
    ("BF.EXISTS", &["bf_exists"]),
    ("BF.MEXISTS", &["bf_mexists", "maybe_seen"]),
    ("JSON.GET", &["json_get"]),
    (
        "JSON.INDEX",
        &["json_index_create", "json_index_drop", "json_index_list"],
    ),
    ("JSON.FIND", &["json_find"]),
    ("JSON.MGET", &["json_mget"]),
    ("JSON.SET", &["json_set", "json_set_with"]),
    ("JSON.DEL", &["json_del"]),
    (
        "XADD",
        &["xadd_nomkstream", "xadd_delay", "xadd", "xadd_maxlen"],
    ),
    ("XLEN", &["xlen"]),
    ("XRANGE", &["xrange"]),
    ("XREVRANGE", &["xrevrange"]),
    ("XREAD", &["xread"]),
    ("XDEL", &["xdel"]),
    ("XSETID", &["xsetid"]),
    ("XTRIM", &["xtrim_minid", "xtrim_maxlen"]),
    ("XGROUP CREATE", &["xgroup_create"]),
    ("XGROUP CREATECONSUMER", &["xgroup_create_consumer"]),
    ("XREADGROUP", &["xreadgroup"]),
    ("XGROUP DESTROY", &["xgroup_destroy"]),
    ("XGROUP SETID", &["xgroup_setid"]),
    ("XGROUP DELCONSUMER", &["xgroup_delconsumer"]),
    ("XACK", &["xack"]),
    ("XPENDING", &["xpending_summary", "xpending"]),
    ("XAUTOCLAIM", &["xautoclaim"]),
    ("XCLAIM", &["xclaim", "xclaim_ids"]),
    ("XINFO STREAM", &["xinfo_stream"]),
    ("XINFO GROUPS", &["xinfo_groups"]),
    ("XINFO CONSUMERS", &["xinfo_consumers"]),
    ("INFO", &["info"]),
    ("WAIT", &["wait"]),
    ("CONFIG GET", &["config_get"]),
    ("SAVE", &["save"]),
    ("BGSAVE", &["bgsave"]),
    ("MULTI", &["multi"]),
    ("EXEC", &["exec"]),
    ("DISCARD", &["discard"]),
    ("WATCH", &["watch", "watch_key"]),
    ("UNWATCH", &["unwatch"]),
    ("EVAL", &["eval"]),
    ("EVALSHA", &["evalsha"]),
    ("SCRIPT LOAD", &["script_load"]),
    ("SCRIPT EXISTS", &["script_exists"]),
    ("SCRIPT FLUSH", &["script_flush"]),
    ("SCRIPT KILL", &["script_kill"]),
    ("FCALL", &["fcall"]),
    ("FUNCTION LOAD", &["function_load"]),
    ("FUNCTION LIST", &["function_list"]),
    ("FUNCTION DELETE", &["function_delete"]),
    ("CLUSTER SLOTS", &["cluster_slots"]),
    ("CLUSTER NODES", &["cluster_nodes"]),
    ("CLUSTER SHARDS", &["cluster_shards"]),
    ("CLUSTER INFO", &["cluster_info"]),
    ("CLUSTER MYID", &["cluster_myid"]),
    ("CLUSTER KEYSLOT", &["cluster_keyslot"]),
    ("READONLY", &["readonly"]),
    ("READWRITE", &["readwrite"]),
    ("SPUBLISH", &["spublish"]),
    ("SSUBSCRIBE", &["ssubscribe"]),
    ("SUNSUBSCRIBE", &["sunsubscribe"]),
    ("PUBLISH", &["publish"]),
    ("SUBSCRIBE", &["subscribe", "subscriber"]),
    ("UNSUBSCRIBE", &["unsubscribe"]),
    ("PSUBSCRIBE", &["psubscribe"]),
    ("PUNSUBSCRIBE", &["punsubscribe"]),
    ("CLIENT GETNAME", &["client_getname"]),
    ("CLIENT SETNAME", &["client_setname"]),
    ("CLIENT TRACKING", &["client_tracking"]),
    ("SSCAN", &["sscan", "sscan_iter"]),
    ("ZSCAN", &["zscan", "zscan_iter"]),
    ("JSON.MERGE", &["json_merge"]),
    ("JSON.TYPE", &["json_type"]),
    ("JSON.STRLEN", &["json_strlen"]),
    ("JSON.ARRLEN", &["json_arrlen"]),
    ("JSON.OBJKEYS", &["json_objkeys"]),
    ("JSON.NUMINCRBY", &["json_numincrby"]),
    ("JSON.TOGGLE", &["json_toggle"]),
    ("JSON.ARRAPPEND", &["json_arrappend", "json_arrappend_many"]),
    ("JSON.CLEAR", &["json_clear"]),
    ("JSON.OBJLEN", &["json_objlen"]),
    ("JSON.STRAPPEND", &["json_strappend"]),
    ("JSON.NUMMULTBY", &["json_nummultby"]),
    ("JSON.ARRINSERT", &["json_arrinsert"]),
    ("JSON.ARRPOP", &["json_arrpop"]),
    ("JSON.ARRTRIM", &["json_arrtrim"]),
    ("JSON.ARRINDEX", &["json_arrindex"]),
    ("JSON.FORGET", &["json_forget"]),
    ("JMERGE", &["jmerge"]),
    (
        "COMMAND",
        &[
            "command_list",
            "command_count",
            "command_info",
            "command_docs",
        ],
    ),
    ("SETBIT", &["setbit"]),
    ("BITFIELD", &["bitfield"]),
    ("BITFIELD_RO", &["bitfield_ro"]),
    ("GETBIT", &["getbit"]),
    ("BITCOUNT", &["bitcount", "bitcount_range"]),
    ("BITPOS", &["bitpos", "bitpos_range"]),
    ("BITOP", &["bitop"]),
    ("PFADD", &["pfadd"]),
    ("PFCOUNT", &["pfcount"]),
    ("PFMERGE", &["pfmerge"]),
    ("FILLGET", &["fill_get", "fill_get_wait"]),
    ("FILLSET", &["fill_set"]),
    ("SETEX", &["setex"]),
    ("PSETEX", &["psetex"]),
    ("SETNX", &["setnx"]),
    ("GETDEL", &["getdel"]),
    ("MSETNX", &["msetnx"]),
    ("INCRBYFLOAT", &["incr_by_float"]),
    ("GETRANGE", &["getrange"]),
    ("SUBSTR", &["substr"]),
    ("SETRANGE", &["setrange"]),
    ("SMISMEMBER", &["smismember"]),
    ("SINTERCARD", &["sintercard"]),
    ("ZCOUNT", &["zcount"]),
    ("ZLEXCOUNT", &["zlexcount"]),
    ("ZMSCORE", &["zmscore"]),
    ("ZUNIONSTORE", &["zunionstore"]),
    ("ZINTERSTORE", &["zinterstore"]),
    ("ZDIFFSTORE", &["zdiffstore"]),
    ("ZUNION", &["zunion", "zunion_with_scores"]),
    ("ZINTER", &["zinter", "zinter_with_scores"]),
    ("ZDIFF", &["zdiff", "zdiff_with_scores"]),
    (
        "ZRANDMEMBER",
        &[
            "zrandmember",
            "zrandmember_count",
            "zrandmember_with_scores",
        ],
    ),
    (
        "HRANDFIELD",
        &["hrandfield", "hrandfield_count", "hrandfield_with_values"],
    ),
    ("HINCRBYFLOAT", &["hincr_by_float"]),
    ("LINSERT", &["linsert"]),
    ("LSET", &["lset"]),
    ("LREM", &["lrem"]),
    ("LPOS", &["lpos", "lpos_with", "lpos_count"]),
    ("LMOVE", &["lmove"]),
    ("RPOPLPUSH", &["rpoplpush"]),
    ("BLMOVE", &["blmove"]),
    ("LPUSHX", &["lpushx"]),
    ("RPUSHX", &["rpushx"]),
    ("LMPOP", &["lmpop"]),
    ("BLMPOP", &["blmpop"]),
    ("KEYS", &["keys"]),
    ("RANDOMKEY", &["random_key"]),
    ("TOUCH", &["touch"]),
    ("RENAMENX", &["rename_nx"]),
    ("EXPIRETIME", &["expire_time"]),
    ("PEXPIRETIME", &["pexpire_time"]),
    ("COPY", &["copy"]),
    ("ECHO", &["echo"]),
    (
        "OBJECT",
        &["object_encoding", "object_idletime", "object_freq"],
    ),
    ("MEMORY", &["memory_usage", "memory_usage_samples"]),
    ("CLIENT ID", &["client_id"]),
    ("CLIENT LIST", &["client_list"]),
    ("CLIENT INFO", &["client_info"]),
    ("CLIENT KILL", &["client_kill_id", "client_kill_addr"]),
    ("SLOWLOG", &["slowlog_len", "slowlog_reset", "slowlog_get"]),
    ("LATENCY", &["latency_latest"]),
    ("DUMP", &["dump"]),
    ("RESTORE", &["restore"]),
];

fn matrix_names() -> Vec<String> {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../docs/compatibility-matrix.json");
    let Ok(text) = std::fs::read_to_string(&path) else {
        // Outside the repository (for example a published crate) there is no matrix to check.
        eprintln!("skipping: {} not found", path.display());

        return Vec::new();
    };
    let matrix: serde_json::Value = serde_json::from_str(&text).expect("matrix is JSON");

    matrix["commands"]
        .as_array()
        .expect("commands array")
        .iter()
        .map(|command| command["name"].as_str().expect("name").to_owned())
        .collect()
}

fn crate_source() -> String {
    let mut source = String::new();
    let directory = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");

    for entry in std::fs::read_dir(directory).expect("src") {
        let path = entry.expect("entry").path();

        if path.extension().is_some_and(|extension| extension == "rs") {
            source.push_str(&std::fs::read_to_string(path).expect("source"));
        }
    }

    source
}

#[test]
fn every_matrix_command_has_a_typed_method() {
    let names = matrix_names();

    if names.is_empty() {
        return;
    }

    let table: BTreeSet<&str> = TABLE.iter().map(|(name, _)| *name).collect();
    let missing: Vec<&String> = names
        .iter()
        .filter(|name| !table.contains(name.as_str()))
        .collect();
    let stale: Vec<&&str> = table
        .iter()
        .filter(|name| !names.iter().any(|known| known == *name))
        .collect();

    assert!(
        missing.is_empty(),
        "matrix commands without a typed method: {missing:?}"
    );
    assert!(stale.is_empty(), "table rows not in the matrix: {stale:?}");
    assert_eq!(TABLE.len(), names.len(), "duplicate table rows");

    let source = crate_source();

    for (name, methods) in TABLE {
        assert!(!methods.is_empty(), "{name} maps to no method");

        for method in *methods {
            assert!(
                source.contains(&format!("pub fn {method}("))
                    || source.contains(&format!("pub fn {method}<")),
                "{name}: method {method} is not a pub fn of the crate"
            );
        }
    }

    println!(
        "typed coverage: {}/{} matrix commands",
        TABLE.len(),
        names.len()
    );
}

#[allow(dead_code)]
fn methods_exist() {
    let _ = Client::acl_cat;
    let _ = Client::acl_getuser;
    let _ = Client::acl_list;
    let _ = Client::acl_load;
    let _ = Client::acl_save;
    let _ = Client::acl_setuser;
    let _ = Client::acl_users;
    let _ = Client::acl_whoami;
    let _ = Client::append;
    let _ = Client::auth;
    let _ = Client::auth_user;
    let _ = Client::bf_add;
    let _ = Client::bf_card;
    let _ = Client::bf_exists;
    let _ = Client::bf_info;
    let _ = Client::bf_info_field;
    let _ = Client::bf_insert;
    let _ = Client::bf_madd;
    let _ = Client::bf_mexists;
    let _ = Client::bf_reserve;
    let _ = Client::bf_reserve_with;
    let _ = Client::bgsave;
    let _ = Client::bitcount;
    let _ = Client::bitcount_range;
    let _ = Client::bitfield;
    let _ = Client::bitfield_ro;
    let _ = Client::bitop;
    let _ = Client::bitpos;
    let _ = Client::bitpos_range;
    let _ = Client::blmove;
    let _ = Client::blmpop;
    let _ = Client::blpop;
    let _ = Client::blpop_key;
    let _ = Client::brpop;
    let _ = Client::brpop_key;
    let _ = Client::bzpopmax;
    let _ = Client::bzpopmin;
    let _ = Client::changes;
    let _ = Client::changes_start;
    let _ = Client::client_getname;
    let _ = Client::client_id;
    let _ = Client::client_info;
    let _ = Client::client_kill_addr;
    let _ = Client::client_kill_id;
    let _ = Client::client_list;
    let _ = Client::client_setname;
    let _ = Client::client_tracking;
    let _ = Client::cluster_info;
    let _ = Client::cluster_keyslot;
    let _ = Client::cluster_myid;
    let _ = Client::cluster_nodes;
    let _ = Client::cluster_shards;
    let _ = Client::cluster_slots;
    let _ = Client::command_count;
    let _ = Client::command_docs;
    let _ = Client::command_info;
    let _ = Client::command_list;
    let _ = Client::config_get;
    let _ = Client::copy;
    let _ = Client::dbsize;
    let _ = Client::decr;
    let _ = Client::decr_by;
    let _ = Client::del;
    let _ = Client::del_key;
    let _ = Client::delex;
    let _ = Client::discard;
    let _ = Client::dump;
    let _ = Client::echo;
    let _ = Client::eval;
    let _ = Client::evalsha;
    let _ = Client::exec;
    let _ = Client::exists;
    let _ = Client::exists_key;
    let _ = Client::expire;
    let _ = Client::expire_at;
    let _ = Client::expire_time;
    let _ = Client::fcall;
    let _ = Client::fill_get;
    let _ = Client::fill_get_wait;
    let _ = Client::fill_set;
    let _ = Client::flushall;
    let _ = Client::flushdb;
    let _ = Client::function_delete;
    let _ = Client::function_list;
    let _ = Client::function_load;
    let _ = Client::geo_add;
    let _ = Client::geo_add_with;
    let _ = Client::geo_dist;
    let _ = Client::geo_hash;
    let _ = Client::geo_pos;
    let _ = Client::geo_radius;
    let _ = Client::geo_radius_by_member;
    let _ = Client::geo_search;
    let _ = Client::geo_search_store;
    let _ = Client::geo_search_with;
    let _ = Client::get;
    let _ = Client::get_string;
    let _ = Client::getbit;
    let _ = Client::getdel;
    let _ = Client::getex;
    let _ = Client::getrange;
    let _ = Client::getset;
    let _ = Client::hdel;
    let _ = Client::hello;
    let _ = Client::hexists;
    let _ = Client::hexpire_at;
    let _ = Client::hexpire_with;
    let _ = Client::hexpiretime;
    let _ = Client::hget;
    let _ = Client::hgetall;
    let _ = Client::hincr_by;
    let _ = Client::hincr_by_float;
    let _ = Client::hkeys;
    let _ = Client::hlen;
    let _ = Client::hmget;
    let _ = Client::hpersist;
    let _ = Client::hpexpire;
    let _ = Client::hpexpiretime;
    let _ = Client::hpttl;
    let _ = Client::hrandfield;
    let _ = Client::hrandfield_count;
    let _ = Client::hrandfield_with_values;
    let _ = Client::hscan;
    let _ = Client::hscan_iter;
    let _ = Client::hset;
    let _ = Client::hsetnx;
    let _ = Client::hstrlen;
    let _ = Client::httl;
    let _ = Client::hvals;
    let _ = Client::idem_begin;
    let _ = Client::idem_complete;
    let _ = Client::idempotency_begin;
    let _ = Client::idempotency_complete;
    let _ = Client::incr;
    let _ = Client::incr_by;
    let _ = Client::incr_by_float;
    let _ = Client::incr_by_max;
    let _ = Client::info;
    let _ = Client::jmerge;
    let _ = Client::json_arrappend;
    let _ = Client::json_arrappend_many;
    let _ = Client::json_arrindex;
    let _ = Client::json_arrinsert;
    let _ = Client::json_arrlen;
    let _ = Client::json_arrpop;
    let _ = Client::json_arrtrim;
    let _ = Client::json_clear;
    let _ = Client::json_del;
    let _ = Client::json_find;
    let _ = Client::json_forget;
    let _ = Client::json_get;
    let _ = Client::json_index_create;
    let _ = Client::json_index_drop;
    let _ = Client::json_index_list;
    let _ = Client::json_merge;
    let _ = Client::json_mget;
    let _ = Client::json_numincrby;
    let _ = Client::json_nummultby;
    let _ = Client::json_objkeys;
    let _ = Client::json_objlen;
    let _ = Client::json_set;
    let _ = Client::json_set_with;
    let _ = Client::json_strappend;
    let _ = Client::json_strlen;
    let _ = Client::json_toggle;
    let _ = Client::json_type;
    let _ = Client::key_type;
    let _ = Client::keys;
    let _ = Client::latency_latest;
    let _ = Client::lease;
    let _ = Client::lease_guard;
    let _ = Client::limit;
    let _ = Client::lindex;
    let _ = Client::linsert;
    let _ = Client::llen;
    let _ = Client::lmove;
    let _ = Client::lmpop;
    let _ = Client::lpop;
    let _ = Client::lpop_count;
    let _ = Client::lpos;
    let _ = Client::lpos_count;
    let _ = Client::lpos_with;
    let _ = Client::lpush;
    let _ = Client::lpushcap;
    let _ = Client::lpushx;
    let _ = Client::lrange;
    let _ = Client::lrem;
    let _ = Client::lset;
    let _ = Client::ltrim;
    let _ = Client::maybe_seen;
    let _ = Client::memory_usage;
    let _ = Client::memory_usage_samples;
    let _ = Client::mget;
    let _ = Client::move_key;
    let _ = Client::mset;
    let _ = Client::msetnx;
    let _ = Client::multi;
    let _ = Client::object_encoding;
    let _ = Client::object_freq;
    let _ = Client::object_idletime;
    let _ = Client::once;
    let _ = Client::persist;
    let _ = Client::pexpire;
    let _ = Client::pexpire_time;
    let _ = Client::pfadd;
    let _ = Client::pfcount;
    let _ = Client::pfmerge;
    let _ = Client::ping;
    let _ = Client::ping_message;
    let _ = Client::psetex;
    let _ = Client::psubscribe;
    let _ = Client::pttl;
    let _ = Client::publish;
    let _ = Client::punsubscribe;
    let _ = Client::push_recent;
    let _ = Client::random_key;
    let _ = Client::rate_limit;
    let _ = Client::readonly;
    let _ = Client::readwrite;
    let _ = Client::release;
    let _ = Client::release_lock;
    let _ = Client::rename;
    let _ = Client::rename_nx;
    let _ = Client::restore;
    let _ = Client::rpop;
    let _ = Client::rpop_count;
    let _ = Client::rpoplpush;
    let _ = Client::rpush;
    let _ = Client::rpushcap;
    let _ = Client::rpushx;
    let _ = Client::ruvio;
    let _ = Client::sadd;
    let _ = Client::save;
    let _ = Client::scan;
    let _ = Client::scan_iter;
    let _ = Client::scard;
    let _ = Client::script_exists;
    let _ = Client::script_flush;
    let _ = Client::script_kill;
    let _ = Client::script_load;
    let _ = Client::sdiff;
    let _ = Client::sdiffstore;
    let _ = Client::seen_before;
    let _ = Client::select;
    let _ = Client::semaphore;
    let _ = Client::semaphore_permit;
    let _ = Client::set;
    let _ = Client::set_and_get;
    let _ = Client::set_bytes;
    let _ = Client::set_ex;
    let _ = Client::set_exat;
    let _ = Client::set_expires_at;
    let _ = Client::set_if_eq;
    let _ = Client::set_if_eq_keep_ttl;
    let _ = Client::set_keep_ttl;
    let _ = Client::set_with;
    let _ = Client::setbit;
    let _ = Client::setex;
    let _ = Client::setnx;
    let _ = Client::setrange;
    let _ = Client::setv;
    let _ = Client::sinter;
    let _ = Client::sintercard;
    let _ = Client::sinterstore;
    let _ = Client::sismember;
    let _ = Client::slowlog_get;
    let _ = Client::slowlog_len;
    let _ = Client::slowlog_reset;
    let _ = Client::smembers;
    let _ = Client::smismember;
    let _ = Client::smove;
    let _ = Client::spop;
    let _ = Client::spop_count;
    let _ = Client::spublish;
    let _ = Client::srandmember;
    let _ = Client::srandmember_count;
    let _ = Client::srem;
    let _ = Client::sscan;
    let _ = Client::sscan_iter;
    let _ = Client::ssubscribe;
    let _ = Client::strlen;
    let _ = Client::subscribe;
    let _ = Client::subscriber;
    let _ = Client::substr;
    let _ = Client::sunion;
    let _ = Client::sunionstore;
    let _ = Client::sunsubscribe;
    let _ = Client::swapdb;
    let _ = Client::take;
    let _ = Client::touch;
    let _ = Client::ttl;
    let _ = Client::unlink;
    let _ = Client::unlink_key;
    let _ = Client::unsubscribe;
    let _ = Client::unwatch;
    let _ = Client::wait;
    let _ = Client::watch;
    let _ = Client::watch_key;
    let _ = Client::xack;
    let _ = Client::xadd;
    let _ = Client::xadd_delay;
    let _ = Client::xadd_maxlen;
    let _ = Client::xadd_nomkstream;
    let _ = Client::xautoclaim;
    let _ = Client::xclaim;
    let _ = Client::xclaim_ids;
    let _ = Client::xdel;
    let _ = Client::xgroup_create;
    let _ = Client::xgroup_create_consumer;
    let _ = Client::xgroup_delconsumer;
    let _ = Client::xgroup_destroy;
    let _ = Client::xgroup_setid;
    let _ = Client::xinfo_consumers;
    let _ = Client::xinfo_groups;
    let _ = Client::xinfo_stream;
    let _ = Client::xlen;
    let _ = Client::xpending;
    let _ = Client::xpending_summary;
    let _ = Client::xrange;
    let _ = Client::xread;
    let _ = Client::xreadgroup;
    let _ = Client::xrevrange;
    let _ = Client::xsetid;
    let _ = Client::xtrim_maxlen;
    let _ = Client::xtrim_minid;
    let _ = Client::zadd;
    let _ = Client::zadd_incr;
    let _ = Client::zadd_with;
    let _ = Client::zcard;
    let _ = Client::zcount;
    let _ = Client::zdiff;
    let _ = Client::zdiff_with_scores;
    let _ = Client::zdiffstore;
    let _ = Client::zincr_by;
    let _ = Client::zinter;
    let _ = Client::zinter_with_scores;
    let _ = Client::zinterstore;
    let _ = Client::zlexcount;
    let _ = Client::zmscore;
    let _ = Client::zpopmax;
    let _ = Client::zpopmin;
    let _ = Client::zrandmember;
    let _ = Client::zrandmember_count;
    let _ = Client::zrandmember_with_scores;
    let _ = Client::zrange;
    let _ = Client::zrange_by_lex;
    let _ = Client::zrange_by_score;
    let _ = Client::zrange_by_score_with_scores;
    let _ = Client::zrange_query;
    let _ = Client::zrange_query_with_scores;
    let _ = Client::zrange_with_scores;
    let _ = Client::zrangebylex;
    let _ = Client::zrank;
    let _ = Client::zrem;
    let _ = Client::zremrangebylex;
    let _ = Client::zremrangebyrank;
    let _ = Client::zremrangebyscore;
    let _ = Client::zrevrange;
    let _ = Client::zrevrange_with_scores;
    let _ = Client::zrevrangebylex;
    let _ = Client::zrevrank;
    let _ = Client::zscan;
    let _ = Client::zscan_iter;
    let _ = Client::zscore;
    let _ = Client::zunion;
    let _ = Client::zunion_with_scores;
    let _ = Client::zunionstore;
}
