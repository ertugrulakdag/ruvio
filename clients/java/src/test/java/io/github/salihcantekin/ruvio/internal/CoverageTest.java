// SPDX-License-Identifier: MIT
package io.github.salihcantekin.ruvio.internal;

import io.github.salihcantekin.ruvio.RuvioClient;
import io.github.salihcantekin.ruvio.RuvioAsync;
import io.github.salihcantekin.ruvio.Pipeline;
import io.github.salihcantekin.ruvio.Transaction;
import io.github.salihcantekin.ruvio.RuvioPubSub;


import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.junit.jupiter.api.Test;

/**
 * Fails when a command of docs/compatibility-matrix.json has no typed method. The table maps each
 * matrix command to "method" (on RuvioClient and RuvioAsync) or "Class#method".
 */
class CoverageTest {
    static Map<String, String> table() {
        Map<String, String> m = new LinkedHashMap<>();
        m.put("RUVIO", "ruvio");
        m.put("PING", "ping");
        m.put("HELLO", "hello");
        m.put("AUTH", "auth");
        m.put("ACL WHOAMI", "aclWhoami");
        m.put("ACL USERS", "aclUsers");
        m.put("ACL LIST", "aclList");
        m.put("ACL GETUSER", "aclGetuser");
        m.put("ACL CAT", "aclCat");
        m.put("ACL SETUSER", "aclSetuser");
        m.put("ACL LOAD", "aclLoad");
        m.put("ACL SAVE", "aclSave");
        m.put("GET", "get");
        m.put("SETV", "setv");
        m.put("TAKE", "take");
        m.put("DELEX", "delex");
        m.put("GETEX", "getex");
        m.put("LIMIT", "limit");
        m.put("IDEM", "idemBegin");
        m.put("ONCE", "once");
        m.put("LEASE", "lease");
        m.put("SEMAPHORE", "semaphore");
        m.put("RELEASE", "release");
        m.put("CHANGES", "changes");
        m.put("SET", "set");
        m.put("MGET", "mget");
        m.put("MSET", "mset");
        m.put("GETSET", "getset");
        m.put("APPEND", "append");
        m.put("STRLEN", "strlen");
        m.put("INCR", "incr");
        m.put("DECR", "decr");
        m.put("INCRBY", "incrby");
        m.put("DECRBY", "decrby");
        m.put("DEL", "del");
        m.put("UNLINK", "unlink");
        m.put("EXISTS", "exists");
        m.put("TYPE", "type");
        m.put("RENAME", "rename");
        m.put("SCAN", "scan");
        m.put("DBSIZE", "dbsize");
        m.put("SELECT", "select");
        m.put("FLUSHDB", "flushdb");
        m.put("FLUSHALL", "flushall");
        m.put("SWAPDB", "swapdb");
        m.put("MOVE", "move");
        m.put("EXPIRE", "expire");
        m.put("PEXPIRE", "pexpire");
        m.put("EXPIREAT", "expireat");
        m.put("PEXPIREAT", "pexpireat");
        m.put("TTL", "ttl");
        m.put("PTTL", "pttl");
        m.put("PERSIST", "persist");
        m.put("LPUSH", "lpush");
        m.put("LPUSHCAP", "lpushcap");
        m.put("RPUSHCAP", "rpushcap");
        m.put("RPUSH", "rpush");
        m.put("LPOP", "lpop");
        m.put("RPOP", "rpop");
        m.put("LLEN", "llen");
        m.put("LINDEX", "lindex");
        m.put("LRANGE", "lrange");
        m.put("LTRIM", "ltrim");
        m.put("BLPOP", "blpop");
        m.put("BRPOP", "brpop");
        m.put("SADD", "sadd");
        m.put("SREM", "srem");
        m.put("SISMEMBER", "sismember");
        m.put("SCARD", "scard");
        m.put("SMEMBERS", "smembers");
        m.put("SINTER", "sinter");
        m.put("SUNION", "sunion");
        m.put("SDIFF", "sdiff");
        m.put("SINTERSTORE", "sinterstore");
        m.put("SUNIONSTORE", "sunionstore");
        m.put("SDIFFSTORE", "sdiffstore");
        m.put("SMOVE", "smove");
        m.put("SPOP", "spop");
        m.put("SRANDMEMBER", "srandmember");
        m.put("HSET", "hset");
        m.put("HGET", "hget");
        m.put("HDEL", "hdel");
        m.put("HLEN", "hlen");
        m.put("HGETALL", "hgetall");
        m.put("HMGET", "hmget");
        m.put("HEXISTS", "hexists");
        m.put("HKEYS", "hkeys");
        m.put("HVALS", "hvals");
        m.put("HINCRBY", "hincrby");
        m.put("HSETNX", "hsetnx");
        m.put("HSTRLEN", "hstrlen");
        m.put("HSCAN", "hscan");
        m.put("HEXPIRE", "hexpire");
        m.put("HPEXPIRE", "hpexpire");
        m.put("HEXPIREAT", "hexpireat");
        m.put("HPEXPIREAT", "hpexpireat");
        m.put("HTTL", "httl");
        m.put("HPTTL", "hpttl");
        m.put("HEXPIRETIME", "hexpiretime");
        m.put("HPEXPIRETIME", "hpexpiretime");
        m.put("HPERSIST", "hpersist");
        m.put("ZADD", "zadd");
        m.put("ZRANGE", "zrange");
        m.put("ZREVRANGE", "zrevrange");
        m.put("ZRANGEBYSCORE", "zrangebyscore");
        m.put("ZRANGEBYLEX", "zrangebylex");
        m.put("ZREVRANGEBYLEX", "zrevrangebylex");
        m.put("ZREM", "zrem");
        m.put("ZINCRBY", "zincrby");
        m.put("ZCARD", "zcard");
        m.put("ZSCORE", "zscore");
        m.put("ZRANK", "zrank");
        m.put("ZREVRANK", "zrevrank");
        m.put("ZPOPMIN", "zpopmin");
        m.put("ZPOPMAX", "zpopmax");
        m.put("BZPOPMIN", "bzpopmin");
        m.put("BZPOPMAX", "bzpopmax");
        m.put("ZREMRANGEBYRANK", "zremrangebyrank");
        m.put("ZREMRANGEBYSCORE", "zremrangebyscore");
        m.put("ZREMRANGEBYLEX", "zremrangebylex");
        m.put("GEOADD", "geoadd");
        m.put("GEODIST", "geodist");
        m.put("GEOHASH", "geohash");
        m.put("GEOPOS", "geopos");
        m.put("GEOSEARCH", "geosearch");
        m.put("GEOSEARCHSTORE", "geosearchstore");
        m.put("GEORADIUS", "georadius");
        m.put("GEORADIUSBYMEMBER", "georadiusbymember");
        m.put("BF.RESERVE", "bfReserve");
        m.put("BF.ADD", "bfAdd");
        m.put("BF.MADD", "bfMadd");
        m.put("BF.INSERT", "bfInsert");
        m.put("BF.INFO", "bfInfo");
        m.put("BF.CARD", "bfCard");
        m.put("BF.EXISTS", "bfExists");
        m.put("BF.MEXISTS", "bfMexists");
        m.put("JSON.GET", "jsonGet");
        m.put("JSON.INDEX", "jsonIndexCreate");
        m.put("JSON.FIND", "jsonFind");
        m.put("JSON.MGET", "jsonMget");
        m.put("JSON.SET", "jsonSet");
        m.put("JSON.DEL", "jsonDel");
        m.put("XADD", "xadd");
        m.put("XLEN", "xlen");
        m.put("XRANGE", "xrange");
        m.put("XREVRANGE", "xrevrange");
        m.put("XREAD", "xread");
        m.put("XDEL", "xdel");
        m.put("XSETID", "xsetid");
        m.put("XTRIM", "xtrimMaxlen");
        m.put("XGROUP CREATE", "xgroupCreate");
        m.put("XGROUP CREATECONSUMER", "xgroupCreateconsumer");
        m.put("XREADGROUP", "xreadgroup");
        m.put("XGROUP DESTROY", "xgroupDestroy");
        m.put("XGROUP SETID", "xgroupSetid");
        m.put("XGROUP DELCONSUMER", "xgroupDelconsumer");
        m.put("XACK", "xack");
        m.put("XPENDING", "xpending");
        m.put("XAUTOCLAIM", "xautoclaim");
        m.put("XCLAIM", "xclaim");
        m.put("XINFO STREAM", "xinfoStream");
        m.put("XINFO GROUPS", "xinfoGroups");
        m.put("XINFO CONSUMERS", "xinfoConsumers");
        m.put("INFO", "info");
        m.put("WAIT", "waitReplicas");
        m.put("CONFIG GET", "configGet");
        m.put("SAVE", "save");
        m.put("BGSAVE", "bgsave");
        m.put("MULTI", "RuvioClient#multi");
        m.put("EXEC", "Transaction#exec");
        m.put("DISCARD", "Transaction#discard");
        m.put("WATCH", "Transaction#watch");
        m.put("UNWATCH", "Transaction#unwatch");
        m.put("EVAL", "eval");
        m.put("EVALSHA", "evalsha");
        m.put("SCRIPT LOAD", "scriptLoad");
        m.put("SCRIPT EXISTS", "scriptExists");
        m.put("SCRIPT FLUSH", "scriptFlush");
        m.put("SCRIPT KILL", "scriptKill");
        m.put("FCALL", "fcall");
        m.put("FUNCTION LOAD", "functionLoad");
        m.put("FUNCTION LIST", "functionList");
        m.put("FUNCTION DELETE", "functionDelete");
        m.put("CLUSTER SLOTS", "clusterSlots");
        m.put("CLUSTER NODES", "clusterNodes");
        m.put("CLUSTER SHARDS", "clusterShards");
        m.put("CLUSTER INFO", "clusterInfo");
        m.put("CLUSTER MYID", "clusterMyid");
        m.put("CLUSTER KEYSLOT", "clusterKeyslot");
        m.put("READONLY", "readonly");
        m.put("READWRITE", "readwrite");
        m.put("SPUBLISH", "spublish");
        m.put("SSUBSCRIBE", "RuvioPubSub#ssubscribe");
        m.put("SUNSUBSCRIBE", "RuvioPubSub#sunsubscribe");
        m.put("PUBLISH", "publish");
        m.put("SUBSCRIBE", "RuvioPubSub#subscribe");
        m.put("UNSUBSCRIBE", "RuvioPubSub#unsubscribe");
        m.put("PSUBSCRIBE", "RuvioPubSub#psubscribe");
        m.put("PUNSUBSCRIBE", "RuvioPubSub#punsubscribe");
        m.put("CLIENT GETNAME", "clientGetname");
        m.put("CLIENT SETNAME", "clientSetname");
        m.put("CLIENT TRACKING", "clientTracking");
        m.put("SSCAN", "sscan");
        m.put("ZSCAN", "zscan");
        m.put("JSON.MERGE", "jsonMerge");
        m.put("JSON.TYPE", "jsonType");
        m.put("JSON.STRLEN", "jsonStrlen");
        m.put("JSON.ARRLEN", "jsonArrlen");
        m.put("JSON.OBJKEYS", "jsonObjkeys");
        m.put("JSON.NUMINCRBY", "jsonNumincrby");
        m.put("JSON.TOGGLE", "jsonToggle");
        m.put("JSON.ARRAPPEND", "jsonArrappend");
        m.put("JSON.CLEAR", "jsonClear");
        m.put("JSON.OBJLEN", "jsonObjlen");
        m.put("JSON.STRAPPEND", "jsonStrappend");
        m.put("JSON.NUMMULTBY", "jsonNummultby");
        m.put("JSON.ARRINSERT", "jsonArrinsert");
        m.put("JSON.ARRPOP", "jsonArrpop");
        m.put("JSON.ARRTRIM", "jsonArrtrim");
        m.put("JSON.ARRINDEX", "jsonArrindex");
        m.put("JSON.FORGET", "jsonForget");
        m.put("JMERGE", "jmerge");
        m.put("COMMAND", "command");
        m.put("SETBIT", "setbit");
        m.put("BITFIELD", "bitfield");
        m.put("BITFIELD_RO", "bitfieldRo");
        m.put("GETBIT", "getbit");
        m.put("BITCOUNT", "bitcount");
        m.put("BITPOS", "bitpos");
        m.put("BITOP", "bitop");
        m.put("PFADD", "pfadd");
        m.put("PFCOUNT", "pfcount");
        m.put("PFMERGE", "pfmerge");
        m.put("FILLGET", "fillget");
        m.put("FILLSET", "fillset");
        m.put("SETEX", "setex");
        m.put("PSETEX", "psetex");
        m.put("SETNX", "setnx");
        m.put("GETDEL", "getdel");
        m.put("MSETNX", "msetnx");
        m.put("INCRBYFLOAT", "incrbyfloat");
        m.put("GETRANGE", "getrange");
        m.put("SUBSTR", "substr");
        m.put("SETRANGE", "setrange");
        m.put("SMISMEMBER", "smismember");
        m.put("SINTERCARD", "sintercard");
        m.put("ZCOUNT", "zcount");
        m.put("ZLEXCOUNT", "zlexcount");
        m.put("ZMSCORE", "zmscore");
        m.put("ZUNIONSTORE", "zunionstore");
        m.put("ZINTERSTORE", "zinterstore");
        m.put("ZDIFFSTORE", "zdiffstore");
        m.put("ZUNION", "zunion");
        m.put("ZINTER", "zinter");
        m.put("ZDIFF", "zdiff");
        m.put("ZRANDMEMBER", "zrandmember");
        m.put("HRANDFIELD", "hrandfield");
        m.put("HINCRBYFLOAT", "hincrbyfloat");
        m.put("LINSERT", "linsert");
        m.put("LSET", "lset");
        m.put("LREM", "lrem");
        m.put("LPOS", "lpos");
        m.put("LMOVE", "lmove");
        m.put("RPOPLPUSH", "rpoplpush");
        m.put("BLMOVE", "blmove");
        m.put("LPUSHX", "lpushx");
        m.put("RPUSHX", "rpushx");
        m.put("LMPOP", "lmpop");
        m.put("BLMPOP", "blmpop");
        m.put("KEYS", "keys");
        m.put("RANDOMKEY", "randomkey");
        m.put("TOUCH", "touch");
        m.put("RENAMENX", "renamenx");
        m.put("EXPIRETIME", "expiretime");
        m.put("PEXPIRETIME", "pexpiretime");
        m.put("COPY", "copy");
        m.put("ECHO", "echo");
        m.put("OBJECT", "objectEncoding");
        m.put("MEMORY", "memoryUsage");
        m.put("CLIENT ID", "clientId");
        m.put("CLIENT LIST", "clientList");
        m.put("CLIENT INFO", "clientInfo");
        m.put("CLIENT KILL", "clientKill");
        m.put("SLOWLOG", "slowlogGet");
        m.put("LATENCY", "latencyLatest");
        m.put("DUMP", "dump");
        m.put("RESTORE", "restore");
        return m;
    }

    static List<String> matrixCommands() throws IOException {
        Path p = Paths.get(System.getProperty("user.dir")).resolve("../../docs/compatibility-matrix.json").normalize();
        String json = new String(Files.readAllBytes(p), StandardCharsets.UTF_8);
        int start = json.indexOf("\"commands\"");
        int end = json.indexOf("\"unsupported\"");
        String section = json.substring(start, end);
        Matcher m = Pattern.compile("\"name\"\\s*:\\s*\"([^\"]+)\"").matcher(section);
        List<String> names = new ArrayList<>();
        while (m.find()) {
            names.add(m.group(1));
        }
        return names;
    }

    private static boolean hasPublic(Class<?> type, String method) {
        for (Method m : type.getMethods()) {
            if (m.getName().equals(method) && Modifier.isPublic(m.getModifiers())) {
                return true;
            }
        }
        return false;
    }

    @Test
    void everyMatrixCommandHasATypedMethod() throws IOException {
        List<String> commands = matrixCommands();
        Map<String, String> table = table();
        List<String> missing = new ArrayList<>();
        int covered = 0;
        for (String command : commands) {
            String target = table.get(command);
            if (target == null) {
                missing.add(command + " (no table entry)");
                continue;
            }
            boolean ok;
            if (target.contains("#")) {
                String[] parts = target.split("#");
                Class<?> type;
                switch (parts[0]) {
                    case "RuvioClient": type = RuvioClient.class; break;
                    case "Transaction": type = Transaction.class; break;
                    case "RuvioPubSub": type = RuvioPubSub.class; break;
                    default: throw new IllegalStateException(target);
                }
                ok = hasPublic(type, parts[1]);
            } else {
                ok = hasPublic(RuvioClient.class, target) && hasPublic(RuvioAsync.class, target)
                        && hasPublic(Pipeline.class, target);
            }
            if (ok) {
                covered++;
            } else {
                missing.add(command + " -> " + target);
            }
        }
        System.out.println("[coverage] typed methods for " + covered + "/" + commands.size() + " matrix commands");
        assertTrue(missing.isEmpty(), "commands without a typed method: " + missing);
        Set<String> extra = new LinkedHashSet<>(table.keySet());
        extra.removeAll(commands);
        assertTrue(extra.isEmpty(), "table entries not in the matrix: " + extra);
    }

    /**
     * Every one of the matrix commands must have a routing rule in the server's command table (key
     * positions themselves are checked against the server in {@link KeyVectorsTest}), and no command
     * the matrix marks "same_slot" may be routed by its first argument alone.
     */
    @Test
    void everyMatrixCommandHasAClusterRoutingRule() throws IOException {
        Path p = Paths.get(System.getProperty("user.dir")).resolve("../../docs/compatibility-matrix.json").normalize();
        String json = new String(Files.readAllBytes(p), StandardCharsets.UTF_8);
        String section = json.substring(json.indexOf("\"commands\""), json.indexOf("\"unsupported\""));
        Matcher entry = Pattern.compile("\"name\"\\s*:\\s*\"([^\"]+)\"(.*?)\"same_slot\"\\s*:\\s*(true|false)", Pattern.DOTALL).matcher(section);
        int commands = 0;
        int sameSlot = 0;
        List<String> problems = new ArrayList<>();
        while (entry.find()) {
            commands++;
            String word = entry.group(1).split(" ")[0];
            if (!CommandKeyRules.RULES.containsKey(word)) {
                problems.add(word + " is not in the command key table");
            }
            if (Boolean.parseBoolean(entry.group(3))) {
                sameSlot++;
                if (ClusterKeys.spec(word).first) {
                    problems.add(word + " is same_slot in the matrix but is routed by its first argument only");
                }
            }
        }
        assertTrue(problems.isEmpty(), problems.toString());
        System.out.println("[cluster] routing rule for " + commands + " matrix commands (" + sameSlot + " flagged same_slot)");
    }
}
