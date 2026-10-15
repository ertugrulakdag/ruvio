// Copyright (c) 2026 Salih Cantekin. MIT License; see LICENSE.

package ruvio

import "bytes"

// slotCount is the number of cluster hash slots.
const slotCount = 16384

var crcTable = func() (t [256]uint16) {
	for i := range t {
		crc := uint16(i) << 8
		for b := 0; b < 8; b++ {
			if crc&0x8000 != 0 {
				crc = crc<<1 ^ 0x1021
			} else {
				crc <<= 1
			}
		}
		t[i] = crc
	}
	return
}()

// hashSlot is CRC16-XMODEM of the key, or of the first non-empty {tag} in it, mod 16384.
func hashSlot(key []byte) int {
	if i := bytes.IndexByte(key, '{'); i >= 0 {
		if j := bytes.IndexByte(key[i+1:], '}'); j > 0 {
			key = key[i+1 : i+1+j]
		}
	}
	var crc uint16
	for _, b := range key {
		crc = crc<<8 ^ crcTable[byte(crc>>8)^b]
	}
	return int(crc) & (slotCount - 1)
}

// KeySlot returns the cluster hash slot of key (CRC16-XMODEM mod 16384, {hashtag} aware).
func KeySlot(key string) int {
	if i := indexByte(key, '{'); i >= 0 {
		if j := indexByte(key[i+1:], '}'); j > 0 {
			key = key[i+1 : i+1+j]
		}
	}
	var crc uint16
	for k := 0; k < len(key); k++ {
		crc = crc<<8 ^ crcTable[byte(crc>>8)^key[k]]
	}
	return int(crc) & (slotCount - 1)
}

func indexByte(s string, c byte) int {
	for i := 0; i < len(s); i++ {
		if s[i] == c {
			return i
		}
	}
	return -1
}

// keyKind says where a command carries its keys.
type keyKind uint8

const (
	kFirst   keyKind = iota // the key is argument 1 (the common case)
	kNone                   // no key: any node
	kAll                    // every argument after the name is a key
	kTwo                    // arguments 1 and 2
	kPairs                  // key value key value ... (MSET)
	kBlock                  // BLPOP key... timeout
	kCount1                 // numkeys at argument 1, keys follow (ZUNION, LMPOP, SINTERCARD)
	kCount2                 // numkeys at argument 2, keys follow (EVAL, FCALL, BLMPOP)
	kZStore                 // destination at 1, numkeys at 2, keys follow
	kStreams                // XREAD / XREADGROUP: keys between STREAMS and the ids
	kSub2                   // subcommand first, key at argument 2 (OBJECT, XGROUP, ...)
	kBitop                  // BITOP op dest src...: keys from argument 2
	kJMGet                  // JSON.MGET key... path
	kGeoRad                 // GEORADIUS: key at 1 plus the STORE/STOREDIST target
)

// fanKind marks commands that act on a whole cluster rather than one node.
type fanKind uint8

const (
	fanNone   fanKind = iota
	fanSum            // DBSIZE: replies are added
	fanFirst          // FLUSHDB, FLUSHALL, SCRIPT LOAD ...: sent to every node, first reply returned
	fanConcat         // KEYS: arrays are joined
	fanSub            // like fanFirst, only for subcommands LOAD/FLUSH/DELETE/RESTORE
)

// cmdSpec describes how the router treats one command.
type cmdSpec struct {
	kind  keyKind
	split bool // keyed multi-key command that is split per slot when its keys differ
	fan   fanKind
	scan  bool // SCAN: walks every node
}

// cmdSpecs lists every command of docs/compatibility-matrix.json (first word of the
// name). A command that is not listed is routed by its first argument.
var cmdSpecs = func() map[string]cmdSpec {
	m := map[string]cmdSpec{}
	set := func(k keyKind, names ...string) {
		for _, n := range names {
			m[n] = cmdSpec{kind: k}
		}
	}
	set(kNone, "RUVIO", "PING", "HELLO", "AUTH", "ACL", "CHANGES", "SELECT", "RANDOMKEY", "SWAPDB", "INFO", "WAIT",
		"CONFIG", "SAVE", "BGSAVE", "MULTI", "EXEC", "DISCARD", "UNWATCH", "CLUSTER", "READONLY", "READWRITE",
		"PUBLISH", "SUBSCRIBE", "UNSUBSCRIBE", "PSUBSCRIBE", "PUNSUBSCRIBE", "CLIENT", "COMMAND", "ECHO",
		"SLOWLOG", "LATENCY", "JSON.INDEX", "JSON.FIND", "LASTSAVE", "TIME", "QUIT", "RESET")
	set(kAll, "PFCOUNT", "PFMERGE", "SDIFF", "SDIFFSTORE", "SINTER", "SINTERSTORE", "SUNION", "SUNIONSTORE",
		"SSUBSCRIBE", "SUNSUBSCRIBE", "WATCH")
	set(kTwo, "BLMOVE", "BRPOPLPUSH", "COPY", "LMOVE", "RENAME", "RENAMENX", "RPOPLPUSH", "SMOVE", "GEOSEARCHSTORE")
	set(kPairs, "MSETNX")
	set(kBlock, "BLPOP", "BRPOP", "BZPOPMIN", "BZPOPMAX")
	set(kCount1, "ZUNION", "ZINTER", "ZDIFF", "LMPOP", "SINTERCARD")
	set(kCount2, "EVAL", "EVALSHA", "EVAL_RO", "EVALSHA_RO", "FCALL", "FCALL_RO", "BLMPOP")
	set(kZStore, "ZUNIONSTORE", "ZINTERSTORE", "ZDIFFSTORE")
	set(kStreams, "XREAD", "XREADGROUP")
	set(kSub2, "OBJECT", "MEMORY", "XGROUP", "XINFO")
	set(kBitop, "BITOP")
	set(kJMGet, "JSON.MGET")
	set(kGeoRad, "GEORADIUS", "GEORADIUSBYMEMBER")
	// scripts and functions: LOAD/FLUSH/DELETE must reach every node.
	m["SCRIPT"] = cmdSpec{kind: kNone, fan: fanSub}
	m["FUNCTION"] = cmdSpec{kind: kNone, fan: fanSub}
	m["DBSIZE"] = cmdSpec{kind: kNone, fan: fanSum}
	m["FLUSHDB"] = cmdSpec{kind: kNone, fan: fanFirst}
	m["FLUSHALL"] = cmdSpec{kind: kNone, fan: fanFirst}
	m["KEYS"] = cmdSpec{kind: kNone, fan: fanConcat}
	m["SCAN"] = cmdSpec{kind: kNone, scan: true}
	// keyed multi-key commands that are split per slot
	for _, n := range []string{"DEL", "UNLINK", "EXISTS", "TOUCH", "MGET"} {
		m[n] = cmdSpec{kind: kAll, split: true}
	}
	m["MSET"] = cmdSpec{kind: kPairs, split: true}
	// everything else with its key at argument 1 (listed so the coverage test can prove
	// that no matrix command was forgotten).
	for _, n := range firstKeyed {
		m[n] = cmdSpec{kind: kFirst}
	}
	return m
}()

// firstKeyed are the matrix commands whose key is argument 1.
var firstKeyed = []string{
	"GET", "SETV", "TAKE", "DELEX", "GETEX", "LIMIT", "IDEM", "ONCE", "LEASE", "SEMAPHORE", "RELEASE", "SET",
	"GETSET", "APPEND", "STRLEN", "INCR", "DECR", "INCRBY", "DECRBY", "TYPE", "MOVE", "EXPIRE", "PEXPIRE",
	"EXPIREAT", "PEXPIREAT", "TTL", "PTTL", "PERSIST", "LPUSH", "LPUSHCAP", "RPUSHCAP", "RPUSH", "LPOP", "RPOP",
	"LLEN", "LINDEX", "LRANGE", "LTRIM", "SADD", "SREM", "SISMEMBER", "SCARD", "SMEMBERS", "SPOP", "SRANDMEMBER",
	"HSET", "HGET", "HDEL", "HLEN", "HGETALL", "HMGET", "HEXISTS", "HKEYS", "HVALS", "HINCRBY", "HSETNX",
	"HSTRLEN", "HSCAN", "HEXPIRE", "HPEXPIRE", "HEXPIREAT", "HPEXPIREAT", "HTTL", "HPTTL", "HEXPIRETIME",
	"HPEXPIRETIME", "HPERSIST", "ZADD", "ZRANGE", "ZREVRANGE", "ZRANGEBYSCORE", "ZRANGEBYLEX", "ZREVRANGEBYLEX",
	"ZREM", "ZINCRBY", "ZCARD", "ZSCORE", "ZRANK", "ZREVRANK", "ZPOPMIN", "ZPOPMAX", "ZREMRANGEBYRANK",
	"ZREMRANGEBYSCORE", "ZREMRANGEBYLEX", "GEOADD", "GEODIST", "GEOHASH", "GEOPOS", "GEOSEARCH", "BF.RESERVE",
	"BF.ADD", "BF.MADD", "BF.INSERT", "BF.INFO", "BF.CARD", "BF.EXISTS", "BF.MEXISTS", "JSON.GET", "JSON.SET",
	"JSON.DEL", "XADD", "XLEN", "XRANGE", "XREVRANGE", "XDEL", "XSETID", "XTRIM", "XACK", "XPENDING",
	"XAUTOCLAIM", "XCLAIM", "SPUBLISH", "SSCAN", "ZSCAN", "JSON.MERGE", "JSON.TYPE", "JSON.STRLEN",
	"JSON.ARRLEN", "JSON.OBJKEYS", "JSON.NUMINCRBY", "JSON.TOGGLE", "JSON.ARRAPPEND", "JSON.CLEAR",
	"JSON.OBJLEN", "JSON.STRAPPEND", "JSON.NUMMULTBY", "JSON.ARRINSERT", "JSON.ARRPOP", "JSON.ARRTRIM",
	"JSON.ARRINDEX", "JSON.FORGET", "JMERGE", "SETBIT", "BITFIELD", "BITFIELD_RO", "GETBIT", "BITCOUNT",
	"BITPOS", "PFADD", "FILLGET", "FILLSET", "SETEX", "PSETEX", "SETNX", "GETDEL", "INCRBYFLOAT", "GETRANGE",
	"SUBSTR", "SETRANGE", "SMISMEMBER", "ZCOUNT", "ZLEXCOUNT", "ZMSCORE", "ZRANDMEMBER", "HRANDFIELD",
	"HINCRBYFLOAT", "LINSERT", "LSET", "LREM", "LPOS", "LPUSHX", "RPUSHX", "EXPIRETIME", "PEXPIRETIME", "DUMP",
	"RESTORE",
}

// specOf looks a command name up case-insensitively without allocating.
func specOf(name []byte) (cmdSpec, bool) {
	var up [32]byte
	if len(name) > len(up) {
		return cmdSpec{}, false
	}
	for i, c := range name {
		if c >= 'a' && c <= 'z' {
			c -= 'a' - 'A'
		}
		up[i] = c
	}
	s, ok := cmdSpecs[string(up[:len(name)])]
	if !ok {
		return cmdSpec{kind: kFirst}, false
	}
	return s, true
}

// ---- reading an encoded command frame ---------------------------------------------------

// frameHeader reads "*N\r\n" at b[0:] and returns N and the offset of the first bulk.
func frameHeader(b []byte) (n, off int, ok bool) {
	if len(b) < 4 || b[0] != '*' {
		return 0, 0, false
	}
	i := 1
	for ; i < len(b) && b[i] != '\r'; i++ {
		if b[i] < '0' || b[i] > '9' || n > 1<<24 {
			return 0, 0, false
		}
		n = n*10 + int(b[i]-'0')
	}
	if i+1 >= len(b) || b[i+1] != '\n' {
		return 0, 0, false
	}
	return n, i + 2, true
}

// frameBulk reads one "$len\r\n...\r\n" at b[off:] and returns the payload and the next offset.
func frameBulk(b []byte, off int) (arg []byte, next int, ok bool) {
	if off >= len(b) || b[off] != '$' {
		return nil, 0, false
	}
	i, n := off+1, 0
	for ; i < len(b) && b[i] != '\r'; i++ {
		if b[i] < '0' || b[i] > '9' || n > 1<<30 {
			return nil, 0, false
		}
		n = n*10 + int(b[i]-'0')
	}
	start := i + 2
	end := start + n
	if end+2 > len(b) {
		return nil, 0, false
	}
	return b[start:end], end + 2, true
}

// frameArgs appends up to max arguments (all when max <= 0) of the frame at b[0:] to dst.
// It returns the number of arguments the frame declares and the offset just past the
// arguments that were read.
func frameArgs(b []byte, max int, dst [][]byte) (args [][]byte, n, end int, ok bool) {
	n, off, ok := frameHeader(b)
	if !ok {
		return dst, 0, 0, false
	}
	want := n
	if max > 0 && max < n {
		want = max
	}
	for i := 0; i < want; i++ {
		var a []byte
		if a, off, ok = frameBulk(b, off); !ok {
			return dst, n, off, false
		}
		dst = append(dst, a)
	}
	return dst, n, off, true
}

// frameEnd returns the offset just past the frame at b[0:].
func frameEnd(b []byte) (int, bool) {
	n, off, ok := frameHeader(b)
	if !ok {
		return 0, false
	}
	for i := 0; i < n; i++ {
		if _, off, ok = frameBulk(b, off); !ok {
			return 0, false
		}
	}
	return off, true
}

// ---- key extraction ----------------------------------------------------------------------------

func upperEq(b []byte, s string) bool {
	if len(b) != len(s) {
		return false
	}
	for i := 0; i < len(b); i++ {
		c := b[i]
		if c >= 'a' && c <= 'z' {
			c -= 'a' - 'A'
		}
		if c != s[i] {
			return false
		}
	}
	return true
}

func atoiBytes(b []byte) (int, bool) {
	if len(b) == 0 || len(b) > 9 {
		return 0, false
	}
	n := 0
	for _, c := range b {
		if c < '0' || c > '9' {
			return 0, false
		}
		n = n*10 + int(c-'0')
	}
	return n, true
}

// keysOf appends the keys of the command args (args[0] is the name) to dst, in argument
// order. Keyless commands add nothing. The returned slices alias args.
func keysOf(kind keyKind, args [][]byte, dst [][]byte) [][]byte {
	n := len(args)
	if n < 2 || kind == kNone {
		return dst
	}
	switch kind {
	case kFirst:
		return append(dst, args[1])
	case kAll:
		return append(dst, args[1:]...)
	case kTwo:
		if n < 3 {
			return append(dst, args[1])
		}
		return append(dst, args[1], args[2])
	case kPairs:
		for i := 1; i < n; i += 2 {
			dst = append(dst, args[i])
		}
	case kBlock:
		if n > 2 {
			dst = append(dst, args[1:n-1]...)
		}
	case kCount1, kCount2, kZStore:
		at := 1
		if kind != kCount1 {
			at = 2
		}
		if kind == kZStore {
			dst = append(dst, args[1])
		}
		if at < n {
			if c, ok := atoiBytes(args[at]); ok {
				if c > n-at-1 {
					c = n - at - 1
				}
				dst = append(dst, args[at+1:at+1+c]...)
			}
		}
	case kStreams:
		for i := 1; i < n; i++ {
			if upperEq(args[i], "STREAMS") {
				rest := n - i - 1
				return append(dst, args[i+1:i+1+rest/2]...)
			}
		}
	case kSub2:
		if n > 2 {
			dst = append(dst, args[2])
		}
	case kBitop:
		if n > 2 {
			dst = append(dst, args[2:]...)
		}
	case kJMGet:
		if n > 2 {
			dst = append(dst, args[1:n-1]...)
		}
	case kGeoRad:
		dst = append(dst, args[1])
		for i := 2; i+1 < n; i++ {
			if upperEq(args[i], "STORE") || upperEq(args[i], "STOREDIST") {
				dst = append(dst, args[i+1])
			}
		}
	}
	return dst
}

// CommandKeys returns the keys the router would use for the command (name first), in
// argument order. It exists for tests and tooling.
func CommandKeys(args ...string) []string {
	bs := make([][]byte, len(args))
	for i, a := range args {
		bs[i] = []byte(a)
	}
	if len(bs) == 0 {
		return nil
	}
	spec, _ := specOf(bs[0])
	var out []string
	for _, k := range keysOf(spec.kind, bs, nil) {
		out = append(out, string(k))
	}
	return out
}
