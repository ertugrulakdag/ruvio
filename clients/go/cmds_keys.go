// Copyright (c) 2026 Salih Cantekin. MIT License; see LICENSE.

package ruvio

import (
	"context"
	"time"
)

// Del removes keys and returns how many existed.
func (c *cmds) Del(ctx context.Context, keys ...string) (int64, error) {
	return c.intR(ctx, newCmd(1+len(keys), cDEL).ss(keys))
}

// Unlink is DEL (synchronous in Ruvio).
func (c *cmds) Unlink(ctx context.Context, keys ...string) (int64, error) {
	return c.intR(ctx, newCmd(1+len(keys), cUNLINK).ss(keys))
}

// Exists counts existing keys.
func (c *cmds) Exists(ctx context.Context, keys ...string) (int64, error) {
	return c.intR(ctx, newCmd(1+len(keys), cEXISTS).ss(keys))
}

// Touch counts existing keys.
func (c *cmds) Touch(ctx context.Context, keys ...string) (int64, error) {
	return c.intR(ctx, newCmd(1+len(keys), cTOUCH).ss(keys))
}

// Type returns the key type ("none" when missing).
func (c *cmds) Type(ctx context.Context, key string) (string, error) {
	return c.strR(ctx, newCmd(2, cTYPE).s(key))
}

// Rename renames key.
func (c *cmds) Rename(ctx context.Context, key, newKey string) error {
	return c.ok(ctx, newCmd(3, cRENAME).s(key).s(newKey))
}

// RenameNX renames only when newKey is missing.
func (c *cmds) RenameNX(ctx context.Context, key, newKey string) (bool, error) {
	return c.boolR(ctx, newCmd(3, cRENAMENX).s(key).s(newKey))
}

// Copy copies src to dst (REPLACE when replace).
func (c *cmds) Copy(ctx context.Context, src, dst string, replace bool) (bool, error) {
	if replace {
		return c.boolR(ctx, newCmd(4, cCOPY).s(src).s(dst).t(tREPLACE))
	}
	return c.boolR(ctx, newCmd(3, cCOPY).s(src).s(dst))
}

// ScanOptions filter SCAN-family pages.
type ScanOptions struct {
	Match string
	Count int64
	Type  string // SCAN only
}

func (o *ScanOptions) n() int {
	n := 0
	if o.Match != "" {
		n += 2
	}
	if o.Count > 0 {
		n += 2
	}
	if o.Type != "" {
		n += 2
	}
	return n
}

func (o *ScanOptions) append(r *request) {
	if o.Match != "" {
		r.t(tMATCH).s(o.Match)
	}
	if o.Count > 0 {
		r.t(tCOUNT).i(o.Count)
	}
	if o.Type != "" {
		r.t(tTYPE).s(o.Type)
	}
}

func scanReply(v Value) (string, []string, error) {
	if !v.isAgg() || len(v.Array) != 2 {
		return "", nil, unexpected(v)
	}
	items, err := v.Array[1].Strings()
	return v.Array[0].Str(), items, err
}

// Scan returns one page of keys and the next cursor ("0" when done).
func (c *cmds) Scan(ctx context.Context, cursor string, o ScanOptions) (string, []string, error) {
	r := newCmd(2+o.n(), cSCAN).s(cursor)
	o.append(r)
	v, err := c.value(ctx, r)
	if err != nil {
		return "", nil, err
	}
	return scanReply(v)
}

// Keys returns keys matching a glob (refused on very large databases: use Scan).
func (c *cmds) Keys(ctx context.Context, pattern string) ([]string, error) {
	return c.strs(ctx, newCmd(2, cKEYS).s(pattern))
}

// RandomKey returns a random key; ok is false on an empty database.
func (c *cmds) RandomKey(ctx context.Context) (string, bool, error) {
	return c.strNil(ctx, newCmd(1, cRANDOMKEY))
}

// DBSize counts keys in the selected database.
func (c *cmds) DBSize(ctx context.Context) (int64, error) {
	return c.intR(ctx, newCmd(1, cDBSIZE))
}

// Select switches the logical database. On a Client this changes the shared
// connection for every caller and is re-applied on reconnect; prefer Options.DB or a
// dedicated Conn.
func (c *cmds) Select(ctx context.Context, db int) error {
	err := c.ok(ctx, newCmd(2, cSELECT).i(int64(db)))
	if err == nil {
		c.ex.selected(int64(db))
	}
	return err
}

// FlushDB clears the selected database.
func (c *cmds) FlushDB(ctx context.Context) error { return c.ok(ctx, newCmd(1, cFLUSHDB)) }

// FlushAll clears every database.
func (c *cmds) FlushAll(ctx context.Context) error { return c.ok(ctx, newCmd(1, cFLUSHALL)) }

// SwapDB exchanges two databases.
func (c *cmds) SwapDB(ctx context.Context, a, b int) error {
	return c.ok(ctx, newCmd(3, cSWAPDB).i(int64(a)).i(int64(b)))
}

// Move moves key to database db.
func (c *cmds) Move(ctx context.Context, key string, db int) (bool, error) {
	return c.boolR(ctx, newCmd(3, cMOVE).s(key).i(int64(db)))
}

// Expire sets a relative TTL in whole seconds.
func (c *cmds) Expire(ctx context.Context, key string, ttl time.Duration) (bool, error) {
	return c.boolR(ctx, newCmd(3, cEXPIRE).s(key).i(int64(ttl/time.Second)))
}

// PExpire sets a relative TTL in milliseconds.
func (c *cmds) PExpire(ctx context.Context, key string, ttl time.Duration) (bool, error) {
	return c.boolR(ctx, newCmd(3, cPEXPIRE).s(key).i(ms(ttl)))
}

// ExpireAt sets an absolute deadline (seconds).
func (c *cmds) ExpireAt(ctx context.Context, key string, at time.Time) (bool, error) {
	return c.boolR(ctx, newCmd(3, cEXPIREAT).s(key).i(at.Unix()))
}

// PExpireAt sets an absolute deadline (milliseconds).
func (c *cmds) PExpireAt(ctx context.Context, key string, at time.Time) (bool, error) {
	return c.boolR(ctx, newCmd(3, cPEXPIREAT).s(key).i(at.UnixMilli()))
}

// TTL returns seconds left, -1 without a TTL, -2 when missing.
func (c *cmds) TTL(ctx context.Context, key string) (int64, error) {
	return c.intR(ctx, newCmd(2, cTTL).s(key))
}

// PTTL returns milliseconds left, -1 without a TTL, -2 when missing.
func (c *cmds) PTTL(ctx context.Context, key string) (int64, error) {
	return c.intR(ctx, newCmd(2, cPTTL).s(key))
}

// ExpireTime returns the Unix-seconds deadline (-1 none, -2 missing).
func (c *cmds) ExpireTime(ctx context.Context, key string) (int64, error) {
	return c.intR(ctx, newCmd(2, cEXPIRETIME).s(key))
}

// PExpireTime returns the Unix-milliseconds deadline (-1 none, -2 missing).
func (c *cmds) PExpireTime(ctx context.Context, key string) (int64, error) {
	return c.intR(ctx, newCmd(2, cPEXPIRETIME).s(key))
}

// Persist removes the TTL.
func (c *cmds) Persist(ctx context.Context, key string) (bool, error) {
	return c.boolR(ctx, newCmd(2, cPERSIST).s(key))
}

// ObjectEncoding is OBJECT ENCODING key.
func (c *cmds) ObjectEncoding(ctx context.Context, key string) (string, bool, error) {
	return c.strNil(ctx, newCmd(3, cOBJECT).t(tENCODING).s(key))
}

// ObjectIdleTime is OBJECT IDLETIME key (needs an LRU maxmemory-policy).
func (c *cmds) ObjectIdleTime(ctx context.Context, key string) (int64, error) {
	return c.intR(ctx, newCmd(3, cOBJECT).t(tIDLETIME).s(key))
}

// ObjectFreq is OBJECT FREQ key (needs allkeys-lfu).
func (c *cmds) ObjectFreq(ctx context.Context, key string) (int64, error) {
	return c.intR(ctx, newCmd(3, cOBJECT).t(tFREQ).s(key))
}

// MemoryUsage is MEMORY USAGE key [SAMPLES n] (samples 0 omits it).
func (c *cmds) MemoryUsage(ctx context.Context, key string, samples int64) (int64, bool, error) {
	if samples > 0 {
		return c.intNil(ctx, newCmd(5, cMEMORY).t(tUSAGE).s(key).t(tSAMPLES).i(samples))
	}
	return c.intNil(ctx, newCmd(3, cMEMORY).t(tUSAGE).s(key))
}

// Dump serializes key in Ruvio's own payload format.
func (c *cmds) Dump(ctx context.Context, key string) ([]byte, bool, error) {
	return c.bytesNil(ctx, newCmd(2, cDUMP).s(key))
}

// RestoreOptions are RESTORE modifiers.
type RestoreOptions struct {
	Replace bool
	AbsTTL  bool
}

// Restore recreates key from a Dump payload. ttlMs is a relative TTL in
// milliseconds (0 = none), or an absolute Unix-ms deadline with AbsTTL.
func (c *cmds) Restore(ctx context.Context, key string, ttlMs int64, payload []byte, o RestoreOptions) error {
	n := 4
	if o.Replace {
		n++
	}
	if o.AbsTTL {
		n++
	}
	r := newCmd(n, cRESTORE).s(key).i(ttlMs).b(payload)
	if o.Replace {
		r.t(tREPLACE)
	}
	if o.AbsTTL {
		r.t(tABSTTL)
	}
	return c.ok(ctx, r)
}

// ---- connection --------------------------------------------------------------

// Ping sends PING.
func (c *cmds) Ping(ctx context.Context) error { return c.ok(ctx, newCmd(1, cPING)) }

// Echo returns message.
func (c *cmds) Echo(ctx context.Context, message string) (string, error) {
	return c.strR(ctx, newCmd(2, cECHO).s(message))
}

// Hello switches protocol (2 or 3) and returns the server map. On a Client this
// changes the shared connection; the parser understands both protocols.
func (c *cmds) Hello(ctx context.Context, protocol int) (map[string]Value, error) {
	v, err := c.value(ctx, newCmd(2, cHELLO).i(int64(protocol)))
	if err != nil {
		return nil, err
	}
	return toValueMap(v)
}

// HelloAuth is HELLO protover AUTH username password.
func (c *cmds) HelloAuth(ctx context.Context, protocol int, username, password string) (map[string]Value, error) {
	v, err := c.value(ctx, newCmd(5, cHELLO).i(int64(protocol)).t(tAUTH).s(username).s(password))
	if err != nil {
		return nil, err
	}
	return toValueMap(v)
}

func toValueMap(v Value) (map[string]Value, error) {
	if !v.isAgg() || len(v.Array)%2 != 0 {
		return nil, unexpected(v)
	}
	m := make(map[string]Value, len(v.Array)/2)
	for i := 0; i < len(v.Array); i += 2 {
		m[v.Array[i].Str()] = v.Array[i+1]
	}
	return m, nil
}

// Auth authenticates the connection (username may be empty).
func (c *cmds) Auth(ctx context.Context, username, password string) error {
	if username == "" {
		return c.ok(ctx, newCmd(2, cAUTH).s(password))
	}
	return c.ok(ctx, newCmd(3, cAUTH).s(username).s(password))
}

// ClientID returns the connection id.
func (c *cmds) ClientID(ctx context.Context) (int64, error) {
	return c.intR(ctx, newCmd(2, cCLIENT).t(tID))
}

// ClientList returns CLIENT LIST text (optionally for ids).
func (c *cmds) ClientList(ctx context.Context, ids ...int64) (string, error) {
	if len(ids) == 0 {
		return c.strR(ctx, newCmd(2, cCLIENT).t(tLIST))
	}
	r := newCmd(3+len(ids), cCLIENT).t(tLIST).t(tID)
	for _, id := range ids {
		r.i(id)
	}
	return c.strR(ctx, r)
}

// ClientInfo returns this connection's CLIENT LIST line.
func (c *cmds) ClientInfo(ctx context.Context) (string, error) {
	return c.strR(ctx, newCmd(2, cCLIENT).t(tINFO))
}

// ClientKillOptions select connections for CLIENT KILL (filter form).
type ClientKillOptions struct {
	ID     int64
	Addr   string
	SkipMe *bool
}

// ClientKill kills by filters and returns how many were closed.
func (c *cmds) ClientKill(ctx context.Context, o ClientKillOptions) (int64, error) {
	n := 2
	if o.ID != 0 {
		n += 2
	}
	if o.Addr != "" {
		n += 2
	}
	if o.SkipMe != nil {
		n += 2
	}
	r := newCmd(n, cCLIENT).t(tKILL)
	if o.ID != 0 {
		r.t(tID).i(o.ID)
	}
	if o.Addr != "" {
		r.t(tADDR).s(o.Addr)
	}
	if o.SkipMe != nil {
		if *o.SkipMe {
			r.t(tSKIPME).s("yes")
		} else {
			r.t(tSKIPME).s("no")
		}
	}
	return c.intR(ctx, r)
}

// ClientGetName returns the connection name ("" when unset).
func (c *cmds) ClientGetName(ctx context.Context) (string, error) {
	v, err := c.value(ctx, newCmd(2, cCLIENT).t(tGETNAME))
	return v.Str(), err
}

// ClientSetName names the connection (empty clears it).
func (c *cmds) ClientSetName(ctx context.Context, name string) error {
	return c.ok(ctx, newCmd(3, cCLIENT).t(tSETNAME).s(name))
}

// ClientTracking turns tracking ON/OFF (ON requires RESP3: Options.Protocol=3).
// Invalidations arrive on Options.OnPush.
func (c *cmds) ClientTracking(ctx context.Context, on bool) error {
	if on {
		return c.ok(ctx, newCmd(3, cCLIENT).t(tTRACKING).t(tON))
	}
	return c.ok(ctx, newCmd(3, cCLIENT).t(tTRACKING).t(tOFF))
}

// ---- server ------------------------------------------------------------------

// Info returns INFO text for the given sections.
func (c *cmds) Info(ctx context.Context, sections ...string) (string, error) {
	return c.strR(ctx, newCmd(1+len(sections), cINFO).ss(sections))
}

// Wait blocks until replicas acknowledge (runs on a dedicated connection).
func (c *cmds) Wait(ctx context.Context, replicas int64, timeout time.Duration) (int64, error) {
	v, err := c.ex.execBlocking(ctx, newCmd(3, cWAIT).i(replicas).i(ms(timeout)))
	if err != nil {
		return 0, err
	}
	return v.AsInt64()
}

// ConfigGet returns matching settings.
func (c *cmds) ConfigGet(ctx context.Context, pattern string) (map[string]string, error) {
	return c.strMap(ctx, newCmd(3, cCONFIG).t(tGET).s(pattern))
}

// Save runs an in-process checkpoint.
func (c *cmds) Save(ctx context.Context) error { return c.ok(ctx, newCmd(1, cSAVE)) }

// BgSave starts a checkpoint.
func (c *cmds) BgSave(ctx context.Context) (string, error) { return c.strR(ctx, newCmd(1, cBGSAVE)) }

// Command returns COMMAND (every command in COMMAND INFO form).
func (c *cmds) Command(ctx context.Context) (Value, error) { return c.value(ctx, newCmd(1, cCOMMAND)) }

// CommandCount returns the number of commands.
func (c *cmds) CommandCount(ctx context.Context) (int64, error) {
	return c.intR(ctx, newCmd(2, cCOMMAND).t(tCOUNT))
}

// CommandInfo returns COMMAND INFO for names.
func (c *cmds) CommandInfo(ctx context.Context, names ...string) (Value, error) {
	return c.value(ctx, newCmd(2+len(names), cCOMMAND).t(tINFO).ss(names))
}

// CommandDocs returns COMMAND DOCS for names.
func (c *cmds) CommandDocs(ctx context.Context, names ...string) (Value, error) {
	return c.value(ctx, newCmd(2+len(names), cCOMMAND).t(tDOCS).ss(names))
}

// SlowLogGet returns up to n entries (n<0 omits the count).
func (c *cmds) SlowLogGet(ctx context.Context, n int64) (Value, error) {
	if n < 0 {
		return c.value(ctx, newCmd(2, cSLOWLOG).t(tGET))
	}
	return c.value(ctx, newCmd(3, cSLOWLOG).t(tGET).i(n))
}

// SlowLogLen returns the slow log length.
func (c *cmds) SlowLogLen(ctx context.Context) (int64, error) {
	return c.intR(ctx, newCmd(2, cSLOWLOG).t(tLEN))
}

// SlowLogReset clears the slow log.
func (c *cmds) SlowLogReset(ctx context.Context) error {
	return c.ok(ctx, newCmd(2, cSLOWLOG).t(tRESET))
}

// LatencyLatest is LATENCY LATEST (always empty on Ruvio).
func (c *cmds) LatencyLatest(ctx context.Context) (Value, error) {
	return c.value(ctx, newCmd(2, cLATENCY).t(tLATEST))
}

// ---- ACL -----------------------------------------------------------------------

// ACLWhoAmI returns the authenticated user.
func (c *cmds) ACLWhoAmI(ctx context.Context) (string, error) {
	return c.strR(ctx, newCmd(2, cACL).t(tWHOAMI))
}

// ACLUsers lists user names.
func (c *cmds) ACLUsers(ctx context.Context) ([]string, error) {
	return c.strs(ctx, newCmd(2, cACL).t(tUSERS))
}

// ACLList lists rules (passwords redacted).
func (c *cmds) ACLList(ctx context.Context) ([]string, error) {
	return c.strs(ctx, newCmd(2, cACL).t(tLIST))
}

// ACLGetUser returns the flat field list of a user (nil Value when missing).
func (c *cmds) ACLGetUser(ctx context.Context, user string) (Value, error) {
	return c.value(ctx, newCmd(3, cACL).t(tGETUSER).s(user))
}

// ACLCat lists categories, or the commands of one category.
func (c *cmds) ACLCat(ctx context.Context, category ...string) ([]string, error) {
	return c.strs(ctx, newCmd(2+len(category), cACL).t(tCAT).ss(category))
}

// ACLSetUser applies rules to user.
func (c *cmds) ACLSetUser(ctx context.Context, user string, rules ...string) error {
	return c.ok(ctx, newCmd(3+len(rules), cACL).t(tSETUSER).s(user).ss(rules))
}

// ACLLoad reloads security.acl_file.
func (c *cmds) ACLLoad(ctx context.Context) error { return c.ok(ctx, newCmd(2, cACL).t(tLOAD)) }

// ACLSave writes security.acl_file.
func (c *cmds) ACLSave(ctx context.Context) error { return c.ok(ctx, newCmd(2, cACL).t(tSAVE)) }

// ---- cluster -------------------------------------------------------------------

// ClusterSlots returns the slot table.
func (c *cmds) ClusterSlots(ctx context.Context) (Value, error) {
	return c.value(ctx, newCmd(2, cCLUSTER).t(tSLOTS))
}

// ClusterNodes returns CLUSTER NODES text.
func (c *cmds) ClusterNodes(ctx context.Context) (string, error) {
	return c.strR(ctx, newCmd(2, cCLUSTER).t(tNODES))
}

// ClusterShards returns CLUSTER SHARDS.
func (c *cmds) ClusterShards(ctx context.Context) (Value, error) {
	return c.value(ctx, newCmd(2, cCLUSTER).t(tSHARDS))
}

// ClusterInfo returns CLUSTER INFO text.
func (c *cmds) ClusterInfo(ctx context.Context) (string, error) {
	return c.strR(ctx, newCmd(2, cCLUSTER).t(tINFO))
}

// ClusterMyID returns the receiving shard id.
func (c *cmds) ClusterMyID(ctx context.Context) (string, error) {
	return c.strR(ctx, newCmd(2, cCLUSTER).t(tMYID))
}

// ClusterKeySlot returns the hash slot of key (also computed locally by KeySlot).
func (c *cmds) ClusterKeySlot(ctx context.Context, key string) (int64, error) {
	return c.intR(ctx, newCmd(3, cCLUSTER).t(tKEYSLOT).s(key))
}

// ReadOnly sends READONLY.
func (c *cmds) ReadOnly(ctx context.Context) error { return c.ok(ctx, newCmd(1, cREADONLY)) }

// ReadWrite sends READWRITE.
func (c *cmds) ReadWrite(ctx context.Context) error { return c.ok(ctx, newCmd(1, cREADWRITE)) }

// ---- scripting ------------------------------------------------------------------

func scriptReq(name []byte, head string, keys []string, args []any) (*request, error) {
	r := newCmd(3+len(keys)+len(args), name).s(head).i(int64(len(keys))).ss(keys)
	var err error
	for _, a := range args {
		if r.buf, err = appendArg(r.buf, a); err != nil {
			putReq(r)
			return nil, err
		}
	}
	return r, nil
}

// Eval runs a Lua script.
func (c *cmds) Eval(ctx context.Context, script string, keys []string, args ...any) (Value, error) {
	r, err := scriptReq(cEVAL, script, keys, args)
	if err != nil {
		return Value{}, err
	}
	return c.value(ctx, r)
}

// EvalSHA runs a cached script.
func (c *cmds) EvalSHA(ctx context.Context, sha string, keys []string, args ...any) (Value, error) {
	r, err := scriptReq(cEVALSHA, sha, keys, args)
	if err != nil {
		return Value{}, err
	}
	return c.value(ctx, r)
}

// ScriptLoad caches a script and returns its SHA1.
func (c *cmds) ScriptLoad(ctx context.Context, script string) (string, error) {
	return c.strR(ctx, newCmd(3, cSCRIPT).t(tLOAD).s(script))
}

// ScriptExists reports which SHA1s are cached.
func (c *cmds) ScriptExists(ctx context.Context, shas ...string) ([]bool, error) {
	return c.bools(ctx, newCmd(2+len(shas), cSCRIPT).t(tEXISTS).ss(shas))
}

// ScriptFlush empties the script cache.
func (c *cmds) ScriptFlush(ctx context.Context) error {
	return c.ok(ctx, newCmd(2, cSCRIPT).t(tFLUSH))
}

// ScriptKill stops a running script (NOTBUSY when none).
func (c *cmds) ScriptKill(ctx context.Context) error {
	return c.ok(ctx, newCmd(2, cSCRIPT).t(tKILL))
}

// FCall runs a function loaded with FunctionLoad.
func (c *cmds) FCall(ctx context.Context, function string, keys []string, args ...any) (Value, error) {
	r, err := scriptReq(cFCALL, function, keys, args)
	if err != nil {
		return Value{}, err
	}
	return c.value(ctx, r)
}

// FunctionLoad stores one named Lua body (Ruvio form: FUNCTION LOAD name body).
func (c *cmds) FunctionLoad(ctx context.Context, name, body string) (string, error) {
	return c.strR(ctx, newCmd(4, cFUNCTION).t(tLOAD).s(name).s(body))
}

// FunctionList returns loaded function names.
func (c *cmds) FunctionList(ctx context.Context) ([]string, error) {
	return c.strs(ctx, newCmd(2, cFUNCTION).t(tLIST))
}

// FunctionDelete removes one function.
func (c *cmds) FunctionDelete(ctx context.Context, name string) (bool, error) {
	return c.boolR(ctx, newCmd(3, cFUNCTION).t(tDELETE).s(name))
}

// ---- pub/sub publishing (subscriptions live on PubSub) --------------------------

// Publish returns the number of receivers.
func (c *cmds) Publish(ctx context.Context, channel, message string) (int64, error) {
	return c.intR(ctx, newCmd(3, cPUBLISH).s(channel).s(message))
}

// SPublish publishes on a shard channel.
func (c *cmds) SPublish(ctx context.Context, channel, message string) (int64, error) {
	return c.intR(ctx, newCmd(3, cSPUBLISH).s(channel).s(message))
}
