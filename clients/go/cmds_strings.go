// Copyright (c) 2026 Salih Cantekin. MIT License; see LICENSE.

package ruvio

import (
	"context"
	"time"
)

// ---- strings ---------------------------------------------------------------

// Get returns the string at key; ok is false when the key is missing.
func (c *cmds) Get(ctx context.Context, key string) (string, bool, error) {
	return c.strNil(ctx, newCmd(2, cGET).s(key))
}

// GetBytes is the binary form of Get.
func (c *cmds) GetBytes(ctx context.Context, key string) ([]byte, bool, error) {
	return c.bytesNil(ctx, newCmd(2, cGET).s(key))
}

// Set writes value with no options.
func (c *cmds) Set(ctx context.Context, key, value string) error {
	return c.ok(ctx, newCmd(3, cSET).s(key).s(value))
}

// SetBytes is the binary form of Set.
func (c *cmds) SetBytes(ctx context.Context, key string, value []byte) error {
	return c.ok(ctx, newCmd(3, cSET).s(key).b(value))
}

// SetOptions are the SET modifiers. TTL is sent as PX, ExpireAt as PXAT.
type SetOptions struct {
	TTL      time.Duration
	ExpireAt time.Time
	KeepTTL  bool
	NX       bool
	XX       bool
}

func (o *SetOptions) count() int {
	n := 0
	if o.TTL > 0 || !o.ExpireAt.IsZero() {
		n += 2
	} else if o.KeepTTL {
		n++
	}
	if o.NX || o.XX {
		n++
	}
	return n
}

func (o *SetOptions) append(r *request) {
	switch {
	case o.TTL > 0:
		r.t(tPX).i(ms(o.TTL))
	case !o.ExpireAt.IsZero():
		r.t(tPXAT).i(o.ExpireAt.UnixMilli())
	case o.KeepTTL:
		r.t(tKEEPTTL)
	}
	if o.NX {
		r.t(tNX)
	} else if o.XX {
		r.t(tXX)
	}
}

// SetArgs runs SET with options. It returns false when NX/XX prevented the write.
func (c *cmds) SetArgs(ctx context.Context, key, value string, o SetOptions) (bool, error) {
	v, err := c.value(ctx, o.build(cSET, key, value, false))
	return err == nil && v.Kind != KindNil, err
}

func (o *SetOptions) build(name []byte, key, value string, get bool) *request {
	n := 3 + o.count()
	if get {
		n++
	}
	r := newCmd(n, name).s(key).s(value)
	o.append(r)
	if get {
		r.t(tGET)
	}
	return r
}

// SetGet runs SET ... GET and returns the previous value.
func (c *cmds) SetGet(ctx context.Context, key, value string, o SetOptions) (string, bool, error) {
	return c.strNil(ctx, o.build(cSET, key, value, true))
}

// SetIfEq writes value only when the current string equals expected (Ruvio
// `SET key value IFEQ expected`). TTL, ExpireAt and KeepTTL apply; NX/XX are ignored.
func (c *cmds) SetIfEq(ctx context.Context, key, value, expected string, o SetOptions) (bool, error) {
	o.NX, o.XX = false, false
	r := newCmd(5+o.count(), cSET).s(key).s(value).t(tIFEQ).s(expected)
	o.append(r)
	v, err := c.value(ctx, r)
	return err == nil && v.Kind != KindNil, err
}

// SetEX is SETEX key seconds value.
func (c *cmds) SetEX(ctx context.Context, key, value string, ttl time.Duration) error {
	return c.ok(ctx, newCmd(4, cSETEX).s(key).i(int64(ttl/time.Second)).s(value))
}

// PSetEX is PSETEX key milliseconds value.
func (c *cmds) PSetEX(ctx context.Context, key, value string, ttl time.Duration) error {
	return c.ok(ctx, newCmd(4, cPSETEX).s(key).i(ms(ttl)).s(value))
}

// SetNX returns true when it wrote.
func (c *cmds) SetNX(ctx context.Context, key, value string) (bool, error) {
	return c.boolR(ctx, newCmd(3, cSETNX).s(key).s(value))
}

// MGet returns one nullable value per key.
func (c *cmds) MGet(ctx context.Context, keys ...string) ([]NullString, error) {
	return c.nullStrs(ctx, newCmd(1+len(keys), cMGET).ss(keys))
}

// MSet writes key, value, key, value...
func (c *cmds) MSet(ctx context.Context, keyValues ...string) error {
	return c.ok(ctx, newCmd(1+len(keyValues), cMSET).ss(keyValues))
}

// MSetNX writes every pair only when none of the keys exist.
func (c *cmds) MSetNX(ctx context.Context, keyValues ...string) (bool, error) {
	return c.boolR(ctx, newCmd(1+len(keyValues), cMSETNX).ss(keyValues))
}

// GetSet sets and returns the old value.
func (c *cmds) GetSet(ctx context.Context, key, value string) (string, bool, error) {
	return c.strNil(ctx, newCmd(3, cGETSET).s(key).s(value))
}

// GetDel returns and removes the value.
func (c *cmds) GetDel(ctx context.Context, key string) (string, bool, error) {
	return c.strNil(ctx, newCmd(2, cGETDEL).s(key))
}

// GetEx returns the string and sets its deadline to now + ttl (Ruvio: milliseconds).
func (c *cmds) GetEx(ctx context.Context, key string, ttl time.Duration) (string, bool, error) {
	return c.strNil(ctx, newCmd(3, cGETEX).s(key).i(ms(ttl)))
}

// Append returns the new length.
func (c *cmds) Append(ctx context.Context, key, value string) (int64, error) {
	return c.intR(ctx, newCmd(3, cAPPEND).s(key).s(value))
}

// StrLen returns the string length.
func (c *cmds) StrLen(ctx context.Context, key string) (int64, error) {
	return c.intR(ctx, newCmd(2, cSTRLEN).s(key))
}

// Incr increments by one.
func (c *cmds) Incr(ctx context.Context, key string) (int64, error) {
	return c.intR(ctx, newCmd(2, cINCR).s(key))
}

// Decr decrements by one.
func (c *cmds) Decr(ctx context.Context, key string) (int64, error) {
	return c.intR(ctx, newCmd(2, cDECR).s(key))
}

// IncrBy adds delta.
func (c *cmds) IncrBy(ctx context.Context, key string, delta int64) (int64, error) {
	return c.intR(ctx, newCmd(3, cINCRBY).s(key).i(delta))
}

// IncrByMax adds delta unless the result would pass limit (Ruvio `INCRBY key n MAX
// limit`); ok is false and nothing is written in that case.
func (c *cmds) IncrByMax(ctx context.Context, key string, delta, limit int64) (int64, bool, error) {
	return c.intNil(ctx, newCmd(5, cINCRBY).s(key).i(delta).t(tMAX).i(limit))
}

// DecrBy subtracts delta.
func (c *cmds) DecrBy(ctx context.Context, key string, delta int64) (int64, error) {
	return c.intR(ctx, newCmd(3, cDECRBY).s(key).i(delta))
}

// IncrByFloat adds a float.
func (c *cmds) IncrByFloat(ctx context.Context, key string, delta float64) (float64, error) {
	return c.floatR(ctx, newCmd(3, cINCRBYFLOAT).s(key).f(delta))
}

// GetRange returns a substring (inclusive ends).
func (c *cmds) GetRange(ctx context.Context, key string, start, end int64) (string, error) {
	return c.strR(ctx, newCmd(4, cGETRANGE).s(key).i(start).i(end))
}

// Substr is the GETRANGE alias.
func (c *cmds) Substr(ctx context.Context, key string, start, end int64) (string, error) {
	return c.strR(ctx, newCmd(4, cSUBSTR).s(key).i(start).i(end))
}

// SetRange overwrites at offset and returns the new length.
func (c *cmds) SetRange(ctx context.Context, key string, offset int64, value string) (int64, error) {
	return c.intR(ctx, newCmd(4, cSETRANGE).s(key).i(offset).s(value))
}

// ---- Ruvio string extensions ----------------------------------------------

// SetV writes a versioned string. version 0 creates version 1; otherwise pass the
// current version. Returns the new version; a mismatch is a server error.
func (c *cmds) SetV(ctx context.Context, key, value string, version int64) (int64, error) {
	if version <= 0 {
		return c.intR(ctx, newCmd(3, cSETV).s(key).s(value))
	}
	return c.intR(ctx, newCmd(4, cSETV).s(key).s(value).i(version))
}

// Take subtracts count when the key holds at least that many. ok is false (nothing
// changed) on a missing key or a shortfall.
func (c *cmds) Take(ctx context.Context, key string, count int64) (remaining int64, ok bool, err error) {
	return c.intNil(ctx, newCmd(3, cTAKE).s(key).i(count))
}

// DelIfEq deletes the string only when it equals expected (`DELEX key IFEQ value`).
func (c *cmds) DelIfEq(ctx context.Context, key, expected string) (bool, error) {
	return c.boolR(ctx, newCmd(4, cDELEX).s(key).t(tIFEQ).s(expected))
}

// Limit is a fixed-window counter: allowed is false when the window is full;
// remaining is the allowance left after this call.
func (c *cmds) Limit(ctx context.Context, key string, max int64, window time.Duration) (remaining int64, allowed bool, err error) {
	return c.intNil(ctx, newCmd(4, cLIMIT).s(key).i(max).i(ms(window)))
}

// Once returns true for the first call until ttl passes.
func (c *cmds) Once(ctx context.Context, key string, ttl time.Duration) (bool, error) {
	return c.boolR(ctx, newCmd(3, cONCE).s(key).i(ms(ttl)))
}

// Lease acquires a fenced lease. ok is false while another holder has it.
func (c *cmds) Lease(ctx context.Context, key string, ttl time.Duration) (token int64, ok bool, err error) {
	return c.intNil(ctx, newCmd(3, cLEASE).s(key).i(ms(ttl)))
}

// Semaphore takes one of max slots. ok is false when full.
func (c *cmds) Semaphore(ctx context.Context, key string, max int64, ttl time.Duration) (token int64, ok bool, err error) {
	return c.intNil(ctx, newCmd(4, cSEMAPHORE).s(key).i(max).i(ms(ttl)))
}

// Release frees a lease or semaphore slot held with token.
func (c *cmds) Release(ctx context.Context, key string, token int64) (bool, error) {
	return c.boolR(ctx, newCmd(3, cRELEASE).s(key).i(token))
}

// IdemState is the IDEM BEGIN outcome.
type IdemState string

const (
	IdemAcquired  IdemState = "acquired"
	IdemPending   IdemState = "pending"
	IdemConflict  IdemState = "conflict"
	IdemCompleted IdemState = "completed"
)

// IdemResult is the IDEM BEGIN reply; Result is set when State is IdemCompleted.
type IdemResult struct {
	State  IdemState
	Result []byte
}

// IdemBegin reserves key for (fingerprint, owner) for ttl.
func (c *cmds) IdemBegin(ctx context.Context, key, fingerprint, owner string, ttl time.Duration) (IdemResult, error) {
	v, err := c.value(ctx, newCmd(6, cIDEM).s(key).t(tBEGIN).s(fingerprint).s(owner).i(ms(ttl)))
	if err != nil {
		return IdemResult{}, err
	}
	if !v.isAgg() || len(v.Array) == 0 {
		return IdemResult{}, unexpected(v)
	}
	res := IdemResult{State: IdemState(v.Array[0].Str())}
	if len(v.Array) > 1 {
		res.Result = v.Array[1].Bytes
	}
	return res, nil
}

// IdemComplete publishes result for the live owner. Returns the server's integer.
func (c *cmds) IdemComplete(ctx context.Context, key, fingerprint, owner string, result []byte) (int64, error) {
	return c.intR(ctx, newCmd(6, cIDEM).s(key).t(tCOMPLETE).s(fingerprint).s(owner).b(result))
}

// ChangesStart arms the change feed and returns the cursor.
func (c *cmds) ChangesStart(ctx context.Context) (int64, error) {
	return c.intR(ctx, newCmd(2, cCHANGES).t(tSTART))
}

// Change is one change-feed note.
type Change struct {
	Seq  int64
	Note string
}

// Changes reads notes after cursor (at most 1024).
func (c *cmds) Changes(ctx context.Context, cursor int64) ([]Change, error) {
	v, err := c.value(ctx, newCmd(2, cCHANGES).i(cursor))
	if err != nil {
		return nil, err
	}
	if !v.isAgg() {
		return nil, unexpected(v)
	}
	out := make([]Change, 0, len(v.Array))
	for _, row := range v.Array {
		if len(row.Array) < 2 {
			return nil, unexpected(v)
		}
		out = append(out, Change{Seq: row.Array[0].Int, Note: row.Array[1].Str()})
	}
	return out, nil
}

// FillState is the FILLGET outcome.
type FillState string

const (
	FillHit   FillState = "HIT"
	FillFill  FillState = "FILL"
	FillBusy  FillState = "BUSY"
	FillStale FillState = "STALE"
)

// FillResult is the FILLGET reply. Value is set for HIT and STALE; Token is set for
// FILL and for STALE when this caller should refresh.
type FillResult struct {
	State FillState
	Value string
	Token string
}

func toFill(v Value) (FillResult, error) {
	if !v.isAgg() || len(v.Array) < 2 {
		return FillResult{}, unexpected(v)
	}
	r := FillResult{State: FillState(v.Array[0].Str())}
	switch r.State {
	case FillHit:
		r.Value = v.Array[1].Str()
	case FillFill:
		r.Token = v.Array[1].Str()
	case FillStale:
		r.Value = v.Array[1].Str()
		if len(v.Array) > 2 {
			r.Token = v.Array[2].Str()
		}
	}
	return r, nil
}

// FillGet reads a single-flight cache key; fill is how long a FILL marker lives.
func (c *cmds) FillGet(ctx context.Context, key string, fill time.Duration) (FillResult, error) {
	v, err := c.value(ctx, newCmd(3, cFILLGET).s(key).i(ms(fill)))
	if err != nil {
		return FillResult{}, err
	}
	return toFill(v)
}

// FillGetWait is FILLGET ... WAIT: it parks until the filler writes, so it runs on a
// dedicated connection (never stalls the shared one).
func (c *cmds) FillGetWait(ctx context.Context, key string, fill, wait time.Duration) (FillResult, error) {
	v, err := c.ex.execBlocking(ctx, newCmd(5, cFILLGET).s(key).i(ms(fill)).t(tWAIT).i(ms(wait)))
	if err != nil {
		return FillResult{}, err
	}
	return toFill(v)
}

// FillSet writes the value for a FILL token. stale > 0 adds SWR (stale-while-revalidate).
func (c *cmds) FillSet(ctx context.Context, key, value string, ttl time.Duration, token string, stale time.Duration) (bool, error) {
	if stale > 0 {
		return c.boolR(ctx, newCmd(7, cFILLSET).s(key).s(value).i(ms(ttl)).s(token).t(tSWR).i(ms(stale)))
	}
	return c.boolR(ctx, newCmd(5, cFILLSET).s(key).s(value).i(ms(ttl)).s(token))
}

// Ruvio sends RUVIO (no arguments).
func (c *cmds) Ruvio(ctx context.Context) (string, error) {
	return c.strR(ctx, newCmd(1, cRUVIO))
}

// RuvioOwner sends RUVIO OWNER.
func (c *cmds) RuvioOwner(ctx context.Context) (string, error) {
	return c.strR(ctx, newCmd(2, cRUVIO).t(tOWNER))
}

// ---- bitmaps & HyperLogLog ---------------------------------------------------

// SetBit returns the previous bit.
func (c *cmds) SetBit(ctx context.Context, key string, offset int64, bit int) (int64, error) {
	return c.intR(ctx, newCmd(4, cSETBIT).s(key).i(offset).i(int64(bit)))
}

// GetBit returns the bit at offset.
func (c *cmds) GetBit(ctx context.Context, key string, offset int64) (int64, error) {
	return c.intR(ctx, newCmd(3, cGETBIT).s(key).i(offset))
}

// BitCount counts set bits in the whole string.
func (c *cmds) BitCount(ctx context.Context, key string) (int64, error) {
	return c.intR(ctx, newCmd(2, cBITCOUNT).s(key))
}

// BitCountRange counts set bits in [start, end] bytes, or bits when bitUnits.
func (c *cmds) BitCountRange(ctx context.Context, key string, start, end int64, bitUnits bool) (int64, error) {
	if bitUnits {
		return c.intR(ctx, newCmd(5, cBITCOUNT).s(key).i(start).i(end).t(tBIT))
	}
	return c.intR(ctx, newCmd(4, cBITCOUNT).s(key).i(start).i(end))
}

// BitPos finds the first bit set to bit, optionally within start [end] bytes.
func (c *cmds) BitPos(ctx context.Context, key string, bit int, startEnd ...int64) (int64, error) {
	r := newCmd(3+len(startEnd), cBITPOS).s(key).i(int64(bit))
	for _, x := range startEnd {
		r.i(x)
	}
	return c.intR(ctx, r)
}

// BitOp runs AND, OR, XOR or NOT into dest and returns the result length.
func (c *cmds) BitOp(ctx context.Context, op, dest string, keys ...string) (int64, error) {
	return c.intR(ctx, newCmd(3+len(keys), cBITOP).s(op).s(dest).ss(keys))
}

// BitFieldOp is one BITFIELD operation: Op is GET, SET, INCRBY or OVERFLOW.
// Type is i1..i64/u1..u63, Offset a number or #n; for OVERFLOW, Type holds
// WRAP, SAT or FAIL.
type BitFieldOp struct {
	Op     string
	Type   string
	Offset string
	Value  int64
}

func bitfieldReq(name []byte, key string, ops []BitFieldOp) *request {
	n := 2
	for _, o := range ops {
		switch o.Op {
		case "OVERFLOW", "overflow":
			n += 2
		case "GET", "get":
			n += 3
		default:
			n += 4
		}
	}
	r := newCmd(n, name).s(key)
	for _, o := range ops {
		switch o.Op {
		case "OVERFLOW", "overflow":
			r.s(o.Op).s(o.Type)
		case "GET", "get":
			r.s(o.Op).s(o.Type).s(o.Offset)
		default:
			r.s(o.Op).s(o.Type).s(o.Offset).i(o.Value)
		}
	}
	return r
}

// BitField runs BITFIELD; a nil element (OVERFLOW FAIL) is reported as not Valid.
func (c *cmds) BitField(ctx context.Context, key string, ops ...BitFieldOp) ([]NullInt, error) {
	v, err := c.value(ctx, bitfieldReq(cBITFIELD, key, ops))
	if err != nil {
		return nil, err
	}
	return toNullInts(v)
}

// BitFieldRO runs BITFIELD_RO with GET operations only.
func (c *cmds) BitFieldRO(ctx context.Context, key string, ops ...BitFieldOp) ([]NullInt, error) {
	v, err := c.value(ctx, bitfieldReq(cBITFIELD_RO, key, ops))
	if err != nil {
		return nil, err
	}
	return toNullInts(v)
}

// NullInt is a nullable integer element.
type NullInt struct {
	Val   int64
	Valid bool
}

func toNullInts(v Value) ([]NullInt, error) {
	if !v.isAgg() {
		return nil, unexpected(v)
	}
	out := make([]NullInt, len(v.Array))
	for i, e := range v.Array {
		if e.Kind != KindNil {
			n, err := e.AsInt64()
			if err != nil {
				return nil, err
			}
			out[i] = NullInt{n, true}
		}
	}
	return out, nil
}

// PFAdd adds elements; true when the estimate changed.
func (c *cmds) PFAdd(ctx context.Context, key string, elements ...string) (bool, error) {
	return c.boolR(ctx, newCmd(2+len(elements), cPFADD).s(key).ss(elements))
}

// PFCount estimates the cardinality (union for several keys).
func (c *cmds) PFCount(ctx context.Context, keys ...string) (int64, error) {
	return c.intR(ctx, newCmd(1+len(keys), cPFCOUNT).ss(keys))
}

// PFMerge merges sources into dest.
func (c *cmds) PFMerge(ctx context.Context, dest string, sources ...string) error {
	return c.ok(ctx, newCmd(2+len(sources), cPFMERGE).s(dest).ss(sources))
}
