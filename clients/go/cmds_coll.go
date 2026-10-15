// Copyright (c) 2026 Salih Cantekin. MIT License; see LICENSE.

package ruvio

import (
	"context"
	"time"
)

// ---- lists ---------------------------------------------------------------------

// LPush returns the new length.
func (c *cmds) LPush(ctx context.Context, key string, values ...string) (int64, error) {
	return c.intR(ctx, newCmd(2+len(values), cLPUSH).s(key).ss(values))
}

// RPush returns the new length.
func (c *cmds) RPush(ctx context.Context, key string, values ...string) (int64, error) {
	return c.intR(ctx, newCmd(2+len(values), cRPUSH).s(key).ss(values))
}

// LPushX pushes only when the list exists.
func (c *cmds) LPushX(ctx context.Context, key string, values ...string) (int64, error) {
	return c.intR(ctx, newCmd(2+len(values), cLPUSHX).s(key).ss(values))
}

// RPushX pushes only when the list exists.
func (c *cmds) RPushX(ctx context.Context, key string, values ...string) (int64, error) {
	return c.intR(ctx, newCmd(2+len(values), cRPUSHX).s(key).ss(values))
}

// LPushCap pushes at the head, then trims the tail to at most cap items (Ruvio).
func (c *cmds) LPushCap(ctx context.Context, key string, cap int64, values ...string) (int64, error) {
	return c.intR(ctx, newCmd(3+len(values), cLPUSHCAP).s(key).i(cap).ss(values))
}

// RPushCap pushes at the tail, then trims the head to at most cap items (Ruvio).
func (c *cmds) RPushCap(ctx context.Context, key string, cap int64, values ...string) (int64, error) {
	return c.intR(ctx, newCmd(3+len(values), cRPUSHCAP).s(key).i(cap).ss(values))
}

// LPop pops one element.
func (c *cmds) LPop(ctx context.Context, key string) (string, bool, error) {
	return c.strNil(ctx, newCmd(2, cLPOP).s(key))
}

// LPopCount pops up to count elements.
func (c *cmds) LPopCount(ctx context.Context, key string, count int64) ([]string, error) {
	return c.strs(ctx, newCmd(3, cLPOP).s(key).i(count))
}

// RPop pops one element.
func (c *cmds) RPop(ctx context.Context, key string) (string, bool, error) {
	return c.strNil(ctx, newCmd(2, cRPOP).s(key))
}

// RPopCount pops up to count elements.
func (c *cmds) RPopCount(ctx context.Context, key string, count int64) ([]string, error) {
	return c.strs(ctx, newCmd(3, cRPOP).s(key).i(count))
}

// LLen returns the length.
func (c *cmds) LLen(ctx context.Context, key string) (int64, error) {
	return c.intR(ctx, newCmd(2, cLLEN).s(key))
}

// LIndex returns the element at index.
func (c *cmds) LIndex(ctx context.Context, key string, index int64) (string, bool, error) {
	return c.strNil(ctx, newCmd(3, cLINDEX).s(key).i(index))
}

// LRange returns [start, stop].
func (c *cmds) LRange(ctx context.Context, key string, start, stop int64) ([]string, error) {
	return c.strs(ctx, newCmd(4, cLRANGE).s(key).i(start).i(stop))
}

// LTrim keeps [start, stop].
func (c *cmds) LTrim(ctx context.Context, key string, start, stop int64) error {
	return c.ok(ctx, newCmd(4, cLTRIM).s(key).i(start).i(stop))
}

// LInsert inserts before (or after) pivot; returns the length or -1.
func (c *cmds) LInsert(ctx context.Context, key string, before bool, pivot, value string) (int64, error) {
	where := tAFTER
	if before {
		where = tBEFORE
	}
	return c.intR(ctx, newCmd(5, cLINSERT).s(key).t(where).s(pivot).s(value))
}

// LSet sets the element at index.
func (c *cmds) LSet(ctx context.Context, key string, index int64, value string) error {
	return c.ok(ctx, newCmd(4, cLSET).s(key).i(index).s(value))
}

// LRem removes count matches (0 = all, <0 from the tail).
func (c *cmds) LRem(ctx context.Context, key string, count int64, value string) (int64, error) {
	return c.intR(ctx, newCmd(4, cLREM).s(key).i(count).s(value))
}

// LPosOptions are LPOS modifiers (0 = omitted).
type LPosOptions struct {
	Rank   int64
	MaxLen int64
}

func lposReq(key, elem string, o LPosOptions, count int64, withCount bool) *request {
	n := 3
	if o.Rank != 0 {
		n += 2
	}
	if withCount {
		n += 2
	}
	if o.MaxLen != 0 {
		n += 2
	}
	r := newCmd(n, cLPOS).s(key).s(elem)
	if o.Rank != 0 {
		r.t(tRANK).i(o.Rank)
	}
	if withCount {
		r.t(tCOUNT).i(count)
	}
	if o.MaxLen != 0 {
		r.t(tMAXLEN).i(o.MaxLen)
	}
	return r
}

// LPos returns the index of the first match.
func (c *cmds) LPos(ctx context.Context, key, elem string, o LPosOptions) (int64, bool, error) {
	return c.intNil(ctx, lposReq(key, elem, o, 0, false))
}

// LPosCount returns up to count match indexes (0 = all).
func (c *cmds) LPosCount(ctx context.Context, key, elem string, count int64, o LPosOptions) ([]int64, error) {
	return c.ints(ctx, lposReq(key, elem, o, count, true))
}

// LMove moves an element; from/to are "LEFT" or "RIGHT".
func (c *cmds) LMove(ctx context.Context, src, dst, from, to string) (string, bool, error) {
	return c.strNil(ctx, newCmd(5, cLMOVE).s(src).s(dst).s(from).s(to))
}

// RPopLPush moves the tail of src to the head of dst.
func (c *cmds) RPopLPush(ctx context.Context, src, dst string) (string, bool, error) {
	return c.strNil(ctx, newCmd(3, cRPOPLPUSH).s(src).s(dst))
}

// BLMove is LMove that waits up to timeout (dedicated connection).
func (c *cmds) BLMove(ctx context.Context, src, dst, from, to string, timeout time.Duration) (string, bool, error) {
	v, err := c.ex.execBlocking(ctx, newCmd(6, cBLMOVE).s(src).s(dst).s(from).s(to).f(secs(timeout)))
	if err != nil || v.Kind == KindNil {
		return "", false, err
	}
	return v.Str(), true, nil
}

// KeyValue is a (key, element) pair from a blocking pop.
type KeyValue struct {
	Key   string
	Value string
}

func (c *cmds) bpop(ctx context.Context, name []byte, timeout time.Duration, keys []string) (KeyValue, bool, error) {
	v, err := c.ex.execBlocking(ctx, newCmd(2+len(keys), name).ss(keys).f(secs(timeout)))
	if err != nil || v.Kind == KindNil {
		return KeyValue{}, false, err
	}
	if len(v.Array) != 2 {
		return KeyValue{}, false, unexpected(v)
	}
	return KeyValue{v.Array[0].Str(), v.Array[1].Str()}, true, nil
}

// BLPop waits up to timeout (0 = forever) for an element (dedicated connection).
func (c *cmds) BLPop(ctx context.Context, timeout time.Duration, keys ...string) (KeyValue, bool, error) {
	return c.bpop(ctx, cBLPOP, timeout, keys)
}

// BRPop waits up to timeout for an element at the tail (dedicated connection).
func (c *cmds) BRPop(ctx context.Context, timeout time.Duration, keys ...string) (KeyValue, bool, error) {
	return c.bpop(ctx, cBRPOP, timeout, keys)
}

// KeyValues is the LMPOP reply.
type KeyValues struct {
	Key    string
	Values []string
}

func mpopReq(name []byte, timeout *time.Duration, dir string, count int64, keys []string) *request {
	n := 3 + len(keys)
	if timeout != nil {
		n++
	}
	if count > 0 {
		n += 2
	}
	r := newCmd(n, name)
	if timeout != nil {
		r.f(secs(*timeout))
	}
	r.i(int64(len(keys))).ss(keys).s(dir)
	if count > 0 {
		r.t(tCOUNT).i(count)
	}
	return r
}

func toKeyValues(v Value) (KeyValues, bool, error) {
	if v.Kind == KindNil {
		return KeyValues{}, false, nil
	}
	if len(v.Array) != 2 {
		return KeyValues{}, false, unexpected(v)
	}
	vals, err := v.Array[1].Strings()
	return KeyValues{v.Array[0].Str(), vals}, err == nil, err
}

// LMPop pops up to count (0 = 1) from the first non-empty list; dir is LEFT or RIGHT.
func (c *cmds) LMPop(ctx context.Context, dir string, count int64, keys ...string) (KeyValues, bool, error) {
	v, err := c.value(ctx, mpopReq(cLMPOP, nil, dir, count, keys))
	if err != nil {
		return KeyValues{}, false, err
	}
	return toKeyValues(v)
}

// BLMPop is LMPop that waits up to timeout (dedicated connection).
func (c *cmds) BLMPop(ctx context.Context, timeout time.Duration, dir string, count int64, keys ...string) (KeyValues, bool, error) {
	v, err := c.ex.execBlocking(ctx, mpopReq(cBLMPOP, &timeout, dir, count, keys))
	if err != nil {
		return KeyValues{}, false, err
	}
	return toKeyValues(v)
}

// ---- sets ------------------------------------------------------------------------

// SAdd returns how many members were new.
func (c *cmds) SAdd(ctx context.Context, key string, members ...string) (int64, error) {
	return c.intR(ctx, newCmd(2+len(members), cSADD).s(key).ss(members))
}

// SRem returns how many were removed.
func (c *cmds) SRem(ctx context.Context, key string, members ...string) (int64, error) {
	return c.intR(ctx, newCmd(2+len(members), cSREM).s(key).ss(members))
}

// SIsMember tests membership.
func (c *cmds) SIsMember(ctx context.Context, key, member string) (bool, error) {
	return c.boolR(ctx, newCmd(3, cSISMEMBER).s(key).s(member))
}

// SMIsMember tests several members.
func (c *cmds) SMIsMember(ctx context.Context, key string, members ...string) ([]bool, error) {
	return c.bools(ctx, newCmd(2+len(members), cSMISMEMBER).s(key).ss(members))
}

// SCard returns the set size.
func (c *cmds) SCard(ctx context.Context, key string) (int64, error) {
	return c.intR(ctx, newCmd(2, cSCARD).s(key))
}

// SMembers returns all members (sorted by Ruvio).
func (c *cmds) SMembers(ctx context.Context, key string) ([]string, error) {
	return c.strs(ctx, newCmd(2, cSMEMBERS).s(key))
}

// SInter intersects sets.
func (c *cmds) SInter(ctx context.Context, keys ...string) ([]string, error) {
	return c.strs(ctx, newCmd(1+len(keys), cSINTER).ss(keys))
}

// SUnion unions sets.
func (c *cmds) SUnion(ctx context.Context, keys ...string) ([]string, error) {
	return c.strs(ctx, newCmd(1+len(keys), cSUNION).ss(keys))
}

// SDiff subtracts later sets from the first.
func (c *cmds) SDiff(ctx context.Context, keys ...string) ([]string, error) {
	return c.strs(ctx, newCmd(1+len(keys), cSDIFF).ss(keys))
}

// SInterStore stores the intersection.
func (c *cmds) SInterStore(ctx context.Context, dst string, keys ...string) (int64, error) {
	return c.intR(ctx, newCmd(2+len(keys), cSINTERSTORE).s(dst).ss(keys))
}

// SUnionStore stores the union.
func (c *cmds) SUnionStore(ctx context.Context, dst string, keys ...string) (int64, error) {
	return c.intR(ctx, newCmd(2+len(keys), cSUNIONSTORE).s(dst).ss(keys))
}

// SDiffStore stores the difference.
func (c *cmds) SDiffStore(ctx context.Context, dst string, keys ...string) (int64, error) {
	return c.intR(ctx, newCmd(2+len(keys), cSDIFFSTORE).s(dst).ss(keys))
}

// SInterCard counts the intersection (limit 0 = no limit).
func (c *cmds) SInterCard(ctx context.Context, limit int64, keys ...string) (int64, error) {
	if limit > 0 {
		return c.intR(ctx, newCmd(4+len(keys), cSINTERCARD).i(int64(len(keys))).ss(keys).t(tLIMIT).i(limit))
	}
	return c.intR(ctx, newCmd(2+len(keys), cSINTERCARD).i(int64(len(keys))).ss(keys))
}

// SMove moves member from src to dst.
func (c *cmds) SMove(ctx context.Context, src, dst, member string) (bool, error) {
	return c.boolR(ctx, newCmd(4, cSMOVE).s(src).s(dst).s(member))
}

// SPop removes one random member.
func (c *cmds) SPop(ctx context.Context, key string) (string, bool, error) {
	return c.strNil(ctx, newCmd(2, cSPOP).s(key))
}

// SPopCount removes up to count random members.
func (c *cmds) SPopCount(ctx context.Context, key string, count int64) ([]string, error) {
	return c.strs(ctx, newCmd(3, cSPOP).s(key).i(count))
}

// SRandMember returns one random member.
func (c *cmds) SRandMember(ctx context.Context, key string) (string, bool, error) {
	return c.strNil(ctx, newCmd(2, cSRANDMEMBER).s(key))
}

// SRandMemberCount returns count members (negative may repeat).
func (c *cmds) SRandMemberCount(ctx context.Context, key string, count int64) ([]string, error) {
	return c.strs(ctx, newCmd(3, cSRANDMEMBER).s(key).i(count))
}

func (c *cmds) keyScan(ctx context.Context, name []byte, key, cursor string, o ScanOptions) (string, []string, error) {
	o.Type = ""
	r := newCmd(3+o.n(), name).s(key).s(cursor)
	o.append(r)
	v, err := c.value(ctx, r)
	if err != nil {
		return "", nil, err
	}
	return scanReply(v)
}

// SScan returns one page of members and the next cursor.
func (c *cmds) SScan(ctx context.Context, key, cursor string, o ScanOptions) (string, []string, error) {
	return c.keyScan(ctx, cSSCAN, key, cursor, o)
}

// ---- hashes ----------------------------------------------------------------------

// HSet sets field, value, field, value... and returns how many fields were new.
func (c *cmds) HSet(ctx context.Context, key string, fieldValues ...string) (int64, error) {
	return c.intR(ctx, newCmd(2+len(fieldValues), cHSET).s(key).ss(fieldValues))
}

// HSetBytes sets one binary field.
func (c *cmds) HSetBytes(ctx context.Context, key, field string, value []byte) (int64, error) {
	return c.intR(ctx, newCmd(4, cHSET).s(key).s(field).b(value))
}

// HSetNX sets the field only when it is missing.
func (c *cmds) HSetNX(ctx context.Context, key, field, value string) (bool, error) {
	return c.boolR(ctx, newCmd(4, cHSETNX).s(key).s(field).s(value))
}

// HGet returns a field.
func (c *cmds) HGet(ctx context.Context, key, field string) (string, bool, error) {
	return c.strNil(ctx, newCmd(3, cHGET).s(key).s(field))
}

// HGetBytes is the binary form of HGet.
func (c *cmds) HGetBytes(ctx context.Context, key, field string) ([]byte, bool, error) {
	return c.bytesNil(ctx, newCmd(3, cHGET).s(key).s(field))
}

// HDel removes fields.
func (c *cmds) HDel(ctx context.Context, key string, fields ...string) (int64, error) {
	return c.intR(ctx, newCmd(2+len(fields), cHDEL).s(key).ss(fields))
}

// HLen counts fields.
func (c *cmds) HLen(ctx context.Context, key string) (int64, error) {
	return c.intR(ctx, newCmd(2, cHLEN).s(key))
}

// HGetAll returns every field.
func (c *cmds) HGetAll(ctx context.Context, key string) (map[string]string, error) {
	return c.strMap(ctx, newCmd(2, cHGETALL).s(key))
}

// HMGet returns nullable values for fields.
func (c *cmds) HMGet(ctx context.Context, key string, fields ...string) ([]NullString, error) {
	return c.nullStrs(ctx, newCmd(2+len(fields), cHMGET).s(key).ss(fields))
}

// HExists tests a field.
func (c *cmds) HExists(ctx context.Context, key, field string) (bool, error) {
	return c.boolR(ctx, newCmd(3, cHEXISTS).s(key).s(field))
}

// HKeys returns field names (sorted).
func (c *cmds) HKeys(ctx context.Context, key string) ([]string, error) {
	return c.strs(ctx, newCmd(2, cHKEYS).s(key))
}

// HVals returns values (sorted by field).
func (c *cmds) HVals(ctx context.Context, key string) ([]string, error) {
	return c.strs(ctx, newCmd(2, cHVALS).s(key))
}

// HIncrBy adds delta to a field.
func (c *cmds) HIncrBy(ctx context.Context, key, field string, delta int64) (int64, error) {
	return c.intR(ctx, newCmd(4, cHINCRBY).s(key).s(field).i(delta))
}

// HIncrByFloat adds a float to a field.
func (c *cmds) HIncrByFloat(ctx context.Context, key, field string, delta float64) (float64, error) {
	return c.floatR(ctx, newCmd(4, cHINCRBYFLOAT).s(key).s(field).f(delta))
}

// HStrLen returns a field's length.
func (c *cmds) HStrLen(ctx context.Context, key, field string) (int64, error) {
	return c.intR(ctx, newCmd(3, cHSTRLEN).s(key).s(field))
}

// HScan returns one page of field, value, field, value... and the next cursor.
func (c *cmds) HScan(ctx context.Context, key, cursor string, o ScanOptions) (string, []string, error) {
	return c.keyScan(ctx, cHSCAN, key, cursor, o)
}

// HRandField returns count random fields (negative may repeat).
func (c *cmds) HRandField(ctx context.Context, key string, count int64) ([]string, error) {
	return c.strs(ctx, newCmd(3, cHRANDFIELD).s(key).i(count))
}

// HRandFieldWithValues returns count random field, value pairs as a flat list.
func (c *cmds) HRandFieldWithValues(ctx context.Context, key string, count int64) ([]string, error) {
	return c.strs(ctx, newCmd(4, cHRANDFIELD).s(key).i(count).t(tWITHVALUES))
}

// FieldExpireCond is an HEXPIRE-family condition: "", "NX", "XX", "GT" or "LT".
type FieldExpireCond string

func fieldExpReq(name []byte, key string, t int64, cond FieldExpireCond, fields []string) *request {
	n := 5 + len(fields)
	if cond != "" {
		n++
	}
	r := newCmd(n, name).s(key).i(t)
	if cond != "" {
		r.s(string(cond))
	}
	return r.t(tFIELDS).i(int64(len(fields))).ss(fields)
}

// HExpire sets a field TTL in seconds; one result per field (-2, 0, 1, 2).
func (c *cmds) HExpire(ctx context.Context, key string, ttl time.Duration, cond FieldExpireCond, fields ...string) ([]int64, error) {
	return c.ints(ctx, fieldExpReq(cHEXPIRE, key, int64(ttl/time.Second), cond, fields))
}

// HPExpire sets a field TTL in milliseconds.
func (c *cmds) HPExpire(ctx context.Context, key string, ttl time.Duration, cond FieldExpireCond, fields ...string) ([]int64, error) {
	return c.ints(ctx, fieldExpReq(cHPEXPIRE, key, ms(ttl), cond, fields))
}

// HExpireAt sets an absolute field deadline in Unix seconds.
func (c *cmds) HExpireAt(ctx context.Context, key string, at time.Time, cond FieldExpireCond, fields ...string) ([]int64, error) {
	return c.ints(ctx, fieldExpReq(cHEXPIREAT, key, at.Unix(), cond, fields))
}

// HPExpireAt sets an absolute field deadline in Unix milliseconds.
func (c *cmds) HPExpireAt(ctx context.Context, key string, at time.Time, cond FieldExpireCond, fields ...string) ([]int64, error) {
	return c.ints(ctx, fieldExpReq(cHPEXPIREAT, key, at.UnixMilli(), cond, fields))
}

func fieldsReq(name []byte, key string, fields []string) *request {
	return newCmd(4+len(fields), name).s(key).t(tFIELDS).i(int64(len(fields))).ss(fields)
}

// HTTL returns field TTLs in seconds.
func (c *cmds) HTTL(ctx context.Context, key string, fields ...string) ([]int64, error) {
	return c.ints(ctx, fieldsReq(cHTTL, key, fields))
}

// HPTTL returns field TTLs in milliseconds.
func (c *cmds) HPTTL(ctx context.Context, key string, fields ...string) ([]int64, error) {
	return c.ints(ctx, fieldsReq(cHPTTL, key, fields))
}

// HExpireTime returns field deadlines in Unix seconds.
func (c *cmds) HExpireTime(ctx context.Context, key string, fields ...string) ([]int64, error) {
	return c.ints(ctx, fieldsReq(cHEXPIRETIME, key, fields))
}

// HPExpireTime returns field deadlines in Unix milliseconds.
func (c *cmds) HPExpireTime(ctx context.Context, key string, fields ...string) ([]int64, error) {
	return c.ints(ctx, fieldsReq(cHPEXPIRETIME, key, fields))
}

// HPersist removes field TTLs.
func (c *cmds) HPersist(ctx context.Context, key string, fields ...string) ([]int64, error) {
	return c.ints(ctx, fieldsReq(cHPERSIST, key, fields))
}
