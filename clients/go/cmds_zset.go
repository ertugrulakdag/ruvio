// Copyright (c) 2026 Salih Cantekin. MIT License; see LICENSE.

package ruvio

import (
	"context"
	"time"
)

// ZAddOptions are ZADD modifiers.
type ZAddOptions struct {
	NX, XX, GT, LT, CH bool
}

func (o *ZAddOptions) n() int {
	n := 0
	for _, b := range [...]bool{o.NX, o.XX, o.GT, o.LT, o.CH} {
		if b {
			n++
		}
	}
	return n
}

func (o *ZAddOptions) append(r *request) {
	if o.NX {
		r.t(tNX)
	}
	if o.XX {
		r.t(tXX)
	}
	if o.GT {
		r.t(tGT)
	}
	if o.LT {
		r.t(tLT)
	}
	if o.CH {
		r.t(tCH)
	}
}

// ZAdd adds members and returns how many were new.
func (c *cmds) ZAdd(ctx context.Context, key string, members ...Z) (int64, error) {
	return c.ZAddArgs(ctx, key, ZAddOptions{}, members...)
}

// ZAddArgs is ZADD with NX/XX/GT/LT/CH.
func (c *cmds) ZAddArgs(ctx context.Context, key string, o ZAddOptions, members ...Z) (int64, error) {
	r := newCmd(2+o.n()+2*len(members), cZADD).s(key)
	o.append(r)
	for _, m := range members {
		r.f(m.Score).s(m.Member)
	}
	return c.intR(ctx, r)
}

// ZAddIncr is ZADD ... INCR; ok is false when a condition stopped it.
func (c *cmds) ZAddIncr(ctx context.Context, key string, o ZAddOptions, member Z) (float64, bool, error) {
	r := newCmd(5+o.n(), cZADD).s(key)
	o.append(r)
	r.t(tINCR).f(member.Score).s(member.Member)
	return c.floatNil(ctx, r)
}

// ZRem removes members.
func (c *cmds) ZRem(ctx context.Context, key string, members ...string) (int64, error) {
	return c.intR(ctx, newCmd(2+len(members), cZREM).s(key).ss(members))
}

// ZIncrBy adds to a member's score.
func (c *cmds) ZIncrBy(ctx context.Context, key string, delta float64, member string) (float64, error) {
	return c.floatR(ctx, newCmd(4, cZINCRBY).s(key).f(delta).s(member))
}

// ZCard returns the set size.
func (c *cmds) ZCard(ctx context.Context, key string) (int64, error) {
	return c.intR(ctx, newCmd(2, cZCARD).s(key))
}

// ZScore returns a member's score.
func (c *cmds) ZScore(ctx context.Context, key, member string) (float64, bool, error) {
	return c.floatNil(ctx, newCmd(3, cZSCORE).s(key).s(member))
}

// ZMScore returns nullable scores.
func (c *cmds) ZMScore(ctx context.Context, key string, members ...string) ([]NullFloat, error) {
	v, err := c.value(ctx, newCmd(2+len(members), cZMSCORE).s(key).ss(members))
	if err != nil {
		return nil, err
	}
	if !v.isAgg() {
		return nil, unexpected(v)
	}
	out := make([]NullFloat, len(v.Array))
	for i, e := range v.Array {
		if e.Kind != KindNil {
			f, err := e.AsFloat64()
			if err != nil {
				return nil, err
			}
			out[i] = NullFloat{f, true}
		}
	}
	return out, nil
}

// ZRank returns the ascending rank.
func (c *cmds) ZRank(ctx context.Context, key, member string) (int64, bool, error) {
	return c.intNil(ctx, newCmd(3, cZRANK).s(key).s(member))
}

// ZRevRank returns the descending rank.
func (c *cmds) ZRevRank(ctx context.Context, key, member string) (int64, bool, error) {
	return c.intNil(ctx, newCmd(3, cZREVRANK).s(key).s(member))
}

// ZCount counts scores within [min, max] ("-inf", "(1", ...).
func (c *cmds) ZCount(ctx context.Context, key, min, max string) (int64, error) {
	return c.intR(ctx, newCmd(4, cZCOUNT).s(key).s(min).s(max))
}

// ZLexCount counts members within a lex range.
func (c *cmds) ZLexCount(ctx context.Context, key, min, max string) (int64, error) {
	return c.intR(ctx, newCmd(4, cZLEXCOUNT).s(key).s(min).s(max))
}

// ZRangeArgs is the full ZRANGE form. Limit applies to ByScore/ByLex.
type ZRangeArgs struct {
	Key     string
	Start   string
	Stop    string
	ByScore bool
	ByLex   bool
	Rev     bool
	Limit   *Limit
}

// Limit is LIMIT offset count (a negative count returns the rest).
type Limit struct {
	Offset int64
	Count  int64
}

func zrangeReq(a *ZRangeArgs, withScores bool) *request {
	n := 4
	if a.ByScore || a.ByLex {
		n++
	}
	if a.Rev {
		n++
	}
	if a.Limit != nil {
		n += 3
	}
	if withScores {
		n++
	}
	r := newCmd(n, cZRANGE).s(a.Key).s(a.Start).s(a.Stop)
	if a.ByScore {
		r.t(tBYSCORE)
	} else if a.ByLex {
		r.t(tBYLEX)
	}
	if a.Rev {
		r.t(tREV)
	}
	if a.Limit != nil {
		r.t(tLIMIT).i(a.Limit.Offset).i(a.Limit.Count)
	}
	if withScores {
		r.t(tWITHSCORES)
	}
	return r
}

// ZRange returns members by rank [start, stop].
func (c *cmds) ZRange(ctx context.Context, key string, start, stop int64) ([]string, error) {
	return c.strs(ctx, newCmd(4, cZRANGE).s(key).i(start).i(stop))
}

// ZRangeWithScores returns members and scores by rank.
func (c *cmds) ZRangeWithScores(ctx context.Context, key string, start, stop int64) ([]Z, error) {
	return c.zs(ctx, newCmd(5, cZRANGE).s(key).i(start).i(stop).t(tWITHSCORES))
}

// ZRangeArgs runs the full ZRANGE form.
func (c *cmds) ZRangeArgs(ctx context.Context, a ZRangeArgs) ([]string, error) {
	return c.strs(ctx, zrangeReq(&a, false))
}

// ZRangeArgsWithScores runs the full ZRANGE form WITHSCORES.
func (c *cmds) ZRangeArgsWithScores(ctx context.Context, a ZRangeArgs) ([]Z, error) {
	return c.zs(ctx, zrangeReq(&a, true))
}

// ZRevRange returns members by descending rank.
func (c *cmds) ZRevRange(ctx context.Context, key string, start, stop int64) ([]string, error) {
	return c.strs(ctx, newCmd(4, cZREVRANGE).s(key).i(start).i(stop))
}

// ZRevRangeWithScores returns members and scores by descending rank.
func (c *cmds) ZRevRangeWithScores(ctx context.Context, key string, start, stop int64) ([]Z, error) {
	return c.zs(ctx, newCmd(5, cZREVRANGE).s(key).i(start).i(stop).t(tWITHSCORES))
}

func rangeByReq(name []byte, key, min, max string, lim *Limit, withScores bool) *request {
	n := 4
	if lim != nil {
		n += 3
	}
	if withScores {
		n++
	}
	r := newCmd(n, name).s(key).s(min).s(max)
	if withScores {
		r.t(tWITHSCORES)
	}
	if lim != nil {
		r.t(tLIMIT).i(lim.Offset).i(lim.Count)
	}
	return r
}

// ZRangeByScore returns members with min <= score <= max (lim may be nil).
func (c *cmds) ZRangeByScore(ctx context.Context, key, min, max string, lim *Limit) ([]string, error) {
	return c.strs(ctx, rangeByReq(cZRANGEBYSCORE, key, min, max, lim, false))
}

// ZRangeByScoreWithScores is ZRangeByScore WITHSCORES.
func (c *cmds) ZRangeByScoreWithScores(ctx context.Context, key, min, max string, lim *Limit) ([]Z, error) {
	return c.zs(ctx, rangeByReq(cZRANGEBYSCORE, key, min, max, lim, true))
}

// ZRangeByLex returns members within a lex range.
func (c *cmds) ZRangeByLex(ctx context.Context, key, min, max string, lim *Limit) ([]string, error) {
	return c.strs(ctx, rangeByReq(cZRANGEBYLEX, key, min, max, lim, false))
}

// ZRevRangeByLex takes the high bound first.
func (c *cmds) ZRevRangeByLex(ctx context.Context, key, max, min string, lim *Limit) ([]string, error) {
	return c.strs(ctx, rangeByReq(cZREVRANGEBYLEX, key, max, min, lim, false))
}

// ZPopMin pops up to count lowest members (count <= 0 sends no count).
func (c *cmds) ZPopMin(ctx context.Context, key string, count int64) ([]Z, error) {
	if count <= 0 {
		return c.zs(ctx, newCmd(2, cZPOPMIN).s(key))
	}
	return c.zs(ctx, newCmd(3, cZPOPMIN).s(key).i(count))
}

// ZPopMax pops up to count highest members.
func (c *cmds) ZPopMax(ctx context.Context, key string, count int64) ([]Z, error) {
	if count <= 0 {
		return c.zs(ctx, newCmd(2, cZPOPMAX).s(key))
	}
	return c.zs(ctx, newCmd(3, cZPOPMAX).s(key).i(count))
}

// KeyZ is a BZPOPMIN/BZPOPMAX reply.
type KeyZ struct {
	Key string
	Z
}

func (c *cmds) bzpop(ctx context.Context, name []byte, timeout time.Duration, keys []string) (KeyZ, bool, error) {
	v, err := c.ex.execBlocking(ctx, newCmd(2+len(keys), name).ss(keys).f(secs(timeout)))
	if err != nil || v.Kind == KindNil {
		return KeyZ{}, false, err
	}
	if len(v.Array) != 3 {
		return KeyZ{}, false, unexpected(v)
	}
	f, err := v.Array[2].AsFloat64()
	return KeyZ{v.Array[0].Str(), Z{v.Array[1].Str(), f}}, err == nil, err
}

// BZPopMin waits up to timeout for the lowest member (dedicated connection).
func (c *cmds) BZPopMin(ctx context.Context, timeout time.Duration, keys ...string) (KeyZ, bool, error) {
	return c.bzpop(ctx, cBZPOPMIN, timeout, keys)
}

// BZPopMax waits up to timeout for the highest member (dedicated connection).
func (c *cmds) BZPopMax(ctx context.Context, timeout time.Duration, keys ...string) (KeyZ, bool, error) {
	return c.bzpop(ctx, cBZPOPMAX, timeout, keys)
}

// ZRemRangeByRank removes by rank.
func (c *cmds) ZRemRangeByRank(ctx context.Context, key string, start, stop int64) (int64, error) {
	return c.intR(ctx, newCmd(4, cZREMRANGEBYRANK).s(key).i(start).i(stop))
}

// ZRemRangeByScore removes by score.
func (c *cmds) ZRemRangeByScore(ctx context.Context, key, min, max string) (int64, error) {
	return c.intR(ctx, newCmd(4, cZREMRANGEBYSCORE).s(key).s(min).s(max))
}

// ZRemRangeByLex removes by lex range.
func (c *cmds) ZRemRangeByLex(ctx context.Context, key, min, max string) (int64, error) {
	return c.intR(ctx, newCmd(4, cZREMRANGEBYLEX).s(key).s(min).s(max))
}

// ZStore describes ZUNION/ZINTER inputs. Weights is empty or one per key;
// Aggregate is "", "SUM", "MIN" or "MAX".
type ZStore struct {
	Keys      []string
	Weights   []float64
	Aggregate string
}

func zsetOpReq(name []byte, dst string, z *ZStore, withScores bool) *request {
	n := 2 + len(z.Keys)
	if dst != "" {
		n++
	}
	if len(z.Weights) > 0 {
		n += 1 + len(z.Weights)
	}
	if z.Aggregate != "" {
		n += 2
	}
	if withScores {
		n++
	}
	r := newCmd(n, name)
	if dst != "" {
		r.s(dst)
	}
	r.i(int64(len(z.Keys))).ss(z.Keys)
	if len(z.Weights) > 0 {
		r.t(tWEIGHTS)
		for _, w := range z.Weights {
			r.f(w)
		}
	}
	if z.Aggregate != "" {
		r.t(tAGGREGATE).s(z.Aggregate)
	}
	if withScores {
		r.t(tWITHSCORES)
	}
	return r
}

// ZUnionStore stores the union into dst.
func (c *cmds) ZUnionStore(ctx context.Context, dst string, z ZStore) (int64, error) {
	return c.intR(ctx, zsetOpReq(cZUNIONSTORE, dst, &z, false))
}

// ZInterStore stores the intersection into dst.
func (c *cmds) ZInterStore(ctx context.Context, dst string, z ZStore) (int64, error) {
	return c.intR(ctx, zsetOpReq(cZINTERSTORE, dst, &z, false))
}

// ZDiffStore stores the difference into dst.
func (c *cmds) ZDiffStore(ctx context.Context, dst string, keys ...string) (int64, error) {
	return c.intR(ctx, zsetOpReq(cZDIFFSTORE, dst, &ZStore{Keys: keys}, false))
}

// ZUnion returns the union members.
func (c *cmds) ZUnion(ctx context.Context, z ZStore) ([]string, error) {
	return c.strs(ctx, zsetOpReq(cZUNION, "", &z, false))
}

// ZUnionWithScores returns the union with scores.
func (c *cmds) ZUnionWithScores(ctx context.Context, z ZStore) ([]Z, error) {
	return c.zs(ctx, zsetOpReq(cZUNION, "", &z, true))
}

// ZInter returns the intersection members.
func (c *cmds) ZInter(ctx context.Context, z ZStore) ([]string, error) {
	return c.strs(ctx, zsetOpReq(cZINTER, "", &z, false))
}

// ZInterWithScores returns the intersection with scores.
func (c *cmds) ZInterWithScores(ctx context.Context, z ZStore) ([]Z, error) {
	return c.zs(ctx, zsetOpReq(cZINTER, "", &z, true))
}

// ZDiff returns members of the first set not in the others.
func (c *cmds) ZDiff(ctx context.Context, keys ...string) ([]string, error) {
	return c.strs(ctx, zsetOpReq(cZDIFF, "", &ZStore{Keys: keys}, false))
}

// ZDiffWithScores is ZDiff WITHSCORES.
func (c *cmds) ZDiffWithScores(ctx context.Context, keys ...string) ([]Z, error) {
	return c.zs(ctx, zsetOpReq(cZDIFF, "", &ZStore{Keys: keys}, true))
}

// ZRandMember returns count random members.
func (c *cmds) ZRandMember(ctx context.Context, key string, count int64) ([]string, error) {
	return c.strs(ctx, newCmd(3, cZRANDMEMBER).s(key).i(count))
}

// ZRandMemberWithScores returns count random members with scores.
func (c *cmds) ZRandMemberWithScores(ctx context.Context, key string, count int64) ([]Z, error) {
	return c.zs(ctx, newCmd(4, cZRANDMEMBER).s(key).i(count).t(tWITHSCORES))
}

// ZScan returns one page of member, score, member, score... and the next cursor.
func (c *cmds) ZScan(ctx context.Context, key, cursor string, o ScanOptions) (string, []string, error) {
	return c.keyScan(ctx, cZSCAN, key, cursor, o)
}

// ---- geo -----------------------------------------------------------------------------

// GeoLocation is a member with optional search details.
type GeoLocation struct {
	Name      string
	Longitude float64
	Latitude  float64
	Dist      float64
	GeoHash   int64
}

// GeoAddOptions are GEOADD modifiers.
type GeoAddOptions struct {
	NX, XX, CH bool
}

// GeoAdd adds locations.
func (c *cmds) GeoAdd(ctx context.Context, key string, o GeoAddOptions, locs ...GeoLocation) (int64, error) {
	n := 2 + 3*len(locs)
	for _, b := range [...]bool{o.NX, o.XX, o.CH} {
		if b {
			n++
		}
	}
	r := newCmd(n, cGEOADD).s(key)
	if o.NX {
		r.t(tNX)
	}
	if o.XX {
		r.t(tXX)
	}
	if o.CH {
		r.t(tCH)
	}
	for _, l := range locs {
		r.f(l.Longitude).f(l.Latitude).s(l.Name)
	}
	return c.intR(ctx, r)
}

// GeoDist returns the distance in unit (m, km, ft, mi; "" = m).
func (c *cmds) GeoDist(ctx context.Context, key, m1, m2, unit string) (float64, bool, error) {
	if unit == "" {
		return c.floatNil(ctx, newCmd(4, cGEODIST).s(key).s(m1).s(m2))
	}
	return c.floatNil(ctx, newCmd(5, cGEODIST).s(key).s(m1).s(m2).s(unit))
}

// GeoHash returns 11-character geohashes.
func (c *cmds) GeoHash(ctx context.Context, key string, members ...string) ([]NullString, error) {
	return c.nullStrs(ctx, newCmd(2+len(members), cGEOHASH).s(key).ss(members))
}

// GeoPos is a nullable position.
type GeoPos struct {
	Longitude, Latitude float64
	Valid               bool
}

// GeoPosition returns positions (GEOPOS).
func (c *cmds) GeoPosition(ctx context.Context, key string, members ...string) ([]GeoPos, error) {
	v, err := c.value(ctx, newCmd(2+len(members), cGEOPOS).s(key).ss(members))
	if err != nil {
		return nil, err
	}
	out := make([]GeoPos, len(v.Array))
	for i, e := range v.Array {
		if len(e.Array) == 2 {
			lon, _ := e.Array[0].AsFloat64()
			lat, _ := e.Array[1].AsFloat64()
			out[i] = GeoPos{lon, lat, true}
		}
	}
	return out, nil
}

// GeoSearchQuery is the GEOSEARCH query. Set Member (FROMMEMBER) or Longitude/Latitude
// (FROMLONLAT); Radius (BYRADIUS) or Width/Height (BYBOX). Sort is "", "ASC" or "DESC".
type GeoSearchQuery struct {
	Member              string
	Longitude, Latitude float64
	Radius              float64
	Width, Height       float64
	Unit                string
	Sort                string
	Count               int64
	Any                 bool
	WithDist            bool
	WithHash            bool
	WithCoord           bool
}

func (q *GeoSearchQuery) unit() string {
	if q.Unit == "" {
		return "m"
	}
	return q.Unit
}

func (q *GeoSearchQuery) n(store bool) int {
	n := 2 // FROM...
	if q.Member == "" {
		n++
	}
	if q.Radius > 0 {
		n += 3
	} else {
		n += 4
	}
	if q.Sort != "" {
		n++
	}
	if q.Count > 0 {
		n += 2
		if q.Any {
			n++
		}
	}
	if !store {
		for _, b := range [...]bool{q.WithDist, q.WithHash, q.WithCoord} {
			if b {
				n++
			}
		}
	}
	return n
}

func (q *GeoSearchQuery) append(r *request, store bool) {
	if q.Member != "" {
		r.t(tFROMMEMBER).s(q.Member)
	} else {
		r.t(tFROMLONLAT).f(q.Longitude).f(q.Latitude)
	}
	if q.Radius > 0 {
		r.t(tBYRADIUS).f(q.Radius).s(q.unit())
	} else {
		r.t(tBYBOX).f(q.Width).f(q.Height).s(q.unit())
	}
	if q.Sort != "" {
		r.s(q.Sort)
	}
	if q.Count > 0 {
		r.t(tCOUNT).i(q.Count)
		if q.Any {
			r.t(tANY)
		}
	}
	if !store {
		if q.WithDist {
			r.t(tWITHDIST)
		}
		if q.WithHash {
			r.t(tWITHHASH)
		}
		if q.WithCoord {
			r.t(tWITHCOORD)
		}
	}
}

func toGeoLocations(v Value, dist, hash, coord bool) ([]GeoLocation, error) {
	if !v.isAgg() {
		return nil, unexpected(v)
	}
	out := make([]GeoLocation, len(v.Array))
	for i, e := range v.Array {
		if !e.isAgg() {
			out[i].Name = e.Str()
			continue
		}
		j := 0
		if len(e.Array) == 0 {
			return nil, unexpected(v)
		}
		out[i].Name = e.Array[j].Str()
		j++
		if dist && j < len(e.Array) {
			out[i].Dist, _ = e.Array[j].AsFloat64()
			j++
		}
		if hash && j < len(e.Array) {
			out[i].GeoHash, _ = e.Array[j].AsInt64()
			j++
		}
		if coord && j < len(e.Array) && len(e.Array[j].Array) == 2 {
			out[i].Longitude, _ = e.Array[j].Array[0].AsFloat64()
			out[i].Latitude, _ = e.Array[j].Array[1].AsFloat64()
		}
	}
	return out, nil
}

// GeoSearch runs GEOSEARCH.
func (c *cmds) GeoSearch(ctx context.Context, key string, q GeoSearchQuery) ([]GeoLocation, error) {
	r := newCmd(2+q.n(false), cGEOSEARCH).s(key)
	q.append(r, false)
	v, err := c.value(ctx, r)
	if err != nil {
		return nil, err
	}
	return toGeoLocations(v, q.WithDist, q.WithHash, q.WithCoord)
}

// GeoSearchStore stores GEOSEARCH results into dst (STOREDIST when storeDist).
func (c *cmds) GeoSearchStore(ctx context.Context, dst, src string, q GeoSearchQuery, storeDist bool) (int64, error) {
	n := 3 + q.n(true)
	if storeDist {
		n++
	}
	r := newCmd(n, cGEOSEARCHSTORE).s(dst).s(src)
	q.append(r, true)
	if storeDist {
		r.t(tSTOREDIST)
	}
	return c.intR(ctx, r)
}

// GeoRadiusQuery is the GEORADIUS / GEORADIUSBYMEMBER query.
type GeoRadiusQuery struct {
	Radius    float64
	Unit      string
	Sort      string
	Count     int64
	Any       bool
	WithDist  bool
	WithHash  bool
	WithCoord bool
}

func (q *GeoRadiusQuery) build(r *request, store, storeDist string) {
	u := q.Unit
	if u == "" {
		u = "m"
	}
	r.f(q.Radius).s(u)
	if store == "" {
		if q.WithDist {
			r.t(tWITHDIST)
		}
		if q.WithHash {
			r.t(tWITHHASH)
		}
		if q.WithCoord {
			r.t(tWITHCOORD)
		}
	}
	if q.Count > 0 {
		r.t(tCOUNT).i(q.Count)
		if q.Any {
			r.t(tANY)
		}
	}
	if q.Sort != "" {
		r.s(q.Sort)
	}
	if store != "" {
		r.t(tSTORE).s(store)
	}
	if storeDist != "" {
		r.t(tSTOREDIST).s(storeDist)
	}
}

func (q *GeoRadiusQuery) n(store, storeDist string) int {
	n := 2
	if store == "" {
		for _, b := range [...]bool{q.WithDist, q.WithHash, q.WithCoord} {
			if b {
				n++
			}
		}
	} else {
		n += 2
	}
	if storeDist != "" {
		n += 2
	}
	if q.Count > 0 {
		n += 2
		if q.Any {
			n++
		}
	}
	if q.Sort != "" {
		n++
	}
	return n
}

// GeoRadius searches around a point.
func (c *cmds) GeoRadius(ctx context.Context, key string, lon, lat float64, q GeoRadiusQuery) ([]GeoLocation, error) {
	r := newCmd(4+q.n("", ""), cGEORADIUS).s(key).f(lon).f(lat)
	q.build(r, "", "")
	v, err := c.value(ctx, r)
	if err != nil {
		return nil, err
	}
	return toGeoLocations(v, q.WithDist, q.WithHash, q.WithCoord)
}

// GeoRadiusStore stores GEORADIUS results (store and/or storeDist destination).
func (c *cmds) GeoRadiusStore(ctx context.Context, key string, lon, lat float64, q GeoRadiusQuery, store, storeDist string) (int64, error) {
	r := newCmd(4+q.n(store, storeDist), cGEORADIUS).s(key).f(lon).f(lat)
	q.build(r, store, storeDist)
	return c.intR(ctx, r)
}

// GeoRadiusByMember searches around a member.
func (c *cmds) GeoRadiusByMember(ctx context.Context, key, member string, q GeoRadiusQuery) ([]GeoLocation, error) {
	r := newCmd(3+q.n("", ""), cGEORADIUSBYMEMBER).s(key).s(member)
	q.build(r, "", "")
	v, err := c.value(ctx, r)
	if err != nil {
		return nil, err
	}
	return toGeoLocations(v, q.WithDist, q.WithHash, q.WithCoord)
}

// GeoRadiusByMemberStore stores GEORADIUSBYMEMBER results.
func (c *cmds) GeoRadiusByMemberStore(ctx context.Context, key, member string, q GeoRadiusQuery, store, storeDist string) (int64, error) {
	r := newCmd(3+q.n(store, storeDist), cGEORADIUSBYMEMBER).s(key).s(member)
	q.build(r, store, storeDist)
	return c.intR(ctx, r)
}
