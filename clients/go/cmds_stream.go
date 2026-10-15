// Copyright (c) 2026 Salih Cantekin. MIT License; see LICENSE.

package ruvio

import (
	"context"
	"time"
)

// XMessage is one stream entry; Values is field, value, field, value...
type XMessage struct {
	ID     string
	Values []string
}

// Map returns the fields as a map.
func (m XMessage) Map() map[string]string {
	out := make(map[string]string, len(m.Values)/2)
	for i := 0; i+1 < len(m.Values); i += 2 {
		out[m.Values[i]] = m.Values[i+1]
	}
	return out
}

// XStream is one stream of an XREAD reply.
type XStream struct {
	Stream   string
	Messages []XMessage
}

func toXMessages(v Value) ([]XMessage, error) {
	if v.Kind == KindNil {
		return nil, nil
	}
	if !v.isAgg() {
		return nil, unexpected(v)
	}
	// Tolerate a single unwrapped entry [id, [fields]]: Ruvio 0.3.0 answers a blocked
	// XREAD that wakes up in this shape.
	if len(v.Array) == 2 && v.Array[0].Kind == KindBulk && v.Array[1].isAgg() {
		vals, err := v.Array[1].Strings()
		return []XMessage{{ID: v.Array[0].Str(), Values: vals}}, err
	}
	out := make([]XMessage, 0, len(v.Array))
	for _, e := range v.Array {
		if e.Kind == KindNil || len(e.Array) < 2 {
			if len(e.Array) == 1 {
				out = append(out, XMessage{ID: e.Array[0].Str()})
			} else if e.Kind == KindBulk {
				out = append(out, XMessage{ID: e.Str()})
			}
			continue
		}
		vals, err := e.Array[1].Strings()
		if err != nil {
			return nil, err
		}
		out = append(out, XMessage{ID: e.Array[0].Str(), Values: vals})
	}
	return out, nil
}

func toXStreams(v Value) ([]XStream, error) {
	if v.Kind == KindNil {
		return nil, nil
	}
	if !v.isAgg() {
		return nil, unexpected(v)
	}
	var out []XStream
	if v.Kind == KindMap { // RESP3
		for i := 0; i+1 < len(v.Array); i += 2 {
			msgs, err := toXMessages(v.Array[i+1])
			if err != nil {
				return nil, err
			}
			out = append(out, XStream{v.Array[i].Str(), msgs})
		}
		return out, nil
	}
	for _, s := range v.Array {
		if len(s.Array) != 2 {
			return nil, unexpected(v)
		}
		msgs, err := toXMessages(s.Array[1])
		if err != nil {
			return nil, err
		}
		out = append(out, XStream{s.Array[0].Str(), msgs})
	}
	return out, nil
}

// XAddArgs is XADD. ID defaults to "*". MaxLen/MinID trim (Approx sends "~", which
// Ruvio trims exactly). Delay holds the entry from XREADGROUP (Ruvio).
type XAddArgs struct {
	Stream     string
	ID         string
	NoMkStream bool
	MaxLen     int64
	MinID      string
	Approx     bool
	Delay      time.Duration
	Values     []string
}

// XAdd appends an entry and returns its id; ok is false with NOMKSTREAM on a missing key.
func (c *cmds) XAdd(ctx context.Context, a XAddArgs) (string, bool, error) {
	n := 3 + len(a.Values)
	if a.Delay > 0 {
		n += 2
	}
	if a.NoMkStream {
		n++
	}
	if a.MaxLen > 0 || a.MinID != "" {
		n += 2
		if a.Approx {
			n++
		}
	}
	r := newCmd(n, cXADD).s(a.Stream)
	if a.Delay > 0 {
		r.t(tDELAY).i(ms(a.Delay))
	}
	if a.NoMkStream {
		r.t(tNOMKSTREAM)
	}
	if a.MaxLen > 0 || a.MinID != "" {
		if a.MaxLen > 0 {
			r.t(tMAXLEN)
		} else {
			r.t(tMINID)
		}
		if a.Approx {
			r.t(tTILDE)
		}
		if a.MaxLen > 0 {
			r.i(a.MaxLen)
		} else {
			r.s(a.MinID)
		}
	}
	id := a.ID
	if id == "" {
		id = "*"
	}
	r.s(id).ss(a.Values)
	return c.strNil(ctx, r)
}

// XLen returns the entry count.
func (c *cmds) XLen(ctx context.Context, stream string) (int64, error) {
	return c.intR(ctx, newCmd(2, cXLEN).s(stream))
}

func xrangeReq(name []byte, stream, a, b string, count int64) *request {
	if count > 0 {
		return newCmd(6, name).s(stream).s(a).s(b).t(tCOUNT).i(count)
	}
	return newCmd(4, name).s(stream).s(a).s(b)
}

// XRange returns entries in [start, end] (count 0 = all).
func (c *cmds) XRange(ctx context.Context, stream, start, end string, count int64) ([]XMessage, error) {
	v, err := c.value(ctx, xrangeReq(cXRANGE, stream, start, end, count))
	if err != nil {
		return nil, err
	}
	return toXMessages(v)
}

// XRevRange returns entries from end down to start.
func (c *cmds) XRevRange(ctx context.Context, stream, end, start string, count int64) ([]XMessage, error) {
	v, err := c.value(ctx, xrangeReq(cXREVRANGE, stream, end, start, count))
	if err != nil {
		return nil, err
	}
	return toXMessages(v)
}

// BlockForever as a Block value sends BLOCK 0 (wait until an entry arrives).
const BlockForever time.Duration = -1

func blockMs(d time.Duration) int64 {
	if d < 0 {
		return 0
	}
	return ms(d)
}

// XReadArgs is XREAD. Streams and IDs pair up. Block 0 does not block, Block > 0
// waits that long, BlockForever waits until an entry arrives; blocking reads run on
// a dedicated connection.
type XReadArgs struct {
	Streams []string
	IDs     []string
	Count   int64
	Block   time.Duration
}

// XRead reads entries after the given ids; nil when nothing arrived.
func (c *cmds) XRead(ctx context.Context, a XReadArgs) ([]XStream, error) {
	n := 2 + len(a.Streams) + len(a.IDs)
	if a.Count > 0 {
		n += 2
	}
	if a.Block != 0 {
		n += 2
	}
	r := newCmd(n, cXREAD)
	if a.Count > 0 {
		r.t(tCOUNT).i(a.Count)
	}
	if a.Block != 0 {
		r.t(tBLOCK).i(blockMs(a.Block))
	}
	r.t(tSTREAMS).ss(a.Streams).ss(a.IDs)
	var v Value
	var err error
	if a.Block != 0 {
		v, err = c.ex.execBlocking(ctx, r)
	} else {
		v, err = c.value(ctx, r)
	}
	if err != nil {
		return nil, err
	}
	return toXStreams(v)
}

// XDel deletes ids.
func (c *cmds) XDel(ctx context.Context, stream string, ids ...string) (int64, error) {
	return c.intR(ctx, newCmd(2+len(ids), cXDEL).s(stream).ss(ids))
}

// XSetID sets the last generated id.
func (c *cmds) XSetID(ctx context.Context, stream, id string) error {
	return c.ok(ctx, newCmd(3, cXSETID).s(stream).s(id))
}

// XTrim trims by "MAXLEN" or "MINID"; approx sends "~" (Ruvio trims exactly).
func (c *cmds) XTrim(ctx context.Context, stream, strategy, threshold string, approx bool) (int64, error) {
	if approx {
		return c.intR(ctx, newCmd(5, cXTRIM).s(stream).s(strategy).t(tTILDE).s(threshold))
	}
	return c.intR(ctx, newCmd(4, cXTRIM).s(stream).s(strategy).s(threshold))
}

// XGroupCreate creates a group at id ($, 0 or explicit).
func (c *cmds) XGroupCreate(ctx context.Context, stream, group, id string, mkStream bool) error {
	if mkStream {
		return c.ok(ctx, newCmd(6, cXGROUP).t(tCREATE).s(stream).s(group).s(id).t(tMKSTREAM))
	}
	return c.ok(ctx, newCmd(5, cXGROUP).t(tCREATE).s(stream).s(group).s(id))
}

// XGroupCreateConsumer registers a consumer.
func (c *cmds) XGroupCreateConsumer(ctx context.Context, stream, group, consumer string) (bool, error) {
	return c.boolR(ctx, newCmd(5, cXGROUP).t(tCREATECONSUMER).s(stream).s(group).s(consumer))
}

// XGroupDestroy removes a group.
func (c *cmds) XGroupDestroy(ctx context.Context, stream, group string) (bool, error) {
	return c.boolR(ctx, newCmd(4, cXGROUP).t(tDESTROY).s(stream).s(group))
}

// XGroupSetID moves the group cursor.
func (c *cmds) XGroupSetID(ctx context.Context, stream, group, id string) error {
	return c.ok(ctx, newCmd(5, cXGROUP).t(tSETID).s(stream).s(group).s(id))
}

// XGroupDelConsumer removes a consumer and returns its pending count.
func (c *cmds) XGroupDelConsumer(ctx context.Context, stream, group, consumer string) (int64, error) {
	return c.intR(ctx, newCmd(5, cXGROUP).t(tDELCONSUMER).s(stream).s(group).s(consumer))
}

// XReadGroupArgs is XREADGROUP for one stream with the ">" id. Block works as in XReadArgs.
type XReadGroupArgs struct {
	Group    string
	Consumer string
	Stream   string
	Count    int64
	Block    time.Duration
	NoAck    bool
}

// XReadGroup delivers new entries to a consumer.
func (c *cmds) XReadGroup(ctx context.Context, a XReadGroupArgs) ([]XMessage, error) {
	n := 7
	if a.Count > 0 {
		n += 2
	}
	if a.Block != 0 {
		n += 2
	}
	if a.NoAck {
		n++
	}
	r := newCmd(n, cXREADGROUP).t(tGROUP).s(a.Group).s(a.Consumer)
	if a.Count > 0 {
		r.t(tCOUNT).i(a.Count)
	}
	if a.Block != 0 {
		r.t(tBLOCK).i(blockMs(a.Block))
	}
	if a.NoAck {
		r.t(tNOACK)
	}
	r.t(tSTREAMS).s(a.Stream).s(">")
	var v Value
	var err error
	if a.Block != 0 {
		v, err = c.ex.execBlocking(ctx, r)
	} else {
		v, err = c.value(ctx, r)
	}
	if err != nil {
		return nil, err
	}
	ss, err := toXStreams(v)
	if err != nil || len(ss) == 0 {
		return nil, err
	}
	return ss[0].Messages, nil
}

// XAck acknowledges ids.
func (c *cmds) XAck(ctx context.Context, stream, group string, ids ...string) (int64, error) {
	return c.intR(ctx, newCmd(3+len(ids), cXACK).s(stream).s(group).ss(ids))
}

// XPendingSummary is the short XPENDING form.
type XPendingSummary struct {
	Count     int64
	Lowest    string
	Highest   string
	Consumers map[string]int64
}

// XPending returns the pending summary of a group.
func (c *cmds) XPending(ctx context.Context, stream, group string) (XPendingSummary, error) {
	v, err := c.value(ctx, newCmd(3, cXPENDING).s(stream).s(group))
	if err != nil {
		return XPendingSummary{}, err
	}
	if len(v.Array) != 4 {
		return XPendingSummary{}, unexpected(v)
	}
	s := XPendingSummary{Count: v.Array[0].Int, Lowest: v.Array[1].Str(), Highest: v.Array[2].Str(),
		Consumers: map[string]int64{}}
	for _, e := range v.Array[3].Array {
		if len(e.Array) == 2 {
			n, _ := e.Array[1].AsInt64()
			s.Consumers[e.Array[0].Str()] = n
		}
	}
	return s, nil
}

// XPendingExtArgs is the extended XPENDING form.
type XPendingExtArgs struct {
	Stream   string
	Group    string
	Idle     time.Duration
	Start    string
	End      string
	Count    int64
	Consumer string
}

// XPendingEntry is one extended XPENDING row.
type XPendingEntry struct {
	ID         string
	Consumer   string
	Idle       time.Duration
	Deliveries int64
}

// XPendingExt returns pending entries.
func (c *cmds) XPendingExt(ctx context.Context, a XPendingExtArgs) ([]XPendingEntry, error) {
	n := 6
	if a.Idle > 0 {
		n += 2
	}
	if a.Consumer != "" {
		n++
	}
	r := newCmd(n, cXPENDING).s(a.Stream).s(a.Group)
	if a.Idle > 0 {
		r.t(tIDLE).i(ms(a.Idle))
	}
	r.s(a.Start).s(a.End).i(a.Count)
	if a.Consumer != "" {
		r.s(a.Consumer)
	}
	v, err := c.value(ctx, r)
	if err != nil {
		return nil, err
	}
	out := make([]XPendingEntry, 0, len(v.Array))
	for _, e := range v.Array {
		if len(e.Array) != 4 {
			return nil, unexpected(v)
		}
		out = append(out, XPendingEntry{e.Array[0].Str(), e.Array[1].Str(),
			time.Duration(e.Array[2].Int) * time.Millisecond, e.Array[3].Int})
	}
	return out, nil
}

// XAutoClaimArgs is XAUTOCLAIM.
type XAutoClaimArgs struct {
	Stream   string
	Group    string
	Consumer string
	MinIdle  time.Duration
	Start    string
	Count    int64
}

func xautoclaimReq(a *XAutoClaimArgs, justID bool) *request {
	n := 6
	if a.Count > 0 {
		n += 2
	}
	if justID {
		n++
	}
	r := newCmd(n, cXAUTOCLAIM).s(a.Stream).s(a.Group).s(a.Consumer).i(ms(a.MinIdle)).s(a.Start)
	if a.Count > 0 {
		r.t(tCOUNT).i(a.Count)
	}
	if justID {
		r.t(tJUSTID)
	}
	return r
}

// XAutoClaim claims idle pending entries; returns the next start id, the entries and
// ids dropped because they were deleted.
func (c *cmds) XAutoClaim(ctx context.Context, a XAutoClaimArgs) (string, []XMessage, []string, error) {
	v, err := c.value(ctx, xautoclaimReq(&a, false))
	if err != nil {
		return "", nil, nil, err
	}
	if len(v.Array) < 2 {
		return "", nil, nil, unexpected(v)
	}
	msgs, err := toXMessages(v.Array[1])
	var deleted []string
	if len(v.Array) > 2 {
		deleted, _ = v.Array[2].Strings()
	}
	return v.Array[0].Str(), msgs, deleted, err
}

// XAutoClaimJustID is XAUTOCLAIM ... JUSTID.
func (c *cmds) XAutoClaimJustID(ctx context.Context, a XAutoClaimArgs) (string, []string, error) {
	v, err := c.value(ctx, xautoclaimReq(&a, true))
	if err != nil {
		return "", nil, err
	}
	if len(v.Array) < 2 {
		return "", nil, unexpected(v)
	}
	ids, err := v.Array[1].Strings()
	return v.Array[0].Str(), ids, err
}

// XClaimArgs is XCLAIM. Idle and Time cannot be combined.
type XClaimArgs struct {
	Stream     string
	Group      string
	Consumer   string
	MinIdle    time.Duration
	IDs        []string
	Idle       time.Duration
	Time       time.Time
	RetryCount int64
	Force      bool
	LastID     string
}

func xclaimReq(a *XClaimArgs, justID bool) *request {
	n := 5 + len(a.IDs)
	if a.Idle > 0 {
		n += 2
	}
	if !a.Time.IsZero() {
		n += 2
	}
	if a.RetryCount > 0 {
		n += 2
	}
	if a.Force {
		n++
	}
	if justID {
		n++
	}
	if a.LastID != "" {
		n += 2
	}
	r := newCmd(n, cXCLAIM).s(a.Stream).s(a.Group).s(a.Consumer).i(ms(a.MinIdle)).ss(a.IDs)
	if a.Idle > 0 {
		r.t(tIDLE).i(ms(a.Idle))
	}
	if !a.Time.IsZero() {
		r.t(tTIME).i(a.Time.UnixMilli())
	}
	if a.RetryCount > 0 {
		r.t(tRETRYCOUNT).i(a.RetryCount)
	}
	if a.Force {
		r.t(tFORCE)
	}
	if justID {
		r.t(tJUSTID)
	}
	if a.LastID != "" {
		r.t(tLASTID).s(a.LastID)
	}
	return r
}

// XClaim claims pending entries.
func (c *cmds) XClaim(ctx context.Context, a XClaimArgs) ([]XMessage, error) {
	v, err := c.value(ctx, xclaimReq(&a, false))
	if err != nil {
		return nil, err
	}
	return toXMessages(v)
}

// XClaimJustID is XCLAIM ... JUSTID.
func (c *cmds) XClaimJustID(ctx context.Context, a XClaimArgs) ([]string, error) {
	return c.strs(ctx, xclaimReq(&a, true))
}

// XInfoStream returns XINFO STREAM as a field map.
func (c *cmds) XInfoStream(ctx context.Context, stream string) (map[string]Value, error) {
	v, err := c.value(ctx, newCmd(3, cXINFO).t(tSTREAM).s(stream))
	if err != nil {
		return nil, err
	}
	return toValueMap(v)
}

func toMapList(v Value) ([]map[string]string, error) {
	if !v.isAgg() {
		return nil, unexpected(v)
	}
	out := make([]map[string]string, 0, len(v.Array))
	for _, e := range v.Array {
		m, err := toStrMap(e)
		if err != nil {
			return nil, err
		}
		out = append(out, m)
	}
	return out, nil
}

// XInfoGroups returns one field map per group.
func (c *cmds) XInfoGroups(ctx context.Context, stream string) ([]map[string]string, error) {
	v, err := c.value(ctx, newCmd(3, cXINFO).t(tGROUPS).s(stream))
	if err != nil {
		return nil, err
	}
	return toMapList(v)
}

// XInfoConsumers returns one field map per consumer.
func (c *cmds) XInfoConsumers(ctx context.Context, stream, group string) ([]map[string]string, error) {
	v, err := c.value(ctx, newCmd(4, cXINFO).t(tCONSUMERS).s(stream).s(group))
	if err != nil {
		return nil, err
	}
	return toMapList(v)
}
