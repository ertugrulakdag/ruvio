// Copyright (c) 2026 Salih Cantekin. MIT License; see LICENSE.

package ruvio

import (
	"testing"
	"time"
)

func TestLiveStreams(t *testing.T) {
	c, ctx := live(t)
	mi := must[int64](t)

	id1, ok, err := c.XAdd(ctx, XAddArgs{Stream: "s", ID: "1-1", Values: []string{"a", "1"}})
	noErr(t, err)
	eq(t, []any{id1, ok}, []any{"1-1", true})
	c.XAdd(ctx, XAddArgs{Stream: "s", ID: "2-*", Values: []string{"b", "2"}})
	id3, _, _ := c.XAdd(ctx, XAddArgs{Stream: "s", Values: []string{"c", "3"}})
	if id3 == "" {
		t.Fatal("auto id")
	}
	_, ok, err = c.XAdd(ctx, XAddArgs{Stream: "missing", NoMkStream: true, Values: []string{"x", "1"}})
	noErr(t, err)
	eq(t, ok, false)
	c.XAdd(ctx, XAddArgs{Stream: "s", MaxLen: 3, Approx: true, Values: []string{"d", "4"}})
	eq(t, mi(c.XLen(ctx, "s")), int64(3))
	c.XAdd(ctx, XAddArgs{Stream: "m", ID: "5-0", Values: []string{"x", "1"}})
	c.XAdd(ctx, XAddArgs{Stream: "m", ID: "6-0", MinID: "6-0", Values: []string{"x", "2"}})
	eq(t, mi(c.XLen(ctx, "m")), int64(1))

	all := must[[]XMessage](t)(c.XRange(ctx, "s", "-", "+", 0))
	eq(t, len(all), 3)
	eq(t, all[0].ID, "2-0")
	eq(t, all[0].Map(), map[string]string{"b": "2"})
	eq(t, len(must[[]XMessage](t)(c.XRange(ctx, "s", "-", "+", 1))), 1)
	rev := must[[]XMessage](t)(c.XRevRange(ctx, "s", "+", "-", 1))
	eq(t, rev[0].Values, []string{"d", "4"})

	rd := must[[]XStream](t)(c.XRead(ctx, XReadArgs{Streams: []string{"s"}, IDs: []string{"2-0"}, Count: 1}))
	eq(t, len(rd), 1)
	eq(t, rd[0].Stream, "s")
	eq(t, rd[0].Messages[0].Values, []string{"c", "3"})
	go func() {
		time.Sleep(30 * time.Millisecond)
		c.XAdd(ctx, XAddArgs{Stream: "s", Values: []string{"live", "1"}})
	}()
	rd = must[[]XStream](t)(c.XRead(ctx, XReadArgs{Streams: []string{"s"}, IDs: []string{"$"}, Block: 2 * time.Second}))
	eq(t, rd[0].Messages[0].Values, []string{"live", "1"})
	rd = must[[]XStream](t)(c.XRead(ctx, XReadArgs{Streams: []string{"s"}, IDs: []string{"$"}, Block: 30 * time.Millisecond}))
	eq(t, len(rd), 0)

	eq(t, mi(c.XDel(ctx, "s", all[0].ID, all[0].ID)), int64(1))
	eq(t, mi(c.XTrim(ctx, "s", "MAXLEN", "2", false)), int64(1))
	eq(t, mi(c.XTrim(ctx, "s", "MAXLEN", "1", true)), int64(1))
	noErr(t, c.XSetID(ctx, "s", "99999999999999-0"))

	// consumer groups
	noErr(t, c.XGroupCreate(ctx, "q", "g", "$", true))
	eq(t, must[bool](t)(c.XGroupCreateConsumer(ctx, "q", "g", "alice")), true)
	eq(t, must[bool](t)(c.XGroupCreateConsumer(ctx, "q", "g", "alice")), false)
	for i := 0; i < 3; i++ {
		c.XAdd(ctx, XAddArgs{Stream: "q", Values: []string{"n", string(rune('0' + i))}})
	}
	msgs := must[[]XMessage](t)(c.XReadGroup(ctx, XReadGroupArgs{Group: "g", Consumer: "alice", Stream: "q", Count: 2}))
	eq(t, len(msgs), 2)
	sum, err := c.XPending(ctx, "q", "g")
	noErr(t, err)
	eq(t, []any{sum.Count, sum.Consumers["alice"]}, []any{int64(2), int64(2)})
	pe := must[[]XPendingEntry](t)(c.XPendingExt(ctx, XPendingExtArgs{Stream: "q", Group: "g", Start: "-", End: "+", Count: 10, Consumer: "alice"}))
	eq(t, len(pe), 2)
	eq(t, pe[0].Deliveries, int64(1))
	eq(t, mi(c.XAck(ctx, "q", "g", msgs[0].ID)), int64(1))

	time.Sleep(5 * time.Millisecond)
	next, claimed, deleted, err := c.XAutoClaim(ctx, XAutoClaimArgs{Stream: "q", Group: "g", Consumer: "bob", MinIdle: time.Millisecond, Start: "0-0", Count: 10})
	noErr(t, err)
	eq(t, []any{next, len(claimed), len(deleted)}, []any{"0-0", 1, 0})
	_, ids, err := c.XAutoClaimJustID(ctx, XAutoClaimArgs{Stream: "q", Group: "g", Consumer: "alice", Start: "0-0"})
	noErr(t, err)
	eq(t, ids, []string{msgs[1].ID})
	cl := must[[]XMessage](t)(c.XClaim(ctx, XClaimArgs{Stream: "q", Group: "g", Consumer: "bob", IDs: []string{msgs[1].ID}, RetryCount: 5}))
	eq(t, len(cl), 1)
	cids := must[[]string](t)(c.XClaimJustID(ctx, XClaimArgs{Stream: "q", Group: "g", Consumer: "carol", IDs: []string{msgs[1].ID}, Idle: time.Second, Force: true, LastID: msgs[1].ID}))
	eq(t, cids, []string{msgs[1].ID})

	// NOACK and blocking read on a dedicated connection
	rest := must[[]XMessage](t)(c.XReadGroup(ctx, XReadGroupArgs{Group: "g", Consumer: "dave", Stream: "q", Count: 10}))
	eq(t, len(rest), 1)
	go func() {
		time.Sleep(30 * time.Millisecond)
		c.XAdd(ctx, XAddArgs{Stream: "q", Values: []string{"late", "1"}})
	}()
	got := must[[]XMessage](t)(c.XReadGroup(ctx, XReadGroupArgs{Group: "g", Consumer: "dave", Stream: "q", Count: 10, Block: 2 * time.Second, NoAck: true}))
	if len(got) != 1 || got[0].Map()["late"] != "1" {
		t.Fatalf("blocking XREADGROUP returned %+v", got)
	}

	info, err := c.XInfoStream(ctx, "q")
	noErr(t, err)
	if n, _ := info["length"].AsInt64(); n != 4 {
		t.Fatalf("xinfo stream length %v", info["length"])
	}
	groups := must[[]map[string]string](t)(c.XInfoGroups(ctx, "q"))
	eq(t, groups[0]["name"], "g")
	cons := must[[]map[string]string](t)(c.XInfoConsumers(ctx, "q", "g"))
	if len(cons) == 0 {
		t.Fatal("xinfo consumers")
	}
	noErr(t, c.XGroupSetID(ctx, "q", "g", "0"))
	eq(t, mi(c.XGroupDelConsumer(ctx, "q", "g", "carol")), int64(1))
	eq(t, must[bool](t)(c.XGroupDestroy(ctx, "q", "g")), true)
	eq(t, must[bool](t)(c.XGroupDestroy(ctx, "q", "g")), false)

	// DELAY hides the entry from XREADGROUP until it elapses
	noErr(t, c.XGroupCreate(ctx, "dq", "g", "$", true))
	_, ok, err = c.XAdd(ctx, XAddArgs{Stream: "dq", Delay: 150 * time.Millisecond, Values: []string{"later", "1"}})
	noErr(t, err)
	eq(t, ok, true)
	early := must[[]XMessage](t)(c.XReadGroup(ctx, XReadGroupArgs{Group: "g", Consumer: "a", Stream: "dq"}))
	eq(t, len(early), 0)
	time.Sleep(200 * time.Millisecond)
	late := must[[]XMessage](t)(c.XReadGroup(ctx, XReadGroupArgs{Group: "g", Consumer: "a", Stream: "dq"}))
	eq(t, len(late), 1)
}
