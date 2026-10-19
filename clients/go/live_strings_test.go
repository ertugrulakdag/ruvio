// Copyright (c) 2026 Salih Cantekin. MIT License; see LICENSE.

package ruvio

import (
	"errors"
	"strconv"
	"testing"
	"time"
)

func TestLiveStrings(t *testing.T) {
	c, ctx := live(t)
	mi := must[int64](t)
	mb := must[bool](t)

	noErr(t, c.Set(ctx, "a", "1"))
	v, ok, err := c.Get(ctx, "a")
	noErr(t, err)
	eq(t, []any{v, ok}, []any{"1", true})
	_, ok, _ = c.Get(ctx, "missing")
	eq(t, ok, false)
	noErr(t, c.SetBytes(ctx, "bin", []byte{0, 0xff}))

	eq(t, mb(c.SetArgs(ctx, "a", "2", SetOptions{NX: true})), false)
	eq(t, mb(c.SetArgs(ctx, "a", "2", SetOptions{XX: true, TTL: time.Minute})), true)
	if ttl := mi(c.PTTL(ctx, "a")); ttl <= 0 || ttl > 60000 {
		t.Fatalf("pttl %d", ttl)
	}
	eq(t, mb(c.SetArgs(ctx, "a", "3", SetOptions{KeepTTL: true})), true)
	if mi(c.TTL(ctx, "a")) <= 0 {
		t.Fatal("KEEPTTL lost ttl")
	}
	eq(t, mb(c.SetArgs(ctx, "at", "x", SetOptions{ExpireAt: time.Now().Add(time.Hour)})), true)
	old, ok, err := c.SetGet(ctx, "a", "4", SetOptions{})
	noErr(t, err)
	eq(t, []any{old, ok}, []any{"3", true})

	// SET ... IFEQ
	eq(t, mb(c.SetIfEq(ctx, "a", "5", "nope", SetOptions{})), false)
	eq(t, mb(c.SetIfEq(ctx, "a", "5", "4", SetOptions{TTL: time.Minute})), true)
	v, _, _ = c.Get(ctx, "a")
	eq(t, v, "5")

	// SET ... IFGT / IFLT
	eq(t, mb(c.SetIfGreater(ctx, "wm", 10, SetOptions{})), true)
	eq(t, mb(c.SetIfGreater(ctx, "wm", 10, SetOptions{})), false)
	eq(t, mb(c.SetIfGreater(ctx, "wm", 11, SetOptions{TTL: time.Minute})), true)
	if mi(c.TTL(ctx, "wm")) <= 0 {
		t.Fatal("IFGT lost ttl")
	}
	eq(t, mb(c.SetIfLess(ctx, "wm", 12, SetOptions{})), false)
	eq(t, mb(c.SetIfLess(ctx, "wm", -1, SetOptions{})), true)
	v, _, _ = c.Get(ctx, "wm")
	eq(t, v, "-1")

	noErr(t, c.SetEX(ctx, "ex", "v", 100*time.Second))
	if n := mi(c.TTL(ctx, "ex")); n < 99 || n > 100 {
		t.Fatalf("setex ttl %d", n)
	}
	noErr(t, c.PSetEX(ctx, "pex", "v", 5*time.Second))
	eq(t, mb(c.SetNX(ctx, "pex", "w")), false)
	eq(t, mb(c.SetNX(ctx, "nx", "w")), true)

	noErr(t, c.MSet(ctx, "m1", "a", "m2", "b"))
	mg := must[[]NullString](t)(c.MGet(ctx, "m1", "nope", "m2"))
	eq(t, mg, []NullString{{"a", true}, {}, {"b", true}})
	eq(t, mb(c.MSetNX(ctx, "m1", "x", "m3", "y")), false)
	eq(t, mb(c.MSetNX(ctx, "m3", "x", "m4", "y")), true)

	old, ok, _ = c.GetSet(ctx, "m1", "z")
	eq(t, []any{old, ok}, []any{"a", true})
	old, ok, _ = c.GetDel(ctx, "m1")
	eq(t, []any{old, ok}, []any{"z", true})
	eq(t, mi(c.Exists(ctx, "m1")), int64(0))

	noErr(t, c.Set(ctx, "gx", "val"))
	v, ok, _ = c.GetEx(ctx, "gx", 30*time.Second)
	eq(t, []any{v, ok}, []any{"val", true})
	if p := mi(c.PTTL(ctx, "gx")); p <= 0 || p > 30000 {
		t.Fatalf("getex pttl %d", p)
	}

	eq(t, mi(c.Append(ctx, "ap", "ab")), int64(2))
	eq(t, mi(c.Append(ctx, "ap", "cd")), int64(4))
	eq(t, mi(c.StrLen(ctx, "ap")), int64(4))
	eq(t, must[string](t)(c.GetRange(ctx, "ap", 1, 2)), "bc")
	eq(t, must[string](t)(c.Substr(ctx, "ap", 0, 0)), "a")
	eq(t, mi(c.SetRange(ctx, "ap", 1, "XY")), int64(4))
	v, _, _ = c.Get(ctx, "ap")
	eq(t, v, "aXYd")

	eq(t, mi(c.Incr(ctx, "n")), int64(1))
	eq(t, mi(c.IncrBy(ctx, "n", 9)), int64(10))
	eq(t, mi(c.Decr(ctx, "n")), int64(9))
	eq(t, mi(c.DecrBy(ctx, "n", 4)), int64(5))
	f := must[float64](t)(c.IncrByFloat(ctx, "n", 0.5))
	eq(t, f, 5.5)
	noErr(t, c.Set(ctx, "n2", "5"))
	n, ok, err := c.IncrByMax(ctx, "n2", 5, 10)
	noErr(t, err)
	eq(t, []any{n, ok}, []any{int64(10), true})
	_, ok, _ = c.IncrByMax(ctx, "n2", 1, 10)
	eq(t, ok, false)
}

func TestLiveRuvioExtensions(t *testing.T) {
	c, ctx := live(t)
	// FILLGET markers are memory-only and survive FLUSHDB: use run-unique keys.
	run := strconv.FormatInt(time.Now().UnixNano(), 36)
	mi := must[int64](t)
	mb := must[bool](t)

	eq(t, must[string](t)(c.Ruvio(ctx)), "PONG is taken")
	if s := must[string](t)(c.RuvioOwner(ctx)); s == "" {
		t.Fatal("empty RUVIO OWNER")
	}

	eq(t, mi(c.SetV(ctx, "v", "a", 0)), int64(1))
	eq(t, mi(c.SetV(ctx, "v", "b", 1)), int64(2))
	if _, err := c.SetV(ctx, "v", "c", 1); err == nil {
		t.Fatal("version mismatch must fail")
	}

	noErr(t, c.Set(ctx, "stock", "10"))
	rem, ok, err := c.Take(ctx, "stock", 3)
	noErr(t, err)
	eq(t, []any{rem, ok}, []any{int64(7), true})
	_, ok, _ = c.Take(ctx, "stock", 30)
	eq(t, ok, false)

	noErr(t, c.Set(ctx, "d", "x"))
	eq(t, mb(c.DelIfEq(ctx, "d", "y")), false)
	eq(t, mb(c.DelIfEq(ctx, "d", "x")), true)

	r1, a1, _ := c.Limit(ctx, "lim", 2, time.Minute)
	r2, a2, _ := c.Limit(ctx, "lim", 2, time.Minute)
	_, a3, err := c.Limit(ctx, "lim", 2, time.Minute)
	noErr(t, err)
	eq(t, []any{r1, a1, r2, a2, a3}, []any{int64(1), true, int64(0), true, false})

	eq(t, mb(c.Once(ctx, "once", time.Minute)), true)
	eq(t, mb(c.Once(ctx, "once", time.Minute)), false)

	tok, ok, err := c.Lease(ctx, "lease", time.Minute)
	noErr(t, err)
	eq(t, ok, true)
	_, ok, _ = c.Lease(ctx, "lease", time.Minute)
	eq(t, ok, false)
	eq(t, mb(c.Release(ctx, "lease", tok+100)), false)
	eq(t, mb(c.Release(ctx, "lease", tok)), true)
	tok2, ok, _ := c.Lease(ctx, "lease", time.Minute)
	eq(t, []any{ok, tok2 > tok}, []any{true, true})

	s1, ok1, _ := c.Semaphore(ctx, "sem", 2, time.Minute)
	_, ok2, _ := c.Semaphore(ctx, "sem", 2, time.Minute)
	_, ok3, err := c.Semaphore(ctx, "sem", 2, time.Minute)
	noErr(t, err)
	eq(t, []any{ok1, ok2, ok3}, []any{true, true, false})
	eq(t, mb(c.Release(ctx, "sem", s1)), true)
	_, ok3, _ = c.Semaphore(ctx, "sem", 2, time.Minute)
	eq(t, ok3, true)

	r, err := c.IdemBegin(ctx, "idem", "fp", "o1", time.Minute)
	noErr(t, err)
	eq(t, r.State, IdemAcquired)
	r, _ = c.IdemBegin(ctx, "idem", "fp", "o2", time.Minute)
	eq(t, r.State, IdemPending)
	eq(t, mi(c.IdemComplete(ctx, "idem", "fp", "o1", []byte("result"))), int64(1))
	r, _ = c.IdemBegin(ctx, "idem", "fp", "o2", time.Minute)
	eq(t, r.State, IdemCompleted)
	eq(t, string(r.Result), "result")
	r, _ = c.IdemBegin(ctx, "idem", "other", "o2", time.Minute)
	eq(t, r.State, IdemConflict)

	cur := mi(c.ChangesStart(ctx))
	noErr(t, c.Set(ctx, "changed", "1"))
	ch := must[[]Change](t)(c.Changes(ctx, cur))
	found := false
	for _, x := range ch {
		if x.Note == "SET changed" {
			found = true
		}
	}
	if !found {
		t.Fatalf("change feed: %+v", ch)
	}

	fr, err := c.FillGet(ctx, "fill:"+run, 5*time.Second)
	noErr(t, err)
	eq(t, fr.State, FillFill)
	busy, _ := c.FillGet(ctx, "fill:"+run, 5*time.Second)
	eq(t, busy.State, FillBusy)
	eq(t, mb(c.FillSet(ctx, "fill:"+run, "value", time.Minute, fr.Token, 0)), true)
	hit, _ := c.FillGet(ctx, "fill:"+run, time.Second)
	eq(t, []any{hit.State, hit.Value}, []any{FillHit, "value"})

	// SWR: a short fresh period, then STALE with a refresh token
	fr, _ = c.FillGet(ctx, "swr:"+run, time.Second)
	eq(t, mb(c.FillSet(ctx, "swr:"+run, "old", 50*time.Millisecond, fr.Token, time.Minute)), true)
	time.Sleep(120 * time.Millisecond)
	st, err := c.FillGet(ctx, "swr:"+run, time.Second)
	noErr(t, err)
	eq(t, []any{st.State, st.Value, st.Token != ""}, []any{FillStale, "old", true})

	// FILLGET ... WAIT parks on a dedicated connection until the filler writes
	fr, _ = c.FillGet(ctx, "wait:"+run, 5*time.Second)
	go func() {
		time.Sleep(30 * time.Millisecond)
		c.FillSet(ctx, "wait:"+run, "filled", time.Minute, fr.Token, 0)
	}()
	w, err := c.FillGetWait(ctx, "wait:"+run, 5*time.Second, 2*time.Second)
	noErr(t, err)
	eq(t, []any{w.State, w.Value}, []any{FillHit, "filled"})
}

func TestLiveKeys(t *testing.T) {
	c, ctx := live(t)
	mi := must[int64](t)
	mb := must[bool](t)
	ms := must[string](t)

	noErr(t, c.MSet(ctx, "k1", "a", "k2", "b", "k3", "c"))
	eq(t, mi(c.Exists(ctx, "k1", "k2", "nope")), int64(2))
	eq(t, mi(c.Touch(ctx, "k1", "nope")), int64(1))
	eq(t, ms(c.Type(ctx, "k1")), "string")
	eq(t, mi(c.DBSize(ctx)), int64(3))
	keys := must[[]string](t)(c.Keys(ctx, "k*"))
	eq(t, len(keys), 3)
	rk, ok, _ := c.RandomKey(ctx)
	if !ok || rk == "" {
		t.Fatal("randomkey")
	}
	noErr(t, c.Rename(ctx, "k1", "k1b"))
	eq(t, mb(c.RenameNX(ctx, "k1b", "k2")), false)
	eq(t, mb(c.RenameNX(ctx, "k1b", "k1")), true)
	eq(t, mb(c.Copy(ctx, "k1", "k1c", false)), true)
	eq(t, mb(c.Copy(ctx, "k2", "k1c", false)), false)
	eq(t, mb(c.Copy(ctx, "k2", "k1c", true)), true)
	eq(t, mi(c.Del(ctx, "k1c", "nope")), int64(1))
	eq(t, mi(c.Unlink(ctx, "k3")), int64(1))

	eq(t, mb(c.Expire(ctx, "k1", 100*time.Second)), true)
	eq(t, mb(c.PExpire(ctx, "k1", 50*time.Second)), true)
	at := time.Now().Add(time.Hour)
	eq(t, mb(c.ExpireAt(ctx, "k1", at)), true)
	if et := mi(c.ExpireTime(ctx, "k1")); et < at.Unix()-1 || et > at.Unix()+1 {
		t.Fatalf("expiretime %d vs %d", et, at.Unix())
	}
	eq(t, mb(c.PExpireAt(ctx, "k1", at)), true)
	if et := mi(c.PExpireTime(ctx, "k1")); et/1000 != at.UnixMilli()/1000 {
		t.Fatalf("pexpiretime %d", et)
	}
	eq(t, mb(c.Persist(ctx, "k1")), true)
	eq(t, mi(c.TTL(ctx, "k1")), int64(-1))
	eq(t, mi(c.PTTL(ctx, "nope")), int64(-2))

	// SCAN pages through everything
	for i := 0; i < 30; i++ {
		noErr(t, c.Set(ctx, "scan:"+string(rune('a'+i%26))+string(rune('0'+i/26)), "x"))
	}
	seen := map[string]bool{}
	cursor := "0"
	for {
		next, page, err := c.Scan(ctx, cursor, ScanOptions{Match: "scan:*", Count: 7})
		noErr(t, err)
		for _, k := range page {
			seen[k] = true
		}
		if next == "0" {
			break
		}
		cursor = next
	}
	eq(t, len(seen), 30)
	it := c.ScanIter(ScanOptions{Match: "scan:*", Count: 5, Type: "string"})
	n := 0
	for it.Next(ctx) {
		n++
	}
	noErr(t, it.Err())
	eq(t, n, 30)

	enc, ok, err := c.ObjectEncoding(ctx, "k1")
	noErr(t, err)
	if !ok || enc == "" {
		t.Fatal("object encoding")
	}
	if _, err := c.ObjectIdleTime(ctx, "k1"); err == nil {
		t.Log("OBJECT IDLETIME answered (LRU policy configured)")
	}
	if _, err := c.ObjectFreq(ctx, "k1"); err == nil {
		t.Log("OBJECT FREQ answered (LFU policy configured)")
	}
	mu, ok, err := c.MemoryUsage(ctx, "k1", 0)
	noErr(t, err)
	if !ok || mu <= 0 {
		t.Fatal("memory usage")
	}
	_, _, err = c.MemoryUsage(ctx, "k1", 5)
	noErr(t, err)

	payload, ok, err := c.Dump(ctx, "k1")
	noErr(t, err)
	eq(t, ok, true)
	noErr(t, c.Restore(ctx, "k1r", 0, payload, RestoreOptions{}))
	var se *Error
	if err := c.Restore(ctx, "k1r", 0, payload, RestoreOptions{}); !errors.As(err, &se) {
		t.Fatalf("restore over existing key must fail: %v", err)
	}
	noErr(t, c.Restore(ctx, "k1r", 60000, payload, RestoreOptions{Replace: true}))
	noErr(t, c.Restore(ctx, "k1a", time.Now().Add(time.Hour).UnixMilli(), payload, RestoreOptions{AbsTTL: true}))
	v, _, _ := c.Get(ctx, "k1r")
	eq(t, v, "a")

	// MOVE / SWAPDB / SELECT on a dedicated connection
	d, err := c.Conn(ctx)
	noErr(t, err)
	defer d.Close()
	noErr(t, d.Select(ctx, testDB+1))
	noErr(t, d.FlushDB(ctx))
	noErr(t, d.Select(ctx, testDB))
	eq(t, mb(d.Move(ctx, "k1r", testDB+1)), true)
	noErr(t, d.SwapDB(ctx, testDB, testDB+1))
	eq(t, mi(d.Exists(ctx, "k1r")), int64(1))
	noErr(t, d.SwapDB(ctx, testDB, testDB+1))
	noErr(t, d.Select(ctx, testDB+1))
	noErr(t, d.FlushDB(ctx))
}

func TestLiveBitsAndHLL(t *testing.T) {
	c, ctx := live(t)
	mi := must[int64](t)
	eq(t, mi(c.SetBit(ctx, "b", 7, 1)), int64(0))
	eq(t, mi(c.GetBit(ctx, "b", 7)), int64(1))
	eq(t, mi(c.BitCount(ctx, "b")), int64(1))
	eq(t, mi(c.BitCountRange(ctx, "b", 0, 0, false)), int64(1))
	eq(t, mi(c.BitCountRange(ctx, "b", 0, 6, true)), int64(0))
	eq(t, mi(c.BitPos(ctx, "b", 1)), int64(7))
	eq(t, mi(c.BitPos(ctx, "b", 1, 0, 0)), int64(7))
	noErr(t, c.Set(ctx, "x1", "\xff"))
	noErr(t, c.Set(ctx, "x2", "\x0f"))
	eq(t, mi(c.BitOp(ctx, "AND", "x3", "x1", "x2")), int64(1))
	v, _, _ := c.Get(ctx, "x3")
	eq(t, v, "\x0f")
	res, err := c.BitField(ctx, "bf", BitFieldOp{Op: "SET", Type: "u8", Offset: "0", Value: 200},
		BitFieldOp{Op: "INCRBY", Type: "u8", Offset: "0", Value: 100},
		BitFieldOp{Op: "OVERFLOW", Type: "FAIL"},
		BitFieldOp{Op: "INCRBY", Type: "u8", Offset: "0", Value: 250},
		BitFieldOp{Op: "GET", Type: "u8", Offset: "0"})
	noErr(t, err)
	eq(t, res, []NullInt{{0, true}, {44, true}, {}, {44, true}})
	ro, err := c.BitFieldRO(ctx, "bf", BitFieldOp{Op: "GET", Type: "u8", Offset: "0"})
	noErr(t, err)
	eq(t, ro, []NullInt{{44, true}})

	eq(t, must[bool](t)(c.PFAdd(ctx, "h1", "a", "b", "c")), true)
	noErr(t, must2(c.PFAdd(ctx, "h2", "c", "d")))
	eq(t, mi(c.PFCount(ctx, "h1")), int64(3))
	eq(t, mi(c.PFCount(ctx, "h1", "h2")), int64(4))
	noErr(t, c.PFMerge(ctx, "h3", "h1", "h2"))
	eq(t, mi(c.PFCount(ctx, "h3")), int64(4))
}

func must2[T any](_ T, err error) error { return err }
