// Copyright (c) 2026 Salih Cantekin. MIT License; see LICENSE.

package ruvio

import (
	"context"
	"errors"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestLivePipelineResultsErrorsAndReuse(t *testing.T) {
	c, ctx := live(t)

	p := c.Pipeline()
	a := p.Set("pk", "1")
	b := p.Incr("pk")
	d := p.Do("NOSUCHCOMMAND")
	e := p.Get("pk")
	f := p.DoBytes([]byte("GET"), []byte("pk"))
	if a != 0 || b != 1 || d != 2 || e != 3 || f != 4 {
		t.Fatalf("indexes %d %d %d %d %d", a, b, d, e, f)
	}
	vals, err := p.Exec(ctx)
	if len(vals) != 5 {
		t.Fatalf("want 5 replies even with one error, got %d (err %v)", len(vals), err)
	}
	var se *Error
	if !errors.As(err, &se) {
		t.Fatalf("the first error reply must be returned, got %v", err)
	}
	if vals[1].Str() != "2" || vals[3].Str() != "2" || vals[4].Str() != "2" || vals[2].Kind != KindError {
		t.Fatalf("replies out of order: %v", vals)
	}

	// A pipeline is reusable after Exec; an empty one is a no-op.
	p.Get("pk")
	if vals, err = p.Exec(ctx); err != nil || len(vals) != 1 || vals[0].Str() != "2" {
		t.Fatalf("reuse: %v %v", vals, err)
	}
	if vals, err = p.Exec(ctx); err != nil || len(vals) != 0 {
		t.Fatalf("empty exec: %v %v", vals, err)
	}

	// An argument that cannot be encoded is reported at Exec and does not poison the commands before it.
	bad := c.Pipeline()
	bad.Set("pk2", "x")
	if idx := bad.Do("SET", "pk3", struct{}{}); idx != -1 {
		t.Fatalf("an unencodable argument must return -1, got %d", idx)
	}
	if _, err := bad.Exec(ctx); err == nil {
		t.Fatal("Exec must report the encoding error")
	}

	vals, err = c.Pipelined(ctx, func(p *Pipeline) {
		p.SetPX("px", "v", 60000)
		p.HSet("ph", "f", "v")
		p.HGet("ph", "f")
		p.LPush("pl", "a", "b")
		p.PExpire("px", 120000)
		p.IncrBy("cnt", 5)
		p.Del("px")
	})
	if err != nil || len(vals) != 7 || vals[2].Str() != "v" || vals[5].Str() != "5" || vals[6].Str() != "1" {
		t.Fatalf("pipelined: %v %v", vals, err)
	}
}

func TestLiveBlockingPopsDoNotBlockTheSharedConnection(t *testing.T) {
	c, ctx := live(t)

	// Nothing to pop: the call returns after the timeout with ok=false while other calls keep flowing.
	done := make(chan struct{})
	go func() {
		defer close(done)
		kv, ok, err := c.BLPop(ctx, 300*time.Millisecond, "empty-list")
		if err != nil || ok || kv != (KeyValue{}) {
			t.Errorf("timeout: %v %v %v", kv, ok, err)
		}
	}()
	time.Sleep(50 * time.Millisecond)
	start := time.Now()
	if err := c.Ping(ctx); err != nil || time.Since(start) > 200*time.Millisecond {
		t.Fatalf("the shared connection was blocked: %v after %v", err, time.Since(start))
	}
	<-done

	// A producer wakes a waiting consumer.
	go func() {
		time.Sleep(100 * time.Millisecond)
		c.RPush(ctx, "wake", "item")
	}()
	kv, ok, err := c.BLPop(ctx, 3*time.Second, "wake")
	if err != nil || !ok || kv.Key != "wake" || kv.Value != "item" {
		t.Fatalf("wake: %v %v %v", kv, ok, err)
	}

	// A cancelled context ends the wait and the dedicated connection is discarded, not reused.
	cctx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()
	if _, _, err := c.BRPop(cctx, 5*time.Second, "never"); err == nil {
		t.Fatal("a context deadline must end BRPOP with an error")
	}
	if err := c.Ping(ctx); err != nil {
		t.Fatalf("client unusable after a cancelled blocking call: %v", err)
	}
}

func TestLiveStatsFlushAllAndServerErrorsOnTypedCalls(t *testing.T) {
	c, ctx := live(t)

	before := c.Stats()
	for i := 0; i < 5; i++ {
		if err := c.Set(ctx, "s", "v"); err != nil {
			t.Fatal(err)
		}
	}
	after := c.Stats()
	if after.Writes <= before.Writes || after.BytesWritten <= before.BytesWritten {
		t.Fatalf("stats did not advance: %+v -> %+v", before, after)
	}
	if (NewClient(Options{Addr: "127.0.0.1:1"}).Stats()) != (Stats{}) {
		t.Fatal("a client that never connected reports zero stats")
	}

	// WRONGTYPE comes back as a typed *Error from typed calls.
	c.Set(ctx, "str", "v")
	var se *Error
	if _, err := c.LPush(ctx, "str", "x"); !errors.As(err, &se) || se.Prefix() != "WRONGTYPE" {
		t.Fatalf("got %v", err)
	}
	if _, err := c.ZPopMax(ctx, "str", 1); !errors.As(err, &se) {
		t.Fatalf("ZPopMax on a string: %v", err)
	}

	c.ZAdd(ctx, "zp", Z{"a", 1}, Z{"b", 2}, Z{"c", 3})
	top, err := c.ZPopMax(ctx, "zp", 2)
	if err != nil || len(top) != 2 || top[0] != (Z{"c", 3}) || top[1] != (Z{"b", 2}) {
		t.Fatalf("ZPopMax: %v %v", top, err)
	}
	if none, err := c.ZPopMax(ctx, "no-such-zset", 1); err != nil || len(none) != 0 {
		t.Fatalf("ZPopMax on a missing key: %v %v", none, err)
	}

	// Streams: group creation with and without MKSTREAM, and BUSYGROUP on a repeat.
	if err := c.XGroupCreate(ctx, "missing-stream", "g", "$", false); err == nil {
		t.Fatal("XGROUP CREATE without MKSTREAM on a missing stream must fail")
	}
	if err := c.XGroupCreate(ctx, "made-stream", "g", "$", true); err != nil {
		t.Fatal(err)
	}
	if err := c.XGroupCreate(ctx, "made-stream", "g", "$", true); !errors.As(err, &se) || se.Prefix() != "BUSYGROUP" {
		t.Fatalf("want BUSYGROUP, got %v", err)
	}

	// Indexes live in database 0 only.
	if err := c.JSONIndexCreate(ctx, "idx_extra", "doc:", "$.name", ""); !errors.As(err, &se) {
		t.Fatalf("an index outside database 0 must be refused, got %v", err)
	}
	c0, err := Connect(ctx, Options{Addr: os.Getenv("RUVIO_TEST_ADDR")})
	if err != nil {
		t.Fatal(err)
	}
	defer c0.Close()
	c0.JSONIndexDrop(ctx, "idx_extra")
	c0.JSONIndexDrop(ctx, "idx_extra_n")
	if err := c0.JSONIndexCreate(ctx, "idx_extra", "doc:", "$.name", ""); err != nil {
		t.Fatalf("index without a type: %v", err)
	}
	if err := c0.JSONIndexCreate(ctx, "idx_extra_n", "num:", "$.n", "number"); err != nil {
		t.Fatalf("index with a type: %v", err)
	}
	defer c0.JSONIndexDrop(ctx, "idx_extra")
	defer c0.JSONIndexDrop(ctx, "idx_extra_n")
	if err := c0.JSONIndexCreate(ctx, "idx_extra", "doc:", "$.name", ""); err == nil {
		t.Fatal("creating the same index twice must fail")
	}

	// Do passes through arbitrary commands and surfaces server errors.
	if v, err := c.Do(ctx, "ECHO", "hi"); err != nil || v.Str() != "hi" {
		t.Fatalf("Do: %v %v", v, err)
	}
	if _, err := c.Do(ctx, "NOSUCHCOMMAND"); !errors.As(err, &se) {
		t.Fatalf("Do must return the server error, got %v", err)
	}
	if _, err := c.Do(ctx, "SET", "k", struct{}{}); err == nil {
		t.Fatal("Do must reject an argument it cannot encode")
	}
	if _, err := c.DoBytes(ctx, []byte("ECHO"), []byte("bin\x00ary")); err != nil {
		t.Fatalf("DoBytes: %v", err)
	}
}

func TestLiveGetOrCreateRunsTheFactoryOnceUnderContention(t *testing.T) {
	c, ctx := live(t)

	var calls atomic.Int32
	var wg sync.WaitGroup
	results := make([]string, 8)
	for i := range results {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			v, err := c.GetOrCreate(ctx, "goc", time.Minute, func(ctx context.Context) (string, error) {
				calls.Add(1)
				time.Sleep(150 * time.Millisecond)
				return "computed", nil
			})
			if err != nil {
				t.Error(err)
			}
			results[i] = v
		}(i)
	}
	wg.Wait()
	if calls.Load() != 1 {
		t.Fatalf("factory ran %d times for 8 concurrent callers", calls.Load())
	}
	for i, v := range results {
		if v != "computed" {
			t.Fatalf("caller %d got %q", i, v)
		}
	}

	// A failing factory returns its error and does not cache anything: the next caller computes.
	boom := errors.New("boom")
	if _, err := c.GetOrCreate(ctx, "goc-fail", time.Minute, func(context.Context) (string, error) { return "", boom }, GetOrCreateOptions{FillTimeout: 100 * time.Millisecond}); !errors.Is(err, boom) {
		t.Fatalf("want the factory error, got %v", err)
	}
	time.Sleep(150 * time.Millisecond) // the fill marker lapses after FillTimeout
	v, err := c.GetOrCreate(ctx, "goc-fail", time.Minute, func(context.Context) (string, error) { return "second", nil })
	if err != nil || v != "second" {
		t.Fatalf("after a failed fill: %q %v", v, err)
	}

	// A cancelled context while another caller holds the fill ends the wait.
	c.Set(ctx, "unrelated", "x")
	short, cancel := context.WithTimeout(ctx, 60*time.Millisecond)
	defer cancel()
	go c.GetOrCreate(ctx, "goc-slow", time.Minute, func(context.Context) (string, error) {
		time.Sleep(400 * time.Millisecond)
		return "late", nil
	})
	time.Sleep(30 * time.Millisecond)
	if _, err := c.GetOrCreate(short, "goc-slow", time.Minute, func(context.Context) (string, error) { return "mine", nil }); err == nil {
		t.Fatal("a waiting caller must give up when its context ends")
	}
}

func TestLiveLockHelpers(t *testing.T) {
	c, ctx := live(t)

	// WithLock refuses a second holder and always releases.
	err := c.WithLock(ctx, "lk", time.Minute, func(ctx context.Context) error {
		if err2 := c.WithLock(ctx, "lk", time.Minute, func(context.Context) error { return nil }); !errors.Is(err2, ErrNotAcquired) {
			t.Errorf("nested WithLock: %v", err2)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	l, ok, err := c.AcquireLock(ctx, "lk", time.Minute)
	if err != nil || !ok {
		t.Fatalf("lock must be free again after WithLock: %v %v", ok, err)
	}
	// Refresh works for the owner and Unlock is idempotent in effect (second one reports false).
	if ok, err := l.Refresh(ctx, time.Minute); err != nil || !ok {
		t.Fatalf("refresh: %v %v", ok, err)
	}
	if ok, err := l.Unlock(ctx); err != nil || !ok {
		t.Fatalf("unlock: %v %v", ok, err)
	}
	if ok, _ := l.Unlock(ctx); ok {
		t.Fatal("a second Unlock must report not-owned")
	}
	if ok, _ := l.Refresh(ctx, time.Minute); ok {
		t.Fatal("Refresh after Unlock must fail")
	}

	// AcquireLockWait waits for a holder to leave and honours its context.
	held, _, _ := c.AcquireLock(ctx, "lk-wait", 300*time.Millisecond)
	_ = held
	start := time.Now()
	got, err := c.AcquireLockWait(ctx, "lk-wait", time.Minute, 20*time.Millisecond)
	if err != nil || got == nil || time.Since(start) < 150*time.Millisecond {
		t.Fatalf("wait: %v %v after %v", got, err, time.Since(start))
	}
	short, cancel := context.WithTimeout(ctx, 80*time.Millisecond)
	defer cancel()
	if _, err := c.AcquireLockWait(short, "lk-wait", time.Minute, 10*time.Millisecond); err == nil {
		t.Fatal("AcquireLockWait must return the context error while the lock stays held")
	}

	// Fenced lease tokens increase and WithLease passes the token to the callback.
	var first, second int64
	if err := c.WithLease(ctx, "ls", time.Minute, func(_ context.Context, tok int64) error { first = tok; return nil }); err != nil {
		t.Fatal(err)
	}
	if err := c.WithLease(ctx, "ls", time.Minute, func(_ context.Context, tok int64) error { second = tok; return nil }); err != nil {
		t.Fatal(err)
	}
	if second <= first {
		t.Fatalf("lease tokens must increase: %d then %d", first, second)
	}
	h, ok, _ := c.AcquireLease(ctx, "ls", time.Minute)
	if !ok {
		t.Fatal("lease should be free")
	}
	if err := c.WithLease(ctx, "ls", time.Minute, func(context.Context, int64) error { return nil }); !errors.Is(err, ErrNotAcquired) {
		t.Fatalf("held lease: %v", err)
	}
	if ok, err := h.Release(ctx); err != nil || !ok {
		t.Fatalf("release: %v %v", ok, err)
	}
	if ok, _ := h.Release(ctx); ok {
		t.Fatal("releasing twice must report not-holder")
	}
}
