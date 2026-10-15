// Copyright (c) 2026 Salih Cantekin. MIT License; see LICENSE.

package ruvio

import (
	"context"
	"errors"
	"os"
	"reflect"
	"sync"
	"testing"
	"time"
)

const testDB = 11

// live returns a client on RUVIO_TEST_ADDR (database 11, flushed) or skips.
func live(t testing.TB) (*Client, context.Context) {
	t.Helper()
	addr := os.Getenv("RUVIO_TEST_ADDR")
	if addr == "" {
		t.Skip("RUVIO_TEST_ADDR not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	c, err := Connect(ctx, Options{Addr: addr, DB: testDB})
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	if err := c.FlushDB(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close(); cancel() })
	return c, ctx
}

func must[T any](t testing.TB) func(v T, err error) T {
	return func(v T, err error) T {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
		return v
	}
}

func noErr(t testing.TB, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func eq(t testing.TB, got, want any) {
	t.Helper()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %#v, want %#v", got, want)
	}
}

func TestLiveConcurrencyAndCoalescing(t *testing.T) {
	c, ctx := live(t)
	const workers, per = 32, 200
	var wg sync.WaitGroup
	errs := make(chan error, workers)
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			key := "conc:" + string(rune('a'+w%26)) + string(rune('A'+w/26))
			for i := 0; i < per; i++ {
				n, err := c.Incr(ctx, key)
				if err != nil {
					errs <- err
					return
				}
				if n != int64(i+1) {
					errs <- errors.New("out-of-order reply")
					return
				}
			}
		}(w)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	st := c.Stats()
	if st.Writes >= workers*per {
		t.Logf("no coalescing observed: %d writes for %d commands", st.Writes, workers*per)
	} else {
		t.Logf("%d commands in %d writes", workers*per, st.Writes)
	}
}

func TestLiveContextCancelAndReuse(t *testing.T) {
	c, ctx := live(t)
	// a canceled call must not desynchronize the FIFO for later callers
	cctx, cancel := context.WithCancel(ctx)
	cancel()
	if _, _, err := c.Get(cctx, "x"); !errors.Is(err, context.Canceled) {
		t.Fatalf("want canceled, got %v", err)
	}
	for i := 0; i < 50; i++ {
		tctx, tc := context.WithTimeout(ctx, time.Nanosecond)
		c.Set(tctx, "x", "1")
		tc()
	}
	noErr(t, c.Set(ctx, "x", "v"))
	v, ok, err := c.Get(ctx, "x")
	if err != nil || !ok || v != "v" {
		t.Fatalf("after cancels: %v %v %v", v, ok, err)
	}
}

func TestLiveErrorsAndDo(t *testing.T) {
	c, ctx := live(t)
	noErr(t, c.Set(ctx, "s", "x"))
	_, err := c.LPush(ctx, "s", "a")
	var e *Error
	if !errors.As(err, &e) || e.Prefix() != "WRONGTYPE" {
		t.Fatalf("want WRONGTYPE, got %v", err)
	}
	v, err := c.Do(ctx, "SET", "d", []byte{0, 1, 2})
	noErr(t, err)
	eq(t, v.Str(), "OK")
	b, ok, err := c.GetBytes(ctx, "d")
	noErr(t, err)
	eq(t, ok, true)
	eq(t, b, []byte{0, 1, 2})
	v, err = c.DoBytes(ctx, []byte("GET"), []byte("d"))
	noErr(t, err)
	eq(t, v.Bytes, []byte{0, 1, 2})
	if _, err := c.Do(ctx, "NOSUCHCOMMAND"); !errors.As(err, &e) {
		t.Fatalf("want server error, got %v", err)
	}
}

func TestLiveReconnect(t *testing.T) {
	c, ctx := live(t)
	noErr(t, c.Set(ctx, "r", "1"))
	id := must[int64](t)(c.ClientID(ctx))
	// kill our own shared connection from a dedicated one
	d, err := c.Conn(ctx)
	noErr(t, err)
	skip := false
	must[int64](t)(d.ClientKill(ctx, ClientKillOptions{ID: id, SkipMe: &skip}))
	d.Close()
	// the next call may see the dead connection once; then it redials (DB re-selected)
	var v string
	var ok bool
	for i := 0; i < 3; i++ {
		v, ok, err = c.Get(ctx, "r")
		if err == nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err != nil || !ok || v != "1" {
		t.Fatalf("after reconnect: %q %v %v", v, ok, err)
	}
}

func TestLiveSingleCommandPipeline(t *testing.T) {
	c, ctx := live(t)
	vals, err := c.Pipelined(ctx, func(p *Pipeline) { p.Set("one", "1") })
	noErr(t, err)
	eq(t, len(vals), 1)
	eq(t, vals[0].Str(), "OK")
}
