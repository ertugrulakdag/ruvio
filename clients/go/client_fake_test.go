// Copyright (c) 2026 Salih Cantekin. MIT License; see LICENSE.

package ruvio

import (
	"context"
	"errors"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func okFake(t *testing.T) *fakeNode {
	return newFakeNode(t, func(f *fakeNode, args []string) string {
		switch strings.ToUpper(args[0]) {
		case "PING":
			return "+PONG\r\n"
		case "GET":
			return bulk("value")
		}
		return "+OK\r\n"
	})
}

func TestHandshakeCommandsFollowTheOptions(t *testing.T) {
	cases := []struct {
		name string
		opts Options
		want []string
	}{
		{"plain", Options{}, []string{"PING"}},
		{"password", Options{Password: "pw"}, []string{"AUTH pw", "PING"}},
		{"user and password", Options{Username: "bob", Password: "pw"}, []string{"AUTH bob pw", "PING"}},
		{"resp3", Options{Protocol: 3}, []string{"HELLO 3", "PING"}},
		{"resp3 auth defaults the user", Options{Protocol: 3, Password: "pw"}, []string{"HELLO 3 AUTH default pw", "PING"}},
		{"resp3 auth with user", Options{Protocol: 3, Username: "bob", Password: "pw"}, []string{"HELLO 3 AUTH bob pw", "PING"}},
		{"database", Options{DB: 4}, []string{"SELECT 4", "PING"}},
		{"client name", Options{ClientName: "svc"}, []string{"CLIENT SETNAME svc", "PING"}},
		{"everything", Options{Password: "pw", DB: 2, ClientName: "svc"}, []string{"AUTH pw", "SELECT 2", "CLIENT SETNAME svc", "PING"}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			f := okFake(t)
			c.opts.Addr = f.addr
			cl, err := Connect(context.Background(), c.opts)
			if err != nil {
				t.Fatal(err)
			}
			defer cl.Close()
			if got := f.commands(); strings.Join(got, "|") != strings.Join(c.want, "|") {
				t.Fatalf("got %v want %v", got, c.want)
			}
		})
	}
}

func TestConnectFailures(t *testing.T) {
	// The handshake is refused: Connect reports the server error and the client is unusable.
	denied := newFakeNode(t, func(f *fakeNode, args []string) string {
		if args[0] == "AUTH" {
			return "-WRONGPASS invalid username-password pair\r\n"
		}
		return "+OK\r\n"
	})
	_, err := Connect(context.Background(), Options{Addr: denied.addr, Password: "bad"})
	var se *Error
	if !errors.As(err, &se) || se.Prefix() != "WRONGPASS" {
		t.Fatalf("want WRONGPASS, got %v", err)
	}

	badDB := newFakeNode(t, func(f *fakeNode, args []string) string {
		if args[0] == "SELECT" {
			return "-ERR DB index is out of range\r\n"
		}
		return "+OK\r\n"
	})
	if _, err := Connect(context.Background(), Options{Addr: badDB.addr, DB: 99}); err == nil {
		t.Fatal("SELECT failure must fail Connect")
	}

	badName := newFakeNode(t, func(f *fakeNode, args []string) string {
		if args[0] == "CLIENT" {
			return "-ERR no\r\n"
		}
		return "+OK\r\n"
	})
	if _, err := Connect(context.Background(), Options{Addr: badName.addr, ClientName: "x"}); err == nil {
		t.Fatal("CLIENT SETNAME failure must fail Connect")
	}

	badHello := newFakeNode(t, func(f *fakeNode, args []string) string { return "-ERR unknown command 'HELLO'\r\n" })
	if _, err := Connect(context.Background(), Options{Addr: badHello.addr, Protocol: 3}); err == nil {
		t.Fatal("HELLO failure must fail Connect")
	}

	// Nothing listens: the dial error comes back and is not retried forever.
	ln, _ := net.Listen("tcp", "127.0.0.1:0")
	addr := ln.Addr().String()
	ln.Close()
	if _, err := Connect(context.Background(), Options{Addr: addr, DialTimeout: time.Second}); err == nil {
		t.Fatal("dialing a closed port must fail")
	}
}

func TestCloseAndUseAfterClose(t *testing.T) {
	f := okFake(t)
	cl, err := Connect(context.Background(), Options{Addr: f.addr})
	if err != nil {
		t.Fatal(err)
	}
	if err := cl.Close(); err != nil {
		t.Fatal(err)
	}
	if err := cl.Close(); err != nil {
		t.Fatalf("second Close must be a no-op, got %v", err)
	}
	if _, _, err := cl.Get(context.Background(), "k"); !errors.Is(err, ErrClosed) {
		t.Fatalf("want ErrClosed after Close, got %v", err)
	}
}

func TestServerErrorsSurfaceAsTypedErrors(t *testing.T) {
	f := newFakeNode(t, func(f *fakeNode, args []string) string {
		switch args[0] {
		case "GET":
			return "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"
		case "INCR":
			return "-ERR value is not an integer or out of range\r\n"
		}
		return "+OK\r\n"
	})
	cl := NewClient(Options{Addr: f.addr})
	defer cl.Close()

	_, _, err := cl.Get(context.Background(), "k")
	var se *Error
	if !errors.As(err, &se) || se.Prefix() != "WRONGTYPE" {
		t.Fatalf("got %v", err)
	}
	if _, err := cl.Incr(context.Background(), "k"); !errors.As(err, &se) || se.Prefix() != "ERR" {
		t.Fatalf("got %v", err)
	}
	// The connection stays usable after an error reply.
	if err := cl.Ping(context.Background()); err != nil {
		t.Fatalf("ping after errors: %v", err)
	}
}

func TestDroppedConnectionIsReplacedOnTheNextCall(t *testing.T) {
	var dropped atomic.Bool
	f := newFakeNode(t, func(f *fakeNode, args []string) string {
		if args[0] == "GET" && !dropped.Swap(true) {
			return "DROP"
		}
		if args[0] == "GET" {
			return bulk("again")
		}
		return "+PONG\r\n"
	})
	cl := NewClient(Options{Addr: f.addr})
	defer cl.Close()

	if _, _, err := cl.Get(context.Background(), "k"); err == nil {
		t.Fatal("a dropped connection must fail the in-flight call")
	}
	v, _, err := cl.Get(context.Background(), "k")
	if err != nil || v != "again" {
		t.Fatalf("redial: %q %v", v, err)
	}
	if f.accepts.Load() != 2 {
		t.Fatalf("expected a second connection, accepts=%d", f.accepts.Load())
	}
}

func TestCancelledAndExpiredContexts(t *testing.T) {
	f := okFake(t)
	cl := NewClient(Options{Addr: f.addr})
	defer cl.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, _, err := cl.Get(ctx, "k"); err == nil {
		t.Fatal("a cancelled context must fail the call")
	}

	slow := newFakeNode(t, func(f *fakeNode, args []string) string {
		if args[0] == "GET" {
			time.Sleep(300 * time.Millisecond)
		}
		return "+OK\r\n"
	})
	cl2 := NewClient(Options{Addr: slow.addr})
	defer cl2.Close()
	ctx2, cancel2 := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel2()
	start := time.Now()
	if _, _, err := cl2.Get(ctx2, "k"); err == nil {
		t.Fatal("a slow reply must hit the context deadline")
	}
	if time.Since(start) > 250*time.Millisecond {
		t.Fatalf("the call waited %v instead of honouring the deadline", time.Since(start))
	}
}

func TestCustomDialerIsUsed(t *testing.T) {
	f := okFake(t)
	dials := 0
	cl, err := Connect(context.Background(), Options{
		Addr: f.addr,
		Dial: func(ctx context.Context, network, addr string) (net.Conn, error) {
			dials++
			var d net.Dialer
			return d.DialContext(ctx, network, addr)
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	if dials != 1 {
		t.Fatalf("custom dialer used %d times", dials)
	}

	failing := NewClient(Options{Dial: func(ctx context.Context, network, addr string) (net.Conn, error) {
		return nil, errors.New("no route")
	}})
	defer failing.Close()
	if err := failing.Ping(context.Background()); err == nil || !strings.Contains(err.Error(), "no route") {
		t.Fatalf("dial error must surface, got %v", err)
	}
}
