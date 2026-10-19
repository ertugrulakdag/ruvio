// Copyright (c) 2026 Salih Cantekin. MIT License; see LICENSE.

package ruvio

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"
)

// A server that accepts and never answers: a half-open peer.
func silentServer(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			t.Cleanup(func() { c.Close() })
		}
	}()
	return ln.Addr().String()
}

func TestCommandTimeoutBoundsACallWithoutDeadline(t *testing.T) {
	client := NewClient(Options{Addr: silentServer(t), CommandTimeout: 150 * time.Millisecond})
	defer client.Close()

	start := time.Now()
	_, _, err := client.Get(context.Background(), "k")
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("want DeadlineExceeded, got %v", err)
	}
	if elapsed := time.Since(start); elapsed > 3*time.Second {
		t.Fatalf("took %v", elapsed)
	}
}
