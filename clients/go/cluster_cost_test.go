// Copyright (c) 2026 Salih Cantekin. MIT License; see LICENSE.

package ruvio

import (
	"context"
	"fmt"
	"os"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

// TestClusterCost prints client CPU and allocation per request for the plain client (one
// connection, no cluster routing), the cluster client with keys spread over the nodes, and
// the cluster client with every key in one hash tag (the routing code's own cost):
//
//	RUVIO_COST_ADDR=127.0.0.1:7000 go test -run TestClusterCost -v -count=1
//
// Point it at a per-shard server. Eight workers with a client each, GET and SET at pipeline
// depths 1, 16 and 64, RUVIO_COST_SECONDS (default 4) per row.
func TestClusterCost(t *testing.T) {
	addr := os.Getenv("RUVIO_COST_ADDR")
	if addr == "" {
		t.Skip("RUVIO_COST_ADDR not set")
	}
	seconds := 4
	if s := os.Getenv("RUVIO_COST_SECONDS"); s != "" {
		fmt.Sscan(s, &seconds)
	}
	for _, op := range []string{"GET", "SET"} {
		for _, depth := range []int{1, 16, 64} {
			for _, mode := range []string{"plain", "cluster", "one-node"} {
				costRow(t, addr, mode, op, depth, time.Duration(seconds)*time.Second)
			}
		}
	}
}

func costRow(t *testing.T, addr, mode, op string, depth int, run time.Duration) {
	const workers = 8
	tag := ""
	if mode == "one-node" {
		tag = "{t}"
	}
	keys := make([]string, 4096)
	for i := range keys {
		keys[i] = fmt.Sprintf("%skey:%d", tag, i)
	}
	value := strings.Repeat("x", 32)
	ctx := context.Background()
	clients := make([]*Client, workers)
	for w := range clients {
		c, err := Connect(ctx, Options{Addr: addr, Cluster: mode != "plain"})
		if err != nil {
			t.Fatal(err)
		}
		defer c.Close()
		clients[w] = c
	}
	step := func(c *Client, p *Pipeline, i int) {
		if depth == 1 {
			var err error
			if op == "SET" {
				err = c.Set(ctx, keys[i], value)
			} else {
				_, _, err = c.Get(ctx, keys[i])
			}
			if err != nil {
				t.Error(err)
			}
			return
		}
		for j := 0; j < depth; j++ {
			if op == "SET" {
				p.Set(keys[i+j], value)
			} else {
				p.Get(keys[i+j])
			}
		}
		vals, err := p.Exec(ctx)
		if err != nil || len(vals) != depth {
			t.Error(err, len(vals))
		}
	}
	loop := func(w int, until time.Time) uint64 {
		c := clients[w]
		p := c.Pipeline()
		span := len(keys) - depth
		i := w * 997 % span
		var ops uint64
		for time.Now().Before(until) {
			step(c, p, i)
			ops += uint64(depth)
			i = (i + depth) % span
		}
		return ops
	}
	all := func(until time.Time) uint64 {
		var wg sync.WaitGroup
		counts := make([]uint64, workers)
		for w := range clients {
			wg.Add(1)
			go func(w int) { defer wg.Done(); counts[w] = loop(w, until) }(w)
		}
		wg.Wait()
		var sum uint64
		for _, n := range counts {
			sum += n
		}
		return sum
	}
	all(time.Now().Add(500 * time.Millisecond))
	runtime.GC()
	var ru0, ru1 syscall.Rusage
	var m0, m1 runtime.MemStats
	runtime.ReadMemStats(&m0)
	syscall.Getrusage(syscall.RUSAGE_SELF, &ru0)
	start := time.Now()
	ops := float64(all(start.Add(run)))
	elapsed := time.Since(start).Seconds()
	syscall.Getrusage(syscall.RUSAGE_SELF, &ru1)
	runtime.ReadMemStats(&m1)
	ns := func(a, b syscall.Timeval) float64 {
		return float64((b.Sec-a.Sec)*1e9+int64(b.Usec-a.Usec)*1e3) / ops
	}
	fmt.Printf("%-8s %s P=%d ops/s=%.0f user_ns/op=%.0f sys_ns/op=%.0f allocs/op=%.2f bytes/op=%.0f\n",
		mode, op, depth, ops/elapsed, ns(ru0.Utime, ru1.Utime), ns(ru0.Stime, ru1.Stime),
		float64(m1.Mallocs-m0.Mallocs)/ops, float64(m1.TotalAlloc-m0.TotalAlloc)/ops)
}
